mod blocking_list;
mod command_processor;
mod parser;
mod pubsub;
mod redis_command;
mod redis_response;
mod storage;
mod types;
mod geospatial;

use crate::blocking_list::{BlockedListResponse, BlockingListManager};
use crate::command_processor::CommandProcessor;
use crate::parser::Parser;
use crate::pubsub::{ClientId, PubSubManager};
use crate::redis_command::{CommandResult, RedisCommand};
use crate::redis_response::RedisResponse;
use crate::storage::Storage;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() {
    let (dir, dbfilename) = parse_args();
    let file_path = if let (Some(d), Some(f)) = (&dir, &dbfilename) {
        Some(PathBuf::from(d).join(f))
    } else {
        None
    };

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let storage = Storage::new(file_path, dir, dbfilename).await;
    let pub_sub_manager = PubSubManager::new();
    let blocking_list_manager = BlockingListManager::new();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let storage_clone = storage.clone();
        let pub_sub_manager_clone = pub_sub_manager.clone();
        let blocking_list_manager_clone = blocking_list_manager.clone();
        let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            handle_connection(
                stream,
                storage_clone,
                pub_sub_manager_clone,
                blocking_list_manager_clone,
                client_id,
            )
            .await;
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    storage: Storage,
    pub_sub_manager: PubSubManager,
    blocking_list_manager: BlockingListManager,
    client_id: ClientId,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (blocking_tx, mut blocking_rx) = tokio::sync::mpsc::unbounded_channel();

    pub_sub_manager.register_client(client_id, tx).await;

    let blocking_list_manager_clone = blocking_list_manager.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        loop {
            interval.tick().await;
            blocking_list_manager_clone.check_timeout().await;
        }
    });

    let (read_half, mut write_half) = stream.split();
    let mut reader = tokio::io::BufReader::new(read_half);

    let mut processor = CommandProcessor::new(
        storage,
        pub_sub_manager.clone(),
        blocking_list_manager,
        client_id,
        blocking_tx,
    );
    loop {
        tokio::select! {
            // Handle incoming commands from the client
            result = async {
                let mut buf = [0; 512];
                let bytes_read = reader.read(&mut buf).await?;
                Ok::<(usize, [u8; 512]), std::io::Error>((bytes_read, buf))
            } => {
                match result {
                    Ok((0, _)) => {
                        println!("Connection closed by client");
                        break;
                    }
                    Ok((bytes_read, buf)) => {
                        let command_bytes = bytes::Bytes::copy_from_slice(&buf[..bytes_read]);
                        let parser = Parser::new();

                        let command: RedisCommand = match parser.parse_command(command_bytes) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                eprintln!("Parse error: {}", e);
                                continue;
                            }
                        };

                        let result = processor.execute(command).await;

                        if !matches!(result, CommandResult::Blocked) {
                            let response = RedisResponse::from_result(result);
                            write_half.write_all(response.to_bytes()).await.unwrap();
                        }
                    }
                    Err(e) => {
                        println!("Failed to read from connection: {}", e);
                        break;
                    }
                }
            }

            // Handle pub/sub messages
            Some(pub_sub_msg) = rx.recv() => {
                use crate::redis_command::CommandResult;
                let message_result = CommandResult::Array(vec![
                    CommandResult::Value(Some(String::from("message"))),
                    CommandResult::Value(Some(pub_sub_msg.channel)),
                    CommandResult::Value(Some(pub_sub_msg.message)),
                ]);
                let response = RedisResponse::from_result(message_result);
                write_half.write_all(response.to_bytes()).await.unwrap();
            }

            Some(blocked_response) = blocking_rx.recv() => {
                match blocked_response {
                    BlockedListResponse::Element{ list_key, element } => {
                        let response = RedisResponse::from_result(CommandResult::Array(vec![
                            CommandResult::Value(Some(list_key)),
                            CommandResult::Value(Some(element))
                        ]));
                        if let Err(e) = write_half.write_all(response.to_bytes()).await {
                            eprintln!("Failed to write BLPOP response: {}", e);
                            break;
                        }
                    }
                    BlockedListResponse::Timeout{ .. } => {
                        let response = b"*-1\r\n";
                        if let Err(e) = write_half.write_all(response).await {
                            eprintln!("Failed to write timeout response: {}", e);
                            break;
                        }
                    }
                }
            }
        }
    }

    pub_sub_manager.unregister_client(client_id).await;
}

fn parse_args() -> (Option<String>, Option<String>) {
    let args: Vec<String> = std::env::args().collect();
    let mut dir = None;
    let mut dbfilename = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" => {
                if i + 1 < args.len() {
                    dir = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --dir requires a value");
                    i += 1;
                }
            }
            "--dbfilename" => {
                if i + 1 < args.len() {
                    dbfilename = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --dbfilename requires a value");
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    (dir, dbfilename)
}
