mod command_processor;
mod parser;
mod redis_command;
mod redis_response;
mod storage;
mod types;
mod pubsub;

use crate::command_processor::CommandProcessor;
use crate::parser::Parser;
use crate::redis_command::RedisCommand;
use crate::redis_response::RedisResponse;
use crate::storage::Storage;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            handle_connection(stream, storage_clone).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream, storage: Storage) {
    let mut processor = CommandProcessor::new(storage);

    loop {
        let mut buf = [0; 512];
        match stream.read(&mut buf).await {
            Ok(0) => {
                println!("Connection closed by client");
                break;
            }
            Ok(bytes_read) => {
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
                let response = RedisResponse::from_result(result);
                stream.write_all(response.to_bytes()).await.unwrap();
            }
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        }
    }
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
