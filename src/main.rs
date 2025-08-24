mod parser;
mod redis_command;
mod redis_response;
mod types;
mod storage;
mod command_processor;

use crate::parser::Parser;
use crate::redis_command::RedisCommand;
use crate::redis_response::RedisResponse;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::command_processor::CommandProcessor;
use crate::storage::Storage;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let storage = Storage::new();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            handle_connection(stream, storage_clone).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream, storage: Storage) {
    let processor = CommandProcessor::new(storage);

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
