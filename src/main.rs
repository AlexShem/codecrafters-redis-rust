mod parser;
mod redis_command;
mod redis_response;
mod types;

use crate::parser::Parser;
use crate::redis_command::RedisCommand;
use crate::redis_response::RedisResponse;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(stream).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream) {
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
                let response = RedisResponse::command_response(command);
                stream.write_all(response.to_bytes()).await.unwrap();
            }
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        }
    }
}
