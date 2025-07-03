mod server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener};
use crate::server::{Command, Server};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let server = Server::new(stream);
                tokio::spawn(async move { handle_connection(server).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut server: Server) {
    loop {
        let mut buf = bytes::BytesMut::with_capacity(512);
        match server.reader.read_buf(&mut buf).await {
            Ok(0) => {
                println!("Connection closed by client");
                break;
            }
            Ok(bytes_read) => {
                let command_raw = String::from_utf8_lossy(&buf[..bytes_read]).to_string();
                println!("Bytes read: {}", bytes_read);
                println!("Command: {:?}", command_raw);
                let command = server.parse_resp_array(command_raw);

                match command {
                    Ok(Command::Ping) => {
                        server.writer.write_all(b"+PONG\r\n").await.unwrap();
                        server.writer.flush().await.unwrap();
                    },
                    Ok(Command::Echo(msg)) => {
                        let response = format!("${}\r\n{}\r\n", msg.len(), msg);
                        println!("Created response: {response}");
                        server.writer.write_all(response.as_bytes()).await.unwrap();
                        server.writer.flush().await.unwrap();
                    }
                    Err(_) => {
                        server.writer.write_all(b"-ERR unknown command\r\n").await.unwrap();
                        server.writer.flush().await.unwrap();
                    }
                }
            }
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        }
    }
}