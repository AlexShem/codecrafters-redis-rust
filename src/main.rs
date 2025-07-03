use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(handle_connection(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) {
    loop {
        let mut buf = bytes::BytesMut::with_capacity(512);
        match stream.read_buf(&mut buf).await {
            Ok(0) => {
                println!("Connection closed by client");
                break;
            }
            Ok(bytes_read) => {
                let command = String::from_utf8_lossy(&buf[..bytes_read]);
                println!("Bytes read: {}", bytes_read);
                println!("Command: {:?}", command);
                stream.write_all(b"+PONG\r\n").await.unwrap();
            }
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        }
    }
}