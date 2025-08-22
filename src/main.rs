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
        // match stream {
        //     Ok(stream) => {
        //         println!("accepted new connection");
        //         handle_connection(stream);
        //     }
        //     Err(e) => {
        //         println!("error: {}", e);
        //     }
        // }
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
