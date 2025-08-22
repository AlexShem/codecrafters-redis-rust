use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    loop {
        let mut buf = [0; 512];
        match stream.read(&mut buf) {
            Ok(0) => {
                println!("Connection closed by client");
                break;
            }
            Ok(bytes_read) => {
                let command = String::from_utf8_lossy(&buf[..bytes_read]);
                println!("Bytes read: {}", bytes_read);
                println!("Command: {:?}", command);
                stream.write_all(b"+PONG\r\n").unwrap();
            }
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        }
    }
}
