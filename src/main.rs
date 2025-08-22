#![allow(unused_imports)]

use std::io::Write;
use std::net::{TcpListener, TcpStream};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                handle_connection(_stream).expect("Failed to handle the connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let response = bytes::Bytes::from("+PONG\r\n");
    stream.write_all(&response)?;
    Ok(())
}
