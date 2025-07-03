#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

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
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}