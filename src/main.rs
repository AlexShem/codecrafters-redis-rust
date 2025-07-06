mod server;
mod resp;
mod rdb;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::server::{Command, ReplicationState, Server, ServerConfig, ServerRole};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = parse_args();

    let replication_state = ReplicationState::new(&config);

    if let ServerRole::Replica(_) = replication_state.role {
        let replication_state_clone = replication_state.clone();
        tokio::spawn(async move {
            initiate_replication_handshake(replication_state_clone).await;
        });
    }

    let port = &config.port;
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;

    println!("Server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let server = Server::new(stream, config.clone(), replication_state.clone());
                tokio::spawn(async move { handle_connection(server).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn initiate_replication_handshake(replication_state: ReplicationState) {
    if let ServerRole::Replica(replica_state) = replication_state.role {
        let master_addr = format!("{}:{}", replica_state.master_host, replica_state.master_port);

        // Step 1: Connect to master
        match TcpStream::connect(&master_addr).await {
            Ok(mut stream) => {
                println!("Connected to master at {}", master_addr);

                // Step 2: Send PING command
                let ping_command = Command::Ping;
                let ping_bytes = ping_command.to_string();

                match stream.write_all(ping_bytes.as_bytes()).await {
                    Ok(_) => {
                        println!("Sent PING to master");

                        // Step 3: Read response
                        // You might want to verify the PONG response
                        let mut response_buf = [0; 1024];
                        match stream.read(&mut response_buf).await {
                            Ok(bytes_read) => {
                                let response = String::from_utf8_lossy(&response_buf[..bytes_read]);
                                println!("Received from master: {:?}", response);

                                if response.trim() == "+PONG" {
                                    println!("✔️ PING handshake successful");
                                } else {
                                    eprintln!("❌ Unexpected response to PING: {}", response);
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to read PONG response: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to send PING to master: {}", e);
                    }
                }

                // Step 4. Send REPLCONF command
                // Step 4.1. REPLCONF listening-port <PORT>
                let replcong_listen_port_command = format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                    replica_state.replica_port.len(),
                    replica_state.replica_port
                );

                match stream.write_all(replcong_listen_port_command.as_bytes()).await {
                    Ok(_) => {
                        println!("Sent REPLCONF listening-port to master");

                        let mut response_buf = [0; 1024];
                        match stream.read(&mut response_buf).await {
                            Ok(bytes_read) => {
                                let response = String::from_utf8_lossy(&response_buf[..bytes_read]);
                                println!("Received from master: {:?}", response);

                                if response.trim() == "+OK" {
                                    println!("✔️ REPLCONF handshake successful");
                                } else {
                                    eprintln!("❌ Unexpected response to REPLCONF: {}", response);
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to read REPLCONF response: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to send REPLCONF to master: {}", e);
                    }
                }

                // Step 4.2. REPLCONF capa psync2
                let replcong_capa_command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";

                match stream.write_all(replcong_capa_command.as_bytes()).await {
                    Ok(_) => {
                        println!("Sent REPLCONF capa to master");

                        let mut response_buf = [0; 1024];
                        match stream.read(&mut response_buf).await {
                            Ok(bytes_read) => {
                                let response = String::from_utf8_lossy(&response_buf[..bytes_read]);
                                println!("Received from master: {:?}", response);

                                if response.trim() == "+OK" {
                                    println!("✔️ REPLCONF handshake successful");
                                } else {
                                    eprintln!("❌ Unexpected response to REPLCONF: {}", response);
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to read REPLCONF response: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to send REPLCONF to master: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to master at {}: {}", master_addr, e);
            }
        }
    }
}

fn parse_args() -> ServerConfig {
    let args: Vec<String> = std::env::args().collect();

    let mut dir = String::new();
    let mut dbfilename = String::new();
    let mut port = 6379u16;
    let mut replicaof = None;

    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--dir" => {
                if i + 1 < args.len() {
                    dir = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --dir requires a value");
                    std::process::exit(1);
                }
            }
            "--dbfilename" => {
                if i + 1 < args.len() {
                    dbfilename = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --dbfilename requires a value");
                    std::process::exit(1);
                }
            }
            "--port" => {
                if i + 1 < args.len() {
                    port = args[i + 1].parse()
                        .unwrap_or_else(|_| {
                            eprintln!("Error: --port must be a valid number");
                            std::process::exit(1);
                        });
                    i += 2;
                } else {
                    eprintln!("Error: --port requires a value");
                    std::process::exit(1);
                }
            }
            "--replicaof" => {
                if i + 1 < args.len() {
                    match args[i + 1].split_once(' ') {
                        None => {
                            eprintln!("Error: --replicaof requires '<MASTER_HOST> <MASTER_PORT>' format");
                            std::process::exit(1);
                        }
                        Some((host, port)) => {
                            if port.parse::<u16>().is_err() {
                                eprintln!("Error: MASTER_PORT must be a valid number");
                                std::process::exit(1);
                            }

                            replicaof = Some((host.to_string(), port.to_string()))
                        }
                    }

                    i += 2;
                } else {
                    eprintln!("Error: --replicaof requires a value");
                    std::process::exit(1);
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                i += 1;
            }
        }
    }

    ServerConfig {
        dir,
        dbfilename,
        port: port.to_string(),
        replicaof
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
                
                match server.parse_command(command_raw) {
                    Ok(command) => {
                        let response = server.execute_command(command);
                        println!("Response: {:?}", response);
                        server.writer.write_all(response.as_bytes()).await.unwrap();
                        server.writer.flush().await.unwrap();
                    },
                    Err(e) => {
                        println!("Parse error: {}", e);
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