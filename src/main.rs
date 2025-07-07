mod commands;
mod rdb;
mod rdb_handler;
mod replication;
mod replication_master;
mod resp;
mod server;

use crate::commands::{CommandContext, ExecutionContext, RedisCommand, RedisResponse};
use crate::replication::initiate_replication_handshake;
use crate::server::{ReplicationState, Server, ServerConfig, ServerRole};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
                    port = args[i + 1].parse().unwrap_or_else(|_| {
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
                            eprintln!(
                                "Error: --replicaof requires '<MASTER_HOST> <MASTER_PORT>' format"
                            );
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
        replicaof,
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
                        if matches!(command, RedisCommand::Psync { .. })
                            && matches!(server.replication_state.role, ServerRole::Master(_))
                        {
                            // Send FULLRESYNC response first
                            let fullresync_response = server.execute_command(CommandContext {
                                command,
                                context: ExecutionContext::Replication,
                            });

                            server
                                .writer
                                .write_all(fullresync_response.to_bytes().as_slice())
                                .await
                                .unwrap();
                            server.writer.flush().await.unwrap();

                            // Then send RDB file using the response system
                            let rdb_response = server.get_rdb_file_response();
                            server
                                .writer
                                .write_all(rdb_response.to_bytes().as_slice())
                                .await
                                .unwrap();
                            server.writer.flush().await.unwrap();
                        } else {
                            // Handle normal commands
                            let response = match &server.replication_state.role {
                                ServerRole::Master(_master_state) => {
                                    server.execute_command(CommandContext {
                                        command,
                                        context: ExecutionContext::Client,
                                    })
                                }
                                ServerRole::Replica(_replica_state) => {
                                    server.execute_command(CommandContext {
                                        command,
                                        context: ExecutionContext::Replication,
                                    })
                                }
                            };

                            server
                                .writer
                                .write_all(response.to_bytes().as_slice())
                                .await
                                .unwrap();
                            server.writer.flush().await.unwrap();
                        }
                    }
                    Err(e) => {
                        println!("Parse error: {}", e);
                        server
                            .writer
                            .write_all(
                                RedisResponse::Error("Unknown command".to_string())
                                    .to_resp_string()
                                    .as_bytes(),
                            )
                            .await
                            .unwrap();
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
