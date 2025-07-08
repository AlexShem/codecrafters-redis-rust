mod commands;
mod propagation;
mod rdb;
mod rdb_handler;
mod replication;
mod resp;
mod server;

use crate::commands::{CommandContext, ExecutionContext, RedisCommand, RedisResponse};
use crate::propagation::PropagationManager;
use crate::rdb_handler::RdbHandler;
use crate::replication::initiate_replication_handshake;
use crate::server::{
    ReplicationState, Server, ServerConfig, ServerRole, SharedStorage, StorageValue,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = parse_args();
    let replication_state = ReplicationState::new(&config);
    let propagation_manager = PropagationManager::new();
    let storage: SharedStorage = Arc::new(Mutex::new(HashMap::new()));

    // Load RDB file if it exists
    if !config.dir.is_empty() && !config.dbfilename.is_empty() {
        let rdb_path = format!("{}/{}", config.dir, config.dbfilename);
        if std::path::Path::new(&rdb_path).exists() {
            if let Ok(rdb_data) = RdbHandler::load_rdb_file(&rdb_path).await {
                let mut storage_lock = storage.lock().await;
                for (key, value, expiry) in rdb_data {
                    storage_lock.insert(key, StorageValue::new(value, expiry));
                }
                println!("Loaded RDB file: {}", rdb_path);
            }
        }
    }

    if let ServerRole::Replica(_) = replication_state.role {
        let replication_state_clone = replication_state.clone();
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            initiate_replication_handshake(replication_state_clone, storage_clone).await;
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
                let server = Server::new(
                    stream,
                    config.clone(),
                    replication_state.clone(),
                    storage.clone(),
                );
                let prop_manager = propagation_manager.clone();
                tokio::spawn(async move { handle_connection(server, prop_manager).await });
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

async fn handle_connection(mut server: Server, propagation_manager: PropagationManager) {
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
                        // Handle replication commands
                        if matches!(command, RedisCommand::Psync { .. })
                            && matches!(server.replication_state.role, ServerRole::Master(_))
                        {
                            // Send FULLRESYNC response first
                            let fullresync_response = server
                                .execute_command(CommandContext {
                                    command,
                                    context: ExecutionContext::Replication,
                                })
                                .await;

                            server
                                .writer
                                .write_all(fullresync_response.to_bytes().as_slice())
                                .await
                                .unwrap();
                            server.writer.flush().await.unwrap();

                            // Then send RDB file
                            let rdb_response = server.get_rdb_file_response();
                            server
                                .writer
                                .write_all(rdb_response.to_bytes().as_slice())
                                .await
                                .unwrap();
                            server.writer.flush().await.unwrap();

                            // Move writer to propagation manager and keep reader for replica loop
                            propagation_manager.add_replica(server.writer).await;
                            let mut reader = server.reader;

                            // Replica loop - only read, don't write responses
                            loop {
                                let mut buf = bytes::BytesMut::with_capacity(512);
                                match reader.read_buf(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(_) => {
                                        // Replica receives propagated commands but doesn't respond
                                        // Just consume the data to keep connection alive
                                    }
                                    Err(_) => break,
                                }
                            }
                            break;
                        } else {
                            // Handle normal client commands
                            let response = server
                                .execute_command(CommandContext {
                                    command: command.clone(),
                                    context: ExecutionContext::Client,
                                })
                                .await;

                            // Propagate write commands to replicas if this is a master
                            if matches!(server.replication_state.role, ServerRole::Master(_)) {
                                propagation_manager.propagate_command(&command).await;
                            }

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
