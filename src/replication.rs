use crate::commands::ReplConfArgs::{Capa, ListeningPort};
use crate::commands::{
    RedisCommand, RedisResponse, extract_complete_command, parse_propagated_command,
};
use crate::server::{ReplicationState, ServerRole, SharedStorage, StorageValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct ReplicationHandshake {
    stream: TcpStream,
}

impl ReplicationHandshake {
    pub async fn connect(master_host: &str, master_port: &str) -> Result<Self, String> {
        let master_addr = format!("{}:{}", master_host, master_port);
        let stream = TcpStream::connect(&master_addr)
            .await
            .map_err(|e| format!("Failed to connect to master at {}: {}", master_addr, e))?;

        println!("Connected to master at {}", master_addr);
        Ok(ReplicationHandshake { stream })
    }

    pub async fn execute_handshake(
        &mut self,
        replica_port: &str,
        storage: SharedStorage,
    ) -> Result<(), String> {
        // Send PING
        self.send_and_validate(RedisCommand::Ping).await?;
        // Send REPLCONF listening-port <REPLICA PORT>
        self.send_and_validate(RedisCommand::ReplConf {
            args: ListeningPort(replica_port.to_string()),
        })
            .await?;
        // Send REPLCONF capa psync2
        self.send_and_validate(RedisCommand::ReplConf {
            args: Capa("psync2".to_string()),
        })
            .await?;
        // Send PSYNC replicationid offset
        self.send_and_validate(RedisCommand::Psync {
            repl_id: "?".to_string(),
            offset: "-1".to_string(),
        })
            .await?;

        self.process_propagated_commands(storage).await?;

        Ok(())
    }

    async fn process_propagated_commands(&mut self, storage: SharedStorage) -> Result<(), String> {
        let mut buffer = Vec::new();

        loop {
            let mut temp_buf = [0; 1024];
            match self.stream.read(&mut temp_buf).await {
                Ok(0) => break,
                Ok(n) => {
                    buffer.extend_from_slice(&temp_buf[..n]);

                    while let Some((command_str, remaining)) = extract_complete_command(&buffer) {
                        println!("Processing propagated command: {}", command_str.trim()); // Debug log
                        if let Ok(command) = parse_propagated_command(&command_str) {
                            println!("Parsed command: {:?}", command); // Debug log
                            self.process_replica_command(command, storage.clone()).await;
                        } else {
                            println!("Failed to parse command: {}", command_str); // Debug log
                        }
                        buffer = remaining;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from master: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn process_replica_command(&self, command: RedisCommand, storage: SharedStorage) {
        // Create a minimal server-like processor that uses the shared storage
        Self::execute_command_with_storage(command, storage).await;
    }

    async fn execute_command_with_storage(command: RedisCommand, storage: SharedStorage) {
        match command {
            RedisCommand::Set {
                key,
                value,
                expiry_ms,
            } => {
                let storage_value = StorageValue::new(value, expiry_ms);
                let mut storage = storage.lock().await;
                storage.insert(key, storage_value);
            }
            // Add other write commands as needed
            _ => {} // Ignore read-only commands for replicas
        }
    }

    async fn send_and_validate(&mut self, cmd: RedisCommand) -> Result<(), String> {
        // Send command
        let cmd_str = cmd.to_string();
        self.stream
            .write_all(cmd_str.as_bytes())
            .await
            .map_err(|e| format!("Failed to send {:?}: {}", cmd, e))?;

        // Read response
        let mut response_buf = [0; 1024];
        let bytes_read = self
            .stream
            .read(&mut response_buf)
            .await
            .map_err(|e| format!("Failed to read response for {:?}: {}", cmd, e))?;

        let raw_response = String::from_utf8_lossy(&response_buf[..bytes_read]);
        println!("Received from master: {:?}", raw_response);

        // Validate response
        let response = RedisResponse::from_raw(&raw_response);

        // Special validation for PSYNC
        if matches!(cmd, RedisCommand::Psync { .. }) {
            match response {
                RedisResponse::FullResync { .. } => {
                    println!("✔️ {:?} successful", cmd);
                    Ok(())
                }
                _ => Err(format!("Expected FULLRESYNC response, got {:?}", response)),
            }
        } else {
            // Standard validation for other commands
            let expected = RedisResponse::expected_for_command(&cmd);
            if response == expected {
                println!("✔️ {:?} successful", cmd);
                Ok(())
            } else {
                Err(format!("Expected {:?}, got {:?}", expected, response))
            }
        }
    }
}

pub async fn initiate_replication_handshake(
    replication_state: ReplicationState,
    storage: SharedStorage,
) {
    if let ServerRole::Replica(replica_state) = replication_state.role {
        match ReplicationHandshake::connect(&replica_state.master_host, &replica_state.master_port)
            .await
        {
            Ok(mut handshake) => {
                match handshake
                    .execute_handshake(&replica_state.replica_port, storage)
                    .await
                {
                    Ok(_) => println!("✔️ Replication handshake completed successfully"),
                    Err(e) => eprintln!("❌ Replication handshake failed: {}", e),
                }
            }
            Err(e) => eprintln!("❌ Replication handshake failed: {}", e),
        }
    }
}
