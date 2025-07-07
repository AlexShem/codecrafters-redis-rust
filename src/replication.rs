use crate::commands::ReplConfArgs::{Capa, ListeningPort};
use crate::commands::{RedisCommand, RedisResponse};
use crate::server::{ReplicationState, ServerRole};
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

    pub async fn execute_handshake(&mut self, replica_port: &str) -> Result<(), String> {
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

        println!("✔️ Replication handshake completed successfully");
        Ok(())
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
        let expected = RedisResponse::expected_for_command(&cmd);

        if response == expected {
            println!("✔️ {:?} successful", cmd);
            Ok(())
        } else {
            Err(format!("Expected {:?}, got {:?}", expected, response))
        }
    }
}

pub async fn initiate_replication_handshake(replication_state: ReplicationState) {
    if let ServerRole::Replica(replica_state) = replication_state.role {
        match ReplicationHandshake::connect(&replica_state.master_host, &replica_state.master_port)
            .await
        {
            Ok(mut handshake) => {
                match handshake
                    .execute_handshake(&replica_state.replica_port)
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
