use crate::server::{ReplicationState, ServerRole};
use std::fmt::Display;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub enum ReplicationCommand {
    /// PING
    Ping,
    /// REPLCONF listening-port PORT
    ReplConfListeningPort(String),
    /// REPLCONF capa psync2
    ReplConfCapa,
    /// PSYNC replicationid offset
    Psync(String, String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationResponse {
    Pong,
    Ok,
    /// +FULLRESYNC <REPL_ID> 0\r\n
    FullResync,
    Error(String),
}

impl Display for ReplicationCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationCommand::Ping => write!(f, "*1\r\n$4\r\nPING\r\n"),
            ReplicationCommand::ReplConfListeningPort(port) => write!(
                f,
                "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                port.len(),
                port
            ),
            ReplicationCommand::ReplConfCapa => {
                write!(f, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            }
            ReplicationCommand::Psync(repl_id, offset) => {
                write!(
                    f,
                    "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    repl_id.len(),
                    repl_id,
                    offset.len(),
                    offset
                )
            }
        }
    }
}

impl ReplicationResponse {
    pub fn from_raw(raw: &str) -> Self {
        match raw.trim() {
            "+PONG" => ReplicationResponse::Pong,
            "+OK" => ReplicationResponse::Ok,
            _ => ReplicationResponse::Error(raw.to_string()),
        }
    }

    pub fn expected_for_command(cmd: &ReplicationCommand) -> Self {
        match cmd {
            ReplicationCommand::Ping => ReplicationResponse::Pong,
            ReplicationCommand::ReplConfListeningPort(_) => ReplicationResponse::Ok,
            ReplicationCommand::ReplConfCapa => ReplicationResponse::Ok,
            ReplicationCommand::Psync(_, _) => ReplicationResponse::FullResync,
        }
    }
}

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
        self.send_and_validate(ReplicationCommand::Ping).await?;
        // Send REPLCONF listening-port <REPLICA PORT>
        self.send_and_validate(ReplicationCommand::ReplConfListeningPort(
            replica_port.to_string(),
        ))
        .await?;
        // Send REPLCONF capa psync2
        self.send_and_validate(ReplicationCommand::ReplConfCapa)
            .await?;
        // Send PSYNC replicationid offset
        self.send_and_validate(ReplicationCommand::Psync("?".to_string(), "-1".to_string()))
            .await?;

        println!("✔️ Replication handshake completed successfully");
        Ok(())
    }

    async fn send_and_validate(&mut self, cmd: ReplicationCommand) -> Result<(), String> {
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
        let response = ReplicationResponse::from_raw(&raw_response);
        let expected = ReplicationResponse::expected_for_command(&cmd);

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
