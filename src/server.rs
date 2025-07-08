use crate::commands::{
    CommandContext, ConfigCommand, ExecutionContext, InfoCommand, RedisCommand, RedisResponse,
    ReplConfArgs,
};
use crate::rdb_handler::RdbHandler;
use crate::resp;
use crate::resp::RespValue;
use anyhow::anyhow;
use resp::RespParser;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
#[allow(dead_code)]
/// Defines the role of a server in a replication setup
pub enum ServerRole {
    /// The server is acting as a master in the replication setup.
    /// Contains state specific to master servers.
    Master(MasterState),
    /// The server is acting as a replica in the replication setup.
    /// Contains state specific to replica servers.
    Replica(ReplicaState),
}

#[derive(Debug, Clone)]
pub struct StorageValue {
    pub value: String,
    pub expiry_ms: Option<u128>,
}

impl StorageValue {
    pub fn new(value: String, expiry_ms: Option<u128>) -> Self {
        let expiry_time = match expiry_ms {
            None => None,
            Some(ms) => Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    + ms,
            ),
        };
        StorageValue {
            value,
            expiry_ms: expiry_time,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry_ms {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            now > expiry
        } else {
            false
        }
    }
}

pub type SharedStorage = Arc<Mutex<HashMap<String, StorageValue>>>;

/// Main server structure that handles client connections and commands
pub struct Server {
    /// Reader for incoming client data
    pub reader: BufReader<OwnedReadHalf>,
    /// Writer for outgoing server responses
    pub writer: BufWriter<OwnedWriteHalf>,
    /// In-memory key-value storage
    pub storage: SharedStorage,
    /// Server configuration parameters
    pub config: ServerConfig,
    /// Current replication state of the server
    pub replication_state: ReplicationState,
}

#[derive(Debug, Clone)]
/// Configuration parameters for the Redis server
pub struct ServerConfig {
    /// Directory where persistence files are stored
    pub dir: String,
    /// Filename for the RDB database file
    pub dbfilename: String,
    /// Port the server listens on
    pub port: String,
    /// Master server information if this is a replica (host, port)
    pub replicaof: Option<(String, String)>,
}

#[derive(Debug, Clone)]
/// Represents the state of the server's replication.
/// It can be either a master or a replica.
pub struct ReplicationState {
    /// The role of the server in the replication setup.
    pub role: ServerRole,
}

#[derive(Debug, Clone)]
/// Contains state information specific to a master server
pub struct MasterState {
    /// Unique replication ID of this master
    pub replid: String,
    /// Current replication offset (position in replication stream)
    pub repl_offset: u64,
    // Future: connected_replicas: Vec<ReplicaConnection>
    // Future: replication_backlog: Vec<u8>
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
/// Contains state information specific to a replica server
pub struct ReplicaState {
    /// Hostname of the master server
    pub master_host: String,
    /// Port of the master server
    pub master_port: String,
    pub replica_port: String,
    // Future: master_connection: Option<TcpStream>
    // Future: handshake_state: HandshakeState
    // Future: master_replid: String, master_repl_offset: u64
}

impl Server {
    pub fn new(
        stream: TcpStream,
        config: ServerConfig,
        replication_state: ReplicationState,
        storage: SharedStorage,
    ) -> Self {
        let (reader, writer) = stream.into_split();

        Server {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            storage,
            config,
            replication_state,
        }
    }

    pub fn parse_command(&mut self, command_raw: String) -> anyhow::Result<RedisCommand> {
        let resp_value = RespParser::parse(&command_raw)?;

        match resp_value {
            RespValue::Array(elements) => {
                if elements.is_empty() {
                    return Err(anyhow!("Empty command array"));
                }

                let command_name = match &elements[0] {
                    RespValue::BulkString(Some(cmd)) => cmd.to_uppercase(),
                    _ => return Err(anyhow!("Invalid command format")),
                };

                match command_name.as_str() {
                    "PING" => Ok(RedisCommand::Ping),
                    "ECHO" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("ECHO requires exactly 1 argument"));
                        }
                        match &elements[1] {
                            RespValue::BulkString(Some(arg)) => Ok(RedisCommand::Echo(arg.clone())),
                            _ => Err(anyhow!("ECHO argument must be a string")),
                        }
                    }
                    "SET" => {
                        if elements.len() < 3 {
                            return Err(anyhow!("SET requires at least 2 arguments"));
                        }
                        let key = match &elements[1] {
                            RespValue::BulkString(Some(k)) => k.clone(),
                            _ => return Err(anyhow!("SET key must be a string")),
                        };

                        let value = match &elements[2] {
                            RespValue::BulkString(Some(v)) => v.clone(),
                            _ => return Err(anyhow!("SET value must be a string")),
                        };

                        let mut expiry_ms = None;

                        if elements.len() >= 5 {
                            if let RespValue::BulkString(Some(px_arg)) = &elements[3] {
                                if px_arg.to_uppercase() == "PX" {
                                    if let RespValue::BulkString(Some(expiry_str)) = &elements[4] {
                                        expiry_ms =
                                            Some(expiry_str.parse::<u128>().map_err(|_| {
                                                anyhow!("Invalid expiry time: {}", expiry_str)
                                            })?);
                                    }
                                }
                            }
                        }
                        Ok(RedisCommand::Set {
                            key,
                            value,
                            expiry_ms,
                        })
                    }
                    "GET" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("GET requires exactly 1 arguments"));
                        }

                        match &elements[1] {
                            RespValue::BulkString(Some(key)) => {
                                Ok(RedisCommand::Get { key: key.clone() })
                            }
                            _ => Err(anyhow::anyhow!("GET key must be a string")),
                        }
                    }
                    "CONFIG" => {
                        if elements.len() < 2 {
                            return Err(anyhow!("CONFIG requires a subcommand"));
                        }

                        let subcommand = match &elements[1] {
                            RespValue::BulkString(Some(cmd)) => cmd.to_uppercase(),
                            _ => return Err(anyhow!("Invalid CONFIG subcommand")),
                        };

                        match subcommand.as_str() {
                            "GET" => {
                                if elements.len() != 3 {
                                    return Err(anyhow!(
                                        "CONFIG GET requires exactly one parameter"
                                    ));
                                }
                                match &elements[2] {
                                    RespValue::BulkString(Some(parameter)) => {
                                        Ok(RedisCommand::Config {
                                            subcommand: ConfigCommand::Get {
                                                parameter: parameter.clone(),
                                            },
                                        })
                                    }
                                    _ => Err(anyhow!("CONFIG GET parameter must be a string")),
                                }
                            }
                            _ => Err(anyhow!("Unknown CONFIG subcomand: {}", subcommand)),
                        }
                    }
                    "KEYS" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("KEYS requires exactly 1 arguments"));
                        }

                        match &elements[1] {
                            RespValue::BulkString(Some(pattern)) => Ok(RedisCommand::Keys {
                                pattern: pattern.clone(),
                            }),
                            _ => Err(anyhow::anyhow!("KEYS pattern must be a string")),
                        }
                    }
                    "INFO" => {
                        let mut subcommand = None;

                        if elements.len() == 2 {
                            subcommand = match &elements[1] {
                                RespValue::BulkString(Some(cmd)) => Some(cmd.to_uppercase()),
                                _ => return Err(anyhow!("Invalid INFO subcommand")),
                            }
                        }

                        if let Some(cmd) = subcommand {
                            return match cmd.as_str() {
                                "REPLICATION" => Ok(RedisCommand::Info {
                                    subcommand: Some(InfoCommand::Replication),
                                }),
                                _ => Err(anyhow::anyhow!("Unsupported INFO command: {}", cmd)),
                            };
                        }
                        Ok(RedisCommand::Info { subcommand: None })
                    }
                    "REPLCONF" => {
                        if elements.len() < 2 {
                            return Err(anyhow!(
                                "REPLCONF requires arguments <listening-port> or <capa>"
                            ));
                        }

                        let argument = match &elements[1] {
                            RespValue::BulkString(Some(arg)) => arg.to_lowercase(),
                            _ => return Err(anyhow!("Invalid REPLCONF argument")),
                        };

                        match argument.as_str() {
                            "listening-port" => {
                                if elements.len() != 3 {
                                    return Err(anyhow!(
                                        "REPLCONF argument requires exactly one parameter"
                                    ));
                                }
                                match &elements[2] {
                                    RespValue::BulkString(Some(port)) => {
                                        match port.parse::<u16>() {
                                            Ok(_) => Ok(RedisCommand::ReplConf {
                                                args: ReplConfArgs::ListeningPort(port.clone()),
                                            }),
                                            Err(_) => Err(anyhow!(
                                                "REPLCONF listening-port must be a valid number"
                                            )),
                                        }
                                    }
                                    _ => Err(anyhow!(
                                        "REPLCONF listening-port argument must be a string"
                                    )),
                                }
                            }
                            "capa" => {
                                if elements.len() != 3 {
                                    return Err(anyhow!(
                                        "REPLCONF argument requires exactly one parameter"
                                    ));
                                }
                                match &elements[2] {
                                    RespValue::BulkString(Some(capa)) => match capa.as_str() {
                                        "psync2" => Ok(RedisCommand::ReplConf {
                                            args: ReplConfArgs::Capa(capa.clone()),
                                        }),
                                        _ => Err(anyhow!(
                                            "REPLCONF capa unsupported capability: {}",
                                            capa
                                        )),
                                    },
                                    _ => Err(anyhow!("REPLCONF capa argument must be a string")),
                                }
                            }
                            _ => Err(anyhow!("Unknown REPLCONF argument: {}", argument)),
                        }
                    }
                    "PSYNC" => {
                        if elements.len() < 3 {
                            return Err(anyhow!(
                                "PSYNC requires exactly two arguments: replicationid and offset"
                            ));
                        }

                        let replication_id = match &elements[1] {
                            RespValue::BulkString(Some(id)) => id,
                            _ => return Err(anyhow!("Invalid PSYNC replicationid argument")),
                        };

                        let offset = match &elements[2] {
                            RespValue::BulkString(Some(off)) => off,
                            _ => return Err(anyhow!("Invalid PSYNC offset argument")),
                        };

                        Ok(RedisCommand::Psync {
                            repl_id: replication_id.clone(),
                            offset: offset.clone(),
                        })
                    }
                    _ => Err(anyhow::anyhow!("Unknown command: {}", command_name)),
                }
            }
            _ => Err(anyhow!("Command must be an array")),
        }
    }

    pub async fn execute_command(&mut self, context: CommandContext) -> RedisResponse {
        match context.context {
            ExecutionContext::Propagation => {
                // Process silently without sending response back
                self.process_command_with_storage(context.command).await;
                // Return a dummy response that won't be sent
                RedisResponse::Ok
            }
            _ => {
                // Normal processing for Client and Replication contexts
                self.process_command_with_storage(context.command).await
            }
        }
    }

    pub async fn process_command_with_storage(&mut self, command: RedisCommand) -> RedisResponse {
        match command {
            RedisCommand::Ping => RedisResponse::Pong,
            RedisCommand::Echo(msg) => RedisResponse::BulkString(Some(msg)),
            RedisCommand::Set {
                key,
                value,
                expiry_ms,
            } => {
                let storage_value = StorageValue::new(value, expiry_ms);
                let mut storage = self.storage.lock().await;
                storage.insert(key, storage_value);
                RedisResponse::Ok
            }
            RedisCommand::Get { key } => {
                let mut storage = self.storage.lock().await;
                match storage.get(&key) {
                    Some(storage_value) if !storage_value.is_expired() => {
                        RedisResponse::BulkString(Some(storage_value.value.clone()))
                    }
                    Some(_) => {
                        // Remove expired key
                        storage.remove(&key);
                        RedisResponse::BulkString(None)
                    }
                    None => RedisResponse::BulkString(None),
                }
            }
            RedisCommand::Config { subcommand } => match subcommand {
                ConfigCommand::Get { parameter } => match parameter.as_str() {
                    "dir" => RedisResponse::Array(vec!["dir".to_string(), self.config.dir.clone()]),
                    "dbfilename" => RedisResponse::Array(vec![
                        "dbfilename".to_string(),
                        self.config.dbfilename.clone(),
                    ]),
                    _ => RedisResponse::Array(vec![]),
                },
            },
            RedisCommand::Keys { pattern } => {
                if pattern == "*" {
                    let storage = self.storage.lock().await;
                    let keys: Vec<String> = storage
                        .iter()
                        .filter(|(_, storage_value)| !storage_value.is_expired())
                        .map(|(key, _)| key.clone())
                        .collect();
                    RedisResponse::Array(keys)
                } else {
                    RedisResponse::Array(vec![])
                }
            }
            RedisCommand::Info { subcommand } => match subcommand {
                Some(InfoCommand::Replication) => match &self.replication_state.role {
                    ServerRole::Master(master_state) => {
                        let info = format!(
                            "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                            master_state.replid, master_state.repl_offset
                        );
                        RedisResponse::BulkString(Some(info))
                    }
                    ServerRole::Replica(_) => {
                        RedisResponse::BulkString(Some("role:slave".to_string()))
                    }
                },
                _ => RedisResponse::BulkString(Some("".to_string())),
            },
            RedisCommand::ReplConf { args } => match args {
                ReplConfArgs::ListeningPort(_) => RedisResponse::Ok,
                ReplConfArgs::Capa(_) => RedisResponse::Ok,
            },
            RedisCommand::Psync { repl_id, offset } => {
                if let ServerRole::Master(master_state) = &self.replication_state.role {
                    // Validate PSYNC parameters
                    if repl_id != "?" || offset != "-1" {
                        return RedisResponse::Error("Invalid PSYNC parameters".to_string());
                    }

                    RedisResponse::FullResync {
                        repl_id: master_state.replid.clone(),
                        offset: master_state.repl_offset,
                    }
                } else {
                    RedisResponse::Error("PSYNC not allowed for replica".to_string())
                }
            }
        }
    }

    pub fn get_rdb_file_response(&self) -> RedisResponse {
        let rdb_data = RdbHandler::get_empty_rdb_file();
        let formatted_rdb = RdbHandler::format_rdb_transfer(&rdb_data);
        RedisResponse::RdbFile(formatted_rdb)
    }
}

impl ServerConfig {
    fn is_replica(&self) -> bool {
        self.replicaof.is_some()
    }
}

impl ReplicationState {
    pub fn new(config: &ServerConfig) -> Self {
        let role = if config.is_replica() {
            ServerRole::Replica(ReplicaState {
                master_host: config.replicaof.as_ref().unwrap().0.clone(),
                master_port: config.replicaof.as_ref().unwrap().1.clone(),
                replica_port: config.port.clone(),
            })
        } else {
            ServerRole::Master(MasterState {
                replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                repl_offset: 0,
            })
        };

        ReplicationState { role }
    }

    pub fn _is_master(&self) -> bool {
        matches!(self.role, ServerRole::Master(_))
    }
}
