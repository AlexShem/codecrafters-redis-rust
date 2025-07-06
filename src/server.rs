use crate::rdb::RdbParser;
use crate::resp;
use crate::resp::RespValue;
use crate::server::InfoCommand::Replication;
use anyhow::anyhow;
use resp::RespParser;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
/// Represents a Redis command received from a client.
/// Each variant corresponds to a supported Redis command with its parameters.
pub enum Command {
    /// Simple PING command that responds with PONG
    Ping,
    /// ECHO command that returns the provided message
    Echo(String),
    /// SET command to store a key-value pair with optional expiration time
    Set {
        key: String,
        value: String,
        expiry_ms: Option<u128>,
    },
    /// GET command to retrieve a value by key
    Get { key: String },
    /// CONFIG command with its subcommands
    Config { subcommand: ConfigCommand },
    /// KEYS command to find keys matching a pattern
    Keys { pattern: String },
    /// INFO command with optional subcommand
    Info { subcommand: Option<InfoCommand> },
}

#[derive(Debug, Clone)]
/// Represents available subcommands for the CONFIG command
pub enum ConfigCommand {
    /// CONFIG GET to retrieve configuration parameters
    Get { parameter: String },
    // Future: Set {parameter, value}
}

#[derive(Debug, Clone)]
/// Represents available subcommands for the INFO command
pub enum InfoCommand {
    /// Replication info
    /// This is used to get the replication state of the server.
    /// Can be used by both master and replica.
    Replication,
    // Future: Server, Client, Memory, etc
}

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

/// Main server structure that handles client connections and commands
pub struct Server {
    /// Reader for incoming client data
    pub reader: BufReader<OwnedReadHalf>,
    /// Writer for outgoing server responses
    pub writer: BufWriter<OwnedWriteHalf>,
    /// In-memory key-value storage
    pub storage: HashMap<String, String>,
    /// Maps keys to their expiration times in milliseconds
    pub expiry_times: HashMap<String, u128>,
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
    ) -> Self {
        let (read, write) = stream.into_split();
        let reader = BufReader::new(read);
        let writer = BufWriter::new(write);

        // Load RDB data if file exists
        let mut storage = HashMap::new();
        let mut expiry_times = HashMap::new();

        if let Ok(rdb_data) = RdbParser::parse_file(&config.dir, &config.dbfilename) {
            storage = rdb_data.keys;
            expiry_times = rdb_data.expiry_times;
        }

        Server {
            reader,
            writer,
            storage,
            expiry_times,
            config,
            replication_state,
        }
    }

    fn current_time_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }

    fn is_expired(&self, key: &str) -> bool {
        if let Some(&expiry_time) = self.expiry_times.get(key) {
            Self::current_time_ms() >= expiry_time
        } else {
            false
        }
    }

    pub fn parse_command(&mut self, command_raw: String) -> anyhow::Result<Command> {
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
                    "PING" => Ok(Command::Ping),
                    "ECHO" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("ECHO requires exactly 1 argument"));
                        }
                        match &elements[1] {
                            RespValue::BulkString(Some(arg)) => Ok(Command::Echo(arg.clone())),
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

                        Ok(Command::Set {
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
                                Ok(Command::Get { key: key.clone() })
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
                                    RespValue::BulkString(Some(parameter)) => Ok(Command::Config {
                                        subcommand: ConfigCommand::Get {
                                            parameter: parameter.clone(),
                                        },
                                    }),
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
                            RespValue::BulkString(Some(pattern)) => Ok(Command::Keys {
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
                                "REPLICATION" => Ok(Command::Info {
                                    subcommand: Some(Replication),
                                }),
                                _ => Err(anyhow::anyhow!("Unsupported INFO command: {}", cmd)),
                            };
                        }
                        Ok(Command::Info { subcommand: None })
                    }
                    _ => Err(anyhow::anyhow!("Unknown command: {}", command_name)),
                }
            }
            _ => Err(anyhow!("Command must be an array")),
        }
    }

    pub fn execute_command(&mut self, command: Command) -> String {
        match command {
            Command::Ping => "+PONG\r\n".to_string(),
            Command::Echo(msg) => format!("${}\r\n{}\r\n", msg.len(), msg),
            Command::Set {
                key,
                value,
                expiry_ms,
            } => {
                self.storage.insert(key.clone(), value);
                if let Some(expiry_ms) = expiry_ms {
                    let expiry_time = Self::current_time_ms() + expiry_ms;
                    self.expiry_times.insert(key, expiry_time);
                }
                "+OK\r\n".to_string()
            }
            Command::Get { key } => {
                if self.storage.contains_key(&key) && !self.is_expired(&key) {
                    let value = self.storage.get(&key).unwrap();
                    format!("${}\r\n{}\r\n", value.len(), value)
                } else {
                    if self.is_expired(&key) {
                        self.storage.remove(&key);
                        self.expiry_times.remove(&key);
                    }
                    "$-1\r\n".to_string()
                }
            }
            Command::Config { subcommand } => match subcommand {
                ConfigCommand::Get { parameter } => match parameter.as_str() {
                    "dir" => {
                        let elements = vec!["dir".to_string(), self.config.dir.clone()];
                        format_resp_array(&elements)
                    }
                    "dbfilename" => {
                        let elements =
                            vec!["dbfilename".to_string(), self.config.dbfilename.clone()];
                        format_resp_array(&elements)
                    }
                    _ => format!("$-ERR unknown parameter: {}\r\n", parameter),
                },
            },
            Command::Keys { pattern } => {
                if pattern == "*" {
                    // Get all non-expired keys
                    let mut keys: Vec<String> = self
                        .storage
                        .keys()
                        .filter(|&key| !self.is_expired(key))
                        .cloned()
                        .collect();

                    // Clean up expired keys
                    for key in &keys {
                        if self.is_expired(key) {
                            self.storage.remove(key);
                            self.expiry_times.remove(key);
                        }
                    }

                    // Filter out expired keys
                    keys.retain(|key| !self.is_expired(key));

                    format_resp_array(&keys)
                } else {
                    // For now, only support "*" pattern
                    "-ERR pattern not supported\r\n".to_string()
                }
            }
            Command::Info { subcommand } => {
                match subcommand {
                    Some(Replication) | None => {
                        let mut response = String::new();

                        match &self.replication_state.role {
                            ServerRole::Master(master_state) => {
                                response.push_str(&"role:master\r\n".to_string());
                                response.push_str(&format!(
                                    "master_replid:{}\r\n",
                                    master_state.replid
                                ));
                                response.push_str(&format!(
                                    "master_repl_offset:{}\r\n",
                                    master_state.repl_offset
                                ));
                            }
                            ServerRole::Replica { .. } => {
                                response.push_str("role:slave\r\n");
                                // For replicas, you might show master's replid/offset differently
                            }
                        }

                        // Remove the trailing \r\n
                        response.pop();
                        response.pop();

                        format!("${}\r\n{}\r\n", response.len(), response)
                    }
                }
            }
        }
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

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping => write!(f, "*1\r\n$4\r\nPING\r\n"),
            Command::Echo(msg) => write!(f, "*2\r\n$4\r\nECHO\r\n${}\r\n{}\r\n", msg.len(), msg),
            // Command::Set { .. } => {}
            // Command::Get { .. } => {}
            // Command::Config { .. } => {}
            // Command::Keys { .. } => {}
            // Command::Info { .. } => {}
            _ => write!(f, ""),
        }
    }
}

fn format_resp_array(elements: &[String]) -> String {
    let mut result = format!("*{}\r\n", elements.len());

    for element in elements {
        result.push_str(&format!("${}\r\n{}\r\n", element.len(), element))
    }
    result
}
