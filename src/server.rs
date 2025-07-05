use crate::rdb::RdbParser;
use crate::resp;
use crate::resp::RespValue;
use anyhow::anyhow;
use resp::RespParser;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        expiry_ms: Option<u128>,
    },
    Get {
        key: String,
    },
    Config {
        subcommand: ConfigCommand,
    },
    Keys {
        pattern: String,
    },
}

#[derive(Debug, Clone)]
pub enum ConfigCommand {
    Get { parameter: String },
    // Future: Set {parameter, value}
}

pub struct Server {
    pub reader: BufReader<OwnedReadHalf>,
    pub writer: BufWriter<OwnedWriteHalf>,
    pub storage: HashMap<String, String>,
    pub expiry_times: HashMap<String, u128>,
    pub config: ServerConfig,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub dir: String,
    pub dbfilename: String,
}

impl Server {
    pub fn new(stream: TcpStream, config: ServerConfig) -> Self {
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
