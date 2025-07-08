use crate::resp::{RespParser, RespValue};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum RedisCommand {
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
    Info {
        subcommand: Option<InfoCommand>,
    },
    ReplConf {
        args: ReplConfArgs,
    },
    Psync {
        repl_id: String,
        offset: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RedisResponse {
    Ok,
    Pong,
    Error(String),
    BulkString(Option<String>),
    Array(Vec<String>),
    FullResync { repl_id: String, offset: u64 },
    RdbFile(Vec<u8>),
}

#[derive(Debug, Clone)]
pub enum ConfigCommand {
    Get { parameter: String },
}

#[derive(Debug, Clone)]
pub enum InfoCommand {
    _Clients,
    Replication,
}

#[derive(Debug, Clone)]
pub enum ReplConfArgs {
    ListeningPort(String),
    Capa(String),
}

#[derive(Debug, Clone)]
pub enum ExecutionContext {
    /// Command from client
    Client,
    /// Command for replication handshake
    Replication,
    #[allow(dead_code)]
    /// Command being propagated to replicas
    Propagation,
}

#[derive(Debug, Clone)]
pub struct CommandContext {
    pub command: RedisCommand,
    pub context: ExecutionContext,
}

impl RedisCommand {
    pub fn from_resp_array(resp: &RespValue) -> Result<Self, String> {
        match resp {
            RespValue::Array(elements) => {
                let args: Result<Vec<String>, String> = elements
                    .iter()
                    .map(|elem| match elem {
                        RespValue::BulkString(Some(s)) => Ok(s.clone()),
                        RespValue::SimpleString(s) => Ok(s.clone()),
                        _ => Err("Invalid command argument".to_string()),
                    })
                    .collect();

                let args = args?;
                if args.is_empty() {
                    return Err("Empty command".to_string());
                }

                match args[0].to_uppercase().as_str() {
                    "SET" if args.len() >= 3 => {
                        let mut expiry_ms = None;

                        // Check for PX option (expiry in milliseconds)
                        if args.len() >= 5 && args[3].to_uppercase() == "PX" {
                            if let Ok(ms) = args[4].parse::<u128>() {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                expiry_ms = Some(now + ms);
                            }
                        }

                        Ok(RedisCommand::Set {
                            key: args[1].clone(),
                            value: args[2].clone(),
                            expiry_ms,
                        })
                    }
                    "GET" if args.len() >= 2 => Ok(RedisCommand::Get {
                        key: args[1].clone(),
                    }),
                    _ => Err(format!("Unsupported command: {}", args[0])),
                }
            }
            _ => Err("Command must be a RESP array".to_string()),
        }
    }
}

impl RedisResponse {
    pub fn to_resp_string(&self) -> String {
        match self {
            RedisResponse::Ok => "+OK\r\n".to_string(),
            RedisResponse::Pong => "+PONG\r\n".to_string(),
            RedisResponse::Error(msg) => format!("-ERR {}\r\n", msg),
            RedisResponse::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s),
            RedisResponse::BulkString(None) => "$-1\r\n".to_string(),
            RedisResponse::Array(elements) => format_resp_array(elements),
            RedisResponse::FullResync { repl_id, offset } => {
                format!("+FULLRESYNC {} {}\r\n", repl_id, offset)
            }
            RedisResponse::RdbFile(_) => String::new(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RedisResponse::RdbFile(data) => data.clone(),
            _ => self.to_resp_string().into_bytes(),
        }
    }

    pub fn from_raw(raw: &str) -> Self {
        let trimmed = raw.trim();
        match raw {
            "+PONG\r\n" => RedisResponse::Pong,
            "+OK\r\n" => RedisResponse::Ok,
            _ if trimmed.starts_with("+FULLRESYNC") => {
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.len() >= 3 {
                    RedisResponse::FullResync {
                        repl_id: parts[1].to_string(),
                        offset: parts[2].parse().unwrap_or(0),
                    }
                } else {
                    RedisResponse::Error(trimmed.to_string())
                }
            }
            _ => RedisResponse::Error(trimmed.to_string()),
        }
    }

    pub fn expected_for_command(cmd: &RedisCommand) -> RedisResponse {
        match cmd {
            RedisCommand::Ping => RedisResponse::Pong,
            RedisCommand::Echo(msg) => RedisResponse::BulkString(Some(msg.to_string())),
            RedisCommand::Set { .. } => RedisResponse::Ok,
            RedisCommand::Get { .. } => RedisResponse::BulkString(Some(String::new())),
            RedisCommand::Config { .. } => RedisResponse::Array(vec![]),
            RedisCommand::Keys { .. } => RedisResponse::Array(vec![]),
            RedisCommand::Info { .. } => RedisResponse::BulkString(Some(String::new())),
            RedisCommand::ReplConf { .. } => RedisResponse::Ok,
            RedisCommand::Psync { .. } => RedisResponse::FullResync {
                repl_id: String::new(),
                offset: 0,
            },
        }
    }
}

impl Display for RedisCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisCommand::Ping => write!(f, "*1\r\n$4\r\nPING\r\n"),
            RedisCommand::Echo(msg) => {
                write!(f, "*2\r\n$4\r\nECHO\r\n${}\r\n{}\r\n", msg.len(), msg)
            }
            RedisCommand::Set { key, value, .. } => {
                write!(
                    f,
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                )
            }
            // RedisCommand::Get { .. } => {}
            // RedisCommand::Config { .. } => {}
            // RedisCommand::Keys { .. } => {}
            // RedisCommand::Info { .. } => {}
            RedisCommand::ReplConf { args } => match args {
                ReplConfArgs::ListeningPort(port) => write!(
                    f,
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                    port.len(),
                    port
                ),
                ReplConfArgs::Capa(capa) => {
                    write!(
                        f,
                        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n${}\r\n{}\r\n",
                        capa.len(),
                        capa
                    )
                }
            },
            RedisCommand::Psync { repl_id, offset } => write!(
                f,
                "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                repl_id.len(),
                repl_id,
                offset.len(),
                offset
            ),
            _ => write!(f, ""),
        }
    }
}

pub fn format_resp_array(elements: &[String]) -> String {
    let mut result = format!("*{}\r\n", elements.len());
    for element in elements {
        result.push_str(&format!("${}\r\n{}\r\n", element.len(), element))
    }
    result
}

pub fn parse_propagated_command(data: &str) -> Result<RedisCommand, String> {
    match RespParser::parse(data) {
        Ok(resp_value) => RedisCommand::from_resp_array(&resp_value),
        Err(e) => Err(format!("Failed to parse RESP: {}", e)),
    }
}

pub fn extract_complete_command(buffer: &[u8]) -> Option<(String, Vec<u8>)> {
    let data = String::from_utf8_lossy(buffer);

    if let Some(start) = data.find('*') {
        if let Some(array_len_end) = data[start..].find("\r\n") {
            let array_len_str = &data[start + 1..start + array_len_end];
            if let Ok(array_len) = array_len_str.parse::<usize>() {
                let mut pos = start + array_len_end + 2;
                let mut elements_found = 0;

                while elements_found < array_len && pos < data.len() {
                    if let Some(dollar_pos) = data[pos..].find('$') {
                        pos += dollar_pos;
                        if let Some(len_end) = data[pos..].find("\r\n") {
                            let len_str = &data[pos + 1..pos + len_end];
                            if let Ok(str_len) = len_str.parse::<usize>() {
                                pos += len_end + 2 + str_len + 2;
                                elements_found += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                if elements_found == array_len {
                    let command = data[start..pos].to_string();
                    let remaining = buffer[pos..].to_vec();
                    return Some((command, remaining));
                }
            }
        }
    }
    None
}
