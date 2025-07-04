use crate::resp;
use crate::resp::RespValue;
use anyhow::anyhow;
use resp::RespParser;
use std::collections::HashMap;
use tokio::io::{BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Set { key: String, value: String },
    Get { key: String },
}

pub struct Server {
    pub reader: BufReader<OwnedReadHalf>,
    pub writer: BufWriter<OwnedWriteHalf>,
    pub storage: HashMap<String, String>,
}

impl Server {
    pub fn new(stream: TcpStream) -> Self {
        let (read, write) = stream.into_split();
        let reader = BufReader::new(read);
        let writer = BufWriter::new(write);
        Server {
            reader,
            writer,
            storage: HashMap::new(),
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
                        if elements.len() != 3 {
                            return Err(anyhow!("SET requires exactly 2 arguments"));
                        }
                        let key = match &elements[1] {
                            RespValue::BulkString(Some(k)) => k.clone(),
                            _ => return Err(anyhow!("SET key must be a string")),
                        };

                        let value = match &elements[2] {
                            RespValue::BulkString(Some(v)) => v.clone(),
                            _ => return Err(anyhow!("SET value must be a string")),
                        };

                        Ok(Command::Set { key, value })
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
            Command::Set { key, value } => {
                self.storage.insert(key, value);
                "+OK\r\n".to_string()
            }
            Command::Get { key } => match self.storage.get(&key) {
                None => "-1\r\n".to_string(),
                Some(value) => format!("${}\r\n{}\r\n", value.len(), value),
            },
        }
    }
}
