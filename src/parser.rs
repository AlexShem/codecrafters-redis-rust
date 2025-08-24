use crate::redis_command::RedisCommand;
use crate::types::{parse_value, Value};
use anyhow::anyhow;
use bytes::Bytes;

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Self
    }

    pub(crate) fn parse_command(&self, mut buf: Bytes) -> anyhow::Result<RedisCommand> {
        let value = parse_value(&mut buf)?;
        self.value_to_command(value)
    }

    fn value_to_command(&self, value: Value) -> anyhow::Result<RedisCommand> {
        match value {
            Value::Array(elements) => {
                if elements.is_empty() {
                    return Err(anyhow!("Empty command array"));
                }

                let command_name = match &elements[0] {
                    Value::SimpleString(bytes) => String::from_utf8(bytes.clone())?.to_uppercase(),
                    Value::BulkString(bytes) => String::from_utf8(bytes.clone())?.to_uppercase(),
                    _ => return Err(anyhow!("Invalid command format")),
                };

                match command_name.as_str() {
                    "PING" => Ok(RedisCommand::Ping),
                    "ECHO" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("ECHO command requires exactly one argument"));
                        }

                        let message = match &elements[1] {
                            Value::BulkString(bytes) => String::from_utf8(bytes.clone())?,
                            Value::SimpleString(bytes) => String::from_utf8(bytes.clone())?,
                            _ => return Err(anyhow::anyhow!("ECHO argument must be a string")),
                        };

                        Ok(RedisCommand::Echo(message))
                    }
                    "SET" => {
                        if elements.len() != 3 {
                            return Err(anyhow!("SET command requires exactly two arguments"));
                        }

                        let key = self.extract_string(&elements[1])?;
                        let value = self.extract_string(&elements[2])?;

                        Ok(RedisCommand::Set { key, value })
                    }
                    "GET" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("GET command requires exactly one argument"));
                        }

                        let key = self.extract_string(&elements[1])?;
                        Ok(RedisCommand::Get { key })
                    }
                    _ => Err(anyhow!("Unsupported command: {}", command_name)),
                }
            }
            _ => Err(anyhow!("Commands must be arrays")),
        }
    }

    fn extract_string(&self, value: &Value) -> anyhow::Result<String> {
        match value {
            Value::SimpleString(bytes) => String::from_utf8(bytes.clone())
                .map_err(|e| anyhow!("Invalid UTF-8 in string: {}", e)),
            Value::BulkString(bytes) => String::from_utf8(bytes.clone())
                .map_err(|e| anyhow!("Invalid UTF-8 in string: {}", e)),
            _ => Err(anyhow!("Expected string value")),
        }
    }
}
