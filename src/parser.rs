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
                    _ => Err(anyhow!("Unsupported command: {}", command_name)),
                }
            }
            _ => Err(anyhow!("Commands must be arrays")),
        }
    }
}
