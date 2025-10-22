use crate::redis_command::RedisCommand;
use crate::types::{parse_value, Value};
use anyhow::anyhow;
use bytes::Bytes;
use std::str::FromStr;

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
                        if elements.len() < 3 {
                            return Err(anyhow!("SET command requires exactly two arguments"));
                        }

                        let key = self.extract_string(&elements[1])?;
                        let value = self.extract_string(&elements[2])?;

                        if elements.len() == 5 {
                            let px_arg = self.extract_string(&elements[3])?.to_uppercase();
                            if px_arg == "PX" {
                                let expiry_str = self.extract_string(&elements[4])?;
                                let expiry_ms = expiry_str
                                    .parse::<u64>()
                                    .map_err(|_| anyhow!("Invalid expiry time: {}", expiry_str))?;

                                Ok(RedisCommand::SetWithExpiry {
                                    key,
                                    value,
                                    expiry_ms,
                                })
                            } else {
                                Err(anyhow!("Unsupported SET argument: {}", px_arg))
                            }
                        } else if elements.len() == 3 {
                            Ok(RedisCommand::Set { key, value })
                        } else {
                            Err(anyhow!("Invalid number of arguments for SET command"))
                        }
                    }
                    "GET" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("GET command requires exactly one argument"));
                        }

                        let key = self.extract_string(&elements[1])?;
                        Ok(RedisCommand::Get { key })
                    }
                    "INCR" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("INCR command requires exactly one argument"));
                        }

                        let key = self.extract_string(&elements[1])?;
                        Ok(RedisCommand::Incr(key))
                    }
                    "MULTI" => Ok(RedisCommand::Multi),
                    "EXEC" => Ok(RedisCommand::Exec),
                    "DISCARD" => Ok(RedisCommand::Discard),
                    "CONFIG" => {
                        if elements.len() < 2 {
                            return Err(anyhow!(
                                "CONFIG command must be followed by another keyword"
                            ));
                        }

                        let command_subname = match &elements[1] {
                            Value::SimpleString(bytes) => {
                                String::from_utf8(bytes.clone())?.to_uppercase()
                            }
                            Value::BulkString(bytes) => {
                                String::from_utf8(bytes.clone())?.to_uppercase()
                            }
                            _ => return Err(anyhow!("Invalid command format")),
                        };

                        if command_subname != "GET" {
                            return Err(anyhow!(
                                "CONFIG {} command is not supported",
                                command_subname
                            ));
                        }

                        if elements.len() < 3 {
                            return Err(anyhow!(
                                "CONFIG GET command requires exactly one argument"
                            ));
                        }

                        let argument = self.extract_string(&elements[2])?;
                        Ok(RedisCommand::ConfigGet(argument))
                    }
                    "KEYS" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("KEYS command requires exactly one argument"));
                        }

                        let pattern = self.extract_string(&elements[1])?;
                        Ok(RedisCommand::Keys(pattern))
                    }
                    "ZADD" => {
                        if elements.len() != 4 {
                            return Err(anyhow!("ZADD command requires exactly three arguments"));
                        }
                        let key = self.extract_string(&elements[1])?;
                        let score_str = self.extract_string(&elements[2])?;
                        let member = self.extract_string(&elements[3])?;

                        let score = f64::from_str(&score_str)?;
                        Ok(RedisCommand::Zadd { key, score, member })
                    }
                    "ZRANK" => {
                        if elements.len() != 3 {
                            return Err(anyhow!("ZRANK command requires exactly two arguments"));
                        }
                        let key = self.extract_string(&elements[1])?;
                        let member = self.extract_string(&elements[2])?;

                        Ok(RedisCommand::Zrank { key, member })
                    }
                    "ZRANGE" => {
                        if elements.len() != 4 {
                            return Err(anyhow!("ZRANGE command requires exactly three arguments"));
                        }
                        let key = self.extract_string(&elements[1])?;
                        let start: i32 = self.extract_string(&elements[2])?.parse()?;
                        let end: i32 = self.extract_string(&elements[3])?.parse()?;

                        Ok(RedisCommand::Zrange { key, start, end })
                    }
                    "ZCARD" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("ZCARD command requires exactly one argument"));
                        }
                        let key = self.extract_string(&elements[1])?;

                        Ok(RedisCommand::Zcard { key })
                    }
                    "ZSCORE" => {
                        if elements.len() != 3 {
                            return Err(anyhow!("ZSCORE command requires exactly two arguments"));
                        }

                        let key = self.extract_string(&elements[1])?;
                        let member = self.extract_string(&elements[2])?;

                        Ok(RedisCommand::Zscore { key, member })
                    }
                    "ZREM" => {
                        if elements.len() != 3 {
                            return Err(anyhow!("ZREM command requires exactly two arguments"));
                        }
                        let key = self.extract_string(&elements[1])?;
                        let member = self.extract_string(&elements[2])?;

                        Ok(RedisCommand::Zrem { key, member })
                    }
                    "SUBSCRIBE" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("SUBSCRIBE command requires exactly one argument"));
                        }
                        let channel = self.extract_string(&elements[1])?;

                        Ok(RedisCommand::Subscribe { channel })
                    }
                    "UNSUBSCRIBE" => {
                        if elements.len() != 2 {
                            return Err(anyhow!(
                                "UNSUBSCRIBE command requires exactly one argument"
                            ));
                        }
                        let channel = self.extract_string(&elements[1])?;

                        Ok(RedisCommand::Unsubscribe { channel })
                    }
                    "PUBLISH" => {
                        if elements.len() != 3 {
                            return Err(anyhow!("PUBLISH command requires exactly two argument"));
                        }
                        let channel = self.extract_string(&elements[1])?;
                        let message = self.extract_string(&elements[2])?;

                        Ok(RedisCommand::Publish { channel, message })
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

    #[allow(unused)]
    fn extract_double(&self, value: &Value) -> anyhow::Result<f64> {
        match value {
            Value::Double(val) => Ok(val.clone()),
            _ => Err(anyhow!("Expected double value")),
        }
    }
}
