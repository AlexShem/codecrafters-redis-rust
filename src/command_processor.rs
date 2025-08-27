use crate::redis_command::{CommandResult, RedisCommand};
use crate::storage::Storage;

pub struct CommandProcessor {
    storage: Storage,
}

impl CommandProcessor {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    pub async fn execute(&self, command: RedisCommand) -> CommandResult {
        match command {
            RedisCommand::Ping => CommandResult::Pong,
            RedisCommand::Echo(message) => CommandResult::Echo(message),
            RedisCommand::Set { key, value } => {
                self.storage.set(key, value).await;
                CommandResult::Ok
            }
            RedisCommand::SetWithExpiry {
                key,
                value,
                expiry_ms,
            } => {
                self.storage.set_with_expiry(key, value, expiry_ms).await;
                CommandResult::Ok
            }
            RedisCommand::Get { key } => {
                let value = self.storage.get(&key).await;
                CommandResult::Value(value)
            }
            RedisCommand::Incr(key) => {
                let new_value = match self.storage.get(&key).await {
                    None => 1,
                    Some(value_str) => {
                        if let Ok(value) = value_str.parse::<i64>() {
                            dbg!(&value);
                            value + 1
                        } else {
                            dbg!(&value_str);
                            return CommandResult::RedisError(
                                "value is not an integer or out of range".to_string(),
                            );
                        }
                    }
                };
                self.storage.set(key, new_value.to_string()).await;
                CommandResult::Integer(new_value)
            }
        }
    }
}
