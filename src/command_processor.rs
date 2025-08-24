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
        }
    }
}
