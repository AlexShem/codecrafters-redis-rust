use crate::redis_command::{CommandResult, RedisCommand};
use crate::storage::Storage;

pub struct CommandProcessor {
    storage: Storage,
    tx_state: TransactionState,
}

#[derive(Default)]
struct TransactionState {
    active: bool,
    queue: Vec<RedisCommand>,
}

impl CommandProcessor {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            tx_state: TransactionState::default(),
        }
    }

    pub async fn execute(&mut self, command: RedisCommand) -> CommandResult {
        match command {
            RedisCommand::Multi => {
                self.tx_state.active = true;
                self.tx_state.queue.clear();
                CommandResult::Ok
            }
            RedisCommand::Exec => {
                if !self.tx_state.active {
                    return CommandResult::RedisError("EXEC without MULTI".to_string());
                }

                self.tx_state.active = false;
                let queued = std::mem::take(&mut self.tx_state.queue);
                if queued.is_empty() {
                    return CommandResult::Array(vec![]);
                }

                let mut results = Vec::with_capacity(queued.len());
                for queued_cmd in queued {
                    results.push(self.execute_primitive(queued_cmd).await);
                }

                CommandResult::Array(results)
            }
            RedisCommand::Discard => {
                if !self.tx_state.active {
                    return CommandResult::RedisError("DISCARD without MULTI".to_string());
                }

                self.tx_state.active = false;
                self.tx_state.queue.clear();
                CommandResult::Ok
            }
            other => {
                if self.tx_state.active {
                    self.tx_state.queue.push(other);
                    CommandResult::Queued
                } else {
                    self.execute_primitive(other).await
                }
            }
        }
    }

    pub async fn execute_primitive(&mut self, command: RedisCommand) -> CommandResult {
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
                    Some(value_str) => match value_str.parse::<i64>() {
                        Ok(value) => value + 1,
                        Err(_) => {
                            return CommandResult::RedisError(
                                "value is not an integer or out of range".to_string(),
                            );
                        }
                    },
                };
                self.storage.set(key, new_value.to_string()).await;
                CommandResult::Integer(new_value)
            }
            RedisCommand::Multi | RedisCommand::Exec | RedisCommand::Discard => {
                CommandResult::RedisError("Internal command routing error".to_string())
            }
            RedisCommand::ConfigGet(argument) => match argument.as_str() {
                "dir" | "dbfilename" => {
                    if let Some(value) = self.storage.get_config(&argument) {
                        CommandResult::ConfigValue(argument, value)
                    } else {
                        CommandResult::ConfigValue(argument, String::new())
                    }
                }
                arg => CommandResult::RedisError(format!(
                    "CONFIG GET does not support this argument: {}",
                    arg
                )),
            },
            RedisCommand::Keys(pattern) => {
                if pattern == "*" {
                    if let Some(keys) = self.storage.get_all().await {
                        let mut values = Vec::with_capacity(keys.len());
                        for key in keys {
                            values.push(CommandResult::Value(Some(key)));
                        }
                        CommandResult::Array(values)
                    } else {
                        CommandResult::Value(None)
                    }
                } else {
                    CommandResult::RedisError(format!("Pattern {} is not supported", pattern))
                }
            }
            RedisCommand::Zadd { key, score, member } => {
                let added_count = self.storage.zadd(key, score, member).await;
                CommandResult::Integer(added_count as i64)
            }
            RedisCommand::Zrank { key, member } => {
                if let Some(rank) = self.storage.zrank(key, member).await {
                    CommandResult::Integer(rank as i64)
                } else {
                    CommandResult::Value(None)
                }
            }
            RedisCommand::Zrange { key, start, end } => {
                if let Some(members) = self.storage.zrange(key, start, end).await {
                    let mut values = Vec::with_capacity(members.len());
                    for member in members {
                        values.push(CommandResult::Value(Some(member)));
                    }
                    CommandResult::Array(values)
                } else {
                    CommandResult::Array(vec![])
                }
            }
            RedisCommand::Zcard { key } => {
                if let Some(cardinality) = self.storage.zcard(key).await {
                    CommandResult::Integer(cardinality as i64)
                } else {
                    CommandResult::Integer(0)
                }
            }
            RedisCommand::Zscore {key, member} => {
                if let Some(score) = self.storage.zscore(key, member).await {
                    CommandResult::Value(Some(score.to_string()))
                } else {
                    CommandResult::Value(None)
                }
            }
        }
    }
}
