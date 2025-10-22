use crate::pubsub::{is_command_allowed_in_subscribe_mode, ClientId, PubSubClient, PubSubManager};
use crate::redis_command::{CommandResult, RedisCommand};
use crate::storage::Storage;

pub struct CommandProcessor {
    storage: Storage,
    tx_state: TransactionState,
    pub_sub_manager: PubSubManager,
    pub_sub_client: PubSubClient,
    pub_sub_state: PubSubState,
}

#[derive(Default)]
struct TransactionState {
    active: bool,
    queue: Vec<RedisCommand>,
}

#[derive(Default)]
struct PubSubState {
    active: bool,
}

impl CommandProcessor {
    pub fn new(storage: Storage, pub_sub_manager: PubSubManager, client_id: ClientId) -> Self {
        Self {
            storage,
            tx_state: TransactionState::default(),
            pub_sub_manager,
            pub_sub_client: PubSubClient::new(client_id),
            pub_sub_state: PubSubState::default(),
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
                    return CommandResult::Queued;
                }

                if self.pub_sub_state.active {
                    if matches!(other, RedisCommand::Ping) {
                        return CommandResult::Array(vec![
                            CommandResult::Value(Some(String::from("pong"))),
                            CommandResult::Value(Some(String::new())),
                        ]);
                    }
                    if !is_command_allowed_in_subscribe_mode(&other) {
                        return CommandResult::RedisError(format!("Can't execute '{}'", other));
                    }
                }

                self.execute_primitive(other).await
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
            RedisCommand::Zscore { key, member } => {
                if let Some(score) = self.storage.zscore(key, member).await {
                    CommandResult::Value(Some(score.to_string()))
                } else {
                    CommandResult::Value(None)
                }
            }
            RedisCommand::Zrem { key, member } => {
                if let Some(removed) = self.storage.zrem(key, member).await {
                    CommandResult::Integer(removed as i64)
                } else {
                    CommandResult::Integer(0)
                }
            }
            RedisCommand::Subscribe { channel } => {
                if self.pub_sub_client.subscribe(&channel) {
                    self.pub_sub_state.active = true;
                    let client_id = self.pub_sub_client.client_id();
                    self.pub_sub_manager
                        .subscribe(client_id, channel.clone())
                        .await;

                    let subscribe = String::from("subscribe");
                    let count = self.pub_sub_client.count();
                    CommandResult::Array(vec![
                        CommandResult::Value(Some(subscribe)),
                        CommandResult::Value(Some(channel)),
                        CommandResult::Integer(count as i64),
                    ])
                } else {
                    CommandResult::RedisError(String::from("Failed to subscribe to the channel"))
                }
            }
            RedisCommand::Unsubscribe { channel } => {
                let _ = self.pub_sub_client.unsubscribe(&channel);
                let client_id = self.pub_sub_client.client_id();
                self.pub_sub_manager
                    .unsubscribe(client_id, channel.clone())
                    .await;

                let count = self.pub_sub_client.count();

                if count == 0 {
                    self.pub_sub_state.active = false;
                }

                CommandResult::Array(vec![
                    CommandResult::Value(Some(String::from("unsubscribe"))),
                    CommandResult::Value(Some(channel)),
                    CommandResult::Integer(count as i64),
                ])
            }
            RedisCommand::Publish { channel, message } => {
                let count = self.pub_sub_manager.publish(channel, message).await;
                CommandResult::Integer(count as i64)
            }
            RedisCommand::Rpush { list, elements } => {
                let list_len = self.storage.rpush(list, elements).await;
                CommandResult::Integer(list_len as i64)
            }
            RedisCommand::Lrange {key,start,end} => {
                if let Some(members) = self.storage.lrange(key, start, end).await {
                    let mut values = Vec::with_capacity(members.len());
                    for member in members {
                        values.push(CommandResult::Value(Some(member)));
                    }
                    CommandResult::Array(values)
                } else {
                    CommandResult::Array(vec![])
                }
            }
            RedisCommand::Lpush { list, elements } => {
                let list_len = self.storage.lpush(list, elements).await;
                CommandResult::Integer(list_len as i64)
            }
            RedisCommand::Llen { key } => {
                if let Some(cardinality) = self.storage.llen(key).await {
                    CommandResult::Integer(cardinality as i64)
                } else {
                    CommandResult::Integer(0)
                }
            }
        }
    }
}
