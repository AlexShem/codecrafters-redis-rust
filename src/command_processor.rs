use crate::blocking_list::{BlockedListResponse, BlockingListManager};
use crate::geospatial;
use crate::geospatial::{decode, distance, is_valid_latitude, is_valid_longitude};
use crate::pubsub::{is_command_allowed_in_subscribe_mode, ClientId, PubSubClient, PubSubManager};
use crate::redis_command::{CommandResult, RedisCommand};
use crate::storage::Storage;
use tokio::sync::mpsc::UnboundedSender;

pub struct CommandProcessor {
    storage: Storage,
    tx_state: TransactionState,
    pub_sub_manager: PubSubManager,
    pub_sub_client: PubSubClient,
    pub_sub_state: PubSubState,
    blocking_list_manager: BlockingListManager,
    blocking_tx: UnboundedSender<BlockedListResponse>,
    client_id: ClientId,
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
    pub fn new(
        storage: Storage,
        pub_sub_manager: PubSubManager,
        blocking_list_manager: BlockingListManager,
        client_id: ClientId,
        blocking_tx: UnboundedSender<BlockedListResponse>,
    ) -> Self {
        Self {
            storage,
            tx_state: TransactionState::default(),
            pub_sub_manager,
            pub_sub_client: PubSubClient::new(client_id),
            pub_sub_state: PubSubState::default(),
            blocking_list_manager,
            blocking_tx,
            client_id,
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
                let (list_len, was_empty) = self.storage.rpush(list.clone(), elements).await;

                if was_empty && self.blocking_list_manager.has_waiting_clients(&list).await {
                    if let Some(popped) = self.storage.lpop(list.clone(), Some(1)).await {
                        self.blocking_list_manager
                            .notify_next_waiting_client(&list, popped[0].clone())
                            .await;
                    }
                }

                CommandResult::Integer(list_len as i64)
            }
            RedisCommand::Lrange { key, start, end } => {
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
            RedisCommand::Lpop { key, count } => {
                let elements = self.storage.lpop(key, count).await;
                match elements {
                    None => CommandResult::Value(None),
                    Some(list) => {
                        if list.len() == 1 {
                            CommandResult::Value(Some(list[0].clone()))
                        } else {
                            let list_of_elements = list
                                .iter()
                                .map(|el| CommandResult::Value(Some(el.clone())))
                                .collect();
                            CommandResult::Array(list_of_elements)
                        }
                    }
                }
            }
            RedisCommand::Blpop { key, timeout } => {
                if let Some(elements) = self.storage.lpop(key.clone(), Some(1)).await {
                    return CommandResult::Array(vec![
                        CommandResult::Value(Some(key)),
                        CommandResult::Value(Some(elements[0].clone())),
                    ]);
                }

                self.blocking_list_manager
                    .register_waiting_client(key, self.client_id, self.blocking_tx.clone(), timeout)
                    .await;

                CommandResult::Blocked
            }
            RedisCommand::Geoadd {
                key,
                longitude,
                latitude,
                member,
            } => {
                // Validate longitude and latitude
                if !is_valid_longitude(longitude) || !is_valid_latitude(latitude) {
                    CommandResult::RedisError(format!(
                        "invalid longitude,latitude pair {},{}",
                        longitude, latitude
                    ))
                } else {
                    // Calculate score
                    let score = geospatial::encode(latitude, longitude) as f64;
                    self.storage.zadd(key, score, member).await;
                    CommandResult::Integer(1)
                }
            }
            RedisCommand::Geopos { key, positions } => {
                let sorted_sets = self.storage.sorted_sets.read().await;
                if !sorted_sets.contains_key(&key) {
                    let mut responses = Vec::with_capacity(positions.len());
                    for _ in positions {
                        responses.push(CommandResult::NullArray);
                    }
                    let response = CommandResult::Array(responses);
                    return response;
                }

                let sorted_set = sorted_sets.get(&key).unwrap();
                let mut responses: Vec<CommandResult> = Vec::with_capacity(positions.len());

                for position in positions {
                    if let Some(coord) = sorted_set.by_member.get(&position) {
                        let (lon, lat) = decode(coord.clone() as u64);
                        responses.push(CommandResult::Array(vec![
                            CommandResult::Value(Some(lon.to_string())),
                            CommandResult::Value(Some(lat.to_string())),
                        ]));
                    } else {
                        responses.push(CommandResult::NullArray);
                    }
                }
                CommandResult::Array(responses)
            }
            RedisCommand::Geodist { key, from, to } => {
                let sorted_sets = self.storage.sorted_sets.read().await;
                if !sorted_sets.contains_key(&key) {
                    return CommandResult::NullArray;
                }

                let sorted_set = sorted_sets.get(&key).unwrap();
                if !sorted_set.by_member.contains_key(&from)
                    || !sorted_set.by_member.contains_key(&to)
                {
                    return CommandResult::NullArray;
                }

                let score_from = sorted_set.by_member.get(&from).unwrap();
                let score_to = sorted_set.by_member.get(&to).unwrap();
                let (lon1, lat1) = decode(score_from.clone() as u64);
                let (lon2, lat2) = decode(score_to.clone() as u64);

                let distance = distance(lon1, lat1, lon2, lat2);
                CommandResult::Value(Some(distance.to_string()))
            }
            RedisCommand::Geosearch {
                key,
                longitude,
                latitude,
                radius,
            } => {
                let sorted_sets = self.storage.sorted_sets.read().await;
                if !sorted_sets.contains_key(&key) {
                    return CommandResult::NullArray;
                }
                let mut result = Vec::new();
                let sorted_set = sorted_sets.get(&key).unwrap();
                for location in sorted_set.ordered.iter() {
                    let location_coord = decode(location.score as u64);
                    let distance =
                        distance(longitude, latitude, location_coord.0, location_coord.1);
                    if distance <= radius {
                        result.push(CommandResult::Value(Some(location.member.clone())));
                    }
                }
                CommandResult::Array(result)
            }
        }
    }
}
