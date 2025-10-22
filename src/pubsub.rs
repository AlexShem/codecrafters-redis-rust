use crate::redis_command::RedisCommand;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub type ClientId = u64;

#[derive(Clone)]
pub struct PubSubManager {
    /// Maps channel names to sets of subscribed client IDs
    channels: Arc<RwLock<HashMap<String, HashSet<ClientId>>>>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn subscribe(&self, client_id: ClientId, channel: String) {
        let mut channels = self.channels.write().await;
        channels
            .entry(channel)
            .or_insert_with(HashSet::new)
            .insert(client_id);
    }

    pub async fn get_subscriber_count(&self, channel: String) -> usize {
        let channels = self.channels.read().await;
        channels
            .get(&channel)
            .map(|subscribers| subscribers.len())
            .unwrap_or(0)
    }
}

pub struct PubSubClient {
    client_id: ClientId,
    channels: HashSet<String>,
}

impl PubSubClient {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            channels: HashSet::new(),
        }
    }

    pub fn subscribe(&mut self, channel: &String) -> bool {
        self.channels.insert(channel.clone())
    }

    pub fn count(&self) -> usize {
        self.channels.len()
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
}

pub fn is_command_allowed_in_subscribe_mode(command: &RedisCommand) -> bool {
    matches!(command, RedisCommand::Subscribe { .. } | RedisCommand::Ping)
}
