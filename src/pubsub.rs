use crate::redis_command::RedisCommand;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;

pub type ClientId = u64;

#[derive(Clone)]
pub struct PubSubMessage {
    pub channel: String,
    pub message: String,
}

#[derive(Clone)]
pub struct PubSubManager {
    /// Maps channel names to sets of subscribed client IDs
    channels: Arc<RwLock<HashMap<String, HashSet<ClientId>>>>,
    senders: Arc<RwLock<HashMap<ClientId, UnboundedSender<PubSubMessage>>>>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_client(
        &self,
        client_id: ClientId,
        sender: UnboundedSender<PubSubMessage>,
    ) {
        let mut senders = self.senders.write().await;
        senders.insert(client_id, sender);
    }

    pub async fn unregister_client(&self, client_id: ClientId) {
        let mut senders = self.senders.write().await;
        senders.remove(&client_id);
    }

    pub async fn subscribe(&self, client_id: ClientId, channel: String) {
        let mut channels = self.channels.write().await;
        channels
            .entry(channel)
            .or_insert_with(HashSet::new)
            .insert(client_id);
    }

    pub async fn unsubscribe(&self, client_id: ClientId, channel: String) {
        let mut channels = self.channels.write().await;
        if let Some(target_channel) = channels.get_mut(&channel) {
            target_channel.remove(&client_id);
            
            if target_channel.is_empty() {
                channels.remove(&channel);
            }
        }
    }

    pub async fn publish(&self, channel: String, message: String) -> usize {
        let channels = self.channels.read().await;
        let subscribers = match channels.get(&channel) {
            None => return 0,
            Some(subs) => subs.clone(),
        };

        drop(channels);
        let count = subscribers.len();

        let senders = self.senders.read().await;
        let pub_sub_message = PubSubMessage {
            channel: channel.clone(),
            message: message.clone(),
        };

        for client_id in subscribers {
            if let Some(sender) = senders.get(&client_id) {
                let _ = sender.send(pub_sub_message.clone());
            }
        }

        count
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

    pub fn unsubscribe(&mut self, channel: &String) -> bool {
        self.channels.remove(channel)
    }

    pub fn count(&self) -> usize {
        self.channels.len()
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
}

pub fn is_command_allowed_in_subscribe_mode(command: &RedisCommand) -> bool {
    matches!(
        command,
        RedisCommand::Subscribe { .. } | RedisCommand::Ping | RedisCommand::Unsubscribe { .. }
    )
}
