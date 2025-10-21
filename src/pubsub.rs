use crate::redis_command::RedisCommand;
use std::collections::HashSet;

pub struct PubSubClient {
    channels: HashSet<String>,
}

impl PubSubClient {
    pub fn new() -> Self {
        Self {
            channels: HashSet::new(),
        }
    }

    pub fn subscribe(&mut self, channel: &String) -> anyhow::Result<bool> {
        Ok(self.channels.insert(channel.clone()))
    }

    pub fn count(&self) -> usize {
        self.channels.len()
    }
}

pub fn is_command_allowed_in_subscribe_mode(command: &RedisCommand) -> bool {
    matches!(command, RedisCommand::Subscribe { .. } | RedisCommand::Ping)
}
