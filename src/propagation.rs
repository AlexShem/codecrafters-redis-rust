use crate::commands::RedisCommand;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct PropagationManager {
    replicas: Arc<Mutex<Vec<BufWriter<OwnedWriteHalf>>>>,
}

impl PropagationManager {
    pub fn new() -> Self {
        Self {
            replicas: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn add_replica(&self, writer: BufWriter<OwnedWriteHalf>) {
        let mut replicas = self.replicas.lock().await;
        replicas.push(writer);
    }

    pub async fn propagate_command(&self, command: &RedisCommand) {
        if !is_write_command(command) {
            return;
        }

        let command_str = command.to_string();
        let command_bytes = command_str.as_bytes();
        let mut replicas = self.replicas.lock().await;

        // Remove disconnected replicas
        let mut i = 0;
        while i < replicas.len() {
            match replicas[i].write_all(command_bytes).await {
                Ok(_) => {
                    if replicas[i].flush().await.is_err() {
                        replicas.remove(i);
                    } else {
                        i += 1;
                    }
                }
                Err(_) => {
                    replicas.remove(i);
                }
            }
        }
    }
}

fn is_write_command(redis_command: &RedisCommand) -> bool {
    matches!(redis_command, RedisCommand::Set { .. })
}
