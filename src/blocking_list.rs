use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;
use tokio::time::Instant;

pub type ClientId = u64;

#[derive(Clone)]
pub struct BlockingListManager {
    waiting_clients: Arc<RwLock<HashMap<String, VecDeque<WaitingClient>>>>,
}

struct WaitingClient {
    #[allow(unused)]
    client_id: ClientId,
    tx: UnboundedSender<BlockedListResponse>,
    #[allow(unused)]
    blocked_since: Instant,
    timeout_duration: Option<Duration>,
}

pub enum BlockedListResponse {
    Element { list_key: String, element: String },
    Timeout,
}

impl BlockingListManager {
    pub fn new() -> Self {
        Self {
            waiting_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_waiting_client(
        &self,
        list_key: String,
        client_id: ClientId,
        tx: UnboundedSender<BlockedListResponse>,
        timeout_seconds: f64,
    ) {
        let mut waiting = self.waiting_clients.write().await;
        let queue = waiting.entry(list_key).or_insert_with(VecDeque::new);

        let timeout_duration = if timeout_seconds > 0.0 {
            Some(Duration::from_secs_f64(timeout_seconds))
        } else {
            None
        };

        queue.push_back(WaitingClient {
            client_id,
            tx,
            blocked_since: Instant::now(),
            timeout_duration,
        })
    }

    pub async fn notify_next_waiting_client(&self, list_key: &str, element: String) -> bool {
        let mut waiting = self.waiting_clients.write().await;

        if let Some(queue) = waiting.get_mut(list_key) {
            if let Some(client) = queue.pop_front() {
                let response = BlockedListResponse::Element {
                    list_key: list_key.to_string(),
                    element,
                };
                let _ = client.tx.send(response);

                if queue.is_empty() {
                    waiting.remove(list_key);
                }
                return true;
            }
        }
        false
    }

    pub async fn has_waiting_clients(&self, list_key: &str) -> bool {
        let waiting = self.waiting_clients.read().await;
        waiting.get(list_key).map_or(false, |q| !q.is_empty())
    }

    pub async fn check_timeout(&self) {
        let mut waiting = self.waiting_clients.write().await;
        let mut keys_to_remove = Vec::new();

        for (list_key, queue) in waiting.iter_mut() {
            let now = Instant::now();

            queue.retain(|client| {
                if let Some(timeout) = client.timeout_duration {
                    if now.duration_since(client.blocked_since) >= timeout {
                        let _ = client.tx.send(BlockedListResponse::Timeout);
                        return false;
                    }
                }
                true
            });

            if queue.is_empty() {
                keys_to_remove.push(list_key.clone());
            }
        }

        for key in keys_to_remove {
            waiting.remove(&key);
        }
    }
}
