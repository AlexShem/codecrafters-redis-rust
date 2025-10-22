use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
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
}

pub struct BlockedListResponse {
    pub list_key: String,
    pub element: String,
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
    ) {
        let mut waiting = self.waiting_clients.write().await;
        let queue = waiting.entry(list_key).or_insert_with(VecDeque::new);
        queue.push_back(WaitingClient {
            client_id,
            tx,
            blocked_since: Instant::now(),
        })
    }

    pub async fn notify_next_waiting_client(&self, list_key: &str, element: String) -> bool {
        let mut waiting = self.waiting_clients.write().await;

        if let Some(queue) = waiting.get_mut(list_key) {
            if let Some(client) = queue.pop_front() {
                let response = BlockedListResponse {
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
}
