use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;

#[derive(Clone)]
pub struct Storage {
    data: Arc<RwLock<HashMap<String, StoredValue>>>,
}

struct StoredValue {
    value: String,
    expires_at: Option<Instant>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set(&self, key: String, value: String) {
        let stored_value = StoredValue::new(value);
        let mut data = self.data.write().await;
        data.insert(key, stored_value);
    }

    pub async fn set_with_expiry(&self, key: String, value: String, expiry_ms: u64) {
        let stored_value = StoredValue::with_expiry(value, expiry_ms);
        let mut data = self.data.write().await;
        data.insert(key, stored_value);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let mut data = self.data.write().await;

        if let Some(stored_value) = data.get(key) {
            if stored_value.is_expired() {
                data.remove(key);
                None
            } else {
                Some(stored_value.value.clone())
            }
        } else {
            None
        }
    }
}

impl StoredValue {
    pub fn new(value: String) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }

    fn with_expiry(value: String, duration_ms: u64) -> Self {
        Self {
            value,
            expires_at: Some(Instant::now() + Duration::from_millis(duration_ms)),
        }
    }

    fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() > expires_at
        } else {
            false
        }
    }
}
