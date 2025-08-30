use anyhow::anyhow;
use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tokio::time::Instant;

#[derive(Clone)]
pub struct Storage {
    data: Arc<RwLock<HashMap<String, StoredValue>>>,
    #[allow(unused)]
    file_path: Option<PathBuf>,
    dir: Option<String>,
    dbfilename: Option<String>,
}

struct StoredValue {
    value: String,
    expires_at: Option<Instant>,
}

impl Storage {
    pub async fn new(
        file_path: Option<PathBuf>,
        dir: Option<String>,
        dbfilename: Option<String>,
    ) -> Self {
        if let Some(path) = file_path {
            match read_database_file(path.clone()).await {
                Ok(data) => Self {
                    data: Arc::new(RwLock::new(data)),
                    file_path: Some(path),
                    dir,
                    dbfilename,
                },
                Err(_) => Self {
                    data: Arc::new(RwLock::new(HashMap::new())),
                    file_path: Some(path),
                    dir,
                    dbfilename,
                },
            }
        } else {
            Self {
                data: Arc::new(RwLock::new(HashMap::new())),
                file_path,
                dir,
                dbfilename,
            }
        }
    }

    pub fn get_config(&self, key: &str) -> Option<String> {
        match key {
            "dir" => self.dir.clone(),
            "dbfilename" => self.dbfilename.clone(),
            _ => None,
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

    pub async fn get_all(&self) -> Option<Vec<String>> {
        let data = self.data.write().await;
        let mut keys = Vec::with_capacity(data.len());
        for key in data.keys() {
            keys.push(key.clone());
        }
        if keys.is_empty() {
            None
        } else {
            Some(keys)
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

async fn read_database_file(file_path: PathBuf) -> anyhow::Result<HashMap<String, StoredValue>> {
    let mut file = File::open(file_path).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;

    let mut content = Bytes::from(buf);

    // Start parsing the database

    // 1. Parse header
    if content.len() < 9 {
        return Err(anyhow!("File too short to contain valid RDB header"));
    }
    let magic = content.slice(0..5);
    if &magic[..] != b"REDIS" {
        return Err(anyhow!("Invalid magic string, expected REDIS"));
    }
    let version = content.slice(5..9);
    let version_str = std::str::from_utf8(&version)?;

    content.advance(9);
    println!("RDB Version: {}", version_str);

    // 2. Metadata section
    let metadata = read_metadata(&mut content)?;
    metadata
        .iter()
        .for_each(|data| println!("Metadata: {}", data));

    // 3. Database section
    let database = read_database(&mut content)?;
    database
        .iter()
        .for_each(|entry| println!("Data Key: {}", entry.0));

    // 4. End of file section
    let end_of_file = read_eof(&mut content)?;
    println!("End of file check sum: {}", end_of_file);

    Ok(database)
}

fn read_metadata(content: &mut Bytes) -> anyhow::Result<Vec<String>> {
    let mut metadata = Vec::new();

    while let Some(&first_byte) = content.first() {
        if first_byte == 0xFA {
            content.advance(1);
            if let (Ok(name), Ok(value)) = (read_encoded(content), read_encoded(content)) {
                metadata.push(format!("{}:{}", name, value));
            } else {
                break;
            }
        } else {
            break;
        }
    }
    Ok(metadata)
}

fn read_database(content: &mut Bytes) -> anyhow::Result<HashMap<String, StoredValue>> {
    let mut database: HashMap<String, StoredValue> = HashMap::new();

    while let Some(&first_byte) = content.first() {
        if first_byte == 0xFE {
            content.advance(1);
            let _database_index = read_encoded(content)?;

            let indicator = content.get_u8();
            if indicator != 0xFB {
                return Err(anyhow!(
                    "Database indicator 0xFB was expected. Got: {}",
                    indicator
                ));
            }

            content.advance(2); // todo: Should read the sizes of tables here instead of advancing

            while let Some(&table_type) = content.first() {
                match table_type {
                    0xFD => {
                        content.advance(1);
                        let timestamp_seconds = content.get_u32_le();
                        let key_value_indicator = content.get_u8();
                        if key_value_indicator != 0x00 {
                            return Err(anyhow!(
                                "Expected 0x00 to read key-value. Got: {}",
                                key_value_indicator
                            ));
                        }
                        let (key, value) = (read_encoded(content)?, read_encoded(content)?);
                        // todo This duration is calculated incorrectly
                        let stored_value =
                            StoredValue::with_expiry(value, timestamp_seconds as u64);
                        database.insert(key, stored_value);
                    }
                    0xFC => {
                        content.advance(1);
                        let timestamp_milliseconds = content.get_u64_le();
                        let key_value_indicator = content.get_u8();
                        if key_value_indicator != 0x00 {
                            return Err(anyhow!(
                                "Expected 0x00 to read key-value. Got: {}",
                                key_value_indicator
                            ));
                        }
                        let (key, value) = (read_encoded(content)?, read_encoded(content)?);
                        // todo This duration is calculated incorrectly
                        let stored_value = StoredValue::with_expiry(value, timestamp_milliseconds);
                        database.insert(key, stored_value);
                    }
                    0x00 => {
                        content.advance(1);
                        let (key, value) = (read_encoded(content)?, read_encoded(content)?);
                        let stored_value = StoredValue::new(value);
                        database.insert(key, stored_value);
                    }
                    _ => break,
                }
            }
        } else {
            break;
        }
    }
    Ok(database)
}

fn read_eof(content: &mut Bytes) -> anyhow::Result<String> {
    if let Some(&first_byte) = content.first() {
        if first_byte == 0xFF {
            content.advance(1);
            if content.remaining() != 8 {
                return Err(anyhow!(
                    "End of file is expected to be 8 bytes. Got: {}",
                    content.remaining()
                ));
            }
            let check_sum = content.get_u64();
            Ok(format!("{}", check_sum))
        } else {
            Err(anyhow!(
                "EOF was expected to start with 0xFF. Got: {}",
                first_byte
            ))
        }
    } else {
        Err(anyhow!("EOF cannot be empty"))
    }
}

fn read_encoded(content: &mut Bytes) -> anyhow::Result<String> {
    if content.is_empty() {
        return Err(anyhow!("Encoded value must not be empty"));
    }

    let size_encoding = content.get_u8();
    let first_two_bytes = size_encoding & 0b1100_0000;

    match first_two_bytes >> 6 {
        0b00 => {
            let length = size_encoding as usize;
            let value = content.copy_to_bytes(length);
            Ok(String::from_utf8(value.to_vec())?)
        }
        0b01 => {
            let second_byte = content.get_u8();
            let length = u16::from_be_bytes([size_encoding & 0b0011_1111, second_byte]);
            let value = content.copy_to_bytes(length as usize);
            Ok(String::from_utf8(value.to_vec())?)
        }
        0b10 => {
            let length = content.get_u32();
            let value = content.copy_to_bytes(length as usize);
            Ok(String::from_utf8(value.to_vec())?)
        }
        0b11 => {
            // String encoding
            match size_encoding {
                0xC0 => {
                    let value = content.get_u8();
                    Ok(value.to_string())
                }
                0xC1 => {
                    let value = content.get_u16_le();
                    Ok(value.to_string())
                }
                0xC2 => {
                    let value = content.get_u32_le();
                    Ok(value.to_string())
                }
                0xC3 => todo!(),
                _ => Err(anyhow!("Unexpected string encoding: {}", size_encoding)),
            }
        }
        _ => Err(anyhow!("Unexpected size encoded value: {}", size_encoding)),
    }
}
