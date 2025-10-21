use anyhow::anyhow;
use bytes::{Buf, Bytes};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio::time::Instant;

#[derive(Clone)]
pub struct Storage {
    data: Arc<RwLock<HashMap<String, StoredValue>>>,
    /// Sorted sets, stored as set name `String` and the `SortedSet`.
    sorted_sets: Arc<RwLock<HashMap<String, SortedSet>>>,
    #[allow(unused)]
    file_path: Option<PathBuf>,
    dir: Option<String>,
    dbfilename: Option<String>,
}

struct StoredValue {
    value: String,
    expires_at: Option<Instant>,
}

struct SortedSet {
    by_member: HashMap<String, f64>,
    ordered: BTreeSet<ScoredMember>,
}

#[derive(Clone)]
struct ScoredMember {
    score: f64,
    member: String,
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
                    sorted_sets: Arc::new(RwLock::new(HashMap::new())),
                    file_path: Some(path),
                    dir,
                    dbfilename,
                },
                Err(_) => Self {
                    data: Arc::new(RwLock::new(HashMap::new())),
                    sorted_sets: Arc::new(RwLock::new(HashMap::new())),
                    file_path: Some(path),
                    dir,
                    dbfilename,
                },
            }
        } else {
            Self {
                data: Arc::new(RwLock::new(HashMap::new())),
                sorted_sets: Arc::new(RwLock::new(HashMap::new())),
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
        let mut data = self.data.write().await;
        let mut keys_to_remove = Vec::new();
        let mut valid_keys = Vec::new();

        for (key, stored_value) in data.iter() {
            if stored_value.is_expired() {
                keys_to_remove.push(key.clone());
            } else {
                valid_keys.push(key.clone());
            }
        }

        for key in keys_to_remove {
            data.remove(&key);
        }

        if valid_keys.is_empty() {
            None
        } else {
            Some(valid_keys)
        }
    }

    pub async fn zadd(&self, key: String, score: f64, member: String) -> usize {
        let mut sets = self.sorted_sets.write().await;
        let set = sets.entry(key).or_insert_with(|| SortedSet::new());
        set.zadd(score, member)
    }

    pub async fn zrank(&self, key: String, member: String) -> Option<usize> {
        let sets = self.sorted_sets.read().await;
        if let Some(set) = sets.get(&key) {
            set.zrank(member)
        } else {
            None
        }
    }

    pub async fn zrange(&self, key: String, start: i32, end: i32) -> Option<Vec<String>> {
        let sets = self.sorted_sets.read().await;
        if let Some(set) = sets.get(&key) {
            set.zrange(start, end)
        } else {
            None
        }
    }

    pub async fn zcard(&self, key: String) -> Option<usize> {
        let sets = self.sorted_sets.read().await;
        if let Some(set) = sets.get(&key) {
            Some(set.zcard())
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

impl SortedSet {
    fn new() -> Self {
        Self {
            by_member: HashMap::new(),
            ordered: BTreeSet::new(),
        }
    }

    fn zadd(&mut self, score: f64, member: String) -> usize {
        if !score.is_finite() {
            return 0;
        }
        if let Some(old_score) = self.by_member.get(&member) {
            if *old_score == score {
                return 0;
            }
            let old = ScoredMember {
                score: *old_score,
                member: member.clone(),
            };
            self.ordered.remove(&old);
            self.by_member.insert(member.clone(), score);
            self.ordered.insert(ScoredMember { score, member });
            0
        } else {
            self.by_member.insert(member.clone(), score);
            self.ordered.insert(ScoredMember { score, member });
            1
        }
    }

    fn zrank(&self, member: String) -> Option<usize> {
        if self.by_member.contains_key(&member) {
            for (rank, scored_member) in self.ordered.iter().enumerate() {
                if scored_member.member == member {
                    return Some(rank);
                }
            }
            return None;
        }
        None
    }

    fn zrange(&self, start: i32, end: i32) -> Option<Vec<String>> {
        let members_size = self.by_member.len() as i32;

        let (start, end) = match (start.is_negative(), end.is_negative()) {
            (false, false) => (start, end),
            (false, true) => (start, members_size + end),
            (true, false) => (members_size + start, end),
            (true, true) => (members_size + start, members_size + end),
        };

        if start >= members_size || start > end {
            return None;
        }

        let first = start.max(0);
        let last = end.min(members_size);
        let members: Vec<String> = self
            .ordered
            .iter()
            .enumerate()
            .filter_map(|(idx, scored_member)| {
                if first <= idx as i32 && idx as i32 <= last {
                    Some(scored_member.member.clone())
                } else {
                    None
                }
            })
            .collect();
        Some(members)
    }

    fn zcard(&self) -> usize {
        self.by_member.len()
    }
}

impl Eq for ScoredMember {}

impl PartialEq for ScoredMember {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.member == other.member
    }
}

impl PartialOrd for ScoredMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredMember {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.score.partial_cmp(&other.score) {
            Some(Ordering::Equal) | None => self.member.cmp(&other.member),
            Some(ord) => ord,
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
    let _version_str = std::str::from_utf8(&version)?;

    content.advance(9);

    // 2. Metadata section
    let _metadata = read_metadata(&mut content)?;

    // 3. Database section
    let database = read_database(&mut content)?;

    // 4. End of file section
    let _end_of_file = read_eof(&mut content)?;

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

            // Should read the sizes of tables here instead of advancing
            content.advance(2);

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
                        let expires_at =
                            unix_timestamp_to_instant(timestamp_seconds as u64 * 1000)?;
                        let stored_value = StoredValue {
                            value,
                            expires_at: Some(expires_at),
                        };
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
                        let expires_at = unix_timestamp_to_instant(timestamp_milliseconds)?;
                        let stored_value = StoredValue {
                            value,
                            expires_at: Some(expires_at),
                        };
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
                0xC3 => Err(anyhow!("LZF compressed string is not supported")),
                _ => Err(anyhow!("Unexpected string encoding: {}", size_encoding)),
            }
        }
        _ => Err(anyhow!("Unexpected size encoded value: {}", size_encoding)),
    }
}

fn unix_timestamp_to_instant(timestamp_ms: u64) -> anyhow::Result<Instant> {
    let now_system = SystemTime::now();
    let now_instant = Instant::now();

    // Calculate the offset between SystemTime::now() and Instant::now()
    if let Ok(duration_since_unix) = now_system.duration_since(UNIX_EPOCH) {
        let target_duration = Duration::from_millis(timestamp_ms);

        if target_duration > duration_since_unix {
            // Future time - add the difference to current Instant
            let diff = target_duration - duration_since_unix;
            Ok(now_instant + diff)
        } else {
            // Past - subtract the difference from current Instant
            let diff = duration_since_unix - target_duration;
            Ok(now_instant - diff)
        }
    } else {
        Err(anyhow!("System time is before Unix epoch"))
    }
}
