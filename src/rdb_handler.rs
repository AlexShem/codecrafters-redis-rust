use crate::rdb::RdbParser;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct RdbHandler;

impl RdbHandler {
    pub fn get_empty_rdb_file() -> Vec<u8> {
        // HEX empty RBD data file:
        let rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        rdb_hex
            .as_bytes()
            .chunks_exact(2)
            .map(|chunk| {
                let hex_str = std::str::from_utf8(chunk).unwrap();
                u8::from_str_radix(hex_str, 16).unwrap()
            })
            .collect()
    }

    pub fn format_rdb_transfer(rdb_data: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(format!("${}\r\n", rdb_data.len()).as_bytes());
        result.extend_from_slice(rdb_data);
        result
    }

    pub async fn load_rdb_file(path: &str) -> Result<Vec<(String, String, Option<u128>)>, String> {
        let dir = std::path::Path::new(path)
            .parent()
            .and_then(|p| p.to_str())
            .unwrap_or("");
        let filename = std::path::Path::new(path)
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("");

        match RdbParser::parse_file(dir, filename) {
            Ok(rdb_file) => {
                let mut result = Vec::new();
                for (key, value) in rdb_file.keys {
                    let expiry = rdb_file.expiry_times.get(&key).copied();

                    // Check if key has expired
                    if let Some(exp) = expiry {
                        if SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            > exp
                        {
                            continue; // Skip expired keys
                        }
                    }
                    result.push((key, value, expiry));
                }
                Ok(result)
            }
            Err(e) => Err(format!("Failed to parse RDB file: {}", e)),
        }
    }
}
