use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub struct RdbFile {
    pub keys: HashMap<String, String>,
    pub expiry_times: HashMap<String, u128>,
}

pub struct RdbParser;

impl RdbParser {
    pub fn parse_file(dir: &str, filename: &str) -> Result<RdbFile> {
        let path = Path::new(dir).join(filename);

        if !path.exists() {
            return Ok(RdbFile {
                keys: HashMap::new(),
                expiry_times: HashMap::new(),
            });
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Parse header
        Self::parse_header(&mut reader)?;

        // Skip metadata section
        Self::skip_metadata(&mut reader)?;

        // Parse database section
        let result = Self::parse_database(&mut reader)?;

        Ok(result)
    }

    fn parse_size_encoded(reader: &mut BufReader<File>) -> Result<u64> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;

        let first_byte = buf[0];
        let first_two_bits = (first_byte & 0b11_00_00_00) >> 6;

        match first_two_bits {
            0b00 => {
                // The size is the remaining 6 bits of the byte.
                Ok((first_byte & 0b00_11_11_11) as u64)
            }
            0b01 => {
                // The size is the next 14 bits
                // (remaining 6 bits in the first byte, combined with the next byte),
                // in big-endian (read left-to-right).
                reader.read_exact(&mut buf)?;
                let size = ((first_byte & 0b00_11_11_11) as u16) << 8 | (buf[0] as u16);
                Ok(size as u64)
            }
            0b10 => {
                // Ignore the remaining 6 bits of the first byte.
                // The size is the next 4 bytes, in big-endian (read left-to-right).
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                let size = u32::from_be_bytes(buf);
                Ok(size as u64)
            }
            0b11 => Err(anyhow::anyhow!("Special string encoding in size")),
            _ => unreachable!(),
        }
    }

    fn parse_string_encoded(reader: &mut BufReader<File>) -> Result<String> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;

        let first_byte = buf[0];
        let first_two_bits = (first_byte & 0b11_00_00_00) >> 6;

        if first_two_bits == 0b11 {
            let remaining_bits = first_byte & 0b00_11_11_11;
            match remaining_bits {
                0x00 => {
                    // 8-bit integer
                    reader.read_exact(&mut buf)?;
                    Ok(buf[0].to_string())
                }
                0x01 => {
                    // 16-bit integer (little-endian)
                    let mut buf = [0u8; 2];
                    reader.read_exact(&mut buf)?;
                    let value = u16::from_le_bytes(buf);
                    Ok(value.to_string())
                }
                0x02 => {
                    // 32-bit integer (little-endian)
                    let mut buf = [0u8; 4];
                    reader.read_exact(&mut buf)?;
                    let value = u32::from_le_bytes(buf);
                    Ok(value.to_string())
                }
                _ => Err(anyhow!("Unsupported special string encoding")),
            }
        } else {
            // Regular string - the first byte is part of size encoding
            // We need to "put back" the first byte and parse it as size
            let size = match first_two_bits {
                0b00 => (first_byte & 0b00_11_11_11) as u64,
                0b01 => {
                    reader.read_exact(&mut buf)?;
                    ((((first_byte & 0b00111111) as u16) << 8) | (buf[0] as u16)) as u64
                }
                0b10 => {
                    let mut buf = [0u8; 4];
                    reader.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as u64
                }
                _ => unreachable!(),
            };

            let mut string_buf = vec![0u8; size as usize];
            reader.read_exact(&mut string_buf)?;
            Ok(String::from_utf8(string_buf)?)
        }
    }

    fn parse_header(reader: &mut BufReader<File>) -> Result<()> {
        let mut header = [0u8; 9];
        reader.read_exact(&mut header)?;

        let header_str = str::from_utf8(&header)?;
        if !header_str.starts_with("REDIS") {
            return Err(anyhow!("Invalid RDB file: missing REDIS header"));
        }

        Ok(())
    }

    fn skip_metadata(reader: &mut BufReader<File>) -> Result<()> {
        loop {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;

            match buf[0] {
                0xFA => {
                    // Metadata section - skip name and value
                    let _metadata_name = Self::parse_string_encoded(reader)?;
                    let _metadata_value = Self::parse_string_encoded(reader)?;
                }
                0xFE => {
                    // Database section starts - we need to "put back" this byte
                    // Since we can't easily seek back, we'll handle this differently
                    return Ok(());
                }
                0xFF => {
                    // End of file
                    return Ok(());
                }
                _ => {
                    // Unknown byte, assume database section
                    return Ok(());
                }
            }
        }
    }

    fn parse_database(reader: &mut BufReader<File>) -> Result<RdbFile> {
        let mut keys = HashMap::new();
        let mut expiry_times = HashMap::new();

        // The 0xFE marker was already consumed by skip_metadata
        // Read database index directly
        let _db_index = Self::parse_size_encoded(reader)?;

        // Check for hash table size info
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        if buf[0] == 0xFB {
            let _hash_table_size = Self::parse_size_encoded(reader)?;
            let _expires_size = Self::parse_size_encoded(reader)?;
        } else {
            // This byte might be the start of a key-value pair
            // Put it back by processing it as a marker
            let marker = buf[0];

            match marker {
                0x00 => {
                    let key = Self::parse_string_encoded(reader)?;
                    let value = Self::parse_string_encoded(reader)?;
                    keys.insert(key, value);
                }
                _ => {
                    println!("Unknown marker: 0x{:02X}", marker);
                }
            }
        }

        // Read key-value pairs
        loop {
            match reader.read_exact(&mut buf) {
                Ok(()) => {
                    let marker = buf[0];

                    match marker {
                        0xFF => {
                            break;
                        }
                        0xFD => {
                            let mut expiry_buf = [0u8; 4];
                            reader.read_exact(&mut expiry_buf)?;
                            let expiry_s = u32::from_le_bytes(expiry_buf) as u128;
                            let expiry_ms = expiry_s * 1000;

                            reader.read_exact(&mut buf)?;
                            let _value_type = buf[0];

                            let key = Self::parse_string_encoded(reader)?;
                            let value = Self::parse_string_encoded(reader)?;

                            keys.insert(key.clone(), value);
                            expiry_times.insert(key, expiry_ms);
                        }
                        0xFC => {
                            let mut expiry_buf = [0u8; 8];
                            reader.read_exact(&mut expiry_buf)?;
                            let expiry_ms = u64::from_le_bytes(expiry_buf) as u128;

                            reader.read_exact(&mut buf)?;
                            let _value_type = buf[0];

                            let key = Self::parse_string_encoded(reader)?;
                            let value = Self::parse_string_encoded(reader)?;

                            keys.insert(key.clone(), value);
                            expiry_times.insert(key, expiry_ms);
                        }
                        0x00 => {
                            let key = Self::parse_string_encoded(reader)?;
                            let value = Self::parse_string_encoded(reader)?;

                            keys.insert(key, value);
                        }
                        _ => {
                            println!("Unknown marker: 0x{:02X}, continuing", marker);
                        }
                    }
                }
                Err(_) => {
                    println!("End of file reached during key-value parsing");
                    break;
                }
            }
        }

        Ok(RdbFile { keys, expiry_times })
    }
}
