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
}
