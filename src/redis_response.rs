use crate::redis_command::{CommandResult};

pub struct RedisResponse {
    data: Vec<u8>,
}

impl RedisResponse {
    pub fn from_result(result: CommandResult) -> Self {
        let data = match result {
            CommandResult::Pong => b"+PONG\r\n".to_vec(),
            CommandResult::Echo(message) => {
                format!("${}\r\n{}\r\n", message.len(), message).into_bytes()
            }
            CommandResult::Ok => b"+OK\r\n".to_vec(),
            CommandResult::Value(value) => {
                if let Some(val) = value {
                    format!("${}\r\n{}\r\n", val.len(), val).into_bytes()
                } else {
                    b"$-1\r\n".to_vec()
                }
            }
            CommandResult::Integer(number) => {
                format!(":{}\r\n", number.to_string()).into_bytes()
            }
        };
        Self { data }
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.data
    }
}
