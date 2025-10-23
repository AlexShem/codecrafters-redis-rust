use crate::redis_command::CommandResult;

#[derive(Debug)]
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
            CommandResult::Queued => b"+QUEUED\r\n".to_vec(),
            CommandResult::Value(value) => {
                if let Some(val) = value {
                    format!("${}\r\n{}\r\n", val.len(), val).into_bytes()
                } else {
                    b"$-1\r\n".to_vec()
                }
            }
            CommandResult::Integer(number) => format!(":{}\r\n", number.to_string()).into_bytes(),
            CommandResult::Array(elements) => {
                let mut bytes = format!("*{}\r\n", elements.len()).into_bytes();
                for element in elements {
                    let part = RedisResponse::from_result(element).data;
                    bytes.extend(part);
                }
                bytes
            }
            CommandResult::NullArray => b"*-1\r\n".to_vec(),
            CommandResult::RedisError(error) => format!("-ERR {}\r\n", error).into_bytes(),
            CommandResult::ConfigValue(key, value) => {
                let key_bytes = key.as_bytes();
                let value_bytes = value.as_bytes();

                let response = format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_bytes.len(),
                    key,
                    value_bytes.len(),
                    value
                );
                response.into_bytes()
            }
            CommandResult::Blocked => {
                panic!("Blocked result should not be converted to response")
            }
        };
        Self { data }
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.data
    }
}
