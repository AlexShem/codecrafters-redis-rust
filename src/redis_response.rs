use crate::redis_command::RedisCommand;

pub struct RedisResponse {
    data: Vec<u8>,
}

impl RedisResponse {
    pub fn command_response(command: RedisCommand) -> Self {
        let data = match command {
            RedisCommand::Ping => b"+PONG\r\n".to_vec(),
            RedisCommand::Echo(message) => {
                format!("${}\r\n{}\r\n", message.len(), message).into_bytes()
            }
        };
        Self { data }
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.data
    }
}
