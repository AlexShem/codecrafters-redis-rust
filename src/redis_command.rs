#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Set { key: String, value: String },
    Get { key: String },
}

#[derive(Debug, Clone)]
pub enum CommandResult {
    Pong,
    Echo(String),
    Ok,
    Value(Option<String>),
}
