#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
    },
    SetWithExpiry {
        key: String,
        value: String,
        expiry_ms: u64,
    },
    Get {
        key: String,
    },
    Incr(String),
    Multi,
    Exec,
}

#[derive(Debug, Clone)]
pub enum CommandResult {
    Pong,
    Echo(String),
    Ok,
    Queued,
    Value(Option<String>),
    Integer(i64),
    Array(Vec<CommandResult>),
    RedisError(String),
}
