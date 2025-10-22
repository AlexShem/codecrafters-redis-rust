use std::fmt::{Display, Formatter};

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
    Discard,
    ConfigGet(String),
    Keys(String),
    Zadd {
        key: String,
        score: f64,
        member: String,
    },
    Zrank {
        key: String,
        member: String,
    },
    Zrange {
        key: String,
        start: i32,
        end: i32,
    },
    Zcard {
        key: String,
    },
    Zscore {
        key: String,
        member: String,
    },
    Zrem {
        key: String,
        member: String,
    },
    Subscribe {
        channel: String,
    },
    Publish {
        channel: String,
        message: String,
    },
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
    ConfigValue(String, String),
}

impl Display for RedisCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisCommand::Ping => f.write_str("PING"),
            RedisCommand::Echo(_) => f.write_str("ECHO"),
            RedisCommand::Set { .. } => f.write_str("SET"),
            RedisCommand::SetWithExpiry { .. } => f.write_str("SET"),
            RedisCommand::Get { .. } => f.write_str("GET"),
            RedisCommand::Incr(_) => f.write_str("INCR"),
            RedisCommand::Multi => f.write_str("MULTI"),
            RedisCommand::Exec => f.write_str("EXEC"),
            RedisCommand::Discard => f.write_str("DISCARD"),
            RedisCommand::ConfigGet(_) => f.write_str("CONFIG GET"),
            RedisCommand::Keys(_) => f.write_str("KEYS"),
            RedisCommand::Zadd { .. } => f.write_str("ZADD"),
            RedisCommand::Zrank { .. } => f.write_str("ZRANK"),
            RedisCommand::Zrange { .. } => f.write_str("ZRANGE"),
            RedisCommand::Zcard { .. } => f.write_str("ZCARD"),
            RedisCommand::Zscore { .. } => f.write_str("ZSCORE"),
            RedisCommand::Zrem { .. } => f.write_str("ZREM"),
            RedisCommand::Subscribe { .. } => f.write_str("SUBSCRIBE"),
            RedisCommand::Publish { .. } => f.write_str("PUBLISH"),
        }
    }
}
