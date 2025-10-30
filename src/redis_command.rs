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
    Unsubscribe {
        channel: String,
    },
    Publish {
        channel: String,
        message: String,
    },
    Rpush {
        list: String,
        elements: Vec<String>,
    },
    Lrange {
        key: String,
        start: i32,
        end: i32,
    },
    Lpush {
        list: String,
        elements: Vec<String>,
    },
    Llen {
        key: String,
    },
    Lpop {
        key: String,
        count: Option<usize>,
    },
    Blpop {
        key: String,
        timeout: f64,
    },
    #[allow(unused)]
    Geoadd {
        key: String,
        longitude: f64,
        latitude: f64,
        member: String,
    },
    Geopos {
        key: String,
        positions: Vec<String>,
    },
    Geodist {
        key: String,
        from: String,
        to: String,
    },
    Geosearch {
        key: String,
        longitude: f64,
        latitude: f64,
        radius: f64,
    },
    Type {
        key: String,
    },
}

#[derive(Debug, Clone)]
pub enum CommandResult {
    Pong,
    Echo(String),
    Ok,
    Queued,
    SimpleString(String),
    Value(Option<String>),
    Integer(i64),
    Array(Vec<CommandResult>),
    NullArray,
    RedisError(String),
    ConfigValue(String, String),
    Blocked,
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
            RedisCommand::Unsubscribe { .. } => f.write_str("UNSUBSCRIBE"),
            RedisCommand::Publish { .. } => f.write_str("PUBLISH"),
            RedisCommand::Rpush { .. } => f.write_str("RPUSH"),
            RedisCommand::Lrange { .. } => f.write_str("LRANGE"),
            RedisCommand::Lpush { .. } => f.write_str("LPUSH"),
            RedisCommand::Llen { .. } => f.write_str("LLEN"),
            RedisCommand::Lpop { .. } => f.write_str("LPOP"),
            RedisCommand::Blpop { .. } => f.write_str("BLPOP"),
            RedisCommand::Geoadd { .. } => f.write_str("GEOADD"),
            RedisCommand::Geopos { .. } => f.write_str("GEOPOS"),
            RedisCommand::Geodist { .. } => f.write_str("GEODIST"),
            RedisCommand::Geosearch { .. } => f.write_str("GEOSEARCH"),
            RedisCommand::Type { .. } => f.write_str("TYPE"),
        }
    }
}
