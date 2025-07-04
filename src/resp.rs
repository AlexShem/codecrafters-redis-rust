use anyhow::anyhow;
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RespValue>),
}

pub struct RespParser;

impl RespParser {
    pub fn parse(input: &str) -> anyhow::Result<RespValue> {
        let mut lines: Vec<&str> = input.split("\r\n").collect();
        if lines.is_empty() {
            return Err(anyhow!("Empty input"));
        }

        Self::parse_value(&mut lines, 0).map(|(value, _)| value)
    }

    pub fn parse_value(lines: &mut Vec<&str>, index: usize) -> anyhow::Result<(RespValue, usize)> {
        if index >= lines.len() {
            return Err(anyhow!("Unexpected end of input"));
        }

        let line = lines[index];
        if line.is_empty() {
            return Err(anyhow!("Empty line"));
        }

        let (type_char, content) = line.split_at(1);
        match type_char {
            "+" => Ok((RespValue::SimpleString(content.to_string()), index + 1)),
            "-" => Ok((RespValue::Error(content.to_string()), index + 1)),
            ":" => {
                let num = content
                    .parse::<i64>()
                    .map_err(|_| anyhow!("Invalid integer: {}", content))?;
                Ok((RespValue::Integer(num), index + 1))
            }
            "$" => Self::parse_bulk_string(lines, index, content),
            "*" => Self::parse_array(lines, index, content),
            _ => Err(anyhow!("Invalid RESP type: {}", type_char)),
        }
    }

    fn parse_bulk_string(
        lines: &mut Vec<&str>,
        index: usize,
        length_str: &str,
    ) -> anyhow::Result<(RespValue, usize)> {
        let length = length_str
            .parse::<i32>()
            .map_err(|_| anyhow!("Invalid bulk string length: {}", length_str))?;

        if length == -1 {
            return Ok((RespValue::BulkString(None), index + 1));
        }

        if length < 0 {
            return Err(anyhow!("Invalid bulk string length: {}", length_str));
        }

        if index + 1 > lines.len() {
            return Err(anyhow!("Missing bulk string data"));
        }

        let data = lines[index + 1];
        if data.len() != length as usize {
            return Err(anyhow!(
                "Bulk string length mismatch: expected {}, got {}",
                length,
                data.len()
            ));
        }

        Ok((RespValue::BulkString(Some(data.to_string())), index + 2))
    }

    fn parse_array(
        lines: &mut Vec<&str>,
        mut index: usize,
        count_str: &str,
    ) -> anyhow::Result<(RespValue, usize)> {
        let count = count_str
            .parse::<i32>()
            .map_err(|_| anyhow!("Invalid array count: {}", count_str))?;

        if count < 0 {
            return Err(anyhow!("Invalid array count: {}", count));
        }

        let mut elements = Vec::new();
        index += 1;

        for _ in 0..count {
            let (element, next_index) = Self::parse_value(lines, index)?;
            elements.push(element);
            index = next_index;
        }

        Ok((RespValue::Array(elements), index))
    }
}
