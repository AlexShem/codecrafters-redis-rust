use anyhow::anyhow;
use bytes::{Buf, Bytes};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum Value {
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    Array(Vec<Value>),
    #[allow(unused)]
    Integer(i64),
    #[allow(unused)]
    Double(f64),
}

pub fn parse_value(buf: &mut Bytes) -> anyhow::Result<Value> {
    if buf.is_empty() {
        return Err(anyhow!("Buffer is empty, nothing to parse"));
    }

    let first_byte = buf.get_u8();
    match first_byte {
        b'+' => parse_simple_string(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        b':' => parse_integer(buf),
        b',' => parse_double(buf),
        _ => Err(anyhow!("Unsupported data type: {}", first_byte as char)),
    }
}

fn parse_integer(buf: &mut Bytes) -> anyhow::Result<Value> {
    let line = read_until_crlf(buf)?;
    let sign = match line.first() {
        None => None,
        Some(byte) => match byte {
            b'+' => Some(1_i64),
            b'-' => Some(-1_i64),
            _ => None,
        },
    };
    let number = match sign {
        None => String::from_utf8(line)?.parse::<i64>()?,
        Some(multiple) => String::from_utf8(line[1..].to_vec())?.parse::<i64>()? * multiple,
    };

    Ok(Value::Integer(number))
}

fn parse_double(buf: &mut Bytes) -> anyhow::Result<Value> {
    let line = read_until_crlf(buf)?;
    let number_str = String::from_utf8(line)?;
    let number = f64::from_str(number_str.as_str())?;

    Ok(Value::Double(number))
}

fn parse_array(buf: &mut Bytes) -> anyhow::Result<Value> {
    let count_str = read_until_crlf(buf)?;
    let count = std::str::from_utf8(&count_str)?.parse::<i32>()?;

    if count < 0 {
        return Err(anyhow!("Negative array count not supported"));
    }

    let mut elements = Vec::new();
    for _ in 0..count {
        elements.push(parse_value(buf)?);
    }

    Ok(Value::Array(elements))
}

fn parse_bulk_string(buf: &mut Bytes) -> anyhow::Result<Value> {
    let length_str = read_until_crlf(buf)?;
    let length = std::str::from_utf8(&length_str)?.parse::<i32>()?;

    if length == -1 {
        // Null bulk string
        return Ok(Value::BulkString(vec![]));
    }

    if length < 0 || buf.remaining() < length as usize + 2 {
        return Err(anyhow!("Invalid bulk string length or insufficient data"));
    }

    let mut data = vec![0u8; length as usize];
    buf.copy_to_slice(&mut data);

    // Consume the trailing \r\n
    if buf.remaining() < 2 || buf.get_u16() != 0x0d0a {
        return Err(anyhow!("Expected CRLF after bulk string"));
    }

    Ok(Value::BulkString(data))
}

fn parse_simple_string(buf: &mut Bytes) -> anyhow::Result<Value> {
    let line = read_until_crlf(buf)?;
    Ok(Value::SimpleString(line))
}

fn read_until_crlf(buf: &mut Bytes) -> anyhow::Result<Vec<u8>> {
    let mut result = Vec::new();

    while buf.remaining() >= 2 {
        let byte = buf.get_u8();
        if byte == b'\r' && buf.first() == Some(&b'\n') {
            // Consume '\n'
            buf.advance(1);
            return Ok(result);
        }
        result.push(byte);
    }

    Err(anyhow!("CRLF not found"))
}
