use anyhow::anyhow;
use bytes::{Buf, Bytes};

#[derive(Debug, Clone)]
pub enum Value {
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    Array(Vec<Value>),
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
        _ => Err(anyhow!("Unsupported data type: {}", first_byte as char)),
    }
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
