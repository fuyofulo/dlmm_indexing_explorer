use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;

use crate::models::EventCursor;

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[allow(clippy::manual_map)]
pub fn parse_u64(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(n)) => {
            if let Some(v) = n.as_u64() {
                Some(v)
            } else if let Some(v) = n.as_i64() {
                Some(v.max(0) as u64)
            } else {
                None
            }
        }
        Some(Value::String(s)) => s.parse::<u64>().ok(),
        _ => None,
    }
}

#[allow(clippy::manual_unwrap_or_default, clippy::manual_unwrap_or)]
pub fn parse_u64_or_zero(value: Option<&Value>) -> u64 {
    match parse_u64(value) {
        Some(v) => v,
        None => 0,
    }
}

pub fn parse_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}

#[allow(clippy::manual_unwrap_or)]
pub fn parse_i64_or(value: Option<&Value>, default: i64) -> i64 {
    match parse_i64(value) {
        Some(v) => v,
        None => default,
    }
}

pub fn parse_bool(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Bool(v)) => Some(*v),
        Some(Value::Number(n)) => n.as_u64().map(|v| v > 0),
        Some(Value::String(s)) => match s.as_str() {
            "1" | "true" | "TRUE" => Some(true),
            "0" | "false" | "FALSE" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

pub fn parse_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Bool(v)) => Some(v.to_string()),
        _ => None,
    }
}

#[allow(clippy::manual_unwrap_or_default)]
pub fn parse_string_or_empty(value: Option<&Value>) -> String {
    match parse_string(value) {
        Some(v) => v,
        None => String::new(),
    }
}

#[allow(clippy::manual_unwrap_or)]
pub fn parse_string_or(value: Option<&Value>, default: &str) -> String {
    match parse_string(value) {
        Some(v) => v,
        None => default.to_string(),
    }
}

pub fn value_as_string(value: Option<&Value>) -> String {
    parse_string_or_empty(value)
}

pub fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('\n') || value.contains('"') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

pub fn encode_cursor(cursor: &EventCursor) -> String {
    let payload = serde_json::to_vec(cursor).unwrap();
    URL_SAFE_NO_PAD.encode(payload)
}

pub fn decode_cursor(raw: &str) -> Result<EventCursor, Box<dyn Error + Send + Sync>> {
    let bytes = URL_SAFE_NO_PAD.decode(raw)?;
    let cursor = serde_json::from_slice::<EventCursor>(&bytes)?;
    Ok(cursor)
}

pub fn parse_event_values(raw: &str) -> Result<Vec<String>, String> {
    let items = raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    for item in &items {
        let valid = item
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.' | '-'));
        if !valid {
            return Err(item.clone());
        }
    }

    Ok(items)
}
