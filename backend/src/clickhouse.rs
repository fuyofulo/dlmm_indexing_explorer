use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::Value;

#[derive(Clone, Debug)]
pub struct ClickHouseClient {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    timeout: Duration,
}

impl ClickHouseClient {
    pub fn from_env() -> Self {
        let raw_url = std::env::var("CLICKHOUSE_URL").unwrap();
        let (host, port) = parse_clickhouse_url(&raw_url).unwrap();
        let database = std::env::var("CLICKHOUSE_DATABASE")
            .unwrap()
            .trim()
            .to_string();
        let user = std::env::var("CLICKHOUSE_USER").unwrap();
        let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap();
        let timeout_ms = std::env::var("CLICKHOUSE_TIMEOUT_MS")
            .unwrap()
            .parse::<u64>()
            .unwrap();

        Self {
            host,
            port,
            database,
            user,
            password,
            timeout: Duration::from_millis(timeout_ms),
        }
    }

    pub fn table_ref(&self, table: &str) -> String {
        format!("`{}`.`{}`", quote_ident(&self.database), quote_ident(table))
    }

    pub fn query_rows(&self, sql: &str) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        let query = if sql.contains("FORMAT JSONEachRow") {
            sql.to_string()
        } else {
            format!("{} FORMAT JSONEachRow", sql)
        };

        let body = self.send_post(&query, "")?;
        if body.trim().is_empty() {
            return Ok(Vec::new());
        }
        if body.trim_start().starts_with("Code:") {
            return Err(format!("clickhouse returned error body: {}", body.trim()).into());
        }

        let mut rows = Vec::new();
        for line in body.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if !trimmed.starts_with('{') {
                return Err(
                    format!("unexpected non-JSON response from clickhouse: {}", trimmed).into(),
                );
            }
            rows.push(serde_json::from_str::<Value>(trimmed)?);
        }
        Ok(rows)
    }

    pub fn query_scalar_u8(&self, sql: &str) -> Result<u8, Box<dyn Error + Send + Sync>> {
        let rows = self.query_rows(sql)?;
        let value = rows
            .first()
            .ok_or("missing scalar row")?
            .get("value")
            .and_then(|v| v.as_u64())
            .ok_or("missing scalar value")?;
        Ok(value as u8)
    }

    fn send_post(&self, query: &str, body: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))?;
        stream.set_read_timeout(Some(self.timeout))?;
        stream.set_write_timeout(Some(self.timeout))?;

        let mut path = format!(
            "/?database={}&query={}&user={}",
            percent_encode(&self.database),
            percent_encode(query),
            percent_encode(&self.user)
        );
        if !self.password.is_empty() {
            path.push_str("&password=");
            path.push_str(&percent_encode(&self.password));
        }

        let request = format!(
            "POST {} HTTP/1.0\r\nHost: {}:{}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            path,
            self.host,
            self.port,
            body.len(),
            body
        );

        stream.write_all(request.as_bytes())?;
        stream.flush()?;

        let mut response = String::new();
        stream.read_to_string(&mut response)?;

        let status_line = match response.lines().next() {
            Some(line) => line.to_string(),
            None => String::new(),
        };
        let status_ok = status_line.contains(" 200 ") || status_line.contains(" 204 ");
        let body_start = match response.find("\r\n\r\n") {
            Some(idx) => idx + 4,
            None => response.len(),
        };
        let body_text = response[body_start..].to_string();

        if !status_ok {
            return Err(format!(
                "clickhouse query failed: {} | {}",
                status_line,
                body_text.trim()
            )
            .into());
        }

        Ok(body_text)
    }
}

fn parse_clickhouse_url(url: &str) -> Result<(String, u16), String> {
    let trimmed = url.trim();
    let no_scheme = if let Some(rest) = trimmed.strip_prefix("http://") {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        rest
    } else {
        return Err("CLICKHOUSE_URL must start with http:// or https://".to_string());
    };

    let host_port = match no_scheme.split('/').next() {
        Some(value) => value,
        None => return Err("CLICKHOUSE_URL missing host:port".to_string()),
    };
    let (host, port_raw) = match host_port.split_once(':') {
        Some(value) => value,
        None => return Err("CLICKHOUSE_URL must include host:port".to_string()),
    };
    let port = port_raw
        .parse::<u16>()
        .map_err(|_| "CLICKHOUSE_URL has invalid port".to_string())?;
    Ok((host.trim().to_string(), port))
}

fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_digit((b >> 4) & 0x0f));
            out.push(hex_digit(b & 0x0f));
        }
    }
    out
}

fn hex_digit(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        _ => (b'A' + (v - 10)) as char,
    }
}

fn quote_ident(value: &str) -> String {
    value.replace('`', "``")
}
