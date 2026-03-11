use std::thread;
use std::time::Duration;

use redis::{Value as RedisValue, streams::StreamReadReply};

use crate::models::AppState;

pub fn start_redis_stream_consumer(state: &AppState) {
    let redis_url = std::env::var("REDIS_URL").unwrap();
    let stream_key = std::env::var("REDIS_STREAM_KEY").unwrap();
    let reconnect_ms = std::env::var("REDIS_RECONNECT_MS")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let dashboard_tx = state.dashboard_tx.clone();

    thread::spawn(move || {
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut last_stream_id = "$".to_string();

        loop {
            let mut connection = match client.get_connection() {
                Ok(connection) => connection,
                Err(err) => {
                    eprintln!("Redis stream consumer connect failed: {}", err);
                    thread::sleep(Duration::from_millis(reconnect_ms));
                    continue;
                }
            };

            loop {
                let reply = redis::cmd("XREAD")
                    .arg("BLOCK")
                    .arg(0)
                    .arg("COUNT")
                    .arg(100)
                    .arg("STREAMS")
                    .arg(&stream_key)
                    .arg(&last_stream_id)
                    .query::<StreamReadReply>(&mut connection);

                match reply {
                    Ok(reply) => {
                        let Some((next_stream_id, flushed_at_ms)) = latest_stream_id_and_flush_ms(&reply) else {
                            continue;
                        };
                        last_stream_id = next_stream_id;
                        let _ = dashboard_tx.send(flushed_at_ms);
                    }
                    Err(err) => {
                        eprintln!("Redis stream consumer read failed: {}", err);
                        thread::sleep(Duration::from_millis(reconnect_ms));
                        break;
                    }
                }
            }
        }
    });
}

fn latest_stream_id_and_flush_ms(reply: &StreamReadReply) -> Option<(String, u64)> {
    reply
        .keys
        .iter()
        .flat_map(|key| key.ids.iter())
        .next_back()
        .map(|entry| {
            let flushed_at_ms = entry
                .map
                .get("flushed_at_ms")
                .and_then(parse_redis_u64)
                .unwrap_or(0);
            (entry.id.clone(), flushed_at_ms)
        })
}

fn parse_redis_u64(value: &RedisValue) -> Option<u64> {
    match value {
        RedisValue::Int(v) => (*v).try_into().ok(),
        RedisValue::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse::<u64>().ok(),
        RedisValue::SimpleString(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}
