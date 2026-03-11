use std::env;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::client::ClickHouseHttpClient;
use super::live::{FlushSignal, RedisStreamPublisher};
use super::models::{BatchError, DbRecord};
use super::schema::init_schema;
use super::transform::{WriterState, flush_batch};

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone)]
pub(crate) struct BatchWriter {
    sender: SyncSender<DbRecord>,
    block_on_full: bool,
}

impl BatchWriter {
    pub(crate) fn new(
        db_path: &str,
        batch_size: usize,
        flush_ms: u64,
        queue_size: usize,
    ) -> Result<Self, BatchError> {
        let (sender, receiver) = sync_channel(queue_size);
        let db_name = db_path.trim().to_string();
        let queue_mode = env::var("DB_QUEUE_MODE")
            .unwrap_or_else(|_| "block".to_string())
            .to_ascii_lowercase();
        let block_on_full = queue_mode != "drop";

        thread::spawn(move || {
            if let Err(err) = writer_loop(db_name, receiver, batch_size, flush_ms) {
                eprintln!("Batch writer stopped: {}", err);
            }
        });

        Ok(Self {
            sender,
            block_on_full,
        })
    }

    pub(crate) fn send(&self, record: DbRecord) -> Result<(), BatchError> {
        match self.sender.try_send(record) {
            Ok(()) => Ok(()),
            Err(std::sync::mpsc::TrySendError::Full(record)) => {
                if self.block_on_full {
                    // Apply backpressure instead of silently dropping updates.
                    self.sender
                        .send(record)
                        .map_err(|_| BatchError::QueueDisconnected)
                } else {
                    Err(BatchError::QueueFull)
                }
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                Err(BatchError::QueueDisconnected)
            }
        }
    }
}

fn writer_loop(
    db_name: String,
    receiver: Receiver<DbRecord>,
    batch_size: usize,
    flush_ms: u64,
) -> Result<(), BatchError> {
    let flush_interval = Duration::from_millis(flush_ms);
    let reconnect_interval = Duration::from_secs(
        env::var("CLICKHOUSE_RECONNECT_SECS")
            .unwrap()
            .parse::<u64>()
            .unwrap(),
    );
    let drop_log_interval = Duration::from_secs(
        env::var("CLICKHOUSE_DROP_LOG_SECS")
            .unwrap()
            .parse::<u64>()
            .unwrap(),
    );
    let max_buffer_records = env::var("CLICKHOUSE_MAX_BUFFER_RECORDS")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut last_flush = Instant::now();
    let mut buffer: Vec<DbRecord> = Vec::with_capacity(batch_size);
    let mut writer_state = WriterState::default();
    let mut client: Option<ClickHouseHttpClient> = None;
    let stream_publisher = RedisStreamPublisher::from_env()?;
    let mut next_reconnect_attempt_at = Instant::now();
    let mut last_drop_log = Instant::now();
    let mut last_backpressure_log = Instant::now();
    let mut last_redis_log = Instant::now();

    loop {
        let timeout = flush_interval
            .checked_sub(last_flush.elapsed())
            .unwrap_or(Duration::from_millis(0));

        if buffer.len() < max_buffer_records {
            match receiver.recv_timeout(timeout) {
                Ok(record) => buffer.push(record),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        } else if last_backpressure_log.elapsed() >= drop_log_interval {
            eprintln!(
                "Batch writer backpressure active (buffered={} records, max={}). Waiting for ClickHouse recovery.",
                buffer.len(),
                max_buffer_records
            );
            last_backpressure_log = Instant::now();
        }

        let should_flush = buffer.len() >= batch_size
            || (!buffer.is_empty() && last_flush.elapsed() >= flush_interval);

        if should_flush {
            let mut flushed_records = 0usize;
            let now = Instant::now();

            if client.is_none() && now >= next_reconnect_attempt_at {
                match ClickHouseHttpClient::from_env(db_name.clone()) {
                    Ok(new_client) => match init_schema(&new_client) {
                        Ok(()) => {
                            client = Some(new_client);
                            next_reconnect_attempt_at = Instant::now();
                        }
                        Err(err) => {
                            next_reconnect_attempt_at = Instant::now() + reconnect_interval;
                            if last_drop_log.elapsed() >= drop_log_interval {
                                eprintln!(
                                    "Batch writer ClickHouse init failed; retrying in {}s: {}",
                                    reconnect_interval.as_secs(),
                                    err
                                );
                                last_drop_log = Instant::now();
                            }
                        }
                    },
                    Err(err) => {
                        next_reconnect_attempt_at = Instant::now() + reconnect_interval;
                        if last_drop_log.elapsed() >= drop_log_interval {
                            eprintln!(
                                "Batch writer ClickHouse client init failed; retrying in {}s: {}",
                                reconnect_interval.as_secs(),
                                err
                            );
                            last_drop_log = Instant::now();
                        }
                    }
                }
            }

            if let Some(active_client) = client.as_ref() {
                let flush_size = buffer.len().min(batch_size);
                let flushed_at_ms = now_unix_ms();
                let flush_signal = FlushSignal::from_records(&buffer[..flush_size], flushed_at_ms);
                match flush_batch(active_client, &buffer[..flush_size], &mut writer_state) {
                    Ok(()) => {
                        flushed_records = flush_size;
                        if let Err(err) = stream_publisher.publish_flush(&flush_signal)
                            && last_redis_log.elapsed() >= drop_log_interval
                        {
                            eprintln!("Redis stream publish failed: {}", err);
                            last_redis_log = Instant::now();
                        }
                    }
                    Err(err) => {
                        client = None;
                        next_reconnect_attempt_at = Instant::now() + reconnect_interval;
                        if last_drop_log.elapsed() >= drop_log_interval {
                            eprintln!(
                                "Batch writer flush failed; retrying connection in {}s: {}",
                                reconnect_interval.as_secs(),
                                err
                            );
                            last_drop_log = Instant::now();
                        }
                    }
                }
            }

            if flushed_records > 0 {
                buffer.drain(0..flushed_records);
            } else if last_drop_log.elapsed() >= drop_log_interval {
                eprintln!(
                    "Batch writer buffering {} records while ClickHouse is unavailable.",
                    buffer.len()
                );
                last_drop_log = Instant::now();
            }
            last_flush = Instant::now();
        }
    }

    if !buffer.is_empty() {
        if client.is_none()
            && let Ok(new_client) = ClickHouseHttpClient::from_env(db_name.clone())
            && init_schema(&new_client).is_ok()
        {
            client = Some(new_client);
        }
        if let Some(active_client) = client.as_ref() {
            let flushed_at_ms = now_unix_ms();
            let flush_signal = FlushSignal::from_records(&buffer, flushed_at_ms);
            if let Err(err) = flush_batch(active_client, &buffer, &mut writer_state) {
                eprintln!(
                    "Batch writer final flush failed; dropping {} records: {}",
                    buffer.len(),
                    err
                );
            } else if let Err(err) = stream_publisher.publish_flush(&flush_signal) {
                eprintln!("Redis stream publish failed during final flush: {}", err);
            }
        } else {
            eprintln!(
                "Batch writer final flush skipped; dropping {} records (no ClickHouse connection).",
                buffer.len()
            );
        }
    }

    Ok(())
}
