use super::models::{BatchError, DbRecord};

#[derive(Debug, Clone)]
pub(super) struct RedisStreamPublisher {
    client: redis::Client,
    stream_key: String,
    max_len: usize,
}

#[derive(Debug, Clone)]
pub(super) struct FlushSignal {
    pub flushed_at_ms: u64,
    pub update_count: usize,
    pub event_count: usize,
    pub max_slot: u64,
}

impl FlushSignal {
    pub(super) fn from_records(records: &[DbRecord], flushed_at_ms: u64) -> Self {
        let update_count = records.len();
        let event_count = records.iter().map(|record| record.instructions.len()).sum();
        let max_slot = records
            .iter()
            .filter_map(|record| record.slot)
            .max()
            .unwrap_or(0);

        Self {
            flushed_at_ms,
            update_count,
            event_count,
            max_slot,
        }
    }
}

impl RedisStreamPublisher {
    pub(super) fn from_env() -> Result<Self, BatchError> {
        let redis_url = std::env::var("REDIS_URL").unwrap();
        let stream_key = std::env::var("REDIS_STREAM_KEY").unwrap();
        let max_len = std::env::var("REDIS_STREAM_MAXLEN")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let client = redis::Client::open(redis_url.as_str())
            .map_err(|err| BatchError::Db(format!("redis client init failed: {}", err)))?;

        Ok(Self {
            client,
            stream_key,
            max_len,
        })
    }

    pub(super) fn publish_flush(&self, signal: &FlushSignal) -> Result<(), BatchError> {
        let mut connection = self
            .client
            .get_connection()
            .map_err(|err| BatchError::Db(format!("redis connect failed: {}", err)))?;

        let _: String = redis::cmd("XADD")
            .arg(&self.stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(self.max_len)
            .arg("*")
            .arg("source")
            .arg("indexer")
            .arg("flushed_at_ms")
            .arg(signal.flushed_at_ms)
            .arg("update_count")
            .arg(signal.update_count)
            .arg("event_count")
            .arg(signal.event_count)
            .arg("max_slot")
            .arg(signal.max_slot)
            .query(&mut connection)
            .map_err(|err| BatchError::Db(format!("redis xadd failed: {}", err)))?;

        Ok(())
    }
}
