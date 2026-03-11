use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::clickhouse::ClickHouseClient;
use crate::rpc::SolanaRpcClient;

#[derive(Clone)]
pub struct AppState {
    pub clickhouse: Arc<ClickHouseClient>,
    pub rpc: Option<Arc<SolanaRpcClient>>,
    pub metrics: Arc<AppMetrics>,
    pub started_at_ms: u64,
    pub dashboard_tx: broadcast::Sender<u64>,
}

#[derive(Default)]
pub struct AppMetrics {
    requests_total: AtomicU64,
    requests_failed: AtomicU64,
    clickhouse_errors: AtomicU64,
    bad_requests: AtomicU64,
}

impl AppMetrics {
    pub fn inc_request(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_bad_request(&self) {
        self.bad_requests.fetch_add(1, Ordering::Relaxed);
        self.requests_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_clickhouse_error(&self) {
        self.clickhouse_errors.fetch_add(1, Ordering::Relaxed);
        self.requests_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    pub fn requests_failed(&self) -> u64 {
        self.requests_failed.load(Ordering::Relaxed)
    }

    pub fn clickhouse_errors(&self) -> u64 {
        self.clickhouse_errors.load(Ordering::Relaxed)
    }

    pub fn bad_requests(&self) -> u64 {
        self.bad_requests.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    pub code: String,
    pub message: String,
    pub details: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
    pub clickhouse_ok: bool,
}

#[derive(Debug, Deserialize)]
pub struct TopPoolsQuery {
    pub minutes: Option<u32>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct PoolEventsQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub event: Option<String>,
    pub user: Option<String>,
    pub from_slot: Option<u64>,
    pub to_slot: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ExportEventsQuery {
    pub pool: Option<String>,
    pub event: Option<String>,
    pub user: Option<String>,
    pub from_slot: Option<u64>,
    pub to_slot: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct QualityWindowQuery {
    pub minutes: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct PoolExplorerQuery {
    pub minutes: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct AnalyticsDashboardQuery {
    pub minutes: Option<u32>,
    pub limit: Option<usize>,
    pub anchor_unix_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct DashboardStreamQuery {
    pub minutes: Option<u32>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCursor {
    pub slot: u64,
    pub signature: String,
    pub instruction_index: u16,
    pub inner_index: i16,
}

#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub limit: usize,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TopPoolItem {
    pub pool: String,
    pub swap_count: u64,
    pub volume_raw: String,
}

#[derive(Debug, Serialize)]
pub struct PoolEventItem {
    pub slot: u64,
    pub signature: String,
    pub instruction_index: u16,
    pub inner_index: i16,
    pub block_time_ms: Option<u64>,
    pub event_name: String,
    pub user: Option<String>,
    pub amount_in_raw: Option<String>,
    pub amount_in_mint: Option<String>,
    pub token_x_mint: Option<String>,
    pub token_y_mint: Option<String>,
    pub swap_for_y: Option<bool>,
    pub fee_x_raw: Option<String>,
    pub fee_y_raw: Option<String>,
    pub parse_error: Option<String>,
    pub parse_warning: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QualityBucketItem {
    pub minute_bucket: i64,
    pub total_updates: u64,
    pub dlmm_updates: u64,
    pub parsed_instructions: u64,
    pub failed_instructions: u64,
    pub unknown_discriminator_count: u64,
    pub last_slot: u64,
    pub last_ingested_unix_ms: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct QualityWindowTotals {
    pub total_updates: u64,
    pub dlmm_updates: u64,
    pub parsed_instructions: u64,
    pub failed_instructions: u64,
    pub unknown_discriminator_count: u64,
}
