mod analytics;
mod export;
mod pools;
mod quality;
mod stream;
mod system;

use actix_web::{HttpResponse, web};

use crate::errors::bad_request;
use crate::models::{AppState, EventCursor};
use crate::utils::{encode_cursor, sql_quote};

pub use analytics::v1_analytics_dashboard;
pub use export::v1_export_events_csv;
pub use pools::{v1_pool_events, v1_pool_explorer, v1_pools_top};
pub use quality::{v1_quality_latest, v1_quality_window};
pub use stream::ws_dashboard;
pub use system::{health, healthz, metrics, v1_ingestion_lag};

fn decode_optional_cursor(
    state: &web::Data<AppState>,
    raw: Option<&str>,
) -> Result<Option<EventCursor>, HttpResponse> {
    match raw {
        Some(value) => crate::utils::decode_cursor(value).map(Some).map_err(|err| {
            bad_request(
                state,
                "invalid_cursor",
                format!("invalid cursor: {}", err),
                None,
            )
        }),
        None => Ok(None),
    }
}

fn append_cursor_predicate(sql: &mut String, cursor: &EventCursor) {
    sql.push_str(&format!(
        " AND (
            slot < {slot}
            OR (slot = {slot} AND signature < {sig})
            OR (slot = {slot} AND signature = {sig} AND instruction_index < {ix})
            OR (slot = {slot} AND signature = {sig} AND instruction_index = {ix} AND inner_index < {inner})
        )",
        slot = cursor.slot,
        sig = sql_quote(&cursor.signature),
        ix = cursor.instruction_index,
        inner = cursor.inner_index
    ));
}

fn append_event_in_predicate(sql: &mut String, events: &[String]) {
    if events.is_empty() {
        return;
    }
    sql.push_str(&format!(
        " AND event_name IN ({})",
        events
            .iter()
            .map(|value| sql_quote(value))
            .collect::<Vec<_>>()
            .join(", ")
    ));
}

fn encode_next_cursor(
    slot: u64,
    signature: String,
    instruction_index: u16,
    inner_index: i16,
) -> String {
    encode_cursor(&EventCursor {
        slot,
        signature,
        instruction_index,
        inner_index,
    })
}
