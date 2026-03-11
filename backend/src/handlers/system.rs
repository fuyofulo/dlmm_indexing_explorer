use actix_web::{HttpResponse, Responder, get, web};
use serde_json::json;

use crate::models::{AppState, HealthResponse};
use crate::utils::{now_unix_ms, parse_u64_or_zero};
use crate::validation::first_row_or_empty;

async fn health_payload(state: web::Data<AppState>) -> HttpResponse {
    state.metrics.inc_request();
    let clickhouse_ok = match state
        .clickhouse
        .query_scalar_u8("SELECT toUInt8(1) AS value")
    {
        Ok(v) => v == 1,
        Err(_) => false,
    };

    HttpResponse::Ok().json(HealthResponse {
        status: "ok",
        service: "dune-project-backend",
        clickhouse_ok,
    })
}

#[get("/health")]
pub async fn health(state: web::Data<AppState>) -> impl Responder {
    health_payload(state).await
}

#[get("/healthz")]
pub async fn healthz(state: web::Data<AppState>) -> impl Responder {
    health_payload(state).await
}

#[get("/metrics")]
pub async fn metrics(state: web::Data<AppState>) -> impl Responder {
    state.metrics.inc_request();
    let uptime_seconds = now_unix_ms()
        .saturating_sub(state.started_at_ms)
        .saturating_div(1000);

    let body = format!(
        concat!(
            "# TYPE dune_backend_requests_total counter\n",
            "dune_backend_requests_total {}\n",
            "# TYPE dune_backend_requests_failed_total counter\n",
            "dune_backend_requests_failed_total {}\n",
            "# TYPE dune_backend_clickhouse_errors_total counter\n",
            "dune_backend_clickhouse_errors_total {}\n",
            "# TYPE dune_backend_bad_requests_total counter\n",
            "dune_backend_bad_requests_total {}\n",
            "# TYPE dune_backend_uptime_seconds gauge\n",
            "dune_backend_uptime_seconds {}\n"
        ),
        state.metrics.requests_total(),
        state.metrics.requests_failed(),
        state.metrics.clickhouse_errors(),
        state.metrics.bad_requests(),
        uptime_seconds
    );

    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain; version=0.0.4"))
        .body(body)
}

#[get("/v1/ingestion/lag")]
pub async fn v1_ingestion_lag(state: web::Data<AppState>) -> impl Responder {
    state.metrics.inc_request();
    let metrics_table = state.clickhouse.table_ref("parser_update_metrics");
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let sql = format!(
        "SELECT
            (SELECT coalesce(max(ingested_at_ms), 0) FROM {metrics_table}) AS metrics_ingested_at_ms,
            (SELECT coalesce(max(ingested_at_ms), 0) FROM {events_table}) AS events_ingested_at_ms,
            (SELECT coalesce(max(slot), 0) FROM {metrics_table}) AS last_slot"
    );

    let row = match first_row_or_empty(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let now_ms = now_unix_ms();
    let metrics_ms = parse_u64_or_zero(row.get("metrics_ingested_at_ms"));
    let events_ms = parse_u64_or_zero(row.get("events_ingested_at_ms"));
    let freshness_ms = if metrics_ms == 0 {
        events_ms
    } else if events_ms == 0 {
        metrics_ms
    } else {
        metrics_ms.min(events_ms)
    };

    HttpResponse::Ok().json(json!({
        "now_unix_ms": now_ms,
        "last_slot": parse_u64_or_zero(row.get("last_slot")),
        "metrics_ingested_at_ms": metrics_ms,
        "events_ingested_at_ms": events_ms,
        "freshness_ingested_at_ms": freshness_ms,
        "metrics_lag_ms": now_ms.saturating_sub(metrics_ms),
        "events_lag_ms": now_ms.saturating_sub(events_ms),
        "freshness_lag_ms": now_ms.saturating_sub(freshness_ms)
    }))
}
