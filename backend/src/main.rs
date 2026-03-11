mod clickhouse;
mod errors;
mod handlers;
mod live;
mod models;
mod rpc;
mod utils;
mod validation;

use std::sync::Arc;

use actix_web::{App, HttpServer, web};

use clickhouse::ClickHouseClient;
use handlers::{
    health, healthz, metrics, v1_analytics_dashboard, v1_export_events_csv,
    v1_ingestion_lag, v1_pool_events, v1_pool_explorer,
    v1_pools_top, v1_quality_latest, v1_quality_window, ws_dashboard,
};
use live::start_redis_stream_consumer;
use rpc::SolanaRpcClient;
use models::{AppMetrics, AppState};
use utils::now_unix_ms;

fn load_dotenv_candidates() {
    let candidates = [
        ".env",
        "backend/.env",
        "dune_project/backend/.env",
        "indexer/.env",
        "dune_project/indexer/.env",
        "../.env",
        "../backend/.env",
        "../indexer/.env",
        "../risk_radar/backend/.env",
        "../risk_radar/labs/pool_intel/backend/.env",
    ];

    dotenv::dotenv().ok();
    for path in candidates {
        dotenv::from_filename(path).ok();
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    load_dotenv_candidates();

    let host = std::env::var("BACKEND_HOST")
        .expect("BACKEND_HOST is missing; set it in dune_project/backend/.env");
    let port = std::env::var("BACKEND_PORT")
        .expect("BACKEND_PORT is missing; set it in dune_project/backend/.env")
        .parse::<u16>()
        .expect("BACKEND_PORT must be a valid u16");
    let bind_addr = format!("{}:{}", host, port);
    let (dashboard_tx, _) = tokio::sync::broadcast::channel::<u64>(1024);

    let state = AppState {
        clickhouse: Arc::new(ClickHouseClient::from_env()),
        rpc: SolanaRpcClient::from_env().map(Arc::new),
        metrics: Arc::new(AppMetrics::default()),
        started_at_ms: now_unix_ms(),
        dashboard_tx,
    };

    start_redis_stream_consumer(&state);

    println!("starting dune-project-backend on {}", bind_addr);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .service(health)
            .service(healthz)
            .service(metrics)
            .service(v1_pools_top)
            .service(v1_quality_latest)
            .service(v1_quality_window)
            .service(v1_ingestion_lag)
            .service(v1_pool_explorer)
            .service(v1_pool_events)
            .service(v1_export_events_csv)
            .service(v1_analytics_dashboard)
            .service(ws_dashboard)
    })
    .shutdown_timeout(1)
    .bind(bind_addr)?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use crate::models::EventCursor;
    use crate::utils::{csv_escape, decode_cursor, encode_cursor, parse_event_values};

    #[test]
    fn cursor_round_trip() {
        let input = EventCursor {
            slot: 42,
            signature: "sig".to_string(),
            instruction_index: 7,
            inner_index: -1,
        };
        let encoded = encode_cursor(&input);
        let decoded = decode_cursor(&encoded).expect("decode cursor");
        assert_eq!(decoded.slot, input.slot);
        assert_eq!(decoded.signature, input.signature);
        assert_eq!(decoded.instruction_index, input.instruction_index);
        assert_eq!(decoded.inner_index, input.inner_index);
    }

    #[test]
    fn csv_escape_quotes_when_needed() {
        let escaped = csv_escape("hello,world");
        assert_eq!(escaped, "\"hello,world\"");
        let escaped_quotes = csv_escape("x\"y");
        assert_eq!(escaped_quotes, "\"x\"\"y\"");
    }

    #[test]
    fn parse_event_values_accepts_expected_chars() {
        let values = parse_event_values("swap,swap2,event_cpi::Swap,close-position")
            .expect("valid event list");
        assert_eq!(
            values,
            vec![
                "swap".to_string(),
                "swap2".to_string(),
                "event_cpi::Swap".to_string(),
                "close-position".to_string()
            ]
        );
    }

    #[test]
    fn parse_event_values_rejects_invalid_chars() {
        let err = parse_event_values("swap,drop table").expect_err("must reject spaces");
        assert_eq!(err, "drop table".to_string());
    }
}
