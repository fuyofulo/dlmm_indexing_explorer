use actix_web::{HttpResponse, Responder, get, web};
use serde_json::{Value, json};

use crate::models::{AnalyticsDashboardQuery, AppState};
use crate::utils::{
    now_unix_ms, parse_i64_or, parse_string_or, parse_string_or_empty, parse_u64_or_zero,
};
use crate::validation::{
    first_row_or_empty, query_rows_or_500, validated_limit, validated_minutes,
};

const SWAP_EVENTS_SQL: &str =
    "parsed = 1 AND event_name IN ('swap', 'swap2', 'swap_exact_out2', 'event_cpi::Swap')";
const CLAIM_EVENTS_SQL: &str = "parsed = 1 AND event_name IN ('event_cpi::ClaimFee', 'event_cpi::ClaimFee2', 'claim_fee', 'claim_fee2')";

#[get("/v1/analytics/dashboard")]
pub async fn v1_analytics_dashboard(
    query: web::Query<AnalyticsDashboardQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let minutes = match validated_minutes(&state, "minutes", query.minutes, 1440, 1, 10080) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let limit = match validated_limit(&state, "limit", query.limit, 10, 1, 100) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let anchor_unix_ms = query.anchor_unix_ms.unwrap_or_else(now_unix_ms);

    match analytics_dashboard_payload(&state, minutes, limit, anchor_unix_ms).await {
        Ok(payload) => HttpResponse::Ok().json(payload),
        Err(resp) => resp,
    }
}

fn window_cutoff_ms(anchor_unix_ms: u64, minutes: u32) -> u64 {
    anchor_unix_ms.saturating_sub((minutes as u64) * 60_000)
}

pub(crate) async fn analytics_dashboard_payload(
    state: &web::Data<AppState>,
    minutes: u32,
    limit: usize,
    anchor_unix_ms: u64,
) -> Result<Value, HttpResponse> {
    let overview = overview_payload(state, minutes, anchor_unix_ms)?;
    let swaps_series = series_items(state, "swaps", "hour", minutes, anchor_unix_ms)?;
    let volume_series = series_items(state, "volume", "hour", minutes, anchor_unix_ms)?;
    let top_pools = top_pools_items(state, minutes, anchor_unix_ms, limit).await?;
    let top_users = top_users_items(state, minutes, anchor_unix_ms, limit)?;
    let event_mix = event_mix_items(state, minutes, anchor_unix_ms, 8)?;

    Ok(json!({
        "minutes": minutes,
        "limit": limit,
        "anchor_unix_ms": anchor_unix_ms,
        "overview": overview,
        "swaps_series": swaps_series,
        "volume_series": volume_series,
        "top_pools": top_pools,
        "top_users": top_users,
        "event_mix": event_mix
    }))
}

fn overview_payload(
    state: &web::Data<AppState>,
    minutes: u32,
    anchor_unix_ms: u64,
) -> Result<Value, HttpResponse> {
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let cutoff_ms = window_cutoff_ms(anchor_unix_ms, minutes);

    let row = first_row_or_empty(
        state,
        &format!(
            "SELECT
                countIf({SWAP_EVENTS_SQL}) AS total_swaps,
                countDistinctIf(ifNull(pool, ''), {SWAP_EVENTS_SQL} AND ifNull(pool, '') != '') AS active_pools,
                countDistinctIf(ifNull(user, ''), {SWAP_EVENTS_SQL} AND ifNull(user, '') != '') AS active_users,
                countIf({CLAIM_EVENTS_SQL}) AS claim_events
            FROM {events_table}
            WHERE ingested_at_ms >= {cutoff_ms}
              AND ingested_at_ms <= {anchor_unix_ms}"
        ),
    )?;

    Ok(json!({
        "minutes": minutes,
        "window": {
            "from_ingested_at_ms": cutoff_ms,
            "to_ingested_at_ms": anchor_unix_ms
        },
        "metrics": {
            "total_swaps": parse_u64_or_zero(row.get("total_swaps")),
            "active_pools": parse_u64_or_zero(row.get("active_pools")),
            "active_users": parse_u64_or_zero(row.get("active_users")),
            "claim_events": parse_u64_or_zero(row.get("claim_events"))
        }
    }))
}


fn series_items(
    state: &web::Data<AppState>,
    metric: &str,
    granularity: &str,
    minutes: u32,
    anchor_unix_ms: u64,
) -> Result<Vec<Value>, HttpResponse> {
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let cutoff_ms = window_cutoff_ms(anchor_unix_ms, minutes);
    let bucket_ms = match granularity {
        "minute" => 60_000_u64,
        "hour" => 3_600_000_u64,
        "day" => 86_400_000_u64,
        _ => 60_000_u64,
    };
    let bucket_expr = format!("intDiv(ingested_at_ms, {bucket_ms})");
    let value_expr = match metric {
        "swaps" => format!("countIf({SWAP_EVENTS_SQL})"),
        "volume" => format!("sumIf(ifNull(amount_in_raw, 0), {SWAP_EVENTS_SQL})"),
        "users" => "countDistinctIf(ifNull(user, ''), ifNull(user, '') != '')".to_string(),
        "claims" => format!("countIf({CLAIM_EVENTS_SQL})"),
        _ => "0".to_string(),
    };

    let sql = format!(
        "SELECT
            {bucket_expr} AS bucket,
            toString({value_expr}) AS value
        FROM {events_table}
        WHERE ingested_at_ms >= {cutoff_ms}
          AND ingested_at_ms <= {anchor_unix_ms}
        GROUP BY bucket
        ORDER BY bucket ASC",
    );

    let rows = query_rows_or_500(state, &sql)?;
    let values_by_bucket = rows
        .into_iter()
        .map(|row| {
            (
                parse_i64_or(row.get("bucket"), 0),
                parse_string_or(row.get("value"), "0"),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let start_bucket = (cutoff_ms / bucket_ms) as i64;
    let end_bucket = (anchor_unix_ms / bucket_ms) as i64;
    let max_points = 512_i64;
    let mut step = 1_i64;
    let total_points = end_bucket.saturating_sub(start_bucket) + 1;
    if total_points > max_points {
        step = ((total_points as f64) / (max_points as f64)).ceil() as i64;
    }

    let mut items = Vec::new();
    let mut bucket = start_bucket;
    while bucket <= end_bucket {
        let value = values_by_bucket
            .get(&bucket)
            .cloned()
            .unwrap_or_else(|| "0".to_string());
        items.push(json!({
            "bucket": bucket,
            "value": value
        }));
        bucket += step;
    }

    Ok(items)
}

async fn top_pools_items(
    state: &web::Data<AppState>,
    minutes: u32,
    anchor_unix_ms: u64,
    limit: usize,
) -> Result<Vec<Value>, HttpResponse> {
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let cutoff_ms = window_cutoff_ms(anchor_unix_ms, minutes);
    let sql = format!(
        "SELECT
            ifNull(pool, '') AS pool,
            countIf({SWAP_EVENTS_SQL}) AS swap_count,
            toString(sumIf(ifNull(amount_in_raw, 0), {SWAP_EVENTS_SQL})) AS volume_raw
        FROM {events_table}
        WHERE ingested_at_ms >= {cutoff_ms}
          AND ingested_at_ms <= {anchor_unix_ms}
          AND ifNull(pool, '') != ''
        GROUP BY pool
        ORDER BY toUInt128(volume_raw) DESC, swap_count DESC
        LIMIT {limit}"
    );

    let rows = query_rows_or_500(state, &sql)?;
    let mut items = Vec::new();
    for row in rows {
        let pool = parse_string_or_empty(row.get("pool"));
        let rpc_pair = if let Some(rpc) = &state.rpc {
            rpc.fetch_pool_snapshot(&pool).await.ok()
        } else {
            None
        };
        items.push(json!({
            "pool": pool,
            "swap_count": parse_u64_or_zero(row.get("swap_count")),
            "volume_raw": parse_string_or(row.get("volume_raw"), "0"),
            "active_bin_id": rpc_pair.as_ref().map(|v| v.active_bin_id),
            "bin_step": rpc_pair.as_ref().map(|v| v.bin_step),
            "base_fee_pct": rpc_pair.as_ref().map(|v| v.base_fee_pct),
            "variable_fee_pct": rpc_pair.as_ref().map(|v| v.variable_fee_pct),
            "total_fee_pct": rpc_pair.as_ref().map(|v| v.total_fee_pct),
            "protocol_fee_pct": rpc_pair.as_ref().map(|v| v.protocol_fee_pct),
            "current_price_x_per_y": rpc_pair.as_ref().map(|v| v.current_price_x_per_y),
            "token_x_symbol": rpc_pair.as_ref().map(|v| v.token_x_symbol.clone()),
            "token_y_symbol": rpc_pair.as_ref().map(|v| v.token_y_symbol.clone()),
            "token_x_decimals": rpc_pair.as_ref().and_then(|v| v.token_x_decimals),
            "token_y_decimals": rpc_pair.as_ref().and_then(|v| v.token_y_decimals),
        }));
    }
    Ok(items)
}

fn top_users_items(
    state: &web::Data<AppState>,
    minutes: u32,
    anchor_unix_ms: u64,
    limit: usize,
) -> Result<Vec<Value>, HttpResponse> {
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let cutoff_ms = window_cutoff_ms(anchor_unix_ms, minutes);
    let sql = format!(
        "SELECT
            ifNull(user, '') AS user,
            countIf({SWAP_EVENTS_SQL}) AS swap_count,
            toString(sumIf(ifNull(amount_in_raw, 0), {SWAP_EVENTS_SQL})) AS volume_raw,
            countDistinctIf(ifNull(pool, ''), ifNull(pool, '') != '') AS active_pools
        FROM {events_table}
        WHERE ingested_at_ms >= {cutoff_ms}
          AND ingested_at_ms <= {anchor_unix_ms}
          AND ifNull(user, '') != ''
        GROUP BY user
        ORDER BY toUInt128(volume_raw) DESC, swap_count DESC
        LIMIT {limit}"
    );

    let rows = query_rows_or_500(state, &sql)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            json!({
                "user": parse_string_or_empty(row.get("user")),
                "swap_count": parse_u64_or_zero(row.get("swap_count")),
                "volume_raw": parse_string_or(row.get("volume_raw"), "0"),
                "active_pools": parse_u64_or_zero(row.get("active_pools"))
            })
        })
        .collect::<Vec<_>>())
}

fn event_mix_items(
    state: &web::Data<AppState>,
    minutes: u32,
    anchor_unix_ms: u64,
    limit: usize,
) -> Result<Vec<Value>, HttpResponse> {
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let cutoff_ms = window_cutoff_ms(anchor_unix_ms, minutes);
    let sql = format!(
        "SELECT
            event_name,
            count() AS event_count
        FROM {events_table}
        WHERE ingested_at_ms >= {cutoff_ms}
          AND ingested_at_ms <= {anchor_unix_ms}
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT {limit}"
    );

    let rows = query_rows_or_500(state, &sql)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            json!({
                "event_name": parse_string_or(row.get("event_name"), "unknown"),
                "event_count": parse_u64_or_zero(row.get("event_count"))
            })
        })
        .collect::<Vec<_>>())
}
