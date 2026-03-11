use actix_web::{HttpResponse, Responder, get, web};
use serde_json::json;

use crate::errors::bad_request;
use crate::models::{
    AppState, PaginatedResponse, PoolEventItem, PoolEventsQuery, PoolExplorerQuery, TopPoolItem,
    TopPoolsQuery,
};
use crate::utils::{
    now_unix_ms, parse_bool, parse_i64_or, parse_string, parse_string_or, parse_string_or_empty,
    parse_u64, parse_u64_or_zero, sql_quote,
};
use crate::validation::{
    parse_event_filter, query_rows_or_500, validate_slot_range, validated_limit, validated_minutes,
};

use super::{
    append_cursor_predicate, append_event_in_predicate, decode_optional_cursor, encode_next_cursor,
};

#[get("/v1/pools/top")]
pub async fn v1_pools_top(
    query: web::Query<TopPoolsQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let minutes = match validated_minutes(&state, "minutes", query.minutes, 60, 1, 10080) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let limit = match validated_limit(&state, "limit", query.limit, 20, 1, 200) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let events_table = state.clickhouse.table_ref("dlmm_events");
    let cutoff_ms = now_unix_ms().saturating_sub((minutes as u64) * 60_000);
    let sql = format!(
        "SELECT
            ifNull(pool, '') AS pool,
            countIf(parsed = 1 AND event_name IN ('swap', 'swap2', 'swap_exact_out2', 'event_cpi::Swap')) AS swap_count,
            toString(sumIf(ifNull(amount_in_raw, 0), parsed = 1 AND event_name IN ('swap', 'swap2', 'swap_exact_out2', 'event_cpi::Swap'))) AS volume_raw
        FROM (
            SELECT
                ifNull(pool, '') AS pool,
                parsed,
                event_name,
                amount_in_raw
            FROM {events_table}
            WHERE ingested_at_ms >= {cutoff_ms}
              AND ifNull(pool, '') != ''
        )
        GROUP BY pool
        ORDER BY toUInt128(volume_raw) DESC, swap_count DESC
        LIMIT {limit}"
    );

    let rows = match query_rows_or_500(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let items = rows
        .into_iter()
        .map(|row| TopPoolItem {
            pool: parse_string_or_empty(row.get("pool")),
            swap_count: parse_u64_or_zero(row.get("swap_count")),
            volume_raw: parse_string_or(row.get("volume_raw"), "0"),
        })
        .collect::<Vec<_>>();

    HttpResponse::Ok().json(json!({
        "minutes": minutes,
        "limit": limit,
        "items": items
    }))
}

#[get("/v1/pools/{pool}/explorer")]
pub async fn v1_pool_explorer(
    path: web::Path<String>,
    query: web::Query<PoolExplorerQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let pool = path.into_inner();
    if pool.trim().is_empty() {
        return bad_request(&state, "invalid_pool", "`pool` cannot be empty", None);
    }
    let minutes = match validated_minutes(&state, "minutes", query.minutes, 60, 1, 10080) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let market = if let Some(rpc) = &state.rpc {
        match rpc.fetch_pool_snapshot(&pool).await {
            Ok(snapshot) => json!({
                "name": format!("{}-{}", snapshot.token_x_symbol, snapshot.token_y_symbol),
                "pair_label": format!("{}-{}", snapshot.token_x_symbol, snapshot.token_y_symbol),
                "mint_x": snapshot.token_x_mint,
                "mint_y": snapshot.token_y_mint,
                "token_x_symbol": snapshot.token_x_symbol,
                "token_y_symbol": snapshot.token_y_symbol,
                "token_x_decimals": snapshot.token_x_decimals,
                "token_y_decimals": snapshot.token_y_decimals,
                "current_price": snapshot.current_price_x_per_y,
                "inverse_price": snapshot.current_price_y_per_x,
                "dynamic_fee_pct": snapshot.variable_fee_pct,
                "tvl": null,
                "reserve_x": snapshot.reserve_x_ui.map(|v| v.to_string()).or(snapshot.reserve_x_raw),
                "reserve_y": snapshot.reserve_y_ui.map(|v| v.to_string()).or(snapshot.reserve_y_raw),
                "reserve_x_account": snapshot.reserve_x,
                "reserve_y_account": snapshot.reserve_y,
                "active_bin_id": snapshot.active_bin_id,
                "populated_bin_count": snapshot.populated_bin_count,
                "protocol_fee_x_raw": snapshot.protocol_fee_x_raw,
                "protocol_fee_y_raw": snapshot.protocol_fee_y_raw,
                "bins": snapshot.bins.iter().map(|bin| json!({
                    "bin_id": bin.bin_id,
                    "distance_from_active": bin.distance_from_active,
                    "price_x_per_y": bin.price_x_per_y,
                    "price_y_per_x": bin.price_y_per_x,
                    "onchain_price_raw": bin.onchain_price_raw,
                    "onchain_price_x_per_y": bin.onchain_price_x_per_y,
                    "onchain_price_y_per_x": bin.onchain_price_y_per_x,
                    "amount_x_raw": bin.amount_x_raw,
                    "amount_y_raw": bin.amount_y_raw,
                    "amount_x_ui": bin.amount_x_ui,
                    "amount_y_ui": bin.amount_y_ui
                })).collect::<Vec<_>>(),
                "pool_config": {
                    "bin_step": snapshot.bin_step,
                    "base_fee_pct": snapshot.base_fee_pct,
                    "max_fee_pct": snapshot.total_fee_pct,
                    "protocol_fee_pct": snapshot.protocol_fee_pct,
                    "base_factor": snapshot.base_factor,
                    "base_fee_power_factor": snapshot.base_fee_power_factor,
                    "variable_fee_control": snapshot.variable_fee_control,
                    "volatility_accumulator": snapshot.volatility_accumulator,
                    "protocol_share_bps": snapshot.protocol_share_bps
                }
            }),
            Err(error) => json!({
                "error": error.to_string()
            }),
        }
    } else {
        json!({
            "error": "backend rpc client is not configured"
        })
    };

    HttpResponse::Ok().json(json!({
        "pool": pool,
        "minutes": minutes,
        "market": market,
    }))
}

#[get("/v1/pools/{pool}/events")]
pub async fn v1_pool_events(
    path: web::Path<String>,
    query: web::Query<PoolEventsQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let pool = path.into_inner();
    if pool.trim().is_empty() {
        return bad_request(&state, "invalid_pool", "`pool` cannot be empty", None);
    }
    let limit = match validated_limit(&state, "limit", query.limit, 100, 1, 500) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = validate_slot_range(&state, query.from_slot, query.to_slot) {
        return resp;
    }
    let decoded_cursor = match decode_optional_cursor(&state, query.cursor.as_deref()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let events_table = state.clickhouse.table_ref("dlmm_events");
    let mut sql = format!(
        "SELECT
            slot,
            signature,
            instruction_index,
            inner_index,
            block_time_ms,
            event_name,
            user,
            ifNull(toString(amount_in_raw), '') AS amount_in_raw,
            amount_in_mint,
            token_x_mint,
            token_y_mint,
            swap_for_y,
            ifNull(toString(fee_x_raw), '') AS fee_x_raw,
            ifNull(toString(fee_y_raw), '') AS fee_y_raw,
            parse_error,
            parse_warning
        FROM {events_table}
        WHERE ifNull(pool, '') = {}",
        sql_quote(&pool)
    );

    if let Some(user) = query.user.as_deref() {
        sql.push_str(&format!(" AND ifNull(user, '') = {}", sql_quote(user)));
    }
    if let Some(from_slot) = query.from_slot {
        sql.push_str(&format!(" AND slot >= {}", from_slot));
    }
    if let Some(to_slot) = query.to_slot {
        sql.push_str(&format!(" AND slot <= {}", to_slot));
    }
    let events = match parse_event_filter(&state, query.event.as_deref()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    append_event_in_predicate(&mut sql, &events);
    if let Some(cursor) = decoded_cursor.as_ref() {
        append_cursor_predicate(&mut sql, cursor);
    }

    sql.push_str(&format!(
        " ORDER BY slot DESC, signature DESC, instruction_index DESC, inner_index DESC LIMIT {}",
        limit + 1
    ));

    let rows = match query_rows_or_500(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let mut items = rows
        .into_iter()
        .map(|row| PoolEventItem {
            slot: parse_u64_or_zero(row.get("slot")),
            signature: parse_string_or_empty(row.get("signature")),
            instruction_index: parse_u64_or_zero(row.get("instruction_index")) as u16,
            inner_index: parse_i64_or(row.get("inner_index"), -1) as i16,
            block_time_ms: parse_u64(row.get("block_time_ms")),
            event_name: parse_string_or(row.get("event_name"), "unknown"),
            user: parse_string(row.get("user")),
            amount_in_raw: parse_string(row.get("amount_in_raw")).filter(|v| !v.is_empty()),
            amount_in_mint: parse_string(row.get("amount_in_mint")),
            token_x_mint: parse_string(row.get("token_x_mint")),
            token_y_mint: parse_string(row.get("token_y_mint")),
            swap_for_y: parse_bool(row.get("swap_for_y")),
            fee_x_raw: parse_string(row.get("fee_x_raw")).filter(|v| !v.is_empty()),
            fee_y_raw: parse_string(row.get("fee_y_raw")).filter(|v| !v.is_empty()),
            parse_error: parse_string(row.get("parse_error")),
            parse_warning: parse_string(row.get("parse_warning")),
        })
        .collect::<Vec<_>>();

    let next_cursor = if items.len() > limit {
        let tail = items.pop();
        tail.map(|last| {
            encode_next_cursor(
                last.slot,
                last.signature,
                last.instruction_index,
                last.inner_index,
            )
        })
    } else {
        None
    };

    HttpResponse::Ok().json(PaginatedResponse {
        items,
        limit,
        next_cursor,
    })
}
