use actix_web::{HttpResponse, Responder, get, web};

use crate::models::{AppState, ExportEventsQuery};
use crate::utils::{csv_escape, sql_quote, value_as_string};
use crate::validation::{
    parse_event_filter, query_rows_or_500, validate_slot_range, validated_limit,
};

use super::append_event_in_predicate;

#[get("/v1/export/events.csv")]
pub async fn v1_export_events_csv(
    query: web::Query<ExportEventsQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let limit = match validated_limit(&state, "limit", query.limit, 10_000, 1, 100_000) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = validate_slot_range(&state, query.from_slot, query.to_slot) {
        return resp;
    }
    let events_table = state.clickhouse.table_ref("dlmm_events");

    let mut sql = format!(
        "SELECT
            slot,
            signature,
            instruction_index,
            inner_index,
            block_time_ms,
            event_name,
            pool,
            user,
            amount_in_raw,
            amount_in_mint,
            token_x_mint,
            token_y_mint,
            swap_for_y,
            fee_x_raw,
            fee_y_raw,
            parse_error,
            parse_warning,
            event_id
        FROM {events_table}
        WHERE 1"
    );

    if let Some(pool) = query.pool.as_deref() {
        sql.push_str(&format!(" AND ifNull(pool, '') = {}", sql_quote(pool)));
    }
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

    sql.push_str(&format!(
        " ORDER BY slot DESC, signature DESC, instruction_index DESC, inner_index DESC LIMIT {}",
        limit
    ));

    let rows = match query_rows_or_500(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let headers = [
        "slot",
        "signature",
        "instruction_index",
        "inner_index",
        "block_time_ms",
        "event_name",
        "pool",
        "user",
        "amount_in_raw",
        "amount_in_mint",
        "token_x_mint",
        "token_y_mint",
        "swap_for_y",
        "fee_x_raw",
        "fee_y_raw",
        "parse_error",
        "parse_warning",
        "event_id",
    ];

    let mut csv = String::new();
    csv.push_str(&headers.join(","));
    csv.push('\n');

    for row in rows {
        let values = headers
            .iter()
            .map(|key| csv_escape(&value_as_string(row.get(*key))))
            .collect::<Vec<_>>();
        csv.push_str(&values.join(","));
        csv.push('\n');
    }

    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/csv; charset=utf-8"))
        .insert_header((
            "Content-Disposition",
            "attachment; filename=dlmm_events.csv",
        ))
        .body(csv)
}
