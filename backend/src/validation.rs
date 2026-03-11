use actix_web::{HttpResponse, web};
use serde_json::{Value, json};

use crate::errors::{bad_request, internal_error};
use crate::models::AppState;
use crate::utils::parse_event_values;

pub fn validated_limit(
    state: &web::Data<AppState>,
    field: &str,
    value: Option<usize>,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, HttpResponse> {
    let v = match value {
        Some(v) => v,
        None => default,
    };
    if v < min || v > max {
        return Err(bad_request(
            state,
            "invalid_query",
            format!("`{}` must be between {} and {}", field, min, max),
            Some(json!({ field: v })),
        ));
    }
    Ok(v)
}

pub fn validated_minutes(
    state: &web::Data<AppState>,
    field: &str,
    value: Option<u32>,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32, HttpResponse> {
    let v = match value {
        Some(v) => v,
        None => default,
    };
    if v < min || v > max {
        return Err(bad_request(
            state,
            "invalid_query",
            format!("`{}` must be between {} and {}", field, min, max),
            Some(json!({ field: v })),
        ));
    }
    Ok(v)
}

pub fn validate_slot_range(
    state: &web::Data<AppState>,
    from_slot: Option<u64>,
    to_slot: Option<u64>,
) -> Result<(), HttpResponse> {
    if let (Some(from), Some(to)) = (from_slot, to_slot)
        && from > to
    {
        return Err(bad_request(
            state,
            "invalid_query",
            "`from_slot` must be <= `to_slot`",
            Some(json!({ "from_slot": from, "to_slot": to })),
        ));
    }
    Ok(())
}

pub fn parse_event_filter(
    state: &web::Data<AppState>,
    raw: Option<&str>,
) -> Result<Vec<String>, HttpResponse> {
    let Some(v) = raw else {
        return Ok(Vec::new());
    };
    let items = match parse_event_values(v) {
        Ok(v) => v,
        Err(item) => {
            return Err(bad_request(
                state,
                "invalid_query",
                "invalid `event` filter; only [a-zA-Z0-9_:. -] allowed",
                Some(json!({ "event": item })),
            ));
        }
    };

    Ok(items)
}

pub fn query_rows_or_500(
    state: &web::Data<AppState>,
    sql: &str,
) -> Result<Vec<Value>, HttpResponse> {
    state.clickhouse.query_rows(sql).map_err(|err| {
        state.metrics.inc_clickhouse_error();
        internal_error(
            "clickhouse_query_failed",
            format!("query failed: {}", err),
            None,
        )
    })
}

pub fn first_row_or_empty(state: &web::Data<AppState>, sql: &str) -> Result<Value, HttpResponse> {
    let rows = query_rows_or_500(state, sql)?;
    if let Some(row) = rows.into_iter().next() {
        Ok(row)
    } else {
        Ok(json!({}))
    }
}
