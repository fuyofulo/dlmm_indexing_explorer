use actix_web::{HttpResponse, Responder, get, web};
use serde_json::{Value, json};

use crate::models::{AppState, QualityBucketItem, QualityWindowQuery, QualityWindowTotals};
use crate::utils::{parse_i64_or, parse_u64_or_zero};
use crate::validation::{query_rows_or_500, validated_minutes};

#[get("/v1/quality/latest")]
pub async fn v1_quality_latest(state: web::Data<AppState>) -> impl Responder {
    state.metrics.inc_request();
    let metrics_table = state.clickhouse.table_ref("parser_update_metrics");
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let sql = format!(
        "SELECT
            minute_bucket,
            total_updates,
            dlmm_updates,
            parsed_instructions,
            failed_instructions,
            unknown_discriminator_count,
            last_slot,
            last_ingested_unix_ms
        FROM (
            SELECT
                b.minute_bucket AS minute_bucket,
                b.total_updates AS total_updates,
                b.dlmm_updates AS dlmm_updates,
                b.parsed_instructions AS parsed_instructions,
                b.failed_instructions AS failed_instructions,
                ifNull(s.unknown_discriminator_count, 0) AS unknown_discriminator_count,
                b.last_slot AS last_slot,
                b.last_ingested_unix_ms AS last_ingested_unix_ms
            FROM (
                SELECT
                    intDiv(ingested_at_ms, 60000) AS minute_bucket,
                    count() AS total_updates,
                    sum(dlmm_instruction_count) AS dlmm_updates,
                    sum(parsed_instructions) AS parsed_instructions,
                    sum(failed_instructions) AS failed_instructions,
                    max(slot) AS last_slot,
                    max(ingested_at_ms) AS last_ingested_unix_ms
                FROM {metrics_table}
                GROUP BY minute_bucket
            ) AS b
            LEFT JOIN (
                SELECT
                    intDiv(ingested_at_ms, 60000) AS minute_bucket,
                    countIf(ifNull(parse_error, '') LIKE '%unknown instruction discriminator%') AS unknown_discriminator_count
                FROM {events_table}
                GROUP BY minute_bucket
            ) AS s USING minute_bucket
        )
        ORDER BY minute_bucket DESC
        LIMIT 1"
    );

    let rows = match query_rows_or_500(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let latest = rows
        .first()
        .map(|row| to_quality_bucket(row, "last_ingested_unix_ms"));

    HttpResponse::Ok().json(json!({
        "item": latest
    }))
}

#[get("/v1/quality/window")]
pub async fn v1_quality_window(
    query: web::Query<QualityWindowQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let minutes = match validated_minutes(&state, "minutes", query.minutes, 60, 1, 1440) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let metrics_table = state.clickhouse.table_ref("parser_update_metrics");
    let events_table = state.clickhouse.table_ref("dlmm_events");
    let sql = format!(
        "SELECT
            b.minute_bucket AS minute_bucket,
            b.total_updates AS total_updates,
            b.dlmm_updates AS dlmm_updates,
            b.parsed_instructions AS parsed_instructions,
            b.failed_instructions AS failed_instructions,
            ifNull(s.unknown_discriminator_count, 0) AS unknown_discriminator_count,
            b.last_slot AS last_slot,
            b.last_ingested_unix_ms AS latest_ingested_unix_ms
        FROM (
            SELECT
                intDiv(ingested_at_ms, 60000) AS minute_bucket,
                count() AS total_updates,
                sum(dlmm_instruction_count) AS dlmm_updates,
                sum(parsed_instructions) AS parsed_instructions,
                sum(failed_instructions) AS failed_instructions,
                max(slot) AS last_slot,
                max(ingested_at_ms) AS last_ingested_unix_ms
            FROM {metrics_table}
            WHERE ingested_at_ms >= toUInt64(unixTimestamp64Milli(now64(3))) - ({minutes} * 60 * 1000)
            GROUP BY minute_bucket
        ) AS b
        LEFT JOIN (
            SELECT
                intDiv(ingested_at_ms, 60000) AS minute_bucket,
                countIf(ifNull(parse_error, '') LIKE '%unknown instruction discriminator%') AS unknown_discriminator_count
            FROM {events_table}
            WHERE ingested_at_ms >= toUInt64(unixTimestamp64Milli(now64(3))) - ({minutes} * 60 * 1000)
            GROUP BY minute_bucket
        ) AS s USING minute_bucket
        ORDER BY minute_bucket DESC
        LIMIT {minutes}"
    );

    let rows = match query_rows_or_500(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let buckets = rows
        .iter()
        .map(|row| to_quality_bucket(row, "latest_ingested_unix_ms"))
        .collect::<Vec<_>>();

    let mut totals = QualityWindowTotals::default();
    for bucket in &buckets {
        totals.total_updates += bucket.total_updates;
        totals.dlmm_updates += bucket.dlmm_updates;
        totals.parsed_instructions += bucket.parsed_instructions;
        totals.failed_instructions += bucket.failed_instructions;
        totals.unknown_discriminator_count += bucket.unknown_discriminator_count;
    }

    HttpResponse::Ok().json(json!({
        "minutes": minutes,
        "bucket_count": buckets.len(),
        "totals": totals,
        "items": buckets
    }))
}

fn to_quality_bucket(row: &Value, last_ingested_field: &str) -> QualityBucketItem {
    QualityBucketItem {
        minute_bucket: parse_i64_or(row.get("minute_bucket"), 0),
        total_updates: parse_u64_or_zero(row.get("total_updates")),
        dlmm_updates: parse_u64_or_zero(row.get("dlmm_updates")),
        parsed_instructions: parse_u64_or_zero(row.get("parsed_instructions")),
        failed_instructions: parse_u64_or_zero(row.get("failed_instructions")),
        unknown_discriminator_count: parse_u64_or_zero(row.get("unknown_discriminator_count")),
        last_slot: parse_u64_or_zero(row.get("last_slot")),
        last_ingested_unix_ms: parse_u64_or_zero(row.get(last_ingested_field)),
    }
}
