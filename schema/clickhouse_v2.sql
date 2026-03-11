CREATE DATABASE IF NOT EXISTS dune_project;

-- Parser metrics: compact per-update quality/freshness surface
CREATE TABLE IF NOT EXISTS dune_project.parser_update_metrics (
    update_id String,
    chain LowCardinality(String),
    parser_version String,
    ingested_at_ms UInt64,
    update_type LowCardinality(String),
    slot UInt64,
    signature Nullable(String),
    created_at Nullable(String),
    parsed_ok UInt8,
    parsed_instructions UInt32,
    failed_instructions UInt32,
    dlmm_instruction_count UInt32,
    status Nullable(String),
    has_failed_payload UInt8,
    failed_payload_id Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(intDiv(ingested_at_ms, 1000)))
ORDER BY (slot, update_type, ingested_at_ms)
TTL toDateTime(intDiv(ingested_at_ms, 1000)) + INTERVAL 14 DAY;

-- Failed payload samples: sparse debug storage for unsuccessful updates only
CREATE TABLE IF NOT EXISTS dune_project.failed_payloads (
    failed_payload_id String,
    update_id String,
    chain LowCardinality(String),
    parser_version String,
    ingested_at_ms UInt64,
    update_type LowCardinality(String),
    slot UInt64,
    signature Nullable(String),
    created_at Nullable(String),
    status Nullable(String),
    status_detail_json Nullable(String),
    payload_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(intDiv(ingested_at_ms, 1000)))
ORDER BY (slot, update_type, ingested_at_ms, failed_payload_id)
TTL toDateTime(intDiv(ingested_at_ms, 1000)) + INTERVAL 14 DAY;

-- Canonical DLMM event fact table for API + CSV export
CREATE TABLE IF NOT EXISTS dune_project.dlmm_events (
    chain LowCardinality(String),
    parser_version String,
    ingested_at_ms UInt64,
    block_time_ms Nullable(UInt64),
    slot UInt64,
    signature String,
    instruction_index UInt16,
    inner_index Int16, -- -1 when not inner
    is_inner UInt8,
    event_name LowCardinality(String),
    program_id String,
    discriminator Array(UInt8),
    parsed UInt8,
    parse_error Nullable(String),
    parse_warning Nullable(String),
    pool Nullable(String),
    user Nullable(String),
    amount_in_raw Nullable(UInt64),
    amount_in_mint Nullable(String),
    token_x_mint Nullable(String),
    token_y_mint Nullable(String),
    swap_for_y Nullable(UInt8),
    event_owner Nullable(String),
    fee_x_raw Nullable(UInt64),
    fee_y_raw Nullable(UInt64),
    args_json Nullable(String),
    idl_accounts_json Nullable(String),
    event_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(intDiv(ingested_at_ms, 1000)))
ORDER BY (ifNull(pool, ''), event_name, slot, signature, instruction_index, inner_index, event_id);
