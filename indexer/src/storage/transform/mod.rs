mod extract;
mod rows;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::client::ClickHouseHttpClient;
use super::models::{BatchError, DbRecord};
use extract::{InstructionContext, parse_created_at_ms, parse_instruction_context};
use rows::{DlmmEventRow, EventParts, FailedPayloadRow, ParserUpdateMetricRow};

#[derive(Default)]
pub(super) struct WriterState {
    pool_mints: HashMap<String, (String, String)>,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn hydrate_pool_mints(writer_state: &mut WriterState, context: &mut InstructionContext) {
    if let (Some(pool_value), Some(token_x), Some(token_y)) = (
        context.pool.as_deref(),
        context.token_x_mint.as_deref(),
        context.token_y_mint.as_deref(),
    ) {
        writer_state.pool_mints.insert(
            pool_value.to_string(),
            (token_x.to_string(), token_y.to_string()),
        );
    }

    if let Some(pool_value) = context.pool.as_deref()
        && let Some((token_x, token_y)) = writer_state.pool_mints.get(pool_value)
    {
        if context.token_x_mint.is_none() {
            context.token_x_mint = Some(token_x.clone());
        }
        if context.token_y_mint.is_none() {
            context.token_y_mint = Some(token_y.clone());
        }
    }
}

fn fill_amount_in_mint(context: &mut InstructionContext) {
    if context.amount_in_mint.is_none() {
        context.amount_in_mint = match (
            context.swap_for_y,
            &context.token_x_mint,
            &context.token_y_mint,
        ) {
            (Some(true), Some(x), Some(_)) => Some(x.clone()),
            (Some(false), Some(_), Some(y)) => Some(y.clone()),
            _ => None,
        };
    }
}

pub(super) fn flush_batch(
    client: &ClickHouseHttpClient,
    batch: &[DbRecord],
    writer_state: &mut WriterState,
) -> Result<(), BatchError> {
    let mut parser_metrics = Vec::<ParserUpdateMetricRow>::with_capacity(batch.len());
    let mut failed_payloads = Vec::<FailedPayloadRow>::new();
    let mut dlmm_events = Vec::<DlmmEventRow>::new();

    for record in batch {
        let record_ingested_ms = now_unix_ms();
        let created_at_ms = parse_created_at_ms(record.created_at.as_deref());

        parser_metrics.push(ParserUpdateMetricRow::from_record(
            record,
            record_ingested_ms,
        ));
        if let Some(row) = FailedPayloadRow::from_record(record, record_ingested_ms) {
            failed_payloads.push(row);
        }

        for instruction in &record.instructions {
            let mut context = parse_instruction_context(instruction);
            hydrate_pool_mints(writer_state, &mut context);
            fill_amount_in_mint(&mut context);

            let event = EventParts::from_record_instruction(record, instruction);
            let event_row = DlmmEventRow::from_parts(
                record_ingested_ms,
                created_at_ms,
                instruction,
                context,
                event,
            );
            dlmm_events.push(event_row);
        }
    }

    client.insert_json_rows("parser_update_metrics", &parser_metrics)?;
    client.insert_json_rows("failed_payloads", &failed_payloads)?;
    client.insert_json_rows("dlmm_events", &dlmm_events)?;

    Ok(())
}
