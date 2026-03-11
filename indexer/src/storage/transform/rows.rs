use serde::Serialize;

use super::super::models::{DbInstructionRecord, DbRecord};
use super::extract::InstructionContext;

const CHAIN_NAME: &str = "solana";
const PARSER_VERSION: &str = "v2";

fn bool_to_u8(value: bool) -> u8 {
    if value { 1 } else { 0 }
}

fn u64_to_u32_saturating(value: u64) -> u32 {
    value.min(u64::from(u32::MAX)) as u32
}

#[derive(Serialize)]
pub(super) struct ParserUpdateMetricRow {
    update_id: String,
    chain: &'static str,
    parser_version: &'static str,
    ingested_at_ms: u64,
    update_type: String,
    slot: u64,
    signature: Option<String>,
    created_at: Option<String>,
    parsed_ok: u8,
    parsed_instructions: u32,
    failed_instructions: u32,
    dlmm_instruction_count: u32,
    status: Option<String>,
    has_failed_payload: u8,
    failed_payload_id: Option<String>,
}

impl ParserUpdateMetricRow {
    pub(super) fn from_record(record: &DbRecord, record_ingested_ms: u64) -> Self {
        Self {
            update_id: record.update_id.clone(),
            chain: CHAIN_NAME,
            parser_version: PARSER_VERSION,
            ingested_at_ms: record_ingested_ms,
            update_type: record.update_type.clone(),
            slot: record.slot.unwrap_or(0),
            signature: record.signature.clone(),
            created_at: record.created_at.clone(),
            parsed_ok: bool_to_u8(record.parsed_ok),
            parsed_instructions: u64_to_u32_saturating(record.parsed_instructions),
            failed_instructions: u64_to_u32_saturating(record.failed_instructions),
            dlmm_instruction_count: u64_to_u32_saturating(record.dlmm_instruction_count),
            status: record.status.clone(),
            has_failed_payload: bool_to_u8(record.failed_payload_json.is_some()),
            failed_payload_id: record
                .failed_payload_json
                .as_ref()
                .map(|_| record.update_id.clone()),
        }
    }
}

#[derive(Serialize)]
pub(super) struct FailedPayloadRow {
    failed_payload_id: String,
    update_id: String,
    chain: &'static str,
    parser_version: &'static str,
    ingested_at_ms: u64,
    update_type: String,
    slot: u64,
    signature: Option<String>,
    created_at: Option<String>,
    status: Option<String>,
    status_detail_json: Option<String>,
    payload_json: String,
}

impl FailedPayloadRow {
    pub(super) fn from_record(record: &DbRecord, record_ingested_ms: u64) -> Option<Self> {
        let payload_json = record.failed_payload_json.clone()?;
        Some(Self {
            failed_payload_id: record.update_id.clone(),
            update_id: record.update_id.clone(),
            chain: CHAIN_NAME,
            parser_version: PARSER_VERSION,
            ingested_at_ms: record_ingested_ms,
            update_type: record.update_type.clone(),
            slot: record.slot.unwrap_or(0),
            signature: record.signature.clone(),
            created_at: record.created_at.clone(),
            status: record.status.clone(),
            status_detail_json: record.failed_status_detail_json.clone(),
            payload_json,
        })
    }
}

#[derive(Serialize)]
pub(super) struct DlmmEventRow {
    chain: &'static str,
    parser_version: &'static str,
    ingested_at_ms: u64,
    block_time_ms: Option<u64>,
    slot: u64,
    signature: String,
    instruction_index: u16,
    inner_index: i16,
    is_inner: u8,
    event_name: String,
    program_id: String,
    discriminator: Vec<u8>,
    parsed: u8,
    parse_error: Option<String>,
    parse_warning: Option<String>,
    pool: Option<String>,
    user: Option<String>,
    amount_in_raw: Option<u64>,
    amount_in_mint: Option<String>,
    token_x_mint: Option<String>,
    token_y_mint: Option<String>,
    swap_for_y: Option<u8>,
    event_owner: Option<String>,
    fee_x_raw: Option<u64>,
    fee_y_raw: Option<u64>,
    args_json: Option<String>,
    idl_accounts_json: Option<String>,
    event_id: String,
}

impl DlmmEventRow {
    pub(super) fn from_parts(
        record_ingested_ms: u64,
        created_at_ms: Option<u64>,
        instruction: &DbInstructionRecord,
        context: InstructionContext,
        event: EventParts,
    ) -> Self {
        Self {
            chain: CHAIN_NAME,
            parser_version: PARSER_VERSION,
            ingested_at_ms: record_ingested_ms,
            block_time_ms: created_at_ms,
            slot: event.slot,
            signature: event.signature,
            instruction_index: event.instruction_index,
            inner_index: event.inner_index,
            is_inner: bool_to_u8(instruction.is_inner),
            event_name: event.event_name,
            program_id: instruction.program_id.clone(),
            discriminator: event.discriminator,
            parsed: bool_to_u8(instruction.parsed),
            parse_error: instruction.error.clone(),
            parse_warning: instruction.warning.clone(),
            pool: context.pool,
            user: context.user,
            amount_in_raw: context.amount_in_raw,
            amount_in_mint: context.amount_in_mint,
            token_x_mint: context.token_x_mint,
            token_y_mint: context.token_y_mint,
            swap_for_y: context.swap_for_y.map(bool_to_u8),
            event_owner: context.event_owner,
            fee_x_raw: context.event_fee_x_raw,
            fee_y_raw: context.event_fee_y_raw,
            args_json: instruction.args_json.clone(),
            idl_accounts_json: instruction.idl_accounts_json.clone(),
            event_id: event.event_id,
        }
    }
}

pub(super) struct EventParts {
    pub(super) event_name: String,
    pub(super) signature: String,
    pub(super) slot: u64,
    pub(super) inner_index: i16,
    pub(super) instruction_index: u16,
    pub(super) discriminator: Vec<u8>,
    pub(super) event_id: String,
}

impl EventParts {
    pub(super) fn from_record_instruction(
        record: &DbRecord,
        instruction: &DbInstructionRecord,
    ) -> Self {
        let event_name = instruction
            .name
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let signature = instruction
            .signature
            .clone()
            .or_else(|| record.signature.clone())
            .unwrap_or_default();
        let slot = instruction.slot.or(record.slot).unwrap_or(0);
        let inner_index = instruction
            .inner_index
            .and_then(|v| i16::try_from(v).ok())
            .unwrap_or(-1);
        let instruction_index = instruction.instruction_index.min(u32::from(u16::MAX)) as u16;
        let discriminator = instruction.discriminator.clone().unwrap_or_default();
        let event_id = format!(
            "{}:{}:{}:{}:{}",
            slot, signature, instruction_index, inner_index, event_name
        );

        Self {
            event_name,
            signature,
            slot,
            inner_index,
            instruction_index,
            discriminator,
            event_id,
        }
    }
}
