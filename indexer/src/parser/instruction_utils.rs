use serde_json::{Value, json};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;

use super::idl::{IdlAccountItem, IdlError};
use super::models::{ParsedAccountMeta, ParsedInstruction};

pub(super) fn map_raw_accounts(accounts: &[u8], account_keys: &[String]) -> Vec<ParsedAccountMeta> {
    let mut out = Vec::new();
    for (idx, account_index) in accounts.iter().enumerate() {
        let pubkey = account_keys.get(*account_index as usize).cloned();
        let missing = pubkey.is_none();
        out.push(ParsedAccountMeta {
            name: format!("account_{}", idx),
            pubkey,
            index: Some(*account_index),
            optional: false,
            missing,
        });
    }
    out
}

pub(super) fn map_idl_accounts(
    idl_accounts: &[IdlAccountItem],
    account_indices: &[u8],
    account_keys: &[String],
) -> (Vec<ParsedAccountMeta>, Option<String>) {
    let mut out = Vec::new();
    let mut error = None;

    for (idx, idl_account) in idl_accounts.iter().enumerate() {
        let account_index = account_indices.get(idx).copied();
        let pubkey = account_index.and_then(|i| account_keys.get(i as usize).cloned());
        let missing = pubkey.is_none();
        if missing && !idl_account.optional {
            error = Some(format!("missing required account {}", idl_account.name));
        }
        out.push(ParsedAccountMeta {
            name: idl_account.name.clone(),
            pubkey,
            index: account_index,
            optional: idl_account.optional,
            missing,
        });
    }

    if account_indices.len() > idl_accounts.len() {
        for (extra_idx, account_index) in
            account_indices.iter().enumerate().skip(idl_accounts.len())
        {
            let pubkey = account_keys.get(*account_index as usize).cloned();
            let missing = pubkey.is_none();
            out.push(ParsedAccountMeta {
                name: format!("extra_{}", extra_idx - idl_accounts.len()),
                pubkey,
                index: Some(*account_index),
                optional: true,
                missing,
            });
        }
    }

    (out, error)
}

pub(super) fn update_type_name(update: &UpdateOneof) -> &'static str {
    match update {
        UpdateOneof::Account(_) => "account",
        UpdateOneof::Transaction(_) => "transaction",
        UpdateOneof::TransactionStatus(_) => "transaction_status",
        UpdateOneof::Slot(_) => "slot",
        UpdateOneof::Block(_) => "block",
        UpdateOneof::BlockMeta(_) => "block_meta",
        UpdateOneof::Entry(_) => "entry",
        UpdateOneof::Ping(_) => "ping",
        UpdateOneof::Pong(_) => "pong",
    }
}

pub(super) fn summarize_dlmm_instructions(
    program_id: &str,
    instructions: &[ParsedInstruction],
) -> (u64, u64) {
    let mut parsed = 0u64;
    let mut failed = 0u64;
    for instruction in instructions {
        if instruction.program_id != program_id {
            continue;
        }
        if instruction.parsed {
            parsed += 1;
        } else {
            failed += 1;
        }
    }
    (parsed, failed)
}

pub(super) fn idl_error_to_json(err: &IdlError) -> Value {
    json!({
        "code": err.code,
        "name": err.name,
        "msg": err.msg,
    })
}

pub(super) fn extract_custom_error_code(status: &str) -> Option<u32> {
    let marker = "Custom(";
    let start = status.find(marker)? + marker.len();
    let tail = &status[start..];
    let end = tail.find(')')?;
    tail[..end].trim().parse::<u32>().ok()
}

pub(super) fn extract_instruction_error_index(status: &str) -> Option<u32> {
    let marker = "InstructionError(";
    let start = status.find(marker)? + marker.len();
    let tail = &status[start..];
    let comma = tail.find(',')?;
    tail[..comma].trim().parse::<u32>().ok()
}
