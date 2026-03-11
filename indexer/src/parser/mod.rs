use std::collections::HashMap;
use std::fmt;

use serde_json::{Value, json};
use yellowstone_grpc_proto::prelude::{
    CompiledInstruction, InnerInstruction, Message, SubscribeUpdate, TransactionStatusMeta,
    subscribe_update::UpdateOneof,
};

mod decode;
mod idl;
mod instruction_utils;
mod models;

use decode::{Cursor, decode_idl_type, decode_type_def};
use idl::{Discriminator, Idl, IdlError, IdlEvent, IdlInstruction, IdlType, IdlTypeDef};
use instruction_utils::{
    extract_custom_error_code, extract_instruction_error_index, idl_error_to_json,
    map_idl_accounts, map_raw_accounts, summarize_dlmm_instructions, update_type_name,
};
pub(crate) use models::ParsedUpdate;
use models::{AccountParseResult, ParsedAccountMeta, ParsedInstruction, UpdateParseResult};

// include_str! bakes the JSON into the binary at compile time (fast, no runtime file reads)
const METEORA_IDL: &str =
    include_str!("../../../idls/LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo-idl.json");

#[derive(Debug)]
pub(crate) struct Parser {
    program_id: String,
    instruction_map: HashMap<Discriminator, IdlInstruction>,
    instruction_name_map: HashMap<String, IdlInstruction>,
    error_map: HashMap<u32, IdlError>,
    event_map: HashMap<Discriminator, IdlEvent>,
    account_discriminators: HashMap<Discriminator, String>,
    types: HashMap<String, IdlTypeDef>,
}

impl Parser {
    pub(crate) fn new() -> Result<Self, ParseError> {
        let idl = Idl::from_json(METEORA_IDL)?;
        let mut instruction_map = HashMap::new();
        let mut instruction_name_map = HashMap::new();
        for instruction in &idl.instructions {
            instruction_map.insert(instruction.discriminator, instruction.clone());
            instruction_name_map.insert(instruction.name.clone(), instruction.clone());
        }

        let mut event_map = HashMap::new();
        for event in &idl.events {
            event_map.insert(event.discriminator, event.clone());
        }

        let mut error_map = HashMap::new();
        for err in &idl.errors {
            error_map.insert(err.code, err.clone());
        }

        let mut account_discriminators = HashMap::new();
        for account in &idl.accounts {
            account_discriminators.insert(account.discriminator, account.name.clone());
        }

        let mut types = HashMap::new();
        for ty in &idl.types {
            types.insert(ty.name.clone(), ty.def.clone());
        }

        Ok(Self {
            program_id: idl.address,
            instruction_map,
            instruction_name_map,
            error_map,
            event_map,
            account_discriminators,
            types,
        })
    }

    pub(crate) fn program_id(&self) -> &str {
        &self.program_id
    }

    pub(crate) fn parse_update(&self, update: &SubscribeUpdate) -> ParsedUpdate {
        let created_at = update
            .created_at
            .as_ref()
            .map(|ts| format!("{}.{}", ts.seconds, ts.nanos));

        let result = match update.update_oneof.as_ref() {
            Some(UpdateOneof::Account(account)) => self.parse_account_update(account),
            Some(UpdateOneof::Transaction(tx)) => self.parse_transaction_update(tx),
            Some(UpdateOneof::TransactionStatus(tx_status)) => UpdateParseResult {
                payload: json!({
                    "slot": tx_status.slot,
                    "signature": bs58::encode(&tx_status.signature).into_string(),
                    "index": tx_status.index,
                }),
                slot: Some(tx_status.slot),
                signature: Some(bs58::encode(&tx_status.signature).into_string()),
                parsed_ok: true,
                parsed_instructions: 0,
                failed_instructions: 0,
                dlmm_instruction_count: 0,
            },
            Some(UpdateOneof::Slot(slot)) => UpdateParseResult {
                payload: json!({
                    "slot": slot.slot,
                    "parent": slot.parent,
                    "status": slot.status,
                }),
                slot: Some(slot.slot),
                signature: None,
                parsed_ok: true,
                parsed_instructions: 0,
                failed_instructions: 0,
                dlmm_instruction_count: 0,
            },
            Some(UpdateOneof::Block(block)) => UpdateParseResult {
                payload: json!({
                    "slot": block.slot,
                    "blockhash": block.blockhash,
                    "parent_slot": block.parent_slot,
                }),
                slot: Some(block.slot),
                signature: None,
                parsed_ok: true,
                parsed_instructions: 0,
                failed_instructions: 0,
                dlmm_instruction_count: 0,
            },
            Some(UpdateOneof::BlockMeta(meta)) => UpdateParseResult {
                payload: json!({
                    "slot": meta.slot,
                    "blockhash": meta.blockhash,
                    "rewards_len": meta.rewards.as_ref().map(|r| r.rewards.len()).unwrap_or(0),
                }),
                slot: Some(meta.slot),
                signature: None,
                parsed_ok: true,
                parsed_instructions: 0,
                failed_instructions: 0,
                dlmm_instruction_count: 0,
            },
            Some(UpdateOneof::Entry(entry)) => UpdateParseResult {
                payload: json!({
                    "slot": entry.slot,
                    "index": entry.index,
                    "num_hashes": entry.num_hashes,
                }),
                slot: Some(entry.slot),
                signature: None,
                parsed_ok: true,
                parsed_instructions: 0,
                failed_instructions: 0,
                dlmm_instruction_count: 0,
            },
            Some(UpdateOneof::Ping(ping)) => {
                let _ = ping;
                UpdateParseResult {
                    payload: json!({}),
                    slot: None,
                    signature: None,
                    parsed_ok: true,
                    parsed_instructions: 0,
                    failed_instructions: 0,
                    dlmm_instruction_count: 0,
                }
            }
            Some(UpdateOneof::Pong(pong)) => UpdateParseResult {
                payload: json!({ "id": pong.id }),
                slot: None,
                signature: None,
                parsed_ok: true,
                parsed_instructions: 0,
                failed_instructions: 0,
                dlmm_instruction_count: 0,
            },
            None => UpdateParseResult {
                payload: json!({ "error": "missing update_oneof" }),
                slot: None,
                signature: None,
                parsed_ok: false,
                parsed_instructions: 0,
                failed_instructions: 1,
                dlmm_instruction_count: 0,
            },
        };

        ParsedUpdate {
            update_type: update
                .update_oneof
                .as_ref()
                .map(update_type_name)
                .unwrap_or("unknown"),
            created_at,
            slot: result.slot,
            signature: result.signature,
            parsed_ok: result.parsed_ok,
            parsed_instructions: result.parsed_instructions,
            failed_instructions: result.failed_instructions,
            dlmm_instruction_count: result.dlmm_instruction_count,
            payload: result.payload,
        }
    }

    fn parse_account_update(
        &self,
        account_update: &yellowstone_grpc_proto::prelude::SubscribeUpdateAccount,
    ) -> UpdateParseResult {
        let Some(account) = account_update.account.as_ref() else {
            return UpdateParseResult {
                payload: json!({ "error": "missing account data" }),
                slot: Some(account_update.slot),
                signature: None,
                parsed_ok: false,
                parsed_instructions: 0,
                failed_instructions: 1,
                dlmm_instruction_count: 0,
            };
        };

        let pubkey = bs58::encode(&account.pubkey).into_string();
        let owner = bs58::encode(&account.owner).into_string();
        let data = account.data.as_slice();
        let mut parsed = AccountParseResult::default();

        if data.len() >= 8 {
            let discriminator = Discriminator::from_slice(&data[0..8]);
            if let Some(name) = self.account_discriminators.get(&discriminator) {
                match self.decode_account_data(name, &data[8..]) {
                    Ok(value) => {
                        parsed.account_type = Some(name.clone());
                        parsed.parsed = Some(value);
                    }
                    Err(err) => {
                        parsed.account_type = Some(name.clone());
                        parsed.error = Some(err.to_string());
                    }
                }
            } else {
                parsed.error = Some("unknown account discriminator".to_string());
            }
            parsed.discriminator = Some(discriminator.to_vec());
        } else {
            parsed.error = Some("account data too short for discriminator".to_string());
        }

        let parsed_ok = parsed.error.is_none();

        UpdateParseResult {
            payload: json!({
            "slot": account_update.slot,
            "pubkey": pubkey,
            "owner": owner,
            "lamports": account.lamports,
            "executable": account.executable,
            "rent_epoch": account.rent_epoch,
            "write_version": account.write_version,
            "txn_signature": account.txn_signature.as_ref().map(|sig| bs58::encode(sig).into_string()),
            "data_len": data.len(),
            "account_type": parsed.account_type,
            "account_discriminator": parsed.discriminator,
            "parsed": parsed.parsed,
            "parse_error": parsed.error,
            }),
            slot: Some(account_update.slot),
            signature: account
                .txn_signature
                .as_ref()
                .map(|sig| bs58::encode(sig).into_string()),
            parsed_ok,
            parsed_instructions: 0,
            failed_instructions: if parsed_ok { 0 } else { 1 },
            dlmm_instruction_count: 0,
        }
    }

    fn parse_transaction_update(
        &self,
        tx_update: &yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction,
    ) -> UpdateParseResult {
        let Some(tx_info) = tx_update.transaction.as_ref() else {
            return UpdateParseResult {
                payload: json!({ "error": "missing transaction info" }),
                slot: Some(tx_update.slot),
                signature: None,
                parsed_ok: false,
                parsed_instructions: 0,
                failed_instructions: 1,
                dlmm_instruction_count: 0,
            };
        };

        let signature = bs58::encode(&tx_info.signature).into_string();

        let Some(message) = tx_info.transaction.as_ref() else {
            return UpdateParseResult {
                payload: json!({
                    "slot": tx_update.slot,
                    "signature": signature,
                    "error": "missing transaction message",
                }),
                slot: Some(tx_update.slot),
                signature: Some(signature),
                parsed_ok: false,
                parsed_instructions: 0,
                failed_instructions: 1,
                dlmm_instruction_count: 0,
            };
        };

        let Some(tx_message) = message.message.as_ref() else {
            return UpdateParseResult {
                payload: json!({
                    "slot": tx_update.slot,
                    "signature": signature,
                    "error": "missing transaction message body",
                }),
                slot: Some(tx_update.slot),
                signature: Some(signature),
                parsed_ok: false,
                parsed_instructions: 0,
                failed_instructions: 1,
                dlmm_instruction_count: 0,
            };
        };

        let mut account_keys = tx_message
            .account_keys
            .iter()
            .map(|key| bs58::encode(key).into_string())
            .collect::<Vec<_>>();

        if let Some(meta) = tx_info.meta.as_ref() {
            for key in &meta.loaded_writable_addresses {
                account_keys.push(bs58::encode(key).into_string());
            }
            for key in &meta.loaded_readonly_addresses {
                account_keys.push(bs58::encode(key).into_string());
            }
        }

        let mut instructions = Vec::new();
        for (idx, instruction) in tx_message.instructions.iter().enumerate() {
            instructions.push(self.parse_instruction(
                instruction,
                &account_keys,
                tx_message,
                tx_info.meta.as_ref(),
                false,
                idx as u32,
                None,
            ));
        }

        if let Some(meta) = tx_info.meta.as_ref() {
            for inner in &meta.inner_instructions {
                for (inner_idx, instruction) in inner.instructions.iter().enumerate() {
                    instructions.push(self.parse_inner_instruction(
                        instruction,
                        &account_keys,
                        tx_message,
                        meta,
                        inner.index,
                        inner_idx as u32,
                    ));
                }
            }
        }

        let status = tx_info
            .meta
            .as_ref()
            .and_then(|meta| meta.err.as_ref())
            .map(|err| format!("{err:?}"));
        let status_detail = status
            .as_ref()
            .and_then(|status| self.parse_tx_error_detail(status, &instructions));

        let (parsed_instructions, failed_instructions) =
            summarize_dlmm_instructions(&self.program_id, &instructions);
        let dlmm_instruction_count = parsed_instructions + failed_instructions;
        let parsed_ok = failed_instructions == 0;

        UpdateParseResult {
            payload: json!({
                "slot": tx_update.slot,
                "signature": signature,
                "index": tx_info.index,
                "status": status.unwrap_or_else(|| "ok".to_string()),
                "status_detail": status_detail,
                "instructions": instructions,
            }),
            slot: Some(tx_update.slot),
            signature: Some(signature),
            parsed_ok,
            parsed_instructions,
            failed_instructions,
            dlmm_instruction_count,
        }
    }

    fn parse_inner_instruction(
        &self,
        instruction: &InnerInstruction,
        account_keys: &[String],
        message: &Message,
        meta: &TransactionStatusMeta,
        parent_index: u32,
        inner_index: u32,
    ) -> ParsedInstruction {
        self.parse_instruction(
            &CompiledInstruction {
                program_id_index: instruction.program_id_index,
                accounts: instruction.accounts.clone(),
                data: instruction.data.clone(),
            },
            account_keys,
            message,
            Some(meta),
            true,
            parent_index,
            Some(inner_index),
        )
    }

    fn parse_instruction(
        &self,
        instruction: &CompiledInstruction,
        account_keys: &[String],
        _message: &Message,
        _meta: Option<&TransactionStatusMeta>,
        is_inner: bool,
        instruction_index: u32,
        inner_index: Option<u32>,
    ) -> ParsedInstruction {
        let program_id = instruction
            .program_id_index
            .try_into()
            .ok()
            .and_then(|idx: usize| account_keys.get(idx).cloned())
            .unwrap_or_else(|| "unknown".to_string());

        let data = instruction.data.clone();
        let raw_data = data.clone();

        let raw_accounts = map_raw_accounts(&instruction.accounts, account_keys);

        if program_id != self.program_id {
            return ParsedInstruction {
                program_id,
                is_inner,
                instruction_index,
                inner_index,
                parsed: false,
                name: None,
                raw_accounts,
                idl_accounts: None,
                args: None,
                discriminator: None,
                raw_data,
                error: Some("non-dlmm program".to_string()),
                warning: None,
                remaining_bytes: None,
            };
        }

        if data.len() < 8 {
            return ParsedInstruction {
                program_id,
                is_inner,
                instruction_index,
                inner_index,
                parsed: false,
                name: None,
                raw_accounts,
                idl_accounts: None,
                args: None,
                discriminator: None,
                raw_data,
                error: Some("instruction data too short for discriminator".to_string()),
                warning: None,
                remaining_bytes: None,
            };
        }

        let discriminator = Discriminator::from_slice(&data[0..8]);
        let Some(idl_instruction) = self.instruction_map.get(&discriminator) else {
            if let Some(event_parsed) = self.try_parse_event_cpi_envelope(
                &program_id,
                is_inner,
                instruction_index,
                inner_index,
                &raw_accounts,
                &raw_data,
            ) {
                return event_parsed;
            }

            return ParsedInstruction {
                program_id,
                is_inner,
                instruction_index,
                inner_index,
                parsed: false,
                name: None,
                raw_accounts,
                idl_accounts: None,
                args: None,
                discriminator: Some(discriminator.to_vec()),
                raw_data,
                error: Some("unknown instruction discriminator".to_string()),
                warning: None,
                remaining_bytes: None,
            };
        };

        let (accounts, account_error) = map_idl_accounts(
            &idl_instruction.accounts,
            &instruction.accounts,
            account_keys,
        );

        let mut cursor = Cursor::new(&data[8..]);
        let mut args_map = serde_json::Map::new();
        let mut error: Option<String> = None;
        let mut warning: Option<String> = None;

        for arg in &idl_instruction.args {
            match decode_idl_type(&arg.ty, &mut cursor, &self.types) {
                Ok(value) => {
                    args_map.insert(arg.name.clone(), value);
                }
                Err(err) => {
                    error = Some(format!("arg {}: {}", arg.name, err));
                    break;
                }
            }
        }

        if error.is_none() {
            if let Some(compat_optional) = self
                .try_decode_optional_remaining_accounts_info_compat(idl_instruction, &mut cursor)
            {
                args_map.insert(
                    "_compat_optional_remaining_accounts_info".to_string(),
                    compat_optional,
                );
            }
        }

        if error.is_none() {
            if let Some(extensions) =
                self.try_decode_trailing_compat_extensions(idl_instruction, &mut cursor)
            {
                args_map.insert("_trailing_extensions".to_string(), extensions);
            }
        }

        if error.is_none() {
            if let Some(extra) = cursor.remaining() {
                if !extra.is_empty() {
                    warning = Some(format!(
                        "extra bytes remaining after decode: {}",
                        extra.len()
                    ));
                }
            }
        }

        if error.is_none() {
            error = account_error;
        }

        ParsedInstruction {
            program_id,
            is_inner,
            instruction_index,
            inner_index,
            parsed: error.is_none(),
            name: Some(idl_instruction.name.clone()),
            raw_accounts,
            idl_accounts: Some(accounts),
            args: Some(Value::Object(args_map)),
            discriminator: Some(discriminator.to_vec()),
            raw_data,
            error,
            warning,
            remaining_bytes: cursor.remaining().map(|bytes| bytes.to_vec()),
        }
    }

    fn try_decode_trailing_compat_extensions(
        &self,
        idl_instruction: &IdlInstruction,
        cursor: &mut Cursor<'_>,
    ) -> Option<Value> {
        let remaining = cursor.remaining()?;
        if remaining.is_empty() {
            return None;
        }

        let has_remaining_accounts_info_arg = idl_instruction
            .args
            .iter()
            .any(|arg| arg.name == "remaining_accounts_info");
        if !has_remaining_accounts_info_arg {
            return None;
        }

        if !self.types.contains_key("RemainingAccountsInfo") {
            return None;
        }

        let mut decoded = Vec::new();
        while let Some(extra) = cursor.remaining() {
            if extra.is_empty() {
                break;
            }
            match decode_idl_type(
                &IdlType::Defined("RemainingAccountsInfo".to_string()),
                cursor,
                &self.types,
            ) {
                Ok(value) => decoded.push(value),
                Err(_) => return None,
            }
        }

        if decoded.is_empty() {
            None
        } else {
            Some(Value::Array(decoded))
        }
    }

    fn try_decode_optional_remaining_accounts_info_compat(
        &self,
        idl_instruction: &IdlInstruction,
        cursor: &mut Cursor<'_>,
    ) -> Option<Value> {
        let remaining = cursor.remaining()?;
        if remaining.is_empty() {
            return None;
        }

        let has_remaining_accounts_info_arg = idl_instruction
            .args
            .iter()
            .any(|arg| arg.name == "remaining_accounts_info");
        if has_remaining_accounts_info_arg {
            return None;
        }

        // Backward-compatible pattern: `foo` can carry an optional
        // `RemainingAccountsInfo` if companion `foo2` includes it explicitly.
        let companion_name = format!("{}2", idl_instruction.name);
        let Some(companion) = self.instruction_name_map.get(&companion_name) else {
            return None;
        };
        let companion_has_remaining_accounts_info = companion
            .args
            .iter()
            .any(|arg| arg.name == "remaining_accounts_info");
        if !companion_has_remaining_accounts_info {
            return None;
        }

        if !self.types.contains_key("RemainingAccountsInfo") {
            return None;
        }

        let mut probe = cursor.clone();
        let tag = probe.read_u8().ok()?;
        match tag {
            0 => {
                *cursor = probe;
                Some(json!({
                    "tag": 0,
                    "value": Value::Null,
                }))
            }
            1 => {
                let value = decode_idl_type(
                    &IdlType::Defined("RemainingAccountsInfo".to_string()),
                    &mut probe,
                    &self.types,
                )
                .ok()?;
                *cursor = probe;
                Some(json!({
                    "tag": 1,
                    "value": value,
                }))
            }
            _ => None,
        }
    }

    fn try_parse_event_cpi_envelope(
        &self,
        program_id: &str,
        is_inner: bool,
        instruction_index: u32,
        inner_index: Option<u32>,
        raw_accounts: &[ParsedAccountMeta],
        raw_data: &[u8],
    ) -> Option<ParsedInstruction> {
        if raw_data.len() < 16 {
            return None;
        }

        let wrapper_discriminator = Discriminator::from_slice(&raw_data[0..8]);
        let event_discriminator = Discriminator::from_slice(&raw_data[8..16]);
        let event = self.event_map.get(&event_discriminator)?;

        let mut cursor = Cursor::new(&raw_data[16..]);
        let mut error = None;
        let mut warning: Option<String> = None;

        let decoded_event = match self.decode_event_data(&event.name, &mut cursor) {
            Ok(value) => value,
            Err(err) => {
                error = Some(err.to_string());
                Value::Null
            }
        };

        if error.is_none() {
            if let Some(extra) = cursor.remaining() {
                if !extra.is_empty() {
                    warning = Some(format!(
                        "extra bytes remaining after event decode: {}",
                        extra.len()
                    ));
                }
            }
        }

        Some(ParsedInstruction {
            program_id: program_id.to_string(),
            is_inner,
            instruction_index,
            inner_index,
            parsed: error.is_none(),
            name: Some(format!("event_cpi::{}", event.name)),
            raw_accounts: raw_accounts.to_vec(),
            idl_accounts: None,
            args: Some(json!({
                "wrapper_discriminator": wrapper_discriminator.to_vec(),
                "event_discriminator": event_discriminator.to_vec(),
                "event_name": event.name,
                "event": decoded_event,
            })),
            discriminator: Some(wrapper_discriminator.to_vec()),
            raw_data: raw_data.to_vec(),
            error,
            warning,
            remaining_bytes: cursor.remaining().map(|bytes| bytes.to_vec()),
        })
    }

    fn parse_tx_error_detail(
        &self,
        status: &str,
        instructions: &[ParsedInstruction],
    ) -> Option<Value> {
        let custom_code = extract_custom_error_code(status)?;
        let instruction_index = extract_instruction_error_index(status);

        let matched_dlmm_instruction = match instruction_index {
            Some(ix) => instructions.iter().any(|ins| {
                !ins.is_inner && ins.instruction_index == ix && ins.program_id == self.program_id
            }),
            None => instructions
                .iter()
                .any(|ins| ins.program_id == self.program_id),
        };

        let idl_error = if matched_dlmm_instruction {
            self.error_map.get(&custom_code).map(idl_error_to_json)
        } else {
            None
        };

        Some(json!({
            "raw": status,
            "instruction_index": instruction_index,
            "custom_code": custom_code,
            "matched_dlmm_instruction": matched_dlmm_instruction,
            "idl_error": idl_error,
        }))
    }

    fn decode_event_data(
        &self,
        event_name: &str,
        cursor: &mut Cursor<'_>,
    ) -> Result<Value, ParseError> {
        let candidates = [event_name.to_string(), format!("{event_name}Event")];

        for candidate in candidates {
            if let Some(def) = self.types.get(&candidate) {
                return decode_type_def(def, cursor, &self.types);
            }
        }

        Err(ParseError::new(format!(
            "missing event type definition for {event_name}"
        )))
    }

    fn decode_account_data(&self, account_name: &str, data: &[u8]) -> Result<Value, ParseError> {
        let def = self.types.get(account_name).ok_or_else(|| {
            ParseError::new(format!("missing type definition for {account_name}"))
        })?;
        let mut cursor = Cursor::new(data);
        let value = decode_type_def(def, &mut cursor, &self.types)?;
        Ok(value)
    }
}

#[derive(Debug)]
pub(crate) struct ParseError {
    message: String,
}

impl ParseError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseError {}
