use serde::Serialize;
use serde_json::Value;

#[derive(Default)]
pub(super) struct AccountParseResult {
    pub(super) account_type: Option<String>,
    pub(super) discriminator: Option<Vec<u8>>,
    pub(super) parsed: Option<Value>,
    pub(super) error: Option<String>,
}

pub(super) struct UpdateParseResult {
    pub(super) payload: Value,
    pub(super) slot: Option<u64>,
    pub(super) signature: Option<String>,
    pub(super) parsed_ok: bool,
    pub(super) parsed_instructions: u64,
    pub(super) failed_instructions: u64,
    pub(super) dlmm_instruction_count: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ParsedUpdate {
    pub(super) update_type: &'static str,
    pub(super) created_at: Option<String>,
    pub(super) slot: Option<u64>,
    pub(super) signature: Option<String>,
    pub(super) parsed_ok: bool,
    pub(super) parsed_instructions: u64,
    pub(super) failed_instructions: u64,
    pub(super) dlmm_instruction_count: u64,
    pub(super) payload: Value,
}

impl ParsedUpdate {
    pub(crate) fn update_type(&self) -> &str {
        self.update_type
    }

    pub(crate) fn created_at(&self) -> Option<&str> {
        self.created_at.as_deref()
    }

    pub(crate) fn slot(&self) -> Option<u64> {
        self.slot
    }

    pub(crate) fn signature(&self) -> Option<&str> {
        self.signature.as_deref()
    }

    pub(crate) fn parsed_ok(&self) -> bool {
        self.parsed_ok
    }

    pub(crate) fn parsed_instructions(&self) -> u64 {
        self.parsed_instructions
    }

    pub(crate) fn failed_instructions(&self) -> u64 {
        self.failed_instructions
    }

    pub(crate) fn dlmm_instruction_count(&self) -> u64 {
        self.dlmm_instruction_count
    }

    pub(crate) fn payload(&self) -> &Value {
        &self.payload
    }
}

#[derive(Debug, Serialize)]
pub(super) struct ParsedInstruction {
    pub(super) program_id: String,
    pub(super) is_inner: bool,
    pub(super) instruction_index: u32,
    pub(super) inner_index: Option<u32>,
    pub(super) parsed: bool,
    pub(super) name: Option<String>,
    pub(super) raw_accounts: Vec<ParsedAccountMeta>,
    pub(super) idl_accounts: Option<Vec<ParsedAccountMeta>>,
    pub(super) args: Option<Value>,
    pub(super) discriminator: Option<Vec<u8>>,
    pub(super) raw_data: Vec<u8>,
    pub(super) error: Option<String>,
    pub(super) warning: Option<String>,
    pub(super) remaining_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ParsedAccountMeta {
    pub(super) name: String,
    pub(super) pubkey: Option<String>,
    pub(super) index: Option<u8>,
    pub(super) optional: bool,
    pub(super) missing: bool,
}
