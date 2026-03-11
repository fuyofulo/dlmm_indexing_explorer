use std::fmt;

#[derive(Debug)]
pub(crate) struct DbRecord {
    pub update_id: String,
    pub update_type: String,
    pub slot: Option<u64>,
    pub signature: Option<String>,
    pub created_at: Option<String>,
    pub parsed_ok: bool,
    pub parsed_instructions: u64,
    pub failed_instructions: u64,
    pub dlmm_instruction_count: u64,
    pub status: Option<String>,
    pub failed_status_detail_json: Option<String>,
    pub failed_payload_json: Option<String>,
    pub instructions: Vec<DbInstructionRecord>,
}

#[derive(Debug)]
pub(crate) struct DbInstructionRecord {
    pub slot: Option<u64>,
    pub signature: Option<String>,
    pub instruction_index: u32,
    pub inner_index: Option<u32>,
    pub is_inner: bool,
    pub program_id: String,
    pub name: Option<String>,
    pub discriminator: Option<Vec<u8>>,
    pub parsed: bool,
    pub error: Option<String>,
    pub warning: Option<String>,
    pub args_json: Option<String>,
    pub idl_accounts_json: Option<String>,
}

#[derive(Debug)]
pub(crate) enum BatchError {
    QueueFull,
    QueueDisconnected,
    Db(String),
}

impl fmt::Display for BatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchError::QueueFull => write!(f, "batch queue is full"),
            BatchError::QueueDisconnected => write!(f, "batch queue is disconnected"),
            BatchError::Db(err) => write!(f, "database error: {}", err),
        }
    }
}

impl std::error::Error for BatchError {}
