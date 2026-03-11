use std::collections::HashMap;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions,
};

const PROGRAM_IDS: [&str; 1] = [
    // Add program IDs to subscribe to and change the above count accordingly
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
];

pub(super) fn create_subscription_request() -> SubscribeRequest {
    let program_ids: Vec<String> = PROGRAM_IDS.iter().map(|id| (*id).to_string()).collect();
    let mode = std::env::var("YELLOWSTONE_SUBSCRIBE_MODE")
        .unwrap_or_else(|_| "transactions".to_string())
        .to_lowercase();
    let use_transactions = matches!(mode.as_str(), "transactions" | "both");
    let use_accounts = matches!(mode.as_str(), "accounts" | "both");

    let mut transactions = HashMap::new();
    let mut accounts = HashMap::new();

    if use_transactions {
        transactions.insert(
            "dlmm".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: None,
                signature: None,
                account_include: program_ids.clone(),
                account_exclude: vec![],
                account_required: vec![],
            },
        );
    }

    if use_accounts {
        accounts.insert(
            "dlmm_accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: program_ids,
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );
    }

    SubscribeRequest {
        accounts,
        slots: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}
