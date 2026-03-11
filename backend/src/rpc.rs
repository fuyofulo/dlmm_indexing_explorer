use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;

const DLMM_PROGRAM_ID: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const BIN_ARRAY_DISCRIMINATOR: [u8; 8] = [92, 142, 92, 220, 5, 148, 70, 181];
const LB_PAIR_DISCRIMINATOR: [u8; 8] = [33, 11, 49, 98, 181, 101, 177, 13];
const MAX_BIN_PER_ARRAY: i64 = 70;
const BIN_ARRAY_SIZE: usize = 10_136;
const BIN_SIZE: usize = 144;
const FEE_PRECISION: u128 = 1_000_000_000;
const VARIABLE_FEE_SCALE: u128 = 100_000_000_000;
const VARIABLE_FEE_OFFSET: u128 = 99_999_999_999;
const MAX_FEE_RATE: u128 = 100_000_000;
const Q64_ONE: f64 = 18_446_744_073_709_551_616.0;

#[derive(Clone, Debug)]
pub struct SolanaRpcClient {
    http: Client,
    rpc_url: String,
    cache: Arc<RwLock<HashMap<String, (PoolRpcSnapshot, u64)>>>,
}

#[derive(Clone, Debug)]
pub struct PoolRpcSnapshot {
    pub active_bin_id: i32,
    pub bin_step: u16,
    pub token_x_mint: String,
    pub token_y_mint: String,
    pub token_x_decimals: Option<u8>,
    pub token_y_decimals: Option<u8>,
    pub reserve_x: String,
    pub reserve_y: String,
    pub reserve_x_raw: Option<String>,
    pub reserve_y_raw: Option<String>,
    pub reserve_x_ui: Option<f64>,
    pub reserve_y_ui: Option<f64>,
    pub token_x_symbol: String,
    pub token_y_symbol: String,
    pub current_price_x_per_y: f64,
    pub base_factor: u16,
    pub base_fee_power_factor: u8,
    pub variable_fee_control: u32,
    pub volatility_accumulator: u32,
    pub protocol_share_bps: u16,
    pub protocol_fee_x_raw: String,
    pub protocol_fee_y_raw: String,
    pub base_fee_pct: f64,
    pub variable_fee_pct: f64,
    pub total_fee_pct: f64,
    pub protocol_fee_pct: f64,
    pub current_price_y_per_x: f64,
    pub populated_bin_count: usize,
    pub bins: Vec<PoolBinSnapshot>,
}

#[derive(Clone, Debug)]
pub struct PoolBinSnapshot {
    pub bin_id: i64,
    pub distance_from_active: i64,
    pub price_x_per_y: f64,
    pub price_y_per_x: f64,
    pub onchain_price_raw: String,
    pub onchain_price_x_per_y: f64,
    pub onchain_price_y_per_x: f64,
    pub amount_x_raw: String,
    pub amount_y_raw: String,
    pub amount_x_ui: Option<f64>,
    pub amount_y_ui: Option<f64>,
}

#[derive(Debug)]
struct LbPairSummary {
    token_x_mint: String,
    token_y_mint: String,
    reserve_x: String,
    reserve_y: String,
    protocol_fee_x: u64,
    protocol_fee_y: u64,
    active_bin_id: i32,
    bin_step: u16,
    base_factor: u16,
    base_fee_power_factor: u8,
    variable_fee_control: u32,
    volatility_accumulator: u32,
    protocol_share_bps: u16,
}

#[derive(Debug, Clone)]
struct RawBin {
    amount_x: u64,
    amount_y: u64,
    price: u128,
}

#[derive(Debug, Deserialize)]
struct RpcEnvelope<T> {
    result: T,
}

#[derive(Debug, Deserialize)]
struct RpcAccountInfoResult {
    value: Option<RpcAccountInfo>,
}

#[derive(Debug, Deserialize)]
struct RpcAccountInfo {
    data: (String, String),
}

#[derive(Debug, Deserialize)]
struct RpcProgramAccountResult {
    account: RpcAccountInfo,
}

impl SolanaRpcClient {
    pub fn from_env() -> Option<Self> {
        let rpc_url = std::env::var("BACKEND_RPC_URL")
            .or_else(|_| std::env::var("SOLANA_RPC_URL"))
            .or_else(|_| std::env::var("HELIUS_RPC_URL"))
            .or_else(|_| std::env::var("BACKFILL_RPC_URL"))
            .or_else(|_| std::env::var("POOL_INTEL_RPC_URL"))
            .ok()?;
        let http = Client::builder().build().unwrap();
        Some(Self {
            http,
            rpc_url,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn fetch_pool_snapshot(
        &self,
        pool: &str,
    ) -> Result<PoolRpcSnapshot, Box<dyn std::error::Error + Send + Sync>> {
        const CACHE_TTL_MS: u64 = 30_000;
        let now_ms = now_unix_ms();
        if let Some((snapshot, fetched_at_ms)) = self.cache.read().await.get(pool).cloned()
            && now_ms.saturating_sub(fetched_at_ms) <= CACHE_TTL_MS
        {
            return Ok(snapshot);
        }

        let lb_pair_raw = rpc_get_account_data(&self.http, &self.rpc_url, pool).await?;
        let lb_pair = parse_lb_pair(&lb_pair_raw)?;

        let token_decimals = rpc_get_mint_decimals(
            &self.http,
            &self.rpc_url,
            &[lb_pair.token_x_mint.clone(), lb_pair.token_y_mint.clone()],
        )
        .await
        .unwrap_or_default();

        let reserve_balances = rpc_get_token_account_balances(
            &self.http,
            &self.rpc_url,
            &[lb_pair.reserve_x.clone(), lb_pair.reserve_y.clone()],
        )
        .await
        .unwrap_or_default();

        let raw_bins =
            rpc_get_bin_arrays(&self.http, &self.rpc_url, DLMM_PROGRAM_ID, pool)
                .await
                .unwrap_or_default();

        let decimals_x = token_decimals.get(&lb_pair.token_x_mint).copied().unwrap_or(0);
        let decimals_y = token_decimals.get(&lb_pair.token_y_mint).copied().unwrap_or(0);
        let price_ui_scale = 10f64.powi(decimals_x as i32 - decimals_y as i32);
        let current_price_x_per_y =
            estimate_bin_price(lb_pair.bin_step, lb_pair.active_bin_id as i64) * price_ui_scale;
        let current_price_y_per_x = if current_price_x_per_y > 0.0 {
            1.0 / current_price_x_per_y
        } else {
            0.0
        };

        let left = 24_i64;
        let right = 24_i64;
        let lower = lb_pair.active_bin_id as i64 - left;
        let upper = lb_pair.active_bin_id as i64 + right;
        let mut raw_bins_by_id = HashMap::<i64, RawBin>::new();
        for (bin_id, bin) in raw_bins {
            raw_bins_by_id.insert(bin_id, bin);
        }

        let mut populated_bin_count = 0_usize;
        let mut bins = Vec::new();
        for bin_id in lower..=upper {
            let onchain = raw_bins_by_id.get(&bin_id);
            let amount_x = onchain.map(|b| b.amount_x).unwrap_or(0);
            let amount_y = onchain.map(|b| b.amount_y).unwrap_or(0);
            let onchain_price_raw = onchain.map(|b| b.price).unwrap_or(0);
            let onchain_price_x_per_y = raw_price_to_f64(onchain_price_raw) * price_ui_scale;
            let onchain_price_y_per_x = if onchain_price_x_per_y > 0.0 {
                1.0 / onchain_price_x_per_y
            } else {
                0.0
            };
            let price_x_per_y = estimate_bin_price(lb_pair.bin_step, bin_id) * price_ui_scale;
            let price_y_per_x = if price_x_per_y > 0.0 {
                1.0 / price_x_per_y
            } else {
                0.0
            };

            if amount_x > 0 || amount_y > 0 {
                populated_bin_count = populated_bin_count.saturating_add(1);
            }

            bins.push(PoolBinSnapshot {
                bin_id,
                distance_from_active: bin_id - lb_pair.active_bin_id as i64,
                price_x_per_y,
                price_y_per_x,
                onchain_price_raw: onchain_price_raw.to_string(),
                onchain_price_x_per_y,
                onchain_price_y_per_x,
                amount_x_raw: amount_x.to_string(),
                amount_y_raw: amount_y.to_string(),
                amount_x_ui: token_decimals
                    .get(&lb_pair.token_x_mint)
                    .copied()
                    .map(|d| raw_to_ui(amount_x as f64, d)),
                amount_y_ui: token_decimals
                    .get(&lb_pair.token_y_mint)
                    .copied()
                    .map(|d| raw_to_ui(amount_y as f64, d)),
            });
        }

        let base_fee_rate_raw = compute_base_fee_rate(
            lb_pair.base_factor,
            lb_pair.bin_step,
            lb_pair.base_fee_power_factor,
        );
        let variable_fee_rate_raw = compute_variable_fee_rate(
            lb_pair.volatility_accumulator,
            lb_pair.bin_step,
            lb_pair.variable_fee_control,
        );
        let total_fee_rate_raw = (base_fee_rate_raw + variable_fee_rate_raw).min(MAX_FEE_RATE);
        let base_fee_pct = fee_rate_to_pct(base_fee_rate_raw);
        let variable_fee_pct = fee_rate_to_pct(variable_fee_rate_raw);
        let total_fee_pct = fee_rate_to_pct(total_fee_rate_raw);
        let protocol_fee_pct = total_fee_pct * (lb_pair.protocol_share_bps as f64 / 10_000.0);

        let snapshot = PoolRpcSnapshot {
            active_bin_id: lb_pair.active_bin_id,
            bin_step: lb_pair.bin_step,
            token_x_mint: lb_pair.token_x_mint.clone(),
            token_y_mint: lb_pair.token_y_mint.clone(),
            token_x_decimals: token_decimals.get(&lb_pair.token_x_mint).copied(),
            token_y_decimals: token_decimals.get(&lb_pair.token_y_mint).copied(),
            reserve_x: lb_pair.reserve_x.clone(),
            reserve_y: lb_pair.reserve_y.clone(),
            reserve_x_raw: reserve_balances.get(&lb_pair.reserve_x).map(|v| v.0.clone()),
            reserve_y_raw: reserve_balances.get(&lb_pair.reserve_y).map(|v| v.0.clone()),
            reserve_x_ui: reserve_balances.get(&lb_pair.reserve_x).and_then(|v| v.1),
            reserve_y_ui: reserve_balances.get(&lb_pair.reserve_y).and_then(|v| v.1),
            token_x_symbol: known_symbol(&lb_pair.token_x_mint)
                .unwrap_or_else(|| short_mint(&lb_pair.token_x_mint)),
            token_y_symbol: known_symbol(&lb_pair.token_y_mint)
                .unwrap_or_else(|| short_mint(&lb_pair.token_y_mint)),
            current_price_x_per_y,
            base_factor: lb_pair.base_factor,
            base_fee_power_factor: lb_pair.base_fee_power_factor,
            variable_fee_control: lb_pair.variable_fee_control,
            volatility_accumulator: lb_pair.volatility_accumulator,
            protocol_share_bps: lb_pair.protocol_share_bps,
            protocol_fee_x_raw: lb_pair.protocol_fee_x.to_string(),
            protocol_fee_y_raw: lb_pair.protocol_fee_y.to_string(),
            base_fee_pct,
            variable_fee_pct,
            total_fee_pct,
            protocol_fee_pct,
            current_price_y_per_x,
            populated_bin_count,
            bins,
        };

        self.cache
            .write()
            .await
            .insert(pool.to_string(), (snapshot.clone(), now_ms));
        Ok(snapshot)
    }
}

async fn rpc_get_bin_arrays(
    http: &Client,
    rpc_url: &str,
    program_id: &str,
    pool: &str,
) -> Result<Vec<(i64, RawBin)>, Box<dyn std::error::Error + Send + Sync>> {
    let discr_bytes = bs58::encode(BIN_ARRAY_DISCRIMINATOR).into_string();
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            program_id,
            {
                "encoding": "base64",
                "filters": [
                    {"dataSize": BIN_ARRAY_SIZE},
                    {"memcmp": {"offset": 0, "bytes": discr_bytes}},
                    {"memcmp": {"offset": 24, "bytes": pool}}
                ]
            }
        ]
    });

    let body = rpc_post(http, rpc_url, &payload).await?;
    let parsed: RpcEnvelope<Vec<RpcProgramAccountResult>> = serde_json::from_value(body)?;

    let mut out = Vec::new();
    for account in parsed.result {
        let raw = base64::engine::general_purpose::STANDARD.decode(account.account.data.0.as_bytes())?;
        let bins = parse_bin_array(&raw)?;
        out.extend(bins);
    }
    Ok(out)
}

async fn rpc_get_account_data(
    http: &Client,
    rpc_url: &str,
    pubkey: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [pubkey, {"encoding": "base64"}]
    });

    let body = rpc_post(http, rpc_url, &payload).await?;
    let parsed: RpcEnvelope<RpcAccountInfoResult> = serde_json::from_value(body)?;
    let value = parsed.result.value.ok_or("account not found")?;
    let encoded = value.data.0;
    if value.data.1 != "base64" {
        return Err(format!("unexpected account encoding: {}", value.data.1).into());
    }

    Ok(base64::engine::general_purpose::STANDARD.decode(encoded.as_bytes())?)
}

async fn rpc_get_mint_decimals(
    http: &Client,
    rpc_url: &str,
    mints: &[String],
) -> Result<HashMap<String, u8>, Box<dyn std::error::Error + Send + Sync>> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [mints, {"encoding": "base64"}]
    });

    let body = rpc_post(http, rpc_url, &payload).await?;
    let accounts = body
        .pointer("/result/value")
        .and_then(Value::as_array)
        .ok_or("decode getMultipleAccounts result failed")?;

    let mut out = HashMap::new();
    for (index, maybe_account) in accounts.iter().enumerate() {
        let Some(encoded) = maybe_account.pointer("/data/0").and_then(Value::as_str) else {
            continue;
        };
        let raw = base64::engine::general_purpose::STANDARD.decode(encoded.as_bytes())?;
        if raw.len() > 44 {
            out.insert(mints[index].clone(), raw[44]);
        }
    }
    Ok(out)
}

async fn rpc_get_token_account_balances(
    http: &Client,
    rpc_url: &str,
    token_accounts: &[String],
) -> Result<HashMap<String, (String, Option<f64>)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = HashMap::new();
    for token_account in token_accounts {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountBalance",
            "params": [token_account]
        });
        let body = rpc_post(http, rpc_url, &payload).await?;
        let value = body
            .pointer("/result/value")
            .ok_or("getTokenAccountBalance missing result.value")?;
        let amount_raw = value
            .get("amount")
            .and_then(Value::as_str)
            .unwrap_or("0")
            .to_string();
        let amount_ui = value.get("uiAmount").and_then(Value::as_f64);
        out.insert(token_account.clone(), (amount_raw, amount_ui));
    }
    Ok(out)
}

async fn rpc_post(
    http: &Client,
    rpc_url: &str,
    payload: &Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let res = http.post(rpc_url).json(payload).send().await?;
    let body: Value = res.json().await?;
    if let Some(err) = body.get("error") {
        return Err(format!("rpc error: {err}").into());
    }
    Ok(body)
}

fn parse_lb_pair(
    raw: &[u8],
) -> Result<LbPairSummary, Box<dyn std::error::Error + Send + Sync>> {
    if raw.len() < 8 + 208 {
        return Err("lb pair account data too small".into());
    }
    if raw[0..8] != LB_PAIR_DISCRIMINATOR {
        return Err("account discriminator mismatch for LbPair".into());
    }

    let body = &raw[8..];
    Ok(LbPairSummary {
        active_bin_id: read_i32(body, 68)?,
        bin_step: read_u16(body, 72)?,
        token_x_mint: read_pubkey(body, 80)?,
        token_y_mint: read_pubkey(body, 112)?,
        reserve_x: read_pubkey(body, 144)?,
        reserve_y: read_pubkey(body, 176)?,
        protocol_fee_x: read_u64(body, 208)?,
        protocol_fee_y: read_u64(body, 216)?,
        base_factor: read_u16(body, 0)?,
        base_fee_power_factor: *body
            .get(26)
            .ok_or("out-of-bounds read base_fee_power_factor")?,
        variable_fee_control: read_u32(body, 8)?,
        volatility_accumulator: read_u32(body, 32)?,
        protocol_share_bps: read_u16(body, 24)?,
    })
}

fn parse_bin_array(
    raw: &[u8],
) -> Result<Vec<(i64, RawBin)>, Box<dyn std::error::Error + Send + Sync>> {
    if raw.len() < BIN_ARRAY_SIZE {
        return Err("bin array account data too small".into());
    }
    if raw[0..8] != BIN_ARRAY_DISCRIMINATOR {
        return Err("account discriminator mismatch for BinArray".into());
    }

    let body = &raw[8..];
    let index = read_i64(body, 0)?;
    let bins_offset = 48;
    let mut bins = Vec::with_capacity(MAX_BIN_PER_ARRAY as usize);

    for i in 0..(MAX_BIN_PER_ARRAY as usize) {
        let offset = bins_offset + i * BIN_SIZE;
        let amount_x = read_u64(body, offset)?;
        let amount_y = read_u64(body, offset + 8)?;
        let price = read_u128(body, offset + 16)?;
        let bin_id = index
            .saturating_mul(MAX_BIN_PER_ARRAY)
            .saturating_add(i as i64);
        bins.push((
            bin_id,
            RawBin {
                amount_x,
                amount_y,
                price,
            },
        ));
    }

    Ok(bins)
}

fn estimate_bin_price(bin_step: u16, bin_id: i64) -> f64 {
    let step = 1.0 + (bin_step as f64 / 10_000.0);
    step.powf(bin_id as f64)
}

fn raw_price_to_f64(raw: u128) -> f64 {
    (raw as f64) / Q64_ONE
}

fn compute_base_fee_rate(base_factor: u16, bin_step: u16, base_fee_power_factor: u8) -> u128 {
    let power = 10u128.saturating_pow(base_fee_power_factor as u32);
    (base_factor as u128)
        .saturating_mul(bin_step as u128)
        .saturating_mul(10)
        .saturating_mul(power)
}

fn compute_variable_fee_rate(
    volatility_accumulator: u32,
    bin_step: u16,
    variable_fee_control: u32,
) -> u128 {
    let va_step = (volatility_accumulator as u128).saturating_mul(bin_step as u128);
    let squared = va_step.saturating_mul(va_step);
    squared
        .saturating_mul(variable_fee_control as u128)
        .saturating_add(VARIABLE_FEE_OFFSET)
        / VARIABLE_FEE_SCALE
}

fn fee_rate_to_pct(raw_rate: u128) -> f64 {
    (raw_rate as f64 / FEE_PRECISION as f64) * 100.0
}

fn known_symbol(mint: &str) -> Option<String> {
    match mint {
        "So11111111111111111111111111111111111111112" => Some("SOL".to_string()),
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => Some("USDC".to_string()),
        "Es9vMFrzaCERmJfrF4H2FYD5f2QnR6M4P7m8v6nX8N9w" => Some("USDT".to_string()),
        _ => None,
    }
}

fn short_mint(mint: &str) -> String {
    if mint.len() <= 12 {
        return mint.to_string();
    }
    format!("{}...{}", &mint[..4], &mint[mint.len() - 4..])
}

fn raw_to_ui(raw: f64, decimals: u8) -> f64 {
    raw / 10f64.powi(decimals as i32)
}

fn read_i64(raw: &[u8], offset: usize) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = raw.get(offset..offset + 8).ok_or("out-of-bounds read i64")?;
    Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_i32(raw: &[u8], offset: usize) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = raw.get(offset..offset + 4).ok_or("out-of-bounds read i32")?;
    Ok(i32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u16(raw: &[u8], offset: usize) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = raw.get(offset..offset + 2).ok_or("out-of-bounds read u16")?;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u32(raw: &[u8], offset: usize) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = raw.get(offset..offset + 4).ok_or("out-of-bounds read u32")?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(raw: &[u8], offset: usize) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = raw.get(offset..offset + 8).ok_or("out-of-bounds read u64")?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u128(
    raw: &[u8],
    offset: usize,
) -> Result<u128, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = raw.get(offset..offset + 16).ok_or("out-of-bounds read u128")?;
    Ok(u128::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_pubkey(
    raw: &[u8],
    offset: usize,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = raw.get(offset..offset + 32).ok_or("out-of-bounds read pubkey")?;
    Ok(bs58::encode(bytes).into_string())
}

fn now_unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}
