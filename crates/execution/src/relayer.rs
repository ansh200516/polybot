//! Polymarket deposit-wallet RELAYER client (M6): live merge/redeem via the
//! relayer WALLET batch. See
//! docs/superpowers/specs/2026-06-25-m6-deposit-wallet-relayer-design.md
//!
//! Build order (per the M6 plan): M6-1 the Polygon-137 contract constants +
//! builder credentials; M6-2 the golden-vector-validated EIP-712 `Batch`
//! signing; M6-3 the merge/redeem calldata + deposit-wallet derivation; M6-4
//! (this task) the relayer I/O — builder-auth headers (reusing the CLOB L2 HMAC
//! in `auth.rs`), the WALLET request body, `POST /submit`, and the
//! `/transaction` state-machine poll to `STATE_CONFIRMED`. The live round-trip
//! is validated only at the operator's first FUNDED STAGING run (design §5/§8);
//! everything here is unit-tested offline (body shape, header shape, state
//! parse) with NO network I/O in tests.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy_primitives::{Address, B256, U256, address, b256, hex, keccak256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolStruct, eip712_domain, sol};
use pm_config::Live;
use serde_json::json;

use crate::auth::l2_signature;
use crate::secrets::{BuilderCreds, LiveSecrets};

// ---------------------------------------------------------------------------
// Polygon-137 contracts
// ---------------------------------------------------------------------------

/// Deposit-wallet factory (Polygon 137) — the `to` field of a WALLET batch.
///
/// CORRECTED in M6-4: this is the CURRENT Polymarket Polygon-137 deposit-wallet
/// factory (the one that created the operator's actual wallet; confirmed via
/// on-chain reconciliation + current docs whose own WALLET example uses it).
/// M6-1 had pinned the stale `py-builder-relayer-client@e7108cd`
/// reference-deployment value `0x894Ee6B254f251518206f709E9B115f214ebDf17`
/// (impl `0x55913A0bdecCbB77b7Af781A48300e6394B5EEAE`); those stay only as the
/// derivation-algorithm reference (the M6-3 golden vector uses the Amoy
/// factory/impl, so the derivation check is unaffected by this correction).
///
/// The WALLET request `to` is this factory; a configurable override lands in
/// M6-6 (the `RelayerClient` threads `relayer_url` + an optional factory) —
/// [`build_wallet_request`] already takes `factory` as a parameter so the
/// default const can be overridden without touching the builder.
pub const DEPOSIT_WALLET_FACTORY: Address =
    address!("0x00000000000Fb5C9ADea0298D729A0CB3823Cc07");

/// Deposit-wallet implementation (Polygon 137) — the `e7108cd` reference-client
/// value. Consumed ONLY by the M6-3 CREATE2 / ERC-1967 address-derivation
/// sanity helper (validated against the Amoy golden vector as an algorithm
/// check), NOT by the live WALLET batch.
pub const DEPOSIT_WALLET_IMPL: Address =
    address!("0x55913A0bdecCbB77b7Af781A48300e6394B5EEAE");

/// pUSD-native `CtfCollateralAdapter` (Polygon 137) — the merge/redeem TARGET
/// for STANDARD (binary) CTF markets. A WALLET batch call targets this adapter:
/// it pulls the deposit wallet's YES/NO ERC-1155 positions, runs the underlying
/// CTF merge/redeem, wraps the proceeds back into pUSD, and returns pUSD to the
/// wallet. Calldata (M6-3): `mergePositions(collateral, parentCollectionId=0x0,
/// conditionId, partition=[1,2], amount)` / `redeemPositions(.., indexSets=[1,2])`.
///
/// CONFIRMED (resolves design §6 Unknown B / M6-B) from THREE agreeing sources:
///   1. Polymarket's contracts reference (docs.polymarket.com/resources/contracts),
///   2. Polymarket's "Inventory Management" merge/split code example — same
///      address with exactly the design §2.3 `mergePositions` ABI, and
///   3. a registry verified live on-chain (chainID 137) + Polygonscan (2026-05-09).
///
/// LIVE-SAFETY: a STALE adapter address is a documented failure mode (the relayer
/// rejects the batch). This const is pinned by the test below, but a green test
/// only proves the ENCODED address — the relayer submit → `STATE_CONFIRMED`
/// round-trip is validated for real only at the operator's first FUNDED STAGING
/// run (design §5/§8) before any prod use. The relayer is OFF by default.
pub const CTF_COLLATERAL_ADAPTER: Address =
    address!("0xAdA100Db00Ca00073811820692005400218FcE1f");

/// pUSD-native `NegRiskCtfCollateralAdapter` (Polygon 137) — the merge/redeem
/// TARGET for NEGATIVE-RISK (multi-outcome) markets (design §2.3). Same
/// confirmation sources + live-safety caveat as [`CTF_COLLATERAL_ADAPTER`];
/// selecting standard-vs-NegRisk per market is a later task.
pub const NEGRISK_CTF_COLLATERAL_ADAPTER: Address =
    address!("0xadA2005600Dec949baf300f4C6120000bDB6eAab");

// ---------------------------------------------------------------------------
// EIP-712 deposit-wallet `Batch` signing (M6-2 — THE golden-vector gate)
// ---------------------------------------------------------------------------

sol! {
    /// One call inside a deposit-wallet batch. Field NAMES, ORDER, and TYPES
    /// are the EIP-712 typestring — never reorder or rename. `value` is
    /// `uint256` (NOT u64) and `data` is dynamic `bytes` (NOT a fixed array);
    /// the golden vector enforces this.
    #[derive(Debug)]
    struct Call {
        address target;
        uint256 value;
        bytes data;
    }

    /// The deposit-wallet batch the owner EOA signs (primaryType `Batch`).
    /// `calls` is a struct array — EIP-712 encodes it as the keccak of the
    /// concatenated `Call` member hashes. Matches
    /// `py-builder-relayer-client@e7108cd` `builder/deposit_wallet.py`
    /// (design §2.1); field order `wallet,nonce,deadline,calls` is load-bearing.
    #[derive(Debug)]
    struct Batch {
        address wallet;
        uint256 nonce;
        uint256 deadline;
        Call[] calls;
    }
}

#[derive(Debug)]
pub enum RelayerError {
    /// The owner EOA private key string could not be parsed into a signer.
    BadKey(String),
    /// The local signer failed to produce a signature.
    Sign(String),
    /// Builder-auth HMAC could not be computed (e.g. the builder secret was not
    /// valid base64url). Wraps the underlying `auth::AuthError`.
    Auth(String),
    /// HTTP transport failed, or the relayer returned a non-success status.
    Http(String),
    /// The relayer response was unparseable or missing a required field
    /// (`transactionID` on submit, `transactionHash` on a confirmed poll).
    Response(String),
    /// The relayer reported a TERMINAL failure state (`STATE_FAILED` /
    /// `STATE_INVALID`) — the batch will never confirm.
    Failed(String),
    /// `poll_until_confirmed` exhausted its timeout before reaching a terminal
    /// state (still pending — NOT necessarily a failure on-chain).
    Timeout(String),
}

impl std::fmt::Display for RelayerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelayerError::BadKey(e) => write!(f, "invalid signer key: {e}"),
            RelayerError::Sign(e) => write!(f, "batch signing error: {e}"),
            RelayerError::Auth(e) => write!(f, "builder auth error: {e}"),
            RelayerError::Http(e) => write!(f, "relayer http error: {e}"),
            RelayerError::Response(e) => write!(f, "relayer response error: {e}"),
            RelayerError::Failed(e) => write!(f, "relayer batch failed: {e}"),
            RelayerError::Timeout(e) => write!(f, "relayer poll timeout: {e}"),
        }
    }
}

impl std::error::Error for RelayerError {}

/// Sign a deposit-wallet `Batch` (EIP-712) with the owner EOA `pk`, returning
/// the 65-byte `r || s || v` signature as a `0x`-prefixed hex string (132
/// chars). This is the offline-validatable core of M6 — proven byte-identical
/// to Polymarket's golden vector by `sign_batch_matches_polymarket_golden_vector`.
///
/// The EIP-712 domain is `{ name: "DepositWallet", version: "1", chainId,
/// verifyingContract: <wallet> }` — the verifying contract is the DEPOSIT
/// WALLET itself (NOT the factory). `v` is encoded as 27/28 (Electrum
/// notation, via `Signature::as_bytes`), matching the eth_account convention
/// that produced the reference vector — IDENTICAL to `sign::sign_order` and
/// `auth::l1_signature`.
pub fn sign_batch(
    pk: &str,
    chain_id: u64,
    wallet: Address,
    nonce: u64,
    deadline: u64,
    calls: &[Call],
) -> Result<String, RelayerError> {
    let signer = pk
        .parse::<PrivateKeySigner>()
        .map_err(|e| RelayerError::BadKey(e.to_string()))?;
    let domain = eip712_domain! {
        name: "DepositWallet",
        version: "1",
        chain_id: chain_id,
        verifying_contract: wallet,
    };
    let batch = Batch {
        wallet,
        nonce: U256::from(nonce),
        deadline: U256::from(deadline),
        calls: calls.to_vec(),
    };
    let hash = batch.eip712_signing_hash(&domain);
    let sig = signer
        .sign_hash_sync(&hash)
        .map_err(|e| RelayerError::Sign(e.to_string()))?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

// ---------------------------------------------------------------------------
// (A) CtfCollateralAdapter merge/redeem CALLDATA (M6-3, design §2.3)
// ---------------------------------------------------------------------------

sol! {
    /// `CtfCollateralAdapter.mergePositions` — pulls the deposit wallet's
    /// YES+NO ERC-1155 legs, merges the complete set via the underlying CTF,
    /// and returns `amount` of `collateralToken`. The function NAME, ARG
    /// ORDER, and TYPES form the 4-byte selector preimage
    /// (`mergePositions(address,bytes32,bytes32,uint256[],uint256)`) — never
    /// reorder or rename (design §2.3).
    function mergePositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] partition,
        uint256 amount
    );

    /// `CtfCollateralAdapter.redeemPositions` — redeems a RESOLVED position
    /// back to `collateralToken`. `indexSets` selects the outcome slots
    /// (binary market: `[1, 2]`). Selector preimage
    /// `redeemPositions(address,bytes32,bytes32,uint256[])`.
    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] indexSets
    );
}

/// Build the WALLET-batch [`Call`] that merges a complete YES+NO set back to
/// `collateral` on `adapter` (the standard or NegRisk `CtfCollateralAdapter`).
///
/// `partition = [1, 2]` (the two binary outcome slots) and `parentCollectionId
/// = 0x0` (top-level condition) are fixed by the protocol; `amount` is the set
/// count in `collateral` base units. `value` is `0` — no native token moves.
/// `collateral`/`adapter` are PARAMETERS (the exact pUSD/USDC.e collateral is
/// reconciled in M6-6); nothing is hardcoded here.
pub fn merge_call(adapter: Address, collateral: Address, condition_id: B256, amount: U256) -> Call {
    let data = mergePositionsCall {
        collateralToken: collateral,
        parentCollectionId: B256::ZERO,
        conditionId: condition_id,
        partition: vec![U256::from(1), U256::from(2)],
        amount,
    }
    .abi_encode();
    Call {
        target: adapter,
        value: U256::ZERO,
        data: data.into(),
    }
}

/// Build the WALLET-batch [`Call`] that redeems a RESOLVED position back to
/// `collateral` on `adapter`. `indexSets = [1, 2]` and `parentCollectionId =
/// 0x0` are fixed by the protocol (binary market); `value` is `0`.
pub fn redeem_call(adapter: Address, collateral: Address, condition_id: B256) -> Call {
    let data = redeemPositionsCall {
        collateralToken: collateral,
        parentCollectionId: B256::ZERO,
        conditionId: condition_id,
        indexSets: vec![U256::from(1), U256::from(2)],
    }
    .abi_encode();
    Call {
        target: adapter,
        value: U256::ZERO,
        data: data.into(),
    }
}

// ---------------------------------------------------------------------------
// (B) Deposit-wallet address derivation (M6-3) — CREATE2 over the ERC-1967 clone
// ---------------------------------------------------------------------------
// Ported 1:1 from `py-builder-relayer-client` `builder/derive.py`; proven
// byte-exact against its golden vector (`derive_deposit_wallet_matches_golden_vector`).

/// Low 10 bytes of the ERC-1967 clone init-code, as a big-endian integer.
/// The deploy-time `(len(args) << 56)` is folded in by ADDITION (matching the
/// Python `ERC1967_PREFIX + (n << 56)`), then the low 10 bytes are emitted.
const ERC1967_PREFIX: u128 = 0x61003D3D8160233D3973;

/// Tail constants of the ERC-1967 minimal-clone init-code (Solady-style).
/// `CONST2` is emitted BEFORE `CONST1` (load-bearing order, per the Python).
const ERC1967_CONST1: B256 =
    b256!("0xcc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3");
const ERC1967_CONST2: B256 =
    b256!("0x5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076");

/// `keccak256` of the ERC-1967 minimal-clone init-code for `implementation`
/// with trailing immutable `args`. Layout (port of `init_code_hash_erc1967`):
/// `prefix(10) ‖ implementation(20) ‖ 0x6009 ‖ CONST2(32) ‖ CONST1(32) ‖ args`.
fn init_code_hash_erc1967(implementation: Address, args: &[u8]) -> B256 {
    let n = args.len() as u128;
    let combined = (ERC1967_PREFIX + (n << 56)).to_be_bytes();
    let mut init_code = Vec::with_capacity(10 + 20 + 2 + 32 + 32 + args.len());
    init_code.extend_from_slice(&combined[6..16]); // low 10 bytes, big-endian
    init_code.extend_from_slice(implementation.as_slice());
    init_code.extend_from_slice(&[0x60, 0x09]);
    init_code.extend_from_slice(ERC1967_CONST2.as_slice());
    init_code.extend_from_slice(ERC1967_CONST1.as_slice());
    init_code.extend_from_slice(args);
    keccak256(init_code)
}

/// EIP-1014 CREATE2 address: `keccak256(0xff ‖ from ‖ salt ‖ bytecode_hash)[12:]`.
/// Port of `get_create2_address`.
fn get_create2_address(bytecode_hash: B256, from_address: Address, salt: B256) -> Address {
    let mut buf = [0u8; 85];
    buf[0] = 0xff;
    buf[1..21].copy_from_slice(from_address.as_slice());
    buf[21..53].copy_from_slice(salt.as_slice());
    buf[53..85].copy_from_slice(bytecode_hash.as_slice());
    Address::from_word(keccak256(buf))
}

/// Derive the deterministic deposit-wallet address for `owner` under the
/// relayer's `factory`/`implementation` (CREATE2 over the ERC-1967 minimal
/// clone). Port of `derive_deposit_wallet`:
/// - `wallet_id = keccak256(owner)` (keccak of the 20-byte owner address),
/// - `args = abi_encode(address factory, bytes32 wallet_id)` (64 bytes),
/// - `salt = keccak256(args)`,
/// - `bytecode_hash = init_code_hash_erc1967(implementation, args)`,
/// - return `CREATE2(from = factory, salt, bytecode_hash)`.
///
/// The returned [`Address`] compares byte-for-byte; render with `to_checksum`
/// for the EIP-55 form. Proven byte-exact by the golden-vector test.
pub fn derive_deposit_wallet(owner: Address, factory: Address, implementation: Address) -> Address {
    let wallet_id = keccak256(owner.as_slice());

    // args = ABI encoding of (address factory, bytes32 wallet_id): the address
    // is right-aligned in the first 32-byte word; wallet_id fills the second.
    let mut args = [0u8; 64];
    args[12..32].copy_from_slice(factory.as_slice());
    args[32..64].copy_from_slice(wallet_id.as_slice());

    let salt = keccak256(args);
    let bytecode_hash = init_code_hash_erc1967(implementation, &args);
    get_create2_address(bytecode_hash, factory, salt)
}

// ---------------------------------------------------------------------------
// (C) Relayer I/O (M6-4): builder-auth headers, WALLET request body,
//     POST /submit, and the /transaction state-machine poll (design §2.2/§6).
// ---------------------------------------------------------------------------

/// Production relayer base URL (no trailing slash; join with `format!`).
pub const RELAYER_URL_PROD: &str = "https://relayer-v2.polymarket.com";
/// Staging relayer base URL — the operator's FIRST funded run targets this
/// (design §7 "staging-first").
pub const RELAYER_URL_STAGING: &str = "https://relayer-v2-staging.polymarket.dev";

/// Seconds between `/transaction` polls. Polygon blocks are ~2 s and this runs
/// off the quote hot path (a periodic sweep — design §3), so a relaxed interval
/// is fine.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// The relayer transaction lifecycle (confirmed states, design §"States"):
/// `STATE_NEW` → `STATE_EXECUTED` → `STATE_MINED` → `STATE_CONFIRMED` (terminal
/// OK); `STATE_FAILED` / `STATE_INVALID` are terminal ERRORS. Any unrecognised
/// value is kept as [`RelayerState::Other`] and treated as NON-terminal (keep
/// polling until the timeout) so an unexpected string is never mistaken for
/// success or hard failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayerState {
    New,
    Executed,
    Mined,
    Confirmed,
    Failed,
    Invalid,
    Other(String),
}

impl RelayerState {
    /// Parse a relayer `state` string into the typed state (unknown → `Other`).
    pub fn parse(s: &str) -> Self {
        match s {
            "STATE_NEW" => RelayerState::New,
            "STATE_EXECUTED" => RelayerState::Executed,
            "STATE_MINED" => RelayerState::Mined,
            "STATE_CONFIRMED" => RelayerState::Confirmed,
            "STATE_FAILED" => RelayerState::Failed,
            "STATE_INVALID" => RelayerState::Invalid,
            other => RelayerState::Other(other.to_string()),
        }
    }

    /// Whether polling can stop: `STATE_CONFIRMED` (success) or
    /// `STATE_FAILED`/`STATE_INVALID` (failure). `STATE_MINED` is NOT terminal —
    /// the design requires `STATE_CONFIRMED` before a batch is considered done.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RelayerState::Confirmed | RelayerState::Failed | RelayerState::Invalid
        )
    }

    /// Whether the batch confirmed successfully (`STATE_CONFIRMED`).
    pub fn is_success(&self) -> bool {
        matches!(self, RelayerState::Confirmed)
    }
}

/// Builder-auth headers for a relayer request. The builder auth uses the SAME
/// HMAC scheme as the CLOB L2 auth — `base64url(HMAC-SHA256(base64url-decode(
/// secret), ts + METHOD + path + body))` — so we REUSE [`auth::l2_signature`]
/// rather than reimplementing it; only the header NAMES and the secret differ
/// (`POLY_BUILDER_*` + the builder secret instead of `POLY_*` + the CLOB
/// secret). Mirrors `auth.rs::l2_headers`.
///
/// For a GET, `path` MUST exclude the query string (same rule as the CLOB L2
/// HMAC); `body` is the EXACT serialized request string for a POST, or `None`.
/// Header ORDER is not significant to the relayer (HTTP header names are
/// unordered); we emit key/timestamp/passphrase/signature for a stable test.
pub fn builder_headers(
    creds: &BuilderCreds,
    ts: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<Vec<(&'static str, String)>, RelayerError> {
    let signature = l2_signature(creds.secret.expose(), ts, method, path, body)
        .map_err(|e| RelayerError::Auth(e.to_string()))?;
    Ok(vec![
        ("POLY_BUILDER_API_KEY", creds.key.clone()),
        ("POLY_BUILDER_TIMESTAMP", ts.to_string()),
        ("POLY_BUILDER_PASSPHRASE", creds.passphrase.expose().to_string()),
        ("POLY_BUILDER_SIGNATURE", signature),
    ])
}

/// Build the relayer WALLET-batch request body (design §2.2; matches the py
/// client `to_dict()`). Field shapes (load-bearing): `type` = `"WALLET"`;
/// `nonce`/`deadline`/`value` are STRINGS; `data`/`target`/addresses are
/// `0x`-hex; `to` is the deposit-wallet `factory` (a parameter — the M6-6
/// client can override the [`DEPOSIT_WALLET_FACTORY`] default). `signature` is
/// the 65-byte `Batch` signature from [`sign_batch`].
///
/// Addresses render EIP-55 checksummed (alloy `Address` Display == web3.py's
/// `to_checksum_address`, which the py `to_dict()` emits). The relayer recovers
/// the signer from the `Batch` signature, so the address-field CASE is not the
/// security boundary, but the exact accepted form is a staging-confirmation
/// item.
pub fn build_wallet_request(
    from: Address,
    factory: Address,
    wallet: Address,
    nonce: u64,
    deadline: u64,
    calls: &[Call],
    signature: &str,
) -> serde_json::Value {
    let calls_json: Vec<serde_json::Value> = calls
        .iter()
        .map(|c| {
            json!({
                "target": c.target.to_string(),
                "value": c.value.to_string(),
                "data": format!("0x{}", hex::encode(&c.data[..])),
            })
        })
        .collect();
    json!({
        "type": "WALLET",
        "from": from.to_string(),
        "to": factory.to_string(),
        "nonce": nonce.to_string(),
        "signature": signature,
        "depositWalletParams": {
            "depositWallet": wallet.to_string(),
            "deadline": deadline.to_string(),
            "calls": calls_json,
        }
    })
}

/// Extract the transaction id from a `POST /submit` response
/// (`{ "transactionID": "...", "state": "STATE_NEW", ... }`). Tries
/// `transactionID` then `transactionId` (cheap casing insurance, mirroring
/// `auth.rs`'s `apiKey`/`api_key` fallback). Factored out so submit parsing is
/// unit-testable without HTTP.
fn parse_submit_response(v: &serde_json::Value) -> Option<String> {
    v.get("transactionID")
        .or_else(|| v.get("transactionId"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Parse a `GET /transaction` poll response into `(state, transactionHash?)`.
/// A missing/non-string `state` yields `Other("")` (non-terminal → keep
/// polling). Factored out so the state-machine logic is unit-testable without
/// HTTP.
fn parse_transaction_response(v: &serde_json::Value) -> (RelayerState, Option<String>) {
    let state = v
        .get("state")
        .and_then(serde_json::Value::as_str)
        .map_or_else(|| RelayerState::Other(String::new()), RelayerState::parse);
    let hash = v
        .get("transactionHash")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    (state, hash)
}

/// Unix time in whole seconds. Backs the builder-auth timestamp (stringified
/// via [`unix_seconds_string`]) AND the typed batch `deadline`. Mirrors
/// `live.rs::unix_seconds_string`.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Unix seconds as a decimal string (the builder-auth timestamp).
fn unix_seconds_string() -> String {
    now_secs().to_string()
}

/// Submit a signed WALLET batch: `POST {relayer_url}/submit` with builder-auth
/// headers + the JSON `body`, returning the relayer `transactionID`.
///
/// The body is serialized ONCE to a canonical string; that EXACT string is both
/// HMAC'd (`builder_headers(.., "POST", "/submit", Some(&body_str))`) and sent
/// as the request body — they must be byte-identical (same invariant as the
/// CLOB POST in `live.rs`). Every failure maps to a [`RelayerError`]; this never
/// panics.
pub async fn submit_wallet_batch(
    http: &reqwest::Client,
    relayer_url: &str,
    creds: &BuilderCreds,
    body: &serde_json::Value,
) -> Result<String, RelayerError> {
    let base = relayer_url.trim_end_matches('/');
    let path = "/submit";
    let body_str = serde_json::to_string(body)
        .map_err(|e| RelayerError::Response(format!("serialize WALLET body: {e}")))?;
    let ts = unix_seconds_string();
    let headers = builder_headers(creds, &ts, "POST", path, Some(&body_str))?;

    let url = format!("{base}{path}");
    let mut req = http
        .post(&url)
        .header("Content-Type", "application/json")
        .body(body_str);
    for (k, v) in &headers {
        req = req.header(*k, v);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| RelayerError::Http(format!("POST {path}: {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| RelayerError::Http(format!("POST {path}: read body: {e}")))?;
    if !status.is_success() {
        return Err(RelayerError::Http(format!("POST {path}: HTTP {status}: {text}")));
    }
    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| RelayerError::Response(format!("POST {path}: invalid JSON: {e}: {text}")))?;
    parse_submit_response(&json)
        .ok_or_else(|| RelayerError::Response(format!("POST {path}: missing transactionID: {text}")))
}

/// Poll `GET {relayer_url}/transaction?id={tx_id}` until the batch reaches a
/// terminal state or `timeout` elapses. Returns the `transactionHash` on
/// `STATE_CONFIRMED`; `Err(Failed)` on `STATE_FAILED`/`STATE_INVALID`;
/// `Err(Timeout)` if still pending at the deadline. Sleeps [`POLL_INTERVAL`]
/// between polls. `STATE_MINED` is intentionally NOT accepted — the design
/// requires `STATE_CONFIRMED`.
///
/// The HMAC path is the query-LESS `/transaction` (same GET rule as the CLOB L2
/// HMAC); builder auth is included even though the read may not require it
/// ("include it to be safe"). Every failure maps to a [`RelayerError`]; this
/// never panics.
pub async fn poll_until_confirmed(
    http: &reqwest::Client,
    relayer_url: &str,
    creds: &BuilderCreds,
    tx_id: &str,
    timeout: Duration,
) -> Result<String, RelayerError> {
    let base = relayer_url.trim_end_matches('/');
    let path = "/transaction";
    let deadline = Instant::now() + timeout;
    loop {
        let ts = unix_seconds_string();
        let headers = builder_headers(creds, &ts, "GET", path, None)?;
        let url = format!("{base}{path}?id={tx_id}");
        let mut req = http.get(&url);
        for (k, v) in &headers {
            req = req.header(*k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RelayerError::Http(format!("GET {path}: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| RelayerError::Http(format!("GET {path}: read body: {e}")))?;
        if !status.is_success() {
            return Err(RelayerError::Http(format!("GET {path}: HTTP {status}: {text}")));
        }
        let json: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            RelayerError::Response(format!("GET {path}: invalid JSON: {e}: {text}"))
        })?;
        let (state, hash) = parse_transaction_response(&json);
        if state.is_success() {
            return hash.ok_or_else(|| {
                RelayerError::Response(format!("STATE_CONFIRMED but no transactionHash: {text}"))
            });
        }
        if state.is_terminal() {
            return Err(RelayerError::Failed(format!(
                "relayer terminal state {state:?} for tx {tx_id}: {text}"
            )));
        }
        if Instant::now() >= deadline {
            return Err(RelayerError::Timeout(format!(
                "tx {tx_id} still in state {state:?} after {timeout:?}"
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

// ---------------------------------------------------------------------------
// (D) Relayer WALLET nonce (the corrected M6-5)
// ---------------------------------------------------------------------------

/// Parse the relayer `/nonce` response into the next WALLET batch nonce. The
/// relayer returns `{ "nonce": <n> }` where `<n>` is EITHER a JSON number OR a
/// decimal string (current docs show both shapes), so we accept either; any
/// other shape is a [`RelayerError::Response`]. Factored out so the parse is
/// unit-testable without HTTP.
fn parse_nonce(v: &serde_json::Value) -> Result<u64, RelayerError> {
    let n = v
        .get("nonce")
        .ok_or_else(|| RelayerError::Response(format!("missing nonce field: {v}")))?;
    if let Some(u) = n.as_u64() {
        return Ok(u);
    }
    if let Some(s) = n.as_str() {
        return s
            .trim()
            .parse::<u64>()
            .map_err(|e| RelayerError::Response(format!("nonce string not a u64: {s:?}: {e}")));
    }
    Err(RelayerError::Response(format!(
        "nonce is neither number nor string: {n}"
    )))
}

/// Fetch the deposit wallet's next batch nonce from the RELAYER:
/// `GET {relayer_url}/nonce?address={owner}&type=WALLET` → `{ "nonce": <n> }`.
///
/// CORRECTION (folded into M6-6): the M6 plan's M6-5 read the nonce on-chain via
/// a Polygon RPC `nonce()` view, but per the current Polymarket docs the relayer
/// itself serves the WALLET nonce — and since the relayer is the authority for
/// the next batch nonce it will accept, asking it directly is the correct source
/// and drops the RPC round-trip (the `RPC_URL` secret is now unused by the merge/
/// redeem path).
///
/// The builder-auth HMAC signs the query-LESS path `/nonce` (same GET rule as the
/// CLOB L2 HMAC and the `/transaction` poll); the `address`/`type` query is
/// appended after. `owner` renders EIP-55 checksummed (alloy `Address` Display)
/// — the exact accepted address form is a staging-confirmation item, but the
/// relayer keys the nonce off the recovered owner regardless. Accepts a number
/// OR string nonce (see [`parse_nonce`]). Every failure maps to a
/// [`RelayerError`]; this never panics.
pub async fn fetch_wallet_nonce(
    http: &reqwest::Client,
    relayer_url: &str,
    creds: &BuilderCreds,
    owner: Address,
) -> Result<u64, RelayerError> {
    let base = relayer_url.trim_end_matches('/');
    let path = "/nonce";
    let ts = unix_seconds_string();
    let headers = builder_headers(creds, &ts, "GET", path, None)?;
    let url = format!("{base}{path}?address={owner}&type=WALLET");
    let mut req = http.get(&url);
    for (k, v) in &headers {
        req = req.header(*k, v);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| RelayerError::Http(format!("GET {path}: {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| RelayerError::Http(format!("GET {path}: read body: {e}")))?;
    if !status.is_success() {
        return Err(RelayerError::Http(format!("GET {path}: HTTP {status}: {text}")));
    }
    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| RelayerError::Response(format!("GET {path}: invalid JSON: {e}: {text}")))?;
    parse_nonce(&json)
}

// ---------------------------------------------------------------------------
// (E) RelayerClient — assembled merge/redeem orchestration (M6-6)
// ---------------------------------------------------------------------------

/// Native USDC.e on Polygon 137 — the DEFAULT merge/redeem collateral token.
///
/// CONCERN / STAGING-ITEM: the pUSD-native `CtfCollateralAdapter` may pull/return
/// pUSD rather than USDC.e (design §6 Unknown B / §7). Both are 6-decimal tokens,
/// so the micro-USDC accounting (1 complete set = $1 = 1e6 base units) is
/// IDENTICAL regardless of which one the adapter uses; only the token ADDRESS
/// differs. The address is a configurable [`RelayerClient`] field (defaulted
/// here) so the operator can switch to pUSD at the first funded staging run
/// without a code change.
pub const USDC_E_COLLATERAL: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

/// Default submit→`STATE_CONFIRMED` budget for one merge/redeem batch. The MM
/// runs merge/redeem as a periodic, off-hot-path sweep (design §3/§7), so a
/// generous timeout is fine (Polygon blocks are ~2 s).
const DEFAULT_SUBMIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Seconds added to `now` for the WALLET batch `deadline` — the window the
/// relayer has to land the batch on-chain before the signed `Batch` expires.
const DEADLINE_SECS: u64 = 600;

/// The assembled deposit-wallet relayer client (M6-6): fetch the WALLET nonce,
/// build the signed WALLET request body for a merge/redeem, submit it, and poll
/// to `STATE_CONFIRMED`. Built in on-chain `conditionId` terms (NOT the
/// `MarketId`-based [`crate::venue::ExecutionVenue`] trait — see `live.rs`); the
/// MM (M6-7) holds an `Option<RelayerClient>` and calls `merge`/`redeem`
/// directly off the quote hot path.
///
/// Constructed ONLY when the relayer is enabled AND builder creds + the deposit
/// wallet + a valid EOA key are present (see [`RelayerClient::new`]); otherwise
/// live merge/redeem stays the hold-to-resolution no-op (design §7). OFF by
/// default + staging-first.
pub struct RelayerClient {
    /// Shared HTTP client (reqwest is internally `Arc`, cheap to clone/share).
    http: reqwest::Client,
    /// Staging or prod base URL, from config (no trailing slash assumed; the I/O
    /// helpers trim it). Override > `relayer_staging` ? staging : prod.
    relayer_url: String,
    /// Builder/relayer HMAC credentials (the relayer rejects unauthenticated
    /// WALLET batches, design §1).
    creds: BuilderCreds,
    /// The owner EOA private key (0x-stripped hex, the `expose_key_hex` form the
    /// rest of the app feeds `PrivateKeySigner`). Signs the EIP-712 `Batch` AND
    /// is the WALLET request `from`/owner. Validated to parse in `new`.
    pk: String,
    /// EIP-712 domain `chainId` (Polygon 137 — [`crate::sign::CHAIN_ID`]).
    chain_id: u64,
    /// The deposit wallet (`PM_DEPOSIT_WALLET`): the `Batch` verifyingContract,
    /// the request `depositWallet`, and the on-chain holder of the positions.
    wallet: Address,
    /// The WALLET request `to` — the deposit-wallet factory (default
    /// [`DEPOSIT_WALLET_FACTORY`]; configurable).
    factory: Address,
    /// The merge/redeem call TARGET — the `CtfCollateralAdapter` (default
    /// [`CTF_COLLATERAL_ADAPTER`]; configurable, e.g. the NegRisk adapter).
    adapter: Address,
    /// The collateral token the adapter pulls/returns (default
    /// [`USDC_E_COLLATERAL`]; configurable — pUSD is a staging item).
    collateral: Address,
    /// Per-batch submit→confirm timeout passed to [`poll_until_confirmed`].
    submit_timeout: Duration,
}

impl RelayerClient {
    /// Build the client from the `[live]` config + resolved live secrets, or
    /// `None` when the relayer must NOT be constructed. Returns `Some` ONLY when
    /// ALL of:
    /// - `live_cfg.relayer_enabled` is true (the master switch, default OFF),
    /// - builder creds are present (`BUILDER_*`, all-or-none from the env loader),
    /// - the deposit wallet is present AND parses to an `Address`, and
    /// - the EOA key parses to a `PrivateKeySigner` (validated up-front so
    ///   `owner`/`sign_batch` never surprise the sweep at submit time).
    ///
    /// `relayer_url` = the explicit `live_cfg.relayer_url` override if set, else
    /// [`RELAYER_URL_STAGING`] when `relayer_staging` (the default, staging-first)
    /// or [`RELAYER_URL_PROD`] otherwise. factory/adapter/collateral default to
    /// the pinned Polygon-137 consts (USDC.e collateral).
    pub fn new(live_cfg: &Live, secrets: &LiveSecrets, http: reqwest::Client) -> Option<Self> {
        // Gate 1: the relayer master switch (default OFF — opt in deliberately).
        if !live_cfg.relayer_enabled {
            return None;
        }
        // Gate 2: builder creds (all-or-none; absent → relayer not configured).
        let creds = secrets.builder.clone()?;
        // Gate 3: the deposit wallet must be present AND a valid address.
        let wallet: Address = secrets.deposit_wallet.as_deref()?.parse().ok()?;
        // Gate 4: the EOA key must parse to a signer. Store the 0x-stripped hex
        // (the `expose_key_hex` form the app feeds PrivateKeySigner elsewhere);
        // validate it parses now so `owner()`/`sign_batch` can't fail mid-sweep
        // on a malformed key.
        let pk = secrets.private_key.expose_key_hex();
        if pk.parse::<PrivateKeySigner>().is_err() {
            return None;
        }
        // URL: explicit override wins; else staging-vs-prod from the flag.
        let relayer_url = match live_cfg.relayer_url.as_deref() {
            Some(u) => u.to_string(),
            None if live_cfg.relayer_staging => RELAYER_URL_STAGING.to_string(),
            None => RELAYER_URL_PROD.to_string(),
        };
        Some(RelayerClient {
            http,
            relayer_url,
            creds,
            pk,
            chain_id: crate::sign::CHAIN_ID,
            wallet,
            factory: DEPOSIT_WALLET_FACTORY,
            adapter: CTF_COLLATERAL_ADAPTER,
            collateral: USDC_E_COLLATERAL,
            submit_timeout: DEFAULT_SUBMIT_TIMEOUT,
        })
    }

    /// The owner EOA address derived from `pk` — both the WALLET request
    /// `from`/owner and the EIP-712 `Batch` signer. The key is validated in
    /// `new`, so this only errors on a corrupted key; surfaced (never panics) as
    /// [`RelayerError::BadKey`].
    fn owner(&self) -> Result<Address, RelayerError> {
        self.pk
            .parse::<PrivateKeySigner>()
            .map(|s| s.address())
            .map_err(|e| RelayerError::BadKey(e.to_string()))
    }

    /// Build the signed WALLET request body for a MERGE of a complete YES+NO set
    /// (`amount` base units) on `condition_id`. Pure (no HTTP): nonce/deadline in
    /// → signed JSON body out. The single [`merge_call`] is signed via
    /// [`sign_batch`] and wrapped by [`build_wallet_request`]. UNIT-TESTED.
    fn build_merge_batch(
        &self,
        condition_id: B256,
        amount: U256,
        nonce: u64,
        deadline: u64,
    ) -> Result<serde_json::Value, RelayerError> {
        let calls = [merge_call(self.adapter, self.collateral, condition_id, amount)];
        let sig = sign_batch(&self.pk, self.chain_id, self.wallet, nonce, deadline, &calls)?;
        Ok(build_wallet_request(
            self.owner()?,
            self.factory,
            self.wallet,
            nonce,
            deadline,
            &calls,
            &sig,
        ))
    }

    /// Build the signed WALLET request body for a REDEEM of the resolved
    /// `condition_id` (binary `indexSets = [1, 2]`). Pure (no HTTP), the redeem
    /// mirror of [`build_merge_batch`]. UNIT-TESTED.
    fn build_redeem_batch(
        &self,
        condition_id: B256,
        nonce: u64,
        deadline: u64,
    ) -> Result<serde_json::Value, RelayerError> {
        let calls = [redeem_call(self.adapter, self.collateral, condition_id)];
        let sig = sign_batch(&self.pk, self.chain_id, self.wallet, nonce, deadline, &calls)?;
        Ok(build_wallet_request(
            self.owner()?,
            self.factory,
            self.wallet,
            nonce,
            deadline,
            &calls,
            &sig,
        ))
    }

    /// Live MERGE of a complete YES+NO set back to collateral on `condition_id`:
    /// fetch nonce → build the signed body → `POST /submit` → poll to
    /// `STATE_CONFIRMED`. Returns the recovered amount in MICRO-USDC.
    ///
    /// A merged complete set redeems at exactly $1/set, and `amount` is in CTF
    /// base units (6 decimals — the adapter's 6-decimal collateral), which is 1:1
    /// with micro-USDC; so the recovered micro-USDC == `amount`. Saturates into
    /// `i128` (real inventory is far below `i128::MAX`; `unwrap_or` keeps the
    /// money path panic-free). The LIVE round-trip (auth + endpoints) is the
    /// operator's funded staging validation — every failure here is a typed
    /// [`RelayerError`], never a panic.
    pub async fn merge(&self, condition_id: B256, amount: U256) -> Result<i128, RelayerError> {
        let owner = self.owner()?;
        let nonce = fetch_wallet_nonce(&self.http, &self.relayer_url, &self.creds, owner).await?;
        let deadline = now_secs() + DEADLINE_SECS;
        let body = self.build_merge_batch(condition_id, amount, nonce, deadline)?;
        let tx_id = submit_wallet_batch(&self.http, &self.relayer_url, &self.creds, &body).await?;
        let _hash =
            poll_until_confirmed(&self.http, &self.relayer_url, &self.creds, &tx_id, self.submit_timeout)
                .await?;
        Ok(i128::try_from(amount).unwrap_or(i128::MAX))
    }

    /// Live REDEEM of the resolved `condition_id`: fetch nonce → build the signed
    /// body → `POST /submit` → poll to `STATE_CONFIRMED`. Returns the recovered
    /// amount in MICRO-USDC.
    ///
    /// `redeemPositions` pays out the WINNING-slot balance the CTF holds for this
    /// wallet — there is no `amount` argument and we do NOT know the resolved
    /// balance pre-call. So on success we return `0` micro ("confirmed, amount
    /// unknown") rather than guessing; M6-7 reconciles the actual credit from the
    /// Polymarket Data API (the same `redeemable`/positions read the reconcile
    /// path already uses). Failures map to a typed [`RelayerError`]; never panics.
    pub async fn redeem(&self, condition_id: B256) -> Result<i128, RelayerError> {
        let owner = self.owner()?;
        let nonce = fetch_wallet_nonce(&self.http, &self.relayer_url, &self.creds, owner).await?;
        let deadline = now_secs() + DEADLINE_SECS;
        let body = self.build_redeem_batch(condition_id, nonce, deadline)?;
        let tx_id = submit_wallet_batch(&self.http, &self.relayer_url, &self.creds, &body).await?;
        let _hash =
            poll_until_confirmed(&self.http, &self.relayer_url, &self.creds, &tx_id, self.submit_timeout)
                .await?;
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// THE M6 GATE (M6-2): reproduce Polymarket's deposit-wallet `Batch` golden
    /// signature byte-for-byte. Fixture from `py-builder-relayer-client@e7108cd`
    /// `tests/builder/test_deposit_wallet.py` (chain 137, the public anvil key).
    /// A mismatch means the EIP-712 typed-data encoding is wrong — do not weaken
    /// this test; debug the encoding until it is byte-exact.
    #[test]
    fn sign_batch_matches_polymarket_golden_vector() {
        use alloy_primitives::{Address, U256};
        let pk = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"; // public anvil key
        let wallet: Address = "0xa2927E7834648F1C03b4961CeeA4597292e3c025".parse().unwrap();
        let token: Address = "0x0000000000000000000000000000000000000001".parse().unwrap();
        let data = alloy_primitives::hex::decode(
            "095ea7b30000000000000000000000000000000000000000000000000000000000000002ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        ).unwrap();
        let calls = vec![Call { target: token, value: U256::ZERO, data: data.into() }];
        let sig = sign_batch(pk, 137u64, wallet, 0u64, 1234567890u64, &calls).unwrap();
        assert_eq!(
            sig,
            "0x7827946c566e7860f6c5f2e641587ed6928989c8618e463a00dd56832e7300023b7436c67a2ea82d6d506b1a5eda3e27526e9e2ffaad52128d75c47c2e9d1fac1b"
        );
        assert_eq!(sig.len(), 132);
    }

    /// LIVE GATE (M6-B): the collateral-adapter consts MUST be real, non-zero
    /// Polygon-137 addresses before any live merge/redeem batch is built — a
    /// zero/wrong adapter would silently send a real on-chain batch to the wrong
    /// target. We pin the exact EIP-55-checksummed mainnet addresses: the
    /// `to_checksum` round-trip RE-DERIVES the checksum from the const's own
    /// bytes, so a typo'd const (or a non-canonical literal) fails this test.
    ///
    /// Reminder: green here proves the ENCODED address only; the relayer
    /// submit → `STATE_CONFIRMED` round-trip is validated at the operator's
    /// first funded STAGING run (design §5/§8).
    #[test]
    fn adapter_address_must_be_set_before_live() {
        assert_ne!(
            CTF_COLLATERAL_ADAPTER,
            Address::ZERO,
            "CTF_COLLATERAL_ADAPTER is unset (zero) — refuse to go live (M6-B)"
        );
        assert_eq!(
            CTF_COLLATERAL_ADAPTER.to_checksum(None),
            "0xAdA100Db00Ca00073811820692005400218FcE1f",
            "CTF_COLLATERAL_ADAPTER must equal the confirmed Polygon-137 adapter"
        );

        assert_ne!(
            NEGRISK_CTF_COLLATERAL_ADAPTER,
            Address::ZERO,
            "NEGRISK_CTF_COLLATERAL_ADAPTER is unset (zero) — refuse to go live (M6-B)"
        );
        assert_eq!(
            NEGRISK_CTF_COLLATERAL_ADAPTER.to_checksum(None),
            "0xadA2005600Dec949baf300f4C6120000bDB6eAab",
            "NEGRISK_CTF_COLLATERAL_ADAPTER must equal the confirmed Polygon-137 adapter"
        );
    }

    /// Pin the factory/impl so a careless edit is caught; they are distinct and
    /// non-zero. FACTORY is the CURRENT Polygon-137 deposit-wallet factory
    /// (corrected in M6-4 — it created the operator's actual wallet); IMPL stays
    /// the `e7108cd` reference value the M6-3 derivation-algorithm check uses.
    #[test]
    fn deposit_wallet_contracts_are_pinned() {
        assert_eq!(
            DEPOSIT_WALLET_FACTORY.to_checksum(None),
            "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07",
            "DEPOSIT_WALLET_FACTORY must be the current Polygon-137 factory (M6-4 correction)"
        );
        assert_eq!(
            DEPOSIT_WALLET_IMPL.to_checksum(None),
            "0x55913A0bdecCbB77b7Af781A48300e6394B5EEAE"
        );
        assert_ne!(DEPOSIT_WALLET_FACTORY, Address::ZERO);
        assert_ne!(DEPOSIT_WALLET_IMPL, Address::ZERO);
        assert_ne!(DEPOSIT_WALLET_FACTORY, DEPOSIT_WALLET_IMPL);
        // The stale e7108cd reference factory must NOT be the live `to`.
        assert_ne!(
            DEPOSIT_WALLET_FACTORY,
            address!("0x894Ee6B254f251518206f709E9B115f214ebDf17"),
            "FACTORY must no longer be the stale e7108cd reference value"
        );
    }

    /// (A) `merge_call` must produce the exact `CtfCollateralAdapter.
    /// mergePositions` calldata: a hand-computed keccak selector over the
    /// canonical signature, the protocol-fixed `parentCollectionId = 0x0` /
    /// `partition = [1, 2]`, and a clean round-trip of every arg via the
    /// generated decoder. `target` is the adapter; `value` is 0.
    #[test]
    fn merge_call_selector_and_args() {
        let adapter = CTF_COLLATERAL_ADAPTER;
        // Arbitrary non-zero collateral (USDC.e on 137); the real pUSD/USDC.e
        // address is reconciled in M6-6 — this test only pins the ENCODING.
        let collateral: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".parse().unwrap();
        let condition_id: B256 =
            "0xabcdef0000000000000000000000000000000000000000000000000000000123".parse().unwrap();
        let amount = U256::from(1_500_000u64);

        let call = merge_call(adapter, collateral, condition_id, amount);

        assert_eq!(call.target, adapter, "merge target must be the adapter");
        assert_eq!(call.value, U256::ZERO, "merge value must be 0");

        // Selector = keccak256(canonical signature)[..4], hand-computed.
        let selector = keccak256("mergePositions(address,bytes32,bytes32,uint256[],uint256)".as_bytes());
        assert_eq!(&call.data[..4], &selector[..4], "mergePositions selector mismatch");

        // Args round-trip through the generated decoder (also re-validates the
        // 4-byte selector, since `abi_decode` strips and checks it).
        let decoded = mergePositionsCall::abi_decode(&call.data).unwrap();
        assert_eq!(decoded.collateralToken, collateral);
        assert_eq!(decoded.parentCollectionId, B256::ZERO);
        assert_eq!(decoded.conditionId, condition_id);
        assert_eq!(decoded.partition, vec![U256::from(1), U256::from(2)]);
        assert_eq!(decoded.amount, amount);
    }

    /// (A) `redeem_call` mirror of [`merge_call_selector_and_args`] for
    /// `redeemPositions(address,bytes32,bytes32,uint256[])` with `indexSets =
    /// [1, 2]`. Uses the NegRisk adapter to prove `adapter` is a parameter.
    #[test]
    fn redeem_call_selector_and_args() {
        let adapter = NEGRISK_CTF_COLLATERAL_ADAPTER;
        let collateral: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".parse().unwrap();
        let condition_id: B256 =
            "0x00000000000000000000000000000000000000000000000000000000deadbeef".parse().unwrap();

        let call = redeem_call(adapter, collateral, condition_id);

        assert_eq!(call.target, adapter, "redeem target must be the adapter");
        assert_eq!(call.value, U256::ZERO, "redeem value must be 0");

        let selector = keccak256("redeemPositions(address,bytes32,bytes32,uint256[])".as_bytes());
        assert_eq!(&call.data[..4], &selector[..4], "redeemPositions selector mismatch");

        let decoded = redeemPositionsCall::abi_decode(&call.data).unwrap();
        assert_eq!(decoded.collateralToken, collateral);
        assert_eq!(decoded.parentCollectionId, B256::ZERO);
        assert_eq!(decoded.conditionId, condition_id);
        assert_eq!(decoded.indexSets, vec![U256::from(1), U256::from(2)]);
    }

    /// (B) THE M6-3 derivation GATE: reproduce Polymarket's deposit-wallet
    /// CREATE2/ERC-1967 golden vector byte-for-byte (from py
    /// `tests/builder/test_derive.py`). The vector uses the Amoy factory/impl,
    /// which is fine — it validates the ALGORITHM (keccak owner → wallet_id →
    /// abi-encoded args → salt + ERC-1967 init-code hash → CREATE2). A mismatch
    /// means the derivation is wrong; debug until byte-exact, do not weaken.
    #[test]
    fn derive_deposit_wallet_matches_golden_vector() {
        let owner: Address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".parse().unwrap();
        let factory: Address = "0x801c740Bcd28531d75a5da176D5511F3329Ab049".parse().unwrap();
        let implementation: Address = "0x24f3257BF9451bA575E864777ab6f8D7Eac0139B".parse().unwrap();
        let wallet = derive_deposit_wallet(owner, factory, implementation);
        assert_eq!(
            wallet,
            "0x63cB1B4eC2F274Ed553aD5079c6A2542d1c02bd7".parse::<Address>().unwrap(),
            "deposit-wallet derivation diverged from the Polymarket golden vector"
        );
    }

    // -- (C) Relayer I/O (M6-4) --------------------------------------------

    /// `builder_headers` mirrors `auth.rs::l2_headers_carry_all_five`: the four
    /// `POLY_BUILDER_*` names in order, key/timestamp/passphrase passthrough,
    /// and — crucially — the signature REUSES the CLOB L2 HMAC scheme. We feed
    /// the SAME inputs as the pinned L2 vector (`auth_vectors.json` l2[0]:
    /// secret `QQ==`, POST `/order`, body `{"hello":"world"}`) and assert the
    /// builder signature is byte-identical to that vector — proving we did not
    /// reimplement (or drift from) `l2_signature`.
    #[test]
    fn builder_headers_carry_all_four_and_reuse_l2_hmac() {
        let creds = BuilderCreds {
            key: "703629aa-builder-key".into(),
            secret: crate::secrets::Secret::new("QQ==".into()),
            passphrase: crate::secrets::Secret::new("builder-pass".into()),
        };
        let h = builder_headers(
            &creds,
            "1750000000",
            "POST",
            "/order",
            Some("{\"hello\":\"world\"}"),
        )
        .unwrap();

        let names: Vec<&str> = h.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "POLY_BUILDER_API_KEY",
                "POLY_BUILDER_TIMESTAMP",
                "POLY_BUILDER_PASSPHRASE",
                "POLY_BUILDER_SIGNATURE",
            ]
        );
        assert_eq!(h[0].1, "703629aa-builder-key", "API key passthrough");
        assert_eq!(h[1].1, "1750000000", "timestamp passthrough");
        assert_eq!(h[2].1, "builder-pass", "passphrase passthrough");
        // Same HMAC as the CLOB L2 (auth_vectors.json l2[0]) — REUSE, not reimpl.
        assert_eq!(
            h[3].1, "rL5wbSueMIhsnLDR0rvOx2jaeW5-YHxY5zfKwMrZtQY=",
            "builder signature must equal the pinned L2 HMAC for identical inputs"
        );
    }

    /// An invalid (non-base64url) builder secret surfaces as `RelayerError::Auth`
    /// (mapped from `auth::AuthError`), never a panic.
    #[test]
    fn builder_headers_bad_secret_is_auth_error() {
        let creds = BuilderCreds {
            key: "k".into(),
            // '!' is not a valid base64url char → l2_signature's decode fails.
            secret: crate::secrets::Secret::new("not valid base64!!".into()),
            passphrase: crate::secrets::Secret::new("p".into()),
        };
        let err = builder_headers(&creds, "1", "POST", "/submit", Some("{}")).unwrap_err();
        assert!(matches!(err, RelayerError::Auth(_)), "got {err:?}");
    }

    /// `build_wallet_request` matches the py client `to_dict()` shape (design
    /// §2.2 / `test_client_deposit_wallet.py`): exact top-level + nested keys,
    /// `type` "WALLET", string `nonce`/`deadline`/`value`, `to` = factory, and a
    /// single `{target,value,data}` call carrying the `mergePositions` calldata.
    #[test]
    fn build_wallet_request_matches_py_to_dict_shape() {
        let from: Address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".parse().unwrap();
        let wallet: Address = "0xa2927E7834648F1C03b4961CeeA4597292e3c025".parse().unwrap();
        let collateral: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".parse().unwrap();
        let condition_id: B256 =
            "0xabcdef0000000000000000000000000000000000000000000000000000000123".parse().unwrap();
        let calls = vec![merge_call(
            CTF_COLLATERAL_ADAPTER,
            collateral,
            condition_id,
            U256::from(1_500_000u64),
        )];
        // Reuse the golden-vector signature string — it is passed through verbatim.
        let sig = "0x7827946c566e7860f6c5f2e641587ed6928989c8618e463a00dd56832e7300023b7436c67a2ea82d6d506b1a5eda3e27526e9e2ffaad52128d75c47c2e9d1fac1b";

        let req = build_wallet_request(
            from,
            DEPOSIT_WALLET_FACTORY,
            wallet,
            7u64,
            1234567890u64,
            &calls,
            sig,
        );

        // Top-level shape + EXACT key set.
        let mut keys: Vec<&str> = req.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["depositWalletParams", "from", "nonce", "signature", "to", "type"]
        );
        assert_eq!(req["type"], "WALLET");
        // Addresses are EIP-55 checksummed (alloy Display) — pin the literals.
        assert_eq!(req["from"], "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
        assert_eq!(
            req["to"], "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07",
            "`to` is the (current) deposit-wallet factory"
        );
        // nonce is a STRING, not a JSON number.
        assert_eq!(req["nonce"], "7");
        assert!(req["nonce"].is_string(), "nonce must be a string");
        assert_eq!(req["signature"], sig);

        // Nested depositWalletParams shape + EXACT key set.
        let p = &req["depositWalletParams"];
        let mut pkeys: Vec<&str> = p.as_object().unwrap().keys().map(String::as_str).collect();
        pkeys.sort_unstable();
        assert_eq!(pkeys, vec!["calls", "deadline", "depositWallet"]);
        assert_eq!(p["depositWallet"], "0xa2927E7834648F1C03b4961CeeA4597292e3c025");
        assert_eq!(p["deadline"], "1234567890");
        assert!(p["deadline"].is_string(), "deadline must be a string");

        // calls[0] shape: {target, value, data}, EXACT key set, value "0" string.
        let calls_arr = p["calls"].as_array().unwrap();
        assert_eq!(calls_arr.len(), 1);
        let mut ckeys: Vec<&str> =
            calls_arr[0].as_object().unwrap().keys().map(String::as_str).collect();
        ckeys.sort_unstable();
        assert_eq!(ckeys, vec!["data", "target", "value"]);
        assert_eq!(
            calls_arr[0]["target"], "0xAdA100Db00Ca00073811820692005400218FcE1f",
            "target is the CtfCollateralAdapter"
        );
        assert_eq!(calls_arr[0]["value"], "0", "value is the string \"0\"");
        let data_str = calls_arr[0]["data"].as_str().unwrap();
        assert!(data_str.starts_with("0x"), "data is 0x-hex: {data_str}");
        // data is exactly the ABI-encoded mergePositions calldata for the Call.
        assert_eq!(data_str, format!("0x{}", hex::encode(&calls[0].data[..])));
        // …and that calldata carries the mergePositions selector (sanity).
        let selector =
            keccak256("mergePositions(address,bytes32,bytes32,uint256[],uint256)".as_bytes());
        assert_eq!(&calls[0].data[..4], &selector[..4]);
    }

    /// State parse + `is_terminal`/`is_success` over all six confirmed states
    /// plus the `Other` fallback (the testable core of the poll loop).
    #[test]
    fn relayer_state_parse_terminal_and_success() {
        // Parse the canonical strings.
        assert_eq!(RelayerState::parse("STATE_NEW"), RelayerState::New);
        assert_eq!(RelayerState::parse("STATE_EXECUTED"), RelayerState::Executed);
        assert_eq!(RelayerState::parse("STATE_MINED"), RelayerState::Mined);
        assert_eq!(RelayerState::parse("STATE_CONFIRMED"), RelayerState::Confirmed);
        assert_eq!(RelayerState::parse("STATE_FAILED"), RelayerState::Failed);
        assert_eq!(RelayerState::parse("STATE_INVALID"), RelayerState::Invalid);
        assert_eq!(
            RelayerState::parse("STATE_WHATEVER"),
            RelayerState::Other("STATE_WHATEVER".to_string())
        );

        // Non-terminal: NEW/EXECUTED/MINED keep polling (MINED is NOT enough).
        for s in [RelayerState::New, RelayerState::Executed, RelayerState::Mined] {
            assert!(!s.is_terminal(), "{s:?} must be non-terminal");
            assert!(!s.is_success());
        }
        // Terminal OK.
        assert!(RelayerState::Confirmed.is_terminal());
        assert!(RelayerState::Confirmed.is_success());
        // Terminal ERROR.
        for s in [RelayerState::Failed, RelayerState::Invalid] {
            assert!(s.is_terminal(), "{s:?} must be terminal");
            assert!(!s.is_success(), "{s:?} must not be success");
        }
        // Unknown → non-terminal, non-success (poll to timeout, never mis-judge).
        let other = RelayerState::Other("x".to_string());
        assert!(!other.is_terminal());
        assert!(!other.is_success());
    }

    /// The poll-loop decision logic exercised through the factored parser on
    /// mocked relayer JSON — no HTTP. CONFIRMED → success+hash; FAILED/INVALID →
    /// terminal error; intermediate → keep polling; missing fields handled.
    #[test]
    fn parse_transaction_response_drives_state_machine() {
        // STATE_CONFIRMED with a hash → success, hash extracted.
        let (state, hash) = parse_transaction_response(&serde_json::json!({
            "state": "STATE_CONFIRMED",
            "transactionHash": "0xdeadbeef"
        }));
        assert!(state.is_success());
        assert_eq!(hash.as_deref(), Some("0xdeadbeef"));

        // Intermediate STATE_NEW, no hash yet → non-terminal, keep polling.
        let (state, hash) = parse_transaction_response(&serde_json::json!({"state": "STATE_NEW"}));
        assert_eq!(state, RelayerState::New);
        assert!(!state.is_terminal());
        assert!(hash.is_none());

        // STATE_MINED is NOT terminal (design requires CONFIRMED).
        let (state, _) = parse_transaction_response(&serde_json::json!({"state": "STATE_MINED"}));
        assert!(!state.is_terminal(), "MINED must keep polling, not stop");

        // STATE_FAILED → terminal, not success (poll returns Err(Failed)).
        let (state, _) = parse_transaction_response(&serde_json::json!({"state": "STATE_FAILED"}));
        assert!(state.is_terminal() && !state.is_success());

        // STATE_INVALID → terminal, not success.
        let (state, _) =
            parse_transaction_response(&serde_json::json!({"state": "STATE_INVALID"}));
        assert!(state.is_terminal() && !state.is_success());

        // Missing `state` field → Other("") → non-terminal (keep polling).
        let (state, _) = parse_transaction_response(&serde_json::json!({"foo": "bar"}));
        assert_eq!(state, RelayerState::Other(String::new()));
        assert!(!state.is_terminal());
    }

    /// `parse_submit_response` pulls `transactionID` (with a `transactionId`
    /// casing fallback) and yields `None` when absent — submit parsing without
    /// HTTP.
    #[test]
    fn parse_submit_response_extracts_transaction_id() {
        assert_eq!(
            parse_submit_response(&serde_json::json!({
                "transactionID": "tx-abc-123",
                "state": "STATE_NEW"
            }))
            .as_deref(),
            Some("tx-abc-123")
        );
        // Casing fallback.
        assert_eq!(
            parse_submit_response(&serde_json::json!({"transactionId": "tx-xyz"})).as_deref(),
            Some("tx-xyz")
        );
        // Missing → None (submit_wallet_batch maps this to RelayerError::Response).
        assert!(parse_submit_response(&serde_json::json!({"state": "STATE_NEW"})).is_none());
    }

    // -- (D) relayer WALLET nonce ------------------------------------------

    /// `parse_nonce` accepts the relayer's number OR string nonce, and maps every
    /// other shape (missing / wrong-typed / non-numeric / negative) to a
    /// `RelayerError::Response` — never a panic. No HTTP.
    #[test]
    fn parse_nonce_accepts_number_or_string() {
        // JSON number.
        assert_eq!(parse_nonce(&serde_json::json!({"nonce": 5})).unwrap(), 5);
        assert_eq!(parse_nonce(&serde_json::json!({"nonce": 0})).unwrap(), 0);
        // Decimal string (the relayer may return either form).
        assert_eq!(parse_nonce(&serde_json::json!({"nonce": "42"})).unwrap(), 42);
        // A full-range nonce as a STRING still parses (the string form is exactly
        // why we accept it — JSON numbers lose precision past 2^53 in some stacks).
        assert_eq!(
            parse_nonce(&serde_json::json!({"nonce": "18446744073709551615"})).unwrap(),
            u64::MAX
        );
        // Missing / non-numeric string / wrong type / negative → Response error.
        for bad in [
            serde_json::json!({}),
            serde_json::json!({"nonce": "abc"}),
            serde_json::json!({"nonce": true}),
            serde_json::json!({"nonce": -1}),
        ] {
            assert!(
                matches!(parse_nonce(&bad), Err(RelayerError::Response(_))),
                "unparseable nonce must be a Response error, got {:?}",
                parse_nonce(&bad)
            );
        }
    }

    // -- (E) RelayerClient (M6-6) ------------------------------------------

    /// A valid EOA key (the public anvil key) that parses to a `PrivateKeySigner`.
    const TEST_PK: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    /// A deposit-wallet address fixture (the golden-vector wallet; canonical EIP-55).
    const TEST_WALLET: &str = "0xa2927E7834648F1C03b4961CeeA4597292e3c025";

    /// Build `LiveSecrets` through the REAL env loader, toggling whether the
    /// builder creds / deposit wallet are present so the gating paths are
    /// exercised end-to-end (`PM_PRIVATE_KEY` is always present — the loader
    /// requires it).
    fn test_secrets(builder: bool, deposit_wallet: bool) -> LiveSecrets {
        LiveSecrets::from_lookup(|k| match k {
            "PM_PRIVATE_KEY" => Some(TEST_PK.to_string()),
            "PM_DEPOSIT_WALLET" if deposit_wallet => Some(TEST_WALLET.to_string()),
            "BUILDER_API_KEY" if builder => Some("703629aa-builder-key".to_string()),
            "BUILDER_SECRET" if builder => Some("QQ==".to_string()),
            "BUILDER_PASS_PHRASE" if builder => Some("builder-pass".to_string()),
            _ => None,
        })
        .unwrap()
    }

    /// A `[live]` config with the relayer knobs set; all other fields default.
    /// Struct-update from `Live::default()` (avoids `field_reassign_with_default`).
    fn live_cfg(enabled: bool, staging: bool, url: Option<&str>) -> Live {
        Live {
            relayer_enabled: enabled,
            relayer_staging: staging,
            relayer_url: url.map(str::to_string),
            ..Live::default()
        }
    }

    /// Gating: `None` unless enabled AND builder creds AND deposit wallet present;
    /// when `Some`, the URL follows the override > staging-vs-prod rule and the
    /// factory/adapter/collateral/chain defaults are the pinned consts. No HTTP
    /// (constructing a `reqwest::Client` needs no runtime).
    #[test]
    fn relayer_client_new_gating() {
        let http = reqwest::Client::new();

        // Disabled → None, even with full creds.
        assert!(
            RelayerClient::new(&live_cfg(false, true, None), &test_secrets(true, true), http.clone())
                .is_none(),
            "relayer must not be constructed when disabled"
        );
        // Enabled but builder creds absent → None.
        assert!(
            RelayerClient::new(&live_cfg(true, true, None), &test_secrets(false, true), http.clone())
                .is_none(),
            "no builder creds → None"
        );
        // Enabled + creds but deposit wallet absent → None.
        assert!(
            RelayerClient::new(&live_cfg(true, true, None), &test_secrets(true, false), http.clone())
                .is_none(),
            "no deposit wallet → None"
        );

        // Enabled + creds + wallet → Some, staging URL + the const defaults.
        let c = RelayerClient::new(&live_cfg(true, true, None), &test_secrets(true, true), http.clone())
            .unwrap();
        assert_eq!(c.relayer_url, RELAYER_URL_STAGING, "staging-first by default");
        assert_eq!(c.factory, DEPOSIT_WALLET_FACTORY);
        assert_eq!(c.adapter, CTF_COLLATERAL_ADAPTER);
        assert_eq!(c.collateral, USDC_E_COLLATERAL);
        assert_eq!(c.chain_id, 137, "Polygon mainnet chain id");
        assert_eq!(c.wallet, TEST_WALLET.parse::<Address>().unwrap());
        // owner() derives the EOA from the pk deterministically (no panic).
        assert_eq!(c.owner().unwrap(), c.owner().unwrap());

        // staging = false → prod URL.
        let c =
            RelayerClient::new(&live_cfg(true, false, None), &test_secrets(true, true), http.clone())
                .unwrap();
        assert_eq!(c.relayer_url, RELAYER_URL_PROD, "staging=false → prod URL");

        // Explicit override wins over the staging flag.
        let c = RelayerClient::new(
            &live_cfg(true, true, Some("https://relayer.example")),
            &test_secrets(true, true),
            http,
        )
        .unwrap();
        assert_eq!(c.relayer_url, "https://relayer.example", "explicit URL override wins");
    }

    /// `build_merge_batch` assembles a WALLET request (no HTTP) whose body is the
    /// `to=factory`, `depositWallet=wallet`, single-call `target=adapter`
    /// `mergePositions` batch with a 132-char `0x` signature.
    #[test]
    fn build_merge_batch_produces_wallet_request_with_merge_calldata() {
        let c = RelayerClient::new(
            &live_cfg(true, true, None),
            &test_secrets(true, true),
            reqwest::Client::new(),
        )
        .unwrap();
        let condition_id: B256 =
            "0xabcdef0000000000000000000000000000000000000000000000000000000123".parse().unwrap();
        let body = c
            .build_merge_batch(condition_id, U256::from(1_500_000u64), 7, 1234567890)
            .unwrap();

        assert_eq!(body["type"], "WALLET");
        assert_eq!(body["to"], DEPOSIT_WALLET_FACTORY.to_string(), "`to` is the factory");
        assert_eq!(body["from"], c.owner().unwrap().to_string(), "`from` is the owner EOA");
        assert_eq!(body["nonce"], "7");
        let p = &body["depositWalletParams"];
        assert_eq!(p["depositWallet"], TEST_WALLET, "depositWallet is the wallet");
        assert_eq!(p["deadline"], "1234567890");
        let calls = p["calls"].as_array().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0]["target"],
            CTF_COLLATERAL_ADAPTER.to_string(),
            "calls[0].target is the adapter"
        );
        assert_eq!(calls[0]["value"], "0");
        // calls[0].data starts with the mergePositions selector.
        let data = calls[0]["data"].as_str().unwrap();
        assert!(data.starts_with("0x"), "data is 0x-hex: {data}");
        let selector =
            keccak256("mergePositions(address,bytes32,bytes32,uint256[],uint256)".as_bytes());
        assert_eq!(&data[2..10], hex::encode(&selector[..4]).as_str(), "mergePositions selector");
        // signature is a 132-char 0x string (0x + 65 bytes).
        let sig = body["signature"].as_str().unwrap();
        assert!(sig.starts_with("0x"), "signature is 0x-hex: {sig}");
        assert_eq!(sig.len(), 132, "0x + 65-byte signature");
    }

    /// `build_redeem_batch` mirror of the merge assembly test: the single call
    /// targets the adapter and carries the `redeemPositions` selector.
    #[test]
    fn build_redeem_batch_produces_wallet_request_with_redeem_calldata() {
        let c = RelayerClient::new(
            &live_cfg(true, false, None),
            &test_secrets(true, true),
            reqwest::Client::new(),
        )
        .unwrap();
        let condition_id: B256 =
            "0x00000000000000000000000000000000000000000000000000000000deadbeef".parse().unwrap();
        let body = c.build_redeem_batch(condition_id, 0, 1234567890).unwrap();

        assert_eq!(body["type"], "WALLET");
        assert_eq!(body["to"], DEPOSIT_WALLET_FACTORY.to_string());
        assert_eq!(body["nonce"], "0");
        let calls = body["depositWalletParams"]["calls"].as_array().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["target"], CTF_COLLATERAL_ADAPTER.to_string());
        assert_eq!(calls[0]["value"], "0");
        let data = calls[0]["data"].as_str().unwrap();
        let selector = keccak256("redeemPositions(address,bytes32,bytes32,uint256[])".as_bytes());
        assert_eq!(&data[2..10], hex::encode(&selector[..4]).as_str(), "redeemPositions selector");
        let sig = body["signature"].as_str().unwrap();
        assert_eq!(sig.len(), 132, "0x + 65-byte signature");
    }
}
