//! LiveVenue (spec §14, M5): the real Polymarket CLOB behind `ExecutionVenue`.
//!
//! FAK submit signs an EIP-712 order (`sign.rs`), serialises the RECON-pinned
//! wire body ONCE (used for both the L2 HMAC and the request body — they must
//! be byte-identical, see `auth.rs`), POSTs `/order`, and on `matched` polls
//! `GET /data/trades` for per-fill detail. All money math is integer-exact
//! (micro-units, no f64 in the money path); the decimal→micro parser below is
//! string→integer scaling.
//!
//! `shadow` mode signs and logs but performs NO network I/O (no limiter, no
//! POST) — the operator's pre-funding readiness check (Task 13).
//!
//! split/merge are on-chain ops deferred to M6; they return
//! `VenueError::NotSupportedLive`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use serde_json::json;
use tracing::info;

use pm_core::fees::fee_microusdc;
use pm_core::instrument::TokenId;
use pm_core::num::{Px, Qty, TickSize, Usdc, buy_cost, sell_proceeds};
use pm_engine::Action;

use crate::Order;
use crate::auth::l2_headers;
use crate::secrets::ApiCreds;
use crate::sign::{CHAIN_ID, ClobOrder, Side, clob_amounts, sign_order_1271};
use crate::venue::{ExecutionVenue, Fill, SubmitOutcome, VenueError};

// ---------------------------------------------------------------------------
// Rate limiter — token bucket
//
// Ported verbatim (logic) from `crates/ingestion/src/rest.rs` `TokenBucket` +
// `ClobRest::acquire`. Copied rather than shared to avoid a cross-crate
// dependency (execution must not depend on ingestion); keep the two in sync if
// either changes. Clock is `Instant::now()` in the async `acquire`; the pure
// `try_acquire` takes the clock so it stays testable.
// ---------------------------------------------------------------------------

enum Ready {
    Now,
    After(Duration),
}

struct RateLimiter {
    capacity: u32,
    tokens: f64,
    rate_per_sec: f64,
    last: Option<Instant>,
}

impl RateLimiter {
    fn new(capacity: u32, rate_per_sec: f64) -> Self {
        RateLimiter {
            capacity,
            tokens: f64::from(capacity),
            rate_per_sec,
            last: None,
        }
    }

    fn try_acquire(&mut self, now: Instant) -> Ready {
        if let Some(last) = self.last {
            let dt = now.saturating_duration_since(last).as_secs_f64();
            self.tokens = (self.tokens + dt * self.rate_per_sec).min(f64::from(self.capacity));
        }
        self.last = Some(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ready::Now
        } else {
            let need = 1.0 - self.tokens;
            Ready::After(Duration::from_secs_f64(need / self.rate_per_sec))
        }
    }

    async fn acquire(&mut self) {
        loop {
            match self.try_acquire(Instant::now()) {
                Ready::Now => return,
                Ready::After(d) => tokio::time::sleep(d).await,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Decimal → micro
//
// Minimal local copy of `crates/ingestion/src/decimal.rs::parse_micro` —
// execution cannot depend on ingestion. Exact string→integer scaling (×10⁶),
// never touches f64; the money path stays integer-exact. Returns None on any
// malformed / out-of-range input (caller maps to a VenueError).
// ---------------------------------------------------------------------------

fn decimal_to_micro(s: &str) -> Option<u64> {
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if frac_part.len() > 6 {
        return None;
    }
    let mut value: u64 = 0;
    if !int_part.is_empty() {
        if !int_part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        for b in int_part.bytes() {
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(b - b'0')))?;
        }
    }
    value = value.checked_mul(1_000_000)?;
    if !frac_part.is_empty() {
        if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut frac: u64 = 0;
        for b in frac_part.bytes() {
            frac = frac * 10 + u64::from(b - b'0'); // ≤ 6 digits: cannot overflow
        }
        frac *= 10u64.pow(6 - frac_part.len() as u32);
        value = value.checked_add(frac)?;
    }
    Some(value)
}

/// Convert a venue price decimal string ("0.33") to a tick `Px` for `ts`.
/// The price must be tick-aligned (µUSDC divisible by the tick unit) and an
/// interior tick — both hold for prices our engine ever trades.
fn px_from_decimal(s: &str, ts: TickSize) -> Option<Px> {
    let micro = decimal_to_micro(s)?;
    let unit = ts.unit_microusdc();
    if unit == 0 || micro % unit != 0 {
        return None;
    }
    let tick = micro / unit;
    if tick > u64::from(u16::MAX) {
        return None;
    }
    Px::new(tick as u16, ts).ok()
}

// ---------------------------------------------------------------------------
// Config + venue
// ---------------------------------------------------------------------------

/// Construction config for [`LiveVenue`].
pub struct LiveVenueCfg {
    pub base: String,
    pub creds: ApiCreds,
    pub signer: PrivateKeySigner,
    /// Funder / proxy wallet (legacy POLY_PROXY maker). Retained but no longer
    /// the maker under the V2 deposit-wallet flow — see `deposit_wallet`.
    pub proxy: Address,
    /// V2 deposit wallet: the order `maker` AND the ERC-7739 wallet-domain
    /// verifyingContract (signatureType 3 / POLY_1271, RECON-M5-V2-1271). The
    /// `signer` (EOA) signs; the deposit wallet is the on-chain maker.
    pub deposit_wallet: Address,
    /// How long to poll `/data/trades` for `matched` fills before giving up
    /// (the remainder is treated as killed — FAK semantics).
    pub fill_window: Duration,
    pub rate_per_sec: f64,
    pub rate_capacity: u32,
    /// When true: sign + log, never submit (no limiter, no network).
    pub shadow: bool,
}

/// The live CLOB venue. `submit_all` uses the trait default (sequential).
pub struct LiveVenue {
    http: reqwest::Client,
    cfg: LiveVenueCfg,
    /// token → (venue decimal token id, neg_risk).
    tokens: HashMap<TokenId, (String, bool)>,
    /// Order salt source. Default: SystemTime nanos XOR a bumped counter;
    /// overridable in tests for determinism. `FnMut` so the counter can bump.
    pub salt_src: Box<dyn FnMut() -> u64 + Send>,
    limiter: RateLimiter,
    /// One-shot: log the identity binding (maker/signer/owner/POLY_ADDRESS) on
    /// the FIRST submitted order so the operator run is decisive without
    /// spamming every order. Set false after the first submit.
    log_first_order: bool,
}

impl LiveVenue {
    /// Build a live venue. 10 s HTTP timeout (matches `ingestion::ClobRest`).
    pub fn new(cfg: LiveVenueCfg) -> Result<Self, VenueError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| VenueError::Live(format!("http client build failed: {e}")))?;
        let rate_capacity = cfg.rate_capacity;
        let rate_per_sec = cfg.rate_per_sec;
        Ok(LiveVenue {
            http,
            cfg,
            tokens: HashMap::new(),
            // No rand dependency: SystemTime nanos give wall-clock entropy and a
            // monotonic counter guarantees uniqueness within a process even if
            // two orders land in the same nanosecond. The salt only needs to be
            // unique per order (replay/idempotency), not cryptographically random.
            salt_src: Box::new(default_salt),
            limiter: RateLimiter::new(rate_capacity, rate_per_sec),
            log_first_order: true,
        })
    }

    /// Register a token's venue decimal id and neg-risk flag. Orders for an
    /// unregistered token are rejected before any I/O.
    pub fn register_token(&mut self, token: TokenId, venue_id: String, neg_risk: bool) {
        self.tokens.insert(token, (venue_id, neg_risk));
    }

    /// Walk a `GET /data/{kind}` cursor-paginated endpoint, returning raw rows.
    /// `extra` is appended to the query after the cursor (e.g. order filter).
    async fn data_rows(
        &mut self,
        kind: &str,
        extra: &str,
    ) -> Result<Vec<serde_json::Value>, VenueError> {
        const START: &str = "MA==";
        const END: &str = "LTE=";
        const MAX_PAGES: usize = 200;
        let path = format!("/data/{kind}");
        // L2 POLY_ADDRESS must equal the API key's bound address — the deposit
        // wallet under the V2 POLY_1271 flow (RECON-M5-V2-1271 "Auth binding";
        // clob-client-v2 #65: "the same change is needed in createL2Headers").
        let auth_address = self.cfg.deposit_wallet.to_string();
        let mut cursor = START.to_string();
        let mut out = Vec::new();
        for _ in 0..MAX_PAGES {
            self.limiter.acquire().await;
            let ts = unix_seconds_string();
            // HMAC signs the query-LESS path (RECON-M5); query is appended after.
            let headers = l2_headers(&self.cfg.creds, &auth_address, &ts, "GET", &path, None)
                .map_err(|e| VenueError::Live(e.to_string()))?;
            let url = format!(
                "{}{}?next_cursor={}{}",
                self.cfg.base, path, cursor, extra
            );
            let mut req = self.http.get(&url);
            for (k, v) in &headers {
                req = req.header(*k, v);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| VenueError::Live(e.to_string()))?;
            let status = resp.status();
            let body = resp
                .text()
                .await
                .map_err(|e| VenueError::Live(e.to_string()))?;
            if !status.is_success() {
                return Err(VenueError::Live(format!("{status}: {body}")));
            }
            let page: serde_json::Value =
                serde_json::from_str(&body).map_err(|e| VenueError::Live(e.to_string()))?;
            if let Some(rows) = page.get("data").and_then(|d| d.as_array()) {
                out.extend(rows.iter().cloned());
            }
            let next = page
                .get("next_cursor")
                .and_then(|c| c.as_str())
                .unwrap_or(END);
            if next == END || next.is_empty() {
                return Ok(out);
            }
            cursor = next.to_string();
        }
        Err(VenueError::Live("trades pagination runaway".into()))
    }

    /// Poll `/data/trades` for fills of `venue_order_id` on `venue_token`,
    /// converting each row to a [`Fill`] with the same money helpers as
    /// `PaperVenue::fill_now`. Stops when accumulated µshares reach
    /// `target_shares_micro` or `fill_window` elapses.
    async fn poll_fills(
        &mut self,
        order: &Order,
        venue_order_id: &str,
        venue_token: &str,
        target_shares_micro: u64,
    ) -> Result<(Vec<Fill>, u64), VenueError> {
        let deadline = Instant::now() + self.cfg.fill_window;
        loop {
            let rows = self.data_rows("trades", "").await?;
            let mut fills = Vec::new();
            let mut filled_shares: u64 = 0;
            for row in &rows {
                let taker = row.get("taker_order_id").and_then(|v| v.as_str());
                let asset = row.get("asset_id").and_then(|v| v.as_str());
                if taker != Some(venue_order_id) || asset != Some(venue_token) {
                    continue;
                }
                let size_s = row
                    .get("size")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| VenueError::Live("trade row missing size".into()))?;
                let price_s = row
                    .get("price")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| VenueError::Live("trade row missing price".into()))?;
                let qty_micro = decimal_to_micro(size_s)
                    .ok_or_else(|| VenueError::Live(format!("bad trade size: {size_s}")))?;
                let px = px_from_decimal(price_s, order.ts)
                    .ok_or_else(|| VenueError::Live(format!("bad/unaligned trade price: {price_s}")))?;
                let px_micro = px.microusdc(order.ts);
                let qty = Qty(qty_micro);
                // Same fill math as PaperVenue::fill_now — against-us rounding.
                let fee = fee_microusdc(order.fee_bps, px_micro, qty);
                let cash = match order.action {
                    Action::Buy => Usdc(-(buy_cost(px_micro, qty).0 + fee.0)),
                    Action::Sell => Usdc(sell_proceeds(px_micro, qty).0 - fee.0),
                };
                fills.push(Fill {
                    px,
                    qty,
                    cash,
                    fee,
                });
                filled_shares = filled_shares.saturating_add(qty_micro);
            }
            // Done when the response covers the whole taking amount, or we ran
            // out of time. target 0 (unknown) → rely on the window only.
            if (target_shares_micro > 0 && filled_shares >= target_shares_micro)
                || Instant::now() >= deadline
            {
                return Ok((fills, filled_shares));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Raw open orders (`GET /data/orders`, cursor walk). Used by main's startup
    /// sweep / canary reconciliation.
    pub async fn open_orders(&mut self) -> Result<Vec<serde_json::Value>, VenueError> {
        self.data_rows("orders", "").await
    }

    /// Raw recent trades (`GET /data/trades`, cursor walk).
    pub async fn recent_trades(&mut self) -> Result<Vec<serde_json::Value>, VenueError> {
        self.data_rows("trades", "").await
    }
}

/// Default salt: wall-clock nanos XOR a process-monotonic counter.
fn default_salt() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Unix seconds as a decimal string (the L2 timestamp).
fn unix_seconds_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        .to_string()
}

impl ExecutionVenue for LiveVenue {
    async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
        // Unknown token → reject BEFORE any signing or I/O.
        let (venue_token, neg_risk) = self
            .tokens
            .get(&order.token)
            .cloned()
            .ok_or_else(|| VenueError::Live(format!("unregistered token {}", order.token.0)))?;

        let (maker_amount, taker_amount) =
            clob_amounts(order.action, order.ts, order.limit_px, order.qty);
        let side = match order.action {
            Action::Buy => Side::Buy,
            Action::Sell => Side::Sell,
        };
        // V2 signed struct (RECON-M5-V2): timestamp in ms; metadata/builder zero.
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // V2 deposit-wallet flow (RECON-M5-V2-1271): for a deposit-wallet order
        // the `maker` AND `signer` FIELDS are both the deposit wallet (the
        // reference vector has maker==signer==deposit wallet, and the live venue
        // requires "the order signer address has to be the address of the API
        // KEY" — which binds to the deposit-wallet account). The actual ECDSA is
        // produced by the EOA key (`self.cfg.signer`) inside the ERC-7739 /
        // POLY_1271 wrap (signatureType 3); the EOA is NOT the order signer field.
        let clob_order = ClobOrder {
            salt: (self.salt_src)(),
            maker: self.cfg.deposit_wallet,
            signer: self.cfg.deposit_wallet,
            token_id: venue_token.clone(),
            maker_amount,
            taker_amount,
            side,
            signature_type: 3,
            timestamp,
            metadata: [0u8; 32].into(),
            builder: [0u8; 32].into(),
        };
        let signature =
            sign_order_1271(&self.cfg.signer, &clob_order, neg_risk, CHAIN_ID, self.cfg.deposit_wallet)
                .map_err(|e| VenueError::Live(e.to_string()))?;

        // Build the V2 wire body ONCE (RECON-M5-V2 order_to_json_v2): salt &
        // signatureType are JSON NUMBERS, every other field a STRING; side is
        // "BUY"/"SELL"; metadata/builder are 0x-prefixed 32-byte hex; new
        // top-level `deferExec`. V2 drops taker/nonce/feeRateBps from the wire;
        // `expiration` stays on the wire ("0") though it is NOT in the signed
        // struct. serde_json::to_string is compact, matching the V2 client.
        let side_str = match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        let body_value = json!({
            "order": {
                "salt": clob_order.salt,
                "maker": format!("{:#x}", clob_order.maker),
                "signer": format!("{:#x}", clob_order.signer),
                "tokenId": clob_order.token_id,
                "makerAmount": clob_order.maker_amount.to_string(),
                "takerAmount": clob_order.taker_amount.to_string(),
                "side": side_str,
                "expiration": "0",
                "signatureType": clob_order.signature_type,
                "timestamp": clob_order.timestamp.to_string(),
                "metadata": format!("{:#x}", clob_order.metadata),
                "builder": format!("{:#x}", clob_order.builder),
                "signature": signature,
            },
            "owner": self.cfg.creds.key,
            "orderType": "FAK",
            "deferExec": false,
            "postOnly": false,
        });
        let body = serde_json::to_string(&body_value)
            .map_err(|e| VenueError::Live(e.to_string()))?;

        // First-order diagnostic (no secrets — addresses + api-key id only).
        // RECON-M5-V2-1271 "Auth binding": maker, signer, and the L2
        // POLY_ADDRESS must ALL equal the deposit wallet (the API key's bound
        // address); owner is the api-key id. One line, then silenced.
        if self.log_first_order {
            self.log_first_order = false;
            info!(
                maker = %format!("{:#x}", clob_order.maker),
                signer = %format!("{:#x}", clob_order.signer),
                owner = %self.cfg.creds.key,
                l2_poly_address = %format!("{:#x}", self.cfg.deposit_wallet),
                eoa = %self.cfg.signer.address(),
                "first live order identity binding (maker/signer/POLY_ADDRESS must == deposit wallet)"
            );
        }

        // SHADOW: signed, never submitted. No limiter, no network. Return a
        // zero-fill outcome with no venue id (nothing was placed).
        if self.cfg.shadow {
            info!(
                order = %order.id,
                token = %venue_token,
                side = ?side,
                limit_ticks = order.limit_px.get(),
                qty_micro = order.qty.0,
                "SHADOW: signed, not submitted"
            );
            return Ok(SubmitOutcome::default());
        }

        // The L2 HMAC must sign the EXACT wire body string. POLY_ADDRESS = the
        // deposit wallet (the API key's bound address — RECON-M5-V2-1271).
        self.limiter.acquire().await;
        let ts = unix_seconds_string();
        let auth_address = self.cfg.deposit_wallet.to_string();
        let headers = l2_headers(&self.cfg.creds, &auth_address, &ts, "POST", "/order", Some(&body))
            .map_err(|e| VenueError::Live(e.to_string()))?;
        let url = format!("{}/order", self.cfg.base);
        let mut req = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body);
        for (k, v) in &headers {
            req = req.header(*k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| VenueError::Live(e.to_string()))?;
        let status = resp.status();
        let resp_body = resp
            .text()
            .await
            .map_err(|e| VenueError::Live(e.to_string()))?;
        if !status.is_success() {
            return Err(VenueError::Live(format!("{status}: {resp_body}")));
        }
        let parsed: serde_json::Value =
            serde_json::from_str(&resp_body).map_err(|e| VenueError::Live(e.to_string()))?;
        // HTTP 200 with success:false is a processing failure.
        if parsed.get("success").and_then(|v| v.as_bool()) != Some(true) {
            let msg = parsed
                .get("errorMsg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown processing failure");
            return Err(VenueError::Live(msg.to_string()));
        }
        let venue_order_id = parsed
            .get("orderID")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let venue_status = parsed.get("status").and_then(|v| v.as_str()).unwrap_or("");

        match venue_status {
            "matched" => {
                // The order response's takingAmount/makingAmount are RAW µ-unit
                // integer strings (matched fixture: takingAmount "10000000" =
                // 10e6 µshares for a 10-share BUY taker; makingAmount "3300000" =
                // 3.3e6 µUSDC) — NOT decimals, so parse as plain u64, do NOT run
                // through decimal_to_micro (which would scale by 1e6 again).
                // For a BUY taker the shares-received is `takingAmount`; for a
                // SELL taker the shares-given is `makingAmount`. Use it as the
                // early-exit target (µshares); the fill window is the hard stop.
                let target_field = match order.action {
                    Action::Buy => "takingAmount",
                    Action::Sell => "makingAmount",
                };
                let target = parsed
                    .get(target_field)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or_else(|| {
                        tracing::warn!(
                            field = target_field,
                            "matched response had unparseable fill target; polling on window only"
                        );
                        0
                    });
                let (fills, filled_shares) = self
                    .poll_fills(order, &venue_order_id, &venue_token, target)
                    .await?;
                Ok(SubmitOutcome {
                    fills,
                    filled: Qty(filled_shares),
                    venue_order_id: Some(venue_order_id),
                })
            }
            // "unmatched", or "delayed"/"live" still unfilled at window end:
            // zero fills, but the order WAS accepted (id present).
            _ => Ok(SubmitOutcome {
                fills: Vec::new(),
                filled: Qty(0),
                venue_order_id: Some(venue_order_id),
            }),
        }
    }

    async fn split(
        &mut self,
        _market: pm_core::instrument::MarketId,
        _units: Qty,
    ) -> Result<Usdc, VenueError> {
        Err(VenueError::NotSupportedLive(
            "on-chain ops deferred to M6 (pure-buy live)",
        ))
    }

    async fn merge(
        &mut self,
        _market: pm_core::instrument::MarketId,
        _units: Qty,
    ) -> Result<Usdc, VenueError> {
        Err(VenueError::NotSupportedLive(
            "on-chain ops deferred to M6 (pure-buy live)",
        ))
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::instrument::MarketId;
    use pm_core::num::Bps;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // -- one-shot HTTP/1.1 mock CLOB ---------------------------------------
    //
    // Binds 127.0.0.1:0 (race-free: caller reads the assigned port), serves the
    // scripted (status, body) responses in order, records each raw request, and
    // counts hits. No new deps — hand-rolled HTTP/1.1 over a TcpListener.

    struct MockHandle {
        addr: std::net::SocketAddr,
        requests: Arc<tokio::sync::Mutex<Vec<String>>>,
        hits: Arc<AtomicUsize>,
    }

    /// Read exactly one HTTP/1.1 request (head + Content-Length body) from
    /// `sock`. Returns the raw request string, or None if the peer closed
    /// before sending a full head.
    async fn read_one_request(sock: &mut tokio::net::TcpStream) -> Option<String> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        let mut head_end = None;
        while head_end.is_none() {
            let n = sock.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                return None; // peer closed mid-head
            }
            buf.extend_from_slice(&tmp[..n]);
            head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4);
        }
        let he = head_end?;
        let head = String::from_utf8_lossy(&buf[..he]).to_string();
        let clen = head
            .lines()
            .find_map(|l| {
                let l = l.to_ascii_lowercase();
                l.strip_prefix("content-length:")
                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
            })
            .unwrap_or(0);
        while buf.len() < he + clen {
            let n = sock.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        Some(String::from_utf8_lossy(&buf).to_string())
    }

    fn spawn_mock(script: Vec<(u16, String)>) -> MockHandle {
        // Bound synchronously so the address is ready before we build the venue.
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();
        let requests = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let hits = Arc::new(AtomicUsize::new(0));
        let req_w = requests.clone();
        let hits_w = hits.clone();
        // Shared cursor over the scripted responses: requests may arrive on a
        // reused keep-alive socket OR on fresh connections (reqwest's pool
        // decides), so each accepted connection is served in its own task and
        // all pull the next scripted response from this shared queue, in order.
        let script = Arc::new(tokio::sync::Mutex::new(script.into_iter()));
        tokio::spawn(async move {
            let listener = TcpListener::from_std(std_listener).unwrap();
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let req_w = req_w.clone();
                let hits_w = hits_w.clone();
                let script = script.clone();
                tokio::spawn(async move {
                    loop {
                        let Some(raw) = read_one_request(&mut sock).await else {
                            return; // connection closed
                        };
                        hits_w.fetch_add(1, Ordering::SeqCst);
                        req_w.lock().await.push(raw);
                        let next = script.lock().await.next();
                        let Some((status, body)) = next else {
                            return; // script exhausted
                        };
                        // Keep-alive (no Connection: close) so reqwest may reuse
                        // the socket for the follow-up GET /data/trades.
                        let resp = format!(
                            "HTTP/1.1 {status} X\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                        let _ = sock.flush().await;
                    }
                });
            }
        });
        MockHandle {
            addr,
            requests,
            hits,
        }
    }

    const MATCHED: &str = include_str!("../tests/fixtures/clob_responses/order_matched.json");
    const UNMATCHED: &str = include_str!("../tests/fixtures/clob_responses/order_unmatched.json");
    const PROC_ERR: &str =
        include_str!("../tests/fixtures/clob_responses/order_processing_error.json");
    const TRADES: &str = include_str!("../tests/fixtures/clob_responses/trades_for_order.json");

    fn test_venue(base: String, shadow: bool) -> LiveVenue {
        // 64 hex chars (no 0x): the throwaway "0xadad…ad" key.
        let signer: PrivateKeySigner = "ad".repeat(32).parse().unwrap();
        let proxy: Address = format!("0x{}", "11".repeat(20)).parse().unwrap();
        // Deposit wallet (the V2 maker) — distinct from proxy so the maker
        // assertion is meaningful.
        let deposit_wallet: Address = format!("0x{}", "22".repeat(20)).parse().unwrap();
        let creds = ApiCreds {
            key: "test-key".into(),
            secret: crate::secrets::Secret::new("QQ==".into()),
            passphrase: crate::secrets::Secret::new("pass".into()),
        };
        let cfg = LiveVenueCfg {
            base,
            creds,
            signer,
            proxy,
            deposit_wallet,
            fill_window: Duration::from_millis(200),
            rate_per_sec: 1000.0,
            rate_capacity: 100,
            shadow,
        };
        let mut v = LiveVenue::new(cfg).unwrap();
        v.salt_src = Box::new(|| 42);
        v.register_token(TokenId(7), "123456789".into(), false);
        v
    }

    // BUY at 0.33 (tick 33, Cent), 10 shares, fee 0 — matches the fixture trade
    // price so the fill math derives cleanly.
    fn order(qty_micro: u64) -> Order {
        Order::new(
            "fp".into(),
            TokenId(7),
            Action::Buy,
            TickSize::Cent,
            Px::new(33, TickSize::Cent).unwrap(),
            Qty(qty_micro),
            Bps(0),
        )
    }

    #[tokio::test]
    async fn submit_fak_matched_returns_fills() {
        let mock = spawn_mock(vec![
            (200, MATCHED.to_string()),
            (200, TRADES.to_string()),
        ]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let out = v.submit_fak(&order(10_000_000)).await.unwrap();

        // Fills derived from the trade fixture: 10 shares @ 0.33, fee 0.
        assert!(out.filled.0 > 0);
        let sum: u64 = out.fills.iter().map(|f| f.qty.0).sum();
        assert_eq!(sum, out.filled.0);
        assert_eq!(out.filled.0, 10_000_000, "size '10' shares → 10e6 µshares");
        assert_eq!(out.fills.len(), 1);
        assert_eq!(out.fills[0].px.get(), 33, "price '0.33' → 33 Cent ticks");
        // BUY cash is negative; fee_bps 0 → exactly -(buy_cost) = -3_300_000.
        assert!(out.fills[0].cash.0 < 0);
        assert_eq!(out.fills[0].cash.0, -3_300_000);
        assert_eq!(out.fills[0].fee.0, 0);
        assert_eq!(
            out.venue_order_id.as_deref(),
            Some("0x06bc63e346ed4ceddce9efd6b3af37c8f8f440c92fe7da6b2d0f9e4ccbc50c42")
        );

        let reqs = mock.requests.lock().await;
        assert!(reqs[0].starts_with("POST /order"), "first req: {}", reqs[0]);
        // hyper lowercases header names on the wire (HTTP/1.1 names are
        // case-insensitive); the L2 header IS present as `poly_api_key`.
        assert!(
            reqs[0].to_ascii_lowercase().contains("poly_api_key"),
            "POST carries the POLY_API_KEY auth header: {}",
            reqs[0]
        );
        // L2 POLY_ADDRESS == the DEPOSIT WALLET (0x2222…2222), NOT the EOA — the
        // API key binds to the deposit wallet (RECON-M5-V2-1271 "Auth binding").
        let lower = reqs[0].to_ascii_lowercase();
        assert!(
            lower.contains(&format!("poly_address: 0x{}", "22".repeat(20))),
            "L2 POLY_ADDRESS must be the deposit wallet: {}",
            reqs[0]
        );
        assert!(reqs[0].contains("\"orderType\":\"FAK\""));
        // salt is a JSON NUMBER (pinned to 42 by the test salt_src), not a string.
        assert!(reqs[0].contains("\"salt\":42"), "salt must be a number: {}", reqs[0]);
        // V2 deposit-wallet flow (RECON-M5-V2-1271): signatureType is 3
        // (POLY_1271), a JSON NUMBER; amounts are strings (RECON wire).
        assert!(reqs[0].contains("\"signatureType\":3"), "{}", reqs[0]);
        assert!(reqs[0].contains("\"makerAmount\":\"3300000\""), "{}", reqs[0]);
        // maker AND signer are BOTH the DEPOSIT WALLET (0x2222…2222), not the
        // proxy/EOA — the venue requires order.signer == the API-key (deposit
        // wallet) address; the EOA only produces the inner ERC-7739 ECDSA.
        assert!(
            reqs[0].contains(&format!("\"maker\":\"0x{}\"", "22".repeat(20))),
            "maker must be the deposit wallet: {}",
            reqs[0]
        );
        assert!(
            reqs[0].contains(&format!("\"signer\":\"0x{}\"", "22".repeat(20))),
            "signer field must be the deposit wallet (not the EOA): {}",
            reqs[0]
        );
        // The signature is the ERC-7739 wrapped form (innerSig 65 + appDomainSep
        // 32 + contentsHash 32 + contentsType 186 + 2-byte len = 317 bytes),
        // far longer than a plain 65-byte (132-hex-char) sigType-1 signature.
        let sig_hex = {
            let m = "\"signature\":\"0x";
            let start = reqs[0].find(m).unwrap() + m.len(); // signature field present
            let rest = &reqs[0][start..];
            let end = rest.find('"').unwrap(); // signature field closes
            &rest[..end]
        };
        assert!(
            sig_hex.len() > 132,
            "ERC-7739 wrapped sig hex (> 132 chars) expected, got {} chars",
            sig_hex.len()
        );
        // V2 wire shape (RECON-M5-V2): timestamp/metadata/builder/deferExec are
        // present; the V1-only fields taker/nonce/feeRateBps are GONE.
        assert!(reqs[0].contains("\"timestamp\":\""), "V2 timestamp (string) present: {}", reqs[0]);
        assert!(
            reqs[0].contains("\"metadata\":\"0x0000000000000000000000000000000000000000000000000000000000000000\""),
            "V2 metadata zero bytes32: {}",
            reqs[0]
        );
        assert!(
            reqs[0].contains("\"builder\":\"0x0000000000000000000000000000000000000000000000000000000000000000\""),
            "V2 builder zero bytes32: {}",
            reqs[0]
        );
        assert!(reqs[0].contains("\"deferExec\":false"), "V2 deferExec present: {}", reqs[0]);
        assert!(!reqs[0].contains("\"taker\""), "V2 drops taker: {}", reqs[0]);
        assert!(!reqs[0].contains("\"nonce\""), "V2 drops nonce: {}", reqs[0]);
        assert!(!reqs[0].contains("\"feeRateBps\""), "V2 drops feeRateBps: {}", reqs[0]);
        // second request hits /data/trades with a query string (the HMAC was
        // built from the query-LESS path "/data/trades" — see data_rows).
        assert!(reqs[1].starts_with("GET /data/trades"), "second req: {}", reqs[1]);
        let req_line = reqs[1].lines().next().unwrap_or("");
        assert!(req_line.contains('?'), "trades request carries a query: {req_line}");
    }

    #[tokio::test]
    async fn submit_fak_unmatched_is_zero_fill_not_error() {
        let mock = spawn_mock(vec![(200, UNMATCHED.to_string())]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let out = v.submit_fak(&order(10_000_000)).await.unwrap();
        assert_eq!(out.filled.0, 0);
        assert!(out.fills.is_empty());
        assert_eq!(
            out.venue_order_id.as_deref(),
            Some("0x07aa63e346ed4ceddce9efd6b3af37c8f8f440c92fe7da6b2d0f9e4ccbc50c43")
        );
    }

    #[tokio::test]
    async fn submit_fak_processing_failure_is_live_error() {
        let mock = spawn_mock(vec![(200, PROC_ERR.to_string())]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let err = v.submit_fak(&order(10_000_000)).await.unwrap_err();
        match err {
            VenueError::Live(msg) => assert!(msg.contains("not enough balance"), "{msg}"),
            other => panic!("expected Live, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn submit_fak_http_error_is_venue_error() {
        let mock = spawn_mock(vec![(400, r#"{"error":"invalid order"}"#.to_string())]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let err = v.submit_fak(&order(10_000_000)).await.unwrap_err();
        match err {
            VenueError::Live(msg) => assert!(msg.contains("invalid order"), "{msg}"),
            other => panic!("expected Live, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shadow_signs_but_never_submits() {
        // Empty script: any network hit would fail the connection. Shadow must
        // not touch the network at all.
        let mock = spawn_mock(vec![]);
        let mut v = test_venue(format!("http://{}", mock.addr), true);
        let out = v.submit_fak(&order(10_000_000)).await.unwrap();
        assert_eq!(out.filled.0, 0);
        assert!(out.fills.is_empty());
        assert!(out.venue_order_id.is_none());
        assert_eq!(mock.hits.load(Ordering::SeqCst), 0, "shadow made a network call");
    }

    #[tokio::test]
    async fn split_and_merge_are_not_supported_live() {
        let mut v = test_venue("http://127.0.0.1:1".into(), false);
        assert!(matches!(
            v.split(MarketId(0), Qty(1)).await,
            Err(VenueError::NotSupportedLive(_))
        ));
        assert!(matches!(
            v.merge(MarketId(0), Qty(1)).await,
            Err(VenueError::NotSupportedLive(_))
        ));
    }

    #[tokio::test]
    async fn unknown_token_is_an_error_before_any_network_call() {
        let mock = spawn_mock(vec![]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        // TokenId(99) is not registered.
        let mut o = order(10_000_000);
        o.token = TokenId(99);
        let err = v.submit_fak(&o).await.unwrap_err();
        match err {
            VenueError::Live(msg) => assert!(msg.contains("unregistered token"), "{msg}"),
            other => panic!("expected Live, got {other:?}"),
        }
        assert_eq!(mock.hits.load(Ordering::SeqCst), 0, "no network before token check");
    }

    // -- decimal helper unit test ------------------------------------------

    #[test]
    fn decimal_to_micro_is_exact_and_rejects_garbage() {
        assert_eq!(decimal_to_micro("0.33"), Some(330_000));
        assert_eq!(decimal_to_micro("10"), Some(10_000_000));
        assert_eq!(decimal_to_micro("0.000001"), Some(1));
        assert_eq!(decimal_to_micro("123456.654321"), Some(123_456_654_321));
        assert_eq!(decimal_to_micro("0"), Some(0));
        assert_eq!(decimal_to_micro(""), None);
        assert_eq!(decimal_to_micro("0.0000001"), None); // 7 fractional digits
        assert_eq!(decimal_to_micro("abc"), None);
        assert_eq!(decimal_to_micro("-1"), None);
        assert_eq!(decimal_to_micro("1.2.3"), None);
    }

    #[test]
    fn px_from_decimal_maps_tick_aligned_prices() {
        assert_eq!(px_from_decimal("0.33", TickSize::Cent).unwrap().get(), 33);
        assert_eq!(px_from_decimal("0.046", TickSize::Milli).unwrap().get(), 46);
        // Not tick-aligned on a Cent market (0.335 = 335_000 µ, not a multiple
        // of 10_000) → rejected rather than silently rounded.
        assert!(px_from_decimal("0.335", TickSize::Cent).is_none());
    }
}
