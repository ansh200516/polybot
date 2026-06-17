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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use serde_json::json;
use tracing::info;

use pm_core::book::Side as BookSide;
use pm_core::fees::fee_microusdc;
use pm_core::instrument::TokenId;
use pm_core::num::{Px, Qty, TickSize, Usdc, buy_cost, sell_proceeds};
use pm_engine::Action;

use crate::Order;
use crate::auth::l2_headers;
use crate::fills::{MakerFill, UserFillSource, parse_maker_fills};
use crate::maker::{MakerOrder, MakerVenue, OpenOrder, OrderId, OrderType};
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

pub(crate) fn decimal_to_micro(s: &str) -> Option<u64> {
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
pub(crate) fn px_from_decimal(s: &str, ts: TickSize) -> Option<Px> {
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
    /// The L2 `POLY_ADDRESS`: the address the CLOB API key is bound to. Per the
    /// OFFICIAL Polymarket Rust SDK (`rs-clob-client-v2` `auth.rs`/`client.rs`)
    /// this is ALWAYS the **EOA** — the key derives from a plain-EOA L1 signature
    /// for every signature type, and `Authenticated.address = signer.address()`.
    /// It is NOT the order `signer`: for a POLY_1271 deposit-wallet order BOTH
    /// `order.maker` and `order.signer` are the deposit wallet (the funder), while
    /// `POLY_ADDRESS` and the key binding stay on the EOA. See `submit_fak`.
    pub auth_address: Address,
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
    /// token → (venue decimal token id, neg_risk, tick size). The tick size is
    /// required by the maker path: `MakerOrder.price`/`OpenOrder.price` are bare
    /// tick indices ([`Px`]) carrying no scale, so signing maker `place` amounts
    /// and parsing `open_orders` prices both need the per-token [`TickSize`].
    /// (The taker `submit_fak` path keeps using `order.ts` — see there.)
    tokens: HashMap<TokenId, (String, bool, TickSize)>,
    /// Order salt source. Default: SystemTime nanos XOR a bumped counter;
    /// overridable in tests for determinism. `FnMut` so the counter can bump.
    pub salt_src: Box<dyn FnMut() -> u64 + Send>,
    limiter: RateLimiter,
    /// One-shot: log the identity binding (maker/signer/owner/POLY_ADDRESS) on
    /// the FIRST submitted order so the operator run is decisive without
    /// spamming every order. Set false after the first submit.
    log_first_order: bool,
    /// Dedup state for the [`UserFillSource`] maker-fill poll (Task 3.4):
    /// `"{trade_id}:{order_id}"` keys already emitted. `/data/trades` rows recur
    /// across polls until they scroll off the cursor window, so this guarantees
    /// each maker fill is reported exactly once. Lives on the venue because the
    /// poll reuses the venue's single rate-limit budget + token registry.
    seen_trades: HashSet<String>,
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
            seen_trades: HashSet::new(),
        })
    }

    /// Register a token's venue decimal id, neg-risk flag, and market tick size.
    /// Orders for an unregistered token are rejected before any I/O. The `ts`
    /// must be the token's true market tick size: the maker path uses it to
    /// scale `place` amounts and to parse `open_orders` prices (the taker
    /// `submit_fak` path uses the `Order.ts` carried on each order instead).
    pub fn register_token(
        &mut self,
        token: TokenId,
        venue_id: String,
        neg_risk: bool,
        ts: TickSize,
    ) {
        self.tokens.insert(token, (venue_id, neg_risk, ts));
    }

    /// Reverse-map a venue decimal token id (`asset_id`) back to our interned
    /// [`TokenId`] and its [`TickSize`]. Used by the typed `open_orders` to type
    /// a reported row's price; `None` for an asset id we never registered.
    fn token_for_venue_id(&self, venue_id: &str) -> Option<(TokenId, TickSize)> {
        token_for_venue_id_in(&self.tokens, venue_id)
    }

    /// Build + sign the shared V2 deposit-wallet order struct used by BOTH the
    /// taker FAK path (`submit_fak`) and the maker resting path
    /// (`MakerVenue::place`). Returns the signed [`ClobOrder`] (carrying the
    /// salt), its `0x`-hex wire signature, and the `"BUY"`/`"SELL"` side string.
    ///
    /// V2 deposit-wallet flow (POLY_1271 / sigType 3), matching the OFFICIAL
    /// working Polymarket Rust SDK (`rs-clob-client-v2` order_builder.rs): for a
    /// Poly1271 order BOTH `maker` AND `signer` are the deposit wallet (funder).
    /// `order.signer` is the deposit wallet, NOT the EOA — even though the API key
    /// is bound to the EOA and L2 POLY_ADDRESS = the EOA (cfg.auth_address). The
    /// inner ECDSA is produced by the EOA inside the ERC-7739 wrap and validated
    /// on-chain by the deposit wallet's ERC-1271 isValidSignature. The signed
    /// struct is time-in-force-agnostic: the resting `orderType`/`postOnly` and
    /// the `expiration` are WIRE-ONLY top-level fields (see `order_wire_body`),
    /// so GTC maker orders are signing-identical to the proven taker order.
    fn sign_v2_order(
        &mut self,
        venue_token: &str,
        neg_risk: bool,
        action: Action,
        ts: TickSize,
        price: Px,
        qty: Qty,
    ) -> Result<(ClobOrder, String, &'static str), VenueError> {
        let (maker_amount, taker_amount) = clob_amounts(action, ts, price, qty);
        let side = match action {
            Action::Buy => Side::Buy,
            Action::Sell => Side::Sell,
        };
        // V2 signed struct (RECON-M5-V2): timestamp in ms; metadata/builder zero.
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let clob_order = ClobOrder {
            salt: (self.salt_src)(),
            maker: self.cfg.deposit_wallet,
            signer: self.cfg.deposit_wallet,
            token_id: venue_token.to_string(),
            maker_amount,
            taker_amount,
            side,
            signature_type: 3,
            timestamp,
            metadata: [0u8; 32].into(),
            builder: [0u8; 32].into(),
        };
        let signature = sign_order_1271(
            &self.cfg.signer,
            &clob_order,
            neg_risk,
            CHAIN_ID,
            self.cfg.deposit_wallet,
        )
        .map_err(|e| VenueError::Live(e.to_string()))?;
        let side_str = match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        Ok((clob_order, signature, side_str))
    }

    /// Serialise the V2 wire body (RECON-M5-V2 order_to_json_v2): salt &
    /// signatureType are JSON NUMBERS, every other order field a STRING; side is
    /// "BUY"/"SELL"; metadata/builder are 0x-prefixed 32-byte hex; top-level
    /// `deferExec`. V2 drops taker/nonce/feeRateBps from the wire; `expiration`
    /// stays on the wire though it is NOT in the signed struct.
    ///
    /// The taker FAK path and the maker resting path share this builder and
    /// differ in EXACTLY the three top-level fields passed here: `order_type`
    /// (`"FAK"` vs `"GTC"`/`"GTD"`), `post_only`, and `expiration` (`"0"` for
    /// FAK/GTC; a UTC-seconds string for GTD). Reusing one builder keeps
    /// `submit_fak`'s emitted bytes byte-identical to the proven taker wire.
    fn order_wire_body(
        &self,
        clob_order: &ClobOrder,
        signature: &str,
        side_str: &str,
        expiration: &str,
        order_type: &str,
        post_only: bool,
    ) -> serde_json::Value {
        json!({
            "order": {
                "salt": clob_order.salt,
                "maker": format!("{:#x}", clob_order.maker),
                "signer": format!("{:#x}", clob_order.signer),
                "tokenId": clob_order.token_id,
                "makerAmount": clob_order.maker_amount.to_string(),
                "takerAmount": clob_order.taker_amount.to_string(),
                "side": side_str,
                "expiration": expiration,
                "signatureType": clob_order.signature_type,
                "timestamp": clob_order.timestamp.to_string(),
                "metadata": format!("{:#x}", clob_order.metadata),
                "builder": format!("{:#x}", clob_order.builder),
                "signature": signature,
            },
            "owner": self.cfg.creds.key,
            "orderType": order_type,
            "deferExec": false,
            "postOnly": post_only,
        })
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
        // L2 POLY_ADDRESS = the address the API key is bound to (cfg.auth_address:
        // the deposit wallet for a frontend-minted key, the EOA for an auto-derived
        // one). The CLOB validates the key against this.
        let auth_address = self.cfg.auth_address.to_string();
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

    /// Fetch the public order book for a registered `token` and return its best
    /// (lowest) ask as a tick `Px`, or `None` if the ask side is empty/
    /// unparseable. Public read — no auth, no limiter. Used by `--probe-order`
    /// to find a cheap, fillable level to exercise the signed-order path.
    pub async fn best_ask(
        &self,
        token: TokenId,
        ts: TickSize,
    ) -> Result<Option<Px>, VenueError> {
        let (venue_token, _neg, _ts) = self
            .tokens
            .get(&token)
            .cloned()
            .ok_or_else(|| VenueError::Live(format!("probe: token {} not registered", token.0)))?;
        let url = format!("{}/book?token_id={}", self.cfg.base, venue_token);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| VenueError::Live(e.to_string()))?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| VenueError::Live(e.to_string()))?;
        if !status.is_success() {
            return Err(VenueError::Live(format!("book {status}: {body}")));
        }
        let book: serde_json::Value =
            serde_json::from_str(&body).map_err(|e| VenueError::Live(e.to_string()))?;
        let asks = match book.get("asks").and_then(|a| a.as_array()) {
            Some(a) => a,
            None => return Ok(None),
        };
        // Lowest ask across all levels — don't assume the venue's sort order.
        let best = asks
            .iter()
            .filter_map(|lvl| lvl.get("price").and_then(|p| p.as_str()))
            .filter_map(|p| px_from_decimal(p, ts))
            .min_by_key(|px| px.get());
        Ok(best)
    }
}

/// Reverse-map a venue decimal token id to our [`TokenId`] + [`TickSize`] over a
/// token registry. A free function (not just the `&self` method) so the
/// [`UserFillSource`] poll can pass it as the `resolve` closure while holding a
/// DISJOINT `&mut` borrow of `seen_trades` — a `&self` method would borrow all
/// of `self` and conflict with that mutable field borrow.
fn token_for_venue_id_in(
    tokens: &HashMap<TokenId, (String, bool, TickSize)>,
    venue_id: &str,
) -> Option<(TokenId, TickSize)> {
    tokens
        .iter()
        .find_map(|(tok, (vid, _neg, ts))| (vid == venue_id).then_some((*tok, *ts)))
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
        // Unknown token → reject BEFORE any signing or I/O. The taker path signs
        // amounts from the per-ORDER tick size (`order.ts`), NOT the registry's
        // tick size, so its emitted wire stays byte-identical now that the
        // registry also carries a tick size (used only by the maker path).
        let (venue_token, neg_risk, _ts) = self
            .tokens
            .get(&order.token)
            .cloned()
            .ok_or_else(|| VenueError::Live(format!("unregistered token {}", order.token.0)))?;

        // Sign the shared V2 deposit-wallet (POLY_1271, sigType 3) order struct.
        // This is the SAME signing the maker `place` path uses — see
        // `sign_v2_order`; the two differ only in the three top-level wire fields.
        let (clob_order, signature, side_str) = self.sign_v2_order(
            &venue_token,
            neg_risk,
            order.action,
            order.ts,
            order.limit_px,
            order.qty,
        )?;

        // FAK taker wire: orderType "FAK", postOnly false, expiration "0" — the
        // proven RECON-M5-V2 shape. `order_wire_body` emits the byte-identical
        // body (verified by this module's submit_fak_* assertions).
        let body_value = self.order_wire_body(&clob_order, &signature, side_str, "0", "FAK", false);
        let body = serde_json::to_string(&body_value)
            .map_err(|e| VenueError::Live(e.to_string()))?;

        // First-order diagnostic (no secrets — addresses + api-key id only).
        // Official-SDK binding: the L2 POLY_ADDRESS == the EOA (the API key's
        // owner); the order maker AND signer == the deposit wallet (signatureType
        // 3); owner is the api-key id. One line, then silenced.
        if self.log_first_order {
            self.log_first_order = false;
            info!(
                maker = %format!("{:#x}", clob_order.maker),
                signer = %format!("{:#x}", clob_order.signer),
                owner = %self.cfg.creds.key,
                l2_poly_address = %self.cfg.auth_address,
                eoa = %self.cfg.signer.address(),
                "first live order identity (maker=signer=deposit wallet; L2 POLY_ADDRESS=API-key binding=EOA; sigType 3)"
            );
        }

        // SHADOW: signed, never submitted. No limiter, no network. Return a
        // zero-fill outcome with no venue id (nothing was placed).
        if self.cfg.shadow {
            info!(
                order = %order.id,
                token = %venue_token,
                side = side_str,
                limit_ticks = order.limit_px.get(),
                qty_micro = order.qty.0,
                "SHADOW: signed, not submitted"
            );
            return Ok(SubmitOutcome::default());
        }

        // The L2 HMAC must sign the EXACT wire body string. POLY_ADDRESS = the
        // address the API key is bound to (cfg.auth_address).
        self.limiter.acquire().await;
        let ts = unix_seconds_string();
        let auth_address = self.cfg.auth_address.to_string();
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
// MakerVenue (Task 3.3): resting postOnly GTC/GTD orders on CLOB V2
//
// SEPARATE from the taker ExecutionVenue: `place` rests a signed limit order
// (reusing POST /order with the SAME signed struct + wire body as `submit_fak`,
// differing only in orderType/postOnly/expiration), `cancel` issues DELETE
// /order, `replace` is cancel-then-place (CLOB V2 has no native amend), and
// `open_orders` is the typed, registered-token-only view over GET /data/orders.
//
// This code is INERT until Phase 4 wires the MM strategy behind a default-off
// flag, but it WILL place real orders, so it faithfully mirrors the proven
// taker signing/auth/wire path.
// ===========================================================================

impl MakerVenue for LiveVenue {
    /// Place a resting `postOnly` GTC/GTD order. Reuses POST `/order` with the
    /// SAME signed V2 order + wire body as `submit_fak`; only `orderType`,
    /// `postOnly`, and `expiration` differ.
    ///
    /// `postOnly` guarantees maker-only: the venue REJECTS the order
    /// (`INVALID_POST_ONLY_ORDER`) if it would cross, or if combined with
    /// FOK/FAK. A resting postOnly order normally comes back `status: "live"`;
    /// we do NOT poll fills here (fills arrive via Task 3.4).
    ///
    /// GTD note: `expiration` is the order's UTC-SECONDS expiry (`expiry_ms /
    /// 1000`). In this V2 signing path `expiration` is WIRE-ONLY (not in the
    /// signed struct), exactly as `submit_fak` already sends `"0"` — so GTC is
    /// signing-identical to the proven taker path. GTD's expiration is therefore
    /// UNSIGNED and UNVERIFIED against the live venue; revisit in canary.
    async fn place(&mut self, o: &MakerOrder) -> Result<OrderId, VenueError> {
        // Unknown token → reject BEFORE any signing or I/O (mirrors submit_fak).
        // The registry tick size types the maker price into signed amounts.
        let (venue_token, neg_risk, ts) = self
            .tokens
            .get(&o.token)
            .cloned()
            .ok_or_else(|| VenueError::Live(format!("unregistered token {}", o.token.0)))?;

        // Side map: Bid → Buy, Ask → Sell (same as the taker action map).
        let action = match o.side {
            BookSide::Bid => Action::Buy,
            BookSide::Ask => Action::Sell,
        };
        let (clob_order, signature, side_str) =
            self.sign_v2_order(&venue_token, neg_risk, action, ts, o.price, o.size)?;

        // The three top-level wire fields that distinguish a resting maker order
        // from a FAK taker: orderType, postOnly, expiration. GTC → "0"; GTD →
        // UTC seconds (ms / 1000), wire-only / unsigned (see the doc note above).
        let (order_type_str, expiration) = match o.order_type {
            OrderType::Gtc => ("GTC", "0".to_string()),
            OrderType::Gtd { expiry_ms } => ("GTD", (expiry_ms / 1000).to_string()),
        };
        let body_value = self.order_wire_body(
            &clob_order,
            &signature,
            side_str,
            &expiration,
            order_type_str,
            o.post_only,
        );
        let body = serde_json::to_string(&body_value)
            .map_err(|e| VenueError::Live(e.to_string()))?;

        // SHADOW: signed + logged, never submitted (no limiter, no network).
        // Return a deterministic sentinel id so a dry-run MM strategy can track
        // the "order" without a venue round-trip.
        if self.cfg.shadow {
            info!(
                token = %venue_token,
                side = side_str,
                order_type = order_type_str,
                post_only = o.post_only,
                limit_ticks = o.price.get(),
                qty_micro = o.size.0,
                "SHADOW: maker order signed, not submitted"
            );
            return Ok(OrderId(format!("shadow-{}", clob_order.salt)));
        }

        // The L2 HMAC must sign the EXACT wire body string (byte-identical to the
        // request body). POLY_ADDRESS = the key's bound address (auth_address).
        self.limiter.acquire().await;
        let ts_hdr = unix_seconds_string();
        let auth_address = self.cfg.auth_address.to_string();
        let headers =
            l2_headers(&self.cfg.creds, &auth_address, &ts_hdr, "POST", "/order", Some(&body))
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
        // HTTP 200 with success:false is a processing failure (e.g. a postOnly
        // order that would cross → INVALID_POST_ONLY_ORDER).
        if parsed.get("success").and_then(|v| v.as_bool()) != Some(true) {
            let msg = parsed
                .get("errorMsg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown processing failure");
            return Err(VenueError::Live(msg.to_string()));
        }
        let order_id = parsed
            .get("orderID")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        // Positive confirmation a resting maker order was accepted (debug: an
        // active MM re-places frequently, so keep it off the info stream).
        tracing::debug!(
            token = %venue_token,
            side = side_str,
            order_type = order_type_str,
            post_only = o.post_only,
            limit_ticks = o.price.get(),
            qty_micro = o.size.0,
            order_id = %order_id,
            "LIVE: maker order accepted (resting)"
        );
        Ok(OrderId(order_id))
    }

    /// Cancel a resting order via DELETE `/order` with body `{"orderID":"0x.."}`.
    ///
    /// IDEMPOTENT by design: any HTTP 200 is `Ok(())`, whether the venue reports
    /// the id under `canceled` or under `not_canceled` (already filled/expired/
    /// never resting). This satisfies the QuoteManager's double-cancel-safe
    /// contract — a cancel of an order that is already gone is success, not an
    /// error. Only transport / non-200 HTTP failures return `Err`.
    async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError> {
        // SHADOW: local no-op (nothing was ever placed on the venue).
        if self.cfg.shadow {
            return Ok(());
        }
        // Serialise the body ONCE and reuse the SAME string for the HMAC and the
        // request — they must be byte-identical (like submit_fak's POST body).
        let body = serde_json::to_string(&json!({ "orderID": id.0 }))
            .map_err(|e| VenueError::Live(e.to_string()))?;
        self.limiter.acquire().await;
        let ts_hdr = unix_seconds_string();
        let auth_address = self.cfg.auth_address.to_string();
        let headers =
            l2_headers(&self.cfg.creds, &auth_address, &ts_hdr, "DELETE", "/order", Some(&body))
                .map_err(|e| VenueError::Live(e.to_string()))?;
        let url = format!("{}/order", self.cfg.base);
        let mut req = self
            .http
            .delete(&url)
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
        // Any 200 → Ok: the order is gone (canceled) or was never resting
        // (not_canceled). Both are the desired post-condition for a cancel.
        Ok(())
    }

    /// Replace = cancel-then-place. CLOB V2 has NO native replace/amend, so this
    /// cancels `id` then places `o`, returning the NEW venue id.
    ///
    /// NON-ATOMIC: cancel-then-place leaves a brief no-quote gap between the two
    /// calls. This is the DELIBERATE choice over place-then-cancel, whose brief
    /// double-exposure (two live quotes) is worse for inventory caps. The
    /// QuoteManager on-error contract plus Task 3.5 reconciliation are the safety
    /// net for the gap (and for a cancel that succeeds but place that fails).
    async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError> {
        self.cancel(id).await?;
        self.place(o).await
    }

    /// Typed open orders: reuse the cursor-paginated `GET /data/orders` walk and
    /// map each row to an [`OpenOrder`].
    ///
    /// REGISTERED-TOKEN-ONLY: a row whose `asset_id` is not in our registry is
    /// SKIPPED with a warning — we cannot type its `Px` without the token's tick
    /// size. The raw `LiveVenue::open_orders` (Vec<Value>) remains for the full
    /// startup sweep that must see every resting order regardless of registration.
    ///
    /// `size` is the REMAINING size (`original_size − size_matched`): what is
    /// still resting on the book, which is what a quote-reconciler cares about.
    async fn open_orders(&mut self) -> Result<Vec<OpenOrder>, VenueError> {
        let rows = self.data_rows("orders", "").await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let Some(asset_id) = row.get("asset_id").and_then(|v| v.as_str()) else {
                tracing::warn!("open order row missing asset_id; skipping");
                continue;
            };
            // Reverse-map asset_id → (TokenId, TickSize); skip unregistered.
            let Some((token, ts)) = self.token_for_venue_id(asset_id) else {
                tracing::warn!(
                    asset_id,
                    "open order for unregistered token; skipping (cannot type Px without tick size)"
                );
                continue;
            };
            let side = match row.get("side").and_then(|v| v.as_str()) {
                Some("BUY") => BookSide::Bid,
                Some("SELL") => BookSide::Ask,
                other => {
                    tracing::warn!(?other, asset_id, "open order row has unknown side; skipping");
                    continue;
                }
            };
            let Some(price_s) = row.get("price").and_then(|v| v.as_str()) else {
                tracing::warn!(asset_id, "open order row missing price; skipping");
                continue;
            };
            let Some(price) = px_from_decimal(price_s, ts) else {
                tracing::warn!(asset_id, price = price_s, "open order row has bad/unaligned price; skipping");
                continue;
            };
            let original = row
                .get("original_size")
                .and_then(|v| v.as_str())
                .and_then(decimal_to_micro);
            let Some(original) = original else {
                tracing::warn!(asset_id, "open order row missing/bad original_size; skipping");
                continue;
            };
            // size_matched is optional / may be absent for a brand-new order.
            let matched = row
                .get("size_matched")
                .and_then(|v| v.as_str())
                .and_then(decimal_to_micro)
                .unwrap_or(0);
            let id = row
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            out.push(OpenOrder {
                id: OrderId(id),
                token,
                side,
                price,
                size: Qty(original.saturating_sub(matched)),
            });
        }
        Ok(out)
    }
}

// ===========================================================================
// UserFillSource (Task 3.4): maker-fill poll over GET /data/trades
//
// Tells the (Phase-4) market-making strategy when its RESTING maker orders
// fill, so it can update inventory and re-quote. Reuses the SAME auth'd,
// rate-limited, cursor-paginated `data_rows("trades")` walk as the taker
// `poll_fills`, but selects the MAKER side: a trade where WE were the maker
// (`trader_side == "MAKER"`) carries our resting-order fills in `maker_orders[]`.
//
// REST poll (vs the lower-latency user WS) is the Phase-3 choice — it reuses the
// proven infra and is fully testable against the in-crate HTTP mock. The user
// WS is a drop-in latency upgrade behind this SAME trait; see `fills.rs`.
//
// Placed as an impl ON LiveVenue (not a standalone `LiveUserFills`) so the poll
// shares the venue's ONE token registry, rate limiter, and account-wide
// rate-limit budget, and reuses `data_rows`/`token_for_venue_id` verbatim with
// zero duplicated auth/limiter logic. INERT until Phase 4 wires the MM strategy.
// ===========================================================================

impl UserFillSource for LiveVenue {
    /// One `GET /data/trades` walk → the NEW (deduped) maker fills. `shadow`
    /// returns `Ok(vec![])` with no network I/O (no limiter, no GET). Dedup state
    /// (`seen_trades`) persists on the venue, so each maker fill is emitted
    /// exactly once across polls even though trade rows recur until they scroll
    /// off the cursor window.
    async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError> {
        if self.cfg.shadow {
            return Ok(Vec::new());
        }
        let rows = self.data_rows("trades", "").await?;
        // Disjoint field borrows: the `resolve` closure reads `self.tokens` while
        // `parse_maker_fills` mutably borrows `self.seen_trades`. Routing the
        // lookup through the free `token_for_venue_id_in` (not the `&self`
        // method) keeps the two borrows non-overlapping.
        let tokens = &self.tokens;
        let fills = parse_maker_fills(
            &rows,
            |aid| token_for_venue_id_in(tokens, aid),
            &mut self.seen_trades,
        );
        Ok(fills)
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
    // -- maker (Task 3.3) response fixtures --------------------------------
    const PLACED: &str = include_str!("../tests/fixtures/clob_responses/order_placed_live.json");
    const POST_ONLY_REJECTED: &str =
        include_str!("../tests/fixtures/clob_responses/order_post_only_rejected.json");
    const CANCEL_OK: &str = include_str!("../tests/fixtures/clob_responses/cancel_ok.json");
    const CANCEL_IDEMPOTENT: &str =
        include_str!("../tests/fixtures/clob_responses/cancel_idempotent.json");
    const OPEN_ORDERS_MIXED: &str =
        include_str!("../tests/fixtures/clob_responses/open_orders_mixed.json");
    // -- user fills (Task 3.4) response fixture ----------------------------
    const TRADES_MAKER_FILLS: &str =
        include_str!("../tests/fixtures/clob_responses/trades_maker_fills.json");
    /// The `orderID` the placed / cancel fixtures carry.
    const PLACED_ID: &str =
        "0xa1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";

    fn test_venue(base: String, shadow: bool) -> LiveVenue {
        // 64 hex chars (no 0x): the throwaway "0xadad…ad" key.
        let signer: PrivateKeySigner = "ad".repeat(32).parse().unwrap();
        let proxy: Address = format!("0x{}", "11".repeat(20)).parse().unwrap();
        // Deposit wallet (the V2 maker AND signer) — distinct from proxy so the
        // maker assertion is meaningful.
        let deposit_wallet: Address = format!("0x{}", "22".repeat(20)).parse().unwrap();
        // The EOA the API key binds to (= L2 POLY_ADDRESS). Production-faithful:
        // DISTINCT from the deposit wallet, so the test proves order.signer/maker
        // (deposit wallet) are decoupled from POLY_ADDRESS (the EOA). Conflating
        // these (auth_address == deposit_wallet) is what masked the live bug where
        // order.signer was wrongly set to the EOA.
        let eoa_auth: Address = format!("0x{}", "33".repeat(20)).parse().unwrap();
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
            // L2 POLY_ADDRESS = the EOA the key binds to (NOT the deposit wallet).
            auth_address: eoa_auth,
            fill_window: Duration::from_millis(200),
            rate_per_sec: 1000.0,
            rate_capacity: 100,
            shadow,
        };
        let mut v = LiveVenue::new(cfg).unwrap();
        v.salt_src = Box::new(|| 42);
        v.register_token(TokenId(7), "123456789".into(), false, TickSize::Cent);
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
        // L2 POLY_ADDRESS == cfg.auth_address == the EOA the key binds to (0x3333…),
        // which is DISTINCT from the deposit wallet (the maker/signer). This is the
        // official-SDK shape: POLY_ADDRESS = EOA, order.signer = deposit wallet.
        let lower = reqs[0].to_ascii_lowercase();
        assert!(
            lower.contains(&format!("poly_address: 0x{}", "33".repeat(20))),
            "L2 POLY_ADDRESS must be the EOA the key binds to (auth_address), not the deposit wallet: {}",
            reqs[0]
        );
        assert!(reqs[0].contains("\"orderType\":\"FAK\""));
        // salt is a JSON NUMBER (pinned to 42 by the test salt_src), not a string.
        assert!(reqs[0].contains("\"salt\":42"), "salt must be a number: {}", reqs[0]);
        // V2 deposit-wallet flow (RECON-M5-V2-1271): signatureType is 3
        // (POLY_1271), a JSON NUMBER; amounts are strings (RECON wire).
        assert!(reqs[0].contains("\"signatureType\":3"), "{}", reqs[0]);
        assert!(reqs[0].contains("\"makerAmount\":\"3300000\""), "{}", reqs[0]);
        // maker AND signer are BOTH the DEPOSIT WALLET (0x2222…2222), per the
        // official Rust SDK's Poly1271 order builder (maker = signer = funder).
        // Crucially, signer is the deposit wallet, NOT the EOA (0x3333… = auth_address
        // = POLY_ADDRESS) — proving order.signer is decoupled from POLY_ADDRESS. The
        // EOA only produces the inner ERC-7739 ECDSA inside the wrap.
        assert!(
            reqs[0].contains(&format!("\"maker\":\"0x{}\"", "22".repeat(20))),
            "maker must be the deposit wallet: {}",
            reqs[0]
        );
        assert!(
            reqs[0].contains(&format!("\"signer\":\"0x{}\"", "22".repeat(20))),
            "signer must be the deposit wallet (funder), NOT the EOA/POLY_ADDRESS: {}",
            reqs[0]
        );
        // Explicit decoupling guard: the EOA (0x3333…) must NOT appear as the order
        // signer. (It is only the L2 POLY_ADDRESS / the key's bound address.)
        assert!(
            !reqs[0].contains(&format!("\"signer\":\"0x{}\"", "33".repeat(20))),
            "regression: order.signer must NOT be the EOA/auth_address: {}",
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

    // -- maker venue (Task 3.3) --------------------------------------------

    /// A resting BID on token 7 at 0.33 (tick 33 Cent), 10 shares. Bid → BUY,
    /// so amounts match the taker fixtures (makerAmount 3300000 µUSDC, taker
    /// 10000000 µshares) — lets the place test reuse the submit_fak assertions.
    fn maker_bid(order_type: OrderType, post_only: bool) -> MakerOrder {
        MakerOrder {
            token: TokenId(7),
            side: BookSide::Bid,
            price: Px::new(33, TickSize::Cent).unwrap(),
            size: Qty(10_000_000),
            order_type,
            post_only,
        }
    }

    #[tokio::test]
    async fn place_gtc_postonly_builds_correct_wire() {
        let mock = spawn_mock(vec![(200, PLACED.to_string())]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let id = v.place(&maker_bid(OrderType::Gtc, true)).await.unwrap();
        assert_eq!(id, OrderId(PLACED_ID.into()), "returns the fixture orderID");

        let reqs = mock.requests.lock().await;
        assert_eq!(reqs.len(), 1, "place is a single POST, no fill polling");
        assert!(reqs[0].starts_with("POST /order"), "req: {}", reqs[0]);
        // The THREE maker-distinguishing top-level fields.
        assert!(reqs[0].contains("\"orderType\":\"GTC\""), "{}", reqs[0]);
        assert!(reqs[0].contains("\"postOnly\":true"), "{}", reqs[0]);
        assert!(reqs[0].contains("\"expiration\":\"0\""), "GTC expiration is 0: {}", reqs[0]);
        assert!(
            !reqs[0].contains("\"orderType\":\"FAK\""),
            "a maker order is not FAK: {}",
            reqs[0]
        );
        // Side map Bid → BUY.
        assert!(reqs[0].contains("\"side\":\"BUY\""), "Bid → BUY: {}", reqs[0]);
        // Reuse the submit_fak wire assertions: the signed struct is identical to
        // the proven taker path (sigType 3, salt #, against-us amounts).
        assert!(reqs[0].contains("\"signatureType\":3"), "{}", reqs[0]);
        assert!(reqs[0].contains("\"salt\":42"), "salt is a number: {}", reqs[0]);
        assert!(reqs[0].contains("\"makerAmount\":\"3300000\""), "{}", reqs[0]);
        // maker AND signer are BOTH the deposit wallet (0x2222…), sigType-3 shape.
        assert!(
            reqs[0].contains(&format!("\"maker\":\"0x{}\"", "22".repeat(20))),
            "maker must be the deposit wallet: {}",
            reqs[0]
        );
        assert!(
            reqs[0].contains(&format!("\"signer\":\"0x{}\"", "22".repeat(20))),
            "signer must be the deposit wallet (funder), NOT the EOA: {}",
            reqs[0]
        );
        // L2 POLY_ADDRESS == auth_address == the EOA the key binds to (0x3333…),
        // DISTINCT from the deposit wallet — identical binding to the taker path.
        let lower = reqs[0].to_ascii_lowercase();
        assert!(lower.contains("poly_api_key"), "auth header present: {}", reqs[0]);
        assert!(
            lower.contains(&format!("poly_address: 0x{}", "33".repeat(20))),
            "L2 POLY_ADDRESS must be the EOA (auth_address): {}",
            reqs[0]
        );
        // ERC-7739 wrapped sig (> 132 hex chars), as on the taker path.
        let sig_hex = {
            let m = "\"signature\":\"0x";
            let start = reqs[0].find(m).unwrap() + m.len();
            let rest = &reqs[0][start..];
            &rest[..rest.find('"').unwrap()]
        };
        assert!(sig_hex.len() > 132, "ERC-7739 wrapped sig expected, got {}", sig_hex.len());
    }

    #[tokio::test]
    async fn place_gtd_sets_expiration_seconds() {
        let mock = spawn_mock(vec![(200, PLACED.to_string())]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        // expiry 1_750_000_000_000 ms → "1750000000" UTC seconds (ms / 1000).
        let o = maker_bid(OrderType::Gtd { expiry_ms: 1_750_000_000_000 }, true);
        let id = v.place(&o).await.unwrap();
        assert_eq!(id, OrderId(PLACED_ID.into()));

        let reqs = mock.requests.lock().await;
        assert!(reqs[0].contains("\"orderType\":\"GTD\""), "{}", reqs[0]);
        assert!(
            reqs[0].contains("\"expiration\":\"1750000000\""),
            "GTD expiration is the seconds string: {}",
            reqs[0]
        );
    }

    #[tokio::test]
    async fn place_rejected_postonly_cross_is_error() {
        let mock = spawn_mock(vec![(200, POST_ONLY_REJECTED.to_string())]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let err = v.place(&maker_bid(OrderType::Gtc, true)).await.unwrap_err();
        match err {
            VenueError::Live(msg) => {
                assert!(msg.contains("INVALID_POST_ONLY_ORDER"), "{msg}")
            }
            other => panic!("expected Live, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_success_and_idempotent() {
        // First DELETE: id reported under "canceled". Second DELETE: id reported
        // under "not_canceled" (already filled/expired/never resting). BOTH 200.
        let mock = spawn_mock(vec![
            (200, CANCEL_OK.to_string()),
            (200, CANCEL_IDEMPOTENT.to_string()),
        ]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let id = OrderId(PLACED_ID.into());

        v.cancel(&id).await.unwrap(); // canceled → Ok
        v.cancel(&id).await.unwrap(); // not_canceled → ALSO Ok (idempotent)

        let reqs = mock.requests.lock().await;
        assert_eq!(reqs.len(), 2, "two DELETE calls");
        assert!(reqs[0].starts_with("DELETE /order"), "req0: {}", reqs[0]);
        assert!(reqs[1].starts_with("DELETE /order"), "req1: {}", reqs[1]);
        assert!(
            reqs[0].contains(&format!("\"orderID\":\"{PLACED_ID}\"")),
            "cancel body carries the id: {}",
            reqs[0]
        );
    }

    #[tokio::test]
    async fn replace_is_cancel_then_place() {
        // Script a DELETE 200 (cancel) THEN a POST 200 (place), in order.
        let mock = spawn_mock(vec![
            (200, CANCEL_OK.to_string()),
            (200, PLACED.to_string()),
        ]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        let old = OrderId("0xoldrestingid".into());
        let new_id = v
            .replace(&old, &maker_bid(OrderType::Gtc, true))
            .await
            .unwrap();
        assert_eq!(new_id, OrderId(PLACED_ID.into()), "returns the NEW (placed) id");

        let reqs = mock.requests.lock().await;
        assert_eq!(reqs.len(), 2, "cancel-then-place = exactly two requests");
        // Order is load-bearing: cancel the OLD order BEFORE placing the new one.
        assert!(reqs[0].starts_with("DELETE /order"), "first must be the cancel: {}", reqs[0]);
        assert!(
            reqs[0].contains("\"orderID\":\"0xoldrestingid\""),
            "cancel targets the OLD id: {}",
            reqs[0]
        );
        assert!(reqs[1].starts_with("POST /order"), "second must be the place: {}", reqs[1]);
        assert!(reqs[1].contains("\"orderType\":\"GTC\""), "place body: {}", reqs[1]);
    }

    #[tokio::test]
    async fn open_orders_maps_registered_and_skips_unknown() {
        let mock = spawn_mock(vec![(200, OPEN_ORDERS_MIXED.to_string())]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);
        // The TRAIT method (inherent `open_orders` returns the raw Vec<Value>).
        let open = MakerVenue::open_orders(&mut v).await.unwrap();
        assert_eq!(open.len(), 1, "only the registered-token order is returned");
        let o = &open[0];
        assert_eq!(
            o.id,
            OrderId("0xresting11111111111111111111111111111111111111111111111111111111".into())
        );
        assert_eq!(o.token, TokenId(7), "asset_id 123456789 reverse-maps to token 7");
        assert_eq!(o.side, BookSide::Bid, "BUY → Bid");
        assert_eq!(o.price.get(), 33, "0.33 → 33 Cent ticks");
        // remaining = original 10 − matched 4 = 6 shares = 6e6 µshares.
        assert_eq!(o.size, Qty(6_000_000), "size is remaining = original − matched");
    }

    #[tokio::test]
    async fn shadow_place_no_network_returns_sentinel() {
        // Empty script: any network hit fails the connection. Shadow must not
        // touch the network and must return the deterministic salt sentinel.
        let mock = spawn_mock(vec![]);
        let mut v = test_venue(format!("http://{}", mock.addr), true);
        let id = v.place(&maker_bid(OrderType::Gtc, true)).await.unwrap();
        assert!(id.0.starts_with("shadow-"), "sentinel id: {}", id.0);
        assert_eq!(id, OrderId("shadow-42".into()), "salt-derived sentinel (test salt 42)");
        assert_eq!(mock.hits.load(Ordering::SeqCst), 0, "shadow made a network call");
    }

    // -- user fills (Task 3.4) ---------------------------------------------

    #[tokio::test]
    async fn live_user_fills_poll_maps_rows() {
        // Two copies of the SAME trades page: the first poll maps the maker
        // fills; the second re-serves the identical page and returns NOTHING —
        // dedup state persists on the venue (`seen_trades`).
        let mock = spawn_mock(vec![
            (200, TRADES_MAKER_FILLS.to_string()),
            (200, TRADES_MAKER_FILLS.to_string()),
        ]);
        let mut v = test_venue(format!("http://{}", mock.addr), false);

        let fills = v.poll().await.unwrap();
        assert_eq!(fills.len(), 2, "two maker fills; the TAKER row is skipped");
        // In appearance order: resting-A (10 sh @ 0.33), resting-B (5 sh @ 0.34).
        assert_eq!(fills[0].order_id, OrderId("0xresting-A".into()));
        assert_eq!(fills[0].token, TokenId(7), "asset 123456789 → token 7");
        assert_eq!(fills[0].qty, Qty(10_000_000), "'10' shares → 10e6 µshares");
        assert_eq!(fills[0].px.get(), 33, "'0.33' → 33 Cent ticks");
        assert_eq!(fills[0].trade_id, "trade-aaa");
        assert_eq!(fills[1].order_id, OrderId("0xresting-B".into()));
        assert_eq!(fills[1].qty, Qty(5_000_000));
        assert_eq!(fills[1].px.get(), 34, "'0.34' → 34 Cent ticks");

        // Second poll re-serves the same page → all keys seen → empty.
        let again = v.poll().await.unwrap();
        assert!(again.is_empty(), "dedup persists across polls: {again:?}");
        assert_eq!(mock.hits.load(Ordering::SeqCst), 2, "one GET per poll");
    }

    #[tokio::test]
    async fn shadow_poll_no_network() {
        // Shadow venue: poll short-circuits with no network call at all.
        let mock = spawn_mock(vec![]);
        let mut v = test_venue(format!("http://{}", mock.addr), true);
        let fills = v.poll().await.unwrap();
        assert!(fills.is_empty());
        assert_eq!(mock.hits.load(Ordering::SeqCst), 0, "shadow polled the network");
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
