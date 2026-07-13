//! BTC "Up or Down 5m" strategy (spec 2026-07-13).
//!
//! Phase 0/1 is READ-ONLY: each tick it prices a fair P(up) and logs it against
//! the live book ([`ShadowSample`] → `Btc5mShadow`), emitting NO orders. That
//! SHADOW path is UNCONDITIONAL and unchanged.
//!
//! Phase 2 (this module) adds a LIVE, capital-critical micro-taker on top: WHEN
//! `live == true` AND a taker [`CopyVenue`] is attached, at most ONCE per window,
//! it places one tiny marketable-FAK BUY on the near-certain leader (per the pure
//! [`decide_entry`], gated by the entry window / z-score / net-edge buffer),
//! books the fill into [`InventoryRisk`] + the durable store, and upserts the
//! open position ([`Btc5mPositionRow`]) for the Task 6 settle sweep. The order
//! path is reachable ONLY under `live && venue.is_some() && !entered_this_window
//! && !halted && daily-notional-cap-ok`; with `live == false` (or no venue) the
//! strategy is shadow-only and places nothing.
pub mod entry;
pub mod market;
pub mod model;
pub mod settle;
pub mod shadow;

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use alloy_primitives::B256;
use pm_core::num::{Bps, TickSize, Usdc, buy_cost};
use pm_engine::Action;
use pm_execution::Order;
use pm_execution::relayer::RelayerClient;
use pm_ingestion::gamma::{GammaClient, GammaWindow};
use pm_ingestion::spot::SpotFeed;
use pm_risk::inventory::{InventoryConfig, InventoryRisk};
use pm_store::read::ReadStore;
use pm_store::writer::StoreMsg;
use pm_store::{Btc5mPositionRow, FillRow, OrderRow, usdc_to_i64, utc_day_from_ms};
use tokio::time::MissedTickBehavior;

use super::{Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};
use crate::strategy::btc5m::entry::{Entry, EntryParams, decide_entry};
use crate::strategy::btc5m::market::{Rotation, Window};
use crate::strategy::btc5m::model::fair_p_up;
use crate::strategy::btc5m::shadow::ShadowSample;
use crate::strategy::copy::CopyVenue;

/// Polymarket crypto (crypto_fees_v2) TAKER fee rate — the `rate` in the
/// `rate·p·(1−p)` schedule these 5-minute markets use (see the fee doc-comment
/// in [`entry`]). A VENUE CONSTANT, not a config knob.
const CRYPTO_FEE_RATE: f64 = 0.07;

/// How long AFTER a window's close to wait before trusting its Gamma outcome as
/// FINAL (resolution finality lag). The settle sweep only settles a held
/// position once `now_ms() >= t_close_ms + SETTLE_DELAY_MS`.
const SETTLE_DELAY_MS: i64 = 120_000;

/// One tracked open micro-taker position — the settle sweep's in-memory working
/// set (mirrors the durable [`Btc5mPositionRow`]). Keyed in [`Btc5mStrategy::open`]
/// by `(condition_id, outcome_index)`.
#[derive(Debug, Clone)]
struct OpenPos {
    /// The CLOB token id we bought (audit; settle keys off `condition_id`).
    token: String,
    qty_micro: i64,
    cost_micro: i64,
    /// Did we buy the UP/YES side (outcome_index 0)? Drives realized PnL sign.
    bought_up: bool,
    /// Window close (ms). `open_unix = t_close_ms/1000 - 300` rebuilds the slug.
    t_close_ms: i64,
}

/// Phase-2 entry tuning, resolved from `[btc5m]` config (`Btc5mParamsCfg`) by
/// main and threaded into the strategy at construction.
#[derive(Debug, Clone, Copy)]
pub struct Btc5mParams {
    /// Only enter when `0 < secs_to_go ≤ this`.
    pub entry_window_secs: i64,
    /// Minimum `|z|` (fair-value deviation) to treat a leader as near-certain.
    pub z_threshold: f64,
    /// Required net edge to enter, in probability units (0.02 = 2¢/share).
    pub edge_buffer_c: f64,
    /// Fixed micro notional per entry, USD.
    pub entry_notional_usd: f64,
    /// Max total taker notional deployed per UTC day, USD.
    pub max_daily_notional_usd: f64,
    /// Daily realized-loss floor, USD (the inventory circuit breaker's key).
    pub max_daily_loss_usd: f64,
}

impl Default for Btc5mParams {
    /// Mirrors the conservative `[btc5m]` config defaults.
    fn default() -> Self {
        Btc5mParams {
            entry_window_secs: 20,
            z_threshold: 1.5,
            edge_buffer_c: 0.02,
            entry_notional_usd: 10.0,
            max_daily_notional_usd: 200.0,
            max_daily_loss_usd: 25.0,
        }
    }
}

/// The inventory-ledger caps (gross/notional + `on_fill` realized accounting for
/// the entry fills). NOTE: the daily-LOSS cap (`max_daily_loss_usd`) is NOT
/// enforced through these `inv` floors — btc5m positions realize only at
/// resolution, which the settle sweep books outside `inv`. That cap is instead
/// enforced by the settle sweep's `day_realized_micro` latch (Task 6), which trips
/// `halted` once the day's realized reaches `-(max_daily_loss_usd × 1e6)`.
fn inv_config(params: &Btc5mParams) -> InventoryConfig {
    let daily_loss = Usdc((params.max_daily_loss_usd * 1_000_000.0) as i128);
    let notional = Usdc((params.max_daily_notional_usd * 1_000_000.0) as i128);
    InventoryConfig {
        max_inventory_usd: notional,
        max_gross_inventory_usd: notional,
        inventory_stop_loss_usd: daily_loss,
        daily_loss_usd: daily_loss,
        vol_pull_ticks: 0,
        vol_window: Duration::from_secs(1),
    }
}

/// Persisted decimal-places code → [`TickSize`]; `None` for an unsupported code.
fn tick_from_decimals(d: i64) -> Option<TickSize> {
    match d {
        2 => Some(TickSize::Cent),
        3 => Some(TickSize::Milli),
        _ => None,
    }
}

/// Build a `"btc5m"`-tagged [`OrderRow`] (the FK parent the fills reference).
fn order_row(order: &Order, ts_ms: i64) -> OrderRow {
    OrderRow {
        id: order.id.to_string(),
        ts_ms,
        fingerprint: order.fingerprint.clone(),
        token: order.token.0 as i64,
        action: match order.action {
            Action::Buy => "Buy",
            Action::Sell => "Sell",
        }
        .into(),
        limit_ticks: i64::from(order.limit_px.get()),
        tick_levels: i64::from(order.ts.levels()),
        qty_micro: order.qty.0 as i64,
        strategy: "btc5m".into(),
    }
}

/// Phase-0/1 shadow + Phase-2 live-gated micro-taker. Generic over the taker
/// [`CopyVenue`] (async via RPITIT, so held by value, not `dyn`) exactly like
/// [`CopyStrategy`](crate::strategy::copy::CopyStrategy): main wires the real
/// `LiveVenue`, tests inject a mock, and `venue: None` ⇒ shadow-only. Holds
/// either a live `GammaClient` + `SpotFeed` (production) or a pre-seeded
/// window+spot (tests).
pub struct Btc5mStrategy<V: CopyVenue> {
    id: StrategyId,
    sample_ms: u64,
    /// HTTP client + CLOB REST base for polling the current window's tokens'
    /// `/book` directly (the dynamically-discovered 5m token is in neither the
    /// shared registry nor any WS supervisor, so `ctx.fetcher` can't see it).
    book_http: reqwest::Client,
    clob_base: String,
    gamma: Option<GammaClient>,
    slug_fn: Option<Box<dyn Fn(i64) -> String + Send>>,
    spot: Option<SpotFeed>,
    seed: Option<(GammaWindow, f64, f64)>, // (window, strike, spot); sigma via seed_sigma
    seed_sigma: f64,
    // ── Phase-2 (live-gated micro-taker) ───────────────────────────────────
    /// CAPITAL-CRITICAL master gate. `false` ⇒ shadow-only: the order path is
    /// unreachable regardless of the venue.
    live: bool,
    /// The taker venue (book reads via `ensure_token` + FAK). `None` ⇒ shadow
    /// (no orders). Task 7 wires the real `LiveVenue`; tests inject a mock.
    venue: Option<V>,
    /// Phase-2 entry tuning (window / z / edge / notional / caps).
    params: Btc5mParams,
    /// Signed-net + realized accounting for the taker fills — the realized seed
    /// the Task 6 settle sweep continues.
    inv: InventoryRisk,
    /// Cumulative-loss latch (Task 6 wires the mark that sets it). Gates entries.
    halted: bool,
    /// At most ONE entry per window; reset when the rotation adopts a new window.
    entered_this_window: bool,
    /// Current UTC day for the daily-notional cap (reset on rollover).
    day: i64,
    /// Taker notional deployed so far this UTC day (µUSDC).
    day_notional_micro: i64,
    /// TEST-ONLY leader ask (µUSDC) substituting for the public CLOB `/book`
    /// poll on the seed (no-network) path; `None` in production.
    seed_leader_ask: Option<i64>,
    // ── Phase-2 Task 6 (settlement) ─────────────────────────────────────────
    /// M6 deposit-wallet relayer for on-chain resolved-winner redemption; `None`
    /// until Task 7 wires it (mirrors copy's `relayer`). Redeem is best-effort.
    relayer: Option<Arc<RelayerClient>>,
    /// DB path for the STARTUP open-position reload (via [`ReadStore`]); `None`
    /// until Task 7 wires it (mirrors copy/mm's `store_path`).
    store_path: Option<PathBuf>,
    /// In-memory open-position BACKSTOP keyed by `(condition_id, outcome_index)`
    /// — the settle sweep's working set. Populated on entry ([`Self::try_enter`])
    /// and on the startup reload; a settled position is removed (idempotent: a
    /// key absent from `open` is never swept/booked again).
    open: HashMap<(String, i64), OpenPos>,
    /// Cumulative realized µUSDC THIS UTC day (resets on rollover). Latches
    /// `halted` once it reaches `-(max_daily_loss_usd × 1e6)` — the activation of
    /// the otherwise-inert `max_daily_loss_usd` cap.
    day_realized_micro: i64,
    /// TEST-ONLY resolved-outcome stub (`condition_id` → UP-won) consulted on the
    /// seed (no-gamma) path, mirroring `seed_leader_ask`. Empty in production.
    seed_outcomes: HashMap<String, bool>,
}

impl<V: CopyVenue> Btc5mStrategy<V> {
    /// Construct the production strategy. `venue` is `None` (Task 7 wires the
    /// real `LiveVenue`); pick `V` at the call site (e.g.
    /// `Btc5mStrategy::<LiveVenue>::new(..)`). With `venue: None` the order path
    /// stays unreachable whatever `live` is.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gamma: GammaClient,
        slug_fn: Box<dyn Fn(i64) -> String + Send>,
        spot: SpotFeed,
        sample_ms: u64,
        book_http: reqwest::Client,
        clob_base: String,
        params: Btc5mParams,
        live: bool,
    ) -> Self {
        Btc5mStrategy {
            id: StrategyId("btc5m"),
            sample_ms,
            book_http,
            clob_base,
            gamma: Some(gamma),
            slug_fn: Some(slug_fn),
            spot: Some(spot),
            seed: None,
            seed_sigma: 0.0,
            live,
            venue: None,
            inv: InventoryRisk::new(inv_config(&params)),
            halted: false,
            entered_this_window: false,
            day: utc_day_from_ms(Self::now_ms()),
            day_notional_micro: 0,
            seed_leader_ask: None,
            relayer: None,
            store_path: None,
            open: HashMap::new(),
            day_realized_micro: 0,
            seed_outcomes: HashMap::new(),
            params,
        }
    }

    /// Attach the M6 deposit-wallet relayer for on-chain resolved-winner
    /// redemption (Task 7 wiring; mirrors copy's relayer). Best-effort redeem.
    pub fn with_relayer(mut self, relayer: Arc<RelayerClient>) -> Self {
        self.relayer = Some(relayer);
        self
    }

    /// Set the DB path for the STARTUP open-position reload (Task 7 wiring;
    /// mirrors copy/mm's `with_store_path`), so a restart resumes settling
    /// unsettled positions instead of dropping them.
    pub fn with_store_path(mut self, store_path: PathBuf) -> Self {
        self.store_path = Some(store_path);
        self
    }

    /// Test constructor: a pre-seeded window+spot (no network), an injectable
    /// `venue`, the `live` gate, Phase-2 params, and an optional fake leader-ask
    /// standing in for the CLOB `/book` poll on the seed path.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_test(
        window: GammaWindow,
        strike: f64,
        spot: f64,
        sigma_1min: f64,
        sample_ms: u64,
        live: bool,
        venue: Option<V>,
        params: Btc5mParams,
        seed_leader_ask: Option<i64>,
    ) -> Self {
        Btc5mStrategy {
            id: StrategyId("btc5m"),
            sample_ms,
            // Never used in tests: the seed=Some guard below skips the book poll.
            book_http: reqwest::Client::new(),
            clob_base: String::new(),
            gamma: None,
            slug_fn: None,
            spot: None,
            seed: Some((window, strike, spot)),
            seed_sigma: sigma_1min,
            live,
            venue,
            inv: InventoryRisk::new(inv_config(&params)),
            halted: false,
            entered_this_window: false,
            day: utc_day_from_ms(Self::now_ms()),
            day_notional_micro: 0,
            seed_leader_ask,
            relayer: None,
            store_path: None,
            open: HashMap::new(),
            day_realized_micro: 0,
            seed_outcomes: HashMap::new(),
            params,
        }
    }

    fn now_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Place + book ONE marketable-FAK BUY for a decided [`Entry`] on the
    /// leader, recording the order row (FK parent), the signed fills into
    /// inventory and the durable store (the realized-accounting seed), and the
    /// open position. Enforces the daily-notional cap (conservative: keyed off
    /// the intended marketable cost, an upper bound on a FAK that fills at/under
    /// its limit). Mirrors the copy executor's `enter`/`book_fills`. Sets
    /// `entered_this_window` on a booked fill so the window can't re-enter.
    #[allow(clippy::too_many_arguments)]
    async fn try_enter(
        &mut self,
        store_tx: &tokio::sync::mpsc::Sender<StoreMsg>,
        win: &Window,
        up: bool,
        leader_token_str: &str,
        ts: TickSize,
        entry: Entry,
        now: i64,
    ) {
        // DAILY NOTIONAL CAP — check BEFORE any order I/O.
        let intended_cost = buy_cost(entry.limit_px.microusdc(ts), entry.qty).0;
        let cap_micro = (self.params.max_daily_notional_usd * 1_000_000.0) as i128;
        if i128::from(self.day_notional_micro) + intended_cost > cap_micro {
            tracing::info!(
                condition_id = %win.gamma.condition_id,
                day_notional_micro = self.day_notional_micro,
                intended_cost_micro = intended_cost as i64,
                "btc5m: daily notional cap reached — entry skipped"
            );
            return;
        }
        // Register the dynamically-discovered 5m token on the venue (it is in no
        // static universe) → the internal TokenId to trade + book it by.
        let token = match self
            .venue
            .as_mut()
            .and_then(|v| v.ensure_token(leader_token_str, false, ts))
        {
            Some(t) => t,
            None => {
                tracing::warn!(
                    condition_id = %win.gamma.condition_id,
                    "btc5m: venue could not register the leader token — entry skipped"
                );
                return;
            }
        };
        // Marketable buy: limit == the leader's ask (fills at/under, kills the rest).
        let order = Order::new(
            format!(
                "btc5m:{}:{}",
                win.gamma.condition_id,
                if up { 0 } else { 1 }
            ),
            token,
            Action::Buy,
            ts,
            entry.limit_px,
            entry.qty,
            Bps(0),
        );
        let outcome = match self.venue.as_mut() {
            Some(v) => match v.submit_fak(&order).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, condition_id = %win.gamma.condition_id, "btc5m: entry FAK rejected");
                    return;
                }
            },
            None => return,
        };
        if outcome.filled.0 == 0 {
            tracing::info!(condition_id = %win.gamma.condition_id, "btc5m: entry FAK filled nothing");
            return;
        }
        // Persist the order row (FK parent of the fills) BEFORE the signed fills.
        let _ = store_tx
            .send(StoreMsg::OrderInsert(order_row(&order, now), None))
            .await;
        // Book each fill into inventory + the durable store, accumulating the
        // realized delta (the seed Task 6's settle continues).
        let mut filled_micro: i128 = 0;
        let mut cash_total: i128 = 0;
        for f in &outcome.fills {
            let realized_before = self.inv.realized(token).0;
            self.inv.on_fill(token, f.qty.0 as i128, f.cash);
            let realized_delta = self.inv.realized(token).0 - realized_before;
            filled_micro += f.qty.0 as i128;
            cash_total += f.cash.0;
            let row = FillRow {
                order_id: order.id.to_string(),
                ts_ms: now,
                token: token.0 as i64,
                action: "Buy".into(),
                px_ticks: i64::from(f.px.get()),
                tick_levels: i64::from(ts.levels()),
                qty_micro: f.qty.0 as i64,
                cash_micro: usdc_to_i64(f.cash).unwrap_or(0),
                fee_micro: usdc_to_i64(f.fee).unwrap_or(0),
                strategy: "btc5m".into(),
            };
            let _ = store_tx.send(StoreMsg::FillSigned(row, None)).await;
            if realized_delta != 0 {
                let _ = store_tx
                    .send(StoreMsg::DayRealized {
                        utc_day: utc_day_from_ms(now),
                        strategy: "btc5m".into(),
                        delta_micro: realized_delta,
                    })
                    .await;
            }
        }
        if filled_micro <= 0 {
            return;
        }
        let cost_micro = (-cash_total).clamp(0, i128::from(i64::MAX));
        // Upsert the open position (the Task 6 settle sweep reads this row).
        // Awaited (not try_send): btc5m keeps no in-memory position backstop, so
        // a dropped upsert under backpressure would leave an on-chain fill with
        // no tracked row — an untracked orphan the Task 6 settle sweep never sees.
        let qty_micro = filled_micro.clamp(0, i128::from(i64::MAX)) as i64;
        let outcome_index = if up { 0 } else { 1 };
        let _ = store_tx
            .send(StoreMsg::Btc5mPositionUpsert(Btc5mPositionRow {
                condition_id: win.gamma.condition_id.clone(),
                outcome_index,
                token: leader_token_str.to_string(),
                qty_micro,
                cost_micro: cost_micro as i64,
                entry_ts: now,
                t_close_ms: win.gamma.t_close_ms,
                strike: win.strike,
            }))
            .await;
        // ALSO track it in the in-memory backstop so the settle sweep sees it
        // without waiting on a durable read-back (and a restart reload rebuilds
        // this map from the durable row).
        self.open.insert(
            (win.gamma.condition_id.clone(), outcome_index),
            OpenPos {
                token: leader_token_str.to_string(),
                qty_micro,
                cost_micro: cost_micro as i64,
                bought_up: up,
                t_close_ms: win.gamma.t_close_ms,
            },
        );
        self.day_notional_micro = self.day_notional_micro.saturating_add(cost_micro as i64);
        self.entered_this_window = true;
        tracing::info!(
            condition_id = %win.gamma.condition_id,
            outcome_index = if up { 0 } else { 1 },
            qty_micro = filled_micro as i64,
            cost_micro = cost_micro as i64,
            "btc5m: ENTERED (marketable FAK buy on the near-certain leader)"
        );
    }

    /// The Task-5 LIVE-ENTRY gate, extracted so both the run loop and the settle
    /// test read the same predicate: reachable ONLY when live, a venue is
    /// attached, this window hasn't entered, and the breaker isn't latched. The
    /// daily-loss halt (settle sweep) flips `halted`, so a breach blocks entries.
    fn entry_allowed(&self) -> bool {
        self.live && self.venue.is_some() && !self.entered_this_window && !self.halted
    }

    /// Reset the per-UTC-day counters on a day rollover — the daily-notional cap
    /// AND the daily-realized latch share one window. Idempotent within a day, so
    /// it is safe to call from BOTH the entry gate and the settle sweep (whichever
    /// fires first rolls; the other is a no-op).
    fn roll_day(&mut self, now: i64) {
        let today = utc_day_from_ms(now);
        if today != self.day {
            self.day = today;
            self.day_notional_micro = 0;
            self.day_realized_micro = 0;
        }
    }

    /// Resolve a held window's outcome: `Some(up_won)` if resolved, `None` if not
    /// yet (retry next sweep). Production reads Gamma via the retained client; the
    /// seed (no-network) test path reads the injected `seed_outcomes` stub.
    ///
    /// An ASSOCIATED fn taking only the two `Sync` pieces (`&GammaClient`,
    /// `&HashMap`) rather than `&self`, so its future stays `Send` — `&self` would
    /// require `Btc5mStrategy: Sync`, which the `Box<dyn Fn ..>` `slug_fn` (Send,
    /// not Sync) blocks.
    async fn window_outcome_for(
        gamma: Option<&GammaClient>,
        seed_outcomes: &HashMap<String, bool>,
        condition_id: &str,
        open_unix: i64,
    ) -> Option<bool> {
        match gamma {
            Some(g) => match g.window_outcome(open_unix).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, open_unix, "btc5m: window_outcome fetch failed — retry next sweep");
                    None
                }
            },
            None => seed_outcomes.get(condition_id).copied(),
        }
    }

    /// Book ONE resolved position: realized PnL → the day-realized ledger, close
    /// the durable row, drop it from the in-memory backstop, feed the daily-loss
    /// latch, and (winner + relayer present) redeem on-chain (best-effort). Called
    /// only for positions still present in `open`, and removes the key, so it can
    /// never double-book a position.
    async fn settle_one(
        &mut self,
        store_tx: &tokio::sync::mpsc::Sender<StoreMsg>,
        key: &(String, i64),
        pos: &OpenPos,
        outcome_up: bool,
        now: i64,
    ) {
        let realized = crate::strategy::btc5m::settle::realized_micro(
            outcome_up,
            pos.bought_up,
            pos.qty_micro,
            pos.cost_micro,
        );
        // Realized delta → the cumulative day-realized ledger (best-effort).
        let _ = store_tx
            .send(StoreMsg::DayRealized {
                utc_day: utc_day_from_ms(now),
                strategy: "btc5m".into(),
                delta_micro: realized as i128,
            })
            .await;
        // Close the durable row + drop the in-memory backstop entry. Removing the
        // key here is what makes settle IDEMPOTENT: a later sweep can't re-book it.
        let _ = store_tx
            .send(StoreMsg::Btc5mPositionClose {
                condition_id: key.0.clone(),
                outcome_index: key.1,
            })
            .await;
        self.open.remove(key);
        // DAILY-LOSS LATCH — this is what activates `max_daily_loss_usd`: once the
        // day's realized reaches the floor, latch `halted` (the entry gate blocks).
        self.day_realized_micro = self.day_realized_micro.saturating_add(realized);
        let loss_floor = -((self.params.max_daily_loss_usd * 1_000_000.0) as i64);
        if self.day_realized_micro <= loss_floor {
            self.halted = true;
            tracing::warn!(
                day_realized_micro = self.day_realized_micro,
                loss_floor_micro = loss_floor,
                "btc5m: daily-loss cap breached — halting new entries for the session"
            );
        }
        tracing::info!(
            condition_id = %key.0,
            outcome_index = key.1,
            token = %pos.token,
            outcome_up,
            realized_micro = realized,
            "btc5m: SETTLED position (Gamma outcome → realized PnL)"
        );
        // Redeem winners on-chain (best-effort; off the decision path). btc5m
        // markets are NOT neg-risk. A parse/relayer failure only logs — the PnL is
        // already booked locally.
        let won = outcome_up == pos.bought_up;
        if won && let Some(relayer) = self.relayer.clone() {
            match key.0.parse::<B256>() {
                Ok(condition) => {
                    if let Err(e) = relayer.redeem(condition, false).await {
                        tracing::warn!(error = %e, condition_id = %key.0, "btc5m: winner redeem failed (booked locally; reconcile later)");
                    } else {
                        tracing::info!(condition_id = %key.0, "btc5m: redeemed resolved winner");
                    }
                }
                Err(_) => tracing::warn!(condition_id = %key.0, "btc5m: winning condition_id did not parse as B256 — redeem skipped"),
            }
        }
    }

    /// Sweep the in-memory `open` map: for each position whose window has resolved
    /// (past `t_close_ms + SETTLE_DELAY_MS`) and whose Gamma outcome is known,
    /// settle it. Unresolved windows are left for the next sweep. Snapshots the
    /// ready keys FIRST (so the per-position async outcome fetch + mutation don't
    /// hold a borrow of `open`).
    async fn settle_sweep(&mut self, store_tx: &tokio::sync::mpsc::Sender<StoreMsg>, now: i64) {
        self.roll_day(now);
        let ready: Vec<((String, i64), OpenPos)> = self
            .open
            .iter()
            .filter(|(_, p)| now >= p.t_close_ms + SETTLE_DELAY_MS)
            .map(|(k, p)| (k.clone(), p.clone()))
            .collect();
        for (key, pos) in ready {
            let open_unix = pos.t_close_ms / 1000 - 300;
            // Borrow only the two `Sync` fields for the fetch (NOT `&self`), so the
            // future is `Send`; the borrows end at the await, freeing `&mut self`.
            let outcome =
                Self::window_outcome_for(self.gamma.as_ref(), &self.seed_outcomes, &key.0, open_unix)
                    .await;
            if let Some(outcome_up) = outcome {
                self.settle_one(store_tx, &key, &pos, outcome_up, now).await;
            }
        }
    }

    /// RELOAD persisted open positions at startup so a restart RESUMES settling
    /// them instead of orphaning an on-chain fill with no tracked row. Rebuilds the
    /// in-memory `open` backstop from the durable rows; a read failure just starts
    /// flat (prior behavior). Mirrors copy's `reload_positions` (but btc5m settle
    /// never trades the token, so no venue re-registration is needed).
    fn reload_open(&mut self, read: &ReadStore) {
        let rows = match read.btc5m_open_positions() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "btc5m: position reload read failed — starting flat");
                return;
            }
        };
        let restored = rows.len();
        for row in rows {
            self.open.insert(
                (row.condition_id.clone(), row.outcome_index),
                OpenPos {
                    token: row.token,
                    qty_micro: row.qty_micro,
                    cost_micro: row.cost_micro,
                    bought_up: row.outcome_index == 0,
                    t_close_ms: row.t_close_ms,
                },
            );
        }
        if restored > 0 {
            tracing::info!(restored, "btc5m: reloaded open positions from the store — resuming settlement (restart-safe)");
        }
    }
}

impl<V: CopyVenue + 'static> Strategy for Btc5mStrategy<V> {
    fn id(&self) -> StrategyId {
        self.id
    }

    fn make_on_apply(&self) -> Option<pm_ingestion::supervisor::OnApplyFn> {
        None
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // `registry`/`fetcher` intentionally dropped: they only know the
            // static arb/mm/copy token universe + its WS supervisors, never the
            // dynamically-discovered 5m window token (see the CLOB poll below).
            let StrategyCtx {
                store_tx,
                kill,
                mut ctl_rx,
                status_tx,
                ..
            } = ctx;
            let mut paused = false;
            let mut rot = Rotation::default();
            let mut me = *self;

            if let Some((w, strike, _spot)) = me.seed.clone() {
                rot.adopt(w, strike);
            }

            // RESTART-SAFETY: reload persisted open positions BEFORE the first
            // sweep so a restart RESUMES settling them instead of orphaning an
            // on-chain fill with no tracked row. `store_path` is `None` until Task
            // 7 wires it (like `relayer`), so this is inert until then.
            if let Some(read) = me.store_path.as_deref().and_then(|p| ReadStore::open(p).ok()) {
                me.reload_open(&read);
            }

            let mut tick = tokio::time::interval(Duration::from_millis(me.sample_ms.max(1)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut gamma_poll = tokio::time::interval(Duration::from_millis(1000));
            gamma_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
            // SETTLE sweep on a slow sub-cadence (resolution finality is minutes,
            // not seconds). Fires immediately on the first tick, then every 30s.
            let mut settle_poll = tokio::time::interval(Duration::from_millis(30_000));
            settle_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                if kill.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = gamma_poll.tick() => {
                        // A new window (conditionId change) → re-arm the per-window
                        // entry gate. Computed inside the immutable borrow, applied
                        // after it ends.
                        let mut rotated = false;
                        if let (Some(g), Some(sf), Some(slug_fn)) = (me.gamma.as_ref(), me.spot.as_ref(), me.slug_fn.as_ref()) {
                            let now = Self::now_ms();
                            let slug = slug_fn(now);
                            if let Ok(Some(w)) = g.current_window(&slug).await {
                                rotated = rot.adopt(w, sf.latest().price);
                            }
                        }
                        if rotated { me.entered_this_window = false; }
                    }
                    _ = settle_poll.tick() => {
                        // Settle any resolved held positions (book realized PnL,
                        // close, redeem winners, feed the daily-loss latch). Runs
                        // regardless of `live` — held positions must always settle.
                        me.settle_sweep(&store_tx, Self::now_ms()).await;
                    }
                    _ = tick.tick() => {
                        if paused { continue; }
                        let now = Self::now_ms();
                        let win = match rot.current() { Some(w) => w.clone(), None => continue };
                        let (spot, sigma_1min, vol_ready) = match (me.spot.as_ref(), &me.seed) {
                            (Some(sf), _) => { let s = sf.latest(); (s.price, s.sigma_1min, s.vol_ready) }
                            (None, Some((_, _, seed_spot))) => (*seed_spot, me.seed_sigma, true),
                            _ => continue,
                        };
                        if !vol_ready || !spot.is_finite() { continue; }
                        let secs = win.secs_to_go(now);
                        let sigma_tau = sigma_1min * ((secs.max(0) as f64) / 60.0).sqrt();
                        let p_up = match fair_p_up(spot, win.strike, secs as f64, sigma_1min) { Some(p) => p, None => continue };

                        // Poll the public CLOB /book directly for the rotating
                        // window's YES token — it is discovered dynamically via
                        // Gamma, so `ctx.fetcher`/`registry` (static universe + WS
                        // supervisors) can't see it. µUSDC comes straight from the
                        // parse. READ-ONLY sampling. Tests (seed=Some) skip the
                        // network so the book stays 0; the row still logs fair value.
                        let (mut bid_micro, mut ask_micro) = (0i64, 0i64);
                        if me.seed.is_none()
                            && let Ok((bid, ask)) = pm_ingestion::clob::fetch_book_best(
                                &me.book_http,
                                &me.clob_base,
                                &win.gamma.yes_token,
                            )
                            .await
                        {
                            bid_micro = bid.unwrap_or(0);
                            ask_micro = ask.unwrap_or(0);
                        }

                        // SHADOW write — UNCONDITIONAL, unchanged (Phase-0/1).
                        let sample = ShadowSample {
                            ts_ms: now, condition_id: win.gamma.condition_id.clone(), secs_to_go: secs,
                            strike: win.strike, spot, sigma_tau, p_up,
                            best_bid_micro: bid_micro, best_ask_micro: ask_micro,
                            tick_decimals: win.gamma.tick_decimals,
                        };
                        let _ = store_tx.try_send(StoreMsg::Btc5mShadow(sample.into_row()));
                        let _ = status_tx.send(StrategyStatus { paused, open_positions: 0, ..Default::default() });

                        // ── Phase-2 LIVE ENTRY (capital-critical; gated) ────────
                        // Reachable ONLY when live, a venue is attached, this
                        // window hasn't entered, and the breaker isn't latched.
                        if me.entry_allowed() {
                            // Daily cap window: reset on UTC-day rollover (notional
                            // + realized share the window via `roll_day`).
                            me.roll_day(now);

                            if let Some(ts) = tick_from_decimals(win.gamma.tick_decimals) {
                                // Leader = the side the spot deviation favors. z's
                                // sign picks it; a non-finite z (σ_τ → 0) is rejected
                                // by `decide_entry`.
                                let z = (spot - win.strike) / sigma_tau;
                                let up = z > 0.0;
                                let leader_token_str = if up { win.gamma.yes_token.clone() } else { win.gamma.no_token.clone() };
                                let fair_leader = if up { p_up } else { 1.0 - p_up };
                                // LEADER token's ask: the fake seed hook (tests) or
                                // the public CLOB /book (production — the dynamic
                                // token isn't on the venue for a book read).
                                let leader_ask = if me.seed.is_some() {
                                    me.seed_leader_ask
                                } else {
                                    pm_ingestion::clob::fetch_book_best(&me.book_http, &me.clob_base, &leader_token_str)
                                        .await
                                        .ok()
                                        .and_then(|(_, ask)| ask)
                                };
                                if let Some(leader_ask) = leader_ask {
                                    let params = EntryParams {
                                        entry_window_secs: me.params.entry_window_secs,
                                        z_threshold: me.params.z_threshold,
                                        edge_buffer: me.params.edge_buffer_c,
                                        fee_rate: CRYPTO_FEE_RATE,
                                        notional_usd: me.params.entry_notional_usd,
                                    };
                                    if let Some(entry) = decide_entry(secs, z, fair_leader, leader_ask, ts, params) {
                                        me.try_enter(&store_tx, &win, up, &leader_token_str, ts, entry, now).await;
                                    }
                                }
                            }
                        }
                    }
                    cmd = ctl_rx.recv() => match cmd {
                        Some(StrategyCommand::SetPaused(p)) => paused = p,
                        Some(StrategyCommand::VetoQuote { .. }) => {}
                        None => break,
                    },
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use pm_core::instrument::TokenId;
    use pm_core::num::{Px, Usdc, buy_cost};
    use pm_engine::Action;
    use pm_execution::Order;
    use pm_execution::venue::{Fill, SubmitOutcome, VenueError};
    use pm_store::Store;
    use pm_store::writer::{StoreMsg, run_writer};
    use tokio::sync::{mpsc, watch};

    use super::*;
    use crate::strategy::copy::CopyVenue;
    use crate::strategy::{StrategyCommand, StrategyCtx, StrategyStatus};
    use crate::wiring::BookFetcher;

    /// Recorded `(token, action, limit_ticks, qty_micro)` per submitted FAK.
    type OrderLog = Arc<Mutex<Vec<(TokenId, Action, u16, u64)>>>;

    /// A mock taker [`CopyVenue`] (modeled on copy.rs's): `ensure_token` maps any
    /// leader string → a fixed internal `TokenId`, `submit_fak` records the order
    /// and fully fills at its (marketable) limit. `best_ask`/`best_bid` are unused
    /// (the seed supplies the leader ask).
    struct MockVenue {
        token: TokenId,
        orders: OrderLog,
    }

    impl CopyVenue for MockVenue {
        async fn best_ask(&mut self, _t: TokenId, _ts: TickSize) -> Option<Px> {
            None
        }
        async fn best_bid(&mut self, _t: TokenId, _ts: TickSize) -> Option<Px> {
            None
        }
        fn ensure_token(&mut self, _venue_id: &str, _neg_risk: bool, _ts: TickSize) -> Option<TokenId> {
            Some(self.token)
        }
        async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
            self.orders.lock().unwrap().push((
                order.token,
                order.action,
                order.limit_px.get(),
                order.qty.0,
            ));
            let px_micro = order.limit_px.microusdc(order.ts);
            let cash = Usdc(-buy_cost(px_micro, order.qty).0);
            Ok(SubmitOutcome {
                fills: vec![Fill {
                    px: order.limit_px,
                    qty: order.qty,
                    cash,
                    fee: Usdc(0),
                }],
                filled: order.qty,
                venue_order_id: None,
            })
        }
    }

    fn ctx_with(
        store_tx: mpsc::Sender<StoreMsg>,
        kill: Arc<AtomicBool>,
        ctl_rx: mpsc::Receiver<StrategyCommand>,
        status_tx: watch::Sender<StrategyStatus>,
    ) -> StrategyCtx {
        StrategyCtx {
            registry: Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap()),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx,
            kill,
            ctl_rx,
            status_tx,
        }
    }

    fn window(t_close_ms: i64) -> GammaWindow {
        GammaWindow {
            condition_id: "C".into(),
            yes_token: "999".into(),
            no_token: "998".into(),
            tick_decimals: 2,
            t_open_ms: 0,
            t_close_ms,
        }
    }

    /// REGRESSION: the shadow loop still writes a row, AND with `live == false`
    /// (even with a venue present and a leader ask that WOULD trigger an entry)
    /// the order path is unreachable — NO `submit_fak` is called.
    #[tokio::test]
    async fn shadow_loop_writes_a_row_and_no_orders() {
        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let (store_tx, mut store_rx) = mpsc::channel::<StoreMsg>(16);
        let ctx = ctx_with(store_tx, Arc::clone(&kill), ctl_rx, status_tx);

        let orders: OrderLog = Arc::new(Mutex::new(Vec::new()));
        let venue = MockVenue { token: TokenId(7), orders: Arc::clone(&orders) };
        // live = FALSE + a venue present + the SAME fresh in-window/cheap-ask
        // setup as `live_places_one_fak_and_records_the_position` below (spot
        // above strike, |z| ≥ threshold, a leader ask ⇒ `decide_entry` returns
        // Some and WOULD place an order) ⇒ `live` is the SOLE reason the order
        // path stays unreachable (the gate short-circuits before the venue is
        // ever touched).
        let now = crate::coordinator::now_ms();
        let strat = Btc5mStrategy::new_for_test(
            window(now + 15_000),
            62_900.0,
            62_940.0,
            40.0,
            5,
            false,
            Some(venue),
            Btc5mParams::default(),
            Some(900_000),
        );
        let run = tokio::spawn(Box::new(strat).run(ctx));
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), store_rx.recv())
            .await
            .expect("no store message within timeout")
            .expect("store sender dropped");
        match msg {
            StoreMsg::Btc5mShadow(r) => {
                assert_eq!(r.condition_id, "C");
                assert!(r.p_up > 0.5, "spot above strike ⇒ p_up > 0.5, got {}", r.p_up);
                assert_eq!(r.best_ask_micro, 0, "unknown token ⇒ no book");
            }
            _ => panic!("expected StoreMsg::Btc5mShadow, got a different variant"),
        }
        kill.store(true, Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .unwrap()
            .unwrap();
        assert!(
            orders.lock().unwrap().is_empty(),
            "live=false ⇒ NO taker order may be placed"
        );
    }

    /// LIVE: a near-certain, cheap, late leader ⇒ exactly ONE marketable FAK BUY
    /// on the correct token/side/qty, the fill booked into the store, and the
    /// open position upserted. Drives the full seed-mode loop over a REAL
    /// in-memory store + writer (the OrderInsert→FillSigned FK path end-to-end).
    #[tokio::test]
    async fn live_places_one_fak_and_records_the_position() {
        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        // A file-backed store so a second `ReadStore` connection can read the
        // upserted position back (an in-memory DB is private per connection).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("btc5m.sqlite");
        let store = Store::open(&path).unwrap();
        let (store_tx, store_rx) = mpsc::channel::<StoreMsg>(256);
        let writer = tokio::spawn(run_writer(store, store_rx));
        let ctx = ctx_with(store_tx, Arc::clone(&kill), ctl_rx, status_tx);

        let orders: OrderLog = Arc::new(Mutex::new(Vec::new()));
        let venue = MockVenue { token: TokenId(7), orders: Arc::clone(&orders) };
        // Window closes ~15s out ⇒ secs_to_go ∈ (0, 20]. spot 62_940 vs strike
        // 62_900, σ₁ = 40 ⇒ σ_τ = 40·√(15/60) = 20, z = +2.0 ≥ 1.5 ⇒ UP leads.
        // fair_up = Φ(2) ≈ 0.977; leader ask 0.90 ⇒ net edge ≫ buffer.
        let now = crate::coordinator::now_ms();
        let strat = Btc5mStrategy::new_for_test(
            window(now + 15_000),
            62_900.0,
            62_940.0,
            40.0,
            5,
            true,
            Some(venue),
            Btc5mParams::default(),
            Some(900_000),
        );
        let run = tokio::spawn(Box::new(strat).run(ctx));

        // Poll for the FAK (deterministic: the first in-window tick enters, then
        // `entered_this_window` blocks any repeat). Never hold the lock across await.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !orders.lock().unwrap().is_empty() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "no FAK submitted within timeout");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        kill.store(true, Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .unwrap()
            .unwrap();

        // Exactly ONE order: BUY the UP/YES token (internal TokenId(7)) at 90¢ for
        // floor($10 / $0.90) = 11 shares.
        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![(TokenId(7), Action::Buy, 90, 11_000_000)],
            "one marketable FAK buy on the leader"
        );

        // Recover the store: the loop's sender is the only one, so awaiting the
        // writer drains every buffered message first.
        let store = writer.await.unwrap();
        assert_eq!(
            store.count_fills().unwrap(),
            1,
            "the buy persisted via the signed route (OrderInsert→FillSigned FK ok)"
        );
        drop(store); // flush + release the write connection before reading back.
        let rs = pm_store::read::ReadStore::open(&path).unwrap();
        let positions = rs.btc5m_open_positions().unwrap();
        assert_eq!(positions.len(), 1, "one open btc5m position recorded");
        let p = &positions[0];
        assert_eq!(p.condition_id, "C");
        assert_eq!(p.outcome_index, 0, "z>0 ⇒ bought UP (outcome 0)");
        assert_eq!(p.token, "999", "the UP/YES CLOB token id string");
        assert_eq!(p.qty_micro, 11_000_000);
        assert_eq!(p.cost_micro, 9_900_000, "11 sh × $0.90 = $9.90 cost");
    }

    /// SETTLE SWEEP: two resolved positions past their close — one WIN, one LOSS —
    /// settled via the STUBBED outcome hook (`gamma == None` ⇒ `seed_outcomes`),
    /// over a REAL in-memory store + writer. Asserts each books the correct
    /// `DayRealized` delta (win = +(qty−cost), loss = −cost), each is closed
    /// (removed from BOTH the durable table AND the in-memory `open` backstop),
    /// and the loss breaching `max_daily_loss_usd` latches `halted` so the entry
    /// gate blocks a follow-up entry. `relayer == None` ⇒ no on-chain redeem (no
    /// network); the venue is present only to prove `entry_allowed()` toggles on
    /// the halt (not on the venue), and that settle places NO orders.
    #[tokio::test]
    async fn settle_sweep_books_win_and_loss_and_halts_on_breach() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("btc5m_settle.sqlite");
        let store = Store::open(&path).unwrap();
        let (store_tx, store_rx) = mpsc::channel::<StoreMsg>(256);
        let writer = tokio::spawn(run_writer(store, store_rx));

        let now = crate::coordinator::now_ms();
        let t_close = now - 300_000; // well past close + SETTLE_DELAY_MS (120s).
        // max_daily_loss_usd = $5 ⇒ floor −$5M; the single $9.90 loss breaches it.
        let params = Btc5mParams { max_daily_loss_usd: 5.0, ..Btc5mParams::default() };

        let orders: OrderLog = Arc::new(Mutex::new(Vec::new()));
        let venue = MockVenue { token: TokenId(7), orders: Arc::clone(&orders) };
        // live + venue present so `entry_allowed()` reflects ONLY `halted` here.
        let mut me = Btc5mStrategy::new_for_test(
            window(t_close), 62_900.0, 62_940.0, 40.0, 5, true, Some(venue), params, None,
        );
        assert!(me.entry_allowed(), "pre-settle: entry gate open (live + venue, not halted)");

        // Seed the durable table (so the close DELETE is observable) AND the
        // in-memory backstop with a WIN (bought UP, UP won) and a LOSS (bought UP,
        // DOWN won). Distinct condition_ids so the stub can decide each side.
        for (cid, up_won) in [("CW", true), ("CL", false)] {
            store_tx
                .send(StoreMsg::Btc5mPositionUpsert(Btc5mPositionRow {
                    condition_id: cid.into(),
                    outcome_index: 0,
                    token: "999".into(),
                    qty_micro: 11_000_000,
                    cost_micro: 9_900_000,
                    entry_ts: now,
                    t_close_ms: t_close,
                    strike: 62_900.0,
                }))
                .await
                .unwrap();
            me.open.insert(
                (cid.to_string(), 0),
                OpenPos {
                    token: "999".into(),
                    qty_micro: 11_000_000,
                    cost_micro: 9_900_000,
                    bought_up: true,
                    t_close_ms: t_close,
                },
            );
            me.seed_outcomes.insert(cid.to_string(), up_won);
        }
        assert_eq!(me.open.len(), 2);

        // DRIVE THE SWEEP directly (gamma=None ⇒ the seed_outcomes stub decides).
        me.settle_sweep(&store_tx, now).await;

        // Both settled + removed from the in-memory backstop (idempotent close).
        assert!(me.open.is_empty(), "both positions removed from `open` after settle");
        // Win +$1.10 + loss −$9.90 = −$8.80, and −$8.80 ≤ −$5 floor ⇒ halt latched.
        assert_eq!(me.day_realized_micro, -8_800_000, "cumulative day realized = win − loss");
        assert!(me.halted, "daily-loss cap breach latches `halted`");
        assert!(!me.entry_allowed(), "the latched halt now BLOCKS a follow-up entry");
        assert!(orders.lock().unwrap().is_empty(), "settle places NO orders");

        // Drain the writer (the loop's sender is the only one) and read back.
        drop(store_tx);
        let store = writer.await.unwrap();
        drop(store); // flush + release the write connection before reading.
        let rs = pm_store::read::ReadStore::open(&path).unwrap();
        assert!(
            rs.btc5m_open_positions().unwrap().is_empty(),
            "both durable positions closed (deleted) by the settle sweep"
        );
        let day = pm_store::utc_day_from_ms(now);
        assert_eq!(
            rs.day_realized_micro("btc5m", day).unwrap(),
            -8_800_000,
            "the day-realized ledger accumulated both settle deltas (+1.10 − 9.90)"
        );
    }
}
