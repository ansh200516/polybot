//! `CopyStrategy` — the smart-money COPY executor (Task C4).
//!
//! It runs in the [`StrategyHost`](super::host::StrategyHost) exactly like the
//! market maker ([`MmStrategy`](super::mm::MmStrategy)): it owns a capital
//! envelope, honors `ctx.kill` + the per-strategy `ctl_rx` pause/kill controls,
//! publishes a [`StrategyStatus`], and drives its own async loop on a cadence.
//!
//! What it does:
//!
//! 1. **Whitelist refresh** — every `whitelist_refresh` it rebuilds the follow
//!    whitelist of skilled traders via [`refresh_whitelist`], reusing C1's
//!    shared, validated [`pm_ingestion::smart_money`] ranking
//!    ([`Ranking::EdgePerBet`] over each trader's whole resolved record). A
//!    refresh that hits ANY fetch error keeps the PRIOR whitelist (never trade
//!    on an empty/stale-failed set).
//! 2. **Signal poll → TRADE** — every `signal_poll` (when NOT paused AND a venue
//!    is present) it polls each whitelisted wallet's recent `/trades`, runs the
//!    pure [`select_signals`], and for each fresh candidate places a
//!    FRESHNESS-GATED taker FAK BUY ([`within_drift`] vs the live ask) sized by
//!    the per-position / capital / gross caps + the 5-share floor
//!    ([`copy_position_size_micro`]), booking the fill into [`InventoryRisk`] +
//!    the durable store. Each cycle it also SWEEPS the open positions for exits:
//!    resolution-redeem (held to settle → the M6 [`RelayerClient`]), follow-exit
//!    ([`should_follow_exit`] — the source trader SOLD), and stop-loss
//!    ([`stop_loss_hit`] — marked down past the cap).
//!
//! Order I/O is threaded as `Option` fields — the taker [`CopyVenue`] and the
//! [`RelayerClient`] — so the strategy is fully INERT with no venue
//! (paper-without-venue) and while paused: it places NO orders. The pure
//! deciders are the unit-tested core; main (C5) wires the live venue/relayer, the
//! capital carve, and the TUI. OFF by default; `start_paused` when live-held.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use alloy_primitives::B256;
use pm_core::instrument::TokenId;
use pm_core::num::{Bps, ONE_USDC_MICRO, Px, Qty, TickSize, Usdc};
use pm_engine::{Action, GasTable};
use pm_execution::Order;
use pm_execution::relayer::RelayerClient;
use pm_execution::venue::{BookSource, ExecutionVenue, PaperVenue, SubmitOutcome, VenueError};
use pm_ingestion::data_api::{
    ClosedPos, DataApiClient, LeaderboardEntry, OrderBy, TimePeriod, Trade, TradeSide, TradesFilter,
};
use pm_ingestion::smart_money::{Ranking, rank_wallets_oos, trader_records, within_drift};
use pm_ingestion::supervisor::OnApplyFn;
use pm_risk::inventory::{InventoryConfig, InventoryRisk};
use pm_store::writer::StoreMsg;
use pm_store::{FillRow, OrderRow, usdc_to_i64, utc_day_from_ms};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::coordinator::now_ms;

use super::{CopyStatus, Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};

/// Max fills pulled from each whitelisted wallet's `/trades?user=` history when
/// RANKING (the 6-hourly whitelist refresh). Matches the backtest's
/// `DEFAULT_TRADE_LIMIT` so the live whitelist ranks on the same depth of
/// history the offline grid validated.
const WHITELIST_TRADE_LIMIT: usize = 1000;

/// Max fills pulled from each whitelisted wallet's `/trades?user=` history on
/// the fast SIGNAL poll. We only care about buys inside the reaction window, so
/// one page (the most recent fills) is plenty and keeps the poll light.
const POLL_TRADE_LIMIT: usize = 100;

// ---------------------------------------------------------------------------
// CopyParams — the resolved `[copy]` knobs + capital envelope
// ---------------------------------------------------------------------------

/// The runtime copy-strategy parameters, resolved from `[copy]` +
/// `[strategies.copy]` config (USD notionals converted to µUSDC here; the
/// cadence seconds converted to [`Duration`]). The seam main's wiring uses,
/// mirroring [`MmParams::from_config`](super::mm::MmParams::from_config).
///
/// Most fields are alpha/risk knobs the C4 execution path will consume; C3 (the
/// read-only scaffold) reads only `top_n` / `min_bets` (ranking),
/// `reaction_window_secs` (freshness), and the two cadences. They are all kept
/// here now so C4 can wire sizing/exits without re-plumbing config.
#[derive(Debug, Clone)]
pub struct CopyParams {
    /// Notional opened per copied position, µUSDC (C4 sizing).
    pub per_position_micro: i128,
    /// Cap on simultaneously-open copied positions (C4 gating).
    pub max_concurrent_positions: u32,
    /// Gross exposure cap across open copied positions, µUSDC (C4 gating).
    pub max_gross_micro: i128,
    /// Cut a copied position at this unrealized-loss fraction, in (0, 1] (C4).
    pub stop_loss_pct: f64,
    /// FRESHNESS cap: skip the copy if OUR entry price drifts more than this
    /// fraction off the trader's fill (see
    /// [`pm_ingestion::smart_money::within_drift`]) — C4 entry gate.
    pub max_drift: f64,
    /// Copy a buy only within this many seconds of the trader's fill — the
    /// reaction window applied by [`select_signals`].
    pub reaction_window_secs: i64,
    /// Minimum resolved pre-cutoff bets to rank a trader
    /// ([`rank_wallets_oos`]'s `min_bets`).
    pub min_bets: usize,
    /// Follow-whitelist size: the top-N ranked traders to copy.
    pub top_n: usize,
    /// How often to rebuild the follow whitelist.
    pub whitelist_refresh: Duration,
    /// How often to poll the whitelist's recent trades for fresh signals.
    pub signal_poll: Duration,
    /// Sell our copied position when the copied trader sells out (C4 exit).
    pub follow_exit: bool,
    /// Strategy capital envelope, µUSDC (carved out of the bankroll by main).
    pub capital: Usdc,
}

impl CopyParams {
    /// Resolve `[copy]` ([`pm_config::CopyParamsCfg`]) + `[strategies.copy]`
    /// ([`pm_config::CopyCfg`]) into runtime params. USD notionals (per-position,
    /// gross, capital) are converted to µUSDC here; the cadence seconds become
    /// [`Duration`]s. Mirrors how [`MmParams::from_config`](super::mm::MmParams::from_config)
    /// is built from the config.
    pub fn from_config(
        copy: &pm_config::CopyCfg,
        params: &pm_config::CopyParamsCfg,
    ) -> Result<Self, pm_config::ConfigError> {
        Ok(CopyParams {
            per_position_micro: pm_config::usd_to_microusdc(params.per_position_usd)?,
            max_concurrent_positions: params.max_concurrent_positions,
            max_gross_micro: pm_config::usd_to_microusdc(params.max_gross_usd)?,
            stop_loss_pct: params.stop_loss_pct,
            max_drift: params.max_drift,
            reaction_window_secs: params.reaction_window_secs,
            min_bets: params.min_bets,
            top_n: params.top_n,
            whitelist_refresh: Duration::from_secs(params.whitelist_refresh_secs),
            signal_poll: Duration::from_secs(params.signal_poll_secs),
            follow_exit: params.follow_exit,
            capital: Usdc(pm_config::usd_to_microusdc(copy.capital_usd)?),
        })
    }
}

// ---------------------------------------------------------------------------
// Pure signal selection (the TDD'd core — no async, no I/O)
// ---------------------------------------------------------------------------

/// One candidate copy signal: a fresh, not-yet-acted BUY by a whitelisted
/// trader that C4 will (later) mirror. C3 only logs it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CopyCandidate {
    /// The market (Polymarket `conditionId`) to copy into.
    pub condition_id: String,
    /// The outcome index within that market the trader bought (their side).
    pub outcome_index: i64,
    /// The whitelisted wallet whose (earliest qualifying) buy this represents.
    pub trader: String,
    /// The trader's fill price — our FRESHNESS reference (C4 `within_drift`).
    pub trigger_px: f64,
    /// The trader's fill time (Unix seconds) — the earliest qualifying buy.
    pub timestamp: i64,
}

/// k=1 fresh-buy selection: scan every whitelisted wallet's recent trades and
/// turn each FRESH, not-yet-acted BUY into at most ONE [`CopyCandidate`] per
/// `(condition_id, outcome_index)`.
///
/// A trade qualifies iff ALL hold:
/// * it is a `BUY` (a `SELL` is the trader exiting — not a copy entry);
/// * it is FRESH: `timestamp >= now - reaction_window_secs` (a stale signal,
///   older than the reaction window, is dropped — we'd be chasing);
/// * its `(condition_id, outcome_index)` is NOT in `seen` (the set of keys we
///   have already acted on this session), so a signal never re-fires;
/// * the wallet is in `whitelist` (non-whitelisted wallets are ignored even if
///   `recent_by_wallet` carries them).
///
/// DE-DUP: when several qualifying buys share a `(condition_id, outcome_index)`
/// — whether two wallets on the same side or one wallet twice — only ONE
/// candidate is emitted, the EARLIEST by `timestamp` (ties broken by whitelist
/// order: the first listed wallet wins). The result is returned in a fully
/// DETERMINISTIC order (`timestamp`, then `condition_id`, then `outcome_index`),
/// independent of `HashMap` iteration order, so the live loop and the tests see
/// a stable sequence.
pub(crate) fn select_signals(
    recent_by_wallet: &HashMap<String, Vec<Trade>>,
    whitelist: &[String],
    seen: &HashSet<(String, i64)>,
    now: i64,
    reaction_window_secs: i64,
) -> Vec<CopyCandidate> {
    // Earliest qualifying buy per (condition_id, outcome_index). Iterating the
    // whitelist in order makes the equal-timestamp tie-break deterministic
    // (first-listed wallet wins).
    let mut best: HashMap<(String, i64), CopyCandidate> = HashMap::new();
    let stale_before = now - reaction_window_secs;
    for wallet in whitelist {
        let Some(trades) = recent_by_wallet.get(wallet) else {
            continue;
        };
        for t in trades {
            if t.side != TradeSide::Buy {
                continue;
            }
            if t.timestamp < stale_before {
                continue; // older than the reaction window → stale
            }
            let key = (t.condition_id.clone(), t.outcome_index);
            if seen.contains(&key) {
                continue; // already acted on this market+side
            }
            let replace = match best.get(&key) {
                Some(existing) => t.timestamp < existing.timestamp,
                None => true,
            };
            if replace {
                best.insert(
                    key,
                    CopyCandidate {
                        condition_id: t.condition_id.clone(),
                        outcome_index: t.outcome_index,
                        trader: wallet.clone(),
                        trigger_px: t.price,
                        timestamp: t.timestamp,
                    },
                );
            }
        }
    }
    let mut out: Vec<CopyCandidate> = best.into_values().collect();
    out.sort_by(|a, b| {
        a.timestamp
            .cmp(&b.timestamp)
            .then_with(|| a.condition_id.cmp(&b.condition_id))
            .then(a.outcome_index.cmp(&b.outcome_index))
    });
    out
}

/// Derive `conditionId → winning_outcome_index` from a set of closed positions
/// — the SIMPLER live fallback for the independent Gamma resolutions the
/// backtest fetches (`pm_app` does not depend on `pm-backtest`, and that crate's
/// `gamma_resolutions` is geared to an on-disk cache, so it is not cleanly
/// callable from the live path).
///
/// For a binary market a single [`ClosedPos`] settles it: the held side won
/// (`won()`) ⇒ the winner IS that `outcome_index`; otherwise the *other* side
/// won ⇒ `1 - outcome_index`. Every trader who closed the same binary market
/// agrees, so the map is independent of fold order. Mirrors the backtest's own
/// `fold_resolutions` (the pre-FIX-A resolution source).
fn fold_resolutions(closed: &[ClosedPos]) -> HashMap<String, i64> {
    let mut resolutions: HashMap<String, i64> = HashMap::new();
    for cp in closed {
        let winning_outcome_index = if cp.won() {
            cp.outcome_index
        } else {
            1 - cp.outcome_index
        };
        resolutions.insert(cp.condition_id.clone(), winning_outcome_index);
    }
    resolutions
}

/// Build the trader universe from two leaderboard slices: month-slice rows
/// first, then all-time, de-duped by `proxyWallet` (first occurrence wins, so
/// the order is stable). Mirrors the backtest's `dedup_traders`.
fn dedup_traders(month: Vec<LeaderboardEntry>, all: Vec<LeaderboardEntry>) -> Vec<LeaderboardEntry> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut traders: Vec<LeaderboardEntry> = Vec::new();
    for entry in month.into_iter().chain(all) {
        if seen.insert(entry.proxy_wallet.clone()) {
            traders.push(entry);
        }
    }
    traders
}

// ---------------------------------------------------------------------------
// Thin I/O over C1's pure ranking (exercised live in C5's canary)
// ---------------------------------------------------------------------------

/// Rebuild the follow WHITELIST: pull the PnL leaderboard (month ∪ all-time,
/// `top_n` each, de-duped), fetch each trader's own `/trades` + closed
/// positions, derive resolutions from those closed positions
/// ([`fold_resolutions`]), build their PRE-cutoff records
/// ([`trader_records`]), and rank by [`Ranking::EdgePerBet`].
///
/// CUTOFF: for the LIVE whitelist we want each trader's skill over their WHOLE
/// resolved record, so `cutoff_ts = now` — [`trader_records`] keeps every buy
/// with `timestamp < cutoff_ts`, i.e. all of their past resolved bets.
///
/// Returns `Some(wallets)` on success (possibly empty if nobody clears the
/// `EdgePerBet` bar). Returns `None` on ANY fetch error, so the caller KEEPS the
/// prior whitelist — we never trade on a set that a transient API failure
/// emptied or staled.
async fn refresh_whitelist(client: &DataApiClient, params: &CopyParams) -> Option<Vec<String>> {
    // 1. Trader universe: top PnL, month ∪ all-time, de-duped (month first).
    let month = client
        .leaderboard(OrderBy::Pnl, TimePeriod::Month, params.top_n)
        .await
        .ok()?;
    let all = client
        .leaderboard(OrderBy::Pnl, TimePeriod::All, params.top_n)
        .await
        .ok()?;
    let traders = dedup_traders(month, all);

    // 2. Per trader: their own fills (the BUYS the record scores) + their closed
    //    positions (the resolution source). Any error aborts → None (keep prior).
    let mut trades_by_wallet: HashMap<String, Vec<Trade>> = HashMap::new();
    let mut resolutions: HashMap<String, i64> = HashMap::new();
    for entry in &traders {
        let wallet = entry.proxy_wallet.as_str();
        let trades = client
            .trades(TradesFilter::User(wallet), WHITELIST_TRADE_LIMIT)
            .await
            .ok()?;
        let closed = client.closed_positions(wallet).await.ok()?;
        for (condition_id, winner) in fold_resolutions(&closed) {
            resolutions.insert(condition_id, winner);
        }
        trades_by_wallet.insert(wallet.to_string(), trades);
    }

    // 3. Rank on the WHOLE resolved record (cutoff = now → all past bets), then
    //    take the EdgePerBet top-N. Pure, shared with the backtest (C1).
    let now = now_ms() / 1000;
    let records = trader_records(&trades_by_wallet, &resolutions, now);
    let whitelist = rank_wallets_oos(
        Ranking::EdgePerBet,
        &traders,
        &records,
        params.top_n,
        params.min_bets,
    );
    Some(whitelist)
}

// ---------------------------------------------------------------------------
// Pure deciders (the TDD'd core — no async, no I/O)
// ---------------------------------------------------------------------------

/// The venue's minimum order size: 5 shares = 5_000_000 µshares. A computed copy
/// size below this floor is dropped (the CLOB rejects sub-minimum orders).
const MIN_COPY_SHARES_MICRO: i128 = 5 * (ONE_USDC_MICRO as i128);

/// Cap on the `seen` de-dup set so a long session can't grow it unboundedly; the
/// OLDEST keys are evicted first. Re-firing a long-evicted signal is acceptable —
/// it must still clear the freshness window to act, and the caps still bind.
const SEEN_CAP: usize = 50_000;

/// Shares (µ) to buy for a new copy: the per-position notional clamped by BOTH
/// remaining capital AND remaining gross headroom, valued at the entry price.
/// Returns `0` when any cap is already exhausted, the entry price is degenerate
/// (≤ 0 or ≥ 1 — not a real two-sided market), or the resulting size is below
/// the venue 5-share minimum.
///
/// Money is µUSDC; size is µshares. `µshares = notional_µUSDC / entry_px`
/// (since `entry_px` USD/share == `entry_px` µUSDC/µshare). The notional floors
/// down (against us).
fn copy_position_size_micro(
    per_position_micro: i128,
    capital_left_micro: i128,
    gross_left_micro: i128,
    entry_px: f64,
) -> i128 {
    // A degenerate mark (0 or 1) is not a real two-sided market — never size in.
    if !(entry_px > 0.0 && entry_px < 1.0) {
        return 0;
    }
    // Deployable notional = the per-position target, capped by whichever of
    // remaining capital / gross headroom binds first.
    let notional_micro = per_position_micro.min(capital_left_micro).min(gross_left_micro);
    if notional_micro <= 0 {
        return 0;
    }
    let qty_micro = (notional_micro as f64 / entry_px).floor() as i128;
    if qty_micro < MIN_COPY_SHARES_MICRO {
        return 0;
    }
    qty_micro
}

/// True when an open (long-only) copy is down at least `stop_loss_pct` of its
/// cost at the current `mark_px`. Fires exactly AT the threshold (inclusive),
/// not before. A non-positive cost or size (nothing at risk / not a long) never
/// fires.
///
/// `value = qty_micro · mark_px` (µshares × USD/share = µUSDC). The position is
/// "down ≥ pct" iff `value ≤ cost · (1 − stop_loss_pct)`.
fn stop_loss_hit(cost_micro: i128, qty_micro: i128, mark_px: f64, stop_loss_pct: f64) -> bool {
    if cost_micro <= 0 || qty_micro <= 0 {
        return false;
    }
    let value_micro = qty_micro as f64 * mark_px;
    let floor_micro = cost_micro as f64 * (1.0 - stop_loss_pct);
    value_micro <= floor_micro
}

/// True when the SOURCE trader has SOLD the copied outcome since the signal we
/// mirrored — a `Sell` of `(condition_id, outcome_index)` by the trader with
/// `timestamp > entry_ts`. `trader_recent` is that trader's own recent trades
/// (the loop passes the right wallet's tape), so this is a pure scan.
fn should_follow_exit(
    trader_recent: &[Trade],
    condition_id: &str,
    outcome_index: i64,
    entry_ts: i64,
) -> bool {
    trader_recent.iter().any(|t| {
        t.side == TradeSide::Sell
            && t.timestamp > entry_ts
            && t.condition_id == condition_id
            && t.outcome_index == outcome_index
    })
}

/// Cash value (µUSDC) recovered when a held copy position resolves: the held
/// WINNER pays $1/share (µUSDC == held µshares), the held LOSER pays nothing.
fn resolved_value_micro(qty_micro: i128, outcome_index: i64, winning_outcome: i64) -> i128 {
    if outcome_index == winning_outcome {
        qty_micro
    } else {
        0
    }
}

/// True when another copy may be OPENED — the open-position count is below the
/// concurrency cap. (`open == cap` blocks; the boundary is exclusive.)
fn concurrency_allows(open_positions: usize, max_concurrent: u32) -> bool {
    open_positions < max_concurrent as usize
}

// ---------------------------------------------------------------------------
// Store / inventory helpers (the booking glue, mirroring the MM)
// ---------------------------------------------------------------------------

/// `"Buy"`/`"Sell"` tag for a taker [`Action`] (store row label).
fn action_str(action: Action) -> &'static str {
    match action {
        Action::Buy => "Buy",
        Action::Sell => "Sell",
    }
}

/// Build a `"copy"`-tagged [`OrderRow`] (the FK parent the fills reference).
/// `Order::to_row` hardcodes the `"arb"` strategy tag, so the row is built
/// directly here with the copy tag.
fn order_row(order: &Order, strategy: &str) -> OrderRow {
    OrderRow {
        id: order.id.to_string(),
        ts_ms: now_ms(),
        fingerprint: order.fingerprint.clone(),
        token: order.token.0 as i64,
        action: action_str(order.action).into(),
        limit_ticks: i64::from(order.limit_px.get()),
        tick_levels: i64::from(order.ts.levels()),
        qty_micro: order.qty.0 as i64,
        strategy: strategy.into(),
    }
}

/// Derive the inventory LEDGER's caps from the copy caps. The load-bearing entry
/// gate is the pure deciders + the concurrency gate; this config just keeps the
/// [`InventoryRisk`] accounting ledger's own caps consistent with the envelope
/// (a portfolio backstop). The volatility hint is off — the copy loop quotes
/// nothing.
fn inv_config(params: &CopyParams) -> InventoryConfig {
    InventoryConfig {
        max_inventory_usd: Usdc(params.per_position_micro),
        max_gross_inventory_usd: Usdc(params.max_gross_micro),
        inventory_stop_loss_usd: Usdc(params.max_gross_micro),
        daily_loss_usd: Usdc(params.max_gross_micro),
        vol_pull_ticks: 0,
        vol_window: Duration::from_secs(1),
    }
}

// ---------------------------------------------------------------------------
// The taker venue the copy loop drives (Option-threaded; C5 wires the real one)
// ---------------------------------------------------------------------------

/// The taker venue the copy executor drives — a thin composite over the existing
/// execution interface (mirrors how the MM holds a `MakerVenue + UserFillSource`
/// venue). It reads an outcome token's marketable prices (the freshness +
/// mark-to-market reference) and places taker FAK orders. main (C5) adapts the
/// real `LiveVenue` (which already exposes `best_ask` + `submit_fak`) to this;
/// the tests use a mock.
///
/// The returned futures are `Send` so the GENERIC copy loop is `Send` (the
/// `Strategy` trait boxes it as `dyn Future + Send`) without the arb's
/// builder-closure dance — a concrete `V` is not required to prove `Send`.
pub trait CopyVenue: Send {
    /// Best (lowest) ask for `token`, or `None` when the ask side is empty / the
    /// book is unavailable (nothing to lift).
    fn best_ask(
        &mut self,
        token: TokenId,
        ts: TickSize,
    ) -> impl Future<Output = Option<Px>> + Send;

    /// Best (highest) bid for `token`, or `None` when the bid side is empty / the
    /// book is unavailable (nothing to sell into / mark a long against).
    fn best_bid(
        &mut self,
        token: TokenId,
        ts: TickSize,
    ) -> impl Future<Output = Option<Px>> + Send;

    /// Place a taker fill-and-kill order; the fills carry signed cash already net
    /// of fee (negative on a buy, positive on a sell).
    fn submit_fak(
        &mut self,
        order: &Order,
    ) -> impl Future<Output = Result<SubmitOutcome, VenueError>> + Send;
}

/// How to TRADE a copied `(condition_id, outcome_index)`: the internal venue
/// token (book reads + taker orders + marks), its tick grid, and the on-chain
/// condition id (the M6 relayer redeem key). main (C5) builds this from the
/// registry for the markets the venue can trade; a candidate whose
/// `(condition_id, outcome_index)` is absent is skipped (we can't trade it).
#[derive(Debug, Clone)]
pub struct TradeTokenInfo {
    pub token: TokenId,
    pub ts: TickSize,
    pub condition: B256,
}

// ---------------------------------------------------------------------------
// Concrete CopyVenue impls (C5): the LIVE taker venue + a PAPER taker venue,
// plus the app-level enum main wires into CopyStrategy.
// ---------------------------------------------------------------------------

/// The live CLOB venue IS a [`CopyVenue`]: it already exposes the public-book
/// `best_ask` + the taker `submit_fak`, and C5 added the symmetric `best_bid`.
/// The public book reads return `Result<Option<Px>, _>`; the copy loop only
/// needs "a price or not", so a transport/parse error degrades to `None` (skip
/// this cycle, retry next) — the same best-effort posture the loop's other I/O
/// takes — rather than aborting the loop. `submit_fak` is the
/// [`ExecutionVenue`] taker FAK (signed deposit-wallet sigType-3 order); a
/// `--shadow` venue signs but returns a zero-fill, so a shadow copy run places
/// no real orders. main builds this venue from the SAME live inputs the MM
/// reuses (no second API key).
impl CopyVenue for pm_execution::live::LiveVenue {
    async fn best_ask(&mut self, token: TokenId, ts: TickSize) -> Option<Px> {
        // A transport/parse error is not actionable here — treat "no price"
        // (None) and "errored" identically (skip), matching the loop's posture.
        pm_execution::live::LiveVenue::best_ask(self, token, ts)
            .await
            .ok()
            .flatten()
    }

    async fn best_bid(&mut self, token: TokenId, ts: TickSize) -> Option<Px> {
        pm_execution::live::LiveVenue::best_bid(self, token, ts)
            .await
            .ok()
            .flatten()
    }

    async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
        <pm_execution::live::LiveVenue as ExecutionVenue>::submit_fak(self, order).await
    }
}

/// A PAPER taker [`CopyVenue`]: honest fills against the LIVE book (re-read at
/// fill time — no midpoints, no infinite depth) via the SAME [`PaperVenue`] the
/// arb taker uses, with NO live calls. `best_ask` / `best_bid` read the current
/// book off the same [`BookFetcher`] (the freshness gate + the long's mark).
/// `BookFetcher` is cheaply `Clone` (an `Arc` inside), so it holds two handles:
/// one inside [`PaperVenue`] (the fill path) and one for the book reads
/// (`BookSource::book` needs `&mut self`, so the read path owns its own).
pub struct PaperCopyVenue {
    /// The honest fill-at-book taker (reused, not re-implemented).
    venue: PaperVenue<crate::wiring::BookFetcher>,
    /// A second handle to the SAME books for the best-ask/bid reads.
    books: crate::wiring::BookFetcher,
}

impl PaperCopyVenue {
    /// Build a paper copy venue over `books`, with the same `latency` / `gas`
    /// the arb paper venue uses.
    pub fn new(books: crate::wiring::BookFetcher, latency: Duration, gas: GasTable) -> Self {
        PaperCopyVenue {
            venue: PaperVenue::new(books.clone(), latency, gas),
            books,
        }
    }
}

impl CopyVenue for PaperCopyVenue {
    async fn best_ask(&mut self, token: TokenId, _ts: TickSize) -> Option<Px> {
        // The book carries its own tick grid (== the market's, == `_ts`); read
        // the best ask straight off it.
        self.books.book(token).await.and_then(|b| b.asks.best())
    }

    async fn best_bid(&mut self, token: TokenId, _ts: TickSize) -> Option<Px> {
        self.books.book(token).await.and_then(|b| b.bids.best())
    }

    async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
        self.venue.submit_fak(order).await
    }
}

/// The app's concrete [`CopyVenue`] — the single monomorphic type main wires
/// into [`CopyStrategy`] so the SAME `CopyStrategy<AppCopyVenue>` covers both a
/// live run (a `LiveVenue` for THIS account — the SAME creds/signer the MM
/// reuses, NO second API key) and a paper run (a [`PaperCopyVenue`] over the
/// live `BookFetcher`). Mirrors the MM's `MmLive` enum: the strategy stays
/// monomorphic, the variant just chooses live-vs-paper.
// `Live` is the operative variant on a live run; boxing it would add a heap
// indirection on every taker order for no real benefit (one instance per
// process, chosen once at startup). The size difference is intentional.
#[allow(clippy::large_enum_variant)]
pub enum AppCopyVenue {
    /// LIVE: real taker FAK orders for this account (book reads via the public
    /// `/book` REST). Built only when the copy strategy is cleared for live.
    Live(pm_execution::live::LiveVenue),
    /// PAPER: honest fills at the live book, NO live calls (the default).
    Paper(PaperCopyVenue),
}

impl CopyVenue for AppCopyVenue {
    async fn best_ask(&mut self, token: TokenId, ts: TickSize) -> Option<Px> {
        match self {
            AppCopyVenue::Live(v) => CopyVenue::best_ask(v, token, ts).await,
            AppCopyVenue::Paper(v) => v.best_ask(token, ts).await,
        }
    }

    async fn best_bid(&mut self, token: TokenId, ts: TickSize) -> Option<Px> {
        match self {
            AppCopyVenue::Live(v) => CopyVenue::best_bid(v, token, ts).await,
            AppCopyVenue::Paper(v) => v.best_bid(token, ts).await,
        }
    }

    async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
        match self {
            AppCopyVenue::Live(v) => CopyVenue::submit_fak(v, order).await,
            AppCopyVenue::Paper(v) => v.submit_fak(order).await,
        }
    }
}

/// One OPEN copied position (keyed in `open` by `(condition_id, outcome_index)`).
/// Long-only: `qty_micro > 0`, `cost_micro > 0`.
#[derive(Debug, Clone)]
pub(crate) struct CopyPosition {
    /// Source trader we mirrored — a later SELL by them triggers the follow-exit.
    pub trader: String,
    /// The trader's triggering BUY time (the signal we copied); a follow-exit
    /// SELL must be NEWER than this.
    pub entry_ts: i64,
    /// Held size, µshares (> 0; long-only).
    pub qty_micro: i128,
    /// Cost basis paid, µUSDC incl. fees (> 0) — what the stop-loss measures.
    pub cost_micro: i128,
    /// Venue token for book reads / taker exits / marks.
    pub token: TokenId,
    /// `token`'s tick grid.
    pub ts: TickSize,
    /// On-chain condition id for the resolution redeem.
    pub condition: B256,
}

/// Bounded de-dup of acted-on `(condition_id, outcome_index)` keys: a `HashSet`
/// for O(1) membership (what [`select_signals`] reads) plus an insertion-order
/// queue so the oldest key is evicted once [`SEEN_CAP`] is hit.
#[derive(Default)]
pub(crate) struct SeenKeys {
    set: HashSet<(String, i64)>,
    order: VecDeque<(String, i64)>,
}

impl SeenKeys {
    /// The membership set [`select_signals`] consults.
    fn set(&self) -> &HashSet<(String, i64)> {
        &self.set
    }

    #[cfg(test)]
    fn contains(&self, key: &(String, i64)) -> bool {
        self.set.contains(key)
    }

    /// Mark a key acted-on (idempotent), evicting the oldest if over the cap.
    fn mark(&mut self, key: (String, i64)) {
        if self.set.insert(key.clone()) {
            self.order.push_back(key);
            while self.order.len() > SEEN_CAP {
                if let Some(old) = self.order.pop_front() {
                    self.set.remove(&old);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CopyLoop — the live trading engine (entry + exit sweep), generic over V
// ---------------------------------------------------------------------------

/// The copy strategy's owned trading state. Generic over the taker [`CopyVenue`]
/// so a live venue, a paper venue, or a mock can drive the SAME entry/exit logic
/// (mirroring the MM's `MmLoop<V>`). With `venue == None` (paper-without-venue)
/// it is fully INERT — every method early-returns and no order is ever placed.
pub(crate) struct CopyLoop<V: CopyVenue> {
    /// The taker venue, or `None` → fully inert (no book reads, no orders). C5
    /// wires the real venue; C4 keeps it `None` by default.
    venue: Option<V>,
    /// M6 deposit-wallet relayer for resolved-winner redemption, or `None`
    /// (hold-to-resolution: a resolved position is left for the next reconcile,
    /// mirroring the MM's no-relayer behavior).
    relayer: Option<Arc<RelayerClient>>,
    /// Signed inventory + realized/cost accounting (the booking ledger).
    inv: InventoryRisk,
    /// Open copied positions, keyed by `(condition_id, outcome_index)`.
    open: HashMap<(String, i64), CopyPosition>,
    /// How to trade each copied `(condition_id, outcome_index)` (C5-populated).
    tradeable: HashMap<(String, i64), TradeTokenInfo>,
    /// Durable store sink (order + signed-fill rows, `"copy"`-tagged).
    store_tx: mpsc::Sender<StoreMsg>,
    /// Resolved knobs (caps, drift, stop, reaction window, follow-exit, capital).
    params: CopyParams,
    /// Running realized P&L, µUSDC (status / telemetry).
    realized_micro: i128,
}

impl<V: CopyVenue> CopyLoop<V> {
    /// µUSDC currently DEPLOYED across open positions (Σ cost basis) — what
    /// remaining capital / gross headroom are measured against.
    fn deployed_micro(&self) -> i128 {
        self.open.values().map(|p| p.cost_micro).sum()
    }

    /// View for the per-strategy dashboard. Surfaces the open-position count,
    /// running realized P&L, and (C5) the copy-specific telemetry — the
    /// follow-whitelist size — in a [`CopyStatus`] (mirroring how the MM
    /// surfaces its `RewardFarmStatus`). The per-position lines (market / qty /
    /// entry / mark / uPnL) reach the TUI's Positions panel via the durable
    /// store (every fill is `"copy"`-tagged), exactly as the MM's do.
    fn status(&self, paused: bool, whitelist: usize) -> StrategyStatus {
        StrategyStatus {
            paused,
            open_positions: self.open.len(),
            realized_micro: i64::try_from(self.realized_micro).unwrap_or(0),
            copy: Some(CopyStatus { whitelist }),
            ..Default::default()
        }
    }

    /// ENTRY: for each fresh candidate, when a venue is present and NOT at a cap,
    /// place a FRESHNESS-GATED, capital/gross/floor-sized taker FAK BUY and book
    /// it. A candidate that is untradeable, has no ask, or whose price ran past
    /// the drift gate is consumed (`seen`); a cap-limited candidate is left
    /// un-consumed so a freed slot / capital can still take it while it is fresh.
    async fn run_entries(&mut self, candidates: &[CopyCandidate], seen: &mut SeenKeys) {
        if self.venue.is_none() {
            return; // paper-without-venue → inert
        }
        for c in candidates {
            let key = (c.condition_id.clone(), c.outcome_index);
            // Already holding this market+side (open keys are `seen` so
            // select_signals excludes them — guard defensively anyway).
            if self.open.contains_key(&key) {
                continue;
            }
            // Resolve the tradeable token; untradeable markets are consumed.
            let Some(info) = self.tradeable.get(&key).cloned() else {
                tracing::debug!(
                    condition_id = %c.condition_id,
                    outcome_index = c.outcome_index,
                    "copy: skip entry — no tradeable token for this market"
                );
                seen.mark(key);
                continue;
            };
            // Best ask = our marketable entry reference.
            let ask = match self.venue.as_mut() {
                Some(v) => v.best_ask(info.token, info.ts).await,
                None => return,
            };
            let Some(ask) = ask else {
                tracing::debug!(condition_id = %c.condition_id, "copy: skip entry — no ask in book");
                seen.mark(key);
                continue;
            };
            let entry_px = ask.microusdc(info.ts) as f64 / ONE_USDC_MICRO as f64;
            // FRESHNESS gate (shared primitive): skip if our price ran off the
            // trader's trigger — we'd be chasing a runner whose edge is gone.
            if !within_drift(entry_px, c.trigger_px, self.params.max_drift) {
                tracing::info!(
                    condition_id = %c.condition_id,
                    entry_px,
                    trigger_px = c.trigger_px,
                    "copy: skip entry — price ran past the drift gate"
                );
                seen.mark(key);
                continue;
            }
            // Concurrency cap (NOT consumed — a freed slot can still take it).
            if !concurrency_allows(self.open.len(), self.params.max_concurrent_positions) {
                tracing::info!(
                    open = self.open.len(),
                    "copy: skip entry — at concurrency cap (retry while fresh)"
                );
                continue;
            }
            // Capital + gross caps → size.
            let deployed = self.deployed_micro();
            let capital_left = self.params.capital.0 - deployed;
            let gross_left = self.params.max_gross_micro - deployed;
            let size = copy_position_size_micro(
                self.params.per_position_micro,
                capital_left,
                gross_left,
                entry_px,
            );
            if size <= 0 {
                tracing::info!(
                    condition_id = %c.condition_id,
                    capital_left,
                    gross_left,
                    "copy: skip entry — caps/floor leave no size (retry while fresh)"
                );
                continue; // not consumed: capital/gross may free up
            }
            self.enter(key.clone(), c, &info, ask, size).await;
            seen.mark(key);
        }
    }

    /// Place + book the taker FAK BUY for one sized candidate, recording the open
    /// position. Paper / live both go through [`CopyVenue::submit_fak`]; the fills
    /// are booked into inventory + the store via [`book_fills`](Self::book_fills).
    async fn enter(
        &mut self,
        key: (String, i64),
        c: &CopyCandidate,
        info: &TradeTokenInfo,
        ask: Px,
        size_micro: i128,
    ) {
        // Marketable buy: limit == best ask (the FAK fills at/under it, killing
        // any unfilled remainder).
        let order = Order::new(
            format!("copy:{}:{}", c.condition_id, c.outcome_index),
            info.token,
            Action::Buy,
            info.ts,
            ask,
            Qty(size_micro.max(0) as u64),
            Bps(0),
        );
        let outcome = match self.venue.as_mut() {
            Some(v) => match v.submit_fak(&order).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, condition_id = %c.condition_id, "copy: entry FAK rejected");
                    return;
                }
            },
            None => return,
        };
        if outcome.filled.0 == 0 {
            tracing::info!(condition_id = %c.condition_id, "copy: entry FAK filled nothing");
            return;
        }
        // Persist the order row (FK parent of the fills) BEFORE the signed fills.
        let _ = self
            .store_tx
            .send(StoreMsg::OrderInsert(order_row(&order, "copy"), None))
            .await;
        let (filled_micro, cost_micro) = self
            .book_fills(info.token, info.ts, Action::Buy, &order, &outcome)
            .await;
        if filled_micro <= 0 {
            return;
        }
        self.open.insert(
            key,
            CopyPosition {
                trader: c.trader.clone(),
                entry_ts: c.timestamp,
                qty_micro: filled_micro,
                cost_micro,
                token: info.token,
                ts: info.ts,
                condition: info.condition,
            },
        );
        tracing::info!(
            condition_id = %c.condition_id,
            outcome_index = c.outcome_index,
            trader = %c.trader,
            qty_micro = filled_micro as i64,
            cost_micro = cost_micro as i64,
            "copy: ENTERED (taker FAK buy, freshness-gated)"
        );
    }

    /// EXIT SWEEP (each cycle): for every open position, in deterministic key
    /// order, check (c) RESOLUTION first (a resolved market has no live book to
    /// sell into → redeem), then (a) FOLLOW-EXIT (the source trader sold), then
    /// (b) STOP-LOSS (marked down past the cap). The first that fires closes the
    /// position. Long-only: every sell is `≤` the held size.
    async fn run_exit_sweep(
        &mut self,
        recent_by_wallet: &HashMap<String, Vec<Trade>>,
        resolutions: &HashMap<String, i64>,
    ) {
        if self.venue.is_none() {
            return; // paper-without-venue → inert
        }
        let mut keys: Vec<(String, i64)> = self.open.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let Some(pos) = self.open.get(&key).cloned() else {
                continue;
            };
            // (c) RESOLUTION first — a resolved market has no live book to sell.
            if let Some(&winner) = resolutions.get(&key.0) {
                self.settle_resolution(&key, &pos, winner).await;
                continue;
            }
            // (a) FOLLOW-EXIT — the source trader sold the copied outcome.
            let empty: Vec<Trade> = Vec::new();
            let trader_recent = recent_by_wallet.get(&pos.trader).unwrap_or(&empty);
            let follow = self.params.follow_exit
                && should_follow_exit(trader_recent, &key.0, key.1, pos.entry_ts);
            // Read the bid once — both the marketable sell price AND the long's
            // mark for the stop. No bid (one-sided/illiquid) → can't act this
            // cycle (retry next).
            let bid = match self.venue.as_mut() {
                Some(v) => v.best_bid(pos.token, pos.ts).await,
                None => return,
            };
            let Some(bid) = bid else {
                if follow {
                    tracing::info!(condition_id = %key.0, "copy: follow-exit deferred — no bid to sell into");
                }
                continue;
            };
            if follow {
                tracing::info!(condition_id = %key.0, trader = %pos.trader, "copy: follow-exit — source sold");
                self.taker_exit(&key, &pos, bid).await;
                continue;
            }
            // (b) STOP-LOSS — cut the long if marked down ≥ stop_loss_pct.
            let mark_px = bid.microusdc(pos.ts) as f64 / ONE_USDC_MICRO as f64;
            if stop_loss_hit(pos.cost_micro, pos.qty_micro, mark_px, self.params.stop_loss_pct) {
                tracing::info!(condition_id = %key.0, mark_px, "copy: stop-loss hit");
                self.taker_exit(&key, &pos, bid).await;
            }
        }
    }

    /// Taker FAK SELL of the held qty at `limit` (the best bid, marketable),
    /// booking the fill and reducing/closing the open position. Long-only: never
    /// sells more than held. A zero-fill FAK leaves the position for a retry.
    async fn taker_exit(&mut self, key: &(String, i64), pos: &CopyPosition, limit: Px) {
        let qty = Qty(pos.qty_micro.max(0) as u64);
        if qty.0 == 0 {
            self.open.remove(key);
            return;
        }
        let order = Order::new(
            format!("copy-exit:{}:{}", key.0, key.1),
            pos.token,
            Action::Sell,
            pos.ts,
            limit,
            qty,
            Bps(0),
        );
        let outcome = match self.venue.as_mut() {
            Some(v) => match v.submit_fak(&order).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, condition_id = %key.0, "copy: exit FAK rejected");
                    return;
                }
            },
            None => return,
        };
        if outcome.filled.0 == 0 {
            tracing::info!(condition_id = %key.0, "copy: exit FAK filled nothing — retry next cycle");
            return;
        }
        let _ = self
            .store_tx
            .send(StoreMsg::OrderInsert(order_row(&order, "copy"), None))
            .await;
        let (filled, _proceeds) = self
            .book_fills(pos.token, pos.ts, Action::Sell, &order, &outcome)
            .await;
        let remaining = pos.qty_micro - filled;
        if remaining <= 0 {
            self.open.remove(key);
        } else if let Some(p) = self.open.get_mut(key) {
            // Pro-rata release of cost basis so the caps free correctly on a
            // partial exit (the FAK killed an unfilled remainder).
            p.cost_micro = pos.cost_micro * remaining / pos.qty_micro;
            p.qty_micro = remaining;
        }
    }

    /// RESOLUTION-REDEEM: book a resolved position out at its settled value
    /// (winner $1/share, loser $0), then fire the on-chain redeem via the M6
    /// relayer (best-effort, off the decision path). Without a relayer we cannot
    /// recover the on-chain cash, so the position is HELD for the next session's
    /// reconcile (mirrors the MM's no-relayer hold-to-resolution).
    async fn settle_resolution(&mut self, key: &(String, i64), pos: &CopyPosition, winner: i64) {
        let Some(relayer) = self.relayer.clone() else {
            tracing::debug!(condition_id = %key.0, "copy: market resolved but no relayer — holding for reconcile");
            return;
        };
        let value = resolved_value_micro(pos.qty_micro, key.1, winner);
        let realized_before = self.inv.realized(pos.token).0;
        self.inv.on_fill(pos.token, -pos.qty_micro, Usdc(value));
        let realized_after = self.inv.realized(pos.token).0;
        let delta = realized_after - realized_before;
        self.realized_micro += delta;
        self.open.remove(key);
        if delta != 0 {
            let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                utc_day: utc_day_from_ms(now_ms()),
                strategy: "copy".into(),
                delta_micro: delta,
            });
        }
        match relayer.redeem(pos.condition).await {
            Ok(_) => tracing::info!(
                condition_id = %key.0,
                value_micro = value as i64,
                "copy: redeemed resolved position"
            ),
            Err(e) => tracing::warn!(
                error = %e,
                condition_id = %key.0,
                "copy: relayer redeem failed (booked locally; reconcile next session)"
            ),
        }
    }

    /// Book a taker order's fills into inventory + the durable store (`"copy"`),
    /// accumulating + persisting realized P&L. Returns `(filled_µshares,
    /// value_µUSDC)` where `value` is the POSITIVE cost paid on a BUY or the
    /// POSITIVE proceeds received on a SELL.
    async fn book_fills(
        &mut self,
        token: TokenId,
        ts: TickSize,
        action: Action,
        order: &Order,
        outcome: &SubmitOutcome,
    ) -> (i128, i128) {
        let mut filled_micro: i128 = 0;
        let mut cash_total: i128 = 0;
        for f in &outcome.fills {
            let signed_qty = match action {
                Action::Buy => f.qty.0 as i128,
                Action::Sell => -(f.qty.0 as i128),
            };
            // Authoritative signed inventory + the realized delta this fill books.
            let realized_before = self.inv.realized(token).0;
            self.inv.on_fill(token, signed_qty, f.cash);
            let realized_after = self.inv.realized(token).0;
            let realized_delta = realized_after - realized_before;
            self.realized_micro += realized_delta;
            filled_micro += f.qty.0 as i128;
            cash_total += f.cash.0;
            let row = FillRow {
                order_id: order.id.to_string(),
                ts_ms: now_ms(),
                token: token.0 as i64,
                action: action_str(action).into(),
                px_ticks: i64::from(f.px.get()),
                tick_levels: i64::from(ts.levels()),
                qty_micro: f.qty.0 as i64,
                cash_micro: usdc_to_i64(f.cash).unwrap_or(0),
                fee_micro: usdc_to_i64(f.fee).unwrap_or(0),
                strategy: "copy".into(),
            };
            // SIGNED route (like the MM) so booking stays robust to rounding/short
            // edge cases; copy is long-only so it never actually opens a short.
            let _ = self.store_tx.send(StoreMsg::FillSigned(row, None)).await;
            if realized_delta != 0 {
                let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                    utc_day: utc_day_from_ms(now_ms()),
                    strategy: "copy".into(),
                    delta_micro: realized_delta,
                });
            }
        }
        let value = match action {
            Action::Buy => -cash_total,  // cash is negative on a buy → positive cost
            Action::Sell => cash_total,  // proceeds received
        };
        (filled_micro, value)
    }
}

/// One poll cycle: fetch the recent tape (whitelist + open-position traders),
/// build the resolution map for open markets (only when a relayer can redeem),
/// SWEEP exits, then run fresh ENTRIES. The exit-before-entry order frees caps
/// for new entries the same cycle. Best-effort I/O: a per-wallet error just
/// omits that wallet this cycle.
async fn run_poll_cycle<V: CopyVenue>(
    state: &mut CopyLoop<V>,
    feed: &Option<Arc<DataApiClient>>,
    whitelist: &[String],
    seen: &mut SeenKeys,
) {
    let Some(client) = feed else {
        return;
    };
    let now = now_ms() / 1000;
    let mut recent_by_wallet: HashMap<String, Vec<Trade>> = HashMap::new();
    for wallet in whitelist {
        if let Ok(trades) = client.trades(TradesFilter::User(wallet), POLL_TRADE_LIMIT).await {
            recent_by_wallet.insert(wallet.clone(), trades);
        }
    }
    // Source traders of OPEN positions may have dropped off the whitelist — fetch
    // their tape too so a follow-exit still sees their SELL. Collect the ones not
    // already fetched first, then pull them (keeps the read off the membership map).
    let missing_traders: Vec<String> = state
        .open
        .values()
        .map(|p| p.trader.clone())
        .collect::<HashSet<String>>()
        .into_iter()
        .filter(|t| !recent_by_wallet.contains_key(t))
        .collect();
    for trader in missing_traders {
        if let Ok(trades) = client.trades(TradesFilter::User(&trader), POLL_TRADE_LIMIT).await {
            recent_by_wallet.insert(trader, trades);
        }
    }
    // Resolutions for open markets — only worth fetching when we can actually
    // redeem (a relayer is present) and we hold something.
    let resolutions = if state.open.is_empty() || state.relayer.is_none() {
        HashMap::new()
    } else {
        fetch_open_resolutions(client, &state.open).await
    };
    state.run_exit_sweep(&recent_by_wallet, &resolutions).await;
    let candidates = select_signals(
        &recent_by_wallet,
        whitelist,
        seen.set(),
        now,
        state.params.reaction_window_secs,
    );
    state.run_entries(&candidates, seen).await;
}

/// Build `conditionId → winning_outcome` for the markets we currently hold, from
/// the SOURCE traders' closed positions (reuses [`fold_resolutions`]). A market
/// only appears once it RESOLVED, so a hit means the held position can redeem.
/// Best-effort: a per-trader fetch error just omits that trader this cycle.
async fn fetch_open_resolutions(
    client: &DataApiClient,
    open: &HashMap<(String, i64), CopyPosition>,
) -> HashMap<String, i64> {
    let traders: HashSet<String> = open.values().map(|p| p.trader.clone()).collect();
    let mut resolutions: HashMap<String, i64> = HashMap::new();
    for trader in traders {
        if let Ok(closed) = client.closed_positions(&trader).await {
            for (cond, winner) in fold_resolutions(&closed) {
                resolutions.insert(cond, winner);
            }
        }
    }
    resolutions
}

// ---------------------------------------------------------------------------
// The CopyStrategy shell + its async loop
// ---------------------------------------------------------------------------

/// Smart-money COPY strategy (Task C4). Constructed by main (C5); `run` drives
/// the whitelist→signal→trade pipeline. Generic over the taker [`CopyVenue`] so
/// main can wire a live venue, a paper venue, or (in tests) a mock through the
/// SAME engine — mirroring how [`MmStrategy`](super::mm::MmStrategy) is generic
/// over its maker venue.
///
/// Holds its resolved [`CopyParams`] (the capital envelope), the Data-API feed
/// (signal/whitelist source), the Option-threaded taker `venue` + M6 `relayer`
/// (both `None` ⇒ inert, no orders), the `(condition_id, outcome_index) →`
/// [`TradeTokenInfo`] resolver, and whether to start paused (live held). The live
/// trading state (`open` positions, `inv`, `seen`) lives in [`CopyLoop`] /
/// [`run_copy_loop`], which `run` builds after consuming `self`.
pub struct CopyStrategy<V: CopyVenue> {
    id: StrategyId,
    params: CopyParams,
    /// The Data-API feed (leaderboard / trades / closed positions). `None` (the
    /// default) makes the strategy an inert heartbeat — no whitelist, no polling
    /// — which is what the kill/pause unit tests run against (no network).
    feed: Option<Arc<DataApiClient>>,
    /// The taker venue (book reads + FAK). `None` (the default) ⇒ paper-without-
    /// venue: fully inert, no orders. main (C5) attaches the live/paper venue.
    venue: Option<V>,
    /// M6 deposit-wallet relayer for resolved-winner redemption. `None` ⇒
    /// hold-to-resolution (resolved positions left for the next reconcile).
    relayer: Option<Arc<RelayerClient>>,
    /// How to trade each copied `(condition_id, outcome_index)` — built by main
    /// (C5) from the registry; empty by default (nothing is tradeable ⇒ inert).
    tradeable: HashMap<(String, i64), TradeTokenInfo>,
    /// Start the loop PAUSED when live is held (the operator releases via the
    /// host's `SetPaused(false)`), mirroring the MM. Gates the live path.
    start_paused: bool,
}

impl<V: CopyVenue> CopyStrategy<V> {
    /// Construct the copy strategy from its resolved params. The feed, venue,
    /// relayer, and tradeable map are all absent by default (a fully inert
    /// heartbeat — no whitelist, no orders); attach them with the builders.
    pub fn new(params: CopyParams) -> Self {
        CopyStrategy {
            id: StrategyId("copy"),
            params,
            feed: None,
            venue: None,
            relayer: None,
            tradeable: HashMap::new(),
            start_paused: false,
        }
    }

    /// Attach (or clear) the Data-API feed that sources the whitelist + signals.
    pub fn with_feed(mut self, feed: Option<Arc<DataApiClient>>) -> Self {
        self.feed = feed;
        self
    }

    /// Attach (or clear) the taker venue. `Some` enables trading (paper or live);
    /// `None` keeps the strategy inert (no orders). main (C5) wires this.
    pub fn with_venue(mut self, venue: Option<V>) -> Self {
        self.venue = venue;
        self
    }

    /// Attach (or clear) the M6 relayer used for resolved-winner redemption.
    pub fn with_relayer(mut self, relayer: Option<Arc<RelayerClient>>) -> Self {
        self.relayer = relayer;
        self
    }

    /// Provide the `(condition_id, outcome_index) →` [`TradeTokenInfo`] resolver
    /// (main builds it from the registry for the markets the venue can trade).
    pub fn with_tradeable(mut self, tradeable: HashMap<(String, i64), TradeTokenInfo>) -> Self {
        self.tradeable = tradeable;
        self
    }

    /// Start the loop PAUSED (live held). The host sends `SetPaused(false)` on
    /// release. Mirrors [`MmStrategy::with_start_paused`](super::mm::MmStrategy::with_start_paused).
    pub fn with_start_paused(mut self, start_paused: bool) -> Self {
        self.start_paused = start_paused;
        self
    }
}

impl<V: CopyVenue + 'static> Strategy for CopyStrategy<V> {
    fn id(&self) -> StrategyId {
        self.id
    }

    /// The copy strategy reads the Data API on its own cadence; it observes no
    /// per-supervisor book updates, so it installs no inline hook.
    fn make_on_apply(&self) -> Option<OnApplyFn> {
        None
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let CopyStrategy {
                id: _,
                params,
                feed,
                venue,
                relayer,
                tradeable,
                start_paused,
            } = *self;
            run_copy_loop(ctx, params, feed, venue, relayer, tradeable, start_paused).await;
        })
    }
}

/// The copy strategy's owned async loop. Two cadences via independent interval
/// timers — whitelist refresh (`whitelist_refresh`) and signal poll
/// (`signal_poll`) — plus the per-strategy control channel, exactly like the
/// MM's loop honors `ctx.kill` + `ctl_rx`:
///
/// * the global `kill` is checked at the top of every iteration (final status
///   published on the way out — the trait's out-of-band reporting contract); a
///   kill stops cleanly without placing NEW orders, leaving open positions for
///   the next session's reconcile (mirrors the MM shutdown);
/// * `SetPaused` toggles `paused` — a paused loop refreshes the whitelist but
///   runs NO poll cycle, so it places NO orders (entries OR exits);
/// * a closed control channel (host dropped the sender) shuts the loop down.
///
/// The poll cycle TRADES only when NOT paused AND a venue is present (the
/// [`CopyLoop`] is otherwise inert).
#[allow(clippy::too_many_arguments)]
async fn run_copy_loop<V: CopyVenue>(
    ctx: StrategyCtx,
    params: CopyParams,
    feed: Option<Arc<DataApiClient>>,
    venue: Option<V>,
    relayer: Option<Arc<RelayerClient>>,
    tradeable: HashMap<(String, i64), TradeTokenInfo>,
    start_paused: bool,
) {
    let StrategyCtx {
        kill,
        mut ctl_rx,
        status_tx,
        store_tx,
        ..
    } = ctx;

    // The live trading state the loop owns (the strategy struct is config-only).
    let mut state = CopyLoop {
        venue,
        relayer,
        inv: InventoryRisk::new(inv_config(&params)),
        open: HashMap::new(),
        tradeable,
        store_tx,
        params: params.clone(),
        realized_micro: 0,
    };
    let mut seen = SeenKeys::default();
    let mut whitelist: Vec<String> = Vec::new();
    let mut paused = start_paused;
    if start_paused {
        tracing::info!("copy: live held — trading PAUSED until release (press `l`)");
    }

    // Both intervals fire an immediate first tick, so the whitelist is built and
    // the first poll runs at startup; a steady cadence (Skip) afterwards rather
    // than a catch-up burst after a stall (mirrors the MM / heartbeat).
    let mut whitelist_tick = tokio::time::interval(params.whitelist_refresh);
    let mut poll_tick = tokio::time::interval(params.signal_poll);
    whitelist_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    poll_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        // The global kill is the real shutdown signal; observe it each iteration
        // and publish a final status out-of-band before returning. Open positions
        // are intentionally LEFT for the next session's reconcile (no fire sale).
        if kill.load(Ordering::Relaxed) {
            let _ = status_tx.send(state.status(paused, whitelist.len()));
            return;
        }
        tokio::select! {
            _ = whitelist_tick.tick() => {
                if let Some(client) = &feed {
                    match refresh_whitelist(client, &state.params).await {
                        Some(wl) => {
                            tracing::info!(
                                traders = wl.len(),
                                "copy: whitelist refreshed (EdgePerBet, top {})",
                                state.params.top_n
                            );
                            whitelist = wl;
                        }
                        // Keep the PRIOR whitelist on any fetch error — never
                        // poll/trade on a transient-failure-emptied set.
                        None => tracing::warn!(
                            traders = whitelist.len(),
                            "copy: whitelist refresh failed — keeping prior whitelist"
                        ),
                    }
                }
            }
            _ = poll_tick.tick() => {
                // TRADE only when NOT paused AND a venue is present — otherwise
                // the cycle is skipped entirely (no orders).
                if !paused && state.venue.is_some() {
                    run_poll_cycle(&mut state, &feed, &whitelist, &mut seen).await;
                }
            }
            cmd = ctl_rx.recv() => match cmd {
                Some(StrategyCommand::SetPaused(p)) => paused = p,
                // The copy strategy has no resting orders to veto; ignore.
                Some(StrategyCommand::VetoQuote { .. }) => {}
                // Host dropped the control sender → shut down cleanly.
                None => return,
            },
        }
        // Publish the live paused/positions/realized/whitelist view each event.
        let _ = status_tx.send(state.status(paused, whitelist.len()));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]

    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    use pm_core::num::{buy_cost, sell_proceeds};
    use pm_execution::venue::Fill;
    use pm_store::Store;
    use pm_store::writer::run_writer;
    use tokio::sync::{mpsc, watch};

    use super::*;
    use crate::wiring::BookFetcher;

    // ---- fixture builders -------------------------------------------------

    fn trade(wallet: &str, cid: &str, oi: i64, side: TradeSide, price: f64, ts: i64) -> Trade {
        Trade {
            proxy_wallet: wallet.to_string(),
            condition_id: cid.to_string(),
            asset: String::new(),
            side,
            size: 10.0,
            price,
            timestamp: ts,
            outcome_index: oi,
            title: String::new(),
            slug: String::new(),
        }
    }

    fn tmap(trades: Vec<Trade>) -> HashMap<String, Vec<Trade>> {
        let mut m: HashMap<String, Vec<Trade>> = HashMap::new();
        for t in trades {
            m.entry(t.proxy_wallet.clone()).or_default().push(t);
        }
        m
    }

    fn wl(ws: &[&str]) -> Vec<String> {
        ws.iter().map(|s| (*s).to_string()).collect()
    }

    fn seen_of(keys: &[(&str, i64)]) -> HashSet<(String, i64)> {
        keys.iter().map(|(c, o)| ((*c).to_string(), *o)).collect()
    }

    fn closed(cid: &str, oi: i64, cur_price: f64) -> ClosedPos {
        ClosedPos {
            condition_id: cid.to_string(),
            asset: String::new(),
            avg_price: 0.0,
            outcome_index: oi,
            cur_price,
            cash_pnl: 0.0,
            size: 0.0,
            title: String::new(),
        }
    }

    fn lb(wallet: &str) -> LeaderboardEntry {
        LeaderboardEntry {
            proxy_wallet: wallet.to_string(),
            user_name: String::new(),
            pnl: 0.0,
            vol: 0.0,
        }
    }

    // now = 10_000s, reaction window = 1_800s ⇒ fresh iff timestamp >= 8_200.
    const NOW: i64 = 10_000;
    const WINDOW: i64 = 1_800;

    // ===================== select_signals (the TDD'd core) =====================

    #[test]
    fn select_signals_fresh_whitelisted_buy_is_a_candidate() {
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 9_000)]);
        let out = select_signals(&recent, &wl(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(
            out,
            vec![CopyCandidate {
                condition_id: "m1".to_string(),
                outcome_index: 0,
                trader: "0xA".to_string(),
                trigger_px: 0.4,
                timestamp: 9_000,
            }]
        );
    }

    #[test]
    fn select_signals_buy_older_than_window_is_excluded() {
        // 8_000 < 8_200 (now - window) ⇒ stale.
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 8_000)]);
        let out = select_signals(&recent, &wl(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert!(out.is_empty());
    }

    #[test]
    fn select_signals_buy_exactly_at_window_edge_is_included() {
        // timestamp == now - window (8_200) is still fresh (>=).
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 8_200)]);
        let out = select_signals(&recent, &wl(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn select_signals_sell_is_excluded() {
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Sell, 0.4, 9_000)]);
        let out = select_signals(&recent, &wl(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert!(out.is_empty());
    }

    #[test]
    fn select_signals_non_whitelisted_wallet_is_excluded() {
        // 0xZ has a fresh buy but is NOT on the whitelist.
        let recent = tmap(vec![trade("0xZ", "m1", 0, TradeSide::Buy, 0.4, 9_000)]);
        let out = select_signals(&recent, &wl(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert!(out.is_empty());
    }

    #[test]
    fn select_signals_already_seen_key_is_excluded() {
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 9_000)]);
        let seen = seen_of(&[("m1", 0)]);
        let out = select_signals(&recent, &wl(&["0xA"]), &seen, NOW, WINDOW);
        assert!(out.is_empty());
        // A different outcome on the same market is NOT seen ⇒ still a candidate.
        let recent2 = tmap(vec![trade("0xA", "m1", 1, TradeSide::Buy, 0.4, 9_000)]);
        let out2 = select_signals(&recent2, &wl(&["0xA"]), &seen, NOW, WINDOW);
        assert_eq!(out2.len(), 1);
    }

    #[test]
    fn select_signals_two_wallets_same_key_yield_one_earliest() {
        // Both whitelisted, same (cond, outcome); the EARLIER buy (0xB @9_000)
        // represents the single candidate, regardless of whitelist order.
        let recent = tmap(vec![
            trade("0xA", "m1", 0, TradeSide::Buy, 0.45, 9_500),
            trade("0xB", "m1", 0, TradeSide::Buy, 0.40, 9_000),
        ]);
        let out = select_signals(&recent, &wl(&["0xA", "0xB"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trader, "0xB");
        assert_eq!(out[0].timestamp, 9_000);
        assert_eq!(out[0].trigger_px, 0.40);
    }

    #[test]
    fn select_signals_same_wallet_same_key_twice_yields_one_earliest() {
        let recent = tmap(vec![
            trade("0xA", "m1", 0, TradeSide::Buy, 0.50, 9_400),
            trade("0xA", "m1", 0, TradeSide::Buy, 0.42, 9_100),
        ]);
        let out = select_signals(&recent, &wl(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].timestamp, 9_100);
    }

    #[test]
    fn select_signals_orders_deterministically_by_timestamp_then_key() {
        // Two distinct keys; output is sorted by timestamp (then cond, outcome),
        // independent of insertion / HashMap order.
        let recent = tmap(vec![
            trade("0xA", "m2", 1, TradeSide::Buy, 0.5, 9_500),
            trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 9_000),
        ]);
        let out = select_signals(&recent, &wl(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        let keys: Vec<(&str, i64)> = out
            .iter()
            .map(|c| (c.condition_id.as_str(), c.outcome_index))
            .collect();
        assert_eq!(keys, vec![("m1", 0), ("m2", 1)]);
    }

    // ===================== fold_resolutions (resolution source) =====================

    #[test]
    fn fold_resolutions_maps_won_and_lost_binary() {
        let cps = vec![
            closed("m1", 0, 1.0), // held outcome 0 and WON  ⇒ winner 0
            closed("m2", 1, 0.0), // held outcome 1 and LOST ⇒ winner 0 (1 - 1)
            closed("m3", 0, 0.0), // held outcome 0 and LOST ⇒ winner 1 (1 - 0)
        ];
        let res = fold_resolutions(&cps);
        assert_eq!(res.get("m1"), Some(&0));
        assert_eq!(res.get("m2"), Some(&0));
        assert_eq!(res.get("m3"), Some(&1));
    }

    // ===================== dedup_traders (universe) =====================

    #[test]
    fn dedup_traders_keeps_first_occurrence_month_first() {
        let month = vec![lb("0xA"), lb("0xB")];
        let all = vec![lb("0xB"), lb("0xC")];
        let traders = dedup_traders(month, all);
        let wallets: Vec<&str> = traders.iter().map(|t| t.proxy_wallet.as_str()).collect();
        assert_eq!(wallets, vec!["0xA", "0xB", "0xC"]);
    }

    // ===================== CopyParams::from_config =====================

    #[test]
    fn copy_params_from_config_converts_money_and_cadences() {
        let copy = pm_config::CopyCfg {
            enabled: true,
            live: false,
            capital_usd: 25.0,
        };
        let cp = pm_config::CopyParamsCfg::default();
        let p = CopyParams::from_config(&copy, &cp).expect("defaults resolve");
        assert_eq!(p.capital, Usdc(25_000_000));
        assert_eq!(p.per_position_micro, 5_000_000);
        assert_eq!(p.max_gross_micro, 25_000_000);
        assert_eq!(p.max_concurrent_positions, 3);
        assert_eq!(p.top_n, 30);
        assert_eq!(p.min_bets, 10);
        assert_eq!(p.reaction_window_secs, 1_800);
        assert_eq!(p.signal_poll, Duration::from_secs(90));
        assert_eq!(p.whitelist_refresh, Duration::from_secs(21_600));
        assert!(p.follow_exit);
    }

    // ===================== pure deciders (the C4 TDD'd core) =====================

    #[test]
    fn size_respects_each_cap_and_the_five_share_floor() {
        // $5 per-position, ample capital + gross, entry $0.50 → 10 shares.
        assert_eq!(
            copy_position_size_micro(5_000_000, 25_000_000, 25_000_000, 0.50),
            10_000_000
        );
        // Capital binds: only $3 left → 6 shares.
        assert_eq!(
            copy_position_size_micro(5_000_000, 3_000_000, 25_000_000, 0.50),
            6_000_000
        );
        // Gross binds: only $2 left → 4 shares, BELOW the 5-share floor → 0.
        assert_eq!(
            copy_position_size_micro(5_000_000, 25_000_000, 2_000_000, 0.50),
            0
        );
        // Exactly 5 shares ($2.50 @ $0.50) is AT the floor → allowed.
        assert_eq!(
            copy_position_size_micro(2_500_000, 25_000_000, 25_000_000, 0.50),
            5_000_000
        );
        // A cap already exhausted (≤ 0) → 0.
        assert_eq!(copy_position_size_micro(5_000_000, 0, 25_000_000, 0.50), 0);
        assert_eq!(copy_position_size_micro(5_000_000, 25_000_000, -1, 0.50), 0);
        // Degenerate price (no real two-sided market) → 0, no div blow-up.
        assert_eq!(
            copy_position_size_micro(5_000_000, 25_000_000, 25_000_000, 0.0),
            0
        );
        assert_eq!(
            copy_position_size_micro(5_000_000, 25_000_000, 25_000_000, 1.0),
            0
        );
    }

    #[test]
    fn stop_loss_fires_at_threshold_not_before() {
        // $5 cost on 10 shares (entry $0.50); 25% stop ⇒ floor value $3.75.
        let (cost, qty) = (5_000_000_i128, 10_000_000_i128);
        assert!(!stop_loss_hit(cost, qty, 0.40, 0.25), "value $4.00 > $3.75 → no");
        assert!(!stop_loss_hit(cost, qty, 0.376, 0.25), "value $3.76 > $3.75 → no");
        assert!(stop_loss_hit(cost, qty, 0.375, 0.25), "value $3.75 == floor → fires");
        assert!(stop_loss_hit(cost, qty, 0.30, 0.25), "value $3.00 < $3.75 → fires");
        // Nothing at risk / not a long → never fires.
        assert!(!stop_loss_hit(0, qty, 0.0, 0.25));
        assert!(!stop_loss_hit(cost, 0, 0.0, 0.25));
    }

    #[test]
    fn follow_exit_needs_post_entry_sell_of_the_right_market() {
        let entry_ts = 1_000;
        // A SELL of the right (cond, outcome) AFTER entry → true.
        let sell = vec![trade("0xA", "m1", 0, TradeSide::Sell, 0.6, 1_500)];
        assert!(should_follow_exit(&sell, "m1", 0, entry_ts));
        // A SELL BEFORE entry → false (the trader's own prior activity).
        let before = vec![trade("0xA", "m1", 0, TradeSide::Sell, 0.6, 900)];
        assert!(!should_follow_exit(&before, "m1", 0, entry_ts));
        // A BUY after entry → false (only a SELL is an exit).
        let buy = vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.6, 1_500)];
        assert!(!should_follow_exit(&buy, "m1", 0, entry_ts));
        // A SELL of a DIFFERENT outcome / market → false.
        let other = vec![
            trade("0xA", "m1", 1, TradeSide::Sell, 0.6, 1_500),
            trade("0xA", "m2", 0, TradeSide::Sell, 0.6, 1_500),
        ];
        assert!(!should_follow_exit(&other, "m1", 0, entry_ts));
        // Empty tape → false.
        assert!(!should_follow_exit(&[], "m1", 0, entry_ts));
    }

    #[test]
    fn resolved_value_pays_winner_full_and_loser_zero() {
        assert_eq!(
            resolved_value_micro(10_000_000, 0, 0),
            10_000_000,
            "held the winning outcome → $1/share"
        );
        assert_eq!(
            resolved_value_micro(10_000_000, 1, 0),
            0,
            "held the losing outcome → $0"
        );
    }

    #[test]
    fn concurrency_gate_blocks_at_the_cap() {
        assert!(concurrency_allows(0, 3));
        assert!(concurrency_allows(2, 3));
        assert!(!concurrency_allows(3, 3), "exactly at the cap blocks");
        assert!(!concurrency_allows(4, 3));
        assert!(!concurrency_allows(0, 0));
    }

    // ===================== loop: entry / exits (mock venue) =====================

    /// Recorded `(token, action, limit_ticks, qty_micro)` per submitted FAK.
    type OrderLog = Arc<Mutex<Vec<(TokenId, Action, u16, u64)>>>;

    /// A mock taker [`CopyVenue`]: configurable best ask/bid per token, and a
    /// `submit_fak` that records the order and fully fills the requested qty at
    /// the order's (marketable) limit — the C4 analogue of arb's `GatedVenue`.
    struct MockVenue {
        asks: HashMap<TokenId, Px>,
        bids: HashMap<TokenId, Px>,
        orders: OrderLog,
        fail: bool,
    }

    impl CopyVenue for MockVenue {
        async fn best_ask(&mut self, token: TokenId, _ts: TickSize) -> Option<Px> {
            self.asks.get(&token).copied()
        }

        async fn best_bid(&mut self, token: TokenId, _ts: TickSize) -> Option<Px> {
            self.bids.get(&token).copied()
        }

        async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
            if self.fail {
                return Err(VenueError::Live("mock venue failure".into()));
            }
            self.orders.lock().unwrap().push((
                order.token,
                order.action,
                order.limit_px.get(),
                order.qty.0,
            ));
            let px_micro = order.limit_px.microusdc(order.ts);
            let cash = match order.action {
                Action::Buy => Usdc(-buy_cost(px_micro, order.qty).0),
                Action::Sell => Usdc(sell_proceeds(px_micro, order.qty).0),
            };
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

    fn cent(tick: u16) -> Px {
        Px::new(tick, TickSize::Cent).expect("valid cent tick")
    }

    /// Default-ish copy params (per-position $5, gross/capital $25, 3 concurrent,
    /// 25% stop, 15% drift, follow-exit on).
    fn copy_params() -> CopyParams {
        CopyParams::from_config(
            &pm_config::CopyCfg {
                enabled: true,
                live: false,
                capital_usd: 25.0,
            },
            &pm_config::CopyParamsCfg::default(),
        )
        .expect("defaults resolve")
    }

    /// Build a [`CopyLoop`] over the mock venue, backed by a REAL in-memory store
    /// and writer (so the `OrderInsert`→`FillSigned` FK path is exercised
    /// end-to-end), returning the loop and the writer join handle (await it after
    /// dropping the loop to recover the `Store` for assertions).
    fn loop_with(
        venue: MockVenue,
        tradeable: HashMap<(String, i64), TradeTokenInfo>,
        params: CopyParams,
    ) -> (CopyLoop<MockVenue>, tokio::task::JoinHandle<Store>) {
        let store = Store::open_in_memory().unwrap();
        let (store_tx, store_rx) = mpsc::channel(256);
        let writer = tokio::spawn(run_writer(store, store_rx));
        let state = CopyLoop {
            venue: Some(venue),
            relayer: None,
            inv: InventoryRisk::new(inv_config(&params)),
            open: HashMap::new(),
            tradeable,
            store_tx,
            params,
            realized_micro: 0,
        };
        (state, writer)
    }

    fn tradeable_of(cid: &str, oi: i64, token: TokenId) -> HashMap<(String, i64), TradeTokenInfo> {
        HashMap::from([(
            (cid.to_string(), oi),
            TradeTokenInfo {
                token,
                ts: TickSize::Cent,
                condition: B256::ZERO,
            },
        )])
    }

    fn candidate(cid: &str, oi: i64, trader: &str, trigger_px: f64, ts: i64) -> CopyCandidate {
        CopyCandidate {
            condition_id: cid.to_string(),
            outcome_index: oi,
            trader: trader.to_string(),
            trigger_px,
            timestamp: ts,
        }
    }

    /// A FRESH candidate within drift → a marketable taker BUY is placed, the
    /// fill is booked into inventory + the open map + the store, and the capital /
    /// gross headroom is consumed. The signal is marked `seen` (no re-fire).
    #[tokio::test]
    async fn entry_books_position_and_consumes_caps() {
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::from([(token, cent(50))]),
            bids: HashMap::from([(token, cent(49))]),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with(venue, tradeable_of("m1", 0, token), copy_params());

        let mut seen = SeenKeys::default();
        // trigger $0.50, entry $0.50 → within drift; $5 / $0.50 = 10 shares.
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;

        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![(token, Action::Buy, 50, 10_000_000)],
            "one marketable taker BUY at the ask for 10 shares"
        );
        assert_eq!(state.inv.net(token), 10_000_000, "booked long into inventory");
        let pos = state
            .open
            .get(&("m1".to_string(), 0))
            .expect("position opened");
        assert_eq!(pos.qty_micro, 10_000_000);
        assert_eq!(pos.cost_micro, 5_000_000, "10 sh × $0.50 = $5 cost");
        assert_eq!(
            state.deployed_micro(),
            5_000_000,
            "capital + gross headroom consumed by the cost"
        );
        assert!(
            seen.contains(&("m1".to_string(), 0)),
            "the acted-on signal is marked seen (no re-fire)"
        );

        drop(state);
        let store = writer.await.unwrap();
        assert_eq!(
            store.count_fills().unwrap(),
            1,
            "the buy persisted via the signed route (OrderInsert→FillSigned FK ok)"
        );
    }

    /// The freshness gate: an ask that ran past `max_drift` off the trader's
    /// trigger → NO order, and the (chased) signal is consumed.
    #[tokio::test]
    async fn entry_skipped_when_price_ran_past_drift() {
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::from([(token, cent(70))]), // ran to $0.70
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with(venue, tradeable_of("m1", 0, token), copy_params());

        let mut seen = SeenKeys::default();
        // trigger $0.50, entry $0.70 → drift 0.40 > 0.15 → skip.
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;

        assert!(
            orders.lock().unwrap().is_empty(),
            "no order — price ran past the drift gate"
        );
        assert!(state.open.is_empty());
        assert!(
            seen.contains(&("m1".to_string(), 0)),
            "a drift-skipped signal is consumed (we don't chase)"
        );
        drop(state);
        let _ = writer.await;
    }

    /// The concurrency gate: at the cap, a fresh candidate for a NEW market is not
    /// entered (and is left un-consumed so a freed slot can still take it).
    #[tokio::test]
    async fn entry_blocked_at_concurrency_cap() {
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::from([(token, cent(50))]),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let mut params = copy_params();
        params.max_concurrent_positions = 1;
        let (mut state, writer) = loop_with(venue, tradeable_of("m2", 0, token), params);
        // Already holding one (different) market → at the cap of 1.
        state.open.insert(
            ("m1".to_string(), 0),
            CopyPosition {
                trader: "0xZ".into(),
                entry_ts: 1,
                qty_micro: 10_000_000,
                cost_micro: 5_000_000,
                token: TokenId(9),
                ts: TickSize::Cent,
                condition: B256::ZERO,
            },
        );

        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m2", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;

        assert!(
            orders.lock().unwrap().is_empty(),
            "at the concurrency cap → no new entry"
        );
        assert_eq!(state.open.len(), 1, "still just the pre-existing position");
        assert!(
            !seen.contains(&("m2".to_string(), 0)),
            "a cap-blocked signal is left for a freed slot (not consumed)"
        );
        drop(state);
        let _ = writer.await;
    }

    /// EXIT (a): the source trader SOLD the copied outcome after entry →
    /// follow-exit places a marketable taker SELL of the whole held qty.
    #[tokio::test]
    async fn follow_exit_sells_when_source_sold() {
        let token = TokenId(2);
        let venue = MockVenue {
            asks: HashMap::new(),
            bids: HashMap::from([(token, cent(60))]), // sell into $0.60
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with(venue, HashMap::new(), copy_params());
        // Seed an open long: 10 sh @ $0.50, cost $5.
        state.inv.on_fill(token, 10_000_000, Usdc(-5_000_000));
        state.open.insert(
            ("m1".to_string(), 0),
            CopyPosition {
                trader: "0xA".into(),
                entry_ts: 1_000,
                qty_micro: 10_000_000,
                cost_micro: 5_000_000,
                token,
                ts: TickSize::Cent,
                condition: B256::ZERO,
            },
        );
        // The source trader SOLD the copied outcome after entry.
        let recent = HashMap::from([(
            "0xA".to_string(),
            vec![trade("0xA", "m1", 0, TradeSide::Sell, 0.60, 2_000)],
        )]);

        state.run_exit_sweep(&recent, &HashMap::new()).await;

        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![(token, Action::Sell, 60, 10_000_000)],
            "marketable taker SELL of the full held qty at the bid"
        );
        assert!(state.open.is_empty(), "position closed on follow-exit");
        assert_eq!(state.inv.net(token), 0, "flat after the exit");
        assert_eq!(
            state.realized_micro, 1_000_000,
            "sold $6 vs $5 cost → +$1 realized"
        );
        drop(state);
        let store = writer.await.unwrap();
        assert_eq!(store.count_fills().unwrap(), 1);
    }

    /// EXIT (b): no follow-exit signal, but the mark (best bid) is past the stop
    /// → stop-loss places a marketable taker SELL of the whole held qty.
    #[tokio::test]
    async fn stop_loss_sells_when_marked_below_threshold() {
        let token = TokenId(3);
        let venue = MockVenue {
            asks: HashMap::new(),
            bids: HashMap::from([(token, cent(37))]), // mark $0.37
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with(venue, HashMap::new(), copy_params());
        // Seed an open long: 10 sh @ $0.50, cost $5.
        state.inv.on_fill(token, 10_000_000, Usdc(-5_000_000));
        state.open.insert(
            ("m2".to_string(), 1),
            CopyPosition {
                trader: "0xB".into(),
                entry_ts: 1_000,
                qty_micro: 10_000_000,
                cost_micro: 5_000_000,
                token,
                ts: TickSize::Cent,
                condition: B256::ZERO,
            },
        );

        // No follow-exit (source didn't sell): $3.70 mark ≤ $3.75 floor → stop.
        state.run_exit_sweep(&HashMap::new(), &HashMap::new()).await;

        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![(token, Action::Sell, 37, 10_000_000)],
            "marketable taker SELL of the full held qty at the bid"
        );
        assert!(state.open.is_empty(), "position cut by the stop");
        assert_eq!(state.inv.net(token), 0);
        assert_eq!(
            state.realized_micro, -1_300_000,
            "sold $3.70 vs $5 cost → −$1.30 realized"
        );
        drop(state);
        let _ = writer.await;
    }

    /// Gated OFF: with NO venue the engine is fully inert — no entries, no
    /// booking, no panic — regardless of fresh candidates.
    #[tokio::test]
    async fn inert_without_a_venue() {
        let (store_tx, _store_rx) = mpsc::channel(16);
        let params = copy_params();
        let mut state: CopyLoop<MockVenue> = CopyLoop {
            venue: None,
            relayer: None,
            inv: InventoryRisk::new(inv_config(&params)),
            open: HashMap::new(),
            tradeable: tradeable_of("m1", 0, TokenId(1)),
            store_tx,
            params,
            realized_micro: 0,
        };

        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        state.run_exit_sweep(&HashMap::new(), &HashMap::new()).await;

        assert!(state.open.is_empty(), "no venue → no entry");
        assert_eq!(state.inv.net(TokenId(1)), 0, "no venue → nothing booked");
    }

    // ===================== loop: kill / pause (network-free, feed = None) =====================

    fn empty_registry() -> Arc<pm_registry::Registry> {
        Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap())
    }

    fn test_ctx(
        kill: Arc<AtomicBool>,
        ctl_rx: mpsc::Receiver<StrategyCommand>,
        status_tx: watch::Sender<StrategyStatus>,
    ) -> StrategyCtx {
        let (store_tx, _store_rx) = mpsc::channel(16);
        StrategyCtx {
            registry: empty_registry(),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx,
            kill,
            ctl_rx,
            status_tx,
        }
    }

    /// Short cadences so the loop ticks quickly in tests; no feed ⇒ no network.
    fn fast_params() -> CopyParams {
        let cp = pm_config::CopyParamsCfg {
            signal_poll_secs: 1,
            whitelist_refresh_secs: 3600,
            ..Default::default()
        };
        let mut p = CopyParams::from_config(&pm_config::CopyCfg::default(), &cp).expect("resolve");
        // Sub-second base cadence keeps the kill/pause tests fast.
        p.signal_poll = Duration::from_millis(5);
        p
    }

    /// Setting the global kill makes `run` return within a bounded timeout (the
    /// feed is `None`, so this exercises the loop's shutdown with no network).
    #[tokio::test]
    async fn copy_loop_exits_on_kill() {
        let strat = CopyStrategy::<MockVenue>::new(fast_params());
        assert_eq!(strat.id(), StrategyId("copy"));
        assert!(strat.make_on_apply().is_none());

        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let ctx = test_ctx(Arc::clone(&kill), ctl_rx, status_tx);

        let run = tokio::spawn(Box::new(strat).run(ctx));
        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("copy loop did not exit after kill")
            .expect("copy run task panicked");
    }

    /// `SetPaused(true)` is reflected in the published status (a heartbeat the
    /// host's aggregation sees), and a closed control channel shuts it down.
    #[tokio::test]
    async fn copy_loop_reflects_pause_and_exits_on_closed_control() {
        let strat = CopyStrategy::<MockVenue>::new(fast_params());
        let kill = Arc::new(AtomicBool::new(false));
        let (ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, mut status_rx) = watch::channel(StrategyStatus::default());
        status_rx.borrow_and_update();
        let ctx = test_ctx(Arc::clone(&kill), ctl_rx, status_tx);

        let run = tokio::spawn(Box::new(strat).run(ctx));
        ctl_tx
            .send(StrategyCommand::SetPaused(true))
            .await
            .expect("send pause");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if status_rx.borrow_and_update().paused {
                    break;
                }
                if status_rx.changed().await.is_err() {
                    panic!("status channel closed before reporting paused");
                }
            }
        })
        .await
        .expect("copy loop did not report paused within the timeout");

        // Dropping the control sender closes the channel ⇒ the loop returns.
        drop(ctl_tx);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("copy loop did not exit after control channel closed")
            .expect("copy run task panicked");
    }

    // ===================== C5: paper venue + end-to-end lifecycle =====================

    /// A zero-gas table for the paper-venue test (the copy taker pays venue fees
    /// via `fee_bps`, not gas; gas only matters for split/merge which copy never
    /// does).
    fn gas() -> GasTable {
        GasTable {
            split: 0,
            merge: 0,
            redeem: 0,
            negrisk_convert: 0,
        }
    }

    /// A book responder for one token: answers every `BookSnapshot` with a
    /// single (bid, ask) Cent book. The test analogue of a live feed behind a
    /// [`BookFetcher`] (mirrors the publisher tests' `spawn_book`).
    fn spawn_paper_book(
        bid_tick: u16,
        ask_tick: u16,
    ) -> mpsc::Sender<pm_ingestion::supervisor::SupervisorCommand> {
        let (tx, mut rx) = mpsc::channel(8);
        tokio::spawn(async move {
            use pm_core::book::{Book, Side};
            while let Some(pm_ingestion::supervisor::SupervisorCommand::BookSnapshot {
                reply,
                ..
            }) = rx.recv().await
            {
                let mut b = Book::new(TickSize::Cent);
                b.apply(
                    Side::Bid,
                    Px::new(bid_tick, TickSize::Cent).unwrap(),
                    Qty(200_000_000),
                );
                b.apply(
                    Side::Ask,
                    Px::new(ask_tick, TickSize::Cent).unwrap(),
                    Qty(200_000_000),
                );
                let _ = reply.send(Some((b, true)));
            }
        });
        tx
    }

    /// The PAPER taker [`CopyVenue`] (C5): `best_ask` / `best_bid` read the live
    /// book off the `BookFetcher`, and `submit_fak` fills HONESTLY at that book
    /// (no midpoints, no infinite depth) — with NO live calls. An unknown token
    /// degrades to `None` (no price), never a panic.
    #[tokio::test]
    async fn paper_copy_venue_reads_book_and_fills_at_book() {
        let token = TokenId(7);
        let fetcher = BookFetcher::new(HashMap::from([(token, spawn_paper_book(40, 50))]));
        let mut v = PaperCopyVenue::new(fetcher, Duration::ZERO, gas());

        // best_ask / best_bid read the current book.
        assert_eq!(
            CopyVenue::best_ask(&mut v, token, TickSize::Cent).await,
            Some(cent(50)),
            "best ask is the live $0.50 ask"
        );
        assert_eq!(
            CopyVenue::best_bid(&mut v, token, TickSize::Cent).await,
            Some(cent(40)),
            "best bid is the live $0.40 bid"
        );
        // submit_fak fills honestly at the book: buy 5 sh @ $0.50 → −$2.50 cost.
        let order = Order::new(
            "copy:m:0".into(),
            token,
            Action::Buy,
            TickSize::Cent,
            cent(50),
            Qty(5_000_000),
            Bps(0),
        );
        let out = CopyVenue::submit_fak(&mut v, &order).await.unwrap();
        assert_eq!(out.filled, Qty(5_000_000), "the FAK fully fills against depth");
        assert_eq!(out.fills[0].cash.0, -2_500_000, "5 sh × $0.50 = $2.50 paid");
        // An unknown token has no book → no price (None), no panic.
        assert_eq!(
            CopyVenue::best_ask(&mut v, TokenId(999), TickSize::Cent).await,
            None
        );
    }

    /// PAPER/mocked END-TO-END (Task C5; mirrors the MM's `paper_end_to_end_*`):
    /// ONE `CopyLoop`, backed by a REAL in-memory store + writer, driven through
    /// the full lifecycle on a single shared inventory/cap/store ledger —
    ///   (1) a whitelisted trader's FRESH buy within drift → a copy ENTRY is
    ///       booked and the capital/gross caps are consumed;
    ///   (2) that trader SELLS → the FOLLOW-EXIT sells our copy out;
    ///   (3) a separate marked-down position → the STOP-LOSS sells it; and
    ///   (4) the DORMANT case (no venue — the `strategies.copy.enabled=false`
    ///       analogue, where main never even wires a venue) → a fresh candidate
    ///       books NOTHING.
    /// Exact-integer assertions throughout (spec §21 style); the recorded order
    /// sequence proves the whole pipeline ran end-to-end on one ledger.
    #[tokio::test]
    async fn copy_paper_end_to_end_lifecycle() {
        let t1 = TokenId(1); // m1/0 — entry + follow-exit
        let t2 = TokenId(2); // m2/1 — stop-loss
        let venue = MockVenue {
            asks: HashMap::from([(t1, cent(50))]), // entry ask $0.50
            // m1 exit bid $0.58 (a profit), m2 mark/exit bid $0.37 (past the stop).
            bids: HashMap::from([(t1, cent(58)), (t2, cent(37))]),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let mut tradeable = tradeable_of("m1", 0, t1);
        tradeable.insert(
            ("m2".to_string(), 1),
            TradeTokenInfo {
                token: t2,
                ts: TickSize::Cent,
                condition: B256::ZERO,
            },
        );
        let (mut state, writer) = loop_with(venue, tradeable, copy_params());

        // (1) ENTRY — fresh, within drift → buy 10 sh @ $0.50, $5 cost.
        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        assert_eq!(state.open.len(), 1, "one copy position opened");
        assert_eq!(
            state.deployed_micro(),
            5_000_000,
            "capital + gross headroom consumed by the $5 cost"
        );
        assert!(seen.contains(&("m1".to_string(), 0)), "the entry signal is seen");
        assert_eq!(state.inv.net(t1), 10_000_000, "10 sh long booked");

        // (2) FOLLOW-EXIT — the SOURCE trader 0xA SELLS m1/0 after our entry.
        let recent = HashMap::from([(
            "0xA".to_string(),
            vec![trade("0xA", "m1", 0, TradeSide::Sell, 0.58, 9_500)],
        )]);
        state.run_exit_sweep(&recent, &HashMap::new()).await;
        assert!(
            !state.open.contains_key(&("m1".to_string(), 0)),
            "m1 closed on follow-exit"
        );
        assert_eq!(state.inv.net(t1), 0, "flat on m1 after the follow-exit");
        assert_eq!(
            state.realized_micro, 800_000,
            "sold $5.80 vs $5.00 cost → +$0.80 realized"
        );

        // (3) STOP-LOSS — seed a second long (m2/1, 10 sh @ $0.50); the mark (bid
        //     $0.37) is below the 25% stop floor ($3.75) → cut it.
        state.inv.on_fill(t2, 10_000_000, Usdc(-5_000_000));
        state.open.insert(
            ("m2".to_string(), 1),
            CopyPosition {
                trader: "0xB".into(),
                entry_ts: 1_000,
                qty_micro: 10_000_000,
                cost_micro: 5_000_000,
                token: t2,
                ts: TickSize::Cent,
                condition: B256::ZERO,
            },
        );
        state.run_exit_sweep(&HashMap::new(), &HashMap::new()).await;
        assert!(state.open.is_empty(), "m2 cut by the stop-loss");
        assert_eq!(state.inv.net(t2), 0, "flat on m2 after the stop");
        assert_eq!(
            state.realized_micro,
            800_000 - 1_300_000,
            "sold $3.70 vs $5.00 → −$1.30; cumulative −$0.50"
        );

        // The venue saw exactly: ENTRY buy, FOLLOW-EXIT sell, STOP-LOSS sell.
        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![
                (t1, Action::Buy, 50, 10_000_000),
                (t1, Action::Sell, 58, 10_000_000),
                (t2, Action::Sell, 37, 10_000_000),
            ],
            "the full lifecycle ran on one loop, in order"
        );

        drop(state);
        let store = writer.await.unwrap();
        assert_eq!(
            store.count_fills().unwrap(),
            3,
            "entry + two exits persisted via the OrderInsert→FillSigned FK path"
        );

        // (4) DORMANT — the `strategies.copy.enabled=false` analogue (main never
        //     wires a venue): a no-venue loop books NOTHING from a fresh candidate.
        let (store_tx, _store_rx) = mpsc::channel(16);
        let params = copy_params();
        let mut dormant: CopyLoop<MockVenue> = CopyLoop {
            venue: None,
            relayer: None,
            inv: InventoryRisk::new(inv_config(&params)),
            open: HashMap::new(),
            tradeable: tradeable_of("m1", 0, t1),
            store_tx,
            params,
            realized_micro: 0,
        };
        let mut seen2 = SeenKeys::default();
        dormant
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen2)
            .await;
        assert!(
            dormant.open.is_empty(),
            "a disabled / no-venue copy strategy is dormant — no entries"
        );
    }
}
