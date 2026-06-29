//! `CopyStrategy` — the smart-money COPY executor SCAFFOLD (Task C3).
//!
//! It runs in the [`StrategyHost`](super::host::StrategyHost) exactly like the
//! market maker ([`MmStrategy`](super::mm::MmStrategy)): it owns a capital
//! envelope, honors `ctx.kill` + the per-strategy `ctl_rx` pause/kill controls,
//! publishes a [`StrategyStatus`], and drives its own async loop on a cadence.
//!
//! What it does in C3 (READ-ONLY signal pipeline — NO order placement / exits;
//! those are C4):
//!
//! 1. **Whitelist refresh** — every `whitelist_refresh_secs` it rebuilds the
//!    follow whitelist of skilled traders via [`refresh_whitelist`], reusing
//!    C1's shared, validated [`pm_ingestion::smart_money`] ranking
//!    ([`Ranking::EdgePerBet`] over each trader's whole resolved record). A
//!    refresh that hits ANY fetch error keeps the PRIOR whitelist (never trade
//!    on an empty/stale-failed set).
//! 2. **Fresh-signal poll** — every `signal_poll_secs` (when not paused) it
//!    polls each whitelisted wallet's recent `/trades` and runs the PURE
//!    [`select_signals`] to pick fresh, not-yet-acted buys, then `tracing::info!`s
//!    each [`CopyCandidate`] and marks it `seen`. C4 replaces the log with an
//!    actual entry; the `seen` set is the de-dup that stops a signal re-firing.
//!
//! The strategy is OFF by default (it only runs when `strategies.copy.enabled`,
//! wired by main); when cleared for live it `start_paused` like the MM, and in
//! any case places no orders in C3.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pm_core::num::Usdc;
use pm_ingestion::data_api::{
    ClosedPos, DataApiClient, LeaderboardEntry, OrderBy, TimePeriod, Trade, TradeSide, TradesFilter,
};
use pm_ingestion::smart_money::{Ranking, rank_wallets_oos, trader_records};
use pm_ingestion::supervisor::OnApplyFn;
use tokio::time::MissedTickBehavior;

use crate::coordinator::now_ms;

use super::{Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};

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

/// Poll each whitelisted wallet's recent `/trades`, run the pure
/// [`select_signals`], LOG each fresh [`CopyCandidate`], and mark it `seen` so
/// it never re-fires. Returns the candidate count (for status/telemetry).
///
/// Best-effort: a per-wallet `/trades` error just skips that wallet this cycle
/// (no panic, no abort) — a transient miss costs at most one poll for one
/// trader. With no feed or an empty whitelist there is nothing to poll.
///
/// C4 replaces the `tracing::info!` with an actual (sized, freshness-gated)
/// entry; the `seen` insert stays as the de-dup.
async fn poll_signals(
    feed: &Option<Arc<DataApiClient>>,
    whitelist: &[String],
    seen: &mut HashSet<(String, i64)>,
    params: &CopyParams,
) -> usize {
    let Some(client) = feed else {
        return 0;
    };
    if whitelist.is_empty() {
        return 0;
    }
    let now = now_ms() / 1000;
    let mut recent_by_wallet: HashMap<String, Vec<Trade>> = HashMap::new();
    for wallet in whitelist {
        if let Ok(trades) = client.trades(TradesFilter::User(wallet), POLL_TRADE_LIMIT).await {
            recent_by_wallet.insert(wallet.clone(), trades);
        }
    }
    let candidates = select_signals(
        &recent_by_wallet,
        whitelist,
        seen,
        now,
        params.reaction_window_secs,
    );
    for c in &candidates {
        tracing::info!(
            condition_id = %c.condition_id,
            outcome_index = c.outcome_index,
            trader = %c.trader,
            trigger_px = c.trigger_px,
            timestamp = c.timestamp,
            "copy: candidate copy signal (C3 scaffold — logged only, no order placed)"
        );
        seen.insert((c.condition_id.clone(), c.outcome_index));
    }
    candidates.len()
}

// ---------------------------------------------------------------------------
// The CopyStrategy shell + its async loop
// ---------------------------------------------------------------------------

/// Smart-money COPY strategy (Task C3 scaffold). Constructed by main; `run`
/// drives the read-only whitelist→signal pipeline. Holds its resolved
/// [`CopyParams`] (which carries the capital envelope), the Data-API feed (the
/// signal/whitelist source), and whether to start paused (live held).
///
/// The runtime state — the `seen` de-dup set, the current `whitelist`, and the
/// `paused` flag — lives in [`run_copy_loop`] (the loop owns it after `run`
/// consumes `self`), mirroring how [`MmStrategy`](super::mm::MmStrategy) holds
/// config and `run_mm_loop` owns the live state.
pub struct CopyStrategy {
    id: StrategyId,
    params: CopyParams,
    /// The Data-API feed (leaderboard / trades / closed positions). `None` (the
    /// default) makes the strategy an inert heartbeat — no whitelist, no polling
    /// — which is what the kill/pause unit tests run against (no network).
    feed: Option<Arc<DataApiClient>>,
    /// Start the loop PAUSED when live is held (the operator releases via the
    /// host's `SetPaused(false)`), mirroring the MM. No orders are placed in C3
    /// regardless; this gates the (future C4) live path.
    start_paused: bool,
}

impl CopyStrategy {
    /// Construct the copy strategy from its resolved params. The feed is absent
    /// by default (an inert heartbeat); attach it with [`with_feed`](Self::with_feed).
    pub fn new(params: CopyParams) -> Self {
        CopyStrategy {
            id: StrategyId("copy"),
            params,
            feed: None,
            start_paused: false,
        }
    }

    /// Attach (or clear) the Data-API feed that sources the whitelist + signals.
    /// main passes `Some` when the copy strategy is enabled; `None` keeps it an
    /// inert heartbeat.
    pub fn with_feed(mut self, feed: Option<Arc<DataApiClient>>) -> Self {
        self.feed = feed;
        self
    }

    /// Start the loop PAUSED (live held). The host sends `SetPaused(false)` on
    /// release. Mirrors [`MmStrategy::with_start_paused`](super::mm::MmStrategy::with_start_paused).
    pub fn with_start_paused(mut self, start_paused: bool) -> Self {
        self.start_paused = start_paused;
        self
    }
}

impl Strategy for CopyStrategy {
    fn id(&self) -> StrategyId {
        self.id
    }

    /// The copy strategy reads the Data API on its own cadence; it observes no
    /// per-supervisor book updates, so it installs no inline hook (and emits no
    /// orders).
    fn make_on_apply(&self) -> Option<OnApplyFn> {
        None
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let CopyStrategy {
                id: _,
                params,
                feed,
                start_paused,
            } = *self;
            run_copy_loop(ctx, params, feed, start_paused).await;
        })
    }
}

/// The copy strategy's owned async loop. Two cadences via independent interval
/// timers — whitelist refresh (`whitelist_refresh`) and signal poll
/// (`signal_poll`) — plus the per-strategy control channel, exactly like the
/// MM's loop honors `ctx.kill` + `ctl_rx`:
///
/// * the global `kill` is checked at the top of every iteration (final status
///   published on the way out — the trait's out-of-band reporting contract);
/// * `SetPaused` toggles `paused` (a paused loop refreshes the whitelist but
///   does NOT poll for signals);
/// * a closed control channel (host dropped the sender) shuts the loop down.
///
/// No orders are placed in C3: a signal is logged and marked `seen`.
async fn run_copy_loop(
    ctx: StrategyCtx,
    params: CopyParams,
    feed: Option<Arc<DataApiClient>>,
    start_paused: bool,
) {
    let StrategyCtx {
        kill,
        mut ctl_rx,
        status_tx,
        ..
    } = ctx;

    // Runtime state the loop owns (the strategy struct is config-only).
    let mut seen: HashSet<(String, i64)> = HashSet::new();
    let mut whitelist: Vec<String> = Vec::new();
    let mut paused = start_paused;
    if start_paused {
        tracing::info!("copy: live held — signal polling PAUSED until release (press `l`)");
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
        // and publish a final status out-of-band before returning.
        if kill.load(Ordering::Relaxed) {
            let _ = status_tx.send(StrategyStatus {
                paused,
                ..Default::default()
            });
            return;
        }
        tokio::select! {
            _ = whitelist_tick.tick() => {
                if let Some(client) = &feed {
                    match refresh_whitelist(client, &params).await {
                        Some(wl) => {
                            tracing::info!(
                                traders = wl.len(),
                                "copy: whitelist refreshed (EdgePerBet, top {})",
                                params.top_n
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
                if !paused {
                    let n = poll_signals(&feed, &whitelist, &mut seen, &params).await;
                    if n > 0 {
                        tracing::info!(
                            candidates = n,
                            whitelist = whitelist.len(),
                            "copy: fresh copy signals this poll (C3 scaffold — logged only)"
                        );
                    }
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
        // Publish the live paused/halted view after each handled event. C5 may
        // surface whitelist size / candidate count richly; C3 logs those and
        // publishes the existing zero-valued status (no positions, no money).
        let _ = status_tx.send(StrategyStatus {
            paused,
            ..Default::default()
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]

    use std::sync::atomic::AtomicBool;

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
        let strat = CopyStrategy::new(fast_params());
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
        let strat = CopyStrategy::new(fast_params());
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
}
