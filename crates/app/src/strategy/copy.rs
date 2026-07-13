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
use pm_ingestion::rest::ClobRest;
use pm_ingestion::smart_money::{
    category_adaptive_records, creamy_layer, market_category, trader_records, within_drift,
};
use pm_ingestion::supervisor::OnApplyFn;
use pm_registry::gamma::ClobMarket;
use pm_risk::inventory::{InventoryConfig, InventoryRisk, Marks};
use pm_store::read::ReadStore;
use pm_store::writer::StoreMsg;
use pm_store::{CopyPositionRow, FillRow, OrderRow, usdc_to_i64, utc_day_from_ms};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::coordinator::now_ms;

use super::mm::read_day_loss;
use super::{CopyStatus, Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};

/// Max fills pulled from each whitelisted wallet's `/trades?user=` history when
/// RANKING (the 6-hourly whitelist refresh). Matches the backtest's
/// `DEFAULT_TRADE_LIMIT` so the live whitelist ranks on the same depth of
/// history the offline grid validated.
const WHITELIST_TRADE_LIMIT: usize = 1000;

// SPECIALIST-ROUTING knobs (the two-stage ranking that REPLACED flat EdgePerBet).
// These mirror the values the backtest validated (SpecialistAdaptive beat the old
// global EdgePerBet ~4× on Sharpe, hit 57.5% vs 52.6%, 1/3 the drawdown, and won
// 65/72 matched configs). Kept as consts (not config) so the live ranking is
// EXACTLY the validated one; plumb to `[copy]` later if they need tuning.
/// Recency half-life (DAYS) for the per-(trader, category) adaptive-skill decay.
const SPECIALIST_HALF_LIFE_DAYS: f64 = 45.0;
/// Effective-sample floor per (trader, category) to qualify for the creamy layer.
const SPECIALIST_MIN_BETS: f64 = 5.0;
/// Creamy-layer size PER category (top specialists kept).
const SPECIALIST_TOP_K_PER_CAT: usize = 10;
/// Lower-confidence-bound `z` for the adaptive score (`mean − z·stdev/√n_eff`).
const SPECIALIST_Z: f64 = 1.0;

/// Max fills pulled from each whitelisted wallet's `/trades?user=` history on
/// the fast SIGNAL poll. We only care about buys inside the reaction window, so
/// one page (the most recent fills) is plenty and keeps the poll light.
const POLL_TRADE_LIMIT: usize = 100;

/// Timeout for ONE poll cycle (exit sweep + reconcile + entries). Both this and
/// the whitelist rank are awaited inline in the loop's `select!`, so a hung
/// venue/Data-API await would otherwise silently freeze the whole loop (stopping
/// stop-losses / follow-exits) while the process looks healthy. On timeout the
/// cycle is skipped and retried next tick — the fix for the observed silent stall.
const POLL_CYCLE_TIMEOUT: Duration = Duration::from_secs(120);
/// Timeout for the periodic EdgePerBet whitelist rank (up to `top_n` traders ×
/// paged trade history). Generous — a legit rank can take a while — but bounded
/// so a slow/stuck rank can't freeze the poll arm. On timeout, keep the prior list.
const WHITELIST_REFRESH_TIMEOUT: Duration = Duration::from_secs(300);

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
    /// Gross exposure cap across open copied positions, µUSDC (C4 gating). When
    /// `gross_pct > 0` this is only the FALLBACK; the effective cap is recomputed
    /// each cycle from live equity (see `effective_caps`).
    pub max_gross_micro: i128,
    /// DYNAMIC gross cap as a fraction of live account equity (cash + positions),
    /// in `[0, 1]`. `0` ⇒ static `max_gross_micro`. When `> 0`, each cycle the
    /// loop sets `max_gross = gross_pct × equity` (and the capital carve), keeps
    /// the per-copy notional FIXED at `per_position_micro`, and scales CONCURRENCY
    /// — `max_concurrent = min(max_gross / per_position, max_concurrent_positions)`
    /// — so a funded account opens MORE fixed-size copies (up to the hard cap)
    /// rather than larger ones. Falls back to the static caps if equity is unknown.
    pub gross_pct: f64,
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
            gross_pct: params.gross_pct,
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
    /// The CLOB-tradeable token id of the bought outcome (the trade's `asset`) —
    /// the EXACT token we mirror. Carried so an UNSYNCED market can be registered
    /// on the venue ON-DEMAND (see `resolve_ondemand`) without re-deriving which
    /// side from the market metadata.
    pub asset: String,
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
/// * the wallet is a CREAMY-LAYER SPECIALIST for the market's category
///   ([`market_category`] of the trade's slug/title): a trader is copied ONLY in
///   categories where they're proven, per the two-stage specialist routing.
///
/// DE-DUP: when several qualifying buys share a `(condition_id, outcome_index)`
/// — whether two wallets on the same side or one wallet twice — only ONE
/// candidate is emitted, the MOST RECENT by `timestamp` (ties broken by
/// whitelist order: the first listed wallet wins). The latest buy is the
/// freshest evidence the smart money is STILL buying this side, and is the right
/// drift reference: it MINIMIZES the gap to the current book (the earliest buy in
/// a long reaction window is the stalest, cheapest fill — it maximizes drift and
/// makes the freshness gate reject signals the trader is actively re-buying). The
/// result is returned in a DETERMINISTIC order (`timestamp`, then `condition_id`,
/// then `outcome_index`), independent of `HashMap` iteration order, so the live
/// loop and the tests see a stable sequence.
pub(crate) fn select_signals(
    recent_by_wallet: &HashMap<String, Vec<Trade>>,
    creamy: &HashMap<String, HashSet<String>>,
    seen: &HashSet<(String, i64)>,
    now: i64,
    reaction_window_secs: i64,
) -> Vec<CopyCandidate> {
    // MOST RECENT qualifying buy per (condition_id, outcome_index) — the freshest
    // conviction + the smallest drift to the current book. Tie-break on the wallet
    // string so the pick is deterministic regardless of HashMap iteration order.
    let mut best: HashMap<(String, i64), CopyCandidate> = HashMap::new();
    let stale_before = now - reaction_window_secs;
    for (wallet, trades) in recent_by_wallet {
        for t in trades {
            if t.side != TradeSide::Buy {
                continue;
            }
            if t.timestamp < stale_before {
                continue; // older than the reaction window → stale
            }
            // SPECIALIST ROUTING: copy this trader ONLY where they are a proven
            // creamy-layer specialist for the market's category.
            let cat = market_category(&t.slug, &t.title);
            if !creamy.get(&cat).is_some_and(|set| set.contains(wallet)) {
                continue;
            }
            let key = (t.condition_id.clone(), t.outcome_index);
            if seen.contains(&key) {
                continue; // already acted on this market+side
            }
            let replace = match best.get(&key) {
                Some(existing) => {
                    t.timestamp > existing.timestamp
                        || (t.timestamp == existing.timestamp
                            && wallet.as_str() < existing.trader.as_str())
                }
                None => true,
            };
            if replace {
                best.insert(
                    key,
                    CopyCandidate {
                        condition_id: t.condition_id.clone(),
                        outcome_index: t.outcome_index,
                        asset: t.asset.clone(),
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

/// The follow set produced by [`refresh_whitelist`]: the per-category CREAMY
/// LAYER (`category → specialist wallets`) that [`select_signals`] routes on,
/// plus a FLAT union of all specialist wallets in best-first order (for the
/// universe sync + the whitelist-size telemetry).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Whitelist {
    /// Stage-1 creamy layer: a buy is copyable only if its trader is in the set
    /// for that market's [`market_category`].
    pub creamy: HashMap<String, HashSet<String>>,
    /// Union of all creamy-layer wallets, ordered best-specialist-first (highest
    /// adaptive score across categories) — what the universe sync pulls positions
    /// for and the telemetry counts.
    pub flat: Vec<String>,
}

/// Rebuild the follow set via the TWO-STAGE SPECIALIST ROUTING that replaced the
/// old global [`Ranking::EdgePerBet`] (validated in the backtest: ~4× the Sharpe,
/// higher hit rate, a third of the drawdown, winning 65/72 matched configs).
///
/// Pull the PnL leaderboard (month ∪ all-time, `top_n` each, de-duped), fetch
/// each trader's own `/trades` + closed positions, derive resolutions from the
/// closed positions ([`fold_resolutions`]), then:
/// - STAGE 1 — [`category_adaptive_records`] scores each `(trader, category)` on
///   a recency-decayed (`SPECIALIST_HALF_LIFE_DAYS`), sample-aware edge, and
///   [`creamy_layer`] keeps the top `SPECIALIST_TOP_K_PER_CAT` specialists per
///   category (effective sample ≥ `SPECIALIST_MIN_BETS`, positive score);
/// - STAGE 2 — keep only creamy-layer traders whose GLOBAL pre-cutoff
///   [`trader_records`] `mean_edge > 0` (the "EdgePerBet on the creamy layer"
///   gate).
///
/// CUTOFF: live wants each trader's WHOLE resolved record, so `cutoff_ts = now`
/// and the recency decay is measured back from now.
///
/// Returns `Some(Whitelist)` on success (possibly empty if nobody qualifies).
/// Returns `None` on ANY fetch error, so the caller KEEPS the prior set — we
/// never trade on a set a transient API failure emptied or staled.
pub async fn refresh_whitelist(client: &DataApiClient, params: &CopyParams) -> Option<Whitelist> {
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

    // 3. SPECIALIST ROUTING (cutoff = now → whole resolved record, recency-decayed).
    let now = now_ms() / 1000;
    let half_life_secs = SPECIALIST_HALF_LIFE_DAYS * 86_400.0;
    // Stage 1: per-(trader, category) adaptive records → per-category creamy layer.
    let adaptive =
        category_adaptive_records(&trades_by_wallet, &resolutions, now, half_life_secs, SPECIALIST_Z);
    let mut creamy = creamy_layer(&adaptive, SPECIALIST_TOP_K_PER_CAT, SPECIALIST_MIN_BETS);
    // Stage 2: EdgePerBet gate — drop creamy traders whose GLOBAL mean edge ≤ 0.
    let records = trader_records(&trades_by_wallet, &resolutions, now);
    let edge_ok: HashSet<&str> = records
        .iter()
        .filter(|(_, r)| r.mean_edge > 0.0)
        .map(|(w, _)| w.as_str())
        .collect();
    for set in creamy.values_mut() {
        set.retain(|w| edge_ok.contains(w.as_str()));
    }
    creamy.retain(|_, set| !set.is_empty());

    // Flat union, best-specialist-first (highest adaptive score across categories)
    // so the universe cap keeps the strongest specialists' markets.
    let mut best_score: HashMap<&str, f64> = HashMap::new();
    for ((wallet, cat), r) in &adaptive {
        if creamy.get(cat).is_some_and(|s| s.contains(wallet)) {
            let e = best_score.entry(wallet.as_str()).or_insert(f64::MIN);
            if r.score > *e {
                *e = r.score;
            }
        }
    }
    let mut flat_scored: Vec<(String, f64)> =
        best_score.into_iter().map(|(w, s)| (w.to_string(), s)).collect();
    flat_scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let flat: Vec<String> = flat_scored.into_iter().map(|(w, _)| w).collect();

    Some(Whitelist { creamy, flat })
}

/// De-dup a stream of OPEN-position `conditionId`s into a stable, first-seen list
/// capped at `cap`. The input is built in whitelist order (best trader first)
/// and, within a trader, the API's position order — so when the cap bites the
/// HIGHEST-conviction smart-money markets survive. Pure (the TDD'd core of
/// [`whitelist_universe_conditions`]); `cap == 0` ⇒ empty.
pub(crate) fn dedup_open_conditions(conds: &[String], cap: usize) -> Vec<String> {
    if cap == 0 {
        return Vec::new();
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for c in conds {
        if seen.insert(c.as_str()) {
            out.push(c.clone());
            if out.len() >= cap {
                break;
            }
        }
    }
    out
}

/// Build the copy strategy's smart-money UNIVERSE: the markets its EdgePerBet
/// `whitelist` is ACTIVE in (their OPEN positions), as `conditionId`s for the
/// universe sync ([`pm_ingestion::sync::SyncTask::with_confluence_conditions`]).
///
/// For each whitelisted wallet it pulls `/positions` (size ≥ `size_threshold`),
/// keeps the OPEN ([`pm_ingestion::data_api::Position::is_open`]) ones, and
/// unions their condition ids in whitelist-then-API order, de-duped + capped at
/// `cap` ([`dedup_open_conditions`]). This makes the synced universe == the
/// markets the copy strategy's OWN whitelist holds (≈1:1 coverage with its
/// signals) instead of a generic liquidity scan.
///
/// Returns `None` on ANY positions-fetch error (the caller FALLS BACK to
/// confluence / the liquidity universe rather than syncing a partial set);
/// `Some(vec)` otherwise (empty if the whitelist holds no open positions).
pub async fn whitelist_universe_conditions(
    client: &DataApiClient,
    whitelist: &[String],
    size_threshold: f64,
    cap: usize,
) -> Option<Vec<String>> {
    let mut open: Vec<String> = Vec::new();
    for wallet in whitelist {
        let positions = client.positions(wallet, size_threshold).await.ok()?;
        for p in positions {
            if p.is_open() {
                open.push(p.condition_id);
            }
        }
    }
    Some(dedup_open_conditions(&open, cap))
}

/// Classify a CLOB `minimum_tick_size` float into a supported [`TickSize`], or
/// `None` for an unsupported (e.g. legacy 0.04) grid we won't quote on. Mirrors
/// `pm_ingestion::sync`'s own private classifier (kept local so the on-demand
/// path doesn't widen the sync crate's public surface).
fn classify_tick(tick: f64) -> Option<TickSize> {
    if (tick - 0.01).abs() < 1e-9 {
        Some(TickSize::Cent)
    } else if (tick - 0.001).abs() < 1e-9 {
        Some(TickSize::Milli)
    } else {
        None
    }
}

/// Pure: from a freshly-fetched [`ClobMarket`] + the trade's `asset` token + the
/// market's `condition_id` string, derive the `(tick, neg_risk, condition)`
/// needed to register that token ON-DEMAND ([`CopyVenue::ensure_token`]) and
/// trade it. Returns `None` (⇒ skip, never trade) when:
/// * `asset` is empty or is NOT one of THIS market's outcome tokens (defensive —
///   never register a token that doesn't belong to the condition);
/// * the tick is unsupported ([`classify_tick`]);
/// * the `condition_id` does not parse as a `B256` (the relayer redeem key).
fn ondemand_token_params(
    m: &ClobMarket,
    asset: &str,
    condition_id: &str,
) -> Option<(TickSize, bool, B256)> {
    if asset.is_empty() || !m.tokens.iter().any(|t| t.token_id == asset) {
        return None;
    }
    let ts = classify_tick(m.minimum_tick_size)?;
    let condition = condition_id.parse::<B256>().ok()?;
    Some((ts, m.neg_risk, condition))
}

/// [`TickSize`] → its decimal-places code for persistence (`2` = cent 0.01, `3`
/// = milli 0.001). Inverse of [`tick_from_decimals`].
fn tick_to_decimals(ts: TickSize) -> i64 {
    match ts {
        TickSize::Cent => 2,
        TickSize::Milli => 3,
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

/// Build the persistable [`CopyPositionRow`] for an open position, so entry and
/// partial exits keep the durable row in sync with the live `open` map (what a
/// restart reloads to resume management).
fn position_row(condition_id: &str, outcome_index: i64, pos: &CopyPosition) -> CopyPositionRow {
    CopyPositionRow {
        condition_id: condition_id.to_string(),
        outcome_index,
        asset: pos.asset.clone(),
        neg_risk: pos.neg_risk,
        tick_decimals: tick_to_decimals(pos.ts),
        condition_hex: format!("{:#x}", pos.condition),
        trader: pos.trader.clone(),
        entry_ts: pos.entry_ts,
        // µshares / µUSDC are tiny for a canary; clamp defensively into i64.
        qty_micro: pos.qty_micro.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
        cost_micro: pos.cost_micro.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
    }
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

/// On-chain reconcile thresholds. A wallet position is RESOLVED when its mark
/// degenerates (`≤ eps` ⇒ loser, `≥ 1-eps` ⇒ winner) or it is `redeemable`; LIVE
/// otherwise. A tracked row missing from the wallet for [`RECON_MISS_PRUNE`]
/// consecutive reconciles is pruned (debounce vs a transient Data-API blip).
const RECON_RESOLVED_EPS: f64 = 0.02;
const RECON_MISS_PRUNE: u32 = 2;
const RECON_DUST_SHARES: f64 = 0.01;
/// Max ORPHAN winners the reconcile redeem-sweep collects per pass — resolved-won
/// positions the wallet holds that AREN'T in our tracked book (stranded by a past
/// failed redeem or dropped from tracking). Capped so a backlog drains gradually
/// instead of bursting the relayer; a successful redeem removes the position, so
/// the backlog shrinks each reconcile.
const RECON_SWEEP_MAX_PER_CYCLE: usize = 4;

/// What the on-chain reconcile should do with a tracked position, given the live
/// wallet state for its `(condition_id, outcome_index)` as `(size, cur_price,
/// redeemable)`, or `None` when the wallet doesn't hold it. Pure ⇒ unit-tested
/// with no network/venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconAction {
    /// Still a live, tradeable holding — leave it under management.
    Keep,
    /// Held but resolved — settle (winner ⇒ book gain + redeem, loser ⇒ book loss).
    Settle { won: bool },
    /// Not (meaningfully) held in the wallet — prune candidate (debounced).
    Gone,
}

pub(crate) fn reconcile_action(held: Option<(f64, f64, bool)>) -> ReconAction {
    match held {
        Some((size, cur_price, redeemable)) if size > RECON_DUST_SHARES => {
            if redeemable || cur_price <= RECON_RESOLVED_EPS || cur_price >= 1.0 - RECON_RESOLVED_EPS
            {
                ReconAction::Settle { won: cur_price >= 0.5 }
            } else {
                ReconAction::Keep
            }
        }
        _ => ReconAction::Gone,
    }
}

/// Whether an exit (SELL FAK) rejection is TERMINAL — i.e. the venue is telling us
/// the position isn't held as tracked (a balance/amount error that will recur on
/// every retry: phantom row, already redeemed/sold, stale qty). Such a position
/// must be PRUNED, not retried forever. A transient/other error (network, rate
/// limit, one-sided book) returns `false` → keep and retry. Case-insensitive.
pub(crate) fn exit_rejection_is_terminal(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    e.contains("not enough balance")
        || e.contains("invalid maker amount")
        || e.contains("invalid taker amount")
        || e.contains("invalid amount")
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

/// FALLBACK inventory-ledger config for the INERT / test paths only. Production
/// threads the REAL `[inventory]` caps (`config.inventory`) into the loop via
/// [`CopyStrategy::with_inventory_config`], EXACTLY like the MM — so the
/// cumulative-loss circuit breaker (`inv.halted()` inventory stop-loss + the
/// persistent day-loss cap) keys off the operator-configured floors rather than
/// a value derived from the per-trade caps (which made `inv.halted()` meaningless).
///
/// This fallback only backs a [`CopyStrategy`] built WITHOUT
/// `with_inventory_config` (the no-store inert heartbeat + the unit tests that
/// don't exercise the halt), keeping the accounting ledger's own caps sane
/// there. The volatility hint is off — the copy loop quotes nothing.
fn fallback_inv_config(params: &CopyParams) -> InventoryConfig {
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

    /// Register an UNSYNCED market's outcome token ON-DEMAND, returning the
    /// internal [`TokenId`] to trade it by (book reads + FAK). Idempotent by
    /// `venue_id`. `None` (the default) ⇒ the venue can't trade unsynced markets
    /// (the PAPER venue has no live book feed for them), so the caller skips —
    /// only the LIVE venue overrides this. This is what lets the copy executor
    /// trade a fresh smart-money signal in a market the periodic universe
    /// snapshot never covered, the moment it is seen.
    fn ensure_token(&mut self, _venue_id: &str, _neg_risk: bool, _ts: TickSize) -> Option<TokenId> {
        None
    }

    /// The account's spendable CLOB collateral (µUSDC) — the "cash" leg of live
    /// account equity that the copy strategy sizes its caps against. `None` (the
    /// default) ⇒ unavailable (paper / no live balance), so the caller falls back
    /// to the fixed configured caps. Only the LIVE venue overrides this.
    fn available_collateral_micro(&mut self) -> impl Future<Output = Option<i128>> + Send {
        async { None }
    }

    /// The deposit (proxy) wallet address (lowercase hex) whose Polymarket
    /// portfolio value is the POSITIONS leg of live account equity (fetched via
    /// the Data-API `/value`). `None` (default / paper) ⇒ no account to value, so
    /// the caller falls back to the static caps.
    fn deposit_wallet(&self) -> Option<String> {
        None
    }
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
    /// Neg-risk flag for the market — carried so a RELOADED position can be
    /// re-registered on the venue ([`CopyVenue::ensure_token`]) with the correct
    /// exchange for signing exit orders.
    pub neg_risk: bool,
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

    fn ensure_token(&mut self, venue_id: &str, neg_risk: bool, ts: TickSize) -> Option<TokenId> {
        Some(pm_execution::live::LiveVenue::ensure_token(
            self, venue_id, neg_risk, ts,
        ))
    }

    async fn available_collateral_micro(&mut self) -> Option<i128> {
        match pm_execution::live::LiveVenue::available_collateral_micro(self).await {
            Ok(m) => Some(m),
            Err(e) => {
                tracing::debug!(error = %e, "copy: collateral fetch failed — using cached/fallback caps");
                None
            }
        }
    }

    fn deposit_wallet(&self) -> Option<String> {
        Some(pm_execution::live::LiveVenue::deposit_wallet_hex(self))
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

    fn ensure_token(&mut self, venue_id: &str, neg_risk: bool, ts: TickSize) -> Option<TokenId> {
        match self {
            // Only the live venue can trade an unsynced market (live book reads).
            AppCopyVenue::Live(v) => CopyVenue::ensure_token(v, venue_id, neg_risk, ts),
            AppCopyVenue::Paper(_) => None,
        }
    }

    async fn available_collateral_micro(&mut self) -> Option<i128> {
        match self {
            AppCopyVenue::Live(v) => CopyVenue::available_collateral_micro(v).await,
            AppCopyVenue::Paper(_) => None,
        }
    }

    fn deposit_wallet(&self) -> Option<String> {
        match self {
            AppCopyVenue::Live(v) => CopyVenue::deposit_wallet(v),
            AppCopyVenue::Paper(_) => None,
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
    /// Venue token-id string of the held outcome — PERSISTED so a restart can
    /// re-register the token on the venue and resume managing this position.
    pub asset: String,
    /// Neg-risk flag — persisted for the same re-registration on reload.
    pub neg_risk: bool,
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
    /// Keyed by the VENUE TOKEN-ID (the Data-API trade's `asset`) so a copy buys
    /// the EXACT token the trader bought — never the complement. Grown ON-DEMAND:
    /// a fresh signal in an UNSYNCED market is resolved live via
    /// [`resolver`](Self::resolver) and cached here so the trade fires NOW.
    tradeable: HashMap<String, TradeTokenInfo>,
    /// CLOB metadata resolver for ON-DEMAND market sync: when a signal lands in a
    /// market the universe snapshot never covered, [`resolve_ondemand`](Self::resolve_ondemand)
    /// fetches its tick + neg_risk here (one `/markets/{cid}` call, cached) and
    /// registers the token on the venue, so entry latency isn't bounded by the
    /// snapshot cadence. `None` ⇒ on-demand OFF (paper / no live venue): an
    /// unsynced signal is skipped, as before.
    resolver: Option<Arc<ClobRest>>,
    /// Durable store sink (order + signed-fill rows, `"copy"`-tagged).
    store_tx: mpsc::Sender<StoreMsg>,
    /// Resolved knobs (caps, drift, stop, reaction window, follow-exit, capital).
    params: CopyParams,
    /// Running realized P&L, µUSDC (status / telemetry).
    realized_micro: i128,
    /// Latched once an inventory halt (`inv.halted()` — the inventory stop-loss /
    /// daily-loss floor crossed on the marked book) fires: NO new entries resume
    /// until it clears, mirroring the MM's `halted`. Exits/redeem still run so a
    /// held position can be flattened. Sticky WITHIN a UTC day, but self-heals on
    /// the day rollover — [`refresh_day_loss_gate`](Self::refresh_day_loss_gate)
    /// rolls the [`InventoryRisk`] day and drops this, so a bad day no longer
    /// strands the strategy halted across midnights until a manual restart. It
    /// re-arms the same cycle if the marked book still breaches a floor.
    halted: bool,
    /// UTC-day index ([`utc_day_from_ms`]) this loop is accounting against. Set at
    /// startup and advanced on a day rollover, which releases
    /// [`day_loss_halted`](Self::day_loss_halted) — a fresh day gets a fresh cap
    /// (mirrors the MM).
    day: i64,
    /// PERSISTENT UTC-day loss-cap latch (mirrors the MM's `day_loss_halted`),
    /// SEPARATE from [`halted`](Self::halted). Armed at startup from the persisted
    /// `"copy"` P&L (so the cap BINDS across the periodic auto-restart instead of
    /// resetting every session), re-checked each cycle, and RELEASED on a UTC-day
    /// rollover. Stops NEW entries exactly like `halted`; exits still run.
    day_loss_halted: bool,
    /// Read-only store handle for the persistent day-loss gate, opened ONCE at
    /// startup from the threaded store path. [`arm_day_loss_gate`](Self::arm_day_loss_gate)
    /// latches from it at startup, and [`refresh_day_loss_gate`](Self::refresh_day_loss_gate)
    /// re-reads it each cycle so the cap ALSO binds MID-session (the cumulative
    /// `"copy"` day-realized ledger crossing the cap). `None` on paper/test paths
    /// with no store → the gate stays inert (a fresh run is never day-loss halted).
    day_loss_read: Option<ReadStore>,
    /// Data-API feed (clone of the outer loop's) used to value the WHOLE account's
    /// open positions (`/value`) — the POSITIONS leg of live account equity. `None`
    /// (paper / no feed) ⇒ equity can't be valued → static caps.
    feed: Option<Arc<DataApiClient>>,
    /// EQUITY-SCALED CAPS (`params.gross_pct > 0`): last-fetched spendable CLOB
    /// collateral (µUSDC) — the CASH leg of live account equity. `None` until the
    /// first successful fetch ⇒ fall back to the static configured caps.
    cash_micro: Option<i128>,
    /// Last-fetched value of ALL the account's open positions (µUSDC) via the
    /// Data-API `/value` — the POSITIONS leg of equity (the full Polymarket
    /// portfolio, NOT just the copies this bot opened). `None` until first fetched.
    positions_value_micro: Option<i128>,
    /// Monotonic instant the equity legs were last refreshed (shared TTL gate), by
    /// [`refresh_equity_if_stale`](Self::refresh_equity_if_stale) — so sizing
    /// doesn't issue balance/value calls every poll.
    equity_at: Option<std::time::Instant>,
    /// ON-CHAIN RECONCILE (see [`reconcile_positions`](Self::reconcile_positions)):
    /// consecutive cycles a tracked position was NOT found in the wallet's live
    /// positions. A row is pruned only after [`RECON_MISS_PRUNE`] consecutive
    /// misses, so a single transient Data-API blip can't orphan a real position.
    recon_miss: HashMap<(String, i64), u32>,
    /// Monotonic instant the on-chain reconcile last ran (its own TTL gate).
    recon_at: Option<std::time::Instant>,
}

impl<V: CopyVenue> CopyLoop<V> {
    /// µUSDC currently DEPLOYED across open positions (Σ cost basis) — what
    /// remaining capital / gross headroom are measured against.
    fn deployed_micro(&self) -> i128 {
        self.open.values().map(|p| p.cost_micro).sum()
    }

    /// Live account EQUITY in µUSDC = spendable CLOB collateral (cash) + the value
    /// of ALL open positions in the account (the full Polymarket portfolio). `None`
    /// until BOTH legs have been fetched (⇒ the caller uses the static caps).
    fn equity_micro(&self) -> Option<i128> {
        match (self.cash_micro, self.positions_value_micro) {
            (Some(cash), Some(positions)) => Some(cash + positions),
            _ => None,
        }
    }

    /// The effective `(per_position, capital, max_gross, max_concurrent)` caps for
    /// THIS cycle. With `gross_pct > 0` and known live equity: `max_gross =
    /// gross_pct × equity`, the capital carve = `max_gross` (never binds before
    /// gross), the per-copy notional stays FIXED at the configured
    /// `per_position_usd`, and CONCURRENCY scales instead — `max_concurrent =
    /// min(max_gross / per_position, configured hard cap)` — so a funded account
    /// opens MORE fixed-size copies (never missing a signal for lack of a slot)
    /// rather than fewer larger ones, up to the hard cap. Otherwise (`gross_pct ==
    /// 0`, equity unknown, or non-positive) the static configured caps are returned.
    fn effective_caps(&self) -> (i128, i128, i128, u32) {
        let static_caps = (
            self.params.per_position_micro,
            self.params.capital.0,
            self.params.max_gross_micro,
            self.params.max_concurrent_positions,
        );
        if self.params.gross_pct <= 0.0 {
            return static_caps;
        }
        let Some(equity) = self.equity_micro() else {
            return static_caps;
        };
        if equity <= 0 {
            return static_caps;
        }
        let max_gross = (equity as f64 * self.params.gross_pct) as i128;
        if max_gross <= 0 {
            return static_caps;
        }
        // Per-copy notional is FIXED (config `per_position_usd`); CONCURRENCY
        // scales with the budget — how many fixed-size copies fit inside max_gross,
        // hard-capped at `max_concurrent_positions` so a large account isn't spread
        // across unbounded positions. `per_position > 0` by config validation.
        let per_position = self.params.per_position_micro;
        let hard_cap = i128::from(self.params.max_concurrent_positions);
        let slots = (max_gross / per_position.max(1)).clamp(0, hard_cap) as u32;
        (per_position, max_gross, max_gross, slots)
    }

    /// Refresh live account EQUITY when equity-scaled caps are on (`gross_pct > 0`)
    /// and the cache is older than the TTL. Equity has TWO legs, fetched together:
    /// the CASH (spendable CLOB collateral, via the venue) and the POSITIONS (the
    /// WHOLE account's open-positions value via the Data-API `/value` — the full
    /// Polymarket portfolio, not just the copies we opened). The cache only
    /// advances when BOTH legs succeed, so a half-fetch never sizes off a wrong
    /// equity; a transient failure keeps the prior value (or leaves it unset →
    /// static fallback), never widening caps on a blip.
    async fn refresh_equity_if_stale(&mut self) {
        if self.params.gross_pct <= 0.0 {
            return;
        }
        const TTL: Duration = Duration::from_secs(60);
        if self.equity_at.map(|t| t.elapsed() < TTL).unwrap_or(false) {
            return;
        }
        // Wallet first (immutable borrow) so the mutable collateral fetch is clear.
        let wallet = self.venue.as_ref().and_then(|v| v.deposit_wallet());
        let cash = match self.venue.as_mut() {
            Some(v) => v.available_collateral_micro().await,
            None => None,
        };
        // Positions leg: the account's full open-positions value. Clone the feed
        // Arc so no `self` borrow is held across the await.
        let feed = self.feed.clone();
        let positions = match (feed, wallet) {
            (Some(feed), Some(w)) => feed.portfolio_value_micro(&w).await.ok(),
            _ => None,
        };
        if let (Some(cash), Some(positions)) = (cash, positions) {
            self.cash_micro = Some(cash);
            self.positions_value_micro = Some(positions);
            self.equity_at = Some(std::time::Instant::now());
            let (per_position, _capital, max_gross, max_concurrent) = self.effective_caps();
            tracing::info!(
                cash_micro = cash as i64,
                positions_micro = positions as i64,
                equity_micro = (cash + positions) as i64,
                gross_pct = self.params.gross_pct,
                max_gross_micro = max_gross as i64,
                per_position_micro = per_position as i64,
                max_concurrent,
                "copy: equity refreshed — caps scaled to live account (cash + all positions)"
            );
        } else {
            tracing::debug!(
                cash_known = cash.is_some(),
                positions_known = positions.is_some(),
                "copy: equity refresh incomplete — keeping prior caps (static fallback if unset)"
            );
        }
    }

    /// ON-CHAIN RECONCILE (TTL-gated): make the bot's `open` book match what the
    /// wallet ACTUALLY holds, so a resolved/redeemed/externally-closed position
    /// doesn't linger as a phantom row — which would inflate `deployed`/concurrency
    /// (starving new entries) and mislead the `pnl` dashboard. Fetches the wallet's
    /// live positions (Data API) and, per tracked row: KEEP a live holding, SETTLE
    /// a resolved one (loser ⇒ book −cost, winner ⇒ book +redeem), or — after
    /// [`RECON_MISS_PRUNE`] consecutive misses, to survive a transient blip — PRUNE
    /// one no longer held. Acts only on a SUCCESSFUL fetch; a fetch error is a
    /// no-op (never prunes on missing data). Needs the feed + wallet (live only).
    async fn reconcile_positions(&mut self) {
        if self.open.is_empty() {
            return;
        }
        const TTL: Duration = Duration::from_secs(60);
        if self.recon_at.map(|t| t.elapsed() < TTL).unwrap_or(false) {
            return;
        }
        let Some(wallet) = self.venue.as_ref().and_then(|v| v.deposit_wallet()) else {
            return;
        };
        let Some(feed) = self.feed.clone() else {
            return;
        };
        let positions = match feed.positions(&wallet, 0.0).await {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(error = %e, "copy: reconcile skipped — wallet positions fetch failed");
                return;
            }
        };
        self.recon_at = Some(std::time::Instant::now());
        // Live wallet state keyed by (lower(condition_id), outcome_index).
        let held: HashMap<(String, i64), (f64, f64, bool)> = positions
            .iter()
            .map(|p| {
                (
                    (p.condition_id.to_lowercase(), p.outcome_index),
                    (p.size, p.cur_price, p.redeemable),
                )
            })
            .collect();
        let keys: Vec<(String, i64)> = self.open.keys().cloned().collect();
        for key in keys {
            let Some(pos) = self.open.get(&key).cloned() else {
                continue;
            };
            match reconcile_action(held.get(&(key.0.to_lowercase(), key.1)).copied()) {
                ReconAction::Keep => {
                    self.recon_miss.remove(&key);
                }
                ReconAction::Settle { won } => {
                    self.recon_miss.remove(&key);
                    self.settle_reconciled(&key, &pos, won).await;
                }
                ReconAction::Gone => {
                    let n = self.recon_miss.entry(key.clone()).or_insert(0);
                    *n += 1;
                    if *n >= RECON_MISS_PRUNE {
                        self.prune_gone(&key, &pos);
                        self.recon_miss.remove(&key);
                    }
                }
            }
        }

        // ORPHAN-WINNER SWEEP: redeem RESOLVED-WON positions the wallet holds that
        // are NOT in our tracked book — winners a past (pre-neg-risk-fix) redeem
        // left stranded on-chain, or positions dropped from tracking across
        // restarts. Without this their winnings are never collected. LOSERS and
        // dust are skipped (nothing to redeem — no pointless relayer calls);
        // tracked positions are handled by `settle_reconciled` above. Deduped by
        // condition and capped per pass so a backlog drains gradually.
        if let Some(relayer) = self.relayer.clone() {
            let tracked: HashSet<(String, i64)> = self
                .open
                .keys()
                .map(|(c, o)| (c.to_lowercase(), *o))
                .collect();
            let mut swept = 0usize;
            let mut done: HashSet<B256> = HashSet::new();
            for p in &positions {
                if swept >= RECON_SWEEP_MAX_PER_CYCLE {
                    break;
                }
                // Resolved WINNER with real balance — not a loser/dust, not tracked.
                let is_winner = p.redeemable && p.cur_price >= 0.5 && p.size > RECON_DUST_SHARES;
                if !is_winner || tracked.contains(&(p.condition_id.to_lowercase(), p.outcome_index)) {
                    continue;
                }
                let Ok(condition) = p.condition_id.parse::<B256>() else {
                    continue;
                };
                if !done.insert(condition) {
                    continue; // one redeem per condition clears its winning slot
                }
                match relayer.redeem(condition, p.neg_risk).await {
                    Ok(_) => {
                        swept += 1;
                        tracing::info!(
                            condition_id = %p.condition_id,
                            neg_risk = p.neg_risk,
                            "copy: swept orphan winner (redeemed an untracked resolved position)"
                        );
                    }
                    Err(e) => tracing::warn!(
                        error = %e,
                        condition_id = %p.condition_id,
                        neg_risk = p.neg_risk,
                        "copy: orphan-winner sweep redeem failed"
                    ),
                }
            }
        }
    }

    /// Settle a tracked position the wallet shows as RESOLVED: book the outcome
    /// (winner ⇒ +qty redeemed, loser ⇒ 0) into inventory + the day-realized
    /// ledger, drop it from `open`, persist the close, and redeem on-chain when a
    /// relayer is present + it won. Same booking shape as [`settle_resolution`].
    async fn settle_reconciled(&mut self, key: &(String, i64), pos: &CopyPosition, won: bool) {
        let value = if won { pos.qty_micro } else { 0 };
        let realized_before = self.inv.realized(pos.token).0;
        self.inv.on_fill(pos.token, -pos.qty_micro, Usdc(value));
        let delta = self.inv.realized(pos.token).0 - realized_before;
        self.realized_micro += delta;
        self.open.remove(key);
        self.persist_close(key);
        if delta != 0 {
            let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                utc_day: utc_day_from_ms(now_ms()),
                strategy: "copy".into(),
                delta_micro: delta,
            });
        }
        tracing::info!(
            condition_id = %key.0,
            outcome_index = key.1,
            won,
            value_micro = value as i64,
            realized_delta = delta as i64,
            "copy: reconciled a resolved position from live on-chain state"
        );
        if let Some(relayer) = self.relayer.clone().filter(|_| won) {
        match relayer.redeem(pos.condition, pos.neg_risk).await {
            Ok(_) => tracing::info!(condition_id = %key.0, "copy: redeemed reconciled winner"),
            Err(e) => tracing::warn!(error = %e, condition_id = %key.0, "copy: reconciled-winner redeem failed (booked locally)"),
        }
        }
    }

    /// Prune a tracked position the wallet NO LONGER holds (redeemed/sold/settled
    /// off-bot). Flatten inventory at cost (NEUTRAL realized — the true P&L already
    /// happened on-chain and is reflected in live equity; we don't fabricate it),
    /// drop from `open`, and persist the close, so `deployed`/concurrency + `pnl`
    /// track reality. Distinct from a settle: here there is no wallet mark to book.
    fn prune_gone(&mut self, key: &(String, i64), pos: &CopyPosition) {
        self.inv.on_fill(pos.token, -pos.qty_micro, Usdc(pos.cost_micro));
        self.open.remove(key);
        self.persist_close(key);
        tracing::warn!(
            condition_id = %key.0,
            outcome_index = key.1,
            cost_micro = pos.cost_micro as i64,
            "copy: reconciled AWAY a position no longer held on-chain (pruned stale row; \
             realized P&L already settled on-chain)"
        );
    }

    /// View for the per-strategy dashboard. Surfaces the latched HALT reason, the
    /// open-position count, running realized P&L, and (C5) the copy-specific
    /// telemetry — the follow-whitelist size — in a [`CopyStatus`] (mirroring how
    /// the MM surfaces its halt + `RewardFarmStatus`). The per-position lines
    /// (market / qty / entry / mark / uPnL) reach the TUI's Positions panel via
    /// the durable store (every fill is `"copy"`-tagged), exactly as the MM's do.
    fn status(&self, paused: bool, whitelist: usize) -> StrategyStatus {
        // Surface BOTH halt sources to the dashboard (mirrors the MM): the
        // in-session inventory halt reason (StopLoss/DailyLoss) takes precedence
        // as the more specific cause; otherwise, when only the PERSISTENT UTC-day
        // loss cap is latched, show `DayLossCap` so operators see WHY the copy
        // executor refuses new entries instead of it appearing un-halted.
        // `self.halted` mirrors `self.inv.halted().is_some()` (set together in
        // `mark_and_check`), so reading `inv.halted()` here is equivalent.
        let halted = self
            .inv
            .halted()
            .map(|h| format!("{h:?}"))
            .or_else(|| self.day_loss_halted.then(|| "DayLossCap".to_string()));
        StrategyStatus {
            paused,
            halted,
            open_positions: self.open.len(),
            realized_micro: i64::try_from(self.realized_micro).unwrap_or(0),
            copy: Some(CopyStatus { whitelist }),
            ..Default::default()
        }
    }

    /// ON-DEMAND market sync for a fresh signal in an UNSYNCED market: resolve the
    /// market's tick + neg_risk LIVE (one `/markets/{cid}` call via the
    /// [`resolver`](Self::resolver)), register the bought token on the venue
    /// ([`CopyVenue::ensure_token`]), cache the resulting [`TradeTokenInfo`], and
    /// return it — so the entry fires immediately rather than waiting for the next
    /// universe snapshot. Best-effort: `None` (caller skips) when on-demand is off
    /// (no resolver / paper venue), the fetch fails, the market metadata is
    /// unusable ([`ondemand_token_params`]), or the venue can't register it.
    async fn resolve_ondemand(&mut self, c: &CopyCandidate) -> Option<TradeTokenInfo> {
        if c.asset.is_empty() {
            return None;
        }
        // Fetch market metadata in a scope that ENDS the immutable resolver borrow
        // before we take `&mut self.venue` below.
        let market = {
            let clob = self.resolver.as_ref()?;
            match clob.market(&c.condition_id).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::debug!(
                        condition_id = %c.condition_id,
                        error = %e,
                        "copy: on-demand market resolve fetch failed — skipping"
                    );
                    return None;
                }
            }
        };
        let (ts, neg_risk, condition) =
            ondemand_token_params(&market, &c.asset, &c.condition_id)?;
        let token = self.venue.as_mut()?.ensure_token(&c.asset, neg_risk, ts)?;
        let info = TradeTokenInfo {
            token,
            ts,
            condition,
            neg_risk,
        };
        self.tradeable.insert(c.asset.clone(), info.clone());
        tracing::info!(
            condition_id = %c.condition_id,
            outcome_index = c.outcome_index,
            "copy: on-demand synced an unsynced smart-money market — trading it now"
        );
        Some(info)
    }

    /// ENTRY: for each fresh candidate, when a venue is present and NOT at a cap,
    /// place a FRESHNESS-GATED, capital/gross/floor-sized taker FAK BUY and book
    /// it. A candidate that is untradeable, has no ask, or whose price ran past
    /// the drift gate is consumed (`seen`); a cap-limited candidate is left
    /// un-consumed so a freed slot / capital can still take it while it is fresh.
    async fn run_entries(&mut self, candidates: &[CopyCandidate], seen: &mut SeenKeys) {
        // Cumulative-loss circuit breaker (mirrors the MM's quote gate): a latched
        // inventory halt (`inv.halted()` → `self.halted`) or the persistent
        // UTC-day loss cap stops ALL new entries — only exits run on a bad day so
        // an open position can still be flattened. Defensive twin of the gate in
        // `run_poll_cycle`, so a direct caller is gated too.
        if self.halted || self.day_loss_halted {
            return;
        }
        if self.venue.is_none() {
            return; // paper-without-venue → inert
        }
        // EQUITY-SCALED CAPS: equity is refreshed once per cycle by the caller
        // (`run_poll_cycle`, TTL-gated) so it's fresh here even while halted/quiet.
        // Snapshot the effective caps ONCE so every candidate this cycle sizes
        // against a single equity view.
        let (eff_per_position, eff_capital, eff_max_gross, eff_max_concurrent) =
            self.effective_caps();
        // Per-cycle FUNNEL counters, logged once at the end so an operator can SEE
        // exactly where this cycle's candidates died instead of inferring it from
        // scattered per-candidate lines.
        let mut entered = 0u32;
        let mut skip_drift = 0u32;
        let mut skip_no_ask = 0u32;
        let mut skip_untradeable = 0u32;
        let mut skip_at_cap = 0u32;
        let mut skip_no_size = 0u32;
        let mut already_holding = 0u32;
        for c in candidates {
            let key = (c.condition_id.clone(), c.outcome_index);
            // Already holding this market+side (open keys are `seen` so
            // select_signals excludes them — guard defensively anyway).
            if self.open.contains_key(&key) {
                already_holding += 1;
                continue;
            }
            // Resolve the tradeable token BY THE TRADE'S `asset` (the exact token
            // the trader bought) so we mirror their SIDE, never the complement.
            // SYNCED markets hit the prebuilt map; an UNSYNCED market (a fresh
            // signal the snapshot never covered) is synced ON-DEMAND right here so
            // we trade it now instead of waiting for the next snapshot (entry
            // latency is a copy-edge contributor). Still untradeable (paper /
            // resolve failed / no asset) ⇒ consumed + skipped.
            let info = match self.tradeable.get(&c.asset).cloned() {
                Some(info) => info,
                None => match self.resolve_ondemand(c).await {
                    Some(info) => info,
                    None => {
                        tracing::debug!(
                            condition_id = %c.condition_id,
                            outcome_index = c.outcome_index,
                            "copy: skip entry — market unsynced and on-demand resolve unavailable"
                        );
                        skip_untradeable += 1;
                        seen.mark(key);
                        continue;
                    }
                },
            };
            // Best ask = our marketable entry reference.
            let ask = match self.venue.as_mut() {
                Some(v) => v.best_ask(info.token, info.ts).await,
                None => return,
            };
            let Some(ask) = ask else {
                tracing::debug!(condition_id = %c.condition_id, "copy: skip entry — no ask in book");
                skip_no_ask += 1;
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
                    max_drift = self.params.max_drift,
                    "copy: skip entry — price ran past the drift gate"
                );
                skip_drift += 1;
                seen.mark(key);
                continue;
            }
            // Concurrency cap (NOT consumed — a freed slot can still take it).
            // DEBUG, not INFO: when full this fires for EVERY remaining candidate
            // each poll (dozens of lines); the per-cycle funnel's `skip_at_cap`
            // already reports the count.
            if !concurrency_allows(self.open.len(), eff_max_concurrent) {
                tracing::debug!(
                    open = self.open.len(),
                    max_concurrent = eff_max_concurrent,
                    "copy: skip entry — at concurrency cap (retry while fresh)"
                );
                skip_at_cap += 1;
                continue;
            }
            // Capital + gross caps → size. With `gross_pct > 0` these SCALE off
            // live account equity (cash + positions), recomputed per cycle; else
            // they are the static configured caps. `eff_*` is resolved once before
            // the loop so every candidate this cycle sizes against one snapshot.
            let deployed = self.deployed_micro();
            let capital_left = eff_capital - deployed;
            let gross_left = eff_max_gross - deployed;
            let size = copy_position_size_micro(eff_per_position, capital_left, gross_left, entry_px);
            if size <= 0 {
                // DEBUG, not INFO: like the concurrency cap, this fires for every
                // remaining candidate once the gross/capital budget is exhausted;
                // the funnel's `skip_no_size` reports the count.
                tracing::debug!(
                    condition_id = %c.condition_id,
                    capital_left,
                    gross_left,
                    "copy: skip entry — caps/floor leave no size (retry while fresh)"
                );
                skip_no_size += 1;
                continue; // not consumed: capital/gross may free up
            }
            // `entered` counts CONFIRMED opens (a non-zero fill booked), not mere
            // attempts — a FAK that fills nothing / is rejected returns false.
            if self.enter(key.clone(), c, &info, ask, size).await {
                entered += 1;
            }
            seen.mark(key);
        }
        // FUNNEL summary (once per poll cycle that had candidates). entered=0 with
        // skip_drift>0 ⇒ signals are real but the price already ran past the gate;
        // NO funnel line at all ⇒ no fresh whitelisted buys this cycle (a signal
        // problem, not a rejection). Lets an operator diagnose "why no trades".
        if !candidates.is_empty() {
            tracing::info!(
                candidates = candidates.len(),
                entered,
                skip_drift,
                skip_no_ask,
                skip_untradeable,
                skip_at_cap,
                skip_no_size,
                already_holding,
                "copy: entry funnel"
            );
        }
    }

    /// Place + book the taker FAK BUY for one sized candidate, recording the open
    /// position. Paper / live both go through [`CopyVenue::submit_fak`]; the fills
    /// are booked into inventory + the store via [`book_fills`](Self::book_fills).
    ///
    /// Returns `true` iff a position was actually OPENED (a non-zero fill booked)
    /// so the caller's funnel counts confirmed entries, not mere attempts.
    async fn enter(
        &mut self,
        key: (String, i64),
        c: &CopyCandidate,
        info: &TradeTokenInfo,
        ask: Px,
        size_micro: i128,
    ) -> bool {
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
                    return false;
                }
            },
            None => return false,
        };
        if outcome.filled.0 == 0 {
            tracing::info!(condition_id = %c.condition_id, "copy: entry FAK filled nothing");
            return false;
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
            return false;
        }
        let pos = CopyPosition {
            trader: c.trader.clone(),
            entry_ts: c.timestamp,
            qty_micro: filled_micro,
            cost_micro,
            token: info.token,
            ts: info.ts,
            condition: info.condition,
            asset: c.asset.clone(),
            neg_risk: info.neg_risk,
        };
        // PERSIST for restart-safety: a reload rebuilds `open` + inventory from
        // this row so the position keeps its follow-exit / stop-loss / redeem
        // management across a restart instead of being orphaned + double-deployed.
        let _ = self
            .store_tx
            .try_send(StoreMsg::CopyPositionUpsert(position_row(
                &c.condition_id,
                c.outcome_index,
                &pos,
            )));
        self.open.insert(key, pos);
        tracing::info!(
            condition_id = %c.condition_id,
            outcome_index = c.outcome_index,
            trader = %c.trader,
            qty_micro = filled_micro as i64,
            cost_micro = cost_micro as i64,
            "copy: ENTERED (taker FAK buy, freshness-gated)"
        );
        true
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
    /// Best-effort delete of the persisted open position on a FULL close, so a
    /// restart does not resurrect a position we no longer hold.
    fn persist_close(&self, key: &(String, i64)) {
        let _ = self.store_tx.try_send(StoreMsg::CopyPositionClose {
            condition_id: key.0.clone(),
            outcome_index: key.1,
        });
    }

    /// RELOAD persisted open positions at startup so a restart RESUMES managing
    /// them (follow-exit / stop-loss / redeem) instead of orphaning them AND
    /// re-deploying on top (the gross cap now reflects them from t0). For each
    /// durable row it re-registers the token on the venue (so the exit path can
    /// trade it even if the market left the synced universe), rebuilds the `open`
    /// entry, and seeds inventory (marks / stop-loss / gross). Rows with an
    /// unsupported tick, an unparseable condition, or no venue (paper) are skipped;
    /// a read failure just starts flat (prior behavior).
    fn reload_positions(&mut self, read: &ReadStore) {
        let rows = match read.copy_open_positions() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "copy: position reload read failed — starting flat");
                return;
            }
        };
        let mut restored = 0usize;
        for row in rows {
            let Some(ts) = tick_from_decimals(row.tick_decimals) else {
                continue;
            };
            let Ok(condition) = row.condition_hex.parse::<B256>() else {
                continue;
            };
            // Re-register on the venue (idempotent); None ⇒ no venue (paper) →
            // nothing to manage, stop reloading.
            let token = match self.venue.as_mut() {
                Some(v) => match v.ensure_token(&row.asset, row.neg_risk, ts) {
                    Some(t) => t,
                    None => continue,
                },
                None => return,
            };
            let qty_micro = i128::from(row.qty_micro);
            let cost_micro = i128::from(row.cost_micro);
            // Seed inventory (buy: +shares, −cash) so marks / stop-loss / realized
            // accounting resume from the real position, not flat.
            self.inv.on_fill(token, qty_micro, Usdc(-cost_micro));
            self.open.insert(
                (row.condition_id.clone(), row.outcome_index),
                CopyPosition {
                    trader: row.trader,
                    entry_ts: row.entry_ts,
                    qty_micro,
                    cost_micro,
                    token,
                    ts,
                    condition,
                    asset: row.asset,
                    neg_risk: row.neg_risk,
                },
            );
            restored += 1;
        }
        if restored > 0 {
            tracing::info!(
                restored,
                "copy: reloaded open positions from the store — resuming management (restart-safe, no double-deploy)"
            );
        }
    }

    async fn taker_exit(&mut self, key: &(String, i64), pos: &CopyPosition, limit: Px) {
        let qty = Qty(pos.qty_micro.max(0) as u64);
        if qty.0 == 0 {
            self.open.remove(key);
            self.persist_close(key);
            return;
        }
        // A holding at/below the venue's minimum sellable size can't be market-sold
        // (the maker amount rounds sub-min → the CLOB rejects "invalid maker
        // amount"). Don't retry it forever — prune it (a negligible on-chain remnant
        // the reconcile also covers). This is the dust case behind the exit spam.
        if pos.qty_micro < MIN_COPY_SHARES_MICRO {
            tracing::warn!(
                condition_id = %key.0,
                qty_micro = pos.qty_micro as i64,
                "copy: exit skipped — below venue minimum; pruning unsellable dust row"
            );
            self.prune_gone(key, pos);
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
                    let msg = e.to_string();
                    // A balance/amount rejection is DEFINITIVE proof we don't hold
                    // this position as tracked (phantom DB row / already
                    // redeemed/sold / stale qty). Prune it NOW instead of retrying
                    // the same doomed sell every ~10s (the observed stop-loss spam
                    // on a not-held token) — the venue is the source of truth here,
                    // faster + surer than waiting on the reconcile's debounce.
                    if exit_rejection_is_terminal(&msg) {
                        tracing::warn!(
                            error = %msg,
                            condition_id = %key.0,
                            "copy: exit rejected for balance/amount — not held as tracked; pruning stale row"
                        );
                        self.prune_gone(key, pos);
                    } else {
                        // Transient (network/rate/etc.) — keep it and retry next cycle.
                        tracing::warn!(error = %msg, condition_id = %key.0, "copy: exit FAK rejected (transient) — retry next cycle");
                    }
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
            self.persist_close(key);
        } else if let Some(p) = self.open.get_mut(key) {
            // Pro-rata release of cost basis so the caps free correctly on a
            // partial exit (the FAK killed an unfilled remainder).
            p.cost_micro = pos.cost_micro * remaining / pos.qty_micro;
            p.qty_micro = remaining;
            // Keep the durable row in sync with the reduced position.
            let row = position_row(&key.0, key.1, p);
            let _ = self.store_tx.try_send(StoreMsg::CopyPositionUpsert(row));
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
        self.persist_close(key);
        if delta != 0 {
            let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                utc_day: utc_day_from_ms(now_ms()),
                strategy: "copy".into(),
                delta_micro: delta,
            });
        }
        match relayer.redeem(pos.condition, pos.neg_risk).await {
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

    /// Mark every open (long-only) copy at the live best BID — the conservative
    /// liquidation mark, the SAME reference the per-position stop uses — and feed
    /// it to [`InventoryRisk::mark`], then latch the session
    /// [`halted`](Self::halted) flag when the inventory stop-loss / daily-loss
    /// floor crosses (the SAME machinery the MM's `mark_and_check` uses). This is
    /// what makes `inv.halted()` MEAN something for copy: without a mark the latch
    /// can never fire. A held position with no live bid is left UNMARKED, which
    /// (per the `mark` contract) WITHHOLDS the latch that cycle — a transient data
    /// gap never permanently halts the strategy. Sticky WITHIN a UTC day; the day
    /// rollover self-heals it (see [`refresh_day_loss_gate`](Self::refresh_day_loss_gate)).
    async fn mark_and_check(&mut self) {
        if self.halted {
            return; // already latched (sticky) — nothing more to check.
        }
        let mut marks: Marks = HashMap::new();
        let keys: Vec<(String, i64)> = self.open.keys().cloned().collect();
        for key in keys {
            let Some(pos) = self.open.get(&key).cloned() else {
                continue;
            };
            let bid = match self.venue.as_mut() {
                Some(v) => v.best_bid(pos.token, pos.ts).await,
                None => return, // no venue → inert (nothing to mark)
            };
            // Omitting an unmarkable held token makes `mark` withhold the latch
            // that cycle (its Marks contract) — a transient gap won't halt.
            if let Some(bid) = bid {
                marks.insert(pos.token, bid.microusdc(pos.ts));
            }
        }
        let _ = self.inv.mark(&marks);
        if let Some(reason) = self.inv.halted() {
            self.halted = true;
            tracing::warn!(
                ?reason,
                "copy: inventory halt latched (cumulative-loss circuit breaker) — \
                 no new entries this session; exits still run to flatten"
            );
        }
    }

    /// Arm the PERSISTENT UTC-day loss-cap latch at startup (mirrors the MM's
    /// `arm_day_loss_gate`). Records the current UTC day and reads BOTH gate arms
    /// for the `"copy"` strategy for that day via the SHARED [`read_day_loss`]
    /// helper (NOT reimplemented); if EITHER the cumulative day-realized LEDGER or
    /// the worst-point snapshot is already at/under the daily-loss cap
    /// (`InventoryConfig::daily_loss_usd`, the SAME floor the in-session
    /// `InvHalt::DailyLoss` keys off), latch [`day_loss_halted`](Self::day_loss_halted)
    /// so the cap BINDS across the periodic auto-restart instead of resetting
    /// every session. No handle / a read error / no data today → both arms read
    /// `0` → a fresh run is NEVER halted by default. Called once before the loop
    /// ticks; [`refresh_day_loss_gate`](Self::refresh_day_loss_gate) re-checks each
    /// cycle.
    fn arm_day_loss_gate(&mut self, read: Option<&ReadStore>, now_ms: i64) {
        let today = utc_day_from_ms(now_ms);
        self.day = today;
        let cap_micro = self.inv.config().daily_loss_usd.0;
        let Some(read) = read else { return };
        let (realized, snapshot) = read_day_loss(read, "copy", today);
        if realized <= -cap_micro || snapshot <= -cap_micro {
            self.day_loss_halted = true;
            tracing::warn!(
                utc_day = today,
                day_realized_micro = realized as i64,
                day_pnl_micro = snapshot as i64,
                daily_loss_cap_micro = cap_micro as i64,
                "copy: daily loss cap ALREADY hit for the UTC day (persisted across \
                 restart) — refusing NEW entries until the day rolls over"
            );
        }
    }

    /// Per-cycle PERSISTENT day-loss maintenance (mirrors the top of the MM's
    /// `tick`): (1) on a UTC-day ROLLOVER advance the tracked day and RELEASE both
    /// loss latches — a fresh day gets a fresh cap. This releases the cross-restart
    /// `day_loss_halted` cap AND self-heals the in-session inventory
    /// [`halted`](Self::halted) latch by rolling the [`InventoryRisk`] day
    /// ([`InventoryRisk::roll_day`]: clear the latch + rebase realized, keeping open
    /// net/basis). Without this the inventory halt was sticky for the whole session,
    /// so one bad day stranded the strategy halted across every following midnight
    /// until a MANUAL restart (the observed "still halted days later"). It re-arms
    /// safely: [`mark_and_check`](Self::mark_and_check) re-latches THIS cycle if the
    /// current marked book still breaches a floor. (2) Otherwise, while un-halted,
    /// RE-READ both gate arms for `"copy"` so the cap also binds MID-session (the
    /// cumulative day-realized ledger crossing the cap), not only across
    /// auto-restarts. Inert when there is no store handle. Takes an explicit
    /// `now_ms` (like [`arm_day_loss_gate`](Self::arm_day_loss_gate)) so the loop
    /// passes [`now_ms`] and tests drive the rollover deterministically.
    fn refresh_day_loss_gate(&mut self, now_ms: i64) {
        let today = utc_day_from_ms(now_ms);
        if today > self.day {
            self.day = today;
            if self.day_loss_halted {
                tracing::warn!(
                    utc_day = today,
                    "copy: UTC day rolled over — releasing persistent day-loss cap latch"
                );
                self.day_loss_halted = false;
            }
            // Self-heal the IN-SESSION inventory halt too: rebase the InventoryRisk
            // day (clear the sticky latch + zero realized, keeping open net/basis)
            // and drop `self.halted`, so a prior day's banked losses no longer keep
            // the strategy halted across midnights until a manual restart. Genuinely
            // bad OPEN risk is not cleared: `mark_and_check` later this cycle re-arms
            // the latch if the current marked book still breaches a floor.
            if self.halted {
                tracing::warn!(
                    utc_day = today,
                    "copy: UTC day rolled over — self-healing in-session inventory halt \
                     (re-arms this cycle if the marked book still breaches a floor)"
                );
            }
            self.inv.roll_day();
            self.halted = false;
        }
        if !self.day_loss_halted {
            let cap_micro = self.inv.config().daily_loss_usd.0;
            // The breach is computed under an immutable borrow that ENDS before
            // the latch is set (mirrors the MM), satisfying the borrow checker.
            let breach = self.day_loss_read.as_ref().map(|read| {
                let (realized, snapshot) = read_day_loss(read, "copy", self.day);
                (
                    realized,
                    snapshot,
                    realized <= -cap_micro || snapshot <= -cap_micro,
                )
            });
            if let Some((realized, snapshot, true)) = breach {
                self.day_loss_halted = true;
                tracing::warn!(
                    utc_day = self.day,
                    day_realized_micro = realized as i64,
                    day_pnl_micro = snapshot as i64,
                    daily_loss_cap_micro = cap_micro as i64,
                    "copy: daily loss cap crossed MID-session (cumulative ledger / \
                     worst-point snapshot) — halting NEW entries until the UTC day rolls over"
                );
            }
        }
    }
}

/// One poll cycle: refresh the PERSISTENT day-loss gate (rollover release /
/// mid-session re-latch), fetch the recent tape (whitelist + open-position
/// traders), build the resolution map for open markets (only when a relayer can
/// redeem), SWEEP exits, MARK the held book + latch the inventory halt, then —
/// only when the cumulative-loss circuit breaker is clear — run fresh ENTRIES.
/// The exit-before-entry order frees caps for new entries the same cycle.
/// Best-effort I/O: a per-wallet error just omits that wallet this cycle.
async fn run_poll_cycle<V: CopyVenue>(
    state: &mut CopyLoop<V>,
    feed: &Option<Arc<DataApiClient>>,
    whitelist: &[String],
    creamy: &HashMap<String, HashSet<String>>,
    seen: &mut SeenKeys,
) {
    let Some(client) = feed else {
        return;
    };
    let now_ms_val = now_ms();
    let now = now_ms_val / 1000;
    // PERSISTENT day-loss maintenance FIRST (mirrors the top of the MM's `tick`):
    // RELEASE the latch on a UTC-day rollover, else RE-LATCH if the cumulative
    // `"copy"` ledger (or worst-point snapshot) crossed the cap mid-session.
    state.refresh_day_loss_gate(now_ms_val);
    // ON-CHAIN RECONCILE (TTL-gated inside): prune phantom rows + settle resolved
    // ones BEFORE the exit sweep / marks / entries, so deployed + concurrency and
    // the marked book all reflect what the wallet actually holds this cycle.
    state.reconcile_positions().await;
    // LIVE EQUITY (TTL-gated inside): refresh EVERY cycle — NOT only when entering —
    // so the equity-scaled caps AND the `pnl` account summary stay current even
    // while HALTED or quiet (previously it only ran in the entry path, so it froze
    // for the whole halt, making the dashboard portfolio go stale).
    state.refresh_equity_if_stale().await;
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
    // Mark the held book + latch the inventory halt (cumulative-loss circuit
    // breaker) AFTER the exit sweep — exactly like the MM marks after consuming
    // fills — so `inv.halted()` actually binds on a bad day.
    state.mark_and_check().await;
    // Gate NEW entries on the circuit breaker (exits above ALWAYS run so a bad day
    // can still be flattened): mirrors the MM's `!halted && !day_loss_halted`
    // quote gate (the loop already gated `!paused && venue.is_some()`).
    if !state.halted && !state.day_loss_halted {
        let candidates = select_signals(
            &recent_by_wallet,
            creamy,
            seen.set(),
            now,
            state.params.reaction_window_secs,
        );
        state.run_entries(&candidates, seen).await;
    }
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
    /// How to trade a copied market — keyed by the venue token-id (the trade's
    /// `asset`) — built by main (C5) from the registry; empty by default (nothing
    /// tradeable ⇒ inert), grown on-demand for unsynced signals.
    tradeable: HashMap<String, TradeTokenInfo>,
    /// Start the loop PAUSED when live is held (the operator releases via the
    /// host's `SetPaused(false)`), mirroring the MM. Gates the live path.
    start_paused: bool,
    /// Inventory-ledger caps the cumulative-loss circuit breaker keys off — the
    /// REAL `[inventory]` floors (`inventory_stop_loss_usd` / `daily_loss_usd` /
    /// gross), threaded by main via
    /// [`with_inventory_config`](Self::with_inventory_config) EXACTLY like the MM.
    /// Defaults to [`fallback_inv_config`] (inert/test) so a strategy built
    /// without it still has sane ledger caps.
    inv_cfg: InventoryConfig,
    /// Durable-store path, threaded so the loop arms the PERSISTENT UTC-day loss
    /// cap at startup from the persisted `"copy"` P&L (and re-checks it each
    /// cycle) — what makes the cap BIND across the periodic auto-restart. `None`
    /// (the default) → the day-loss gate is inert (a fresh run reads no prior
    /// P&L). Set via [`with_store_path`](Self::with_store_path), mirroring the MM.
    store_path: Option<std::path::PathBuf>,
    /// Pre-computed follow whitelist, SEEDED by main so the whitelist-driven
    /// universe sync and the strategy share ONE snapshot (and the heavy
    /// EdgePerBet rank isn't run twice at startup). `Some(non-empty)` ⇒ the loop
    /// starts with this whitelist and DEFERS its first refresh by
    /// `whitelist_refresh`; `None`/empty (the default) ⇒ the loop builds it
    /// immediately (first tick), as before. Set via
    /// [`with_initial_whitelist`](Self::with_initial_whitelist).
    initial_whitelist: Option<Whitelist>,
    /// CLOB metadata resolver enabling ON-DEMAND market sync (live runs): when a
    /// signal lands in an UNSYNCED market, the loop resolves its tick + neg_risk
    /// via this client and registers the token on the venue so the entry isn't
    /// delayed to the next universe snapshot. `None` (the default) ⇒ on-demand
    /// OFF. Set via [`with_resolver`](Self::with_resolver); main wires it for live.
    resolver: Option<Arc<ClobRest>>,
}

impl<V: CopyVenue> CopyStrategy<V> {
    /// Construct the copy strategy from its resolved params. The feed, venue,
    /// relayer, and tradeable map are all absent by default (a fully inert
    /// heartbeat — no whitelist, no orders); attach them with the builders.
    pub fn new(params: CopyParams) -> Self {
        let inv_cfg = fallback_inv_config(&params);
        CopyStrategy {
            id: StrategyId("copy"),
            params,
            feed: None,
            venue: None,
            relayer: None,
            tradeable: HashMap::new(),
            start_paused: false,
            inv_cfg,
            store_path: None,
            initial_whitelist: None,
            resolver: None,
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

    /// Provide the `asset (venue token-id) →` [`TradeTokenInfo`] resolver (main
    /// builds it from the registry for the markets the venue can trade).
    pub fn with_tradeable(mut self, tradeable: HashMap<String, TradeTokenInfo>) -> Self {
        self.tradeable = tradeable;
        self
    }

    /// Start the loop PAUSED (live held). The host sends `SetPaused(false)` on
    /// release. Mirrors [`MmStrategy::with_start_paused`](super::mm::MmStrategy::with_start_paused).
    pub fn with_start_paused(mut self, start_paused: bool) -> Self {
        self.start_paused = start_paused;
        self
    }

    /// Thread the REAL `[inventory]` caps (`config.inventory`) the cumulative-loss
    /// circuit breaker keys off — the inventory stop-loss / daily-loss / gross
    /// floors — EXACTLY like the MM's `inv_cfg` constructor arg. main builds it via
    /// `wiring::inventory_config`. Without it the loop falls back to
    /// [`fallback_inv_config`] (the inert/test default), so `inv.halted()` would
    /// not reflect the operator-configured floors.
    pub fn with_inventory_config(mut self, inv_cfg: InventoryConfig) -> Self {
        self.inv_cfg = inv_cfg;
        self
    }

    /// Thread the durable-store path so the loop arms the PERSISTENT UTC-day loss
    /// cap at startup from the persisted `"copy"` P&L (and re-checks it each cycle),
    /// making the daily-loss cap bind across the periodic auto-restart. main
    /// sources it from `config.store.path`. Without it the day-loss gate is inert
    /// (a fresh run reads no prior P&L), mirroring
    /// [`MmStrategy::with_store_path`](super::mm::MmStrategy::with_store_path).
    pub fn with_store_path(mut self, store_path: std::path::PathBuf) -> Self {
        self.store_path = Some(store_path);
        self
    }

    /// Seed the follow whitelist. main builds it ONCE for the whitelist-driven
    /// universe ([`whitelist_universe_conditions`]) and shares the SAME snapshot
    /// here, so the EdgePerBet rank doesn't run twice at startup and the universe
    /// matches the traders the strategy copies. `Some(non-empty)` makes the loop
    /// start with it and DEFER the first refresh by `whitelist_refresh`;
    /// `None`/empty keeps the immediate-refresh behavior (the loop builds its own).
    pub fn with_initial_whitelist(mut self, whitelist: Option<Whitelist>) -> Self {
        self.initial_whitelist = whitelist.filter(|w| !w.flat.is_empty());
        self
    }

    /// Attach (or clear) the CLOB metadata resolver that enables ON-DEMAND market
    /// sync: with it, a fresh signal in a market the universe snapshot never
    /// covered is resolved + registered live and traded immediately, instead of
    /// being skipped until the next snapshot. main wires it for LIVE runs (where
    /// the venue reads books live); paper leaves it `None` (no live book feed for
    /// unsynced markets ⇒ on-demand can't fill anyway).
    pub fn with_resolver(mut self, resolver: Option<Arc<ClobRest>>) -> Self {
        self.resolver = resolver;
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
                inv_cfg,
                store_path,
                initial_whitelist,
                resolver,
            } = *self;
            run_copy_loop(
                ctx, params, feed, venue, relayer, tradeable, start_paused, inv_cfg, store_path,
                initial_whitelist, resolver,
            )
            .await;
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
    tradeable: HashMap<String, TradeTokenInfo>,
    start_paused: bool,
    inv_cfg: InventoryConfig,
    store_path: Option<std::path::PathBuf>,
    initial_whitelist: Option<Whitelist>,
    resolver: Option<Arc<ClobRest>>,
) {
    let StrategyCtx {
        kill,
        mut ctl_rx,
        status_tx,
        store_tx,
        ..
    } = ctx;

    // The live trading state the loop owns (the strategy struct is config-only).
    // `inv` carries the REAL `[inventory]` caps so the cumulative-loss circuit
    // breaker (`inv.halted()` inventory stop-loss/daily-loss + the persistent
    // day-loss cap) is meaningful, not a per-trade-derived placeholder.
    let mut state = CopyLoop {
        venue,
        relayer,
        inv: InventoryRisk::new(inv_cfg),
        open: HashMap::new(),
        tradeable,
        resolver,
        store_tx,
        params: params.clone(),
        realized_micro: 0,
        halted: false,
        day: utc_day_from_ms(now_ms()),
        day_loss_halted: false,
        day_loss_read: None,
        feed: feed.clone(),
        cash_micro: None,
        positions_value_micro: None,
        equity_at: None,
        recon_miss: HashMap::new(),
        recon_at: None,
    };
    // PERSISTENT UTC-day loss cap (mirrors the MM): before the first tick, read
    // today's persisted `"copy"` P&L and latch `day_loss_halted` if the day is
    // already at/under the daily-loss cap, so the cap binds across the periodic
    // auto-restart. The read-only handle is RETAINED so the per-cycle re-check
    // (`refresh_day_loss_gate`) also catches a MID-session crossing; a
    // missing/failed DB → no handle → not halted (fresh run) + an inert re-check.
    let day_loss_read = store_path.as_deref().and_then(|p| ReadStore::open(p).ok());
    // RESTART-SAFETY: reload persisted open positions BEFORE the first poll so the
    // loop resumes managing them (follow-exit / stop-loss / redeem) and the gross
    // cap reflects them — instead of orphaning them and re-deploying on top.
    if let Some(read) = day_loss_read.as_ref() {
        state.reload_positions(read);
    }
    state.arm_day_loss_gate(day_loss_read.as_ref(), now_ms());
    state.day_loss_read = day_loss_read;

    let mut seen = SeenKeys::default();
    // SEEDED whitelist: main shares the snapshot it built for the whitelist-driven
    // universe, so we start with it and DEFER the first re-rank (the heavy
    // EdgePerBet fetch already ran in main; the auto_restart re-execs sooner than
    // `whitelist_refresh` anyway). Unseeded ⇒ build it immediately (first tick).
    let seeded = initial_whitelist.as_ref().is_some_and(|w| !w.flat.is_empty());
    let seed = initial_whitelist.unwrap_or_default();
    // `whitelist` is the FLAT union (recent-trades fetch + universe + telemetry);
    // `creamy` is the per-category routing map `select_signals` gates on.
    let mut whitelist: Vec<String> = seed.flat;
    let mut creamy: HashMap<String, HashSet<String>> = seed.creamy;
    let mut paused = start_paused;
    if start_paused {
        tracing::info!("copy: live held — trading PAUSED until release (press `l`)");
    }
    if seeded {
        tracing::info!(
            traders = whitelist.len(),
            "copy: seeded whitelist from the universe build — first refresh deferred"
        );
    }

    // The poll fires an immediate first tick so trading starts at once. The
    // whitelist refresh fires immediately too UNLESS seeded (then the first
    // re-rank is deferred one full period). Skip-on-stall (steady cadence, not a
    // catch-up burst) — mirrors the MM / heartbeat.
    let mut whitelist_tick = if seeded {
        tokio::time::interval_at(
            tokio::time::Instant::now() + params.whitelist_refresh,
            params.whitelist_refresh,
        )
    } else {
        tokio::time::interval(params.whitelist_refresh)
    };
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
                    // GUARD the heavy EdgePerBet rank (up to top_n traders × paged
                    // trade history) with a timeout: it shares this `select!` with
                    // the poll arm, so a slow/stuck refresh would otherwise FREEZE
                    // the whole loop (no polling, no position management). On
                    // timeout, keep the prior whitelist and let the loop continue.
                    match tokio::time::timeout(
                        WHITELIST_REFRESH_TIMEOUT,
                        refresh_whitelist(client, &state.params),
                    )
                    .await
                    {
                        Ok(Some(wl)) => {
                            tracing::info!(
                                traders = wl.flat.len(),
                                categories = wl.creamy.len(),
                                "copy: whitelist refreshed (specialist routing: per-category \
                                 creamy layer, top {} per category)",
                                SPECIALIST_TOP_K_PER_CAT
                            );
                            whitelist = wl.flat;
                            creamy = wl.creamy;
                        }
                        // Keep the PRIOR whitelist on any fetch error — never
                        // poll/trade on a transient-failure-emptied set.
                        Ok(None) => tracing::warn!(
                            traders = whitelist.len(),
                            "copy: whitelist refresh failed — keeping prior whitelist"
                        ),
                        Err(_) => tracing::warn!(
                            traders = whitelist.len(),
                            "copy: whitelist refresh TIMED OUT — keeping prior whitelist (guards \
                             against a hung Data-API rank freezing the loop)"
                        ),
                    }
                }
            }
            _ = poll_tick.tick() => {
                // TRADE only when NOT paused AND a venue is present — otherwise
                // the cycle is skipped entirely (no orders).
                if !paused && state.venue.is_some() {
                    // GUARD the whole cycle with a timeout: a hung venue/Data-API
                    // await here would otherwise silently STALL the loop (stopping
                    // stop-losses / follow-exits / reconciles) while the rest of the
                    // process looks healthy. On timeout, skip this cycle; the next
                    // tick retries. (This is the reliability fix for the observed
                    // ~1 h silent stall.)
                    if tokio::time::timeout(
                        POLL_CYCLE_TIMEOUT,
                        run_poll_cycle(&mut state, &feed, &whitelist, &creamy, &mut seen),
                    )
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            timeout_s = POLL_CYCLE_TIMEOUT.as_secs(),
                            "copy: poll cycle TIMED OUT — skipped, retrying next tick \
                             (a network/venue call hung)"
                        );
                    }
                }
                // HEARTBEAT (every poll tick, paused or not): makes the loop's
                // liveness unambiguous in the logs — a stall now shows as the
                // heartbeat STOPPING, instead of silent ambiguity vs. "just quiet".
                tracing::info!(
                    open = state.open.len(),
                    deployed_usd = state.deployed_micro() as f64 / 1e6,
                    whitelist = whitelist.len(),
                    paused,
                    halted = state.halted || state.day_loss_halted,
                    "copy: heartbeat"
                );
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
            asset: asset_of(cid, oi),
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

    /// A creamy layer for `select_signals` tests. All test trades carry empty
    /// slug/title, so `market_category` classifies them "other" — listing the
    /// wallets there routes them exactly like the old flat whitelist did.
    fn creamy_of(ws: &[&str]) -> HashMap<String, HashSet<String>> {
        HashMap::from([("other".to_string(), ws.iter().map(|s| (*s).to_string()).collect())])
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
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(
            out,
            vec![CopyCandidate {
                condition_id: "m1".to_string(),
                outcome_index: 0,
                asset: asset_of("m1", 0),
                trader: "0xA".to_string(),
                trigger_px: 0.4,
                timestamp: 9_000,
            }]
        );
    }

    #[test]
    fn dedup_open_conditions_dedups_preserving_first_seen_order() {
        let conds = vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "b".to_string(),
        ];
        assert_eq!(
            dedup_open_conditions(&conds, 10),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn dedup_open_conditions_caps_unique_count_keeping_earliest() {
        // Built whitelist-first, so capping keeps the HIGHEST-conviction ids.
        let conds = vec![
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        assert_eq!(
            dedup_open_conditions(&conds, 2),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn dedup_open_conditions_zero_cap_is_empty() {
        assert!(dedup_open_conditions(&["a".to_string()], 0).is_empty());
    }

    #[test]
    fn ondemand_token_params_accepts_a_valid_market() {
        let cond = format!("0x{}", "ab".repeat(32));
        let m: ClobMarket = serde_json::from_str(&format!(
            r#"{{"condition_id":{cond:?},"minimum_tick_size":0.01,"neg_risk":true,
                "tokens":[{{"token_id":"111","outcome":"Yes","price":0.4}},
                          {{"token_id":"222","outcome":"No","price":0.6}}],"active":true}}"#
        ))
        .unwrap();
        let (ts, neg_risk, condition) = ondemand_token_params(&m, "111", &cond).unwrap();
        assert_eq!(ts, TickSize::Cent);
        assert!(neg_risk, "neg_risk carried from the market");
        assert_eq!(condition, cond.parse::<B256>().unwrap());
    }

    #[test]
    fn ondemand_token_params_rejects_foreign_or_empty_asset() {
        let cond = format!("0x{}", "cd".repeat(32));
        let m: ClobMarket = serde_json::from_str(&format!(
            r#"{{"condition_id":{cond:?},"minimum_tick_size":0.01,
                "tokens":[{{"token_id":"111","outcome":"Yes","price":0.4}}],"active":true}}"#
        ))
        .unwrap();
        assert!(ondemand_token_params(&m, "999", &cond).is_none(), "asset not in market");
        assert!(ondemand_token_params(&m, "", &cond).is_none(), "empty asset");
    }

    #[test]
    fn ondemand_token_params_rejects_bad_tick_or_condition() {
        let cond = format!("0x{}", "ef".repeat(32));
        let legacy: ClobMarket = serde_json::from_str(&format!(
            r#"{{"condition_id":{cond:?},"minimum_tick_size":0.04,
                "tokens":[{{"token_id":"111","outcome":"Yes","price":0.4}}],"active":true}}"#
        ))
        .unwrap();
        assert!(ondemand_token_params(&legacy, "111", &cond).is_none(), "legacy 0.04 tick");
        let m: ClobMarket = serde_json::from_str(
            r#"{"condition_id":"not-hex","minimum_tick_size":0.01,
                "tokens":[{"token_id":"111","outcome":"Yes","price":0.4}],"active":true}"#,
        )
        .unwrap();
        assert!(
            ondemand_token_params(&m, "111", "not-hex").is_none(),
            "unparseable condition id"
        );
    }

    #[test]
    fn select_signals_buy_older_than_window_is_excluded() {
        // 8_000 < 8_200 (now - window) ⇒ stale.
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 8_000)]);
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert!(out.is_empty());
    }

    #[test]
    fn select_signals_buy_exactly_at_window_edge_is_included() {
        // timestamp == now - window (8_200) is still fresh (>=).
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 8_200)]);
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn select_signals_sell_is_excluded() {
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Sell, 0.4, 9_000)]);
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert!(out.is_empty());
    }

    #[test]
    fn select_signals_non_whitelisted_wallet_is_excluded() {
        // 0xZ has a fresh buy but is NOT on the whitelist.
        let recent = tmap(vec![trade("0xZ", "m1", 0, TradeSide::Buy, 0.4, 9_000)]);
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert!(out.is_empty());
    }

    #[test]
    fn select_signals_routes_by_specialist_category() {
        // 0xA is a BASEBALL specialist only. Their baseball buy (mlb slug) is a
        // candidate; the SAME trader's soccer buy (fifwc slug) is NOT — specialist
        // routing copies a trader only where they're proven.
        let mut base = trade("0xA", "mBASE", 0, TradeSide::Buy, 0.5, 9_000);
        base.slug = "mlb-nyy-bos-2026".to_string();
        let mut socc = trade("0xA", "mSOCC", 0, TradeSide::Buy, 0.5, 9_100);
        socc.slug = "fifwc-bra-arg-2026".to_string();
        let recent = tmap(vec![base, socc]);
        let creamy = HashMap::from([("baseball".to_string(), HashSet::from(["0xA".to_string()]))]);
        let out = select_signals(&recent, &creamy, &HashSet::new(), NOW, WINDOW);
        assert_eq!(out.len(), 1, "only the baseball (specialist) buy is a candidate");
        assert_eq!(out[0].condition_id, "mBASE");
    }

    #[test]
    fn select_signals_already_seen_key_is_excluded() {
        let recent = tmap(vec![trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 9_000)]);
        let seen = seen_of(&[("m1", 0)]);
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &seen, NOW, WINDOW);
        assert!(out.is_empty());
        // A different outcome on the same market is NOT seen ⇒ still a candidate.
        let recent2 = tmap(vec![trade("0xA", "m1", 1, TradeSide::Buy, 0.4, 9_000)]);
        let out2 = select_signals(&recent2, &creamy_of(&["0xA"]), &seen, NOW, WINDOW);
        assert_eq!(out2.len(), 1);
    }

    #[test]
    fn select_signals_two_wallets_same_key_yield_one_latest() {
        // Both whitelisted, same (cond, outcome); the MOST RECENT buy (0xA @9_500)
        // represents the single candidate (freshest conviction / smallest drift),
        // regardless of whitelist order.
        let recent = tmap(vec![
            trade("0xA", "m1", 0, TradeSide::Buy, 0.45, 9_500),
            trade("0xB", "m1", 0, TradeSide::Buy, 0.40, 9_000),
        ]);
        let out = select_signals(&recent, &creamy_of(&["0xA", "0xB"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trader, "0xA");
        assert_eq!(out[0].timestamp, 9_500);
        assert_eq!(out[0].trigger_px, 0.45);
    }

    #[test]
    fn select_signals_same_wallet_same_key_twice_yields_one_latest() {
        let recent = tmap(vec![
            trade("0xA", "m1", 0, TradeSide::Buy, 0.50, 9_400),
            trade("0xA", "m1", 0, TradeSide::Buy, 0.42, 9_100),
        ]);
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &HashSet::new(), NOW, WINDOW);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].timestamp, 9_400);
    }

    #[test]
    fn select_signals_orders_deterministically_by_timestamp_then_key() {
        // Two distinct keys; output is sorted by timestamp (then cond, outcome),
        // independent of insertion / HashMap order.
        let recent = tmap(vec![
            trade("0xA", "m2", 1, TradeSide::Buy, 0.5, 9_500),
            trade("0xA", "m1", 0, TradeSide::Buy, 0.4, 9_000),
        ]);
        let out = select_signals(&recent, &creamy_of(&["0xA"]), &HashSet::new(), NOW, WINDOW);
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
    /// dropping the loop to recover the `Store` for assertions). Uses the
    /// inert/test [`fallback_inv_config`]; the circuit-breaker tests use
    /// [`loop_with_inv`] to pass TIGHT inventory caps.
    fn loop_with(
        venue: MockVenue,
        tradeable: HashMap<String, TradeTokenInfo>,
        params: CopyParams,
    ) -> (CopyLoop<MockVenue>, tokio::task::JoinHandle<Store>) {
        let inv_cfg = fallback_inv_config(&params);
        loop_with_inv(venue, tradeable, params, inv_cfg)
    }

    /// As [`loop_with`], but with an explicit [`InventoryConfig`] so the
    /// cumulative-loss circuit-breaker tests can exercise `inv.halted()` with a
    /// TIGHT inventory stop-loss / daily-loss floor.
    fn loop_with_inv(
        venue: MockVenue,
        tradeable: HashMap<String, TradeTokenInfo>,
        params: CopyParams,
        inv_cfg: InventoryConfig,
    ) -> (CopyLoop<MockVenue>, tokio::task::JoinHandle<Store>) {
        let store = Store::open_in_memory().unwrap();
        let (store_tx, store_rx) = mpsc::channel(256);
        let writer = tokio::spawn(run_writer(store, store_rx));
        let state = CopyLoop {
            venue: Some(venue),
            relayer: None,
            inv: InventoryRisk::new(inv_cfg),
            open: HashMap::new(),
            tradeable,
            resolver: None,
            store_tx,
            params,
            realized_micro: 0,
            halted: false,
            day: utc_day_from_ms(now_ms()),
            day_loss_halted: false,
            day_loss_read: None,
            feed: None,
            cash_micro: None,
            positions_value_micro: None,
            equity_at: None,
            recon_miss: HashMap::new(),
            recon_at: None,
        };
        (state, writer)
    }

    /// Deterministic stand-in for a market+outcome's venue token-id (the trade's
    /// `asset`), so the tradeable map and the candidates that look it up agree.
    fn asset_of(cid: &str, oi: i64) -> String {
        format!("{cid}#tok{oi}")
    }

    fn tradeable_of(cid: &str, oi: i64, token: TokenId) -> HashMap<String, TradeTokenInfo> {
        HashMap::from([(
            asset_of(cid, oi),
            TradeTokenInfo {
                token,
                ts: TickSize::Cent,
                condition: B256::ZERO,
                neg_risk: false,
            },
        )])
    }

    fn candidate(cid: &str, oi: i64, trader: &str, trigger_px: f64, ts: i64) -> CopyCandidate {
        CopyCandidate {
            condition_id: cid.to_string(),
            outcome_index: oi,
            asset: asset_of(cid, oi),
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
    async fn effective_caps_fix_per_position_and_scale_concurrency() {
        let venue = MockVenue {
            asks: HashMap::new(),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let mut params = copy_params(); // per-copy $5
        params.gross_pct = 0.5;
        params.max_concurrent_positions = 20; // the HARD cap on scaled concurrency
        let (mut state, writer) = loop_with(venue, HashMap::new(), params.clone());

        // (1) No balance fetched yet ⇒ SAFE static fallback (incl. the hard cap).
        assert_eq!(
            state.effective_caps(),
            (
                params.per_position_micro,
                params.capital.0,
                params.max_gross_micro,
                params.max_concurrent_positions
            ),
            "equity unknown ⇒ static configured caps"
        );

        // (2) Equity = cash $20 + positions $20 = $40 → max_gross = 50% = $20.
        // Per-copy stays FIXED at $5; concurrency = min($20 / $5, 20) = 4.
        state.cash_micro = Some(20_000_000);
        state.positions_value_micro = Some(20_000_000);
        let (per_position, capital, max_gross, slots) = state.effective_caps();
        assert_eq!(per_position, 5_000_000, "per-copy is FIXED at $5 (does not scale)");
        assert_eq!(max_gross, 20_000_000, "max_gross = gross_pct × equity");
        assert_eq!(capital, 20_000_000, "capital carve = max_gross");
        assert_eq!(slots, 4, "concurrency = min(max_gross / per_position, hard cap)");

        // (3) Big account → concurrency HARD-CAPPED at 20 (not $250/$5 = 50).
        state.cash_micro = Some(500_000_000); // equity $500 → max_gross $250
        state.positions_value_micro = Some(0);
        let (pp_big, _cap_big, mg_big, slots_big) = state.effective_caps();
        assert_eq!(pp_big, 5_000_000, "per-copy still $5");
        assert_eq!(mg_big, 250_000_000);
        assert_eq!(slots_big, 20, "concurrency hard-capped at 20 trades");

        // (4) gross_pct = 0 ⇒ static caps regardless of equity.
        let mut static_params = copy_params();
        static_params.gross_pct = 0.0;
        let (mut s2, w2) = loop_with(
            MockVenue {
                asks: HashMap::new(),
                bids: HashMap::new(),
                orders: Arc::new(Mutex::new(Vec::new())),
                fail: false,
            },
            HashMap::new(),
            static_params.clone(),
        );
        s2.cash_micro = Some(1_000_000_000);
        s2.positions_value_micro = Some(1_000_000_000);
        assert_eq!(
            s2.effective_caps(),
            (
                static_params.per_position_micro,
                static_params.capital.0,
                static_params.max_gross_micro,
                static_params.max_concurrent_positions
            ),
            "gross_pct = 0 ⇒ static caps regardless of equity"
        );

        drop(state);
        drop(s2);
        let _ = writer.await;
        let _ = w2.await;
    }

    #[test]
    fn reconcile_action_categorizes_wallet_state() {
        use ReconAction::*;
        // Live holding (held, mid mark, not redeemable) → keep.
        assert_eq!(reconcile_action(Some((10.0, 0.55, false))), Keep);
        // Resolved loser (mark ~0) → settle lost.
        assert_eq!(reconcile_action(Some((10.0, 0.0, false))), Settle { won: false });
        assert_eq!(reconcile_action(Some((10.0, 0.01, false))), Settle { won: false });
        // Resolved winner (mark ~1) → settle won.
        assert_eq!(reconcile_action(Some((10.0, 1.0, false))), Settle { won: true });
        assert_eq!(reconcile_action(Some((10.0, 0.99, false))), Settle { won: true });
        // Redeemable ⇒ resolved regardless of mid mark.
        assert_eq!(reconcile_action(Some((10.0, 0.6, true))), Settle { won: true });
        assert_eq!(reconcile_action(Some((10.0, 0.4, true))), Settle { won: false });
        // Not held / dust / absent → gone.
        assert_eq!(reconcile_action(None), Gone);
        assert_eq!(reconcile_action(Some((0.0, 0.5, false))), Gone);
        assert_eq!(reconcile_action(Some((0.005, 0.5, false))), Gone);
    }

    #[tokio::test]
    async fn reconcile_settle_and_prune_clean_the_book() {
        let win = TokenId(1); // resolved winner
        let lose = TokenId(2); // resolved loser
        let gone = TokenId(3); // no longer held on-chain
        let venue = MockVenue {
            asks: HashMap::new(),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let (mut state, writer) = loop_with(venue, HashMap::new(), copy_params());
        // Seed 3 opens: each 10 sh bought @ $0.50 (cost $5).
        for (tok, cid) in [(win, "win"), (lose, "lose"), (gone, "gone")] {
            state.inv.on_fill(tok, 10_000_000, Usdc(-5_000_000));
            state.open.insert(
                (cid.to_string(), 0),
                CopyPosition {
                    trader: "0xabc".into(),
                    entry_ts: 0,
                    qty_micro: 10_000_000,
                    cost_micro: 5_000_000,
                    token: tok,
                    ts: TickSize::Cent,
                    condition: B256::ZERO,
                    asset: format!("{cid}#0"),
                    neg_risk: false,
                },
            );
        }
        assert_eq!(state.open.len(), 3);
        assert_eq!(state.deployed_micro(), 15_000_000);

        // Winner: redeemed for $10 → realized +$5.
        let p = state.open[&("win".to_string(), 0)].clone();
        state.settle_reconciled(&("win".to_string(), 0), &p, true).await;
        // Loser: 0 proceeds → realized −$5.
        let p = state.open[&("lose".to_string(), 0)].clone();
        state.settle_reconciled(&("lose".to_string(), 0), &p, false).await;
        // Gone: neutral flatten at cost → realized unchanged.
        let p = state.open[&("gone".to_string(), 0)].clone();
        state.prune_gone(&("gone".to_string(), 0), &p);

        assert_eq!(state.open.len(), 0, "all three reconciled out of the book");
        assert_eq!(state.deployed_micro(), 0, "deployed frees up for correct sizing");
        assert_eq!(
            state.realized_micro, 0,
            "net realized = +$5 (win) − $5 (lose) + $0 (neutral prune)"
        );

        drop(state);
        let _ = writer.await;
    }

    #[test]
    fn exit_rejection_terminal_vs_transient() {
        // TERMINAL (venue says the position isn't held as tracked) → prune.
        assert!(exit_rejection_is_terminal(
            "live venue error: 400 Bad Request: {\"error\":\"invalid maker amount\"}"
        ));
        assert!(exit_rejection_is_terminal(
            "not enough balance / allowance: the balance is not enough -> balance: 4575, order amount: 8470000"
        ));
        assert!(exit_rejection_is_terminal("invalid amounts, the sell orders maker amount ..."));
        assert!(exit_rejection_is_terminal("INVALID MAKER AMOUNT")); // case-insensitive
        // TRANSIENT / other → keep and retry.
        assert!(!exit_rejection_is_terminal("connection reset by peer"));
        assert!(!exit_rejection_is_terminal("429 Too Many Requests"));
        assert!(!exit_rejection_is_terminal("request timed out"));
    }

    #[tokio::test]
    async fn taker_exit_prunes_sub_minimum_dust() {
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::new(),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with(venue, HashMap::new(), copy_params());
        // Seed a sub-minimum holding: 3 shares (< the 5-share venue min), cost $2.
        state.inv.on_fill(token, 3_000_000, Usdc(-2_000_000));
        let pos = CopyPosition {
            trader: "0xabc".into(),
            entry_ts: 0,
            qty_micro: 3_000_000,
            cost_micro: 2_000_000,
            token,
            ts: TickSize::Cent,
            condition: B256::ZERO,
            asset: "dust#0".into(),
            neg_risk: false,
        };
        state.open.insert(("dust".to_string(), 0), pos.clone());
        assert_eq!(state.open.len(), 1);
        // A market sell of sub-min dust would be rejected by the venue; taker_exit
        // must prune it instead of looping — no submit attempted.
        state
            .taker_exit(&("dust".to_string(), 0), &pos, Px::new(50, TickSize::Cent).unwrap())
            .await;
        assert_eq!(state.open.len(), 0, "sub-minimum dust position pruned, not retried");
        assert!(
            orders.lock().unwrap().is_empty(),
            "no order submitted for unsellable dust"
        );

        drop(state);
        let _ = writer.await;
    }

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
                asset: String::new(),
                neg_risk: false,
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
                asset: String::new(),
                neg_risk: false,
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
                asset: String::new(),
                neg_risk: false,
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
            inv: InventoryRisk::new(fallback_inv_config(&params)),
            open: HashMap::new(),
            tradeable: tradeable_of("m1", 0, TokenId(1)),
            resolver: None,
            store_tx,
            params,
            realized_micro: 0,
            halted: false,
            day: utc_day_from_ms(now_ms()),
            day_loss_halted: false,
            day_loss_read: None,
            feed: None,
            cash_micro: None,
            positions_value_micro: None,
            equity_at: None,
            recon_miss: HashMap::new(),
            recon_at: None,
        };

        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        state.run_exit_sweep(&HashMap::new(), &HashMap::new()).await;

        assert!(state.open.is_empty(), "no venue → no entry");
        assert_eq!(state.inv.net(TokenId(1)), 0, "no venue → nothing booked");
    }

    // ============ cumulative-loss circuit breaker (mirrors the MM) ============

    /// A generous-elsewhere inventory config with explicit stop-loss / daily-loss
    /// floors, for the cumulative-loss circuit-breaker tests (`daily ≥ stop`).
    fn breaker_inv_cfg(stop_loss_micro: i128, daily_loss_micro: i128) -> InventoryConfig {
        InventoryConfig {
            max_inventory_usd: Usdc(1_000_000_000),
            max_gross_inventory_usd: Usdc(1_000_000_000),
            inventory_stop_loss_usd: Usdc(stop_loss_micro),
            daily_loss_usd: Usdc(daily_loss_micro),
            vol_pull_ticks: 0,
            vol_window: Duration::from_secs(1),
        }
    }

    /// INVENTORY HALT: once the marked book pushes a held copy past the inventory
    /// STOP-LOSS floor, `mark_and_check` latches `halted` (and `inv.halted()` ==
    /// StopLoss), the halt is SURFACED in the status, and NEW entries are refused
    /// — while EXITS still run so the position can be flattened. Mirrors the MM's
    /// `mark_and_check` stop-loss latch.
    #[tokio::test]
    async fn inventory_stop_loss_latches_halt_blocks_entries_exits_still_run() {
        use pm_risk::inventory::InvHalt;
        let held = TokenId(1); // the bleeding long
        let entry = TokenId(2); // a fresh candidate's market
        let venue = MockVenue {
            asks: HashMap::from([(entry, cent(50))]),
            bids: HashMap::from([(held, cent(37))]), // marks the long down to $0.37
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        // TIGHT inventory caps: $1 unrealized stop ($6 daily ≥ stop).
        let (mut state, writer) = loop_with_inv(
            venue,
            tradeable_of("m2", 0, entry),
            copy_params(),
            breaker_inv_cfg(1_000_000, 6_000_000),
        );
        // Seed a held long: 10 sh @ $0.50, cost $5.
        state.inv.on_fill(held, 10_000_000, Usdc(-5_000_000));
        state.open.insert(
            ("m1".to_string(), 0),
            CopyPosition {
                trader: "0xA".into(),
                entry_ts: 1_000,
                qty_micro: 10_000_000,
                cost_micro: 5_000_000,
                token: held,
                ts: TickSize::Cent,
                condition: B256::ZERO,
                asset: String::new(),
                neg_risk: false,
            },
        );

        // Mark the book: unrealized = $3.70 − $5.00 = −$1.30 ≤ −$1 stop → StopLoss.
        state.mark_and_check().await;
        assert!(state.halted, "inventory stop-loss latches the session halt");
        assert_eq!(state.inv.halted(), Some(InvHalt::StopLoss));
        assert_eq!(
            state.status(false, 0).halted.as_deref(),
            Some("StopLoss"),
            "the inventory halt is surfaced in StrategyStatus.halted"
        );

        // A fresh, otherwise-tradeable candidate for a DIFFERENT market is REFUSED.
        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m2", 0, "0xB", 0.50, 9_000)], &mut seen)
            .await;
        assert!(
            orders.lock().unwrap().is_empty(),
            "a halted loop places NO new entry"
        );
        assert!(
            !seen.contains(&("m2".to_string(), 0)),
            "a halt-blocked signal is not consumed"
        );
        assert_eq!(state.open.len(), 1, "only the pre-existing held position");

        // EXITS still run while halted: the source sells m1 → flatten it.
        let recent = HashMap::from([(
            "0xA".to_string(),
            vec![trade("0xA", "m1", 0, TradeSide::Sell, 0.37, 2_000)],
        )]);
        state.run_exit_sweep(&recent, &HashMap::new()).await;
        assert!(
            state.open.is_empty(),
            "exits still flatten the held position while halted"
        );
        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![(held, Action::Sell, 37, 10_000_000)],
            "the only order is the flattening SELL — no entry, but the exit ran"
        );

        drop(state);
        let _ = writer.await;
    }

    /// PERSISTENT day-loss cap: when the persisted `"copy"` day-realized ledger is
    /// already at/under the daily-loss cap for the UTC day, `arm_day_loss_gate`
    /// latches `day_loss_halted` at STARTUP (so the cap binds across the periodic
    /// auto-restart — many sub-cap exits whose realized losses SUM over the cap),
    /// the halt is surfaced as `DayLossCap`, and new entries are refused. Mirrors
    /// the MM's `starts_halted_when_day_already_at_loss_cap` /
    /// `day_loss_latches_on_summed_sub_cap_realized`.
    #[tokio::test]
    async fn day_loss_ledger_at_cap_arms_halt_at_startup_and_blocks_entries() {
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::from([(token, cent(50))]),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with_inv(
            venue,
            tradeable_of("m1", 0, token),
            copy_params(),
            breaker_inv_cfg(6_000_000, 6_000_000), // $6 daily cap
        );

        // A real file-backed store whose "copy" day-realized ledger is past the
        // cap for UTC day 0 (FOUR sub-cap −$2 exits summing to −$8 > −$6).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("copy-daycap.sqlite");
        let mut s = Store::open(&path).unwrap();
        let ts = 1_000i64; // within UTC day 0
        let day = utc_day_from_ms(ts);
        for _ in 0..4 {
            s.add_day_realized(day, "copy", -2_000_000).unwrap();
        }
        drop(s);
        let read = ReadStore::open(&path).unwrap();
        assert_eq!(read.day_realized_micro("copy", day).unwrap(), -8_000_000);

        // Arm exactly as run_copy_loop does at startup, for that ledger's day.
        state.arm_day_loss_gate(Some(&read), ts);
        assert!(
            state.day_loss_halted,
            "today already past the daily-loss cap → latched at startup"
        );
        assert_eq!(
            state.status(false, 0).halted.as_deref(),
            Some("DayLossCap"),
            "the persistent day-loss latch is surfaced in StrategyStatus.halted"
        );

        // A fresh, otherwise-tradeable candidate is REFUSED.
        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        assert!(
            orders.lock().unwrap().is_empty(),
            "the persistent day-loss latch blocks new entries"
        );
        assert!(!seen.contains(&("m1".to_string(), 0)), "blocked → not consumed");

        drop(state);
        let _ = writer.await;
    }

    /// PERSISTENT day-loss cap RE-LATCH mid-session: a session that started UNDER
    /// the cap still halts once the cumulative `"copy"` ledger crosses it — not
    /// only at the next auto-restart. Mirrors the MM's
    /// `day_loss_re_latches_mid_session_from_ledger`.
    #[tokio::test]
    async fn day_loss_re_latches_mid_session_from_copy_ledger() {
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::from([(token, cent(50))]),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with_inv(
            venue,
            tradeable_of("m1", 0, token),
            copy_params(),
            breaker_inv_cfg(6_000_000, 6_000_000),
        );
        assert!(!state.day_loss_halted, "starts un-halted (no handle armed yet)");

        // Seed the LEDGER past the cap for TODAY (the day the loop is accounting
        // against), then attach the read handle exactly as run_copy_loop retains it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("copy-midsession.sqlite");
        let mut s = Store::open(&path).unwrap();
        let today = utc_day_from_ms(now_ms());
        for _ in 0..4 {
            s.add_day_realized(today, "copy", -2_000_000).unwrap();
        }
        drop(s);
        state.day_loss_read = Some(ReadStore::open(&path).unwrap());

        // One per-cycle refresh must RE-LATCH from the ledger and block entries.
        state.refresh_day_loss_gate(now_ms());
        assert!(
            state.day_loss_halted,
            "per-cycle re-check latches once the cumulative ledger crosses the cap"
        );
        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        assert!(
            orders.lock().unwrap().is_empty(),
            "no entry after the mid-session day-loss latch"
        );

        drop(state);
        let _ = writer.await;
    }

    /// UTC-day ROLLOVER release: a `day_loss_halted` latch is RELEASED once the
    /// UTC day rolls over (a fresh day gets a fresh cap), so entries RESUME.
    /// Mirrors the MM's rollover release at the top of `tick`.
    #[tokio::test]
    async fn day_loss_releases_on_utc_rollover_and_entries_resume() {
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::from([(token, cent(50))]),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with_inv(
            venue,
            tradeable_of("m1", 0, token),
            copy_params(),
            breaker_inv_cfg(6_000_000, 6_000_000),
        );

        // Ledger past the cap for UTC day 0 → arm halted on day 0.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("copy-rollover.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.add_day_realized(0, "copy", -8_000_000).unwrap();
        drop(s);
        let read = ReadStore::open(&path).unwrap();
        let day0_ms = 1_000i64; // within UTC day 0
        state.arm_day_loss_gate(Some(&read), day0_ms);
        state.day_loss_read = Some(read); // retain for the per-cycle re-check
        assert!(state.day_loss_halted, "day 0 ledger past the cap → latched");

        // Entries are blocked BEFORE the rollover.
        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        assert!(
            orders.lock().unwrap().is_empty(),
            "day-loss latch blocks entries before the rollover"
        );

        // UTC day rolls over → the persistent latch RELEASES (the ledger for the
        // fresh day is 0, so the per-cycle re-check does not re-latch).
        let day1_ms = day0_ms + 24 * 3_600 * 1_000;
        state.refresh_day_loss_gate(day1_ms);
        assert!(
            !state.day_loss_halted,
            "rollover releases the persistent day-loss latch"
        );
        assert_eq!(state.day, utc_day_from_ms(day1_ms), "now accounting the fresh day");

        // Entries RESUME on the fresh day: the same fresh candidate now enters.
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![(token, Action::Buy, 50, 10_000_000)],
            "a fresh UTC day re-opens entries"
        );

        drop(state);
        let _ = writer.await;
    }

    /// UTC-day ROLLOVER SELF-HEAL of the IN-SESSION inventory halt: a latched
    /// `state.halted` (the `InventoryRisk` daily/stop floor, previously sticky for
    /// the whole session) is now RELEASED on the rollover, so one bad day no longer
    /// strands the strategy halted across every following midnight until a MANUAL
    /// restart. (Re-arm-when-still-breaching is covered by the pure
    /// `InventoryRisk::roll_day` tests.)
    #[tokio::test]
    async fn inventory_halt_self_heals_on_utc_rollover_and_entries_resume() {
        use pm_risk::inventory::InvHalt;
        let token = TokenId(1);
        let venue = MockVenue {
            asks: HashMap::from([(token, cent(50))]),
            bids: HashMap::new(),
            orders: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        let orders = Arc::clone(&venue.orders);
        let (mut state, writer) = loop_with_inv(
            venue,
            tradeable_of("m1", 0, token),
            copy_params(),
            breaker_inv_cfg(6_000_000, 6_000_000), // $6 stop / $6 daily
        );

        // Latch the in-session inventory halt on UTC day 0 exactly as
        // `mark_and_check` would: bank −$10 realized via a flat round-trip, mark,
        // and mirror the resulting latch onto `state.halted`.
        state.inv.on_fill(token, 100_000_000, Usdc(-50_000_000)); // buy 100 @ $0.50
        state.inv.on_fill(token, -100_000_000, Usdc(40_000_000)); // sell 100 @ $0.40 → −$10, flat
        assert_eq!(state.inv.mark(&Marks::new()).halted, Some(InvHalt::DailyLoss));
        state.halted = true;
        state.day = utc_day_from_ms(1_000); // accounting UTC day 0

        // Entries are blocked BEFORE the rollover.
        let mut seen = SeenKeys::default();
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        assert!(
            orders.lock().unwrap().is_empty(),
            "the in-session inventory halt blocks entries before the rollover"
        );

        // UTC day rolls over → self-heal: `state.halted` drops and the
        // InventoryRisk latch clears (the fresh day's flat book won't re-latch).
        let day1_ms = 1_000i64 + 24 * 3_600 * 1_000;
        state.refresh_day_loss_gate(day1_ms);
        assert!(!state.halted, "rollover self-heals the in-session inventory halt");
        assert_eq!(state.inv.halted(), None, "InventoryRisk latch cleared by roll_day");
        assert_eq!(state.day, utc_day_from_ms(day1_ms), "now accounting the fresh day");

        // Entries RESUME on the fresh day.
        state
            .run_entries(&[candidate("m1", 0, "0xA", 0.50, 9_000)], &mut seen)
            .await;
        assert_eq!(
            orders.lock().unwrap().clone(),
            vec![(token, Action::Buy, 50, 10_000_000)],
            "a fresh UTC day re-opens entries after the inventory halt self-heals"
        );

        drop(state);
        let _ = writer.await;
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
            asset_of("m2", 1),
            TradeTokenInfo {
                token: t2,
                ts: TickSize::Cent,
                condition: B256::ZERO,
                neg_risk: false,
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
                asset: String::new(),
                neg_risk: false,
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
            inv: InventoryRisk::new(fallback_inv_config(&params)),
            open: HashMap::new(),
            tradeable: tradeable_of("m1", 0, t1),
            resolver: None,
            store_tx,
            params,
            realized_micro: 0,
            halted: false,
            day: utc_day_from_ms(now_ms()),
            day_loss_halted: false,
            day_loss_read: None,
            feed: None,
            cash_micro: None,
            positions_value_micro: None,
            equity_at: None,
            recon_miss: HashMap::new(),
            recon_at: None,
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
