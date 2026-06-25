//! `MmStrategy` — the market-making quoting loop (multi-strategy platform,
//! Task 4.2). The platform's first risk-taking strategy: it quotes a symmetric
//! bid/ask around the book mid on a fixed cadence, sizes each quote by a
//! notional cap clamped by the inventory caps, books fills into inventory +
//! accounting + the store, and latches a safety stop on an inventory halt.
//!
//! # Scope (Task 4.2 — the CORE loop only)
//! - **Fair = mid** quoting; bid = fair − spread/2, ask = fair + spread/2.
//! - **Sizing** = `max_quote_usd` notional per side (and never above the
//!   strategy's capital), gated by [`InventoryRisk::check_quote`].
//! - **Fills** → [`InventoryRisk::on_fill`] (authoritative signed inventory) +
//!   [`PositionBook`] (reporting) + a `"mm"`-tagged store fill row.
//! - **Safety stop**: `InventoryRisk::mark` on the mid feed; a latched
//!   [`InvHalt`](pm_risk::inventory::InvHalt) cancels all quotes and stops
//!   quoting (latched).
//! - **Pause/kill**: mirrors the [`stub`](super::stub) lifecycle — pause cancels
//!   resting quotes and stops quoting (fills are always consumed); the global
//!   kill cancels and exits cleanly.
//! - **Paper by default**: runs over the [`PaperMakerVenue`] unless main clears
//!   it for live (the gated live arm below — Task 4.5).
//!
//! # Scope (Task 4.3 — skew + volatility pull)
//! - **Inventory SKEW**: [`skew_fair`] shifts `fair` (= mid) against inventory
//!   inside [`compute_quotes`] — a long lowers BOTH quotes, a short raises them,
//!   scaled by `clamp(net / inventory_cap, ±1)` up to `inventory_skew_bps`.
//! - **Volatility pull**: [`InventoryRisk::vol_hint`] fires on a large + fast
//!   mid move in [`MmLoop::quote`], excluding that token from the desired set so
//!   `reconcile` cancels its resting quotes without replacing (a pull).
//!
//! # Scope (Task 4.4 — maker-rebate accrual, this task)
//! - **Rebate accrual** (estimate): [`MmLoop::consume_fills`] accrues
//!   `rebate_bps · fill_notional / 10_000` per maker fill into a running
//!   `rebate_accrued_micro`, surfaced as the SEPARATE [`StrategyStatus::rebate_micro`].
//!   Deliberately NOT folded into cash/equity/realized — it is an unverified,
//!   paid-out-of-band ESTIMATE; folding it would inflate position P&L (spec §7).
//!
//! # Scope (Task 4.5 — gated live arm)
//! - **Default-off live venue**: [`MmStrategy`] carries an `Option<MmLive>` set
//!   by main ONLY when cleared for live by
//!   [`mm_use_live`](crate::wiring::mm_use_live) (process `--live` AND
//!   `[strategies.mm].live`, which implies the startup confirmation ran). `run`
//!   drives the SAME generic [`run_mm_loop`] over the live venue when present,
//!   else the [`PaperMakerVenue`]. PAPER is the default and the paper arm NEVER
//!   constructs or holds a live venue. Live MM's safety is the capital carve +
//!   inventory caps + postOnly + the confirmation — no new mechanism.
//!
//! # Scope (Task 4.6 — user-WS fills source, this task)
//! - **Low-latency live fills**: the live arm is now an [`MmLive`] enum — either
//!   the Task-4.5 [`LiveVenue`] REST poll ([`Rest`](MmLive::Rest)) OR the
//!   user-WS feed ([`Ws`](MmLive::Ws)): the live `LiveVenue` (the `MakerVenue`)
//!   paired via a [`SplitVenue`] with [`LiveUserWsFills`] (the `UserFillSource`).
//!   main picks the variant from `[strategies.mm].live_fills_source` (default
//!   `"ws"`). BOTH are `MakerVenue + UserFillSource`, so the SAME `run_mm_loop`
//!   drives either with zero loop changes — they differ ONLY in fill latency.
//!
//! # Accounting note (why InventoryRisk is authoritative)
//! [`InventoryRisk`] tracks SIGNED net inventory + realized/unrealized P&L and
//! is the source of truth for risk. [`PositionBook`] is fed in lock-step (cost
//! basis deltas + cash mirror inventory exactly) so `positions.pnl` reports the
//! same equity; held tokens are valued from the signed net at the current book,
//! so the report is correct for both long and short inventory even though
//! `PositionBook` itself is append-only.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pm_core::book::{Book, Side};
use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{Px, Qty, TickSize, Usdc, buy_cost, sell_proceeds};
use alloy_primitives::{B256, U256};
use pm_execution::fills::UserFillSource;
use pm_execution::live::LiveVenue;
use pm_execution::maker::{MakerOrder, MakerVenue, OrderId, OrderType};
use pm_execution::paper_maker::PaperMakerVenue;
use pm_execution::quote_manager::QuoteManager;
use pm_execution::relayer::{RelayerClient, RelayerError};
use pm_execution::split_venue::SplitVenue;
use pm_execution::user_ws::LiveUserWsFills;
use pm_ingestion::supervisor::OnApplyFn;
use pm_risk::inventory::{InventoryConfig, InventoryRisk, Marks, QuoteIntent, QuoteVerdict};
use pm_store::read::ReadStore;
use pm_store::writer::StoreMsg;
use pm_store::{FillRow, OrderRow, PnlRow, RfDecisionRow, RfOutcomeRow, usdc_to_i64, utc_day_from_ms};
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tracing::warn;

use crate::coordinator::now_ms;
use crate::positions::PositionBook;
use crate::wiring::BookFetcher;

use super::quote_policy::{
    Policy, adjusted_mid, combined_signal, imbalance, ladder_depth, microprice, needs_requote,
    needs_requote_size, reward_quote_prices, should_pull, skewed_sizes,
};
use super::reward_score::{ScoredOrder, est_daily_reward_usd, order_score, q_min, quote_set_q_min};
use super::signals::SignalState;
use super::{
    RestingOrderSnapshot, RewardFarmStatus, Strategy, StrategyCommand, StrategyCtx, StrategyId,
    StrategyStatus,
};

/// Resolved quote-loop parameters (USD → µUSDC done once, up front).
#[derive(Debug, Clone, Copy)]
pub struct MmParams {
    /// Total quoted spread around fair, in bps of $1 (1 bp = 100 µUSDC/share).
    pub spread_bps: u32,
    /// Quote-loop cadence.
    pub quote_refresh: Duration,
    /// Max notional per single quote (one side), µUSDC.
    pub max_quote_micro: i128,
    /// Inventory-skew limit (Task 4.3): MAX fair-value shift at full per-market
    /// inventory, bps of $1 (1 bp = 100 µUSDC/share). `0` disables skew. See
    /// [`skew_fair`].
    pub inventory_skew_bps: u32,
    /// Maker-rebate ESTIMATE (Task 4.4): bps of each maker fill's NOTIONAL
    /// accrued as an estimated rebate. `0` assumes no rebate. Surfaced as a
    /// SEPARATE display quantity — never folded into cash/equity/realized (it is
    /// an unverified, out-of-band estimate). See [`MmLoop::consume_fills`].
    pub rebate_bps: u32,
    /// PAPER-only passive-taker-flow fill rate (% of remaining per poll) for the
    /// `PaperMakerVenue` demo aid. `0` = conservative adverse-only sim. Ignored
    /// on the live path (live fills come from the real user feed).
    pub paper_taker_fill_pct: u32,
    /// Quote policy (Task 5): [`Policy::SpreadCapture`] (default, legacy spread
    /// quoting — unchanged) or [`Policy::RewardFarm`] (tight two-sided
    /// liquidity-reward quoting, spec §8). Resolved from `[strategies.mm].policy`
    /// in [`MmParams::from_config`]; selects the quote-computation branch in
    /// [`MmLoop::quote`]. The two paths share the SAME veto / no-naked-short /
    /// inventory gating, so they differ ONLY in which prices/sizes they propose.
    pub policy: Policy,
    /// RewardFarm size-skew cap (Task 6, spec §8.3): the MAX bigger:smaller size
    /// ratio between the two sides when leaning against inventory. The reward
    /// score is quadratic in TIGHTNESS, so the lean is expressed by skewing
    /// SIZES (bigger on the reducing side), never prices — prices stay pinned at
    /// the tight reward band. `1.0` disables the lean (balanced). Read by
    /// [`reward_compute_quotes`] via [`skewed_sizes`]; inert under SpreadCapture.
    ///
    /// SOURCE: the operator knob is `[reward_farm].size_skew_max_ratio`
    /// (validated finite & ≥ 1.0 in `pm_config`). That section is a SIBLING of
    /// `[strategies.mm]`, so [`MmParams::from_config`] takes
    /// `&pm_config::RewardFarm` alongside `&pm_config::Mm` and copies the value
    /// through; main passes `&config.reward_farm` at the call site.
    pub size_skew_max_ratio: f64,
    /// RewardFarm STICKY re-quote band (Task 8, spec §8): keep a resting quote
    /// in place until its target price drifts more than this many ticks away,
    /// instead of replacing it every cycle. A replace is a cancel+place, which
    /// resets Polymarket's TIME-WEIGHTED reward score — so re-quoting on every
    /// sub-band wiggle would erode the very reward this policy farms. `0` means
    /// "replace on any tick change" (no stickiness). Read by [`MmLoop::quote`]
    /// via [`needs_requote`]; inert under SpreadCapture.
    ///
    /// SOURCE: the operator knob is `[reward_farm].requote_band_ticks` (the
    /// SIBLING `[reward_farm]` section), threaded through [`MmParams::from_config`]
    /// exactly like `size_skew_max_ratio`.
    pub requote_band_ticks: u16,
    /// RewardFarm ESTIMATOR sampling cadence (Task 11, spec §9): how often the
    /// loop recomputes the local liquidity-reward estimate (`q_min`, est $/day,
    /// balance) on our resting quotes, mirroring Polymarket's per-minute
    /// sampling. The FIRST cycle always samples (so the dashboard shows a figure
    /// immediately); thereafter [`MmLoop::tick`] re-samples only once this
    /// interval has elapsed. Inert under SpreadCapture (no estimator runs).
    ///
    /// SOURCE: the operator knob is `[reward_farm].sample_interval_ms` (the
    /// SIBLING `[reward_farm]` section), threaded through [`MmParams::from_config`]
    /// exactly like `size_skew_max_ratio` / `requote_band_ticks`.
    pub sample_interval: Duration,
    /// RewardFarm Phase-A: book levels used for the microprice + imbalance
    /// signal. Threaded from `[reward_farm].microprice_levels` exactly like
    /// `size_skew_max_ratio`; consumed by the Phase-A adverse-selection logic.
    pub microprice_levels: u16,
    /// RewardFarm Phase-A: rolling window (ms) for the momentum signal.
    /// Threaded from `[reward_farm].signal_window_ms` (sibling section).
    pub signal_window_ms: u64,
    /// RewardFarm Phase-A: `|signal|` above this pulls the endangered side
    /// ([0,1]). Threaded from `[reward_farm].pull_threshold` (sibling section).
    pub pull_threshold: f64,
    /// RewardFarm Phase-A: suppress re-quoting a pulled side this long (ms).
    /// Threaded from `[reward_farm].pull_cooldown_ms` (sibling section).
    pub pull_cooldown_ms: u64,
    /// RewardFarm Phase-A: re-place a side when its size lean drifts more than
    /// this fraction. Threaded from `[reward_farm].size_rebalance_pct` (sibling).
    pub size_rebalance_pct: f64,
    /// RewardFarm Phase-B: opt-in complement-pair quoting (BID-YES + BID-NO) for
    /// two-sided-from-flat reward farming. Threaded from
    /// `[reward_farm].hedging_enabled` (sibling section); consumed by Phase-B.
    pub hedging_enabled: bool,
    /// RewardFarm Phase-B: merge a held complete YES+NO set once the matched
    /// pair exceeds this (USD). Threaded from `[reward_farm].merge_threshold_usd`
    /// (sibling section); consumed by Phase-B.
    pub merge_threshold_usd: f64,
}

impl MmParams {
    /// Resolve `[strategies.mm]` + `[reward_farm]` config into runtime params
    /// (the USD notional is converted to µUSDC here). The seam the Task-4.5 main
    /// wiring uses. `rf` supplies `[reward_farm].size_skew_max_ratio` — a SIBLING
    /// section of `[strategies.mm]`, validated finite & ≥ 1.0 by `pm_config`.
    pub fn from_config(
        mm: &pm_config::Mm,
        rf: &pm_config::RewardFarm,
    ) -> Result<Self, pm_config::ConfigError> {
        Ok(MmParams {
            spread_bps: mm.spread_bps,
            quote_refresh: Duration::from_millis(mm.quote_refresh_ms),
            max_quote_micro: pm_config::usd_to_microusdc(mm.max_quote_usd)?,
            inventory_skew_bps: mm.inventory_skew_bps,
            rebate_bps: mm.rebate_bps,
            paper_taker_fill_pct: mm.paper_taker_fill_pct,
            policy: Policy::from_cfg(&mm.policy),
            // `[reward_farm]` is a sibling of `[strategies.mm]`; main passes it in
            // (`&config.reward_farm`) so the operator's cap reaches the loop.
            size_skew_max_ratio: rf.size_skew_max_ratio,
            // Same SIBLING-section threading: the anti-flicker re-quote band.
            requote_band_ticks: rf.requote_band_ticks,
            // Same SIBLING-section threading: the estimator sampling cadence.
            sample_interval: Duration::from_millis(rf.sample_interval_ms),
            // Phase-A adverse-selection knobs — same SIBLING-section threading.
            microprice_levels: rf.microprice_levels,
            signal_window_ms: rf.signal_window_ms,
            pull_threshold: rf.pull_threshold,
            pull_cooldown_ms: rf.pull_cooldown_ms,
            size_rebalance_pct: rf.size_rebalance_pct,
            // Phase-B complement-pair + merge knobs — same SIBLING-section threading.
            hedging_enabled: rf.hedging_enabled,
            merge_threshold_usd: rf.merge_threshold_usd,
        })
    }
}

/// The LIVE market-maker venue variant (Task 4.6), built by main from
/// `[strategies.mm].live_fills_source` when the MM is cleared for live. Both
/// variants are `MakerVenue + UserFillSource`, so `run` drives the SAME generic
/// [`run_mm_loop`] over either — they differ ONLY in where fills come from:
///
/// * [`Rest`](MmLive::Rest) — the Task-4.5 [`LiveVenue`]: live maker orders AND
///   the REST `/data/trades` fill poll on ONE object (the offline-verified
///   fallback if the WS misbehaves in canary).
/// * [`Ws`](MmLive::Ws) — the low-latency scalping upgrade: the live
///   [`LiveVenue`] as the `MakerVenue`, paired via a [`SplitVenue`] with the
///   user-WS [`LiveUserWsFills`] as the `UserFillSource`.
pub enum MmLive {
    /// Live maker orders + REST `/data/trades` fill poll (one [`LiveVenue`]).
    Rest(LiveVenue),
    /// Live maker orders + low-latency user-WS fills.
    Ws(SplitVenue<LiveVenue, LiveUserWsFills>),
}

/// Which live fills source the MM uses, parsed from the VALIDATED
/// `[strategies.mm].live_fills_source` config string (Task 4.6). A pure decision
/// (no venue construction), so main's `"ws"`-vs-`"rest"` branch is unit-tested
/// directly without any network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MmFillsSource {
    /// Low-latency user-WS feed (the default; the scalping upgrade).
    Ws,
    /// REST `/data/trades` poll (the Task-4.5 offline-verified fallback).
    Rest,
}

impl MmFillsSource {
    /// Map the (config-validated) string to the source: `"rest"` → [`Rest`](Self::Rest);
    /// anything else → [`Ws`](Self::Ws). Config validation already pins the value
    /// to `"ws"` | `"rest"` and `"ws"` is the default, so the catch-all is the WS
    /// feed.
    pub fn from_config(s: &str) -> Self {
        match s {
            "rest" => MmFillsSource::Rest,
            _ => MmFillsSource::Ws,
        }
    }
}

/// Market-making strategy (spec §7). Constructed by main; `run` builds the
/// inventory risk + position book + quote manager and drives [`run_mm_loop`]
/// over either the PAPER maker venue (default) or — when main clears MM for live
/// — an [`MmLive`] venue (REST poll or user-WS fills).
pub struct MmStrategy {
    id: StrategyId,
    /// Markets to quote (provided; Phase 5 refines the universe per segment).
    tokens: Vec<TokenId>,
    /// `token → market` for [`PositionBook::apply`] (from the registry).
    token_market: HashMap<TokenId, MarketId>,
    /// Spec-2 Phase B (§5.1): yes↔no complement map for the quoted markets,
    /// populated by main ONLY under reward-farm hedging (empty otherwise). Lets
    /// the quote loop / estimator pair a market's two complement bids; threaded
    /// here and consumed by B3/B4 (set via [`with_complement`](Self::with_complement)).
    complement: HashMap<TokenId, TokenId>,
    params: MmParams,
    inv_cfg: InventoryConfig,
    capital: Usdc,
    /// Live venue selection (Task 4.5 / 4.6). `None` (the default produced by
    /// [`MmStrategy::new`]) → the PAPER maker venue. `Some(MmLive::…)` ONLY when
    /// main called [`with_live_venue`](MmStrategy::with_live_venue) after
    /// [`mm_use_live`](crate::wiring::mm_use_live) cleared MM for live (process
    /// `--live` AND `[strategies.mm].live`, which implies the startup
    /// confirmation ran). The variant ([`Rest`](MmLive::Rest) vs [`Ws`](MmLive::Ws))
    /// is chosen from `[strategies.mm].live_fills_source`. The paper arm
    /// therefore never constructs or holds a live venue.
    live_venue: Option<MmLive>,
    /// Startup inventory seed (Phase-4 reload): `(token, net µshares, basis cash)`
    /// from `store.open_positions()` scoped to `"mm"`. `run` applies each via
    /// [`InventoryRisk::seed`] BEFORE quoting, so a restart resumes from the REAL
    /// held position (and can offload it via the ask side) instead of starting
    /// flat — which also makes the auto-restart loop position-correct. Empty by
    /// default (a fresh session with no persisted MM lots).
    seed: Vec<(TokenId, i128, Usdc)>,
    /// Start the quote loop PAUSED (the live-release latch is HELD). For TUI live
    /// the operator must press `l` to release — until then the MM must NOT quote
    /// (it trades real money). main sets this when live trading is held; the host
    /// later sends `SetPaused(false)` on release. Default `false` (paper / shadow
    /// / headless-confirmed all run immediately).
    start_paused: bool,
    /// Path to the durable SQLite store, threaded so the loop can open a
    /// READ-ONLY connection at startup to arm the PERSISTENT UTC-day loss cap
    /// (Task 9) from the persisted `"mm"` P&L. `None` (the default) → the loop
    /// reads no prior P&L and the day-loss gate stays inert (a fresh run). main
    /// sets it from `config.store.path`; the seed reload already reads the same
    /// file, so this only opens a second short-lived read connection at startup.
    store_path: Option<std::path::PathBuf>,
    /// M6-7: the LIVE on-chain merge relayer ([`RelayerClient`], `Arc`-shared into
    /// each spawned merge task). main builds it via [`RelayerClient::new`] ONLY
    /// when the relayer is enabled + configured AND MM is cleared for live; `None`
    /// (the default from [`new`](Self::new)) on paper / arb / non-relayer live, so
    /// the live merge stays the hold-to-resolution no-op there. Set via
    /// [`with_merger`](Self::with_merger).
    merger: Option<Arc<RelayerClient>>,
    /// M6-7: `token → on-chain conditionId` for the quoted reward-farm universe
    /// (BOTH legs of a market → its single `conditionId`), so the live merge sweep
    /// can build the WALLET `mergePositions` batch. main builds it from the
    /// registry; empty by default (only populated for reward-farm hedging). Set
    /// via [`with_conditions`](Self::with_conditions).
    cond_by_token: HashMap<TokenId, B256>,
    /// R1 (auto-redeem): `token → CLOB asset id` string for the quoted reward-farm
    /// universe, so the resolved-position feed can match a Data-API
    /// [`Position.asset`](pm_ingestion::data_api::Position) back to the MM's
    /// internal `TokenId`. main builds it from the registry (`token_venue_id`);
    /// empty by default, so non-redeem paths never consult it. Set via
    /// [`with_venue_ids`](Self::with_venue_ids).
    venue_by_token: HashMap<TokenId, String>,
    /// R1 (auto-redeem): the Polymarket Data-API positions feed used to discover
    /// RESOLVED (`redeemable`) markets the MM still holds. main passes `Some` ONLY
    /// on a relayer-backed reward-farm live run; `None` (the default) on paper /
    /// arb / non-relayer live keeps the redeem feed inert. Set via
    /// [`with_data_api`](Self::with_data_api).
    data_api: Option<Arc<pm_ingestion::data_api::DataApiClient>>,
    /// R1 (auto-redeem): the LOWERCASED deposit-wallet address the positions query
    /// is keyed on (the wallet that actually holds the resolved positions). `Some`
    /// only alongside [`data_api`](Self::data_api); `None` by default. Set via
    /// [`with_deposit_wallet`](Self::with_deposit_wallet).
    deposit_wallet: Option<String>,
}

impl MmStrategy {
    /// Construct the (PAPER-default) market maker: `live_venue` is `None`, so
    /// `run` builds a [`PaperMakerVenue`]. Live is opt-in via
    /// [`with_live_venue`](MmStrategy::with_live_venue) — keeping the paper path
    /// the structural default (the only way to reach a live venue is the explicit
    /// builder call, which main makes solely behind
    /// [`mm_use_live`](crate::wiring::mm_use_live)).
    pub fn new(
        tokens: Vec<TokenId>,
        token_market: HashMap<TokenId, MarketId>,
        params: MmParams,
        inv_cfg: InventoryConfig,
        capital: Usdc,
    ) -> Self {
        MmStrategy {
            id: StrategyId("mm"),
            tokens,
            token_market,
            complement: HashMap::new(),
            params,
            inv_cfg,
            capital,
            live_venue: None,
            seed: Vec::new(),
            start_paused: false,
            store_path: None,
            merger: None,
            cond_by_token: HashMap::new(),
            venue_by_token: HashMap::new(),
            data_api: None,
            deposit_wallet: None,
        }
    }

    /// M6-7: attach the LIVE on-chain merge relayer so a reward-farm live run
    /// RECYCLES a complete YES+NO set on-chain (via a periodic, non-blocking
    /// sweep) instead of holding it to resolution. main passes `Some` ONLY when
    /// the relayer is enabled + configured AND MM is cleared for live; `None`
    /// (the default) keeps the hold-to-resolution no-op. Paired with
    /// [`with_conditions`](Self::with_conditions) (the sweep needs both).
    pub fn with_merger(mut self, merger: Option<Arc<RelayerClient>>) -> Self {
        self.merger = merger;
        self
    }

    /// M6-7: attach the `token → on-chain conditionId` map the live merge sweep
    /// needs to build each `mergePositions` batch. main builds it from the
    /// registry for the quoted reward-farm universe (both legs of a market map to
    /// its single conditionId); the default is empty, so non-merge paths are
    /// unaffected.
    pub fn with_conditions(mut self, cond_by_token: HashMap<TokenId, B256>) -> Self {
        self.cond_by_token = cond_by_token;
        self
    }

    /// R1 (auto-redeem): attach the `token → CLOB asset id` map the resolved-position
    /// feed needs to match a Data-API `Position.asset` back to the MM's `TokenId`.
    /// main builds it from the registry for the quoted reward-farm universe; the
    /// default is empty, so non-redeem paths are unaffected. Mirrors
    /// [`with_conditions`](Self::with_conditions).
    pub fn with_venue_ids(mut self, venue_by_token: HashMap<TokenId, String>) -> Self {
        self.venue_by_token = venue_by_token;
        self
    }

    /// R1 (auto-redeem): attach the Polymarket Data-API positions feed used to
    /// discover RESOLVED markets the MM still holds. main passes `Some` ONLY on a
    /// relayer-backed reward-farm live run; `None` (the default) keeps the redeem
    /// feed inert. (R1 only HOLDS the client — the redeem sweep that polls it is R2;
    /// the client is NOT constructed here.)
    pub fn with_data_api(
        mut self,
        data_api: Option<Arc<pm_ingestion::data_api::DataApiClient>>,
    ) -> Self {
        self.data_api = data_api;
        self
    }

    /// R1 (auto-redeem): attach the LOWERCASED deposit-wallet address the positions
    /// query is keyed on. main passes `Some` alongside
    /// [`with_data_api`](Self::with_data_api); `None` (the default) otherwise.
    pub fn with_deposit_wallet(mut self, deposit_wallet: Option<String>) -> Self {
        self.deposit_wallet = deposit_wallet;
        self
    }

    /// Spec-2 Phase B (§5.1): attach the yes↔no complement map for the quoted
    /// markets so the reward-farm hedging path can pair a market's two complement
    /// bids. main builds it (yes→no and no→yes for each quoted market) ONLY when
    /// reward-farm hedging is on; the default is empty, so non-hedging / paper /
    /// test paths behave exactly as before. A builder (like [`with_seed`](Self::with_seed))
    /// keeps [`new`](Self::new)'s signature stable.
    pub fn with_complement(mut self, complement: HashMap<TokenId, TokenId>) -> Self {
        self.complement = complement;
        self
    }

    /// Attach a live venue (Task 4.5 / 4.6), switching `run` from the paper maker
    /// venue to real maker orders. The [`MmLive`] variant selects the fill source
    /// (REST poll vs user-WS). main calls this ONLY when
    /// [`mm_use_live`](crate::wiring::mm_use_live) is true (process `--live` AND
    /// `[strategies.mm].live`), so the live path is a deliberate, doubly-gated
    /// opt-in on top of the startup confirmation.
    pub fn with_live_venue(mut self, live: MmLive) -> Self {
        self.live_venue = Some(live);
        self
    }

    /// Seed startup inventory (Phase-4 reload) from persisted lots. main sources
    /// it from `store.open_positions()` (scoped to `"mm"`, quoted tokens only);
    /// `run` applies it via [`InventoryRisk::seed`] before the first quote so the
    /// MM resumes from — and can offload — its real position across restarts.
    pub fn with_seed(mut self, seed: Vec<(TokenId, i128, Usdc)>) -> Self {
        self.seed = seed;
        self
    }

    /// Start the quote loop PAUSED — the live-release latch is HELD (TUI live
    /// before the operator presses `l`). The MM trades real money, so it must
    /// not quote until released; the host sends `SetPaused(false)` on release.
    pub fn with_start_paused(mut self, start_paused: bool) -> Self {
        self.start_paused = start_paused;
        self
    }

    /// Thread the durable-store path so the loop can arm the PERSISTENT UTC-day
    /// loss cap (Task 9) at startup from the persisted `"mm"` P&L. main sources it
    /// from `config.store.path`. Without it the day-loss gate is inert (a fresh
    /// run reads no prior P&L), so paper/test paths that don't set it behave
    /// exactly as before.
    pub fn with_store_path(mut self, store_path: std::path::PathBuf) -> Self {
        self.store_path = Some(store_path);
        self
    }
}

impl Strategy for MmStrategy {
    fn id(&self) -> StrategyId {
        self.id
    }

    /// The MM reads books on its OWN cadence via `ctx.fetcher`, not the
    /// per-supervisor inline hook (that is arb's hot path).
    fn make_on_apply(&self) -> Option<OnApplyFn> {
        None
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let MmStrategy {
                id: _,
                tokens,
                token_market,
                complement,
                params,
                inv_cfg,
                capital,
                live_venue,
                seed,
                start_paused,
                store_path,
                merger,
                cond_by_token,
                venue_by_token,
                data_api,
                deposit_wallet,
            } = *self;
            // Per-strategy state is identical for both venues; build it once.
            let qm = QuoteManager::new();
            // Reload persisted inventory BEFORE quoting (Phase-4 seed): resume the
            // real signed net + basis per token so a restart manages (and offloads)
            // held positions instead of starting flat.
            let mut inv = InventoryRisk::new(inv_cfg);
            for (token, net, basis) in &seed {
                inv.seed(*token, *net, *basis);
            }
            let positions = PositionBook::default();
            // VENUE SELECTION (Task 4.5 / 4.6): the live venue is present ONLY when
            // main cleared MM for live (process `--live` AND `[strategies.mm].live`
            // → `mm_use_live`, which implies the startup confirmation ran), and its
            // variant (`MmLive::Rest` REST poll vs `MmLive::Ws` user-WS fills) was
            // chosen from `[strategies.mm].live_fills_source`. Otherwise build the
            // CONCRETE paper venue here — the unchanged 4.4 default. ALL THREE arms
            // drive the SAME generic `run_mm_loop`, so there is ZERO quoting-logic
            // duplication; they differ only in the concrete `V` (each is
            // `MakerVenue + UserFillSource`). Building the concrete venue in this
            // non-generic context keeps the returned future provably `Send` even
            // though `run_mm_loop` is generic (the pattern arb uses for
            // `run_execution`). Each value below is moved into exactly one arm; the
            // arms are mutually exclusive, so this is a clean move.
            match live_venue {
                Some(MmLive::Rest(live)) => {
                    run_mm_loop(
                        live, qm, inv, positions, ctx, params, tokens, token_market, complement,
                        merger, cond_by_token, venue_by_token, data_api, deposit_wallet,
                        capital, true, start_paused, store_path,
                    )
                    .await;
                }
                Some(MmLive::Ws(split)) => {
                    run_mm_loop(
                        split, qm, inv, positions, ctx, params, tokens, token_market, complement,
                        merger, cond_by_token, venue_by_token, data_api, deposit_wallet,
                        capital, true, start_paused, store_path,
                    )
                    .await;
                }
                None => {
                    // Paper: optionally enable the passive-taker-flow demo aid so
                    // resting quotes actually fill in a calm market (0 = off). The
                    // relayer/conditions are unused here (no_naked_shorts = false →
                    // the paper recycle path), keeping paper byte-for-byte unchanged.
                    let venue = PaperMakerVenue::new(ctx.fetcher.clone())
                        .with_taker_fill_pct(params.paper_taker_fill_pct);
                    run_mm_loop(
                        venue, qm, inv, positions, ctx, params, tokens, token_market, complement,
                        merger, cond_by_token, venue_by_token, data_api, deposit_wallet,
                        capital, false, start_paused, store_path,
                    )
                    .await;
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Pure quote math (no async, no inventory — unit-tested directly)
// ---------------------------------------------------------------------------

/// `Buy`/`Sell` action string for a resting side (store row tag).
fn side_action(side: Side) -> &'static str {
    match side {
        Side::Bid => "Buy",
        Side::Ask => "Sell",
    }
}

/// Mid price in µUSDC/share, or `None` if either side of the book is empty.
fn mid_micro(book: &Book) -> Option<u64> {
    let ts = book.ts();
    let bid = book.bids.best()?;
    let ask = book.asks.best()?;
    Some((bid.microusdc(ts) + ask.microusdc(ts)) / 2)
}

/// Signed mark value of `net` µshares at `price_micro` µUSDC/share, floored
/// toward −∞ (against us on BOTH sides — mirrors [`InventoryRisk::mark`]).
fn signed_value(net: i128, price_micro: u64) -> i128 {
    (net * i128::from(price_micro)).div_euclid(1_000_000)
}

/// Venue minimum order size, µshares: the CLOB rejects any order below 5 shares
/// ("Size (N) lower than the minimum: 5"). The MM skips a quote below this
/// rather than placing it to be rejected.
const MM_MIN_ORDER_SHARES_MICRO: i128 = 5_000_000;

/// M6-7: minimum wall-clock between LIVE on-chain merge sweeps. The relayer
/// submit→`STATE_CONFIRMED` round-trip is multi-second + rate-limited, so the
/// sweep runs at a relaxed cadence well OFF the quote hot path; in-flight pairs
/// are latched so a slow confirm is never re-submitted within the interval.
const MERGE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// R2 (auto-redeem): the resolved-winner redeem sweep fires at most once per this
/// interval — slower than the merge sweep because market resolutions are
/// infrequent. Bounds the Data-API + relayer load of the (non-blocking) sweep.
const REDEEM_SWEEP_INTERVAL: Duration = Duration::from_secs(120);

/// Competing in-band depth FALLBACK for the reward $/day estimator (Task 11,
/// spec §9), reused from main's selection-time constant (`competing_depth =
/// 1.0` when no live book depth is plumbed). The live book's resting in-band
/// depth (excluding our own orders) is NOT threaded into the quote loop here, so
/// the estimator pins the competing term to this constant — which makes the
/// `$/day` figure OPTIMISTIC (our share ≈ `our_depth / (our_depth + 1)`). It is
/// an explicit, documented estimate (spec §9/§17 label the dollar figure
/// approximate); real depth plumbing + live midnight-payout reconciliation are
/// deferred. `q_min` / `balance_ratio` are exact (they need no competing depth).
const REWARD_COMPETING_DEPTH_FALLBACK: f64 = 1.0;

/// One complement leg's reward contribution for the pair-aware hedging estimator
/// (Spec-2 Phase B, Task B3): a token's resting orders scored against ITS OWN
/// adjusted mid + reward band via the pure [`reward_score`](super::reward_score)
/// primitives (never reimplemented). The caller combines a market's two legs
/// into one two-sided `q_min` (bids on one token + asks on its complement → Q1,
/// and vice-versa → Q2).
struct RewardLeg {
    /// Σ `order_score(v, s)·size` over the leg's BID orders.
    q_bids: f64,
    /// Σ over the leg's ASK orders (0 in bid-only hedging).
    q_asks: f64,
    /// In-band USD notional (shares × price) of the leg, for the est-$/day proxy.
    depth_usd: f64,
    /// The market's reward pool $/day (identical for both complement tokens, so
    /// the caller takes it ONCE per market — never summed across the pair).
    daily_rate: f64,
    /// The adjusted mid the leg's orders were scored against (the market's
    /// representative mid for the `q_min` in-band band check).
    adj_mid: f64,
}

/// Build one resting `postOnly` Gtc maker order sized by `notional_micro /
/// price`. `None` when the price/size is non-positive OR below the venue's
/// 5-share minimum.
fn quote_order(
    token: TokenId,
    side: Side,
    price: Px,
    ts: TickSize,
    notional_micro: i128,
) -> Option<MakerOrder> {
    let price_micro = i128::from(price.microusdc(ts));
    if price_micro <= 0 {
        return None;
    }
    // size µshares = notional µUSDC × 1e6 µshare/share ÷ (µUSDC/share).
    let size_micro = notional_micro.saturating_mul(1_000_000) / price_micro;
    // QUANTISE THE SIZE to the venue's order-amount rules (RECON, two live
    // rejections): for a maker order the CLOB enforces (a) price =
    // makerAmount/takerAmount on the tick grid, (b) shares ≤ 2 decimals
    // (multiple of 0.01 share = 10_000 µ), and (c) USDC ≤ 4 decimals (multiple
    // of 100 µ). Rounding size DOWN to `grain = max(10_000, 1e8 / unit)` µshares
    // satisfies all three: 10_000 µ (= 0.01 share) for Cent, 100_000 µ (= 0.1
    // share) for Milli — at any tick `price·size` is then a whole, ≤4-decimal
    // µUSDC and the maker/taker ratio is exactly the tick price.
    let grain = (100_000_000 / i128::from(ts.unit_microusdc())).max(10_000);
    let size_micro = if grain > 0 {
        size_micro - size_micro.rem_euclid(grain)
    } else {
        size_micro
    };
    if size_micro <= 0 {
        return None;
    }
    // VENUE MIN ORDER SIZE (RECON: "Size (N) lower than the minimum: 5"): the
    // CLOB rejects any order below 5 shares. SKIP rather than place-and-be-rejected
    // (the flood we hit): the MM simply won't quote a market where
    // `max_quote_usd / price` can't afford 5 shares. Raise max_quote_usd (and fund
    // accordingly) to cover pricier markets.
    if size_micro < MM_MIN_ORDER_SHARES_MICRO {
        return None;
    }
    Some(MakerOrder {
        token,
        side,
        price,
        size: Qty(size_micro as u64),
        order_type: OrderType::Gtc,
        post_only: true,
    })
}

/// Shift `fair` (µUSDC/share) AGAINST inventory (Task 4.3, spec §7): a long
/// (`net > 0`) LOWERS fair so BOTH quotes drop (less eager to buy more, keener
/// to sell down); a short RAISES it. The magnitude is `skew_bps · 100 · r`
/// µUSDC, where `r = clamp(net / max_inventory_shares, −1, +1)` and
/// `max_inventory_shares` is the per-market cap (`max_inventory_micro` µUSDC)
/// valued at `fair` — the same notional→µshares scaling as [`quote_order`].
///
/// All-integer (µUSDC), no f64 in the money path: `net` is clamped to
/// `±max_shares` so the ratio never exceeds 1, then `shift = skew_micro ·
/// net_clamped / max_shares` truncates toward 0 (a slight UNDER-shift, never an
/// over-shift; symmetric for long/short, matching the codebase's truncating
/// integer money math). At FULL inventory the shift is exactly `skew_bps · 100`
/// µUSDC. A flat book (`net == 0`), a non-positive cap/price, or `skew_bps == 0`
/// returns `fair` unchanged. The skew only moves `fair`; [`compute_quotes`]'s
/// existing tick clamps then keep the quote non-crossing and inside the interior
/// range (a skew that pushes a side out just skips that quote — never a cross).
fn skew_fair(fair: i128, net_micro: i128, max_inventory_micro: i128, skew_bps: u32) -> i128 {
    if skew_bps == 0 || net_micro == 0 || max_inventory_micro <= 0 || fair <= 0 {
        return fair;
    }
    // Per-market cap (µUSDC) valued at `fair` → max inventory in µshares
    // (µUSDC × µshares/share ÷ µUSDC/share), mirroring `quote_order`'s sizing.
    let max_shares = max_inventory_micro.saturating_mul(1_000_000) / fair;
    if max_shares <= 0 {
        return fair;
    }
    // r = clamp(net / max_shares, −1, +1), realized by clamping net to ±max.
    let net_clamped = net_micro.clamp(-max_shares, max_shares);
    // Max shift at full inventory, µUSDC (1 bp = 100 µUSDC/share).
    let skew_micro = i128::from(skew_bps).saturating_mul(100);
    let shift = skew_micro.saturating_mul(net_clamped) / max_shares;
    fair - shift
}

/// Compute the `(bid, ask)` quotes from a book + params, BEFORE the inventory
/// cap check (that gate is applied in the loop). Pure so the quoting math is
/// unit-tested without async.
///
/// `fair = mid` shifted against inventory by [`skew_fair`] (Task 4.3: a long
/// lowers BOTH quotes, a short raises them; `net == 0` or `inventory_skew_bps ==
/// 0` leaves `fair = mid`, i.e. the Task-4.2 symmetric quote). The half-spread
/// (µUSDC) is `spread_bps · 100 / 2` (1 bp = 100 µUSDC/share since $1.00 =
/// 10_000 bps = 1_000_000 µUSDC). The bid rounds DOWN to a tick and the ask
/// rounds UP (maker-favorable / never narrower). Both are then CLAMPED to the
/// live book so a post-only quote never crosses — the bid stays strictly below
/// the best ask, the ask strictly above the best bid (when skew would push a
/// side across, it clamps to the most-aggressive non-crossing price). Finally
/// they are bumped apart to stay strictly non-crossing of each other, and both
/// must be interior ticks `[1, levels−1]` — otherwise the token is skipped
/// (`(None, None)`). So skew can never produce a crossing (venue-rejected) quote.
///
/// `net_micro` is the strategy's current signed net for `token` (µshares) and
/// `max_inventory_micro` the per-market inventory cap (µUSDC) the skew
/// normalizes against — sourced from [`InventoryRisk`] in the loop.
fn compute_quotes(
    book: &Book,
    token: TokenId,
    params: &MmParams,
    notional_micro: i128,
    net_micro: i128,
    max_inventory_micro: i128,
) -> (Option<MakerOrder>, Option<MakerOrder>) {
    let ts = book.ts();
    let (Some(best_bid), Some(best_ask)) = (book.bids.best(), book.asks.best()) else {
        return (None, None);
    };
    let unit = i128::from(ts.unit_microusdc());
    let levels = i128::from(ts.levels());

    // fair = mid skewed against inventory (Task 4.3); half-spread in µUSDC.
    let mid = (i128::from(best_bid.microusdc(ts)) + i128::from(best_ask.microusdc(ts))) / 2;
    let fair = skew_fair(mid, net_micro, max_inventory_micro, params.inventory_skew_bps);
    let half = i128::from(params.spread_bps) * 100 / 2;

    // bid rounds DOWN (floor), ask rounds UP (ceil) — never narrower than asked.
    let mut bid_tick = (fair - half).div_euclid(unit);
    let mut ask_tick = {
        let n = fair + half;
        (n + unit - 1).div_euclid(unit)
    };
    // CLAMP TO THE LIVE BOOK (post-only safety): a maker quote must NOT cross, or
    // the venue rejects it (`INVALID_POST_ONLY_ORDER`) — and a single crossing
    // side aborts the whole reconcile pass. Skew can push a side across the book
    // (a big long skews the ASK below the best bid), so keep the bid STRICTLY
    // below the best ask and the ask STRICTLY above the best bid. When skew would
    // cross, this clamps to the most-aggressive NON-crossing price (the correct
    // offload quote), e.g. a heavy long rests its ask at best_bid+1.
    let best_bid_tick = i128::from(best_bid.get());
    let best_ask_tick = i128::from(best_ask.get());
    bid_tick = bid_tick.min(best_ask_tick - 1);
    ask_tick = ask_tick.max(best_bid_tick + 1);
    // Never cross / collapse onto one tick: keep the ask strictly above the bid.
    if ask_tick <= bid_tick {
        ask_tick = bid_tick + 1;
    }
    // Both must be interior ticks [1, levels-1], else no valid non-crossing quote.
    if bid_tick < 1 || ask_tick > levels - 1 {
        return (None, None);
    }
    let (Ok(bid_px), Ok(ask_px)) = (Px::new(bid_tick as u16, ts), Px::new(ask_tick as u16, ts))
    else {
        return (None, None);
    };
    (
        quote_order(token, Side::Bid, bid_px, ts, notional_micro),
        quote_order(token, Side::Ask, ask_px, ts, notional_micro),
    )
}

/// RewardFarm per-token quote computation (Task 5, spec §8.2): tight,
/// non-crossing, two-sided prices from the live book via [`reward_quote_prices`],
/// converted to venue [`MakerOrder`]s. Returns `(bid, ask)` mirroring
/// [`compute_quotes`] so the loop's veto / no-naked-short / inventory gating is
/// IDENTICAL for both policies — the policies differ ONLY in the prices/sizes.
///
/// BASE size (per side, valued at the adjusted mid) is leaned against signed
/// inventory into a balanced bid/ask via [`skewed_sizes`] (Task 6, spec §8.3):
/// bigger on the REDUCING side, capped at `max_ratio`, both floored at the
/// reward `min_size` (`min_size_shares`) so each side keeps the two-sided bonus.
/// PRICES stay tight (the lean is in SIZES only). `max_spread_cents` is the
/// per-market reward scoring band; `0.0` (no metrics / not reward-eligible)
/// skips BOTH sides.
///
/// Inventory inputs mirror [`compute_quotes`]/[`skew_fair`]: `net_micro` is the
/// strategy's signed net for `token` (µshares) and `max_inventory_micro` the
/// per-market cap (µUSDC) the lean normalizes against (valued at the mid →
/// shares). When a side price is `None` (out of band) OR the inventory cap is
/// hit, the loop's EXISTING no-naked-short / `check_quote` gating quotes only
/// the reducing side — NOT duplicated here; this just emits the two candidates.
///
/// The pure pricing seam works in DOLLARS (0..1); each returned price is already
/// floored/ceiled to the tick grid, so [`reward_price_to_px`] maps it back to an
/// interior [`Px`]. The fair value is the size-weighted [`microprice`] of the top
/// of book with sub-`min_size` levels dropped (spec §8.1, closes Spec-1 deferral
/// I2) via [`reward_fair_value`], NOT the raw mid.
#[allow(clippy::too_many_arguments)]
fn reward_compute_quotes(
    book: &Book,
    token: TokenId,
    notional_micro: i128,
    max_spread_cents: f64,
    net_micro: i128,
    max_inventory_micro: i128,
    max_ratio: f64,
    min_size_shares: f64,
) -> (Option<MakerOrder>, Option<MakerOrder>) {
    let ts = book.ts();
    let (Some(best_bid), Some(best_ask)) = (book.bids.best(), book.asks.best()) else {
        return (None, None);
    };
    let tick = ts.unit_microusdc() as f64 / 1_000_000.0;
    let bb = best_bid.microusdc(ts) as f64 / 1_000_000.0;
    let ba = best_ask.microusdc(ts) as f64 / 1_000_000.0;
    // Size-weighted fair value (spec §8.1, closes I2): microprice of the top of
    // book with sub-`min_size` levels dropped. Both best levels exist (checked
    // above) so this is always `Some`; the raw-mid fallback is unreachable.
    let adj_mid =
        reward_fair_value(book, ts, min_size_shares).unwrap_or_else(|| adjusted_mid(bb, ba));
    let (bid_px, ask_px) = reward_quote_prices(adj_mid, bb, ba, tick, max_spread_cents);

    // BALANCED, INVENTORY-SKEWED SIZES (spec §8.3), all in SHARES so the reward
    // `min_size` floor applies directly:
    //   base       = per-side notional valued at the adj mid (= notional_usd /
    //                mid; quote_order's notional→shares, kept in SHARES).
    //   cap_shares = per-market inventory cap valued at the mid (mirrors
    //                skew_fair's `max_shares`, in SHARES not µshares).
    //   net        = signed µshare inventory / 1e6.
    // Each side's resulting SHARE size is converted back to a per-side notional
    // AT ITS OWN PRICE and handed to quote_order, which re-derives µshares,
    // quantizes to the venue grain, and enforces the 5-share floor — so no
    // amount rounding is hand-rolled here (quote_order owns it for BOTH policies).
    let adj_mid_micro = adj_mid * 1_000_000.0;
    let (base_shares, cap_shares) = if adj_mid_micro > 0.0 {
        (
            notional_micro as f64 / adj_mid_micro,
            max_inventory_micro as f64 / adj_mid_micro,
        )
    } else {
        (0.0, 0.0)
    };
    let net_shares = net_micro as f64 / 1_000_000.0;
    let (bid_shares, ask_shares) =
        skewed_sizes(base_shares, net_shares, cap_shares, max_ratio, min_size_shares);

    // shares → per-side notional (µUSDC) at the side's own price (notional =
    // shares × price_micro), so quote_order's `notional·1e6 / price_micro`
    // recovers the share size before quantizing it to the venue grain.
    let order = |side: Side, price: f64, shares: f64| -> Option<MakerOrder> {
        let px = reward_price_to_px(price, ts)?;
        let notional = (shares * px.microusdc(ts) as f64) as i128;
        quote_order(token, side, px, ts, notional)
    };
    (
        bid_px.and_then(|p| order(Side::Bid, p, bid_shares)),
        ask_px.and_then(|p| order(Side::Ask, p, ask_shares)),
    )
}

/// Reward-farm fair value (spec §8.1, closes Spec-1 deferral I2): the
/// size-weighted [`microprice`] of the top of book with the sub-`min_size`
/// size-cutoff applied. THE single source of fair value for the reward path —
/// quoting ([`reward_compute_quotes`]), the estimator ([`MmLoop::sample_reward_estimate`])
/// and the decision telemetry ([`MmLoop::log_rf_decision`]) all route through it
/// so they price off an IDENTICAL mid.
///
/// SIZE-CUTOFF: a top level resting LESS than `min_size_shares` (below the reward
/// two-sided floor) is a sub-incentive level we don't want defining fair value,
/// so its qty is treated as `0.0` in the microprice weighting. With one side
/// dropped the microprice resolves via the surviving side; with BOTH dropped it
/// falls back to the raw midpoint (`microprice`'s own zero-size fallback).
///
/// `Qty` is µshares (`pm_core::num`: "sizes are micro-shares"), so each resting
/// size is divided by 1e6 to shares — both to compare against `min_size_shares`
/// (shares) and to weight the microprice in that same unit. Returns `None` when
/// the book is one-sided/empty (no mid to anchor on); callers skip the token.
fn reward_fair_value(book: &Book, ts: TickSize, min_size_shares: f64) -> Option<f64> {
    let (best_bid, best_ask) = (book.bids.best()?, book.asks.best()?);
    let bb = best_bid.microusdc(ts) as f64 / 1_000_000.0;
    let ba = best_ask.microusdc(ts) as f64 / 1_000_000.0;
    // µshares → shares, then drop the level (qty 0) if it is below the reward
    // min_incentive_size so a sub-incentive top can't skew the fair value.
    let cutoff = |raw_micro: u64| {
        let shares = raw_micro as f64 / 1_000_000.0;
        if shares < min_size_shares { 0.0 } else { shares }
    };
    let bid_qty = cutoff(book.bids.qty_at(best_bid).0);
    let ask_qty = cutoff(book.asks.qty_at(best_ask).0);
    Some(microprice(bb, ba, bid_qty, ask_qty))
}

/// Map a reward-farm dollar price (already a tick multiple from
/// [`reward_quote_prices`]) to an interior [`Px`], or `None` if it is not a valid
/// interior tick `[1, levels-1]`. The price sits on the tick grid, so the
/// division lands on an integer tick (rounded to absorb f64 error).
fn reward_price_to_px(price: f64, ts: TickSize) -> Option<Px> {
    let tick = ts.unit_microusdc() as f64 / 1_000_000.0;
    if tick <= 0.0 || !price.is_finite() {
        return None;
    }
    let idx = (price / tick).round();
    if !(1.0..f64::from(ts.levels())).contains(&idx) {
        return None;
    }
    Px::new(idx as u16, ts).ok()
}

// ---------------------------------------------------------------------------
// The quote loop
// ---------------------------------------------------------------------------

/// What we recorded when we placed a resting order: enough to resolve a later
/// [`MakerFill`](pm_execution::fills::MakerFill) authoritatively. The WS fill
/// carries no side, no tick size, and a TRADE-level token that can be the
/// complement on a cross-token match — but we PLACED the order, so we know its
/// real token, side, and tick size.
#[derive(Debug, Clone, Copy)]
struct Placed {
    token: TokenId,
    side: Side,
    ts: TickSize,
}

/// The market maker's owned per-strategy state + handles. Generic over the
/// venue so Task 4.5 can drive a live `MakerVenue + UserFillSource` through the
/// SAME loop; `run` builds the concrete [`PaperMakerVenue`].
struct MmLoop<V: MakerVenue + UserFillSource> {
    venue: V,
    qm: QuoteManager,
    inv: InventoryRisk,
    positions: PositionBook,
    fetcher: BookFetcher,
    store_tx: mpsc::Sender<StoreMsg>,
    status_tx: watch::Sender<StrategyStatus>,
    params: MmParams,
    tokens: Vec<TokenId>,
    token_market: HashMap<TokenId, MarketId>,
    /// Spec-2 Phase B (§5.1): yes↔no complement map for the quoted markets,
    /// populated by main ONLY under reward-farm hedging (empty otherwise). The
    /// hedging quote path is BID-ONLY per token (no naked short); this map pairs
    /// a market's two complement bids for the pair-aware estimator/budget (B3),
    /// the delta-neutral sizing + pull mapping (B4), and the set merge (B5).
    complement: HashMap<TokenId, TokenId>,
    /// Quote policy (Task 5): which quote-computation path [`quote`](Self::quote)
    /// takes — [`Policy::SpreadCapture`] (the UNCHANGED spread path) or
    /// [`Policy::RewardFarm`] (tight two-sided reward quoting). Set from
    /// [`MmParams::policy`] in [`run_mm_loop`].
    policy: Policy,
    /// Per-token reward-program params `(min_size_shares, max_spread_cents,
    /// daily_rate_usd)`, resolved ONCE at construction from the registry
    /// [`MarketMetrics`] for each token's market (spec §6). RewardFarm reads
    /// `max_spread_cents` (`.1`) as the scoring band; `min_size` (`.0`) is the
    /// Task-6 sizing floor; `daily_rate_usd` (`.2`) feeds the Task-11 reward
    /// $/day estimator. A token with no metrics / not reward-eligible maps to
    /// zeros, gating its quotes (and estimator contribution) out.
    reward_by_token: HashMap<TokenId, (f64, f64, f64)>,
    /// Per-side notional cap: `min(max_quote_usd, capital)`, µUSDC.
    notional_micro: i128,
    /// `order_id → resting-order metadata`, for fill→side resolution and the
    /// write-ahead order row. Grows as ids churn; old ids are harmless.
    placed: HashMap<OrderId, Placed>,
    /// Last-known tick size per token, learned from the books the quote loop
    /// fetches. Resolves a fill's tick size when its order_id isn't in `placed`
    /// (a paper fill the venue stamped with its side, so it is still bookable).
    token_ts: HashMap<TokenId, TickSize>,
    /// Running maker-rebate ESTIMATE (Task 4.4), µUSDC: `Σ rebate_bps ·
    /// fill_notional / 10_000` over every maker fill. Tracked SEPARATELY and
    /// published in [`StrategyStatus::rebate_micro`] — never added to
    /// cash/equity/realized (an unverified, out-of-band estimate).
    rebate_accrued_micro: i128,
    paused: bool,
    /// Latched once an inventory halt fires — quoting never resumes this session.
    halted: bool,
    /// UTC-day index ([`utc_day_from_ms`]) this loop is currently accounting
    /// against. Set at startup and advanced when [`tick`](Self::tick) detects a
    /// day rollover, which releases [`day_loss_halted`](Self::day_loss_halted).
    day: i64,
    /// PERSISTENT UTC-day loss-cap latch (Task 9), SEPARATE from
    /// [`halted`](Self::halted). Set at startup by
    /// [`arm_day_loss_gate`](Self::arm_day_loss_gate) when the store shows today's
    /// `"mm"` P&L is already at/under the daily-loss cap, so the bot does NOT
    /// re-arm and keep bleeding across the periodic auto-restart. Stops quoting
    /// exactly like `halted`, but is RELEASED on a UTC-day rollover (a fresh day
    /// gets a fresh cap) — whereas `halted` (the in-session `InvHalt` latch) is
    /// deliberately left untouched by the rollover so an inventory halt stays
    /// latched for the whole session.
    day_loss_halted: bool,
    /// Read-only store handle for the PERSISTENT day-loss gate (Task 9 + I3),
    /// opened ONCE at startup from `store_path`. `arm_day_loss_gate` uses it (via
    /// the borrowed arg) to latch at startup, and [`tick`](Self::tick) re-reads it
    /// each cycle so the gate ALSO binds when the cumulative day-realized ledger
    /// (or the worst-point snapshot) crosses the cap MID-session — not only at the
    /// next auto-restart. `None` on paper/test paths with no store, leaving the
    /// per-cycle re-check inert (a fresh run is never halted by default).
    day_loss_read: Option<ReadStore>,
    /// Venue capability: `true` for the live CLOB (NO naked shorts) → an ASK
    /// sells at most the current LONG inventory and is skipped when flat
    /// (bid-only). `false` for the paper venue, which models signed/short fills.
    no_naked_shorts: bool,
    /// Operator-VETOED `(token, side)` quotes (dashboard per-order cancel): each
    /// is cancelled on veto and then EXCLUDED from `desired` every cycle, so the
    /// MM stops re-quoting it until the operator un-vetoes. Published in the
    /// status snapshot so the dashboard can show + un-veto them.
    vetoed: HashSet<(TokenId, Side)>,
    /// RewardFarm ESTIMATOR (Task 11, spec §9): last sample instant. `None`
    /// until the first sample, so the first RewardFarm cycle always samples;
    /// thereafter [`tick`](Self::tick) re-samples once `params.sample_interval`
    /// has elapsed. Untouched under SpreadCapture (the estimator never runs).
    last_reward_sample: Option<Instant>,
    /// Cached reward estimate from the last sample, published by
    /// [`publish_status`](Self::publish_status). `None` until first sampled and
    /// always `None` under SpreadCapture — so the dashboard field is populated
    /// ONLY for RewardFarm.
    reward_status: Option<RewardFarmStatus>,
    /// Session-cumulative sum of the per-sample est $/day (a running estimate
    /// proxy, NOT a realized payout); surfaced as
    /// [`RewardFarmStatus::cumulative_est`].
    reward_cumulative_est: f64,
    /// RewardFarm Phase-A (spec §4) per-token rolling signal state: the windowed
    /// (ts, microprice) history feeding the MOMENTUM half of the adverse-selection
    /// quote-pull signal. Populated/consulted ONLY in the RewardFarm branch of
    /// [`quote`](Self::quote); never touched under SpreadCapture (so that path
    /// stays byte-for-byte unchanged).
    signals: HashMap<TokenId, SignalState>,
    /// RewardFarm Phase-A (spec §4) quote-pull COOLDOWN: `(token, side)` →
    /// wall-clock ms ([`now_ms`]) deadline until which a pulled side stays
    /// omitted, so an easing-then-re-firing signal can't flicker the quote in/out.
    /// Re-armed each time the live signal endangers the side, read each cycle;
    /// RewardFarm-only (SpreadCapture never inserts or reads it).
    pulled_until: HashMap<(TokenId, Side), i64>,
    /// RewardFarm Phase-A (spec §4) TUI surfacing: the strongest-magnitude blended
    /// adverse signal seen on the most recent [`quote`](Self::quote) cycle, folded
    /// into the next [`RewardFarmStatus`] sample so the dashboard's "rew" line
    /// shows the live pull pressure. `0.0` under SpreadCapture (never sampled).
    last_signal: f64,
    /// RewardFarm Phase-A (spec §4) TUI surfacing: whether the most recent quote
    /// cycle PULLED any side. Folded into [`RewardFarmStatus::pulled`] for the
    /// dashboard's "rew" PULL indicator. `false` under SpreadCapture.
    last_pulled: bool,
    /// RewardFarm Phase-B (Task B5) one-shot log latch: set the FIRST time
    /// [`maybe_merge_sets`](Self::maybe_merge_sets) finds a mergeable complete set
    /// on a LIVE venue (`no_naked_shorts`) WITHOUT a relayer configured, where the
    /// on-chain merge stays deferred and the pair is held. Prevents the "complete
    /// set held" warning from spamming every cycle a set sits above the threshold.
    /// Never set on paper, and never set on the relayer-backed live sweep (M6-7).
    merge_live_warned: bool,
    /// M6-7: the LIVE on-chain merge relayer, `Some` ONLY on a reward-farm live
    /// run with the relayer enabled + configured (main builds it; `None` on paper,
    /// arb, and non-relayer live). When present, [`maybe_merge_sets`] runs a
    /// periodic NON-BLOCKING on-chain merge sweep instead of the hold-to-resolution
    /// no-op; `Arc` so each spawned sweep task shares the one client.
    merger: Option<Arc<RelayerClient>>,
    /// M6-7: `token → on-chain conditionId` for the quoted reward-farm universe
    /// (BOTH legs of a market map to its single `conditionId`). Built by main from
    /// the registry `market_condition`; empty off reward-farm-hedging-live, so
    /// non-merge paths never consult it. The sweep needs the conditionId to build
    /// the WALLET `mergePositions` batch.
    cond_by_token: HashMap<TokenId, B256>,
    /// M6-7: outcomes of spawned on-chain merges flow back here. The sweep task
    /// SENDS a [`MergeOutcome`]; [`drain_merge_outcomes`](Self::drain_merge_outcomes)
    /// (top of each cycle) RECEIVES + settles it. Unbounded so a spawned task never
    /// blocks on send; the volume is tiny (one per merged set per sweep).
    merge_tx: mpsc::UnboundedSender<MergeOutcome>,
    merge_rx: mpsc::UnboundedReceiver<MergeOutcome>,
    /// M6-7: ordered complement pairs `(a, b)` with an on-chain merge IN FLIGHT
    /// (spawned, not yet drained). A pair here is skipped by the sweep so a slow
    /// submit→confirm is never double-merged; cleared by `drain_merge_outcomes` on
    /// the outcome (success OR failure).
    merge_inflight: HashSet<(TokenId, TokenId)>,
    /// M6-7: last on-chain merge sweep instant — the sweep fires at most once per
    /// [`MERGE_SWEEP_INTERVAL`] so the slow, rate-limited relayer never stalls the
    /// quote loop. Initialised to "now" at construction (the first sweep waits one
    /// interval).
    last_merge_sweep: Instant,
    /// R1/R2 (auto-redeem): `token → CLOB asset id` for the quoted reward-farm
    /// universe — lets the resolved-position feed match a Data-API
    /// `Position.asset` back to this loop's `TokenId`. Built by main from the
    /// registry; empty off reward-farm-redeem-live. Read by the redeem sweep.
    venue_by_token: HashMap<TokenId, String>,
    /// R1/R2 (auto-redeem): the Polymarket Data-API positions feed, `Some` ONLY on
    /// a relayer-backed reward-farm live run (main builds it; `None` on paper, arb,
    /// non-relayer live). Polled by the redeem sweep for `redeemable` (resolved)
    /// markets. `Arc` so a spawned sweep can share the one client.
    data_api: Option<Arc<pm_ingestion::data_api::DataApiClient>>,
    /// R1/R2 (auto-redeem): the LOWERCASED deposit-wallet address the positions
    /// query is keyed on. `Some` only alongside [`data_api`](Self::data_api);
    /// `None` by default. Used by the redeem sweep's positions query.
    deposit_wallet: Option<String>,
    /// R2: confirmed redeem sweeps flow back here as the set of successfully
    /// redeemed [`RedeemTarget`]s. The spawned sweep SENDS once it finishes;
    /// [`drain_redeem_outcomes`](Self::drain_redeem_outcomes) RECEIVES + settles.
    /// Unbounded so the spawn never blocks; the volume is tiny (resolutions rare).
    redeem_tx: mpsc::UnboundedSender<Vec<RedeemTarget>>,
    redeem_rx: mpsc::UnboundedReceiver<Vec<RedeemTarget>>,
    /// R2: a single redeem sweep IN FLIGHT (spawned, not yet drained). Only one at
    /// a time — resolutions are infrequent, so a per-condition set is unneeded;
    /// cleared when the sweep's outcome is drained.
    redeem_sweep_inflight: bool,
    /// R2: last redeem-sweep instant — the sweep fires at most once per
    /// [`REDEEM_SWEEP_INTERVAL`]. Initialised to "now" (the first sweep waits one
    /// interval).
    last_redeem_sweep: Instant,
}

/// Read BOTH arms of the persistent UTC-day loss gate for `"mm"` on `utc_day`,
/// in µUSDC: the cumulative day-realized LEDGER ([`ReadStore::day_realized_micro`],
/// I3) and the worst-point snapshot P&L ([`ReadStore::day_pnl_micro`], Task 9).
/// A read error on either arm is treated as `0` (logged), so a transient DB
/// error can never trip the gate. The caller latches the day-loss halt when
/// EITHER value is at/under the daily-loss cap:
/// - the LEDGER catches MANY sub-cap *realized* sessions whose losses SUM over
///   the cap across the day — per-session realized resets each restart, so no
///   single snapshot row shows it;
/// - the snapshot MIN catches any single-session cap breach and held/unrealized
///   drawdowns.
///
/// Both scope to the same UTC day, so a day rollover resets both to 0 → released.
fn read_day_loss(read: &ReadStore, utc_day: i64) -> (i128, i128) {
    let realized = read.day_realized_micro("mm", utc_day).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "mm: day-realized ledger read failed — treating as 0");
        0
    });
    let snapshot = read.day_pnl_micro("mm", utc_day).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "mm: day-loss snapshot read failed — treating as 0");
        0
    });
    (realized, snapshot)
}

impl<V: MakerVenue + UserFillSource> MmLoop<V> {
    /// One quote cycle: re-quote (when active), consume fills, mark + safety
    /// stop, publish status.
    async fn tick(&mut self) {
        // Task 9 — persistent UTC-day loss cap ROLLOVER: when the UTC day advances
        // past the tracked day, release the cross-restart day-loss latch so the
        // fresh day may quote again. The in-session `InvHalt` latch (`halted`) is
        // deliberately NOT cleared here (an inventory halt stays latched for the
        // whole session, per the existing behavior).
        let today = utc_day_from_ms(now_ms());
        if today > self.day {
            self.day = today;
            if self.day_loss_halted {
                tracing::warn!(
                    utc_day = today,
                    "mm: UTC day rolled over — releasing persistent day-loss cap latch"
                );
                self.day_loss_halted = false;
            }
        }
        // I3 — per-cycle day-loss RE-LATCH: a fresh process arms the gate at
        // startup, but a long-running session can cross the cap MID-session as the
        // cumulative day-realized ledger accrues (or held inventory marks down).
        // Re-read both arms each cycle (cheap indexed reads on the WAL read conn)
        // so the gate binds WITHIN a session too, not only across auto-restarts.
        // Inert when there is no store handle (paper/tests → `day_loss_read` None).
        // The breach is computed under an immutable borrow that ENDS before the
        // latch is set, satisfying the borrow checker.
        if !self.day_loss_halted {
            let cap_micro = self.inv.config().daily_loss_usd.0;
            let breach = self.day_loss_read.as_ref().map(|read| {
                let (realized, snapshot) = read_day_loss(read, self.day);
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
                    "mm: daily loss cap crossed MID-session (cumulative ledger / worst-point \
                     snapshot) — halting until the UTC day rolls over"
                );
            }
        }
        // M6-7: apply any LIVE on-chain merges that confirmed since the last cycle
        // BEFORE quoting, so this cycle's sizing leans off the post-merge (recycled)
        // inventory. Drained EVERY cycle (even paused/halted) — like consume_fills,
        // settled inventory/accounting must stay correct regardless of quoting. A
        // no-op until a relayer-backed live merge sweep produces outcomes.
        self.drain_merge_outcomes();
        // R2 (auto-redeem): settle confirmed redeems + (rate-limited) kick off the
        // next resolved-winner sweep. Run EVERY cycle regardless of pause/halt —
        // claiming a RESOLVED winner frees locked capital and must not wait on
        // quoting. Both are internally gated to a relayer-backed reward-farm live
        // run (no-op otherwise) and never block the loop (non-blocking spawn/drain).
        self.drain_redeem_outcomes();
        self.sweep_onchain_redeems();
        if !self.paused && !self.halted && !self.day_loss_halted {
            self.quote().await;
        }
        // Fills are consumed even when paused/halted — resting orders may still
        // settle in-flight, and inventory/accounting must stay correct.
        self.consume_fills().await;
        self.mark_and_check().await;
        // Task 11 — REWARD ESTIMATOR sampling (RewardFarm only, spec §9): after
        // quoting/fills settle, recompute the local liquidity-reward estimate on
        // our CURRENT resting quotes, at most once per `sample_interval` (mirrors
        // Polymarket's per-minute sampling). The first cycle always samples
        // (`last_reward_sample` is `None`) so the dashboard shows a figure right
        // away. SpreadCapture never enters here, so its status stays reward-free.
        if self.policy == Policy::RewardFarm {
            let now = Instant::now();
            let due = self
                .last_reward_sample
                .is_none_or(|t| now.duration_since(t) >= self.params.sample_interval);
            if due {
                let mut st = self.sample_reward_estimate().await;
                self.reward_cumulative_est += st.est_reward_usd_day;
                st.cumulative_est = self.reward_cumulative_est;
                // Phase-A TUI surfacing (Task A7): carry the latest cycle's
                // adverse signal + pull state into the published status so the
                // dashboard "rew" line shows the live pull pressure.
                st.signal = self.last_signal;
                st.pulled = self.last_pulled;
                self.reward_status = Some(st);
                self.last_reward_sample = Some(now);
            }
        }
        self.publish_status().await;
    }

    /// Build the desired quote set (inventory-gated) and reconcile it onto the
    /// venue, then record any newly-placed orders (+ write their order rows).
    async fn quote(&mut self) {
        // Task 9 safety net: a latched PERSISTENT day-loss cap stops quoting even
        // for a direct caller (the per-cycle gate in `tick` already skips `quote`).
        // The latch is only ever SET at startup before any quote rests, so there
        // is nothing to cancel here.
        if self.day_loss_halted {
            return;
        }
        // Spec-2 Phase B (Task B5): recycle locked capital by merging any complete
        // YES+NO set back to collateral BEFORE re-quoting, so this cycle's sizing
        // leans off the post-merge (more balanced, less capital-locked) inventory.
        // Cheap and a guaranteed no-op outside RewardFarm+hedging, so SpreadCapture
        // and non-hedging RewardFarm are byte-for-byte unchanged. Paper-only
        // effect; on a live venue it is a logged no-op (NO live merge is called —
        // the on-chain op is deferred to M6, and the gross cap is the control).
        self.maybe_merge_sets().await;
        let tokens = self.tokens.clone();
        // Per-market inventory cap (µUSDC) the skew normalizes against — read
        // once; the skew is otherwise a pure function of the per-token net.
        let max_inventory_micro = self.inv.config().max_inventory_usd.0;
        // Task 8 — STICKY RE-QUOTING (RewardFarm only): snapshot what is resting
        // ONCE up front (reconcile mutates it only at the end of this pass), keyed
        // by `(token, side)`, so the sticky check below can keep an in-band resting
        // quote VERBATIM rather than churn it. A replace is a cancel+place, which
        // resets Polymarket's time-weighted reward score. Empty (never consulted)
        // under SpreadCapture, so that path stays byte-for-byte unchanged.
        let resting: HashMap<(TokenId, Side), MakerOrder> = if self.policy == Policy::RewardFarm {
            self.qm
                .resting_orders()
                .into_iter()
                .map(|(_, o)| ((o.token, o.side), o))
                .collect()
        } else {
            HashMap::new()
        };
        let mut desired: Vec<MakerOrder> = Vec::new();
        // Phase-A TUI surfacing (Task A7): track the strongest-magnitude blended
        // signal + whether any side was pulled THIS cycle, folded into the reward
        // status sample so the dashboard's "rew" line shows the live pull pressure.
        let mut cycle_signal = 0.0_f64;
        let mut cycle_pulled = false;
        for token in tokens {
            // Need a VALID two-sided book; skip the token otherwise.
            let fetched = self.fetcher.fetch(token).await;
            match &fetched {
                None => tracing::debug!(token = token.0, "mm quote: skip — no route / dead supervisor"),
                Some((_, false)) => tracing::debug!(
                    token = token.0,
                    "mm quote: skip — book INVALID (crossed/stale/off-tick)"
                ),
                Some((_, true)) => {}
            }
            let Some((book, true)) = fetched else {
                continue;
            };
            let ts = book.ts();
            // Remember this token's tick size so a later fill resolves even when
            // its order_id never made it into `placed` (a partial-reconcile gap).
            self.token_ts.insert(token, ts);
            // VOLATILITY PULL (Task 4.3, spec §7): a large + FAST mid move makes
            // `vol_hint` fire — PULL this token's quotes for this tick by leaving
            // them OUT of `desired`, so `reconcile` cancels any resting quotes and
            // places none (a pull, NOT a replace — we don't want to be run over
            // during the move). Non-sticky + per-token: `vol_hint` only fires on a
            // fresh large move, so a calmer later tick re-quotes with no cooldown
            // bookkeeping. A pulled token produces no quotes regardless of skew.
            if let Some(mid) = mid_micro(&book)
                && self.inv.vol_hint(token, mid, Instant::now())
            {
                continue;
            }
            // Current signed net for this token: drives SpreadCapture's skew and
            // the no-naked-short ask cap below (the latter for BOTH policies).
            let net_micro = self.inv.net(token);
            // POLICY BRANCH (Task 5): SpreadCapture keeps the EXACT spread-capture
            // quote math (byte-for-byte unchanged); RewardFarm computes tight,
            // non-crossing, two-sided reward-band prices. Both feed the SAME veto /
            // no-naked-short / inventory-gate path below — isolation by quote
            // computation only (spec §5).
            let (bid, ask) = match self.policy {
                Policy::SpreadCapture => compute_quotes(
                    &book,
                    token,
                    &self.params,
                    self.notional_micro,
                    net_micro,
                    max_inventory_micro,
                ),
                Policy::RewardFarm => {
                    // Per-token reward params: `.0` = min_incentive_size (shares,
                    // the two-sided size floor), `.1` = max_spread_cents (scoring
                    // band). No metrics / not reward-eligible → `(0.0, 0.0)` → band
                    // 0.0 gates BOTH sides out (skip). `net_micro` /
                    // `max_inventory_micro` (read above) drive the SIZE lean,
                    // capped by `size_skew_max_ratio` (spec §8.3) — prices stay tight.
                    let (min_size_shares, max_spread_cents, _daily_rate) =
                        self.reward_by_token.get(&token).copied().unwrap_or((0.0, 0.0, 0.0));
                    // Spec-2 Phase B (Task B4): under hedging the quoting UNIT is
                    // the complement PAIR (bid-YES + bid-NO), so the bid SIZE lean
                    // is driven by the PAIR's net delta, NOT this token's net
                    // alone. `pair_net = net(self) − net(complement)` (µshares):
                    // long THIS token vs its complement (>0) SHRINKS this leg's
                    // bid via skewed_sizes' BID return (`base/lean`) and — because
                    // the complement token's pair_net is the exact negation — the
                    // complement leg symmetrically GROWS its bid (`base·lean`) when
                    // it is processed, so the heavy leg bids LESS and the light leg
                    // MORE, rebalancing the pair toward delta-neutral. The
                    // grown:shrunk ratio is `max_ratio^(|pair_net|/cap) ≤
                    // size_skew_max_ratio`. (Equivalent to ONE
                    // `skewed_sizes(base, pair_net = yes−no, …)` call mapping its
                    // returned `(bid, ask)` to `(YES bid, NO bid)`.)
                    // reward_compute_quotes uses the net ONLY for skewed_sizes and
                    // the ask leg it returns is dropped just below, so feeding the
                    // pair delta here re-leans exactly the surviving bid. Non-hedging
                    // keeps this token's own net (bid-vs-ask lean) — byte-for-byte.
                    let sizing_net = if self.params.hedging_enabled {
                        net_micro - self.complement.get(&token).map_or(0, |c| self.inv.net(*c))
                    } else {
                        net_micro
                    };
                    let (bid, ask) = reward_compute_quotes(
                        &book,
                        token,
                        self.notional_micro,
                        max_spread_cents,
                        sizing_net,
                        max_inventory_micro,
                        self.params.size_skew_max_ratio,
                        min_size_shares,
                    );
                    // Spec-2 Phase B (§5.1, closes M3): complement-pair HEDGING is
                    // BID-ONLY per token. A flat MM cannot place an ask (Polymarket
                    // has no naked short), and the ask is the WRONG leg for the pair
                    // — the second side comes from the complement BID on the OTHER
                    // token of the market (the reward formula's `m`/`m'` books). So
                    // DROP the ask leg entirely; the bid still flows through the SAME
                    // pull / veto / no-naked-short / inventory / sticky gating below.
                    // Gated on `hedging_enabled` WITHIN this `RewardFarm` arm, so
                    // non-hedging RewardFarm (ask kept) and SpreadCapture stay
                    // byte-for-byte.
                    let ask = if self.params.hedging_enabled { None } else { ask };
                    (bid, ask)
                }
            };
            // Phase-A ADVERSE-SELECTION QUOTE-PULL (spec §4; RewardFarm ONLY).
            // Step aside from a fill about to be run over: blend book IMBALANCE
            // (summed top-`microprice_levels` depth, +ve = buy pressure) with
            // short-term microprice MOMENTUM into one signed pressure, and PULL
            // the endangered side — strong UP pressure lifts the ASK just before
            // the price rises; strong DOWN hits the BID. A pulled side is also
            // held out for `pull_cooldown_ms` so an easing-then-re-firing signal
            // can't flicker the quote in/out. The whole block is gated on
            // `Policy::RewardFarm`, so SpreadCapture/arb stay byte-for-byte
            // unchanged (and `pull_bid`/`pull_ask` remain `false` for them, the
            // per-side drop below being independently RewardFarm-gated too).
            let mut pull_bid = false;
            let mut pull_ask = false;
            let (mut sig_imb, mut sig_mom, mut sig_blend) = (0.0_f64, 0.0_f64, 0.0_f64);
            if self.policy == Policy::RewardFarm {
                let now = now_ms();
                // `.0` = min_incentive_size (shares); the cutoff fair value uses it.
                let min_size_shares =
                    self.reward_by_token.get(&token).map_or(0.0, |r| r.0);
                // MOMENTUM: feed this cycle's size-weighted fair value (the SAME
                // microprice the quote priced off, spec §8.1) into the token's
                // rolling window, then read its windowed relative change. A
                // one-sided book has no mid (`None`) — but then the quote is
                // already `(None, None)`, so there is nothing to pull.
                if let Some(fair) = reward_fair_value(&book, ts, min_size_shares) {
                    let window = Duration::from_millis(self.params.signal_window_ms);
                    let st = self
                        .signals
                        .entry(token)
                        .or_insert_with(|| SignalState::new(window));
                    st.observe(now, fair);
                    sig_mom = st.momentum(now);
                }
                // IMBALANCE over the summed top-`microprice_levels` depth each side.
                sig_imb = imbalance(
                    ladder_depth(&book.bids, self.params.microprice_levels),
                    ladder_depth(&book.asks, self.params.microprice_levels),
                );
                sig_blend = combined_signal(sig_imb, sig_mom);
                // Spec-2 Phase B (Task B4): the COMPLEMENT leg's blended signal,
                // used ONLY under hedging to make the bid pull PAIR-aware (below).
                // The pair quotes bid-YES + bid-NO, so a leg's bid is endangered
                // when its OWN book runs DOWN *or* its COMPLEMENT book runs UP (the
                // complement rising ⇒ this token = 1 − complement falling ⇒ buying
                // it is adverse). Read off the complement's OWN book (imbalance) +
                // its rolling momentum, mirroring the own-leg signal; a missing /
                // one-sided complement book leaves it 0 (no complement-driven pull).
                // This realizes "the YES book drives the pair, the NO book is the
                // mirror": up-YES pulls the NO bid, down-YES pulls the YES bid —
                // and it still fires when the two real books haven't re-mirrored.
                let mut sig_complement = 0.0_f64;
                if self.params.hedging_enabled
                    && let Some(&comp) = self.complement.get(&token)
                    && let Some((comp_book, true)) = self.fetcher.fetch(comp).await
                {
                    let comp_imb = imbalance(
                        ladder_depth(&comp_book.bids, self.params.microprice_levels),
                        ladder_depth(&comp_book.asks, self.params.microprice_levels),
                    );
                    let comp_mom = self.signals.get(&comp).map_or(0.0, |s| s.momentum(now));
                    sig_complement = combined_signal(comp_imb, comp_mom);
                }
                // Per side: pull when the live signal endangers it OR a prior
                // pull's cooldown is still running. An active pull (re)arms the
                // cooldown deadline; once it lapses AND the signal has eased the
                // side is allowed again.
                let thresh = self.params.pull_threshold;
                let cooldown_ms = self.params.pull_cooldown_ms as i64;
                for side in [Side::Bid, Side::Ask] {
                    // Live signal endangers this side. Under HEDGING only the BID
                    // exists per leg and the quoting unit is the complement PAIR,
                    // so the bid is ALSO pulled when the COMPLEMENT book signals
                    // strong UP (Ask semantics on `sig_complement`): up-complement
                    // ≡ down-self, i.e. up-YES pulls the NO bid and down-YES pulls
                    // the YES bid. Cooldown stays keyed by `(token, Side::Bid)`.
                    let active = should_pull(side, sig_blend, thresh)
                        || (self.params.hedging_enabled
                            && side == Side::Bid
                            && should_pull(Side::Ask, sig_complement, thresh));
                    if active {
                        self.pulled_until.insert((token, side), now + cooldown_ms);
                    }
                    let cooling = self
                        .pulled_until
                        .get(&(token, side))
                        .is_some_and(|&until| now < until);
                    let pulled = active || cooling;
                    match side {
                        Side::Bid => pull_bid = pulled,
                        Side::Ask => pull_ask = pulled,
                    }
                }
            }
            // Phase-A TUI surfacing (Task A7): remember the strongest blended
            // signal + any pull for the "rew" line (inert under SpreadCapture:
            // `sig_blend` stays 0 and the pull flags stay false there).
            if sig_blend.abs() >= cycle_signal.abs() {
                cycle_signal = sig_blend;
            }
            cycle_pulled |= pull_bid || pull_ask;
            if bid.is_none() && ask.is_none() {
                let bb = book.bids.best().map(|p| p.get());
                let ba = book.asks.best().map(|p| p.get());
                tracing::debug!(
                    token = token.0,
                    ?bb,
                    ?ba,
                    "mm quote: compute_quotes produced NO sides (extreme price / no interior room)"
                );
            }
            // Task 10 — RewardFarm DECISION instrumentation (spec §12): log this
            // token's (state, action) as best-effort telemetry for the future
            // Spec-3 tuner. RewardFarm-only (SpreadCapture writes nothing new) and
            // fire-and-forget (a full/closed writer channel drops the row, never
            // stalls the quote loop). Emitted BEFORE the veto / no-naked-short /
            // inventory gating so it records the POLICY's chosen quote; `bid`/`ask`
            // are borrowed (`as_ref`) so the gating loop below still consumes them.
            if self.policy == Policy::RewardFarm {
                self.log_rf_decision(
                    token,
                    &book,
                    net_micro,
                    bid.as_ref(),
                    ask.as_ref(),
                    sig_imb,
                    sig_mom,
                    sig_blend,
                    pull_bid,
                    pull_ask,
                );
            }
            for o in [bid, ask].into_iter().flatten() {
                // Operator VETO (dashboard per-order cancel): this (token, side)
                // was manually cancelled — leave it OUT of `desired` so reconcile
                // never re-places it, until the operator un-vetoes.
                if self.vetoed.contains(&(token, o.side)) {
                    continue;
                }
                // Phase-A ADVERSE-SELECTION QUOTE-PULL (spec §4; RewardFarm ONLY,
                // independently gated): the signal/cooldown block above marked this
                // side endangered — omit it this cycle exactly like the veto path,
                // so `reconcile` cancels any resting quote on that side and places
                // none (a pull, not a replace). `pull_bid`/`pull_ask` are `false`
                // under SpreadCapture, but the policy gate keeps the path provably
                // inert there too.
                if self.policy == Policy::RewardFarm
                    && match o.side {
                        Side::Bid => pull_bid,
                        Side::Ask => pull_ask,
                    }
                {
                    tracing::debug!(
                        token = token.0,
                        side = ?o.side,
                        signal = sig_blend,
                        "mm quote: PULL side (strong adverse signal / cooldown)"
                    );
                    continue;
                }
                // NO NAKED SHORTS on the live CLOB (RECON: an ASK with 0 share
                // balance is rejected "not enough balance"): an ask may sell at
                // most the current LONG inventory. When flat/short, skip the ask
                // and quote BID-ONLY — inventory accrues via fills, then the ask
                // offloads it (the natural MM bootstrap). The bid is USDC-funded,
                // so it is unconstrained here; `check_quote` still caps how much
                // we buy. (True shorting on Polymarket = buying the complement —
                // a future enhancement; this keeps the live MM long-only.)
                let o = if self.no_naked_shorts && o.side == Side::Ask {
                    let long = net_micro.max(0);
                    let grain = (100_000_000 / i128::from(ts.unit_microusdc())).max(10_000);
                    let sellable = (o.size.0 as i128).min(long);
                    let sellable = sellable - sellable.rem_euclid(grain);
                    // Below the venue's 5-share floor → nothing worth offloading.
                    if sellable < MM_MIN_ORDER_SHARES_MICRO {
                        tracing::debug!(token = token.0, net = net_micro as i64, "mm quote: skip ask — long inventory below venue minimum");
                        continue;
                    }
                    MakerOrder { size: Qty(sellable as u64), ..o }
                } else {
                    o
                };
                let signed_qty = match o.side {
                    Side::Bid => o.size.0 as i128,
                    Side::Ask => -(o.size.0 as i128),
                };
                let intent = QuoteIntent {
                    token,
                    signed_qty,
                    price_micro: o.price.microusdc(ts),
                };
                // Inventory cap gate (Task 2.2): only quote sides it approves.
                if matches!(self.inv.check_quote(&intent), QuoteVerdict::Approve) {
                    // STICKY RE-QUOTING (RewardFarm only; the SpreadCapture path is
                    // byte-for-byte unchanged). When a quote for this `(token, side)`
                    // is ALREADY resting, keep the RESTING order VERBATIM so
                    // `reconcile` value-compares it equal (MakerOrder: Eq) and issues
                    // no cancel+place — a replace resets Polymarket's TIME-WEIGHTED
                    // reward score, so we churn only when something material moved.
                    //
                    // KEEP only when BOTH are in tolerance (Spec-2 §8.4b):
                    //   (a) PRICE has NOT drifted past `requote_band_ticks` (Spec-1
                    //       anti-flicker; compared in DOLLARS, matching
                    //       `reward_compute_quotes`), AND
                    //   (b) the resting SIZE has NOT drifted past `size_rebalance_pct`
                    //       from the freshly-computed target size — so an inventory
                    //       shift that re-leans `skewed_sizes` enough is RESTORED
                    //       rather than left stale, without re-leaning every tick.
                    // Either out of tolerance ⇒ replace with the fresh order `o`
                    // (which carries the current price + inventory-skewed size lean).
                    // Sizes compare in SHARES (`Qty` is µshares ÷ 1e6); `o` is the
                    // order we would actually place (post no-naked-short cap), so the
                    // comparison is against the real replacement candidate.
                    let o = if self.policy == Policy::RewardFarm
                        && let Some(r) = resting.get(&(token, o.side))
                        && !needs_requote(
                            r.price.microusdc(ts) as f64 / 1_000_000.0,
                            o.price.microusdc(ts) as f64 / 1_000_000.0,
                            ts.unit_microusdc() as f64 / 1_000_000.0,
                            self.params.requote_band_ticks,
                        )
                        && !needs_requote_size(
                            r.size.0 as f64 / 1_000_000.0,
                            o.size.0 as f64 / 1_000_000.0,
                            self.params.size_rebalance_pct,
                        ) {
                        tracing::debug!(token = token.0, side = ?o.side, resting_tick = r.price.get(), target_tick = o.price.get(), resting_size = r.size.0, target_size = o.size.0, "mm quote: STICKY keep (price + size in band)");
                        r.clone()
                    } else {
                        o
                    };
                    tracing::debug!(token = token.0, side = ?o.side, tick = o.price.get(), "mm quote: DESIRED");
                    desired.push(o);
                } else {
                    tracing::debug!(token = token.0, side = ?o.side, "mm quote: inventory REJECTED side");
                }
            }
        }
        // Phase-A TUI surfacing (Task A7): publish this cycle's signal/pull state
        // onto the loop so the next reward-status sample carries it to the "rew"
        // line. Inert for SpreadCapture (it never samples a reward status).
        self.last_signal = cycle_signal;
        self.last_pulled = cycle_pulled;
        // QuoteManager leaves consistent state on error and the next tick
        // retries (reconnect orchestration is the Task-3.5/4.5 seam). Record
        // placements even on a PARTIAL error: `reconcile` only ever tracks
        // SUCCESSFULLY-placed orders (its on-error contract), so recording them
        // now keeps `placed` — and the write-ahead order rows their fills
        // FK-reference — complete. Otherwise a quote that filled before a later
        // side's place was rejected (e.g. a skewed quote that crossed) would book
        // a fill whose FK-parent order row was never written.
        if let Err(e) = self.qm.reconcile(&mut self.venue, &desired).await {
            // A live venue rejecting an order (postOnly cross, min-size, auth, …)
            // was previously discarded — surface it so a persistent reject (every
            // tick) is visible rather than silently quoting nothing.
            tracing::warn!(error = %e, desired = desired.len(), "mm quote: reconcile failed (venue rejected an order)");
        }
        self.record_placed(&desired).await;
    }

    /// Spec-2 Phase B (Task B5) + M6-7: recycle the capital locked in a hedged
    /// complete YES+NO set by merging it back to collateral. Called once per
    /// [`quote`](Self::quote) cycle (cheap); GATED on `RewardFarm` +
    /// `hedging_enabled`, so SpreadCapture and non-hedging RewardFarm never enter
    /// — their inventory / cash / store rows are untouched (byte-for-byte
    /// unchanged). The mergeable sets are the pure [`mergeable_pairs`] selection
    /// (each unordered complement pair's `min(net(a), net(b))` long depth over the
    /// `merge_threshold_usd` floor); a complete set always redeems to EXACTLY $1.
    ///
    /// VENUE SPLIT — the live/paper discriminator is `no_naked_shorts`:
    /// - PAPER (`no_naked_shorts == false`): model the economics directly via the
    ///   shared [`apply_merge_result`] — reduce both legs by the matched set count
    ///   and credit the recovered $1/set as cash — recycling EACH cycle (the
    ///   aggregate realized feeds the persistent day-loss ledger like a fill).
    /// - LIVE (`no_naked_shorts == true`):
    ///   * WITH a [`RelayerClient`] (M6-7): kick off a RATE-LIMITED, NON-BLOCKING
    ///     on-chain merge sweep ([`sweep_onchain_merges`]) — the multi-second
    ///     submit→confirm runs OFF the quote hot path; the confirmed result is
    ///     applied later by [`drain_merge_outcomes`] via the SAME
    ///     `apply_merge_result`, so live and paper CONVERGE.
    ///   * WITHOUT a relayer (the default / non-relayer live): the on-chain merge
    ///     stays the hold-to-resolution no-op — log ONCE (`merge_live_warned`),
    ///     change NOTHING (the gross inventory cap is the control), spawn nothing.
    async fn maybe_merge_sets(&mut self) {
        // GATE: RewardFarm + hedging only — the only mode that holds BOTH legs of
        // a complement pair. Everything below is then provably inert elsewhere.
        if self.policy != Policy::RewardFarm || !self.params.hedging_enabled {
            return;
        }
        // The B5 selection (pure, unit-tested): each unordered complement pair
        // whose matched complete set clears the per-pair µUSDC floor. The floor is
        // a COARSE gate — `merge_threshold_usd` → µUSDC once — and the money
        // movement below stays in integer µUSDC.
        let threshold_micro = (self.params.merge_threshold_usd * 1_000_000.0) as i128;
        let candidates = mergeable_pairs(&self.complement, &self.inv, threshold_micro);
        if candidates.is_empty() {
            return;
        }
        if self.no_naked_shorts {
            // LIVE: a relayer-backed NON-BLOCKING on-chain sweep (M6-7) when a
            // relayer is configured, else the hold-to-resolution no-op.
            if self.merger.is_some() {
                self.sweep_onchain_merges(&candidates);
            } else if !self.merge_live_warned {
                tracing::warn!(
                    "reward-farm: complete set held (live merge unsupported — no relayer \
                     configured, deferred to M6); gross cap is the control"
                );
                self.merge_live_warned = true;
            }
            return;
        }
        // ── PAPER recycle ──────────────────────────────────────────────────────
        // Apply EXACTLY the shared reduction for each set, then persist the
        // realized delta to the cumulative UTC-day ledger so the PERSISTENT loss
        // cap accounts for merge P&L like a fill (fire-and-forget; a closed/full
        // channel drops it, never blocks).
        for c in candidates {
            let realized_delta = apply_merge_result(
                &mut self.inv,
                &mut self.positions,
                &self.token_market,
                c.a,
                c.b,
                c.amount_micro,
            );
            if realized_delta != 0 {
                let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                    utc_day: utc_day_from_ms(now_ms()),
                    strategy: "mm".into(),
                    delta_micro: realized_delta,
                });
            }
            tracing::debug!(
                market_a = c.a.0,
                market_b = c.b.0,
                matched_micro = c.amount_micro as i64,
                recovered_micro = c.amount_micro as i64,
                realized_delta = realized_delta as i64,
                "mm: merged complete YES+NO set (paper) — recycled locked collateral"
            );
        }
    }

    /// M6-7 LIVE merge sweep — RATE-LIMITED + NON-BLOCKING. For each mergeable
    /// candidate NOT already in flight whose two legs map to the SAME on-chain
    /// `conditionId`, mark the ordered pair in-flight and SPAWN a task that runs
    /// the multi-second relayer `merge` (submit → poll to `STATE_CONFIRMED`) OFF
    /// the quote hot path, sending the typed result back to
    /// [`drain_merge_outcomes`]. At most ONE sweep per [`MERGE_SWEEP_INTERVAL`]
    /// (the on-chain op is slow + rate-limited), so the loop is never stalled and
    /// a pair is never double-merged (the in-flight latch). Only ever reached on a
    /// LIVE venue with a configured relayer (gated in [`maybe_merge_sets`]).
    fn sweep_onchain_merges(&mut self, candidates: &[MergeCandidate]) {
        // The relayer is `Arc`-shared into each spawned task (cheap clone).
        let Some(merger) = self.merger.clone() else {
            return;
        };
        // Rate-limit the WHOLE sweep (not per pair): the submit→confirm round-trip
        // is seconds, so a relaxed cadence is plenty and bounds relayer load.
        let now = Instant::now();
        if now.duration_since(self.last_merge_sweep) < MERGE_SWEEP_INTERVAL {
            return;
        }
        self.last_merge_sweep = now;
        for &MergeCandidate { a, b, amount_micro } in candidates {
            let pair = (a, b);
            // Never double-merge an in-flight pair (a prior sweep's task is still
            // submitting/confirming) — its drain clears the latch on completion.
            if self.merge_inflight.contains(&pair) {
                continue;
            }
            // Both legs of a market share ONE conditionId; require both present +
            // equal (defensive — main builds the map from the one market).
            let (Some(&cond_a), Some(&cond_b)) =
                (self.cond_by_token.get(&a), self.cond_by_token.get(&b))
            else {
                continue;
            };
            if cond_a != cond_b {
                tracing::warn!(
                    token_a = a.0,
                    token_b = b.0,
                    "mm: complement legs map to DIFFERENT conditionIds — skipping merge (misconfig)"
                );
                continue;
            }
            self.merge_inflight.insert(pair);
            let merger = merger.clone();
            let tx = self.merge_tx.clone();
            // µshares → CTF base units (6-decimal, 1:1 with µUSDC). `amount_micro`
            // is `> threshold_micro ≥ 0` by construction, so the cast is exact.
            let amount = U256::from(amount_micro.max(0) as u128);
            tracing::info!(
                token_a = a.0,
                token_b = b.0,
                amount_micro = amount_micro as i64,
                "mm: spawning LIVE on-chain merge of complete set (non-blocking)"
            );
            tokio::spawn(async move {
                let result = merger.merge(cond_a, amount).await;
                // The receiver lives for the loop's lifetime; a send error only
                // means the loop is shutting down — drop the outcome quietly.
                let _ = tx.send(MergeOutcome { a, b, amount_micro, result });
            });
        }
    }

    /// M6-7: DRAIN every on-chain merge that confirmed (or failed) since the last
    /// cycle and settle it — called once per loop cycle (even when paused, like
    /// [`consume_fills`](Self::consume_fills), so settled inventory/accounting
    /// stays correct). On SUCCESS, apply EXACTLY the paper reduction via the
    /// shared [`apply_merge_result`] (recycle the matched set + credit the
    /// recovered µUSDC) and persist the realized delta to the day-loss ledger; on
    /// a relayer ERROR, log + leave inventory untouched (the set is retried next
    /// sweep). EITHER WAY the in-flight latch on the pair is CLEARED so the pair
    /// can be re-swept. Non-blocking (`try_recv`); never panics.
    fn drain_merge_outcomes(&mut self) {
        while let Ok(MergeOutcome { a, b, amount_micro, result }) = self.merge_rx.try_recv() {
            // Clear the in-flight latch on BOTH success and failure so the pair is
            // eligible again (re-swept next cycle on failure; flat on success).
            self.merge_inflight.remove(&(a, b));
            match result {
                Ok(recovered_micro) => {
                    // The relayer returns the recovered µUSDC; for a $1 complete
                    // set it equals the merged amount. Reduce by the µshares
                    // actually merged on-chain (`amount_micro`) — the shared apply.
                    let realized_delta = apply_merge_result(
                        &mut self.inv,
                        &mut self.positions,
                        &self.token_market,
                        a,
                        b,
                        amount_micro,
                    );
                    if realized_delta != 0 {
                        let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                            utc_day: utc_day_from_ms(now_ms()),
                            strategy: "mm".into(),
                            delta_micro: realized_delta,
                        });
                    }
                    tracing::info!(
                        token_a = a.0,
                        token_b = b.0,
                        amount_micro = amount_micro as i64,
                        recovered_micro = recovered_micro as i64,
                        realized_delta = realized_delta as i64,
                        "mm: LIVE on-chain merge CONFIRMED — recycled locked collateral"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        token_a = a.0,
                        token_b = b.0,
                        amount_micro = amount_micro as i64,
                        error = %e,
                        "mm: LIVE on-chain merge FAILED — inventory unchanged, retry next sweep"
                    );
                }
            }
        }
    }

    /// R2 LIVE redeem sweep — RATE-LIMITED + NON-BLOCKING. Claims RESOLVED winners:
    /// fetches the deposit wallet's positions (Data API), selects the RESOLVED
    /// conditions still held ([`redeem_targets`]), and SPAWNS the multi-second
    /// relayer `redeem` for each OFF the quote hot path, sending the confirmed
    /// targets back to [`drain_redeem_outcomes`](Self::drain_redeem_outcomes). At
    /// most ONE sweep in flight (`redeem_sweep_inflight`) and one per
    /// [`REDEEM_SWEEP_INTERVAL`], so the loop is never stalled. Gated to a
    /// relayer-backed reward-farm live run (merger + data_api + deposit_wallet all
    /// present); a no-op otherwise. The resolved market gets NO new fills, so the
    /// net SNAPSHOT below is the redeem amount (the drain re-clamps to current net).
    fn sweep_onchain_redeems(&mut self) {
        let (Some(merger), Some(data_api), Some(wallet)) = (
            self.merger.clone(),
            self.data_api.clone(),
            self.deposit_wallet.clone(),
        ) else {
            return;
        };
        if self.redeem_sweep_inflight {
            return;
        }
        let now = Instant::now();
        if now.duration_since(self.last_redeem_sweep) < REDEEM_SWEEP_INTERVAL {
            return;
        }
        self.last_redeem_sweep = now;
        // Snapshot current nets for the quoted-condition tokens to DECIDE what to
        // redeem (the spawned task has no inventory access). The drain applies at
        // the CURRENT net, so a stale snapshot can never over-reduce.
        let net_snapshot: HashMap<TokenId, i128> = self
            .cond_by_token
            .keys()
            .map(|&t| (t, self.inv.net(t)))
            .collect();
        let cond_by_token = self.cond_by_token.clone();
        let venue_by_token = self.venue_by_token.clone();
        let tx = self.redeem_tx.clone();
        self.redeem_sweep_inflight = true;
        tokio::spawn(async move {
            // A fetch failure sends an empty set — the drain just clears the
            // in-flight latch so the next interval retries; never panics.
            let positions = match data_api.positions(&wallet, 0.0).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "mm: redeem sweep — positions fetch failed; retry next sweep");
                    let _ = tx.send(Vec::new());
                    return;
                }
            };
            let targets = redeem_targets(&positions, &cond_by_token, &venue_by_token, |t| {
                net_snapshot.get(&t).copied().unwrap_or(0)
            });
            let mut done = Vec::new();
            for target in targets {
                match merger.redeem(target.condition_id).await {
                    Ok(_) => {
                        tracing::info!(
                            condition = %target.condition_id,
                            legs = target.legs.len(),
                            "mm: LIVE on-chain redeem CONFIRMED — resolved winner claimed"
                        );
                        done.push(target);
                    }
                    Err(e) => tracing::warn!(
                        condition = %target.condition_id,
                        error = %e,
                        "mm: redeem FAILED — retry next sweep"
                    ),
                }
            }
            let _ = tx.send(done);
        });
    }

    /// R2: DRAIN every redeem sweep that finished since the last cycle — called
    /// once per loop cycle (even when paused, like [`drain_merge_outcomes`]). For
    /// each confirmed target, REBUILD its legs at the CURRENT net (clamped ≥ 0) —
    /// the on-chain `redeemPositions` cleared the whole holding, so flatten exactly
    /// what we hold now at the same resolved prices — and settle via the shared
    /// [`apply_redeem`] (credit the recovered µUSDC, release basis), persisting the
    /// realized delta to the day-loss ledger like a fill. Receiving ANY message
    /// (incl. an empty set on fetch failure / no targets) clears the in-flight
    /// latch so the next interval can sweep. Non-blocking (`try_recv`); never panics.
    fn drain_redeem_outcomes(&mut self) {
        while let Ok(done) = self.redeem_rx.try_recv() {
            self.redeem_sweep_inflight = false;
            for target in done {
                let legs: Vec<RedeemLeg> = target
                    .legs
                    .iter()
                    .filter_map(|l| {
                        let net = self.inv.net(l.token).max(0);
                        (net > 0).then_some(RedeemLeg {
                            token: l.token,
                            resolved_price: l.resolved_price,
                            net_micro: net,
                        })
                    })
                    .collect();
                if legs.is_empty() {
                    continue;
                }
                let rebuilt = RedeemTarget {
                    condition_id: target.condition_id,
                    legs,
                };
                let realized_delta =
                    apply_redeem(&mut self.inv, &mut self.positions, &self.token_market, &rebuilt);
                if realized_delta != 0 {
                    let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                        utc_day: utc_day_from_ms(now_ms()),
                        strategy: "mm".into(),
                        delta_micro: realized_delta,
                    });
                }
                tracing::info!(
                    condition = %rebuilt.condition_id,
                    realized_delta = realized_delta as i64,
                    "mm: redeem settled — resolved position flattened + collateral credited"
                );
            }
        }
    }

    /// Record every newly-tracked resting order into `placed` (so a later fill
    /// always resolves its side + tick size) and emit its write-ahead order row
    /// (so the FK-referencing fill rows persist). Idempotent per id.
    ///
    /// Side comes from the `(token, side)` tracking KEY and the tick size from
    /// the per-token `token_ts`, so EVERY tracked id is recorded — not only those
    /// in the current `desired` set. Only the order ROW needs the price/size
    /// detail the desired quote carries, so it is emitted when the order is
    /// present in `desired` (the common case).
    async fn record_placed(&mut self, desired: &[MakerOrder]) {
        for ((token, side), id) in self.qm.tracked() {
            if self.placed.contains_key(&id) {
                continue;
            }
            let ts = self
                .token_ts
                .get(&token)
                .copied()
                .unwrap_or(TickSize::Cent);
            self.placed.insert(id.clone(), Placed { token, side, ts });
            if let Some(order) = desired.iter().find(|o| o.token == token && o.side == side) {
                let row = OrderRow {
                    id: id.0.clone(),
                    ts_ms: now_ms(),
                    fingerprint: id.0.clone(),
                    token: token.0 as i64,
                    action: side_action(side).into(),
                    limit_ticks: i64::from(order.price.get()),
                    tick_levels: i64::from(ts.levels()),
                    qty_micro: order.size.0 as i64,
                    strategy: "mm".into(),
                };
                let _ = self.store_tx.send(StoreMsg::OrderInsert(row, None)).await;
            }
        }
    }

    /// Emit a best-effort RewardFarm DECISION row (Task 10, spec §12): the state
    /// features (adjusted mid, best bid/ask, signed inventory net, reward band)
    /// and the action taken (the chosen bid/ask price + size in shares, or skip).
    /// Fire-and-forget via `try_send` — a full/closed writer channel DROPS the
    /// record rather than ever blocking the quote loop (telemetry is never a
    /// trading-path failure). Keyed by the token id: Spec 1 quotes a single token
    /// two-sided (spec §9), so the token IS the quoting unit, and using it as the
    /// `market` key gives the tightest decision↔outcome correlation (no YES/NO
    /// cross-attribution). The JSON is opaque to the store; only the future Spec-3
    /// tuner reads it.
    #[allow(clippy::too_many_arguments)]
    fn log_rf_decision(
        &self,
        token: TokenId,
        book: &Book,
        net_micro: i128,
        bid: Option<&MakerOrder>,
        ask: Option<&MakerOrder>,
        imb: f64,
        mom: f64,
        signal: f64,
        pull_bid: bool,
        pull_ask: bool,
    ) {
        let ts = book.ts();
        let bb = book.bids.best().map(|p| p.microusdc(ts) as f64 / 1_000_000.0);
        let ba = book.asks.best().map(|p| p.microusdc(ts) as f64 / 1_000_000.0);
        // `.0` = min_incentive_size (shares), `.1` = the per-market reward scoring
        // band (max_spread_cents); `(0.0, 0.0)` when not reward-eligible / no metrics.
        let (min_size_shares, max_spread_cents) =
            self.reward_by_token.get(&token).map_or((0.0, 0.0), |r| (r.0, r.1));
        // Log the SAME size-weighted fair value (spec §8.1) that drove the quote,
        // so the recorded state matches the decision the future Spec-3 tuner reads.
        let adj_mid = reward_fair_value(book, ts, min_size_shares);
        let side = |o: Option<&MakerOrder>| {
            o.map(|o| {
                serde_json::json!({
                    "px": o.price.microusdc(ts) as f64 / 1_000_000.0,
                    "size_shares": o.size.0 as f64 / 1_000_000.0,
                })
            })
        };
        let state = serde_json::json!({
            "adj_mid": adj_mid,
            "best_bid": bb,
            "best_ask": ba,
            "inv_net_micro": net_micro as i64,
            "max_spread_cents": max_spread_cents,
            // Phase-A adverse-selection signal (spec §4): the blended pull
            // pressure and its two components, so the Spec-3 tuner can learn the
            // pull threshold from the same features the loop decided on.
            "imbalance": imb,
            "momentum": mom,
            "signal": signal,
        });
        let action = serde_json::json!({
            "bid": side(bid),
            "ask": side(ask),
            "skip": bid.is_none() && ask.is_none(),
            // Phase-A: whether each side was PULLED this cycle (adverse signal or
            // an active cooldown) — recorded even though the pulled side is still
            // shown above as the policy's chosen quote (pre-gating).
            "pull_bid": pull_bid,
            "pull_ask": pull_ask,
        });
        let row = RfDecisionRow {
            ts_ms: now_ms(),
            market: token.0.to_string(),
            state_json: state.to_string(),
            action_json: action.to_string(),
        };
        let _ = self.store_tx.try_send(StoreMsg::RfDecision(row));
    }

    /// Poll the venue for fills and book each into inventory + positions + the
    /// store (`"mm"`-tagged, via the SIGNED store route so sell-to-open SHORTS
    /// persist). Makers pay 0 fee on CLOB V2.
    async fn consume_fills(&mut self) {
        let fills = match self.venue.poll().await {
            Ok(f) => f,
            Err(_) => return,
        };
        for f in fills {
            // Resolve the resting SIDE: prefer the side the venue stamped on the
            // fill (the PAPER sim knows it authoritatively), else the side we
            // recorded when we PLACED the order (the LIVE path, where the fill
            // carries none — the MM placed those orders, so `placed` is reliable).
            // Only a fill with no side AND no `placed` entry is unattributable.
            let placed = self.placed.get(&f.order_id).copied();
            let Some(side) = f.side.or_else(|| placed.map(|m| m.side)) else {
                warn!(order_id = %f.order_id.0, "mm: fill for an unknown resting order; skipping");
                continue;
            };
            // AUTHORITATIVE TOKEN: the user-WS stamps a fill with the TRADE's
            // top-level asset_id, which for a COMPLEMENTARY (cross-token) match is
            // NOT our maker order's token — so booking `f.token` mis-attributes the
            // fill (e.g. a YES sell booked as a NO short, corrupting inventory and
            // P&L). We PLACED the order, so its token from `placed` is the truth;
            // fall back to the WS token only for an untracked order (a foreign
            // maker the owner-filter should already have dropped).
            let token = placed.map(|m| m.token).unwrap_or(f.token);
            // Tick size: the recorded one if present, else this token's
            // last-known ts from the quote loop (all of a token's orders share
            // its book's tick size) — so a venue-sided fill books even when its
            // order_id never reached `placed`.
            let ts = placed
                .map(|m| m.ts)
                .or_else(|| self.token_ts.get(&token).copied())
                .unwrap_or(TickSize::Cent);
            // Keep the QuoteManager in sync with the venue's resting set: a fill
            // the venue applied (and a full fill it removed) must be reflected, so
            // reconcile re-quotes a filled market instead of no-oping forever.
            self.qm.note_fill(&f.order_id, f.qty);
            let px_micro = f.px.microusdc(ts);
            let (signed_qty, cash) = match side {
                // bid → +qty, cash = −buy_cost; ask → −qty, cash = +sell_proceeds.
                Side::Bid => (f.qty.0 as i128, Usdc(-buy_cost(px_micro, f.qty).0)),
                Side::Ask => (-(f.qty.0 as i128), sell_proceeds(px_micro, f.qty)),
            };
            // Authoritative signed inventory + realized/unrealized.
            let basis_before = self.inv.basis(token).0;
            // Task 10 — RewardFarm outcome telemetry reads the realized-P&L delta
            // this fill books (negative ⇒ adverse selection); cheap getters, only
            // consumed in the RewardFarm-gated outcome write below.
            let realized_before = self.inv.realized(token).0;
            self.inv.on_fill(token, signed_qty, cash);
            let basis_after = self.inv.basis(token).0;
            let realized_after = self.inv.realized(token).0;
            // The realized-P&L delta this fill BOOKED (signed µUSDC; negative ⇒
            // adverse selection ran us over). This is the authoritative per-fill
            // realized figure `InventoryRisk` already computes — NOT re-derived
            // from cost basis — and feeds BOTH the cumulative day-realized ledger
            // (I3, below) and the RewardFarm outcome telemetry's `adverse_pnl`.
            let realized_delta = realized_after - realized_before;
            // Mirror into the reporting PositionBook in lock-step: the cost-basis
            // delta tracks inventory exactly, and `qty` (the filled volume) keeps
            // the token present in `pnl` even for shorts (value comes from the
            // signed marks we supply in `publish_status`, not from `qty`).
            let cost_delta = Usdc(basis_after - basis_before);
            self.positions
                .apply(&[(token, f.qty, cost_delta)], cash, &self.token_market);
            // REBATE ACCRUAL (Task 4.4): makers EARN an estimated rebate on the
            // filled NOTIONAL. fill_notional = price · qty (µUSDC): price_micro
            // µUSDC/share × qty µshares ÷ 1e6 µshares/share (side-agnostic, so we
            // recompute it here rather than reuse the signed `cash`). Accrue
            // `rebate_bps · notional / 10_000` into the running estimate. Kept
            // SEPARATE — never added to cash/equity/realized (it is an unverified,
            // out-of-band estimate; folding it would inflate position P&L).
            let fill_notional_micro =
                i128::from(px_micro) * i128::from(f.qty.0) / 1_000_000;
            let fill_rebate_micro =
                i128::from(self.params.rebate_bps) * fill_notional_micro / 10_000;
            self.rebate_accrued_micro += fill_rebate_micro;
            let row = FillRow {
                order_id: f.order_id.0.clone(),
                ts_ms: now_ms(),
                token: token.0 as i64,
                action: side_action(side).into(),
                px_ticks: i64::from(f.px.get()),
                tick_levels: i64::from(ts.levels()),
                qty_micro: f.qty.0 as i64,
                cash_micro: usdc_to_i64(cash).unwrap_or(0),
                fee_micro: 0,
                strategy: "mm".into(),
            };
            // SIGNED route: an ask-fill opens a SHORT (no long holdings), which
            // the strict `Fill` path would Oversell-drop — `FillSigned` persists it.
            let _ = self.store_tx.send(StoreMsg::FillSigned(row, None)).await;
            // I3 — CUMULATIVE DAY-REALIZED LEDGER: persist this fill's realized
            // delta into the running UTC-day total so the PERSISTENT loss cap also
            // catches MANY sub-cap *realized* sessions whose losses SUM over the
            // cap across a day — the gap the per-session snapshot-MIN gate
            // (`day_pnl_micro`) cannot see (each session's realized resets to 0).
            // Applies to the MM REGARDLESS of policy (the cap protects the MM, not
            // just RewardFarm); arb is a SEPARATE strategy tag and is untouched.
            // Only a NON-ZERO delta matters (a short-open / pure accrual realizes
            // nothing). Fire-and-forget: `try_send` DROPS on a full/closed channel
            // so this safety telemetry never blocks the quote loop.
            if realized_delta != 0 {
                let _ = self.store_tx.try_send(StoreMsg::DayRealized {
                    utc_day: utc_day_from_ms(now_ms()),
                    strategy: "mm".into(),
                    delta_micro: realized_delta,
                });
            }
            // Task 10 — RewardFarm OUTCOME instrumentation (spec §12): the realized
            // components of the reward signal for this fill. RewardFarm-only and
            // fire-and-forget (`try_send` drops on a full/closed channel, never
            // blocks). The writer correlates it to the most-recent decision for this
            // token (`record_rf_outcome_for_latest`). Only the cheap components are
            // filled: `rebate` (the per-fill maker-rebate estimate) and `adverse_pnl`
            // (this fill's realized-P&L delta — negative when we were run over);
            // `reward_score_delta` / `inv_penalty` need the §9 estimator + marks and
            // are deferred (0 for now).
            if self.policy == Policy::RewardFarm {
                let outcome = RfOutcomeRow {
                    market: token.0.to_string(),
                    ts_ms: now_ms(),
                    reward_score_delta_micro: 0,
                    rebate_micro: i64::try_from(fill_rebate_micro).unwrap_or(0),
                    adverse_pnl_micro: i64::try_from(realized_delta).unwrap_or(0),
                    inv_penalty_micro: 0,
                };
                let _ = self.store_tx.try_send(StoreMsg::RfOutcome(outcome));
            }
        }
    }

    /// Arm the PERSISTENT UTC-day loss-cap latch at startup (Task 9 + I3). Records
    /// the current UTC day and reads BOTH gate arms for that day from the read
    /// store (see [`read_day_loss`]); if EITHER the cumulative day-realized LEDGER
    /// (I3 — summed realized) OR the worst-point snapshot `realized + unrealized`
    /// (held/unrealized) is already at/under the daily-loss cap
    /// (`InventoryConfig::daily_loss_usd`, µUSDC — the SAME floor the in-session
    /// `InvHalt::DailyLoss` keys off, inclusive boundary), latch
    /// [`day_loss_halted`](Self::day_loss_halted) so the bot refuses to quote
    /// until the day rolls over. This is what makes the daily loss cap BIND across
    /// the periodic auto-restart instead of resetting every session — the LEDGER
    /// arm specifically closes the summed-sub-cap-realized gap the snapshot MIN
    /// alone cannot see (many sub-cap sessions whose realized losses SUM over it).
    ///
    /// No read handle (the DB does not exist yet / failed to open), a read error,
    /// or no data today → both arms read `0`, so a fresh run is NEVER halted by
    /// default. Called ONCE, before the loop starts ticking, so no quote is
    /// resting yet (nothing to cancel); [`tick`](Self::tick) then re-checks the
    /// same two arms each cycle so a MID-session crossing also latches. SEPARATE
    /// from `halted`: the rollover can release this latch without disturbing an
    /// inventory halt.
    fn arm_day_loss_gate(&mut self, read: Option<&ReadStore>, now_ms: i64) {
        let today = utc_day_from_ms(now_ms);
        self.day = today;
        let cap_micro = self.inv.config().daily_loss_usd.0;
        let Some(read) = read else { return };
        let (realized, snapshot) = read_day_loss(read, today);
        if realized <= -cap_micro || snapshot <= -cap_micro {
            self.day_loss_halted = true;
            tracing::warn!(
                utc_day = today,
                day_realized_micro = realized as i64,
                day_pnl_micro = snapshot as i64,
                daily_loss_cap_micro = cap_micro as i64,
                "mm: daily loss cap ALREADY hit for the UTC day (persisted across restart; \
                 summed-realized ledger or worst-point snapshot) — refusing to quote until \
                 the day rolls over"
            );
        }
    }

    /// Mark held inventory at the mid feed and latch the safety stop: an
    /// inventory halt cancels all quotes and stops quoting (latched).
    async fn mark_and_check(&mut self) {
        if self.halted {
            return; // already latched; quotes already cancelled.
        }
        let tokens = self.tokens.clone();
        let mut marks: Marks = HashMap::new();
        for token in tokens {
            if self.inv.net(token) == 0 {
                continue;
            }
            // Omitting an unmarkable held token makes `mark` withhold the latch
            // that cycle (per its Marks contract) — a transient gap won't halt.
            if let Some((book, true)) = self.fetcher.fetch(token).await
                && let Some(mid) = mid_micro(&book)
            {
                marks.insert(token, mid);
            }
        }
        // VOLATILITY PULL (Task 4.3) lives in `quote()`, not here: the pull must
        // exclude a token from THIS tick's desired set (so `reconcile` cancels
        // without replacing), which is built there. `mark` only owns the latched
        // safety stop below.
        let _ = self.inv.mark(&marks);
        if self.inv.halted().is_some() {
            self.halted = true;
            self.cancel_all().await;
        }
    }

    /// Compute the local liquidity-reward ESTIMATE (Task 11, spec §9) on our
    /// CURRENT resting reward quotes, REUSING the pure
    /// [`reward_score`](super::reward_score) scoring (never reimplemented here).
    ///
    /// Per quoted token (grouped from [`QuoteManager::resting_orders`]): fetch
    /// the book for the adjusted mid, build a [`ScoredOrder`] per resting side
    /// (`spread_cents = |price − adj_mid|·100`, `size` in shares), then
    /// - `q_min` via [`quote_set_q_min`] (spec §9 two-sided minimum);
    /// - `Q1`/`Q2` via [`order_score`] for the `balance_ratio = min/max`;
    /// - the rough $/day via [`est_daily_reward_usd`] using the per-token
    ///   `daily_rate` and our in-band depth (USD notional of in-band sides),
    ///   with the competing term pinned to [`REWARD_COMPETING_DEPTH_FALLBACK`]
    ///   (no live book depth is plumbed — a documented, optimistic estimate).
    ///
    /// Contributions are AGGREGATED across quoted tokens (Spec 1 typically
    /// quotes a single token two-sided): `q_min`/est sum, and `balance_ratio`
    /// is taken from the summed `Q1`/`Q2`. `cumulative_est` is filled by the
    /// caller.
    ///
    /// HEDGING (Spec-2 Phase B, Task B3): when `hedging_enabled`, a market's
    /// reward score is two-sided ACROSS the complement pair — the YES bid scores
    /// on book `m` (Q1), the NO bid on book `m'` (Q2). The resting orders are
    /// grouped by MARKET via the `complement` map and each market is scored ONCE
    /// (its two legs paired into one `q_min` via [`Self::score_reward_leg`]), so
    /// a bid-only token is NOT penalized as single-sided. The non-hedging path
    /// is unchanged (each token scores against its own mid, two-sided per token).
    ///
    /// Takes `&mut self` only to match the loop's other async methods
    /// (a shared `&self` across `.await` is not `Send` for the live venue); it
    /// mutates no trading state — it just reads the resting set + books.
    async fn sample_reward_estimate(&mut self) -> RewardFarmStatus {
        // Group our resting quotes by token so each token scores against its own
        // adjusted mid + reward band.
        let mut by_token: HashMap<TokenId, Vec<MakerOrder>> = HashMap::new();
        for (_, o) in self.qm.resting_orders() {
            by_token.entry(o.token).or_default().push(o);
        }

        let mut q1_total = 0.0_f64;
        let mut q2_total = 0.0_f64;
        let mut q_min_total = 0.0_f64;
        let mut est_total = 0.0_f64;

        if self.params.hedging_enabled {
            // PAIR-AWARE (Spec-2 Phase B, Task B3): under hedging the MM bids
            // YES + bids NO, and the reward score is two-sided ACROSS the
            // complement pair — the YES bid scores on book `m` (Q1), the NO bid
            // on book `m'` (Q2). Group the resting orders by MARKET via the
            // `complement` map and score each market ONCE, so a bid-only token is
            // PAIRED into a two-sided market score rather than penalized as
            // single-sided (the 1/C floor) per token. `reward_score::q_min` is
            // symmetric in (Q1, Q2), so the market score is independent of which
            // complement we start from; the `processed` set counts each market
            // exactly once (no double-counting) and the shared reward pool /
            // daily rate is taken ONCE per market (never summed across the pair).
            let mut processed: HashSet<TokenId> = HashSet::new();
            let heads: Vec<TokenId> = by_token.keys().copied().collect();
            for token in heads {
                if !processed.insert(token) {
                    continue; // this market was already scored via its complement
                }
                let complement = self.complement.get(&token).copied();
                if let Some(c) = complement {
                    processed.insert(c);
                }
                // Own + complement resting orders, cloned so neither immutable
                // borrow of `by_token` is held across the two async leg scorings.
                let my_orders = by_token.get(&token).cloned().unwrap_or_default();
                let comp_orders = complement.and_then(|c| by_token.get(&c).cloned());
                let leg = self.score_reward_leg(token, &my_orders).await;
                let comp_leg = match (complement, comp_orders) {
                    (Some(c), Some(orders)) => self.score_reward_leg(c, &orders).await,
                    _ => None,
                };
                // Cross-complement two-sided sums (spec §2): Q1 = this leg's bids
                // + the complement's asks; Q2 = this leg's asks + the complement's
                // bids. Hedging is bid-only, so this reduces to Q1 = our bids /
                // Q2 = complement bids, but the full form stays correct if an
                // inventory-offload ask ever rests. A missing leg contributes 0.
                let market_q1 = leg.as_ref().map_or(0.0, |l| l.q_bids)
                    + comp_leg.as_ref().map_or(0.0, |l| l.q_asks);
                let market_q2 = leg.as_ref().map_or(0.0, |l| l.q_asks)
                    + comp_leg.as_ref().map_or(0.0, |l| l.q_bids);
                // The market's representative mid + reward pool: both complement
                // tokens share the SAME pool, so the daily rate is taken ONCE
                // (never summed) and the depth is the pair's combined in-band
                // notional. Skip the market if neither leg had a scorable mid.
                let Some(repr) = leg.as_ref().or(comp_leg.as_ref()) else {
                    continue;
                };
                let depth = leg.as_ref().map_or(0.0, |l| l.depth_usd)
                    + comp_leg.as_ref().map_or(0.0, |l| l.depth_usd);
                q1_total += market_q1;
                q2_total += market_q2;
                q_min_total += q_min(market_q1, market_q2, repr.adj_mid);
                est_total +=
                    est_daily_reward_usd(repr.daily_rate, depth, REWARD_COMPETING_DEPTH_FALLBACK);
            }
        } else {
            for (token, orders) in by_token {
                // `.1` = max_spread_cents (scoring band `v`), `.2` = daily_rate_usd.
                // A token with no metrics / not reward-eligible (band ≤ 0) scores 0,
                // so it is skipped (it earns nothing and would divide by a 0 band).
                let (min_size_shares, max_spread_cents, daily_rate) =
                    self.reward_by_token.get(&token).copied().unwrap_or((0.0, 0.0, 0.0));
                if max_spread_cents <= 0.0 {
                    continue;
                }
                // Adjusted mid from the live book; skip the token if it is gone /
                // one-sided (we cannot score a distance-from-mid without a mid).
                let Some((book, true)) = self.fetcher.fetch(token).await else {
                    continue;
                };
                let ts = book.ts();
                // SAME size-weighted fair value the quoting path prices off (spec §8.1,
                // closes I2): microprice with the sub-`min_size` cutoff, so the scored
                // distance-from-mid matches what we actually quoted. `None` ⇒ the book
                // is one-sided/empty — skip (no mid to score against).
                let Some(adj_mid) = reward_fair_value(&book, ts, min_size_shares) else {
                    continue;
                };

                let mut bids: Vec<ScoredOrder> = Vec::new();
                let mut asks: Vec<ScoredOrder> = Vec::new();
                let mut our_in_band_depth = 0.0_f64; // USD notional of in-band sides
                for o in &orders {
                    let price = o.price.microusdc(ts) as f64 / 1_000_000.0;
                    let shares = o.size.0 as f64 / 1_000_000.0;
                    let so = ScoredOrder {
                        spread_cents: (price - adj_mid).abs() * 100.0,
                        size: shares,
                    };
                    // In-band (score > 0) sides contribute to our reward depth, in
                    // USD (shares × price) to stay consistent with the competing
                    // in-band depth a future live-book plumb would supply (spec §7).
                    if order_score(max_spread_cents, so.spread_cents) > 0.0 {
                        our_in_band_depth += shares * price;
                    }
                    match o.side {
                        Side::Bid => bids.push(so),
                        Side::Ask => asks.push(so),
                    }
                }

                // Q1/Q2 (per-side score sums) drive the balance ratio; `q_min` is the
                // two-sided minimum from the same primitives via `quote_set_q_min`.
                let q1: f64 = bids
                    .iter()
                    .map(|o| order_score(max_spread_cents, o.spread_cents) * o.size)
                    .sum();
                let q2: f64 = asks
                    .iter()
                    .map(|o| order_score(max_spread_cents, o.spread_cents) * o.size)
                    .sum();
                q1_total += q1;
                q2_total += q2;
                q_min_total += quote_set_q_min(max_spread_cents, adj_mid, &bids, &asks);
                est_total +=
                    est_daily_reward_usd(daily_rate, our_in_band_depth, REWARD_COMPETING_DEPTH_FALLBACK);
            }
        }

        let balance_ratio = {
            let hi = q1_total.max(q2_total);
            if hi > 0.0 { q1_total.min(q2_total) / hi } else { 0.0 }
        };
        RewardFarmStatus {
            est_reward_usd_day: est_total,
            q_min: q_min_total,
            in_band: q_min_total > 0.0,
            balance_ratio,
            // The caller adds this sample to (and copies in) the session total.
            cumulative_est: 0.0,
            // The caller overwrites these from the loop's latest quote cycle
            // (Phase-A TUI surfacing); the scorer itself has no signal context.
            signal: 0.0,
            pulled: false,
        }
    }

    /// Score ONE complement leg's resting orders for the pair-aware hedging
    /// estimator (Task B3): each order scores against the token's OWN adjusted
    /// mid (`reward_fair_value`, spec §8.1) and reward band, via the pure
    /// [`reward_score`](super::reward_score) primitives. Returns `None` when the
    /// token is not reward-eligible (band ≤ 0) or its book has no two-sided mid
    /// to score against — the caller then treats this leg as absent (its `Q`
    /// contribution is 0). Mirrors the non-hedging per-token scoring exactly; the
    /// only difference is the caller pairs two legs into a market-level `q_min`.
    async fn score_reward_leg(&mut self, token: TokenId, orders: &[MakerOrder]) -> Option<RewardLeg> {
        // `.1` = max_spread_cents (scoring band `v`), `.2` = daily_rate_usd.
        let (min_size_shares, max_spread_cents, daily_rate) =
            self.reward_by_token.get(&token).copied().unwrap_or((0.0, 0.0, 0.0));
        if max_spread_cents <= 0.0 {
            return None;
        }
        // One-sided/empty book ⇒ no mid to anchor distance-from-mid → leg absent.
        let Some((book, true)) = self.fetcher.fetch(token).await else {
            return None;
        };
        let ts = book.ts();
        let adj_mid = reward_fair_value(&book, ts, min_size_shares)?;

        let mut q_bids = 0.0_f64;
        let mut q_asks = 0.0_f64;
        let mut depth_usd = 0.0_f64; // USD notional of in-band sides
        for o in orders {
            let price = o.price.microusdc(ts) as f64 / 1_000_000.0;
            let shares = o.size.0 as f64 / 1_000_000.0;
            let score = order_score(max_spread_cents, (price - adj_mid).abs() * 100.0);
            // In-band (score > 0) sides count toward our reward depth, in USD
            // (shares × price), mirroring the non-hedging path.
            if score > 0.0 {
                depth_usd += shares * price;
            }
            match o.side {
                Side::Bid => q_bids += score * shares,
                Side::Ask => q_asks += score * shares,
            }
        }
        Some(RewardLeg { q_bids, q_asks, depth_usd, daily_rate, adj_mid })
    }

    /// Compute bid- and mid-marked P&L and publish the full [`StrategyStatus`]
    /// (+ a durable, bid-marked `PnlRow`). Held tokens are valued from the
    /// SIGNED net at the current book — long at the best bid, short at the best
    /// ask (the conservative reporting side) — so `positions.pnl` is correct for
    /// either sign even though `PositionBook` is append-only.
    async fn publish_status(&mut self) {
        let tokens = self.tokens.clone();
        let mut bid_marks: HashMap<TokenId, Usdc> = HashMap::new();
        let mut mid_marks: HashMap<TokenId, Usdc> = HashMap::new();
        let mut open_positions = 0usize;
        for token in tokens {
            let net = self.inv.net(token);
            if net == 0 {
                continue;
            }
            open_positions += 1;
            if let Some((book, true)) = self.fetcher.fetch(token).await
                && let (Some(bb), Some(ba)) = (book.bids.best(), book.asks.best())
            {
                let ts = book.ts();
                let bid_micro = bb.microusdc(ts);
                let ask_micro = ba.microusdc(ts);
                let mid = (bid_micro + ask_micro) / 2;
                // Conservative reporting price per side: long → best bid (what we
                // could sell into), short → best ask (what we'd buy back at).
                let bid_price = if net > 0 { bid_micro } else { ask_micro };
                bid_marks.insert(token, Usdc(signed_value(net, bid_price)));
                mid_marks.insert(token, Usdc(signed_value(net, mid)));
            }
        }
        let pnl = self.positions.pnl(&bid_marks); // bid-marked (reporting)
        let pnl_mid = self.positions.pnl(&mid_marks); // mid-marked (risk feed)
        // Surface BOTH halt sources to the dashboard (DISPLAY ONLY — InvHalt
        // semantics are unchanged). The in-session InvHalt reason
        // (StopLoss/DailyLoss) takes precedence as the more specific cause;
        // otherwise, when only the PERSISTENT UTC-day loss cap (Task 9) is
        // latched, show `DayLossCap` so operators see WHY the MM refuses to quote
        // instead of it appearing un-halted. Equivalent to
        // `self.halted || self.day_loss_halted` for the is-halted flag, since
        // `self.halted` mirrors `self.inv.halted().is_some()`.
        let halted = self
            .inv
            .halted()
            .map(|h| format!("{h:?}"))
            .or_else(|| self.day_loss_halted.then(|| "DayLossCap".to_string()));

        let row = PnlRow {
            ts_ms: now_ms(),
            cash_micro: usdc_to_i64(pnl.cash).unwrap_or(i64::MAX),
            realized_micro: usdc_to_i64(pnl.realized).unwrap_or(i64::MAX),
            unrealized_micro: usdc_to_i64(pnl.unrealized).unwrap_or(i64::MAX),
            equity_micro: usdc_to_i64(pnl.equity).unwrap_or(i64::MAX),
            strategy: "mm".into(),
        };
        let _ = self.store_tx.send(StoreMsg::PnlSnapshot(row)).await;

        // Open-orders snapshot for the dashboard: every LIVE resting quote, plus
        // each VETOED (cancelled + suppressed) (token, side) so the operator can
        // see and un-veto them. `tick_levels` lets the publisher format the price.
        let mut resting_orders: Vec<RestingOrderSnapshot> = self
            .qm
            .resting_orders()
            .into_iter()
            .map(|(_, o)| RestingOrderSnapshot {
                token: o.token,
                side: o.side,
                px_ticks: o.price.get(),
                tick_levels: self.token_ts.get(&o.token).map_or(100, |ts| ts.levels()),
                qty_micro: o.size.0,
                vetoed: false,
            })
            .collect();
        for &(token, side) in &self.vetoed {
            resting_orders.push(RestingOrderSnapshot {
                token,
                side,
                px_ticks: 0,
                tick_levels: self.token_ts.get(&token).map_or(100, |ts| ts.levels()),
                qty_micro: 0,
                vetoed: true,
            });
        }

        let _ = self.status_tx.send(StrategyStatus {
            paused: self.paused,
            halted,
            cash_micro: usdc_to_i64(pnl.cash).unwrap_or(i64::MAX),
            equity_micro: usdc_to_i64(pnl.equity).unwrap_or(i64::MAX),
            equity_mid_micro: usdc_to_i64(pnl_mid.equity).unwrap_or(i64::MAX),
            realized_micro: usdc_to_i64(pnl.realized).unwrap_or(i64::MAX),
            unrealized_micro: usdc_to_i64(pnl.unrealized).unwrap_or(i64::MAX),
            open_positions,
            // SEPARATE maker-rebate estimate (Task 4.4): published distinctly,
            // NOT folded into the marked equity/cash/realized above.
            rebate_micro: usdc_to_i64(Usdc(self.rebate_accrued_micro)).unwrap_or(i64::MAX),
            resting_orders,
            // Task 11 — RewardFarm liquidity-reward ESTIMATE (spec §9): the last
            // sample's cached telemetry. `Some` ONLY under RewardFarm (the sampler
            // never runs otherwise), so SpreadCapture/arb publish `None` here.
            reward_farm: self.reward_status,
        });
    }

    /// Cancel every resting quote (best-effort — the next tick re-quotes when
    /// active, or stays flat when paused/halted).
    async fn cancel_all(&mut self) {
        let n = self.qm.tracked().len();
        if n > 0 {
            tracing::info!(resting = n, "mm: canceling all resting quotes");
        }
        let _ = self.qm.cancel_all(&mut self.venue).await;
    }

    /// Apply a dashboard veto. `veto = true` CANCELS the resting `(token, side)`
    /// immediately (so the operator's action feels instant rather than waiting
    /// for the next quote cycle) and records the suppression so `quote()` never
    /// re-places it; `veto = false` lifts the suppression and the next tick
    /// re-quotes it normally.
    async fn set_veto(&mut self, token: TokenId, side: Side, veto: bool) {
        if veto {
            self.vetoed.insert((token, side));
            match self.qm.cancel_one(&mut self.venue, token, side).await {
                Ok(()) => tracing::info!(
                    token = token.0,
                    side = ?side,
                    "mm: quote VETOED (cancelled + re-quote suppressed)"
                ),
                // Suppression is already recorded, so the next reconcile cancels
                // it anyway — just surface the transient failure.
                Err(e) => tracing::warn!(
                    token = token.0,
                    side = ?side,
                    error = %e,
                    "mm: veto cancel failed (suppressed; reconcile will retry)"
                ),
            }
        } else if self.vetoed.remove(&(token, side)) {
            tracing::info!(
                token = token.0,
                side = ?side,
                "mm: quote UN-VETOED (re-quotes next cycle)"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Pure complete-set MERGE selection + apply (Task B5 / M6-7)
// ---------------------------------------------------------------------------

/// A complete YES+NO set worth recycling: the ordered complement pair `(a, b)`
/// (token-id order, `a.0 <= b.0`) and the matched depth `amount_micro` (µshares)
/// that can be merged back to collateral. A complete set always redeems to
/// EXACTLY $1, and the CTF base unit is 6-decimal (1:1 with µUSDC), so
/// `amount_micro` is BOTH the µshare reduction per leg AND the µUSDC recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MergeCandidate {
    a: TokenId,
    b: TokenId,
    amount_micro: i128,
}

/// The result of a (live) on-chain merge, sent back from the spawned sweep task
/// to the quote loop's [`MmLoop::drain_merge_outcomes`] for inventory/cash
/// settlement. Carries the ordered pair + the merged `amount_micro` so the
/// drain can clear the in-flight latch and apply EXACTLY the paper reduction on
/// success. `result` is the relayer's recovered µUSDC (`Ok`) or a typed
/// [`RelayerError`] (`Err`, retried next sweep) — never a panic.
#[derive(Debug)]
struct MergeOutcome {
    a: TokenId,
    b: TokenId,
    amount_micro: i128,
    result: Result<i128, RelayerError>,
}

/// THE B5 selection (pure, unit-tested): the set of mergeable complete YES+NO
/// sets over `threshold_micro`. Dedups the bidirectional `complement` map down
/// to each unordered market-pair (processed in deterministic token-id order),
/// and for each pair computes `matched = min(net(a).max(0), net(b).max(0))`
/// µshares — the matched LONG depth on both legs (a short on either leg is not
/// part of a set). A complete set redeems to EXACTLY $1, so the recovered µUSDC
/// equals `matched` (6-decimal base unit, 1:1 with µUSDC); the pair is INCLUDED
/// only when that exceeds `threshold_micro` (the per-pair "worth the merge"
/// floor). Each emitted pair is canonicalised `a.0 <= b.0` so callers can key an
/// in-flight set on `(a, b)` deterministically.
fn mergeable_pairs(
    complement: &HashMap<TokenId, TokenId>,
    inv: &InventoryRisk,
    threshold_micro: i128,
) -> Vec<MergeCandidate> {
    // Deterministic order: sort the complement keys, take each unordered pair
    // once (skipping a token already consumed as the other leg of a pair).
    let mut keys: Vec<TokenId> = complement.keys().copied().collect();
    keys.sort_by_key(|t| t.0);
    let mut seen: HashSet<TokenId> = HashSet::new();
    let mut out: Vec<MergeCandidate> = Vec::new();
    for key in keys {
        if seen.contains(&key) {
            continue;
        }
        let Some(&other) = complement.get(&key) else {
            continue;
        };
        seen.insert(key);
        seen.insert(other);
        // Canonicalise so the in-flight key / conditionId lookup is order-stable.
        let (a, b) = if key.0 <= other.0 { (key, other) } else { (other, key) };
        // Mergeable complete set = the matched LONG depth on both legs (µshares).
        let matched_micro = inv.net(a).max(0).min(inv.net(b).max(0));
        if matched_micro <= 0 {
            continue;
        }
        // Recovered µUSDC == matched µshares (a $1 set, 6-decimal base unit). The
        // threshold is a coarse "worth the merge" floor; the money below stays in
        // integer µUSDC. Mirrors the B5 `recovered_usd <= merge_threshold_usd`
        // skip (here `recovered_micro <= threshold_micro`).
        if matched_micro <= threshold_micro {
            continue;
        }
        out.push(MergeCandidate { a, b, amount_micro: matched_micro });
    }
    out
}

/// Apply EXACTLY the B5 paper-merge reduction for one complete set (pure, no
/// async/IO — unit-tested, and SHARED by the paper recycle AND the live
/// drain so the two converge): reduce BOTH legs' long inventory by
/// `amount_micro` via the tested signed-lot [`InventoryRisk::on_fill`] sell path
/// (a pure reduction — `amount_micro <= net` on each leg by construction) and
/// CREDIT the recovered `amount_micro` µUSDC (a complete set redeems to $1) as
/// cash in the reporting [`PositionBook`], releasing each leg's basis pro-rata.
/// The recovered $1/set is split evenly across the two legs for realized
/// ATTRIBUTION only — the AGGREGATE realized (`recovered − basis released`) is
/// invariant to the split. Returns that aggregate `realized_delta` (µUSDC) so
/// the caller can persist it to the day-loss ledger exactly like a fill.
fn apply_merge_result(
    inv: &mut InventoryRisk,
    positions: &mut PositionBook,
    token_market: &HashMap<TokenId, MarketId>,
    a: TokenId,
    b: TokenId,
    amount_micro: i128,
) -> i128 {
    // Defensive clamp: a merge can reduce at most each leg's CURRENT long net.
    // By construction this holds (the live sweep runs ONLY under bid-only
    // hedging, so between selection and this drain-time apply a leg's net only
    // GROWS via bid fills; the paper path applies the same cycle it selects).
    // The clamp keeps the money path a pure reduction — never a phantom short —
    // even if asks were ever re-enabled for these legs. No-op when the
    // invariant holds, so paper recycle + the existing tests are unchanged.
    let amount_micro = amount_micro.max(0).min(inv.net(a).max(0)).min(inv.net(b).max(0));
    if amount_micro == 0 {
        return 0;
    }
    // A complete set redeems to EXACTLY $1, so recovered µUSDC == merged µshares.
    let recovered_micro = amount_micro;
    let realized_before = inv.realized(a).0 + inv.realized(b).0;
    let basis_before_a = inv.basis(a).0;
    let basis_before_b = inv.basis(b).0;
    // Split the recovered $1/set evenly (attribution only; aggregate invariant).
    let cash_a = recovered_micro / 2;
    let cash_b = recovered_micro - cash_a;
    inv.on_fill(a, -amount_micro, Usdc(cash_a));
    inv.on_fill(b, -amount_micro, Usdc(cash_b));
    // Mirror into the reporting book in lock-step: credit the recovered
    // collateral as cash + release each leg's basis. Qty 0 — the append-only
    // book derives position VALUE from `inv.net` marks, so a REDUCTION must not
    // grow phantom qty; only cash + basis move.
    let cost_delta_a = Usdc(inv.basis(a).0 - basis_before_a);
    let cost_delta_b = Usdc(inv.basis(b).0 - basis_before_b);
    positions.apply(
        &[(a, Qty(0), cost_delta_a), (b, Qty(0), cost_delta_b)],
        Usdc(recovered_micro),
        token_market,
    );
    (inv.realized(a).0 + inv.realized(b).0) - realized_before
}

// ---------------------------------------------------------------------------
// Pure resolved-winner REDEEM selection + apply (R1)
// ---------------------------------------------------------------------------

/// One held leg of a RESOLVED condition to redeem: the `token`, its Data-API
/// resolved `cur_price` (≈1.0 for the winner, ≈0.0 for the loser), and the long
/// `net_micro` (µshares) currently held. The on-chain `redeemPositions` clears
/// BOTH outcome slots of a resolved condition at once, so the loser leg is
/// redeemed too (at ~$0) — hence a leg per held outcome.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RedeemLeg {
    pub token: TokenId,
    pub resolved_price: f64,
    pub net_micro: i128,
}

/// A RESOLVED condition the MM still holds, with every held leg priced at its
/// Data-API resolved price. Emitted by [`redeem_targets`] and settled by
/// [`apply_redeem`]; the on-chain redeem (R2) is keyed on `condition_id`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RedeemTarget {
    pub condition_id: B256,
    pub legs: Vec<RedeemLeg>,
}

/// THE R1 selection (pure, unit-tested): the RESOLVED conditions the MM still
/// HOLDS, each with its held legs priced at the Data-API resolved `cur_price`.
///
/// A condition is "resolved" when ANY of its tokens appears as a `redeemable`
/// [`Position`](pm_ingestion::data_api::Position) (the market settled). For each
/// such condition EVERY held (`net_of > 0`) leg is included — winner AND loser —
/// because the on-chain `redeemPositions` clears both outcome slots in one call.
/// Each leg is priced at the resolved `cur_price` for its CLOB asset (≈1.0 the
/// winner, ≈0.0 the loser), defaulting to `0.0` when the leg isn't itself listed
/// redeemable (the resolved loser is often dropped from the positions feed).
/// Deterministic order (legs by token id, targets by condition_id) for stable
/// callers + tests.
pub(crate) fn redeem_targets(
    positions: &[pm_ingestion::data_api::Position],
    cond_by_token: &HashMap<TokenId, B256>,
    venue_by_token: &HashMap<TokenId, String>,
    net_of: impl Fn(TokenId) -> i128,
) -> Vec<RedeemTarget> {
    // RESOLVED conditions = the conditionId of every `redeemable` position (skip
    // an unparseable id), plus the resolved price per CLOB asset for leg pricing.
    let mut resolved: HashSet<B256> = HashSet::new();
    let mut price_by_asset: HashMap<&str, f64> = HashMap::new();
    for p in positions {
        if !p.redeemable {
            continue;
        }
        if let Ok(cid) = p.condition_id.parse::<B256>() {
            resolved.insert(cid);
        }
        price_by_asset.insert(p.asset.as_str(), p.cur_price);
    }
    if resolved.is_empty() {
        return Vec::new();
    }
    // Every HELD (net > 0) leg of a resolved condition, grouped by conditionId —
    // winner AND loser, because `redeemPositions` clears both slots at once.
    let mut by_cond: HashMap<B256, Vec<RedeemLeg>> = HashMap::new();
    for (&token, &cond) in cond_by_token {
        if !resolved.contains(&cond) {
            continue;
        }
        let net_micro = net_of(token);
        if net_micro <= 0 {
            continue;
        }
        // Resolved price for this leg's CLOB asset; the loser (not itself listed
        // redeemable) defaults to 0.0.
        let resolved_price = venue_by_token
            .get(&token)
            .and_then(|asset| price_by_asset.get(asset.as_str()).copied())
            .unwrap_or(0.0);
        by_cond.entry(cond).or_default().push(RedeemLeg {
            token,
            resolved_price,
            net_micro,
        });
    }
    // Deterministic order: legs by token id, targets by condition_id.
    let mut out: Vec<RedeemTarget> = by_cond
        .into_iter()
        .map(|(condition_id, mut legs)| {
            legs.sort_by_key(|l| l.token.0);
            RedeemTarget { condition_id, legs }
        })
        .collect();
    out.sort_by_key(|t| t.condition_id);
    out
}

/// Settle one RESOLVED condition's held legs at their resolved prices (pure, no
/// async/IO — unit-tested, and the SHARED sell path so a future live redeem
/// drain CONVERGES with it). For each leg: credit the resolved value
/// `round(net_micro · resolved_price)` µUSDC (clamped ≥ 0; winner ≈ $1/share,
/// loser $0) via the tested signed-lot [`InventoryRisk::on_fill`] sell path and
/// mirror it into the reporting [`PositionBook`] like [`apply_merge_result`]
/// (cash in, basis released, qty 0). Returns the aggregate realized delta
/// (µUSDC) so the caller can persist it to the day-loss ledger like a fill.
fn apply_redeem(
    inv: &mut InventoryRisk,
    positions: &mut PositionBook,
    token_market: &HashMap<TokenId, MarketId>,
    target: &RedeemTarget,
) -> i128 {
    let realized_before: i128 = target.legs.iter().map(|l| inv.realized(l.token).0).sum();
    let mut book_rows: Vec<(TokenId, Qty, Usdc)> = Vec::with_capacity(target.legs.len());
    let mut recovered_micro: i128 = 0;
    for leg in &target.legs {
        // Resolved value of the held leg, integer µUSDC. A resolved price is a
        // probability in [0, 1]; clamp it BOTH ways defensively so a noisy/buggy
        // Data-API `curPrice` can neither credit a debit (< 0) nor over-credit
        // beyond the true $1/share on-chain recovery (> 1).
        let price = leg.resolved_price.clamp(0.0, 1.0);
        let cash_micro = (((leg.net_micro as f64) * price).round() as i128).max(0);
        let basis_before = inv.basis(leg.token).0;
        // The shared signed-lot SELL: reduce the long by `net_micro`, credit the
        // recovered cash (winner ≈ $1/share, loser $0) and release basis pro-rata.
        inv.on_fill(leg.token, -leg.net_micro, Usdc(cash_micro));
        // Mirror into the reporting book in lock-step (cash + released basis); qty
        // 0 — the book derives value from `inv.net` marks, so a REDUCTION must not
        // grow phantom qty (exactly as `apply_merge_result`).
        let cost_delta = Usdc(inv.basis(leg.token).0 - basis_before);
        book_rows.push((leg.token, Qty(0), cost_delta));
        recovered_micro += cash_micro;
    }
    positions.apply(&book_rows, Usdc(recovered_micro), token_market);
    let realized_after: i128 = target.legs.iter().map(|l| inv.realized(l.token).0).sum();
    realized_after - realized_before
}

/// The market maker's owned async loop, generic over the venue (Task 4.5 passes
/// a live one). Mirrors the [`stub`](super::stub) lifecycle: a `quote_refresh`
/// interval, honoring `ctx.kill` each iteration and draining `ctl_rx` for
/// pause, exiting cleanly when killed or the control channel closes.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_mm_loop<V: MakerVenue + UserFillSource>(
    venue: V,
    qm: QuoteManager,
    inv: InventoryRisk,
    positions: PositionBook,
    ctx: StrategyCtx,
    params: MmParams,
    tokens: Vec<TokenId>,
    token_market: HashMap<TokenId, MarketId>,
    complement: HashMap<TokenId, TokenId>,
    // M6-7: the LIVE on-chain merge relayer (`Some` only on reward-farm live with
    // the relayer configured) + the `token → conditionId` map the merge sweep
    // needs. Both default to empty/None on paper / arb / non-relayer live, so
    // those paths are byte-for-byte unchanged.
    merger: Option<Arc<RelayerClient>>,
    cond_by_token: HashMap<TokenId, B256>,
    // R1 (auto-redeem): the resolved-position feed inputs — the `token → CLOB asset`
    // map, the Data-API positions client, and the deposit wallet to query. All
    // default empty/None on paper / arb / non-relayer live (only populated on a
    // relayer-backed reward-farm live run), so those paths are byte-for-byte
    // unchanged. R1 only HOLDS them in the loop; the redeem sweep that consumes
    // them is R2.
    venue_by_token: HashMap<TokenId, String>,
    data_api: Option<Arc<pm_ingestion::data_api::DataApiClient>>,
    deposit_wallet: Option<String>,
    capital: Usdc,
    no_naked_shorts: bool,
    start_paused: bool,
    store_path: Option<std::path::PathBuf>,
) {
    let StrategyCtx {
        registry,
        fetcher,
        store_tx,
        kill,
        mut ctl_rx,
        status_tx,
    } = ctx;
    // Per-side notional is capped by max_quote_usd AND the whole capital envelope.
    let notional_micro = params.max_quote_micro.min(capital.0).max(0);
    // Per-token reward-program params `(min_size, max_spread_cents)` resolved ONCE
    // from the registry MarketMetrics for each token's market (spec §6). Read by
    // the RewardFarm policy; harmlessly built (and ignored) under SpreadCapture. A
    // token with no metrics maps to zeros → not reward-eligible → gated out. The
    // registry `Arc` is dropped after this — only the resolved map is retained.
    let reward_by_token: HashMap<TokenId, (f64, f64, f64)> = tokens
        .iter()
        .filter_map(|t| {
            let m = registry.metrics(*token_market.get(t)?)?;
            Some((
                *t,
                (
                    m.reward_min_size,
                    m.reward_max_spread_cents,
                    m.reward_daily_rate_usd,
                ),
            ))
        })
        .collect();
    // M6-7: the spawned on-chain merge tasks send their outcomes back to the loop
    // over this channel; `drain_merge_outcomes` receives + settles them each cycle.
    let (merge_tx, merge_rx) = mpsc::unbounded_channel();
    // R2: the spawned redeem sweeps send their confirmed targets back here.
    let (redeem_tx, redeem_rx) = mpsc::unbounded_channel();
    let mut mm = MmLoop {
        venue,
        qm,
        inv,
        positions,
        fetcher,
        store_tx,
        status_tx,
        params,
        tokens,
        token_market,
        complement,
        policy: params.policy,
        reward_by_token,
        notional_micro,
        placed: HashMap::new(),
        token_ts: HashMap::new(),
        rebate_accrued_micro: 0,
        // HELD live (TUI before `l`) starts paused so the MM never quotes real
        // money until released; the host sends `SetPaused(false)` on release.
        paused: start_paused,
        halted: false,
        day: utc_day_from_ms(now_ms()),
        day_loss_halted: false,
        day_loss_read: None,
        no_naked_shorts,
        vetoed: HashSet::new(),
        last_reward_sample: None,
        reward_status: None,
        reward_cumulative_est: 0.0,
        signals: HashMap::new(),
        pulled_until: HashMap::new(),
        last_signal: 0.0,
        last_pulled: false,
        merge_live_warned: false,
        merger,
        cond_by_token,
        merge_tx,
        merge_rx,
        merge_inflight: HashSet::new(),
        last_merge_sweep: Instant::now(),
        venue_by_token,
        data_api,
        deposit_wallet,
        redeem_tx,
        redeem_rx,
        redeem_sweep_inflight: false,
        last_redeem_sweep: Instant::now(),
    };
    if start_paused {
        tracing::info!("mm: live held — quoting PAUSED until release (press `l`)");
    }
    // Task 9 + I3 — PERSISTENT UTC-day loss cap: before the first tick, read
    // today's persisted `"mm"` data from the store and latch `day_loss_halted` if
    // the day is already at/under the daily-loss cap, so the cap binds across the
    // periodic auto-restart. The read-only handle is RETAINED in the loop so
    // `tick` can re-check both gate arms (summed-realized ledger + worst-point
    // snapshot) MID-session (I3), not only at the next restart; a missing/failed
    // DB → no handle → not halted (fresh run) and the per-cycle re-check is inert.
    let day_loss_read = store_path.as_deref().and_then(|p| ReadStore::open(p).ok());
    mm.arm_day_loss_gate(day_loss_read.as_ref(), now_ms());
    mm.day_loss_read = day_loss_read;

    let mut tick = tokio::time::interval(params.quote_refresh);
    // A steady cadence, not a catch-up burst after a stall (mirrors the stub).
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        // The global kill is the real shutdown signal; observe it each iteration.
        if kill.load(Ordering::Relaxed) {
            mm.cancel_all().await;
            mm.publish_status().await; // final state out-of-band (trait contract)
            return;
        }
        tokio::select! {
            _ = tick.tick() => mm.tick().await,
            cmd = ctl_rx.recv() => match cmd {
                Some(StrategyCommand::SetPaused(p)) => {
                    mm.paused = p;
                    // Pause cancels resting quotes and stops quoting; resume just
                    // re-enables — the next tick re-quotes.
                    if p {
                        mm.cancel_all().await;
                    }
                }
                Some(StrategyCommand::VetoQuote { token, side, veto }) => {
                    // Dashboard per-order cancel/un-veto. Publish immediately so
                    // the open-orders panel reflects the change without waiting
                    // for the next tick.
                    mm.set_veto(token, side, veto).await;
                    mm.publish_status().await;
                }
                None => {
                    // Host dropped the control sender → shut down cleanly.
                    mm.cancel_all().await;
                    return;
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    use pm_ingestion::supervisor::SupervisorCommand;
    use pm_risk::inventory::InvHalt;

    const SH: u64 = 1_000_000; // one share in µshares

    type SharedBooks = Arc<Mutex<HashMap<TokenId, (Book, bool)>>>;

    fn px(tick: u16) -> Px {
        Px::new(tick, TickSize::Cent).unwrap()
    }

    /// A Cent book from `(tick, qty)` bid and ask levels.
    fn cent_book(bids: &[(u16, u64)], asks: &[(u16, u64)]) -> Book {
        let mut b = Book::new(TickSize::Cent);
        for &(t, q) in bids {
            b.apply(Side::Bid, px(t), Qty(q));
        }
        for &(t, q) in asks {
            b.apply(Side::Ask, px(t), Qty(q));
        }
        b
    }

    /// One valid two-sided Cent book bid 0.48 / ask 0.52 → mid 0.50.
    fn mid50_book() -> Book {
        cent_book(&[(48, 100 * SH)], &[(52, 100 * SH)])
    }

    fn empty_registry() -> Arc<pm_registry::Registry> {
        Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap())
    }

    /// A [`BookFetcher`] backed by a shared, MUTABLE book map served over a
    /// supervisor channel (mirrors `coordinator::tests::served_fetcher`). Tests
    /// rewrite the map between steps to drive crosses + marks; every `tokens`
    /// entry routes to the one server. Returns the fetcher + the shared handle.
    fn controllable_fetcher(
        tokens: &[TokenId],
        initial: HashMap<TokenId, (Book, bool)>,
    ) -> (BookFetcher, SharedBooks) {
        let shared: SharedBooks = Arc::new(Mutex::new(initial));
        let shared2 = Arc::clone(&shared);
        let (tx, mut rx) = mpsc::channel::<SupervisorCommand>(64);
        tokio::spawn(async move {
            while let Some(SupervisorCommand::BookSnapshot { token, reply }) = rx.recv().await {
                let snap = shared2.lock().unwrap().get(&token).cloned();
                let _ = reply.send(snap);
            }
        });
        let routes = tokens.iter().map(|t| (*t, tx.clone())).collect();
        (BookFetcher::new(routes), shared)
    }

    fn mk_params(spread_bps: u32, max_quote_usd: f64) -> MmParams {
        // Skew OFF by default so the Task-4.2 quoting/fill/lifecycle tests keep
        // their exact symmetric expectations; the skew tests opt in below.
        mk_params_skew(spread_bps, max_quote_usd, 0)
    }

    fn mk_params_skew(spread_bps: u32, max_quote_usd: f64, inventory_skew_bps: u32) -> MmParams {
        MmParams {
            spread_bps,
            quote_refresh: Duration::from_millis(10),
            max_quote_micro: pm_config::usd_to_microusdc(max_quote_usd).unwrap(),
            inventory_skew_bps,
            // Rebate OFF by default so the Task-4.2/4.3 tests are unaffected; the
            // rebate / e2e tests set it explicitly.
            rebate_bps: 0,
            // Paper taker-flow OFF by default (conservative adverse-only sim);
            // the demo / fill tests opt in.
            paper_taker_fill_pct: 0,
            // SpreadCapture by default so the Task-4.2/4.3 tests run the unchanged
            // spread path; reward-farm pricing is unit-tested in `quote_policy`.
            policy: Policy::SpreadCapture,
            // 2.0 = the spec/§8.3 + `RewardFarm` default ≤2:1 lean cap; the
            // reward-farm sizing tests below set inventory/cap explicitly.
            size_skew_max_ratio: 2.0,
            // 1 = the `RewardFarm` default 1-tick anti-flicker band; the sticky
            // re-quote test below sets it (and the policy) explicitly.
            requote_band_ticks: 1,
            // 60s = the `RewardFarm` default estimator cadence. The first cycle
            // always samples regardless, so the estimator tests see a figure
            // after one tick without waiting out the interval.
            sample_interval: Duration::from_millis(60_000),
            // Phase-A adverse-selection knobs at their `RewardFarm` defaults; the
            // Phase-A tests that exercise them set them explicitly.
            microprice_levels: 3,
            signal_window_ms: 3000,
            pull_threshold: 0.6,
            pull_cooldown_ms: 5000,
            size_rebalance_pct: 0.25,
            // Phase-B complement-pair + merge knobs at their `RewardFarm`
            // defaults (off / $5); the Phase-B tests that exercise them set
            // them explicitly.
            hedging_enabled: false,
            merge_threshold_usd: 5.0,
        }
    }

    /// Generous inventory caps (no halt) for the quoting/fill tests.
    fn generous_inv() -> InventoryConfig {
        InventoryConfig {
            max_inventory_usd: Usdc(1_000_000_000),       // $1000
            max_gross_inventory_usd: Usdc(2_000_000_000), // $2000
            inventory_stop_loss_usd: Usdc(1_000_000_000), // $1000
            daily_loss_usd: Usdc(1_000_000_000),          // $1000
            vol_pull_ticks: 5,
            vol_window: Duration::from_millis(2000),
        }
    }

    fn token_market_for(tokens: &[TokenId]) -> HashMap<TokenId, MarketId> {
        tokens.iter().map(|t| (*t, MarketId(0))).collect()
    }

    /// A SHADOW [`LiveVenue`] for the live-gating tests: shadow mode signs but
    /// performs NO network I/O, so it can stand in for the real venue when
    /// asserting the venue-selection plumbing without ever hitting the network.
    /// Mirrors `live::tests::test_venue` minimally.
    fn shadow_live_venue() -> LiveVenue {
        use pm_execution::live::LiveVenueCfg;
        use pm_execution::secrets::{ApiCreds, Secret};
        let signer: alloy_signer_local::PrivateKeySigner = "ad".repeat(32).parse().unwrap();
        let addr =
            |b: &str| -> alloy_primitives::Address { format!("0x{}", b.repeat(20)).parse().unwrap() };
        LiveVenue::new(LiveVenueCfg {
            base: "https://clob.invalid".into(),
            creds: ApiCreds {
                key: "test-key".into(),
                secret: Secret::new("QQ==".into()),
                passphrase: Secret::new("pass".into()),
            },
            signer,
            proxy: addr("11"),
            deposit_wallet: addr("22"),
            auth_address: addr("33"),
            fill_window: Duration::from_millis(100),
            rate_per_sec: 1000.0,
            rate_capacity: 100,
            shadow: true,
        })
        .unwrap()
    }

    #[allow(clippy::type_complexity)]
    fn build_loop(
        fetcher: BookFetcher,
        inv_cfg: InventoryConfig,
        params: MmParams,
        tokens: Vec<TokenId>,
        capital: Usdc,
    ) -> (
        MmLoop<PaperMakerVenue<BookFetcher>>,
        mpsc::Receiver<StoreMsg>,
        watch::Receiver<StrategyStatus>,
    ) {
        let (store_tx, store_rx) = mpsc::channel(256);
        let (status_tx, status_rx) = watch::channel(StrategyStatus::default());
        let venue = PaperMakerVenue::new(fetcher.clone());
        let notional_micro = params.max_quote_micro.min(capital.0).max(0);
        let token_market = token_market_for(&tokens);
        let (merge_tx, merge_rx) = mpsc::unbounded_channel();
        let (redeem_tx, redeem_rx) = mpsc::unbounded_channel();
        let mm = MmLoop {
            venue,
            qm: QuoteManager::new(),
            inv: InventoryRisk::new(inv_cfg),
            positions: PositionBook::default(),
            fetcher,
            store_tx,
            status_tx,
            params,
            tokens,
            token_market,
            complement: HashMap::new(),
            policy: params.policy,
            reward_by_token: HashMap::new(),
            notional_micro,
            placed: HashMap::new(),
            token_ts: HashMap::new(),
            rebate_accrued_micro: 0,
            paused: false,
            halted: false,
            day: utc_day_from_ms(now_ms()),
            day_loss_halted: false,
            day_loss_read: None,
            no_naked_shorts: false,
            vetoed: HashSet::new(),
            last_reward_sample: None,
            reward_status: None,
            reward_cumulative_est: 0.0,
            signals: HashMap::new(),
            pulled_until: HashMap::new(),
            last_signal: 0.0,
            last_pulled: false,
            merge_live_warned: false,
            merger: None,
            cond_by_token: HashMap::new(),
            merge_tx,
            merge_rx,
            merge_inflight: HashSet::new(),
            last_merge_sweep: Instant::now(),
            venue_by_token: HashMap::new(),
            data_api: None,
            deposit_wallet: None,
            redeem_tx,
            redeem_rx,
            redeem_sweep_inflight: false,
            last_redeem_sweep: Instant::now(),
        };
        (mm, store_rx, status_rx)
    }

    // ── Live gating: venue selection (Task 4.5) ────────────────────────────────

    /// PAPER is the default and is NEVER backed by a live venue. For every
    /// `mm_use_live` combination that is NOT (process `--live`, `mm.live`) the
    /// predicate is false, so main constructs `MmStrategy` WITHOUT a live venue;
    /// and the paper constructor [`MmStrategy::new`] holds `live_venue == None`,
    /// so `run` builds a [`PaperMakerVenue`]. (Selection helper + the venue field
    /// — `main` is not run.)
    #[test]
    fn paper_path_never_selects_live_venue() {
        use crate::wiring::mm_use_live;
        for (process_live, mm_live) in [(false, false), (false, true), (true, false)] {
            assert!(
                !mm_use_live(process_live, mm_live),
                "({process_live}, {mm_live}) must NOT clear MM for live"
            );
        }
        // The paper construction main uses in those cases holds no LiveVenue.
        let tokens = vec![TokenId(1)];
        let mm = MmStrategy::new(
            tokens.clone(),
            token_market_for(&tokens),
            mk_params(200, 5.0),
            generous_inv(),
            Usdc(1_000_000),
        );
        assert!(
            mm.live_venue.is_none(),
            "the PAPER market maker must never hold a LiveVenue"
        );
    }

    /// Only `(process --live, mm.live)` clears MM for live, and the live path
    /// attaches an [`MmLive`] via `with_live_venue` — the call main makes ONLY
    /// behind `mm_use_live` — so `run` drives the SAME loop over the live venue
    /// rather than the paper one. (REST variant; the WS variant is covered by
    /// `ws_vs_rest_selection_picks_the_right_live_variant`.)
    #[test]
    fn cleared_for_live_attaches_live_venue() {
        use crate::wiring::mm_use_live;
        assert!(mm_use_live(true, true), "only (--live, mm.live) is live");
        let tokens = vec![TokenId(1)];
        let mm = MmStrategy::new(
            tokens.clone(),
            token_market_for(&tokens),
            mk_params(200, 5.0),
            generous_inv(),
            Usdc(1_000_000),
        )
        .with_live_venue(MmLive::Rest(shadow_live_venue()));
        assert!(
            matches!(mm.live_venue, Some(MmLive::Rest(_))),
            "with_live_venue (set only when mm_use_live) puts MM on the live venue"
        );
    }

    /// Task 6 wiring LOCK: the operator's `[reward_farm].size_skew_max_ratio`
    /// must flow through [`MmParams::from_config`] into the runtime params (where
    /// `reward_compute_quotes` reads it). A NON-default `1.5` proves the SIBLING
    /// `[reward_farm]` section is threaded — guarding against silently defaulting
    /// back to `2.0`. The default config still flows its own default.
    #[test]
    fn from_config_threads_reward_farm_size_skew_ratio() {
        let mm = pm_config::Mm::default();
        let rf = pm_config::RewardFarm {
            size_skew_max_ratio: 1.5,
            // Task 11: the estimator cadence is threaded through the SAME seam.
            sample_interval_ms: 30_000,
            ..Default::default()
        };
        let params = MmParams::from_config(&mm, &rf).expect("from_config");
        assert_eq!(params.size_skew_max_ratio, 1.5, "operator ratio reaches the loop");
        assert_eq!(
            params.sample_interval,
            Duration::from_millis(30_000),
            "operator estimator cadence reaches the loop"
        );

        // Default config flows the validated default (not a stale hardcoded 2.0).
        let params_def =
            MmParams::from_config(&pm_config::Mm::default(), &pm_config::RewardFarm::default())
                .expect("from_config default");
        assert_eq!(
            params_def.size_skew_max_ratio,
            pm_config::RewardFarm::default().size_skew_max_ratio
        );
        assert_eq!(
            params_def.sample_interval,
            Duration::from_millis(pm_config::RewardFarm::default().sample_interval_ms)
        );
    }

    /// Task 4.6 venue SELECTION: `MmFillsSource::from_config` maps the validated
    /// config string to the source, and main builds the matching [`MmLive`]
    /// variant — `"ws"` → [`MmLive::Ws`] (the live maker venue paired with the
    /// user-WS fills via a [`SplitVenue`]), `"rest"` → [`MmLive::Rest`] (the
    /// Task-4.5 `LiveVenue` REST poll). NO network: the WS source is built over a
    /// parked MOCK transport, and the maker venue is a shadow `LiveVenue`.
    #[tokio::test]
    async fn ws_vs_rest_selection_picks_the_right_live_variant() {
        use pm_execution::secrets::{ApiCreds, Secret};
        use pm_execution::venue::VenueError;

        // The pure selection: validated string → source.
        assert_eq!(MmFillsSource::from_config("ws"), MmFillsSource::Ws);
        assert_eq!(MmFillsSource::from_config("rest"), MmFillsSource::Rest);

        // A WsTransport that never sends/receives (parks) — ZERO network.
        struct ParkTransport;
        impl pm_execution::user_ws::WsTransport for ParkTransport {
            async fn recv(&mut self) -> Option<Result<String, VenueError>> {
                std::future::pending().await
            }
            async fn send(&mut self, _text: &str) -> Result<(), VenueError> {
                Ok(())
            }
        }
        let creds = ApiCreds {
            key: "k".into(),
            secret: Secret::new("s".into()),
            passphrase: Secret::new("p".into()),
        };

        let tokens = vec![TokenId(1)];

        // "ws" → build the user-WS source over the mock + pair it with the live
        // maker venue in a SplitVenue → MmLive::Ws.
        let ws_fills = LiveUserWsFills::with_transport_factory(
            creds,
            vec!["0xcond".into()],
            HashMap::new(),
            || async { Ok::<_, VenueError>(ParkTransport) },
        );
        let split = SplitVenue::new(shadow_live_venue(), ws_fills);
        let mm_ws = MmStrategy::new(
            tokens.clone(),
            token_market_for(&tokens),
            mk_params(200, 5.0),
            generous_inv(),
            Usdc(1_000_000),
        )
        .with_live_venue(MmLive::Ws(split));
        assert!(
            matches!(mm_ws.live_venue, Some(MmLive::Ws(_))),
            "live_fills_source=\"ws\" must select the user-WS SplitVenue variant"
        );

        // "rest" → the Task-4.5 LiveVenue REST poll → MmLive::Rest.
        let mm_rest = MmStrategy::new(
            tokens.clone(),
            token_market_for(&tokens),
            mk_params(200, 5.0),
            generous_inv(),
            Usdc(1_000_000),
        )
        .with_live_venue(MmLive::Rest(shadow_live_venue()));
        assert!(
            matches!(mm_rest.live_venue, Some(MmLive::Rest(_))),
            "live_fills_source=\"rest\" must select the REST LiveVenue variant"
        );
    }

    // ── Pure quote math ───────────────────────────────────────────────────────

    /// mid 0.50, spread_bps 200 → bid 0.49 / ask 0.51 (symmetric), both postOnly
    /// Gtc, each sized by `max_quote_usd / price`.
    #[test]
    fn compute_quotes_symmetric_around_mid() {
        let book = mid50_book(); // mid 0.50
        let params = mk_params(200, 5.0);
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        let bid = bid.expect("bid");
        let ask = ask.expect("ask");

        assert_eq!(bid.price, px(49), "bid = mid − half = 0.49");
        assert_eq!(ask.price, px(51), "ask = mid + half = 0.51");
        assert_eq!(bid.side, Side::Bid);
        assert_eq!(ask.side, Side::Ask);
        // Symmetric: 49 and 51 are equidistant from the mid tick 50.
        assert_eq!(50 - bid.price.get(), ask.price.get() - 50);
        // postOnly Gtc (a maker never wants to take).
        assert!(bid.post_only && ask.post_only);
        assert_eq!(bid.order_type, OrderType::Gtc);
        assert_eq!(ask.order_type, OrderType::Gtc);
        // Size clamped by max_quote_usd: notional / price (µshares), then
        // quantised DOWN to a 10_000-µshare grain (0.01 share, Cent) so the
        // venue's amount-precision + tick-grid rules all hold.
        assert_eq!(bid.size, Qty(5_000_000 * 1_000_000 / 490_000 / 10_000 * 10_000));
        assert_eq!(ask.size, Qty(5_000_000 * 1_000_000 / 510_000 / 10_000 * 10_000));
    }

    /// REGRESSION (two live rejections): an arbitrary notional/price gives a
    /// µshare size that (1) drifts off the 0.01 price grid
    /// (5_000_000 / 10_416_666 = 0.48000003…) and (2) exceeds the venue's
    /// amount-precision (shares ≤ 2 dec, USDC ≤ 4 dec). The 10_000-µshare grain
    /// fixes all three: shares land on 0.01 and `price_micro · size` is a whole,
    /// ≤4-decimal µUSDC, so makerAmount is a multiple of 100 µ and
    /// makerAmount/takerAmount is exactly the tick price.
    #[test]
    fn quote_order_size_satisfies_venue_amount_rules() {
        for tick in [1u16, 7, 33, 48, 49, 99] {
            let o = quote_order(TokenId(1), Side::Bid, px(tick), TickSize::Cent, 5_000_000)
                .expect("quote");
            let price_micro = u128::from(tick) * 10_000;
            let size = u128::from(o.size.0);
            // takerAmount (shares) ≤ 2 decimals → multiple of 0.01 share = 10_000 µ.
            assert_eq!(size % 10_000, 0, "tick {tick}: shares exceed 2 decimals");
            // price·size % 1e8 == 0 ⟹ makerAmount = price·size/1e6 is a whole µUSDC
            // AND a multiple of 100 µ (≤ 4 decimals) AND maker/taker = the tick px.
            assert_eq!(
                (price_micro * size) % 100_000_000,
                0,
                "tick {tick}: makerAmount off the 4-decimal / tick grid"
            );
        }
    }

    /// REGRESSION (live "Size (N) lower than the minimum: 5"): a quote whose
    /// `max_quote_usd / price` can't afford 5 shares is SKIPPED, not placed to be
    /// rejected. (max_quote_usd = $1 on a > $0.20 market was the flood we hit.)
    #[test]
    fn quote_order_skips_below_venue_min_size() {
        // $1 at $0.90 → ~1 share (< 5) → no order.
        assert!(
            quote_order(TokenId(1), Side::Bid, px(90), TickSize::Cent, 1_000_000).is_none(),
            "$1 / $0.90 ≈ 1 share is below the 5-share venue minimum"
        );
        // $1 at $0.10 → 10 shares (≥ 5) → an order.
        assert!(
            quote_order(TokenId(1), Side::Bid, px(10), TickSize::Cent, 1_000_000).is_some(),
            "$1 / $0.10 = 10 shares clears the minimum"
        );
        // $5 at $0.90 → ~5.5 shares (≥ 5) → an order.
        assert!(
            quote_order(TokenId(1), Side::Ask, px(90), TickSize::Cent, 5_000_000).is_some(),
            "$5 / $0.90 ≈ 5.5 shares clears the minimum"
        );
    }

    /// Live venues have NO naked shorts: a FLAT MM quotes BID-ONLY — the ask is
    /// skipped until a bid fills and gives it inventory to offload. (Paper keeps
    /// quoting both sides; this is gated on the `no_naked_shorts` venue flag.)
    #[tokio::test]
    async fn no_naked_shorts_quotes_bid_only_when_flat() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let (mut mm, _store_rx, _status_rx) = build_loop(
            fetcher,
            generous_inv(),
            mk_params(200, 5.0),
            tokens,
            Usdc(1_000_000_000),
        );
        mm.no_naked_shorts = true; // emulate the live CLOB capability

        mm.quote().await;
        assert!(
            mm.qm.tracked().contains_key(&(TokenId(1), Side::Bid)),
            "flat → bid rests"
        );
        assert!(
            !mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)),
            "flat + no naked shorts → ask skipped (nothing to sell)"
        );
    }

    /// The restart/offload path: with a seeded LONG (resumed inventory), the
    /// no-naked-shorts MM DOES quote an ask — to offload what it holds — so a
    /// position carried across a restart gets worked off, not stranded.
    #[tokio::test]
    async fn no_naked_shorts_quotes_ask_when_seeded_long() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let (mut mm, _store_rx, _status_rx) = build_loop(
            fetcher,
            generous_inv(),
            mk_params(200, 5.0),
            tokens,
            Usdc(1_000_000_000),
        );
        mm.no_naked_shorts = true;
        // Resume a 20-share long (cost $10) — the seed a restart applies.
        mm.inv.seed(TokenId(1), 20_000_000, Usdc(10_000_000));

        mm.quote().await;
        assert!(
            mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)),
            "a seeded long → ask quotes to offload it"
        );
    }

    /// SAFETY GATE: a MM started PAUSED (live HELD — TUI before `l`) must NOT
    /// quote on its first tick; only after release (`SetPaused(false)`) does it
    /// place real orders. Mirrors `with_start_paused` + the host release path.
    #[tokio::test]
    async fn start_paused_holds_quoting_until_released() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let (mut mm, _store_rx, _status_rx) = build_loop(
            fetcher,
            generous_inv(),
            mk_params(200, 5.0),
            tokens,
            Usdc(1_000_000_000),
        );
        mm.paused = true; // live HELD (what `start_paused` sets at init)

        mm.tick().await;
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "a HELD MM must place nothing until released"
        );

        // Release → the next tick quotes for real.
        mm.paused = false;
        mm.tick().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "released MM quotes bid + ask"
        );
    }

    /// A spread so wide it pushes a side outside the interior tick range yields
    /// no quote (the token is skipped).
    #[test]
    fn compute_quotes_skips_when_out_of_range() {
        let book = mid50_book();
        let params = mk_params(20_000, 5.0); // half = 100 ticks → out of [1, 99]
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        assert!(bid.is_none() && ask.is_none());
    }

    /// A one-sided book (no ask) cannot form a mid → no quote.
    #[test]
    fn compute_quotes_skips_one_sided_book() {
        let book = cent_book(&[(48, 100 * SH)], &[]);
        let params = mk_params(200, 5.0);
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        assert!(bid.is_none() && ask.is_none());
    }

    /// A sub-tick spread still yields a strictly non-crossing quote: the bid/ask
    /// are bumped at least one tick apart (never narrower).
    #[test]
    fn compute_quotes_never_crosses_on_tiny_spread() {
        let book = mid50_book();
        let params = mk_params(1, 5.0); // 1 bp ≪ one tick
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        let bid = bid.expect("bid");
        let ask = ask.expect("ask");
        assert!(
            ask.price.get() > bid.price.get(),
            "ask must stay strictly above bid: {} / {}",
            bid.price.get(),
            ask.price.get()
        );
    }

    // ── RewardFarm balanced size-skew (Task 6, spec §8.3) ──────────────────────

    /// A LONG net leans the SIZES — bigger ask (to sell down), smaller bid —
    /// within the ≤2:1 ratio and the reward `min_size` floor, while the PRICES
    /// stay pinned tight to the reward band (bid at the mid, ask one tick up).
    /// Exercises the full path through `quote_order`'s grain quantization.
    #[test]
    fn reward_compute_quotes_long_skews_ask_size_bigger_prices_tight() {
        // Wide Cent book (best 0.40 / 0.60, mid 0.50) → tight two-sided reward
        // quotes; $50 per-side notional, $1000 per-market inventory cap.
        let book = cent_book(&[(40, 100 * SH)], &[(60, 100 * SH)]);
        let notional = 50_000_000; // $50 → base 100 sh @ the 0.50 mid
        let max_inv = 1_000_000_000; // $1000 → cap 2000 sh @ 0.50
        let net = 1_000 * SH as i128; // +1000 sh long → r = 1000/2000 = 0.5
        let (bid, ask) =
            reward_compute_quotes(&book, TokenId(1), notional, 3.0, net, max_inv, 2.0, 5.0);
        let bid = bid.expect("bid");
        let ask = ask.expect("ask");
        // SIZES lean against the long: bigger ask, within the ≤2:1 cap, both ≥ min.
        assert!(ask.size.0 > bid.size.0, "long → bigger ask size");
        assert!(
            (ask.size.0 as f64) / (bid.size.0 as f64) <= 2.0 + 1e-9,
            "two-sided ratio within max_ratio: {} / {}",
            ask.size.0,
            bid.size.0
        );
        assert!(bid.size.0 >= MM_MIN_ORDER_SHARES_MICRO as u64, "bid ≥ venue min");
        assert!(ask.size.0 >= MM_MIN_ORDER_SHARES_MICRO as u64, "ask ≥ venue min");
        // PRICES stay tight (NOT skewed): bid at the mid tick, ask one tick up.
        assert_eq!(bid.price, px(50), "bid pinned at the tight reward band");
        assert_eq!(ask.price, px(51), "ask one tick up (post-only non-cross)");
    }

    /// Flat inventory → EQUAL share sizes on both sides (delta-neutral base),
    /// even though the bid and ask sit at different prices (the size, not the
    /// notional, is balanced — what the quadratic reward score rewards).
    #[test]
    fn reward_compute_quotes_flat_is_balanced_in_shares() {
        let book = cent_book(&[(40, 100 * SH)], &[(60, 100 * SH)]);
        let (bid, ask) =
            reward_compute_quotes(&book, TokenId(1), 50_000_000, 3.0, 0, 1_000_000_000, 2.0, 5.0);
        let bid = bid.expect("bid");
        let ask = ask.expect("ask");
        assert_eq!(bid.size.0, ask.size.0, "flat → balanced share sizes");
    }

    /// A NET SHORT leans the other way — bigger BID (to buy back) — confirming
    /// the lean direction flips with the sign of inventory.
    #[test]
    fn reward_compute_quotes_short_skews_bid_size_bigger() {
        let book = cent_book(&[(40, 100 * SH)], &[(60, 100 * SH)]);
        let net = -(1_000 * SH as i128); // short → bigger bid
        let (bid, ask) =
            reward_compute_quotes(&book, TokenId(1), 50_000_000, 3.0, net, 1_000_000_000, 2.0, 5.0);
        let bid = bid.expect("bid");
        let ask = ask.expect("ask");
        assert!(bid.size.0 > ask.size.0, "short → bigger bid size");
    }

    // ── RewardFarm microprice fair value + size-cutoff (Task A2, spec §8.1) ─────

    /// The reward fair value is the size-weighted microprice, and a top level
    /// resting BELOW the reward `min_incentive_size` is dropped from the
    /// weighting (spec §8.1, closes Spec-1 deferral I2) so a sub-incentive level
    /// can't define our fair value. With one side dropped the microprice resolves
    /// via the surviving side; with BOTH dropped it falls back to the raw mid.
    #[test]
    fn reward_fair_value_microprice_and_size_cutoff() {
        // Wide Cent book bid 0.50 / ask 0.60 (raw mid 0.55); min_incentive_size 5 sh.

        // (a) Both tops are REAL (≥ 5 sh) and the bid is heavier → microprice
        //     leans UP toward the ask, strictly ABOVE the raw 0.55 mid.
        let book = cent_book(&[(50, 300 * SH)], &[(60, 100 * SH)]);
        let fv = reward_fair_value(&book, TickSize::Cent, 5.0).expect("two-sided");
        assert!(fv > 0.55 && fv < 0.60, "bid-heavy microprice leans up past mid: {fv}");

        // (b) A sub-min top BID (4 sh < 5) is dropped → its qty is 0 in the
        //     weighting, so it no longer skews fair value: the microprice resolves
        //     via the surviving ask to the bid price 0.50 (NOT the raw 0.55 mid).
        let book = cent_book(&[(50, 4 * SH)], &[(60, 100 * SH)]);
        let fv = reward_fair_value(&book, TickSize::Cent, 5.0).expect("two-sided");
        assert!((fv - 0.50).abs() < 1e-9, "sub-min bid dropped → resolves to bid, got {fv}");

        // (c) BOTH tops sub-min (4 sh / 2 sh) → both dropped → fall back to the
        //     raw midpoint 0.55.
        let book = cent_book(&[(50, 4 * SH)], &[(60, 2 * SH)]);
        let fv = reward_fair_value(&book, TickSize::Cent, 5.0).expect("two-sided");
        assert!((fv - 0.55).abs() < 1e-9, "both tops sub-min → raw mid, got {fv}");

        // A one-sided book has no mid to anchor on → None (callers skip the token).
        let one_sided = cent_book(&[(50, 100 * SH)], &[]);
        assert!(
            reward_fair_value(&one_sided, TickSize::Cent, 5.0).is_none(),
            "one-sided book ⇒ None"
        );
    }

    // ── RewardFarm sticky re-quoting (Task 8, anti-flicker) ─────────────────────

    /// Task 8 (mm-loop level): under `RewardFarm`, a resting quote whose fresh
    /// target price is still WITHIN `requote_band_ticks` must NOT be re-quoted —
    /// a replace is a cancel+place that resets Polymarket's time-weighted reward
    /// score. We place a two-sided reward quote, nudge the book ONE tick (so each
    /// side's new target lands exactly one tick away, inside the 1-tick band), and
    /// re-quote: both sides must stay put — same resting PRICE and same venue id.
    /// The paper venue re-keys a replaced order, so an UNCHANGED id is a direct,
    /// message-independent proof that NO cancel/replace happened (the count stays 0).
    #[tokio::test]
    async fn rewardfarm_sticky_requote_keeps_in_band_resting_order() {
        fn resting_px(qm: &QuoteManager, side: Side) -> Px {
            qm.resting_orders()
                .into_iter()
                .find(|(_, o)| o.side == side)
                .map(|(_, o)| o.price)
                .expect("a resting order for this side")
        }

        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // `requote_band_ticks` defaults to 1 in `mk_params`; $5 per-side quote.
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        // Drive the REWARD-FARM path with a reward-eligible 3¢ band (min_size 1 sh)
        // for token 1; `build_loop` defaults to SpreadCapture / empty metrics.
        mm.policy = Policy::RewardFarm;
        mm.reward_by_token.insert(TokenId(1), (1.0, 3.0, 100.0));

        // Cycle 1 (bid 0.48 / ask 0.52 → mid 0.50): the tight two-sided reward
        // quote rests at bid 0.50 / ask 0.51.
        mm.quote().await;
        let bid_id0 = mm.qm.tracked().get(&(TokenId(1), Side::Bid)).cloned().expect("bid placed");
        let ask_id0 = mm.qm.tracked().get(&(TokenId(1), Side::Ask)).cloned().expect("ask placed");
        assert_eq!(resting_px(&mm.qm, Side::Bid), px(50), "cycle-1 bid rests at 0.50");
        assert_eq!(resting_px(&mm.qm, Side::Ask), px(51), "cycle-1 ask rests at 0.51");

        // Nudge the book up ONE tick (bid 0.49 / ask 0.53 → mid 0.51). The fresh
        // reward target becomes bid 0.51 / ask 0.52 — each exactly one tick from
        // its resting quote, i.e. INSIDE the 1-tick band (would-replace without
        // the sticky gate).
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(49, 100 * SH)], &[(53, 100 * SH)]), true));

        // Cycle 2: STICKY — both in-band sides are kept verbatim, so reconcile
        // value-compares equal and issues NO cancel/replace.
        mm.quote().await;
        assert_eq!(
            mm.qm.tracked().get(&(TokenId(1), Side::Bid)).cloned(),
            Some(bid_id0),
            "in-band bid must NOT be re-quoted (unchanged id ⇒ no cancel+place)"
        );
        assert_eq!(
            mm.qm.tracked().get(&(TokenId(1), Side::Ask)).cloned(),
            Some(ask_id0),
            "in-band ask must NOT be re-quoted (unchanged id ⇒ no cancel+place)"
        );
        assert_eq!(resting_px(&mm.qm, Side::Bid), px(50), "bid stays at its resting 0.50, not the 0.51 target");
        assert_eq!(resting_px(&mm.qm, Side::Ask), px(51), "ask stays at its resting 0.51, not the 0.52 target");
    }

    /// Task 8 isolation LOCK: `SpreadCapture` must be UNAFFECTED by the sticky
    /// gate. The same one-tick book nudge that the reward path holds through must
    /// still RE-QUOTE under SpreadCapture (its prices track the mid every cycle),
    /// so the resting quote moves and the venue id re-keys (a replace happened).
    #[tokio::test]
    async fn spreadcapture_still_requotes_on_tick_move() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0); // SpreadCapture (mk_params default)
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        // Cycle 1 (mid 0.50): symmetric spread quote bid 0.49 / ask 0.51.
        mm.quote().await;
        let bid_id0 = mm.qm.tracked().get(&(TokenId(1), Side::Bid)).cloned().expect("bid placed");

        // Same one-tick nudge as the sticky test (mid 0.50 → 0.51).
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(49, 100 * SH)], &[(53, 100 * SH)]), true));

        // Cycle 2: SpreadCapture re-quotes (no stickiness) — the bid moves and the
        // venue re-keys it, proving the Task-8 gate did not leak into this path.
        mm.quote().await;
        let bid_id1 = mm.qm.tracked().get(&(TokenId(1), Side::Bid)).cloned().expect("bid still resting");
        assert_ne!(bid_id1, bid_id0, "SpreadCapture must re-quote on a tick move (id re-keys)");
    }

    // ── RewardFarm size-rebalance sticky trigger (Spec-2 §8.4b) ─────────────────

    /// Spec-2 §8.4(b): the sticky keep must ALSO re-place a resting reward side
    /// when the inventory-implied SIZE lean has drifted past `size_rebalance_pct`
    /// — restoring the delta-neutral skew — even while the PRICE stays in band
    /// (so the Spec-1 price-drift gate alone would have kept it). We hold the book
    /// STATIC (price target never moves) and shift inventory between cycles: a
    /// large shift re-leans `skewed_sizes` enough to replace; a small shift stays
    /// sticky. The paper venue re-keys a replaced order, so a CHANGED id proves a
    /// cancel+place happened and an UNCHANGED id proves it did not — the same
    /// message-independent technique as
    /// `rewardfarm_sticky_requote_keeps_in_band_resting_order`.
    #[tokio::test]
    async fn rewardfarm_size_rebalance_replaces_in_band_on_size_drift() {
        // One scenario: cycle 1 FLAT (balanced 10-share quote), then SEED a long
        // and re-quote on the SAME book. Returns whether each side's venue id
        // re-keyed across the two cycles (`true` ⇒ a replace happened).
        async fn ids_change_after_seed(seed_net_shares: i128) -> (bool, bool) {
            let tokens = vec![TokenId(1)];
            // Static in-band book (bid 0.48 / ask 0.52 → mid 0.50) served unchanged
            // both cycles, so the PRICE target never drifts (the Spec-1 gate stays
            // put) and ONLY the size trigger can drive a replace.
            let (fetcher, _shared) =
                controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
            let mut params = mk_params(200, 5.0);
            params.policy = Policy::RewardFarm;
            // A 4:1 lean cap pushes the per-side size drift COMFORTABLY across (big
            // seed) / under (small seed) the 0.25 `size_rebalance_pct` on BOTH
            // sides, so the id assertions aren't margin-fragile. The rebalance pct
            // stays at its 0.25 default.
            params.size_skew_max_ratio = 4.0;
            // generous_inv(): $1000 per-market cap ⇒ 2000-share cap at the 0.50 mid;
            // $5 per-side quote ⇒ a 10-share balanced base when flat.
            let (mut mm, _store_rx, _status_rx) =
                build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            mm.reward_by_token.insert(TokenId(1), (1.0, 3.0, 100.0));

            // Cycle 1 (flat → balanced): bid 0.50 / ask 0.51, 10 shares each.
            mm.quote().await;
            let bid0 =
                mm.qm.tracked().get(&(TokenId(1), Side::Bid)).cloned().expect("cycle-1 bid");
            let ask0 =
                mm.qm.tracked().get(&(TokenId(1), Side::Ask)).cloned().expect("cycle-1 ask");

            // SHIFT inventory: a long re-leans the fresh quote (bigger ask to sell
            // down, smaller bid) so the target sizes now differ from the resting
            // balanced 10/10. The tiny bid buy stays well inside the $1000 cap.
            mm.inv.seed(TokenId(1), seed_net_shares * SH as i128, Usdc(0));

            // Cycle 2 (SAME book ⇒ price in band): only the size trigger is in play.
            mm.quote().await;
            let bid1 =
                mm.qm.tracked().get(&(TokenId(1), Side::Bid)).cloned().expect("cycle-2 bid");
            let ask1 =
                mm.qm.tracked().get(&(TokenId(1), Side::Ask)).cloned().expect("cycle-2 ask");
            (bid1 != bid0, ask1 != ask0)
        }

        // (1) LARGE long: 1900 sh = r 0.95 of the 2000-sh cap ⇒ ~1.93× lean, so
        // the bid shrinks ~48% and the ask grows ~93% — both past the 25% band ⇒
        // the in-band sides ARE re-placed (ids re-key) to restore the lean.
        let (bid_replaced, ask_replaced) = ids_change_after_seed(1900).await;
        assert!(
            bid_replaced,
            "size drift > pct must re-place the bid even though the price is in band"
        );
        assert!(
            ask_replaced,
            "size drift > pct must re-place the ask even though the price is in band"
        );

        // (2) SMALL long: 400 sh = r 0.2 ⇒ ~1.15× lean, so bid ~13% / ask ~15%
        // drift — both UNDER the 25% band ⇒ the sticky keep holds, ids stable
        // (no cancel+place churn from a sub-threshold lean wobble).
        let (bid_replaced, ask_replaced) = ids_change_after_seed(400).await;
        assert!(
            !bid_replaced,
            "sub-threshold size drift must KEEP the bid sticky (id stable)"
        );
        assert!(
            !ask_replaced,
            "sub-threshold size drift must KEEP the ask sticky (id stable)"
        );
    }

    // ── RewardFarm paper integration (Task 11, spec §9/§15) ─────────────────────

    /// PRIORITY-2 paper integration (spec §9/§15): drive a RewardFarm `MmLoop`
    /// over the `PaperMakerVenue` on a synthetic ~mid-0.50 book with real reward
    /// params (min_size 5 sh, max_spread 3¢, $100/day) and assert the spec's
    /// paper success criteria end to end in one quote cycle:
    ///   (a) BOTH a bid and an ask are placed (two-sided),
    ///   (b) both within `max_spread` of the mid (in-band),
    ///   (c) sizes are balanced when flat,
    ///   (d) a second cycle on the UNCHANGED book does NOT replace (sticky), and
    ///   (e) the PUBLISHED estimator reports `q_min > 0` (a scoring position).
    /// Drives the whole loop via [`MmLoop::tick`] so the estimator sample +
    /// status publish are exercised exactly as in production.
    #[tokio::test]
    async fn reward_farm_paper_quotes_in_band_two_sided_balanced() {
        let tokens = vec![TokenId(1)];
        // `_shared` is never mutated: the book stays at mid 0.50 for both cycles,
        // which is what makes cycle 2 the sticky no-replace case (d).
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // RewardFarm with the default 1-tick sticky band + 60s estimator cadence
        // (the first cycle samples regardless of the interval). $5 per-side quote,
        // ample capital + inventory caps so flat stays balanced (no size lean).
        let mut params = mk_params(200, 5.0);
        params.policy = Policy::RewardFarm;
        let (mut mm, _store_rx, status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        // Reward params for token 1: min_size 5 sh, max_spread 3¢, $100/day —
        // set on the loop's `reward_by_token` exactly as the other mm tests do.
        mm.reward_by_token.insert(TokenId(1), (5.0, 3.0, 100.0));

        // Cycle 1: quote → consume (no cross → no fills) → sample → publish.
        mm.tick().await;

        // (a) TWO-SIDED: both a bid and an ask rest (capture ids for the sticky
        // check (d) — the paper venue re-keys a replaced order, so an UNCHANGED
        // id is message-independent proof that no cancel+place happened).
        let bid_id0 = mm
            .qm
            .tracked()
            .get(&(TokenId(1), Side::Bid))
            .cloned()
            .expect("a bid is placed (two-sided)");
        let ask_id0 = mm
            .qm
            .tracked()
            .get(&(TokenId(1), Side::Ask))
            .cloned()
            .expect("an ask is placed (two-sided)");

        let resting: HashMap<Side, MakerOrder> = mm
            .qm
            .resting_orders()
            .into_iter()
            .map(|(_, o)| (o.side, o))
            .collect();
        let bid = resting.get(&Side::Bid).expect("resting bid");
        let ask = resting.get(&Side::Ask).expect("resting ask");

        // (b) IN-BAND: |price − mid| ≤ max_spread (3¢). Mid = 0.50 on this book.
        let mid = 0.50_f64;
        let max_spread = 0.03_f64;
        let bid_px = bid.price.microusdc(TickSize::Cent) as f64 / 1_000_000.0;
        let ask_px = ask.price.microusdc(TickSize::Cent) as f64 / 1_000_000.0;
        assert!(
            (bid_px - mid).abs() <= max_spread + 1e-9,
            "bid within max_spread of mid: {bid_px}"
        );
        assert!(
            (ask_px - mid).abs() <= max_spread + 1e-9,
            "ask within max_spread of mid: {ask_px}"
        );

        // (c) BALANCED when flat: equal SHARE sizes (the quadratic reward score
        // rewards size balance, not notional balance).
        assert_eq!(bid.size.0, ask.size.0, "flat → balanced share sizes");

        // (e) PUBLISHED estimator (RewardFarm only): q_min > 0 ⇒ a two-sided
        // scoring position; in_band set; a funded market ⇒ a positive $/day.
        let rf = status_rx
            .borrow()
            .reward_farm
            .expect("RewardFarm publishes reward telemetry");
        assert!(rf.q_min > 0.0, "published q_min must be > 0, got {}", rf.q_min);
        assert!(rf.in_band, "two-sided in-band quotes ⇒ in_band");
        assert!(
            rf.est_reward_usd_day > 0.0,
            "a funded reward market ⇒ est $/day > 0, got {}",
            rf.est_reward_usd_day
        );
        assert!(
            rf.cumulative_est >= rf.est_reward_usd_day - 1e-9,
            "cumulative est accrues the sample"
        );

        // (d) STICKY: a second cycle on the UNCHANGED book keeps both sides
        // verbatim — unchanged venue ids prove NO cancel+place (which would reset
        // Polymarket's time-weighted reward score).
        mm.tick().await;
        assert_eq!(
            mm.qm.tracked().get(&(TokenId(1), Side::Bid)).cloned(),
            Some(bid_id0),
            "in-band bid must NOT be re-quoted on an unchanged book (sticky)"
        );
        assert_eq!(
            mm.qm.tracked().get(&(TokenId(1), Side::Ask)).cloned(),
            Some(ask_id0),
            "in-band ask must NOT be re-quoted on an unchanged book (sticky)"
        );
    }

    // ── RewardFarm Phase-B complement-pair bid quoting (Task B2, spec §5.1) ─────

    /// Spec-2 Phase B (§5.1, closes M3): with `hedging_enabled`, a FLAT RewardFarm
    /// MM quotes the COMPLEMENT PAIR — a BID on YES and a BID on NO (both buys) —
    /// and NO ask on either. A flat MM has no inventory to sell and Polymarket has
    /// no naked short, so the ask leg is dropped; the second side the reward score
    /// reads is the complement BID (the `m`/`m'` books). The complement map is set
    /// exactly as main builds it under hedging (B3/B4 pair the two bids).
    #[tokio::test]
    async fn rewardfarm_hedging_quotes_bid_on_both_complement_tokens() {
        let yes = TokenId(1);
        let no = TokenId(2);
        let tokens = vec![yes, no];
        // Both outcomes have a valid two-sided book at mid 0.50 (a YES at
        // 0.48/0.52 ⇒ its NO complement is also 0.48/0.52), each quoted on its own.
        let (fetcher, _shared) = controllable_fetcher(
            &tokens,
            HashMap::from([(yes, (mid50_book(), true)), (no, (mid50_book(), true))]),
        );
        let mut params = mk_params(200, 5.0);
        params.policy = Policy::RewardFarm;
        params.hedging_enabled = true;
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        // Both outcomes are funded, reward-eligible (min_size 1 sh, 3¢, $100/day).
        mm.reward_by_token.insert(yes, (1.0, 3.0, 100.0));
        mm.reward_by_token.insert(no, (1.0, 3.0, 100.0));
        // The yes↔no complement map main wires under hedging (consumed by B3/B4).
        mm.complement.insert(yes, no);
        mm.complement.insert(no, yes);

        mm.quote().await;

        // A BID rests on BOTH complement tokens (two-sided-from-flat for the score).
        assert!(
            mm.qm.tracked().contains_key(&(yes, Side::Bid)),
            "a BID is placed on YES (complement pair)"
        );
        assert!(
            mm.qm.tracked().contains_key(&(no, Side::Bid)),
            "a BID is placed on NO (complement pair)"
        );
        // NO ask on either token — the ask leg is dropped under hedging (no naked
        // short; the complement bid is the second side).
        assert!(
            !mm.qm.tracked().contains_key(&(yes, Side::Ask)),
            "NO ask on YES under hedging (bid-only)"
        );
        assert!(
            !mm.qm.tracked().contains_key(&(no, Side::Ask)),
            "NO ask on NO under hedging (bid-only)"
        );
    }

    /// Converse / isolation LOCK: with `hedging_enabled = false`, RewardFarm keeps
    /// the Spec-1 single-token TWO-SIDED quote (bid + ask) on the one token — the
    /// Phase-B bid-only drop is gated strictly on `hedging_enabled`, so the default
    /// reward-farm path is byte-for-byte unchanged.
    #[tokio::test]
    async fn rewardfarm_non_hedging_keeps_single_token_bid_and_ask() {
        let yes = TokenId(1);
        let tokens = vec![yes];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(yes, (mid50_book(), true))]));
        let mut params = mk_params(200, 5.0);
        params.policy = Policy::RewardFarm;
        params.hedging_enabled = false; // Spec-1 single-token bid+ask
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        mm.reward_by_token.insert(yes, (1.0, 3.0, 100.0));

        mm.quote().await;

        assert!(
            mm.qm.tracked().contains_key(&(yes, Side::Bid)),
            "non-hedging RewardFarm still places the bid"
        );
        assert!(
            mm.qm.tracked().contains_key(&(yes, Side::Ask)),
            "non-hedging RewardFarm still places the ASK (Spec-1 single-token two-sided)"
        );
    }

    /// Spec-2 Phase B (Task B3): in hedging mode a market's reward score is
    /// two-sided ACROSS the complement pair — the YES bid scores on book `m`
    /// (Q1) and the NO bid on book `m'` (Q2). The estimator must therefore PAIR
    /// a market's two complement bids into ONE market-level `q_min`, NOT score
    /// each bid-only token alone (which would penalize each as single-sided at
    /// the 1/C floor). We rest a bid on YES and a bid on NO (the B2 hedging
    /// path), then assert the published `q_min` equals the two-sided
    /// `q_min(Q_bidYES, Q_bidNO, mid)` of the combined legs — strictly MORE than
    /// the single-sided sum `Q_bidYES/C + Q_bidNO/C` the per-token path yields.
    #[tokio::test]
    async fn rewardfarm_hedging_estimator_pairs_yes_no_into_qmin() {
        let yes = TokenId(1);
        let no = TokenId(2);
        let tokens = vec![yes, no];
        // Both outcomes sit at mid 0.50 (a YES 0.48/0.52 ⇒ its NO complement is
        // also 0.48/0.52), each with a valid two-sided book of its own.
        let (fetcher, _shared) = controllable_fetcher(
            &tokens,
            HashMap::from([(yes, (mid50_book(), true)), (no, (mid50_book(), true))]),
        );
        let mut params = mk_params(200, 5.0);
        params.policy = Policy::RewardFarm;
        params.hedging_enabled = true;
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        // Both outcomes funded + reward-eligible (min_size 1 sh, 3¢ band, $100/day).
        mm.reward_by_token.insert(yes, (1.0, 3.0, 100.0));
        mm.reward_by_token.insert(no, (1.0, 3.0, 100.0));
        // The yes↔no complement map main wires under hedging (consumed by B3).
        mm.complement.insert(yes, no);
        mm.complement.insert(no, yes);

        // B2 hedging path: a BID rests on BOTH complement tokens.
        mm.quote().await;

        // Per-leg score from the SAME primitives the estimator uses (order_score
        // against each token's reward_fair_value), so the assertion tracks the
        // exact tick each bid lands on rather than a hardcoded figure.
        let v = 3.0_f64;
        let adj_mid = reward_fair_value(&mid50_book(), TickSize::Cent, 1.0).expect("two-sided mid");
        let mut q_yes = 0.0_f64;
        let mut q_no = 0.0_f64;
        for (_, o) in mm.qm.resting_orders() {
            let price = o.price.microusdc(TickSize::Cent) as f64 / 1_000_000.0;
            let shares = o.size.0 as f64 / 1_000_000.0;
            let s = order_score(v, (price - adj_mid).abs() * 100.0) * shares;
            if o.token == yes {
                q_yes += s;
            } else if o.token == no {
                q_no += s;
            }
        }
        assert!(q_yes > 0.0 && q_no > 0.0, "both complement bids rest and score in-band");

        // PAIRED (desired): the two bids combine into one market Q_min.
        let expected_paired = q_min(q_yes, q_no, adj_mid);
        // SINGLE-SIDED (the old per-token path): each bid-only token scored alone
        // collapses to the 1/C floor, so the sum is strictly smaller.
        let single_sided = q_min(q_yes, 0.0, adj_mid) + q_min(0.0, q_no, adj_mid);

        let st = mm.sample_reward_estimate().await;
        assert!(
            (st.q_min - expected_paired).abs() < 1e-9,
            "hedging estimator must pair the complement bids: got q_min {}, expected paired {}",
            st.q_min,
            expected_paired
        );
        assert!(
            st.q_min > single_sided + 1e-9,
            "paired q_min {} must EXCEED the single-sided/penalized sum {} (NOT per-token scoring)",
            st.q_min,
            single_sided
        );
    }

    /// Spec-2 Phase B (Task B4): under hedging the quoting unit is the complement
    /// PAIR (bid-YES + bid-NO), so the two bid SIZES are leaned by the PAIR's net
    /// delta to drive it back toward delta-neutral — NOT by each token's own net
    /// in isolation. Net LONG YES (yes inventory > no) ⇒ the YES bid is sized
    /// SMALLER and the NO bid LARGER (bid LESS of the heavy leg, MORE of the light
    /// leg), the grown:shrunk ratio capped at `size_skew_max_ratio`; a FLAT pair
    /// quotes balanced bids. We compare a seeded run against a flat baseline: the
    /// per-token Spec-1 lean would leave the NO bid at the balanced base
    /// (net(no)=0 ⇒ no skew), so asserting the NO bid GROWS above the flat base is
    /// what pins the PAIR-aware sizing (and fails the per-token path).
    #[tokio::test]
    async fn rewardfarm_hedging_skews_bids_by_net_delta() {
        // Run one hedging cycle over a YES/NO pair (both at mid 0.50) seeded with
        // `seed_yes_shares` of LONG YES inventory; return the placed (YES bid, NO
        // bid) sizes in µshares.
        async fn pair_bid_sizes(seed_yes_shares: i128) -> (u64, u64) {
            let yes = TokenId(1);
            let no = TokenId(2);
            let tokens = vec![yes, no];
            let (fetcher, _shared) = controllable_fetcher(
                &tokens,
                HashMap::from([(yes, (mid50_book(), true)), (no, (mid50_book(), true))]),
            );
            let mut params = mk_params(200, 5.0);
            params.policy = Policy::RewardFarm;
            params.hedging_enabled = true;
            params.size_skew_max_ratio = 2.0;
            let (mut mm, _store_rx, _status_rx) =
                build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            mm.reward_by_token.insert(yes, (1.0, 3.0, 100.0));
            mm.reward_by_token.insert(no, (1.0, 3.0, 100.0));
            mm.complement.insert(yes, no);
            mm.complement.insert(no, yes);
            // generous_inv(): $1000 per-market cap ⇒ a 2000-share cap at the 0.50
            // mid, so 1000 sh of long YES is r 0.5 and the YES bid buy still fits.
            if seed_yes_shares != 0 {
                mm.inv.seed(yes, seed_yes_shares * SH as i128, Usdc(0));
            }
            mm.quote().await;
            let resting = mm.qm.resting_orders();
            let bid_of = |tok: TokenId| {
                resting
                    .iter()
                    .find(|(_, o)| o.token == tok && o.side == Side::Bid)
                    .map(|(_, o)| o.size.0)
            };
            (bid_of(yes).expect("YES bid placed"), bid_of(no).expect("NO bid placed"))
        }

        // FLAT pair ⇒ the two bids are balanced (equal base size).
        let (yb_flat, nb_flat) = pair_bid_sizes(0).await;
        assert_eq!(yb_flat, nb_flat, "flat pair ⇒ balanced YES/NO bids");

        // LONG YES (1000 sh, half the 2000-sh cap) ⇒ smaller YES bid, larger NO
        // bid, both vs the flat base, ratio within the 2:1 cap.
        let (yb, nb) = pair_bid_sizes(1000).await;
        assert!(yb < nb, "long YES ⇒ YES bid SMALLER than NO bid (rebalance)");
        assert!(yb < yb_flat, "long YES SHRINKS the YES bid below the flat base");
        assert!(
            nb > nb_flat,
            "long YES GROWS the NO bid above the flat base (PAIR-delta-neutral, not \
             the per-token lean that leaves net(no)=0 at base)"
        );
        let ratio = nb as f64 / yb as f64;
        assert!(
            ratio <= 2.0 + 1e-9,
            "grown:shrunk bid ratio {ratio} must stay within size_skew_max_ratio (2:1)"
        );
    }

    /// Spec-2 Phase B (Task B4): under hedging the quoting unit is the complement
    /// PAIR (bid-YES + bid-NO), so the adverse-selection quote-pull is PAIR-aware.
    /// The signal drives the pair off the YES book (the NO book is its mirror): a
    /// strong UP on YES (≡ DOWN on NO) endangers the NO bid (you'd buy NO right
    /// before it falls) ⇒ pull the NO bid, keep the YES bid; a strong DOWN on YES
    /// endangers the YES bid ⇒ pull the YES bid, keep the NO bid. Each leg's bid
    /// is pulled when its COMPLEMENT book is running UP, so a NEUTRAL own-book leg
    /// is STILL pulled (which the per-token Spec-1 pull — reading only the leg's
    /// OWN book — would miss; that is what this locks).
    #[tokio::test]
    async fn rewardfarm_hedging_pull_maps_up_yes_to_no_bid() {
        // Strong UP on YES: balanced TOP (clean 0.50 microprice ⇒ momentum 0) with
        // heavy bid DEPTH behind the touch ⇒ summed top-3 imbalance ~0.9 ⇒ blended
        // signal ~0.45. Its NO complement book is left NEUTRAL (mid 0.50), so ONLY
        // the PAIR-aware (complement-driven) pull can endanger the NO bid.
        fn yes_up() -> Book {
            cent_book(&[(48, 100 * SH), (47, 1000 * SH), (46, 1000 * SH)], &[(52, 100 * SH)])
        }
        // Strong DOWN on YES: the mirror — heavy ASK depth behind a balanced touch.
        fn yes_down() -> Book {
            cent_book(&[(48, 100 * SH)], &[(52, 100 * SH), (53, 1000 * SH), (54, 1000 * SH)])
        }

        // Build a fresh hedging loop over the pair with the given YES book and a
        // NEUTRAL NO book; run one cycle. Returns (yes_bid_quoting, no_bid_quoting).
        async fn run(yes_book: Book) -> (bool, bool) {
            let yes = TokenId(1);
            let no = TokenId(2);
            let tokens = vec![yes, no];
            let (fetcher, _shared) = controllable_fetcher(
                &tokens,
                HashMap::from([(yes, (yes_book, true)), (no, (mid50_book(), true))]),
            );
            let mut params = mk_params(200, 5.0);
            params.policy = Policy::RewardFarm;
            params.hedging_enabled = true;
            // imbalance/2 (~0.45) clears 0.3; the default 0.6 is unreachable on
            // imbalance alone (the blend halves it), per the Phase-A convention.
            params.pull_threshold = 0.3;
            let (mut mm, _store_rx, _status_rx) =
                build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            mm.reward_by_token.insert(yes, (1.0, 3.0, 100.0));
            mm.reward_by_token.insert(no, (1.0, 3.0, 100.0));
            mm.complement.insert(yes, no);
            mm.complement.insert(no, yes);
            mm.quote().await;
            (
                mm.qm.tracked().contains_key(&(yes, Side::Bid)),
                mm.qm.tracked().contains_key(&(no, Side::Bid)),
            )
        }

        // Strong UP on YES ⇒ NO bid PULLED (endangered), YES bid stays.
        let (yes_bid, no_bid) = run(yes_up()).await;
        assert!(yes_bid, "strong UP on YES keeps the (safe) YES bid");
        assert!(!no_bid, "strong UP on YES PULLS the NO bid (buying NO right before it falls)");

        // Strong DOWN on YES ⇒ YES bid PULLED, NO bid stays.
        let (yes_bid, no_bid) = run(yes_down()).await;
        assert!(!yes_bid, "strong DOWN on YES PULLS the YES bid");
        assert!(no_bid, "strong DOWN on YES keeps the (safe) NO bid");
    }

    // ── RewardFarm Phase-B complete-set merge (Task B5, spec §5.1) ──────────────

    /// Spec-2 Phase B (Task B5): under hedging the MM accumulates long YES AND
    /// long NO; a COMPLETE set `min(yes, no)` can be MERGED back to collateral —
    /// a set always redeems to exactly $1 — to RECYCLE the locked capital. On the
    /// PAPER venue (`no_naked_shorts = false`) the economics are modeled directly:
    /// both legs drop by the matched set count, the recovered `matched × $1`
    /// collateral is credited as cash, and gross inventory falls. On a LIVE venue
    /// (`no_naked_shorts = true`) the on-chain merge is unsupported (deferred to
    /// M6), so the step is a NO-OP — the pair is HELD (the gross cap is the
    /// control) and NO live merge is ever attempted. This locks BOTH behaviors.
    #[tokio::test]
    async fn merge_recycles_complete_set_in_paper() {
        // A RewardFarm+hedging loop over a YES/NO pair, seeded LONG on BOTH legs:
        // 100 YES @ $0.50 ($50 basis) + 80 NO @ $0.45 ($36 basis). The matched
        // set is min(100, 80) = 80; avg set cost $0.95 < $1 ⇒ recycling 80 sets
        // returns $80 collateral and books a small +$4 realized profit.
        fn seeded_pair(no_naked_shorts: bool) -> MmLoop<PaperMakerVenue<BookFetcher>> {
            let yes = TokenId(1);
            let no = TokenId(2);
            let tokens = vec![yes, no];
            let (fetcher, _shared) = controllable_fetcher(
                &tokens,
                HashMap::from([(yes, (mid50_book(), true)), (no, (mid50_book(), true))]),
            );
            let mut params = mk_params(200, 5.0);
            params.policy = Policy::RewardFarm;
            params.hedging_enabled = true;
            params.merge_threshold_usd = 5.0; // $5 floor; $80 matched clears it
            let (mut mm, _store_rx, _status_rx) =
                build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            mm.complement.insert(yes, no);
            mm.complement.insert(no, yes);
            mm.no_naked_shorts = no_naked_shorts;
            mm.inv.seed(yes, 100 * SH as i128, Usdc(50_000_000));
            mm.inv.seed(no, 80 * SH as i128, Usdc(36_000_000));
            mm
        }

        let yes = TokenId(1);
        let no = TokenId(2);
        let matched = 80 * SH as i128; // min(100, 80) = 80 complete sets

        // ── PAPER (no_naked_shorts = false): the complete set is recycled ───────
        let mut paper = seeded_pair(false);
        let gross_before = paper.inv.net(yes).abs() + paper.inv.net(no).abs();
        paper.maybe_merge_sets().await;
        // Both legs drop by the matched set count (NO goes flat; YES keeps its
        // unmatched 20-share surplus).
        assert_eq!(
            paper.inv.net(yes),
            100 * SH as i128 - matched,
            "YES leg reduced by the matched set count"
        );
        assert_eq!(
            paper.inv.net(no),
            80 * SH as i128 - matched,
            "NO leg reduced by the matched set count (to flat)"
        );
        // Recovered collateral = matched × $1 (a complete set redeems to exactly
        // $1): 80 shares × $1 = $80 = 80_000_000 µUSDC, credited as cash.
        assert_eq!(
            paper.positions.cash(),
            Usdc(80_000_000),
            "recovered matched × $1 collateral credited as cash"
        );
        // Aggregate realized = recovered − basis released = $80 − ($40 YES + $36
        // NO) = +$4 (the avg set cost was $0.95 < $1).
        let realized = paper.inv.realized(yes).0 + paper.inv.realized(no).0;
        assert_eq!(realized, 4_000_000, "merge books recovered − basis released = +$4");
        // Gross inventory FALLS — the locked capital is recycled.
        let gross_after = paper.inv.net(yes).abs() + paper.inv.net(no).abs();
        assert!(gross_after < gross_before, "gross inventory falls after the merge");
        assert_eq!(
            gross_after,
            20 * SH as i128,
            "only the unmatched 20-share YES surplus remains"
        );

        // ── LIVE (no_naked_shorts = true): NO-OP — the pair is HELD (M6) ────────
        let mut live = seeded_pair(true);
        live.maybe_merge_sets().await;
        assert_eq!(
            live.inv.net(yes),
            100 * SH as i128,
            "live: YES leg UNCHANGED (on-chain merge deferred to M6)"
        );
        assert_eq!(
            live.inv.net(no),
            80 * SH as i128,
            "live: NO leg UNCHANGED (pair held; gross cap is the control)"
        );
        assert_eq!(
            live.positions.cash(),
            Usdc(0),
            "live: NO collateral recovered (logged no-op)"
        );
        assert_eq!(
            live.inv.realized(yes).0 + live.inv.realized(no).0,
            0,
            "live: NO realized booked"
        );
    }

    /// Task B5 gates: the merge step skips below `merge_threshold_usd`, and is
    /// inert OUTSIDE RewardFarm+hedging — so SpreadCapture and non-hedging
    /// RewardFarm never recycle a set (their paths stay byte-for-byte unchanged).
    #[tokio::test]
    async fn merge_skips_below_threshold_and_outside_hedging() {
        fn loop_with(
            policy: Policy,
            hedging: bool,
            threshold_usd: f64,
            shares: i128,
        ) -> MmLoop<PaperMakerVenue<BookFetcher>> {
            let yes = TokenId(1);
            let no = TokenId(2);
            let tokens = vec![yes, no];
            let (fetcher, _shared) = controllable_fetcher(
                &tokens,
                HashMap::from([(yes, (mid50_book(), true)), (no, (mid50_book(), true))]),
            );
            let mut params = mk_params(200, 5.0);
            params.policy = policy;
            params.hedging_enabled = hedging;
            params.merge_threshold_usd = threshold_usd;
            let (mut mm, _store_rx, _status_rx) =
                build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            mm.complement.insert(yes, no);
            mm.complement.insert(no, yes);
            mm.inv.seed(yes, shares * SH as i128, Usdc(0));
            mm.inv.seed(no, shares * SH as i128, Usdc(0));
            mm
        }

        let yes = TokenId(1);

        // Below threshold: 3 matched sets × $1 = $3 ≤ $5 floor → skip.
        let mut below = loop_with(Policy::RewardFarm, true, 5.0, 3);
        below.maybe_merge_sets().await;
        assert_eq!(below.inv.net(yes), 3 * SH as i128, "below threshold → no merge");
        assert_eq!(below.positions.cash(), Usdc(0), "below threshold → no cash recycled");

        // Non-hedging RewardFarm: inert even with a mergeable $50 set.
        let mut non_hedging = loop_with(Policy::RewardFarm, false, 5.0, 50);
        non_hedging.maybe_merge_sets().await;
        assert_eq!(
            non_hedging.inv.net(yes),
            50 * SH as i128,
            "non-hedging RewardFarm → merge step inert"
        );
        assert_eq!(non_hedging.positions.cash(), Usdc(0));

        // SpreadCapture: inert (the merge step belongs to RewardFarm hedging only).
        let mut spread = loop_with(Policy::SpreadCapture, true, 5.0, 50);
        spread.maybe_merge_sets().await;
        assert_eq!(
            spread.inv.net(yes),
            50 * SH as i128,
            "SpreadCapture → merge step inert"
        );
        assert_eq!(spread.positions.cash(), Usdc(0));
    }

    // ── M6-7: live on-chain merge sweep — pure selection / apply / drain / gate ──

    /// M6-7 (pure): [`mergeable_pairs`] is the B5 selection — it returns ONLY the
    /// complement pairs whose matched complete set (`min(net(a), net(b))` long
    /// depth) clears the per-pair µUSDC floor, canonicalised in token-id order
    /// with the right merged `amount_micro`. A sub-threshold pair is excluded.
    #[test]
    fn mergeable_pairs_selects_complete_sets_over_threshold() {
        let (yes_a, no_a) = (TokenId(1), TokenId(2));
        let (yes_b, no_b) = (TokenId(3), TokenId(4));
        // Bidirectional complement map (yes↔no for each market), as main builds it.
        let complement = HashMap::from([
            (yes_a, no_a),
            (no_a, yes_a),
            (yes_b, no_b),
            (no_b, yes_b),
        ]);
        let mut inv = InventoryRisk::new(generous_inv());
        // Pair A: matched min(60, 50) = 50 sets = $50 — OVER the $5 floor.
        inv.seed(yes_a, 60 * SH as i128, Usdc(0));
        inv.seed(no_a, 50 * SH as i128, Usdc(0));
        // Pair B: matched min(3, 9) = 3 sets = $3 — UNDER the $5 floor.
        inv.seed(yes_b, 3 * SH as i128, Usdc(0));
        inv.seed(no_b, 9 * SH as i128, Usdc(0));

        let threshold_micro = 5 * SH as i128; // $5 floor
        let got = mergeable_pairs(&complement, &inv, threshold_micro);

        assert_eq!(got.len(), 1, "only the over-threshold pair is selected");
        assert_eq!(got[0].a, yes_a, "pair canonicalised in token-id order (a.0 <= b.0)");
        assert_eq!(got[0].b, no_a);
        assert_eq!(
            got[0].amount_micro,
            50 * SH as i128,
            "amount = matched min(60, 50) = 50 sets"
        );

        // A SHORT on either leg means there is no complete set — excluded.
        let mut shorted = InventoryRisk::new(generous_inv());
        shorted.seed(yes_a, 60 * SH as i128, Usdc(0));
        shorted.seed(no_a, -50 * SH as i128, Usdc(0));
        assert!(
            mergeable_pairs(&complement, &shorted, threshold_micro)
                .iter()
                .all(|c| c.a != yes_a),
            "a short leg yields no mergeable set"
        );
    }

    /// M6-7 (pure): [`apply_merge_result`] is the SHARED paper/live reduction —
    /// it drops BOTH legs' long inventory by `amount_micro`, credits the recovered
    /// `amount_micro` µUSDC ($1/set) as cash, releases each leg's basis, and
    /// returns the aggregate realized delta. Mirrors the B5 paper-merge assertion.
    #[test]
    fn apply_merge_result_reduces_both_legs_and_credits_cash() {
        let (yes, no) = (TokenId(1), TokenId(2));
        let token_market = HashMap::from([(yes, MarketId(0)), (no, MarketId(0))]);
        let mut inv = InventoryRisk::new(generous_inv());
        let mut positions = PositionBook::default();
        // The B5 fixture: 100 YES @ $0.50 ($50 basis) + 80 NO @ $0.45 ($36 basis).
        inv.seed(yes, 100 * SH as i128, Usdc(50_000_000));
        inv.seed(no, 80 * SH as i128, Usdc(36_000_000));
        let matched = 80 * SH as i128; // min(100, 80)

        let realized_delta =
            apply_merge_result(&mut inv, &mut positions, &token_market, yes, no, matched);

        assert_eq!(inv.net(yes), 100 * SH as i128 - matched, "YES reduced by matched");
        assert_eq!(inv.net(no), 80 * SH as i128 - matched, "NO reduced by matched (flat)");
        assert_eq!(
            positions.cash(),
            Usdc(matched),
            "recovered matched × $1 collateral credited as cash"
        );
        let realized = inv.realized(yes).0 + inv.realized(no).0;
        assert_eq!(realized, 4_000_000, "recovered $80 − basis released $76 = +$4");
        assert_eq!(realized_delta, 4_000_000, "returned realized_delta == the aggregate");
    }

    /// M6-7: [`drain_merge_outcomes`] applies ONLY a confirmed (`Ok`) merge to
    /// inventory/cash; a failed (`Err`) merge leaves inventory untouched (retried
    /// next sweep). EITHER outcome clears the pair's in-flight latch, so it can be
    /// re-swept. No HTTP — the outcomes are pushed directly onto the channel.
    #[tokio::test]
    async fn drain_merge_outcomes_applies_only_success() {
        let (yes_ok, no_ok) = (TokenId(1), TokenId(2));
        let (yes_err, no_err) = (TokenId(3), TokenId(4));
        let tokens = vec![yes_ok, no_ok, yes_err, no_err];
        let (fetcher, _shared) = controllable_fetcher(&tokens, HashMap::new());
        let (mut mm, _store_rx, _status_rx) = build_loop(
            fetcher,
            generous_inv(),
            mk_params(200, 5.0),
            tokens,
            Usdc(1_000_000_000),
        );
        let amount = 40 * SH as i128; // a $40 set on each pair
        for t in [yes_ok, no_ok, yes_err, no_err] {
            mm.inv.seed(t, amount, Usdc(0));
        }
        // Both pairs were marked in-flight by a prior sweep.
        mm.merge_inflight.insert((yes_ok, no_ok));
        mm.merge_inflight.insert((yes_err, no_err));
        // A confirmed merge (Ok, recovered $40) and a failed one (Err).
        mm.merge_tx
            .send(MergeOutcome { a: yes_ok, b: no_ok, amount_micro: amount, result: Ok(amount) })
            .unwrap();
        mm.merge_tx
            .send(MergeOutcome {
                a: yes_err,
                b: no_err,
                amount_micro: amount,
                result: Err(RelayerError::Http("boom".into())),
            })
            .unwrap();

        mm.drain_merge_outcomes();

        // Ok → both legs reduced to flat + recovered $40 credited as cash (once).
        assert_eq!(mm.inv.net(yes_ok), 0, "confirmed merge reduced the YES leg");
        assert_eq!(mm.inv.net(no_ok), 0, "confirmed merge reduced the NO leg");
        assert_eq!(mm.positions.cash(), Usdc(amount), "recovered collateral credited (Ok only)");
        // Err → inventory untouched.
        assert_eq!(mm.inv.net(yes_err), amount, "failed merge left the YES leg untouched");
        assert_eq!(mm.inv.net(no_err), amount, "failed merge left the NO leg untouched");
        // BOTH outcomes release the in-flight latch.
        assert!(mm.merge_inflight.is_empty(), "success AND failure clear the in-flight latch");
    }

    /// M6-7 GATING: on a LIVE venue with NO relayer (`merger = None`),
    /// `maybe_merge_sets` keeps the hold-to-resolution no-op — inventory UNCHANGED,
    /// the one-shot warn latched, and NOTHING spawned / marked in-flight (even with
    /// a conditionId map present). PAPER (`no_naked_shorts = false`) still recycles
    /// in-line and likewise never marks an in-flight merge. So paper + non-relayer
    /// live are byte-for-byte unchanged.
    #[tokio::test]
    async fn merge_sweep_gating_holds_without_relayer_and_never_spawns_on_paper() {
        fn seeded(no_naked_shorts: bool) -> (MmLoop<PaperMakerVenue<BookFetcher>>, mpsc::Receiver<StoreMsg>) {
            let (yes, no) = (TokenId(1), TokenId(2));
            let tokens = vec![yes, no];
            let (fetcher, _shared) = controllable_fetcher(
                &tokens,
                HashMap::from([(yes, (mid50_book(), true)), (no, (mid50_book(), true))]),
            );
            let mut params = mk_params(200, 5.0);
            params.policy = Policy::RewardFarm;
            params.hedging_enabled = true;
            params.merge_threshold_usd = 5.0;
            let (mut mm, store_rx, _status_rx) =
                build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            mm.complement.insert(yes, no);
            mm.complement.insert(no, yes);
            mm.no_naked_shorts = no_naked_shorts;
            // A conditionId map IS present — but with `merger = None` (the build_loop
            // default) the live branch must still NOT sweep / spawn.
            mm.cond_by_token.insert(yes, B256::ZERO);
            mm.cond_by_token.insert(no, B256::ZERO);
            mm.inv.seed(yes, 100 * SH as i128, Usdc(50_000_000));
            mm.inv.seed(no, 80 * SH as i128, Usdc(36_000_000));
            (mm, store_rx)
        }
        let (yes, no) = (TokenId(1), TokenId(2));

        // LIVE + merger None → hold-to-resolution no-op.
        let (mut live, _r1) = seeded(true);
        live.maybe_merge_sets().await;
        assert_eq!(live.inv.net(yes), 100 * SH as i128, "no relayer → YES leg held");
        assert_eq!(live.inv.net(no), 80 * SH as i128, "no relayer → NO leg held");
        assert_eq!(live.positions.cash(), Usdc(0), "no relayer → no collateral recovered");
        assert!(live.merge_live_warned, "hold-to-resolution warn latched once");
        assert!(live.merge_inflight.is_empty(), "no relayer → nothing spawned / in-flight");

        // PAPER → existing in-line recycle, and NEVER an in-flight (live-only) set.
        let (mut paper, _r2) = seeded(false);
        paper.maybe_merge_sets().await;
        assert_eq!(
            paper.inv.net(yes),
            100 * SH as i128 - 80 * SH as i128,
            "paper recycles the matched set (YES surplus remains)"
        );
        assert_eq!(paper.inv.net(no), 0, "paper recycles the NO leg to flat");
        assert_eq!(paper.positions.cash(), Usdc(80_000_000), "paper credits recovered $80");
        assert!(!paper.merge_live_warned, "paper never sets the live hold latch");
        assert!(paper.merge_inflight.is_empty(), "paper never marks an in-flight merge");
    }

    // ── R1: resolved-winner redeem — pure selection (`redeem_targets`) ──────────

    /// One Data-API position row for the redeem tests. Only the fields
    /// `redeem_targets` reads matter (`condition_id`, `asset`, `cur_price`,
    /// `redeemable`); the rest are zeroed. `cond_hex` is the SAME hex string the
    /// test parses into the `cond_by_token` B256, so the parsed ids match exactly.
    fn mk_data_pos(
        cond_hex: &str,
        asset: &str,
        cur_price: f64,
        redeemable: bool,
    ) -> pm_ingestion::data_api::Position {
        pm_ingestion::data_api::Position {
            condition_id: cond_hex.to_string(),
            asset: asset.to_string(),
            size: 0.0,
            outcome: String::new(),
            outcome_index: 0,
            cur_price,
            redeemable,
        }
    }

    /// R1 (pure): a RESOLVED condition where the MM holds BOTH legs is one target
    /// with two legs — winner (curPrice 1.0) AND loser (curPrice 0.0), since the
    /// on-chain `redeemPositions` clears both slots — at their resolved prices and
    /// held nets, sorted by token id.
    #[test]
    fn redeem_targets_includes_both_held_legs_of_resolved_condition() {
        let (yes, no) = (TokenId(1), TokenId(2));
        let cond_hex = format!("0x{}", "ab".repeat(32));
        let cond: B256 = cond_hex.parse().unwrap();
        let cond_by_token = HashMap::from([(yes, cond), (no, cond)]);
        let venue_by_token =
            HashMap::from([(yes, "asset_yes".to_string()), (no, "asset_no".to_string())]);
        // RESOLVED: both legs listed redeemable — winner curPrice 1.0, loser 0.0.
        let positions = vec![
            mk_data_pos(&cond_hex, "asset_yes", 1.0, true),
            mk_data_pos(&cond_hex, "asset_no", 0.0, true),
        ];
        // We hold BOTH legs long.
        let net = HashMap::from([(yes, 100 * SH as i128), (no, 60 * SH as i128)]);
        let got = redeem_targets(&positions, &cond_by_token, &venue_by_token, |t| {
            net.get(&t).copied().unwrap_or(0)
        });

        assert_eq!(got.len(), 1, "one resolved+held condition → one target");
        assert_eq!(got[0].condition_id, cond);
        assert_eq!(
            got[0].legs,
            vec![
                RedeemLeg { token: yes, resolved_price: 1.0, net_micro: 100 * SH as i128 },
                RedeemLeg { token: no, resolved_price: 0.0, net_micro: 60 * SH as i128 },
            ],
            "both held legs, sorted by token id, at their resolved prices/nets"
        );
    }

    /// R1 (pure): a resolved condition's LOSER leg that the feed doesn't list as
    /// redeemable is STILL redeemed (held + resolved), priced at the default 0.0.
    #[test]
    fn redeem_targets_includes_unlisted_loser_leg_at_zero() {
        let (yes, no) = (TokenId(1), TokenId(2));
        let cond_hex = format!("0x{}", "bc".repeat(32));
        let cond: B256 = cond_hex.parse().unwrap();
        let cond_by_token = HashMap::from([(yes, cond), (no, cond)]);
        let venue_by_token =
            HashMap::from([(yes, "asset_yes".to_string()), (no, "asset_no".to_string())]);
        // Only the WINNER (yes) is listed redeemable; the resolved LOSER (no) is
        // dropped from the positions feed entirely.
        let positions = vec![mk_data_pos(&cond_hex, "asset_yes", 1.0, true)];
        let net = HashMap::from([(yes, 50 * SH as i128), (no, 70 * SH as i128)]);
        let got = redeem_targets(&positions, &cond_by_token, &venue_by_token, |t| {
            net.get(&t).copied().unwrap_or(0)
        });

        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].legs,
            vec![
                RedeemLeg { token: yes, resolved_price: 1.0, net_micro: 50 * SH as i128 },
                RedeemLeg { token: no, resolved_price: 0.0, net_micro: 70 * SH as i128 },
            ],
            "the held loser is still redeemed, defaulted to price 0.0"
        );
    }

    /// R1 (pure): a still-LIVE condition (no `redeemable` position) is never a
    /// redeem target, even when fully held.
    #[test]
    fn redeem_targets_skips_still_live_condition() {
        let (yes, no) = (TokenId(1), TokenId(2));
        let cond_hex = format!("0x{}", "cd".repeat(32));
        let cond: B256 = cond_hex.parse().unwrap();
        let cond_by_token = HashMap::from([(yes, cond), (no, cond)]);
        let venue_by_token =
            HashMap::from([(yes, "asset_yes".to_string()), (no, "asset_no".to_string())]);
        // Held, but the market has NOT resolved (redeemable = false).
        let positions = vec![
            mk_data_pos(&cond_hex, "asset_yes", 0.55, false),
            mk_data_pos(&cond_hex, "asset_no", 0.45, false),
        ];
        let net = HashMap::from([(yes, 100 * SH as i128), (no, 100 * SH as i128)]);
        let got = redeem_targets(&positions, &cond_by_token, &venue_by_token, |t| {
            net.get(&t).copied().unwrap_or(0)
        });

        assert!(got.is_empty(), "a still-live (non-redeemable) condition is never a target");
    }

    /// R1 (pure): a RESOLVED condition the MM doesn't hold (net 0) is not a
    /// target — `redeemPositions` would clear nothing.
    #[test]
    fn redeem_targets_skips_resolved_condition_we_dont_hold() {
        let (yes, no) = (TokenId(1), TokenId(2));
        let cond_hex = format!("0x{}", "de".repeat(32));
        let cond: B256 = cond_hex.parse().unwrap();
        let cond_by_token = HashMap::from([(yes, cond), (no, cond)]);
        let venue_by_token =
            HashMap::from([(yes, "asset_yes".to_string()), (no, "asset_no".to_string())]);
        // RESOLVED, but we hold NOTHING (net 0 on both legs).
        let positions = vec![
            mk_data_pos(&cond_hex, "asset_yes", 1.0, true),
            mk_data_pos(&cond_hex, "asset_no", 0.0, true),
        ];
        let got = redeem_targets(&positions, &cond_by_token, &venue_by_token, |_| 0);

        assert!(got.is_empty(), "resolved but net 0 (nothing held) → not a target");
    }

    // ── R1: resolved-winner redeem — pure apply (`apply_redeem`) ────────────────

    /// R1 (pure): [`apply_redeem`] is the SHARED settlement — it credits each held
    /// leg's resolved value (winner ≈ $1/share, loser $0) via the signed-lot sell
    /// path, clears both legs to flat, mirrors the recovered cash into the
    /// reporting book, and returns the aggregate realized delta (recovered − basis).
    #[test]
    fn apply_redeem_clears_legs_and_credits_resolved_value() {
        let (winner, loser) = (TokenId(1), TokenId(2));
        let token_market = HashMap::from([(winner, MarketId(0)), (loser, MarketId(0))]);
        let mut inv = InventoryRisk::new(generous_inv());
        let mut positions = PositionBook::default();
        // 100 shares each: winner bought @ $0.40 ($40 basis), loser @ $0.40 ($40 basis).
        inv.seed(winner, 100 * SH as i128, Usdc(40_000_000));
        inv.seed(loser, 100 * SH as i128, Usdc(40_000_000));
        let cond: B256 = format!("0x{}", "ef".repeat(32)).parse().unwrap();
        let target = RedeemTarget {
            condition_id: cond,
            legs: vec![
                RedeemLeg { token: winner, resolved_price: 1.0, net_micro: 100 * SH as i128 },
                RedeemLeg { token: loser, resolved_price: 0.0, net_micro: 100 * SH as i128 },
            ],
        };

        let realized_delta = apply_redeem(&mut inv, &mut positions, &token_market, &target);

        // Winner: recovered $100, basis $40 → realized +$60, cleared to flat.
        assert_eq!(inv.net(winner), 0, "winner leg cleared to net 0");
        assert_eq!(inv.realized(winner), Usdc(60_000_000), "recovered $100 − basis $40");
        // Loser: recovered $0, basis $40 → realized −$40, cleared to flat.
        assert_eq!(inv.net(loser), 0, "loser leg cleared to net 0");
        assert_eq!(inv.realized(loser), Usdc(-40_000_000), "recovered $0 − basis $40");
        // Recovered cash credited to the reporting book: winner $100 + loser $0.
        assert_eq!(positions.cash(), Usdc(100_000_000), "recovered resolved value credited");
        // Aggregate realized = recovered $100 − basis $80 = +$20.
        assert_eq!(realized_delta, 20_000_000, "returned realized_delta == the aggregate");
    }

    // ── R2: resolved-winner redeem — sweep drain + gating ───────────────────────

    /// R2: [`drain_redeem_outcomes`] settles a CONFIRMED redeem at the CURRENT net
    /// — clears every held leg of the resolved condition (winner credited ≈
    /// $1/share, loser $0), credits the recovered cash once, and clears the
    /// in-flight latch. No HTTP — the sweep's confirmed targets are pushed directly
    /// onto the channel.
    #[tokio::test]
    async fn drain_redeem_applies_at_current_net_and_credits() {
        let (winner, loser) = (TokenId(1), TokenId(2));
        let tokens = vec![winner, loser];
        let (fetcher, _shared) = controllable_fetcher(&tokens, HashMap::new());
        let (mut mm, _store_rx, _status_rx) = build_loop(
            fetcher,
            generous_inv(),
            mk_params(200, 5.0),
            tokens,
            Usdc(1_000_000_000),
        );
        mm.inv.seed(winner, 100 * SH as i128, Usdc(40_000_000));
        mm.inv.seed(loser, 100 * SH as i128, Usdc(40_000_000));
        mm.redeem_sweep_inflight = true;
        let cond: B256 = format!("0x{}", "ab".repeat(32)).parse().unwrap();
        mm.redeem_tx
            .send(vec![RedeemTarget {
                condition_id: cond,
                legs: vec![
                    RedeemLeg { token: winner, resolved_price: 1.0, net_micro: 100 * SH as i128 },
                    RedeemLeg { token: loser, resolved_price: 0.0, net_micro: 100 * SH as i128 },
                ],
            }])
            .unwrap();

        mm.drain_redeem_outcomes();

        assert_eq!(mm.inv.net(winner), 0, "winner leg flattened on redeem");
        assert_eq!(mm.inv.net(loser), 0, "loser leg flattened on redeem");
        assert_eq!(
            mm.positions.cash(),
            Usdc(100_000_000),
            "recovered resolved value credited once ($100 winner + $0 loser)"
        );
        assert!(!mm.redeem_sweep_inflight, "a finished sweep clears the in-flight latch");
    }

    /// R2: the drain applies at the CURRENT net, NOT the (possibly stale) snapshot
    /// baked into the target — so a leg whose `net_micro` exceeds the current
    /// holding is CLAMPED to the current net (never an over-reduction / phantom
    /// short), and only the current holding is credited.
    #[tokio::test]
    async fn drain_redeem_clamps_to_current_net() {
        let winner = TokenId(1);
        let tokens = vec![winner];
        let (fetcher, _shared) = controllable_fetcher(&tokens, HashMap::new());
        let (mut mm, _store_rx, _status_rx) = build_loop(
            fetcher,
            generous_inv(),
            mk_params(200, 5.0),
            tokens,
            Usdc(1_000_000_000),
        );
        // Hold only 30 shares NOW; the sweep's snapshot target over-claims 100.
        mm.inv.seed(winner, 30 * SH as i128, Usdc(12_000_000));
        let cond: B256 = format!("0x{}", "cd".repeat(32)).parse().unwrap();
        mm.redeem_tx
            .send(vec![RedeemTarget {
                condition_id: cond,
                legs: vec![RedeemLeg {
                    token: winner,
                    resolved_price: 1.0,
                    net_micro: 100 * SH as i128,
                }],
            }])
            .unwrap();

        mm.drain_redeem_outcomes();

        assert_eq!(mm.inv.net(winner), 0, "cleared to flat — never driven negative");
        assert_eq!(
            mm.positions.cash(),
            Usdc(30_000_000),
            "credited the CURRENT 30 shares × $1, not the stale 100"
        );
    }

    /// R2 GATING: `sweep_onchain_redeems` only fires on a relayer-backed reward-farm
    /// live run. With no positions feed (`data_api`/`merger`/`deposit_wallet` all
    /// `None`, the `build_loop` default) it spawns NOTHING and leaves the in-flight
    /// latch clear — so paper / arb / non-relayer live are byte-for-byte unchanged
    /// (the per-cycle drain is an inert empty `try_recv`).
    #[tokio::test]
    async fn redeem_sweep_gated_without_feed() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) = controllable_fetcher(&tokens, HashMap::new());
        let (mut mm, _store_rx, _status_rx) = build_loop(
            fetcher,
            generous_inv(),
            mk_params(200, 5.0),
            tokens,
            Usdc(1_000_000_000),
        );
        mm.sweep_onchain_redeems();
        assert!(
            !mm.redeem_sweep_inflight,
            "no positions feed (data_api/merger/deposit_wallet None) → nothing spawned"
        );
    }

    // ── RewardFarm Phase-B integration (Task B6, spec §5.1) ─────────────────────

    /// Spec-2 Phase B (Task B6) END-TO-END: a RewardFarm + `hedging_enabled` loop
    /// over a complement market with BOTH books and the paper taker-fill sim ON,
    /// driven for several cycles, ties the whole reward-farm hedging story
    /// together:
    ///   (1) FROM FLAT it bids the complement PAIR (buy YES + buy NO) and places
    ///       NO ask on either — two-sided-from-flat with no naked short (closes M3).
    ///   (2) as the taker sim lifts both bids, inventory accumulates toward a
    ///       DELTA-NEUTRAL pair: `|yes_net − no_net|` stays bounded (the mirror
    ///       books + pair-symmetric sizing hold it at exactly 0 here, so the pair
    ///       never runs away one-sided; the size-skew that rebalances an ALREADY
    ///       unbalanced pair is locked by `rewardfarm_hedging_skews_bids_by_net_delta`).
    ///   (3) once a complete set clears `merge_threshold_usd`, `maybe_merge_sets`
    ///       (the SAME step `quote()` runs each cycle) recycles it — BOTH legs drop
    ///       by the matched count and the recovered `matched × $1` collateral is
    ///       credited as cash (gross inventory falls). The end-to-end tail then
    ///       shows the loop's OWN `quote()` auto-merging a re-accumulated set.
    ///   (4) the published `RewardFarmStatus` is two-sided: `q_min > 0`, and the
    ///       PAIRED score strictly exceeds the single-sided/penalized sum a
    ///       per-token (bid-only) scoring would yield.
    ///
    /// Determinism note: the bids land at the 0.50 mid on both legs (a complete
    /// set costs exactly $1 and merges to exactly $1), so the merge is BREAK-EVEN
    /// (realized ≈ 0) — the capital-recycling benefit shows as the inventory drop
    /// + recovered collateral, NOT a realized profit. The exact merge cash/realized
    /// accounting (incl. a below-$1 set booking a profit) is locked by
    /// `merge_recycles_complete_set_in_paper` (B5); here we assert the integration
    /// fact that a set ACCUMULATED BY REAL FILLS is merged and drops inventory.
    #[tokio::test]
    async fn reward_farm_hedging_two_sided_from_flat_and_merges() {
        let yes = TokenId(1);
        let no = TokenId(2);
        let tokens = vec![yes, no];
        // Both outcomes: an identical mirror book at mid 0.50 (a YES 0.48/0.52 ⇒
        // its NO complement is also 0.48/0.52), each two-sided + reward-eligible.
        let (fetcher, _shared) = controllable_fetcher(
            &tokens,
            HashMap::from([(yes, (mid50_book(), true)), (no, (mid50_book(), true))]),
        );
        let mut params = mk_params(200, 5.0); // $5/side notional ⇒ a 10-share base bid at 0.50
        params.policy = Policy::RewardFarm;
        params.hedging_enabled = true;
        params.merge_threshold_usd = 5.0; // a complete set above $5 is recycled
        // Steady taker flow lifts ~25% of each resting bid per poll, so BOTH legs
        // accumulate long toward a complete set over several cycles.
        params.paper_taker_fill_pct = 25;
        let merge_threshold_usd = params.merge_threshold_usd;
        // `build_loop_tally` wires `paper_taker_fill_pct` into the PaperMakerVenue
        // (no_naked_shorts = false, so the paper merge path is live) and hands back
        // a tally of every venue-produced fill so we can prove fills actually land.
        let (mut mm, _store_rx, produced) =
            build_loop_tally(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        mm.reward_by_token.insert(yes, (1.0, 3.0, 100.0));
        mm.reward_by_token.insert(no, (1.0, 3.0, 100.0));
        // The yes↔no complement map main wires under hedging (pairs the two bids
        // for the estimator + delta-neutral sizing + the merge).
        mm.complement.insert(yes, no);
        mm.complement.insert(no, yes);

        // ── (1) TWO-SIDED FROM FLAT (no naked short) ───────────────────────────
        mm.quote().await;
        assert!(
            mm.qm.tracked().contains_key(&(yes, Side::Bid)),
            "from flat: a BID rests on YES (complement pair)"
        );
        assert!(
            mm.qm.tracked().contains_key(&(no, Side::Bid)),
            "from flat: a BID rests on NO (complement pair)"
        );
        assert!(
            !mm.qm.tracked().contains_key(&(yes, Side::Ask)),
            "from flat: NO ask on YES (no naked short — the complement bid is the second side)"
        );
        assert!(
            !mm.qm.tracked().contains_key(&(no, Side::Ask)),
            "from flat: NO ask on NO (no naked short)"
        );

        // ── (4) THE PUBLISHED q_min IS TWO-SIDED (PAIRED, not single-sided) ────
        // Per-leg score from the SAME primitives the estimator uses, against each
        // bid's own mid (both bids rest at the 0.50 mid, full size, pre-fill).
        let v = 3.0_f64;
        let adj_mid = reward_fair_value(&mid50_book(), TickSize::Cent, 1.0).expect("two-sided mid");
        let (mut q_yes, mut q_no) = (0.0_f64, 0.0_f64);
        for (_, o) in mm.qm.resting_orders() {
            let price = o.price.microusdc(TickSize::Cent) as f64 / 1_000_000.0;
            let shares = o.size.0 as f64 / 1_000_000.0;
            let s = order_score(v, (price - adj_mid).abs() * 100.0) * shares;
            if o.token == yes {
                q_yes += s;
            } else if o.token == no {
                q_no += s;
            }
        }
        assert!(q_yes > 0.0 && q_no > 0.0, "both complement bids rest and score in-band");
        // PAIRED: the YES bid (Q1) + NO bid (Q2) combine into ONE market q_min.
        let expected_paired = q_min(q_yes, q_no, adj_mid);
        // SINGLE-SIDED (the per-token path): each bid-only token collapses to the
        // 1/C floor, so the sum is strictly smaller.
        let single_sided = q_min(q_yes, 0.0, adj_mid) + q_min(0.0, q_no, adj_mid);
        let st = mm.sample_reward_estimate().await;
        assert!(st.q_min > 0.0, "two-sided q_min > 0 from flat, got {}", st.q_min);
        assert!(st.in_band, "two-sided in-band this sample");
        assert!(
            (st.q_min - expected_paired).abs() < 1e-9,
            "published q_min {} is the PAIRED two-sided score {}",
            st.q_min,
            expected_paired
        );
        assert!(
            st.q_min > single_sided + 1e-9,
            "paired q_min {} EXCEEDS the single-sided/penalized sum {} (paired, not per-token)",
            st.q_min,
            single_sided
        );

        // ── (2) FILLS ACCUMULATE TOWARD A DELTA-NEUTRAL PAIR ───────────────────
        // Drive cycles until a complete set first clears the merge floor. The
        // mirror books + symmetric bids fill both legs in lock-step, so the pair
        // stays delta-neutral as it grows (here exactly 0 — it never runs away
        // one-sided). The loop's own quote() has NOT yet auto-merged the set (that
        // runs at the START of the NEXT cycle, off the PREVIOUS cycle's inventory).
        const DELTA_BOUND: i128 = SH as i128; // 1 share; the symmetric run holds it at 0
        let mut cycles = 0;
        loop {
            mm.tick().await;
            cycles += 1;
            let (yn, nn) = (mm.inv.net(yes), mm.inv.net(no));
            assert!(
                (yn - nn).abs() <= DELTA_BOUND,
                "pair stays delta-neutral as it accumulates: |{yn} − {nn}| within {DELTA_BOUND}"
            );
            if (yn.min(nn)) as f64 / 1_000_000.0 > merge_threshold_usd {
                break;
            }
            assert!(cycles < 50, "fills must accumulate a mergeable set within a few cycles");
        }
        assert!(cycles >= 2, "the set is built over SEVERAL cycles, not one fill (got {cycles})");
        assert!(!produced.lock().unwrap().is_empty(), "the paper taker sim actually produced fills");

        // ── (3) MERGE RECYCLES THE COMPLETE SET (legs drop, collateral credited) ─
        let (yes_pre, no_pre) = (mm.inv.net(yes), mm.inv.net(no));
        let matched = yes_pre.min(no_pre); // the matched complete-set count (µshares)
        let gross_pre = yes_pre.abs() + no_pre.abs();
        let cash_pre = mm.positions.cash();
        assert!(matched > 0, "a complete YES+NO set is held");

        mm.maybe_merge_sets().await;

        assert_eq!(mm.inv.net(yes), yes_pre - matched, "YES leg reduced by the matched set count");
        assert_eq!(mm.inv.net(no), no_pre - matched, "NO leg reduced by the matched set count");
        let gross_post = mm.inv.net(yes).abs() + mm.inv.net(no).abs();
        assert!(
            gross_post < gross_pre,
            "merge recycles the set ⇒ gross inventory falls ({gross_pre} → {gross_post} µshares)"
        );
        // Recovered collateral = matched × $1 (a complete set redeems to exactly
        // $1), credited as cash. For a $1 set the µUSDC recovered equals the
        // matched µshares numerically, so the cash delta == `matched`.
        assert_eq!(
            mm.positions.cash().0 - cash_pre.0,
            matched,
            "recovered matched × $1 collateral credited as cash"
        );

        // ── (3, end-to-end) the loop's OWN quote() auto-merges a re-accumulated set ─
        // Bids-only means gross long inventory can ONLY fall via a merge, so an
        // observed drop across cycles proves quote()'s maybe_merge_sets fired in
        // the loop (and the pair stays delta-neutral throughout).
        let mut gross_seq = Vec::new();
        for _ in 0..8 {
            mm.tick().await;
            assert!(
                (mm.inv.net(yes) - mm.inv.net(no)).abs() <= DELTA_BOUND,
                "pair stays delta-neutral across the auto-merge cycles"
            );
            gross_seq.push(mm.inv.net(yes).abs() + mm.inv.net(no).abs());
        }
        assert!(
            gross_seq.windows(2).any(|w| w[1] < w[0]),
            "quote()'s own merge step recycles a re-accumulated set (gross long inventory falls): {gross_seq:?}"
        );

        // ── (4, published) the loop's published reward telemetry is two-sided ──
        let published = mm.reward_status.expect("RewardFarm publishes a reward estimate");
        assert!(
            published.q_min > 0.0,
            "published RewardFarmStatus stays two-sided (q_min > 0), got {}",
            published.q_min
        );
    }

    /// PRIORITY-4 (light) A/B (spec §15): run BOTH policies over the SAME
    /// synthetic mid-0.50 book with identical simulated taker flow and surface
    /// each one's estimated reward + realized P&L so the difference is VISIBLE.
    /// Uses [`build_loop_tally`] so `paper_taker_fill_pct` actually drives fills
    /// (so realized P&L is non-trivial). The realized figure is NOT hard-asserted
    /// (the paper sim models no adverse selection — it only captures the quoted
    /// spread, which inverts the live reality the spec warns about); the firm
    /// invariant is qualitative: RewardFarm surfaces a positive-`Q_min` reward
    /// estimate, SpreadCapture surfaces NONE. Returns `(reward_estimate,
    /// realized_µUSDC)` per policy (the cached sample `publish_status` would
    /// emit, read directly since `build_loop_tally` keeps no status receiver).
    #[tokio::test]
    async fn ab_reward_farm_vs_spread_capture_same_book() {
        async fn run_policy(policy: Policy) -> (Option<RewardFarmStatus>, i128) {
            let tokens = vec![TokenId(1)];
            // Same synthetic book + identical taker flow for both policies.
            let (fetcher, _shared) =
                controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
            let mut params = mk_params(200, 5.0);
            params.policy = policy;
            params.paper_taker_fill_pct = 20; // wired by build_loop_tally
            let (mut mm, _store_rx, _produced) =
                build_loop_tally(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            if policy == Policy::RewardFarm {
                mm.reward_by_token.insert(TokenId(1), (5.0, 3.0, 100.0));
            }
            // Several cycles over the unchanged book: quote → taker fills → mark
            // → (RewardFarm) sample. `reward_status` holds what `publish_status`
            // would emit; `inv.realized` is the realized P&L both policies book.
            for _ in 0..5 {
                mm.tick().await;
            }
            (mm.reward_status, mm.inv.realized(TokenId(1)).0)
        }

        let (rf_reward, rf_realized) = run_policy(Policy::RewardFarm).await;
        let (sc_reward, sc_realized) = run_policy(Policy::SpreadCapture).await;

        // Visible A/B summary (the numbers differ between policies; the hard
        // assert below is the qualitative reward-vs-no-reward distinction).
        let rf_est = rf_reward.map_or(0.0, |r| r.est_reward_usd_day);
        let rf_qmin = rf_reward.map_or(0.0, |r| r.q_min);
        eprintln!(
            "A/B same-book: reward_farm est ${rf_est:.2}/day q_min {rf_qmin:.2} realized {rf_realized}µ \
             | spread_capture reward {sc_reward:?} realized {sc_realized}µ"
        );

        let rf = rf_reward.expect("RewardFarm surfaces a reward estimate");
        assert!(rf.q_min > 0.0, "RewardFarm two-sided ⇒ q_min > 0, got {}", rf.q_min);
        assert!(
            rf.est_reward_usd_day > 0.0,
            "funded reward market ⇒ est $/day > 0, got {}",
            rf.est_reward_usd_day
        );
        assert!(
            sc_reward.is_none(),
            "SpreadCapture earns no liquidity reward (reward telemetry must be None)"
        );
    }

    // ── RewardFarm instrumentation telemetry (Task 10, spec §12) ────────────────

    /// Under RewardFarm a quote cycle emits a best-effort `RfDecision` for the
    /// quoted token, and a consumed fill emits a best-effort `RfOutcome` — both
    /// fire-and-forget on the SAME store channel as fills/pnl, keyed by the token
    /// id so the writer can correlate the outcome to the decision.
    #[tokio::test]
    async fn rewardfarm_emits_decision_and_outcome_telemetry() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, mut store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        mm.policy = Policy::RewardFarm;
        mm.reward_by_token.insert(TokenId(1), (1.0, 3.0, 100.0));

        // Cycle rests bid 0.50 / ask 0.51; logs a decision for token 1.
        mm.quote().await;
        // Cross the resting bid (best_ask 0.49 ≤ 0.50) so a fill is booked.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(48, 100 * SH)], &[(49, 100 * SH)]), true));
        mm.consume_fills().await;

        let mut decisions = Vec::new();
        let mut outcomes = Vec::new();
        while let Ok(msg) = store_rx.try_recv() {
            match msg {
                StoreMsg::RfDecision(row) => decisions.push(row),
                StoreMsg::RfOutcome(row) => outcomes.push(row),
                _ => {}
            }
        }
        assert!(!decisions.is_empty(), "a RewardFarm quote logs a decision row");
        let d = &decisions[0];
        assert_eq!(d.market, "1", "decision is keyed by the token id (Spec-1 quoting unit)");
        assert!(d.state_json.contains("adj_mid"), "state JSON carries the features");
        assert!(d.action_json.contains("bid"), "action JSON carries the chosen quote");

        assert_eq!(outcomes.len(), 1, "the consumed fill logs exactly one outcome");
        assert_eq!(outcomes[0].market, "1", "outcome shares the token-id key for correlation");
    }

    /// Isolation LOCK: `SpreadCapture` writes NOTHING new — a full quote+fill
    /// cycle emits zero RewardFarm telemetry (the instrumentation is gated behind
    /// `Policy::RewardFarm`), so the legacy/arb paths are byte-for-byte unaffected.
    #[tokio::test]
    async fn spreadcapture_emits_no_rf_telemetry() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0); // SpreadCapture (mk_params default)
        let (mut mm, mut store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.quote().await; // bid 0.49 / ask 0.51
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(48, 100 * SH)], &[(49, 100 * SH)]), true));
        mm.consume_fills().await;

        let mut rf_msgs = 0usize;
        while let Ok(msg) = store_rx.try_recv() {
            if matches!(msg, StoreMsg::RfDecision(_) | StoreMsg::RfOutcome(_)) {
                rf_msgs += 1;
            }
        }
        assert_eq!(rf_msgs, 0, "SpreadCapture must emit no rf_decision/rf_outcome telemetry");
    }

    // ── RewardFarm Phase-A adverse-selection quote-pull (Task A5, spec §4) ──────

    /// A strongly bid-imbalanced book makes the ASK the endangered side (it gets
    /// lifted just before the price rises), so under RewardFarm the ask is PULLED
    /// — omitted from the placed orders while the BID still rests. Once the signal
    /// eases (a balanced book) AND the cooldown has lapsed, the ask returns.
    ///
    /// SIGNAL SETUP (Step-5 note): `combined_signal` AVERAGES imbalance + momentum
    /// (`(imb + mom)/2`), so a single observation (momentum 0) gives `signal =
    /// imbalance/2`. Imbalance alone can therefore never reach the default 0.6
    /// threshold (`imbalance ≤ 1 ⇒ signal ≤ 0.5`), so this test LOWERS
    /// `pull_threshold` to 0.3 and uses a book with a balanced TOP (equal best
    /// sizes ⇒ a clean 0.50 microprice, hence momentum 0 on both cycles) but heavy
    /// bid DEPTH BEHIND the touch ⇒ a ~0.9 summed imbalance ⇒ signal ~0.45 ≥ 0.3.
    /// `pull_cooldown_ms = 0` so the recovery cycle is gated only on the signal
    /// easing (the cooldown's own effect is covered by the test below).
    #[tokio::test]
    async fn rewardfarm_pulls_ask_on_strong_bid_imbalance() {
        let tokens = vec![TokenId(1)];
        // Balanced TOP (100 sh each ⇒ microprice 0.50) but heavy bid depth behind
        // the touch (47¢/46¢) ⇒ strong POSITIVE imbalance over the top-3 levels.
        let imbalanced = cent_book(
            &[(48, 100 * SH), (47, 1000 * SH), (46, 1000 * SH)],
            &[(52, 100 * SH)],
        );
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (imbalanced, true))]));
        let mut params = mk_params(200, 5.0);
        params.policy = Policy::RewardFarm;
        // imbalance/2 (~0.45) clears 0.3; the default 0.6 is unreachable on
        // imbalance alone (the blend halves it).
        params.pull_threshold = 0.3;
        // Recovery gated only on the signal easing (not a lingering cooldown).
        params.pull_cooldown_ms = 0;
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        mm.reward_by_token.insert(TokenId(1), (1.0, 3.0, 100.0));

        // Cycle 1: strong UP pressure ⇒ the ASK is pulled; the BID is still placed.
        mm.quote().await;
        assert!(
            mm.qm.tracked().contains_key(&(TokenId(1), Side::Bid)),
            "the safe BID side is still placed"
        );
        assert!(
            !mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)),
            "the endangered ASK is PULLED (omitted) on a strong adverse up-signal"
        );

        // Cycle 2: a balanced book eases the signal (imbalance 0; microprice stays
        // 0.50 ⇒ momentum 0) and the 0-ms cooldown has lapsed ⇒ the ask returns
        // (the bid stays put, sticky).
        shared.lock().unwrap().insert(TokenId(1), (mid50_book(), true));
        mm.quote().await;
        assert!(
            mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)),
            "the ASK returns once the signal eases and the cooldown lapses"
        );
        assert!(
            mm.qm.tracked().contains_key(&(TokenId(1), Side::Bid)),
            "the BID remains placed throughout"
        );
    }

    /// The pull COOLDOWN holds a side out even AFTER the live signal eases, so a
    /// flickering signal can't churn the quote (a cancel+replace would reset
    /// Polymarket's time-weighted reward score). Same strong-imbalance cycle pulls
    /// the ask; with a long cooldown the following BALANCED cycle keeps it pulled.
    #[tokio::test]
    async fn rewardfarm_pulls_respect_cooldown_after_signal_eases() {
        let tokens = vec![TokenId(1)];
        let imbalanced = cent_book(
            &[(48, 100 * SH), (47, 1000 * SH), (46, 1000 * SH)],
            &[(52, 100 * SH)],
        );
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (imbalanced, true))]));
        let mut params = mk_params(200, 5.0);
        params.policy = Policy::RewardFarm;
        params.pull_threshold = 0.3;
        // Long cooldown ⇒ the next cycle is still within it.
        params.pull_cooldown_ms = 60_000;
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
        mm.reward_by_token.insert(TokenId(1), (1.0, 3.0, 100.0));

        // Cycle 1: ask pulled on the adverse signal (cooldown armed to now + 60s).
        mm.quote().await;
        assert!(
            !mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)),
            "ask pulled on the adverse signal"
        );

        // Cycle 2: a balanced book eases the live signal, but the cooldown has NOT
        // lapsed ⇒ the ask STAYS pulled (no flicker back in).
        shared.lock().unwrap().insert(TokenId(1), (mid50_book(), true));
        mm.quote().await;
        assert!(
            !mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)),
            "ask stays pulled while the cooldown runs, even though the signal eased"
        );
    }

    /// Isolation LOCK: SpreadCapture must be byte-for-byte unaffected by the
    /// Phase-A signal/pull block. On the SAME strongly bid-imbalanced book that
    /// pulls the ask under RewardFarm, SpreadCapture still quotes BOTH sides (the
    /// signal block is gated out entirely, as is the per-side drop).
    #[tokio::test]
    async fn spreadcapture_ignores_adverse_signal_quotes_both_sides() {
        let tokens = vec![TokenId(1)];
        let imbalanced = cent_book(
            &[(48, 100 * SH), (47, 1000 * SH), (46, 1000 * SH)],
            &[(52, 100 * SH)],
        );
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (imbalanced, true))]));
        // SpreadCapture (mk_params default); a low pull_threshold that WOULD pull
        // under RewardFarm — proving the gate, not the threshold, is what isolates.
        let mut params = mk_params(200, 5.0);
        params.pull_threshold = 0.3;
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.quote().await;
        assert!(
            mm.qm.tracked().contains_key(&(TokenId(1), Side::Bid)),
            "SpreadCapture still quotes the bid"
        );
        assert!(
            mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)),
            "SpreadCapture is unaffected by the adverse signal — ask still quoted"
        );
    }

    // ── Task A7: integration A/B — quote-pull lowers adverse fills (spec §4) ────

    /// Per-run outcome of [`reward_farm_pull_reduces_adverse_fills_on_adverse_feed`]'s
    /// scripted feed: the ASK-fill count plus, per phase, whether each side was
    /// QUOTING (tracked) right after that cycle's `quote()` — captured BEFORE
    /// fills settle so a same-cycle full fill can't mask a side that WAS quoted.
    struct AdverseRun {
        /// Total ASK ("Sell") fills booked across the whole feed (the proxy).
        ask_fills: u32,
        /// Per adverse cycle: was the ASK still quoting after `quote()`?
        ask_quoting_adverse: Vec<bool>,
        /// Per adverse cycle: was the (safe) BID still quoting after `quote()`?
        bid_quoting_adverse: Vec<bool>,
        /// Per balanced cycle: was the ASK quoting after `quote()`?
        ask_quoting_balanced: Vec<bool>,
    }

    /// Integration A/B proving the Phase-A quote-pull (Task A5) reduces adverse
    /// selection on a synthetic *adverse* feed. Two RewardFarm loops run the SAME
    /// scripted book sequence with the paper taker-fill sim ON; the ONLY
    /// difference is `pull_threshold`:
    ///   * pull-ON  (0.3): the blended signal clears the threshold during the
    ///     adverse up-move ⇒ the endangered ASK is pulled.
    ///   * pull-OFF (1.0): `combined_signal = (imbalance + clamp(momentum))/2` is
    ///     HALVED, so on this feed it tops out well under 1.0 ⇒ the ASK is NEVER
    ///     pulled (the spec's "pull effectively off" arm).
    ///
    /// SIGNAL SETUP (the implementer's job, spec note): `combined_signal` averages
    /// imbalance + momentum, so a coherent pull needs BOTH. The feed scripts a
    /// balanced phase (imbalance 0, flat microprice ⇒ momentum 0 ⇒ no pull), then
    /// a strongly bid-imbalanced + upward-trending phase: heavy bid depth BEHIND a
    /// balanced 100-share touch (⇒ summed top-3 imbalance ~0.9) while the whole
    /// book ticks UP one cent per cycle (⇒ the size-weighted microprice rises ⇒
    /// positive momentum). Blended ⇒ signal ~0.45 (clears 0.3, far below 1.0).
    ///
    /// PROXY (cleanest the paper harness measures): ASK ("Sell") fill COUNT. Both
    /// runs take IDENTICAL ask fills in the balanced phase (neither pulls); then
    /// pull-ON takes ZERO further ask fills in the adverse phase (ask omitted)
    /// while pull-OFF keeps resting the ask into the up-move and is repeatedly
    /// lifted — so pull-ON ends with STRICTLY FEWER adverse ask fills (less
    /// adverse exposure). Asserted as `pull_on_ask_fills < pull_off_ask_fills`.
    #[tokio::test]
    async fn reward_farm_pull_reduces_adverse_fills_on_adverse_feed() {
        const N_BAL: usize = 2; // balanced cycles
        const N_ADV: usize = 5; // adverse (bid-heavy, rising) cycles

        // Balanced book: symmetric top-3 depth ⇒ imbalance 0; equal touch sizes
        // pin the microprice flat at 0.505 across cycles ⇒ momentum 0 ⇒ no pull.
        fn balanced() -> Book {
            cent_book(
                &[(50, 100 * SH), (49, 100 * SH), (48, 100 * SH)],
                &[(51, 100 * SH), (52, 100 * SH), (53, 100 * SH)],
            )
        }
        // Adverse cycle k: heavy bid depth BEHIND a 100-share touch (⇒ summed
        // top-3 imbalance ~0.9) with the whole book ticked UP one cent per cycle
        // (⇒ the size-weighted microprice rises 0.505→… ⇒ positive momentum). The
        // 100-share touch keeps the microprice ON the mid so momentum comes purely
        // from the rising price, and the depth behind it drives the imbalance.
        fn adverse(k: u16) -> Book {
            cent_book(
                &[(50 + k, 100 * SH), (49 + k, 1000 * SH), (48 + k, 1000 * SH)],
                &[(51 + k, 100 * SH)],
            )
        }

        async fn run_adverse_feed(pull_threshold: f64) -> AdverseRun {
            let token = TokenId(1);
            let tokens = vec![token];
            let (fetcher, shared) =
                controllable_fetcher(&tokens, HashMap::from([(token, (balanced(), true))]));
            let mut params = mk_params(200, 5.0);
            params.policy = Policy::RewardFarm;
            params.pull_threshold = pull_threshold;
            // The adverse signal re-fires EVERY adverse cycle, so a 0 cooldown
            // keeps the proxy a clean function of the live feed (not a lingering
            // cooldown) — the cooldown's own effect is covered by its dedicated test.
            params.pull_cooldown_ms = 0;
            // Steady taker flow so a RESTING side actually fills each cycle.
            params.paper_taker_fill_pct = 25;
            let (mut mm, mut store_rx, _produced) =
                build_loop_tally(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));
            // A funded, eligible reward market: min_incentive_size 1 sh (below the
            // 100-sh touch ⇒ no size-cutoff), 3¢ band, $100/day.
            mm.reward_by_token.insert(token, (1.0, 3.0, 100.0));

            let mut run = AdverseRun {
                ask_fills: 0,
                ask_quoting_adverse: Vec::new(),
                bid_quoting_adverse: Vec::new(),
                ask_quoting_balanced: Vec::new(),
            };
            for i in 0..(N_BAL + N_ADV) {
                let adverse_phase = i >= N_BAL;
                let book = if adverse_phase {
                    adverse((i - N_BAL) as u16)
                } else {
                    balanced()
                };
                shared.lock().unwrap().insert(token, (book, true));
                mm.quote().await;
                // Inspect the PLACEMENT decision before fills settle this cycle.
                let ask_quoting = mm.qm.tracked().contains_key(&(token, Side::Ask));
                let bid_quoting = mm.qm.tracked().contains_key(&(token, Side::Bid));
                if adverse_phase {
                    run.ask_quoting_adverse.push(ask_quoting);
                    run.bid_quoting_adverse.push(bid_quoting);
                } else {
                    run.ask_quoting_balanced.push(ask_quoting);
                }
                mm.consume_fills().await;
                // Count ASK ("Sell") fills booked this cycle; drain the rest so the
                // store channel never backs up.
                while let Ok(msg) = store_rx.try_recv() {
                    if let StoreMsg::FillSigned(row, _) = msg
                        && row.action == side_action(Side::Ask)
                    {
                        run.ask_fills += 1;
                    }
                }
            }
            run
        }

        let on = run_adverse_feed(0.3).await; // pull ON
        let off = run_adverse_feed(1.0).await; // pull effectively OFF

        // (a) The pull is a ONE-SIDED, signal-specific step-aside:
        //   - balanced phase: pull-ON still quotes the ask (proves it's not a
        //     blanket stop), and
        //   - adverse phase: pull-ON omits the ask but KEEPS the safe bid, while
        //     pull-OFF keeps quoting the ask throughout the up-move.
        assert!(
            on.ask_quoting_balanced.iter().all(|&q| q),
            "pull-ON still quotes the ask in the BALANCED phase (pull is adverse-specific): {:?}",
            on.ask_quoting_balanced
        );
        assert!(
            on.ask_quoting_adverse.iter().all(|&q| !q),
            "pull-ON must OMIT the ask every adverse cycle: {:?}",
            on.ask_quoting_adverse
        );
        assert!(
            on.bid_quoting_adverse.iter().all(|&q| q),
            "pull-ON keeps the safe BID quoting (one-sided pull): {:?}",
            on.bid_quoting_adverse
        );
        assert!(
            off.ask_quoting_adverse.iter().all(|&q| q),
            "pull-OFF (threshold 1.0) keeps quoting the ask through the adverse phase: {:?}",
            off.ask_quoting_adverse
        );

        // (b) PROXY: pull-ON books strictly FEWER adverse ASK fills than pull-OFF.
        eprintln!(
            "A7 adverse-feed A/B: pull_on_ask_fills={} pull_off_ask_fills={}",
            on.ask_fills, off.ask_fills
        );
        assert!(off.ask_fills > 0, "sanity: pull-OFF must take some ask fills");
        assert!(
            on.ask_fills < off.ask_fills,
            "quote-pull must lower adverse ask fills: pull_on={} !< pull_off={}",
            on.ask_fills,
            off.ask_fills
        );
    }

    // ── Inventory skew (Task 4.3) ──────────────────────────────────────────────

    /// $5 per-market cap valued at the 0.50 mid → 10 shares = full inventory.
    const SKEW_CAP_MICRO: i128 = 5_000_000; // $5
    const FULL_CAP_SHARES: i128 = 10 * SH as i128; // 10 sh @ $0.50 = $5

    /// Same book/spread as `compute_quotes_symmetric_around_mid`, but a LONG net
    /// shifts BOTH quotes strictly DOWN (offload), a SHORT shifts BOTH strictly
    /// UP, and flat (net 0) is byte-identical to the Task-4.2 symmetric quote.
    #[test]
    fn skew_long_inventory_shifts_both_quotes_down() {
        let book = mid50_book(); // mid 0.50
        let params = mk_params_skew(200, 5.0, 150); // half = 0.01; full skew = 1.5¢

        // Flat → no skew → the Task-4.2 symmetric quote (0.49 / 0.51).
        let (fb, fa) = compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, SKEW_CAP_MICRO);
        let (fb, fa) = (fb.expect("flat bid"), fa.expect("flat ask"));
        assert_eq!((fb.price, fa.price), (px(49), px(51)), "flat == Task 4.2");

        // Full-cap LONG → fair −1.5¢ to 0.485 → bid 0.47 / ask 0.50, both strictly
        // below the flat quote.
        let (lb, la) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, FULL_CAP_SHARES, SKEW_CAP_MICRO);
        let (lb, la) = (lb.expect("long bid"), la.expect("long ask"));
        assert_eq!((lb.price, la.price), (px(47), px(50)), "long lowers both quotes");
        assert!(lb.price.get() < fb.price.get() && la.price.get() < fa.price.get());

        // Full-cap SHORT → fair +1.5¢ to 0.515 → bid 0.50 / ask 0.53, both strictly
        // above the flat quote.
        let (sb, sa) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, -FULL_CAP_SHARES, SKEW_CAP_MICRO);
        let (sb, sa) = (sb.expect("short bid"), sa.expect("short ask"));
        assert_eq!((sb.price, sa.price), (px(50), px(53)), "short raises both quotes");
        assert!(sb.price.get() > fb.price.get() && sa.price.get() > fa.price.get());
    }

    /// The skew magnitude scales LINEARLY with inventory: full cap ≈ the full
    /// `inventory_skew_bps` shift, half cap ≈ half, flat = none, and inventory
    /// beyond the cap clamps to the full shift (`r` saturates at ±1).
    #[test]
    fn skew_magnitude_scales_with_inventory() {
        const MID: i128 = 500_000;
        let full_micro = i128::from(150u32) * 100; // 1.5¢ = 15_000 µUSDC at full cap

        // Flat → no shift.
        assert_eq!(skew_fair(MID, 0, SKEW_CAP_MICRO, 150), MID);

        // Full-cap long → exactly the configured max shift, DOWN.
        let full_long = skew_fair(MID, FULL_CAP_SHARES, SKEW_CAP_MICRO, 150);
        assert_eq!(MID - full_long, full_micro, "full cap → full skew");

        // Half-cap long → ~half the shift (exact here: 7_500).
        let half_long = skew_fair(MID, FULL_CAP_SHARES / 2, SKEW_CAP_MICRO, 150);
        assert_eq!(MID - half_long, full_micro / 2, "half cap → half skew");

        // Short is symmetric: same magnitude, opposite sign (UP).
        let full_short = skew_fair(MID, -FULL_CAP_SHARES, SKEW_CAP_MICRO, 150);
        assert_eq!(full_short - MID, full_micro, "short skews up by the same amount");

        // Beyond the cap clamps to the full shift (no runaway).
        let over_cap = skew_fair(MID, 10 * FULL_CAP_SHARES, SKEW_CAP_MICRO, 150);
        assert_eq!(over_cap, full_long, "inventory past the cap clamps to full skew");
    }

    /// Extreme inventory + a tiny spread near a book edge must never cross or
    /// leave the interior tick range, and never panic: a side pushed out is just
    /// skipped, and a still-valid quote stays strictly non-crossing.
    #[test]
    fn skew_never_crosses_or_leaves_range() {
        let extreme = 1_000_000_000i128; // ≫ any cap → ratio saturates at ±1
        let tiny_cap = 1_000_000i128; // $1
        let params = mk_params_skew(1, 5.0, 150); // 1 bp spread, 1.5¢ full skew

        // Low edge: bid 0.01 / ask 0.03 (mid 0.02). A full-cap LONG skews fair
        // below tick 1 → the bid leaves [1, 99] → token skipped (no panic).
        let low = cent_book(&[(1, 100 * SH)], &[(3, 100 * SH)]);
        let (lb, la) = compute_quotes(&low, TokenId(1), &params, params.max_quote_micro, extreme, tiny_cap);
        assert!(lb.is_none() && la.is_none(), "skew past the low edge skips the token");

        // High edge: bid 0.97 / ask 0.99 (mid 0.98). A full-cap SHORT skews fair
        // above tick 99 → the ask leaves the range → token skipped.
        let high = cent_book(&[(97, 100 * SH)], &[(99, 100 * SH)]);
        let (hb, ha) = compute_quotes(&high, TokenId(1), &params, params.max_quote_micro, -extreme, tiny_cap);
        assert!(hb.is_none() && ha.is_none(), "skew past the high edge skips the token");

        // Mid-book: an extreme long with a sub-tick spread still yields a VALID,
        // strictly non-crossing quote (skew shifts it, the clamps keep ask > bid).
        let mid = mid50_book();
        let (mb, ma) = compute_quotes(&mid, TokenId(1), &params, params.max_quote_micro, extreme, SKEW_CAP_MICRO);
        let (mb, ma) = (mb.expect("mid bid"), ma.expect("mid ask"));
        assert!(ma.price.get() > mb.price.get(), "skewed quote stays non-crossing");
    }

    /// Regression (the dropped-fills root trigger): a heavy long + large skew
    /// would push the ASK below the best bid — a marketable post-only the venue
    /// REJECTS, which used to abort the whole reconcile pass and orphan the bid.
    /// The book clamp pins the ask to the most-aggressive NON-crossing price
    /// (best_bid + 1) instead of emitting a crossing quote.
    #[test]
    fn skew_clamps_a_crossing_ask_to_the_book() {
        let book = mid50_book(); // best bid 0.49, best ask 0.51
        let params = mk_params_skew(200, 5.0, 400); // 4¢ full skew → raw ask 0.47 (crosses)
        let (bid, ask) = compute_quotes(
            &book,
            TokenId(1),
            &params,
            params.max_quote_micro,
            FULL_CAP_SHARES,
            SKEW_CAP_MICRO,
        );
        // mid50_book is best bid 0.48 / best ask 0.52, so the most-aggressive
        // non-crossing ask is best_bid + 1 = 49 (the raw skewed ask 0.47 crosses).
        let ask = ask.expect("ask present");
        assert_eq!(ask.price.get(), 49, "crossing ask clamped to best_bid+1, not the raw 0.47");
        assert!(ask.price.get() > 48, "ask must never cross (sit at/below) the best bid");
        if let Some(b) = bid {
            assert!(b.price.get() < 52, "bid must never cross (sit at/above) the best ask");
            assert!(b.price.get() < ask.price.get(), "bid stays below ask");
        }
    }

    // ── Inventory cap gating ───────────────────────────────────────────────────

    /// `InventoryRisk::check_quote` is consulted: a quote it rejects is NOT
    /// placed, while a de-risking quote on the same token still is. A long is
    /// seeded above the per-side size and the per-market cap set tight, so the
    /// BID (further increase) is rejected while the ASK (reduce) is approved.
    #[tokio::test]
    async fn inventory_rejected_bid_is_not_placed_but_ask_is() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let mut inv_cfg = generous_inv();
        inv_cfg.max_inventory_usd = Usdc(5_000_000); // $5 per-market cap
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // Seed a long well above the per-side size so the ASK de-risks (always
        // approved) while the BID (further increase) breaches the $5 cap.
        mm.inv
            .on_fill(TokenId(1), (11 * SH) as i128, Usdc(-5_500_000));

        mm.quote().await;
        let tracked = mm.qm.tracked();
        assert!(
            !tracked.contains_key(&(TokenId(1), Side::Bid)),
            "the cap-breaching bid must not be placed"
        );
        assert!(
            tracked.contains_key(&(TokenId(1), Side::Ask)),
            "the de-risking ask must still be placed"
        );
    }

    // ── Fills → inventory + positions + store ──────────────────────────────────

    /// A bid fill flows to `InventoryRisk::on_fill` + `PositionBook` and writes a
    /// `"mm"`-tagged fill row (plus its FK-parent order row); equity/inventory
    /// move as expected.
    #[tokio::test]
    async fn bid_fill_books_inventory_positions_and_store() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, mut store_rx, status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.quote().await; // bid 0.49 / ask 0.51
        assert!(mm.qm.tracked().contains_key(&(TokenId(1), Side::Bid)));

        // Seller crosses DOWN to our bid (best_ask ≤ 0.49) but not up to our ask.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(48, 100 * SH)], &[(49, 100 * SH)]), true));
        mm.consume_fills().await;

        let net = mm.inv.net(TokenId(1));
        assert!(net > 0, "a bid fill makes us long");

        let mut fills = Vec::new();
        let mut order_rows = 0usize;
        while let Ok(msg) = store_rx.try_recv() {
            match msg {
                StoreMsg::FillSigned(row, _) => fills.push(row),
                StoreMsg::OrderInsert(row, _) => {
                    assert_eq!(row.strategy, "mm");
                    order_rows += 1;
                }
                _ => {}
            }
        }
        assert_eq!(fills.len(), 1, "exactly one fill booked");
        let f = &fills[0];
        assert_eq!(f.strategy, "mm");
        assert_eq!(f.action, "Buy");
        assert_eq!(f.qty_micro, net as i64, "fill qty == net (flat → long)");
        assert!(f.cash_micro < 0, "a buy pays cash out");
        assert_eq!(f.fee_micro, 0, "makers pay 0 fee");
        assert!(order_rows >= 1, "the resting order row (FK parent) was written");

        // Status reflects the open long + cash paid out (no profit yet).
        mm.publish_status().await;
        let st = status_rx.borrow();
        assert_eq!(st.open_positions, 1);
        assert!(st.cash_micro < 0);
        assert!(st.halted.is_none());
    }

    /// An ask fill books a SHORT with the correct signs (proves the signed
    /// fill→inventory mapping for both sides).
    #[tokio::test]
    async fn ask_fill_books_a_short_with_correct_signs() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, mut store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.quote().await; // bid 0.49 / ask 0.51
        // Buyer crosses UP to our ask (best_bid ≥ 0.51), not down to our bid.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(51, 100 * SH)], &[(52, 100 * SH)]), true));
        mm.consume_fills().await;

        let net = mm.inv.net(TokenId(1));
        assert!(net < 0, "an ask fill makes us short");

        let mut fill = None;
        while let Ok(msg) = store_rx.try_recv() {
            if let StoreMsg::FillSigned(row, _) = msg {
                fill = Some(row);
            }
        }
        let f = fill.expect("a fill row");
        assert_eq!(f.action, "Sell");
        assert_eq!(f.strategy, "mm");
        assert!(f.cash_micro > 0, "a sell receives cash");
        assert_eq!(f.qty_micro, (-net) as i64, "fill qty == |net| (flat → short)");
    }

    /// End-to-end (Task 4.2b): an MM ask-fill opens a SHORT, and the SIGNED store
    /// route must DURABLY persist the fill row. The strict `Fill` route would
    /// Oversell-drop it (a `write_error`, no row). Wires the loop's store channel
    /// to a real writer + store and asserts the short-open fill landed cleanly.
    #[tokio::test]
    async fn ask_fill_opens_short_and_persists_via_signed_route() {
        use pm_store::Store;
        use pm_store::writer::run_writer;

        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        // Wire the loop's store channel to a real writer over an in-memory store.
        let store = Store::open_in_memory().unwrap();
        let writer = tokio::spawn(run_writer(store, store_rx));

        mm.quote().await; // places bid + ask and their FK-parent order rows
        // Buyer crosses UP to our ask (best_bid ≥ 0.51) → we SELL to open a short.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(51, 100 * SH)], &[(52, 100 * SH)]), true));
        mm.consume_fills().await;
        assert!(mm.inv.net(TokenId(1)) < 0, "an ask fill makes us short");

        // Drop the loop (and thus its store_tx) so the writer drains and returns.
        drop(mm);
        let store = writer.await.unwrap();
        assert_eq!(
            store.write_errors, 0,
            "the signed short-open fill must persist, not Oversell-drop"
        );
        assert_eq!(store.count_fills().unwrap(), 1, "exactly one fill row persisted");
        // Durable signed position reflects the open short (net < 0, basis < 0).
        let (net, cost) = store.position(1).unwrap();
        assert!(
            net < 0 && cost < 0,
            "signed position reflects the open short: ({net}, {cost})"
        );
    }

    // ── Pause / resume ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn pause_cancels_quotes_then_resume_requotes() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.tick().await; // active → places bid + ask
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "two resting quotes when active"
        );

        // Pause cancels resting quotes and stops quoting.
        mm.paused = true;
        mm.cancel_all().await;
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "pause cancels resting quotes"
        );
        mm.tick().await; // paused → no new quotes
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "paused → none placed"
        );

        // Resume → quotes return on the next tick.
        mm.paused = false;
        mm.tick().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "resume re-quotes"
        );
    }

    // ── Volatility quote-pull (Task 4.3) ───────────────────────────────────────

    /// A large + FAST mid move makes `vol_hint` fire → the token's resting quotes
    /// are CANCELLED and NOT replaced this tick (a pull, not a replace). A later
    /// calm tick (the move settled) re-quotes — the pull is non-sticky.
    #[tokio::test]
    async fn vol_hint_pulls_quotes_without_replace() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // vol_pull_ticks = 5 (5¢ threshold). A long window so the test never
        // races the wall clock between successive quote() calls.
        let mut inv_cfg = generous_inv();
        inv_cfg.vol_window = Duration::from_secs(3600);
        let params = mk_params(200, 5.0); // skew OFF — isolate the pull
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // Tick 1 (mid 0.50): vol_hint's FIRST observation can't fire → quotes rest.
        mm.quote().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "a calm first tick rests bid + ask"
        );

        // A large + FAST jump 0.50 → 0.70 (+20¢ ≫ 5¢) within the window.
        shared.lock().unwrap().insert(
            TokenId(1),
            (cent_book(&[(68, 100 * SH)], &[(72, 100 * SH)]), true),
        );

        // Tick 2: vol_hint fires → PULL. The resting quotes are cancelled and
        // NONE are placed (asserting the cancel happened and there was no replace).
        mm.quote().await;
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "vol pull cancels the resting quotes WITHOUT replacing"
        );
        assert!(mm.qm.tracked().is_empty(), "nothing tracked while pulled");

        // Tick 3 (book unchanged at 0.70): the move has settled (0¢ since last) →
        // vol_hint quiet → the token re-quotes (the pull is non-sticky).
        mm.quote().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "a calm later tick re-quotes"
        );
    }

    // ── Inventory halt (safety stop) ───────────────────────────────────────────

    #[tokio::test]
    async fn inventory_stop_loss_halts_and_cancels_quotes() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let mut inv_cfg = generous_inv();
        inv_cfg.inventory_stop_loss_usd = Usdc(500_000); // $0.50 stop
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // Acquire a ~$5 long via a bid fill.
        mm.quote().await;
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(48, 100 * SH)], &[(49, 100 * SH)]), true));
        mm.consume_fills().await;
        assert!(mm.inv.net(TokenId(1)) > 0);
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            1,
            "the unfilled ask still rests before the halt"
        );

        // Crash the price (mid ≈ 0.10): the unrealized bleed on the long far
        // exceeds the $0.50 stop → StopLoss latches.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(9, 100 * SH)], &[(11, 100 * SH)]), true));
        mm.mark_and_check().await;

        assert!(mm.halted, "stop-loss latched");
        assert_eq!(mm.inv.halted(), Some(InvHalt::StopLoss));
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "a halt cancels all resting quotes"
        );

        mm.publish_status().await;
        assert!(
            status_rx.borrow().halted.is_some(),
            "status reflects the latched halt"
        );

        // Latched: even with a healthy book back, a further tick does NOT re-quote.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (mid50_book(), true));
        mm.tick().await;
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "the halt is latched → no re-quote"
        );
    }

    // ── Task 9: persistent UTC-day loss cap (binds across auto-restarts) ───────

    /// SAFETY NET: when the persisted store shows today's `"mm"` P&L is already
    /// at/under the daily-loss cap, the loop must START latched — so the periodic
    /// auto-restart can NOT re-arm the bot and let it keep bleeding. Mirrors what
    /// `run_mm_loop` does at startup (arm the gate from a `ReadStore`), then proves
    /// a quote pass places NOTHING.
    #[tokio::test]
    async fn starts_halted_when_day_already_at_loss_cap() {
        use pm_store::Store;
        use pm_store::read::ReadStore;

        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // $50 daily-loss cap (same floor as the in-session InvHalt::DailyLoss).
        let mut inv_cfg = generous_inv();
        inv_cfg.daily_loss_usd = Usdc(50_000_000);
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // A real file-backed store with a losing "mm" snapshot on UTC day 0.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daycap.sqlite");
        let mut s = Store::open(&path).unwrap();
        let ts = 1_000i64; // within UTC day 0
        // realized −$60 + unrealized $0 = −$60 ≤ −$50 cap → past it.
        s.record_pnl_at(ts, 0, -60_000_000, 0, -60_000_000, "mm").unwrap();
        drop(s);
        let read = ReadStore::open(&path).unwrap();

        // Arm exactly as `run_mm_loop` does at startup, for that snapshot's day.
        mm.arm_day_loss_gate(Some(&read), ts);
        assert!(
            mm.day_loss_halted,
            "today already at/under the daily-loss cap → latched at startup"
        );

        // The persisted latch stops quoting: a full quote pass places NOTHING.
        mm.quote().await;
        assert!(mm.qm.tracked().is_empty(), "no quotes tracked under the day-loss latch");
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "quote() places nothing while the day-loss latch is set"
        );

        // Hardening 2: the latch is SURFACED in the published status so operators
        // see the MM is halted — even though InvHalt is None here (only the
        // persistent day-loss gate fired).
        mm.publish_status().await;
        assert_eq!(
            status_rx.borrow().halted.as_deref(),
            Some("DayLossCap"),
            "the persistent day-loss latch is surfaced in StrategyStatus.halted"
        );
    }

    /// The DEFAULT must not halt a normal run: a persisted day P&L comfortably
    /// inside the cap leaves the latch clear and the MM quotes both sides as
    /// usual. Guards against the gate accidentally binding when it should not
    /// (the converse of the safety net above). The "no snapshot → P&L 0 → not
    /// halted" path is covered by the store-level `day_pnl_micro` test.
    #[tokio::test]
    async fn does_not_start_halted_when_day_pnl_within_cap() {
        use pm_store::Store;
        use pm_store::read::ReadStore;

        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let mut inv_cfg = generous_inv();
        inv_cfg.daily_loss_usd = Usdc(50_000_000); // $50 cap
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // A small loss WELL inside the cap (−$10 > −$50).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.record_pnl_at(1_000, 0, -10_000_000, 0, -10_000_000, "mm").unwrap();
        drop(s);
        let read = ReadStore::open(&path).unwrap();

        mm.arm_day_loss_gate(Some(&read), 1_000);
        assert!(!mm.day_loss_halted, "within the cap → NOT latched");

        // And it quotes normally — the gate did not interfere.
        mm.quote().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "within the cap → the MM quotes both sides normally"
        );
    }

    // ── I3: cumulative day-realized ledger closes the summed-sub-cap gap ────────

    /// I3 SAFETY NET: the persistent day-loss cap must also catch MANY sub-cap
    /// *realized* sessions whose losses SUM over the cap across a day — the gap
    /// the per-session snapshot-MIN gate (`day_pnl_micro`) cannot see. Seed the
    /// store's `day_realized` ledger for "mm" today with several deltas EACH under
    /// the cap that SUM past it, plus a couple of SUB-cap `pnl_snapshots` rows (so
    /// the snapshot gate ALONE would NOT latch). The loop must arm HALTED at
    /// startup from the LEDGER arm and `quote()` must place nothing.
    #[tokio::test]
    async fn day_loss_latches_on_summed_sub_cap_realized() {
        use pm_store::Store;
        use pm_store::read::ReadStore;

        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // $6 daily-loss cap (the SAME floor the in-session InvHalt::DailyLoss uses).
        let mut inv_cfg = generous_inv();
        inv_cfg.daily_loss_usd = Usdc(6_000_000);
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("summed.sqlite");
        let mut s = Store::open(&path).unwrap();
        let ts = 1_000i64; // within UTC day 0
        let day = utc_day_from_ms(ts);
        // FIVE sub-cap realizing sessions (−$2 each, each < the $6 cap) that SUM
        // to −$10 — past the cap. This is exactly the summed-sub-cap-realized case.
        for _ in 0..5 {
            s.add_day_realized(day, "mm", -2_000_000).unwrap();
        }
        // A couple of SUB-cap snapshots: worst point −$2 (> −$6), so the
        // snapshot-MIN gate ALONE would NOT latch — proving the LEDGER is what
        // catches this day.
        s.record_pnl_at(ts, 0, -2_000_000, 0, -2_000_000, "mm").unwrap();
        s.record_pnl_at(ts + 1, 0, -1_000_000, 0, -1_000_000, "mm").unwrap();
        drop(s);
        let read = ReadStore::open(&path).unwrap();

        // PROOF the snapshot gate is blind here: its worst point is −$2, well
        // inside the −$6 cap — so without the ledger the bot would re-arm and bleed.
        assert_eq!(
            read.day_pnl_micro("mm", day).unwrap(),
            -2_000_000,
            "no single snapshot row breaches the cap — the MIN gate alone would NOT halt"
        );
        // The cumulative realized ledger, however, is past the cap.
        assert_eq!(read.day_realized_micro("mm", day).unwrap(), -10_000_000);

        // Arm exactly as run_mm_loop does at startup: the LEDGER arm latches.
        mm.arm_day_loss_gate(Some(&read), ts);
        assert!(
            mm.day_loss_halted,
            "summed sub-cap realized past the cap → latched at startup via the ledger"
        );

        // The latch stops quoting: a full quote pass places NOTHING.
        mm.quote().await;
        assert!(
            mm.qm.tracked().is_empty(),
            "no quotes tracked under the summed-realized day-loss latch"
        );
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "quote() places nothing while the ledger-armed day-loss latch is set"
        );

        // Surfaced in status as DayLossCap (same as the snapshot-armed path).
        mm.publish_status().await;
        assert_eq!(
            status_rx.borrow().halted.as_deref(),
            Some("DayLossCap"),
            "the ledger-armed day-loss latch is surfaced in StrategyStatus.halted"
        );
    }

    /// I3 per-cycle RE-LATCH: a session that started UNDER the cap must still
    /// halt MID-session once the cumulative day-realized ledger crosses it — not
    /// only at the next auto-restart. Start un-halted, attach a read handle whose
    /// ledger (seeded for TODAY, since `tick` keys off `now_ms()`) is past the
    /// cap, then a single `tick()` must latch the gate and place nothing.
    #[tokio::test]
    async fn day_loss_re_latches_mid_session_from_ledger() {
        use pm_store::Store;
        use pm_store::read::ReadStore;

        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let mut inv_cfg = generous_inv();
        inv_cfg.daily_loss_usd = Usdc(6_000_000); // $6 cap
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));
        assert!(!mm.day_loss_halted, "starts un-halted (no handle armed yet)");

        // Seed the LEDGER past the cap for TODAY via several sub-cap adds (−$8 >
        // −$6), then attach the read handle exactly as run_mm_loop retains it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("midsession.sqlite");
        let mut s = Store::open(&path).unwrap();
        let today = utc_day_from_ms(now_ms());
        for _ in 0..4 {
            s.add_day_realized(today, "mm", -2_000_000).unwrap();
        }
        drop(s);
        mm.day_loss_read = Some(ReadStore::open(&path).unwrap());

        // One cycle must RE-LATCH from the ledger and quote nothing thereafter.
        mm.tick().await;
        assert!(
            mm.day_loss_halted,
            "per-cycle re-check latches once the cumulative ledger crosses the cap mid-session"
        );
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "no quotes placed on the tick that trips the mid-session latch"
        );
    }

    // ── Kill → cancel + clean exit (drives the real run_mm_loop) ───────────────

    #[tokio::test]
    async fn kill_cancels_and_exits_cleanly() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, status_rx) = watch::channel(StrategyStatus::default());
        let (store_tx, _store_rx) = mpsc::channel(256);
        let ctx = StrategyCtx {
            registry: empty_registry(),
            fetcher: fetcher.clone(),
            store_tx,
            kill: Arc::clone(&kill),
            ctl_rx,
            status_tx,
        };
        let venue = PaperMakerVenue::new(fetcher);
        let token_market = token_market_for(&tokens);
        let run = tokio::spawn(run_mm_loop(
            venue,
            QuoteManager::new(),
            InventoryRisk::new(generous_inv()),
            PositionBook::default(),
            ctx,
            mk_params(200, 5.0),
            tokens,
            token_market,
            HashMap::new(),
            None,            // merger (M6-7): no relayer in this loop-lifecycle test
            HashMap::new(),  // cond_by_token (M6-7)
            HashMap::new(),  // venue_by_token (R1)
            None,            // data_api (R1)
            None,            // deposit_wallet (R1)
            Usdc(1_000_000_000),
            false,
            false,
            None,
        ));

        // Let it run a few quote cycles (10 ms interval), then kill it.
        tokio::time::sleep(Duration::from_millis(40)).await;
        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("mm did not exit within the timeout after kill")
            .expect("mm run task panicked");

        // It published at least one status while running (loop was live).
        let _ = status_rx;
    }

    // ── Paper END-TO-END: full MM cycle, ZERO live orders (Task 4.4) ──────────

    /// The centerpiece (spec §7/§11): drive the WHOLE market-making cycle —
    /// quote → fill → inventory → MtM → rebate — over a synthetic, controllable
    /// book through several ticks against the [`PaperMakerVenue`], asserting the
    /// captured spread, net-flat round-trip, tracked equity, and a positive
    /// maker-rebate accrual, with the rebate kept SEPARATE from equity.
    ///
    /// ZERO-LIVE INVARIANT (what this test guards): the only execution venue in
    /// the path is the `PaperMakerVenue` that [`build_loop`] constructs — the
    /// loop's `venue` field is statically `PaperMakerVenue<BookFetcher>`, so a
    /// `LiveVenue` is unrepresentable here. As a runtime witness, every persisted
    /// fill carries a `paper-…` order id (the paper venue's mint), proving no
    /// live order was ever placed.
    ///
    /// Fill-driving approach: place a symmetric bid+ask once, then rewrite the
    /// shared book between ticks so the paper sim crosses to ONE resting side per
    /// tick — best ask down to our bid, then best bid up to our ask — exactly as
    /// the Task-4.2 fill tests do (we drive `consume_fills` directly so the
    /// resting orders stay put at their posted prices rather than being re-quoted
    /// away). The crossing-liquidity CAP is set to a fixed 5 shares on each side
    /// so the buy and the equal sell net inventory back to zero.
    ///
    /// The risk-stop arm of the cycle is covered by
    /// [`inventory_stop_loss_halts_and_cancels_quotes`] above (drives an
    /// unrealized loss past `inventory_stop_loss_usd` → quotes cancelled +
    /// `status.halted` set); it is referenced here rather than duplicated.
    #[tokio::test]
    async fn paper_end_to_end_quote_fill_inventory_mtm_rebate() {
        const FIVE_SH: u64 = 5 * SH; // the fixed per-side crossing-liquidity cap

        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // spread 200 bps → bid 0.49 / ask 0.51 around the 0.50 mid; a non-zero
        // rebate estimate (50 bps of filled notional) so the accrual is visible.
        let mut params = mk_params(200, 5.0);
        params.rebate_bps = 50;
        // Generous caps: this arm proves the happy-path cycle, NOT the halt
        // (which `inventory_stop_loss_halts_and_cancels_quotes` owns).
        let (mut mm, mut store_rx, status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        // ── Tick 1: QUOTE — a symmetric postOnly Gtc bid 0.49 + ask 0.51. ──
        mm.quote().await;
        assert!(mm.qm.tracked().contains_key(&(TokenId(1), Side::Bid)), "bid placed");
        assert!(mm.qm.tracked().contains_key(&(TokenId(1), Side::Ask)), "ask placed");
        assert_eq!(mm.venue.open_orders().await.unwrap().len(), 2, "two resting quotes");

        // ── Tick 2: a SELLER crosses down to our bid (best_ask ≤ 0.49), with
        // exactly 5 shares of crossing liquidity → the BID fills 5 sh @ 0.49. The
        // resting ask (no buyer at ≥ 0.51) does not fill this tick. ──
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(48, 100 * SH)], &[(49, FIVE_SH)]), true));
        mm.consume_fills().await;
        assert_eq!(mm.inv.net(TokenId(1)), FIVE_SH as i128, "bid fill → long 5 sh");

        // MtM: the live mark runs and stays benign under the generous stop.
        mm.mark_and_check().await;
        assert!(!mm.halted, "a 5-sh long marked near cost does not halt");

        // Status after the buy: one open long, marked at the 0.48 bid → a small
        // unrealized loss vs the 0.49 basis, no realized yet, and the rebate has
        // begun to accrue (50 bps × $2.45 notional = 12_250 µUSDC).
        mm.publish_status().await;
        {
            let st = status_rx.borrow();
            assert_eq!(st.open_positions, 1, "one open long");
            assert_eq!(st.cash_micro, -2_450_000, "paid 5 sh × $0.49");
            assert_eq!(st.realized_micro, 0, "no realized while still open");
            assert_eq!(st.unrealized_micro, -50_000, "bid-marked at 0.48 vs 0.49 basis");
            assert_eq!(st.equity_micro, -50_000, "equity = cash + bid mark");
            assert_eq!(st.rebate_micro, 12_250, "rebate accrued on the bid fill");
        }

        // ── Tick 3: a BUYER crosses up to our ask (best_bid ≥ 0.51), again with
        // exactly 5 shares → the ASK fills 5 sh @ 0.51. The (reduced) resting bid
        // has no seller at ≤ 0.49, so it does not fill. ──
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(51, FIVE_SH)], &[(52, 100 * SH)]), true));
        mm.consume_fills().await;

        // Inventory nets back to ZERO (bought 5 sh, sold 5 sh).
        assert_eq!(mm.inv.net(TokenId(1)), 0, "buy then equal sell → net flat");
        // Realized reflects the CAPTURED SPREAD: sold $0.51 − bought $0.49 over
        // 5 sh = +$0.10 (the authoritative inventory realized).
        assert_eq!(mm.inv.realized(TokenId(1)), Usdc(100_000), "captured spread +$0.10");

        mm.mark_and_check().await;
        assert!(!mm.halted, "flat + profitable → no halt");

        mm.publish_status().await;
        let st = status_rx.borrow();
        assert_eq!(st.open_positions, 0, "flat → no open positions");
        assert_eq!(st.unrealized_micro, 0, "nothing open to mark");
        assert_eq!(st.realized_micro, 100_000, "captured spread shows as realized");
        assert_eq!(st.equity_micro, 100_000, "equity tracks the booked spread");
        // REBATE is accrued and POSITIVE: 50 bps × ($2.45 + $2.55) notional =
        // 12_250 + 12_750 = 25_000 µUSDC ($0.025).
        assert_eq!(st.rebate_micro, 25_000, "rebate accrued across both fills");
        assert!(st.rebate_micro > 0, "a non-zero maker rebate accrued");
        // SEPARATION INVARIANT: equity is the booked spread ($0.10) ALONE. Had
        // the unverified, out-of-band rebate been folded in, equity would read
        // $0.125 (125_000) and inflate position P&L — assert it did NOT.
        assert_eq!(st.equity_micro, st.realized_micro, "equity is the spread alone");
        assert_ne!(
            st.equity_micro,
            st.realized_micro + st.rebate_micro,
            "the rebate must NOT be folded into equity"
        );
        drop(st);

        // ── ZERO-LIVE witness: exactly two fills persisted, BOTH via the paper
        // venue (paper-… order ids), a Buy then a Sell, makers paying 0 fee. ──
        let mut fills = Vec::new();
        while let Ok(msg) = store_rx.try_recv() {
            if let StoreMsg::FillSigned(row, _) = msg {
                fills.push(row);
            }
        }
        assert_eq!(fills.len(), 2, "exactly the bid fill + the ask fill persisted");
        assert!(
            fills.iter().all(|f| f.order_id.starts_with("paper-")),
            "every fill came from the PaperMakerVenue — no live order in the path"
        );
        assert!(fills.iter().all(|f| f.fee_micro == 0), "makers pay 0 fee on CLOB V2");
        assert_eq!(fills[0].action, "Buy", "first the bid fill");
        assert_eq!(fills[1].action, "Sell", "then the ask fill");
    }

    // ── Fill-accounting invariant: the MM must NEVER drop a venue fill ─────────
    //
    // The market maker books inventory/P&L from the fills its venue produces. If
    // it ever fails to resolve a fill (and `warn!`s "unknown resting order;
    // skipping"), that fill's inventory + realized P&L are silently lost — the
    // accounting bug these two tests guard against (Fix #1: `MakerFill.side`;
    // Fix #2: `QuoteManager::note_fill`).

    use std::sync::Mutex as StdMutex;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::{Context as LayerContext, Layer, SubscriberExt};

    use pm_execution::fills::MakerFill;
    use pm_execution::maker::OpenOrder;
    use pm_execution::venue::VenueError;

    /// A tracing layer that records every emitted event's `message`, so a test
    /// can assert the "unknown resting order" drop-warning never fired.
    #[derive(Clone, Default)]
    struct WarnCapture(Arc<StdMutex<Vec<String>>>);

    struct MsgVisitor<'a>(&'a mut String);
    impl Visit for MsgVisitor<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                use std::fmt::Write;
                let _ = write!(self.0, "{value:?}");
            }
        }
    }

    impl<S: Subscriber> Layer<S> for WarnCapture {
        fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
            let mut msg = String::new();
            event.record(&mut MsgVisitor(&mut msg));
            self.0.lock().unwrap().push(msg);
        }
    }

    /// Pass-through venue that records EVERY [`MakerFill`] the inner venue emits
    /// on `poll`, so a test can compare what the venue PRODUCED against what the
    /// MM actually BOOKED — a direct, message-text-independent check of the
    /// "no fill is dropped" invariant.
    struct TallyVenue<V> {
        inner: V,
        produced: Arc<StdMutex<Vec<(OrderId, u64)>>>,
    }

    impl<V: MakerVenue> MakerVenue for TallyVenue<V> {
        async fn place(&mut self, o: &MakerOrder) -> Result<OrderId, VenueError> {
            self.inner.place(o).await
        }
        async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError> {
            self.inner.cancel(id).await
        }
        async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError> {
            self.inner.replace(id, o).await
        }
        async fn open_orders(&mut self) -> Result<Vec<OpenOrder>, VenueError> {
            self.inner.open_orders().await
        }
    }

    impl<V: UserFillSource> UserFillSource for TallyVenue<V> {
        async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError> {
            let fills = self.inner.poll().await?;
            let mut p = self.produced.lock().unwrap();
            for f in &fills {
                p.push((f.order_id.clone(), f.qty.0));
            }
            Ok(fills)
        }
    }

    /// `build_loop` over a [`PaperMakerVenue`] (honoring `paper_taker_fill_pct`)
    /// WRAPPED in a [`TallyVenue`], returning the produced-fills tally handle so a
    /// test can assert booked == produced.
    #[allow(clippy::type_complexity)]
    fn build_loop_tally(
        fetcher: BookFetcher,
        inv_cfg: InventoryConfig,
        params: MmParams,
        tokens: Vec<TokenId>,
        capital: Usdc,
    ) -> (
        MmLoop<TallyVenue<PaperMakerVenue<BookFetcher>>>,
        mpsc::Receiver<StoreMsg>,
        Arc<StdMutex<Vec<(OrderId, u64)>>>,
    ) {
        let (store_tx, store_rx) = mpsc::channel(8192);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let produced = Arc::new(StdMutex::new(Vec::new()));
        let venue = TallyVenue {
            inner: PaperMakerVenue::new(fetcher.clone())
                .with_taker_fill_pct(params.paper_taker_fill_pct),
            produced: Arc::clone(&produced),
        };
        let notional_micro = params.max_quote_micro.min(capital.0).max(0);
        let token_market = token_market_for(&tokens);
        let (merge_tx, merge_rx) = mpsc::unbounded_channel();
        let (redeem_tx, redeem_rx) = mpsc::unbounded_channel();
        let mm = MmLoop {
            venue,
            qm: QuoteManager::new(),
            inv: InventoryRisk::new(inv_cfg),
            positions: PositionBook::default(),
            fetcher,
            store_tx,
            status_tx,
            params,
            tokens,
            token_market,
            complement: HashMap::new(),
            policy: params.policy,
            reward_by_token: HashMap::new(),
            notional_micro,
            placed: HashMap::new(),
            token_ts: HashMap::new(),
            rebate_accrued_micro: 0,
            paused: false,
            halted: false,
            day: utc_day_from_ms(now_ms()),
            day_loss_halted: false,
            day_loss_read: None,
            no_naked_shorts: false,
            vetoed: HashSet::new(),
            last_reward_sample: None,
            reward_status: None,
            reward_cumulative_est: 0.0,
            signals: HashMap::new(),
            pulled_until: HashMap::new(),
            last_signal: 0.0,
            last_pulled: false,
            merge_live_warned: false,
            merger: None,
            cond_by_token: HashMap::new(),
            merge_tx,
            merge_rx,
            merge_inflight: HashSet::new(),
            last_merge_sweep: Instant::now(),
            venue_by_token: HashMap::new(),
            data_api: None,
            deposit_wallet: None,
            redeem_tx,
            redeem_rx,
            redeem_sweep_inflight: false,
            last_redeem_sweep: Instant::now(),
        };
        (mm, store_rx, produced)
    }

    /// A venue that places nothing real and yields one SCRIPTED fill on its first
    /// `poll` — lets a test book a fill with a chosen `(order_id, token)`.
    struct ScriptVenue {
        fill: Option<MakerFill>,
    }
    impl MakerVenue for ScriptVenue {
        async fn place(&mut self, _: &MakerOrder) -> Result<OrderId, VenueError> {
            Ok(OrderId("script".into()))
        }
        async fn cancel(&mut self, _: &OrderId) -> Result<(), VenueError> {
            Ok(())
        }
        async fn replace(&mut self, _: &OrderId, _: &MakerOrder) -> Result<OrderId, VenueError> {
            Ok(OrderId("script".into()))
        }
        async fn open_orders(&mut self) -> Result<Vec<OpenOrder>, VenueError> {
            Ok(vec![])
        }
    }
    impl UserFillSource for ScriptVenue {
        async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError> {
            Ok(self.fill.take().into_iter().collect())
        }
    }

    /// REGRESSION (live cross-token fill mis-attribution): the user-WS stamps a
    /// fill with the TRADE's top-level asset_id, which on a COMPLEMENTARY match is
    /// the OTHER token of the market. The fill MUST book against OUR placed
    /// order's token (YES), not the WS-reported token (NO) — otherwise a YES sell
    /// is recorded as a phantom NO short (the live bug we hit).
    #[tokio::test]
    async fn fill_books_against_placed_token_not_ws_token() {
        let yes = TokenId(12);
        let no = TokenId(13);
        let oid = OrderId("0xours".into());
        // A WS fill stamped with the NO token (the trade's asset_id), no side, for
        // OUR order id — which we PLACED on YES as an Ask.
        let fill = MakerFill {
            order_id: oid.clone(),
            token: no,
            qty: Qty(10_000_000),
            px: px(40),
            side: None,
            trade_id: "t1".into(),
        };
        let (store_tx, _store_rx) = mpsc::channel(64);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let (merge_tx, merge_rx) = mpsc::unbounded_channel();
        let (redeem_tx, redeem_rx) = mpsc::unbounded_channel();
        let mut mm = MmLoop {
            venue: ScriptVenue { fill: Some(fill) },
            qm: QuoteManager::new(),
            inv: InventoryRisk::new(generous_inv()),
            positions: PositionBook::default(),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx,
            status_tx,
            params: mk_params(200, 5.0),
            tokens: vec![yes, no],
            token_market: HashMap::from([(yes, MarketId(0)), (no, MarketId(0))]),
            complement: HashMap::new(),
            policy: Policy::SpreadCapture,
            reward_by_token: HashMap::new(),
            notional_micro: 5_000_000,
            placed: HashMap::new(),
            token_ts: HashMap::new(),
            rebate_accrued_micro: 0,
            paused: false,
            halted: false,
            day: utc_day_from_ms(now_ms()),
            day_loss_halted: false,
            day_loss_read: None,
            no_naked_shorts: true,
            vetoed: HashSet::new(),
            last_reward_sample: None,
            reward_status: None,
            reward_cumulative_est: 0.0,
            signals: HashMap::new(),
            pulled_until: HashMap::new(),
            last_signal: 0.0,
            last_pulled: false,
            merge_live_warned: false,
            merger: None,
            cond_by_token: HashMap::new(),
            merge_tx,
            merge_rx,
            merge_inflight: HashSet::new(),
            last_merge_sweep: Instant::now(),
            venue_by_token: HashMap::new(),
            data_api: None,
            deposit_wallet: None,
            redeem_tx,
            redeem_rx,
            redeem_sweep_inflight: false,
            last_redeem_sweep: Instant::now(),
        };
        // We placed this order on YES (an Ask). The fill must follow THIS token.
        mm.placed.insert(
            oid,
            Placed {
                token: yes,
                side: Side::Ask,
                ts: TickSize::Cent,
            },
        );

        mm.consume_fills().await;

        assert_eq!(
            mm.inv.net(yes),
            -10_000_000,
            "the sell booked against YES (our placed order's token)"
        );
        assert_eq!(
            mm.inv.net(no),
            0,
            "nothing booked against NO (the WS trade's top-level asset_id)"
        );
    }

    /// REPRODUCTION (Fix #1): drive the WHOLE loop (`tick`) over the real
    /// [`PaperMakerVenue`] with taker flow ON, through many ticks on a book that
    /// moves enough to (a) fully fill resting orders and (b) shift inventory skew
    /// so quotes get replaced — the churn that happens live. As the asymmetric
    /// taker flow builds a long, the skew pushes a quote across the live book; the
    /// post-only place is rejected, so `reconcile` returns `Err` after the OTHER
    /// side was already (re)placed, and the QM's tracked set drifts from the
    /// venue's resting set. Pre-fix, `consume_fills` cannot resolve those fills'
    /// side from `placed` and DROPS them (warn-flood; inventory freezes).
    ///
    /// INVARIANT: every fill the venue produces is booked AND durably persisted —
    /// the store ends with exactly as many fill rows as the venue emitted, with
    /// ZERO write errors (no FK-orphaned fills), and the "unknown resting order"
    /// warning never fires.
    #[tokio::test]
    async fn mm_books_every_venue_fill_under_churn() {
        use pm_store::Store;
        use pm_store::writer::run_writer;

        let warns = WarnCapture::default();
        let logs = Arc::clone(&warns.0);
        let subscriber = tracing_subscriber::registry().with(warns);
        let _guard = tracing::subscriber::set_default(subscriber);

        let tokens = vec![TokenId(1)];
        let (fetcher, shared) = controllable_fetcher(
            &tokens,
            HashMap::from([(TokenId(1), (cent_book(&[(49, 100 * SH)], &[(51, 100 * SH)]), true))]),
        );
        // Tight book + strong skew + a small per-market cap, so the long the
        // (asymmetric) taker flow accumulates drives the skew hard enough to make
        // a quote cross the book — exactly the live churn that strands fills.
        let mut params = mk_params_skew(100, 5.0, 2000);
        params.paper_taker_fill_pct = 50;
        let mut inv_cfg = generous_inv();
        inv_cfg.max_inventory_usd = Usdc(30_000_000);
        inv_cfg.max_gross_inventory_usd = Usdc(60_000_000);
        let (mut mm, store_rx, produced) =
            build_loop_tally(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // Wire the loop's store channel to a REAL writer over an in-memory store,
        // so every booked fill (and its FK-parent order row) is actually persisted
        // — surfacing any orphaned-fill FK error as a store `write_error`.
        let store = Store::open_in_memory().unwrap();
        let writer = tokio::spawn(run_writer(store, store_rx));

        for i in 0..120u32 {
            // Oscillate the mid to force re-quotes (replaces) alongside the fills.
            let (b, a) = if i % 4 < 2 { (49u16, 51u16) } else { (48, 50) };
            shared
                .lock()
                .unwrap()
                .insert(TokenId(1), (cent_book(&[(b, 100 * SH)], &[(a, 100 * SH)]), true));
            mm.tick().await;
        }

        let produced_count = produced.lock().unwrap().len();
        let dropped = logs
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.contains("unknown resting order"))
            .count();

        // Drop the loop (and thus its store_tx) so the writer drains and returns.
        drop(mm);
        let store = writer.await.unwrap();

        assert!(
            produced_count > 0,
            "the scenario must actually produce fills (else it proves nothing)"
        );
        assert_eq!(
            dropped, 0,
            "the MM dropped {dropped} venue fills (\"unknown resting order\")"
        );
        assert_eq!(
            store.write_errors, 0,
            "every booked fill must persist (no FK-orphaned fill rows)"
        );
        assert_eq!(
            store.count_fills().unwrap(),
            produced_count as i64,
            "the store must hold exactly one row per venue-produced fill (none dropped)"
        );
    }

    /// REPRODUCTION (Fix #2): after the paper sim FULLY fills a resting order it
    /// removes it, but the [`QuoteManager`] is unaware of fills — so it keeps
    /// "tracking" the now-gone id and, on an identical next desired, NO-OPs
    /// (never re-places). Its tracked set drifts from the venue's empty resting
    /// set and the market goes dark: the MM stops quoting a filled market.
    ///
    /// With `consume_fills` calling `QuoteManager::note_fill`, a fully-filled
    /// (token, side) is dropped from tracking, so the next `reconcile` RE-PLACES
    /// it and the MM keeps quoting + filling. Asserts the venue keeps producing
    /// fills across many ticks (not frozen after the first).
    #[tokio::test]
    async fn mm_requotes_after_full_fill() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // Skew OFF (identical desired each tick) + full taker fills each poll, on
        // a STATIC book: pre-fix this is the canonical drift — fill once, then the
        // identical-desired no-op never re-places.
        let mut params = mk_params(200, 5.0);
        params.paper_taker_fill_pct = 100;
        let (mut mm, _store_rx, produced) =
            build_loop_tally(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        for _ in 0..10u32 {
            mm.tick().await;
        }

        let produced_count = produced.lock().unwrap().len();
        // Pre-fix: the venue fills the first quotes once, then the drifted QM
        // no-ops forever → ~2 fills total. Post-fix: it re-quotes every tick, so
        // many fills accrue. A threshold well above the frozen count proves the
        // MM keeps quoting a filled market.
        assert!(
            produced_count >= 10,
            "the MM stopped re-quoting after fills (drift): only {produced_count} fills in 10 ticks"
        );
    }

    #[tokio::test]
    async fn closed_control_channel_exits_cleanly() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let kill = Arc::new(AtomicBool::new(false));
        let (ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let (store_tx, _store_rx) = mpsc::channel(256);
        let ctx = StrategyCtx {
            registry: empty_registry(),
            fetcher: fetcher.clone(),
            store_tx,
            kill,
            ctl_rx,
            status_tx,
        };
        let venue = PaperMakerVenue::new(fetcher);
        let token_market = token_market_for(&tokens);
        let run = tokio::spawn(run_mm_loop(
            venue,
            QuoteManager::new(),
            InventoryRisk::new(generous_inv()),
            PositionBook::default(),
            ctx,
            mk_params(200, 5.0),
            tokens,
            token_market,
            HashMap::new(),
            None,            // merger (M6-7): no relayer in this control-channel test
            HashMap::new(),  // cond_by_token (M6-7)
            HashMap::new(),  // venue_by_token (R1)
            None,            // data_api (R1)
            None,            // deposit_wallet (R1)
            Usdc(1_000_000_000),
            false,
            false,
            None,
        ));
        // Dropping the control sender closes ctl_rx → the loop shuts down cleanly.
        drop(ctl_tx);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("mm did not exit after its control channel closed")
            .expect("mm run task panicked");
    }
}
