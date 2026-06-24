//! Display-ready dashboard state (published as watch<Arc<AppState>>) and the
//! commands the TUI emits. NO business logic lives in this crate.

/// One opportunity-feed row.
#[derive(Debug, Clone, PartialEq)]
pub struct OppLine {
    pub age_s: u64,
    pub class: String,
    /// Pre-resolved market name(s), e.g. "Will X win? (+1)" for multi-leg.
    pub market: String,
    pub edge_bps: i64,
    pub size_shares: f64,
    pub est_profit_usd: f64,
    pub dispatched: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PositionLine {
    /// Owning strategy (e.g. "arb" / "mm") so the operator can tell whose
    /// position this is — the same token can be held independently by each.
    pub strategy: String,
    pub market: String,
    /// SIGNED net shares: positive = long, negative = short (e.g. -5.0).
    pub qty_shares: f64,
    pub basis_usd: f64,
    pub mark_usd: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillLine {
    pub ago_s: u64,
    /// Strategy that traded this fill (e.g. "arb" / "mm").
    pub strategy: String,
    pub market: String,
    pub action: String,
    pub px: String, // pre-formatted, e.g. "0.44"
    pub qty_shares: f64,
    pub cash_usd: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderLine {
    pub ago_s: u64,
    pub order_id_short: String, // first 8 chars of the uuid
    pub state: String,
    pub detail: String,
}

/// One row of the OPEN-ORDERS panel: either a LIVE resting maker quote, or a
/// VETOED (manually cancelled + re-quote-suppressed) slot. Selectable; the
/// cancel/un-veto command carries `key` back to the app.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenOrderLine {
    /// Owning strategy (e.g. "mm").
    pub strategy: String,
    pub market: String,
    /// "Bid" (buy) or "Ask" (sell).
    pub side: String,
    /// Pre-formatted limit price (e.g. "0.44"); "—" for a vetoed slot.
    pub px: String,
    /// Remaining size in shares; `0.0` for a vetoed slot.
    pub qty_shares: f64,
    /// `true` ⇒ this slot is VETOED (no live order) — selecting it un-vetoes.
    pub vetoed: bool,
    /// Opaque "<token_u64>:<b|a>" handle; the app decodes it to (token, side)
    /// to target the cancel/un-veto. Display code never interprets it.
    pub key: String,
}

/// One strategy's display-only money + control flags for the per-strategy
/// dashboard breakdown (multi-strategy platform). Same "display only" rule as
/// the header: the publisher converts µUSDC→USD (`usd()`) and these `f64`
/// dollars are never fed back into accounting. `id` is the strategy's stable
/// label (e.g. "arb").
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyLine {
    pub id: String,
    pub equity_usd: f64,
    pub cash_usd: f64,
    pub realized_usd: f64,
    pub unrealized_usd: f64,
    /// Count of open positions (tokens with non-zero net) for this strategy,
    /// so the MM's live exposure is legible even before fills scroll.
    pub open_positions: usize,
    pub paused: bool,
    pub halted: Option<String>,
    /// RewardFarm liquidity-reward ESTIMATE summary (Task 11); `Some` only for
    /// the MM in reward-farm mode, `None` otherwise. Display-only — rendered as a
    /// compact "rew" segment on this strategy's Health line.
    pub reward: Option<RewardLine>,
}

/// RewardFarm liquidity-reward ESTIMATE summary for a [`StrategyLine`] (Task 11,
/// spec §9). Display-only estimates carried up from the strategy's status; the
/// `$/day` figure especially is a rough proxy (true payout needs epoch-wide
/// maker totals only Polymarket has).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RewardLine {
    /// Rough estimated reward, $/day.
    pub est_usd_day: f64,
    /// Two-sided minimum score `Q_min` (> 0 ⇒ a scoring position).
    pub q_min: f64,
    /// Whether the quotes are two-sided in-band this sample.
    pub in_band: bool,
    /// Score balance `min(Q1,Q2)/max(Q1,Q2)` (1.0 = balanced).
    pub balance_ratio: f64,
    /// Session-cumulative estimated reward, $ (a running proxy).
    pub cumulative_est: f64,
    /// Phase-A (spec §4) latest blended adverse-selection signal in [-1, 1]
    /// (+ endangers the ask, − the bid). Rendered as `sig {:+.2}`.
    pub signal: f64,
    /// Phase-A (spec §4): the latest quote cycle pulled a side. Rendered as a
    /// `PULL` flag on the "rew" line.
    pub pulled: bool,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Health {
    pub ws_connected: bool,
    /// Number of WS feeds currently connected (supervisor sessions live).
    pub feeds_up: u64,
    /// Total number of WS feeds configured (supervisor count).
    pub feeds_total: u64,
    /// Age in seconds of the oldest applied frame across all feeds.
    /// Feeds that have never received a frame (last_frame_ms == 0) contribute
    /// age 0 so a brand-new session doesn't inflate this gauge.
    pub oldest_frame_age_s: u64,
    pub books: u64,
    pub stale: u64,
    pub frames: u64,
    pub frames_per_s: f64,
    pub reconnects: u64,
    pub parse_errors: u64,
    pub detect_p50_us: u64,
    pub detect_p99_us: u64,
    pub dispatch_p50_us: u64,
    pub dispatch_p99_us: u64,
    pub opps_emitted: u64,
    pub admitted: u64,
    pub dispatched: u64,
    pub baskets_clean: u64,
    pub baskets_repaired: u64,
    pub baskets_unwound: u64,
    pub solver_queue: u64,
    pub lp_solved: u64,
    /// Baskets rejected by the live-mode gates (cap, min-leg, class filter).
    pub live_rej: u64,
    /// Baskets held because live_released is still false (pre-release shadow).
    pub live_held: u64,
}

/// The full dashboard snapshot. Publisher assembles ~10 Hz.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AppState {
    pub uptime_s: u64,
    pub mode_paper: bool,
    /// True once the operator has typed-confirmed the live toggle (TUI `l`).
    /// In live mode, controls whether the header badge shows LIVE·HELD or LIVE.
    pub live_released: bool,
    /// True in `--live --shadow`: the venue signs but never submits. Display
    /// only — drives the distinct SHADOW badge so a harmless shadow session is
    /// never mistaken for armed-and-released real money.
    pub shadow: bool,
    pub paused: bool,
    pub halted: Option<String>,
    pub killed: bool,
    /// Rendered by Task 5's busy indicator if needed; field published for completeness.
    pub busy: bool,
    pub cash_usd: f64,
    pub equity_usd: f64,     // bid-marked (conservative, durable)
    pub equity_mid_usd: f64, // mid-marked (risk/halt signal)
    pub realized_usd: f64,
    pub unrealized_usd: f64,
    pub opportunities: Vec<OppLine>,
    pub positions: Vec<PositionLine>,
    pub fills: Vec<FillLine>,
    pub orders: Vec<OrderLine>,
    /// LIVE resting maker quotes + VETOED slots — the open-orders panel. The
    /// operator selects a row and cancels/un-vetoes it (MM only today).
    pub open_orders: Vec<OpenOrderLine>,
    pub health: Health,
    /// (level, formatted line) — level: 1=ERROR 2=WARN 3=INFO 4=DEBUG 5=TRACE.
    pub log: Vec<(u8, String)>,
    /// Per-strategy money/control breakdown (multi-strategy platform). Empty
    /// when the publisher is driven by the single `CoordStatus` (today's
    /// wiring); populated when fed the `StrategyHost`'s aggregated view (Task
    /// 1.8 switches the source). Display only — never fed back into accounting.
    pub per_strategy: Vec<StrategyLine>,
}

/// Commands the TUI emits toward the app (translated in main.rs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiCommand {
    SetPaused(bool),
    /// Confirmed via modal; sets the app kill flag.
    Kill,
    /// Typed-confirmed live toggle — M4 stub: app logs "unavailable until M5".
    GoLive,
    /// Per-order cancel/un-veto from the open-orders panel. `key` is the opaque
    /// "<token>:<b|a>" handle from the selected [`OpenOrderLine`]; `veto = true`
    /// cancels + suppresses the order, `veto = false` lifts the suppression.
    SetVeto { key: String, veto: bool },
    Quit,
}
