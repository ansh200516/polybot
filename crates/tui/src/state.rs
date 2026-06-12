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
    pub market: String,
    pub qty_shares: f64,
    pub basis_usd: f64,
    pub mark_usd: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillLine {
    pub ago_s: u64,
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
    pub health: Health,
    /// (level, formatted line) — level: 1=ERROR 2=WARN 3=INFO 4=DEBUG 5=TRACE.
    pub log: Vec<(u8, String)>,
}

/// Commands the TUI emits toward the app (translated in main.rs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiCommand {
    SetPaused(bool),
    /// Confirmed via modal; sets the app kill flag.
    Kill,
    /// Typed-confirmed live toggle — M4 stub: app logs "unavailable until M5".
    GoLive,
    Quit,
}
