//! Typed configuration skeleton (spec §18). Defaults are the spec §2 locked
//! values. Secrets never live here — env vars only (M3+).

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub capital: Capital,
    pub edges: Edges,
    pub gas: Gas,
    pub lp: Lp,
    pub dedup: Dedup,
    pub mode: Mode,
    pub endpoints: Endpoints,
    pub universe: Universe,
    pub ingestion: Ingestion,
    pub execution: Execution,
    pub risk: Risk,
    pub store: Store,
    pub tui: Tui,
    pub live: Live,
    pub inventory: Inventory,
    pub strategies: Strategies,
    pub segments: Segments,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Capital {
    pub bankroll_usd: f64,
    pub per_market_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Edges {
    pub min_edge_class12_bps: i32,
    pub min_edge_class3_bps: i32,
    pub min_profit_usd: f64,
    /// Plausibility ceiling, bps. A genuine risk-free arb is bounded (tens to
    /// low-hundreds of bps); an edge above this is a structural artifact —
    /// stale/dead books with dust asks, or a NegRisk set assumed exhaustive that
    /// isn't — and must NEVER be dispatched. The coordinator suppresses any opp
    /// over this ceiling (counted as `implausible`, logged). Set very high to
    /// effectively disable; must be ≥ `min_edge_class3_bps`.
    pub max_edge_bps: i32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Gas {
    pub split_microusdc: u64,
    pub merge_microusdc: u64,
    pub redeem_microusdc: u64,
    pub negrisk_convert_microusdc: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Lp {
    pub max_worlds: usize,
    pub min_resolve_interval_ms: u64,
    pub solver_concurrency: usize,
    /// Opt-in: let the LP analyse NegRisk events that are mutually exclusive but
    /// NOT verified-exhaustive, modelling them as at-most-one-winner (k+1 worlds)
    /// instead of dropping them (→ 2^k free vars → `TooManyWorlds` skip). OFF by
    /// default: it enables new live trade surface on big multi-outcome events, so
    /// prove it in paper and review before enabling for a live run.
    pub nonexhaustive_negrisk_worlds: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Dedup {
    pub cooldown_ms: u64,
    pub reemit_improvement_pct: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Mode {
    pub paper: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Endpoints {
    pub gamma_base: String,
    pub clob_base: String,
    pub ws_market_url: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Universe {
    pub max_markets: usize,
    pub require_active: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Ingestion {
    pub staleness_ms: u64,
    pub feed_silence_ms: u64,
    pub ws_chunk_size: usize,
    pub resync_interval_s: u64,
    pub sweep_interval_ms: u64,
    pub rest_rate_capacity: u32,
    pub rest_rate_per_sec: f64,
    pub backoff_base_ms: u64,
    pub backoff_cap_ms: u64,
    pub relationships_path: String,
}

#[derive(Debug, PartialEq)]
pub enum ConfigError {
    Parse(String),
    BadMoney(&'static str),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Parse(s) => write!(f, "config parse error: {s}"),
            ConfigError::BadMoney(s) => write!(f, "bad money value: {s}"),
        }
    }
}
impl std::error::Error for ConfigError {}

impl Default for Capital {
    fn default() -> Self {
        Capital {
            bankroll_usd: 10_000.0,
            per_market_usd: 1_000.0,
        }
    }
}
impl Default for Edges {
    fn default() -> Self {
        Edges {
            min_edge_class12_bps: 30,
            min_edge_class3_bps: 100,
            min_profit_usd: 1.0,
            // 5000 bps = 50% risk-free return: never real on a live book, but
            // far above any genuine arb, so it only ever catches false positives.
            max_edge_bps: 5000,
        }
    }
}
impl Default for Gas {
    fn default() -> Self {
        Gas {
            split_microusdc: 10_000,
            merge_microusdc: 10_000,
            redeem_microusdc: 15_000,
            negrisk_convert_microusdc: 20_000,
        }
    }
}
impl Default for Lp {
    fn default() -> Self {
        Lp {
            max_worlds: 4096,
            min_resolve_interval_ms: 500,
            solver_concurrency: 2,
            nonexhaustive_negrisk_worlds: false,
        }
    }
}
impl Default for Dedup {
    fn default() -> Self {
        Dedup {
            cooldown_ms: 2000,
            reemit_improvement_pct: 20,
        }
    }
}
impl Default for Mode {
    fn default() -> Self {
        Mode { paper: true }
    }
}
impl Default for Endpoints {
    fn default() -> Self {
        Endpoints {
            gamma_base: "https://gamma-api.polymarket.com".to_string(),
            clob_base: "https://clob.polymarket.com".to_string(),
            ws_market_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
        }
    }
}
impl Default for Universe {
    fn default() -> Self {
        Universe {
            max_markets: 200,
            require_active: true,
        }
    }
}
impl Default for Ingestion {
    fn default() -> Self {
        Ingestion {
            staleness_ms: 1_500,
            feed_silence_ms: 15_000,
            ws_chunk_size: 50,
            resync_interval_s: 300,
            sweep_interval_ms: 1000,
            rest_rate_capacity: 10,
            rest_rate_per_sec: 5.0,
            backoff_base_ms: 250,
            backoff_cap_ms: 30_000,
            relationships_path: "relationships.toml".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Execution {
    /// Simulated venue latency before a paper fill re-reads the book (spec §14).
    pub paper_latency_ms: u64,
    /// Basket fill window: legs not resolved within this are treated as expired.
    pub fill_window_ms: u64,
    /// Class-1 long redemption: "merge" (default) or "hold" (spec §6).
    pub redeem_strategy: String,
}

impl Default for Execution {
    fn default() -> Self {
        Execution {
            paper_latency_ms: 200,
            fill_window_ms: 500,
            redeem_strategy: "merge".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Risk {
    pub max_unhedged_usd: f64,
    pub max_open_orders: usize,
    pub max_basket_legs: usize,
    pub daily_drawdown_pct: f64,
    pub error_halt_count: u32,
    pub error_halt_window_s: u64,
    pub restart_storm_count: u32,
    pub restart_storm_window_s: u64,
    pub kill_file: String,
    /// Opportunities older than this at the coordinator are discarded (staleness gate proxy, §15).
    pub max_opportunity_age_ms: u64,
    /// Drawdown-feed mid-mark clamp: mid ≤ bid + this many ticks (all modes).
    pub mid_spread_cap_ticks: u16,
}

impl Default for Risk {
    fn default() -> Self {
        Risk {
            max_unhedged_usd: 200.0,
            max_open_orders: 32,
            max_basket_legs: 16,
            daily_drawdown_pct: 2.0,
            error_halt_count: 5,
            error_halt_window_s: 60,
            restart_storm_count: 5,
            restart_storm_window_s: 300,
            kill_file: "kill.switch".into(),
            max_opportunity_age_ms: 1000,
            mid_spread_cap_ticks: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Store {
    pub path: String,
}

impl Default for Store {
    fn default() -> Self {
        Store {
            path: "pm.sqlite".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Tui {
    /// AppState publish + redraw cadence (spec §17 "~10 Hz").
    pub refresh_ms: u64,
    /// Rows kept in the opportunity feed panel.
    pub feed_rows: usize,
    /// Rows in the fills/orders panel.
    pub fills_rows: usize,
    /// Ring-buffer capacity for the scrolling log panel.
    pub log_lines: usize,
}

impl Default for Tui {
    fn default() -> Self {
        Tui {
            refresh_ms: 100,
            feed_rows: 50,
            fills_rows: 20,
            log_lines: 200,
        }
    }
}

/// M5 live-trading canary parameters (spec 2026-06-13 §Config & env).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Live {
    /// Live per-basket basis cap, USD.
    pub basket_cap_usd: f64,
    /// Latched session-loss dispatch halt (bid-marked), USD.
    pub session_loss_usd: f64,
    /// Venue minimum order size per leg, SHARES (RECON-pinned: 5; a basket
    /// with any leg below this is rejected whole — never resized upward).
    pub min_leg_shares: f64,
    /// Venue minimum order VALUE per leg, USD (Polymarket V2 rejects marketable
    /// orders under $1: `"invalid amount for a marketable BUY order, min size: 1"`).
    /// A basket with any buy leg whose makerAmount is below this is rejected
    /// whole — never resized upward. The 5-share `min_leg_shares` floor is too
    /// weak on cheap tokens (5 × $0.10 = $0.50 < $1); this is the real gate.
    pub min_leg_value_usd: f64,
    /// Phrase typed at startup (headless --live) to release dispatch.
    pub confirm_phrase: String,
}

impl Default for Live {
    fn default() -> Self {
        Live {
            basket_cap_usd: 10.0,
            session_loss_usd: 25.0,
            min_leg_shares: 5.0,
            min_leg_value_usd: 1.0,
            confirm_phrase: "I understand this trades real money".into(),
        }
    }
}

/// Per-strategy inventory-risk caps for inventory-bearing strategies (spec §5,
/// Phase 2). Maps to `pm_risk::inventory::InventoryConfig` via
/// `pm_app::wiring::inventory_config`. INERT until a strategy opts in (Phase 4);
/// defaults are deliberately conservative (small caps, tight stop).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Inventory {
    /// Per-market net exposure cap, USD.
    pub max_inventory_usd: f64,
    /// Gross inventory cap summed across markets, USD.
    pub max_gross_inventory_usd: f64,
    /// Open-inventory UNREALIZED mark-to-market loss that latches the inventory
    /// stop-loss halt + flatten, USD. Keys off unrealized P&L only.
    pub inventory_stop_loss_usd: f64,
    /// Per-strategy TOTAL (realized + unrealized) P&L floor that latches the
    /// daily-loss halt, USD. A SESSION floor — there is no calendar-day reset yet;
    /// it accumulates over the whole run. Independent of `inventory_stop_loss_usd`
    /// (no ordering constraint): each floor can fire without the other.
    pub daily_loss_usd: f64,
    /// Volatility "pull-quotes" hint threshold, in ticks (1 tick = 1 cent): a mid
    /// move beyond this many ticks within `vol_window_ms` advises pulling quotes.
    pub vol_pull_ticks: u32,
    /// Look-back window for the volatility hint, milliseconds.
    pub vol_window_ms: u64,
}

impl Default for Inventory {
    fn default() -> Self {
        Inventory {
            max_inventory_usd: 50.0,
            max_gross_inventory_usd: 100.0,
            inventory_stop_loss_usd: 25.0,
            daily_loss_usd: 50.0,
            vol_pull_ticks: 5,
            vol_window_ms: 2000,
        }
    }
}

/// Per-strategy configuration for the risk-taking strategies the platform runs
/// alongside arb (Phase 4+). Each strategy is its own sub-table so they stay
/// independent; today only the market maker (`[strategies.mm]`) exists. Every
/// risk-taking strategy ships DEFAULT-OFF (`enabled = false`), matching the
/// cross-cutting safety model.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Strategies {
    pub mm: Mm,
}

/// Market-making strategy config (`[strategies.mm]`, spec §7). Maps into the
/// Phase-4 `MmStrategy` via `pm_app`'s wiring. DEFAULT-OFF and paper-only until
/// an operator flips both `enabled` and (later, Task 4.5) `live`; the inventory
/// caps come from the shared `[inventory]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Mm {
    /// Master switch. `false` (default) → the MM strategy never quotes; the
    /// platform behaves exactly as before. Mirrors the established default-off
    /// flag pattern (`lp.nonexhaustive_negrisk_worlds`).
    pub enabled: bool,
    /// Live arm. `false` (default) → quotes only on the PAPER maker venue
    /// (`PaperMakerVenue`); the live CLOB arm is Task 4.5. Inert until then.
    pub live: bool,
    /// Total quoted spread around fair (= mid), in bps of $1 (100 bps = $0.01 =
    /// one Cent tick). Split half each side; the loop rounds the bid DOWN and the
    /// ask UP to ticks (maker-favorable / never narrower) and bumps them apart if
    /// they collapse to one tick.
    pub spread_bps: u32,
    /// Quote-loop cadence: how often the strategy re-evaluates books, reconciles
    /// resting quotes, polls fills, and marks/halts.
    pub quote_refresh_ms: u64,
    /// Max notional per single quote (one side), USD. Per-side size =
    /// notional / price; the inventory caps then clamp it further.
    pub max_quote_usd: f64,
    /// Inventory SKEW (Task 4.3, spec §7): the MAXIMUM fair-value shift applied
    /// at FULL per-market inventory, in bps of $1 (100 bps = $0.01 = one Cent
    /// tick). The MM shifts BOTH quotes against inventory — a long lowers them
    /// (less eager to buy more, keener to sell down), a short raises them —
    /// scaled linearly by `clamp(net / inventory_cap_shares, −1, +1)`. `0`
    /// disables skew (the Task-4.2 symmetric quoting); any `u32` is valid (a
    /// value large enough to push a side out of range just skips that quote).
    pub inventory_skew_bps: u32,
    /// Maker-rebate ESTIMATE (Task 4.4, spec §7): bps of each maker fill's
    /// NOTIONAL credited as an estimated rebate. Makers pay 0 fees on CLOB V2
    /// and may EARN a rebate from the maker-rewards program, but the live rate
    /// varies by category (Phase 5 can refine per-segment), so this is a flat,
    /// operator-set ESTIMATE. `0` (default) assumes NO rebate — conservative,
    /// the operator opts in by setting the real program rate. The accrued
    /// estimate is displayed SEPARATELY and is never folded into cash/equity/
    /// realized P&L (it is paid out-of-band and unverified — folding it would
    /// inflate position P&L). Any `u32` is a valid estimate rate.
    pub rebate_bps: u32,
    /// Bankroll slice allocated to the MM, USD. When `enabled`, this is carved
    /// OUT of the platform bankroll and arb's risk cap is reduced by the same
    /// amount, so the two strategies SHARE the bankroll without overlapping real
    /// funds (Σ capital == bankroll). Must be ≥ 0 and, when `enabled`, ≤ the
    /// platform bankroll (`capital.bankroll_usd`). Inert while disabled.
    pub capital_usd: f64,
    /// Cap on how many markets the MM quotes on first enablement: the MM takes
    /// the first `max_markets` markets of the registry universe (both sides).
    /// Phase 5 replaces this "first N" cap with liquid-segment selection. `0`
    /// quotes nothing (inert); any `usize` is valid — documented, not bounded.
    pub max_markets: usize,
    /// LIVE maker-fill source (Task 4.6): `"ws"` (default) | `"rest"`. INERT
    /// unless the MM is cleared for live (process `--live` AND
    /// `[strategies.mm].live`); paper always uses the `PaperMakerVenue` sim.
    ///
    /// * `"ws"` — the LOW-LATENCY user-WS feed (`pm_execution`'s
    ///   `LiveUserWsFills`, paired with the live maker venue via a `SplitVenue`).
    ///   The scalping upgrade: fills arrive with WS latency instead of a REST
    ///   poll's.
    /// * `"rest"` — the Task-4.5 `LiveVenue` REST `/data/trades` poll. The
    ///   offline-verified FALLBACK if the WS misbehaves in canary (the REST path
    ///   is fully testable against the in-crate HTTP mock).
    ///
    /// Validated to exactly `"ws"` or `"rest"` (mirrors `execution.redeem_strategy`).
    pub live_fills_source: String,
}

impl Default for Mm {
    fn default() -> Self {
        Mm {
            enabled: false,
            live: false,
            spread_bps: 200,
            quote_refresh_ms: 1500,
            max_quote_usd: 5.0,
            inventory_skew_bps: 150,
            // Conservative: assume NO maker rebate unless the operator sets the
            // real (category-dependent) program rate.
            rebate_bps: 0,
            // A small slice of the bankroll (the rest stays with arb), and a
            // conservative market cap for the first enablement (Phase 5 refines).
            capital_usd: 25.0,
            max_markets: 20,
            // Default to the low-latency user-WS fills feed (the scalping
            // upgrade); operators can pin "rest" to fall back to the
            // offline-verified Task-4.5 REST poll.
            live_fills_source: "ws".into(),
        }
    }
}

/// Market-segmentation thresholds (`[segments]`, spec Phase 5 / Task 5.1). Each
/// market is classified `LiquidStable` / `Liquid` / `Illiquid` from its static
/// Gamma volume + liquidity; these are the USD cutoffs (mirrors
/// `pm_registry::segment::SegmentThresholds`, wired up in Task 5.2). Bounds are
/// INCLUSIVE (`metric >= threshold` clears the bar).
///
/// Opt-in / forward-only: producing a classification changes NO existing
/// behaviour. The defaults match the registry classifier's defaults, and this
/// section is the seat that Task 5.2's per-segment routing rules will grow into.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Segments {
    /// Minimum lifetime volume (USD) for the `LiquidStable` tier.
    pub liquid_stable_min_volume: f64,
    /// Minimum resting liquidity (USD) for the `LiquidStable` tier.
    pub liquid_stable_min_liquidity: f64,
    /// Minimum lifetime volume (USD) for the `Liquid` tier.
    pub liquid_min_volume: f64,
    /// Minimum resting liquidity (USD) for the `Liquid` tier.
    pub liquid_min_liquidity: f64,
}

impl Default for Segments {
    fn default() -> Self {
        Segments {
            liquid_stable_min_volume: 100_000.0,
            liquid_stable_min_liquidity: 50_000.0,
            liquid_min_volume: 10_000.0,
            liquid_min_liquidity: 5_000.0,
        }
    }
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Sanity checks beyond shape: positive capital, per-market ≤ bankroll,
    /// percentage domains. Includes M5 live-section canary checks.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.capital.bankroll_usd <= 0.0 || !self.capital.bankroll_usd.is_finite() {
            return Err(ConfigError::BadMoney("bankroll_usd must be > 0"));
        }
        if self.capital.per_market_usd <= 0.0
            || !self.capital.per_market_usd.is_finite()
            || self.capital.per_market_usd > self.capital.bankroll_usd
        {
            return Err(ConfigError::BadMoney(
                "per_market_usd must be in (0, bankroll]",
            ));
        }
        if self.dedup.reemit_improvement_pct > 100 {
            return Err(ConfigError::BadMoney(
                "reemit_improvement_pct must be ≤ 100",
            ));
        }
        // The plausibility ceiling must sit above the trade floors, else nothing
        // could ever pass (floor ≤ edge ≤ ceiling). This also keeps it positive.
        if self.edges.max_edge_bps < self.edges.min_edge_class3_bps
            || self.edges.max_edge_bps < self.edges.min_edge_class12_bps
        {
            return Err(ConfigError::BadMoney(
                "edges.max_edge_bps must be ≥ the min-edge floors",
            ));
        }
        // Ingestion validation
        if self.ingestion.staleness_ms < 100 {
            return Err(ConfigError::BadMoney("staleness_ms must be ≥ 100"));
        }
        if self.ingestion.feed_silence_ms < 1000 {
            return Err(ConfigError::BadMoney("feed_silence_ms must be ≥ 1000"));
        }
        if self.ingestion.ws_chunk_size < 1 {
            return Err(ConfigError::BadMoney("ws_chunk_size must be ≥ 1"));
        }
        if self.ingestion.rest_rate_per_sec <= 0.0 {
            return Err(ConfigError::BadMoney("rest_rate_per_sec must be > 0.0"));
        }
        if self.ingestion.rest_rate_capacity < 1 {
            return Err(ConfigError::BadMoney("rest_rate_capacity must be ≥ 1"));
        }
        if self.ingestion.backoff_base_ms > self.ingestion.backoff_cap_ms {
            return Err(ConfigError::BadMoney(
                "backoff_base_ms must be ≤ backoff_cap_ms",
            ));
        }
        // Endpoints validation
        if self.endpoints.gamma_base.is_empty() {
            return Err(ConfigError::BadMoney("gamma_base must be non-empty"));
        }
        if !self.endpoints.gamma_base.starts_with("https://") {
            return Err(ConfigError::BadMoney("gamma_base must start with https://"));
        }
        if self.endpoints.clob_base.is_empty() {
            return Err(ConfigError::BadMoney("clob_base must be non-empty"));
        }
        if !self.endpoints.clob_base.starts_with("https://") {
            return Err(ConfigError::BadMoney("clob_base must start with https://"));
        }
        if self.endpoints.ws_market_url.is_empty() {
            return Err(ConfigError::BadMoney("ws_market_url must be non-empty"));
        }
        if !self.endpoints.ws_market_url.starts_with("wss://") {
            return Err(ConfigError::BadMoney(
                "ws_market_url must start with wss://",
            ));
        }
        if self.ingestion.relationships_path.is_empty() {
            return Err(ConfigError::BadMoney(
                "relationships_path must be non-empty",
            ));
        }
        if !matches!(self.execution.redeem_strategy.as_str(), "merge" | "hold") {
            return Err(ConfigError::BadMoney(
                "execution.redeem_strategy must be \"merge\" or \"hold\"",
            ));
        }
        if self.execution.fill_window_ms < self.execution.paper_latency_ms {
            return Err(ConfigError::BadMoney(
                "execution.fill_window_ms must be >= paper_latency_ms",
            ));
        }
        if !(self.risk.daily_drawdown_pct > 0.0 && self.risk.daily_drawdown_pct <= 100.0) {
            return Err(ConfigError::BadMoney(
                "risk.daily_drawdown_pct must be in (0, 100]",
            ));
        }
        if self.risk.max_basket_legs < 2 {
            return Err(ConfigError::BadMoney("risk.max_basket_legs must be >= 2"));
        }
        if self.risk.max_unhedged_usd < 0.0 || !self.risk.max_unhedged_usd.is_finite() {
            return Err(ConfigError::BadMoney(
                "risk.max_unhedged_usd must be finite and >= 0",
            ));
        }
        if self.lp.solver_concurrency == 0 {
            return Err(ConfigError::BadMoney("lp.solver_concurrency must be >= 1"));
        }
        if self.store.path.is_empty() {
            return Err(ConfigError::BadMoney("store.path must not be empty"));
        }
        if self.tui.refresh_ms < 50 {
            return Err(ConfigError::BadMoney("tui.refresh_ms must be >= 50"));
        }
        if self.tui.feed_rows == 0 || self.tui.fills_rows == 0 || self.tui.log_lines == 0 {
            return Err(ConfigError::BadMoney("tui row/line counts must be >= 1"));
        }
        // Live / risk canary checks (M5)
        if self.live.basket_cap_usd <= 0.0 || !self.live.basket_cap_usd.is_finite() {
            return Err(ConfigError::BadMoney("live.basket_cap_usd must be > 0"));
        }
        if self.live.session_loss_usd <= 0.0 || !self.live.session_loss_usd.is_finite() {
            return Err(ConfigError::BadMoney("live.session_loss_usd must be > 0"));
        }
        if self.live.min_leg_shares < 0.0 || !self.live.min_leg_shares.is_finite() {
            return Err(ConfigError::BadMoney("live.min_leg_shares must be ≥ 0"));
        }
        if self.live.min_leg_value_usd < 0.0 || !self.live.min_leg_value_usd.is_finite() {
            return Err(ConfigError::BadMoney("live.min_leg_value_usd must be ≥ 0"));
        }
        if self.live.confirm_phrase.is_empty() {
            return Err(ConfigError::BadMoney("live.confirm_phrase must not be empty"));
        }
        if self.risk.mid_spread_cap_ticks == 0 {
            return Err(ConfigError::BadMoney("risk.mid_spread_cap_ticks must be ≥ 1"));
        }
        // Inventory-risk caps (Phase 2; inert until a strategy opts in). All money
        // must be positive + finite. The stop-loss (open-inventory unrealized) and
        // the daily floor (total session P&L) are INDEPENDENT measures, so there
        // is no ordering constraint between them.
        if self.inventory.max_inventory_usd <= 0.0 || !self.inventory.max_inventory_usd.is_finite() {
            return Err(ConfigError::BadMoney(
                "inventory.max_inventory_usd must be > 0",
            ));
        }
        if self.inventory.max_gross_inventory_usd <= 0.0
            || !self.inventory.max_gross_inventory_usd.is_finite()
        {
            return Err(ConfigError::BadMoney(
                "inventory.max_gross_inventory_usd must be > 0",
            ));
        }
        if self.inventory.inventory_stop_loss_usd <= 0.0
            || !self.inventory.inventory_stop_loss_usd.is_finite()
        {
            return Err(ConfigError::BadMoney(
                "inventory.inventory_stop_loss_usd must be > 0",
            ));
        }
        if self.inventory.daily_loss_usd <= 0.0 || !self.inventory.daily_loss_usd.is_finite() {
            return Err(ConfigError::BadMoney("inventory.daily_loss_usd must be > 0"));
        }
        if self.inventory.vol_pull_ticks == 0 {
            return Err(ConfigError::BadMoney("inventory.vol_pull_ticks must be ≥ 1"));
        }
        if self.inventory.vol_window_ms < 1 {
            return Err(ConfigError::BadMoney("inventory.vol_window_ms must be ≥ 1"));
        }
        // Market-making strategy (`[strategies.mm]`; inert until `enabled`). The
        // spread must be at least 1 bp (a sub-bp spread is meaningless; the loop
        // additionally enforces a ≥1-tick non-crossing quote at runtime), the
        // refresh cadence must be ≥ 100 ms (a tighter loop would churn the book /
        // burn rate-limit budget for no benefit), and the per-quote notional must
        // be positive + finite.
        if self.strategies.mm.spread_bps < 1 {
            return Err(ConfigError::BadMoney(
                "strategies.mm.spread_bps must be ≥ 1",
            ));
        }
        if self.strategies.mm.quote_refresh_ms < 100 {
            return Err(ConfigError::BadMoney(
                "strategies.mm.quote_refresh_ms must be ≥ 100",
            ));
        }
        if self.strategies.mm.max_quote_usd <= 0.0 || !self.strategies.mm.max_quote_usd.is_finite() {
            return Err(ConfigError::BadMoney(
                "strategies.mm.max_quote_usd must be > 0",
            ));
        }
        // Live maker-fill source (Task 4.6): exactly "ws" or "rest" (mirrors
        // execution.redeem_strategy). Inert unless the MM is cleared for live.
        if !matches!(self.strategies.mm.live_fills_source.as_str(), "ws" | "rest") {
            return Err(ConfigError::BadMoney(
                "strategies.mm.live_fills_source must be \"ws\" or \"rest\"",
            ));
        }
        // MM capital must be finite + non-negative always; when ENABLED it is
        // carved out of the bankroll (arb's cap shrinks by it), so it can never
        // exceed the bankroll. Disabled → inert, so the bankroll bound is
        // skipped (mirrors the per_market ≤ bankroll style above).
        if self.strategies.mm.capital_usd < 0.0 || !self.strategies.mm.capital_usd.is_finite() {
            return Err(ConfigError::BadMoney(
                "strategies.mm.capital_usd must be finite and ≥ 0",
            ));
        }
        if self.strategies.mm.enabled
            && self.strategies.mm.capital_usd > self.capital.bankroll_usd
        {
            return Err(ConfigError::BadMoney(
                "strategies.mm.capital_usd must be ≤ capital.bankroll_usd when enabled",
            ));
        }
        // Task 4.5 — LIVE market-maker gating (config half). The live arm trades
        // REAL maker orders, so it is the tiniest, most-gated canary: arming it
        // requires the strategy to be ENABLED (a live flag on a disabled strategy
        // is a silent no-op footgun) and a strictly POSITIVE capital slice to fund
        // quotes (a $0 slice would carve nothing and quote nothing). The slice is
        // already bounded ABOVE by the `enabled` over-bankroll check above; live
        // only tightens the lower bound. Process-level `--live` PLUS the typed
        // confirmation are ALSO required at runtime (enforced in main) — this is
        // the config half of the gate; live MM's safety is the capital carve +
        // the inventory caps + postOnly + the confirmation (no new mechanism).
        if self.strategies.mm.live {
            if !self.strategies.mm.enabled {
                return Err(ConfigError::BadMoney(
                    "strategies.mm.live requires strategies.mm.enabled",
                ));
            }
            if self.strategies.mm.capital_usd <= 0.0 {
                return Err(ConfigError::BadMoney(
                    "strategies.mm.capital_usd must be > 0 when strategies.mm.live",
                ));
            }
        }
        // `max_markets` (usize) needs no bound: it is always ≥ 0; `0` simply
        // quotes nothing (inert). Documented, not checked.
        // `inventory_skew_bps` needs no bound: as a `u32` it is always ≥ 0 (0
        // disables skew), and the quote loop clamps the skewed fair to the
        // interior tick range — an over-large skew just skips that side rather
        // than producing an invalid or crossed quote. Documented, not checked.
        //
        // `rebate_bps` likewise needs no bound: as a `u32` it is always ≥ 0 (0
        // assumes no rebate), and it only scales a SEPARATE, display-only rebate
        // estimate that is never folded into cash/equity/realized — an unrealistic
        // value inflates only that out-of-band estimate, never real accounting.
        // Documented, not checked.
        //
        // Market segmentation (`[segments]`, Task 5.1; inert until 5.2 wires
        // routing). All thresholds must be finite and ≥ 0, and the high
        // (`LiquidStable`) bar must sit at or above the low (`Liquid`) bar on
        // each axis — otherwise the tiers would be incoherent (a market could
        // clear the high bar but not the low one).
        let seg = &self.segments;
        for v in [
            seg.liquid_stable_min_volume,
            seg.liquid_stable_min_liquidity,
            seg.liquid_min_volume,
            seg.liquid_min_liquidity,
        ] {
            if v < 0.0 || !v.is_finite() {
                return Err(ConfigError::BadMoney(
                    "segments thresholds must be finite and ≥ 0",
                ));
            }
        }
        if seg.liquid_stable_min_volume < seg.liquid_min_volume {
            return Err(ConfigError::BadMoney(
                "segments.liquid_stable_min_volume must be ≥ liquid_min_volume",
            ));
        }
        if seg.liquid_stable_min_liquidity < seg.liquid_min_liquidity {
            return Err(ConfigError::BadMoney(
                "segments.liquid_stable_min_liquidity must be ≥ liquid_min_liquidity",
            ));
        }
        Ok(())
    }
}

/// Checked dollars → µUSDC (round-to-nearest). Rejects NaN/∞/negative/overflow.
pub fn usd_to_microusdc(usd: f64) -> Result<i128, ConfigError> {
    if !usd.is_finite() || !(0.0..=1e18).contains(&usd) {
        return Err(ConfigError::BadMoney("must be finite, non-negative, sane"));
    }
    Ok((usd * 1e6).round() as i128)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::float_cmp)]
    use super::*;

    #[test]
    fn defaults_are_the_locked_values() {
        let c = Config::default();
        assert_eq!(c.capital.bankroll_usd, 10_000.0);
        assert_eq!(c.capital.per_market_usd, 1_000.0);
        assert_eq!(c.edges.min_edge_class12_bps, 30);
        assert_eq!(c.edges.min_edge_class3_bps, 100);
        assert_eq!(c.edges.min_profit_usd, 1.0);
        assert_eq!(c.edges.max_edge_bps, 5000);
        assert_eq!(c.gas.split_microusdc, 10_000);
        assert_eq!(c.gas.merge_microusdc, 10_000);
        assert_eq!(c.gas.redeem_microusdc, 15_000);
        assert_eq!(c.gas.negrisk_convert_microusdc, 20_000);
        assert_eq!(c.lp.max_worlds, 4096);
        assert!(!c.lp.nonexhaustive_negrisk_worlds);
        assert_eq!(c.dedup.cooldown_ms, 2000);
        assert_eq!(c.dedup.reemit_improvement_pct, 20);
        assert!(c.mode.paper);
        assert_eq!(c.endpoints.gamma_base, "https://gamma-api.polymarket.com");
        assert_eq!(c.endpoints.clob_base, "https://clob.polymarket.com");
        assert_eq!(
            c.endpoints.ws_market_url,
            "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        );
        assert_eq!(c.universe.max_markets, 200);
        assert!(c.universe.require_active);
        assert_eq!(c.ingestion.staleness_ms, 1_500);
        assert_eq!(c.ingestion.feed_silence_ms, 15_000);
        assert_eq!(c.ingestion.ws_chunk_size, 50);
        assert_eq!(c.ingestion.resync_interval_s, 300);
        assert_eq!(c.ingestion.sweep_interval_ms, 1000);
        assert_eq!(c.ingestion.rest_rate_capacity, 10);
        assert_eq!(c.ingestion.rest_rate_per_sec, 5.0);
        assert_eq!(c.ingestion.backoff_base_ms, 250);
        assert_eq!(c.ingestion.backoff_cap_ms, 30_000);
        assert_eq!(c.ingestion.relationships_path, "relationships.toml");
    }

    #[test]
    fn empty_toml_is_all_defaults() {
        assert_eq!(Config::from_toml_str("").unwrap(), Config::default());
    }

    #[test]
    fn partial_override_parses() {
        // bankroll override must be ≥ per_market default (1_000); use a value that passes.
        let c = Config::from_toml_str("[capital]\nbankroll_usd = 5000.0\n").unwrap();
        assert_eq!(c.capital.bankroll_usd, 5000.0);
        assert_eq!(c.capital.per_market_usd, 1_000.0);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(Config::from_toml_str("[capital]\nbankrol = 1.0\n").is_err());
        assert!(Config::from_toml_str("[typo_section]\nx = 1\n").is_err());
    }

    #[test]
    fn validate_rejects_insane_values() {
        assert!(Config::from_toml_str("[capital]\nbankroll_usd = -5.0\n").is_err());
        assert!(Config::from_toml_str("[capital]\nper_market_usd = 99999.0\n").is_err());
        assert!(Config::from_toml_str("[dedup]\nreemit_improvement_pct = 150\n").is_err());
    }

    #[test]
    fn validate_rejects_implausible_ceiling_below_floor() {
        // Ceiling below the class-3 floor would make every opp unrepresentable
        // (floor ≤ edge ≤ ceiling is empty) — must be rejected.
        assert!(Config::from_toml_str("[edges]\nmax_edge_bps = 50\n").is_err());
        // A ceiling at/above the floors parses fine.
        let c = Config::from_toml_str("[edges]\nmax_edge_bps = 100\n").unwrap();
        assert_eq!(c.edges.max_edge_bps, 100);
        // A custom higher ceiling round-trips.
        let c = Config::from_toml_str("[edges]\nmax_edge_bps = 8000\n").unwrap();
        assert_eq!(c.edges.max_edge_bps, 8000);
    }

    #[test]
    fn money_conversion_is_checked() {
        assert_eq!(usd_to_microusdc(10_000.0).unwrap(), 10_000_000_000);
        assert_eq!(usd_to_microusdc(0.000001).unwrap(), 1);
        assert!(usd_to_microusdc(-1.0).is_err());
        assert!(usd_to_microusdc(f64::NAN).is_err());
        assert!(usd_to_microusdc(f64::INFINITY).is_err());
    }

    #[test]
    fn endpoints_override_parses() {
        let c = Config::from_toml_str("[endpoints]\ngamma_base = \"https://custom-gamma.com\"\n")
            .unwrap();
        assert_eq!(c.endpoints.gamma_base, "https://custom-gamma.com");
        assert_eq!(c.endpoints.clob_base, "https://clob.polymarket.com");
        assert_eq!(
            c.endpoints.ws_market_url,
            "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        );
    }

    #[test]
    fn universe_override_parses() {
        let c = Config::from_toml_str("[universe]\nmax_markets = 100\n").unwrap();
        assert_eq!(c.universe.max_markets, 100);
        assert!(c.universe.require_active);
    }

    #[test]
    fn ingestion_override_parses() {
        let c = Config::from_toml_str("[ingestion]\nstaleness_ms = 2000\n").unwrap();
        assert_eq!(c.ingestion.staleness_ms, 2000);
        assert_eq!(c.ingestion.ws_chunk_size, 50);
    }

    #[test]
    fn validate_rejects_bad_ingestion_values() {
        // staleness_ms < 100
        assert!(Config::from_toml_str("[ingestion]\nstaleness_ms = 50\n").is_err());
        // feed_silence_ms < 1000
        assert!(Config::from_toml_str("[ingestion]\nfeed_silence_ms = 500\n").is_err());
        // ws_chunk_size < 1
        assert!(Config::from_toml_str("[ingestion]\nws_chunk_size = 0\n").is_err());
        // rest_rate_per_sec <= 0.0
        assert!(Config::from_toml_str("[ingestion]\nrest_rate_per_sec = 0.0\n").is_err());
        // rest_rate_capacity < 1
        assert!(Config::from_toml_str("[ingestion]\nrest_rate_capacity = 0\n").is_err());
        // backoff_base_ms > backoff_cap_ms
        assert!(
            Config::from_toml_str("[ingestion]\nbackoff_base_ms = 40000\nbackoff_cap_ms = 30000\n")
                .is_err()
        );
    }

    #[test]
    fn validate_rejects_bad_endpoint_urls() {
        // gamma_base empty
        assert!(Config::from_toml_str("[endpoints]\ngamma_base = \"\"\n").is_err());
        // gamma_base with http:// instead of https://
        assert!(Config::from_toml_str("[endpoints]\ngamma_base = \"http://gamma.com\"\n").is_err());
        // clob_base empty
        assert!(Config::from_toml_str("[endpoints]\nclob_base = \"\"\n").is_err());
        // clob_base with http:// instead of https://
        assert!(Config::from_toml_str("[endpoints]\nclob_base = \"http://clob.com\"\n").is_err());
        // ws_market_url empty
        assert!(Config::from_toml_str("[endpoints]\nws_market_url = \"\"\n").is_err());
        // ws_market_url with http:// instead of wss://
        assert!(Config::from_toml_str("[endpoints]\nws_market_url = \"http://ws.com\"\n").is_err());
    }

    #[test]
    fn validate_rejects_empty_relationships_path() {
        assert!(Config::from_toml_str("[ingestion]\nrelationships_path = \"\"\n").is_err());
    }

    #[test]
    fn m3_sections_default_to_spec_values() {
        let c = Config::default();
        assert_eq!(c.execution.paper_latency_ms, 200);
        assert_eq!(c.execution.fill_window_ms, 500);
        assert_eq!(c.execution.redeem_strategy, "merge");
        assert_eq!(c.risk.max_unhedged_usd, 200.0);
        assert_eq!(c.risk.max_open_orders, 32);
        assert_eq!(c.risk.max_basket_legs, 16);
        assert_eq!(c.risk.daily_drawdown_pct, 2.0);
        assert_eq!(c.risk.error_halt_count, 5);
        assert_eq!(c.risk.error_halt_window_s, 60);
        assert_eq!(c.risk.restart_storm_count, 5);
        assert_eq!(c.risk.restart_storm_window_s, 300);
        assert_eq!(c.risk.kill_file, "kill.switch");
        assert_eq!(c.risk.max_opportunity_age_ms, 1000);
        assert_eq!(c.risk.mid_spread_cap_ticks, 5);
        assert_eq!(c.store.path, "pm.sqlite");
        assert_eq!(c.lp.min_resolve_interval_ms, 500);
        assert_eq!(c.lp.solver_concurrency, 2);
        c.validate().unwrap();
    }

    #[test]
    fn m3_validation_rejects_bad_values() {
        let mut c = Config::default();
        c.execution.redeem_strategy = "yolo".into();
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.execution.fill_window_ms = 100; // < paper_latency_ms (200)
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.risk.daily_drawdown_pct = 0.0;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.risk.max_basket_legs = 1;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.lp.solver_concurrency = 0;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.store.path = String::new();
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.risk.max_unhedged_usd = f64::NAN;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.risk.max_unhedged_usd = f64::INFINITY;
        assert!(c.validate().is_err());
    }

    #[test]
    fn m3_sections_parse_from_toml() {
        let c = Config::from_toml_str(
            "[execution]\npaper_latency_ms = 50\n[risk]\nmax_open_orders = 8\n[store]\npath = \"x.sqlite\"\n[lp]\nsolver_concurrency = 1\n",
        )
        .unwrap();
        assert_eq!(c.execution.paper_latency_ms, 50);
        assert_eq!(c.risk.max_open_orders, 8);
        assert_eq!(c.store.path, "x.sqlite");
        assert_eq!(c.lp.solver_concurrency, 1);
    }

    #[test]
    fn m4_tui_section_defaults_and_validation() {
        let c = Config::default();
        assert_eq!(c.tui.refresh_ms, 100);
        assert_eq!(c.tui.feed_rows, 50);
        assert_eq!(c.tui.fills_rows, 20);
        assert_eq!(c.tui.log_lines, 200);
        c.validate().unwrap();

        let mut c = Config::default();
        c.tui.refresh_ms = 20; // < 50 floor: would melt the read connection
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.tui.log_lines = 0;
        assert!(c.validate().is_err());

        let c = Config::from_toml_str("[tui]\nrefresh_ms = 250\n").unwrap();
        assert_eq!(c.tui.refresh_ms, 250);
    }

    #[test]
    fn live_defaults_are_canary_values() {
        let c = Config::default();
        assert!((c.live.basket_cap_usd - 10.0).abs() < 1e-9);
        assert!((c.live.session_loss_usd - 25.0).abs() < 1e-9);
        assert!((c.live.min_leg_shares - 5.0).abs() < 1e-9);
        assert!((c.live.min_leg_value_usd - 1.0).abs() < 1e-9);
        assert_eq!(c.live.confirm_phrase, "I understand this trades real money");
    }

    #[test]
    fn live_section_parses_and_validates() {
        let c = Config::from_toml_str(
            "[live]\nbasket_cap_usd = 12.5\nsession_loss_usd = 30.0\n[risk]\nmid_spread_cap_ticks = 3\n",
        )
        .unwrap();
        assert!((c.live.basket_cap_usd - 12.5).abs() < 1e-9);
        assert_eq!(c.risk.mid_spread_cap_ticks, 3);
    }

    #[test]
    fn live_caps_must_be_positive() {
        assert!(Config::from_toml_str("[live]\nbasket_cap_usd = 0.0\n").is_err());
        assert!(Config::from_toml_str("[live]\nsession_loss_usd = -1.0\n").is_err());
        assert!(Config::from_toml_str("[live]\nmin_leg_shares = -1.0\n").is_err());
        assert!(Config::from_toml_str("[live]\nmin_leg_value_usd = -1.0\n").is_err());
        assert!(Config::from_toml_str("[risk]\nmid_spread_cap_ticks = 0\n").is_err());
        // empty confirm_phrase must be rejected
        assert!(Config::from_toml_str("[live]\nconfirm_phrase = \"\"\n").is_err());
    }

    #[test]
    fn live_floats_must_be_finite() {
        let mut c = Config::default();
        c.live.basket_cap_usd = f64::NAN;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.live.session_loss_usd = f64::NAN;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.live.min_leg_shares = f64::NAN;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.live.min_leg_value_usd = f64::NAN;
        assert!(c.validate().is_err());
    }

    #[test]
    fn inventory_defaults_are_conservative() {
        let c = Config::default();
        assert!((c.inventory.max_inventory_usd - 50.0).abs() < 1e-9);
        assert!((c.inventory.max_gross_inventory_usd - 100.0).abs() < 1e-9);
        assert!((c.inventory.inventory_stop_loss_usd - 25.0).abs() < 1e-9);
        assert!((c.inventory.daily_loss_usd - 50.0).abs() < 1e-9);
        assert_eq!(c.inventory.vol_pull_ticks, 5);
        assert_eq!(c.inventory.vol_window_ms, 2000);
        c.validate().unwrap();
    }

    #[test]
    fn inventory_section_parses() {
        let c = Config::from_toml_str(
            "[inventory]\nmax_inventory_usd = 75.0\ndaily_loss_usd = 60.0\nvol_pull_ticks = 8\nvol_window_ms = 1500\n",
        )
        .unwrap();
        assert!((c.inventory.max_inventory_usd - 75.0).abs() < 1e-9);
        assert!((c.inventory.daily_loss_usd - 60.0).abs() < 1e-9);
        assert_eq!(c.inventory.vol_pull_ticks, 8);
        assert_eq!(c.inventory.vol_window_ms, 1500);
        // Untouched fields keep their conservative defaults.
        assert!((c.inventory.inventory_stop_loss_usd - 25.0).abs() < 1e-9);
    }

    #[test]
    fn inventory_validation_rejects_bad_values() {
        // stop-loss must be > 0
        assert!(Config::from_toml_str("[inventory]\ninventory_stop_loss_usd = 0.0\n").is_err());
        // daily floor must be > 0
        assert!(Config::from_toml_str("[inventory]\ndaily_loss_usd = 0.0\n").is_err());
        // vol_pull_ticks must be ≥ 1
        assert!(Config::from_toml_str("[inventory]\nvol_pull_ticks = 0\n").is_err());
        // vol_window_ms must be ≥ 1
        assert!(Config::from_toml_str("[inventory]\nvol_window_ms = 0\n").is_err());
        // caps must be positive + finite
        assert!(Config::from_toml_str("[inventory]\nmax_inventory_usd = 0.0\n").is_err());
        assert!(Config::from_toml_str("[inventory]\nmax_gross_inventory_usd = -1.0\n").is_err());

        // The stop-loss (unrealized) and daily (total) floors are INDEPENDENT
        // now — daily < stop is a valid config (no ordering constraint), so it
        // must parse rather than be rejected.
        let c = Config::from_toml_str(
            "[inventory]\ninventory_stop_loss_usd = 40.0\ndaily_loss_usd = 30.0\n",
        )
        .unwrap();
        assert!((c.inventory.inventory_stop_loss_usd - 40.0).abs() < 1e-9);
        assert!((c.inventory.daily_loss_usd - 30.0).abs() < 1e-9);
    }

    #[test]
    fn inventory_floats_must_be_finite() {
        let mut c = Config::default();
        c.inventory.inventory_stop_loss_usd = f64::INFINITY;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.inventory.daily_loss_usd = f64::NAN;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.inventory.max_inventory_usd = f64::NAN;
        assert!(c.validate().is_err());
    }

    #[test]
    fn mm_strategy_defaults_are_off_and_paper() {
        let c = Config::default();
        assert!(!c.strategies.mm.enabled, "MM is OFF by default");
        assert!(!c.strategies.mm.live, "MM is paper by default");
        assert_eq!(c.strategies.mm.spread_bps, 200);
        assert_eq!(c.strategies.mm.quote_refresh_ms, 1500);
        assert!((c.strategies.mm.max_quote_usd - 5.0).abs() < 1e-9);
        assert_eq!(c.strategies.mm.inventory_skew_bps, 150);
        assert_eq!(c.strategies.mm.rebate_bps, 0, "no assumed rebate by default");
        assert!((c.strategies.mm.capital_usd - 25.0).abs() < 1e-9, "default MM slice $25");
        assert_eq!(c.strategies.mm.max_markets, 20, "default first-N market cap");
        assert_eq!(c.strategies.mm.live_fills_source, "ws", "default fills source is the WS feed");
        c.validate().unwrap();
    }

    #[test]
    fn mm_strategy_section_parses() {
        let c = Config::from_toml_str(
            "[strategies.mm]\nenabled = true\nlive = false\nspread_bps = 300\nquote_refresh_ms = 1000\nmax_quote_usd = 7.5\ninventory_skew_bps = 250\nrebate_bps = 20\ncapital_usd = 100.0\nmax_markets = 8\nlive_fills_source = \"rest\"\n",
        )
        .unwrap();
        assert!(c.strategies.mm.enabled);
        assert!(!c.strategies.mm.live);
        assert_eq!(c.strategies.mm.spread_bps, 300);
        assert_eq!(c.strategies.mm.quote_refresh_ms, 1000);
        assert!((c.strategies.mm.max_quote_usd - 7.5).abs() < 1e-9);
        assert_eq!(c.strategies.mm.inventory_skew_bps, 250);
        assert_eq!(c.strategies.mm.rebate_bps, 20);
        assert!((c.strategies.mm.capital_usd - 100.0).abs() < 1e-9);
        assert_eq!(c.strategies.mm.max_markets, 8);
        assert_eq!(c.strategies.mm.live_fills_source, "rest");
        c.validate().unwrap();
    }

    #[test]
    fn mm_live_fills_source_parses_validates_and_defaults() {
        // Default (Task 4.6): the low-latency WS feed.
        assert_eq!(Config::default().strategies.mm.live_fills_source, "ws");
        // Both accepted values parse + validate.
        let ws = Config::from_toml_str("[strategies.mm]\nlive_fills_source = \"ws\"\n").unwrap();
        assert_eq!(ws.strategies.mm.live_fills_source, "ws");
        let rest = Config::from_toml_str("[strategies.mm]\nlive_fills_source = \"rest\"\n").unwrap();
        assert_eq!(rest.strategies.mm.live_fills_source, "rest");
        // Anything else is rejected (mirrors execution.redeem_strategy).
        assert!(
            Config::from_toml_str("[strategies.mm]\nlive_fills_source = \"grpc\"\n").is_err(),
            "an unknown fills source must be rejected"
        );
        // An omitted field keeps the WS default.
        let partial = Config::from_toml_str("[strategies.mm]\nenabled = true\n").unwrap();
        assert_eq!(partial.strategies.mm.live_fills_source, "ws");
    }

    #[test]
    fn mm_partial_section_keeps_other_defaults() {
        // Enabling MM without restating every field keeps the rest at defaults.
        let c = Config::from_toml_str("[strategies.mm]\nenabled = true\n").unwrap();
        assert!(c.strategies.mm.enabled);
        assert_eq!(c.strategies.mm.spread_bps, 200, "untouched field stays default");
        assert_eq!(c.strategies.mm.quote_refresh_ms, 1500);
        assert_eq!(c.strategies.mm.inventory_skew_bps, 150, "untouched field stays default");
        assert_eq!(c.strategies.mm.rebate_bps, 0, "untouched field stays default");
        assert!((c.strategies.mm.capital_usd - 25.0).abs() < 1e-9, "untouched field stays default");
        assert_eq!(c.strategies.mm.max_markets, 20, "untouched field stays default");
    }

    #[test]
    fn mm_capital_carve_out_parses_and_validates() {
        // An enabled MM with a capital slice well within the bankroll round-trips.
        let c = Config::from_toml_str(
            "[capital]\nbankroll_usd = 1000.0\n[strategies.mm]\nenabled = true\ncapital_usd = 250.0\nmax_markets = 5\n",
        )
        .unwrap();
        assert!(c.strategies.mm.enabled);
        assert!((c.strategies.mm.capital_usd - 250.0).abs() < 1e-9);
        assert_eq!(c.strategies.mm.max_markets, 5);
        c.validate().unwrap();
    }

    #[test]
    fn mm_capital_must_be_finite_and_nonnegative() {
        // Negative capital is rejected even when disabled.
        assert!(Config::from_toml_str("[strategies.mm]\ncapital_usd = -1.0\n").is_err());
        let mut c = Config::default();
        c.strategies.mm.capital_usd = f64::NAN;
        assert!(c.validate().is_err());
    }

    #[test]
    fn mm_capital_over_bankroll_rejected_only_when_enabled() {
        // Disabled: a capital slice over the bankroll is inert → still parses.
        // (per_market_usd is set ≤ bankroll so the unrelated capital check passes.)
        let c = Config::from_toml_str(
            "[capital]\nbankroll_usd = 100.0\nper_market_usd = 50.0\n[strategies.mm]\nenabled = false\ncapital_usd = 500.0\n",
        )
        .unwrap();
        assert!((c.strategies.mm.capital_usd - 500.0).abs() < 1e-9);
        // Enabled: the same over-bankroll slice can't be carved out → rejected.
        assert!(
            Config::from_toml_str(
                "[capital]\nbankroll_usd = 100.0\nper_market_usd = 50.0\n[strategies.mm]\nenabled = true\ncapital_usd = 500.0\n",
            )
            .is_err(),
            "an enabled MM slice above the bankroll must be rejected"
        );
    }

    #[test]
    fn mm_rebate_bps_parses_and_validates() {
        // A non-zero operator-set estimate parses and validates (any u32 is a
        // valid estimate rate — documented, not bounded).
        let c = Config::from_toml_str("[strategies.mm]\nrebate_bps = 25\n").unwrap();
        assert_eq!(c.strategies.mm.rebate_bps, 25);
        c.validate().unwrap();
    }

    #[test]
    fn mm_inventory_skew_zero_is_valid() {
        // 0 disables skew (Task-4.2 symmetric quoting) and must parse/validate.
        let c = Config::from_toml_str("[strategies.mm]\ninventory_skew_bps = 0\n").unwrap();
        assert_eq!(c.strategies.mm.inventory_skew_bps, 0);
        c.validate().unwrap();
    }

    #[test]
    fn mm_validation_rejects_bad_values() {
        // spread_bps must be ≥ 1
        assert!(Config::from_toml_str("[strategies.mm]\nspread_bps = 0\n").is_err());
        // quote_refresh_ms floor
        assert!(Config::from_toml_str("[strategies.mm]\nquote_refresh_ms = 50\n").is_err());
        // max_quote_usd must be > 0 and finite
        assert!(Config::from_toml_str("[strategies.mm]\nmax_quote_usd = 0.0\n").is_err());
        assert!(Config::from_toml_str("[strategies.mm]\nmax_quote_usd = -1.0\n").is_err());

        let mut c = Config::default();
        c.strategies.mm.max_quote_usd = f64::NAN;
        assert!(c.validate().is_err());
    }

    #[test]
    fn mm_strategy_unknown_field_is_rejected() {
        assert!(Config::from_toml_str("[strategies.mm]\nbogus = 1\n").is_err());
        assert!(Config::from_toml_str("[strategies]\nbogus = 1\n").is_err());
    }

    #[test]
    fn mm_live_requires_enabled_and_positive_capital() {
        // live + DISABLED → rejected (a live flag on a disabled strategy is a
        // silent no-op footgun).
        assert!(
            Config::from_toml_str("[strategies.mm]\nlive = true\n").is_err(),
            "mm.live without mm.enabled must be rejected"
        );
        // live + enabled + ZERO capital → rejected (can't fund any quotes).
        assert!(
            Config::from_toml_str(
                "[strategies.mm]\nenabled = true\nlive = true\ncapital_usd = 0.0\n"
            )
            .is_err(),
            "live MM with $0 capital must be rejected"
        );
        // live + enabled + OVER-BANKROLL capital → rejected (the enabled
        // over-bankroll carve guard fires).
        assert!(
            Config::from_toml_str(
                "[capital]\nbankroll_usd = 100.0\nper_market_usd = 50.0\n[strategies.mm]\nenabled = true\nlive = true\ncapital_usd = 500.0\n"
            )
            .is_err(),
            "live MM whose slice exceeds the bankroll must be rejected"
        );
        // live + enabled + a tiny POSITIVE slice within the bankroll → OK.
        let c = Config::from_toml_str(
            "[strategies.mm]\nenabled = true\nlive = true\ncapital_usd = 25.0\n",
        )
        .unwrap();
        assert!(c.strategies.mm.live && c.strategies.mm.enabled);
        assert!((c.strategies.mm.capital_usd - 25.0).abs() < 1e-9);
        // Default config (mm.live = false) is unaffected.
        Config::default().validate().unwrap();
    }

    #[test]
    fn segments_defaults_are_sane() {
        let c = Config::default();
        assert!((c.segments.liquid_stable_min_volume - 100_000.0).abs() < 1e-6);
        assert!((c.segments.liquid_stable_min_liquidity - 50_000.0).abs() < 1e-6);
        assert!((c.segments.liquid_min_volume - 10_000.0).abs() < 1e-6);
        assert!((c.segments.liquid_min_liquidity - 5_000.0).abs() < 1e-6);
        // High bar ≥ low bar on each axis.
        assert!(c.segments.liquid_stable_min_volume >= c.segments.liquid_min_volume);
        assert!(c.segments.liquid_stable_min_liquidity >= c.segments.liquid_min_liquidity);
        c.validate().unwrap();
    }

    #[test]
    fn segments_section_parses_and_round_trips() {
        // A full-config slice that overrides [segments] alongside another section
        // round-trips, and untouched segment fields keep their defaults.
        let c = Config::from_toml_str(
            "[capital]\nbankroll_usd = 5000.0\n[segments]\nliquid_stable_min_volume = 250000.0\nliquid_min_volume = 20000.0\n",
        )
        .unwrap();
        assert!((c.segments.liquid_stable_min_volume - 250_000.0).abs() < 1e-6);
        assert!((c.segments.liquid_min_volume - 20_000.0).abs() < 1e-6);
        // Untouched liquidity fields keep their defaults.
        assert!((c.segments.liquid_stable_min_liquidity - 50_000.0).abs() < 1e-6);
        assert!((c.segments.liquid_min_liquidity - 5_000.0).abs() < 1e-6);
        assert!((c.capital.bankroll_usd - 5000.0).abs() < 1e-6);
    }

    #[test]
    fn segments_validation_rejects_bad_values() {
        // Negative threshold rejected.
        assert!(Config::from_toml_str("[segments]\nliquid_min_volume = -1.0\n").is_err());
        // High bar below low bar (volume axis) rejected.
        assert!(
            Config::from_toml_str(
                "[segments]\nliquid_stable_min_volume = 5000.0\nliquid_min_volume = 10000.0\n"
            )
            .is_err()
        );
        // High bar below low bar (liquidity axis) rejected.
        assert!(
            Config::from_toml_str(
                "[segments]\nliquid_stable_min_liquidity = 1000.0\nliquid_min_liquidity = 5000.0\n"
            )
            .is_err()
        );
        // Non-finite rejected.
        let mut c = Config::default();
        c.segments.liquid_min_liquidity = f64::NAN;
        assert!(c.validate().is_err());
        // Equal high == low bars are allowed (inclusive).
        let c = Config::from_toml_str(
            "[segments]\nliquid_stable_min_volume = 10000.0\nliquid_min_volume = 10000.0\n",
        )
        .unwrap();
        assert!((c.segments.liquid_stable_min_volume - 10_000.0).abs() < 1e-6);
    }

    #[test]
    fn segments_unknown_field_is_rejected() {
        assert!(Config::from_toml_str("[segments]\nbogus = 1.0\n").is_err());
    }
}
