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
    pub confluence: Confluence,
    pub reward_farm: RewardFarm,
    /// Smart-money copy-trading tuning (`[copy]`, Task C2). Renamed so the TOML
    /// section is the terse `[copy]` while the Rust field stays `copy_params`
    /// (and the per-strategy entry is `[strategies.copy]`).
    #[serde(rename = "copy")]
    pub copy_params: CopyParamsCfg,
    /// BTC 5-minute strategy tuning (`[btc5m]`). Renamed so the TOML section
    /// is the terse `[btc5m]` while the Rust field stays `btc5m_params` (and
    /// the per-strategy entry is `[strategies.btc5m]`).
    #[serde(rename = "btc5m")]
    pub btc5m_params: Btc5mParamsCfg,
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
    /// Task 5.3 — opt-in universe prioritization. **Default `false`** ⇒ the
    /// universe cap keeps the first `max_markets` markets in Gamma keyset order
    /// (historical, byte-identical behaviour). `true` ⇒ sync gathers a candidate
    /// pool, ranks it by (segment tier, then liquidity, then volume), and keeps
    /// the highest-priority `max_markets`. OFF by default because it changes
    /// which markets the LIVE sync selects, so it must be opted into deliberately.
    pub prioritize_by_liquidity: bool,
    /// Task 5.3 — candidate pool size for prioritization. **Default `0`**, the
    /// sentinel for "= `max_markets`" (no extra fetching ⇒ identical market set,
    /// just internally ranked). A non-zero value MUST be ≥ `max_markets`: sync
    /// gathers up to this many ACCEPTED candidates (in keyset order), ranks them,
    /// and keeps the top `max_markets`. Bounded at sync time by
    /// `pm_ingestion::sync::MAX_CANDIDATE_POOL` (5000) to cap CLOB API cost.
    /// Inert unless `prioritize_by_liquidity`.
    pub candidate_pool: usize,
    /// Periodic AUTO-RESTART interval, seconds. **Default `0`** ⇒ off (one-shot
    /// session, as before). When `> 0`, the process re-execs itself every
    /// `auto_restart_secs` to pick up a FRESH (and possibly larger) market
    /// universe — the pragmatic "new markets over time" mechanism, since the
    /// registry + WS supervisors are built once per run. The restart is a
    /// graceful shutdown (store flushed, terminal restored) then `exec` of the
    /// same argv; positions / P&L persist via the SQLite store + the existing
    /// startup reconciliation, so the only cost is a brief re-quote gap during
    /// the ~30-60 s re-sync. Set it well above the sync time (e.g. ≥ 600) so the
    /// bot spends most of its time trading. A `kill` / `quit` / `--duration-secs`
    /// end exits normally — only this timer re-execs.
    pub auto_restart_secs: u64,
}

/// `[confluence]` — "follow the smart money" market selection (opt-in). When
/// `enabled`, the universe is built from the OPEN positions of the top
/// leaderboard traders (public Data API) instead of liquidity-ranked Gamma
/// markets, and the MM leans toward the side those traders hold. OFF by default
/// (the normal liquidity universe).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Confluence {
    /// Master switch. `false` ⇒ the normal liquidity-ranked universe.
    pub enabled: bool,
    /// Number of top traders WITH open positions to follow.
    pub top_traders: usize,
    /// How deep to scan the leaderboard (1..=50) to find `top_traders` of them.
    pub scan_limit: usize,
    /// Leaderboard ranking metric: `"pnl"` (top performers) or `"vol"` (active).
    pub order_by: String,
    /// Leaderboard window: `"day"` | `"week"` | `"month"` | `"all"`.
    pub time_period: String,
    /// Drop a trader's positions below this many shares (API `sizeThreshold`).
    pub size_threshold: f64,
}

impl Default for Confluence {
    fn default() -> Self {
        Confluence {
            enabled: false,
            top_traders: 10,
            scan_limit: 50,
            order_by: "pnl".into(),
            time_period: "month".into(),
            size_threshold: 1.0,
        }
    }
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
            // Task 5.3 defaults keep the historical keyset-order cap unchanged.
            prioritize_by_liquidity: false,
            candidate_pool: 0,
            // Off by default → one-shot session (no periodic re-exec).
            auto_restart_secs: 0,
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
    /// M6 deposit-wallet relayer (live on-chain merge/redeem) master switch.
    /// `false` (default) → no relayer is constructed and live merge/redeem stays
    /// the hold-to-resolution no-op; the relayer also requires relayer creds
    /// (`RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS`) from env (spec 2026-06-25
    /// §7). Opt in deliberately.
    pub relayer_enabled: bool,
    /// Staging-first: `true` (default) targets Polymarket's relayer STAGING
    /// environment so the first funded batch is off prod. The live MM constructor
    /// picks the staging-vs-prod URL from this flag unless `relayer_url` overrides.
    pub relayer_staging: bool,
    /// Explicit relayer base URL override. `None` (default) → derive the URL from
    /// `relayer_staging`. When `Some`, it MUST be non-empty (validated).
    pub relayer_url: Option<String>,
}

impl Default for Live {
    fn default() -> Self {
        Live {
            basket_cap_usd: 10.0,
            session_loss_usd: 25.0,
            min_leg_shares: 5.0,
            min_leg_value_usd: 1.0,
            confirm_phrase: "I understand this trades real money".into(),
            // M6 relayer: OFF by default, staging-first, no URL override.
            relayer_enabled: false,
            relayer_staging: true,
            relayer_url: None,
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
    pub copy: CopyCfg,
    pub btc5m: Btc5mCfg,
}

/// Smart-money COPY strategy config (`[strategies.copy]`, Task C2). The copy
/// executor follows a whitelist of top-ranked traders (see
/// `pm_ingestion::smart_money`) and mirrors their fresh buys within its capital
/// envelope; the per-trade sizing / freshness / whitelist knobs live in the
/// top-level `[copy]` section ([`CopyParamsCfg`]). DEFAULT-OFF and paper-only
/// until an operator flips both `enabled` and `live`, matching the cross-cutting
/// safety model (mirrors [`Mm`]). Named `CopyCfg` (not `Copy`) so it never
/// shadows the `Copy` trait.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CopyCfg {
    /// Master switch. `false` (default) → the copy strategy never trades; the
    /// platform behaves exactly as before. Mirrors the established default-off
    /// flag pattern (`strategies.mm.enabled`).
    pub enabled: bool,
    /// Live arm. `false` (default) → paper only; the live arm trades real
    /// orders. Inert (and unvalidated as a gate here) until the executor wires
    /// it up.
    pub live: bool,
    /// Strategy capital envelope, USD. When `enabled`, this is carved OUT of the
    /// platform bankroll (mirrors `strategies.mm.capital_usd`): must be finite
    /// and ≥ 0 always, and ≤ `capital.bankroll_usd` when enabled. Inert while
    /// disabled.
    pub capital_usd: f64,
}

impl Default for CopyCfg {
    fn default() -> Self {
        CopyCfg {
            enabled: false,
            live: false,
            capital_usd: 25.0,
        }
    }
}

/// BTC 5-minute (up/down) strategy config (`[strategies.btc5m]`). Trades
/// short-dated BTC up/down markets off a composite spot feed and a
/// volatility-normalized fair-value model; the model / feed tuning knobs live
/// in the top-level `[btc5m]` section ([`Btc5mParamsCfg`]). DEFAULT-OFF and
/// paper-only until an operator flips both `enabled` and `live`, matching the
/// cross-cutting safety model (mirrors [`CopyCfg`] / [`Mm`]).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Btc5mCfg {
    /// Master switch. `false` (default) → the btc5m strategy never trades;
    /// the platform behaves exactly as before.
    pub enabled: bool,
    /// Live arm. `false` (default) → paper only.
    pub live: bool,
    /// Strategy capital envelope, USD. When `enabled`, this is carved OUT of
    /// the platform bankroll (mirrors `strategies.copy.capital_usd`): must be
    /// finite and ≥ 0 always, and ≤ `capital.bankroll_usd` when enabled.
    /// Inert while disabled.
    pub capital_usd: f64,
}

impl Default for Btc5mCfg {
    fn default() -> Self {
        Btc5mCfg {
            enabled: false,
            live: false,
            capital_usd: 25.0,
        }
    }
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
    /// Cap on how many markets the MM quotes (both YES+NO sides). Per-segment
    /// routing (Task 5.2, `pm_app::wiring::mm_market_selection`) first filters
    /// the universe to the MM's allowed liquidity segments (`[segments]
    /// .mm_segments`, fee-free markets skipped per `mm_exclude_fee_free`), ranks
    /// them by PER-MARKET volume (then liquidity), and de-concentrates them to
    /// at most `max_per_event` per event/component; the MM then quotes the top
    /// `max_markets` of that capped, ranked set. `0` quotes nothing (inert); any
    /// `usize` is valid — documented, not bounded.
    pub max_markets: usize,
    /// Max markets the MM quotes from any ONE event/component — the
    /// de-concentration cap (`pm_app::wiring::mm_market_selection`). A NegRisk
    /// event's outcomes — and any relationship-linked markets — collapse into
    /// ONE component (`pm_registry::Registry::component_of`, union-find). Without
    /// this cap the MM piled into ~20 outcomes of a SINGLE event (e.g. World-Cup
    /// -winner longshots) because every outcome inherits the event's liquidity
    /// and so ranks together at the top. Walking the volume-ranked candidates,
    /// once this many markets from a component have been chosen the rest of that
    /// component is skipped, so the MM spreads across DISTINCT markets. `0` ⇒ NO
    /// cap (unlimited markets per event); any `usize` is valid — documented, not
    /// bounded. Default `2`.
    pub max_per_event: usize,
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
    /// PAPER-only demo aid: simulated passive-taker-flow fill rate, percent of
    /// each resting quote's remaining size lifted per quote cycle (`0`–`100`).
    /// `0` (default) = the conservative sim that fills ONLY on an adverse book
    /// cross — so in a calm market the MM never fills and looks idle in paper.
    /// A positive value makes the `PaperMakerVenue` simulate takers hitting the
    /// resting quotes (at the maker's own price), exercising the full quote →
    /// fill → inventory → skew → P&L loop in paper. DELIBERATELY OPTIMISTIC (no
    /// queue-position modelling) and IGNORED on the live path — realistic fills
    /// come from real taker flow in a live canary.
    pub paper_taker_fill_pct: u32,
    /// Quote policy: "spread_capture" (default, legacy) or "reward_farm".
    #[serde(default = "default_mm_policy")]
    pub policy: String,
}

fn default_mm_policy() -> String {
    "spread_capture".into()
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
            // broad market cap (Phase 5 refines) so the MM covers many DISTINCT
            // markets rather than a single event's outcomes.
            capital_usd: 25.0,
            max_markets: 60,
            // Quote at most 2 markets from any one event/component, so a big
            // multi-outcome NegRisk event can't crowd out the rest.
            max_per_event: 2,
            // Default to the low-latency user-WS fills feed (the scalping
            // upgrade); operators can pin "rest" to fall back to the
            // offline-verified Task-4.5 REST poll.
            live_fills_source: "ws".into(),
            // OFF by default → the conservative adverse-only paper sim (no
            // synthetic fills); a paper-MM demo sets this > 0.
            paper_taker_fill_pct: 0,
            // Legacy spread-capture quoting by default; opt into "reward_farm"
            // (tuned by the top-level [reward_farm] section) deliberately.
            policy: default_mm_policy(),
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
    /// Liquidity segments the MARKET MAKER may quote (Task 5.2 routing). Each
    /// entry names a `MarketSegment`; default `["LiquidStable", "Liquid"]` —
    /// deliberately NOT `Illiquid`, the conservative arb-only tier (thin or
    /// unknown-liquidity markets must never be quoted). Names are matched
    /// CASE-INSENSITIVELY with underscores IGNORED, so `"LiquidStable"`,
    /// `"liquid_stable"`, and `"liquidstable"` all select the LiquidStable tier;
    /// the CANONICAL form is the PascalCase enum name (`LiquidStable` / `Liquid`
    /// / `Illiquid`). `Config::validate` rejects any unrecognised name (see
    /// [`normalize_segment_name`]). An empty list routes the MM to NO markets
    /// (inert). The string→`MarketSegment` mapping lives in
    /// `pm_app::wiring::mm_allowed_segments`, keeping this crate decoupled from
    /// `pm-registry` (Task 5.1): config validates the spellings, the app maps
    /// them to the enum.
    ///
    /// ARB IS NOT ROUTED BY THIS — it runs on EVERY market unconditionally as
    /// the universal safety net; segment routing is MM-only.
    pub mm_segments: Vec<String>,
    /// Skip fee-free markets (`fee_bps == 0`) when routing the MARKET MAKER.
    /// Default `true`: the MM is rebate-driven, so a fee-free market earns it no
    /// rebate and its spread economics differ — quoting it is off the strategy's
    /// edge. This is the fee signal that ACTUALLY EXISTS standing in for the
    /// spec's "fee-free Geopolitics excluded from rebate-driven MM" rule: the
    /// Gamma feed carries no category yet (`MarketMetrics::category` is always
    /// `None`), so the exclusion is expressed via the fee we do have rather than
    /// a category we don't. Set `false` to let the MM quote fee-free markets too.
    ///
    /// MM-ONLY — arb is unaffected (it never filters on fees for routing).
    pub mm_exclude_fee_free: bool,
}

impl Default for Segments {
    fn default() -> Self {
        Segments {
            liquid_stable_min_volume: 100_000.0,
            liquid_stable_min_liquidity: 50_000.0,
            liquid_min_volume: 10_000.0,
            liquid_min_liquidity: 5_000.0,
            // MM quotes the liquid tiers only (never Illiquid), and skips
            // fee-free markets (no maker rebate there) — see field docs.
            mm_segments: vec!["LiquidStable".to_string(), "Liquid".to_string()],
            mm_exclude_fee_free: true,
        }
    }
}

/// Reward-farming MM tuning (`[reward_farm]`). A TOP-LEVEL section (sibling of
/// `[segments]` / `[universe]`), inert unless `[strategies.mm].policy =
/// "reward_farm"` selects the reward-farming quoting engine. The defaults mirror
/// Polymarket's maker-rewards mechanics (e.g. 1/min sampling).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RewardFarm {
    /// Re-quote a side only when it drifts this many ticks past target (anti-flicker).
    pub requote_band_ticks: u16,
    /// Max bid:ask size lean for inventory skew.
    pub size_skew_max_ratio: f64,
    /// Estimator sampling cadence (ms), mirroring Polymarket's 1/min sampling.
    pub sample_interval_ms: u64,
    /// Minimum reward-eligible markets to quote.
    pub min_markets: u32,
    /// Book levels summed for the order-book IMBALANCE depth. (The microprice
    /// fair value itself is strictly top-of-book; this knob does not widen it.)
    pub microprice_levels: u16,
    /// Rolling window (ms) for the momentum signal.
    pub signal_window_ms: u64,
    /// |signal| above this pulls the endangered side ([0,1]).
    pub pull_threshold: f64,
    /// Suppress re-quoting a pulled side this long (ms).
    pub pull_cooldown_ms: u64,
    /// Re-place a side when its size lean drifts more than this fraction.
    pub size_rebalance_pct: f64,
    /// Opt-in: quote the complement pair (BID-YES + BID-NO) for two-sided-from-flat
    /// reward farming (required for live, which has no naked short). Off = Spec-1
    /// single-token bid+ask.
    pub hedging_enabled: bool,
    /// Merge a held complete YES+NO set once the matched pair exceeds this (USD),
    /// recycling locked collateral. Paper-simulated; live merge is deferred.
    pub merge_threshold_usd: f64,
}
impl Default for RewardFarm {
    fn default() -> Self {
        RewardFarm {
            requote_band_ticks: 1,
            size_skew_max_ratio: 2.0,
            sample_interval_ms: 60_000,
            min_markets: 1,
            microprice_levels: 3,
            signal_window_ms: 3000,
            pull_threshold: 0.6,
            pull_cooldown_ms: 5000,
            size_rebalance_pct: 0.25,
            hedging_enabled: false,
            merge_threshold_usd: 5.0,
        }
    }
}

/// Smart-money copy-trading tuning (`[copy]`, Task C2). A TOP-LEVEL section
/// (sibling of `[reward_farm]` / `[segments]`), inert unless
/// `[strategies.copy].enabled` selects the copy executor. The sizing/risk knobs
/// (`per_position_usd`, `max_gross_usd`, `max_concurrent_positions`,
/// `stop_loss_pct`) bound the copied book; the alpha knobs (`max_drift`,
/// `reaction_window_secs`, `min_bets`, `top_n`) feed the shared ranking +
/// freshness math in `pm_ingestion::smart_money` (`rank_wallets_oos`,
/// `within_drift`). Defaults are deliberately conservative (tiny book, tight
/// stop) so the strategy is safe to flip on in paper.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CopyParamsCfg {
    /// Notional opened per copied position, USD.
    pub per_position_usd: f64,
    /// Cap on simultaneously-open copied positions. In DYNAMIC mode
    /// (`gross_pct > 0`) this is the HARD CAP on the budget-scaled concurrency
    /// (`min(max_gross / per_position, this)`); in static mode it's the fixed cap.
    pub max_concurrent_positions: u32,
    /// Gross exposure cap summed across open copied positions, USD. Used as the
    /// FALLBACK when `gross_pct == 0` OR the live account equity can't be fetched.
    pub max_gross_usd: f64,
    /// DYNAMIC gross cap as a fraction of live account EQUITY (cash + open
    /// positions value), in `[0, 1]`. `0` (default) ⇒ use the fixed
    /// `max_gross_usd`. When `> 0`, the copy strategy each cycle sets
    /// `max_gross = gross_pct × equity` (also its capital carve) and keeps the
    /// per-copy notional FIXED at `per_position_usd`, scaling the CONCURRENCY
    /// instead — `max_concurrent = min(max_gross / per_position, max_concurrent_positions)`
    /// — so a funded account opens more fixed-size copies (up to the cap), never
    /// missing a signal for lack of a slot. Falls back to `max_gross_usd` if the
    /// balance fetch fails. Example: `0.5` = never more than half the account at risk.
    pub gross_pct: f64,
    /// Cut a copied position at this unrealized-loss fraction (0.25 = −25%).
    /// A fraction in (0, 1].
    pub stop_loss_pct: f64,
    /// FRESHNESS gate: skip the copy if OUR entry price is more than this
    /// fraction off the trader's fill — i.e. we'd be chasing a runner whose
    /// edge is gone (see `pm_ingestion::smart_money::within_drift`). A fraction
    /// in (0, 1]; default 0.15 (15%).
    pub max_drift: f64,
    /// Copy a buy only within this many seconds of the trader's fill (the
    /// reaction window); a stale signal is dropped. Default 1800 (30 min).
    pub reaction_window_secs: i64,
    /// Minimum resolved PRE-cutoff bets required to rank a trader at all
    /// (`pm_ingestion::smart_money::rank_wallets_oos`'s `min_bets`). `0` ranks
    /// even zero-sample wallets — documented, not bounded.
    pub min_bets: usize,
    /// Follow-whitelist size: the top-N ranked traders to copy. Must be ≥ 1
    /// (an empty whitelist would copy nobody).
    pub top_n: usize,
    /// How often to rebuild the follow whitelist, seconds. Default 21600 (6 h).
    pub whitelist_refresh_secs: u64,
    /// How often to poll the whitelist's recent trades for new copy signals,
    /// seconds. Default 90.
    pub signal_poll_secs: u64,
    /// Sell our copied position when the copied trader sells out. Default
    /// `true` (track their exit); `false` holds to our own stop / resolution.
    pub follow_exit: bool,
}

impl Default for CopyParamsCfg {
    fn default() -> Self {
        CopyParamsCfg {
            per_position_usd: 5.0,
            max_concurrent_positions: 3,
            max_gross_usd: 25.0,
            // 0 ⇒ fixed max_gross_usd; set > 0 (e.g. 0.5) for equity-scaled caps.
            gross_pct: 0.0,
            // Tight stop / freshness by default — conservative.
            stop_loss_pct: 0.25,
            max_drift: 0.15,
            // Copy a buy only within 30 min of the trader's fill.
            reaction_window_secs: 1800,
            // Rank a trader only with a real sample; whitelist the top 30.
            min_bets: 10,
            top_n: 30,
            // Rebuild the whitelist every 6 h; poll for signals every 90 s.
            whitelist_refresh_secs: 21_600,
            signal_poll_secs: 90,
            // Track the trader's exit by default.
            follow_exit: true,
        }
    }
}

/// BTC 5-minute strategy tuning (`[btc5m]`). Feeds the composite spot feed and
/// volatility-normalized fair-value model that back the `[strategies.btc5m]`
/// executor. Defaults are deliberately conservative so the strategy is safe
/// to flip on in paper.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Btc5mParamsCfg {
    /// Half-life, in minutes, of the EWMA volatility estimate used to
    /// normalize the fair-value model's z-score.
    pub vol_half_life_min: f64,
    /// Minimum volatility samples required before the model is considered
    /// warmed up (and the strategy will act on its signal).
    pub vol_warmup_samples: u32,
    /// Minimum |z-score| required to treat the fair-value deviation as a
    /// tradeable signal.
    pub z_threshold: f64,
    /// How often the fair-value model samples the composite spot feed, ms.
    pub sample_interval_ms: u64,
    /// Composite spot feed sources (e.g. `"coinbase"`, `"kraken"`); must list
    /// at least one.
    pub spot_sources: Vec<String>,
    /// How often each spot source is polled, ms.
    pub spot_poll_ms: u64,
    /// Window, seconds, over which spot samples are kept at full (dense)
    /// resolution before being thinned.
    pub dense_window_secs: i64,
}

impl Default for Btc5mParamsCfg {
    fn default() -> Self {
        Btc5mParamsCfg {
            vol_half_life_min: 120.0,
            vol_warmup_samples: 180,
            z_threshold: 1.5,
            sample_interval_ms: 1000,
            spot_sources: vec!["coinbase".into(), "kraken".into()],
            spot_poll_ms: 1000,
            dense_window_secs: 60,
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
        // Universe scaling (Task 5.3). A non-zero candidate pool must be able to
        // fill the cap, so it must be ≥ max_markets — you cannot rank fewer
        // candidates than you intend to keep. `0` is the sentinel for
        // "= max_markets" (no extra fetching). Values above the sync-time ceiling
        // (`pm_ingestion::sync::MAX_CANDIDATE_POOL`) are CLAMPED there, not
        // rejected, so they need no bound here. Defaults
        // (prioritize_by_liquidity = false, candidate_pool = 0) keep the
        // historical keyset-order cap unchanged.
        if self.universe.candidate_pool != 0
            && self.universe.candidate_pool < self.universe.max_markets
        {
            return Err(ConfigError::BadMoney(
                "universe.candidate_pool must be 0 (= max_markets) or ≥ max_markets",
            ));
        }
        // Auto-restart: 0 = off, else must clear the sync time so the bot does
        // not thrash (re-sync alone is tens of seconds). Reject a too-small
        // non-zero interval as a near-certain footgun.
        if self.universe.auto_restart_secs != 0 && self.universe.auto_restart_secs < 60 {
            return Err(ConfigError::BadMoney(
                "universe.auto_restart_secs must be 0 (off) or ≥ 60 (a smaller interval would re-sync more than it trades)",
            ));
        }
        // Confluence (only meaningful when enabled).
        if self.confluence.enabled {
            if self.confluence.top_traders == 0 {
                return Err(ConfigError::BadMoney("confluence.top_traders must be > 0"));
            }
            if self.confluence.scan_limit < self.confluence.top_traders
                || self.confluence.scan_limit > 50
            {
                return Err(ConfigError::BadMoney(
                    "confluence.scan_limit must be ≥ top_traders and ≤ 50 (leaderboard cap)",
                ));
            }
            if !matches!(self.confluence.order_by.to_ascii_lowercase().as_str(), "pnl" | "vol") {
                return Err(ConfigError::BadMoney("confluence.order_by must be \"pnl\" or \"vol\""));
            }
            if !matches!(
                self.confluence.time_period.to_ascii_lowercase().as_str(),
                "day" | "week" | "month" | "all"
            ) {
                return Err(ConfigError::BadMoney(
                    "confluence.time_period must be day|week|month|all",
                ));
            }
            if self.confluence.size_threshold < 0.0 || !self.confluence.size_threshold.is_finite() {
                return Err(ConfigError::BadMoney("confluence.size_threshold must be ≥ 0"));
            }
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
        // M6 relayer knobs (spec 2026-06-25 §7). `relayer_enabled`/`relayer_staging`
        // are plain booleans (default OFF / staging-first) and need no check; the
        // relayer is additionally gated at runtime on relayer creds. A
        // `relayer_url` OVERRIDE, if set, must be non-empty — an empty string is a
        // silent misconfig (the URL is what reaches the relayer).
        if self.live.relayer_url.as_deref().is_some_and(str::is_empty) {
            return Err(ConfigError::BadMoney(
                "live.relayer_url, if set, must be non-empty",
            ));
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
        // Paper-only taker-flow demo aid: a percentage (0–100). Inert on live.
        if self.strategies.mm.paper_taker_fill_pct > 100 {
            return Err(ConfigError::BadMoney(
                "strategies.mm.paper_taker_fill_pct must be 0–100",
            ));
        }
        // Quote policy (Task 3): exactly "spread_capture" (legacy) or
        // "reward_farm" (mirrors the execution.redeem_strategy string-enum style).
        if !matches!(self.strategies.mm.policy.as_str(), "spread_capture" | "reward_farm") {
            return Err(ConfigError::BadMoney(
                "strategies.mm.policy must be spread_capture or reward_farm",
            ));
        }
        // Reward-farming tuning (`[reward_farm]`, top-level). The size lean is a
        // bid:ask ratio, so it must be FINITE and ≥ 1.0 (1.0 = no lean): a sub-1
        // value would invert the lean, and toml can parse `nan`/`inf`, so a
        // non-finite ratio must be rejected too — mirroring the finiteness
        // convention of the other float checks. Checked unconditionally — cheap,
        // and the section is inert unless the reward_farm policy is selected.
        if self.reward_farm.size_skew_max_ratio < 1.0
            || !self.reward_farm.size_skew_max_ratio.is_finite()
        {
            return Err(ConfigError::BadMoney(
                "reward_farm.size_skew_max_ratio must be finite and >= 1.0",
            ));
        }
        if self.reward_farm.microprice_levels < 1 {
            return Err(ConfigError::BadMoney("reward_farm.microprice_levels must be >= 1"));
        }
        if !(0.0..=1.0).contains(&self.reward_farm.pull_threshold) || !self.reward_farm.pull_threshold.is_finite() {
            return Err(ConfigError::BadMoney("reward_farm.pull_threshold must be in [0,1]"));
        }
        if !(0.0..=1.0).contains(&self.reward_farm.size_rebalance_pct) || !self.reward_farm.size_rebalance_pct.is_finite() {
            return Err(ConfigError::BadMoney("reward_farm.size_rebalance_pct must be in [0,1]"));
        }
        if self.reward_farm.merge_threshold_usd < 0.0 || !self.reward_farm.merge_threshold_usd.is_finite() {
            return Err(ConfigError::BadMoney("reward_farm.merge_threshold_usd must be finite and >= 0"));
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
        // `max_per_event` (usize) likewise needs no bound: it is always ≥ 0; `0`
        // is the sentinel for "no per-event cap" (unlimited markets from one
        // event/component) and any positive value caps how many of a component's
        // markets the MM quotes. Documented, not checked.
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
        // Per-segment routing (Task 5.2): every `mm_segments` entry must name a
        // known `MarketSegment` (case-insensitive, underscores ignored). An
        // empty list is allowed (the MM then quotes nothing — inert). Only the
        // MM list is validated: arb runs on ALL markets unconditionally (the
        // universal safety net) and takes no routing config. `mm_exclude_fee_free`
        // is a plain bool — no validation needed.
        for name in &seg.mm_segments {
            if normalize_segment_name(name).is_none() {
                return Err(ConfigError::BadMoney(
                    "segments.mm_segments contains an unknown segment name \
                     (expected LiquidStable, Liquid, or Illiquid)",
                ));
            }
        }
        // Smart-money COPY strategy (`[strategies.copy]` + `[copy]`, Task C2).
        // Capital mirrors the MM carve-out: finite + ≥ 0 ALWAYS, and ≤ the
        // bankroll when ENABLED (it is carved out of the platform bankroll, so
        // it can never exceed it — disabled is inert, so the bankroll bound is
        // skipped). The `[copy]` tuning knobs are validated UNCONDITIONALLY
        // (cheap; the section is inert unless the executor is selected), exactly
        // as the MM / reward_farm / inventory knobs are.
        if self.strategies.copy.capital_usd < 0.0 || !self.strategies.copy.capital_usd.is_finite() {
            return Err(ConfigError::BadMoney(
                "strategies.copy.capital_usd must be finite and ≥ 0",
            ));
        }
        if self.strategies.copy.enabled
            && self.strategies.copy.capital_usd > self.capital.bankroll_usd
        {
            return Err(ConfigError::BadMoney(
                "strategies.copy.capital_usd must be ≤ capital.bankroll_usd when enabled",
            ));
        }
        let cp = &self.copy_params;
        if cp.per_position_usd <= 0.0 || !cp.per_position_usd.is_finite() {
            return Err(ConfigError::BadMoney("copy.per_position_usd must be > 0"));
        }
        if cp.max_gross_usd <= 0.0 || !cp.max_gross_usd.is_finite() {
            return Err(ConfigError::BadMoney("copy.max_gross_usd must be > 0"));
        }
        // Dynamic equity-scaled gross: 0 = off (use fixed max_gross_usd). A value
        // > 1 would risk more than the whole account; NaN/∞ is caught by the range.
        if !(cp.gross_pct.is_finite() && (0.0..=1.0).contains(&cp.gross_pct)) {
            return Err(ConfigError::BadMoney("copy.gross_pct must be in [0, 1]"));
        }
        if cp.max_concurrent_positions < 1 {
            return Err(ConfigError::BadMoney(
                "copy.max_concurrent_positions must be ≥ 1",
            ));
        }
        // Both fractions are in (0, 1]: a 0 stop/drift would never trigger a cut
        // / always reject the copy, and a value > 1 is nonsensical (>100%). The
        // range check also catches NaN/∞, but keep the explicit `is_finite` for
        // intent (mirrors `reward_farm.pull_threshold`).
        if !(cp.stop_loss_pct.is_finite() && cp.stop_loss_pct > 0.0 && cp.stop_loss_pct <= 1.0) {
            return Err(ConfigError::BadMoney("copy.stop_loss_pct must be in (0, 1]"));
        }
        if !(cp.max_drift.is_finite() && cp.max_drift > 0.0 && cp.max_drift <= 1.0) {
            return Err(ConfigError::BadMoney("copy.max_drift must be in (0, 1]"));
        }
        if cp.reaction_window_secs <= 0 {
            return Err(ConfigError::BadMoney(
                "copy.reaction_window_secs must be > 0",
            ));
        }
        if cp.signal_poll_secs == 0 {
            return Err(ConfigError::BadMoney("copy.signal_poll_secs must be > 0"));
        }
        if cp.whitelist_refresh_secs == 0 {
            return Err(ConfigError::BadMoney(
                "copy.whitelist_refresh_secs must be > 0",
            ));
        }
        if cp.top_n < 1 {
            return Err(ConfigError::BadMoney("copy.top_n must be ≥ 1"));
        }
        // `min_bets` (usize) needs no bound: `0` simply ranks even zero-sample
        // wallets (degenerate but valid); any usize is accepted. Documented,
        // not checked — mirrors `strategies.mm.max_markets`.
        if self.strategies.btc5m.capital_usd < 0.0 || !self.strategies.btc5m.capital_usd.is_finite()
        {
            return Err(ConfigError::BadMoney(
                "strategies.btc5m.capital_usd must be finite and ≥ 0",
            ));
        }
        if self.strategies.btc5m.enabled
            && self.strategies.btc5m.capital_usd > self.capital.bankroll_usd
        {
            return Err(ConfigError::BadMoney(
                "strategies.btc5m.capital_usd must be ≤ capital.bankroll_usd when enabled",
            ));
        }
        let bp = &self.btc5m_params;
        if !(bp.vol_half_life_min.is_finite() && bp.vol_half_life_min > 0.0) {
            return Err(ConfigError::BadMoney("btc5m.vol_half_life_min must be > 0"));
        }
        if !(bp.z_threshold.is_finite() && bp.z_threshold >= 0.0) {
            return Err(ConfigError::BadMoney(
                "btc5m.z_threshold must be finite and ≥ 0",
            ));
        }
        if bp.sample_interval_ms == 0 || bp.spot_poll_ms == 0 {
            return Err(ConfigError::BadMoney(
                "btc5m.sample_interval_ms and spot_poll_ms must be > 0",
            ));
        }
        if bp.spot_sources.is_empty() {
            return Err(ConfigError::BadMoney(
                "btc5m.spot_sources must list at least one source",
            ));
        }
        Ok(())
    }
}

/// Canonical `MarketSegment` name for a `[segments].mm_segments` entry, or
/// `None` if unrecognised. Matching is CASE-INSENSITIVE and ignores underscores,
/// so `"LiquidStable"`, `"liquid_stable"`, `"LIQUID_STABLE"`, and `"liquidstable"`
/// all canonicalise to `"LiquidStable"`.
///
/// Kept string-only here so `pm-config` stays decoupled from `pm-registry`'s
/// `MarketSegment` enum (Task 5.1): this crate validates the accepted spellings,
/// and `pm_app::wiring` does the final canonical-name → `MarketSegment` mapping.
/// The accepted spellings are the shared contract between the two crates.
pub fn normalize_segment_name(s: &str) -> Option<&'static str> {
    match s.trim().to_ascii_lowercase().replace('_', "").as_str() {
        "liquidstable" => Some("LiquidStable"),
        "liquid" => Some("Liquid"),
        "illiquid" => Some("Illiquid"),
        _ => None,
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
    fn btc5m_defaults_and_validation() {
        let c = Config::default();
        assert!(!c.strategies.btc5m.enabled);
        assert_eq!(c.btc5m_params.vol_half_life_min, 120.0);

        let c = Config::from_toml_str(
            "[strategies.btc5m]\nenabled = true\ncapital_usd = 50.0\n\n[btc5m]\nz_threshold = 1.5\n"
        ).unwrap();
        assert!(c.strategies.btc5m.enabled);
        assert_eq!(c.strategies.btc5m.capital_usd, 50.0);
        assert_eq!(c.btc5m_params.z_threshold, 1.5);

        let bad = Config::from_toml_str(
            "[capital]\nbankroll_usd = 10.0\n[strategies.btc5m]\nenabled = true\ncapital_usd = 999.0\n"
        );
        assert!(bad.is_err());
    }

    #[test]
    fn btc5m_validation_rejects_bad_bounds() {
        // `[btc5m]` params are validated unconditionally (even with the
        // strategy disabled), so none of these need `[strategies.btc5m]`.

        // vol_half_life_min must be > 0 (zero and negative both rejected).
        assert!(Config::from_toml_str("[btc5m]\nvol_half_life_min = 0.0\n").is_err());
        assert!(Config::from_toml_str("[btc5m]\nvol_half_life_min = -5.0\n").is_err());

        // z_threshold must be finite and >= 0.
        assert!(Config::from_toml_str("[btc5m]\nz_threshold = -1.0\n").is_err());

        // sample_interval_ms must be > 0.
        assert!(Config::from_toml_str("[btc5m]\nsample_interval_ms = 0\n").is_err());

        // spot_poll_ms must be > 0.
        assert!(Config::from_toml_str("[btc5m]\nspot_poll_ms = 0\n").is_err());

        // spot_sources must list at least one source.
        assert!(Config::from_toml_str("[btc5m]\nspot_sources = []\n").is_err());
    }

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

    // ---- Task 5.3: universe scaling knobs ----------------------------------

    #[test]
    fn universe_scaling_knobs_default_and_parse() {
        // Defaults: prioritization OFF, candidate_pool sentinel 0 — the live
        // sync path is unchanged unless the operator opts in.
        let c = Config::default();
        assert!(!c.universe.prioritize_by_liquidity);
        assert_eq!(c.universe.candidate_pool, 0);
        c.validate().unwrap();

        // Parse + round-trip alongside another section; untouched field defaults.
        let c = Config::from_toml_str(
            "[capital]\nbankroll_usd = 5000.0\n[universe]\nmax_markets = 300\nprioritize_by_liquidity = true\ncandidate_pool = 1500\n",
        )
        .unwrap();
        assert!(c.universe.prioritize_by_liquidity);
        assert_eq!(c.universe.max_markets, 300);
        assert_eq!(c.universe.candidate_pool, 1500);
        assert!(c.universe.require_active, "untouched field keeps its default");
        assert!((c.capital.bankroll_usd - 5000.0).abs() < 1e-6);
    }

    #[test]
    fn universe_candidate_pool_zero_and_boundary_allowed() {
        // 0 (= max_markets) is always valid, regardless of max_markets.
        let c = Config::from_toml_str("[universe]\nmax_markets = 1000\ncandidate_pool = 0\n").unwrap();
        assert_eq!(c.universe.candidate_pool, 0);
        // candidate_pool == max_markets is the boundary and is allowed.
        let c =
            Config::from_toml_str("[universe]\nmax_markets = 200\ncandidate_pool = 200\n").unwrap();
        assert_eq!(c.universe.candidate_pool, 200);
        // candidate_pool > max_markets is allowed (the intended scaling case).
        let c =
            Config::from_toml_str("[universe]\nmax_markets = 200\ncandidate_pool = 2000\n").unwrap();
        assert_eq!(c.universe.candidate_pool, 2000);
    }

    #[test]
    fn universe_auto_restart_parses_and_validates() {
        // Default off.
        assert_eq!(Config::default().universe.auto_restart_secs, 0);
        // A sane interval parses + validates.
        let c = Config::from_toml_str("[universe]\nauto_restart_secs = 600\n").unwrap();
        assert_eq!(c.universe.auto_restart_secs, 600);
        c.validate().unwrap();
        // 0 = off is allowed.
        Config::from_toml_str("[universe]\nauto_restart_secs = 0\n")
            .unwrap()
            .validate()
            .unwrap();
        // A too-small non-zero interval is rejected (would re-sync more than trade).
        assert!(Config::from_toml_str("[universe]\nauto_restart_secs = 30\n").is_err());
    }

    #[test]
    fn confluence_parses_and_validates() {
        // Default off.
        assert!(!Config::default().confluence.enabled);
        // A sane enabled block parses + validates.
        let c = Config::from_toml_str(
            "[confluence]\nenabled = true\ntop_traders = 10\nscan_limit = 50\norder_by = \"pnl\"\ntime_period = \"month\"\n",
        )
        .unwrap();
        assert!(c.confluence.enabled);
        assert_eq!(c.confluence.top_traders, 10);
        // scan_limit < top_traders → rejected.
        assert!(
            Config::from_toml_str("[confluence]\nenabled = true\ntop_traders = 20\nscan_limit = 10\n")
                .is_err()
        );
        // unknown order_by / time_period → rejected.
        assert!(Config::from_toml_str("[confluence]\nenabled = true\norder_by = \"sharpe\"\n").is_err());
        assert!(Config::from_toml_str("[confluence]\nenabled = true\ntime_period = \"forever\"\n").is_err());
        // Disabled → confluence values are NOT validated (inert).
        Config::from_toml_str("[confluence]\nenabled = false\norder_by = \"nonsense\"\n").unwrap();
    }

    #[test]
    fn universe_candidate_pool_below_max_markets_is_rejected() {
        // A non-zero pool below max_markets cannot fill the cap → rejected.
        assert!(
            Config::from_toml_str("[universe]\nmax_markets = 200\ncandidate_pool = 100\n").is_err(),
            "candidate_pool < max_markets (non-zero) must be rejected"
        );
        // Default max_markets is 200; a pool of 1 is below it → rejected.
        assert!(Config::from_toml_str("[universe]\ncandidate_pool = 1\n").is_err());
    }

    #[test]
    fn universe_scaling_unknown_field_is_rejected() {
        assert!(Config::from_toml_str("[universe]\nbogus = 1\n").is_err());
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
    fn live_relayer_knobs_parse_and_default() {
        // M6 defaults: relayer OFF, staging-first, no URL override.
        let d = Config::default();
        assert!(!d.live.relayer_enabled, "relayer is OFF by default");
        assert!(d.live.relayer_staging, "staging-first by default");
        assert_eq!(d.live.relayer_url, None);
        d.validate().unwrap();

        // Explicit override parses + round-trips.
        let c = Config::from_toml_str(
            "[live]\nrelayer_enabled = true\nrelayer_staging = false\nrelayer_url = \"https://x\"\n",
        )
        .unwrap();
        assert!(c.live.relayer_enabled);
        assert!(!c.live.relayer_staging);
        assert_eq!(c.live.relayer_url.as_deref(), Some("https://x"));
        // Untouched [live] fields keep their canary defaults.
        assert!((c.live.basket_cap_usd - 10.0).abs() < 1e-9);
        assert_eq!(c.live.confirm_phrase, "I understand this trades real money");
        c.validate().unwrap();
    }

    #[test]
    fn live_relayer_url_if_set_must_be_non_empty() {
        // An explicit empty relayer_url is a silent misconfig → rejected.
        assert!(Config::from_toml_str("[live]\nrelayer_url = \"\"\n").is_err());
        // Enabling the relayer without a URL override is fine (URL derives from
        // the staging flag), and the default (None) validates.
        Config::from_toml_str("[live]\nrelayer_enabled = true\n")
            .unwrap()
            .validate()
            .unwrap();
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
        assert_eq!(c.strategies.mm.max_markets, 60, "default broad market cap");
        assert_eq!(c.strategies.mm.max_per_event, 2, "default per-event de-concentration cap");
        assert_eq!(c.strategies.mm.live_fills_source, "ws", "default fills source is the WS feed");
        c.validate().unwrap();
    }

    #[test]
    fn mm_strategy_section_parses() {
        let c = Config::from_toml_str(
            "[strategies.mm]\nenabled = true\nlive = false\nspread_bps = 300\nquote_refresh_ms = 1000\nmax_quote_usd = 7.5\ninventory_skew_bps = 250\nrebate_bps = 20\ncapital_usd = 100.0\nmax_markets = 8\nmax_per_event = 3\nlive_fills_source = \"rest\"\n",
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
        assert_eq!(c.strategies.mm.max_per_event, 3);
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
        assert_eq!(c.strategies.mm.max_markets, 60, "untouched field stays default");
        assert_eq!(c.strategies.mm.max_per_event, 2, "untouched field stays default");
    }

    #[test]
    fn mm_max_per_event_parses_validates_and_defaults() {
        // Default is the de-concentration cap of 2 markets per event/component.
        assert_eq!(Config::default().strategies.mm.max_per_event, 2);
        // A custom positive cap parses + validates alongside max_markets.
        let c =
            Config::from_toml_str("[strategies.mm]\nmax_markets = 60\nmax_per_event = 4\n").unwrap();
        assert_eq!(c.strategies.mm.max_per_event, 4);
        assert_eq!(c.strategies.mm.max_markets, 60);
        c.validate().unwrap();
        // `0` is the documented "unlimited per event" sentinel and is valid —
        // any usize is accepted, there is no bound.
        let c = Config::from_toml_str("[strategies.mm]\nmax_per_event = 0\n").unwrap();
        assert_eq!(c.strategies.mm.max_per_event, 0);
        c.validate().unwrap();
        // Omitting it keeps the default.
        let c = Config::from_toml_str("[strategies.mm]\nenabled = true\n").unwrap();
        assert_eq!(
            c.strategies.mm.max_per_event, 2,
            "omitted field keeps the default"
        );
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
        // MM routing defaults (Task 5.2): liquid tiers only (never Illiquid),
        // fee-free markets skipped.
        assert_eq!(
            c.segments.mm_segments,
            vec!["LiquidStable".to_string(), "Liquid".to_string()],
            "MM defaults to the liquid tiers, NOT Illiquid"
        );
        assert!(
            c.segments.mm_exclude_fee_free,
            "fee-free markets are excluded from the rebate-driven MM by default"
        );
        c.validate().unwrap();
    }

    #[test]
    fn mm_routing_segments_parse_and_round_trip() {
        // Override mm_segments + mm_exclude_fee_free alongside another section.
        let c = Config::from_toml_str(
            "[capital]\nbankroll_usd = 5000.0\n[segments]\nmm_segments = [\"LiquidStable\"]\nmm_exclude_fee_free = false\n",
        )
        .unwrap();
        assert_eq!(c.segments.mm_segments, vec!["LiquidStable".to_string()]);
        assert!(!c.segments.mm_exclude_fee_free);
        // Untouched threshold fields keep their defaults.
        assert!((c.segments.liquid_min_volume - 10_000.0).abs() < 1e-6);
        c.validate().unwrap();
    }

    #[test]
    fn mm_segment_names_are_case_and_underscore_insensitive() {
        // snake_case, lowercase, and mixed-case spellings all validate.
        let c = Config::from_toml_str(
            "[segments]\nmm_segments = [\"liquid_stable\", \"LIQUID\", \"illiquid\"]\n",
        )
        .unwrap();
        assert_eq!(c.segments.mm_segments.len(), 3);
        c.validate().unwrap();
        // The canonicaliser maps every accepted spelling to one canonical name.
        assert_eq!(normalize_segment_name("liquid_stable"), Some("LiquidStable"));
        assert_eq!(normalize_segment_name("LiquidStable"), Some("LiquidStable"));
        assert_eq!(normalize_segment_name("  liquidstable "), Some("LiquidStable"));
        assert_eq!(normalize_segment_name("LIQUID"), Some("Liquid"));
        assert_eq!(normalize_segment_name("Illiquid"), Some("Illiquid"));
        assert_eq!(normalize_segment_name("bogus"), None);
    }

    #[test]
    fn mm_segments_unknown_name_is_rejected() {
        // An unrecognised segment name fails validation.
        assert!(
            Config::from_toml_str("[segments]\nmm_segments = [\"Liquid\", \"Sparkling\"]\n")
                .is_err(),
            "an unknown segment name must be rejected"
        );
        // A single bad name is enough to reject.
        assert!(Config::from_toml_str("[segments]\nmm_segments = [\"deep\"]\n").is_err());
    }

    #[test]
    fn mm_segments_empty_list_is_allowed() {
        // An empty allow-list is valid (the MM then quotes nothing — inert).
        let c = Config::from_toml_str("[segments]\nmm_segments = []\n").unwrap();
        assert!(c.segments.mm_segments.is_empty());
        c.validate().unwrap();
    }

    #[test]
    fn mm_partial_segments_keeps_other_defaults() {
        // Setting only mm_exclude_fee_free leaves mm_segments at its default.
        let c = Config::from_toml_str("[segments]\nmm_exclude_fee_free = false\n").unwrap();
        assert!(!c.segments.mm_exclude_fee_free);
        assert_eq!(
            c.segments.mm_segments,
            vec!["LiquidStable".to_string(), "Liquid".to_string()],
            "untouched mm_segments stays default"
        );
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

    #[test]
    fn reward_farm_config_parses_and_defaults() {
        let c = Config::from_toml_str(
            "[strategies.mm]\nenabled=true\npolicy=\"reward_farm\"\n\
             [reward_farm]\nrequote_band_ticks=2\nsize_skew_max_ratio=2.0\nsample_interval_ms=60000\n",
        ).unwrap();
        assert_eq!(c.strategies.mm.policy, "reward_farm");
        assert_eq!(c.reward_farm.requote_band_ticks, 2);
        let d = Config::default();
        assert_eq!(d.strategies.mm.policy, "spread_capture");
        assert_eq!(d.reward_farm.size_skew_max_ratio, 2.0);
    }

    #[test]
    fn reward_farm_rejects_bad_policy_and_ratio() {
        let mut c = Config::default();
        c.strategies.mm.policy = "nonsense".into();
        assert!(c.validate().is_err());
        let mut c2 = Config::default();
        c2.reward_farm.size_skew_max_ratio = 0.5; // must be >= 1.0
        assert!(c2.validate().is_err());
        let mut c3 = Config::default();
        c3.reward_farm.size_skew_max_ratio = f64::NAN;
        assert!(c3.validate().is_err());
        let mut c4 = Config::default();
        c4.reward_farm.size_skew_max_ratio = f64::INFINITY;
        assert!(c4.validate().is_err());
    }

    #[test]
    fn reward_farm_phase_a_knobs_parse_and_default() {
        let c = Config::from_toml_str(
            "[reward_farm]\nmicroprice_levels=3\nsignal_window_ms=3000\npull_threshold=0.6\npull_cooldown_ms=5000\nsize_rebalance_pct=0.25\n",
        ).unwrap();
        assert_eq!(c.reward_farm.microprice_levels, 3);
        assert_eq!(c.reward_farm.pull_threshold, 0.6);
        let d = Config::default();
        assert_eq!(d.reward_farm.microprice_levels, 3);
        assert_eq!(d.reward_farm.pull_cooldown_ms, 5000);
    }

    #[test]
    fn reward_farm_phase_a_validation() {
        let mut c = Config::default();
        c.reward_farm.pull_threshold = 1.5; // must be in [0,1]
        assert!(c.validate().is_err());
        let mut c2 = Config::default();
        c2.reward_farm.microprice_levels = 0; // must be >= 1
        assert!(c2.validate().is_err());
    }

    #[test]
    fn reward_farm_phase_b_knobs() {
        let c = Config::from_toml_str("[reward_farm]\nhedging_enabled=true\nmerge_threshold_usd=5.0\n").unwrap();
        assert!(c.reward_farm.hedging_enabled);
        assert_eq!(c.reward_farm.merge_threshold_usd, 5.0);
        let d = Config::default();
        assert!(!d.reward_farm.hedging_enabled);
        assert_eq!(d.reward_farm.merge_threshold_usd, 5.0);
    }

    #[test]
    fn reward_farm_merge_threshold_must_be_finite_nonneg() {
        let mut c = Config::default();
        c.reward_farm.merge_threshold_usd = -1.0;
        assert!(c.validate().is_err());
        let mut c2 = Config::default();
        c2.reward_farm.merge_threshold_usd = f64::NAN;
        assert!(c2.validate().is_err());
    }

    // ---- Task C2: smart-money copy executor ([strategies.copy] + [copy]) ----

    #[test]
    fn copy_strategy_defaults_are_off_and_paper() {
        let c = Config::default();
        // [strategies.copy] — default-OFF, paper-only, conservative slice.
        assert!(!c.strategies.copy.enabled, "copy strategy is OFF by default");
        assert!(!c.strategies.copy.live, "copy strategy is paper by default");
        assert!(
            (c.strategies.copy.capital_usd - 25.0).abs() < 1e-9,
            "default copy slice $25"
        );
        // [copy] — the tuning knobs.
        assert!((c.copy_params.per_position_usd - 5.0).abs() < 1e-9);
        assert_eq!(c.copy_params.max_concurrent_positions, 3);
        assert!((c.copy_params.max_gross_usd - 25.0).abs() < 1e-9);
        assert!((c.copy_params.stop_loss_pct - 0.25).abs() < 1e-9);
        assert!((c.copy_params.max_drift - 0.15).abs() < 1e-9);
        assert_eq!(c.copy_params.reaction_window_secs, 1800);
        assert_eq!(c.copy_params.min_bets, 10);
        assert_eq!(c.copy_params.top_n, 30);
        assert_eq!(c.copy_params.whitelist_refresh_secs, 21_600);
        assert_eq!(c.copy_params.signal_poll_secs, 90);
        assert!(c.copy_params.follow_exit, "follow the trader's exit by default");
        c.validate().unwrap();
    }

    #[test]
    fn copy_disabled_when_sections_omitted() {
        // A config with NEITHER section still parses, with copy disabled and the
        // tuning knobs at their defaults.
        let c = Config::from_toml_str("[capital]\nbankroll_usd = 5000.0\n").unwrap();
        assert!(!c.strategies.copy.enabled);
        assert!(!c.strategies.copy.live);
        assert!((c.copy_params.per_position_usd - 5.0).abs() < 1e-9);
        assert_eq!(c.copy_params.top_n, 30);
        // The empty TOML is all defaults (copy included).
        assert!(!Config::from_toml_str("").unwrap().strategies.copy.enabled);
    }

    #[test]
    fn copy_sections_parse_from_toml() {
        let c = Config::from_toml_str(
            "[strategies.copy]\nenabled = true\nlive = false\ncapital_usd = 50.0\n\
             [copy]\nper_position_usd = 10.0\nmax_concurrent_positions = 5\nmax_gross_usd = 100.0\n\
             stop_loss_pct = 0.3\nmax_drift = 0.2\nreaction_window_secs = 900\nmin_bets = 20\n\
             top_n = 15\nwhitelist_refresh_secs = 3600\nsignal_poll_secs = 30\nfollow_exit = false\n",
        )
        .unwrap();
        assert!(c.strategies.copy.enabled);
        assert!(!c.strategies.copy.live);
        assert!((c.strategies.copy.capital_usd - 50.0).abs() < 1e-9);
        assert!((c.copy_params.per_position_usd - 10.0).abs() < 1e-9);
        assert_eq!(c.copy_params.max_concurrent_positions, 5);
        assert!((c.copy_params.max_gross_usd - 100.0).abs() < 1e-9);
        assert!((c.copy_params.stop_loss_pct - 0.3).abs() < 1e-9);
        assert!((c.copy_params.max_drift - 0.2).abs() < 1e-9);
        assert_eq!(c.copy_params.reaction_window_secs, 900);
        assert_eq!(c.copy_params.min_bets, 20);
        assert_eq!(c.copy_params.top_n, 15);
        assert_eq!(c.copy_params.whitelist_refresh_secs, 3600);
        assert_eq!(c.copy_params.signal_poll_secs, 30);
        assert!(!c.copy_params.follow_exit);
        c.validate().unwrap();
    }

    #[test]
    fn copy_partial_sections_keep_other_defaults() {
        // Overriding a single knob in each section leaves the rest at defaults.
        let c = Config::from_toml_str(
            "[strategies.copy]\nenabled = true\n[copy]\ntop_n = 50\n",
        )
        .unwrap();
        assert!(c.strategies.copy.enabled);
        assert!(!c.strategies.copy.live, "untouched field stays default");
        assert!(
            (c.strategies.copy.capital_usd - 25.0).abs() < 1e-9,
            "untouched field stays default"
        );
        assert_eq!(c.copy_params.top_n, 50);
        assert!(
            (c.copy_params.per_position_usd - 5.0).abs() < 1e-9,
            "untouched field stays default"
        );
        assert!(c.copy_params.follow_exit, "untouched field stays default");
    }

    #[test]
    fn copy_unknown_field_is_rejected() {
        assert!(Config::from_toml_str("[strategies.copy]\nbogus = 1\n").is_err());
        assert!(Config::from_toml_str("[copy]\nbogus = 1\n").is_err());
        // The renamed field is NOT addressable under its Rust name.
        assert!(Config::from_toml_str("[copy_params]\nper_position_usd = 5.0\n").is_err());
    }

    #[test]
    fn copy_capital_carve_out_parses_and_validates() {
        // An enabled copy strategy with a slice within the bankroll round-trips.
        let c = Config::from_toml_str(
            "[capital]\nbankroll_usd = 1000.0\n[strategies.copy]\nenabled = true\ncapital_usd = 250.0\n",
        )
        .unwrap();
        assert!(c.strategies.copy.enabled);
        assert!((c.strategies.copy.capital_usd - 250.0).abs() < 1e-9);
        c.validate().unwrap();
    }

    #[test]
    fn copy_capital_must_be_finite_and_nonnegative() {
        // Negative capital is rejected even when disabled.
        assert!(Config::from_toml_str("[strategies.copy]\ncapital_usd = -1.0\n").is_err());
        let mut c = Config::default();
        c.strategies.copy.capital_usd = f64::NAN;
        assert!(c.validate().is_err());
    }

    #[test]
    fn copy_capital_over_bankroll_rejected_only_when_enabled() {
        // Disabled: an over-bankroll slice is inert → still parses.
        let c = Config::from_toml_str(
            "[capital]\nbankroll_usd = 100.0\nper_market_usd = 50.0\n[strategies.copy]\nenabled = false\ncapital_usd = 500.0\n",
        )
        .unwrap();
        assert!((c.strategies.copy.capital_usd - 500.0).abs() < 1e-9);
        // Enabled: the same over-bankroll slice can't be carved out → rejected.
        assert!(
            Config::from_toml_str(
                "[capital]\nbankroll_usd = 100.0\nper_market_usd = 50.0\n[strategies.copy]\nenabled = true\ncapital_usd = 500.0\n",
            )
            .is_err(),
            "an enabled copy slice above the bankroll must be rejected"
        );
    }

    #[test]
    fn copy_validation_rejects_bad_bounds() {
        // stop_loss_pct must be in (0, 1].
        assert!(Config::from_toml_str("[copy]\nstop_loss_pct = 0.0\n").is_err());
        assert!(Config::from_toml_str("[copy]\nstop_loss_pct = 1.5\n").is_err());
        // max_drift must be in (0, 1].
        assert!(Config::from_toml_str("[copy]\nmax_drift = 0.0\n").is_err());
        assert!(Config::from_toml_str("[copy]\nmax_drift = 2.0\n").is_err());
        // per_position_usd / max_gross_usd must be > 0.
        assert!(Config::from_toml_str("[copy]\nper_position_usd = 0.0\n").is_err());
        assert!(Config::from_toml_str("[copy]\nmax_gross_usd = 0.0\n").is_err());
        // max_concurrent_positions must be ≥ 1.
        assert!(Config::from_toml_str("[copy]\nmax_concurrent_positions = 0\n").is_err());
        // Time/size knobs must be positive.
        assert!(Config::from_toml_str("[copy]\nreaction_window_secs = 0\n").is_err());
        assert!(Config::from_toml_str("[copy]\nsignal_poll_secs = 0\n").is_err());
        assert!(Config::from_toml_str("[copy]\nwhitelist_refresh_secs = 0\n").is_err());
        assert!(Config::from_toml_str("[copy]\ntop_n = 0\n").is_err());
        // The boundary (1.0) is allowed for both fractions.
        Config::from_toml_str("[copy]\nstop_loss_pct = 1.0\nmax_drift = 1.0\n")
            .unwrap()
            .validate()
            .unwrap();
    }

    #[test]
    fn copy_floats_must_be_finite() {
        let mut c = Config::default();
        c.copy_params.per_position_usd = f64::NAN;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.copy_params.max_gross_usd = f64::INFINITY;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.copy_params.stop_loss_pct = f64::NAN;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.copy_params.max_drift = f64::INFINITY;
        assert!(c.validate().is_err());
    }
}
