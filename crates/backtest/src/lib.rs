//! `pm-backtest` — OFFLINE smart-money copy-trading edge analysis. **NO trading.**
//!
//! BT-2 builds the *fetch + cache* pipeline that assembles everything the
//! simulator (BT-4) needs from the public Polymarket **Data API** (via
//! [`pm_ingestion::data_api`]):
//!
//! - the trader **universe** (top PnL leaderboard, month ∪ all-time),
//! - each trader's own **trades** and **closed positions**,
//! - market **resolutions** taken from an INDEPENDENT source — Polymarket
//!   Gamma's resolved `outcomePrices` (FIX-A) — fetched for EVERY market a
//!   trader bought (wins AND losses), yielding a `conditionId →
//!   winning_outcome_index` map. This replaces the earlier circular source
//!   (the copied traders' OWN closed positions): we picked traders by their
//!   wins, so scoring them against their own wins was both biased and
//!   low-coverage. Gamma is independent of any trader and covers far more of
//!   the bought-market population. See [`gamma_resolutions`].
//! - the full trade **tape** for every *candidate* market (a market a trader
//!   BOUGHT into that Gamma also resolved decisively).
//!
//! Every raw request is cached to a directory as JSON; a present cache file is
//! read instead of re-fetching (unless [`FetchParams::refresh`]), so a BT-4 run
//! is reproducible and re-runs entirely offline. The assembled bundle is also
//! written to `fetched.json`.
//!
//! BT-3 adds the pure analytical core (ranking, signals, simulation, metrics)
//! in [`core`]; BT-4 wires it to the fetched data.

pub mod core;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use pm_ingestion::IngestError;
use pm_ingestion::data_api::{
    ClosedPos, DataApiClient, LeaderboardEntry, OrderBy, TimePeriod, Trade, TradeSide, TradesFilter,
};
use pm_registry::gamma::GammaMarket;

use crate::core::{
    ExitMode, Metrics, PRICE_BUCKETS, Ranking, SimParams, SimResult, metrics, price_bucket,
    rank_wallets_oos, signals_after, simulate_signal, trader_records,
};

/// Default size of the leaderboard pull, per `(orderBy, timePeriod)` slice.
pub const DEFAULT_TRADERS: usize = 30;
/// Default cap on the per-trader `/trades?user=` history.
pub const DEFAULT_TRADE_LIMIT: usize = 1000;
/// Default cap on a per-market `/trades?market=` tape.
pub const DEFAULT_TAPE_LIMIT: usize = 2000;
/// Default polite pause between *network* requests (cache hits never sleep).
pub const DEFAULT_THROTTLE_MS: u64 = 200;
/// Default cache directory.
pub const DEFAULT_CACHE_DIR: &str = "./bt-cache";
/// Default Gamma API base. Source of the INDEPENDENT market resolutions
/// (`outcomePrices`) that replace the circular closed-position source (FIX-A).
pub const DEFAULT_GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
/// Gamma `/markets?condition_ids=` ids per request. Kept small so the query
/// string stays well under Gamma's length cap and each response is light
/// (mirrors the `pm-ingestion` confluence fetcher).
const GAMMA_BATCH: usize = 20;

/// Tuning knobs for [`fetch_all`].
#[derive(Debug, Clone)]
pub struct FetchParams {
    /// Leaderboard depth per slice (month ∪ all). The union is de-duped.
    pub n_traders: usize,
    /// Max fills pulled from each trader's `/trades?user=` history.
    pub trade_limit: usize,
    /// Max fills pulled from each candidate market's `/trades?market=` tape.
    pub tape_limit: usize,
    /// Polite pause inserted after each request that actually hit the network.
    pub throttle: Duration,
    /// Bypass the cache: always re-fetch and overwrite the cache files.
    pub refresh: bool,
    /// Gamma API base used for the INDEPENDENT `outcomePrices` resolutions.
    /// Defaults to [`DEFAULT_GAMMA_BASE`].
    pub gamma_base: String,
}

impl Default for FetchParams {
    fn default() -> Self {
        Self {
            n_traders: DEFAULT_TRADERS,
            trade_limit: DEFAULT_TRADE_LIMIT,
            tape_limit: DEFAULT_TAPE_LIMIT,
            throttle: Duration::from_millis(DEFAULT_THROTTLE_MS),
            refresh: false,
            gamma_base: DEFAULT_GAMMA_BASE.to_string(),
        }
    }
}

/// Everything the simulator needs, assembled once and cached. Round-trips
/// through JSON (`fetched.json`) so BT-4 can replay entirely offline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FetchedData {
    /// The de-duped trader universe (leaderboard rows), month-slice first.
    pub traders: Vec<LeaderboardEntry>,
    /// Each trader's own fills, keyed by `proxyWallet`.
    pub trades_by_wallet: HashMap<String, Vec<Trade>>,
    /// Each trader's resolved/closed positions, keyed by `proxyWallet`. Still
    /// fetched and retained (FIX-B builds track records from them), but NO
    /// LONGER the resolution source.
    pub closed_by_wallet: HashMap<String, Vec<ClosedPos>>,
    /// `conditionId → winning_outcome_index`, from the INDEPENDENT Gamma
    /// `outcomePrices` source (FIX-A), fetched over every bought market. A
    /// market absent here was not decisively resolved by Gamma → excluded.
    pub resolutions: HashMap<String, i64>,
    /// Full ascending-by-`timestamp` trade tape per candidate market.
    pub tape_by_market: HashMap<String, Vec<Trade>>,
    /// `conditionId → human title`, for display in BT-4.
    pub titles: HashMap<String, String>,
}

/// Errors raised while fetching/caching. No panics on the fetch path.
#[derive(Debug)]
pub enum BacktestError {
    /// A Data API transport/parse error bubbled up from [`pm_ingestion`].
    Ingest(IngestError),
    /// A cache-directory or cache-file I/O error.
    Io(std::io::Error),
    /// A (de)serialization error on a cache file or the assembled bundle.
    Json(serde_json::Error),
}

impl std::fmt::Display for BacktestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BacktestError::Ingest(e) => write!(f, "data API error: {e}"),
            BacktestError::Io(e) => write!(f, "cache I/O error: {e}"),
            BacktestError::Json(e) => write!(f, "cache JSON error: {e}"),
        }
    }
}

impl std::error::Error for BacktestError {}

impl From<IngestError> for BacktestError {
    fn from(e: IngestError) -> Self {
        BacktestError::Ingest(e)
    }
}

impl From<std::io::Error> for BacktestError {
    fn from(e: std::io::Error) -> Self {
        BacktestError::Io(e)
    }
}

impl From<serde_json::Error> for BacktestError {
    fn from(e: serde_json::Error) -> Self {
        BacktestError::Json(e)
    }
}

// ---------------------------------------------------------------------------
// Pure, I/O-free helpers (unit-tested)
// ---------------------------------------------------------------------------

/// Derive `conditionId → winning_outcome_index` from the union of all traders'
/// closed positions. For a binary market a single [`ClosedPos`] settles it: the
/// held side won (`won()`) ⇒ the winner IS that `outcome_index`; otherwise the
/// *other* side won ⇒ `1 - outcome_index`. Every trader who closed the same
/// binary market agrees, so the map value is independent of fold order.
pub fn fold_resolutions<'a>(
    closed: impl IntoIterator<Item = &'a ClosedPos>,
) -> HashMap<String, i64> {
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

/// Parse a Gamma `/markets` response body (a JSON array of [`GammaMarket`]) into
/// a `conditionId → winning_outcome_index` map, keeping ONLY markets that Gamma
/// resolved DECISIVELY ([`GammaMarket::resolved_winner`] is `Some`) and that
/// carry a non-empty condition id.
///
/// Pure (no I/O) so the batch fetch logic is unit-testable without HTTP. A body
/// that does not parse as a `GammaMarket` array yields an EMPTY map rather than
/// an error — a single malformed/HTML batch response must not poison the run;
/// the markets in that batch are simply treated as unresolved (best-effort,
/// safe direction).
pub fn parse_gamma_resolutions(body: &str) -> HashMap<String, i64> {
    let markets: Vec<GammaMarket> = match serde_json::from_str(body) {
        Ok(m) => m,
        Err(_) => return HashMap::new(),
    };
    let mut out: HashMap<String, i64> = HashMap::new();
    for market in markets {
        if market.condition_id.is_empty() {
            continue;
        }
        if let Some(winner) = market.resolved_winner() {
            out.insert(market.condition_id.clone(), winner);
        }
    }
    out
}

/// The candidate market UNIVERSE before resolution filtering: every
/// `condition_id` that appears as a BUY in ANY trader's own trades (empty ids
/// dropped). Returned sorted (a [`BTreeSet`]) so the Gamma resolution batches
/// are deterministic and the per-batch cache files are stable across runs.
///
/// This is the full set we ask Gamma to resolve (wins AND losses) — the source
/// of FIX-A's much higher coverage versus the old closed-position resolutions.
pub fn bought_condition_ids(trades_by_wallet: &HashMap<String, Vec<Trade>>) -> BTreeSet<String> {
    let mut bought: BTreeSet<String> = BTreeSet::new();
    for trades in trades_by_wallet.values() {
        for trade in trades {
            if trade.side == TradeSide::Buy && !trade.condition_id.is_empty() {
                bought.insert(trade.condition_id.clone());
            }
        }
    }
    bought
}

/// The set of markets worth simulating: those that (a) appear as a BUY in some
/// trader's own trades AND (b) Gamma resolved (present in `resolutions`).
/// Returned sorted (a [`BTreeSet`]) so the heavy tape-fetch loop is deterministic.
pub fn candidate_markets(
    trades_by_wallet: &HashMap<String, Vec<Trade>>,
    resolutions: &HashMap<String, i64>,
) -> BTreeSet<String> {
    let mut candidates: BTreeSet<String> = BTreeSet::new();
    for trades in trades_by_wallet.values() {
        for trade in trades {
            if trade.side == TradeSide::Buy && resolutions.contains_key(&trade.condition_id) {
                candidates.insert(trade.condition_id.clone());
            }
        }
    }
    candidates
}

/// Build the trader universe: month-slice rows first, then all-time rows,
/// de-duped by `proxyWallet` (first occurrence wins, so order is stable).
pub fn dedup_traders(
    month: Vec<LeaderboardEntry>,
    all: Vec<LeaderboardEntry>,
) -> Vec<LeaderboardEntry> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut traders: Vec<LeaderboardEntry> = Vec::new();
    for entry in month.into_iter().chain(all) {
        if seen.insert(entry.proxy_wallet.clone()) {
            traders.push(entry);
        }
    }
    traders
}

/// Collect `conditionId → title` from every trade/closed-position we touched
/// (first non-empty title wins).
fn collect_titles(
    trades_by_wallet: &HashMap<String, Vec<Trade>>,
    tape_by_market: &HashMap<String, Vec<Trade>>,
    closed_by_wallet: &HashMap<String, Vec<ClosedPos>>,
) -> HashMap<String, String> {
    let mut titles: HashMap<String, String> = HashMap::new();
    let trades = trades_by_wallet
        .values()
        .flatten()
        .chain(tape_by_market.values().flatten());
    for trade in trades {
        if !trade.title.is_empty() {
            titles
                .entry(trade.condition_id.clone())
                .or_insert_with(|| trade.title.clone());
        }
    }
    for cp in closed_by_wallet.values().flatten() {
        if !cp.title.is_empty() {
            titles
                .entry(cp.condition_id.clone())
                .or_insert_with(|| cp.title.clone());
        }
    }
    titles
}

/// Build a filesystem-safe cache file name `"<prefix>-<key>.json"`. Wallets and
/// condition ids are `0x…` hex (already safe), but any stray character is
/// folded to `_` defensively.
fn cache_file_name(prefix: &str, key: &str) -> String {
    let safe_key: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{prefix}-{safe_key}.json")
}

// ---------------------------------------------------------------------------
// Cached fetch
// ---------------------------------------------------------------------------

/// Read `cache_dir/file_name` if it exists (and `!refresh`); otherwise run
/// `fetch`, persist the result as pretty JSON, and return it. The returned
/// `bool` is `true` when the network was actually hit (so the caller can
/// throttle only on real requests — a full cache replay never sleeps).
async fn cached<T, Fut, F>(
    cache_dir: &Path,
    file_name: &str,
    refresh: bool,
    fetch: F,
) -> Result<(T, bool), BacktestError>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, IngestError>>,
{
    let path = cache_dir.join(file_name);
    if !refresh && path.exists() {
        let body = std::fs::read_to_string(&path)?;
        let value: T = serde_json::from_str(&body)?;
        return Ok((value, false));
    }
    let value = fetch().await?;
    std::fs::create_dir_all(cache_dir)?;
    let body = serde_json::to_string_pretty(&value)?;
    std::fs::write(&path, body)?;
    Ok((value, true))
}

// ---------------------------------------------------------------------------
// Gamma resolutions (INDEPENDENT source — FIX-A)
// ---------------------------------------------------------------------------

/// GET a Gamma URL, returning the raw response body. Transport/status errors
/// are funnelled through [`IngestError::Http`] so they share the existing
/// [`BacktestError::Ingest`] channel (no new error variant needed).
async fn gamma_get(http: &reqwest::Client, url: &str) -> Result<String, BacktestError> {
    let body = http
        .get(url)
        .send()
        .await
        .map_err(|e| BacktestError::Ingest(IngestError::Http(e.to_string())))?
        .error_for_status()
        .map_err(|e| BacktestError::Ingest(IngestError::Http(e.to_string())))?
        .text()
        .await
        .map_err(|e| BacktestError::Ingest(IngestError::Http(e.to_string())))?;
    Ok(body)
}

/// Deterministic content hash of a batch's condition ids, used as the cache-file
/// key so a batch's cache is keyed by its CONTENT (not a positional index that
/// would go stale the moment the trader set or ordering changes).
fn batch_cache_key(ids: &[String]) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    ids.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Fetch INDEPENDENT market resolutions from Gamma's `outcomePrices`.
///
/// Batches `condition_ids` (≤ [`GAMMA_BATCH`] per request) into
/// `GET {gamma_base}/markets?limit=100&condition_ids=<c>&condition_ids=<c>…`
/// with NO `active`/`closed` filter — we specifically WANT resolved (closed)
/// markets back, the opposite of the live-universe confluence fetcher. Each
/// batch's raw response is parsed via [`parse_gamma_resolutions`] and the
/// decisive winners are merged into one `conditionId → winning_outcome_index`
/// map.
///
/// Each batch response is cached as a readable JSON file under `cache_dir`
/// (`gamma-markets-<content-hash>.json`); a present cache file is read instead
/// of re-fetching unless `refresh`. Only real network requests are throttled
/// (cache hits are free), mirroring [`cached`]. Read-only — places NO orders.
pub async fn gamma_resolutions(
    http: &reqwest::Client,
    gamma_base: &str,
    condition_ids: &[String],
    cache_dir: &Path,
    refresh: bool,
) -> Result<HashMap<String, i64>, BacktestError> {
    let base = gamma_base.trim_end_matches('/');
    let mut resolutions: HashMap<String, i64> = HashMap::new();

    for chunk in condition_ids.chunks(GAMMA_BATCH) {
        // Repeated `&condition_ids=` params; NO active/closed filter so resolved
        // (closed) markets ARE returned. ≤20 ids ⇒ ≤20 markets, well under limit.
        let params: String = chunk
            .iter()
            .map(|c| format!("&condition_ids={c}"))
            .collect();
        let url = format!("{base}/markets?limit=100{params}");

        let file_name = cache_file_name("gamma-markets", &batch_cache_key(chunk));
        let path = cache_dir.join(&file_name);

        // Read-before-fetch (like `cached`), but the cache file is the raw Gamma
        // response array so it stays human-readable and re-derivable.
        let (body, hit_network) = if !refresh && path.exists() {
            (std::fs::read_to_string(&path)?, false)
        } else {
            let body = gamma_get(http, &url).await?;
            std::fs::create_dir_all(cache_dir)?;
            std::fs::write(&path, &body)?;
            (body, true)
        };

        for (condition_id, winner) in parse_gamma_resolutions(&body) {
            resolutions.insert(condition_id, winner);
        }

        // Throttle politely only on real network requests.
        if hit_network {
            sleep(Duration::from_millis(DEFAULT_THROTTLE_MS)).await;
        }
    }

    Ok(resolutions)
}

/// Fetch (or load from cache) everything the simulator needs and assemble it
/// into a [`FetchedData`], also written to `cache_dir/fetched.json`.
///
/// Pipeline: universe → per-trader trades+closed → resolutions → candidate
/// markets → per-market tapes. Network requests are throttled by
/// [`FetchParams::throttle`]; cache hits are free. This is read-only analysis —
/// it places NO orders.
pub async fn fetch_all(
    client: &DataApiClient,
    params: &FetchParams,
    cache_dir: &Path,
) -> Result<FetchedData, BacktestError> {
    std::fs::create_dir_all(cache_dir)?;

    // 1. Universe: top PnL over the last month ∪ all-time, de-duped.
    let (month, month_net) = cached(
        cache_dir,
        "leaderboard-PNL-MONTH.json",
        params.refresh,
        || client.leaderboard(OrderBy::Pnl, TimePeriod::Month, params.n_traders),
    )
    .await?;
    if month_net {
        sleep(params.throttle).await;
    }
    let (all, all_net) = cached(
        cache_dir,
        "leaderboard-PNL-ALL.json",
        params.refresh,
        || client.leaderboard(OrderBy::Pnl, TimePeriod::All, params.n_traders),
    )
    .await?;
    if all_net {
        sleep(params.throttle).await;
    }
    let traders = dedup_traders(month, all);

    // 2. Per trader: their own fills + their resolved track record.
    let mut trades_by_wallet: HashMap<String, Vec<Trade>> = HashMap::new();
    let mut closed_by_wallet: HashMap<String, Vec<ClosedPos>> = HashMap::new();
    for entry in &traders {
        let wallet: &str = &entry.proxy_wallet;

        let (trades, trades_net) = cached(
            cache_dir,
            &cache_file_name("trades-user", wallet),
            params.refresh,
            || client.trades(TradesFilter::User(wallet), params.trade_limit),
        )
        .await?;
        if trades_net {
            sleep(params.throttle).await;
        }

        let (closed, closed_net) = cached(
            cache_dir,
            &cache_file_name("closed", wallet),
            params.refresh,
            || client.closed_positions(wallet),
        )
        .await?;
        if closed_net {
            sleep(params.throttle).await;
        }

        trades_by_wallet.insert(wallet.to_string(), trades);
        closed_by_wallet.insert(wallet.to_string(), closed);
    }

    // 3. Resolutions from the INDEPENDENT Gamma `outcomePrices` source (FIX-A),
    //    fetched over EVERY market a trader BOUGHT (wins AND losses) — not the
    //    circular, low-coverage closed-position source. Closed positions are
    //    still fetched/retained above (FIX-B uses them) but no longer decide
    //    winners. A dedicated keyless HTTP client is built here because the Data
    //    API client's reqwest client is private (and points at a different host).
    let bought: Vec<String> = bought_condition_ids(&trades_by_wallet).into_iter().collect();
    let gamma_http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("pm-arb-bot/1.0")
        .build()
        .map_err(|e| BacktestError::Ingest(IngestError::Http(e.to_string())))?;
    let resolutions =
        gamma_resolutions(&gamma_http, &params.gamma_base, &bought, cache_dir, params.refresh)
            .await?;

    // 4. Candidate markets: BUY-by-a-trader ∩ Gamma-resolved.
    let candidates = candidate_markets(&trades_by_wallet, &resolutions);

    // 5. Tapes: the full ascending trade history of each candidate market.
    //    This is the heavy part — bounded by the candidate count, throttled.
    let mut tape_by_market: HashMap<String, Vec<Trade>> = HashMap::new();
    for condition_id in &candidates {
        let market: &str = condition_id;
        let (mut tape, tape_net) = cached(
            cache_dir,
            &cache_file_name("trades-market", market),
            params.refresh,
            || client.trades(TradesFilter::Market(market), params.tape_limit),
        )
        .await?;
        tape.sort_by_key(|trade| trade.timestamp);
        tape_by_market.insert(condition_id.clone(), tape);
        if tape_net {
            sleep(params.throttle).await;
        }
    }

    let titles = collect_titles(&trades_by_wallet, &tape_by_market, &closed_by_wallet);

    let data = FetchedData {
        traders,
        trades_by_wallet,
        closed_by_wallet,
        resolutions,
        tape_by_market,
        titles,
    };

    // Persist the assembled bundle for a reproducible, offline BT-4 re-run.
    let body = serde_json::to_string_pretty(&data)?;
    std::fs::write(cache_dir.join("fetched.json"), body)?;

    Ok(data)
}

// ---------------------------------------------------------------------------
// BT-4: the parameter-grid runner (pure, I/O-free, unit-tested)
// ---------------------------------------------------------------------------

/// One row of the BT-4 result grid: the [`Metrics`] for a single
/// `(ranking, k, lag, exit, freshness)` cell under one `scope`.
#[derive(Debug, Clone, Serialize)]
pub struct GridResult {
    /// Ranking label ([`Ranking::as_str`]).
    pub ranking: String,
    /// Convergence threshold K (distinct whitelisted wallets).
    pub k: usize,
    /// Detection/execution lag in MINUTES.
    pub lag_min: i64,
    /// Exit-rule label ([`ExitMode::as_str`]).
    pub exit: String,
    /// FRESHNESS filter: max tolerated entry-vs-trigger price drift, or `null`
    /// (no filter). Serialized as a number or null ([`SimParams::max_drift`]).
    pub max_drift: Option<f64>,
    /// `"all"` | `"sports"` | `"nonsports"` | `"px:<bucket>"` (see
    /// [`crate::core::PRICE_BUCKETS`]).
    pub scope: String,
    /// Aggregate statistics for this cell + scope.
    pub metrics: Metrics,
}

/// The parameter grid swept by [`run_grid`]. [`Default`] is the FIX-B spec grid:
/// rankings × Ks × lags × exits × freshness = 3 × 3 × 4 × 2 × 3 = 216 cells.
#[derive(Debug, Clone)]
pub struct GridConfig {
    /// Wallet-selection rankings to sweep.
    pub rankings: Vec<Ranking>,
    /// Convergence thresholds K.
    pub ks: Vec<usize>,
    /// Detection/execution lags, in MINUTES (converted to seconds internally).
    pub lags_min: Vec<i64>,
    /// Exit rules.
    pub exits: Vec<ExitMode>,
    /// FRESHNESS thresholds to sweep: `None` (no filter) and the fractional
    /// entry-vs-trigger drift caps ([`SimParams::max_drift`]).
    pub freshness: Vec<Option<f64>>,
    /// Whitelist size cap (per ranking).
    pub top_n: usize,
    /// Minimum PRE-cutoff resolved-bet sample for the skill rankings
    /// ([`trader_records`]).
    pub min_bets: usize,
    /// Convergence window for [`signals_after`], in seconds (default 24h).
    pub window_secs: i64,
    /// Round-trip fee/slippage fraction subtracted from each gross return.
    pub fee_frac: f64,
    /// OUT-OF-SAMPLE split point (epoch seconds). BUYS strictly BEFORE it build
    /// the trader records that SELECT wallets; BUYS at/after it are the COPY-TEST
    /// signals. Callers MUST set this (e.g. `now − 90 days`); [`Default`] leaves
    /// it at `0` (epoch), which makes the whole history the test set.
    pub cutoff_ts: i64,
}

impl Default for GridConfig {
    fn default() -> Self {
        Self {
            rankings: vec![Ranking::RawLeaderboard, Ranking::TrackRecord, Ranking::EdgePerBet],
            ks: vec![1, 2, 3],
            lags_min: vec![1, 5, 30, 60],
            exits: vec![ExitMode::Resolution, ExitMode::FollowExit],
            freshness: vec![None, Some(0.35), Some(0.15)],
            top_n: 30,
            min_bets: 10,
            window_secs: 86_400,
            fee_frac: 0.0,
            cutoff_ts: 0,
        }
    }
}

/// Run the entire parameter grid over `data`, returning every [`GridResult`].
///
/// Pure and deterministic. This is the FIX-B (OUT-OF-SAMPLE) pipeline: trader
/// SELECTION uses only PRE-`cutoff_ts` history and the COPY TEST uses only
/// POST-`cutoff_ts` signals, so the two are DISJOINT (no survivorship bias).
///
/// For each `(ranking, k, lag, exit, freshness)` cell it emits one row per scope:
/// `"all"`, `"sports"`, `"nonsports"`, then one `"px:<bucket>"` per
/// [`PRICE_BUCKETS`] entry — `3 + PRICE_BUCKETS.len()` rows in a fixed order. The
/// total length is `rankings × ks × lags_min × exits × freshness × (3 + buckets)`
/// in grid order.
///
/// Per cell:
/// 1. `records = trader_records(trades, resolutions, cutoff_ts)` (computed once);
///    `whitelist = rank_wallets_oos(ranking, traders, records, …)`.
/// 2. `sigs = signals_after(whitelist, k, window, cutoff_ts)`; re-sorted ascending
///    by timestamp so [`metrics`]' max-drawdown sees true time order.
/// 3. Each signal with a known resolution AND a tape is simulated with
///    `SimParams { lag_secs: lag_min*60, fee_frac, exit, max_drift }`, in
///    signal-time order (the freshness filter may turn a copy into `Skipped`).
/// 4. Scope split: `"all"` gets every result (so `Skipped` are counted there);
///    `"sports"`/`"nonsports"`/`"px:<bucket>"` get only the `Filled` results,
///    partitioned by the sports flag / entry-price bucket (time order preserved).
pub fn run_grid(data: &FetchedData, cfg: &GridConfig) -> Vec<GridResult> {
    let mut out: Vec<GridResult> = Vec::new();

    // PRE-cutoff records (the OUT-OF-SAMPLE selection set) — computed once.
    let records = trader_records(&data.trades_by_wallet, &data.resolutions, cfg.cutoff_ts);

    for &ranking in &cfg.rankings {
        // Selection ranks on PRE-cutoff records only (never the test trades).
        let whitelist = rank_wallets_oos(ranking, &data.traders, &records, cfg.top_n, cfg.min_bets);

        for &k in &cfg.ks {
            // POST-cutoff signals only (the copy-test set). `signals_after` returns
            // them sorted by (timestamp, …); re-sort by timestamp defensively so
            // the cumulative-equity / max-drawdown always sees true time order.
            let mut sigs =
                signals_after(&whitelist, &data.trades_by_wallet, k, cfg.window_secs, cfg.cutoff_ts);
            sigs.sort_by_key(|s| s.timestamp);

            for &lag_min in &cfg.lags_min {
                for &exit in &cfg.exits {
                    for &max_drift in &cfg.freshness {
                        let p = SimParams {
                            lag_secs: lag_min * 60,
                            fee_frac: cfg.fee_frac,
                            exit,
                            max_drift,
                        };

                        // Simulate each signal that has BOTH a resolution and a
                        // tape, in signal-timestamp order.
                        let mut all: Vec<SimResult> = Vec::with_capacity(sigs.len());
                        for sig in &sigs {
                            let (Some(&winning_outcome), Some(tape)) = (
                                data.resolutions.get(&sig.condition_id),
                                data.tape_by_market.get(&sig.condition_id),
                            ) else {
                                continue;
                            };
                            let title =
                                data.titles.get(&sig.condition_id).map_or("", String::as_str);
                            all.push(simulate_signal(
                                sig,
                                tape,
                                winning_outcome,
                                &data.trades_by_wallet,
                                &p,
                                title,
                            ));
                        }

                        emit_scopes(&mut out, ranking, k, lag_min, exit, max_drift, &all);
                    }
                }
            }
        }
    }

    out
}

/// Partition one cell's `Filled` results by sports flag and entry-price bucket,
/// and push the `"all"` + `"sports"` + `"nonsports"` + `"px:<bucket>"` scope rows
/// (in that fixed order) for the `(ranking, k, lag, exit, max_drift)` cell. Time
/// order is preserved within every partition.
fn emit_scopes(
    out: &mut Vec<GridResult>,
    ranking: Ranking,
    k: usize,
    lag_min: i64,
    exit: ExitMode,
    max_drift: Option<f64>,
    all: &[SimResult],
) {
    let mut sports: Vec<SimResult> = Vec::new();
    let mut nonsports: Vec<SimResult> = Vec::new();
    let mut buckets: Vec<(&'static str, Vec<SimResult>)> =
        PRICE_BUCKETS.iter().map(|&b| (b, Vec::new())).collect();

    for r in all {
        if let SimResult::Filled { sports: is_sp, entry_px, .. } = r {
            if *is_sp {
                sports.push(r.clone());
            } else {
                nonsports.push(r.clone());
            }
            // `price_bucket` always returns a `PRICE_BUCKETS` member, so the
            // lookup never misses (no unwrap/panic).
            let label = price_bucket(*entry_px);
            if let Some(slot) = buckets.iter_mut().find(|(b, _)| *b == label) {
                slot.1.push(r.clone());
            }
        }
    }

    let push = |out: &mut Vec<GridResult>, scope: String, results: &[SimResult]| {
        out.push(GridResult {
            ranking: ranking.as_str().to_string(),
            k,
            lag_min,
            exit: exit.as_str().to_string(),
            max_drift,
            scope,
            metrics: metrics(results),
        });
    };

    push(out, "all".to_string(), all);
    push(out, "sports".to_string(), &sports);
    push(out, "nonsports".to_string(), &nonsports);
    for (label, results) in &buckets {
        push(out, format!("px:{label}"), results);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    use super::*;
    use pm_ingestion::data_api::{parse_closed_positions, parse_leaderboard, parse_trades};

    /// Two traders' closed positions covering the same markets from BOTH sides:
    /// a winner (`curPrice≈1`) and the matching loser (`curPrice≈0`) must agree
    /// on the winning outcome index, and a held-loser flips the index.
    #[test]
    fn fold_resolutions_winner_and_loser_agree() {
        let closed = parse_closed_positions(
            r#"[
                {"conditionId":"0xAAA","asset":"1","avgPrice":0.4,"curPrice":1.0,
                 "cashPnl":10.0,"size":100.0,"outcomeIndex":0,"title":"Market A"},
                {"conditionId":"0xBBB","asset":"2","avgPrice":0.6,"curPrice":0.0,
                 "cashPnl":-60.0,"size":100.0,"outcomeIndex":0,"title":"Market B"}
            ]"#,
        )
        .unwrap();
        // Same markets seen by a second trader from the opposite side.
        let closed2 = parse_closed_positions(
            r#"[
                {"conditionId":"0xAAA","asset":"3","avgPrice":0.5,"curPrice":0.0,
                 "cashPnl":-50.0,"size":100.0,"outcomeIndex":1,"title":"Market A"},
                {"conditionId":"0xBBB","asset":"4","avgPrice":0.5,"curPrice":1.0,
                 "cashPnl":50.0,"size":100.0,"outcomeIndex":1,"title":"Market B"}
            ]"#,
        )
        .unwrap();

        let res = fold_resolutions(closed.iter().chain(closed2.iter()));
        assert_eq!(res.len(), 2);
        // 0xAAA: trader1 HELD outcome 0 and WON ⇒ winner = 0. trader2 HELD
        // outcome 1 and LOST ⇒ winner = 1 - 1 = 0. They agree.
        assert_eq!(res.get("0xAAA"), Some(&0));
        // 0xBBB: trader1 HELD outcome 0 and LOST ⇒ winner = 1 - 0 = 1. trader2
        // HELD outcome 1 and WON ⇒ winner = 1. They agree.
        assert_eq!(res.get("0xBBB"), Some(&1));
    }

    #[test]
    fn fold_resolutions_empty_is_empty() {
        let res = fold_resolutions(std::iter::empty());
        assert!(res.is_empty());
    }

    /// Candidate = BOUGHT by a trader AND resolved. A SELL-only market, an
    /// unresolved market, and a resolved-but-never-traded market are all
    /// excluded; a bought+resolved market is included exactly once.
    #[test]
    fn candidate_markets_requires_buy_and_resolution() {
        let trades = parse_trades(
            r#"[
                {"proxyWallet":"0xW","side":"BUY","asset":"a","conditionId":"0xBUY_RESOLVED",
                 "size":10.0,"price":0.5,"timestamp":100,"outcomeIndex":0,"title":"T1","slug":"s1"},
                {"proxyWallet":"0xW","side":"BUY","asset":"a","conditionId":"0xBUY_RESOLVED",
                 "size":5.0,"price":0.5,"timestamp":150,"outcomeIndex":0,"title":"T1","slug":"s1"},
                {"proxyWallet":"0xW","side":"SELL","asset":"b","conditionId":"0xSELL_ONLY",
                 "size":10.0,"price":0.5,"timestamp":200,"outcomeIndex":0,"title":"T2","slug":"s2"},
                {"proxyWallet":"0xW","side":"BUY","asset":"c","conditionId":"0xUNRESOLVED",
                 "size":10.0,"price":0.5,"timestamp":300,"outcomeIndex":0,"title":"T3","slug":"s3"}
            ]"#,
        )
        .unwrap();
        let mut trades_by_wallet: HashMap<String, Vec<Trade>> = HashMap::new();
        trades_by_wallet.insert("0xW".to_string(), trades);

        let mut resolutions: HashMap<String, i64> = HashMap::new();
        resolutions.insert("0xBUY_RESOLVED".to_string(), 0);
        resolutions.insert("0xSELL_ONLY".to_string(), 1);
        resolutions.insert("0xRESOLVED_UNTRADED".to_string(), 0);

        let candidates = candidate_markets(&trades_by_wallet, &resolutions);
        // Only the BOUGHT-and-resolved market qualifies (and only once).
        assert_eq!(candidates.len(), 1);
        assert!(candidates.contains("0xBUY_RESOLVED"));
        assert!(!candidates.contains("0xSELL_ONLY"));
        assert!(!candidates.contains("0xUNRESOLVED"));
        assert!(!candidates.contains("0xRESOLVED_UNTRADED"));
    }

    /// The candidate universe (pre-resolution) is the DISTINCT set of markets a
    /// trader BOUGHT — SELL-only markets and empty ids are excluded, duplicates
    /// collapse, and the result is sorted (deterministic Gamma batch order).
    #[test]
    fn bought_condition_ids_collects_distinct_buys_only() {
        let trades = parse_trades(
            r#"[
                {"proxyWallet":"0xW","side":"BUY","asset":"a","conditionId":"0xB2",
                 "size":10.0,"price":0.5,"timestamp":100,"outcomeIndex":0,"title":"T","slug":"s"},
                {"proxyWallet":"0xW","side":"BUY","asset":"a","conditionId":"0xB2",
                 "size":5.0,"price":0.5,"timestamp":150,"outcomeIndex":0,"title":"T","slug":"s"},
                {"proxyWallet":"0xW","side":"SELL","asset":"b","conditionId":"0xSELL",
                 "size":10.0,"price":0.5,"timestamp":200,"outcomeIndex":0,"title":"T","slug":"s"},
                {"proxyWallet":"0xW","side":"BUY","asset":"c","conditionId":"0xB1",
                 "size":10.0,"price":0.5,"timestamp":300,"outcomeIndex":0,"title":"T","slug":"s"},
                {"proxyWallet":"0xW","side":"BUY","asset":"d","conditionId":"",
                 "size":10.0,"price":0.5,"timestamp":400,"outcomeIndex":0,"title":"T","slug":"s"}
            ]"#,
        )
        .unwrap();
        let mut trades_by_wallet: HashMap<String, Vec<Trade>> = HashMap::new();
        trades_by_wallet.insert("0xW".to_string(), trades);

        let bought = bought_condition_ids(&trades_by_wallet);
        let v: Vec<&str> = bought.iter().map(String::as_str).collect();
        // Sorted, distinct, BUY-only, no empty id.
        assert_eq!(v, vec!["0xB1", "0xB2"]);
    }

    /// FIX-A parse: a Gamma `/markets` body → only the DECISIVELY resolved
    /// winners. An unresolved (`closed=false`) market, an ambiguous 0.5/0.5
    /// split, and an empty condition id are all dropped; outcome-0 and outcome-1
    /// winners map to the right index.
    #[test]
    fn parse_gamma_resolutions_keeps_only_decisive() {
        let body = r#"[
            {"conditionId":"0xWIN0","closed":true,"outcomePrices":"[\"1\", \"0\"]"},
            {"conditionId":"0xWIN1","closed":true,"outcomePrices":"[\"0\", \"1\"]"},
            {"conditionId":"0xLIVE","closed":false,"outcomePrices":"[\"1\", \"0\"]"},
            {"conditionId":"0xAMB","closed":true,"outcomePrices":"[\"0.5\", \"0.5\"]"},
            {"conditionId":"","closed":true,"outcomePrices":"[\"1\", \"0\"]"}
        ]"#;
        let res = parse_gamma_resolutions(body);
        assert_eq!(res.len(), 2);
        assert_eq!(res.get("0xWIN0"), Some(&0));
        assert_eq!(res.get("0xWIN1"), Some(&1));
        assert!(!res.contains_key("0xLIVE"));
        assert!(!res.contains_key("0xAMB"));
        assert!(!res.contains_key(""));
    }

    /// A non-array / error / empty body must never panic — it just resolves
    /// nothing (best-effort: a poisoned batch leaves its markets unresolved).
    #[test]
    fn parse_gamma_resolutions_tolerates_malformed_body() {
        assert!(parse_gamma_resolutions("not json").is_empty());
        assert!(parse_gamma_resolutions(r#"{"error":"rate limited"}"#).is_empty());
        assert!(parse_gamma_resolutions("[]").is_empty());
    }

    /// `gamma_resolutions` reads a pre-seeded per-batch cache file entirely from
    /// disk (no network): it proves the content-addressed cache-file name
    /// derivation AND that the cached body flows through `parse_gamma_resolutions`
    /// — only the decisive winner survives.
    #[tokio::test]
    async fn gamma_resolutions_reads_cache_without_network() {
        let dir = tempfile::tempdir().unwrap();
        let ids = vec!["0xWIN0".to_string(), "0xAMB".to_string()];

        // Pre-seed the EXACT cache file the fetcher will look for (one batch).
        let body = r#"[
            {"conditionId":"0xWIN0","closed":true,"outcomePrices":"[\"0\", \"1\"]"},
            {"conditionId":"0xAMB","closed":true,"outcomePrices":"[\"0.5\", \"0.5\"]"}
        ]"#;
        let file_name = cache_file_name("gamma-markets", &batch_cache_key(&ids));
        std::fs::write(dir.path().join(&file_name), body).unwrap();

        // The client is required by the signature but must NOT be hit; the base
        // is unroutable so any accidental network attempt would error loudly.
        let http = reqwest::Client::new();
        let res = gamma_resolutions(&http, "http://127.0.0.1:0", &ids, dir.path(), false)
            .await
            .unwrap();

        assert_eq!(res.len(), 1, "only the decisive market resolves");
        assert_eq!(res.get("0xWIN0"), Some(&1));
        assert!(!res.contains_key("0xAMB"));
    }

    #[test]
    fn dedup_traders_keeps_first_occurrence_in_order() {
        let month = parse_leaderboard(
            r#"[
                {"proxyWallet":"0xA","userName":"alice","vol":1.0,"pnl":9.0},
                {"proxyWallet":"0xB","userName":"bob","vol":1.0,"pnl":8.0}
            ]"#,
        )
        .unwrap();
        let all = parse_leaderboard(
            r#"[
                {"proxyWallet":"0xB","userName":"bob","vol":1.0,"pnl":8.0},
                {"proxyWallet":"0xC","userName":"carol","vol":1.0,"pnl":7.0}
            ]"#,
        )
        .unwrap();
        let traders = dedup_traders(month, all);
        let wallets: Vec<&str> = traders.iter().map(|t| t.proxy_wallet.as_str()).collect();
        assert_eq!(wallets, vec!["0xA", "0xB", "0xC"]);
    }

    /// `FetchedData` (and therefore the `Trade`/`ClosedPos`/`LeaderboardEntry`
    /// it holds, incl. the custom-serialized `TradeSide`) round-trips through
    /// JSON unchanged — the property `fetched.json` relies on.
    #[test]
    fn fetched_data_round_trips_through_json() {
        let trades = parse_trades(
            r#"[
                {"proxyWallet":"0xW","side":"BUY","asset":"a","conditionId":"0xM",
                 "size":10.0,"price":0.5,"timestamp":100,"outcomeIndex":0,
                 "title":"Market M","slug":"market-m"},
                {"proxyWallet":"0xW","side":"SELL","asset":"a","conditionId":"0xM",
                 "size":4.0,"price":0.7,"timestamp":250,"outcomeIndex":0,
                 "title":"Market M","slug":"market-m"}
            ]"#,
        )
        .unwrap();
        let closed = parse_closed_positions(
            r#"[
                {"conditionId":"0xM","asset":"a","avgPrice":0.5,"curPrice":1.0,
                 "cashPnl":2.0,"size":10.0,"outcomeIndex":0,"title":"Market M"}
            ]"#,
        )
        .unwrap();
        let traders = parse_leaderboard(
            r#"[{"proxyWallet":"0xW","userName":"whale","vol":5.0,"pnl":3.0}]"#,
        )
        .unwrap();

        let mut trades_by_wallet = HashMap::new();
        trades_by_wallet.insert("0xW".to_string(), trades.clone());
        let mut closed_by_wallet = HashMap::new();
        closed_by_wallet.insert("0xW".to_string(), closed);
        let resolutions = fold_resolutions(closed_by_wallet.values().flatten());
        let mut tape_by_market = HashMap::new();
        tape_by_market.insert("0xM".to_string(), trades);
        let titles = collect_titles(&trades_by_wallet, &tape_by_market, &closed_by_wallet);

        let data = FetchedData {
            traders,
            trades_by_wallet,
            closed_by_wallet,
            resolutions,
            tape_by_market,
            titles,
        };

        // Round-trip and compare the canonical JSON value (the structs do not
        // derive PartialEq, so compare via serde_json::Value).
        let value = serde_json::to_value(&data).unwrap();
        let back: FetchedData = serde_json::from_value(value.clone()).unwrap();
        let value_again = serde_json::to_value(&back).unwrap();
        assert_eq!(value, value_again);

        // Spot-check the round-tripped content survived, incl. TradeSide.
        assert_eq!(back.resolutions.get("0xM"), Some(&0));
        assert_eq!(back.titles.get("0xM").map(String::as_str), Some("Market M"));
        let tape = back.tape_by_market.get("0xM").unwrap();
        assert_eq!(tape[0].side, TradeSide::Buy);
        assert_eq!(tape[1].side, TradeSide::Sell);
    }

    #[test]
    fn cache_file_name_is_filesystem_safe() {
        assert_eq!(
            cache_file_name("trades-user", "0xAbC123"),
            "trades-user-0xAbC123.json"
        );
        // Defensive folding of stray characters.
        assert_eq!(cache_file_name("closed", "a/b c"), "closed-a_b_c.json");
    }

    // ===================== run_grid (BT-4) =====================

    fn lb_entry(wallet: &str) -> LeaderboardEntry {
        LeaderboardEntry {
            proxy_wallet: wallet.to_string(),
            user_name: String::new(),
            pnl: 0.0,
            vol: 0.0,
        }
    }

    fn buy(wallet: &str, cid: &str, price: f64, ts: i64) -> Trade {
        Trade {
            proxy_wallet: wallet.to_string(),
            condition_id: cid.to_string(),
            asset: String::new(),
            side: TradeSide::Buy,
            size: 10.0,
            price,
            timestamp: ts,
            outcome_index: 0,
            title: String::new(),
            slug: String::new(),
        }
    }

    /// Locate one grid cell + scope (helper for the hand-computed assertions).
    fn cell<'a>(
        results: &'a [GridResult],
        ranking: &str,
        k: usize,
        lag_min: i64,
        exit: &str,
        max_drift: Option<f64>,
        scope: &str,
    ) -> &'a GridResult {
        results
            .iter()
            .find(|r| {
                r.ranking == ranking
                    && r.k == k
                    && r.lag_min == lag_min
                    && r.exit == exit
                    && r.max_drift == max_drift
                    && r.scope == scope
            })
            .expect("grid cell present")
    }

    /// `run_grid` over a small synthetic `FetchedData` with an OUT-OF-SAMPLE
    /// cutoff: assert the grid cardinality (incl. the freshness dimension and the
    /// 3 + price-bucket scopes), that PRE-cutoff BUYS do NOT become signals (the
    /// trust fix), a hand-computable cell, and that the freshness filter skips a
    /// chased copy.
    #[test]
    fn run_grid_oos_split_freshness_and_known_cell() {
        use crate::core::{ExitMode, PRICE_BUCKETS, Ranking};

        const CUTOFF: i64 = 1_000_000;

        let traders = vec![lb_entry("0xA"), lb_entry("0xB")];

        // PRE-cutoff SELECTION buys (resolved winners) so the skill rankings pick
        // both wallets; plus a PRE-cutoff buy in 0xM1 that MUST be excluded from
        // the POST-cutoff signal set. POST-cutoff buys in 0xM1/0xM2 are the test.
        let mut trades_by_wallet: HashMap<String, Vec<Trade>> = HashMap::new();
        trades_by_wallet.insert(
            "0xA".to_string(),
            vec![
                buy("0xA", "0xH1", 0.4, 100),
                buy("0xA", "0xH2", 0.4, 200),
                buy("0xA", "0xM1", 0.20, 500), // PRE-cutoff M1 buy -> excluded from signals
                buy("0xA", "0xM1", 0.45, CUTOFF), // POST (>= cutoff): triggers M1
                buy("0xA", "0xM2", 0.35, 1_001_000), // POST: triggers M2
            ],
        );
        trades_by_wallet.insert(
            "0xB".to_string(),
            vec![
                buy("0xB", "0xH3", 0.4, 100),
                buy("0xB", "0xH4", 0.4, 200),
                buy("0xB", "0xM1", 0.46, 1_000_100), // POST
                buy("0xB", "0xM2", 0.36, 1_001_100), // POST
            ],
        );

        // closed positions are no longer consulted by run_grid (OOS uses trades).
        let closed_by_wallet: HashMap<String, Vec<ClosedPos>> = HashMap::new();

        let mut resolutions: HashMap<String, i64> = HashMap::new();
        for cid in ["0xH1", "0xH2", "0xH3", "0xH4", "0xM1", "0xM2"] {
            resolutions.insert(cid.to_string(), 0); // every market resolves to #0
        }

        // Tapes: first trade at/after the lag-1min entry (t+60) is the entry px.
        let mut tape_by_market: HashMap<String, Vec<Trade>> = HashMap::new();
        tape_by_market.insert(
            "0xM1".to_string(),
            vec![
                buy("0xMM", "0xM1", 0.45, CUTOFF),
                buy("0xMM", "0xM1", 0.50, CUTOFF + 60),
                buy("0xMM", "0xM1", 0.55, CUTOFF + 200),
            ],
        );
        tape_by_market.insert(
            "0xM2".to_string(),
            vec![
                buy("0xMM", "0xM2", 0.30, 1_001_000),
                buy("0xMM", "0xM2", 0.40, 1_001_060),
                buy("0xMM", "0xM2", 0.50, 1_001_200),
            ],
        );

        let mut titles: HashMap<String, String> = HashMap::new();
        titles.insert("0xM1".to_string(), "Will the Fed cut rates?".to_string());
        titles.insert("0xM2".to_string(), "Lakers vs Celtics tonight".to_string());

        let data = FetchedData {
            traders,
            trades_by_wallet,
            closed_by_wallet,
            resolutions,
            tape_by_market,
            titles,
        };

        let cfg = GridConfig {
            rankings: vec![Ranking::RawLeaderboard, Ranking::TrackRecord, Ranking::EdgePerBet],
            ks: vec![1, 2],
            lags_min: vec![1, 5],
            exits: vec![ExitMode::Resolution, ExitMode::FollowExit],
            freshness: vec![None, Some(0.13)],
            top_n: 10,
            min_bets: 2,
            window_secs: 86_400,
            fee_frac: 0.0,
            cutoff_ts: CUTOFF,
        };

        let results = run_grid(&data, &cfg);

        // ---- cardinality: rankings × ks × lags × exits × freshness × scopes ----
        let n_scopes = 3 + PRICE_BUCKETS.len();
        let expected = cfg.rankings.len()
            * cfg.ks.len()
            * cfg.lags_min.len()
            * cfg.exits.len()
            * cfg.freshness.len()
            * n_scopes;
        assert_eq!(results.len(), expected);
        assert_eq!(expected, 3 * 2 * 2 * 2 * 2 * 8);

        // Each cell emits its scope rows in a fixed all→sports→nonsports→px order.
        for chunk in results.chunks(n_scopes) {
            assert_eq!(chunk[0].scope, "all");
            assert_eq!(chunk[1].scope, "sports");
            assert_eq!(chunk[2].scope, "nonsports");
            for (i, b) in PRICE_BUCKETS.iter().enumerate() {
                assert_eq!(chunk[3 + i].scope, format!("px:{b}"));
            }
        }

        // ---- hand-computed cell: RawLeaderboard, k=1, lag=1min, Resolution, NO
        //      freshness. POST-cutoff k=1 signals: M1 @CUTOFF (trigger 0.45),
        //      M2 @1_001_000 (trigger 0.35). The PRE-cutoff M1 buy @500 is NOT a
        //      signal (the OOS trust fix) — else M1's entry/return would differ.
        //      lag 60s → entries M1=0.50, M2=0.40; winner #0 → exit 1.0:
        //        ret(M1) = (1-0.50)/0.50 = 1.0  (non-sports)
        //        ret(M2) = (1-0.40)/0.40 = 1.5  (sports)
        //      all = [1.0, 1.5]: mean 1.25, total 2.5, hit 1.0, sharpe 5.0, DD 0.
        let all = cell(&results, "RawLeaderboard", 1, 1, "Resolution", None, "all");
        assert_eq!(all.metrics.n, 2, "exactly the two POST-cutoff signals filled");
        assert_eq!(all.metrics.skipped, 0);
        assert!((all.metrics.mean_ret - 1.25).abs() < 1e-12);
        assert!((all.metrics.total_ret - 2.5).abs() < 1e-12);
        assert!((all.metrics.hit_rate - 1.0).abs() < 1e-12);
        assert!((all.metrics.sharpe - 5.0).abs() < 1e-9);
        assert!((all.metrics.max_drawdown - 0.0).abs() < 1e-12);

        let sports = cell(&results, "RawLeaderboard", 1, 1, "Resolution", None, "sports");
        assert_eq!(sports.metrics.n, 1);
        assert!((sports.metrics.total_ret - 1.5).abs() < 1e-12);
        let nonsports = cell(&results, "RawLeaderboard", 1, 1, "Resolution", None, "nonsports");
        assert_eq!(nonsports.metrics.n, 1);
        assert!((nonsports.metrics.total_ret - 1.0).abs() < 1e-12);

        // ---- price-bucket scope: both entries (0.50, 0.40) fall in 30-70. ----
        let px = cell(&results, "RawLeaderboard", 1, 1, "Resolution", None, "px:30-70");
        assert_eq!(px.metrics.n, 2);
        assert!((px.metrics.total_ret - 2.5).abs() < 1e-12);
        let px_low = cell(&results, "RawLeaderboard", 1, 1, "Resolution", None, "px:lt10");
        assert_eq!(px_low.metrics.n, 0);

        // ---- freshness cell: max_drift=0.13. M1 drift |0.50-0.45|/0.45=0.111 OK;
        //      M2 drift |0.40-0.35|/0.35=0.143 > 0.13 -> chased -> Skipped. ----
        let fresh = cell(&results, "RawLeaderboard", 1, 1, "Resolution", Some(0.13), "all");
        assert_eq!(fresh.metrics.n, 1, "M2 skipped as too far chased");
        assert_eq!(fresh.metrics.skipped, 1);
        assert!((fresh.metrics.total_ret - 1.0).abs() < 1e-12);
        let fresh_ns = cell(&results, "RawLeaderboard", 1, 1, "Resolution", Some(0.13), "nonsports");
        assert_eq!(fresh_ns.metrics.n, 1); // only M1 (non-sports) survives
        let fresh_sp = cell(&results, "RawLeaderboard", 1, 1, "Resolution", Some(0.13), "sports");
        assert_eq!(fresh_sp.metrics.n, 0);
    }
}
