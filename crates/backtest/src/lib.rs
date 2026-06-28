//! `pm-backtest` — OFFLINE smart-money copy-trading edge analysis. **NO trading.**
//!
//! BT-2 builds the *fetch + cache* pipeline that assembles everything the
//! simulator (BT-4) needs from the public Polymarket **Data API** (via
//! [`pm_ingestion::data_api`]):
//!
//! - the trader **universe** (top PnL leaderboard, month ∪ all-time),
//! - each trader's own **trades** and **closed positions**,
//! - market **resolutions** derived *purely from the traders' own closed
//!   positions* (no Gamma): for a binary market a single [`ClosedPos`] reveals
//!   the winner, so we fold the union of everyone's closed positions into a
//!   `conditionId → winning_outcome_index` map,
//! - the full trade **tape** for every *candidate* market (a market a trader
//!   BOUGHT into that we also have a resolution for).
//!
//! Every raw request is cached to a directory as JSON; a present cache file is
//! read instead of re-fetching (unless [`FetchParams::refresh`]), so a BT-4 run
//! is reproducible and re-runs entirely offline. The assembled bundle is also
//! written to `fetched.json`.

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
}

impl Default for FetchParams {
    fn default() -> Self {
        Self {
            n_traders: DEFAULT_TRADERS,
            trade_limit: DEFAULT_TRADE_LIMIT,
            tape_limit: DEFAULT_TAPE_LIMIT,
            throttle: Duration::from_millis(DEFAULT_THROTTLE_MS),
            refresh: false,
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
    /// Each trader's resolved/closed positions, keyed by `proxyWallet`.
    pub closed_by_wallet: HashMap<String, Vec<ClosedPos>>,
    /// `conditionId → winning_outcome_index`, derived from closed positions.
    /// A market absent here is UNRESOLVED among our traders → excluded.
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

/// The set of markets worth simulating: those that (a) appear as a BUY in some
/// trader's own trades AND (b) we have a resolution for. Returned sorted (a
/// [`BTreeSet`]) so the heavy tape-fetch loop is deterministic.
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

    // 3. Resolutions from the union of all closed positions (no Gamma).
    let resolutions = fold_resolutions(closed_by_wallet.values().flatten());

    // 4. Candidate markets: BUY-by-a-trader ∩ resolved.
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
}
