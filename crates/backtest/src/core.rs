//! Pure analytical core of the smart-money copy-trading backtest (BT-3).
//!
//! Everything here is **I/O-free and deterministic**: pure functions over the
//! data [`crate::FetchedData`] assembled by BT-2. BT-4 just loops over the
//! parameter grid (ranking × K × lag × exit) and aggregates these results.
//!
//! The pipeline modelled is:
//! 1. [`rank_wallets`] — pick a WHITELIST of wallets to follow under a ranking.
//! 2. [`signals`] — when ≥K whitelisted wallets BUY the same (market, outcome)
//!    inside a time window, emit one follow signal.
//! 3. [`simulate_signal`] — copy that signal with a detection/execution lag and
//!    score the trade (entry off the tape, exit at resolution or on the leaders'
//!    own exit).
//! 4. [`metrics`] — aggregate a batch of simulated trades.
//!
//! ## Binary price normalization (used everywhere)
//! All markets are BINARY (`outcome_index ∈ {0, 1}`). The price to take outcome
//! `O` implied by any tape trade is the trade's price if it traded `O`, else the
//! complement `1 - price`. See [`normalize_price`]. This single trick converts a
//! tape (a stream of trades on either side) into a price series for *our* side.

use std::collections::{HashMap, HashSet};

use pm_ingestion::data_api::{ClosedPos, LeaderboardEntry, Trade, TradeSide};

// ---------------------------------------------------------------------------
// 1. Ranking — the spectrum under test
// ---------------------------------------------------------------------------

/// The wallet-selection spectrum: from the raw PnL leaderboard (size-driven
/// baseline) through consistency (track record) to true price edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ranking {
    /// Trust the leaderboard as-is (already PnL-sorted). Baseline.
    RawLeaderboard,
    /// Rank by a sample-aware win rate over the wallet's resolved positions.
    TrackRecord,
    /// Rank by realized edge vs. entry price (`won − avg_price`) per bet.
    EdgePerBet,
}

/// Return the WHITELIST of wallets to follow under `ranking`, given the trader
/// universe (`traders`) and their closed positions (`closed`). `min_bets`
/// filters luck by requiring a minimum resolved-position sample.
///
/// - [`Ranking::RawLeaderboard`]: `traders` (already PnL-sorted) truncated to
///   `top_n`. `closed`/`min_bets` are ignored.
/// - [`Ranking::TrackRecord`]: keep wallets with `≥ min_bets` resolved
///   positions, rank by the Wilson lower bound of their win rate (see
///   [`wilson_lower_bound`]), break ties by realized PnL then wallet, take
///   `top_n`.
/// - [`Ranking::EdgePerBet`]: keep wallets with `≥ min_bets` resolved positions
///   AND a strictly positive mean edge (`mean(won − avg_price)`), rank by mean
///   edge (then wallet), take `top_n`.
///
/// Duplicate wallets in `traders` are de-duped (first occurrence wins).
pub fn rank_wallets(
    ranking: Ranking,
    traders: &[LeaderboardEntry],
    closed: &HashMap<String, Vec<ClosedPos>>,
    top_n: usize,
    min_bets: usize,
) -> Vec<String> {
    match ranking {
        Ranking::RawLeaderboard => {
            let mut seen: HashSet<&str> = HashSet::new();
            traders
                .iter()
                .filter(|t| seen.insert(t.proxy_wallet.as_str()))
                .map(|t| t.proxy_wallet.clone())
                .take(top_n)
                .collect()
        }
        Ranking::TrackRecord => {
            let mut seen: HashSet<&str> = HashSet::new();
            // (wallet, wilson_score, realized_pnl)
            let mut scored: Vec<(String, f64, f64)> = Vec::new();
            for t in traders {
                if !seen.insert(t.proxy_wallet.as_str()) {
                    continue;
                }
                let Some(positions) = closed.get(&t.proxy_wallet) else {
                    continue;
                };
                let total = positions.len();
                if total < min_bets {
                    continue;
                }
                let wins = positions.iter().filter(|cp| cp.won()).count();
                let realized: f64 = positions.iter().map(|cp| cp.cash_pnl).sum();
                scored.push((t.proxy_wallet.clone(), wilson_lower_bound(wins, total), realized));
            }
            scored.sort_by(|a, b| {
                b.1.total_cmp(&a.1)
                    .then(b.2.total_cmp(&a.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            scored.into_iter().take(top_n).map(|(w, _, _)| w).collect()
        }
        Ranking::EdgePerBet => {
            let mut seen: HashSet<&str> = HashSet::new();
            let mut scored: Vec<(String, f64)> = Vec::new();
            for t in traders {
                if !seen.insert(t.proxy_wallet.as_str()) {
                    continue;
                }
                let Some(positions) = closed.get(&t.proxy_wallet) else {
                    continue;
                };
                let total = positions.len();
                if total < min_bets {
                    continue;
                }
                let edge_sum: f64 = positions
                    .iter()
                    .map(|cp| {
                        let won = if cp.won() { 1.0 } else { 0.0 };
                        won - cp.avg_price
                    })
                    .sum();
                let mean_edge = edge_sum / total as f64;
                if mean_edge > 0.0 {
                    scored.push((t.proxy_wallet.clone(), mean_edge));
                }
            }
            scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            scored.into_iter().take(top_n).map(|(w, _)| w).collect()
        }
    }
}

/// Wilson score interval lower bound (95%, `z = 1.96`) of a win rate
/// `wins / total`. This is the "hit rate weighted by sample size": it shrinks
/// toward 0 for small samples, so a lucky 2/2 scores below a solid 8/10.
/// Returns 0.0 for an empty sample.
fn wilson_lower_bound(wins: usize, total: usize) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let n = total as f64;
    let phat = wins as f64 / n;
    let z = 1.96_f64;
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let centre = phat + z2 / (2.0 * n);
    let margin = z * ((phat * (1.0 - phat) + z2 / (4.0 * n)) / n).sqrt();
    (centre - margin) / denom
}

// ---------------------------------------------------------------------------
// 2. Signals + convergence
// ---------------------------------------------------------------------------

/// A copy signal: ≥K whitelisted wallets BOUGHT `(condition_id, outcome_index)`
/// within the window. `timestamp` is the moment convergence was first met (the
/// Kth distinct wallet's BUY time); `wallets` are the convergent wallets (in
/// convergence order — ascending by their BUY time, then wallet).
#[derive(Debug, Clone, PartialEq)]
pub struct FollowSignal {
    pub condition_id: String,
    pub outcome_index: i64,
    pub timestamp: i64,
    pub wallets: Vec<String>,
}

/// From the whitelisted wallets' BUY trades, emit ONE signal per
/// `(market, outcome)` at the moment `k` DISTINCT whitelisted wallets have
/// BOUGHT it within `window_secs` (inclusive). Output is sorted by `(timestamp,
/// condition_id, outcome_index)`. `k = 1` ⇒ a signal at the first whitelisted
/// BUY of each `(market, outcome)`. Only BUY trades count; a wallet buying the
/// same side repeatedly counts once.
pub fn signals(
    whitelist: &[String],
    trades_by_wallet: &HashMap<String, Vec<Trade>>,
    k: usize,
    window_secs: i64,
) -> Vec<FollowSignal> {
    // Group whitelisted BUY trades by (market, outcome): list of (ts, wallet).
    let mut groups: HashMap<(String, i64), Vec<(i64, String)>> = HashMap::new();
    for wallet in whitelist {
        let Some(trades) = trades_by_wallet.get(wallet) else {
            continue;
        };
        for t in trades {
            if t.side == TradeSide::Buy {
                groups
                    .entry((t.condition_id.clone(), t.outcome_index))
                    .or_default()
                    .push((t.timestamp, wallet.clone()));
            }
        }
    }

    let mut out: Vec<FollowSignal> = Vec::new();
    for ((condition_id, outcome_index), mut buys) in groups {
        buys.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        if let Some((timestamp, wallets)) = first_convergence(&buys, k, window_secs) {
            out.push(FollowSignal {
                condition_id,
                outcome_index,
                timestamp,
                wallets,
            });
        }
    }

    out.sort_by(|a, b| {
        a.timestamp
            .cmp(&b.timestamp)
            .then_with(|| a.condition_id.cmp(&b.condition_id))
            .then_with(|| a.outcome_index.cmp(&b.outcome_index))
    });
    out
}

/// The earliest BUY time `t` such that `k` DISTINCT wallets have a BUY in the
/// trailing window `[t - window_secs, t]`, paired with those distinct wallets
/// (in convergence order). `buys` must be sorted ascending by timestamp. Uses
/// no information after `t` (no look-ahead). `None` if `k` is never reached.
fn first_convergence(
    buys: &[(i64, String)],
    k: usize,
    window_secs: i64,
) -> Option<(i64, Vec<String>)> {
    for &(t, _) in buys {
        let lo = t - window_secs;
        let mut seen: HashSet<&str> = HashSet::new();
        let mut distinct: Vec<String> = Vec::new();
        for (ts, wallet) in buys {
            if *ts >= lo && *ts <= t && seen.insert(wallet.as_str()) {
                distinct.push(wallet.clone());
            }
        }
        if distinct.len() >= k {
            return Some((t, distinct));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// 3. Simulate one signal — copy with lag
// ---------------------------------------------------------------------------

/// How a copied position is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitMode {
    /// Hold to market resolution (1.0 if our outcome won, else 0.0).
    Resolution,
    /// Exit when the leaders do: the earliest SELL of our outcome by any signal
    /// wallet (priced off the tape after the lag); falls back to resolution if
    /// there is no such SELL or no tape after the exit time.
    FollowExit,
}

/// Copy-trade execution parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimParams {
    /// Detection/execution lag in seconds applied to entry (and follow-exit).
    pub lag_secs: i64,
    /// Round-trip fee/slippage fraction subtracted from the gross return.
    pub fee_frac: f64,
    /// Exit rule.
    pub exit: ExitMode,
}

/// The outcome of simulating one [`FollowSignal`].
#[derive(Debug, Clone, PartialEq)]
pub enum SimResult {
    /// The copy was filled. `ret` is net of fees; `sports` tags the market.
    Filled {
        ret: f64,
        entry_px: f64,
        exit_px: f64,
        sports: bool,
    },
    /// No fill (no liquidity after the lag, or a degenerate entry price).
    Skipped,
}

/// Simulate copying `sig` off `tape` (the market's ascending-by-timestamp trade
/// tape). `winning_outcome` is the resolved winning outcome index of the market.
///
/// Entry: the first tape trade at/after `sig.timestamp + lag`, priced
/// (binary-normalized) to `sig.outcome_index`. Returns [`SimResult::Skipped`]
/// if there is no such trade (no liquidity after the lag) or the entry price is
/// degenerate (≤0 or ≥1). Exit: per [`SimParams::exit`]. There is no look-ahead
/// — only tape at/after the entry/exit time is read.
pub fn simulate_signal(
    sig: &FollowSignal,
    tape: &[Trade],
    winning_outcome: i64,
    trades_by_wallet: &HashMap<String, Vec<Trade>>,
    p: &SimParams,
    title: &str,
) -> SimResult {
    let entry_time = sig.timestamp + p.lag_secs;
    let Some(entry_px) = tape_price_at_or_after(tape, entry_time, sig.outcome_index) else {
        return SimResult::Skipped;
    };
    // Degenerate entry (a 0/1 mark means no real two-sided market) — cannot copy.
    if entry_px <= 0.0 || entry_px >= 1.0 {
        return SimResult::Skipped;
    }

    let resolution = resolution_px(winning_outcome, sig.outcome_index);
    let exit_px = match p.exit {
        ExitMode::Resolution => resolution,
        // Earliest leader SELL → price off the tape after the lag; fall back to
        // resolution if there is no such SELL or no tape after the exit time.
        ExitMode::FollowExit => earliest_follow_sell(sig, trades_by_wallet)
            .and_then(|sell_ts| {
                tape_price_at_or_after(tape, sell_ts + p.lag_secs, sig.outcome_index)
            })
            .unwrap_or(resolution),
    };

    let ret = (exit_px - entry_px) / entry_px - p.fee_frac;
    SimResult::Filled {
        ret,
        entry_px,
        exit_px,
        sports: is_sports(title),
    }
}

/// Binary-normalized price of taking `outcome` implied by a tape trade: the
/// trade's price if it traded `outcome`, else the complement `1 - price`.
fn normalize_price(t: &Trade, outcome: i64) -> f64 {
    if t.outcome_index == outcome {
        t.price
    } else {
        1.0 - t.price
    }
}

/// Resolution payoff of holding `outcome`: 1.0 if it won, else 0.0.
fn resolution_px(winning_outcome: i64, outcome: i64) -> f64 {
    if winning_outcome == outcome {
        1.0
    } else {
        0.0
    }
}

/// Binary-normalized price of the FIRST tape trade at/after `at` (the tape is
/// ascending by timestamp). `None` if there is no trade at/after `at`.
fn tape_price_at_or_after(tape: &[Trade], at: i64, outcome: i64) -> Option<f64> {
    tape.iter()
        .find(|t| t.timestamp >= at)
        .map(|t| normalize_price(t, outcome))
}

/// Earliest timestamp (strictly after `sig.timestamp`) at which any signal
/// wallet SELLs `sig.outcome_index`. `None` if no such sell exists.
fn earliest_follow_sell(
    sig: &FollowSignal,
    trades_by_wallet: &HashMap<String, Vec<Trade>>,
) -> Option<i64> {
    let mut earliest: Option<i64> = None;
    for wallet in &sig.wallets {
        let Some(trades) = trades_by_wallet.get(wallet) else {
            continue;
        };
        for t in trades {
            if t.side == TradeSide::Sell
                && t.outcome_index == sig.outcome_index
                && t.timestamp > sig.timestamp
            {
                earliest = Some(earliest.map_or(t.timestamp, |e| e.min(t.timestamp)));
            }
        }
    }
    earliest
}

// ---------------------------------------------------------------------------
// 4. Metrics
// ---------------------------------------------------------------------------

/// Aggregate statistics over a batch of simulated trades.
#[derive(Debug, Clone, PartialEq)]
pub struct Metrics {
    /// Number of FILLED trades (the basis of every return statistic below).
    pub n: usize,
    /// Number of SKIPPED signals (no fill).
    pub skipped: usize,
    pub mean_ret: f64,
    pub median_ret: f64,
    /// Fraction of filled trades with a strictly positive net return.
    pub hit_rate: f64,
    /// Equal-weight sum of net returns.
    pub total_ret: f64,
    /// `mean / stdev` (population stdev); 0 if stdev is 0 (or no fills).
    pub sharpe: f64,
    /// Max peak-to-trough decline of the equal-weight cumulative-sum equity
    /// curve, in return units (input order = caller's time order).
    pub max_drawdown: f64,
}

/// Aggregate [`SimResult`]s. Return statistics are over the `Filled` results in
/// input order (which the caller is expected to keep time-sorted); `Skipped`
/// are only counted. Empty/degenerate input yields all-zero statistics.
pub fn metrics(results: &[SimResult]) -> Metrics {
    let mut rets: Vec<f64> = Vec::new();
    let mut skipped = 0usize;
    for r in results {
        match r {
            SimResult::Filled { ret, .. } => rets.push(*ret),
            SimResult::Skipped => skipped += 1,
        }
    }

    let n = rets.len();
    if n == 0 {
        return Metrics {
            n: 0,
            skipped,
            mean_ret: 0.0,
            median_ret: 0.0,
            hit_rate: 0.0,
            total_ret: 0.0,
            sharpe: 0.0,
            max_drawdown: 0.0,
        };
    }

    let nf = n as f64;
    let total_ret: f64 = rets.iter().sum();
    let mean_ret = total_ret / nf;

    let mut sorted = rets.clone();
    sorted.sort_by(|a, b| a.total_cmp(b));
    // Works for odd and even n: odd ⇒ both indices coincide on the middle.
    let median_ret = (sorted[(n - 1) / 2] + sorted[n / 2]) / 2.0;

    let wins = rets.iter().filter(|&&r| r > 0.0).count();
    let hit_rate = wins as f64 / nf;

    let variance = rets
        .iter()
        .map(|r| {
            let d = *r - mean_ret;
            d * d
        })
        .sum::<f64>()
        / nf;
    let stdev = variance.sqrt();
    let sharpe = if stdev > 0.0 { mean_ret / stdev } else { 0.0 };

    // Max drawdown on the additive (cumulative-sum) equity curve starting at 0.
    let mut cum = 0.0;
    let mut peak = 0.0;
    let mut max_drawdown = 0.0;
    for &r in &rets {
        cum += r;
        if cum > peak {
            peak = cum;
        }
        let drawdown = peak - cum;
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }

    Metrics {
        n,
        skipped,
        mean_ret,
        median_ret,
        hit_rate,
        total_ret,
        sharpe,
        max_drawdown,
    }
}

/// Heuristic keyword classifier: does `title` look like a sports market?
///
/// Best-effort and case-insensitive: it matches versus-patterns (`vs`/`v`),
/// league acronyms (NBA/NFL/MLB/NHL/…), competition phrases (World Cup, FIFA,
/// Champions League, …) and sport verbs/nouns (beat/match/draw). It is NOT
/// authoritative and can mislabel edge cases (a lottery "draw", a legal case
/// "X v Y", a market phrased "win on …"). Used only to split the verdict by
/// category.
pub fn is_sports(title: &str) -> bool {
    let lower = title.to_ascii_lowercase();

    // Multi-word / punctuation-bearing patterns matched as substrings.
    const PHRASES: &[&str] = &[
        " vs ",
        " vs. ",
        " v ",
        " v. ",
        "win on ",
        "world cup",
        "fifa",
        "uefa",
        "champions league",
        "premier league",
        "la liga",
        "serie a",
        "bundesliga",
        "ligue 1",
        "super bowl",
        "grand prix",
        "formula 1",
        "stanley cup",
        "ballon d",
        "playoff",
    ];
    if PHRASES.iter().any(|p| lower.contains(p)) {
        return true;
    }

    // Single tokens matched on word boundaries (split on non-alphanumerics) to
    // avoid substring false positives (e.g. "nba" inside another word).
    const WORDS: &[&str] = &[
        "vs", "nba", "nfl", "mlb", "nhl", "mls", "ufc", "ncaa", "epl", "wnba",
        "f1", "soccer", "football", "basketball", "baseball", "hockey", "tennis",
        "golf", "cricket", "rugby", "boxing", "olympics", "olympic", "match",
        "beat", "beats", "draw",
    ];
    lower
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|tok| WORDS.contains(&tok))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    use super::*;

    // ---- fixture builders ----
    fn lb(wallet: &str, pnl: f64) -> LeaderboardEntry {
        LeaderboardEntry {
            proxy_wallet: wallet.to_string(),
            user_name: String::new(),
            pnl,
            vol: 0.0,
        }
    }

    fn cp(cid: &str, oi: i64, avg_price: f64, won: bool, cash_pnl: f64) -> ClosedPos {
        ClosedPos {
            condition_id: cid.to_string(),
            asset: String::new(),
            avg_price,
            outcome_index: oi,
            cur_price: if won { 1.0 } else { 0.0 },
            cash_pnl,
            size: 100.0,
            title: String::new(),
        }
    }

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

    fn closed_map(entries: Vec<(&str, Vec<ClosedPos>)>) -> HashMap<String, Vec<ClosedPos>> {
        entries
            .into_iter()
            .map(|(w, v)| (w.to_string(), v))
            .collect()
    }

    fn wl(ws: &[&str]) -> Vec<String> {
        ws.iter().map(|s| (*s).to_string()).collect()
    }

    fn tmap(trades: Vec<Trade>) -> HashMap<String, Vec<Trade>> {
        let mut m: HashMap<String, Vec<Trade>> = HashMap::new();
        for t in trades {
            m.entry(t.proxy_wallet.clone()).or_default().push(t);
        }
        m
    }

    fn find<'a>(sigs: &'a [FollowSignal], cid: &str, oi: i64) -> Option<&'a FollowSignal> {
        sigs.iter()
            .find(|s| s.condition_id == cid && s.outcome_index == oi)
    }

    fn sig(cid: &str, oi: i64, ts: i64, wallets: &[&str]) -> FollowSignal {
        FollowSignal {
            condition_id: cid.to_string(),
            outcome_index: oi,
            timestamp: ts,
            wallets: wl(wallets),
        }
    }

    fn fixture_tape() -> Vec<Trade> {
        // Ascending by timestamp. Note t=1020 trades OUTCOME 1 @0.70, so the
        // implied price of OUTCOME 0 there is 1 - 0.70 = 0.30.
        vec![
            trade("0xMM", "0xM", 0, TradeSide::Buy, 0.40, 1000),
            trade("0xMM", "0xM", 0, TradeSide::Buy, 0.45, 1010),
            trade("0xMM", "0xM", 1, TradeSide::Buy, 0.70, 1020),
            trade("0xMM", "0xM", 0, TradeSide::Buy, 0.50, 1030),
            trade("0xMM", "0xM", 0, TradeSide::Buy, 0.60, 1100),
        ]
    }

    fn assert_filled(r: &SimResult, entry: f64, exit: f64, ret: f64, sports: bool) {
        match r {
            SimResult::Filled {
                ret: g_ret,
                entry_px,
                exit_px,
                sports: g_sp,
            } => {
                assert!((entry_px - entry).abs() < 1e-9, "entry_px {entry_px} != {entry}");
                assert!((exit_px - exit).abs() < 1e-9, "exit_px {exit_px} != {exit}");
                assert!((g_ret - ret).abs() < 1e-9, "ret {g_ret} != {ret}");
                assert_eq!(*g_sp, sports, "sports flag");
            }
            SimResult::Skipped => panic!("expected Filled, got Skipped"),
        }
    }

    fn filled(ret: f64) -> SimResult {
        SimResult::Filled {
            ret,
            entry_px: 0.5,
            exit_px: 0.5,
            sports: false,
        }
    }

    // ===================== Ranking =====================
    #[test]
    fn raw_leaderboard_is_pnl_order_truncated() {
        let traders = vec![lb("0xA", 100.0), lb("0xB", 90.0), lb("0xC", 80.0)];
        let closed = HashMap::new();
        let out = rank_wallets(Ranking::RawLeaderboard, &traders, &closed, 2, 20);
        assert_eq!(out, wl(&["0xA", "0xB"]));
    }

    #[test]
    fn track_record_filters_low_sample_and_ranks_by_skill() {
        let traders = vec![lb("0xLucky", 1.0), lb("0xSkilled", 1.0), lb("0xMediocre", 1.0)];
        let closed = closed_map(vec![
            // 2/2 hit rate but only 2 bets -> filtered by min_bets.
            ("0xLucky", vec![cp("m1", 0, 0.3, true, 1.0), cp("m2", 0, 0.3, true, 1.0)]),
            // 8/10.
            ("0xSkilled", (0..10).map(|i| cp(&format!("s{i}"), 0, 0.4, i < 8, 1.0)).collect()),
            // 5/10.
            ("0xMediocre", (0..10).map(|i| cp(&format!("d{i}"), 0, 0.4, i < 5, 1.0)).collect()),
        ]);
        let top1 = rank_wallets(Ranking::TrackRecord, &traders, &closed, 1, 5);
        assert_eq!(top1, wl(&["0xSkilled"]));
        let top2 = rank_wallets(Ranking::TrackRecord, &traders, &closed, 2, 5);
        assert_eq!(top2, wl(&["0xSkilled", "0xMediocre"]));
        assert!(!top2.contains(&"0xLucky".to_string()));
    }

    #[test]
    fn edge_per_bet_drops_negative_and_low_sample_ranks_by_edge() {
        let traders = vec![lb("0xPos", 1.0), lb("0xPos2", 1.0), lb("0xNeg", 1.0), lb("0xLow", 1.0)];
        let closed = closed_map(vec![
            // mean edge = (3*(1-0.4) + 2*(0-0.3)) / 5 = (1.8 - 0.6)/5 = 0.24
            (
                "0xPos",
                vec![
                    cp("a", 0, 0.4, true, 0.0),
                    cp("b", 0, 0.4, true, 0.0),
                    cp("c", 0, 0.4, true, 0.0),
                    cp("d", 0, 0.3, false, 0.0),
                    cp("e", 0, 0.3, false, 0.0),
                ],
            ),
            // mean edge = 5*(1-0.6)/5 = 0.40 (ranks above 0xPos)
            ("0xPos2", (0..5).map(|i| cp(&format!("p{i}"), 0, 0.6, true, 0.0)).collect()),
            // mean edge = (1*(1-0.8) + 4*(0-0.5))/5 = (0.2 - 2.0)/5 = -0.36 -> dropped
            (
                "0xNeg",
                vec![
                    cp("n0", 0, 0.8, true, 0.0),
                    cp("n1", 0, 0.5, false, 0.0),
                    cp("n2", 0, 0.5, false, 0.0),
                    cp("n3", 0, 0.5, false, 0.0),
                    cp("n4", 0, 0.5, false, 0.0),
                ],
            ),
            // positive edge but only 2 bets -> dropped by min_bets
            ("0xLow", vec![cp("l0", 0, 0.3, true, 0.0), cp("l1", 0, 0.3, true, 0.0)]),
        ]);
        let out = rank_wallets(Ranking::EdgePerBet, &traders, &closed, 10, 5);
        assert_eq!(out, wl(&["0xPos2", "0xPos"]));
    }

    #[test]
    fn wilson_lower_bound_matches_known_values() {
        assert!((wilson_lower_bound(7, 10) - 0.3967735).abs() < 1e-6);
        assert_eq!(wilson_lower_bound(0, 0), 0.0);
        assert!(wilson_lower_bound(8, 10) > wilson_lower_bound(5, 10));
        // Small-sample penalty: a perfect 2/2 ranks below a solid 8/10.
        assert!(wilson_lower_bound(2, 2) < wilson_lower_bound(8, 10));
    }

    // ===================== Signals =====================
    #[test]
    fn signals_k1_one_per_market_outcome_at_first_buy() {
        let trades = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 200),
            trade("0xA", "0xN", 1, TradeSide::Buy, 0.5, 50),
        ]);
        let sigs = signals(&wl(&["0xA", "0xB"]), &trades, 1, 10);
        assert_eq!(sigs.len(), 2);
        // Sorted by timestamp: N@50, then M@100.
        assert_eq!(sigs[0].condition_id, "0xN");
        assert_eq!(sigs[0].timestamp, 50);
        assert_eq!(sigs[1].condition_id, "0xM");
        assert_eq!(sigs[1].timestamp, 100);
        assert_eq!(sigs[1].wallets, wl(&["0xA"]));
    }

    #[test]
    fn signals_k2_requires_two_distinct_within_window() {
        let trades = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 200),
            trade("0xA", "0xN", 1, TradeSide::Buy, 0.5, 50),
        ]);
        let sigs = signals(&wl(&["0xA", "0xB"]), &trades, 2, 200);
        assert_eq!(sigs.len(), 1);
        let m = find(&sigs, "0xM", 0).unwrap();
        assert_eq!(m.timestamp, 200);
        assert_eq!(m.wallets, wl(&["0xA", "0xB"]));
        assert!(find(&sigs, "0xN", 1).is_none());
    }

    #[test]
    fn signals_k2_near_miss_outside_window() {
        let trades = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 400),
        ]);
        let sigs = signals(&wl(&["0xA", "0xB"]), &trades, 2, 200);
        assert!(sigs.is_empty());
    }

    #[test]
    fn signals_k2_window_boundary_inclusive() {
        let trades = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 300),
        ]);
        let sigs = signals(&wl(&["0xA", "0xB"]), &trades, 2, 200);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].timestamp, 300);
    }

    #[test]
    fn signals_distinct_wallets_not_repeat_buys() {
        let trades = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 105),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 110),
        ]);
        let sigs = signals(&wl(&["0xA", "0xB"]), &trades, 2, 100);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].timestamp, 110);
        assert_eq!(sigs[0].wallets, wl(&["0xA", "0xB"]));
    }

    #[test]
    fn signals_only_buys_count() {
        let trades = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xC", "0xM", 0, TradeSide::Sell, 0.5, 105),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 106),
        ]);
        assert!(signals(&wl(&["0xA", "0xB", "0xC"]), &trades, 3, 100).is_empty());
        let sigs = signals(&wl(&["0xA", "0xB", "0xC"]), &trades, 2, 100);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].wallets, wl(&["0xA", "0xB"]));
        assert_eq!(sigs[0].timestamp, 106);
    }

    #[test]
    fn signals_k3_converges_and_near_miss() {
        let converge = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 150),
            trade("0xC", "0xM", 0, TradeSide::Buy, 0.5, 160),
        ]);
        let sigs = signals(&wl(&["0xA", "0xB", "0xC"]), &converge, 3, 100);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].timestamp, 160);
        assert_eq!(sigs[0].wallets, wl(&["0xA", "0xB", "0xC"]));

        let miss = tmap(vec![
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 100),
            trade("0xB", "0xM", 0, TradeSide::Buy, 0.5, 150),
            trade("0xC", "0xM", 0, TradeSide::Buy, 0.5, 300),
        ]);
        assert!(signals(&wl(&["0xA", "0xB", "0xC"]), &miss, 3, 100).is_empty());
    }

    // ===================== simulate_signal =====================
    #[test]
    fn sim_entry_picks_first_trade_at_lag_boundary_and_resolution_winner() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1000, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::Resolution };
        let r = simulate_signal(&s, &tape, 0, &HashMap::new(), &p, "Will the Fed cut rates?");
        assert_filled(&r, 0.45, 1.0, (1.0 - 0.45) / 0.45, false);
    }

    #[test]
    fn sim_resolution_loser_and_fee() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1000, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.02, exit: ExitMode::Resolution };
        let r = simulate_signal(&s, &tape, 1, &HashMap::new(), &p, "Market");
        assert_filled(&r, 0.45, 0.0, (0.0 - 0.45) / 0.45 - 0.02, false);
    }

    #[test]
    fn sim_binary_normalization_buy_outcome1() {
        let tape = fixture_tape();
        let s = sig("0xM", 1, 1000, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::Resolution };
        // Entry trade @1010 is an OUTCOME 0 trade @0.45 -> price of outcome 1 = 0.55.
        let r = simulate_signal(&s, &tape, 1, &HashMap::new(), &p, "Market");
        assert_filled(&r, 0.55, 1.0, (1.0 - 0.55) / 0.55, false);
    }

    #[test]
    fn sim_binary_normalization_entry_on_opposite_outcome_trade() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1010, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::Resolution };
        // Entry_time = 1020 -> the OUTCOME 1 trade @0.70 -> price of outcome 0 = 0.30.
        let r = simulate_signal(&s, &tape, 0, &HashMap::new(), &p, "Market");
        assert_filled(&r, 0.30, 1.0, (1.0 - 0.30) / 0.30, false);
    }

    #[test]
    fn sim_skips_when_no_tape_after_lag() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1100, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::Resolution };
        assert_eq!(
            simulate_signal(&s, &tape, 0, &HashMap::new(), &p, "Market"),
            SimResult::Skipped
        );
    }

    #[test]
    fn sim_skips_on_empty_tape() {
        let s = sig("0xM", 0, 1000, &["0xA"]);
        let p = SimParams { lag_secs: 0, fee_frac: 0.0, exit: ExitMode::Resolution };
        assert_eq!(
            simulate_signal(&s, &[], 0, &HashMap::new(), &p, "Market"),
            SimResult::Skipped
        );
    }

    #[test]
    fn sim_skips_on_degenerate_entry_price() {
        let tape = vec![trade("0xMM", "0xM", 0, TradeSide::Buy, 1.0, 2000)];
        let s = sig("0xM", 0, 1990, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::Resolution };
        assert_eq!(
            simulate_signal(&s, &tape, 0, &HashMap::new(), &p, "Market"),
            SimResult::Skipped
        );
    }

    #[test]
    fn sim_follow_exit_picks_earliest_valid_sell() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1000, &["0xA", "0xB"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::FollowExit };
        let follows = tmap(vec![
            trade("0xA", "0xM", 1, TradeSide::Sell, 0.5, 1005), // wrong outcome
            trade("0xB", "0xM", 0, TradeSide::Sell, 0.5, 999),  // before signal ts
            trade("0xA", "0xM", 0, TradeSide::Buy, 0.5, 1012),  // a buy
            trade("0xB", "0xM", 0, TradeSide::Sell, 0.5, 1015), // earliest valid sell
            trade("0xA", "0xM", 0, TradeSide::Sell, 0.5, 1025), // later valid sell
        ]);
        // Earliest sell @1015 -> exit_time = 1025 -> first tape ts>=1025 is t=1030 @0.50.
        let r = simulate_signal(&s, &tape, 0, &follows, &p, "Market");
        assert_filled(&r, 0.45, 0.50, (0.50 - 0.45) / 0.45, false);
    }

    #[test]
    fn sim_follow_exit_falls_back_to_resolution_without_sell() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1000, &["0xA", "0xB"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::FollowExit };
        let r = simulate_signal(&s, &tape, 0, &HashMap::new(), &p, "Market");
        assert_filled(&r, 0.45, 1.0, (1.0 - 0.45) / 0.45, false);
    }

    #[test]
    fn sim_follow_exit_falls_back_when_no_tape_after_exit() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1000, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::FollowExit };
        // Sell @1095 -> exit_time = 1105 > last tape (1100) -> fall back to resolution (loser -> 0.0).
        let follows = tmap(vec![trade("0xA", "0xM", 0, TradeSide::Sell, 0.5, 1095)]);
        let r = simulate_signal(&s, &tape, 1, &follows, &p, "Market");
        assert_filled(&r, 0.45, 0.0, (0.0 - 0.45) / 0.45, false);
    }

    #[test]
    fn sim_sets_sports_flag_from_title() {
        let tape = fixture_tape();
        let s = sig("0xM", 0, 1000, &["0xA"]);
        let p = SimParams { lag_secs: 10, fee_frac: 0.0, exit: ExitMode::Resolution };
        let r = simulate_signal(&s, &tape, 0, &HashMap::new(), &p, "France vs Brazil: who wins?");
        match r {
            SimResult::Filled { sports, .. } => assert!(sports),
            SimResult::Skipped => panic!("expected Filled"),
        }
    }

    // ===================== metrics =====================
    #[test]
    fn metrics_known_set() {
        let results = vec![
            filled(0.10),
            filled(-0.20),
            SimResult::Skipped,
            filled(0.30),
            filled(-0.05),
        ];
        let m = metrics(&results);
        assert_eq!(m.n, 4);
        assert_eq!(m.skipped, 1);
        assert!((m.mean_ret - 0.0375).abs() < 1e-12);
        assert!((m.median_ret - 0.025).abs() < 1e-12);
        assert!((m.hit_rate - 0.5).abs() < 1e-12);
        assert!((m.total_ret - 0.15).abs() < 1e-12);
        assert!((m.max_drawdown - 0.20).abs() < 1e-12);
        assert!((m.sharpe - 0.2027212).abs() < 1e-6);
    }

    #[test]
    fn metrics_empty_is_all_zero() {
        let m = metrics(&[]);
        assert_eq!(m.n, 0);
        assert_eq!(m.skipped, 0);
        assert_eq!(m.mean_ret, 0.0);
        assert_eq!(m.median_ret, 0.0);
        assert_eq!(m.hit_rate, 0.0);
        assert_eq!(m.total_ret, 0.0);
        assert_eq!(m.sharpe, 0.0);
        assert_eq!(m.max_drawdown, 0.0);
    }

    #[test]
    fn metrics_zero_variance_sharpe_is_zero() {
        let m = metrics(&[filled(0.05), filled(0.05)]);
        assert!((m.mean_ret - 0.05).abs() < 1e-12);
        assert_eq!(m.sharpe, 0.0);
        assert!((m.hit_rate - 1.0).abs() < 1e-12);
        assert_eq!(m.max_drawdown, 0.0);
    }

    // ===================== is_sports =====================
    #[test]
    fn is_sports_classifies_titles() {
        assert!(is_sports("Will France win the 2026 FIFA World Cup?"));
        assert!(is_sports("Lakers vs Celtics tonight?"));
        assert!(is_sports("USA vs. PAR draw?"));
        assert!(is_sports("Will Manchester City beat Real Madrid?"));
        assert!(is_sports("Chiefs win on 2026-02-08?"));
        assert!(is_sports("NBA Finals: who takes the title?"));
        assert!(!is_sports("Will the Fed cut rates in July?"));
        assert!(!is_sports("Will Bitcoin reach $100k in 2026?"));
        assert!(!is_sports("Will Trump win the 2024 election?"));
    }
}
