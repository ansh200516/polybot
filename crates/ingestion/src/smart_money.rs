//! Shared smart-money primitives: the **validated, pure** trader-ranking and
//! freshness math, extracted from the backtest so the OFFLINE backtest
//! (`pm-backtest`) and the LIVE copy strategy (`pm-app`) share ONE source of
//! truth and can never drift apart.
//!
//! Everything here is **I/O-free and deterministic** — pure functions over the
//! [`crate::data_api`] types ([`Trade`], [`LeaderboardEntry`]). The two pillars:
//!
//! 1. **Ranking** — [`trader_records`] builds each wallet's OUT-OF-SAMPLE
//!    (PRE-cutoff) resolved-bet record, and [`rank_wallets_oos`] turns those
//!    records into a follow WHITELIST under a [`Ranking`] (raw leaderboard,
//!    track record, or edge-per-bet), using the sample-size-aware
//!    [`wilson_lower_bound`].
//! 2. **Freshness** — [`within_drift`] decides whether OUR entry price is close
//!    enough to the smart trader's triggering price to still be worth copying
//!    (vs. chasing a runner whose edge is already gone).

use std::collections::{HashMap, HashSet};

use crate::data_api::{LeaderboardEntry, Trade, TradeSide};

// ---------------------------------------------------------------------------
// Ranking — the wallet-selection spectrum under test
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

impl Ranking {
    /// Stable string label (used as a grid-result key and in the report JSON).
    pub fn as_str(self) -> &'static str {
        match self {
            Ranking::RawLeaderboard => "RawLeaderboard",
            Ranking::TrackRecord => "TrackRecord",
            Ranking::EdgePerBet => "EdgePerBet",
        }
    }
}

/// Wilson score interval lower bound (95%, `z = 1.96`) of a win rate
/// `wins / total`. This is the "hit rate weighted by sample size": it shrinks
/// toward 0 for small samples, so a lucky 2/2 scores below a solid 8/10.
/// Returns 0.0 for an empty sample.
pub fn wilson_lower_bound(wins: usize, total: usize) -> f64 {
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
// Out-of-sample trader records (FIX-B — the trust fix)
// ---------------------------------------------------------------------------

/// A trader's OUT-OF-SAMPLE record, built from their own PRE-cutoff resolved
/// BUYS (the SELECTION set). This is the trades-based replacement for the
/// shallow closed-positions track record: it is computed from `/trades` BUYS
/// scored against the INDEPENDENT Gamma `resolutions`, and it is deliberately
/// blind to anything at/after `cutoff_ts` — so trader SELECTION and the COPY
/// TEST run on DISJOINT trades and the leaderboard can no longer be ranked on
/// the same history it is tested on (the survivorship bias FIX-B removes).
#[derive(Debug, Clone, PartialEq)]
pub struct TraderRecord {
    /// The trader's proxy wallet.
    pub wallet: String,
    /// Number of qualifying PRE-cutoff resolved BUYS (each BUY counts once).
    pub n_bets: usize,
    /// Fraction of those bets that WON (`resolutions[cond] == outcome_index`).
    pub hit_rate: f64,
    /// Mean realized edge per bet: `mean((won ? 1 : 0) − entry_price)`.
    pub mean_edge: f64,
    /// Wilson 95% lower bound of the hit rate (sample-size aware, see
    /// [`wilson_lower_bound`]).
    pub wilson: f64,
}

/// Build every trader's PRE-cutoff resolved-bet record (the OUT-OF-SAMPLE
/// SELECTION set) from their own BUYS.
///
/// For each BUY with `timestamp < cutoff_ts` in a market with a known Gamma
/// resolution, the bet WON iff `resolutions[condition_id] == outcome_index` and
/// its realized edge is `(won ? 1 : 0) − price`. These are aggregated per wallet
/// into a [`TraderRecord`] (`n_bets`, `hit_rate`, `mean_edge`, and the
/// [`wilson_lower_bound`] of the hit rate).
///
/// BUYS at/after `cutoff_ts` (the COPY-TEST set) and BUYS in markets Gamma did
/// not resolve are ignored. A wallet with zero qualifying bets is omitted, so no
/// record ever carries a NaN. Pure and deterministic (the output map is keyed by
/// wallet, independent of iteration order).
pub fn trader_records(
    trades_by_wallet: &HashMap<String, Vec<Trade>>,
    resolutions: &HashMap<String, i64>,
    cutoff_ts: i64,
) -> HashMap<String, TraderRecord> {
    let mut out: HashMap<String, TraderRecord> = HashMap::new();
    for (wallet, trades) in trades_by_wallet {
        let mut n_bets = 0usize;
        let mut wins = 0usize;
        let mut edge_sum = 0.0_f64;
        for t in trades {
            // SELECTION set only: pre-cutoff BUYS in Gamma-resolved markets.
            if t.side != TradeSide::Buy || t.timestamp >= cutoff_ts {
                continue;
            }
            let Some(&winner) = resolutions.get(&t.condition_id) else {
                continue;
            };
            let won = winner == t.outcome_index;
            n_bets += 1;
            if won {
                wins += 1;
            }
            edge_sum += if won { 1.0 } else { 0.0 } - t.price;
        }
        if n_bets == 0 {
            continue;
        }
        let nf = n_bets as f64;
        out.insert(
            wallet.clone(),
            TraderRecord {
                wallet: wallet.clone(),
                n_bets,
                hit_rate: wins as f64 / nf,
                mean_edge: edge_sum / nf,
                wilson: wilson_lower_bound(wins, n_bets),
            },
        );
    }
    out
}

/// Return the WHITELIST of wallets to follow under `ranking`, selected purely
/// from PRE-cutoff [`TraderRecord`]s (OUT-OF-SAMPLE), so selection never peeks
/// at the post-cutoff trades the copy test scores. This is the FIX-B replacement
/// for the closed-positions ranking (which scored on the same positions it
/// tested).
///
/// - [`Ranking::RawLeaderboard`]: `traders` (already PnL-sorted) truncated to
///   `top_n`. `records`/`min_bets` are ignored — the size-driven baseline.
/// - [`Ranking::TrackRecord`]: keep wallets with `n_bets ≥ min_bets`, rank by
///   the Wilson lower bound (then `mean_edge`, then wallet), take `top_n`.
/// - [`Ranking::EdgePerBet`]: keep wallets with `n_bets ≥ min_bets` AND a
///   strictly positive `mean_edge`, rank by `mean_edge` (then wallet), take
///   `top_n`.
///
/// Only wallets present in `traders` are considered (de-duped, first occurrence
/// wins) so the universe stays stable and deterministic.
pub fn rank_wallets_oos(
    ranking: Ranking,
    traders: &[LeaderboardEntry],
    records: &HashMap<String, TraderRecord>,
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
            let mut scored = eligible_records(traders, records, min_bets, |_| true);
            scored.sort_by(|a, b| {
                b.wilson
                    .total_cmp(&a.wilson)
                    .then(b.mean_edge.total_cmp(&a.mean_edge))
                    .then_with(|| a.wallet.cmp(&b.wallet))
            });
            scored
                .into_iter()
                .take(top_n)
                .map(|r| r.wallet.clone())
                .collect()
        }
        Ranking::EdgePerBet => {
            let mut scored = eligible_records(traders, records, min_bets, |r| r.mean_edge > 0.0);
            scored.sort_by(|a, b| {
                b.mean_edge
                    .total_cmp(&a.mean_edge)
                    .then_with(|| a.wallet.cmp(&b.wallet))
            });
            scored
                .into_iter()
                .take(top_n)
                .map(|r| r.wallet.clone())
                .collect()
        }
    }
}

/// The de-duped, `min_bets`-filtered records of the wallets in `traders` that
/// also pass `keep`. Shared by the two skill rankings in [`rank_wallets_oos`].
fn eligible_records<'a>(
    traders: &[LeaderboardEntry],
    records: &'a HashMap<String, TraderRecord>,
    min_bets: usize,
    keep: impl Fn(&TraderRecord) -> bool,
) -> Vec<&'a TraderRecord> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut scored: Vec<&TraderRecord> = Vec::new();
    for t in traders {
        if !seen.insert(t.proxy_wallet.as_str()) {
            continue;
        }
        let Some(rec) = records.get(&t.proxy_wallet) else {
            continue;
        };
        if rec.n_bets >= min_bets && keep(rec) {
            scored.push(rec);
        }
    }
    scored
}

// ---------------------------------------------------------------------------
// Freshness filter (alpha)
// ---------------------------------------------------------------------------

/// FRESHNESS test: is OUR fill price `entry_px` close enough to the smart
/// trader's triggering price `trigger_px` to still be worth copying?
///
/// Returns `true` iff `trigger_px > 0` AND the fractional drift
/// `|entry_px − trigger_px| / trigger_px ≤ max_drift`. A drift above `max_drift`
/// means we'd be CHASING a runner (the smart-money edge — being in near their
/// price — is gone), so the caller should skip. A degenerate `trigger_px == 0`
/// is treated as NOT within drift (skip): the drift would be `+∞`, never
/// finite, so it can never satisfy any finite cap.
///
/// Shared by the backtest's `simulate_signal` and the live copy strategy's
/// entry gate so both apply the SAME freshness rule.
pub fn within_drift(entry_px: f64, trigger_px: f64, max_drift: f64) -> bool {
    trigger_px > 0.0 && (entry_px - trigger_px).abs() / trigger_px <= max_drift
}

// ---------------------------------------------------------------------------
// Specialist routing — per-(trader, category) ADAPTIVE-SKILL ranking
// ---------------------------------------------------------------------------
//
// A single global score ranks a trader by ALL their history at once, so a
// politics whale's random sports flyer counts the same as their bread-and-butter
// bet, and a stale-but-legendary record keeps them "top" long after they cool.
// Specialist routing fixes both: rank each `(trader, CATEGORY)` pair on a
// RECENCY-DECAYED, sample-aware edge, keep only the top specialists per category
// (the "creamy layer"), and copy a trader ONLY in a category where they're
// proven.

/// Classify a market into a coarse CATEGORY from its `slug` (primary) and
/// `title` (fallback). Polymarket slugs lead with a league/topic token —
/// `mlb-…`, `nba-…`, `fifwc-…`, `cs2-…`, `atp-…`, `ufc-…` — that maps cleanly to
/// a sport. Generic `will-…` question markets carry no league token, so they
/// fall back to keyword-classifying the title (politics / crypto / econ), else
/// `"other"`. Deterministic and allocation-light; the returned label is stable
/// (used as a routing key).
pub fn market_category(slug: &str, title: &str) -> String {
    let s = slug.to_ascii_lowercase();
    // Scan slug tokens for the FIRST recognized league/topic token (the leading
    // token is usually the league, but some slugs lead with a team code, so scan
    // a few tokens in).
    for tok in s.split('-').take(4) {
        if let Some(cat) = league_category(tok) {
            return cat.to_string();
        }
    }
    // No league token (generic `will-…` etc.) → keyword-classify the title.
    title_category(title)
}

/// Map a single slug token to a sport category, or `None` if unrecognized.
fn league_category(tok: &str) -> Option<&'static str> {
    // Grouped by sport so per-category samples stay thick enough to rank.
    const SOCCER: &[&str] = &[
        "fifwc", "fifac", "wcq", "epl", "ucl", "uel", "uecl", "laliga", "seriea", "bun", "dfb",
        "ligue1", "mls", "bra", "bra2", "ere", "ered", "por", "ned", "copa", "euro", "afcon",
        "concacaf", "soccer", "fut", "ita", "eng", "esp", "ger", "fra", "libertadores", "sudamericana",
    ];
    const BASKETBALL: &[&str] = &["nba", "wnba", "cbb", "ncaab", "basketball", "euroleague"];
    const BASEBALL: &[&str] = &["mlb", "npb", "kbo", "baseball"];
    const HOCKEY: &[&str] = &["nhl", "hockey", "khl"];
    const AMFOOTBALL: &[&str] = &["nfl", "cfb", "ncaaf"];
    const TENNIS: &[&str] = &["atp", "wta", "tennis", "ao", "rg", "wimbledon", "usopen"];
    const COMBAT: &[&str] = &["ufc", "mma", "boxing", "box", "bellator", "pfl"];
    const ESPORTS: &[&str] = &[
        "cs2", "csgo", "cs", "lol", "dota2", "dota", "valorant", "val", "owl", "r6", "rl", "esports",
    ];
    const CRICKET: &[&str] = &["cricket", "ipl", "t20", "bbl", "odi"];
    const MOTORSPORT: &[&str] = &["f1", "nascar", "motogp", "indycar"];
    const GOLF: &[&str] = &["golf", "pga", "liv", "masters"];
    for (cat, toks) in [
        ("soccer", SOCCER),
        ("basketball", BASKETBALL),
        ("baseball", BASEBALL),
        ("hockey", HOCKEY),
        ("amfootball", AMFOOTBALL),
        ("tennis", TENNIS),
        ("combat", COMBAT),
        ("esports", ESPORTS),
        ("cricket", CRICKET),
        ("motorsport", MOTORSPORT),
        ("golf", GOLF),
    ] {
        if toks.contains(&tok) {
            return Some(cat);
        }
    }
    None
}

/// Keyword-classify a non-league market by its title into politics / crypto /
/// econ, else `"other"`. Case-insensitive substring match.
fn title_category(title: &str) -> String {
    let t = title.to_ascii_lowercase();
    let any = |kws: &[&str]| kws.iter().any(|k| t.contains(k));
    if any(&[
        "election", "senate", "president", "congress", "governor", "primary", "democrat",
        "republican", "parliament", "referendum", "prime minister", "trump", "biden", "poll",
        "vote", "nomin", "cabinet", "impeach",
    ]) {
        "politics".to_string()
    } else if any(&[
        "bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "$sol", "dogecoin", "xrp", "binance",
        "stablecoin",
    ]) {
        "crypto".to_string()
    } else if any(&[
        "fed", "cpi", "gdp", "inflation", "interest rate", "rate cut", "rate hike", "recession",
        "unemployment", "jobs report", "powell", "fomc",
    ]) {
        "econ".to_string()
    } else {
        "other".to_string()
    }
}

/// One `(trader, category)` ADAPTIVE record: a recency-decayed, sample-aware
/// view of the trader's resolved BUYS in that category. `score` — the value the
/// creamy layer ranks by — is the lower-confidence bound
/// `wmean_edge − z · wstdev / √n_eff`: it rewards a high recent edge but shrinks
/// it for a thin or volatile record, so a lucky 2-bet fluke can't out-rank a
/// steady specialist.
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveRecord {
    /// Kish EFFECTIVE sample size `(Σw)² / Σw²` — the decay-adjusted bet count.
    pub n_eff: f64,
    /// Recency-weighted mean edge (`won − price`), decayed by `half_life`.
    pub wmean_edge: f64,
    /// Lower-confidence-bound skill score (the creamy-layer sort key).
    pub score: f64,
}

/// Build per-`(wallet, category)` [`AdaptiveRecord`]s from each trader's resolved
/// PRE-`cutoff_ts` BUYS (the OUT-OF-SAMPLE selection set — post-cutoff trades are
/// the copy-test and are excluded), category derived per market via
/// [`market_category`]. Each bet's edge `(won?1:0) − price` is weighted by an
/// EXPONENTIAL recency decay `0.5^((cutoff_ts − t)/half_life_secs)` (measured
/// back from the decision point `cutoff_ts`, so recent-relative-to-cutoff bets
/// dominate). Resolutions come from the INDEPENDENT Gamma map (same as
/// [`trader_records`]). Pure and deterministic.
pub fn category_adaptive_records(
    trades_by_wallet: &HashMap<String, Vec<Trade>>,
    resolutions: &HashMap<String, i64>,
    cutoff_ts: i64,
    half_life_secs: f64,
    z: f64,
) -> HashMap<(String, String), AdaptiveRecord> {
    // Weighted accumulators per (wallet, category): Σw, Σw·e, Σw·e², Σw².
    #[derive(Default)]
    struct Acc {
        w: f64,
        we: f64,
        we2: f64,
        w2: f64,
    }
    let mut acc: HashMap<(String, String), Acc> = HashMap::new();
    for (wallet, trades) in trades_by_wallet {
        for t in trades {
            if t.side != TradeSide::Buy || t.timestamp >= cutoff_ts {
                continue;
            }
            let Some(&win) = resolutions.get(&t.condition_id) else {
                continue;
            };
            let cat = market_category(&t.slug, &t.title);
            let won = if win == t.outcome_index { 1.0 } else { 0.0 };
            let edge = won - t.price;
            let age = (cutoff_ts - t.timestamp) as f64;
            let w = 0.5_f64.powf(age / half_life_secs);
            let e = acc.entry((wallet.clone(), cat)).or_default();
            e.w += w;
            e.we += w * edge;
            e.we2 += w * edge * edge;
            e.w2 += w * w;
        }
    }
    let mut out: HashMap<(String, String), AdaptiveRecord> = HashMap::new();
    for (key, a) in acc {
        if a.w <= 0.0 || a.w2 <= 0.0 {
            continue;
        }
        let wmean = a.we / a.w;
        let n_eff = a.w * a.w / a.w2;
        // Weighted population variance, floored at 0 against fp drift.
        let var = (a.we2 / a.w - wmean * wmean).max(0.0);
        let stdev = var.sqrt();
        let score = wmean - z * stdev / n_eff.sqrt();
        out.insert(
            key,
            AdaptiveRecord {
                n_eff,
                wmean_edge: wmean,
                score,
            },
        );
    }
    out
}

/// Select the per-category CREAMY LAYER from [`category_adaptive_records`]: in
/// each category, keep wallets with `n_eff ≥ min_bets` AND `score > 0`, rank by
/// `score` (then wallet for determinism), take the top `k_per_cat`. Returns
/// `category → set of specialist wallets` for O(1) routing lookups.
pub fn creamy_layer(
    records: &HashMap<(String, String), AdaptiveRecord>,
    k_per_cat: usize,
    min_bets: f64,
) -> HashMap<String, HashSet<String>> {
    let mut by_cat: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for ((wallet, cat), r) in records {
        if r.n_eff >= min_bets && r.score > 0.0 {
            by_cat
                .entry(cat.clone())
                .or_default()
                .push((wallet.clone(), r.score));
        }
    }
    by_cat
        .into_iter()
        .map(|(cat, mut v)| {
            v.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            v.truncate(k_per_cat);
            (cat, v.into_iter().map(|(w, _)| w).collect())
        })
        .collect()
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

    fn res_map(entries: &[(&str, i64)]) -> HashMap<String, i64> {
        entries.iter().map(|(c, w)| ((*c).to_string(), *w)).collect()
    }

    fn rec(wallet: &str, n_bets: usize, hit_rate: f64, mean_edge: f64, wilson: f64) -> TraderRecord {
        TraderRecord {
            wallet: wallet.to_string(),
            n_bets,
            hit_rate,
            mean_edge,
            wilson,
        }
    }

    fn rec_map(records: Vec<TraderRecord>) -> HashMap<String, TraderRecord> {
        records.into_iter().map(|r| (r.wallet.clone(), r)).collect()
    }

    // ===================== wilson_lower_bound =====================
    #[test]
    fn wilson_lower_bound_matches_known_values() {
        assert!((wilson_lower_bound(7, 10) - 0.3967735).abs() < 1e-6);
        assert_eq!(wilson_lower_bound(0, 0), 0.0);
        assert!(wilson_lower_bound(8, 10) > wilson_lower_bound(5, 10));
        // Small-sample penalty: a perfect 2/2 ranks below a solid 8/10.
        assert!(wilson_lower_bound(2, 2) < wilson_lower_bound(8, 10));
    }

    // ===================== trader_records (OOS, FIX-B) =====================
    #[test]
    fn trader_records_pre_cutoff_only_scored_with_edges() {
        let cutoff = 1000;
        let trades = tmap(vec![
            trade("0xS", "m1", 0, TradeSide::Buy, 0.4, 100), // res 0 -> WON,  edge  0.6
            trade("0xS", "m2", 0, TradeSide::Buy, 0.5, 200), // res 1 -> LOST, edge -0.5
            trade("0xS", "m3", 0, TradeSide::Buy, 0.3, 300), // res 0 -> WON,  edge  0.7
            trade("0xS", "m4", 1, TradeSide::Buy, 0.6, 400), // res 1 -> WON,  edge  0.4
            trade("0xS", "m5", 0, TradeSide::Buy, 0.9, 2000), // post-cutoff -> EXCLUDED
            trade("0xS", "m6", 0, TradeSide::Buy, 0.5, 500),  // unresolved  -> EXCLUDED
            trade("0xS", "m1", 0, TradeSide::Sell, 0.4, 150),  // a SELL      -> EXCLUDED
        ]);
        let res = res_map(&[("m1", 0), ("m2", 1), ("m3", 0), ("m4", 1), ("m5", 1)]);
        let recs = trader_records(&trades, &res, cutoff);
        let r = recs.get("0xS").expect("0xS has a record");
        assert_eq!(r.n_bets, 4, "post-cutoff + unresolved + sells excluded");
        assert!((r.hit_rate - 0.75).abs() < 1e-12);
        assert!((r.mean_edge - 0.3).abs() < 1e-12); // (0.6 - 0.5 + 0.7 + 0.4)/4
        assert_eq!(r.wilson, wilson_lower_bound(3, 4));
        assert_eq!(r.wallet, "0xS");
    }

    #[test]
    fn trader_records_omits_wallet_with_no_qualifying_bets() {
        let cutoff = 1000;
        let trades = tmap(vec![
            trade("0xEmpty", "m1", 0, TradeSide::Buy, 0.4, 2000), // post-cutoff
            trade("0xEmpty", "m2", 0, TradeSide::Buy, 0.4, 100),  // unresolved
        ]);
        let res = res_map(&[("m1", 0)]); // m1 resolved but is post-cutoff
        let recs = trader_records(&trades, &res, cutoff);
        assert!(!recs.contains_key("0xEmpty"));
        assert!(recs.is_empty());
    }

    // ===================== rank_wallets_oos (OOS, FIX-B) =====================
    #[test]
    fn rank_wallets_oos_raw_leaderboard_is_pnl_order_truncated() {
        let traders = vec![lb("0xA", 100.0), lb("0xB", 90.0), lb("0xC", 80.0)];
        let records = HashMap::new(); // ignored by RawLeaderboard
        let out = rank_wallets_oos(Ranking::RawLeaderboard, &traders, &records, 2, 10);
        assert_eq!(out, wl(&["0xA", "0xB"]));
    }

    #[test]
    fn rank_wallets_oos_track_record_ranks_by_wilson_and_filters() {
        let traders = vec![lb("0xLucky", 1.0), lb("0xSkilled", 1.0), lb("0xMediocre", 1.0)];
        let records = rec_map(vec![
            rec("0xLucky", 2, 1.0, 0.5, 0.90),     // n_bets < min_bets -> filtered
            rec("0xSkilled", 10, 0.8, 0.3, 0.60),  // highest wilson among eligible
            rec("0xMediocre", 10, 0.5, 0.1, 0.40),
        ]);
        let top1 = rank_wallets_oos(Ranking::TrackRecord, &traders, &records, 1, 5);
        assert_eq!(top1, wl(&["0xSkilled"]));
        let top2 = rank_wallets_oos(Ranking::TrackRecord, &traders, &records, 2, 5);
        assert_eq!(top2, wl(&["0xSkilled", "0xMediocre"]));
        assert!(!top2.contains(&"0xLucky".to_string()));
    }

    #[test]
    fn rank_wallets_oos_edge_per_bet_drops_negative_and_low_sample() {
        let traders = vec![lb("0xPos", 1.0), lb("0xPos2", 1.0), lb("0xNeg", 1.0), lb("0xLow", 1.0)];
        let records = rec_map(vec![
            rec("0xPos", 5, 0.6, 0.24, 0.30),
            rec("0xPos2", 5, 1.0, 0.40, 0.50), // ranks above 0xPos by mean_edge
            rec("0xNeg", 5, 0.2, -0.36, 0.05), // mean_edge <= 0 -> dropped
            rec("0xLow", 2, 1.0, 0.50, 0.40),  // n_bets < min_bets -> dropped
        ]);
        let out = rank_wallets_oos(Ranking::EdgePerBet, &traders, &records, 10, 5);
        assert_eq!(out, wl(&["0xPos2", "0xPos"]));
    }

    /// The TRUST FIX: selection ranks on PRE-cutoff records only. A trader whose
    /// post-cutoff trades are terrible still ranks on their pre-cutoff edge, and
    /// post-cutoff trades NEVER leak into the record.
    #[test]
    fn rank_wallets_oos_uses_pre_cutoff_record_not_post_cutoff() {
        let cutoff = 1000;
        let mut trades_vec: Vec<Trade> = Vec::new();
        let mut resolutions: HashMap<String, i64> = HashMap::new();
        // 0xStrong: 5 winning PRE-cutoff buys @0.3 -> edge 0.7 each.
        for i in 0..5i64 {
            let cid = format!("s{i}");
            trades_vec.push(trade("0xStrong", &cid, 0, TradeSide::Buy, 0.3, 100 + i));
            resolutions.insert(cid, 0);
        }
        // 0xOk: 5 winning PRE-cutoff buys @0.8 -> edge 0.2 each.
        for i in 0..5i64 {
            let cid = format!("o{i}");
            trades_vec.push(trade("0xOk", &cid, 0, TradeSide::Buy, 0.8, 100 + i));
            resolutions.insert(cid, 0);
        }
        // 0xStrong's POST-cutoff disasters (10 losses @0.95). If selection peeked
        // here, 0xStrong's edge would go NEGATIVE and it would drop out entirely.
        for i in 0..10i64 {
            let cid = format!("p{i}");
            trades_vec.push(trade("0xStrong", &cid, 0, TradeSide::Buy, 0.95, 2000 + i));
            resolutions.insert(cid, 1);
        }
        let trades = tmap(trades_vec);

        let records = trader_records(&trades, &resolutions, cutoff);
        let strong = records.get("0xStrong").expect("0xStrong has a record");
        assert_eq!(strong.n_bets, 5, "post-cutoff trades excluded from record");
        assert!((strong.mean_edge - 0.7).abs() < 1e-12);

        let traders = vec![lb("0xStrong", 1.0), lb("0xOk", 1.0)];
        let out = rank_wallets_oos(Ranking::EdgePerBet, &traders, &records, 10, 5);
        assert_eq!(out, wl(&["0xStrong", "0xOk"]));
    }

    // ===================== within_drift (freshness, alpha) =====================
    #[test]
    fn within_drift_just_inside_cap_is_true() {
        // drift = |0.50 - 0.45| / 0.45 = 0.111… <= 0.2 -> still fresh.
        assert!(within_drift(0.50, 0.45, 0.2));
    }

    #[test]
    fn within_drift_just_outside_cap_is_false() {
        // drift = |0.40 - 0.35| / 0.35 = 0.142857… > 0.13 -> chasing -> skip.
        assert!(!within_drift(0.40, 0.35, 0.13));
    }

    #[test]
    fn within_drift_zero_trigger_is_false() {
        // Degenerate trigger price: drift would be +∞, never within any finite
        // cap (matches the backtest's old inline `+inf > max_drift` -> skip).
        assert!(!within_drift(0.50, 0.0, 1.0));
        assert!(!within_drift(0.50, 0.0, f64::INFINITY));
    }

    // ===================== specialist routing =====================
    fn buy_s(wallet: &str, cid: &str, oi: i64, price: f64, ts: i64, slug: &str) -> Trade {
        let mut t = trade(wallet, cid, oi, TradeSide::Buy, price, ts);
        t.slug = slug.to_string();
        t
    }

    #[test]
    fn market_category_maps_leagues_and_title_fallback() {
        // League slugs → sport categories (first recognized token wins).
        assert_eq!(market_category("mlb-nyy-bos-2026-05-01", ""), "baseball");
        assert_eq!(market_category("nba-lal-bos-2026-01-15", ""), "basketball");
        assert_eq!(market_category("nhl-bos-mtl-2026", ""), "hockey");
        assert_eq!(market_category("cs2-navi-vitality-2026", ""), "esports");
        assert_eq!(
            market_category("fifwc-jor-alg-2026-06-22-alg", "Will Algeria win on 2026-06-22?"),
            "soccer"
        );
        // Generic `will-…` slugs carry no league token → title keyword fallback.
        assert_eq!(
            market_category("will-trump-2028", "Will Trump win the 2028 election?"),
            "politics"
        );
        assert_eq!(
            market_category("will-btc-100k", "Will Bitcoin hit $100k in 2026?"),
            "crypto"
        );
        assert_eq!(
            market_category("will-fed-cut", "Will the Fed cut the interest rate in March?"),
            "econ"
        );
        assert_eq!(
            market_category("some-market", "A market about nothing in particular"),
            "other"
        );
    }

    #[test]
    fn category_adaptive_records_score_and_recency() {
        let cutoff = 1_000_000i64;
        let hl = 30.0 * 86_400.0; // 30-day half-life

        let mut trades: Vec<Trade> = Vec::new();
        // A: 5 winning baseball buys @0.5 (edge +0.5), ALL just before cutoff →
        // near-equal weights → wmean ≈ 0.5, var ≈ 0, n_eff ≈ 5, score ≈ 0.5.
        for i in 0..5i64 {
            trades.push(buy_s("0xA", &format!("a{i}"), 0, 0.5, cutoff - 10 - i, "mlb-a"));
        }
        // B: 5 winning baseball buys @0.5 OLD (120d back), 5 LOSING @0.5 RECENT →
        // the recent losses dominate the recency-weighted mean.
        for i in 0..5i64 {
            trades.push(buy_s("0xB", &format!("bw{i}"), 0, 0.5, cutoff - 120 * 86_400 - i, "mlb-b"));
        }
        for i in 0..5i64 {
            trades.push(buy_s("0xB", &format!("bl{i}"), 0, 0.5, cutoff - 10 - i, "mlb-b"));
        }
        let tm = tmap(trades);
        let mut res: HashMap<String, i64> = HashMap::new();
        for i in 0..5 {
            res.insert(format!("a{i}"), 0); // A wins (res == oi 0)
            res.insert(format!("bw{i}"), 0); // B old wins
            res.insert(format!("bl{i}"), 1); // B recent losses (res 1 != oi 0)
        }
        let recs = category_adaptive_records(&tm, &res, cutoff, hl, 1.0);
        let a = recs
            .get(&("0xA".to_string(), "baseball".to_string()))
            .expect("A baseball record");
        let b = recs
            .get(&("0xB".to_string(), "baseball".to_string()))
            .expect("B baseball record");
        assert!((a.wmean_edge - 0.5).abs() < 1e-6, "A ~ +0.5 recency-weighted edge");
        assert!((a.n_eff - 5.0).abs() < 0.1, "A effective sample ~ 5 (equal weights)");
        assert!(a.score > 0.0, "A is a positive specialist");
        assert!(
            b.wmean_edge < a.wmean_edge,
            "B's recent losses drag its recency-weighted edge below A ({} < {})",
            b.wmean_edge,
            a.wmean_edge
        );
    }

    #[test]
    fn creamy_layer_keeps_top_specialists_and_gates() {
        let ar = |n_eff, score| AdaptiveRecord { n_eff, wmean_edge: score, score };
        let mut records: HashMap<(String, String), AdaptiveRecord> = HashMap::new();
        records.insert(("0xStrong".into(), "baseball".into()), ar(8.0, 0.30));
        records.insert(("0xWeak".into(), "baseball".into()), ar(8.0, -0.20)); // score ≤ 0 → dropped
        records.insert(("0xThin".into(), "baseball".into()), ar(2.0, 0.80)); // n_eff < min → dropped
        records.insert(("0xPol".into(), "politics".into()), ar(6.0, 0.15));

        let cl = creamy_layer(&records, 1, 3.0);
        let base = cl.get("baseball").expect("baseball layer");
        assert!(base.contains("0xStrong"), "top positive specialist kept");
        assert!(!base.contains("0xWeak"), "non-positive score dropped");
        assert!(!base.contains("0xThin"), "thin effective sample dropped by min_bets");
        assert_eq!(base.len(), 1, "top-1 per category");
        assert!(cl.get("politics").expect("politics layer").contains("0xPol"));
    }
}
