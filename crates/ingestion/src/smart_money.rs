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
}
