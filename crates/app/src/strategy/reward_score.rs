//! Pure local reproduction of Polymarket's liquidity-reward scoring, used to
//! ESTIMATE rewards on paper before risking money. No I/O. See the spec §2/§9.

/// Quadratic order score `S(v,s) = ((v-s)/v)^2`, 0 when `s > v` or `v <= 0`.
/// `v` = max_incentive_spread (cents), `s` = distance from adjusted mid (cents).
pub fn order_score(v: f64, s: f64) -> f64 {
    if v <= 0.0 || s < 0.0 || s > v {
        return 0.0;
    }
    let r = (v - s) / v;
    r * r
}

const C: f64 = 3.0; // single-sided scaling factor (Polymarket current)

/// Two-sided minimum score. In [0.10, 0.90] single-sided scores at 1/C;
/// outside, liquidity must be two-sided or it scores zero.
pub fn q_min(q_one: f64, q_two: f64, mid: f64) -> f64 {
    if (0.10..=0.90).contains(&mid) {
        f64::max(q_one.min(q_two), f64::max(q_one / C, q_two / C))
    } else {
        q_one.min(q_two)
    }
}

/// One resting order for scoring: distance from adjusted mid (cents) and size (shares).
#[derive(Debug, Clone, Copy)]
pub struct ScoredOrder {
    pub spread_cents: f64,
    pub size: f64,
}

/// Q_min for a single-token two-sided quote set (bids -> Q1, asks -> Q2).
pub fn quote_set_q_min(v: f64, mid: f64, bids: &[ScoredOrder], asks: &[ScoredOrder]) -> f64 {
    let sum = |os: &[ScoredOrder]| os.iter().map(|o| order_score(v, o.spread_cents) * o.size).sum::<f64>();
    q_min(sum(bids), sum(asks), mid)
}

/// Rough $/day estimate = daily_rate * our_depth / (our_depth + competing_depth).
/// EXPLICITLY an estimate — true payout needs epoch-wide maker totals.
pub fn est_daily_reward_usd(daily_rate_usd: f64, our_in_band_depth: f64, competing_in_band_depth: f64) -> f64 {
    let denom = our_in_band_depth + competing_in_band_depth;
    if denom <= 0.0 { 0.0 } else { daily_rate_usd * our_in_band_depth / denom }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn order_score_matches_docs_worked_example() {
        let v = 3.0;
        let q1 = order_score(v, 1.0) * 100.0 + order_score(v, 2.0) * 200.0 + order_score(v, 1.0) * 100.0;
        assert!((q1 - 111.111).abs() < 0.01, "got {q1}");
    }

    #[test]
    fn out_of_band_scores_zero_and_qmin_two_sided() {
        assert_eq!(order_score(3.0, 4.0), 0.0);
        let q = q_min(60.0, 60.0, 0.50);
        assert!((q - 60.0).abs() < 1e-9);
        let q2 = q_min(90.0, 0.0, 0.50);
        assert!((q2 - 30.0).abs() < 1e-9);
        assert_eq!(q_min(90.0, 0.0, 0.95), 0.0);
    }

    #[test]
    fn quote_set_and_daily_reward_estimates() {
        // Balanced two-sided quote at mid 0.50: both sides score identically, so
        // Q_min collapses to that per-side score (the 1/C floor is the smaller term).
        let bids = [ScoredOrder { spread_cents: 1.0, size: 100.0 }];
        let asks = [ScoredOrder { spread_cents: 1.0, size: 100.0 }];
        let q = quote_set_q_min(3.0, 0.50, &bids, &asks);
        let expected = order_score(3.0, 1.0) * 100.0;
        assert!((q - expected).abs() < 1e-9, "got {q}, expected {expected}");

        assert_eq!(est_daily_reward_usd(100.0, 50.0, 50.0), 50.0);
        assert_eq!(est_daily_reward_usd(100.0, 0.0, 0.0), 0.0);
    }
}
