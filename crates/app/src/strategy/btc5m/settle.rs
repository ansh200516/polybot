//! Pure realized-PnL for a resolved btc5m binary position. Winners redeem to $1/
//! share, losers to $0. No I/O — the outcome comes from Gamma (settle sweep).

/// Realized µUSDC for a resolved binary position. `outcome_up` = did UP win;
/// `bought_up` = did we buy the UP/YES token. `qty_micro` µshares, `cost_micro`
/// µUSDC paid. Win: shares redeem to $1 each → proceeds = qty_micro µUSDC, so
/// PnL = qty_micro − cost_micro. Lose: shares are worthless → PnL = −cost_micro.
pub fn realized_micro(outcome_up: bool, bought_up: bool, qty_micro: i64, cost_micro: i64) -> i64 {
    if outcome_up == bought_up { qty_micro - cost_micro } else { -cost_micro }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    #[test]
    fn realized_pnl_win_and_loss() {
        // bought 11 shares UP for $9.90; window resolves UP → +$1.10
        assert_eq!(realized_micro(true, true, 11_000_000, 9_900_000), 1_100_000);
        // resolves DOWN → lose the $9.90 cost
        assert_eq!(realized_micro(false, true, 11_000_000, 9_900_000), -9_900_000);
        // bought DOWN, resolves DOWN → win
        assert_eq!(realized_micro(false, false, 11_000_000, 9_900_000), 1_100_000);
        // bought DOWN, resolves UP → lose
        assert_eq!(realized_micro(true, false, 11_000_000, 9_900_000), -9_900_000);
    }
}
