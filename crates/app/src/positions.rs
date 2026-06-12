//! Session position book: per-token holdings (qty + cost basis), cash, marks.

use std::collections::HashMap;

use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{Qty, Usdc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pnl {
    pub cash: Usdc,
    pub realized: Usdc,
    pub unrealized: Usdc,
    pub equity: Usdc,
}

#[derive(Default)]
pub struct PositionBook {
    /// token → (qty µshares, cost basis µUSDC).
    pos: HashMap<TokenId, (u64, i128)>,
    cash: i128,
}

impl PositionBook {
    /// Apply a basket report's leftovers + cash. Returns per-market exposure
    /// deltas (cost basis added) for `RiskEngine::commit`, sorted by market.
    pub fn apply(
        &mut self,
        positions: &[(TokenId, Qty, Usdc)],
        cash_delta: Usdc,
        token_market: &HashMap<TokenId, MarketId>,
    ) -> Vec<(MarketId, Usdc)> {
        self.cash += cash_delta.0;
        let mut by_market: HashMap<MarketId, i128> = HashMap::new();
        for &(token, qty, cost) in positions {
            let e = self.pos.entry(token).or_insert((0, 0));
            e.0 += qty.0;
            e.1 += cost.0;
            if let Some(&m) = token_market.get(&token) {
                *by_market.entry(m).or_insert(0) += cost.0;
            }
        }
        let mut deltas: Vec<(MarketId, Usdc)> =
            by_market.into_iter().map(|(m, c)| (m, Usdc(c))).collect();
        deltas.sort_by_key(|(m, _)| *m);
        deltas
    }

    pub fn cash(&self) -> Usdc {
        Usdc(self.cash)
    }

    pub fn holdings(&self) -> Vec<(TokenId, Qty, Usdc)> {
        let mut out: Vec<(TokenId, Qty, Usdc)> = self
            .pos
            .iter()
            .filter(|(_, (q, _))| *q > 0)
            .map(|(&t, &(q, c))| (t, Qty(q), Usdc(c)))
            .collect();
        out.sort_by_key(|(t, _, _)| *t);
        out
    }

    /// P&L given conservative marks (bid-side value per held token).
    /// Tokens missing from `marks` are valued at 0 (maximally conservative).
    pub fn pnl(&self, marks: &HashMap<TokenId, Usdc>) -> Pnl {
        let mut mark_value: i128 = 0;
        let mut basis: i128 = 0;
        for (&t, &(q, c)) in &self.pos {
            if q == 0 {
                continue;
            }
            basis += c;
            mark_value += marks.get(&t).map(|m| m.0).unwrap_or(0);
        }
        let equity = self.cash + mark_value;
        let unrealized = mark_value - basis;
        let realized = equity - unrealized;
        Pnl {
            cash: Usdc(self.cash),
            realized: Usdc(realized),
            unrealized: Usdc(unrealized),
            equity: Usdc(equity),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn apply_report_accumulates_and_returns_market_deltas() {
        let tm = HashMap::from([(TokenId(1), MarketId(0)), (TokenId(2), MarketId(0))]);
        let mut pb = PositionBook::default();
        let deltas = pb.apply(
            &[
                (TokenId(1), Qty(100), Usdc(44)),
                (TokenId(2), Qty(100), Usdc(50)),
            ],
            Usdc(-94),
            &tm,
        );
        assert_eq!(pb.cash(), Usdc(-94));
        assert_eq!(deltas, vec![(MarketId(0), Usdc(94))]);
        // a clean-merge report: cash up, no leftovers, no deltas
        let deltas = pb.apply(&[], Usdc(5_990_000), &tm);
        assert!(deltas.is_empty());
        assert_eq!(pb.cash(), Usdc(5_990_000 - 94));
        assert_eq!(pb.holdings().len(), 2);
    }

    #[test]
    fn equity_is_cash_plus_marks() {
        let tm = HashMap::from([(TokenId(1), MarketId(0))]);
        let mut pb = PositionBook::default();
        pb.apply(
            &[(TokenId(1), Qty(100_000_000), Usdc(44_000_000))],
            Usdc(-44_000_000),
            &tm,
        );
        let marks = HashMap::from([(TokenId(1), Usdc(42_000_000))]);
        let pnl = pb.pnl(&marks);
        assert_eq!(pnl.cash, Usdc(-44_000_000));
        assert_eq!(pnl.unrealized, Usdc(-2_000_000));
        assert_eq!(pnl.equity, Usdc(-2_000_000));
        assert_eq!(pnl.realized, Usdc(0));
    }
}
