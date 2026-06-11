//! Class 2: NegRisk multi-outcome arbitrage (spec §8).

use std::collections::HashMap;

use crate::walker::{walk, BasketSpec, LegSpec};
use crate::{Action, ArbClass, EngineParams, Opportunity};
use pm_core::book::Book;
use pm_core::instrument::{Market, Partition, TokenId};

/// Detect underpriced YES sets (long) and underpriced NO sets (short, via the
/// NegRisk identity: a full NO set pays $(n−1) per unit at resolution).
pub fn detect(
    part: &Partition,
    markets: &[Market],
    books: &HashMap<TokenId, Book>,
    p: &EngineParams,
) -> Vec<Opportunity> {
    let mut out = Vec::new();
    if !part.verified_exhaustive || !part.is_well_formed() {
        return out;
    }
    let n = part.yes_tokens.len() as u64;

    let fee_map: HashMap<TokenId, pm_core::num::Bps> = markets
        .iter()
        .flat_map(|m| [(m.yes, m.fee_bps), (m.no, m.fee_bps)])
        .collect();

    let build = |tokens: &[TokenId]| -> Option<Vec<LegSpec<'_>>> {
        tokens
            .iter()
            .map(|&t| {
                Some(LegSpec {
                    token: t,
                    action: Action::Buy,
                    ladder: &books.get(&t)?.asks,
                    fee_bps: *fee_map.get(&t)?,
                })
            })
            .collect()
    };

    if let Some(legs) = build(&part.yes_tokens) {
        let spec = BasketSpec {
            legs,
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: p.gas.redeem,
        };
        if let Some(w) = walk(&spec, p.max_basis, p.min_profit, p.floor_c12) {
            out.push(w.into_opportunity(ArbClass::C2Long));
        }
    }
    if let Some(legs) = build(&part.no_tokens) {
        let spec = BasketSpec {
            legs,
            payout_per_share: (n - 1) * 1_000_000, // NegRisk identity: n NOs resolve to exactly $(n−1)
            collateral_per_share: 0,
            gas: p.gas.negrisk_convert,
        };
        if let Some(w) = walk(&spec, p.max_basis, p.min_profit, p.floor_c12) {
            out.push(w.into_opportunity(ArbClass::C2Short));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::instrument::{EventId, MarketId};
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    /// n-outcome partition; per outcome: (yes_ask, no_ask) at 100 shares deep.
    fn fixture(quotes: &[(u16, u16)], verified: bool) -> (Partition, Vec<Market>, HashMap<TokenId, Book>) {
        let n = quotes.len() as u32;
        let mut markets = Vec::new();
        let mut books = HashMap::new();
        let mut yes_tokens = Vec::new();
        let mut no_tokens = Vec::new();
        let mut market_ids = Vec::new();
        for (i, &(ya, na)) in quotes.iter().enumerate() {
            let i = i as u32;
            let yes = TokenId(u64::from(i) * 2 + 10);
            let no = TokenId(u64::from(i) * 2 + 11);
            markets.push(Market {
                id: MarketId(i),
                yes,
                no,
                tick: TS,
                fee_bps: Bps(0),
                neg_risk: true,
            });
            let mut yb = Book::new(TS);
            yb.apply(Side::Ask, px(ya), Qty(100_000_000));
            let mut nb = Book::new(TS);
            nb.apply(Side::Ask, px(na), Qty(100_000_000));
            books.insert(yes, yb);
            books.insert(no, nb);
            yes_tokens.push(yes);
            no_tokens.push(no);
            market_ids.push(MarketId(i));
        }
        let part = Partition {
            event: EventId(n),
            markets: market_ids,
            yes_tokens,
            no_tokens,
            verified_exhaustive: verified,
        };
        (part, markets, books)
    }

    fn zero_gas_params() -> EngineParams {
        EngineParams {
            gas: crate::GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[test]
    fn finds_long_when_yes_set_sums_below_one() {
        // 0.30 + 0.30 + 0.35 = 0.95 → 5¢/unit, NO asks fair (sum 2.10 > 2).
        let (part, markets, books) = fixture(&[(30, 72), (30, 72), (35, 66)], true);
        let ops = detect(&part, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C2Long);
        assert_eq!(ops[0].net, Usdc(5_000_000));
        assert_eq!(ops[0].fills.len(), 3);
        assert!(ops[0].splits.is_empty());
    }

    #[test]
    fn finds_short_via_cheap_no_set() {
        // NO asks 0.62+0.62+0.70 = 1.94 < n−1 = 2 → 6¢/unit. YES asks fair.
        let (part, markets, books) = fixture(&[(35, 62), (35, 62), (32, 70)], true);
        let ops = detect(&part, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C2Short);
        assert_eq!(ops[0].net, Usdc(6_000_000));
        assert!(ops[0].splits.is_empty());
    }

    #[test]
    fn unverified_partition_is_untouchable() {
        let (part, markets, books) = fixture(&[(30, 72), (30, 72), (35, 66)], false);
        assert!(detect(&part, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn malformed_partition_is_untouchable() {
        let (mut part, markets, books) = fixture(&[(30, 72), (30, 72), (35, 66)], true);
        part.no_tokens.pop(); // lanes no longer parallel
        assert!(detect(&part, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn fair_partition_yields_nothing() {
        // YES sums to 1.00, NO sums to 2.00 exactly.
        let (part, markets, books) = fixture(&[(33, 67), (33, 67), (34, 66)], true);
        assert!(detect(&part, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn missing_book_skips_cleanly() {
        let (part, markets, mut books) = fixture(&[(30, 72), (30, 72), (35, 66)], true);
        books.remove(&part.yes_tokens[2]);
        assert!(detect(&part, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn both_sets_can_fire_on_crossed_quotes() {
        // Degenerate: YES set 0.95 AND NO set 1.94 simultaneously cheap.
        let (part, markets, books) = fixture(&[(30, 62), (30, 62), (35, 70)], true);
        let ops = detect(&part, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().any(|o| o.class == ArbClass::C2Long));
        assert!(ops.iter().any(|o| o.class == ArbClass::C2Short));
    }
}
