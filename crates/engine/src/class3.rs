//! Class 3: cross-market logical arbitrage (spec §8). Buy-only, 100 bps floor.

use std::collections::HashMap;

use crate::walker::{walk, BasketSpec, LegSpec};
use crate::{Action, ArbClass, EngineParams, Opportunity};
use pm_core::book::Book;
use pm_core::instrument::{Market, Relationship, TokenId};

/// Buy `first` + buy `second`. The approved relationship guarantees at least
/// one leg pays $1 in every reachable world, so payout_per_share = $1 is a
/// floor, not an estimate. Class-3 floor (100 bps) applies.
fn pair_basket(
    class: ArbClass,
    first: LegSpec<'_>,
    second: LegSpec<'_>,
    p: &EngineParams,
) -> Option<Opportunity> {
    let spec = BasketSpec {
        legs: vec![first, second],
        payout_per_share: 1_000_000,
        collateral_per_share: 0,
        gas: p.gas.redeem,
    };
    walk(&spec, p.max_basis, p.min_profit, p.floor_c3).map(|w| w.into_opportunity(class))
}

/// Detect Dutch books across one approved relationship.
pub fn detect(
    rel: &Relationship,
    markets: &[Market],
    books: &HashMap<TokenId, Book>,
    p: &EngineParams,
) -> Vec<Opportunity> {
    let mut out = Vec::new();
    let leg = |token: TokenId, m: &Market| -> Option<LegSpec<'_>> {
        Some(LegSpec {
            token,
            action: Action::Buy,
            ladder: &books.get(&token)?.asks,
            fee_bps: m.fee_bps,
        })
    };
    match *rel {
        Relationship::Implies { a, b } => {
            let (Some(ma), Some(mb)) = (crate::find_market(markets, a), crate::find_market(markets, b)) else { return out };
            let (Some(no_a), Some(yes_b)) = (leg(ma.no, ma), leg(mb.yes, mb)) else { return out };
            if let Some(op) = pair_basket(ArbClass::C3Implies, no_a, yes_b, p) {
                out.push(op);
            }
        }
        Relationship::MutuallyExclusive { a, b } => {
            let (Some(ma), Some(mb)) = (crate::find_market(markets, a), crate::find_market(markets, b)) else { return out };
            let (Some(no_a), Some(no_b)) = (leg(ma.no, ma), leg(mb.no, mb)) else { return out };
            if let Some(op) = pair_basket(ArbClass::C3MutEx, no_a, no_b, p) {
                out.push(op);
            }
        }
        Relationship::Equivalent { a, b } => {
            let (Some(ma), Some(mb)) = (crate::find_market(markets, a), crate::find_market(markets, b)) else { return out };
            // a⇒b direction: buy NO_a + YES_b
            if let (Some(no_a), Some(yes_b)) = (leg(ma.no, ma), leg(mb.yes, mb))
                && let Some(op) = pair_basket(ArbClass::C3Equiv, no_a, yes_b, p) {
                    out.push(op);
                }
            // b⇒a direction: buy NO_b + YES_a (evaluated independently)
            if let (Some(no_b), Some(yes_a)) = (leg(mb.no, mb), leg(ma.yes, ma))
                && let Some(op) = pair_basket(ArbClass::C3Equiv, no_b, yes_a, p) {
                    out.push(op);
                }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::instrument::MarketId;
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    /// Two binary markets with the given (yes_ask, no_ask) quotes, 100sh deep.
    fn fixture(a: (u16, u16), b: (u16, u16)) -> (Vec<Market>, HashMap<TokenId, Book>) {
        let mut markets = Vec::new();
        let mut books = HashMap::new();
        for (i, &(ya, na)) in [a, b].iter().enumerate() {
            let i = i as u32;
            let yes = TokenId(u64::from(i) * 2 + 10);
            let no = TokenId(u64::from(i) * 2 + 11);
            markets.push(Market {
                id: MarketId(i),
                yes,
                no,
                tick: TS,
                fee_bps: Bps(0),
                neg_risk: false,
            });
            let mut yb = Book::new(TS);
            yb.apply(Side::Ask, px(ya), Qty(100_000_000));
            let mut nb = Book::new(TS);
            nb.apply(Side::Ask, px(na), Qty(100_000_000));
            books.insert(yes, yb);
            books.insert(no, nb);
        }
        (markets, books)
    }

    fn zero_gas_params() -> EngineParams {
        EngineParams {
            gas: crate::GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[test]
    fn implies_violation_is_tradable() {
        // P(A)≈0.65 (NO_a ask 0.35), YES_b ask 0.55 < P(A): A⇒B violated.
        // Basket NO_a + YES_b = 0.90 → 10¢/unit ≥ 100 bps.
        let (markets, books) = fixture((68, 35), (55, 50));
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C3Implies);
        assert_eq!(ops[0].net, Usdc(10_000_000));
        let toks: Vec<TokenId> = ops[0].fills.iter().map(|f| f.token).collect();
        assert_eq!(toks, vec![TokenId(11), TokenId(12)]); // NO_a, YES_b
        assert!(ops[0].splits.is_empty());
    }

    #[test]
    fn coherent_implication_is_quiet() {
        // NO_a 0.70 + YES_b 0.75 = 1.45 → no arb.
        let (markets, books) = fixture((32, 70), (75, 27));
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        assert!(detect(&rel, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn mutex_violation_is_tradable() {
        // NO_a 0.55 + NO_b 0.40 = 0.95 → 5¢/unit ≥ 100 bps.
        let (markets, books) = fixture((47, 55), (62, 40));
        let rel = Relationship::MutuallyExclusive { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C3MutEx);
        assert_eq!(ops[0].net, Usdc(5_000_000));
    }

    #[test]
    fn hundred_bps_floor_bites() {
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        // NO_a 0.50 + YES_b 0.50 = 1.00 → no profit at all.
        let (markets, books) = fixture((68, 50), (50, 51));
        assert!(detect(&rel, &markets, &books, &zero_gas_params()).is_empty());
        // NO_a 0.49 + YES_b 0.50 = 0.99 → ~101 bps ≥ 100 → trades.
        let (markets, books) = fixture((68, 49), (50, 52));
        assert_eq!(detect(&rel, &markets, &books, &zero_gas_params()).len(), 1);
    }

    #[test]
    fn equivalent_checks_both_directions() {
        // b⇒a direction: buy NO_b (0.45) + YES_a (0.40) = 0.85 → 15¢/unit.
        // a⇒b direction: NO_a (0.62) + YES_b (0.57) = 1.19 → quiet.
        let (markets, books) = fixture((40, 62), (57, 45));
        let rel = Relationship::Equivalent { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C3Equiv);
        assert_eq!(ops[0].net, Usdc(15_000_000));
    }

    #[test]
    fn missing_market_or_book_is_quiet() {
        let (markets, mut books) = fixture((68, 35), (55, 50));
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(7) }; // unknown market
        assert!(detect(&rel, &markets, &books, &zero_gas_params()).is_empty());
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        books.remove(&TokenId(12)); // YES_b book gone
        assert!(detect(&rel, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn equivalent_directions_are_independent_under_missing_books() {
        // b⇒a is priced (NO_b 0.45 + YES_a 0.40 = 0.85); remove YES_b's book so
        // a⇒b can't even be evaluated — b⇒a must still fire.
        let (markets, mut books) = fixture((40, 62), (57, 45));
        books.remove(&TokenId(12)); // YES_b
        let rel = Relationship::Equivalent { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].net, Usdc(15_000_000));
    }

    #[test]
    fn equivalent_both_directions_can_fire() {
        // Degenerate: NO_a+YES_b = 0.35+0.40 = 0.75 AND NO_b+YES_a = 0.45+0.30 = 0.75.
        let (markets, books) = fixture((30, 35), (40, 45));
        let rel = Relationship::Equivalent { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|o| o.class == ArbClass::C3Equiv));
    }

    #[test]
    fn coherent_mutex_is_quiet() {
        // NO_a 0.60 + NO_b 0.55 = 1.15 → no arb.
        let (markets, books) = fixture((42, 60), (47, 55));
        let rel = Relationship::MutuallyExclusive { a: MarketId(0), b: MarketId(1) };
        assert!(detect(&rel, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn fees_shrink_class3_net() {
        // Same books as implies_violation but 100 bps fee on both markets:
        // fee/share = 1% × (min(0.35,0.65) + min(0.55,0.45)) = 1% × 0.80 = 0.8¢
        // → net = $10 − $0.80 = $9.20 on 100sh.
        let (mut markets, books) = fixture((68, 35), (55, 50));
        for m in &mut markets {
            m.fee_bps = Bps(100);
        }
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].net, Usdc(9_200_000));
    }
}
