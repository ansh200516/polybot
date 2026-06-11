//! Class 1: binary complete-set arbitrage (spec §8).

use crate::walker::{walk, BasketSpec, LegSpec};
use crate::{Action, ArbClass, EngineParams, Opportunity, RedeemStrategy};
use pm_core::book::Book;
use pm_core::instrument::Market;

/// Detect long (cheap set) and short (rich set) complete-set arbs.
pub fn detect(m: &Market, yes: &Book, no: &Book, p: &EngineParams) -> Vec<Opportunity> {
    let mut out = Vec::new();
    let long_gas = match p.redeem {
        RedeemStrategy::Merge => p.gas.merge,
        RedeemStrategy::Hold => p.gas.redeem,
    };
    let long = BasketSpec {
        legs: vec![
            LegSpec { token: m.yes, action: Action::Buy, ladder: &yes.asks, fee_bps: m.fee_bps },
            LegSpec { token: m.no, action: Action::Buy, ladder: &no.asks, fee_bps: m.fee_bps },
        ],
        payout_per_share: 1_000_000,
        collateral_per_share: 0,
        gas: long_gas,
    };
    if let Some(w) = walk(&long, p.max_basis, p.min_profit, p.floor_c12) {
        out.push(w.into_opportunity(ArbClass::C1Long));
    }
    let short = BasketSpec {
        legs: vec![
            LegSpec { token: m.yes, action: Action::Sell, ladder: &yes.bids, fee_bps: m.fee_bps },
            LegSpec { token: m.no, action: Action::Sell, ladder: &no.bids, fee_bps: m.fee_bps },
        ],
        payout_per_share: 0,
        collateral_per_share: 1_000_000,
        gas: p.gas.split,
    };
    if let Some(w) = walk(&short, p.max_basis, p.min_profit, p.floor_c12) {
        let units = w.units;
        let mut op = w.into_opportunity(ArbClass::C1Short);
        op.splits = vec![(m.id, units)]; // execution must split before selling
        out.push(op);
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::instrument::{MarketId, TokenId};
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    fn market(fee: i32) -> Market {
        Market {
            id: MarketId(1),
            yes: TokenId(10),
            no: TokenId(11),
            tick: TS,
            fee_bps: Bps(fee),
            neg_risk: false,
        }
    }

    fn books(yes_ask: u16, no_ask: u16, yes_bid: u16, no_bid: u16, q: u64) -> (Book, Book) {
        let mut yes = Book::new(TS);
        let mut no = Book::new(TS);
        yes.apply(Side::Ask, px(yes_ask), Qty(q));
        no.apply(Side::Ask, px(no_ask), Qty(q));
        yes.apply(Side::Bid, px(yes_bid), Qty(q));
        no.apply(Side::Bid, px(no_bid), Qty(q));
        (yes, no)
    }

    fn zero_gas_params() -> EngineParams {
        EngineParams {
            gas: crate::GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[test]
    fn finds_long_when_set_is_cheap() {
        // asks 0.46+0.52 = 0.98; bids fair.
        let (yes, no) = books(46, 52, 44, 50, 100_000_000);
        let ops = detect(&market(0), &yes, &no, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C1Long);
        assert_eq!(ops[0].net, Usdc(2_000_000));
        assert_eq!(ops[0].fills[0].action, Action::Buy);
        assert!(ops[0].splits.is_empty());
    }

    #[test]
    fn finds_short_when_set_is_rich() {
        // bids 0.55+0.50 = 1.05; asks fair.
        let (yes, no) = books(57, 52, 55, 50, 100_000_000);
        let ops = detect(&market(0), &yes, &no, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C1Short);
        assert_eq!(ops[0].net, Usdc(5_000_000)); // 5¢ × 100
        assert_eq!(ops[0].basis, Usdc(100_000_000)); // split collateral
        assert_eq!(ops[0].splits, vec![(MarketId(1), Qty(100_000_000))]);
    }

    #[test]
    fn fair_books_yield_nothing() {
        let (yes, no) = books(51, 50, 49, 48, 100_000_000);
        assert!(detect(&market(0), &yes, &no, &zero_gas_params()).is_empty());
    }

    #[test]
    fn thirty_bps_floor_filters_thin_edges() {
        // asks sum to 0.998 on a Milli book → ~20 bps gross: below the 30 floor.
        let ts = TickSize::Milli;
        let mk_px = |t: u16| Px::new(t, ts).unwrap();
        let mut yes = Book::new(ts);
        let mut no = Book::new(ts);
        yes.apply(Side::Ask, mk_px(499), Qty(100_000_000));
        no.apply(Side::Ask, mk_px(499), Qty(100_000_000));
        let m = Market { tick: ts, ..market(0) };
        assert!(detect(&m, &yes, &no, &zero_gas_params()).is_empty());
    }

    #[test]
    fn redeem_strategy_picks_gas() {
        let (yes, no) = books(46, 52, 44, 50, 100_000_000);
        let mut p = zero_gas_params();
        p.gas.merge = 500_000; // $0.50
        p.gas.redeem = 1_500_000;
        p.redeem = RedeemStrategy::Merge;
        let merge_net = detect(&market(0), &yes, &no, &p)[0].net;
        p.redeem = RedeemStrategy::Hold;
        let hold_net = detect(&market(0), &yes, &no, &p)[0].net;
        assert_eq!(merge_net, Usdc(1_500_000));
        assert_eq!(hold_net, Usdc(500_000));
    }

    #[test]
    fn both_sides_can_fire_on_a_crossed_market() {
        // Degenerate books: cheap asks AND rich bids (won't happen live; math
        // must still be independent per side).
        let (yes, no) = books(46, 50, 55, 52, 100_000_000);
        let ops = detect(&market(0), &yes, &no, &zero_gas_params());
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().any(|o| o.class == ArbClass::C1Long));
        assert!(ops.iter().any(|o| o.class == ArbClass::C1Short));
    }
}
