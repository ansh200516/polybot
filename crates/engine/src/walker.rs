//! Generalized depth walker (spec §7): exact sizing for multi-leg baskets.

use crate::{Action, LegFill};
use pm_core::book::Ladder;
use pm_core::fees::fee_microusdc;
use pm_core::instrument::TokenId;
use pm_core::num::{buy_cost, edge_bps, sell_proceeds, Bps, Px, Qty, Usdc};

#[derive(Clone, Copy, Debug)]
pub struct LegSpec<'a> {
    pub token: TokenId,
    pub action: Action,
    pub ladder: &'a Ladder,
    pub fee_bps: Bps,
}

#[derive(Clone, Debug)]
pub struct BasketSpec<'a> {
    pub legs: Vec<LegSpec<'a>>,
    /// µUSDC received per whole share-unit at resolution/merge.
    pub payout_per_share: u64,
    /// µUSDC paid per whole share-unit up front (splits).
    pub collateral_per_share: u64,
    /// Flat µUSDC per basket.
    pub gas: u64,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WalkResult {
    pub units: Qty,
    pub net: Usdc,
    pub basis: Usdc,
    pub edge: Bps,
    pub fills: Vec<LegFill>,
}

/// Marginal value of one share of `leg` at price `pm`, scaled ×10⁴ so the fee
/// term stays integral. Positive contributions reduce profit for buys.
fn scaled_leg_cost(action: Action, fee_bps: Bps, pm: u64) -> i128 {
    let p = i128::from(pm);
    let fee = i128::from(fee_bps.0.max(0)) * i128::from(pm.min(1_000_000 - pm));
    match action {
        Action::Buy => 10_000 * p + fee,
        Action::Sell => -(10_000 * p - fee),
    }
}

struct Cursor<'a, I: Iterator<Item = (Px, Qty)>> {
    leg: LegSpec<'a>,
    levels: I,
    cur: Option<(Px, u64)>, // price, remaining at level
    segs: Vec<(Px, u64)>,   // (price, filled) per touched level
}

/// Size the basket against depth. Returns None when nothing clears the gates.
pub fn walk(
    spec: &BasketSpec,
    max_basis: Usdc,
    min_profit: Usdc,
    floor: Bps,
) -> Option<WalkResult> {
    if spec.legs.is_empty() {
        return None;
    }
    let mut cursors: Vec<Cursor<_>> = spec
        .legs
        .iter()
        .map(|leg| {
            let mut levels = leg.ladder.iter_from_best();
            let cur = levels.next().map(|(p, q)| (p, q.0));
            Cursor { leg: *leg, levels, cur, segs: Vec::new() }
        })
        .collect();

    let fixed_scaled =
        10_000 * (i128::from(spec.payout_per_share) - i128::from(spec.collateral_per_share));
    let mut units: u64 = 0;
    let mut basis_used: i128 = 0; // µUSDC committed so far (approximate, for the cap)

    loop {
        // Marginal scaled net per share at the current level combo.
        let mut marginal = fixed_scaled;
        let mut chunk = u64::MAX;
        let mut basis_per_share_scaled: i128 = 10_000 * i128::from(spec.collateral_per_share);
        let mut exhausted = false;
        for c in &cursors {
            match c.cur {
                Some((p, rem)) => {
                    let pm = p.microusdc(c.leg.ladder.ts());
                    let cost = scaled_leg_cost(c.leg.action, c.leg.fee_bps, pm);
                    marginal -= cost;
                    if c.leg.action == Action::Buy {
                        basis_per_share_scaled += cost;
                    }
                    chunk = chunk.min(rem);
                }
                None => {
                    exhausted = true;
                }
            }
        }
        if exhausted || marginal <= 0 || chunk == 0 || chunk == u64::MAX {
            break;
        }
        if basis_per_share_scaled > 0 {
            let remaining = max_basis.0.saturating_sub(basis_used);
            if remaining <= 0 {
                break;
            }
            // micro-shares allowed = remaining µUSDC × 10⁴ × 10⁶ / scaled
            // basis per share. Clamp `remaining` so the multiply can't
            // overflow i128 even when max_basis is i128::MAX in tests.
            let rem = remaining.min(1_000_000_000_000_000_000);
            let cap = (rem * 10_000_000_000 / basis_per_share_scaled)
                .min(i128::from(u64::MAX)) as u64;
            if cap == 0 {
                break;
            }
            chunk = chunk.min(cap);
        }
        // Take the chunk on every leg.
        for c in &mut cursors {
            if let Some((p, rem)) = c.cur {
                let new_rem = rem - chunk;
                match c.segs.last_mut() {
                    Some(last) if last.0 == p => last.1 += chunk,
                    _ => c.segs.push((p, chunk)),
                }
                c.cur = if new_rem == 0 { c.levels.next().map(|(p, q)| (p, q.0)) } else { Some((p, new_rem)) };
            }
        }
        units += chunk;
        basis_used += basis_per_share_scaled * i128::from(chunk) / (10_000 * 1_000_000);
    }

    if units == 0 {
        return None;
    }

    // Exact accounting over recorded segments (authoritative).
    let mut cash: i128 = 0;
    let mut buy_outlay: i128 = 0;
    let mut fills = Vec::with_capacity(cursors.len());
    for c in &cursors {
        let ts = c.leg.ladder.ts();
        let mut leg_cash: i128 = 0;
        let mut worst: Option<Px> = None;
        let mut qty: u64 = 0;
        for &(p, q) in &c.segs {
            let pm = p.microusdc(ts);
            let fee = fee_microusdc(c.leg.fee_bps, pm, Qty(q)).0;
            match c.leg.action {
                Action::Buy => {
                    let cost = buy_cost(pm, Qty(q)).0 + fee;
                    leg_cash -= cost;
                    buy_outlay += cost;
                }
                Action::Sell => leg_cash += sell_proceeds(pm, Qty(q)).0 - fee,
            }
            worst = Some(p); // segments are walked best→worst
            qty += q;
        }
        cash += leg_cash;
        let worst = worst?;
        fills.push(LegFill {
            token: c.leg.token,
            action: c.leg.action,
            ts,
            limit_px: worst,
            qty: Qty(qty),
            cash: Usdc(leg_cash),
        });
    }
    // Payout floors (income), collateral ceils (cost). units is micro-shares,
    // payout/collateral are per whole share → ÷10⁶ with against-us rounding.
    let payout = i128::from(spec.payout_per_share) * i128::from(units) / 1_000_000;
    let collateral =
        (i128::from(spec.collateral_per_share) * i128::from(units) + 999_999) / 1_000_000;
    let net = Usdc(cash + payout - collateral - i128::from(spec.gas));
    let basis = Usdc(buy_outlay + collateral);

    if net < min_profit {
        return None;
    }
    let edge = edge_bps(net, basis)?;
    if edge < floor {
        return None;
    }
    Some(WalkResult { units: Qty(units), net, basis, edge, fills })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::num::TickSize;
    use proptest::prelude::*;

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    fn ladder(side: Side, lvls: &[(u16, u64)]) -> Ladder {
        let mut l = Ladder::new(TS, side);
        for &(t, q) in lvls {
            l.set(px(t), Qty(q));
        }
        l
    }

    fn buy_leg(token: u64, l: &Ladder, fee: i32) -> LegSpec<'_> {
        LegSpec { token: TokenId(token), action: Action::Buy, ladder: l, fee_bps: Bps(fee) }
    }

    fn sell_leg(token: u64, l: &Ladder, fee: i32) -> LegSpec<'_> {
        LegSpec { token: TokenId(token), action: Action::Sell, ladder: l, fee_bps: Bps(fee) }
    }

    /// Independent exact recompute of basket net at `units`, for cross-checking.
    fn brute_net(spec: &BasketSpec, units: u64) -> i128 {
        let mut cash: i128 = 0;
        for leg in &spec.legs {
            let mut remaining = units;
            for (p, q) in leg.ladder.iter_from_best() {
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(q.0);
                let pm = p.microusdc(leg.ladder.ts());
                let fee = fee_microusdc(leg.fee_bps, pm, Qty(take)).0;
                match leg.action {
                    Action::Buy => cash -= buy_cost(pm, Qty(take)).0 + fee,
                    Action::Sell => cash += sell_proceeds(pm, Qty(take)).0 - fee,
                }
                remaining -= take;
            }
            assert_eq!(remaining, 0, "brute_net called beyond depth");
        }
        let payout = (spec.payout_per_share as i128 * units as i128) / 1_000_000;
        let collateral = (spec.collateral_per_share as i128 * units as i128 + 999_999) / 1_000_000;
        cash + payout - collateral - spec.gas as i128
    }

    #[test]
    fn class1_long_shape_full_depth() {
        // YES asks 0.46×100sh, NO asks 0.52×100sh, no fees/gas → 2¢/unit, 100 sh.
        let yes = ladder(Side::Ask, &[(46, 100_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(100_000_000));
        assert_eq!(w.net, Usdc(2_000_000)); // $2
        assert_eq!(w.basis, Usdc(98_000_000)); // $98
        assert_eq!(w.edge, Bps(204));
        assert_eq!(w.fills.len(), 2);
        assert_eq!(w.fills[0].limit_px, px(46));
        assert_eq!(w.fills[1].limit_px, px(52));
    }

    #[test]
    fn stops_at_unprofitable_level() {
        // Second YES level pushes the sum past $1 → only first level taken.
        let yes = ladder(Side::Ask, &[(46, 50_000_000), (49, 50_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(50_000_000));
        assert_eq!(w.net, Usdc(1_000_000)); // 2¢ × 50
    }

    #[test]
    fn sell_basket_split_and_dump() {
        // Bids: YES 0.55×40sh, NO 0.50×40sh → split $1, sell at $1.05 → 5¢/unit.
        let yes = ladder(Side::Bid, &[(55, 40_000_000)]);
        let no = ladder(Side::Bid, &[(50, 40_000_000)]);
        let spec = BasketSpec {
            legs: vec![sell_leg(1, &yes, 0), sell_leg(2, &no, 0)],
            payout_per_share: 0,
            collateral_per_share: 1_000_000,
            gas: 0,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(40_000_000));
        assert_eq!(w.net, Usdc(2_000_000)); // 5¢ × 40
        assert_eq!(w.basis, Usdc(40_000_000)); // collateral $40
        assert!(w.fills.iter().all(|f| f.cash.0 > 0)); // sells bring cash in
    }

    #[test]
    fn fees_and_gas_reduce_net_exactly() {
        let yes = ladder(Side::Ask, &[(46, 100_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 100), buy_leg(2, &no, 100)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 123_456,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        // fees: 1% × min(p,1−p) per share per leg = (0.46+0.48)¢ ×100 sh = $0.94
        let expected_fees = 460_000 + 480_000;
        assert_eq!(w.net, Usdc(2_000_000 - expected_fees - 123_456));
    }

    #[test]
    fn respects_max_basis_cap() {
        let yes = ladder(Side::Ask, &[(46, 100_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        // $9.80 cap → exactly 10 shares (98¢ basis each).
        let w = walk(&spec, Usdc(9_800_000), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(10_000_000));
        assert!(w.basis <= Usdc(9_800_000));
    }

    #[test]
    fn gates_min_profit_and_floor() {
        let yes = ladder(Side::Ask, &[(49, 100_000_000)]);
        let no = ladder(Side::Ask, &[(50, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        // 1¢/unit on 99¢ basis ≈ 101 bps: passes 30, fails 150.
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(30)).is_some());
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(150)).is_none());
        // min_profit above $1 total → rejected.
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(1_100_000), Bps(0)).is_none());
    }

    #[test]
    fn no_edge_returns_none() {
        let yes = ladder(Side::Ask, &[(50, 100_000_000)]);
        let no = ladder(Side::Ask, &[(51, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).is_none());
    }

    proptest! {
        /// Walker's chosen extent is optimal among level boundaries and its
        /// accounting matches the independent recompute.
        #[test]
        fn optimal_at_level_boundaries(
            ya in 30u16..70, yq1 in 1u64..30_000_000, yq2 in 1u64..30_000_000,
            na in 30u16..70, nq1 in 1u64..30_000_000, nq2 in 1u64..30_000_000,
            fee in 0i32..200, gas in 0u64..50_000,
        ) {
            let yes = ladder(Side::Ask, &[(ya, yq1), (ya + 20, yq2)]);
            let no = ladder(Side::Ask, &[(na, nq1), (na + 20, nq2)]);
            let spec = BasketSpec {
                legs: vec![buy_leg(1, &yes, fee), buy_leg(2, &no, fee)],
                payout_per_share: 1_000_000,
                collateral_per_share: 0,
                gas,
            };
            let res = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0));
            // candidate extents: every level-boundary prefix
            let depths = [yq1, yq1 + yq2, nq1, nq1 + nq2];
            let max_units = (yq1 + yq2).min(nq1 + nq2);
            let mut best: i128 = 0;
            for &d in depths.iter() {
                let u = d.min(max_units);
                let n = brute_net(&spec, u);
                if n > best { best = n; }
            }
            match res {
                Some(w) => {
                    prop_assert_eq!(w.net.0, brute_net(&spec, w.units.0));
                    prop_assert!(w.net.0 >= best,
                        "walker {} < best boundary {}", w.net.0, best);
                }
                None => prop_assert!(best <= 0, "walker missed profit {}", best),
            }
        }
    }
}
