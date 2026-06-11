//! Class 4: unified LP detector (spec §10). Part A: worlds + exact reval.

use std::collections::HashMap;

use crate::{Action, LegFill};
use pm_core::book::Book;
use pm_core::fees::fee_microusdc;
use pm_core::instrument::{Market, MarketId, Partition, Relationship, TokenId};
use pm_core::num::{buy_cost, sell_proceeds, Qty, Usdc};

/// A logical component: the set of markets + partitions + relationships +
/// live books the LP will solve over.
#[derive(Clone, Debug)]
pub struct ComponentSpec<'a> {
    pub markets: Vec<Market>,
    pub partitions: Vec<Partition>,
    pub relationships: Vec<Relationship>,
    pub books: &'a HashMap<TokenId, Book>,
}

/// One resolution world: truth-value of every market's YES outcome, stored in
/// `ComponentSpec::markets` order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct World {
    yes_true: Vec<bool>,
}

/// A candidate solution the LP solver emits (or that tests inject directly).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LpSolution {
    pub fills: Vec<LegFill>,
    /// Each entry is `(market_id, qty_micro_shares)` — split that many complete
    /// sets before the fills.
    pub splits: Vec<(MarketId, Qty)>,
}

// ---------------------------------------------------------------------------
// token_pays
// ---------------------------------------------------------------------------

/// Returns `Some(true)` if `token` pays $1 in world `w`, `Some(false)` if it
/// pays $0, and `None` if the token is unknown (not in `spec.markets`).
pub fn token_pays(spec: &ComponentSpec, w: &World, token: TokenId) -> Option<bool> {
    for (idx, m) in spec.markets.iter().enumerate() {
        if m.yes == token {
            return Some(w.yes_true[idx]);
        }
        if m.no == token {
            return Some(!w.yes_true[idx]);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// enumerate_worlds
// ---------------------------------------------------------------------------

/// Returns the index of `id` in `spec.markets`, or `None`.
fn market_index(spec: &ComponentSpec, id: MarketId) -> Option<usize> {
    spec.markets.iter().position(|m| m.id == id)
}

/// Check whether `world` satisfies every relationship in `spec`.
/// Relationships referencing markets outside the component are treated as
/// trivially satisfied (unknown id → `None` → skip).
fn consistent(spec: &ComponentSpec, w: &World) -> bool {
    for rel in &spec.relationships {
        match *rel {
            Relationship::Implies { a, b } => {
                let Some(ai) = market_index(spec, a) else { continue };
                let Some(bi) = market_index(spec, b) else { continue };
                // Implies: ¬a ∨ b
                if w.yes_true[ai] && !w.yes_true[bi] {
                    return false;
                }
            }
            Relationship::MutuallyExclusive { a, b } => {
                let Some(ai) = market_index(spec, a) else { continue };
                let Some(bi) = market_index(spec, b) else { continue };
                // MutEx: ¬(a ∧ b)
                if w.yes_true[ai] && w.yes_true[bi] {
                    return false;
                }
            }
            Relationship::Equivalent { a, b } => {
                let Some(ai) = market_index(spec, a) else { continue };
                let Some(bi) = market_index(spec, b) else { continue };
                // Equivalent: a == b
                if w.yes_true[ai] != w.yes_true[bi] {
                    return false;
                }
            }
        }
    }
    true
}

/// Enumerate all worlds consistent with the component's relationships.
///
/// Returns `None` if the pre-prune cartesian-product size exceeds `max_worlds`.
///
/// Each partition contributes exactly one winning YES outcome (the others are
/// false). Markets that belong to no partition are "free" binary variables.
pub fn enumerate_worlds(spec: &ComponentSpec, max_worlds: usize) -> Option<Vec<World>> {
    debug_assert!(spec.partitions.iter().all(Partition::is_well_formed));

    let n_markets = spec.markets.len();

    // Which partition owns each market index (if any)?
    let mut partition_owner: Vec<Option<usize>> = vec![None; n_markets];
    for (pi, part) in spec.partitions.iter().enumerate() {
        for mid in &part.markets {
            if let Some(idx) = market_index(spec, *mid) {
                partition_owner[idx] = Some(pi);
            }
        }
    }

    // Free markets: those not owned by any partition.
    let free_indices: Vec<usize> =
        (0..n_markets).filter(|i| partition_owner[*i].is_none()).collect();
    let n_free = free_indices.len();

    // Pre-prune size = Π partition_sizes × 2^free_markets.
    // Use u128 with saturating mul so we never overflow on absurd inputs.
    let mut preprune: u128 = 1u128;
    for part in &spec.partitions {
        preprune = preprune.saturating_mul(part.markets.len() as u128);
    }
    // 2^n_free, saturating at u128::MAX to avoid overflow.
    let free_combos: u128 = if n_free >= 128 {
        u128::MAX
    } else {
        1u128 << n_free
    };
    preprune = preprune.saturating_mul(free_combos);

    if preprune > max_worlds as u128 {
        return None;
    }

    // Generate worlds via odometer: iterate partition winner choices × free
    // market bitmask.
    //
    // For each partition p with k members, `part_choice[p]` ∈ 0..k is the
    // index of the winning outcome. For free markets, `free_mask` is a bitmask
    // (bit i = 1 means YES is true for free_indices[i]).

    let n_partitions = spec.partitions.len();
    let mut part_choice: Vec<usize> = vec![0; n_partitions];
    let mut worlds = Vec::with_capacity(preprune as usize);

    // Outer loop: all partition combinations via manual odometer.
    // Inner loop: all free-market bitmasks.
    loop {
        for free_mask in 0u64..(1u64 << n_free) {
            // Build yes_true array.
            let mut yes_true = vec![false; n_markets];

            // Apply partition choices.
            for (pi, part) in spec.partitions.iter().enumerate() {
                let winner_mid = part.markets[part_choice[pi]];
                for mid in &part.markets {
                    if let Some(idx) = market_index(spec, *mid) {
                        yes_true[idx] = *mid == winner_mid;
                    }
                }
            }

            // Apply free-market bits.
            for (fi, &mi) in free_indices.iter().enumerate() {
                yes_true[mi] = (free_mask >> fi) & 1 == 1;
            }

            let w = World { yes_true };
            if consistent(spec, &w) {
                worlds.push(w);
            }
        }

        // Advance partition odometer.
        if n_partitions == 0 {
            break;
        }
        let mut carry = true;
        for pi in (0..n_partitions).rev() {
            if carry {
                part_choice[pi] += 1;
                if part_choice[pi] < spec.partitions[pi].markets.len() {
                    carry = false;
                    break;
                }
                part_choice[pi] = 0;
            }
        }
        if carry {
            // Odometer wrapped — all partition combinations exhausted.
            break;
        }
    }

    Some(worlds)
}

// ---------------------------------------------------------------------------
// exact_worst_net
// ---------------------------------------------------------------------------

/// Find the `Market` in `spec.markets` that owns `token` (yes or no side).
fn find_market_for_token<'a>(spec: &'a ComponentSpec, token: TokenId) -> Option<&'a Market> {
    spec.markets.iter().find(|m| m.yes == token || m.no == token)
}

/// Exact re-validation of a candidate LP solution.
///
/// Recomputes cash and positions from `limit_px`/`qty` (ignoring the `cash`
/// field in each `LegFill` — it may be a solver approximation or a test lie).
/// Then evaluates `cash + payoff − gas` in every world and returns
/// `Some((worst, basis))`.
///
/// Returns `None` if any token ends up in a naked-short position (net negative
/// holding after all fills and splits).
///
/// Payoff: each micro-share of a paying token contributes exactly 1 µUSDC
/// (at $1/share × qty µshares, $1/share cancels the /1e6 shares-per-unit
/// since we work in µshares and µUSDC throughout).
pub fn exact_worst_net(
    spec: &ComponentSpec,
    worlds: &[World],
    sol: &LpSolution,
    gas_micro: u64,
) -> Option<(Usdc, Usdc)> {
    // Accumulate signed positions (µshares) and signed cash (µUSDC).
    let mut positions: HashMap<TokenId, i128> = HashMap::new();
    let mut cash: i128 = 0i128;
    let mut basis: i128 = 0i128;

    // Process splits first: each split spends $1 collateral per share and
    // creates one YES + one NO micro-share per micro-share split.
    // collateral = qty µshares × $1/share = qty µUSDC exactly.
    for &(market_id, qty) in &sol.splits {
        let market = crate::find_market(&spec.markets, market_id)?;
        // 1 µshare of a $1 instrument = 1 µUSDC collateral.
        let collateral = qty.0 as i128;
        cash -= collateral;
        basis += collateral;
        *positions.entry(market.yes).or_insert(0) += qty.0 as i128;
        *positions.entry(market.no).or_insert(0) += qty.0 as i128;
    }

    // Process fills: recompute cash from limit_px/qty, ignoring sol.fills[i].cash.
    for fill in &sol.fills {
        let market = find_market_for_token(spec, fill.token)?;
        let pm = fill.limit_px.microusdc(fill.ts);
        let fee = fee_microusdc(market.fee_bps, pm, fill.qty);

        match fill.action {
            Action::Buy => {
                let cost = buy_cost(pm, fill.qty);
                // cash out = cost + fee
                let outlay = cost.0 + fee.0;
                cash -= outlay;
                basis += outlay;
                *positions.entry(fill.token).or_insert(0) += fill.qty.0 as i128;
            }
            Action::Sell => {
                let proceeds = sell_proceeds(pm, fill.qty);
                // cash in = proceeds − fee
                let inflow = proceeds.0 - fee.0;
                cash += inflow;
                *positions.entry(fill.token).or_insert(0) -= fill.qty.0 as i128;
            }
        }
    }

    // Reject naked shorts: any token with a net negative position.
    for &pos in positions.values() {
        if pos < 0 {
            return None;
        }
    }

    // Evaluate in every world: net = cash + payoff − gas
    // payoff = Σ pos[t] for every paying token t (1 µUSDC per µshare)
    let gas = gas_micro as i128;
    let mut worst: Option<i128> = None;

    for w in worlds {
        let mut payoff: i128 = 0;
        for (&token, &pos) in &positions {
            if token_pays(spec, w, token) == Some(true) {
                payoff += pos;
            }
        }
        let net = cash + payoff - gas;
        worst = Some(match worst {
            None => net,
            Some(prev) => prev.min(net),
        });
    }

    // If there are no worlds (degenerate empty component), treat worst as
    // cash − gas (position contributes nothing — shouldn't happen in practice).
    let worst_val = worst.unwrap_or(cash - gas);

    Some((Usdc(worst_val), Usdc(basis)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::instrument::EventId;
    use pm_core::num::{Bps, Px, TickSize};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    fn mk(i: u32) -> Market {
        Market {
            id: MarketId(i),
            yes: TokenId(u64::from(i) * 2 + 10),
            no: TokenId(u64::from(i) * 2 + 11),
            tick: TS,
            fee_bps: Bps(0),
            neg_risk: false,
        }
    }

    fn quote(books: &mut HashMap<TokenId, Book>, t: TokenId, ask: u16, bid: u16, q: u64) {
        let mut b = Book::new(TS);
        b.apply(Side::Ask, px(ask), Qty(q));
        b.apply(Side::Bid, px(bid), Qty(q));
        books.insert(t, b);
    }

    #[test]
    fn two_free_binaries_make_four_worlds() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        assert_eq!(enumerate_worlds(&spec, 4096).unwrap().len(), 4);
    }

    #[test]
    fn implies_prunes_a_and_not_b() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::Implies { a: MarketId(0), b: MarketId(1) }],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        assert_eq!(worlds.len(), 3);
        assert!(worlds.iter().all(|w| {
            let a = token_pays(&spec, w, TokenId(10)).unwrap();
            let b = token_pays(&spec, w, TokenId(12)).unwrap();
            !a || b
        }));
    }

    #[test]
    fn mutex_and_equivalent_prune() {
        let books = HashMap::new();
        let spec_mutex = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::MutuallyExclusive { a: MarketId(0), b: MarketId(1) }],
            books: &books,
        };
        assert_eq!(enumerate_worlds(&spec_mutex, 4096).unwrap().len(), 3);
        let spec_eq = ComponentSpec {
            relationships: vec![Relationship::Equivalent { a: MarketId(0), b: MarketId(1) }],
            ..spec_mutex.clone()
        };
        assert_eq!(enumerate_worlds(&spec_eq, 4096).unwrap().len(), 2);
    }

    #[test]
    fn foreign_relationships_are_ignored() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![Relationship::Implies { a: MarketId(0), b: MarketId(99) }],
            books: &books,
        };
        assert_eq!(enumerate_worlds(&spec, 4096).unwrap().len(), 2);
    }

    #[test]
    fn partition_contributes_n_outcomes() {
        let books = HashMap::new();
        let part = Partition {
            event: EventId(0),
            markets: vec![MarketId(0), MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(12), TokenId(14)],
            no_tokens: vec![TokenId(11), TokenId(13), TokenId(15)],
            verified_exhaustive: true,
        };
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1), mk(2), mk(3)], // 3 in partition + 1 free
            partitions: vec![part],
            relationships: vec![],
            books: &books,
        };
        // 3 partition outcomes × 2 for the free market.
        assert_eq!(enumerate_worlds(&spec, 4096).unwrap().len(), 6);
        // exactly one partition YES pays per world
        for w in enumerate_worlds(&spec, 4096).unwrap() {
            let paying = [TokenId(10), TokenId(12), TokenId(14)]
                .iter()
                .filter(|&&t| token_pays(&spec, &w, t).unwrap())
                .count();
            assert_eq!(paying, 1);
        }
    }

    #[test]
    fn world_cap_applies_to_preprune_product() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: (0..3).map(mk).collect(), // 2^3 = 8 pre-prune
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        assert!(enumerate_worlds(&spec, 7).is_none());
        assert!(enumerate_worlds(&spec, 8).is_some());
    }

    #[test]
    fn exact_reval_of_hedged_class1_basket() {
        // Buy YES@0.46 + NO@0.52 ×100sh: payoff $100 in both worlds, cost $98.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![
                LegFill {
                    token: TokenId(10),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(46),
                    qty: Qty(100_000_000),
                    cash: Usdc(-46_000_000),
                },
                LegFill {
                    token: TokenId(11),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(52),
                    qty: Qty(100_000_000),
                    cash: Usdc(-52_000_000),
                },
            ],
            splits: vec![],
        };
        let (worst, basis) = exact_worst_net(&spec, &worlds, &sol, 0).unwrap();
        assert_eq!(worst, Usdc(2_000_000));
        assert_eq!(basis, Usdc(98_000_000));
    }

    #[test]
    fn exact_reval_split_and_sell() {
        // Split 100 sets, sell YES@0.55 + NO@0.50: $5 risk-free.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 57, 55, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![
                LegFill {
                    token: TokenId(10),
                    action: Action::Sell,
                    ts: TS,
                    limit_px: px(55),
                    qty: Qty(100_000_000),
                    cash: Usdc(55_000_000),
                },
                LegFill {
                    token: TokenId(11),
                    action: Action::Sell,
                    ts: TS,
                    limit_px: px(50),
                    qty: Qty(100_000_000),
                    cash: Usdc(50_000_000),
                },
            ],
            splits: vec![(MarketId(0), Qty(100_000_000))],
        };
        let (worst, basis) = exact_worst_net(&spec, &worlds, &sol, 0).unwrap();
        assert_eq!(worst, Usdc(5_000_000));
        assert_eq!(basis, Usdc(100_000_000));
    }

    #[test]
    fn unhedged_position_has_negative_worst_world() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![LegFill {
                token: TokenId(10),
                action: Action::Buy,
                ts: TS,
                limit_px: px(46),
                qty: Qty(100_000_000),
                cash: Usdc(-46_000_000),
            }],
            splits: vec![],
        };
        let (worst, _) = exact_worst_net(&spec, &worlds, &sol, 0).unwrap();
        assert_eq!(worst, Usdc(-46_000_000)); // YES loses world
    }

    #[test]
    fn naked_short_is_rejected() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![LegFill {
                token: TokenId(10),
                action: Action::Sell,
                ts: TS,
                limit_px: px(44),
                qty: Qty(1_000_000),
                cash: Usdc(440_000),
            }],
            splits: vec![],
        };
        assert!(exact_worst_net(&spec, &worlds, &sol, 0).is_none());
    }

    #[test]
    fn reval_recomputes_cash_ignoring_lies() {
        // Same hedged basket but with absurd lying `cash` values in the fills:
        // reval must recompute from px/qty and return the true numbers.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![
                LegFill {
                    token: TokenId(10),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(46),
                    qty: Qty(100_000_000),
                    cash: Usdc(999_999_999), // lie
                },
                LegFill {
                    token: TokenId(11),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(52),
                    qty: Qty(100_000_000),
                    cash: Usdc(0), // lie
                },
            ],
            splits: vec![],
        };
        let (worst, basis) = exact_worst_net(&spec, &worlds, &sol, 0).unwrap();
        assert_eq!(worst, Usdc(2_000_000));
        assert_eq!(basis, Usdc(98_000_000));
    }

    #[test]
    fn gas_is_charged_flat() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![
                LegFill { token: TokenId(10), action: Action::Buy, ts: TS, limit_px: px(46), qty: Qty(100_000_000), cash: Usdc(0) },
                LegFill { token: TokenId(11), action: Action::Buy, ts: TS, limit_px: px(52), qty: Qty(100_000_000), cash: Usdc(0) },
            ],
            splits: vec![],
        };
        let (worst, _) = exact_worst_net(&spec, &worlds, &sol, 123_456).unwrap();
        assert_eq!(worst, Usdc(2_000_000 - 123_456));
    }
}
