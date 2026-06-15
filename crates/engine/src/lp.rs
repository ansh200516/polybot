//! Class 4: unified LP detector (spec §10). Part A: worlds + exact reval.

use std::collections::HashMap;

use crate::{Action, ArbClass, LegFill, Opportunity};
use pm_core::book::Book;
use pm_core::fees::fee_microusdc;
use pm_core::instrument::{Market, MarketId, Partition, Relationship, TokenId};
use pm_core::num::{Bps, Qty, Usdc, buy_cost, edge_bps, sell_proceeds};

/// A logical component: the set of markets + partitions + relationships +
/// live books the LP will solve over.
#[derive(Clone, Debug)]
pub struct ComponentSpec<'a> {
    pub markets: Vec<Market>,
    pub partitions: Vec<Partition>,
    pub relationships: Vec<Relationship>,
    /// Used by the part-B solver to read book depth; part-A functions don't read it.
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
                let Some(ai) = market_index(spec, a) else {
                    continue;
                };
                let Some(bi) = market_index(spec, b) else {
                    continue;
                };
                // Implies: ¬a ∨ b
                if w.yes_true[ai] && !w.yes_true[bi] {
                    return false;
                }
            }
            Relationship::MutuallyExclusive { a, b } => {
                let Some(ai) = market_index(spec, a) else {
                    continue;
                };
                let Some(bi) = market_index(spec, b) else {
                    continue;
                };
                // MutEx: ¬(a ∧ b)
                if w.yes_true[ai] && w.yes_true[bi] {
                    return false;
                }
            }
            Relationship::Equivalent { a, b } => {
                let Some(ai) = market_index(spec, a) else {
                    continue;
                };
                let Some(bi) = market_index(spec, b) else {
                    continue;
                };
                // Equivalent: a == b
                if w.yes_true[ai] != w.yes_true[bi] {
                    return false;
                }
            }
        }
    }
    true
}

/// World-choices a partition contributes:
/// - `verified_exhaustive` → EXACTLY one winner → `k` choices.
/// - mutually-exclusive only (NegRisk, not provably exhaustive) → AT MOST one
///   winner → `k + 1` choices (each member wins, or none win).
///
/// The at-most-one model is the conservative truth for a NegRisk set we can't
/// prove complete: it never assumes a $1 payout (the "none win" world pays $0),
/// yet keeps the world count linear (`k+1`) instead of the `2^k` blow-up that a
/// fall-back to free binary variables would cause.
fn partition_choices(p: &Partition) -> usize {
    p.markets.len() + usize::from(!p.verified_exhaustive)
}

/// Enumerate all worlds consistent with the component's relationships.
///
/// Returns `None` if the pre-prune cartesian-product size exceeds `max_worlds`.
///
/// A verified-exhaustive partition contributes exactly one winning YES outcome;
/// a mutually-exclusive-only (NegRisk) partition adds a "no member wins" world.
/// Markets that belong to no partition are "free" binary variables.
pub fn enumerate_worlds(spec: &ComponentSpec, max_worlds: usize) -> Option<Vec<World>> {
    debug_assert!(spec.partitions.iter().all(Partition::is_well_formed));
    // A partition must be at least mutually exclusive (NegRisk) to constrain
    // worlds; verified_exhaustive is the stronger exactly-one case.
    debug_assert!(spec.partitions.iter().all(|p| p.neg_risk));

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
    let free_indices: Vec<usize> = (0..n_markets)
        .filter(|i| partition_owner[*i].is_none())
        .collect();
    let n_free = free_indices.len();

    // Pre-prune size = Π partition_sizes × 2^free_markets.
    // Use u128 with saturating mul so we never overflow on absurd inputs.
    let mut preprune: u128 = 1u128;
    for part in &spec.partitions {
        preprune = preprune.saturating_mul(partition_choices(part) as u128);
    }
    // 2^n_free, saturating at u128::MAX to avoid overflow.
    // loop shifts 1u64<<n_free: safe — preprune check already returned None for n_free ≥ 64
    let free_combos: u128 = if n_free >= 64 {
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
                // `get` is None at choice == k — the "no member wins" world that
                // only a non-exhaustive (at-most-one) partition reaches.
                let winner_mid = part.markets.get(part_choice[pi]).copied();
                for mid in &part.markets {
                    if let Some(idx) = market_index(spec, *mid) {
                        yes_true[idx] = Some(*mid) == winner_mid;
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
                if part_choice[pi] < partition_choices(&spec.partitions[pi]) {
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
    spec.markets
        .iter()
        .find(|m| m.yes == token || m.no == token)
}

/// Exact re-validation of a candidate LP solution.
///
/// Recomputes cash and positions from `limit_px`/`qty` (ignoring the `cash`
/// field in each `LegFill` — it may be a solver approximation or a test lie).
/// Then evaluates `cash + payoff − gas` in every world and returns
/// `Some((worst, basis))`.
///
/// `basis` is total gross cash outflow (buy outlays incl. fees + split
/// collateral), excluding sell proceeds — the denominator for edge bps.
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

// ---------------------------------------------------------------------------
// Part B: HiGHS solver
// ---------------------------------------------------------------------------

/// Reason a component was skipped without solving.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SkipReason {
    TooManyWorlds,
    SolverFailed,
}

/// Result of `solve_component`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LpResult {
    Found(Opportunity),
    NoEdge,
    Skipped(SkipReason),
}

// ---------------------------------------------------------------------------
// Variable metadata
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct VarMeta {
    token: TokenId,
    px_micro: u64,
    ts: pm_core::num::TickSize,
    fee_bps: Bps,
    depth_micro: u64,
}

#[derive(Clone, Debug)]
enum VarKind {
    Trade { action: Action, meta: VarMeta },
}

/// Solve one component. Gas-less LP objective; exact reval applies gas and the
/// §10 floor rule (uses `p.floor_c12` when the component has no relationships,
/// `p.floor_c3` otherwise (spec §10)).
pub fn solve_component(spec: &ComponentSpec, p: &crate::EngineParams) -> LpResult {
    use highs::{Col, HighsModelStatus, RowProblem, Sense};

    // --- enumerate worlds ---------------------------------------------------
    let worlds = match enumerate_worlds(spec, p.max_worlds) {
        Some(w) => w,
        None => return LpResult::Skipped(SkipReason::TooManyWorlds),
    };
    // If no worlds survive pruning there's nothing to check.
    if worlds.is_empty() {
        return LpResult::NoEdge;
    }

    let max_basis_dollars = p.max_basis.0 as f64 / 1_000_000.0;

    // --- build LP -----------------------------------------------------------
    // Column layout (in order of addition):
    //   [0]     t        — objective var, coef 1.0, bounds (-∞, +∞)
    //   [1..B]  buy/sell — one per non-empty book level; coef 0.0
    //   [B..B+M] split   — one per market; coef 0.0
    //
    // We store the Col handles returned by add_column and use them directly
    // in add_row — Col.0 is pub(crate), so we must not access it externally.
    // For solution extraction we rely on positional indexing into sol_cols[]:
    //   sol_cols[0] = t, sol_cols[1..=B-1] = buy/sell, sol_cols[B..] = split.

    let mut pb = RowProblem::default();

    // col 0: t
    let col_t: Col = pb.add_column(1.0, f64::NEG_INFINITY..f64::INFINITY);

    // Per-variable metadata (parallel to sol_cols[1..])
    let mut var_meta: Vec<VarKind> = Vec::new();
    // Col handles for world rows / holdings rows / budget row
    let mut buy_sell_cols: Vec<Col> = Vec::new();
    let mut split_cols: Vec<Col> = Vec::new();

    for market in &spec.markets {
        for token in [market.yes, market.no] {
            let Some(book) = spec.books.get(&token) else {
                continue;
            };
            let ts = book.ts();

            // Buy levels (asks)
            for (px, qty) in book.asks.iter_from_best() {
                if qty.0 == 0 {
                    continue;
                }
                let pm = px.microusdc(ts);
                let depth_shares = qty.0 as f64 / 1_000_000.0;
                let col = pb.add_column(0.0, 0.0..depth_shares);
                buy_sell_cols.push(col);
                var_meta.push(VarKind::Trade {
                    action: Action::Buy,
                    meta: VarMeta {
                        token,
                        px_micro: pm,
                        ts,
                        fee_bps: market.fee_bps,
                        depth_micro: qty.0,
                    },
                });
            }

            // Sell levels (bids)
            for (px, qty) in book.bids.iter_from_best() {
                if qty.0 == 0 {
                    continue;
                }
                let pm = px.microusdc(ts);
                let depth_shares = qty.0 as f64 / 1_000_000.0;
                let col = pb.add_column(0.0, 0.0..depth_shares);
                buy_sell_cols.push(col);
                var_meta.push(VarKind::Trade {
                    action: Action::Sell,
                    meta: VarMeta {
                        token,
                        px_micro: pm,
                        ts,
                        fee_bps: market.fee_bps,
                        depth_micro: qty.0,
                    },
                });
            }
        }
    }

    // Split vars: one per market in spec.markets order
    for _ in &spec.markets {
        let col = pb.add_column(0.0, 0.0..max_basis_dollars);
        split_cols.push(col);
    }

    // Helper: cash-per-share in dollars (negative = outflow for buys)
    let cash_per_share = |kind: &VarKind| -> f64 {
        let VarKind::Trade { action, meta } = kind;
        match action {
            Action::Buy => {
                let pm = meta.px_micro;
                let fee = fee_microusdc(meta.fee_bps, pm, Qty(1_000_000));
                let cost = buy_cost(pm, Qty(1_000_000));
                -(cost.0 + fee.0) as f64 / 1_000_000.0
            }
            Action::Sell => {
                let pm = meta.px_micro;
                let fee = fee_microusdc(meta.fee_bps, pm, Qty(1_000_000));
                let proceeds = sell_proceeds(pm, Qty(1_000_000));
                (proceeds.0 - fee.0) as f64 / 1_000_000.0
            }
        }
    };

    // Helper: payoff-per-share in a given world (for world-row coefficients)
    let payoff_per_share = |kind: &VarKind, w: &World| -> f64 {
        let VarKind::Trade { action, meta } = kind;
        match action {
            Action::Buy => {
                if token_pays(spec, w, meta.token) == Some(true) {
                    1.0
                } else {
                    0.0
                }
            }
            Action::Sell => {
                // Sells are short positions: costs 1 if token pays
                if token_pays(spec, w, meta.token) == Some(true) {
                    -1.0
                } else {
                    0.0
                }
            }
        }
    };

    // --- world rows: (profit_w - t) >= 0 ------------------------------------
    // coef(t) = -1
    // coef(buy/sell_v) = cash_per_share + payoff_per_share(w)
    // coef(split_v)    = 0 (omitted)
    for w in &worlds {
        let mut row: Vec<(Col, f64)> = Vec::with_capacity(1 + buy_sell_cols.len());
        row.push((col_t, -1.0));
        for (col, kind) in buy_sell_cols.iter().zip(var_meta.iter()) {
            let coef = cash_per_share(kind) + payoff_per_share(kind, w);
            if coef != 0.0 {
                row.push((*col, coef));
            }
        }
        pb.add_row(0.0.., &row);
    }

    // --- holdings rows: Σ buys + Σ splits − Σ sells >= 0 per token ---------
    let all_tokens: Vec<TokenId> = spec.markets.iter().flat_map(|m| [m.yes, m.no]).collect();
    for &token in &all_tokens {
        let mut row: Vec<(Col, f64)> = Vec::new();
        for (col, kind) in buy_sell_cols.iter().zip(var_meta.iter()) {
            let VarKind::Trade { action, meta } = kind;
            if meta.token == token {
                match action {
                    Action::Buy => row.push((*col, 1.0)),
                    Action::Sell => row.push((*col, -1.0)),
                }
            }
        }
        // A split on market m creates one YES and one NO per share
        for (split_col, market) in split_cols.iter().zip(spec.markets.iter()) {
            if market.yes == token || market.no == token {
                row.push((*split_col, 1.0));
            }
        }
        if !row.is_empty() {
            pb.add_row(0.0.., &row);
        }
    }

    // --- budget row: Σ outlays <= max_basis$ --------------------------------
    {
        let mut row: Vec<(Col, f64)> = Vec::new();
        for (col, kind) in buy_sell_cols.iter().zip(var_meta.iter()) {
            let c = cash_per_share(kind);
            if c < 0.0 {
                row.push((*col, -c)); // outlay = positive
            }
        }
        for split_col in &split_cols {
            row.push((*split_col, 1.0));
        }
        if !row.is_empty() {
            pb.add_row(..max_basis_dollars, &row);
        }
    }

    // --- solve --------------------------------------------------------------
    let solved = pb.optimise(Sense::Maximise).solve();
    if solved.status() != HighsModelStatus::Optimal {
        return LpResult::Skipped(SkipReason::SolverFailed);
    }

    let sol_cols = solved.get_solution().columns().to_vec();
    // Layout: sol_cols[0]=t, sol_cols[1..=n_bs]=buy/sell, sol_cols[n_bs+1..]=split
    let n_bs = buy_sell_cols.len();

    // --- extract solution ---------------------------------------------------
    // Floor each var to micro-shares and clamp to book depth.
    let mut fills: Vec<LegFill> = Vec::new();
    for (i, kind) in var_meta.iter().enumerate() {
        let raw = sol_cols[1 + i];
        let micro = (raw * 1_000_000.0).floor() as u64;
        if micro == 0 {
            continue;
        }
        let VarKind::Trade { action, meta } = kind;
        let qty = Qty(micro.min(meta.depth_micro));
        if qty.0 == 0 {
            continue;
        }
        // px_micro comes from a valid book Px value; reconstruction is infallible.
        let Some(px) =
            pm_core::num::Px::new((meta.px_micro / meta.ts.unit_microusdc()) as u16, meta.ts).ok()
        else {
            continue;
        };
        match action {
            Action::Buy => {
                let cost = buy_cost(meta.px_micro, qty);
                fills.push(LegFill {
                    token: meta.token,
                    action: Action::Buy,
                    ts: meta.ts,
                    limit_px: px,
                    qty,
                    cash: Usdc(-cost.0),
                });
            }
            Action::Sell => {
                let proceeds = sell_proceeds(meta.px_micro, qty);
                fills.push(LegFill {
                    token: meta.token,
                    action: Action::Sell,
                    ts: meta.ts,
                    limit_px: px,
                    qty,
                    cash: proceeds,
                });
            }
        }
    }

    // Gather splits (zip split_cols with spec.markets directly — no separate id vec)
    let mut splits: Vec<(MarketId, Qty)> = Vec::new();
    for (i, market) in spec.markets.iter().enumerate() {
        let raw = sol_cols[1 + n_bs + i];
        let micro = (raw * 1_000_000.0).floor() as u64;
        if micro > 0 {
            splits.push((market.id, Qty(micro)));
        }
    }

    // Clamp sells to holdings (buys + splits per token), then recompute cash.
    // Invariant: after clamping a sell, decrement holdings by clamped qty so
    // that multi-level sells on the same token cannot double-count.
    let mut holdings: HashMap<TokenId, u64> = HashMap::new();
    for &(market_id, qty) in &splits {
        if let Some(m) = crate::find_market(&spec.markets, market_id) {
            *holdings.entry(m.yes).or_insert(0) += qty.0;
            *holdings.entry(m.no).or_insert(0) += qty.0;
        }
    }
    for fill in &fills {
        if fill.action == Action::Buy {
            *holdings.entry(fill.token).or_insert(0) += fill.qty.0;
        }
    }
    for fill in fills.iter_mut() {
        if fill.action == Action::Sell {
            let avail = holdings.get(&fill.token).copied().unwrap_or(0);
            let clamped = fill.qty.0.min(avail);
            fill.qty = Qty(clamped);
            let proceeds = sell_proceeds(fill.limit_px.microusdc(fill.ts), fill.qty);
            fill.cash = proceeds;
            // Consume holdings so subsequent sells on the same token can't reuse them.
            *holdings.entry(fill.token).or_insert(0) = avail.saturating_sub(clamped);
        }
    }
    // Drop zero fills
    fills.retain(|f| f.qty.0 > 0);

    if fills.is_empty() && splits.is_empty() {
        return LpResult::NoEdge;
    }

    // --- exact re-validation ------------------------------------------------
    let lp_sol = LpSolution {
        fills: fills.clone(),
        splits: splits.clone(),
    };
    // Charge one split gas per on-chain split (one per market that is split).
    let split_gas = (splits.len() as u64).saturating_mul(p.gas.split);
    let gas = split_gas + p.gas.redeem;

    let (worst, basis) = match exact_worst_net(spec, &worlds, &lp_sol, gas) {
        Some(r) => r,
        None => return LpResult::Skipped(SkipReason::SolverFailed),
    };

    if worst < p.min_profit {
        return LpResult::NoEdge;
    }

    let floor = if spec.relationships.is_empty() {
        p.floor_c12
    } else {
        p.floor_c3
    };

    let edge = match edge_bps(worst, basis) {
        Some(e) => e,
        None => return LpResult::NoEdge,
    };

    if edge < floor {
        return LpResult::NoEdge;
    }

    LpResult::Found(Opportunity {
        class: ArbClass::C4Lp,
        fills,
        units: Qty(0), // heterogeneous: per-leg qtys authoritative
        net: worst,
        basis,
        edge,
        splits,
    })
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

    fn simple_spec<'a>(books: &'a HashMap<TokenId, Book>) -> ComponentSpec<'a> {
        ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books,
        }
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
            relationships: vec![Relationship::Implies {
                a: MarketId(0),
                b: MarketId(1),
            }],
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
            relationships: vec![Relationship::MutuallyExclusive {
                a: MarketId(0),
                b: MarketId(1),
            }],
            books: &books,
        };
        assert_eq!(enumerate_worlds(&spec_mutex, 4096).unwrap().len(), 3);
        let spec_eq = ComponentSpec {
            relationships: vec![Relationship::Equivalent {
                a: MarketId(0),
                b: MarketId(1),
            }],
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
            relationships: vec![Relationship::Implies {
                a: MarketId(0),
                b: MarketId(99),
            }],
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
            neg_risk: true,
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
    fn nonexhaustive_partition_is_at_most_one_winner() {
        // Same 3-member set, but NOT verified exhaustive (mutually exclusive
        // only). The exhaustive model gives 3 worlds; at-most-one gives 4 — the
        // extra one is the "no member wins" world that makes a cheap YES set NOT
        // a guaranteed $1 (so the LP can't fabricate a complete-set arb).
        let books = HashMap::new();
        let part = Partition {
            event: EventId(0),
            markets: vec![MarketId(0), MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(12), TokenId(14)],
            no_tokens: vec![TokenId(11), TokenId(13), TokenId(15)],
            verified_exhaustive: false,
            neg_risk: true,
        };
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1), mk(2)],
            partitions: vec![part],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        assert_eq!(worlds.len(), 4, "k members → k+1 worlds (k winners + none)");
        let yes = [TokenId(10), TokenId(12), TokenId(14)];
        // Mutual exclusivity holds in every world: never 2+ YES paying.
        for w in &worlds {
            let paying = yes
                .iter()
                .filter(|&&t| token_pays(&spec, w, t).unwrap())
                .count();
            assert!(paying <= 1, "mutual exclusivity violated: {paying} winners");
        }
        // Exactly one world has NO winner (the case the exhaustive model omits).
        let none_win = worlds
            .iter()
            .filter(|w| yes.iter().all(|&t| !token_pays(&spec, w, t).unwrap()))
            .count();
        assert_eq!(none_win, 1, "must include the 'no member wins' world");
    }

    #[test]
    fn nonexhaustive_partition_stays_linear_not_exponential() {
        // 30-member NegRisk set: at-most-one is 31 worlds (fits 4096); the old
        // free-variable fallback would be 2^30 → would return None (skip).
        let books = HashMap::new();
        let n = 30u32;
        let part = Partition {
            event: EventId(0),
            markets: (0..n).map(MarketId).collect(),
            yes_tokens: (0..n).map(|i| TokenId(u64::from(i) * 2 + 10)).collect(),
            no_tokens: (0..n).map(|i| TokenId(u64::from(i) * 2 + 11)).collect(),
            verified_exhaustive: false,
            neg_risk: true,
        };
        let spec = ComponentSpec {
            markets: (0..n).map(mk).collect(),
            partitions: vec![part],
            relationships: vec![],
            books: &books,
        };
        // 31 worlds fit the 4096 cap (the 2^30 free-var fallback would not).
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        assert_eq!(worlds.len(), 31);
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
        let spec = simple_spec(&books);
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
        let spec = simple_spec(&books);
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
        let spec = simple_spec(&books);
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
        let spec = simple_spec(&books);
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
        let spec = simple_spec(&books);
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
        let spec = simple_spec(&books);
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![
                LegFill {
                    token: TokenId(10),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(46),
                    qty: Qty(100_000_000),
                    cash: Usdc(0),
                },
                LegFill {
                    token: TokenId(11),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(52),
                    qty: Qty(100_000_000),
                    cash: Usdc(0),
                },
            ],
            splits: vec![],
        };
        let (worst, _) = exact_worst_net(&spec, &worlds, &sol, 123_456).unwrap();
        assert_eq!(worst, Usdc(2_000_000 - 123_456));
    }

    // ---- part B: solver ----

    fn solver_params() -> crate::EngineParams {
        crate::EngineParams {
            gas: crate::GasTable {
                split: 0,
                merge: 0,
                redeem: 0,
                negrisk_convert: 0,
            },
            min_profit: Usdc(0),
            ..crate::EngineParams::default()
        }
    }

    #[test]
    fn lp_recovers_class1_long() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = simple_spec(&books);
        let LpResult::Found(op) = solve_component(&spec, &solver_params()) else {
            panic!("expected Found");
        };
        assert_eq!(op.net, Usdc(2_000_000));
        assert_eq!(op.basis, Usdc(98_000_000));
        assert!(op.splits.is_empty());
        assert_eq!(op.fills.len(), 2);
        assert!(op.fills.iter().all(|f| f.action == Action::Buy));
        assert_eq!(op.class, crate::ArbClass::C4Lp);
        assert_eq!(op.units, Qty(0)); // heterogeneous: per-leg qtys authoritative
    }

    #[test]
    fn lp_recovers_class1_short_via_split() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 57, 55, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = simple_spec(&books);
        let LpResult::Found(op) = solve_component(&spec, &solver_params()) else {
            panic!("expected Found");
        };
        assert_eq!(op.net, Usdc(5_000_000));
        assert_eq!(op.splits, vec![(MarketId(0), Qty(100_000_000))]);
        assert!(op.fills.iter().all(|f| f.action == Action::Sell));
    }

    #[test]
    fn lp_recovers_class2_long() {
        let mut books = HashMap::new();
        // YES asks 0.30/0.30/0.35 (sum .95); NOs rich enough to not compete.
        quote(&mut books, TokenId(10), 30, 28, 100_000_000);
        quote(&mut books, TokenId(11), 72, 70, 100_000_000);
        quote(&mut books, TokenId(12), 30, 28, 100_000_000);
        quote(&mut books, TokenId(13), 72, 70, 100_000_000);
        quote(&mut books, TokenId(14), 35, 33, 100_000_000);
        quote(&mut books, TokenId(15), 66, 64, 100_000_000);
        let part = Partition {
            event: EventId(0),
            markets: vec![MarketId(0), MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(12), TokenId(14)],
            no_tokens: vec![TokenId(11), TokenId(13), TokenId(15)],
            verified_exhaustive: true,
            neg_risk: true,
        };
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1), mk(2)],
            partitions: vec![part],
            relationships: vec![],
            books: &books,
        };
        let LpResult::Found(op) = solve_component(&spec, &solver_params()) else {
            panic!("expected Found");
        };
        // LP must recover at least the class-2 arb ($5M); it may find more
        // via split+sell strategies using the cheap NO books.
        assert!(op.net >= Usdc(5_000_000), "net was {:?}", op.net);
    }

    #[test]
    fn lp_recovers_class3_implies_via_pruning() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 68, 64, 100_000_000); // YES_a
        quote(&mut books, TokenId(11), 35, 33, 100_000_000); // NO_a ask 0.35
        quote(&mut books, TokenId(12), 55, 53, 100_000_000); // YES_b ask 0.55
        quote(&mut books, TokenId(13), 48, 44, 100_000_000); // NO_b
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::Implies {
                a: MarketId(0),
                b: MarketId(1),
            }],
            books: &books,
        };
        let LpResult::Found(op) = solve_component(&spec, &solver_params()) else {
            panic!("expected Found");
        };
        // NO_a + YES_b = 0.90 → ≥ $10 on 100sh (LP may find more; never less).
        assert!(op.net >= Usdc(10_000_000), "net was {:?}", op.net);
        // Without the relationship the same books are no-arb:
        let spec_free = ComponentSpec {
            relationships: vec![],
            ..spec.clone()
        };
        assert!(matches!(
            solve_component(&spec_free, &solver_params()),
            LpResult::NoEdge
        ));
    }

    #[test]
    fn floor_rule_uses_c3_floor_when_relationships_present() {
        // NO_a 0.49 + YES_b 0.50 = 0.99 → ≈101 bps. With a relationship in
        // the component the class-3 floor applies: at 150 it must be rejected,
        // at 100 it must pass.
        // Books are chosen so splits are unprofitable (bid_YES + bid_NO < 100
        // for both markets), isolating the relationship-arb signal.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 55, 48, 100_000_000); // YES_a: ask 55, bid 48
        quote(&mut books, TokenId(11), 49, 45, 100_000_000); // NO_a: ask 49, bid 45  → bid sum=93 < 100
        quote(&mut books, TokenId(12), 50, 48, 100_000_000); // YES_b: ask 50, bid 48
        quote(&mut books, TokenId(13), 55, 46, 100_000_000); // NO_b: ask 55, bid 46  → bid sum=94 < 100
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::Implies {
                a: MarketId(0),
                b: MarketId(1),
            }],
            books: &books,
        };
        let p = crate::EngineParams {
            floor_c3: Bps(150),
            ..solver_params()
        };
        assert!(matches!(solve_component(&spec, &p), LpResult::NoEdge));
        let p = crate::EngineParams {
            floor_c3: Bps(100),
            ..solver_params()
        };
        assert!(matches!(solve_component(&spec, &p), LpResult::Found(_)));
    }

    #[test]
    fn solver_tolerance_cannot_fake_an_edge() {
        // Perfectly fair books: asks sum to exactly 1 everywhere. Any t* the
        // solver reports is numerical noise; exact reval must kill it.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 50, 48, 100_000_000);
        quote(&mut books, TokenId(11), 50, 48, 100_000_000);
        let spec = simple_spec(&books);
        assert!(matches!(
            solve_component(&spec, &solver_params()),
            LpResult::NoEdge
        ));
    }

    #[test]
    fn too_many_worlds_skips() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: (0..13).map(mk).collect(), // 8192 > 4096
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        assert!(matches!(
            solve_component(&spec, &solver_params()),
            LpResult::Skipped(SkipReason::TooManyWorlds)
        ));
    }

    #[test]
    fn respects_budget_cap() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = simple_spec(&books);
        let p = crate::EngineParams {
            max_basis: Usdc(9_800_000),
            ..solver_params()
        }; // $9.80
        let LpResult::Found(op) = solve_component(&spec, &p) else {
            panic!("expected Found");
        };
        assert!(op.basis <= Usdc(9_800_010), "basis {:?}", op.basis); // small reval slack
        assert!(op.net >= Usdc(190_000)); // ~2% of ~$9.8
    }

    #[test]
    fn multi_market_split_gas_charged_per_split() {
        // Same fixture as lp_recovers_class2_long: optimum splits all 3
        // markets (net $9 gasless). With per-split gas the net must drop by
        // 3×split + redeem.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 30, 28, 100_000_000);
        quote(&mut books, TokenId(11), 72, 70, 100_000_000);
        quote(&mut books, TokenId(12), 30, 28, 100_000_000);
        quote(&mut books, TokenId(13), 72, 70, 100_000_000);
        quote(&mut books, TokenId(14), 35, 33, 100_000_000);
        quote(&mut books, TokenId(15), 66, 64, 100_000_000);
        let part = Partition {
            event: EventId(0),
            markets: vec![MarketId(0), MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(12), TokenId(14)],
            no_tokens: vec![TokenId(11), TokenId(13), TokenId(15)],
            verified_exhaustive: true,
            neg_risk: true,
        };
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1), mk(2)],
            partitions: vec![part],
            relationships: vec![],
            books: &books,
        };
        let mut p = solver_params();
        p.gas.split = 10_000;
        p.gas.redeem = 15_000;
        let LpResult::Found(op) = solve_component(&spec, &p) else {
            panic!("expected Found");
        };
        assert_eq!(op.splits.len(), 3);
        assert_eq!(op.net, Usdc(9_000_000 - 3 * 10_000 - 15_000));
    }
}
