//! Leg coordinator: the happy path of paper execution (spec §14/§15).
//!
//! `execute_basket` drives a single [`Opportunity`] to a terminal state:
//! optional collateral splits, write-ahead order persistence, a single FAK
//! window across all legs, and merge-back settlement of any complete sets.
//!
//! # Write-ahead ordering (spec §14)
//! Every store write that precedes a venue side effect is *acked before the
//! venue call*: `open_order` acks OrderInsert → Signed → Submitted before any
//! `submit_all`; split Conversion rows are acked before the next venue
//! interaction; fills are acked before the in-memory ledger mutates. A dropped
//! ack (write failure) surfaces as [`ExecError::StoreClosed`] and aborts.
//!
//! Partial fills route through `repair_and_unwind` (Task 10): one break-even
//! repair round, a mid-basket unhedged guard, then market-out (buy legs) or
//! hold (sell legs) of any residual surplus.
//!
//! An `Err` return from `execute_basket` leaves committed partial store rows:
//! each acked write is its own transaction and there is no basket-level
//! rollback. Restart/Task-10 reconciliation reads them back from the store;
//! every acked row is durable and ordered.

use std::collections::HashMap;
use std::time::Duration;

use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{Bps, Px, Qty, Usdc};
use pm_engine::{Action, ArbClass, Opportunity, RedeemStrategy};
use pm_store::writer::StoreMsg;
use pm_store::{ConversionRow, FillRow, usdc_to_i64};
use tokio::sync::{mpsc, oneshot};

use crate::venue::{ExecutionVenue, SubmitOutcome, VenueError};
use crate::{ExecError, Order, OrderState, persist_transition};

/// Merges below 0.01 share are not worth their gas: the ~10k-µUSDC merge gas
/// exceeds the collateral recovered from < 10_000 µshare dust. Hold the dust
/// instead (spec §6).
const MERGE_DUST_MICRO: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct ExecParams {
    pub fill_window: Duration,
    /// In-path unhedged cap: max single-leg cash at risk (spec §14/§15).
    pub max_unhedged: Usdc,
    pub redeem: RedeemStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasketOutcome {
    FilledClean,
    Repaired,
    Unwound,
    NoFill,
    RejectedUnhedged,
}

#[derive(Debug, Clone)]
pub struct BasketReport {
    pub outcome: BasketOutcome,
    /// Σ of every venue cash flow in this basket (fills, splits, merges).
    pub cash_delta: Usdc,
    /// Leftover holdings: (token, qty, cost basis) — empty when flat. Sorted by token.
    ///
    /// Average-cost basis: may differ from the store's FIFO lots by per-lot
    /// rounding. For position/risk tracking only — the store is the realized-P&L
    /// truth; never reconcile the two.
    pub positions: Vec<(TokenId, Qty, Usdc)>,
    pub order_errors: u32,
}

// ---------------------------------------------------------------------------
// Ledger
// ---------------------------------------------------------------------------

/// `ceil(a*b/c)` for non-negative `a`, `b` and positive `c` — consumed cost
/// always ceils (against us).
fn ceil_mul_div(a: i128, b: i128, c: i128) -> i128 {
    debug_assert!(a >= 0 && b >= 0 && c > 0);
    (a * b + c - 1) / c
}

/// In-memory cash + holdings tape for one basket. `cash` and per-token cost are
/// signed µUSDC; quantities are µshares.
#[derive(Debug, Default)]
struct Ledger {
    cash: i128,
    /// token -> (held µshares, remaining cost basis µUSDC).
    hold: HashMap<TokenId, (u64, i128)>,
}

impl Ledger {
    /// Buy: `cash` is ≤ 0 (cost + fee out). Adds qty, adds −cash to cost.
    fn buy(&mut self, token: TokenId, qty: Qty, cash: Usdc) {
        self.cash += cash.0;
        let e = self.hold.entry(token).or_insert((0, 0));
        e.0 += qty.0;
        e.1 += -cash.0;
    }

    /// Sell: `cash` may be NEGATIVE at dust sizes (floored proceeds < ceiled
    /// fee) — no sign assert. Adds cash, reduces qty (min with held), reduces
    /// cost by the ceiled proportional share consumed (against us).
    fn sell(&mut self, token: TokenId, qty: Qty, cash: Usdc) {
        self.cash += cash.0;
        if let Some(e) = self.hold.get_mut(&token) {
            let held = e.0;
            let take = qty.0.min(held);
            let consumed = if held == 0 {
                0
            } else {
                ceil_mul_div(e.1, take as i128, held as i128)
            };
            e.0 -= take;
            e.1 -= consumed;
        }
    }

    /// Split: `cash` ≤ 0 (collateral + gas out). YES gets ceil(total/2), NO the
    /// remainder — matches the store's lot split so cost never under-counts.
    fn split(&mut self, yes: TokenId, no: TokenId, units: Qty, cash: Usdc) {
        self.cash += cash.0;
        let total = -cash.0;
        let yes_cost = (total + 1) / 2;
        let no_cost = total - yes_cost;
        let ye = self.hold.entry(yes).or_insert((0, 0));
        ye.0 += units.0;
        ye.1 += yes_cost;
        let ne = self.hold.entry(no).or_insert((0, 0));
        ne.0 += units.0;
        ne.1 += no_cost;
    }

    /// Merge: `cash` ≥ 0 (collateral in net of gas). Consumes min(units, held)
    /// from each side; cost reduced proportionally with ceil.
    fn merge(&mut self, yes: TokenId, no: TokenId, units: Qty, cash: Usdc) {
        self.cash += cash.0;
        for token in [yes, no] {
            if let Some(e) = self.hold.get_mut(&token) {
                let held = e.0;
                let take = units.0.min(held);
                let consumed = if held == 0 {
                    0
                } else {
                    ceil_mul_div(e.1, take as i128, held as i128)
                };
                e.0 -= take;
                e.1 -= consumed;
            }
        }
    }

    fn held(&self, token: TokenId) -> u64 {
        self.hold.get(&token).map(|e| e.0).unwrap_or(0)
    }

    /// Non-empty holdings (qty > 0), sorted by token id.
    fn positions(&self) -> Vec<(TokenId, Qty, Usdc)> {
        let mut out: Vec<(TokenId, Qty, Usdc)> = self
            .hold
            .iter()
            .filter(|(_, (q, _))| *q > 0)
            .map(|(t, (q, c))| (*t, Qty(*q), Usdc(*c)))
            .collect();
        out.sort_by_key(|(t, _, _)| t.0);
        out
    }
}

// ---------------------------------------------------------------------------
// Store helper
// ---------------------------------------------------------------------------

/// Send an acked `StoreMsg` and await its ack. Channel-closed or dropped ack →
/// `ExecError::StoreClosed`. Build any `usdc_to_i64` conversions BEFORE calling
/// (so the message is fully formed and no `?` happens inside a closure).
async fn store_acked(
    store: &mpsc::Sender<StoreMsg>,
    make: impl FnOnce(oneshot::Sender<()>) -> StoreMsg,
) -> Result<(), ExecError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    store
        .send(make(ack_tx))
        .await
        .map_err(|_| ExecError::StoreClosed)?;
    ack_rx.await.map_err(|_| ExecError::StoreClosed)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Order lifecycle
// ---------------------------------------------------------------------------

/// Persist the order row then walk Draft → Signed → Submitted (all acked) —
/// the full write-ahead prelude before the order can hit the venue.
async fn open_order(
    store: &mpsc::Sender<StoreMsg>,
    order: &mut Order,
    ts_ms: i64,
) -> Result<(), ExecError> {
    let row = order.to_row(ts_ms);
    store_acked(store, |ack| StoreMsg::OrderInsert(row, Some(ack))).await?;
    persist_transition(store, order, OrderState::Signed, "", ts_ms).await?;
    persist_transition(store, order, OrderState::Submitted, "", ts_ms).await?;
    Ok(())
}

/// Persist each venue fill (acked) then apply it to the ledger.
async fn record_fills(
    store: &mpsc::Sender<StoreMsg>,
    ledger: &mut Ledger,
    order: &Order,
    out: &SubmitOutcome,
    ts_ms: i64,
) -> Result<(), ExecError> {
    for f in &out.fills {
        // An amount that can't be represented for the store can't be durably
        // recorded — fail closed (write-ahead contract).
        let cash_micro = usdc_to_i64(f.cash).map_err(|_| ExecError::StoreClosed)?;
        let fee_micro = usdc_to_i64(f.fee).map_err(|_| ExecError::StoreClosed)?;
        let row = FillRow {
            order_id: order.id.to_string(),
            ts_ms,
            token: order.token.0 as i64,
            action: order.action_str().into(),
            px_ticks: i64::from(f.px.get()),
            tick_levels: i64::from(order.ts.levels()),
            qty_micro: f.qty.0 as i64,
            cash_micro,
            fee_micro,
        };
        store_acked(store, |ack| StoreMsg::Fill(row, Some(ack))).await?;
        match order.action {
            Action::Buy => ledger.buy(order.token, f.qty, f.cash),
            Action::Sell => ledger.sell(order.token, f.qty, f.cash),
        }
    }
    Ok(())
}

/// Close out one submitted order against its venue result. Returns the filled
/// quantity (Qty(0) on venue error). Drives the terminal state machine.
async fn close_order(
    store: &mpsc::Sender<StoreMsg>,
    ledger: &mut Ledger,
    order: &mut Order,
    result: Result<SubmitOutcome, VenueError>,
    ts_ms: i64,
    errors: &mut u32,
) -> Result<Qty, ExecError> {
    let out = match result {
        Err(_) => {
            *errors += 1;
            persist_transition(store, order, OrderState::Rejected, "venue error", ts_ms).await?;
            return Ok(Qty(0));
        }
        Ok(out) => out,
    };
    persist_transition(store, order, OrderState::Live, "", ts_ms).await?;
    record_fills(store, ledger, order, &out, ts_ms).await?;
    if out.filled.0 == order.qty.0 {
        persist_transition(store, order, OrderState::Filled, "", ts_ms).await?;
    } else if out.filled.0 > 0 {
        persist_transition(store, order, OrderState::PartFilled, "", ts_ms).await?;
        persist_transition(store, order, OrderState::Cancelled, "FAK remainder", ts_ms).await?;
    } else {
        persist_transition(store, order, OrderState::Cancelled, "FAK zero fill", ts_ms).await?;
    }
    Ok(out.filled)
}

/// Submit the whole basket within `window`. `None` means the window expired
/// before the venue produced any response.
async fn submit_window<V: ExecutionVenue>(
    venue: &mut V,
    orders: &[Order],
    window: Duration,
) -> Option<Vec<Result<SubmitOutcome, VenueError>>> {
    tokio::time::timeout(window, venue.submit_all(orders))
        .await
        .ok()
}

// ---------------------------------------------------------------------------
// Settlement
// ---------------------------------------------------------------------------

/// Merge any complete sets back to collateral (spec §6). Skipped entirely for
/// the C1Long hold strategy (positions are kept to redeem at resolution).
async fn settle<V: ExecutionVenue>(
    venue: &mut V,
    store: &mpsc::Sender<StoreMsg>,
    opp: &Opportunity,
    ledger: &mut Ledger,
    market_tokens: &HashMap<MarketId, (TokenId, TokenId)>,
    p: &ExecParams,
    ts_ms: i64,
) -> Result<(), ExecError> {
    if opp.class == ArbClass::C1Long && p.redeem == RedeemStrategy::Hold {
        return Ok(());
    }
    // HashMap iteration order is fine: each market settles independently.
    for (market, (yes, no)) in market_tokens {
        let units = ledger.held(*yes).min(ledger.held(*no));
        if units == 0 {
            continue;
        }
        // Below 0.01 share the ~10k-µUSDC merge gas exceeds recovered
        // collateral — hold the dust instead of paying to merge it.
        if units < MERGE_DUST_MICRO {
            continue;
        }
        let cash = venue
            .merge(*market, Qty(units))
            .await
            .map_err(|e| ExecError::Venue(e.to_string()))?;
        let cash_micro = usdc_to_i64(cash).map_err(|_| ExecError::StoreClosed)?;
        let row = ConversionRow {
            kind: "merge".into(),
            ts_ms,
            market: i64::from(market.0),
            yes_token: yes.0 as i64,
            no_token: no.0 as i64,
            units_micro: units as i64,
            cash_micro,
        };
        store_acked(store, |ack| StoreMsg::Conversion(row, Some(ack))).await?;
        ledger.merge(*yes, *no, Qty(units), cash);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Repair / unwind (Task 10)
// ---------------------------------------------------------------------------

/// Per-leg surplus: shares filled beyond the most-constrained leg's fill
/// fraction (spec §14, Design decision 9). `j* = argmin_j filled_j/intended_j`
/// (i128 cross-multiplication); `target_i = floor(intended_i·filled_{j*}/intended_{j*})`;
/// `surplus_i = max(0, filled_i − target_i)`. For uniform baskets this reduces
/// to `filled_i − min(filled)`. A degenerate `intended_{j*} == 0` is treated as
/// "no surplus" (target := filled).
fn surpluses(intended: &[Qty], filled: &[Qty]) -> Vec<u64> {
    // Find j* minimising filled_j / intended_j via cross-multiplication:
    // a/b < c/d  <=>  a·d < c·b (all non-negative, b,d > 0). Legs with
    // intended 0 are skipped (no constraint).
    let mut best: Option<usize> = None;
    for j in 0..intended.len() {
        if intended[j].0 == 0 {
            continue;
        }
        match best {
            None => best = Some(j),
            Some(b) => {
                let lhs = filled[j].0 as i128 * intended[b].0 as i128;
                let rhs = filled[b].0 as i128 * intended[j].0 as i128;
                if lhs < rhs {
                    best = Some(j);
                }
            }
        }
    }
    let Some(j) = best else {
        return vec![0; intended.len()];
    };
    let (fj, ij) = (filled[j].0 as i128, intended[j].0 as i128);
    if ij == 0 {
        return vec![0; intended.len()];
    }
    intended
        .iter()
        .zip(filled)
        .map(|(i, f)| {
            let target = (i.0 as i128 * fj) / ij; // floor
            (f.0 as i128 - target).max(0) as u64
        })
        .collect()
}

/// Repair partial fills (one round) then market out / hold any residual
/// surplus (spec §14, Design decisions 8 & 9).
///
/// # Repair (break-even bump)
/// The detected `opp.net` is the basket's total slack budget. For each unfilled
/// remainder `rem_i = intended_i − filled_i > 0` we apportion slack by the
/// leg's filled-out cash weight (`w_i = ceil(|cash_i|·rem_i/intended_i)`,
/// `s_i = floor(net·w_i/Σw)`), convert to a per-share concession
/// (`c_i = floor(s_i·1e6/rem_i)` µUSDC) and a tick bump (`floor(c_i/unit)`).
/// Since Σ allocated slack ≤ net, the repaired basket cannot fall below
/// break-even at submission prices. Buys bump UP (clamp ≤ levels−1), sells DOWN
/// (clamp ≥ 1). One round only.
///
/// # Mid-basket unhedged guard
/// If the total surplus COST (`Σ ceil(|cash_i|·surplus_i/intended_i)`) exceeds
/// `max_unhedged`, repair is SKIPPED entirely and we go straight to unwind
/// (spec §14: the cap is enforced "during repair").
///
/// # Unwind
/// Buy-leg surplus is sold back at the floor tick (1); SELL-leg surplus is NOT
/// unwound — buying it back at the ceiling tick would destroy value, and sell
/// legs only arise from split-funded baskets where the unsold side stays held
/// and `settle` recovers `min(yes,no)`. Unwind submits are individual FAKs
/// OUTSIDE the fill window (no timeout) — the paper venue's latency bounds them.
///
/// # Outcome
/// `Repaired` iff every leg is complete after repair; otherwise `Unwound`.
/// `Unwound` therefore also covers a uniformly-shrunken-but-hedged basket
/// (every leg filled the same fraction, zero surplus, nothing to unwind) — the
/// report's positions/cash tell the real story. (Revisit in M4 if the
/// distinction becomes a tracked statistic.)
#[allow(clippy::too_many_arguments)]
async fn repair_and_unwind<V: ExecutionVenue>(
    venue: &mut V,
    store: &mpsc::Sender<StoreMsg>,
    opp: &Opportunity,
    orders: &mut Vec<Order>,
    filled: &mut [Qty],
    ledger: &mut Ledger,
    token_fee: &HashMap<TokenId, Bps>,
    p: &ExecParams,
    ts_ms: i64,
    errors: &mut u32,
) -> Result<BasketOutcome, ExecError> {
    let n = opp.fills.len();
    let intended: Vec<Qty> = opp.fills.iter().map(|l| l.qty).collect();
    let fp = format!("{:016x}", opp.fingerprint().as_u64());

    // --- Mid-basket unhedged guard: total surplus cost vs cap. ----------
    let surplus = surpluses(&intended, filled);
    let surplus_cost: i128 = (0..n)
        .map(|i| {
            if intended[i].0 == 0 {
                0
            } else {
                ceil_mul_div(
                    opp.fills[i].cash.0.abs(),
                    surplus[i] as i128,
                    intended[i].0 as i128,
                )
            }
        })
        .sum();

    // --- Repair round (skipped if surplus cost breaches the cap). -------
    if surplus_cost <= p.max_unhedged.0 {
        // Weights for legs with a positive remainder.
        let mut weights = vec![0i128; n];
        for i in 0..n {
            let rem = intended[i].0.saturating_sub(filled[i].0);
            if rem > 0 && intended[i].0 > 0 {
                weights[i] = ceil_mul_div(
                    opp.fills[i].cash.0.abs(),
                    rem as i128,
                    intended[i].0 as i128,
                );
            }
        }
        let total_w: i128 = weights.iter().sum();

        if total_w > 0 {
            // Build the repair orders (track which original leg each maps to).
            let mut repair: Vec<(usize, Order)> = Vec::new();
            for i in 0..n {
                let rem = intended[i].0.saturating_sub(filled[i].0);
                if rem == 0 || weights[i] == 0 {
                    continue;
                }
                let slack = opp.net.0 * weights[i] / total_w; // floor, >= 0
                let per_share = slack * 1_000_000 / rem as i128; // floor µUSDC/share
                let unit = i128::from(opp.fills[i].ts.unit_microusdc());
                let bump = (per_share / unit).min(i128::from(u16::MAX)) as u16; // floor
                if bump == 0 {
                    continue; // not worth repairing
                }
                let leg = &opp.fills[i];
                let levels = leg.ts.levels();
                let base = leg.limit_px.get();
                let new_tick = match leg.action {
                    Action::Buy => (base + bump).min(levels - 1),
                    Action::Sell => base.saturating_sub(bump).max(1),
                };
                // Tick is in [1, levels-1] by clamp, so Px::new cannot fail.
                let Ok(px) = Px::new(new_tick, leg.ts) else {
                    continue;
                };
                let fee = token_fee.get(&leg.token).copied().unwrap_or(Bps(0));
                let mut order =
                    Order::new(fp.clone(), leg.token, leg.action, leg.ts, px, Qty(rem), fee);
                open_order(store, &mut order, ts_ms).await?;
                repair.push((i, order));
            }

            if !repair.is_empty() {
                let repair_orders: Vec<Order> = repair.iter().map(|(_, o)| o.clone()).collect();
                match submit_window(venue, &repair_orders, p.fill_window).await {
                    Some(outs) => {
                        for ((idx, order), result) in repair.iter_mut().zip(outs) {
                            let f =
                                close_order(store, ledger, order, result, ts_ms, errors).await?;
                            filled[*idx] = Qty(filled[*idx].0 + f.0);
                        }
                    }
                    None => {
                        for (_, order) in &mut repair {
                            persist_transition(
                                store,
                                order,
                                OrderState::Expired,
                                "repair window",
                                ts_ms,
                            )
                            .await?;
                        }
                    }
                }
                for (_, order) in repair {
                    orders.push(order);
                }
            }
        }
    }

    // --- Unwind any residual surplus (buy legs only). -------------------
    let post = surpluses(&intended, filled);
    for (leg, &surplus) in opp.fills.iter().zip(&post) {
        if surplus == 0 {
            continue;
        }
        // SELL-leg surplus is left to settle/hold — buying it back at the
        // ceiling tick would destroy value (see fn doc).
        let (action, tick) = match leg.action {
            Action::Buy => (Action::Sell, 1u16),
            Action::Sell => continue,
        };
        // Floor (1) / ceiling (levels-1) ticks are always representable.
        let Ok(px) = Px::new(tick, leg.ts) else {
            continue;
        };
        let fee = token_fee.get(&leg.token).copied().unwrap_or(Bps(0));
        let mut order = Order::new(fp.clone(), leg.token, action, leg.ts, px, Qty(surplus), fee);
        open_order(store, &mut order, ts_ms).await?;
        // Unwind FAK is OUTSIDE the fill window: the paper venue's own latency
        // bounds it; a live venue's FAK terminates by contract.
        let result = venue.submit_fak(&order).await;
        close_order(store, ledger, &mut order, result, ts_ms, errors).await?;
        orders.push(order);
    }

    // --- Outcome. -------------------------------------------------------
    let complete = (0..n).all(|i| filled[i].0 == intended[i].0);
    Ok(if complete {
        BasketOutcome::Repaired
    } else {
        BasketOutcome::Unwound
    })
}

// ---------------------------------------------------------------------------
// execute_basket
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub async fn execute_basket<V: ExecutionVenue>(
    venue: &mut V,
    store: &mpsc::Sender<StoreMsg>,
    opp: &Opportunity,
    _token_market: &HashMap<TokenId, MarketId>,
    market_tokens: &HashMap<MarketId, (TokenId, TokenId)>,
    token_fee: &HashMap<TokenId, Bps>,
    p: &ExecParams,
    ts_ms: i64,
) -> Result<BasketReport, ExecError> {
    // 1. Unhedged pre-guard: reject before ANY store write or venue call.
    let max_leg = opp.fills.iter().map(|l| l.cash.0.abs()).max().unwrap_or(0);
    if max_leg > p.max_unhedged.0 {
        return Ok(BasketReport {
            outcome: BasketOutcome::RejectedUnhedged,
            cash_delta: Usdc(0),
            positions: Vec::new(),
            order_errors: 0,
        });
    }

    let mut ledger = Ledger::default();

    // 2. Splits first (sell legs need the complete set in hand).
    for (market, units) in &opp.splits {
        let cash = venue
            .split(*market, *units)
            .await
            .map_err(|e| ExecError::Venue(e.to_string()))?;
        // A split for a market we don't know the token pair for is unexecutable:
        // we cannot write a valid Conversion row or update the ledger. Fail
        // closed rather than silently dropping collateral.
        let (yes, no) = market_tokens
            .get(market)
            .copied()
            .ok_or_else(|| ExecError::Venue("unknown market in splits".into()))?;
        let cash_micro = usdc_to_i64(cash).map_err(|_| ExecError::StoreClosed)?;
        let row = ConversionRow {
            kind: "split".into(),
            ts_ms,
            market: i64::from(market.0),
            yes_token: yes.0 as i64,
            no_token: no.0 as i64,
            units_micro: units.0 as i64,
            cash_micro,
        };
        store_acked(store, |ack| StoreMsg::Conversion(row, Some(ack))).await?;
        ledger.split(yes, no, *units, cash);
    }

    // 3. Orders: one per leg, write-ahead opened sequentially.
    let fp = format!("{:016x}", opp.fingerprint().as_u64());
    let mut orders: Vec<Order> = Vec::with_capacity(opp.fills.len());
    for l in &opp.fills {
        // Fee rate comes from the registry sync; a missing entry can only be a
        // token outside the universe, so 0 bps is the safe paper default.
        let fee = token_fee.get(&l.token).copied().unwrap_or(Bps(0));
        let mut order = Order::new(fp.clone(), l.token, l.action, l.ts, l.limit_px, l.qty, fee);
        open_order(store, &mut order, ts_ms).await?;
        orders.push(order);
    }

    // 4. Single FAK window across all legs.
    let mut errors = 0u32;
    let intended: Vec<Qty> = orders.iter().map(|o| o.qty).collect();
    let mut filled: Vec<Qty> = match submit_window(venue, &orders, p.fill_window).await {
        Some(outs) => {
            let mut filled = Vec::with_capacity(orders.len());
            for (order, result) in orders.iter_mut().zip(outs) {
                let f = close_order(store, &mut ledger, order, result, ts_ms, &mut errors).await?;
                filled.push(f);
            }
            filled
        }
        None => {
            // Window expired with no venue response: expire every leg flat.
            for order in &mut orders {
                persist_transition(store, order, OrderState::Expired, "fill window", ts_ms).await?;
            }
            vec![Qty(0); orders.len()]
        }
    };

    // 5. Assess.
    let all_intended = filled.iter().zip(&intended).all(|(f, i)| f.0 == i.0);
    let all_zero = filled.iter().all(|f| f.0 == 0);
    let outcome = if all_intended {
        BasketOutcome::FilledClean
    } else if all_zero && opp.splits.is_empty() {
        // Nothing filled and no collateral to recover: harmless settle no-op,
        // then a flat NoFill report.
        settle(venue, store, opp, &mut ledger, market_tokens, p, ts_ms).await?;
        return Ok(BasketReport {
            outcome: BasketOutcome::NoFill,
            cash_delta: Usdc(ledger.cash),
            positions: ledger.positions(),
            order_errors: errors,
        });
    } else {
        // Partial (or zero fills but splits left holdings to recover): Task 10.
        repair_and_unwind(
            venue,
            store,
            opp,
            &mut orders,
            &mut filled,
            &mut ledger,
            token_fee,
            p,
            ts_ms,
            &mut errors,
        )
        .await?
    };

    // 6. Settlement (merge back complete sets).
    settle(venue, store, opp, &mut ledger, market_tokens, p, ts_ms).await?;

    // 7. Report.
    Ok(BasketReport {
        outcome,
        cash_delta: Usdc(ledger.cash),
        positions: ledger.positions(),
        order_errors: errors,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::collections::{HashSet, VecDeque};

    use pm_core::instrument::{MarketId, TokenId};
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};
    use pm_engine::{Action, ArbClass, GasTable, LegFill, Opportunity, RedeemStrategy};
    use pm_store::Store;
    use pm_store::writer::run_writer;

    use crate::venue::{Fill, SubmitOutcome};

    fn gas() -> GasTable {
        GasTable {
            split: 10_000,
            merge: 10_000,
            redeem: 15_000,
            negrisk_convert: 20_000,
        }
    }

    // ---- MockVenue -------------------------------------------------------

    struct MockVenue {
        script: HashMap<TokenId, VecDeque<SubmitOutcome>>,
        errors: HashSet<TokenId>,
        gas: GasTable,
        merges: Vec<(MarketId, Qty)>,
        splits: Vec<(MarketId, Qty)>,
    }

    impl MockVenue {
        fn new() -> Self {
            MockVenue {
                script: HashMap::new(),
                errors: HashSet::new(),
                gas: gas(),
                merges: Vec::new(),
                splits: Vec::new(),
            }
        }

        fn script(&mut self, token: TokenId, out: SubmitOutcome) {
            self.script.entry(token).or_default().push_back(out);
        }
    }

    impl ExecutionVenue for MockVenue {
        async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
            if self.errors.contains(&order.token) {
                return Err(VenueError::BookUnavailable(order.token));
            }
            Ok(self
                .script
                .get_mut(&order.token)
                .and_then(|q| q.pop_front())
                .unwrap_or_default())
        }

        async fn split(&mut self, market: MarketId, units: Qty) -> Result<Usdc, VenueError> {
            self.splits.push((market, units));
            // $1/share collateral + gas, µshares as µUSDC 1:1.
            Ok(Usdc(-(units.0 as i128 + i128::from(self.gas.split))))
        }

        async fn merge(&mut self, market: MarketId, units: Qty) -> Result<Usdc, VenueError> {
            self.merges.push((market, units));
            Ok(Usdc(units.0 as i128 - i128::from(self.gas.merge)))
        }
    }

    /// Build a correct-cash SubmitOutcome for a full fill at `ticks` (Cent), fee 0.
    fn will_fill(_token: TokenId, action: Action, ticks: u16, qty: Qty) -> SubmitOutcome {
        let px = Px::new(ticks, TickSize::Cent).unwrap();
        let px_micro = px.microusdc(TickSize::Cent);
        let cash = match action {
            Action::Buy => Usdc(-pm_core::num::buy_cost(px_micro, qty).0),
            Action::Sell => Usdc(pm_core::num::sell_proceeds(px_micro, qty).0),
        };
        SubmitOutcome {
            fills: vec![Fill {
                px,
                qty,
                cash,
                fee: Usdc(0),
            }],
            filled: qty,
        }
    }

    // ---- Fixtures --------------------------------------------------------

    const SH: u64 = 100_000_000; // 100 shares in µshares
    const UNITS: u64 = 100_000_000;

    fn leg(token: u64, action: Action, ticks: u16, qty: u64, cash: i128) -> LegFill {
        LegFill {
            token: TokenId(token),
            action,
            ts: TickSize::Cent,
            limit_px: Px::new(ticks, TickSize::Cent).unwrap(),
            qty: Qty(qty),
            cash: Usdc(cash),
        }
    }

    fn c1long() -> Opportunity {
        Opportunity {
            class: ArbClass::C1Long,
            fills: vec![
                leg(1, Action::Buy, 44, SH, -44_000_000),
                leg(2, Action::Buy, 50, SH, -50_000_000),
            ],
            units: Qty(UNITS),
            net: Usdc(5_990_000),
            basis: Usdc(94_000_000),
            edge: Bps(637),
            splits: vec![],
        }
    }

    fn c1short() -> Opportunity {
        Opportunity {
            class: ArbClass::C1Short,
            fills: vec![
                leg(1, Action::Sell, 52, SH, 52_000_000),
                leg(2, Action::Sell, 53, SH, 53_000_000),
            ],
            units: Qty(UNITS),
            net: Usdc(4_990_000),
            basis: Usdc(100_010_000),
            edge: Bps(498),
            splits: vec![(MarketId(0), Qty(UNITS))],
        }
    }

    fn maps() -> (
        HashMap<TokenId, MarketId>,
        HashMap<MarketId, (TokenId, TokenId)>,
    ) {
        let token_market = HashMap::from([(TokenId(1), MarketId(0)), (TokenId(2), MarketId(0))]);
        let market_tokens = HashMap::from([(MarketId(0), (TokenId(1), TokenId(2)))]);
        (token_market, market_tokens)
    }

    fn params(redeem: RedeemStrategy) -> ExecParams {
        ExecParams {
            fill_window: Duration::from_millis(500),
            max_unhedged: Usdc(200_000_000),
            redeem,
        }
    }

    /// Spawn an in-memory store writer, run execute_basket with an empty fee
    /// map, drop tx, and return (report, Store) for inspection.
    async fn run<V: ExecutionVenue>(
        venue: &mut V,
        opp: &Opportunity,
        p: ExecParams,
    ) -> (BasketReport, Store) {
        let store = Store::open_in_memory().unwrap();
        let (tx, rx) = mpsc::channel(64);
        let writer = tokio::spawn(run_writer(store, rx));
        let (token_market, market_tokens) = maps();
        let token_fee: HashMap<TokenId, Bps> = HashMap::new();
        let report = execute_basket(
            venue,
            &tx,
            opp,
            &token_market,
            &market_tokens,
            &token_fee,
            &p,
            1,
        )
        .await
        .unwrap();
        drop(tx);
        let store = writer.await.unwrap();
        (report, store)
    }

    // ---- Tests -----------------------------------------------------------

    #[tokio::test]
    async fn c1long_clean_fill_merges_and_realizes() {
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Buy, 44, Qty(SH)));
        v.script(TokenId(2), will_fill(TokenId(2), Action::Buy, 50, Qty(SH)));
        let (report, store) = run(&mut v, &c1long(), params(RedeemStrategy::Merge)).await;

        assert_eq!(report.outcome, BasketOutcome::FilledClean);
        assert_eq!(report.cash_delta, Usdc(5_990_000));
        assert!(report.positions.is_empty());
        assert_eq!(report.order_errors, 0);
        assert_eq!(v.merges, vec![(MarketId(0), Qty(100_000_000))]);
        assert_eq!(store.realized_total().unwrap(), 5_990_000);
        assert!(store.open_orders().unwrap().is_empty());
    }

    #[tokio::test]
    async fn c1long_hold_keeps_positions_and_skips_merge() {
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Buy, 44, Qty(SH)));
        v.script(TokenId(2), will_fill(TokenId(2), Action::Buy, 50, Qty(SH)));
        let (report, store) = run(&mut v, &c1long(), params(RedeemStrategy::Hold)).await;

        assert_eq!(report.outcome, BasketOutcome::FilledClean);
        assert!(v.merges.is_empty());
        assert_eq!(report.cash_delta, Usdc(-94_000_000));
        assert_eq!(
            report.positions,
            vec![
                (TokenId(1), Qty(100_000_000), Usdc(44_000_000)),
                (TokenId(2), Qty(100_000_000), Usdc(50_000_000)),
            ]
        );
        assert_eq!(store.position(1).unwrap(), (100_000_000, 44_000_000));
    }

    #[tokio::test]
    async fn c1short_splits_first_then_sells_clean() {
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Sell, 52, Qty(SH)));
        v.script(TokenId(2), will_fill(TokenId(2), Action::Sell, 53, Qty(SH)));
        let (report, store) = run(&mut v, &c1short(), params(RedeemStrategy::Merge)).await;

        assert_eq!(v.splits, vec![(MarketId(0), Qty(100_000_000))]);
        assert_eq!(report.outcome, BasketOutcome::FilledClean);
        assert_eq!(report.cash_delta, Usdc(4_990_000));
        assert!(report.positions.is_empty());
        assert!(v.merges.is_empty());
        assert_eq!(store.realized_total().unwrap(), 4_990_000);
    }

    #[tokio::test]
    async fn unhedged_pre_guard_rejects_without_orders() {
        let mut v = MockVenue::new();
        let mut p = params(RedeemStrategy::Merge);
        p.max_unhedged = Usdc(40_000_000); // c1long NO leg is $50 > $40
        let (report, store) = run(&mut v, &c1long(), p).await;

        assert_eq!(report.outcome, BasketOutcome::RejectedUnhedged);
        assert_eq!(report.cash_delta, Usdc(0));
        assert!(report.positions.is_empty());
        assert_eq!(report.order_errors, 0);
        assert!(store.open_orders().unwrap().is_empty());
        assert_eq!(store.count_fills().unwrap(), 0);
    }

    #[test]
    fn dust_sell_negative_cash_flows_through_ledger() {
        let mut l = Ledger::default();
        l.buy(TokenId(1), Qty(2), Usdc(-2));
        l.sell(TokenId(1), Qty(1), Usdc(-1));
        assert_eq!(l.cash, -3); // negative sell cash accepted (dust fee > floored proceeds)
        assert_eq!(l.held(TokenId(1)), 1);
    }

    // ---- Task 10: repair / unwind / mid-basket unhedged guard ------------

    /// Build a C4Lp 3-leg buy opportunity spanning two markets, plus its maps.
    /// legs: buy tok1@44, buy tok3@44, buy tok2@50 (each 100sh). units Qty(0).
    #[allow(clippy::type_complexity)]
    fn c4_three_leg() -> (
        Opportunity,
        HashMap<TokenId, MarketId>,
        HashMap<MarketId, (TokenId, TokenId)>,
    ) {
        let opp = Opportunity {
            class: ArbClass::C4Lp,
            fills: vec![
                leg(1, Action::Buy, 44, SH, -44_000_000),
                leg(3, Action::Buy, 44, SH, -44_000_000),
                leg(2, Action::Buy, 50, SH, -50_000_000),
            ],
            units: Qty(0),
            net: Usdc(1_500_000),
            basis: Usdc(138_000_000),
            edge: Bps(108),
            splits: vec![],
        };
        // mkt0 = (tok1, tok2); mkt1 = (tok3, tok4).
        let token_market = HashMap::from([
            (TokenId(1), MarketId(0)),
            (TokenId(2), MarketId(0)),
            (TokenId(3), MarketId(1)),
            (TokenId(4), MarketId(1)),
        ]);
        let market_tokens = HashMap::from([
            (MarketId(0), (TokenId(1), TokenId(2))),
            (MarketId(1), (TokenId(3), TokenId(4))),
        ]);
        (opp, token_market, market_tokens)
    }

    /// Like `run`, but with caller-supplied maps (for multi-market baskets).
    async fn run_with_maps<V: ExecutionVenue>(
        venue: &mut V,
        opp: &Opportunity,
        token_market: &HashMap<TokenId, MarketId>,
        market_tokens: &HashMap<MarketId, (TokenId, TokenId)>,
        p: ExecParams,
    ) -> (BasketReport, Store) {
        let store = Store::open_in_memory().unwrap();
        let (tx, rx) = mpsc::channel(64);
        let writer = tokio::spawn(run_writer(store, rx));
        let token_fee: HashMap<TokenId, Bps> = HashMap::new();
        let report = execute_basket(
            venue,
            &tx,
            opp,
            token_market,
            market_tokens,
            &token_fee,
            &p,
            1,
        )
        .await
        .unwrap();
        drop(tx);
        let store = writer.await.unwrap();
        (report, store)
    }

    #[tokio::test]
    async fn partial_fill_repairs_at_break_even_bump_and_completes() {
        // c1long; YES fills 100sh@44; NO fills 60sh@50 then repair tops up 40sh@64.
        // Repair math: rem_NO=40e6; w=ceil(50e6*40e6/100e6)=20e6; Sw=20e6;
        // slack=floor(5_990_000*20e6/20e6)=5_990_000; per-share=floor(5_990_000*1e6/40e6)=149_750;
        // bump=floor(149_750/10_000)=14 -> limit 50+14=64.
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Buy, 44, Qty(SH)));
        // NO: first the initial partial (60sh), then the repair fill (40sh@64).
        v.script(
            TokenId(2),
            will_fill(TokenId(2), Action::Buy, 50, Qty(60_000_000)),
        );
        v.script(
            TokenId(2),
            will_fill(TokenId(2), Action::Buy, 64, Qty(40_000_000)),
        );
        let (report, store) = run(&mut v, &c1long(), params(RedeemStrategy::Merge)).await;

        // cash: -44e6 (YES) -30e6 (NO initial) -25_600_000 (NO repair @64) +99_990_000 (merge)
        assert_eq!(report.outcome, BasketOutcome::Repaired);
        assert_eq!(report.cash_delta, Usdc(390_000));
        assert!(report.positions.is_empty());
        assert_eq!(report.order_errors, 0);
        assert_eq!(v.merges, vec![(MarketId(0), Qty(100_000_000))]);
        assert_eq!(store.realized_total().unwrap(), 390_000);
        assert!(store.open_orders().unwrap().is_empty());
    }

    #[tokio::test]
    async fn repair_fails_then_unwind_markets_out_surplus() {
        // c1long; YES fills 100sh@44; NO never fills (initial nor repair);
        // surplus_YES=100e6 -> unwind Sell; bids absorb only 30sh@40.
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Buy, 44, Qty(SH)));
        // YES unwind: Sell, only 30sh fill at the extreme limit.
        v.script(
            TokenId(1),
            will_fill(TokenId(1), Action::Sell, 40, Qty(30_000_000)),
        );
        let (report, store) = run(&mut v, &c1long(), params(RedeemStrategy::Merge)).await;

        // cash: -44e6 (YES buy) + 12e6 (unwind sell 30sh@40) = -32e6
        assert_eq!(report.outcome, BasketOutcome::Unwound);
        assert_eq!(report.cash_delta, Usdc(-32_000_000));
        // YES held 70sh; cost 44e6 - ceil(44e6*30/100)=44e6-13_200_000=30_800_000
        assert_eq!(
            report.positions,
            vec![(TokenId(1), Qty(70_000_000), Usdc(30_800_000))]
        );
        assert!(v.merges.is_empty());
        let _ = store;
    }

    #[tokio::test]
    async fn unhedged_imbalance_skips_repair_and_unwinds_immediately() {
        // C4Lp 3-leg; tok1 & tok3 fill 100sh, tok2 zero. Surplus cost 88e6 > 50e6
        // -> repair SKIPPED entirely; the tok2 repair script entry stays unconsumed.
        let (opp, token_market, market_tokens) = c4_three_leg();
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Buy, 44, Qty(SH)));
        v.script(TokenId(3), will_fill(TokenId(3), Action::Buy, 44, Qty(SH)));
        // tok2: explicit zero initial fill, then a would-be repair fill that must
        // NOT be consumed (repair is skipped by the unhedged guard).
        v.script(TokenId(2), SubmitOutcome::default());
        v.script(TokenId(2), will_fill(TokenId(2), Action::Buy, 64, Qty(SH)));
        // unwind exits tok1/tok3 fully at 40.
        v.script(TokenId(1), will_fill(TokenId(1), Action::Sell, 40, Qty(SH)));
        v.script(TokenId(3), will_fill(TokenId(3), Action::Sell, 40, Qty(SH)));

        let mut p = params(RedeemStrategy::Merge);
        p.max_unhedged = Usdc(50_000_000); // passes pre-guard (max leg 50e6 <= 50e6)
        let (report, _store) = run_with_maps(&mut v, &opp, &token_market, &market_tokens, p).await;

        assert_eq!(report.outcome, BasketOutcome::Unwound);
        // The tok2 repair entry was never consumed.
        assert_eq!(v.script.get(&TokenId(2)).map(|q| q.len()), Some(1));
        assert!(report.positions.is_empty());
        // cash: -44e6 -44e6 -0 + 40e6 + 40e6 = -8e6
        assert_eq!(report.cash_delta, Usdc(-8_000_000));
    }

    #[tokio::test]
    async fn venue_error_leg_is_rejected_and_counted() {
        // c1long; YES fills; NO errors (BookUnavailable) on initial AND repair.
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Buy, 44, Qty(SH)));
        v.errors.insert(TokenId(2));
        // YES unwind exits fully at 40.
        v.script(TokenId(1), will_fill(TokenId(1), Action::Sell, 40, Qty(SH)));
        let (report, _store) = run(&mut v, &c1long(), params(RedeemStrategy::Merge)).await;

        assert!(report.order_errors >= 1);
        assert_eq!(report.outcome, BasketOutcome::Unwound);
        assert!(report.positions.is_empty());
        // cash: -44e6 + 40e6 = -4e6
        assert_eq!(report.cash_delta, Usdc(-4_000_000));
    }

    #[tokio::test]
    async fn c1short_partial_sell_merges_back_unsold() {
        // c1short (split 100); YES Sell@52 fills 100sh; NO Sell@53 fills 60sh.
        // NO repair (bump=12 -> Sell@41) gets zero fill. surplus_YES=40e6 is a
        // SELL-leg surplus -> unwind SKIPS it (no value-destroying buy-back); the
        // unsold NO remainder stays held and settle merges back min(yes,no)=0.
        let mut v = MockVenue::new();
        v.script(TokenId(1), will_fill(TokenId(1), Action::Sell, 52, Qty(SH)));
        v.script(
            TokenId(2),
            will_fill(TokenId(2), Action::Sell, 53, Qty(60_000_000)),
        );
        let (report, store) = run(&mut v, &c1short(), params(RedeemStrategy::Merge)).await;

        assert_eq!(report.outcome, BasketOutcome::Unwound);
        assert!(v.merges.is_empty());
        // NO lot: split cost 50_005_000; sold 60sh -> consumed ceil(50_005_000*60/100)
        // = 30_003_000; remaining (40e6, 20_002_000).
        assert_eq!(
            report.positions,
            vec![(TokenId(2), Qty(40_000_000), Usdc(20_002_000))]
        );
        // cash: -100_010_000 (split) + 52e6 (YES) + 31_800_000 (NO) = -16_210_000
        assert_eq!(report.cash_delta, Usdc(-16_210_000));
        let _ = store;
    }

    #[tokio::test]
    async fn fill_window_expiry_expires_all_legs_flat() {
        tokio::time::pause();

        struct NeverVenue;
        impl ExecutionVenue for NeverVenue {
            async fn submit_fak(&mut self, _o: &Order) -> Result<SubmitOutcome, VenueError> {
                std::future::pending().await
            }
            async fn submit_all(
                &mut self,
                _orders: &[Order],
            ) -> Vec<Result<SubmitOutcome, VenueError>> {
                std::future::pending().await
            }
            async fn split(&mut self, _m: MarketId, _u: Qty) -> Result<Usdc, VenueError> {
                unreachable!()
            }
            async fn merge(&mut self, _m: MarketId, _u: Qty) -> Result<Usdc, VenueError> {
                unreachable!()
            }
        }

        let mut v = NeverVenue;
        let (report, store) = run(&mut v, &c1long(), params(RedeemStrategy::Merge)).await;

        assert_eq!(report.outcome, BasketOutcome::NoFill);
        assert_eq!(report.cash_delta, Usdc(0));
        assert!(store.open_orders().unwrap().is_empty());
    }
}
