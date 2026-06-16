//! FIFO lot consumption over an open rusqlite transaction (spec §16).

use rusqlite::Transaction;

use crate::StoreError;

/// ceil(a·b/c) for non-negative i64 inputs, computed in i128.
/// Uses checked cast — panics in debug, returns error-safe value in release
/// only if the result fits i64 (mathematically it always does since a ≤ i64::MAX).
fn ceil_mul_div(a: i64, b: i64, c: i64) -> Result<i64, StoreError> {
    debug_assert!(a >= 0 && b >= 0 && c > 0);
    let n = i128::from(a) * i128::from(b);
    let d = i128::from(c);
    let result = (n + d - 1) / d;
    i64::try_from(result).map_err(|_| StoreError::Overflow("ceil_mul_div result overflows i64"))
}

/// floor(a·b/c) for non-negative i64 inputs, computed in i128 — the flooring
/// counterpart of [`ceil_mul_div`]. Used by the SIGNED path to split a fill's
/// proceeds/cost so the CURRENT realized stays a floor (against us): the
/// closing side's proceeds (Sell) and a covered short's consumed basis
/// magnitude (Buy) both round DOWN.
fn floor_mul_div(a: i64, b: i64, c: i64) -> Result<i64, StoreError> {
    debug_assert!(a >= 0 && b >= 0 && c > 0);
    let n = i128::from(a) * i128::from(b);
    let d = i128::from(c);
    let result = n / d; // non-negative inputs → truncation is floor
    i64::try_from(result).map_err(|_| StoreError::Overflow("floor_mul_div result overflows i64"))
}

/// Insert a new lot for `token`.
pub fn insert_lot(
    tx: &Transaction,
    token: i64,
    ts_ms: i64,
    qty_micro: i64,
    cost_micro: i64,
) -> Result<(), StoreError> {
    tx.execute(
        "INSERT INTO lots (token, ts_ms, qty_micro, remaining_micro, cost_micro, cost_remaining_micro)
         VALUES (?1, ?2, ?3, ?3, ?4, ?4)",
        rusqlite::params![token, ts_ms, qty_micro, cost_micro],
    )?;
    Ok(())
}

/// Consume `qty_micro` shares of `token` oldest-lot-first. Returns the cost
/// basis consumed (µUSDC, rounded UP against us per partial lot).
/// Errors with `Oversell` (caller must roll back the tx) if holdings are short.
pub fn consume_lots(tx: &Transaction, token: i64, qty_micro: i64) -> Result<i64, StoreError> {
    let mut need = qty_micro;
    let mut consumed_cost: i64 = 0;

    let lots: Vec<(i64, i64, i64)> = {
        let mut stmt = tx.prepare(
            "SELECT id, remaining_micro, cost_remaining_micro FROM lots
             WHERE token = ?1 AND remaining_micro > 0 ORDER BY id",
        )?;
        stmt.query_map([token], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?
    };

    for (id, remaining, cost_remaining) in lots {
        if need == 0 {
            break;
        }
        let take = need.min(remaining);
        let cost = if take == remaining {
            // Full consumption: exact, no rounding — conserves cost_remaining exactly.
            cost_remaining
        } else {
            // Partial consumption: round UP (against us) so realized profit is a floor.
            ceil_mul_div(cost_remaining, take, remaining)?
        };
        tx.execute(
            "UPDATE lots SET remaining_micro = remaining_micro - ?2,
             cost_remaining_micro = cost_remaining_micro - ?3 WHERE id = ?1",
            rusqlite::params![id, take, cost],
        )?;
        need -= take;
        consumed_cost += cost;
    }

    if need > 0 {
        return Err(StoreError::Oversell {
            token,
            missing_micro: need,
        });
    }
    Ok(consumed_cost)
}

/// Consume up to `want_qty` µshares of the token's OPEN lots on ONE side,
/// oldest-first, returning `(consumed_qty, consumed_cost_signed)`. The
/// signed-path counterpart of [`consume_lots`] that NEVER errors when holdings
/// run short — it consumes what exists and the caller opens the opposite side
/// for any remainder.
///
/// - `long == true` walks LONG lots (`remaining_micro > 0`, positive cost) —
///   the signed Sell's long-closing leg.
/// - `long == false` walks SHORT lots (`remaining_micro < 0`, negative basis) —
///   the signed Buy's short-covering leg.
///
/// `consumed_qty` is the positive share count actually consumed (`≤ want_qty`,
/// less only if that side runs out). `consumed_cost_signed` is the signed
/// cost/basis removed: `> 0` closing longs, `< 0` covering shorts.
///
/// PARTIAL-lot rounding is "against us" so the caller's realized is a floor,
/// mirroring [`consume_lots`]: a long's consumed cost rounds UP, a short's
/// consumed basis MAGNITUDE rounds DOWN (so its negative value rounds toward 0).
/// A FULL lot consumes its exact `cost_remaining`, conserving basis precisely
/// across repeated partials (same invariant the strict path's tests assert).
fn consume_open_side(
    tx: &Transaction,
    token: i64,
    want_qty: i64,
    long: bool,
) -> Result<(i64, i64), StoreError> {
    let lots: Vec<(i64, i64, i64)> = {
        // Internal predicate (no user input) selects the side; the partial
        // `lots_token` index (`remaining_micro <> 0`) serves both directions.
        let sql = if long {
            "SELECT id, remaining_micro, cost_remaining_micro FROM lots
             WHERE token = ?1 AND remaining_micro > 0 ORDER BY id"
        } else {
            "SELECT id, remaining_micro, cost_remaining_micro FROM lots
             WHERE token = ?1 AND remaining_micro < 0 ORDER BY id"
        };
        let mut stmt = tx.prepare(sql)?;
        stmt.query_map([token], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?
    };

    let mut need = want_qty;
    let mut consumed_qty: i64 = 0;
    let mut consumed_cost: i64 = 0;

    for (id, remaining, cost_remaining) in lots {
        if need == 0 {
            break;
        }
        let avail = remaining.abs(); // share magnitude in this lot
        let take = need.min(avail);
        let cost = if take == avail {
            // Full consumption: exact, no rounding (conserves cost_remaining).
            cost_remaining
        } else if long {
            // Partial long: consumed cost rounds UP (realized floor).
            ceil_mul_div(cost_remaining, take, avail)?
        } else {
            // Partial short: consumed basis MAGNITUDE rounds DOWN (realized
            // floor); the signed basis is negative so we negate the magnitude.
            -floor_mul_div(-cost_remaining, take, avail)?
        };
        // Both columns move toward 0: subtract a positive delta for longs and a
        // negative one for shorts (so a short's remaining_micro rises toward 0).
        let delta_qty = if long { take } else { -take };
        tx.execute(
            "UPDATE lots SET remaining_micro = remaining_micro - ?2,
             cost_remaining_micro = cost_remaining_micro - ?3 WHERE id = ?1",
            rusqlite::params![id, delta_qty, cost],
        )?;
        need -= take;
        consumed_qty += take;
        consumed_cost += cost;
    }
    Ok((consumed_qty, consumed_cost))
}

/// SIGNED-mode SELL (market-making): close FIFO LONG lots first, then open a
/// SHORT lot for any uncovered remainder. Returns realized µUSDC. NEVER errors
/// with `Oversell` (that strict safety check stays on [`consume_lots`]).
///
/// `qty_micro` is shares sold (`> 0`); `cash_micro` is the proceeds (`≥ 0`, net
/// of fee). Proceeds are split by share count: the closed leg's portion is
/// floored (against us → realized floor) and `realized = proceeds_close −
/// consumed_long_cost`; the remainder opens a short (`qty −remainder`, basis
/// `−remaining_proceeds`), so total proceeds are conserved. When the longs fully
/// cover the sell this reduces to the strict Sell (`proceeds == cash_micro`,
/// `realized == cash_micro − consumed`).
pub fn sell_signed(
    tx: &Transaction,
    token: i64,
    ts_ms: i64,
    qty_micro: i64,
    cash_micro: i64,
) -> Result<i64, StoreError> {
    let (closed_qty, consumed_cost) = consume_open_side(tx, token, qty_micro, true)?;
    let proceeds_close = if closed_qty == qty_micro {
        cash_micro // fully covered by longs → no split, identical to strict Sell
    } else {
        floor_mul_div(cash_micro, closed_qty, qty_micro)?
    };
    let realized = proceeds_close - consumed_cost;
    let open_qty = qty_micro - closed_qty;
    if open_qty > 0 {
        let proceeds_open = cash_micro - proceeds_close;
        // Short lot: negative qty + negative basis (cash taken in), so SUM-based
        // `position` reports a signed short directly.
        insert_lot(tx, token, ts_ms, -open_qty, -proceeds_open)?;
    }
    Ok(realized)
}

/// SIGNED-mode BUY (market-making): cover FIFO SHORT lots first, then open a
/// LONG lot for any remainder. Returns realized µUSDC.
///
/// `qty_micro` is shares bought (`> 0`); `cash_micro` is signed cash (`≤ 0` for
/// a buy, so cost paid = `−cash_micro`). The cost is split by share count: the
/// cover leg's portion is ceiled (against us → realized floor) and `realized =
/// proceeds_consumed − cost_cover`, where `proceeds_consumed` is the magnitude
/// of the short basis released (the proceeds originally received). The remainder
/// opens a long (cost = `cost_paid − cost_cover`), conserving total cost. With
/// no shorts to cover this reduces to the strict Buy (opens a long, realized 0).
pub fn buy_signed(
    tx: &Transaction,
    token: i64,
    ts_ms: i64,
    qty_micro: i64,
    cash_micro: i64,
) -> Result<i64, StoreError> {
    let cost_paid = -cash_micro; // cash_micro ≤ 0 for a buy → cost_paid ≥ 0
    let (covered_qty, consumed_basis) = consume_open_side(tx, token, qty_micro, false)?;
    let cost_cover = if covered_qty == qty_micro {
        cost_paid // fully covers existing shorts → no split
    } else {
        ceil_mul_div(cost_paid, covered_qty, qty_micro)?
    };
    // consumed_basis ≤ 0 (short basis); its magnitude is the proceeds released.
    let realized = -consumed_basis - cost_cover;
    let open_qty = qty_micro - covered_qty;
    if open_qty > 0 {
        let cost_open = cost_paid - cost_cover;
        insert_lot(tx, token, ts_ms, open_qty, cost_open)?;
    }
    Ok(realized)
}
