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
