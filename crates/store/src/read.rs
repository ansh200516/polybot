//! Read-only second connection to the WAL database for the TUI dashboard.
//!
//! ## Rationale
//!
//! SQLite's WAL mode allows arbitrarily many concurrent readers alongside a
//! single writer. By opening with `SQLITE_OPEN_READ_ONLY` the dashboard
//! connection is structurally incapable of initiating a write transaction,
//! which makes it impossible for the TUI to block the ingestion/producer path
//! (spec §17). `busy_timeout(100 ms)` bounds contention in the rare case where
//! a WAL checkpoint briefly holds the read lock.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::{CopyPositionRow, DAY_MS, PnlRow, StoreError};

// ---------------------------------------------------------------------------
// View types — subset of columns surfaced to the dashboard
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct OppView {
    pub ts_ms: i64,
    pub class: String,
    pub edge_bps: i64,
    pub units_micro: i64,
    pub net_micro: i64,
    pub legs_json: String,
    pub dispatched: bool,
}

#[derive(Debug, Clone)]
pub struct FillView {
    pub ts_ms: i64,
    pub token: i64,
    pub action: String,
    pub px_ticks: i64,
    pub tick_levels: i64,
    pub qty_micro: i64,
    pub cash_micro: i64,
    /// Strategy that produced the fill (`"arb"` / `"mm"`); lets the dashboard
    /// tag each fill row by which strategy traded.
    pub strategy: String,
}

#[derive(Debug, Clone)]
pub struct OrderEventView {
    pub ts_ms: i64,
    pub order_id: String,
    pub state: String,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct HaltView {
    pub ts_ms: i64,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// ReadStore
// ---------------------------------------------------------------------------

pub struct ReadStore {
    conn: Connection,
}

impl ReadStore {
    /// Open read-only. Errors if the database file does not exist (the writer
    /// creates it; the dashboard must never create an empty shadow db).
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        // Bounded wait if the writer holds the lock during a checkpoint.
        conn.busy_timeout(std::time::Duration::from_millis(100))?;
        Ok(ReadStore { conn })
    }

    /// Most-recent `n` opportunities, newest first.
    pub fn recent_opportunities(&self, n: usize) -> Result<Vec<OppView>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, class, edge_bps, units_micro, net_micro, legs_json, dispatched
             FROM opportunities ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map([n as i64], |row| {
                Ok(OppView {
                    ts_ms: row.get(0)?,
                    class: row.get(1)?,
                    edge_bps: row.get(2)?,
                    units_micro: row.get(3)?,
                    net_micro: row.get(4)?,
                    legs_json: row.get(5)?,
                    dispatched: row.get::<_, i64>(6)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Most-recent `n` fills, newest first.
    pub fn recent_fills(&self, n: usize) -> Result<Vec<FillView>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, token, action, px_ticks, tick_levels, qty_micro, cash_micro, strategy
             FROM fills ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map([n as i64], |row| {
                Ok(FillView {
                    ts_ms: row.get(0)?,
                    token: row.get(1)?,
                    action: row.get(2)?,
                    px_ticks: row.get(3)?,
                    tick_levels: row.get(4)?,
                    qty_micro: row.get(5)?,
                    cash_micro: row.get(6)?,
                    strategy: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Most-recent `n` order events, newest first.
    pub fn recent_order_events(&self, n: usize) -> Result<Vec<OrderEventView>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, order_id, state, detail
             FROM order_events ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map([n as i64], |row| {
                Ok(OrderEventView {
                    ts_ms: row.get(0)?,
                    order_id: row.get(1)?,
                    state: row.get(2)?,
                    detail: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Most-recent `n` orders, newest first. The durable `orders` table is the
    /// order ledger (one row per submitted order); `recent_order_events`
    /// surfaces the per-state transition log. The dashboard's Orders panel wants
    /// the former — `order_events` stays empty until the executor emits
    /// transitions, whereas every submitted order lands here immediately.
    pub fn recent_orders(&self, n: usize) -> Result<Vec<OrderEventView>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, id, state, '' FROM orders ORDER BY ts_ms DESC, id DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map([n as i64], |row| {
                Ok(OrderEventView {
                    ts_ms: row.get(0)?,
                    order_id: row.get(1)?,
                    state: row.get(2)?,
                    detail: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Open positions from FIFO lots, grouped per `(token, strategy)`:
    /// `(token, strategy, signed net µshares, signed remaining cost µUSDC)`,
    /// ordered by token then strategy.
    ///
    /// Each strategy's lots are summed independently, so an arb long and an mm
    /// short on the SAME token surface as two distinct rows. The `HAVING ... <>
    /// 0` predicate (was `> 0`) keeps SHORT positions (negative net, negative
    /// basis) as well as longs — only fully-closed `(token, strategy)` groups
    /// (net 0) are dropped. On an arb-only DB every lot is `'arb'`, so this
    /// returns exactly the long rows the old per-token query did, each now
    /// carrying the `"arb"` tag.
    pub fn open_positions(&self) -> Result<Vec<(i64, String, i64, i64)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT token, strategy, SUM(remaining_micro), SUM(cost_remaining_micro)
             FROM lots GROUP BY token, strategy HAVING SUM(remaining_micro) <> 0
             ORDER BY token, strategy",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The most-recent P&L snapshot, or `None` if the table is empty.
    pub fn latest_pnl(&self) -> Result<Option<PnlRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, cash_micro, realized_micro, unrealized_micro, equity_micro, strategy
             FROM pnl_snapshots ORDER BY id DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map([], |row| {
            Ok(PnlRow {
                ts_ms: row.get(0)?,
                cash_micro: row.get(1)?,
                realized_micro: row.get(2)?,
                unrealized_micro: row.get(3)?,
                equity_micro: row.get(4)?,
                strategy: row.get(5)?,
            })
        })?;
        rows.next().transpose().map_err(StoreError::from)
    }

    /// Most-recent `n` P&L snapshots for a single `strategy`, newest first.
    /// Mirrors the other `recent_*` readers; backs per-strategy dashboards.
    pub fn recent_pnl_by_strategy(
        &self,
        strategy: &str,
        n: usize,
    ) -> Result<Vec<PnlRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, cash_micro, realized_micro, unrealized_micro, equity_micro, strategy
             FROM pnl_snapshots WHERE strategy = ?1 ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![strategy, n as i64], |row| {
                Ok(PnlRow {
                    ts_ms: row.get(0)?,
                    cash_micro: row.get(1)?,
                    realized_micro: row.get(2)?,
                    unrealized_micro: row.get(3)?,
                    equity_micro: row.get(4)?,
                    strategy: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The WORST (most negative) intraday `realized_micro + unrealized_micro`
    /// over ALL of `strategy`'s snapshots in the UTC day `[utc_day·DAY_MS,
    /// +DAY_MS)`, in µUSDC (i128); `0` if the strategy logged no snapshot that
    /// day. `utc_day` is a whole-day index (see [`crate::utc_day_from_ms`]).
    ///
    /// Backs the market-maker's PERSISTENT UTC-day loss cap: on startup the MM
    /// reads this and refuses to quote when the day's worst point is already
    /// at/under its daily-loss cap, so the cap binds across the periodic
    /// auto-restart instead of resetting every session.
    ///
    /// Uses the day's MINIMUM, NOT the latest snapshot, deliberately. A latest-
    /// snapshot read re-armed the bot after a cap breach: once a losing session
    /// halts and goes flat it writes fresh ~0 P&L snapshots, so the newest row
    /// recovers to ~0 and a restart would quote again. The worst-point snapshot
    /// stays in the day's history, so `MIN` keeps the gate latched across every
    /// restart that day — and it is also the correct "max intraday drawdown"
    /// semantic for a daily loss cap (down the cap at ANY point → stop for the day).
    ///
    /// SCOPE (what this query alone does / does NOT catch): `realized_micro` is
    /// cumulative PER SESSION and resets on each restart, so MANY sub-cap sessions
    /// — none individually reaching the cap, but whose realized losses SUM over it
    /// across the day — are NOT visible to THIS query (each session's worst point
    /// is itself sub-cap). MIN catches the dominant cases: any SINGLE-session cap
    /// breach, and held/unrealized drawdowns (the seeded position keeps marking
    /// negative). The summed-sub-cap-*realized* gap is now CLOSED by the separate
    /// cumulative day-realized ledger ([`day_realized_micro`](Self::day_realized_micro),
    /// I3): the MM latches the day-loss cap when EITHER this snapshot MIN OR that
    /// persisted running realized total reaches the cap, so the two are
    /// complementary arms of one gate rather than this query needing to catch all.
    ///
    /// `MIN` over the day returns SQL `NULL` when no row matches → `Ok(0)`,
    /// exactly like a fresh day.
    pub fn day_pnl_micro(&self, strategy: &str, utc_day: i64) -> Result<i128, StoreError> {
        let lo = utc_day * DAY_MS;
        let hi = lo + DAY_MS;
        // Aggregate (no GROUP BY) → always exactly one row; the value is NULL when
        // no snapshot matches, read as `Option<i64>` (None → fresh day → 0).
        let worst: Option<i64> = self.conn.query_row(
            "SELECT MIN(realized_micro + unrealized_micro) FROM pnl_snapshots
             WHERE strategy = ?1 AND ts_ms >= ?2 AND ts_ms < ?3",
            rusqlite::params![strategy, lo, hi],
            |r| r.get(0),
        )?;
        Ok(worst.map_or(0, i128::from))
    }

    /// The CUMULATIVE day-realized ledger total for `strategy` on `utc_day`, in
    /// µUSDC (i128); `0` if no ledger row exists (a fresh day/strategy). Written
    /// by [`crate::Store::add_day_realized`] as a running sum of per-fill realized
    /// deltas.
    ///
    /// Backs the SECOND arm of the market-maker's persistent UTC-day loss cap
    /// (I3): unlike [`day_pnl_micro`](Self::day_pnl_micro) — whose snapshot MIN
    /// only catches a SINGLE session's cap breach or a held/unrealized drawdown —
    /// this persists and accrues across the periodic auto-restart, so MANY sub-cap
    /// *realized* sessions whose losses SUM over the cap across a day ARE caught.
    /// The MM latches when EITHER query reaches the cap; both scope to the UTC
    /// day, so a day rollover resets both to 0 and releases the gate.
    /// All persisted OPEN copy positions, for the copy strategy to RELOAD on
    /// startup and resume managing them (follow-exit / stop-loss / redeem) instead
    /// of orphaning them. Ordered by `(condition_id, outcome_index)` for a stable
    /// reload. Empty on a fresh DB.
    pub fn copy_open_positions(&self) -> Result<Vec<CopyPositionRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT condition_id, outcome_index, asset, neg_risk, tick_decimals,
                    condition_hex, trader, entry_ts, qty_micro, cost_micro
             FROM copy_positions ORDER BY condition_id, outcome_index",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(CopyPositionRow {
                    condition_id: r.get(0)?,
                    outcome_index: r.get(1)?,
                    asset: r.get(2)?,
                    neg_risk: r.get::<_, i64>(3)? != 0,
                    tick_decimals: r.get(4)?,
                    condition_hex: r.get(5)?,
                    trader: r.get(6)?,
                    entry_ts: r.get(7)?,
                    qty_micro: r.get(8)?,
                    cost_micro: r.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Most-recent `limit` BTC-5m shadow observations, newest first.
    pub fn btc5m_shadow_rows(&self, limit: usize) -> Result<Vec<crate::Btc5mShadowRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, condition_id, secs_to_go, strike, spot, sigma_tau, p_up,
                    best_bid_micro, best_ask_micro, tick_decimals
             FROM btc5m_shadow ORDER BY ts_ms DESC LIMIT ?1")?;
        let rows = stmt.query_map([limit as i64], |r| Ok(crate::Btc5mShadowRow {
            ts_ms: r.get(0)?, condition_id: r.get(1)?, secs_to_go: r.get(2)?,
            strike: r.get(3)?, spot: r.get(4)?, sigma_tau: r.get(5)?, p_up: r.get(6)?,
            best_bid_micro: r.get(7)?, best_ask_micro: r.get(8)?, tick_decimals: r.get(9)?,
        }))?.collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// All persisted OPEN BTC-5m micro-taker positions, for the Phase-2
    /// strategy to RELOAD on startup and resume managing them (settle sweep at
    /// window close) instead of orphaning them. Ordered by `(condition_id,
    /// outcome_index)` for a stable reload. Empty on a fresh DB. Mirrors
    /// [`Self::copy_open_positions`].
    pub fn btc5m_open_positions(&self) -> Result<Vec<crate::Btc5mPositionRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT condition_id, outcome_index, token, qty_micro, cost_micro, entry_ts, t_close_ms, strike
             FROM btc5m_positions ORDER BY condition_id, outcome_index",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(crate::Btc5mPositionRow {
                    condition_id: r.get(0)?,
                    outcome_index: r.get(1)?,
                    token: r.get(2)?,
                    qty_micro: r.get(3)?,
                    cost_micro: r.get(4)?,
                    entry_ts: r.get(5)?,
                    t_close_ms: r.get(6)?,
                    strike: r.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn day_realized_micro(&self, strategy: &str, utc_day: i64) -> Result<i128, StoreError> {
        let total: Option<i64> = self
            .conn
            .query_row(
                "SELECT realized_micro FROM day_realized WHERE strategy = ?1 AND utc_day = ?2",
                rusqlite::params![strategy, utc_day],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })?;
        Ok(total.map_or(0, i128::from))
    }

    /// Most-recent `n` halts, newest first.
    pub fn recent_halts(&self, n: usize) -> Result<Vec<HaltView>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT ts_ms, reason FROM halts ORDER BY id DESC LIMIT ?1")?;
        let rows = stmt
            .query_map([n as i64], |row| {
                Ok(HaltView {
                    ts_ms: row.get(0)?,
                    reason: row.get(1)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Expose the underlying connection for write-rejection tests only.
    #[cfg(test)]
    pub(crate) fn conn_for_test(&self) -> &Connection {
        &self.conn
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{FillRow, HaltRow, OppRow, OrderEventRow, OrderRow, PnlRow, Store, utc_day_from_ms};

    /// Seed a real file-backed store (read-only conns can't see another conn's
    /// in-memory db), return (dir, path).
    fn seeded() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.sqlite");
        let mut s = Store::open(&path).unwrap();
        for i in 0..3i64 {
            s.insert_opportunity(&OppRow {
                ts_ms: 100 + i,
                class: "C1Long".into(),
                fingerprint: format!("{i:016x}"),
                edge_bps: 600 + i,
                units_micro: 100_000_000,
                net_micro: 5_000_000 + i,
                basis_micro: 94_000_000,
                legs_json: format!(
                    "[{{\"token\":{i},\"action\":\"Buy\",\"px\":44,\"qty\":100000000}}]"
                ),
                dispatched: i == 2,
                strategy: "arb".into(),
            })
            .unwrap();
        }
        s.insert_order(&OrderRow {
            id: "o1".into(),
            ts_ms: 110,
            fingerprint: "fp".into(),
            token: 7,
            action: "Buy".into(),
            limit_ticks: 44,
            tick_levels: 100,
            qty_micro: 100_000_000,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_order_event(&OrderEventRow {
            order_id: "o1".into(),
            ts_ms: 111,
            state: "Signed".into(),
            detail: String::new(),
        })
        .unwrap();
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 112,
            token: 7,
            action: "Buy".into(),
            px_ticks: 44,
            tick_levels: 100,
            qty_micro: 100_000_000,
            cash_micro: -44_000_000,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_pnl_snapshot(&PnlRow {
            ts_ms: 120,
            cash_micro: -44_000_000,
            realized_micro: 0,
            unrealized_micro: -1_000_000,
            equity_micro: -45_000_000,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_halt(&HaltRow {
            ts_ms: 130,
            reason: "KillSwitch".into(),
            detail: String::new(),
        })
        .unwrap();
        (dir, path)
    }

    #[test]
    fn read_store_serves_dashboard_views() {
        let (_dir, path) = seeded();
        let r = ReadStore::open(&path).unwrap();

        let opps = r.recent_opportunities(2).unwrap();
        assert_eq!(opps.len(), 2);
        // newest first
        assert_eq!(opps[0].ts_ms, 102);
        assert_eq!(opps[0].class, "C1Long");
        assert_eq!(opps[0].edge_bps, 602);
        assert!(opps[0].dispatched);
        assert_eq!(opps[1].ts_ms, 101);

        let fills = r.recent_fills(10).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].token, 7);
        assert_eq!(fills[0].cash_micro, -44_000_000);
        assert_eq!(fills[0].strategy, "arb", "recent_fills surfaces the strategy tag");

        let events = r.recent_order_events(10).unwrap();
        assert!(events.iter().any(|e| e.state == "Signed"));

        let pos = r.open_positions().unwrap();
        assert_eq!(pos, vec![(7, "arb".to_string(), 100_000_000, 44_000_000)]);

        let pnl = r.latest_pnl().unwrap().unwrap();
        assert_eq!(pnl.equity_micro, -45_000_000);

        let halts = r.recent_halts(5).unwrap();
        assert_eq!(halts[0].reason, "KillSwitch");
    }

    #[test]
    fn open_positions_groups_by_strategy_and_returns_signed_shorts() {
        // An arb long and an mm short on the SAME token surface as two distinct,
        // strategy-tagged rows. The mm row is a SHORT — negative net + negative
        // basis — proving `open_positions` returns shorts (the `<> 0` HAVING),
        // and the per-(token,strategy) grouping keeps the strategies independent.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pos.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.insert_order(&OrderRow {
            id: "arb1".into(),
            ts_ms: 1,
            fingerprint: "fp".into(),
            token: 7,
            action: "Buy".into(),
            limit_ticks: 44,
            tick_levels: 100,
            qty_micro: 100_000_000,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_fill(&FillRow {
            order_id: "arb1".into(),
            ts_ms: 2,
            token: 7,
            action: "Buy".into(),
            px_ticks: 44,
            tick_levels: 100,
            qty_micro: 100_000_000,
            cash_micro: -44_000_000,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_order(&OrderRow {
            id: "mm1".into(),
            ts_ms: 3,
            fingerprint: "fp".into(),
            token: 7,
            action: "Sell".into(),
            limit_ticks: 40,
            tick_levels: 100,
            qty_micro: 50_000_000,
            strategy: "mm".into(),
        })
        .unwrap();
        // mm Sell-to-open on the same token: scoped consume means it opens an
        // INDEPENDENT short (realized 0) rather than closing arb's long.
        let realized = s
            .insert_fill_signed(&FillRow {
                order_id: "mm1".into(),
                ts_ms: 4,
                token: 7,
                action: "Sell".into(),
                px_ticks: 40,
                tick_levels: 100,
                qty_micro: 50_000_000,
                cash_micro: 20_000_000,
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        assert_eq!(realized, 0, "mm short-open must NOT consume arb's long");
        drop(s);

        let r = ReadStore::open(&path).unwrap();
        let pos = r.open_positions().unwrap();
        assert_eq!(
            pos,
            vec![
                (7, "arb".to_string(), 100_000_000, 44_000_000),
                (7, "mm".to_string(), -50_000_000, -20_000_000),
            ],
            "two tagged rows on one token: arb long + mm short (signed)"
        );
    }

    #[test]
    fn read_store_is_truly_read_only() {
        let (_dir, path) = seeded();
        let r = ReadStore::open(&path).unwrap();
        // Any write through the read connection must fail at the sqlite level.
        assert!(
            r.conn_for_test()
                .execute(
                    "INSERT INTO halts (ts_ms, reason, detail) VALUES (1,'x','')",
                    []
                )
                .is_err()
        );
    }

    #[test]
    fn read_store_sees_writer_updates_live() {
        let (_dir, path) = seeded();
        let r = ReadStore::open(&path).unwrap();
        assert_eq!(r.recent_halts(5).unwrap().len(), 1);
        // a second writer connection appends; the reader sees it on next query
        let mut w = Store::open(&path).unwrap();
        w.insert_halt(&HaltRow {
            ts_ms: 131,
            reason: "DailyDrawdown".into(),
            detail: String::new(),
        })
        .unwrap();
        assert_eq!(r.recent_halts(5).unwrap().len(), 2);
    }

    #[test]
    fn missing_db_file_is_an_error_not_a_create() {
        let dir = tempfile::tempdir().unwrap();
        assert!(ReadStore::open(&dir.path().join("absent.sqlite")).is_err());
    }

    #[test]
    fn recent_orders_serves_current_state_newest_first() {
        let (_dir, path) = seeded();
        // a second, newer order with NO events: must appear first, state Draft
        let mut w = Store::open(&path).unwrap();
        w.insert_order(&OrderRow {
            id: "o2".into(),
            ts_ms: 999,
            fingerprint: "fp".into(),
            token: 8,
            action: "Sell".into(),
            limit_ticks: 50,
            tick_levels: 100,
            qty_micro: 1_000_000,
            strategy: "arb".into(),
        })
        .unwrap();
        let r = ReadStore::open(&path).unwrap();
        let orders = r.recent_orders(10).unwrap();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].order_id, "o2");
        assert_eq!(orders[0].state, "Draft"); // present immediately, before any events
        assert_eq!(orders[1].order_id, "o1");
        assert_eq!(orders[1].state, "Signed"); // current state, not the event history
    }

    #[test]
    fn day_pnl_is_worst_point_in_day() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.sqlite");
        let mut s = Store::open(&path).unwrap();
        // Worst point FIRST (realized −5M + unrealized −1M = −6M), then a LATER,
        // BETTER snapshot (−2M). day_pnl_micro must return the WORST point's
        // realized+unrealized (−6M) — proving (a) it sums realized+unrealized and
        // (b) it uses the day's MIN, not the latest snapshot (which would have
        // wrongly returned −2M).
        s.record_pnl_at(1_000, 0, -5_000_000, -1_000_000, 0, "mm").unwrap();
        s.record_pnl_at(2_000, 0, -2_000_000, 0, 0, "mm").unwrap();
        let rs = ReadStore::open(&path).unwrap();
        let day = utc_day_from_ms(2_000);
        assert_eq!(rs.day_pnl_micro("mm", day).unwrap(), -6_000_000);
    }

    #[test]
    fn day_pnl_stays_latched_after_flat_session() {
        // The cross-restart hazard MIN fixes: an early session breaches the cap
        // (worst −6M), then later HALTED+FLAT sessions write ~0 P&L snapshots
        // (realized resets each restart). MIN over the day keeps returning the
        // worst point, so the loss-cap gate stays latched across every restart —
        // a "latest snapshot" read would wrongly recover to ~0 and re-arm.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flat.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.record_pnl_at(1_000, 0, -6_000_000, 0, -6_000_000, "mm").unwrap();
        s.record_pnl_at(5_000, 0, 0, 0, 0, "mm").unwrap();
        s.record_pnl_at(9_000, 0, 0, 0, 0, "mm").unwrap();
        s.record_pnl_at(13_000, 0, 0, 0, 0, "mm").unwrap();
        let rs = ReadStore::open(&path).unwrap();
        let day = utc_day_from_ms(1_000);
        assert_eq!(rs.day_pnl_micro("mm", day).unwrap(), -6_000_000);
    }

    #[test]
    fn day_pnl_excludes_other_utc_days_and_is_zero_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("xday.sqlite");
        let mut s = Store::open(&path).unwrap();
        // Day 0 worst −6M; day 1 carries an even-worse −9M that must NOT bleed
        // into day 0's window (and vice-versa). 86_400_000 ms = one UTC day.
        s.record_pnl_at(1_000, 0, -6_000_000, 0, -6_000_000, "mm").unwrap();
        s.record_pnl_at(86_400_000 + 1_000, 0, -9_000_000, 0, -9_000_000, "mm").unwrap();
        let rs = ReadStore::open(&path).unwrap();
        assert_eq!(rs.day_pnl_micro("mm", 0).unwrap(), -6_000_000, "day 0 sees only day-0 snapshots");
        assert_eq!(rs.day_pnl_micro("mm", 1).unwrap(), -9_000_000, "day 1 sees only day-1 snapshots");
        // A day with no snapshots → NULL MIN → 0 (a fresh day never latches).
        assert_eq!(rs.day_pnl_micro("mm", 2).unwrap(), 0, "no snapshots that day → 0");
    }

    // ── I3: cumulative day-realized ledger (closes the summed-sub-cap gap) ──────

    #[test]
    fn day_realized_micro_accumulates_and_is_day_scoped() {
        // The cumulative day-realized LEDGER (I3): per (utc_day, strategy) each
        // `add_day_realized` ADDS its delta. Two −$4 adds on the SAME day sum to
        // −$8 — exactly the summed-sub-cap-realized case the snapshot-MIN gate
        // (`day_pnl_micro`) misses (no single row is past a −$6 cap, but the day
        // is). A different day is independent; an unseen day/strategy reads 0.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ledger.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.add_day_realized(0, "mm", -4_000_000).unwrap();
        s.add_day_realized(0, "mm", -4_000_000).unwrap();
        s.add_day_realized(1, "mm", -4_000_000).unwrap();
        drop(s);

        let rs = ReadStore::open(&path).unwrap();
        assert_eq!(
            rs.day_realized_micro("mm", 0).unwrap(),
            -8_000_000,
            "two same-day adds SUM (−4M + −4M)"
        );
        assert_eq!(
            rs.day_realized_micro("mm", 1).unwrap(),
            -4_000_000,
            "a different UTC day is an independent ledger row"
        );
        assert_eq!(
            rs.day_realized_micro("mm", 2).unwrap(),
            0,
            "an unseen day reads 0 (no row → fresh)"
        );
        assert_eq!(
            rs.day_realized_micro("arb", 0).unwrap(),
            0,
            "a different strategy is independent"
        );
    }
}
