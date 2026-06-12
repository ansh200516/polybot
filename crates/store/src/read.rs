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

use crate::{PnlRow, StoreError};

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
            "SELECT ts_ms, token, action, px_ticks, tick_levels, qty_micro, cash_micro
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

    /// Open positions from FIFO lots: `(token, remaining µshares, remaining cost µUSDC)`.
    /// Only tokens with a positive remaining quantity are returned, ordered by token id.
    pub fn open_positions(&self) -> Result<Vec<(i64, i64, i64)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT token, SUM(remaining_micro), SUM(cost_remaining_micro)
             FROM lots GROUP BY token HAVING SUM(remaining_micro) > 0 ORDER BY token",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The most-recent P&L snapshot, or `None` if the table is empty.
    pub fn latest_pnl(&self) -> Result<Option<PnlRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, cash_micro, realized_micro, unrealized_micro, equity_micro
             FROM pnl_snapshots ORDER BY id DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map([], |row| {
            Ok(PnlRow {
                ts_ms: row.get(0)?,
                cash_micro: row.get(1)?,
                realized_micro: row.get(2)?,
                unrealized_micro: row.get(3)?,
                equity_micro: row.get(4)?,
            })
        })?;
        rows.next().transpose().map_err(StoreError::from)
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
    use crate::{FillRow, HaltRow, OppRow, OrderEventRow, OrderRow, PnlRow, Store};

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
        })
        .unwrap();
        s.insert_pnl_snapshot(&PnlRow {
            ts_ms: 120,
            cash_micro: -44_000_000,
            realized_micro: 0,
            unrealized_micro: -1_000_000,
            equity_micro: -45_000_000,
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

        let events = r.recent_order_events(10).unwrap();
        assert!(events.iter().any(|e| e.state == "Signed"));

        let pos = r.open_positions().unwrap();
        assert_eq!(pos, vec![(7, 100_000_000, 44_000_000)]);

        let pnl = r.latest_pnl().unwrap().unwrap();
        assert_eq!(pnl.equity_micro, -45_000_000);

        let halts = r.recent_halts(5).unwrap();
        assert_eq!(halts[0].reason, "KillSwitch");
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
}
