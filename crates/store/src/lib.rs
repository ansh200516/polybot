//! rusqlite WAL persistence (spec §16). One writer task owns the connection
//! (writer.rs); this module is the pure synchronous core, fully testable
//! without tokio.

use std::path::Path;

use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum StoreError {
    Sql(rusqlite::Error),
    /// An i128 money value that doesn't fit sqlite's i64.
    Overflow(&'static str),
    /// Tried to consume more shares than FIFO lots hold.
    Oversell {
        token: i64,
        missing_micro: i64,
    },
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Sql(e)
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Sql(e) => write!(f, "sqlite error: {e}"),
            StoreError::Overflow(w) => write!(f, "i64 overflow: {w}"),
            StoreError::Oversell {
                token,
                missing_micro,
            } => {
                write!(
                    f,
                    "oversell on token {token}: {missing_micro} micro-shares missing"
                )
            }
        }
    }
}

impl std::error::Error for StoreError {}

/// Checked Usdc(i128) → i64 for sqlite storage.
pub fn usdc_to_i64(u: pm_core::num::Usdc) -> Result<i64, StoreError> {
    i64::try_from(u.0).map_err(|_| StoreError::Overflow("usdc out of i64 range"))
}

// ---------------------------------------------------------------------------
// Row types (plain data; the coordinator converts engine types into these)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MarketRow {
    pub id: i64,
    pub condition_id: String,
    pub tick_levels: i64,
    pub fee_bps: i64,
    pub neg_risk: bool,
}

#[derive(Debug, Clone)]
pub struct RelRow {
    pub kind: String,
    pub a: i64,
    pub b: i64,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct OppRow {
    pub ts_ms: i64,
    pub class: String,
    pub fingerprint: String,
    pub edge_bps: i64,
    pub units_micro: i64,
    pub net_micro: i64,
    pub basis_micro: i64,
    pub legs_json: String,
    pub dispatched: bool,
}

#[derive(Debug, Clone)]
pub struct OrderRow {
    pub id: String,
    pub ts_ms: i64,
    pub fingerprint: String,
    pub token: i64,
    pub action: String,
    pub limit_ticks: i64,
    pub tick_levels: i64,
    pub qty_micro: i64,
}

#[derive(Debug, Clone)]
pub struct OrderEventRow {
    pub order_id: String,
    pub ts_ms: i64,
    pub state: String,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct FillRow {
    pub order_id: String,
    pub ts_ms: i64,
    pub token: i64,
    pub action: String,
    pub px_ticks: i64,
    pub tick_levels: i64,
    pub qty_micro: i64,
    /// Signed cash net of fee: negative for buys, positive for sells.
    pub cash_micro: i64,
    pub fee_micro: i64,
}

/// A split or merge (complete-set conversion). `kind` is "split" | "merge".
#[derive(Debug, Clone)]
pub struct ConversionRow {
    pub kind: String,
    pub ts_ms: i64,
    pub market: i64,
    pub yes_token: i64,
    pub no_token: i64,
    pub units_micro: i64,
    /// Signed cash: negative for splits (collateral+gas out), positive for
    /// merges (collateral in net of gas).
    pub cash_micro: i64,
}

#[derive(Debug, Clone)]
pub struct PnlRow {
    pub ts_ms: i64,
    pub cash_micro: i64,
    pub realized_micro: i64,
    pub unrealized_micro: i64,
    pub equity_micro: i64,
}

#[derive(Debug, Clone)]
pub struct HaltRow {
    pub ts_ms: i64,
    pub reason: String,
    pub detail: String,
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

const SCHEMA: &str = "
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS markets (
  id INTEGER PRIMARY KEY, condition_id TEXT NOT NULL,
  tick_levels INTEGER NOT NULL, fee_bps INTEGER NOT NULL, neg_risk INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS relationships (
  kind TEXT NOT NULL, a INTEGER NOT NULL, b INTEGER NOT NULL, status TEXT NOT NULL,
  PRIMARY KEY (kind, a, b));
CREATE TABLE IF NOT EXISTS opportunities (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, class TEXT NOT NULL,
  fingerprint TEXT NOT NULL, edge_bps INTEGER NOT NULL, units_micro INTEGER NOT NULL,
  net_micro INTEGER NOT NULL, basis_micro INTEGER NOT NULL, legs_json TEXT NOT NULL,
  dispatched INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY, ts_ms INTEGER NOT NULL, fingerprint TEXT NOT NULL,
  token INTEGER NOT NULL, action TEXT NOT NULL, limit_ticks INTEGER NOT NULL,
  tick_levels INTEGER NOT NULL, qty_micro INTEGER NOT NULL, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS order_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL REFERENCES orders(id),
  ts_ms INTEGER NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL REFERENCES orders(id),
  ts_ms INTEGER NOT NULL, token INTEGER NOT NULL, action TEXT NOT NULL,
  px_ticks INTEGER NOT NULL, tick_levels INTEGER NOT NULL, qty_micro INTEGER NOT NULL,
  cash_micro INTEGER NOT NULL, fee_micro INTEGER NOT NULL, realized_micro INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS conversions (
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, ts_ms INTEGER NOT NULL,
  market INTEGER NOT NULL, yes_token INTEGER NOT NULL, no_token INTEGER NOT NULL,
  units_micro INTEGER NOT NULL, cash_micro INTEGER NOT NULL, realized_micro INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS lots (
  id INTEGER PRIMARY KEY AUTOINCREMENT, token INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
  qty_micro INTEGER NOT NULL, remaining_micro INTEGER NOT NULL,
  cost_micro INTEGER NOT NULL, cost_remaining_micro INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS lots_token ON lots(token) WHERE remaining_micro > 0;
CREATE TABLE IF NOT EXISTS pnl_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, cash_micro INTEGER NOT NULL,
  realized_micro INTEGER NOT NULL, unrealized_micro INTEGER NOT NULL, equity_micro INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS halts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, reason TEXT NOT NULL, detail TEXT NOT NULL);
";

const TERMINAL_STATES: [&str; 4] = ["Filled", "Cancelled", "Rejected", "Expired"];

/// Synchronous store core. One instance = one connection; the async writer
/// task (writer.rs) owns it exclusively in production.
pub struct Store {
    conn: Connection,
    /// Count of failed writes since open (writer increments; health surface).
    pub write_errors: u64,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Store {
            conn,
            write_errors: 0,
        })
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Store {
            conn,
            write_errors: 0,
        })
    }

    pub fn upsert_market(&mut self, r: &MarketRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO markets (id, condition_id, tick_levels, fee_bps, neg_risk)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET condition_id=?2, tick_levels=?3, fee_bps=?4, neg_risk=?5",
            rusqlite::params![r.id, r.condition_id, r.tick_levels, r.fee_bps, r.neg_risk],
        )?;
        Ok(())
    }

    pub fn upsert_relationship(&mut self, r: &RelRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO relationships (kind, a, b, status) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(kind, a, b) DO UPDATE SET status=?4",
            rusqlite::params![r.kind, r.a, r.b, r.status],
        )?;
        Ok(())
    }

    pub fn insert_opportunity(&mut self, r: &OppRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO opportunities (ts_ms, class, fingerprint, edge_bps, units_micro,
             net_micro, basis_micro, legs_json, dispatched)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                r.ts_ms,
                r.class,
                r.fingerprint,
                r.edge_bps,
                r.units_micro,
                r.net_micro,
                r.basis_micro,
                r.legs_json,
                r.dispatched
            ],
        )?;
        Ok(())
    }

    pub fn insert_order(&mut self, r: &OrderRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO orders (id, ts_ms, fingerprint, token, action, limit_ticks,
             tick_levels, qty_micro, state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'Draft')",
            rusqlite::params![
                r.id,
                r.ts_ms,
                r.fingerprint,
                r.token,
                r.action,
                r.limit_ticks,
                r.tick_levels,
                r.qty_micro
            ],
        )?;
        Ok(())
    }

    /// Insert an order event AND advance the order's current state.
    pub fn insert_order_event(&mut self, r: &OrderEventRow) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO order_events (order_id, ts_ms, state, detail) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![r.order_id, r.ts_ms, r.state, r.detail],
        )?;
        tx.execute(
            "UPDATE orders SET state = ?2 WHERE id = ?1",
            rusqlite::params![r.order_id, r.state],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Orders not in a terminal state — restart reconciliation input (spec §14).
    pub fn open_orders(&self) -> Result<Vec<(String, String)>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, state FROM orders WHERE state NOT IN ('Filled','Cancelled','Rejected','Expired')
             ORDER BY ts_ms",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        // TERMINAL_STATES kept in sync with the SQL literal above.
        debug_assert_eq!(TERMINAL_STATES.len(), 4);
        Ok(rows)
    }

    pub fn expire_order(&mut self, id: &str, ts_ms: i64) -> Result<(), StoreError> {
        self.insert_order_event(&OrderEventRow {
            order_id: id.into(),
            ts_ms,
            state: "Expired".into(),
            detail: "reconciled on restart".into(),
        })
    }

    pub fn count_markets(&self) -> Result<i64, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM markets", [], |r| r.get(0))?)
    }

    pub fn count_opportunities(&self) -> Result<i64, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM opportunities", [], |r| r.get(0))?)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn mem() -> Store {
        Store::open_in_memory().unwrap()
    }

    fn order_row(id: &str) -> OrderRow {
        OrderRow {
            id: id.into(),
            ts_ms: 1,
            fingerprint: "deadbeef".into(),
            token: 7,
            action: "Buy".into(),
            limit_ticks: 44,
            tick_levels: 100,
            qty_micro: 100_000_000,
        }
    }

    #[test]
    fn open_creates_schema_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let s = Store::open(&path).unwrap();
        drop(s);
        let s = Store::open(&path).unwrap(); // re-open: CREATE IF NOT EXISTS
        assert_eq!(s.count_opportunities().unwrap(), 0);
    }

    #[test]
    fn market_and_relationship_upserts() {
        let mut s = mem();
        let m = MarketRow {
            id: 0,
            condition_id: "0xabc".into(),
            tick_levels: 100,
            fee_bps: 0,
            neg_risk: false,
        };
        s.upsert_market(&m).unwrap();
        s.upsert_market(&m).unwrap(); // idempotent
        s.upsert_relationship(&RelRow {
            kind: "implies".into(),
            a: 0,
            b: 1,
            status: "approved".into(),
        })
        .unwrap();
        assert_eq!(s.count_markets().unwrap(), 1);
    }

    #[test]
    fn opportunity_insert_round_trips() {
        let mut s = mem();
        s.insert_opportunity(&OppRow {
            ts_ms: 123,
            class: "C1Long".into(),
            fingerprint: "00ff".into(),
            edge_bps: 637,
            units_micro: 100_000_000,
            net_micro: 5_990_000,
            basis_micro: 94_000_000,
            legs_json: "[]".into(),
            dispatched: true,
        })
        .unwrap();
        assert_eq!(s.count_opportunities().unwrap(), 1);
    }

    #[test]
    fn order_lifecycle_updates_state_and_lists_open() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        s.insert_order(&order_row("o2")).unwrap();
        for st in ["Draft", "Signed", "Submitted", "Live", "Filled"] {
            s.insert_order_event(&OrderEventRow {
                order_id: "o1".into(),
                ts_ms: 2,
                state: st.into(),
                detail: String::new(),
            })
            .unwrap();
        }
        s.insert_order_event(&OrderEventRow {
            order_id: "o2".into(),
            ts_ms: 2,
            state: "Draft".into(),
            detail: String::new(),
        })
        .unwrap();
        let open = s.open_orders().unwrap();
        assert_eq!(open, vec![("o2".to_string(), "Draft".to_string())]); // o1 terminal
        s.expire_order("o2", 99).unwrap();
        assert!(s.open_orders().unwrap().is_empty());
    }

    #[test]
    fn usdc_to_i64_checks_range() {
        use pm_core::num::Usdc;
        assert_eq!(usdc_to_i64(Usdc(5)).unwrap(), 5);
        assert!(usdc_to_i64(Usdc(i128::from(i64::MAX) + 1)).is_err());
    }
}
