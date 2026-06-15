//! rusqlite WAL persistence (spec §16). One writer task owns the connection
//! (writer.rs); this module is the pure synchronous core, fully testable
//! without tokio.

use std::path::Path;

use rusqlite::Connection;

pub mod lots;
pub mod read;
pub mod writer;

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
    /// An enum-like string field contained an unrecognised value.
    /// `kind` names the field (e.g. `"action"`); `got` is the received value.
    BadVariant {
        kind: &'static str,
        got: String,
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
            StoreError::BadVariant { kind, got } => {
                write!(f, "unknown {kind} variant: {got}")
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
    /// Strategy that produced this row (default `"arb"`; legacy DBs back-fill).
    pub strategy: String,
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
    /// Strategy that produced this row (default `"arb"`; legacy DBs back-fill).
    pub strategy: String,
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
    /// Informational only — never used in P&L math (cash_micro is already net
    /// of fee).
    pub fee_micro: i64,
    /// Strategy that produced this row (default `"arb"`; legacy DBs back-fill).
    pub strategy: String,
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
    /// Strategy that produced this row (default `"arb"`; legacy DBs back-fill).
    pub strategy: String,
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
  dispatched INTEGER NOT NULL, strategy TEXT NOT NULL DEFAULT 'arb');
CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY, ts_ms INTEGER NOT NULL, fingerprint TEXT NOT NULL,
  token INTEGER NOT NULL, action TEXT NOT NULL, limit_ticks INTEGER NOT NULL,
  tick_levels INTEGER NOT NULL, qty_micro INTEGER NOT NULL, state TEXT NOT NULL,
  strategy TEXT NOT NULL DEFAULT 'arb');
CREATE TABLE IF NOT EXISTS order_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL REFERENCES orders(id),
  ts_ms INTEGER NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL REFERENCES orders(id),
  ts_ms INTEGER NOT NULL, token INTEGER NOT NULL, action TEXT NOT NULL,
  px_ticks INTEGER NOT NULL, tick_levels INTEGER NOT NULL, qty_micro INTEGER NOT NULL,
  cash_micro INTEGER NOT NULL, fee_micro INTEGER NOT NULL, realized_micro INTEGER NOT NULL,
  strategy TEXT NOT NULL DEFAULT 'arb');
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
  realized_micro INTEGER NOT NULL, unrealized_micro INTEGER NOT NULL, equity_micro INTEGER NOT NULL,
  strategy TEXT NOT NULL DEFAULT 'arb');
CREATE TABLE IF NOT EXISTS halts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, reason TEXT NOT NULL, detail TEXT NOT NULL);
";

const TERMINAL_STATES: [&str; 4] = ["Filled", "Cancelled", "Rejected", "Expired"];

/// Tables that carry a per-strategy tag. Pre-strategy databases (created before
/// the column existed) are upgraded in `open` by the idempotent migration below.
const STRATEGY_TABLES: [&str; 4] = ["opportunities", "orders", "fills", "pnl_snapshots"];

/// Whether `table` already has a column named `column` (via `PRAGMA table_info`).
/// Table names are crate-internal constants, so the formatted SQL is injection-safe.
fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool, StoreError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        // PRAGMA table_info columns: 0=cid, 1=name, 2=type, 3=notnull, 4=dflt, 5=pk.
        if row.get::<_, String>(1)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Add the `strategy` column to any `STRATEGY_TABLES` that predate it.
///
/// Idempotent and safe on both new and old databases: a freshly created DB
/// already has the column (from `SCHEMA`), so this is a no-op there; a legacy
/// DB gets `ALTER TABLE ... ADD COLUMN strategy TEXT NOT NULL DEFAULT 'arb'`,
/// which back-fills every existing row with `'arb'`.
fn migrate_strategy_columns(conn: &Connection) -> Result<(), StoreError> {
    for table in STRATEGY_TABLES {
        if !column_exists(conn, table, "strategy")? {
            conn.execute(
                &format!("ALTER TABLE {table} ADD COLUMN strategy TEXT NOT NULL DEFAULT 'arb'"),
                [],
            )?;
        }
    }
    Ok(())
}

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
        migrate_strategy_columns(&conn)?;
        Ok(Store {
            conn,
            write_errors: 0,
        })
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        migrate_strategy_columns(&conn)?;
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
             net_micro, basis_micro, legs_json, dispatched, strategy)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                r.ts_ms,
                r.class,
                r.fingerprint,
                r.edge_bps,
                r.units_micro,
                r.net_micro,
                r.basis_micro,
                r.legs_json,
                r.dispatched,
                r.strategy
            ],
        )?;
        Ok(())
    }

    pub fn insert_order(&mut self, r: &OrderRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO orders (id, ts_ms, fingerprint, token, action, limit_ticks,
             tick_levels, qty_micro, state, strategy)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'Draft', ?9)",
            rusqlite::params![
                r.id,
                r.ts_ms,
                r.fingerprint,
                r.token,
                r.action,
                r.limit_ticks,
                r.tick_levels,
                r.qty_micro,
                r.strategy
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

    /// Insert a fill. Buys create a lot (cost = −cash, which includes fee).
    /// Sells consume FIFO lots; returns the realized P&L delta in µUSDC
    /// (sell cash − consumed cost). Oversell rolls the whole fill back.
    pub fn insert_fill(&mut self, r: &FillRow) -> Result<i64, StoreError> {
        // Fail-closed: reject unrecognised actions BEFORE opening a transaction
        // so no lot mutation can occur on a typo'd string.
        match r.action.as_str() {
            "Buy" | "Sell" => {}
            other => {
                return Err(StoreError::BadVariant {
                    kind: "action",
                    got: other.to_string(),
                });
            }
        }
        let tx = self.conn.transaction()?;
        let realized: i64 = match r.action.as_str() {
            "Buy" => {
                lots::insert_lot(&tx, r.token, r.ts_ms, r.qty_micro, -r.cash_micro)?;
                0
            }
            _ => {
                // "Sell" — validated above
                let consumed = lots::consume_lots(&tx, r.token, r.qty_micro)?;
                r.cash_micro - consumed
            }
        };
        tx.execute(
            "INSERT INTO fills (order_id, ts_ms, token, action, px_ticks, tick_levels,
             qty_micro, cash_micro, fee_micro, realized_micro, strategy)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                r.order_id,
                r.ts_ms,
                r.token,
                r.action,
                r.px_ticks,
                r.tick_levels,
                r.qty_micro,
                r.cash_micro,
                r.fee_micro,
                realized,
                r.strategy
            ],
        )?;
        tx.commit()?;
        Ok(realized)
    }

    /// Apply a split (creates two lots, cost split ceil/remainder so the total
    /// is conserved and YES never under-counts) or a merge (consumes one lot
    /// quantity from each side; returns realized = proceeds − consumed costs).
    pub fn apply_conversion(&mut self, r: &ConversionRow) -> Result<i64, StoreError> {
        // Fail-closed: validate kind and cash sign BEFORE opening a transaction
        // so no lot mutation can occur on a typo'd string or wrong-sign cash.
        match r.kind.as_str() {
            "split" => {
                if r.cash_micro >= 0 {
                    return Err(StoreError::BadVariant {
                        kind: "conversion cash sign",
                        got: format!("split requires cash_micro < 0, got {}", r.cash_micro),
                    });
                }
            }
            "merge" => {
                if r.cash_micro <= 0 {
                    return Err(StoreError::BadVariant {
                        kind: "conversion cash sign",
                        got: format!("merge requires cash_micro > 0, got {}", r.cash_micro),
                    });
                }
            }
            other => {
                return Err(StoreError::BadVariant {
                    kind: "conversion kind",
                    got: other.to_string(),
                });
            }
        }
        let tx = self.conn.transaction()?;
        let realized: i64 = match r.kind.as_str() {
            "split" => {
                let total_cost = -r.cash_micro; // cash is negative for splits (validated above)
                let yes_cost = (total_cost + 1) / 2; // ceil(total/2)
                let no_cost = total_cost - yes_cost;
                lots::insert_lot(&tx, r.yes_token, r.ts_ms, r.units_micro, yes_cost)?;
                lots::insert_lot(&tx, r.no_token, r.ts_ms, r.units_micro, no_cost)?;
                0
            }
            _ => {
                // "merge" — validated above
                let cost_yes = lots::consume_lots(&tx, r.yes_token, r.units_micro)?;
                let cost_no = lots::consume_lots(&tx, r.no_token, r.units_micro)?;
                r.cash_micro - cost_yes - cost_no
            }
        };
        tx.execute(
            "INSERT INTO conversions (kind, ts_ms, market, yes_token, no_token, units_micro,
             cash_micro, realized_micro) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                r.kind,
                r.ts_ms,
                r.market,
                r.yes_token,
                r.no_token,
                r.units_micro,
                r.cash_micro,
                realized
            ],
        )?;
        tx.commit()?;
        Ok(realized)
    }

    /// Open position for a token from lots: (remaining µshares, remaining cost µUSDC).
    pub fn position(&self, token: i64) -> Result<(i64, i64), StoreError> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(SUM(remaining_micro),0), COALESCE(SUM(cost_remaining_micro),0)
             FROM lots WHERE token = ?1",
            [token],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    /// Total realized P&L: closing fills + merges.
    pub fn realized_total(&self) -> Result<i64, StoreError> {
        Ok(self.conn.query_row(
            "SELECT (SELECT COALESCE(SUM(realized_micro),0) FROM fills)
                  + (SELECT COALESCE(SUM(realized_micro),0) FROM conversions)",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn insert_pnl_snapshot(&mut self, r: &PnlRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO pnl_snapshots (ts_ms, cash_micro, realized_micro, unrealized_micro, equity_micro, strategy)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![r.ts_ms, r.cash_micro, r.realized_micro, r.unrealized_micro, r.equity_micro, r.strategy],
        )?;
        Ok(())
    }

    pub fn insert_halt(&mut self, r: &HaltRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO halts (ts_ms, reason, detail) VALUES (?1, ?2, ?3)",
            rusqlite::params![r.ts_ms, r.reason, r.detail],
        )?;
        Ok(())
    }

    pub fn count_fills(&self) -> Result<i64, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM fills", [], |r| r.get(0))?)
    }

    pub fn count_halts(&self) -> Result<i64, StoreError> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM halts", [], |r| r.get(0))?)
    }

    /// Record a session start at `now_ms`, prune entries older than `window_ms`,
    /// and return how many session starts (including this one) fall inside the
    /// window — the restart-storm input (spec §15).
    ///
    /// Clock skew / backwards clock: if `now_ms` is earlier than a stored
    /// entry, `now_ms - t` is negative, which is always `< window_ms`, so the
    /// entry is retained. This fails safe: a corrupt or rewound clock inflates
    /// the count toward tripping the storm detector rather than suppressing it.
    /// Corrupt CSV entries (non-numeric tokens) are silently dropped since the
    /// meta table is bot-owned and such entries are never written by this code.
    pub fn record_session_start(
        &mut self,
        now_ms: i64,
        window_ms: i64,
    ) -> Result<usize, StoreError> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'session_starts'",
                [],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })?;
        let mut starts: Vec<i64> = raw
            .map(|s| s.split(',').filter_map(|x| x.parse().ok()).collect())
            .unwrap_or_default();
        starts.push(now_ms);
        starts.retain(|&t| now_ms - t < window_ms);
        let joined = starts
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES ('session_starts', ?1)
             ON CONFLICT(key) DO UPDATE SET value = ?1",
            [joined],
        )?;
        Ok(starts.len())
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
            strategy: "arb".into(),
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
            strategy: "arb".into(),
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

    #[test]
    fn buy_fill_creates_lot_and_position() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        let realized = s
            .insert_fill(&FillRow {
                order_id: "o1".into(),
                ts_ms: 1,
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
        assert_eq!(realized, 0);
        assert_eq!(s.position(7).unwrap(), (100_000_000, 44_000_000));
    }

    #[test]
    fn sell_fill_consumes_fifo_and_realizes() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        s.insert_order(&order_row("o2")).unwrap();
        // Two buy lots: 100 sh @ .44, 100 sh @ .46
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
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
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 2,
            token: 7,
            action: "Buy".into(),
            px_ticks: 46,
            tick_levels: 100,
            qty_micro: 100_000_000,
            cash_micro: -46_000_000,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        // Sell 150 sh @ .50 → proceeds 75. FIFO: 100@.44 (44) + 50@.46 (23) → realized 75−67 = 8
        let realized = s
            .insert_fill(&FillRow {
                order_id: "o2".into(),
                ts_ms: 3,
                token: 7,
                action: "Sell".into(),
                px_ticks: 50,
                tick_levels: 100,
                qty_micro: 150_000_000,
                cash_micro: 75_000_000,
                fee_micro: 0,
                strategy: "arb".into(),
            })
            .unwrap();
        assert_eq!(realized, 8_000_000);
        assert_eq!(s.position(7).unwrap(), (50_000_000, 23_000_000));
        assert_eq!(s.realized_total().unwrap(), 8_000_000);
    }

    #[test]
    fn partial_lot_consumption_rounds_cost_up() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        // 3 µshares costing 10 µUSDC total; sell 1 µshare for 4 µUSDC.
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 9,
            action: "Buy".into(),
            px_ticks: 1,
            tick_levels: 100,
            qty_micro: 3,
            cash_micro: -10,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        // consumed = ceil(10·1/3) = 4 → realized = 4 − 4 = 0 (NOT 4 − 3 = 1)
        let realized = s
            .insert_fill(&FillRow {
                order_id: "o1".into(),
                ts_ms: 2,
                token: 9,
                action: "Sell".into(),
                px_ticks: 4,
                tick_levels: 100,
                qty_micro: 1,
                cash_micro: 4,
                fee_micro: 0,
                strategy: "arb".into(),
            })
            .unwrap();
        assert_eq!(realized, 0);
        assert_eq!(s.position(9).unwrap(), (2, 6)); // cost conserved: 10 − 4
    }

    #[test]
    fn oversell_is_an_error_and_rolls_back() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        let err = s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 7,
            action: "Sell".into(),
            px_ticks: 50,
            tick_levels: 100,
            qty_micro: 1_000_000,
            cash_micro: 500_000,
            fee_micro: 0,
            strategy: "arb".into(),
        });
        assert!(matches!(err, Err(StoreError::Oversell { token: 7, .. })));
        // The fill row must NOT exist (tx rollback).
        assert_eq!(s.count_fills().unwrap(), 0);
    }

    #[test]
    fn split_creates_two_lots_with_against_us_cost_allocation() {
        let mut s = mem();
        // Split 100 units, cost 100_010_000 (collateral 100 + gas .01): yes ceil(half)=50_005_000, no remainder
        s.apply_conversion(&ConversionRow {
            kind: "split".into(),
            ts_ms: 1,
            market: 0,
            yes_token: 7,
            no_token: 8,
            units_micro: 100_000_000,
            cash_micro: -100_010_000,
        })
        .unwrap();
        assert_eq!(s.position(7).unwrap(), (100_000_000, 50_005_000));
        assert_eq!(s.position(8).unwrap(), (100_000_000, 50_005_000));
    }

    #[test]
    fn merge_consumes_both_lots_and_realizes() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        s.insert_order(&order_row("o2")).unwrap();
        // Buy 100 YES @ .44 and 100 NO @ .50 (complete set for 94), merge for 99.99 (gas .01)
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
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
        s.insert_fill(&FillRow {
            order_id: "o2".into(),
            ts_ms: 1,
            token: 8,
            action: "Buy".into(),
            px_ticks: 50,
            tick_levels: 100,
            qty_micro: 100_000_000,
            cash_micro: -50_000_000,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        let realized = s
            .apply_conversion(&ConversionRow {
                kind: "merge".into(),
                ts_ms: 2,
                market: 0,
                yes_token: 7,
                no_token: 8,
                units_micro: 100_000_000,
                cash_micro: 99_990_000,
            })
            .unwrap();
        assert_eq!(realized, 5_990_000);
        assert_eq!(s.position(7).unwrap(), (0, 0));
        assert_eq!(s.position(8).unwrap(), (0, 0));
        assert_eq!(s.realized_total().unwrap(), 5_990_000);
    }

    #[test]
    fn repeated_partial_sells_conserve_lot_cost_exactly() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        // 3 µshares costing 10 µUSDC; sell 1 µshare three times at 4 µUSDC each.
        // Consumed cost per sell: ceil(10·1/3)=4, ceil(6·1/2)=3, exact=3 → total 10.
        // Total realized: (4−4) + (4−3) + (4−3) = 0 + 1 + 1 = 2.
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 9,
            action: "Buy".into(),
            px_ticks: 1,
            tick_levels: 100,
            qty_micro: 3,
            cash_micro: -10,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        let mut total_realized = 0;
        for i in 0..3 {
            total_realized += s
                .insert_fill(&FillRow {
                    order_id: "o1".into(),
                    ts_ms: 2 + i,
                    token: 9,
                    action: "Sell".into(),
                    px_ticks: 4,
                    tick_levels: 100,
                    qty_micro: 1,
                    cash_micro: 4,
                    fee_micro: 0,
                    strategy: "arb".into(),
                })
                .unwrap();
        }
        assert_eq!(total_realized, 2);
        assert_eq!(s.position(9).unwrap(), (0, 0));
        assert_eq!(s.realized_total().unwrap(), 2);
    }

    #[test]
    fn sell_spanning_partial_lots_conserves() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        // Lot 1: 7 µshares costing 13; Lot 2: 5 µshares costing 11.
        // Sell 9: full lot 1 (consumed=13) + 2/5 of lot 2 (ceil(11·2/5)=5) → consumed=18.
        // realized = 20 − 18 = 2; lot 2 remaining: 3 µshares, 6 µUSDC cost.
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 9,
            action: "Buy".into(),
            px_ticks: 1,
            tick_levels: 100,
            qty_micro: 7,
            cash_micro: -13,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 2,
            token: 9,
            action: "Buy".into(),
            px_ticks: 1,
            tick_levels: 100,
            qty_micro: 5,
            cash_micro: -11,
            fee_micro: 0,
            strategy: "arb".into(),
        })
        .unwrap();
        let realized = s
            .insert_fill(&FillRow {
                order_id: "o1".into(),
                ts_ms: 3,
                token: 9,
                action: "Sell".into(),
                px_ticks: 2,
                tick_levels: 100,
                qty_micro: 9,
                cash_micro: 20,
                fee_micro: 0,
                strategy: "arb".into(),
            })
            .unwrap();
        assert_eq!(realized, 2);
        assert_eq!(s.position(9).unwrap(), (3, 6));
    }

    #[test]
    fn unknown_action_and_bad_conversion_signs_are_rejected() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();

        // lowercase "sell" is not a valid action
        let r = s.insert_fill(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 7,
            action: "sell".into(),
            px_ticks: 50,
            tick_levels: 100,
            qty_micro: 1,
            cash_micro: 1,
            fee_micro: 0,
            strategy: "arb".into(),
        });
        assert!(matches!(r, Err(StoreError::BadVariant { .. })));
        assert_eq!(s.count_fills().unwrap(), 0); // no lot mutation

        // capitalised "Split" is not a valid kind
        let r = s.apply_conversion(&ConversionRow {
            kind: "Split".into(),
            ts_ms: 1,
            market: 0,
            yes_token: 7,
            no_token: 8,
            units_micro: 1,
            cash_micro: -1,
        });
        assert!(matches!(r, Err(StoreError::BadVariant { .. })));

        // split with non-negative cash is wrong sign
        let r = s.apply_conversion(&ConversionRow {
            kind: "split".into(),
            ts_ms: 1,
            market: 0,
            yes_token: 7,
            no_token: 8,
            units_micro: 1,
            cash_micro: 5,
        });
        assert!(r.is_err());

        // merge with non-positive cash is wrong sign
        let r = s.apply_conversion(&ConversionRow {
            kind: "merge".into(),
            ts_ms: 1,
            market: 0,
            yes_token: 7,
            no_token: 8,
            units_micro: 1,
            cash_micro: -5,
        });
        assert!(r.is_err());

        assert_eq!(s.position(7).unwrap(), (0, 0)); // no lots leaked
    }

    #[test]
    fn pnl_snapshot_halt_and_session_history() {
        let mut s = mem();
        s.insert_pnl_snapshot(&PnlRow {
            ts_ms: 1,
            cash_micro: -10,
            realized_micro: 0,
            unrealized_micro: 5,
            equity_micro: -5,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_halt(&HaltRow {
            ts_ms: 2,
            reason: "DailyDrawdown".into(),
            detail: "dd".into(),
        })
        .unwrap();
        assert_eq!(s.count_halts().unwrap(), 1);
        // session-start history: record at t=1000, 2000, 3000 with window 1500 → last call sees 2 in window
        assert_eq!(s.record_session_start(1000, 1500).unwrap(), 1);
        assert_eq!(s.record_session_start(2000, 1500).unwrap(), 2);
        assert_eq!(s.record_session_start(3000, 1500).unwrap(), 2); // 1000 pruned
    }

    #[test]
    fn pnl_snapshot_is_tagged_by_strategy_and_filterable() {
        // File-backed: the strategy-filtered reader lives on ReadStore, which
        // needs a real file (a read-only conn can't see another conn's :memory:).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strat.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.insert_pnl_snapshot(&PnlRow {
            ts_ms: 1,
            cash_micro: -10,
            realized_micro: 0,
            unrealized_micro: 5,
            equity_micro: -5,
            strategy: "arb".into(),
        })
        .unwrap();
        s.insert_pnl_snapshot(&PnlRow {
            ts_ms: 2,
            cash_micro: -20,
            realized_micro: 1,
            unrealized_micro: 6,
            equity_micro: -14,
            strategy: "mm".into(),
        })
        .unwrap();
        drop(s);

        let r = crate::read::ReadStore::open(&path).unwrap();
        let mm = r.recent_pnl_by_strategy("mm", 10).unwrap();
        assert_eq!(mm.len(), 1, "only the mm-tagged snapshot matches");
        assert_eq!(mm[0].strategy, "mm");
        assert_eq!(mm[0].equity_micro, -14);
        let arb = r.recent_pnl_by_strategy("arb", 10).unwrap();
        assert_eq!(arb.len(), 1);
        assert_eq!(arb[0].strategy, "arb");
    }

    #[test]
    fn legacy_db_without_strategy_column_opens_and_backfills_arb() {
        // Simulate a pre-strategy database: pnl_snapshots created WITHOUT the
        // strategy column, with one row inserted the old way.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.sqlite");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE pnl_snapshots (
                   id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
                   cash_micro INTEGER NOT NULL, realized_micro INTEGER NOT NULL,
                   unrealized_micro INTEGER NOT NULL, equity_micro INTEGER NOT NULL);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO pnl_snapshots
                   (ts_ms, cash_micro, realized_micro, unrealized_micro, equity_micro)
                 VALUES (7, 1, 2, 3, 4)",
                [],
            )
            .unwrap();
        }

        // Opening a legacy DB must succeed: the idempotent migration adds the
        // missing column rather than failing on the absent field.
        let s = Store::open(&path).unwrap();
        drop(s);

        // The pre-existing row is back-filled with the 'arb' default.
        let r = crate::read::ReadStore::open(&path).unwrap();
        let arb = r.recent_pnl_by_strategy("arb", 10).unwrap();
        assert_eq!(arb.len(), 1);
        assert_eq!(arb[0].ts_ms, 7);
        assert_eq!(arb[0].equity_micro, 4);
        assert_eq!(arb[0].strategy, "arb");
    }
}
