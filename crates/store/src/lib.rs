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

/// Milliseconds in a UTC day (24h), the divisor for [`utc_day_from_ms`].
const DAY_MS: i64 = 86_400_000;

/// UTC-day index for a millisecond timestamp: `ts_ms / 86_400_000` (whole days
/// since the Unix epoch). A pure, allocation-free helper shared by the store's
/// per-day P&L query ([`read::ReadStore::day_pnl_micro`]) and the market-maker's
/// persistent UTC-day loss cap, so both agree on exactly which snapshots fall in
/// "today". Floors toward −∞ for the realistic (non-negative) timestamp range.
pub fn utc_day_from_ms(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(DAY_MS)
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

/// One OPEN copy-strategy position, persisted so a RESTART resumes managing it
/// (follow-exit / stop-loss / redeem) instead of orphaning it — mirrors the MM's
/// inventory reload. Keyed by `(condition_id, outcome_index)`. `asset` is the
/// venue token id (re-registered on the venue at reload so exits can trade it),
/// `tick_decimals` encodes the [`pm_core::num::TickSize`] (2 = cent, 3 = milli),
/// `condition_hex` is the on-chain redeem key, and `trader` is the source wallet
/// a later SELL follow-exits against.
#[derive(Debug, Clone, PartialEq)]
pub struct CopyPositionRow {
    pub condition_id: String,
    pub outcome_index: i64,
    pub asset: String,
    pub neg_risk: bool,
    pub tick_decimals: i64,
    pub condition_hex: String,
    pub trader: String,
    pub entry_ts: i64,
    pub qty_micro: i64,
    pub cost_micro: i64,
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

/// A reward-farm DECISION row (Task 10, spec §12): the per-cycle `(state,
/// action)` the RewardFarm policy logged for one quoting unit. `market` is an
/// opaque TEXT key the writer correlates outcomes against (the MM keys it by
/// token id — the Spec-1 single-token quoting unit). `state_json` / `action_json`
/// are caller-built JSON blobs (state features + the chosen quote / skip); the
/// store treats them as opaque text. Append-only; no Spec-1 consumer reads it.
#[derive(Debug, Clone)]
pub struct RfDecisionRow {
    pub ts_ms: i64,
    pub market: String,
    pub state_json: String,
    pub action_json: String,
}

/// A reward-farm OUTCOME row (Task 10, spec §12): the realized components of the
/// reward signal for a decision. `market` is the SAME key the decision used; the
/// writer resolves it to the most-recent `rf_decisions.id` for that market (the
/// fire-and-forget telemetry path never returns the autoincrement id to the MM).
/// All amounts are µUSDC; components not computed in Spec 1 are `0`.
#[derive(Debug, Clone)]
pub struct RfOutcomeRow {
    pub market: String,
    pub ts_ms: i64,
    pub reward_score_delta_micro: i64,
    pub rebate_micro: i64,
    pub adverse_pnl_micro: i64,
    pub inv_penalty_micro: i64,
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
  cost_micro INTEGER NOT NULL, cost_remaining_micro INTEGER NOT NULL,
  strategy TEXT NOT NULL DEFAULT 'arb');
CREATE INDEX IF NOT EXISTS lots_token ON lots(token) WHERE remaining_micro <> 0;
CREATE TABLE IF NOT EXISTS pnl_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, cash_micro INTEGER NOT NULL,
  realized_micro INTEGER NOT NULL, unrealized_micro INTEGER NOT NULL, equity_micro INTEGER NOT NULL,
  strategy TEXT NOT NULL DEFAULT 'arb');
CREATE TABLE IF NOT EXISTS halts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, reason TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rf_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
  market TEXT NOT NULL, state_json TEXT NOT NULL, action_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rf_outcomes (
  decision_id INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
  reward_score_delta_micro INTEGER NOT NULL, rebate_micro INTEGER NOT NULL,
  adverse_pnl_micro INTEGER NOT NULL, inv_penalty_micro INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS day_realized (
  utc_day INTEGER NOT NULL, strategy TEXT NOT NULL,
  realized_micro INTEGER NOT NULL, PRIMARY KEY (utc_day, strategy));
CREATE TABLE IF NOT EXISTS copy_positions (
  condition_id TEXT NOT NULL, outcome_index INTEGER NOT NULL,
  asset TEXT NOT NULL, neg_risk INTEGER NOT NULL, tick_decimals INTEGER NOT NULL,
  condition_hex TEXT NOT NULL, trader TEXT NOT NULL, entry_ts INTEGER NOT NULL,
  qty_micro INTEGER NOT NULL, cost_micro INTEGER NOT NULL,
  PRIMARY KEY (condition_id, outcome_index));";

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

/// Add the `strategy` column to the `lots` table if it predates per-strategy
/// lot accounting. Kept separate from [`migrate_strategy_columns`] because
/// `lots` is a cost-basis ledger (not one of the `STRATEGY_TABLES` event logs),
/// but the mechanism is identical: idempotent and safe on new and old DBs — a
/// fresh DB already has the column (from `SCHEMA`), so this is a no-op there; a
/// legacy DB gets `ALTER TABLE lots ADD COLUMN strategy TEXT NOT NULL DEFAULT
/// 'arb'`, which back-fills every existing lot to `'arb'`. That back-fill is the
/// arb invariant's foundation: on an arb-only DB every lot is `'arb'`, so the
/// strategy-scoped lot queries match exactly the rows the un-scoped queries did.
fn migrate_lots_strategy(conn: &Connection) -> Result<(), StoreError> {
    if !column_exists(conn, "lots", "strategy")? {
        conn.execute(
            "ALTER TABLE lots ADD COLUMN strategy TEXT NOT NULL DEFAULT 'arb'",
            [],
        )?;
    }
    Ok(())
}

/// Widen the partial `lots_token` index to index SHORT lots too.
///
/// The pre-signed-lots index was `... WHERE remaining_micro > 0`, which omits
/// short lots (`remaining_micro < 0`) and so can't serve the signed Buy's
/// short-cover scan. `CREATE INDEX IF NOT EXISTS` in `SCHEMA` won't rebuild an
/// index that already exists, so an open DB keeps the old predicate until this
/// drops + recreates it as `remaining_micro <> 0`.
///
/// Idempotent and safe on both new and old databases: a freshly created DB
/// already has the `<> 0` predicate from `SCHEMA` (the `LIKE` finds no `> 0`
/// definition), so this is a no-op there; a legacy DB is migrated exactly once.
fn migrate_lots_index(conn: &Connection) -> Result<(), StoreError> {
    let old_predicate: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'index' AND name = 'lots_token' AND sql LIKE '%remaining_micro > 0%'",
        [],
        |r| r.get(0),
    )?;
    if old_predicate > 0 {
        conn.execute_batch(
            "DROP INDEX IF EXISTS lots_token;
             CREATE INDEX IF NOT EXISTS lots_token ON lots(token) WHERE remaining_micro <> 0;",
        )?;
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
        migrate_lots_strategy(&conn)?;
        migrate_lots_index(&conn)?;
        Ok(Store {
            conn,
            write_errors: 0,
        })
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        migrate_strategy_columns(&conn)?;
        migrate_lots_strategy(&conn)?;
        migrate_lots_index(&conn)?;
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

    /// Insert a fill (LONG-ONLY / arb path). Buys create a lot (cost = −cash,
    /// which includes fee). Sells consume FIFO lots; returns the realized P&L
    /// delta in µUSDC (sell cash − consumed cost). Oversell rolls the whole fill
    /// back. This is the strict, inventory-free entry point the arbitrage engine
    /// uses; market-making sells-to-open via [`insert_fill_signed`].
    pub fn insert_fill(&mut self, r: &FillRow) -> Result<i64, StoreError> {
        self.insert_fill_core(r, false)
    }

    /// Insert a fill on the SIGNED (market-making) path: a token's open lots may
    /// be a LONG stack (qty > 0) or a SHORT stack (qty < 0), never mixed.
    /// Returns realized µUSDC.
    ///
    /// - **Sell** closes FIFO longs first, then opens a SHORT for any uncovered
    ///   remainder (the proceeds received become the short's negative basis) —
    ///   NO `Oversell`.
    /// - **Buy** covers FIFO shorts first, then opens a LONG for any remainder.
    /// - A single fill may cross zero (e.g. close a long and open a short);
    ///   rounding is against us so realized is a floor, matching the strict path.
    ///
    /// Used by inventory-bearing strategies whose [`FillRow`] is routed through
    /// `StoreMsg::FillSigned`; the arb path keeps using [`insert_fill`].
    pub fn insert_fill_signed(&mut self, r: &FillRow) -> Result<i64, StoreError> {
        self.insert_fill_core(r, true)
    }

    /// Shared core for [`insert_fill`] (`signed == false`, strict long-only) and
    /// [`insert_fill_signed`] (`signed == true`, signed long/short). Validates
    /// the action fail-closed, applies the lot effect, writes the fill row with
    /// its computed realized, and commits atomically (any error rolls back).
    fn insert_fill_core(&mut self, r: &FillRow, signed: bool) -> Result<i64, StoreError> {
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
        let realized: i64 = match (r.action.as_str(), signed) {
            // Strict (arb) path — byte-identical to the original insert_fill:
            // Buy opens a long lot; Sell consumes FIFO longs (Oversell + rollback).
            // Every lot is tagged with the fill's `strategy` (`"arb"` here) and
            // each consume is scoped to that same tag, so an arb-only DB keys on
            // the identical row set it always did.
            ("Buy", false) => {
                lots::insert_lot(&tx, r.token, r.ts_ms, r.qty_micro, -r.cash_micro, &r.strategy)?;
                0
            }
            ("Sell", false) => {
                let consumed = lots::consume_lots(&tx, r.token, r.qty_micro, &r.strategy)?;
                r.cash_micro - consumed
            }
            // Signed (market-making) path — Buy may cover a short, Sell may open one.
            ("Buy", true) => {
                lots::buy_signed(&tx, r.token, r.ts_ms, r.qty_micro, r.cash_micro, &r.strategy)?
            }
            // Only ("Sell", true) remains (action validated to Buy|Sell above).
            _ => lots::sell_signed(&tx, r.token, r.ts_ms, r.qty_micro, r.cash_micro, &r.strategy)?,
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
                // Complete-set conversions are an arb-only mechanism, so the
                // lots they create/consume are tagged + scoped to `"arb"`.
                lots::insert_lot(&tx, r.yes_token, r.ts_ms, r.units_micro, yes_cost, "arb")?;
                lots::insert_lot(&tx, r.no_token, r.ts_ms, r.units_micro, no_cost, "arb")?;
                0
            }
            _ => {
                // "merge" — validated above
                let cost_yes = lots::consume_lots(&tx, r.yes_token, r.units_micro, "arb")?;
                let cost_no = lots::consume_lots(&tx, r.no_token, r.units_micro, "arb")?;
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

    /// Open SIGNED position for a token, summed over its lots:
    /// `(net_micro, cost_micro)`.
    ///
    /// Sign convention (so `InventoryRisk::seed(token, net_micro, Usdc(cost_micro))`
    /// is correct):
    /// - **Long** stack → `net_micro > 0`, `cost_micro > 0` (cash paid in). These
    ///   are the identical positive values long-only (arb) callers saw before
    ///   signed lots existed.
    /// - **Short** stack → `net_micro < 0`, `cost_micro < 0` — the signed short
    ///   basis (cash taken in: `cost_micro = −proceeds_received`).
    /// - **Flat** (all lots consumed) → `(0, 0)`.
    ///
    /// A token never mixes long and short open lots (crossing zero closes the
    /// opposite side first), so the SUM is an unambiguous single-sided position.
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

    /// Insert a P&L snapshot at an EXPLICIT `ts_ms` (positional convenience over
    /// [`insert_pnl_snapshot`], which takes a full [`PnlRow`]). Production stamps
    /// `now` at the call site and uses `insert_pnl_snapshot`; this lets a caller —
    /// notably the store tests and the UTC-day loss-cap tests — record a snapshot
    /// at a deterministic timestamp. Delegates to `insert_pnl_snapshot` so there
    /// is a single INSERT path.
    pub fn record_pnl_at(
        &mut self,
        ts_ms: i64,
        cash_micro: i64,
        realized_micro: i64,
        unrealized_micro: i64,
        equity_micro: i64,
        strategy: &str,
    ) -> Result<(), StoreError> {
        self.insert_pnl_snapshot(&PnlRow {
            ts_ms,
            cash_micro,
            realized_micro,
            unrealized_micro,
            equity_micro,
            strategy: strategy.to_string(),
        })
    }

    /// ADD `delta_micro` to the cumulative day-realized LEDGER for
    /// `(utc_day, strategy)` (I3). Upsert that ACCUMULATES — a per-fill realized
    /// delta is added to the running total for the UTC day, so MANY sub-cap
    /// realizing sessions across a day SUM into one figure. Unlike the
    /// per-session `pnl_snapshots` (whose `realized_micro` resets each restart),
    /// this ledger persists and accrues, which is what closes the
    /// summed-sub-cap-realized loss-cap gap that [`ReadStore::day_pnl_micro`]'s
    /// snapshot-MIN gate cannot catch.
    ///
    /// `delta_micro` is i128 but stored as sqlite i64; an out-of-range delta is
    /// SATURATED (clamped) rather than erroring — real per-fill deltas are tiny,
    /// so this is purely a defensive guard on the wider engine type.
    pub fn add_day_realized(
        &mut self,
        utc_day: i64,
        strategy: &str,
        delta_micro: i128,
    ) -> Result<(), StoreError> {
        let delta_i64 = delta_micro.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
        self.conn.execute(
            "INSERT INTO day_realized (utc_day, strategy, realized_micro) VALUES (?1, ?2, ?3)
             ON CONFLICT(utc_day, strategy) DO UPDATE SET realized_micro = realized_micro + excluded.realized_micro",
            rusqlite::params![utc_day, strategy, delta_i64],
        )?;
        Ok(())
    }

    /// UPSERT one open copy position (keyed by `(condition_id, outcome_index)`),
    /// so a restart can reload + resume managing it. Called on entry and on a
    /// PARTIAL exit (with the reduced `qty_micro`/`cost_micro`).
    pub fn upsert_copy_position(&mut self, r: &CopyPositionRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO copy_positions
               (condition_id, outcome_index, asset, neg_risk, tick_decimals,
                condition_hex, trader, entry_ts, qty_micro, cost_micro)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(condition_id, outcome_index) DO UPDATE SET
               asset=excluded.asset, neg_risk=excluded.neg_risk,
               tick_decimals=excluded.tick_decimals, condition_hex=excluded.condition_hex,
               trader=excluded.trader, entry_ts=excluded.entry_ts,
               qty_micro=excluded.qty_micro, cost_micro=excluded.cost_micro",
            rusqlite::params![
                r.condition_id,
                r.outcome_index,
                r.asset,
                r.neg_risk as i64,
                r.tick_decimals,
                r.condition_hex,
                r.trader,
                r.entry_ts,
                r.qty_micro,
                r.cost_micro,
            ],
        )?;
        Ok(())
    }

    /// DELETE the persisted open copy position on a FULL close (follow-exit,
    /// stop-loss, or resolution redeem), so a restart does not resurrect it.
    pub fn close_copy_position(
        &mut self,
        condition_id: &str,
        outcome_index: i64,
    ) -> Result<(), StoreError> {
        self.conn.execute(
            "DELETE FROM copy_positions WHERE condition_id = ?1 AND outcome_index = ?2",
            rusqlite::params![condition_id, outcome_index],
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

    /// Append a reward-farm DECISION (Task 10, spec §12) and return its
    /// autoincrement `id` (`last_insert_rowid()`), the handle an outcome
    /// references. `state_json` / `action_json` are stored verbatim (opaque to
    /// the store). Append-only instrumentation; no Spec-1 consumer reads it yet.
    pub fn record_rf_decision(
        &mut self,
        ts_ms: i64,
        market: &str,
        state_json: &str,
        action_json: &str,
    ) -> Result<i64, StoreError> {
        self.conn.execute(
            "INSERT INTO rf_decisions (ts_ms, market, state_json, action_json)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![ts_ms, market, state_json, action_json],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Append a reward-farm OUTCOME (Task 10, spec §12) for an EXPLICIT
    /// `decision_id`. All amounts are µUSDC; components not computed in Spec 1
    /// are passed as `0`. The FK is intentionally NOT enforced (append-only
    /// telemetry) so a late/best-effort outcome never errors the write path.
    pub fn record_rf_outcome(
        &mut self,
        decision_id: i64,
        ts_ms: i64,
        reward_score_delta_micro: i64,
        rebate_micro: i64,
        adverse_pnl_micro: i64,
        inv_penalty_micro: i64,
    ) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO rf_outcomes (decision_id, ts_ms, reward_score_delta_micro,
             rebate_micro, adverse_pnl_micro, inv_penalty_micro)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                decision_id,
                ts_ms,
                reward_score_delta_micro,
                rebate_micro,
                adverse_pnl_micro,
                inv_penalty_micro
            ],
        )?;
        Ok(())
    }

    /// Append a reward-farm OUTCOME attributed to the MOST RECENT decision logged
    /// for `market`, returning whether one was found (`false` ⇒ the outcome is
    /// dropped). This is the correlation the fire-and-forget writer path uses: the
    /// MM never learns a decision's autoincrement id, so an outcome is tied to the
    /// latest `rf_decisions` row for its market key. Best-effort and heuristic
    /// (NOT a guarantee the filling order belongs to exactly that decision —
    /// sticky quotes mean a fill can post against a quote from an earlier cycle);
    /// it degrades cleanly to a drop after a restart, before this session's first
    /// decision for the market lands.
    pub fn record_rf_outcome_for_latest(
        &mut self,
        market: &str,
        ts_ms: i64,
        reward_score_delta_micro: i64,
        rebate_micro: i64,
        adverse_pnl_micro: i64,
        inv_penalty_micro: i64,
    ) -> Result<bool, StoreError> {
        let decision_id: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM rf_decisions WHERE market = ?1 ORDER BY id DESC LIMIT 1",
                [market],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })?;
        match decision_id {
            Some(id) => {
                self.record_rf_outcome(
                    id,
                    ts_ms,
                    reward_score_delta_micro,
                    rebate_micro,
                    adverse_pnl_micro,
                    inv_penalty_micro,
                )?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Count outcomes recorded for `decision_id` (test/inspection helper).
    pub fn count_rf_outcomes_for(&self, decision_id: i64) -> Result<i64, StoreError> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM rf_outcomes WHERE decision_id = ?1",
            [decision_id],
            |r| r.get(0),
        )?)
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

    // ── Task 4.2b: signed / short-inventory fills (market-making path) ─────────
    // The strict `insert_fill` tests above are UNCHANGED and still pass; these
    // exercise the new `insert_fill_signed` entry point, which never Oversells.

    #[test]
    fn signed_sell_opens_short_and_position_is_negative() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        // No longs → a signed Sell 100 @ $0.40 opens a PURE short: realized 0,
        // and `position` is the signed short (net < 0, basis = −proceeds).
        let realized = s
            .insert_fill_signed(&FillRow {
                order_id: "o1".into(),
                ts_ms: 1,
                token: 7,
                action: "Sell".into(),
                px_ticks: 40,
                tick_levels: 100,
                qty_micro: 100_000_000,
                cash_micro: 40_000_000, // proceeds received
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        assert_eq!(realized, 0, "a pure short-open realizes nothing");
        // Signed: net −100µsh, short basis −$40 → seed(-100µ, Usdc(-40µ)) is right.
        assert_eq!(s.position(7).unwrap(), (-100_000_000, -40_000_000));
        assert_eq!(s.count_fills().unwrap(), 1, "the short-open fill row persisted");
    }

    #[test]
    fn signed_buy_covers_short_and_realizes() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        s.insert_order(&order_row("o2")).unwrap();
        // Open short 100 @ $0.40 (proceeds $40 → basis −$40).
        s.insert_fill_signed(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 7,
            action: "Sell".into(),
            px_ticks: 40,
            tick_levels: 100,
            qty_micro: 100_000_000,
            cash_micro: 40_000_000,
            fee_micro: 0,
            strategy: "mm".into(),
        })
        .unwrap();
        // Buy 100 @ $0.30 fully covers → realized = 40 − 30 = +$10, flat.
        let realized = s
            .insert_fill_signed(&FillRow {
                order_id: "o2".into(),
                ts_ms: 2,
                token: 7,
                action: "Buy".into(),
                px_ticks: 30,
                tick_levels: 100,
                qty_micro: 100_000_000,
                cash_micro: -30_000_000, // cash paid
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        assert_eq!(realized, 10_000_000, "bought back $0.10/sh cheaper on 100 sh");
        assert_eq!(s.position(7).unwrap(), (0, 0), "short fully covered → flat");
        assert_eq!(s.realized_total().unwrap(), 10_000_000);
    }

    #[test]
    fn signed_fill_crosses_zero() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        s.insert_order(&order_row("o2")).unwrap();
        // Long 100 @ $0.45 (basis $45).
        s.insert_fill_signed(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 7,
            action: "Buy".into(),
            px_ticks: 45,
            tick_levels: 100,
            qty_micro: 100_000_000,
            cash_micro: -45_000_000,
            fee_micro: 0,
            strategy: "mm".into(),
        })
        .unwrap();
        // Sell 150 @ $0.50 (proceeds $75): close the 100 long AND open a 50 short.
        //   proceeds_close = floor(75·100/150) = 50; realized = 50 − 45 = +$5.
        //   short opens with proceeds_open = 75 − 50 = 25 → basis −$25.
        let realized = s
            .insert_fill_signed(&FillRow {
                order_id: "o2".into(),
                ts_ms: 2,
                token: 7,
                action: "Sell".into(),
                px_ticks: 50,
                tick_levels: 100,
                qty_micro: 150_000_000,
                cash_micro: 75_000_000,
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        assert_eq!(realized, 5_000_000, "closed the 100 long for +$5");
        assert_eq!(
            s.position(7).unwrap(),
            (-50_000_000, -25_000_000),
            "now short 50 @ $0.50 (signed net + signed basis)"
        );
        assert_eq!(s.realized_total().unwrap(), 5_000_000);
    }

    #[test]
    fn signed_partial_short_cover_conserves() {
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        s.insert_order(&order_row("o2")).unwrap();
        // Open short 3 µshares for 10 µUSDC proceeds (basis −10) — odd, indivisible
        // numbers force the pro-rata rounding (mirrors the strict partial tests).
        s.insert_fill_signed(&FillRow {
            order_id: "o1".into(),
            ts_ms: 1,
            token: 9,
            action: "Sell".into(),
            px_ticks: 1,
            tick_levels: 100,
            qty_micro: 3,
            cash_micro: 10,
            fee_micro: 0,
            strategy: "mm".into(),
        })
        .unwrap();
        assert_eq!(s.position(9).unwrap(), (-3, -10));
        // Cover 1 µshare for 4 µUSDC cost. Partial: consumed basis magnitude =
        // floor(10·1/3) = 3 → proceeds_consumed 3; realized = 3 − 4 = −1 (a FLOOR,
        // against us). 2 µshares remain with basis −7 (magnitude ceiled).
        let realized = s
            .insert_fill_signed(&FillRow {
                order_id: "o2".into(),
                ts_ms: 2,
                token: 9,
                action: "Buy".into(),
                px_ticks: 4,
                tick_levels: 100,
                qty_micro: 1,
                cash_micro: -4,
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        assert_eq!(realized, -1);
        // 2 µshares of short remain; the residual basis −7 has its magnitude
        // ceiled (against us), and consuming the rest later conserves it to −10.
        assert_eq!(s.position(9).unwrap(), (-2, -7));
        assert_eq!(s.realized_total().unwrap(), -1);
    }

    #[test]
    fn insert_fill_strict_still_oversells() {
        // The signed path now exists, but the strict (arb) `insert_fill` MUST
        // still Oversell + roll back when a Sell exceeds long holdings — the
        // deliberate long-only safety error is unchanged. The SAME fill on the
        // signed path succeeds, opening a short instead.
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        let strict = s.insert_fill(&FillRow {
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
        assert!(matches!(strict, Err(StoreError::Oversell { token: 7, .. })));
        assert_eq!(s.count_fills().unwrap(), 0, "strict oversell rolls the fill back");

        let realized = s
            .insert_fill_signed(&FillRow {
                order_id: "o1".into(),
                ts_ms: 1,
                token: 7,
                action: "Sell".into(),
                px_ticks: 50,
                tick_levels: 100,
                qty_micro: 1_000_000,
                cash_micro: 500_000,
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        assert_eq!(realized, 0, "signed sell-to-open realizes nothing");
        assert_eq!(s.position(7).unwrap(), (-1_000_000, -500_000));
        assert_eq!(s.count_fills().unwrap(), 1, "the signed short-open fill persisted");
    }

    #[test]
    fn signed_buy_with_no_short_opens_long_like_strict() {
        // With nothing to cover, a signed Buy must behave exactly like the strict
        // Buy: open a long lot, realized 0, identical position.
        let mut s = mem();
        s.insert_order(&order_row("o1")).unwrap();
        let realized = s
            .insert_fill_signed(&FillRow {
                order_id: "o1".into(),
                ts_ms: 1,
                token: 7,
                action: "Buy".into(),
                px_ticks: 44,
                tick_levels: 100,
                qty_micro: 100_000_000,
                cash_micro: -44_000_000,
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        assert_eq!(realized, 0);
        assert_eq!(s.position(7).unwrap(), (100_000_000, 44_000_000));
    }

    #[test]
    fn legacy_lots_index_is_migrated_to_include_shorts() {
        // A pre-signed DB carries the partial lots_token index with the old
        // `remaining_micro > 0` predicate (short lots invisible). Opening must
        // recreate it as `remaining_micro <> 0` so short lots are indexed too.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy_idx.sqlite");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE lots (
                   id INTEGER PRIMARY KEY AUTOINCREMENT, token INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
                   qty_micro INTEGER NOT NULL, remaining_micro INTEGER NOT NULL,
                   cost_micro INTEGER NOT NULL, cost_remaining_micro INTEGER NOT NULL);
                 CREATE INDEX lots_token ON lots(token) WHERE remaining_micro > 0;",
            )
            .unwrap();
        }
        Store::open(&path).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='lots_token'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            sql.contains("remaining_micro <> 0"),
            "index predicate must be migrated to `<> 0`, got: {sql}"
        );
        assert!(
            !sql.contains("remaining_micro > 0"),
            "the old `> 0` predicate must be gone, got: {sql}"
        );
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

    #[test]
    fn legacy_db_migrates_strategy_column_across_all_four_tables() {
        // A pre-strategy database has EVERY tagged table without the strategy
        // column. Opening it must migrate all four (not just whichever table a
        // later write happens to touch first): `CREATE TABLE IF NOT EXISTS` is a
        // no-op on the existing tables, so only the ALTER path can add the column.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy_all.sqlite");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            // Column-less legacy schema: the real pre-strategy tables minus the
            // strategy column (FK clauses dropped so each seed row stands alone).
            conn.execute_batch(
                "CREATE TABLE opportunities (
                   id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, class TEXT NOT NULL,
                   fingerprint TEXT NOT NULL, edge_bps INTEGER NOT NULL, units_micro INTEGER NOT NULL,
                   net_micro INTEGER NOT NULL, basis_micro INTEGER NOT NULL, legs_json TEXT NOT NULL,
                   dispatched INTEGER NOT NULL);
                 CREATE TABLE orders (
                   id TEXT PRIMARY KEY, ts_ms INTEGER NOT NULL, fingerprint TEXT NOT NULL,
                   token INTEGER NOT NULL, action TEXT NOT NULL, limit_ticks INTEGER NOT NULL,
                   tick_levels INTEGER NOT NULL, qty_micro INTEGER NOT NULL, state TEXT NOT NULL);
                 CREATE TABLE fills (
                   id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL, ts_ms INTEGER NOT NULL,
                   token INTEGER NOT NULL, action TEXT NOT NULL, px_ticks INTEGER NOT NULL,
                   tick_levels INTEGER NOT NULL, qty_micro INTEGER NOT NULL, cash_micro INTEGER NOT NULL,
                   fee_micro INTEGER NOT NULL, realized_micro INTEGER NOT NULL);
                 CREATE TABLE pnl_snapshots (
                   id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, cash_micro INTEGER NOT NULL,
                   realized_micro INTEGER NOT NULL, unrealized_micro INTEGER NOT NULL, equity_micro INTEGER NOT NULL);",
            )
            .unwrap();
            // One pre-existing row per table, inserted the old (column-less) way.
            conn.execute(
                "INSERT INTO opportunities
                   (ts_ms, class, fingerprint, edge_bps, units_micro, net_micro, basis_micro, legs_json, dispatched)
                 VALUES (1, 'C1Long', 'fp', 600, 1, 2, 3, '[]', 0)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO orders
                   (id, ts_ms, fingerprint, token, action, limit_ticks, tick_levels, qty_micro, state)
                 VALUES ('o1', 1, 'fp', 7, 'Buy', 44, 100, 100, 'Draft')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO fills
                   (order_id, ts_ms, token, action, px_ticks, tick_levels, qty_micro, cash_micro, fee_micro, realized_micro)
                 VALUES ('o1', 1, 7, 'Buy', 44, 100, 100, -44, 0, 0)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO pnl_snapshots
                   (ts_ms, cash_micro, realized_micro, unrealized_micro, equity_micro)
                 VALUES (1, -44, 0, 0, -44)",
                [],
            )
            .unwrap();
        }

        // Opening migrates every tagged table (idempotent ALTER per table).
        Store::open(&path).unwrap();

        // For each table the strategy column must now exist AND the pre-existing
        // row must back-fill to 'arb'. A `SELECT strategy` round-trip on a raw
        // connection proves both at once (it errors if the column is absent).
        let conn = rusqlite::Connection::open(&path).unwrap();
        for table in STRATEGY_TABLES {
            let strategy: String = conn
                .query_row(&format!("SELECT strategy FROM {table}"), [], |row| row.get(0))
                .unwrap_or_else(|e| panic!("table `{table}` was not migrated: {e}"));
            assert_eq!(strategy, "arb", "table `{table}` must back-fill 'arb'");
        }
    }

    #[test]
    fn legacy_lots_without_strategy_column_backfills_arb() {
        // A pre-strategy DB has a `lots` table WITHOUT the strategy column, with
        // one open long lot. Opening must ALTER in the column (idempotent),
        // back-filling the legacy lot to 'arb' — so the arb-scoped consume and
        // `open_positions` see exactly the rows they always did.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy_lots.sqlite");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE lots (
                   id INTEGER PRIMARY KEY AUTOINCREMENT, token INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
                   qty_micro INTEGER NOT NULL, remaining_micro INTEGER NOT NULL,
                   cost_micro INTEGER NOT NULL, cost_remaining_micro INTEGER NOT NULL);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO lots (token, ts_ms, qty_micro, remaining_micro, cost_micro, cost_remaining_micro)
                 VALUES (7, 1, 100000000, 100000000, 44000000, 44000000)",
                [],
            )
            .unwrap();
        }

        // Opening migrates the lots table (adds + back-fills the strategy column).
        let s = Store::open(&path).unwrap();
        // The un-scoped, per-token position is unchanged by the migration.
        assert_eq!(s.position(7).unwrap(), (100_000_000, 44_000_000));
        drop(s);

        // open_positions now tags the back-filled legacy lot 'arb'.
        let r = crate::read::ReadStore::open(&path).unwrap();
        assert_eq!(
            r.open_positions().unwrap(),
            vec![(7, "arb".to_string(), 100_000_000, 44_000_000)],
            "the legacy lot back-fills to the 'arb' tag"
        );

        // A raw SELECT proves the column exists and the row back-filled.
        let conn = rusqlite::Connection::open(&path).unwrap();
        let strategy: String = conn
            .query_row("SELECT strategy FROM lots", [], |row| row.get(0))
            .unwrap();
        assert_eq!(strategy, "arb");
    }

    #[test]
    fn arb_consume_is_strategy_scoped_oversells_despite_mm_holdings() {
        // Lot consumption is scoped to the fill's strategy: a strategy only ever
        // draws down its OWN lots. mm holds a long 100 on token 7, but arb holds
        // nothing there — so a strict arb Sell must STILL Oversell + roll back
        // (without scoping it would wrongly consume mm's long). This is the
        // converse of the arb invariant: arb-only DBs are unchanged precisely
        // BECAUSE the scope is the fill's own tag.
        let mut s = mem();
        // mm long via the signed Buy path (no shorts → opens a long, realized 0).
        s.insert_order(&order_row("mm1")).unwrap();
        s.insert_fill_signed(&FillRow {
            order_id: "mm1".into(),
            ts_ms: 1,
            token: 7,
            action: "Buy".into(),
            px_ticks: 44,
            tick_levels: 100,
            qty_micro: 100_000_000,
            cash_micro: -44_000_000,
            fee_micro: 0,
            strategy: "mm".into(),
        })
        .unwrap();
        assert_eq!(s.position(7).unwrap(), (100_000_000, 44_000_000), "mm's long exists");

        // A strict arb Sell on the same token: arb owns no lots → Oversell.
        s.insert_order(&order_row("arb1")).unwrap();
        let err = s.insert_fill(&FillRow {
            order_id: "arb1".into(),
            ts_ms: 2,
            token: 7,
            action: "Sell".into(),
            px_ticks: 50,
            tick_levels: 100,
            qty_micro: 10_000_000,
            cash_micro: 5_000_000,
            fee_micro: 0,
            strategy: "arb".into(),
        });
        assert!(
            matches!(err, Err(StoreError::Oversell { token: 7, .. })),
            "arb must oversell — it cannot consume mm's lots"
        );
        // mm's long is untouched (arb's failed sell rolled back); the un-scoped
        // per-token sum is unchanged.
        assert_eq!(
            s.position(7).unwrap(),
            (100_000_000, 44_000_000),
            "mm long untouched by arb's rolled-back sell"
        );
    }

    // ── Task 10: reward-farm instrumentation tables (feed future Spec 3) ────────

    #[test]
    fn rf_decision_and_outcome_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(&dir.path().join("rf.sqlite")).unwrap();
        let id = s
            .record_rf_decision(1_000, "0xcond", r#"{"mid":0.5}"#, r#"{"bid":0.49}"#)
            .unwrap();
        s.record_rf_outcome(id, 2_000, 12_000, 0, -3_000, -1_000).unwrap();
        assert!(id > 0);
        assert_eq!(s.count_rf_outcomes_for(id).unwrap(), 1);
    }

    #[test]
    fn legacy_db_gains_rf_tables_additively() {
        // A pre-Task-10 database lacks the rf_* tables (here only a legacy
        // pnl_snapshots table with one row). Opening must ADD them via the
        // additive `CREATE TABLE IF NOT EXISTS` path WITHOUT disturbing existing
        // data — the new tables are purely additive, no migration logic needed.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy_rf.sqlite");
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
            let rf_tables: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type='table' AND name IN ('rf_decisions','rf_outcomes')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(rf_tables, 0, "legacy DB starts WITHOUT the rf_* tables");
        }

        // Opening creates the rf_* tables additively; they now accept rows.
        let mut s = Store::open(&path).unwrap();
        let id = s.record_rf_decision(1_000, "0xcond", "{}", "{}").unwrap();
        s.record_rf_outcome(id, 2_000, 0, 0, 0, 0).unwrap();
        assert_eq!(s.count_rf_outcomes_for(id).unwrap(), 1);
        drop(s);

        // The pre-existing legacy pnl row is intact (back-filled to 'arb').
        let r = crate::read::ReadStore::open(&path).unwrap();
        let arb = r.recent_pnl_by_strategy("arb", 10).unwrap();
        assert_eq!(arb.len(), 1);
        assert_eq!(arb[0].equity_micro, 4);
    }

    #[test]
    fn rf_outcome_for_latest_correlates_to_newest_decision_per_market() {
        // The writer's fire-and-forget correlation: an outcome attaches to the
        // MOST RECENT decision for its market key (the MM never learns the id).
        let mut s = Store::open_in_memory().unwrap();
        let _a1 = s.record_rf_decision(1, "A", "{}", "{}").unwrap();
        let b1 = s.record_rf_decision(2, "B", "{}", "{}").unwrap();
        let a2 = s.record_rf_decision(3, "A", "{}", "{}").unwrap();

        // "A" → A's LATEST decision (a2), never the earlier a1 or the other market.
        assert!(s.record_rf_outcome_for_latest("A", 4, 0, 5, -2, 0).unwrap());
        assert_eq!(s.count_rf_outcomes_for(a2).unwrap(), 1);
        assert_eq!(s.count_rf_outcomes_for(_a1).unwrap(), 0);
        assert_eq!(s.count_rf_outcomes_for(b1).unwrap(), 0);

        // A market with no decision yet → the outcome is dropped (Ok(false)).
        assert!(!s.record_rf_outcome_for_latest("ZZZ", 5, 0, 0, 0, 0).unwrap());
    }

    // ── I3: cumulative day-realized ledger (closes the summed-sub-cap loss gap) ──

    #[test]
    fn legacy_db_gains_day_realized_table_additively() {
        // A pre-I3 database lacks the `day_realized` ledger table. Opening must
        // ADD it via the additive `CREATE TABLE IF NOT EXISTS` path WITHOUT
        // disturbing existing data — purely additive, mirroring the rf_* tables.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy_ledger.sqlite");
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
            let ledger: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type='table' AND name='day_realized'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(ledger, 0, "legacy DB starts WITHOUT the day_realized table");
        }

        // Opening creates the table additively; it now accepts adds that ACCUMULATE.
        let mut s = Store::open(&path).unwrap();
        s.add_day_realized(0, "mm", -4_000_000).unwrap();
        s.add_day_realized(0, "mm", -4_000_000).unwrap();
        drop(s);

        let r = crate::read::ReadStore::open(&path).unwrap();
        assert_eq!(r.day_realized_micro("mm", 0).unwrap(), -8_000_000);
        // The pre-existing legacy pnl row is intact (back-filled to 'arb').
        let arb = r.recent_pnl_by_strategy("arb", 10).unwrap();
        assert_eq!(arb.len(), 1);
        assert_eq!(arb[0].equity_micro, 4);
    }

    #[test]
    fn add_day_realized_saturates_out_of_i64_range() {
        // `delta_micro` is i128 but stored as i64; an out-of-range delta SATURATES
        // (clamps) rather than panicking or silently wrapping. Real per-fill
        // deltas are tiny, but the type is i128, so the conversion is guarded.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sat.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.add_day_realized(0, "mm", i128::from(i64::MIN) - 1000).unwrap();
        drop(s);
        let r = crate::read::ReadStore::open(&path).unwrap();
        assert_eq!(
            r.day_realized_micro("mm", 0).unwrap(),
            i128::from(i64::MIN),
            "an out-of-range negative delta clamps to i64::MIN"
        );
    }

    #[test]
    fn copy_positions_upsert_read_close_round_trip() {
        // The copy-position durable ledger (restart-safety): UPSERT is keyed by
        // (condition_id, outcome_index) so a partial-exit re-persist REPLACES the
        // row (no duplicate); the reader returns it; CLOSE deletes it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("copypos.sqlite");
        let mut s = Store::open(&path).unwrap();
        let row = CopyPositionRow {
            condition_id: "0xabc".into(),
            outcome_index: 1,
            asset: "12345678901234567890".into(),
            neg_risk: true,
            tick_decimals: 3,
            condition_hex: "0xabc".into(),
            trader: "0xtrader".into(),
            entry_ts: 1_000,
            qty_micro: 12_345_600,
            cost_micro: 5_000_000,
        };
        s.upsert_copy_position(&row).unwrap();
        // Partial exit → re-upsert with a reduced qty/cost REPLACES the same key.
        let mut reduced = row.clone();
        reduced.qty_micro = 6_000_000;
        reduced.cost_micro = 2_500_000;
        s.upsert_copy_position(&reduced).unwrap();
        drop(s);

        let rs = crate::read::ReadStore::open(&path).unwrap();
        let got = rs.copy_open_positions().unwrap();
        assert_eq!(got.len(), 1, "UPSERT replaces on the PK — no duplicate row");
        assert_eq!(got[0], reduced, "reader returns the latest persisted state");
        drop(rs);

        let mut s = Store::open(&path).unwrap();
        s.close_copy_position("0xabc", 1).unwrap();
        drop(s);
        let rs = crate::read::ReadStore::open(&path).unwrap();
        assert!(
            rs.copy_open_positions().unwrap().is_empty(),
            "CLOSE deletes the row so a restart won't resurrect it"
        );
    }
}
