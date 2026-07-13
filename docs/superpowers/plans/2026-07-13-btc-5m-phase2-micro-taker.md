# BTC 5m — Phase 2 Micro-Taker — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** With `[strategies.btc5m].live=true`, place *tiny* marketable-FAK buys on the near-certain leader in the last seconds of a window when it's offered below fair (net of the 7% fee), hold to resolution, and record realized PnL — proving positive live expectancy on real (micro) capital before Phase 3.

**Architecture:** Extends the Phase-1 shadow loop. The read-only shadow path is unchanged; when `live=true` AND a window is in its final `entry_window_secs`, the loop computes the leader and — if that leader's own token is offered at `≤ fair − fee − edge_buffer` — sends ONE marketable FAK for a fixed micro notional via the existing `LiveVenue` (sigType-3 taker path, `ensure_token` for the dynamic token). Positions are recorded in a new `btc5m_positions` table and **held to resolution** (no active flatten — the binary self-resolves in ≤5 min). A settle sweep reads each closed window's outcome from Gamma (the same `eventMetadata`/`outcomePrices` the report backfill uses), books realized PnL to `day_realized(strategy='btc5m')`, and closes the row; on-chain redemption of winners rides the existing shared-wallet redeem sweep. `RiskEngine`/`InventoryRisk` enforce hard daily-loss + session kill.

**Tech Stack:** Rust (edition 2024, `pm-*` crates), tokio, reqwest, rusqlite. Reuses `LiveVenue` (`crates/execution/src/live.rs`), `InventoryRisk`/`RiskEngine` (`crates/risk`), the Phase-1 `btc5m` model/gamma/clob modules.

---

## ⛔ SAFETY — preconditions & invariants (read before implementing)

- **Gate-1 must have passed first.** Phase 2 is only enabled after `deploy/btc5m_report.py` shows (over days of shadow data) a positive median net edge + high realized win% in the `[0,20)s` buckets with a small proxy↔true basis. **This plan builds the capability; a human flips it on.**
- **`live=false` is the shipped default.** `enabled=true, live=false` = Phase-1 shadow (unchanged). Only `enabled=true, live=true` arms the taker. The operator sets `live=true` explicitly.
- **Hard invariants enforced in code + tests:** FAK-only (never rests a quote → no adverse selection); **at most one entry per window**; micro fixed notional; enter **only** when `0 < secs_to_go ≤ entry_window_secs` (default 20s) — a FAK placed too late simply fails to fill, so the ~2–5s relay dead zone is fail-safe (a `min_entry_secs` floor is a recommended refinement to avoid wasted late attempts); **hard daily-loss kill** (halts the strategy) and session-loss halt via `RiskEngine`; buys the leader at its **own token's ask** (near a price extreme where the 7% fee ≈ 0).
- **Gate-2 stop criteria (post-deploy):** halt + review if realized daily PnL breaches the floor, or the live realized win-rate materially undershoots the shadow-measured rate (adverse selection / model drift). Do not proceed to Phase 3 until live realized PnL lower-CI > 0 over K trades **and** the executor is relocated to London (spec §5, Gate 2→3).

---

## File structure

| File | New/Mod | Responsibility |
|---|---|---|
| `crates/app/src/strategy/btc5m/entry.rs` | New | Pure taker-entry decision: leader + offer + net-edge + size → `Option<Entry>`. |
| `crates/app/src/strategy/btc5m/settle.rs` | New | Pure realized-PnL from resolved outcome + entry. |
| `crates/app/src/strategy/btc5m/mod.rs` | Mod | Loop: (live) entry via venue + position record; settle sweep. |
| `crates/config/src/lib.rs` | Mod | Phase-2 knobs in `Btc5mParamsCfg` + validate. |
| `crates/store/src/{lib,read,writer}.rs` | Mod | `btc5m_positions` table/row/upsert/close/read + `StoreMsg`. |
| `crates/app/src/wiring.rs` | Mod | btc5m `InventoryConfig`/`RiskConfig`; envelope already carved (Phase 1). |
| `crates/app/src/main.rs` | Mod | Build a `LiveVenue` for btc5m when `live`; inject venue + inventory + store path (position reload). |
| `deploy/status.sh` | Mod | Add btc5m open positions + realized to the BTC `pnl` section. |
| `mm-live-copy-canary.toml` | Mod | Phase-2 knobs; `live=false`. |

> **Execution note:** as in the Phase-0/1 plan, re-verify the exact `LiveVenue`/`CopyVenue` `submit_fak`/`ensure_token` signatures, `Order::new`, `InventoryRisk`, and the copy-registration wiring against the real code before implementing Tasks 5–7 (dispatch an interface-extraction pass first). Tasks 1–4 are self-contained and fully specified here.

---

## Task 1: Config — Phase-2 knobs

**Files:** Modify `crates/config/src/lib.rs` (`Btc5mParamsCfg` + `validate`).

- [ ] **Step 1: Write the failing test** (in the config `tests` mod):
```rust
    #[test]
    fn btc5m_phase2_knobs_default_and_validate() {
        let c = Config::default();
        assert_eq!(c.btc5m_params.entry_window_secs, 20);
        assert_eq!(c.btc5m_params.entry_notional_usd, 10.0);
        // edge_buffer must be in (0,1); reject 0
        assert!(Config::from_toml_str("[btc5m]\nedge_buffer_c = 0.0\n").is_err());
        // entry_window must be > 0 and < 300
        assert!(Config::from_toml_str("[btc5m]\nentry_window_secs = 0\n").is_err());
        assert!(Config::from_toml_str("[btc5m]\nentry_window_secs = 400\n").is_err());
        // daily loss floor must be > 0
        assert!(Config::from_toml_str("[btc5m]\nmax_daily_loss_usd = 0.0\n").is_err());
    }
```
- [ ] **Step 2: Run → FAIL to compile.** `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-config -- btc5m_phase2_knobs`
- [ ] **Step 3: Add the fields** to `Btc5mParamsCfg` (after the Phase-1 fields):
```rust
    /// Only enter when secs_to_go ≤ this (and > 0). Keeps us out of the final
    /// ~2–5s relay-latency dead zone. Phase 2.
    pub entry_window_secs: i64,
    /// Required net edge to enter, in PROBABILITY units (0.02 = 2¢/share). Phase 2.
    pub edge_buffer_c: f64,
    /// Fixed micro notional per entry, USD. Phase 2.
    pub entry_notional_usd: f64,
    /// Max total taker notional deployed per UTC day, USD (circuit breaker). Phase 2.
    pub max_daily_notional_usd: f64,
    /// Daily realized-loss floor, USD; breaching it halts the strategy. Phase 2.
    pub max_daily_loss_usd: f64,
```
Add to `Default`:
```rust
            entry_window_secs: 20,
            edge_buffer_c: 0.02,
            entry_notional_usd: 10.0,
            max_daily_notional_usd: 200.0,
            max_daily_loss_usd: 25.0,
```
- [ ] **Step 4: Add validation** (in the btc5m block of `Config::validate`):
```rust
        if !(bp.entry_window_secs > 0 && bp.entry_window_secs < 300) {
            return Err(ConfigError::BadMoney("btc5m.entry_window_secs must be in (0, 300)"));
        }
        if !(bp.edge_buffer_c.is_finite() && bp.edge_buffer_c > 0.0 && bp.edge_buffer_c < 1.0) {
            return Err(ConfigError::BadMoney("btc5m.edge_buffer_c must be in (0, 1)"));
        }
        if !(bp.entry_notional_usd.is_finite() && bp.entry_notional_usd > 0.0) {
            return Err(ConfigError::BadMoney("btc5m.entry_notional_usd must be > 0"));
        }
        if !(bp.max_daily_notional_usd.is_finite() && bp.max_daily_notional_usd >= bp.entry_notional_usd) {
            return Err(ConfigError::BadMoney("btc5m.max_daily_notional_usd must be ≥ entry_notional_usd"));
        }
        if !(bp.max_daily_loss_usd.is_finite() && bp.max_daily_loss_usd > 0.0) {
            return Err(ConfigError::BadMoney("btc5m.max_daily_loss_usd must be > 0"));
        }
```
- [ ] **Step 5: Run → PASS.** `cargo test -p pm-config`
- [ ] **Step 6: Commit.** `git add crates/config/src/lib.rs && git commit -m "feat(btc5m): Phase-2 config knobs (entry window, edge buffer, sizing, daily caps)"`

---

## Task 2: Store — `btc5m_positions`

**Files:** Modify `crates/store/src/{lib,read,writer}.rs`. Mirror the Phase-1 `btc5m_shadow` and existing `copy_positions` patterns exactly.

- [ ] **Step 1: Write the failing round-trip test** (store `tests` mod, tempfile idiom):
```rust
    #[test]
    fn btc5m_position_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.upsert_btc5m_position(&Btc5mPositionRow {
            condition_id: "0xC".into(), outcome_index: 0, token: "111".into(),
            qty_micro: 11_000_000, cost_micro: 9_900_000, entry_ts: 1, t_close_ms: 300_000,
            strike: 62_900.0,
        }).unwrap();
        let rs = pm_store::read::ReadStore::open(&path).unwrap();
        assert_eq!(rs.btc5m_open_positions().unwrap().len(), 1);
        s.close_btc5m_position("0xC", 0).unwrap();
        assert_eq!(pm_store::read::ReadStore::open(&path).unwrap().btc5m_open_positions().unwrap().len(), 0);
    }
```
- [ ] **Step 2: Run → FAIL.** `cargo test -p pm-store -- btc5m_position_roundtrip`
- [ ] **Step 3: Add row + table + methods** in `lib.rs` (mirror `copy_positions`):
```rust
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Btc5mPositionRow {
    pub condition_id: String,
    pub outcome_index: i64,   // 0 = Up/YES bought, 1 = Down/NO bought
    pub token: String,        // the CLOB token id we bought
    pub qty_micro: i64,
    pub cost_micro: i64,
    pub entry_ts: i64,
    pub t_close_ms: i64,      // window close (for the settle sweep)
    pub strike: f64,          // proxy strike at entry (audit)
}
```
Append to `SCHEMA`:
```sql
CREATE TABLE IF NOT EXISTS btc5m_positions (
  condition_id TEXT NOT NULL, outcome_index INTEGER NOT NULL, token TEXT NOT NULL,
  qty_micro INTEGER NOT NULL, cost_micro INTEGER NOT NULL, entry_ts INTEGER NOT NULL,
  t_close_ms INTEGER NOT NULL, strike REAL NOT NULL,
  PRIMARY KEY (condition_id, outcome_index));
```
Add `upsert_btc5m_position` (INSERT … ON CONFLICT(condition_id,outcome_index) DO UPDATE …, mirroring `upsert_copy_position`) and `close_btc5m_position(condition_id, outcome_index)` (DELETE, mirroring `close_copy_position`).
- [ ] **Step 4: Add read** in `read.rs` (mirror `copy_open_positions`): `btc5m_open_positions() -> Result<Vec<Btc5mPositionRow>, StoreError>` (`SELECT … FROM btc5m_positions ORDER BY condition_id`).
- [ ] **Step 5: Add `StoreMsg` variants + writer arms** in `writer.rs` (mirror `CopyPositionUpsert`/`CopyPositionClose`): `Btc5mPositionUpsert(Btc5mPositionRow)`, `Btc5mPositionClose { condition_id: String, outcome_index: i64 }`.
- [ ] **Step 6: Run → PASS.** `cargo test -p pm-store` (round-trip + legacy-migration tests still green — additive schema).
- [ ] **Step 7: Commit.** `git add crates/store/src/lib.rs crates/store/src/read.rs crates/store/src/writer.rs && git commit -m "feat(btc5m): store — btc5m_positions table + write/read path"`

---

## Task 3: Pure entry decision (`entry.rs`)

**Files:** Create `crates/app/src/strategy/btc5m/entry.rs`; add `pub mod entry;` to `btc5m/mod.rs`.

- [ ] **Step 1: Write the failing tests:**
```rust
//! Pure taker-entry decision for the btc5m micro-taker (Phase 2). No I/O. Given
//! the leader (by z), its own token's ask, and the fair value, decide whether to
//! buy the near-certain leader as a marketable FAK, and at what size.
use pm_core::num::{Px, Qty, TickSize};

/// Parameters governing a Phase-2 entry (from config).
#[derive(Debug, Clone, Copy)]
pub struct EntryParams {
    pub entry_window_secs: i64,
    pub z_threshold: f64,
    pub edge_buffer: f64,   // probability units
    pub fee_rate: f64,      // 0.07
    pub notional_usd: f64,
}

/// A decided entry: buy `up ? YES : NO` at `limit_px` (marketable = the ask), `qty` µshares.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Entry { pub up: bool, pub limit_px: Px, pub qty: Qty }

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn enters_a_cheap_late_leader() {
        // T-15s, z≈+2 (UP leads), YES ask 0.90, fair 0.98 → net 0.98-0.90-fee ≈ 0.074 ≥ 0.02
        let p = EntryParams { entry_window_secs: 20, z_threshold: 1.5, edge_buffer: 0.02, fee_rate: 0.07, notional_usd: 10.0 };
        let e = decide_entry(15, 2.0, 0.98, 900_000, TickSize::Cent, p).unwrap();
        assert!(e.up);
        assert_eq!(e.limit_px.get(), 90);         // 0.90 on the Cent grid
        assert_eq!(e.qty, Qty(11_000_000));        // floor($10 / $0.90) = 11 shares
    }

    #[test]
    fn rejects_outside_window_thin_edge_and_no_book() {
        let p = EntryParams { entry_window_secs: 20, z_threshold: 1.5, edge_buffer: 0.02, fee_rate: 0.07, notional_usd: 10.0 };
        assert!(decide_entry(25, 2.0, 0.98, 900_000, TickSize::Cent, p).is_none()); // too early
        assert!(decide_entry(0, 2.0, 0.98, 900_000, TickSize::Cent, p).is_none());  // expired
        assert!(decide_entry(15, 1.0, 0.98, 900_000, TickSize::Cent, p).is_none()); // |z| < thresh
        assert!(decide_entry(15, 2.0, 0.995, 990_000, TickSize::Cent, p).is_none());// net edge < buffer
        assert!(decide_entry(15, 2.0, 0.98, 0, TickSize::Cent, p).is_none());       // no book
    }
}
```
- [ ] **Step 2: Run → FAIL to compile.** `cargo test -p pm-app -- strategy::btc5m::entry::tests`
- [ ] **Step 3: Implement `decide_entry`** (add `pub mod entry;` to `btc5m/mod.rs`):
```rust
/// Decide a taker entry on the leader. `secs` = seconds-to-go; `z` = normalized
/// deviation (sign = leader: >0 UP, <0 DOWN); `fair_leader` = fair P(leader wins);
/// `leader_ask_micro` = the LEADER token's best ask in µUSDC (0 = no book). Returns
/// the buy on the leader's own token if it's offered ≥ `edge_buffer` below fair
/// net of the fee, sized to `notional_usd`. Pure; no I/O.
pub fn decide_entry(secs: i64, z: f64, fair_leader: f64, leader_ask_micro: i64, ts: TickSize, p: EntryParams) -> Option<Entry> {
    if secs <= 0 || secs > p.entry_window_secs { return None; }
    if !z.is_finite() || z.abs() < p.z_threshold { return None; }
    if leader_ask_micro <= 0 { return None; }
    let offer = leader_ask_micro as f64 / 1_000_000.0;
    if !(offer > 0.0 && offer < 1.0) { return None; }
    let fee = p.fee_rate * offer * (1.0 - offer);
    if !fair_leader.is_finite() || fair_leader - offer - fee < p.edge_buffer { return None; }
    // Marketable buy AT the ask (a FAK crossing exactly the resting offer).
    let unit = ts.unit_microusdc();
    let ticks = (leader_ask_micro as u64) / unit;                 // ask is already tick-aligned
    let limit_px = Px::new(ticks as u16, ts).ok()?;
    let shares = (p.notional_usd / offer).floor();
    if !(shares >= 1.0 && shares.is_finite()) { return None; }
    let qty = Qty((shares * 1_000_000.0) as u64);
    Some(Entry { up: z > 0.0, limit_px, qty })
}
```
- [ ] **Step 4: Run → PASS.** `cargo test -p pm-app -- strategy::btc5m::entry`
- [ ] **Step 5: Commit.** `git add crates/app/src/strategy/btc5m/entry.rs crates/app/src/strategy/btc5m/mod.rs && git commit -m "feat(btc5m): pure taker-entry decision (late cheap leader → sized FAK)"`

---

## Task 4: Pure settlement PnL (`settle.rs`)

**Files:** Create `crates/app/src/strategy/btc5m/settle.rs`; `pub mod settle;` in `btc5m/mod.rs`.

- [ ] **Step 1: Write the failing test:**
```rust
//! Pure realized-PnL for a resolved btc5m binary position. Winners redeem to $1/
//! share, losers to $0. No I/O — the outcome comes from Gamma (settle sweep).

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    #[test]
    fn realized_pnl_win_and_loss() {
        // bought 11 shares UP for $9.90; window resolves UP → +$1.10
        assert_eq!(realized_micro(true, true, 11_000_000, 9_900_000), 1_100_000);
        // resolves DOWN → lose the $9.90 cost
        assert_eq!(realized_micro(false, true, 11_000_000, 9_900_000), -9_900_000);
        // bought DOWN, resolves DOWN → win
        assert_eq!(realized_micro(false, false, 11_000_000, 9_900_000), 1_100_000);
    }
}
```
- [ ] **Step 2: Run → FAIL.** `cargo test -p pm-app -- strategy::btc5m::settle`
- [ ] **Step 3: Implement:**
```rust
/// Realized µUSDC for a resolved binary position. `outcome_up` = did UP win;
/// `bought_up` = did we buy the UP/YES token. `qty_micro` µshares, `cost_micro`
/// µUSDC paid. Win: shares redeem to $1 each → proceeds = qty_micro µUSDC.
pub fn realized_micro(outcome_up: bool, bought_up: bool, qty_micro: i64, cost_micro: i64) -> i64 {
    if outcome_up == bought_up { qty_micro - cost_micro } else { -cost_micro }
}
```
- [ ] **Step 4: Run → PASS; Commit.** `git add crates/app/src/strategy/btc5m/settle.rs crates/app/src/strategy/btc5m/mod.rs && git commit -m "feat(btc5m): pure settlement PnL for resolved positions"`

---

## Task 5: Loop — live entry via venue (⚠ verify interfaces first)

**Files:** Modify `crates/app/src/strategy/btc5m/mod.rs`. Requires a venue injected into `Btc5mStrategy` (Task 7 wires it).

**Design:** In the sample tick, AFTER writing the shadow row (unchanged), if `live` && we have a venue && `!entered_this_window` && risk not halted: compute `z`, `fair_up` (Phase-1 model), the leader + its token id (YES=`clobTokenIds[0]` if `z>0` else NO=`clobTokenIds[1]`), fetch the LEADER token's best ask via `clob::fetch_book_best`, call `decide_entry`. If `Some(entry)`: check `RiskEngine`/daily caps allow it, `venue.ensure_token(...)` to register the dynamic token, build a marketable `Order` (Buy, `entry.limit_px`, `entry.qty`, the leader token), `venue.submit_fak(&order)`, and on a fill send `StoreMsg::Btc5mPositionUpsert` + `StoreMsg::FillSigned` and set `entered_this_window = true`. Reset `entered_this_window` on window rotation.

- [ ] **Step 1: Extract the real order-path interfaces** (dispatch a read-only extraction): `LiveVenue`/`CopyVenue::{submit_fak, ensure_token, best_ask, available_collateral_micro}` exact signatures; `Order::new` args; `SubmitOutcome`/`VenueError` shapes; how `copy.rs::enter()` builds+submits a taker order and records the fill/position + `day_realized`; how `InventoryRisk`/`RiskEngine` gate an entry (the `allow`/`on_fill`/`halted` calls). Confirm the venue trait btc5m should hold (reuse `CopyVenue`, or a minimal `Btc5mVenue`).
- [ ] **Step 2: Write a failing loop test** with a fake venue that records submitted orders: assert that in shadow mode (`live=false`) NO order is submitted (regression), and in a `live=true` test harness with a seeded cheap-late-leader + fake book, exactly ONE FAK is submitted per window on the correct token/side/size, and a `Btc5mPositionUpsert` is emitted. (Model the fake venue on `execution`'s `PaperVenue`/`CopyVenue` test doubles.)
- [ ] **Step 3: Implement** the live-entry branch per the design above, reusing the exact signatures from Step 1. Add `venue: Option<V>`, `entered_this_window: bool`, and the risk handles to `Btc5mStrategy`. Keep the shadow write unchanged and unconditional. **Guard every order behind `live && venue.is_some() && !entered_this_window && risk_ok`.**
- [ ] **Step 4: Grep-guard is now EXPECTED to show `submit_fak`** in `mod.rs` (Phase 2 introduces the order path) — but assert it is reachable ONLY under the `live` guard (a test proving `live=false` submits nothing).
- [ ] **Step 5: Run → PASS.** `cargo test -p pm-app -- strategy::btc5m`
- [ ] **Step 6: Commit.** `feat(btc5m): live micro-taker entry (one cheap-late FAK/window, gated by live+risk)`

---

## Task 6: Settle sweep — resolve closed windows → realized PnL (⚠ verify)

**Files:** Modify `crates/app/src/strategy/btc5m/mod.rs` (+ reuse `gamma.rs`).

**Design:** On a slow sub-cadence (e.g. every 30s), for each open `btc5m_positions` row whose window closed ≥ `settle_delay_secs` ago (~120s, past the settlement finality), query Gamma for that window's `eventMetadata`/`outcomePrices` (reuse the report backfill logic → add `gamma::window_outcome(condition_id or slug) -> Option<bool /*up won*/>`). Compute `settle::realized_micro`, send `StoreMsg::DayRealized{ strategy:"btc5m", delta_micro }` + `StoreMsg::Btc5mPositionClose`, and update the status. Do NOT block the loop; best-effort `try_send`. On-chain redemption of the winning shares is handled by the existing shared-wallet redeem sweep (verify it picks up btc5m winners; if not, add a btc5m redeem via the relayer as a follow-up).

- [ ] **Step 1: Add `gamma::window_outcome`** (pure parse over the `/events?slug=` response → `Some(true)` if `finalPrice ≥ priceToBeat` / `outcomePrices=["1","0"]`, `Some(false)` if down, `None` if not resolved). TDD with the fixtures used in Phase-1 Task 5 (extend with `eventMetadata`/`outcomePrices`).
- [ ] **Step 2: Write a failing settle test:** a fake store with one open btc5m position whose window is closed + a stubbed outcome → asserts the loop emits `DayRealized` with the correct `realized_micro` and `Btc5mPositionClose`.
- [ ] **Step 3: Implement** the settle sweep in the loop (reuse `settle::realized_micro`, `gamma::window_outcome`). Reload open positions on startup (like `copy.rs`'s restart reload) so a restart doesn't drop unsettled positions.
- [ ] **Step 4: Run → PASS; Commit.** `feat(btc5m): settle sweep — resolve closed windows to realized PnL via Gamma outcome`

---

## Task 7: Risk config + venue injection (⚠ verify wiring)

**Files:** Modify `crates/app/src/wiring.rs`, `crates/app/src/main.rs`.

- [ ] **Step 1:** Add a btc5m `InventoryConfig`/`RiskConfig` builder in `wiring.rs` (mirror `inventory_config`/`risk_config` for copy): daily-loss floor = `max_daily_loss_usd`, gross cap = `max_daily_notional_usd`, session-loss halt, sticky kill. (The capital envelope is already carved — Phase 1 Task 7.)
- [ ] **Step 2:** In `main.rs`'s btc5m registration block, when `mm_use_live(args.live, config.strategies.btc5m.live)` is true, build a `LiveVenue` (reuse the copy live-venue construction: secrets, relayer, sigType-3) and inject it + the inventory/risk + `store.path` (for position reload) into `Btc5mStrategy`. When `live=false`, inject `None` (shadow only — unchanged). Mirror the copy block's live/paper branch.
- [ ] **Step 3:** Build + full app test; confirm `enabled=true, live=false` still submits nothing (shadow regression test) and the process starts with `live=true` wired.
- [ ] **Step 4: Commit.** `feat(btc5m): wire live venue + risk for the micro-taker (live-gated)`

---

## Task 8: `pnl` + canary config + full verification

**Files:** Modify `deploy/status.sh`, `mm-live-copy-canary.toml`.

- [ ] **Step 1:** Extend the `status.sh` "BTC 5M BOT" block: query `btc5m_positions` (open count + cost + the live mark via Data-API) and show realized (`day_realized WHERE strategy='btc5m'`) — mirror the copy positions/realized display. (Guard the table like Phase-1: try/except if absent.)
- [ ] **Step 2:** Add the Phase-2 knobs to `mm-live-copy-canary.toml` under `[btc5m]` (`entry_window_secs=20`, `edge_buffer_c=0.02`, `entry_notional_usd=10.0`, `max_daily_notional_usd=200.0`, `max_daily_loss_usd=25.0`) and keep `[strategies.btc5m] live = false`.
- [ ] **Step 3:** `cargo test --workspace` (all green) + `cargo build --release --bin arb` (clean). Confirm `config_smoke` passes with the new knobs.
- [ ] **Step 4: Commit.** `feat(btc5m): Phase-2 pnl positions + canary knobs (live=false); Phase 2 complete`

---

## Self-review checklist (run after drafting the implementation)
- **Spec coverage:** entry (late/|z|/cheap/micro/FAK/one-per-window) ✓; hold-to-resolution + settle ✓; daily-loss kill + session halt ✓; live-gated default-off ✓; realized PnL to `day_realized` + `pnl` ✓.
- **Safety:** the only order path is behind `live && venue && !entered_this_window && risk_ok`; a `live=false` regression test proves silence; FAK-only (no resting).
- **Reuse:** order path = existing `LiveVenue::submit_fak`/`ensure_token`; risk = `InventoryRisk`/`RiskEngine`; outcome = the same Gamma source as the report backfill; positions mirror `copy_positions`.
- **Deferred to Phase 3 (not here):** two-sided maker quoting, liquidity-reward capture, London colocation, and any active flatten/hedge.
