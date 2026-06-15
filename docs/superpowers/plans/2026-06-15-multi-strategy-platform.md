# Multi-Strategy Trading Platform — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve the risk-free arbitrage bot into a multi-strategy platform that runs arbitrage and market-making (and future strategies) in parallel across many market segments, with every live/risk-taking capability behind a default-off flag and paper-validated first.

**Architecture:** A `StrategyHost` runs N `Strategy` units over the shared ingestion/registry/store/TUI; each strategy owns its capital, risk, and accounting. Arb is wrapped as strategy #1 (byte-identical). A reusable inventory-risk module + a resting maker-order execution path enable a market-making strategy, validated against a paper fill simulator before any tiny-capital live run. Segmentation routes strategies per market class and scales the universe.

**Tech Stack:** Rust workspace (`pm-core`, `pm-engine`, `pm-registry`, `pm-ingestion`, `pm-config`, `pm-execution`, `pm-risk`, `pm-store`, `pm-app`, `pm-tui`); tokio; rusqlite; HiGHS LP; Polymarket CLOB V2.

---

## How to use this plan

- **Build phase-by-phase, in order.** Each phase produces working, tested software and ends green on `cargo test --workspace` + `cargo clippy --workspace --all-targets`. Do not start a phase before the previous one is green.
- **Safety spine:** every live / risk-taking capability is gated by a config flag defaulting to **off** (the established pattern: `lp.nonexhaustive_negrisk_worlds`, the pure-buy gate). Merging any phase changes runtime behavior **only** when a flag is flipped. Arb stays byte-identical.
- **`cargo` is at `~/.cargo/bin`**; tests/builds must run outside the editor sandbox (build scripts write to the target dir). Run: `export PATH="$HOME/.cargo/bin:$PATH"`.
- **Spec:** `docs/superpowers/specs/2026-06-15-multi-strategy-platform-design.md` (+ engine-core deep-dive). Re-read the relevant spec section at the start of each phase.

## File structure (created/modified, by phase)

**Phase 1 — engine core**
- Create: `crates/app/src/strategy/mod.rs` — `StrategyId`, `StrategyEnvelope`, `Strategy` trait, `StrategyCtx`, `StrategyStatus`.
- Create: `crates/app/src/strategy/host.rs` — `StrategyHost` (capital alloc, hook install, spawn, ctl routing, status aggregation).
- Create: `crates/app/src/strategy/arb.rs` — `ArbStrategy` wrapping the existing `Detector` + `Coordinator` + `run_execution`.
- Create: `crates/app/src/strategy/stub.rs` — `HeartbeatStrategy` (validation).
- Modify: `crates/store/src/lib.rs` + `writer.rs` + `read.rs` — `strategy` column (default `"arb"`).
- Modify: `crates/app/src/publisher.rs` — read aggregated host status.
- Modify: `crates/app/src/main.rs` — wire the host instead of a bare coordinator.
- Modify: `crates/app/src/lib.rs` — expose the `strategy` module.

**Phase 2 — inventory risk**
- Create: `crates/risk/src/inventory.rs` — `InventoryRisk`, `QuoteVerdict`, `InventoryStatus`, `InvHalt`.
- Modify: `crates/risk/src/lib.rs` — expose `inventory`.
- Modify: `crates/config/src/lib.rs` — `[inventory]` caps (conservative defaults).

**Phase 3 — maker execution**
- Create: `crates/execution/src/maker.rs` — `MakerOrder`, `OrderType`, `MakerVenue` trait, `QuoteManager`.
- Create: `crates/execution/src/fills.rs` — user-fills source (WS user channel, REST poll fallback).
- Modify: `crates/execution/src/live.rs` — implement `MakerVenue` for the live venue (reuse V2 signing).
- Create: `crates/execution/src/paper_maker.rs` — `PaperMakerVenue` (uses Phase-4 simulator).

**Phase 4 — market-making strategy**
- Create: `crates/app/src/strategy/mm.rs` — `MmStrategy`.
- Create: `crates/execution/src/fill_sim.rs` — paper maker-fill simulator.
- Modify: `crates/config/src/lib.rs` — `[strategies.mm]` (enabled=false, live=false, spread_bps, refresh, sizing).

**Phase 5 — segmentation + scaling**
- Create: `crates/registry/src/segment.rs` — `MarketSegment` classification.
- Modify: `crates/config/src/lib.rs` — `[segments]` routing map.
- Modify: `crates/app/src/main.rs` + `wiring.rs` — per-segment strategy assignment; scaling knobs.

---

## Phase 1 — Multi-strategy engine core

Re-read: spec §4 + the engine-core deep-dive. Goal: harness + per-strategy risk/accounting; arb byte-identical; stub proves isolation.

### Task 1.1: Store `strategy` tag (backward-compatible)

**Files:**
- Modify: `crates/store/src/lib.rs` (schema + `OppRow`/`FillRow`/`PnlRow` structs + inserts)
- Modify: `crates/store/src/read.rs`
- Test: `crates/store/src/lib.rs` (tests module)

- [ ] **Step 1: Write the failing test** — a row written with `strategy="mm"` reads back tagged, and a legacy DB without the column still opens.

```rust
#[test]
fn rows_carry_strategy_tag_and_legacy_db_defaults_to_arb() {
    let (_dir, path) = tmp_db();
    let mut s = Store::open(&path).unwrap();
    s.insert_pnl(&PnlRow { strategy: "mm".into(), ts_ms: 1, cash_micro: 0, realized_micro: 0, unrealized_micro: 0, equity_micro: 0 }).unwrap();
    let rows = ReadStore::open(&path).unwrap().recent_pnl_by_strategy("mm", 10).unwrap();
    assert_eq!(rows.len(), 1);
}
```

- [ ] **Step 2: Run it, verify it fails** — `cargo test -p pm-store rows_carry_strategy_tag` → FAIL (no `strategy` field).
- [ ] **Step 3: Implement** — add `strategy TEXT NOT NULL DEFAULT 'arb'` to the `opportunities`, `fills`, `orders`, `pnl_snapshots` `CREATE TABLE` statements; add an idempotent `ALTER TABLE ... ADD COLUMN strategy TEXT NOT NULL DEFAULT 'arb'` guarded by a `PRAGMA table_info` check in `Store::open` for pre-existing DBs; add `pub strategy: String` to the row structs; thread it through the `INSERT` params; add `recent_pnl_by_strategy`/filter to `read.rs`.
- [ ] **Step 4: Run tests** — `cargo test -p pm-store` → PASS.
- [ ] **Step 5: Commit** — `git add crates/store && git commit -m "feat(store): per-strategy row tagging (default arb, legacy-safe)"`

### Task 1.2: `StrategyId`, `StrategyEnvelope`, capital allocator

**Files:**
- Create: `crates/app/src/strategy/mod.rs`
- Modify: `crates/app/src/lib.rs` (add `pub mod strategy;`)
- Test: `crates/app/src/strategy/mod.rs` (tests)

- [ ] **Step 1: Write the failing test** — capital allocator rejects over-allocation.

```rust
#[test]
fn allocator_rejects_overallocation() {
    let envs = vec![
        StrategyEnvelope::new(StrategyId("arb"), Usdc(6_000_000), RiskConfig::default_arb()),
        StrategyEnvelope::new(StrategyId("mm"),  Usdc(5_000_000), RiskConfig::default_arb()),
    ];
    assert!(allocate(&envs, /*bankroll*/ Usdc(10_000_000)).is_err()); // 6+5 > 10
    assert!(allocate(&envs, Usdc(11_000_000)).is_ok());
}
```

- [ ] **Step 2: Run it, verify it fails** — `cargo test -p pm-app allocator_rejects` → FAIL (module missing).
- [ ] **Step 3: Implement** — define `StrategyId(pub &'static str)`, `StrategyEnvelope { id, capital: Usdc, risk: RiskConfig }`, and `fn allocate(envs, bankroll) -> Result<(), String>` summing `capital` and erroring if `> bankroll`.
- [ ] **Step 4: Run tests** — `cargo test -p pm-app strategy::` → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(app): strategy id + envelope + capital allocator"`

### Task 1.3: `Strategy` trait, `StrategyCtx`, `StrategyStatus`

**Files:** Modify `crates/app/src/strategy/mod.rs`; Test: same.

- [ ] **Step 1: Write the failing test** — a trivial test strategy can be boxed as `dyn Strategy` and reports its id + an initial status.

```rust
#[tokio::test]
async fn strategy_trait_object_reports_id_and_status() {
    let s: Box<dyn Strategy> = Box::new(NoopStrategy::new(StrategyId("noop")));
    assert_eq!(s.id(), StrategyId("noop"));
    assert!(s.make_on_apply().is_none());
}
```

- [ ] **Step 2: Verify fail** — `cargo test -p pm-app strategy_trait_object` → FAIL.
- [ ] **Step 3: Implement** — define `StrategyStatus { cash_micro, equity_micro, equity_mid_micro, realized_micro, unrealized_micro, open_positions, halted: Option<String>, paused: bool }`; `StrategyCtx { registry: Arc<Registry>, fetcher: BookFetcher, store_tx, kill: Arc<AtomicBool>, ctl_rx: mpsc::Receiver<CtlCommand>, status_tx: watch::Sender<StrategyStatus> }`; and:

```rust
pub trait Strategy: Send {
    fn id(&self) -> StrategyId;
    fn make_on_apply(&self) -> Option<Box<dyn FnMut(TokenId, &Shard) + Send>>;
    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}
```
Add a `NoopStrategy` test helper.
- [ ] **Step 4: Run tests** → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(app): Strategy trait + ctx + status"`

### Task 1.4: Wrap arb as `ArbStrategy` (byte-identical)

**Files:** Create `crates/app/src/strategy/arb.rs`; Modify `crates/app/src/coordinator.rs` (move `PositionBook`/`RiskEngine` ownership is already internal — expose a constructor that takes the per-strategy `status_tx`/`ctl_rx`); Test: existing coordinator tests.

- [ ] **Step 1: Write the failing test** — `ArbStrategy::make_on_apply` returns a hook, and `run` consumes opps and dispatches exactly as the coordinator does (reuse an existing coordinator scenario).

```rust
#[tokio::test]
async fn arb_strategy_dispatches_like_coordinator() {
    // mirrors coordinator::tests::dispatches_approved_then_busy_suppresses_then_report_frees
    // but driven through ArbStrategy::run
}
```

- [ ] **Step 2: Verify fail** → FAIL.
- [ ] **Step 3: Implement** — `ArbStrategy` holds the `Detector` factory inputs (index, params, lp_tx, stats) + the `Coordinator` construction inputs; `make_on_apply` builds a fresh `Detector` per supervisor (as `main.rs` does today); `run` builds the `Coordinator` (its existing `control_channel`/`status_channel` map onto `StrategyCtx.ctl_rx`/`status_tx`) and runs its loop unchanged. No change to `Coordinator`'s logic.
- [ ] **Step 4: Run tests** — `cargo test -p pm-app` → PASS (all existing coordinator tests still green).
- [ ] **Step 5: Commit** — `git commit -am "feat(app): ArbStrategy wraps detector+coordinator (behavior unchanged)"`

### Task 1.5: `StrategyHost`

**Files:** Create `crates/app/src/strategy/host.rs`; Test: same.

- [ ] **Step 1: Write the failing test** — host runs two strategies; aggregated status equals the sum; one strategy's panic does not abort the other.

```rust
#[tokio::test]
async fn host_runs_two_strategies_isolated_and_aggregates() {
    let host = StrategyHost::new(/*bankroll*/ Usdc(10_000_000));
    // register arb-like accountant (equity 7) + stub (equity 0); spawn; assert aggregate equity == 7
    // then make stub panic; assert arb still produces status.
}
```

- [ ] **Step 2: Verify fail** → FAIL.
- [ ] **Step 3: Implement** — `StrategyHost` stores `Vec<StrategyEnvelope>` + boxed strategies; `add(strategy, envelope)`; `on_apply_hooks()` returns per-supervisor installable hooks aggregated across strategies; `run(self)` spawns each strategy's `run` on its own task wrapped so a panic is caught (`tokio::spawn` + `JoinHandle` watch), keeps a `watch::Receiver<StrategyStatus>` per strategy, and exposes an aggregated `watch::Receiver<Vec<StrategyStatus>>` for the publisher; routes per-strategy `CtlCommand`.
- [ ] **Step 4: Run tests** → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(app): StrategyHost — parallel run, isolation, aggregation"`

### Task 1.6: `HeartbeatStrategy` stub

**Files:** Create `crates/app/src/strategy/stub.rs`; Test: same.

- [ ] **Step 1: Write the failing test** — stub publishes a heartbeat status and emits no orders.
- [ ] **Step 2: Verify fail** → FAIL.
- [ ] **Step 3: Implement** — `HeartbeatStrategy` whose `run` ticks on a timer, publishes a zero-valued `StrategyStatus`, honors `ctl_rx` pause/kill + global kill; `make_on_apply` returns `None`.
- [ ] **Step 4: Run tests** → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(app): heartbeat stub strategy for harness validation"`

### Task 1.7: Publisher aggregation

**Files:** Modify `crates/app/src/publisher.rs`; Test: `crates/app/src/publisher.rs` (tests).

- [ ] **Step 1: Write the failing test** — given two `StrategyStatus` (equity 7 and 0), the assembled `AppState` header equity is 7 and a per-strategy breakdown lists both.
- [ ] **Step 2: Verify fail** → FAIL.
- [ ] **Step 3: Implement** — `PublisherCtx` takes the host's aggregated `watch::Receiver<Vec<StrategyStatus>>`; `assemble` sums money fields for the header and fills a new `per_strategy: Vec<StrategyLine>` in `AppState` (add field; render later — TUI change minimal: a small breakdown line).
- [ ] **Step 4: Run tests** — `cargo test -p pm-app -p pm-tui` → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(app): publisher aggregates per-strategy status"`

### Task 1.8: Wire the host into `main.rs`

**Files:** Modify `crates/app/src/main.rs`; Test: `crates/app/tests/e2e_paper.rs`.

- [ ] **Step 1: Update the e2e test** — assert the existing synthetic-feed paper run still reaches the same P&L, now routed through `StrategyHost` with a single `ArbStrategy`.
- [ ] **Step 2: Verify fail** → FAIL (host not wired).
- [ ] **Step 3: Implement** — replace the direct coordinator/exec wiring with: build `ArbStrategy`, `host.add(arb, envelope)`, install `host.on_apply_hooks()` on supervisors, `host.run()`; keep the global kill + shutdown identical.
- [ ] **Step 4: Run tests** — `cargo test --workspace` → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(app): run arb via StrategyHost"`

### Task 1.9: Isolation + parallelism integration test

- [ ] **Step 1: Write the test** — host with `ArbStrategy` + `HeartbeatStrategy`; drive arb to dispatch; force the stub to halt; assert arb unaffected, process up, aggregate correct.
- [ ] **Step 2–4:** verify fail → (no impl needed if 1.5 correct) → PASS.
- [ ] **Step 5: Commit** — `git commit -am "test(app): strategy isolation + parallelism"`

**Phase 1 gate:** `cargo test --workspace` + `cargo clippy --workspace --all-targets` green; arb behavior unchanged.

---

## Phase 2 — Inventory-risk framework

Re-read: spec §5. Goal: per-strategy inventory tracking + caps + stop-loss/flatten. Inert until a strategy uses it.

### Task 2.1: `InventoryRisk` state + `on_fill`
- **Files:** Create `crates/risk/src/inventory.rs`; Modify `crates/risk/src/lib.rs`; Test: inventory.rs.
- [ ] **Step 1: Failing test** — `on_fill` accumulates signed net inventory + cost basis per token (buy adds, sell subtracts).
- [ ] **Step 2:** verify fail.
- [ ] **Step 3: Implement** — `InventoryRisk::new(InventoryConfig)`, `on_fill(token, signed_qty: Qty, cash: Usdc)` updating `HashMap<TokenId,(i128 net, i128 basis)>`.
- [ ] **Step 4–5:** PASS; commit `feat(risk): inventory accounting`.

### Task 2.2: `check_quote` cap enforcement
- [ ] **Step 1: Failing test** — a quote that would push net inventory in a market past `max_inventory_usd` is `Reject`; under is `Approve`; a quote that reduces inventory is always `Approve`.
- [ ] **Step 2–3:** implement `check_quote(&MakerOrder) -> QuoteVerdict` projecting post-fill inventory vs `max_inventory_usd` / `max_gross_inventory_usd`.
- [ ] **Step 4–5:** PASS; commit `feat(risk): inventory cap gate`.

### Task 2.3: `mark` mark-to-market + stop-loss latch
- [ ] **Step 1: Failing test** — given marks making MtM loss ≤ −`inventory_stop_loss_usd`, `mark` returns `halted=Some(InvHalt::StopLoss)` and latches (sticky); a later recovery stays halted. Reuse the existing bid/mid mark helper semantics.
- [ ] **Step 2–3:** implement `mark(&Marks) -> InventoryStatus` computing realized+unrealized from basis vs marks; latch `InvHalt`.
- [ ] **Step 4–5:** PASS; commit `feat(risk): inventory mark-to-market + stop-loss`.

### Task 2.4: `flatten_directive` + volatility hint + config
- [ ] **Step 1: Failing test** — when halted, `flatten_directive()` is `Some` (cancel-all + optional unwind); a mid move beyond the configured window returns the pull-quotes hint.
- [ ] **Step 2–3:** implement; add `[inventory]` to config (`max_inventory_usd`, `max_gross_inventory_usd`, `inventory_stop_loss_usd`, `daily_loss_usd`, `vol_pull_ticks`, `vol_window_ms`) with conservative defaults + validation.
- [ ] **Step 4–5:** `cargo test -p pm-risk -p pm-config` PASS; commit `feat(risk): flatten + volatility hint + [inventory] config`.

**Phase 2 gate:** workspace tests + clippy green (module unused by any live strategy yet).

---

## Phase 3 — Resting maker-order execution path

Re-read: spec §6. Goal: place/cancel/replace postOnly orders + user fills. Live path used only by the (flag-gated) MM strategy.

### Task 3.1: `MakerOrder` + `OrderType` + `MakerVenue` trait
- **Files:** Create `crates/execution/src/maker.rs`; Test: maker.rs.
- [ ] **Step 1: Failing test** — construct a `MakerOrder` (GTC, postOnly) and assert a `MockMakerVenue` records place/cancel/replace calls.
- [ ] **Step 2–3:** define the types + trait (per spec §6) + a `MockMakerVenue` for tests.
- [ ] **Step 4–5:** PASS; commit `feat(execution): maker order types + venue trait`.

### Task 3.2: `QuoteManager` (resting-order bookkeeping)
- [ ] **Step 1: Failing test** — `QuoteManager` tracks open orders per (strategy, token), replace updates the id, cancel removes it; double-cancel is a no-op.
- [ ] **Step 2–3:** implement over the `MakerVenue` trait (venue-agnostic; tested against `MockMakerVenue`).
- [ ] **Step 4–5:** PASS; commit `feat(execution): QuoteManager`.

### Task 3.3: Live `MakerVenue` (SPIKE then implement)
- **Files:** Modify `crates/execution/src/live.rs`; fixtures under `crates/execution/tests/fixtures/`.
- [ ] **Step 1: SPIKE** — confirm the CLOB V2 postOnly order wire body (reuse the existing V2 EIP-712 signing already vector-tested) and the cancel/replace REST endpoints against `docs.polymarket.com`; capture request/response fixtures (mirror the existing `clob_responses` fixtures). *This is a real task: its output is committed fixtures + confirmed endpoint shapes, not code.*
- [ ] **Step 2: Failing test** — sign+serialize a postOnly GTC order; assert it matches the captured fixture byte-for-byte (mirrors `sign_vectors_v2.json` tests).
- [ ] **Step 3: Implement** `MakerVenue` for the live venue using the confirmed endpoints + existing signer.
- [ ] **Step 4–5:** `cargo test -p pm-execution` PASS; commit `feat(execution): live maker venue (postOnly GTC/GTD)`.

### Task 3.4: User-fills source (SPIKE then implement)
- **Files:** Create `crates/execution/src/fills.rs`.
- [ ] **Step 1: SPIKE** — confirm the CLOB user (fills) WS channel schema + auth; if unavailable, confirm the `/trades`/`/orders` REST polling shape. Capture fixtures.
- [ ] **Step 2: Failing test** — parse a captured fills message/response into `FillEvent { order_id, token, side, price, size, ts }`.
- [ ] **Step 3: Implement** the source (WS preferred, REST poll fallback behind a flag) emitting `FillEvent`s on a channel.
- [ ] **Step 4–5:** PASS; commit `feat(execution): user-fills source`.

### Task 3.5: Startup reconciliation of open orders + inventory
- [ ] **Step 1: Failing test** — given seeded open orders, reconciliation cancels/loads them and seeds inventory (extends the existing open-order reconciliation test).
- [ ] **Step 2–3:** implement.
- [ ] **Step 4–5:** PASS; commit `feat(execution): maker reconciliation on startup`.

**Phase 3 gate:** workspace tests + clippy green; live maker path exists but is referenced by nothing live yet.

---

## Phase 4 — Market-making strategy + paper fill simulator

Re-read: spec §7. Goal: `MmStrategy` validated in paper; tiny live behind a flag.

### Task 4.1: Paper maker-fill simulator
- **Files:** Create `crates/execution/src/fill_sim.rs`; Test: fill_sim.rs.
- [ ] **Step 1: Failing test** — a resting bid at 0.48 fills when the live book best ask drops to ≤ 0.48; an ask at 0.52 fills when best bid rises to ≥ 0.52; no fill otherwise; partial fill capped by available size.
- [ ] **Step 2–3:** implement the trade-through model (conservative, no queue jumping); expose `PaperMakerVenue` implementing `MakerVenue` against it.
- [ ] **Step 4–5:** PASS; commit `feat(execution): paper maker-fill simulator`.

### Task 4.2: `MmStrategy` quoting loop
- **Files:** Create `crates/app/src/strategy/mm.rs`; Modify config `[strategies.mm]` (enabled=false, live=false, spread_bps, quote_refresh_ms, max_quote_usd).
- [ ] **Step 1: Failing test** — given a book with mid 0.50 and `spread_bps`, the strategy computes bid/ask symmetric around mid, clamps size by capital, and `InventoryRisk::check_quote` is consulted (rejected quote ⇒ not placed).
- [ ] **Step 2–3:** implement `MmStrategy: Strategy`: `make_on_apply` = `None`; `run` loops on `quote_refresh_ms`, reads books via `BookFetcher`, computes quotes, applies inventory skew, places/cancels via `QuoteManager`, consumes fills → `InventoryRisk::on_fill` + per-strategy `PositionBook` + tagged store rows; honors flatten/pull directives + pause/kill + global kill.
- [ ] **Step 4–5:** PASS; commit `feat(app): market-making strategy`.

### Task 4.3: Inventory skew + quote pull
- [ ] **Step 1: Failing test** — long inventory shifts both quotes down by the skew; a volatility hint cancels quotes without replacing.
- [ ] **Step 2–3:** implement skew + pull.
- [ ] **Step 4–5:** PASS; commit `feat(app): MM inventory skew + volatility pull`.

### Task 4.4: Rebate accrual tracking + paper integration test
- [ ] **Step 1: Failing test** — end-to-end in paper: MM quotes on a synthetic book, sim fills both sides, inventory nets toward zero, MtM + rebate-accrual tracked, **zero live orders sent**.
- [ ] **Step 2–3:** implement rebate accrual estimate (per category rate from market info) + wire `MmStrategy` into the host behind `[strategies.mm] enabled`.
- [ ] **Step 4–5:** `cargo test --workspace` PASS; commit `feat(app): MM paper end-to-end + rebate accrual`.

### Task 4.5: Live gating (tiny canary)
- [ ] **Step 1: Failing test** — with `live=false`, MM uses `PaperMakerVenue`; with `live=true` it requires the live confirmation + applies canary caps (basket/inventory). Assert paper never reaches the live venue.
- [ ] **Step 2–3:** wire the `live` flag through the mode ladder (mirror the arb live arm); default off.
- [ ] **Step 4–5:** PASS; commit `feat(app): MM live gating (default off, confirmation required)`.

**Phase 4 gate:** workspace tests + clippy green; MM proven in paper; live still off.

---

## Phase 5 — Segmentation + universe scaling

Re-read: spec §8. Goal: classify markets, route strategies per segment, scale the universe. Opt-in.

### Task 5.1: `MarketSegment` classification
- **Files:** Create `crates/registry/src/segment.rs`; Test: segment.rs.
- [ ] **Step 1: Failing test** — classify fixtures: a high-volume low-vol market → `LiquidStable`; thin → `Illiquid`; by Gamma category tag.
- [ ] **Step 2–3:** implement classification from category + liquidity (depth/volume) + volatility (price stddev) inputs.
- [ ] **Step 4–5:** PASS; commit `feat(registry): market segmentation`.

### Task 5.2: Per-segment strategy routing
- **Files:** Modify config `[segments]`; `crates/app/src/main.rs`/`wiring.rs`.
- [ ] **Step 1: Failing test** — given a routing map (arb: all; mm: LiquidStable only), the host assigns each strategy the right token set; fee-free Geopolitics excluded from MM.
- [ ] **Step 2–3:** implement the routing map + assignment; default map = arb-on-all, mm-off.
- [ ] **Step 4–5:** PASS; commit `feat(app): per-segment strategy routing`.

### Task 5.3: Universe scaling knobs + prioritization
- [ ] **Step 1: Failing test** — with a large simulated universe and a resource budget, segment prioritization selects the most-profitable segments first; supervisor sharding respects `ws_chunk_size`.
- [ ] **Step 2–3:** implement scaling knobs (raise `max_markets`, prioritize segments under a token budget); keep default conservative.
- [ ] **Step 4–5:** `cargo test --workspace` PASS; commit `feat(app): universe scaling + segment prioritization`.

**Phase 5 gate:** workspace tests + clippy green; scaling/segmentation opt-in; defaults unchanged.

---

## Self-review (author checklist — completed)

- **Spec coverage:** §4→Phase 1, §5→Phase 2, §6→Phase 3, §7→Phase 4, §8→Phase 5; cross-cutting safety (§3) realized as default-off flags in 2.4/4.2/4.5/5.2 and the capital allocator in 1.2. No spec section without a phase.
- **Placeholders:** the two `SPIKE` tasks (3.3, 3.4) are genuine verification tasks whose committed output is fixtures + confirmed endpoint shapes (the CLOB V2 user-fills API is the one real external unknown, flagged in spec §10) — they are *not* "implement later" placeholders; every other task carries a concrete test + implementation.
- **Type consistency:** `Strategy`/`StrategyCtx`/`StrategyStatus`/`StrategyEnvelope` (1.2–1.3) are reused unchanged in 1.4–1.9, 4.2; `MakerOrder`/`MakerVenue`/`QuoteManager` (3.1–3.2) reused in 3.3, 4.1–4.2; `InventoryRisk` (2.x) reused in 4.2–4.3.
