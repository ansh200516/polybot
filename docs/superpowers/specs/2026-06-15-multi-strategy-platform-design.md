# Multi-Strategy Trading Platform — Design

- **Date:** 2026-06-15
- **Status:** Draft (awaiting review)
- **Scope:** The full broad platform (all 5 subsystems), to be built as sequenced,
  test-gated phases within a single master implementation plan. Supersedes the
  engine-core-only spec (`2026-06-15-multi-strategy-engine-core-design.md`), which remains
  the deep-dive for Phase 1.

## 1. Background & motivation

The bot is a single risk-free arbitrage engine. Pure complement / multi-outcome arbitrage
is saturated in 2026 (sub-0.5% after fees, closed in 2–7s by competitors) — hence live
`opps=0`. The chosen direction is to evolve into a **multi-strategy trading platform**:
keep risk-free arb running, and add **risk-taking** strategies — first market-making +
maker-rebate harvesting (CLOB V2 pays makers 0 fees + 20–25% daily rebates), with the
architecture to add more later — running in parallel across many market segments, targeting
broad market coverage.

This is a live-money system. The whole platform is designed here and planned in one master
plan, but it is **built and activated in a safe order**: each phase produces working,
tested software; every live / risk-taking capability ships **behind a default-off config
flag** and is **paper-validated before any live flag is set**.

## 2. Goals / non-goals

**Goals**
- Run N strategies in parallel over shared ingestion, each with its own capital + risk +
  accounting (Phase 1).
- A reusable inventory-risk framework for risk-taking strategies (Phase 2).
- A resting maker-order execution path: postOnly GTC/GTD place/cancel/replace + user fills
  (Phase 3).
- A market-making strategy with a paper maker-fill simulator; paper → tiny live (Phase 4).
- Market segmentation + universe scaling toward broad coverage (Phase 5).
- Arb stays byte-identical throughout; full existing test suite stays green at every phase.

**Non-goals (explicit, this platform iteration)**
- Directional / statistical-arb / mean-reversion strategies (the harness will admit them
  later, but none are designed or built here).
- Cross-platform arbitrage (Kalshi/etc.) — a future strategy behind the same harness.
- ML/NLP signal generation.
- Auto-activation of live trading: every live capability is operator-gated.

## 3. Cross-cutting safety model

Applies to all phases:
- **Default-off flags.** Every new live / risk-taking capability is gated by a config flag
  defaulting to off (the established pattern: `lp.nonexhaustive_negrisk_worlds`, the
  pure-buy live gate). Merging a phase changes nothing until explicitly enabled.
- **Paper-first.** Risk-taking strategies run against a fill simulator until an operator
  sets their live flag, types the live confirmation phrase, and the canary caps apply.
- **Per-strategy isolation** (Phase 1): a strategy halt or panic cannot affect other
  strategies or abort the process.
- **Global + per-strategy kill/pause**; the existing kill switch still stops everything.
- **Plausibility guard** (already shipped): the `edges.max_edge_bps` ceiling suppresses
  implausible opportunities from any strategy.
- **Capital allocator:** Σ per-strategy capital ≤ bankroll, enforced fatally at startup.

## 4. Phase 1 — Multi-strategy engine core

The harness everything plugs into. (Full detail in the engine-core spec.)

- **`Strategy` boundary:** a self-contained unit owning its market-data access (an optional
  per-supervisor inline `on_apply` hook and/or its own async loop reading via
  `BookFetcher`), its intent→execution backend, and its own `PositionBook` + `RiskEngine`
  + capital envelope + `StrategyId`.
- **`StrategyHost`:** allocates capital, installs inline hooks, spawns strategy loops,
  routes per-strategy pause/kill + global kill, aggregates per-strategy status into the
  publisher (TUI header = Σ per-strategy equity + per-strategy breakdown).
- **Arb = strategy #1**, byte-identical; only ownership of its `PositionBook`/`RiskEngine`
  moves out of `Coordinator` into the strategy.
- **Stub strategy #2** (no-op heartbeat) proves parallelism, isolation, per-strategy
  accounting, pause/kill, aggregation.
- **Store:** rows (`opportunities`/`fills`/`orders`/`pnl_snapshots`) gain a backward-compatible
  `strategy` column (default `"arb"`).

## 5. Phase 2 — Inventory-risk framework

A new per-strategy risk module for **inventory-bearing** strategies (the existing
`RiskEngine` stays for risk-free baskets; arb is unchanged).

**State (per strategy):** signed net inventory per token (µshares); cost basis; live
mark-to-market via `BookFetcher` (bid for conservative, mid for the halt feed, reusing the
existing mid-clamp); realized + unrealized P&L.

**Caps (config, per strategy; conservative defaults, all gating-relevant):**
- `max_inventory_usd` — per-market net exposure cap.
- `max_gross_inventory_usd` — total across markets.
- `inventory_stop_loss_usd` — MtM loss that latches a strategy halt + flatten.
- `daily_loss_usd` — per-strategy daily realized+unrealized floor.

**Interface (`pm-risk::inventory`):**
```rust
pub enum QuoteVerdict { Approve, Reject(InvReason) }
pub struct InventoryStatus { pub net_by_token: /* ... */, pub mtm_pnl: Usdc, pub halted: Option<InvHalt> }

impl InventoryRisk {
    fn on_fill(&mut self, token: TokenId, signed_qty: Qty, cash: Usdc);
    fn check_quote(&self, intended: &MakerOrder) -> QuoteVerdict; // rejects cap-breaching quotes
    fn mark(&mut self, marks: &Marks) -> InventoryStatus;          // computes MtM, may latch halt
    fn flatten_directive(&self) -> Option<Flatten>;                // cancel quotes (+ optional unwind)
}
```

**Triggers:** on each mark cycle, MtM loss ≤ −`inventory_stop_loss_usd` latches an
`InvHalt` (sticky, like `SessionLoss`) → strategy pulls all quotes and stops quoting. A
volatility signal (mid move > N ticks within a window, or spread beyond cap) returns a
"pull quotes" hint the strategy honors. All caps default conservative; inventory risk is
inert for strategies that don't opt in.

## 6. Phase 3 — Resting maker-order execution path

Today `pm-execution` only sends taker marketable BUYs in single-in-flight baskets. This
adds resting maker orders.

**Venue extension (`ExecutionVenue`):**
```rust
pub struct MakerOrder { pub token: TokenId, pub side: Side, pub price: Px, pub size: Qty,
    pub order_type: OrderType /* GTC | GTD(expiry_ms) */, pub post_only: bool /* = true */ }

trait MakerVenue {
    async fn place(&mut self, o: &MakerOrder) -> Result<OrderId, VenueError>;
    async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError>;
    async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError>;
    async fn open_orders(&self) -> Result<Vec<OpenOrder>, VenueError>;
}
```
- **CLOB V2:** postOnly signed V2 order (reuses the existing V2 EIP-712 signing already in
  `pm-execution`); makers pay 0 fees; `orderType` GTC/GTD.
- **`QuoteManager`:** tracks resting orders per (strategy, token); issues place/cancel/replace
  with a cancel/replace cadence; respects REST rate limits; idempotent on reconnect.
- **User fills:** subscribe to the CLOB user channel (WS) — fallback REST poll of
  `/orders`/`/trades` — and emit fill events to the owning strategy + inventory risk + store
  (tagged). Reconciles open orders + inventory on startup (extends existing reconciliation).
- **Paper:** `PaperMakerVenue` backed by the Phase-4 fill simulator.
- **Live default-off:** only the MM strategy uses this path, and it is itself flag-gated +
  paper-first.

## 7. Phase 4 — Market-making strategy + paper maker-fill simulator

**`MmStrategy: Strategy`** runs a quote loop per target market on `quote_refresh_ms`:
- **Fair value** = mid from the book; **bid** = fair − spread/2, **ask** = fair + spread/2,
  with `spread_bps` (≥ venue tick) configurable.
- **Sizing** = f(allocated capital, current inventory), clamped by `InventoryRisk::check_quote`.
- **Inventory skew:** shift both quotes against inventory (long → lower quotes to offload).
- **Cancel/replace** stale quotes on mid move > threshold or on the refresh timer; **pull**
  (cancel without replace) on the inventory-risk volatility hint; **GTD** expiry before
  known events.
- **Economics:** spread capture + maker rebates − adverse selection; rebates accrued and
  displayed.

**Paper maker-fill simulator:** a resting bid at price `p` fills (fully/partially) when the
live book's best ask ≤ `p`; conservative (no queue jumping); models adverse selection by
preferentially filling when the mid is moving against the quote. Validates the full loop —
quote → fill → inventory → MtM → rebate accrual → risk stops — with zero money.

**Gating:** `[strategies.mm] enabled=false` (default). Paper sim until a separate `live`
flag + the existing live confirmation + canary caps (tiny capital). Plausibility +
inventory guards apply.

## 8. Phase 5 — Segmentation + universe scaling

- **Classification:** Gamma category + computed liquidity (book depth / volume) + volatility
  (recent price stddev) → a `MarketSegment` tag per market.
- **Per-segment assignment:** config maps segments → {strategies, params}. E.g., arb on all;
  MM only on segments classified liquid + low-volatility; wider `spread_bps` on higher-vol
  segments; fee-free Geopolitics excluded from rebate-driven MM.
- **Scaling:** raise `max_markets` toward broad coverage; scale supervisors (WS sharding by
  `ws_chunk_size`, already chunked); manage REST rate limits during sync; bound book memory.
  Segment **prioritization** so the most profitable segments are covered first when
  resource-bound.
- **Default:** conservative `max_markets` until enabled; segmentation config opt-in.

## 9. Testing strategy

- **Regression:** full workspace suite + clippy green at **every** phase; arb byte-identical.
- **Phase 1:** parallelism, fault isolation, per-strategy accounting/risk, pause/kill, aggregation.
- **Phase 2:** inventory cap rejection, stop-loss latch + flatten directive, MtM math, volatility hint.
- **Phase 3:** place/cancel/replace + fills against fixtures (mirror existing sign-vector tests); reconciliation.
- **Phase 4:** the fill simulator; full MM loop in paper; rebate accrual; quote skew/pull; zero live fills in paper.
- **Phase 5:** classification, per-segment routing, scaling under simulated large universe.
- **Integration:** arb + MM in paper, in parallel, isolated, aggregated P&L correct.

## 10. Risks & open questions

- **Adverse selection is real loss.** MM is profitable on average, not per-fill; the
  inventory stop-loss + paper validation + tiny live cap bound the downside.
- **CLOB V2 user-fills channel** exact API (WS schema / auth) to be confirmed against live
  docs during Phase 3; REST polling is the fallback.
- **Maker-fill simulator fidelity:** can't model queue position; treated as a plumbing
  validator, not a profitability oracle — real profitability only shows in tiny live.
- **Publisher refactor** for per-strategy aggregation touches `publisher.rs`/`tui`; kept minimal.
- **Hot-path impact** of the Phase-1 hook refactor on arb — guarded by the regression suite
  + the criterion hot-path benchmark.

## 11. Acceptance criteria

- Workspace tests + clippy green at every phase; arb detection/dispatch byte-identical.
- Each live/risk-taking capability lands behind a default-off flag and is paper-proven first.
- MM validated end-to-end in paper (quote→fill→inventory→MtM→rebate→stop) before any live flag.
- No capability auto-activates live; tiny-capital canary + confirmation required.
