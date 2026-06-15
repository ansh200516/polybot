# Multi-Strategy Engine Core — Design

- **Date:** 2026-06-15
- **Status:** Draft (awaiting review)
- **Sub-project:** #1 of the broad multi-strategy platform (see Roadmap)

## 1. Background & motivation

The bot today is a single risk-free arbitrage engine. Pure complement / multi-outcome
arbitrage is a saturated niche in 2026: spreads rarely exceed ~2%, drop under ~0.5%
after fees, and competing bots close them in 2–7s. That is why live sessions sit at
`opps=0` — it is the market, not a bug.

The chosen growth direction is to evolve into a **multi-strategy trading platform** that
keeps the risk-free arb running and adds **risk-taking** strategies — first
market-making + maker-rebate harvesting (CLOB V2 pays makers 0 fees + 20–25% daily
rebates), later directional/stat-arb — running in parallel across many market segments.

That platform ("approach B", engine-first/broad) is ~5 dependency-ordered subsystems and
cannot be specced or built as one unit on a live-money bot. This spec covers **only the
first**: the engine core that lets multiple strategies run in parallel, each with its own
capital + risk + accounting, behind a clean boundary. Initial capital intent is **tiny
(prove-it)**; the success criterion for this sub-project is *correctness, isolation, and
observability*, not profit.

## 2. Goals / non-goals

**Goals**
- Introduce a `Strategy` boundary and a `StrategyHost` that runs N strategies in parallel
  over the shared ingestion/registry/store/TUI.
- Make capital, risk (`RiskEngine`), and accounting (`PositionBook`) **per-strategy**,
  with the host aggregating for display.
- Wrap the existing arbitrage pipeline as "strategy #1" with **byte-identical behavior**
  (full existing test suite stays green).
- Prove the harness with a trivial "strategy #2" stub: parallelism, isolation,
  per-strategy accounting/risk, per-strategy pause/kill, aggregated TUI.

**Non-goals (deferred to later sub-projects)**
- The market-making strategy itself (#4).
- The resting maker-order execution path / user-fills channel (#3).
- The inventory-risk specifics — mark-to-market stop-loss, inventory caps (#2).
- Market segmentation and universe scaling to ~all markets (#5).

## 3. Roadmap (north star = the full broad platform)

1. **Multi-strategy engine core** — this spec. The harness everything plugs into.
2. **Inventory-risk framework** — risk module for risk-taking strategies.
3. **Resting maker-order execution path** — postOnly GTC/GTD place/cancel/replace + user fills.
4. **Market-making strategy + paper maker-fill simulator** — quoting logic; paper → tiny live.
5. **Segmentation + universe scaling** — classify by category/liquidity/volatility; WS sharding.

Each is its own spec → plan → ship cycle.

## 4. Architecture & boundaries

**Shared (single instance, unchanged):** ingestion supervisors + shards + books, the
`Registry`, `BookFetcher`, the SQLite store + writer, the TUI/publisher, the global kill
switch, ingestion `StatsCell`s. Strategies receive handles; they never own these.

**`Strategy` boundary.** Each strategy is a self-contained unit owning:
- its market-data access — an *optional* inline supervisor `on_apply` hook (arb uses this
  for low-latency class-1 detection) and/or its own async task loop reading books via
  `BookFetcher` (market-making will poll/refresh on a cadence);
- its intent → execution backend (arb keeps the existing single-in-flight basket
  `run_execution`; the stub uses a no-op sink; #3/#4 add the maker path);
- its own `PositionBook` + `RiskEngine` envelope + capital allocation;
- a stable `StrategyId` label.

**`StrategyHost`.** Owns the set of strategies and is responsible for:
- capital allocation (Σ per-strategy capital ≤ bankroll; validated at startup);
- installing each strategy's inline hook on every supervisor at wiring time;
- spawning each strategy's run-loop as an isolated task;
- routing per-strategy control (pause/kill) and honoring the global kill;
- aggregating per-strategy status into one `AppState` for the publisher
  (header equity = Σ per-strategy equity; plus a per-strategy breakdown).

**Arb = strategy #1 (byte-identical).** The existing `Detector` (inline `on_apply`) +
`Coordinator` + `run_execution` are wrapped as the arb strategy. The only change: it
reports into *its own* per-strategy `PositionBook`/`RiskEngine` rather than the global
singletons. Detection, dispatch, gates, cooldown, and the plausibility guard are untouched.

**Stub = strategy #2 (validation only).** A no-op heartbeat strategy: consumes market
events, maintains trivial state, emits no real orders. Exists solely to prove the seam.

## 5. Components & interfaces

Representative shapes (final signatures refined in the implementation plan):

```rust
pub struct StrategyId(pub &'static str); // e.g. "arb", "mm", "stub"

/// Per-strategy capital + risk envelope, validated by the host.
pub struct StrategyEnvelope {
    pub id: StrategyId,
    pub capital: Usdc,
    pub risk: RiskConfig, // per-strategy caps; arb keeps today's config
}

/// Read-mostly shared handles the host hands every strategy.
pub struct StrategyCtx {
    pub registry: Arc<Registry>,
    pub fetcher: BookFetcher,
    pub store_tx: mpsc::Sender<StoreMsg>,        // rows tagged with the StrategyId
    pub kill: Arc<AtomicBool>,                    // global kill (read)
    pub ctl_rx: mpsc::Receiver<CtlCommand>,       // per-strategy pause/kill
    pub status_tx: watch::Sender<StrategyStatus>, // host aggregates these
}

/// A strategy: an optional per-supervisor inline hook factory + an async run loop.
pub trait Strategy: Send {
    fn id(&self) -> StrategyId;
    /// Called once per supervisor at wiring time; returns the hook (or None).
    fn make_on_apply(&self) -> Option<Box<dyn FnMut(TokenId, &Shard) + Send>>;
    /// The strategy's owned task loop; returns a final summary on shutdown.
    fn run(self: Box<Self>, ctx: StrategyCtx)
        -> Pin<Box<dyn Future<Output = StrategySummary> + Send>>;
}
```

- **Per-strategy `RiskEngine` + `PositionBook`:** moved out of `Coordinator` into the
  strategy. `pm-risk`/`positions.rs` are unchanged in logic; only ownership moves.
- **Capital allocator:** a startup check in the host; over-allocation is a fatal config error.
- **`StrategyStatus` / aggregation:** each strategy publishes its
  cash/equity/realized/unrealized/positions/halted/paused; the host sums them into the
  existing `AppState` header and adds a per-strategy breakdown for the publisher.

## 6. Data flow

```
supervisors (shared) ──on_apply──> [strategy inline hooks]   (arb: class1/2/3 + LP enqueue)
                     └──books────> BookFetcher ──> [strategy run loops]
                                                        │ intents
                                                        ▼
                                          per-strategy execution backend
                                                        │ reports/fills
                                                        ▼
                                   per-strategy PositionBook + RiskEngine
                                                        │ status + store rows (tagged)
                                                        ▼
                              StrategyHost aggregation ──> publisher ──> TUI
```

Store rows (`opportunities`, `fills`, `orders`, `pnl_snapshots`) gain a `strategy` column
(default `"arb"` for backward compatibility), so P&L and activity are attributable
per-strategy and the existing DB still reads.

## 7. Error handling & isolation

- **Fault isolation:** each strategy runs in its own task. A strategy that panics or
  returns is caught by the host (JoinHandle), logged, and marked dead; **other strategies
  keep running**. The host never lets one strategy's failure abort the process.
- **Per-strategy halt:** a strategy's own `RiskEngine` halt (e.g., DailyDrawdown,
  SessionLoss) freezes only that strategy's dispatch; others are unaffected.
- **Per-strategy pause/kill** via `ctl_rx`; the **global kill** still stops every strategy
  cleanly (drains in-flight, final snapshot) exactly as today.
- **Capital safety:** the host refuses to start if Σ allocations exceed bankroll.

## 8. Testing

- **Regression (must stay green):** the entire existing workspace suite — arb behavior is
  byte-identical when wrapped as strategy #1.
- **Parallelism:** host runs arb + stub concurrently; both receive market events; the stub
  maintains its heartbeat while arb dispatches.
- **Isolation:** force the stub to panic / halt; assert arb is unaffected and the process
  stays up; assert a per-strategy halt freezes only that strategy.
- **Per-strategy accounting:** fills attributed to the right strategy; aggregated header =
  Σ per-strategy; store rows carry the correct `strategy` tag.
- **Per-strategy control:** pause/kill one strategy without affecting the other; global
  kill stops both.
- **Capital allocator:** over-allocation is rejected at startup.

## 9. Risks & open questions

- **Inline-hook factory vs async loop:** arb needs a per-supervisor mutable hook; the exact
  trait shape (`make_on_apply` factory) will be finalized in the plan. Risk: subtle change
  to arb's hot path. Mitigation: the regression suite + a hot-path benchmark check.
- **Publisher refactor:** moving from a single `CoordStatus` watch to host aggregation
  touches `publisher.rs`/`tui`. Kept minimal in #1 (aggregate header + per-strategy rows).
- **Store migration:** adding a `strategy` column must be backward-compatible with existing
  `pm.sqlite` files (default `"arb"`).

## 10. Acceptance criteria

- Existing workspace tests + clippy stay green.
- Arb runs as strategy #1 with identical detection/dispatch behavior.
- A stub strategy #2 runs in parallel, isolated, with its own (zero) accounting visible in
  the aggregated TUI.
- Per-strategy pause/kill and global kill all behave correctly.
- No market-making, maker-order execution, or scaling code is introduced (those are #2–#5).
