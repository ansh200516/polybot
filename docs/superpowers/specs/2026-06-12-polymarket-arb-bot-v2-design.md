# Polymarket Arbitrage Bot — v2 Design Spec

Date: 2026-06-12
Status: approved by user (sections approved in-session 2026-06-12; supersedes the deleted 2026-06-11 v1 spec)
Source requirements: user PRD ("Polymarket Arbitrage Bot — Rust, depth-aware, fee-aware, live TUI dashboard")

## 1. Goal

A production-quality Rust application that continuously scans Polymarket for arbitrage, sizes each opportunity against real order-book depth, and executes — paper mode by default, live behind an explicit flag plus typed confirmation. Every opportunity whose net edge (after fees, gas, and walked depth) clears a configurable floor is captured at the largest size the books and risk caps allow. Math correctness outranks feature breadth; consistent low internal latency is a first-class requirement.

## 2. Locked decisions

User-confirmed 2026-06-11, reconfirmed for v2 on 2026-06-12:

| Decision | Value |
|---|---|
| Bankroll | $10,000 |
| Per-market cap | $1,000 |
| Min net edge, classes 1–2 | 30 bps |
| Min net edge, class 3 | 100 bps |
| Arb classes enabled | 1, 2, 3, 4 (LP) |
| Class 5 (cross-venue) | Out of scope |
| Default mode | Paper |

Design decisions made in-session 2026-06-12:

- **Integer-exact core** (approach A) over f64-pragmatic and exact-rational alternatives: all hot-path money math in integers; floats only at the serde parse boundary and the HiGHS solver interface.
- **Rounding policy: always against us** — costs round up, proceeds round down. Reported edges are floors, never estimates.
- **LP decomposed per connected component** of the relationship graph, not monolithic.
- **Dense per-tick array books**, single-writer shard tasks, no locks.

## 3. Workspace layout

Rust stable, 2024 edition, `tokio` runtime. Cargo workspace:

| Crate | Contents | Depends on |
|---|---|---|
| `core` | numeric types, ladders/books, fee math, instrument metadata types | — |
| `engine` | detectors (classes 1–3), VWAP walker, sizing, LP (HiGHS), dedup/cooldown | `core` |
| `registry` | markets, outcome sets, relationship graph, connected components | `core` |
| `ingestion` | Gamma REST sync, CLOB REST snapshots, CLOB WS deltas, book maintenance, staleness | `core`, `registry` |
| `execution` | `ExecutionVenue` trait, paper venue, live CLOB venue (EIP-712 via `alloy`), order state machine, leg coordinator | `core`, `risk`, `store` |
| `risk` | caps, halts, kill switch | `core` |
| `store` | `rusqlite` WAL persistence, single writer task | `core` |
| `tui` | ratatui dashboard | `core` |
| `config` | TOML config via serde (deny unknown fields), env-only secrets | — |
| `app` (bin) | wiring: tasks, channels, supervisor | all |

Dependency rule: `core` and `engine` do no I/O and have no async; they are pure libraries testable and benchable in isolation.

## 4. Numeric model

- Tick sizes on Polymarket are 0.01 or 0.001 per market. Let `L` = levels per dollar (100 or 1000).
- `Px(u16)`: interior tick index `k ∈ [1, L−1]`. Price 0 and 1 are not representable on purpose.
- Price in micro-USDC per share: `p_µ(k) = k · (1_000_000 / L)` — exact integer because `L | 10⁶`.
- `Qty(u64)`: micro-shares (10⁻⁶ shares — matches USDC's 6 decimals).
- `Usdc(i128)`: signed micro-USDC for costs, proceeds, P&L.
- `Bps(i32)`: basis points for edges and fee rates.
- Notional of `q` micro-shares at tick `k`: `p_µ(k) · q / 10⁶` computed in `i128`; **ceil for costs, floor for proceeds**.
- Edge: `edge_bps = floor(10⁴ · net_profit_µ / cost_basis_µ)`.
- Boundary rule: API prices that do not land exactly on the market's tick grid are rejected and counted, never rounded. Conversions `f64 → Px` exist only in `ingestion`'s parse layer.

## 5. Order books

- `Ladder`: `Box<[Qty]>` of length `L+1` indexed by tick, plus a `best: Option<Px>` pointer. One ladder per side. 0.8–8 KB per side.
- Apply delta: write the level, repair `best` by scanning from the previous best (amortized O(1)).
- `Book`: `{ bids: Ladder, asks: Ladder, seq/hash: u64, last_update: Instant }`. Polymarket book messages carry a hash; on mismatch with local state, mark the book invalid and request a REST resnapshot.
- Ownership: N shard tasks (token id hashed to shard). A shard exclusively owns its books — single writer, zero locks. Detection for a book runs in its owning shard immediately after apply.
- Staleness: a book older than `staleness_ms` (config, default 1500) is excluded from detection and blocks new orders on its market. **Delta-feed amendment (M2, live-verified):** the venue pushes deltas only — a quiet book on a live connection is *current*, not stale. Staleness therefore gates at FEED level: a book is suspect iff it is integrity-invalid or its connection is down/silent beyond `feed_silence_ms` (default 15000, forces reconnect). M3's detection gate must combine book validity with feed liveness, not raw per-book age.

## 6. Cost model

**Fees.** Per-market `fee_rate_bps` fetched at startup from the CLOB API and cached; refreshed on resync; config can override but never defaults to zero silently. Formula (Polymarket's documented schedule — symmetric in price):

```
fee_µ(rate_bps, k, q) = ceil( rate_bps · min(p_µ, 10⁶ − p_µ) · q / (10⁴ · 10⁶) )
```

applied per fill. When the venue levies the buy-side fee in shares, the share amount is the USDC fee divided by price, rounded against us. The exact current schedule and levy asset must be re-verified against live docs in M2 before any reliance on it.

**Gas.** Fixed per-operation estimates in config (µUSDC): `split`, `merge`, `redeem`, `negrisk_convert`. Charged only when the opportunity's execution path performs that operation. Class 1 long uses `redeem_strategy = merge | hold` (config; merge default): `merge` charges gas immediately, `hold` charges `redeem` gas at resolution and accepts capital lockup.

**Eligibility.** An opportunity is reported iff, at its profit-maximizing size: `edge_bps ≥ floor_class` (30/30/100 bps) **and** `net_profit_µ ≥ min_profit_µ` (config, dust filter; default $1.00), where net profit is after fees and gas with against-us rounding.

## 7. Sizing — the depth walker

Never price from midpoints or best quotes. For a candidate basket (one or more legs, each a side of a book):

- Walk each leg's ladder level by level, merging legs by marginal combined price.
- Profit as a function of size is piecewise-linear and concave; the walker advances while marginal profit per share is positive and risk caps are not binding, yielding **max profitable size**, total cost basis, net profit, and edge — all exact integers.
- Output struct carries per-leg limit prices (the worst level touched per leg) for execution.

## 8. Arbitrage classes

**Class 1 — binary complete set.**
- Long: walk YES asks + NO asks combined; profitable while marginal `ask_yes + ask_no < 1` net of fees/gas. Redeem per `redeem_strategy`.
- Short: split $1 collateral, sell both legs into bids; profitable while marginal `bid_yes + bid_no > 1` plus split gas and fees.

**Class 2 — NegRisk multi-outcome.**
- Only on **verified-exhaustive** outcome sets: the venue's NegRisk structure marks the partition; sets containing placeholder/"Other"-style outcomes or venue flags indicating openness are excluded (conservative allowlist; exclusion reasons logged).
- Long: walk all YES asks combined; profitable while `Σ marginal ask(YES_i) < 1` net of costs; exactly one outcome pays $1 at resolution (or NegRisk-convert earlier).
- Short (overpriced set): via the NegRisk conversion identity `NO_i ≡ {YES_j}_{j≠i}` with conversion gas; detected on bids, executed buy-side through conversions.

**Class 3 — cross-market logical (100 bps floor).** Buy-only formulations (no cross-market shorting mechanics needed):
- `Implies(A ⇒ B)` violated: buy `YES_B` + buy `NO_A`; min payoff $1 per pair in every reachable world; arb iff `ask(YES_B) + ask(NO_A) < 1 −` costs − floor.
- `MutuallyExclusive(A, B)` violated: buy `NO_A` + buy `NO_B`; min payoff $1; arb iff `ask(NO_A) + ask(NO_B) < 1 −` costs − floor.
- `Equivalent(A, B)`: both implication directions.

**Class 4 — unified LP detector.** §10.

## 9. Relationship registry

- Typed edges: `Implies(a, b)`, `MutuallyExclusive(a, b)`, `Equivalent(a, b)`, `ExhaustivePartition([markets])`.
- Source of truth: a TOML file, hot-reloaded on change. Each entry: `kind`, market refs, `status: pending | approved | rejected`, `source: manual | suggested`, free-text note.
- **Only `approved` entries are tradable.** The auto-suggester (M6) writes `pending` entries; a human edits them to `approved`. A wrong link is not an arbitrage.
- Registry load validation: symmetric relationships (`MutuallyExclusive`, `Equivalent`) are canonicalized to `a ≤ b` and deduplicated; self-referential entries (`a == b`) are rejected; the registry is the sole production constructor of `Partition` values and enforces parallel-lane well-formedness and exhaustiveness verification at build time.
- The registry exposes connected components over (tokens of one market) ∪ (NegRisk partitions) ∪ (approved relationship edges) — these are the LP scopes and the class-3 trigger index.

## 10. LP detector (class 4)

Per dirty component, on a background task pool (never in shard tasks):

- **Worlds** `W`: product of outcomes per event in the component (binary market = 2 outcomes; NegRisk partition = its outcomes), **pruned to worlds consistent with approved relationship semantics** (`Implies` removes `A ∧ ¬B` worlds, `MutuallyExclusive` removes `A ∧ B`, `Equivalent` removes disagreements) — this pruning is exactly how class-3 violations surface as LP profits. Hard cap `max_worlds` (default 4096) applies to the pre-prune product; components exceeding it are skipped with a warning counter.
- **Variables**: `b_{i,l} ∈ [0, depth]` buy at ask level `l` of token `i`; `s_{i,l} ∈ [0, depth]` sell into bid level `l`; `c_k ≥ 0` conversion actions (split, merge, NegRisk convert) with their gas costs; `t` free.
- **Objective**: maximize `t` (guaranteed profit).
- **Constraints**: for every world `w`: `Σ_i payoff_i(w) · pos_i − total_cost + total_proceeds − gas ≥ t`; budget `total_cost ≤ component cap`; per-market caps; sell quantities bounded by holdings acquirable via splits/conversions within the same basket.
- Solver: HiGHS (`highs` crate), f64 at the boundary only.
- **Integer re-validation**: the candidate basket from the solver is re-priced exactly by the §7 walker logic; if exact `t` fails the floor/dust gates, the opportunity is discarded. Solver tolerance can never manufacture an edge.
- Floor: a class-4 opportunity uses the 30 bps floor when its component contains no relationship edges, and the 100 bps floor when it does (logical links carry resolution risk regardless of which detector found the trade).
- Required tests: synthetic books where classes 1, 2, and 3 each are the optimum — the LP must recover them (PRD acceptance).
- Trigger discipline: book deltas mark their component dirty; per-component min re-solve interval; global solver concurrency cap.

## 11. Dedup & cooldown

- Fingerprint: class + sorted (token, side, limit price) — size excluded.
- A fingerprint in cooldown (config, default 2 s) is suppressed unless net profit improves by more than `reemit_improvement_pct` (default 20%).
- Lazy purge of expired entries.

## 12. Pipeline & channels

```
WS conns ──frames──▶ parser ──deltas──▶ shard router ──▶ shard task (apply + detect 1/2/3, mark LP dirty)
                                                            │ opportunities (bounded mpsc)
LP task pool ◀──dirty components──┘                         ▼
       └─────opportunities────▶ coordinator (dedup → risk pre-check → store log → TUI feed → dispatch)
                                                            │ approved baskets
                                                            ▼
                                                   execution task ──▶ venue (paper | live)
store writer task ◀── bounded mpsc from coordinator/execution      TUI ◀── watch<AppState> @ ~10 Hz
```

- Delta channels never drop; sustained backpressure beyond a lag threshold triggers resnapshot.
- Opportunity channel coalesces by fingerprint under pressure.
- Detection never blocks on store, TUI, or execution.

## 13. Ingestion (M2)

- Gamma REST: events, markets, NegRisk flags, token ids, condition ids — full sync at startup, periodic resync (config interval), diff-applied to registry. Cache tick size, min order size, fee rate per market.
- CLOB REST: book snapshots (startup, recovery, resync), fee schedule, server time.
- CLOB WS: market channel subscriptions for tracked tokens; deltas routed to shards; heartbeat monitoring; exponential backoff + jitter reconnect; books marked stale on disconnect instantly.
- Client-side rate limiting (token bucket per endpoint class) and 429/5xx backoff.
- Market universe: config filter (liquidity floor, category allowlist, max tracked markets) — keeps shard memory and WS subscriptions bounded.

## 14. Execution

- `ExecutionVenue` trait; implementations: `PaperVenue`, `LiveVenue`. Identical order/fill semantics to the caller.
- **Order state machine**: `Draft → Signed → Submitted → Live → {Filled, PartFilled, Cancelled, Rejected, Expired}` with persisted transitions. Client order id = UUIDv7, idempotency key. The transition is written to the store **before** the network call; restart reconciles via open-orders + recent-fills queries.
- **Leg coordinator**: all legs of a basket submitted concurrently as FAK/FOK at the walker's limit prices. Fill window (config, default 500 ms). Partial outcome → **repair**: re-price unfilled legs up to the basket's break-even price; still unfilled → **unwind**: market out filled legs, record realized loss, increment failure counters. Max unhedged exposure enforced before submission and during repair.
- **Paper venue**: fills against the live book after a simulated latency `paper_latency_ms` (default 200): re-read the (possibly moved) book and fill only what remains at the limit. Fees and gas charged as configured. No midpoint fills, no infinite depth.
- **Live venue (M5)**: EIP-712 order signing via `alloy` verified against Polymarket's published example vectors in unit tests; L1/L2 auth headers; allowance checks at startup; `--live` flag + typed confirmation phrase at runtime; canary cap on first live session (config, default $50 per basket).

## 15. Risk

Synchronous pre-submit checks in the execution path:

- Global exposure ≤ $10k bankroll; per-market ≤ $1k; per-basket ≤ remaining headroom.
- Max unhedged exposure (config, default $200), max open orders, max basket legs.
- Staleness gate per market; global pause flag (TUI).
- Halts (trip → stop new orders; optional flatten): daily drawdown limit (config, default 2%), N consecutive order errors in M seconds, restart-storm detector.
- Kill switch: TUI key, `SIGUSR1`, or sentinel file — cancels open orders, halts dispatch, requires manual clear.

## 16. Persistence (`store`)

`rusqlite`, WAL mode, one writer task fed by a bounded mpsc. Tables:

`markets`, `relationships`, `opportunities` (ts, class, fingerprint, edge_bps, size, est_profit, legs json), `orders`, `order_events`, `fills`, `lots` (FIFO cost basis), `pnl_snapshots`, `halts`.

Realized P&L from FIFO lots; unrealized marked conservatively (bid for longs, ask for shorts); resolution settlements close lots at $1/$0. Detection hot path never writes synchronously; opportunity logging may coalesce, order/fill logging may not.

## 17. TUI (M4)

ratatui + crossterm, dark theme, green/red reserved for edge and P&L:

1. Opportunity feed: class, markets, net edge bps, max size, est. profit, age.
2. Positions & realized/unrealized P&L.
3. Recent fills and order states.
4. System health: WS connectivity, per-feed staleness, internal latency p50/p99, scan rate, solver queue depth.
5. Scrolling log (tracing subscriber tee).

Keys: pause scanning, toggle paper/live (typed confirmation modal), kill switch, quit. Input: `watch<AppState>` snapshots at ~10 Hz; the TUI can never block producers.

## 18. Config & secrets

TOML (serde, `deny_unknown_fields`), sections: `capital`, `edges`, `classes`, `fees`, `gas`, `universe`, `ingestion`, `execution`, `risk`, `lp`, `store`, `tui`, `endpoints`. Locked values of §2 are the shipped defaults. Validation at startup with precise errors.

Secrets only via env (`POLY_PRIVATE_KEY`, `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_API_PASSPHRASE`) into a `Secret<T>` newtype whose `Debug`/`Display` redact. Never logged, never in config files, never in the store.

## 19. Errors & ops

- Parse failure: drop message, count, log sampled; persistent failures on one book → resnapshot.
- WS drop: instant staleness, backoff reconnect, resubscribe, resnapshot.
- Solver failure/timeout: skip component, count, surface on health panel.
- Task panic: supervisor restarts; restart storm trips the kill switch.
- `tracing` structured logs; `#![deny(clippy::unwrap_used, clippy::expect_used)]` outside tests; all fallible money paths return `Result`.

## 20. Performance targets & instrumentation

`hdrhistogram` stages: ws-recv→parsed, parsed→applied, applied→detected, detected→submitted. p50/p99 on the TUI; periodic log line; CSV dump flag.

Criterion gates (p99 on dev hardware; v1 measured baselines in parentheses for reference):

| Benchmark | Gate | v1 baseline |
|---|---|---|
| Ladder delta apply | ≤ 1 µs | 5 ns |
| Class-1 detect post-apply | ≤ 20 µs | 185 ns |
| Class-2 scan (n ≤ 16) | ≤ 50 µs | 2 µs |
| LP component solve (8 binary markets, 256 worlds) | ≤ 10 ms | 469 µs |
| Book update + single-market detection (PRD) | < 100 µs | — |

LP rescans run off the hot path by construction (§10, §12).

README must include deployment guidance: measure RTT to Polymarket endpoints from candidate regions; co-location dominates language-level speed.

## 21. Testing & acceptance

- `core`: proptest — Px/Qty/Usdc conversions, rounding always against us, ladder ops vs a BTreeMap reference model; fee formula vs documented examples.
- `engine`: walker vs brute-force on random books; golden synthetic-book cases per class (long and short variants); LP recovers classes 1–3; LP integer re-validation rejects solver-tolerance phantoms; dedup/cooldown.
- `registry`: exhaustiveness verification incl. "Other" exclusion; component computation.
- `execution`: order state machine transitions (incl. crash points), leg repair/unwind, paper-fill honesty (book moved between detect and fill).
- `risk`: every cap and halt.
- Integration (M3): synthetic feed → detection → sizing → paper execution → store → P&L, deterministic.
- EIP-712 (M5): signature vectors vs Polymarket's official examples.
- Acceptance per PRD: runnable workspace + README; tests above green; criterion gates met; a recorded paper-mode session demonstrating detect → size → simulate → dashboard.

## 22. Milestones

| Milestone | Scope | Exit criteria |
|---|---|---|
| M1 | `core` + `engine` (+ `config` skeleton): all math, detectors, LP, dedup; no I/O | tests green incl. LP-recovers-1–3; criterion gates met |
| M2 | `registry` + `ingestion`: Gamma/CLOB REST + WS read-only; live books in memory; probe binary | hours-long live ingest, stable books, staleness/resync proven |
| M3 | `store` + `risk` + `execution` (paper) + `app` wiring | headless end-to-end paper session against live data |
| M4 | `tui` | full dashboard on live paper session |
| M5 | live execution: signing, auth, allowances, typed confirm, canary caps | verified signatures; tiny-size live round-trip |
| M6 | relationship auto-suggester, deployment/RTT docs, polish | PRD deliverables complete |

Each milestone gets its own implementation plan (writing-plans) when it begins. One milestone in flight at a time.

## 23. Out of scope

Class 5 cross-venue arbitrage; market-making; ML price prediction; multi-account support; non-Polymarket venues.
