# Polymarket Arbitrage Bot

Rust workspace implementing depth-aware, fee-aware arbitrage detection for
Polymarket. M1 (math core + detection engine) and M2 (registry + live
ingestion) are complete; paper execution (M3), TUI (M4), live trading (M5)
follow.
Design spec: `docs/superpowers/specs/2026-06-12-polymarket-arb-bot-v2-design.md`.

## Layout

- `crates/core` (`pm-core`) — exact numeric types (native-tick prices,
  micro-share sizes, micro-USDC cash, against-us rounding), dense ladder
  books, venue fee formula, instrument metadata.
- `crates/engine` (`pm-engine`) — generalized depth walker, detectors for
  arb classes 1–3, per-component LP detector (HiGHS) with exact integer
  re-validation, opportunity dedup.
- `crates/registry` (`pm-registry`) — venue-id interning, fixture-verified
  Gamma/CLOB metadata models, NegRisk partition verification with
  conservative exclusions, human-curated relationship TOML (§9 validation),
  market components.
- `crates/ingestion` (`pm-ingestion`) — exact decimal parsing (no f64 in
  the money path), live single-writer book shards with integrity
  (crossed-book / off-tick / hash) and feed-level staleness, CLOB REST
  client with deterministic rate limiting, WS supervisor with jittered
  reconnect and resnapshot healing, registry sync, probe binary.
- `crates/config` (`pm-config`) — typed TOML config (defaults = approved
  spec values), parse-time validation.

## Build & test

cargo lives in `~/.cargo/bin` (add to PATH if needed). The LP detector
compiles vendored HiGHS — `cmake` must be installed.

    cargo test --workspace      # full suite (199 tests, no network)
    cargo bench -p pm-engine    # criterion hot-path suite

## M1 measured baselines (Apple Silicon dev machine, 2026-06-12)

| Benchmark | Spec §20 gate (p99) | Measured (median) |
|---|---|---|
| ladder apply (per set) | ≤ 1 µs | 1.54 ns |
| ladder milli worst-case (per set) | — (informational) | 1.94 ns |
| class1_detect | ≤ 20 µs | 175.57 ns |
| class2_scan_n16 | ≤ 50 µs | 2.68 µs |
| lp_solve_8_markets | ≤ 10 ms | 694 µs |

## M2: live ingestion (registry + books)

`pm-registry` builds an immutable market registry from Gamma/CLOB metadata;
`pm-ingestion` maintains live order books over the WS market channel with
REST resnapshot healing. Staleness is FEED-level by design: the venue pushes
deltas only, so a quiet book on a live connection is current — books are
suspect only on integrity failure or connection silence/loss (spec §5
amendment).

### Probe (M2 acceptance instrument)

    cargo run -p pm-ingestion --bin probe --release -- --duration-secs 1800 --max-markets 200

30-minute acceptance run (Apple Silicon dev machine, 2026-06-12):

| Metric | Value |
|---|---|
| markets / books tracked | 200 / 400 |
| frames / events processed | 199,232 / 199,624 (~110/s) |
| stale books | 0 on every stats line |
| parse errors | 0 |
| reconnects | 0 (failure paths proven by the 5 replay tests) |
| resnapshots | 430 (400 initial + 30 integrity heals) |
| parse p50/p99 | 12 µs / 63 µs |
| apply p50/p99 | 5 µs / 28 µs |
| verdict / exit | healthy / 0 |

Longer soak: increase `--duration-secs` (e.g. 14400 for 4 h). Relationship
file: `relationships.toml` (see comments inside; only `approved` entries
trade).

Deployment note: co-location dominates language-level speed — measure RTT
to Polymarket endpoints from candidate regions before choosing a host
(full guidance lands with M6).

## M3: paper execution (coordinator + risk + store)

`pm-execution` simulates fills against live books with configurable latency
and fill-window; `pm-risk` / `pm-store` enforce daily-drawdown halts and
persist FIFO-lot P&L to SQLite. The binary connects to Polymarket's public
WS/REST endpoints (read-only), detects arb opportunities, dispatches baskets
to the paper-fill engine, and records every order, fill, P&L snapshot, halt,
and session boundary. No orders are ever sent to the venue.

### Config defaults (locked for M3)

| Section | Key | Default |
|---|---|---|
| `[execution]` | `paper_latency_ms` | 200 |
| `[execution]` | `fill_window_ms` | 500 |
| `[execution]` | `redeem_strategy` | `"merge"` |
| `[risk]` | `max_unhedged_usd` | 200.0 |
| `[risk]` | `max_open_orders` | 32 |
| `[risk]` | `max_basket_legs` | 16 |
| `[risk]` | `daily_drawdown_pct` | 2.0 |
| `[risk]` | `kill_file` | `"kill.switch"` |
| `[store]` | `path` | `"pm.sqlite"` |

### Kill switch

Two mechanisms stop the session cleanly (no positions left dangling):

- **File sentinel**: `touch kill.switch` (path resolved relative to cwd at
  launch; overridable via `[risk] kill_file`). The watcher polls every ~1 s;
  shutdown completes within ~2 s of the file appearing.
- **Signal**: `kill -USR1 <pid>`.

Both produce `trigger="kill"` in the log and exit 0. The DailyDrawdown risk
halt freezes new basket dispatch but does not stop the session — the session
keeps running until a kill or natural end.

### Acceptance run (Apple Silicon dev machine, 2026-06-12)

    cargo run -p pm-app --bin arb --release -- --db /tmp/m3-acceptance.sqlite

30-minute paper session (400 markets / 800 tokens, kill-switch test at
minute 27):

| Metric | Value |
|---|---|
| active session duration | 1620 s (27 min) |
| markets / tokens / supervisors | 400 / 800 / 14 |
| WS frames processed | 255,800 |
| reconnects | 0 |
| opportunities admitted to LP | 7,845 |
| baskets dispatched | 8 |
| baskets clean / unwound / nofill | 6 / 1 / 1 |
| paper fills | 100 |
| realized P&L | +$65.40 (paper) |
| DailyDrawdown halt | 1 (coordinator froze dispatch; session continued) |
| KillSwitch halt | 1 (clean stop at elapsed_s=1618) |
| write errors | 0 |
| detect latency p50 / p99 | 34 µs / 377 µs |
| dispatch latency p50 / p99 | 1,976 µs / 6,583 µs |
| verdict / exit | healthy / 0 |

Kill-switch response: sentinel file written at wall 22:30:14 IST;
`trigger="kill"` log line at elapsed_s=1618; process exited within ~4 s.

### Operational notes

**Starting a session**: binary is self-contained — it runs registry sync
(REST, rate-limited 5 req/s), assembles the market universe, spawns WS
supervisors, then begins detection. Sync for 400 markets takes ~4 min.

**Restart reconciliation**: on startup the binary reads the existing SQLite
store, reconciles any open positions from the previous session (counting
prior session starts in the restart-storm window), then resumes. Cold start
on a fresh DB shows `reconciled=0`.

**Drawdown halt**: when cumulative paper losses exceed `daily_drawdown_pct`
of deployed capital, the coordinator stops dispatching new baskets for the
remainder of the session. Ingestion and book-keeping continue normally.
Reset on next session start (new calendar day tracked in the store).
