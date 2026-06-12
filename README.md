# Polymarket Arbitrage Bot

Rust workspace implementing depth-aware, fee-aware arbitrage detection for
Polymarket. M1 (math core + detection engine) is complete; ingestion (M2),
paper execution (M3), TUI (M4), live trading (M5) follow.
Design spec: `docs/superpowers/specs/2026-06-12-polymarket-arb-bot-v2-design.md`.

## Layout

- `crates/core` (`pm-core`) — exact numeric types (native-tick prices,
  micro-share sizes, micro-USDC cash, against-us rounding), dense ladder
  books, venue fee formula, instrument metadata.
- `crates/engine` (`pm-engine`) — generalized depth walker, detectors for
  arb classes 1–3, per-component LP detector (HiGHS) with exact integer
  re-validation, opportunity dedup.
- `crates/config` (`pm-config`) — typed TOML config skeleton (defaults =
  approved spec values).

## Build & test

cargo lives in `~/.cargo/bin` (add to PATH if needed). The LP detector
compiles vendored HiGHS — `cmake` must be installed.

    cargo test --workspace      # full suite (90 tests)
    cargo bench -p pm-engine    # criterion hot-path suite

## M1 measured baselines (Apple Silicon dev machine, 2026-06-12)

| Benchmark | Spec §20 gate (p99) | Measured (median) |
|---|---|---|
| ladder apply (per set) | ≤ 1 µs | 1.54 ns |
| ladder milli worst-case (per set) | — (informational) | 1.94 ns |
| class1_detect | ≤ 20 µs | 175.57 ns |
| class2_scan_n16 | ≤ 50 µs | 2.68 µs |
| lp_solve_8_markets | ≤ 10 ms | 694 µs |

Deployment note: co-location dominates language-level speed — measure RTT
to Polymarket endpoints from candidate regions before choosing a host
(full guidance lands with M6).
