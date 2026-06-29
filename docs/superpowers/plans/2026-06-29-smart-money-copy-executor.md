# Smart-Money Copy Executor — Implementation Plan

> Subagent-driven, TDD. Spec: `docs/superpowers/specs/2026-06-29-smart-money-copy-executor-design.md`.
> LIVE directional strategy — OFF by default, gated + tiny-canary'd like the MM. Reuses the platform.

## C1 — Shared signal module (refactor; keep everything green)
**Files:** new `crates/ingestion/src/smart_money.rs` (+ `pub mod` in `ingestion/src/lib.rs`); `crates/backtest/src/core.rs` (re-point).
- [ ] Move the PURE, validated primitives out of `backtest/core.rs` into `pm_ingestion::smart_money`: `TraderRecord`, `trader_records(trades_by_wallet, resolutions, cutoff_ts)`, the `EdgePerBet`/`TrackRecord` ranking (`rank_wallets_oos`), and a `within_drift(entry_px, trigger_px, max_drift) -> bool` freshness helper (extract from `simulate_signal`). Keep `Ranking` here too.
- [ ] `pm-backtest` depends on `pm-ingestion` (already does) and re-imports these (delete the moved copies; the backtest's sim/metrics stay). Move the relevant unit tests with the code.
- [ ] `cargo test -p pm-ingestion -p pm-backtest && cargo clippy -p pm-ingestion -p pm-backtest --all-targets -- -D warnings` green (the backtest must still pass its golden cells). Commit: `refactor(ingestion): shared smart_money ranking+freshness (reused by backtest + live copy)`.

## C2 — Config
**Files:** `crates/config/src/lib.rs`.
- [ ] `[strategies.copy]` (`enabled` false, `live` false, `capital_usd`) + `[copy]`: `per_position_usd`, `max_concurrent_positions`, `max_gross_usd`, `stop_loss_pct`, `max_drift` (0.15), `reaction_window_secs` (1800), `min_bets` (10), `top_n` (30), `whitelist_refresh_secs` (21600), `signal_poll_secs` (90), `follow_exit` (true). `deny_unknown_fields` + `Default` + `validate()` (positive caps, 0<max_drift<=1, 0<stop_loss_pct<=1). Tests. Commit.

## C3 — CopyStrategy core: whitelist + signal selection (pure-first, TDD)
**Files:** `crates/app/src/strategy/copy.rs` (new, `pub mod` in `strategy/mod.rs`).
- [ ] `CopyParams` (from config). The `Strategy` impl skeleton (mirror `MmStrategy`: capital envelope, `StrategyCtx`, ctl_rx pause/kill, `start_paused`, status publish).
- [ ] Pure `select_signals(recent_trades_by_wallet, whitelist, seen, now, reaction_window) -> Vec<CopyCandidate{condition_id, outcome_index, trader, trigger_px, ts}>` (k=1 dedup by (cond,outcome), fresh-by-time, not-seen). TDD.
- [ ] Whitelist refresh: `refresh_whitelist(DataApiClient, gamma, cfg) -> Vec<wallet>` via `smart_money::trader_records` + `rank_wallets_oos(EdgePerBet)`; failure keeps the prior set. (I/O thin; pure ranking already tested in C1.)
- [ ] Commit: `feat(app): CopyStrategy skeleton + whitelist refresh + pure signal selection`.

## C4 — Entry + exit + risk (pure-first, TDD)
**Files:** `crates/app/src/strategy/copy.rs`, `crates/execution` (reuse taker FAK + book + relayer).
- [ ] Freshness gate at entry: `within_drift` vs the live book's marketable ask → enter or skip.
- [ ] Entry: taker **FAK BUY**, size `min(per_position_usd, capital_left)`, ≥5-share min; book into `InventoryRisk` + store; respect `max_concurrent_positions` / `max_gross_usd` / capital.
- [ ] Exit — pure deciders + thin I/O: (a) `should_follow_exit(trader_activity, held_outcome)`; (b) `stop_loss_hit(cost, mark_px, qty, stop_pct)`; both → taker FAK SELL (≤ held). (c) resolution → `RelayerClient::redeem` (reuse M6). 
- [ ] Risk latches reused (inventory stop / daily / session / day-loss cap). TDD the pure deciders (drift, sizing/caps, stop-loss, follow-exit). Commit: `feat(app): CopyStrategy entry (fresh taker buy) + follow/stop/redeem exits + caps`.

## C5 — main.rs wiring + TUI + integration + review
**Files:** `crates/app/src/main.rs`, `crates/app/src/publisher.rs`, `crates/tui/*`.
- [ ] Construct `CopyStrategy` in `main.rs` when `[strategies.copy].enabled` (capital carve in the `Σ ≤ bankroll` allocator); thread `DataApiClient` + `LiveVenue` + relayer + gating. Surface its status (open copy positions, P&L, whitelist size, last signal) in the TUI.
- [ ] Integration test (paper / mocked feeds): whitelist → fresh signal → entry → follow-exit / stop-loss applied; gated off when disabled. `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` green.
- [ ] Final review subagent (directional-risk focus: caps enforced, stop-loss fires, long-only sells, gating off-by-default, no panics). Commit + a `mm-live-copy-canary.toml` (tiny: per_position ~$5, max_concurrent 3, capital ~$25, stop_loss 0.25).

## Notes
- Everything OFF by default; first live run is a typed-confirmation canary with hard caps.
- Pure deciders are the tested core; live polling/entry/exit are the operator's funded canary (like the MM).
- A wrong copy can lose the whole position → the stop-loss + caps are load-bearing, not optional.
