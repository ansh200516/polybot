# Smart-Money Edge Backtest — Implementation Plan

> Subagent-driven, TDD. Spec: `docs/superpowers/specs/2026-06-28-smart-money-backtest-design.md`.
> Offline analysis tool — NO trading. Terminal deliverable: a report naming the winning ranking (GO/NO-GO).

**Crate:** new `crates/backtest` (`pm-backtest`), bin `backtest`, depending on `pm-ingestion` (Data API) + `pm-registry` (Gamma resolutions) + `serde`/`reqwest`/`tokio`.

## Shared data model (all tasks agree on this)
```rust
enum Side { Buy, Sell }
struct Trade { wallet: String, condition_id: String, asset: String, side: Side,
               size: f64, price: f64, timestamp: i64, outcome_index: i64 }
struct ClosedPos { wallet: String, condition_id: String, asset: String,
                   avg_price: f64, outcome_index: i64, won: bool, cash_pnl: f64 }
struct Resolution { condition_id: String, winning_outcome_index: i64 }  // from Gamma
// market tape = Vec<Trade> for a conditionId, sorted by timestamp
// follow signal = a ranked trader's BUY (condition_id, outcome_index, price, timestamp)
```

## BT-1 — Data API extensions (`pm-ingestion`)
- [ ] `Trade` model + `parse_trades(body)`; `DataApiClient::trades(filter: User|Market, before, after, limit)` with cursor/offset pagination (cap total). Fields per the OpenAPI Trade schema (side BUY/SELL, asset, conditionId, size, price, timestamp, outcomeIndex).
- [ ] `ClosedPos` model + `closed_positions(user)` (`GET /closed-positions?user=`). Add `avg_price`, `cash_pnl` to `Position` (already in the response).
- [ ] Unit tests: parse fixtures for `/trades` and `/closed-positions` (trim real responses).
- [ ] Commit: `feat(ingestion): Data API /trades + /closed-positions for backtest`

## BT-2 — Fetch + cache pipeline (`pm-backtest`)
- [ ] `fetch`: leaderboard(PnL, MONTH+ALL, N=30, de-dup) → per-trader `trades(user)` + `closed_positions(user)` → collect the traded `conditionId` set → per-market `trades(market)` tape + resolution (Gamma `registry`/market lookup → winning_outcome_index). Polite throttle (≤ rate limit).
- [ ] Cache every raw pull to `./bt-cache/*.json`; re-runs read cache (reproducible, no re-fetch). A `--refresh` flag re-pulls.
- [ ] Resolutions: prefer Gamma resolved outcome; fall back to a closed-position `won` for that conditionId. Drop unresolved markets.
- [ ] Commit: `feat(backtest): cached fetch pipeline (traders, trades, tapes, resolutions)`

## BT-3 — Pure core: ranking + sim + metrics (`pm-backtest`) — TDD
- [ ] `rank` (pure): A raw-leaderboard top-N; B track-record (hit-rate+total return on closed pos, min N bets); C edge-per-bet (mean[outcome−entry]>0, min N). Each returns a ranked/whitelisted wallet set. Tests on synthetic trader records.
- [ ] `convergence` (pure): given ranked wallets' BUY signals, group by (condition_id, outcome_index) within a window; K∈{1,2,3} threshold. Tests.
- [ ] `simulate` (pure): `entry_price(tape, t+lag)` = first tape trade ≥ entry time (else None→skip); `exit` hold-to-resolution (0/1) AND follow-exit (trader SELL ts+lag tape price, else resolution); `net_return` with taker fee. Tests against a fixture tape (exact lookups + return math).
- [ ] `metrics` (pure): mean/median return, hit-rate, total, Sharpe-ish (mean/stdev), max drawdown, count; `is_sports(title/slug)` classifier; group-by. Tests.
- [ ] Commit: `feat(backtest): pure ranking + copy-with-lag sim + metrics (TDD)`

## BT-4 — Binary wiring + RUN
- [ ] `main`: load cache (or fetch) → for ranking ∈ {A,B,C} × conv ∈ {1,2,3} × lag ∈ {1,5,30,60}m × exit ∈ {resolution, follow}: build signals → simulate → metrics. Write `bt-results.json` + a stdout summary table (sorted by Sharpe-ish, with sports/non-sports split).
- [ ] `cargo build`/`clippy` clean; `cargo run -p pm-backtest --bin backtest` (NEEDS full_network for the Data API + Gamma).
- [ ] Commit: `feat(backtest): runner + results output` (+ commit `bt-results.json` if useful).

## BT-5 — Canvas report + verdict
- [ ] Render `bt-results.json` as a canvas dashboard (ranking leaderboard, lag-sensitivity curves, sports split, GO/NO-GO banner). Report the winning ranking to the user.

## Notes
- No look-ahead: only use tape data at/after `t + lag`; resolutions are the realized truth.
- Equal-weight per signal (measure per-trade edge, not sizing).
- If a market has no tape trade in the lag window → skip (uncopyable/illiquid), and COUNT skips (a high skip rate is itself a finding).
- Pure fns are the tested core; the live fetch is exercised by the run.
