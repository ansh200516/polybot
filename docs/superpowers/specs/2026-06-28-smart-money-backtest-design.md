# Smart-Money Copy-Trading — Edge Backtest (design)

Date: 2026-06-28
Status: Approved (user pre-approved build) — spec → plan → build → RUN → report best ranking
Scope: An OFFLINE backtest tool that measures whether following "convergent smart
money" on Polymarket has a real, lag-robust edge — BEFORE building any live
executor or risking capital. **No trading, no keys, no orders.**

## 1. Why
The user proposed a hybrid: (1) copy top-leaderboard traders' positions and exit
when they exit, (2) scalp directional markets. Both collapse into one testable
edge: **follow convergent, fresh smart-money flow.** Every prior strategy (arb,
reward-farming) was plausible but unproven; the bottleneck is a *measured* edge.
So we measure first: does copying ranked smart money, *at the worse price we'd
realistically get after a detection lag*, beat how markets actually resolved?

## 2. Hypothesis & success criterion
H: there exists a trader-ranking + convergence + lag regime where copy returns are
net-positive after fees, on enough trades to not be luck.
- **GO** if some ranking is net-positive with a Sharpe-ish ratio meaningfully > 0
  across ≥ a few hundred trades AND survives a realistic lag (≥ 5 min) AND isn't
  driven by a handful of outliers.
- **NO-GO** otherwise (we stop, having spent no capital).

## 3. Data sources (Polymarket Data API — public, keyless)
- `GET /v1/leaderboard?orderBy=PNL&timePeriod=MONTH|ALL&limit=N` — trader universe.
- `GET /trades?user=&limit=&before=&after=` — a trader's timestamped fills
  (`side, asset, conditionId, size, price, timestamp, outcome, outcomeIndex`).
- `GET /closed-positions?user=` — resolved track record (skill ranking).
- `GET /trades?market=` — the market trade tape (our realistic entry/exit price).
- Resolutions: Gamma/registry market outcome (YES/NO) + resolved `curPrice` (0/1).
Rate limit ~1000 req/10s (`/trades` 200, `/positions` 150) — throttle politely.

## 4. Architecture
A new offline binary `backtest` (in `crates/app/src/bin/backtest.rs` or a small
`pm-backtest` crate), reusing `pm-ingestion` (Data API) + `pm-registry` (Gamma
resolutions). Pure, unit-testable modules; the live fetch is I/O at the edges.
- `fetch`: leaderboard → per-trader trades + closed-positions → per-market tape +
  resolution. Cache to local JSON (so re-runs don't re-hit the API).
- `rank`: build the ranking spectrum (below) over the trader universe.
- `simulate`: copy-with-lag per follow signal → return.
- `report`: aggregate metrics per (ranking × convergence × lag), write JSON/CSV +
  render a **canvas** report.

## 5. Ranking spectrum (the thing under test)
- **A. Raw leaderboard** — top-N by P&L (luck-contaminated baseline).
- **B. Track-record** — rank by realized hit-rate + total return on closed
  positions, with a min resolved-bet count to filter luck.
- **C. Edge-per-bet** — keep traders whose entries beat the outcome on average
  (`mean[outcome − entry_price] > 0`, min sample).
- **× Convergence overlay** — additionally require K∈{1,2,3} ranked traders on the
  same market+side within a window.

## 6. Copy-with-lag simulation (the heart)
For each follow signal (a ranked trader's BUY of market+side at `t`):
- **Detection lag** swept over {1, 5, 30, 60} min. Our entry time = `t + lag`.
- **Entry price** = the market tape's first trade price at time ≥ entry time
  (approach (a)); + taker fee (copying crosses). If no tape trade in window, skip
  (illiquid → uncopyable).
- **Exit** (two modes, both reported):
  - **hold-to-resolution** (primary): exit = resolved outcome value (0/1).
  - **follow-exit** (extension): exit when the trader SELLs (from `/trades`/
    `/activity`), priced at their exit time + lag from the tape; else resolution.
- **Return** = (exit − entry)/entry, net of fees. Position sizing = equal-weight
  per signal (so the metric is per-trade edge, not a sizing artifact).

## 7. Metrics & output
Per (ranking × convergence × lag × exit-mode): mean return/trade, median,
hit-rate, total/compounded return, a Sharpe-ish ratio (mean/stdev), max drawdown,
trade count, and the **lag-sensitivity curve**. Plus splits: **sports vs
non-sports** (leaderboard is sports-heavy; that edge may not be copyable), and by
entry-price bucket. Output: a JSON results file + a **canvas** dashboard ranking
the methods, so the GO/NO-GO is visual and the *winning* ranking is obvious.

## 8. Honest caveats (stated in the report)
- Tape-price fills approximate real fills (ignores depth/slippage beyond the fee).
- Resolved-markets only (fine for return measurement; no look-ahead since we use
  only data available at `t + lag`).
- Past edge ≠ future edge (leaderboard churn + competition).
- A sports-driven edge may be a sports-betting skill we can't replicate with lag.

## 9. Defaults (tunable)
trader universe N=30 (PnL, MONTH + ALL, de-duped); history = last ~6 months of
resolved markets; convergence window = 24h; fees = Polymarket taker; lags {1,5,30,60}m.

## 10. Testing
Unit tests on the PURE functions (ranking, the lag entry/exit price lookup against
a fixture tape, return + metric math, sports classifier). The live data fetch is
exercised by the actual run (the deliverable). Cache the raw API pulls so the
analysis is reproducible without re-fetching.

## 11. Out of scope
Any live executor, order placement, or capital. This spec ends at a report that
says GO (with the winning ranking/lag) or NO-GO. Building the executor is a
SEPARATE spec, gated on a GO.
