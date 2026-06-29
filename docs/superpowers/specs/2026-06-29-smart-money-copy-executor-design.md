# Smart-Money Copy-Trading Executor — Design

Date: 2026-06-29
Status: Approved (user picked follow-exit + stop-loss; pre-approved spec → plan → build)
Scope: A LIVE directional strategy that copies convergent fresh smart-money buys,
using the out-of-sample-validated config. Real money — gated + tiny-canary'd like
the MM. The platform's FIRST directional strategy (arb + MM are hedged), so risk
control is the spine.

## 1. The validated edge (from the backtest)
Out-of-sample (50 traders, 1,602 post-cutoff signals, independent Gamma
resolutions), the winning cell: **rank traders by EdgePerBet, copy only fresh
buys (entry within 15% of their fill), ~30-min reaction, k=1** → n=150, 68.7%
hit, +25.6%/bet, Sharpe 0.30, max-DD 5.8%. Skill-ranking + freshness both matter;
raw-leaderboard following is a coin-flip with 97% drawdowns. Spec
`docs/superpowers/specs/2026-06-28-smart-money-backtest-design.md`; canvas
`smart-money-backtest.canvas.tsx`.

## 2. Architecture
A new `CopyStrategy` (`crates/app/src/strategy/copy.rs`) implementing the
existing `Strategy` trait, run by `StrategyHost` alongside arb + MM, with an
isolated capital envelope + `InventoryRisk` + the same live gating (typed
confirmation, `l`-release, `start_paused`, auto-restart). Pure decision logic is
shared with the backtest (see §3.1). Data flow:

```
leaderboard + traders' history → EdgePerBet whitelist (periodic refresh)
        │
   poll whitelisted traders' /trades  → fresh BUY signal (k=1, < reaction window)
        │  freshness gate: our live CLOB price within max_drift of their fill?
        ├─ no  → skip (price ran)
        └─ yes → taker FAK BUY (equal-weight, capital-capped)  ──► position
                    │
        exits: (a) FOLLOW — poll the trader's /activity; they SELL → we FAK SELL
               (b) STOP-LOSS — mark-to-market vs the live book; ≤ -stop% → FAK SELL
               (c) RESOLUTION — held to settle → redeem via the M6 relayer
```

## 3. Components

### 3.1 Shared signal logic (refactor)
Extract the backtest's pure ranking + freshness primitives from
`crates/backtest/src/core.rs` into a new `crates/ingestion/src/smart_money.rs`
(beside `data_api`/`confluence`): `TraderRecord`, `trader_records`, `EdgePerBet`
ranking, the freshness check (`within_drift(entry_px, trigger_px, max_drift)`).
Both `pm-backtest` and the live `CopyStrategy` import it — ONE source of truth,
no reimplementation, no app→backtest dependency. The backtest's sim/metrics stay
in `pm-backtest`. (Net-new behavior is the live loop; the math is the validated
backtest's.)

### 3.2 Whitelist refresh (periodic)
On start + every `whitelist_refresh_secs` (default 6h): `DataApiClient` pulls the
PnL leaderboard (Month+All) + each trader's `/trades` + Gamma resolutions, builds
`trader_records`, applies `EdgePerBet` (min_bets, top_n) → the skilled wallet set.
A refresh failure keeps the prior whitelist (never trades on a stale-empty set).

### 3.3 Live signal poll
Every `signal_poll_secs` (default 90s): for each whitelisted wallet, fetch recent
`/trades`; a BUY with `timestamp ≥ now − reaction_window` and id not seen before
is a candidate. Dedup by `(condition_id, outcome)` (k=1: first whitelisted buyer).
A bounded `seen` set prevents re-firing.

### 3.4 Freshness gate + entry
For a candidate, read the live CLOB book for that outcome; the entry price is the
marketable ask. If `|entry − trader_fill| / trader_fill > max_drift` (default
0.15) → SKIP (logged). Else place a taker **FAK BUY**, sized
`min(per_position_usd, remaining capital)`, never exceeding the venue's 5-share
min / the capital envelope. Book the fill into `InventoryRisk` + the store.

### 3.5 Exit — follow + stop-loss + redeem
- **Follow:** poll each held position's source trader's `/activity` (or `/trades`);
  on a SELL of that outcome, FAK-SELL our position.
- **Stop-loss:** each cycle, mark positions to the live book; if a position's
  unrealized P&L ≤ `-stop_loss_pct` of its cost, FAK-SELL (cut the loser).
- **Resolution:** a position whose market resolved is redeemed via the M6 relayer
  (`RelayerClient::redeem`, already built + the auth is live-validated) — reused,
  not rebuilt.
Sells are taker FAK (we want out); long-only (we only ever sell what we hold).

### 3.6 Risk + gating (the spine — directional)
- **Capital envelope:** part of the `Σ capital ≤ bankroll` startup allocator;
  isolated from arb/MM.
- **Caps:** `per_position_usd`, `max_concurrent_positions`, `max_gross_usd`,
  `inventory_stop_loss_usd` / `daily_loss_usd` / `live.session_loss_usd` latches
  (reused from the MM/risk framework) → halt on a bad day.
- **Live gating:** `--live` + typed confirmation + `start_paused` until `l`;
  `relayer_enabled` for redeem; persistent UTC-day loss cap (reused).
- **Canary:** tiny first run (e.g. `per_position_usd ≈ $5–10`, `max_concurrent ≈
  2–3`, `capital ≈ $25`). It's directional, so caps + stop-loss are HARD.

## 4. Config (`[strategies.copy]` + `[copy]`)
`enabled`, `live`, `capital_usd`, `per_position_usd`, `max_concurrent_positions`,
`max_gross_usd`, `stop_loss_pct`, `max_drift` (0.15), `reaction_window_secs`
(1800), `min_bets`, `top_n`, `whitelist_refresh_secs`, `signal_poll_secs`,
`follow_exit` (true). Validated in `pm-config`. OFF by default.

## 5. Reuse (almost everything exists)
`DataApiClient` (leaderboard/trades/activity), the backtest's ranking + freshness
(§3.1), `LiveVenue` taker FAK + book reads, `RelayerClient::redeem` (M6),
`InventoryRisk` + caps + day-loss cap, `StrategyHost` + gating + TUI + store. The
genuinely new code: the whitelist-refresh + signal-poll + exit loops, and the
config.

## 6. Testing
Pure: freshness gate, signal dedup/freshness selection, sizing/cap math, stop-loss
trigger, exit decision — all TDD with fixtures (mirror the backtest). The live
poll/entry/exit I/O is exercised by a tiny funded canary (the deliverable), like
the MM. Paper mode (no live venue) for the loop wiring.

## 7. Safety call-outs
- DIRECTIONAL: a wrong copy loses the whole position if it resolves against us —
  hence the stop-loss + per-position + concurrent + gross caps + day latch.
- Long-only (sell ≤ held); taker entries bounded by the freshness band so we
  never chase; OFF + gated + canary-first.
- Whitelist failures degrade safe (keep prior / trade nothing).

## 8. Out of scope (later)
Convergence k>1 (the backtest's best was k=1), position pyramiding, the live
walk-forward re-validation (a separate analysis), shorting (buy-the-complement).
