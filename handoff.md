# Handoff — Polymarket copy-trading bot

Context for any agent/developer picking this up. Read this + `README.md` + the
specs/plans in `docs/superpowers/` before patching. `migration.md` covers laptop
access setup.

## TL;DR

- A Rust workspace (`crates/*`, binary `arb`) trading on Polymarket.
- The **live** strategy today is **smart-money COPY-TRADING**. Arb and
  market-making also exist but are **OFF** in the live config.
- It runs **24/7 on an AWS EC2 (us-east, Amazon Linux)** box under systemd
  (service `copybot`), trading a small (~$25) real-money canary.
- Operate it with `pnl` / `botlogs` / `botssh` (see `migration.md`); config is
  `mm-live-copy-canary.toml`; secrets live in `~/copybot/.env` on the box.

## Strategy history (why the code is shaped this way)

1. **Risk-free arbitrage** (original) — binary / NegRisk / cross-market / general
   LP-solver arb. Present in `crates/app/src/strategy/arb.rs`; neutered (huge edge
   thresholds) in the copy canary.
2. **Market-making / reward-farming** — built to farm Polymarket's liquidity
   rewards (`strategy/mm.rs`, `reward_score.rs`, `quote_policy.rs`, `signals.rs`).
   **Abandoned: it lost money to adverse selection.** Kept in-tree but OFF. (The
   rewards program is real but is an infra/latency arms race dominated by pros;
   not viable at our size/capital — see the web research summarized in chat.)
3. **Smart-money copy-trading** (current) — an edge validated offline
   (`crates/backtest`, EdgePerBet ranking: ~68.7% hit, +25.6%/bet, Sharpe ~0.30 at
   n=150, out-of-sample), then a live executor (`strategy/copy.rs`).

## How the copy strategy works (`crates/app/src/strategy/copy.rs`)

- **Whitelist**: rank top leaderboard traders by **EdgePerBet** (do their entry
  prices beat the market's outcome). Shared, tested ranking lives in
  `crates/ingestion/src/smart_money.rs` (also used by the backtest).
- **Universe = the whitelist's ACTIVE markets** (their open positions), synced by
  condition-id via the confluence path. Built once in `main.rs` and shared into
  the strategy (`with_initial_whitelist`) so the heavy rank isn't run twice. A
  signal in a market the snapshot missed is synced **on-demand** at signal time.
- **Signal**: poll each whitelisted trader's recent `/trades`; a fresh BUY within
  `reaction_window_secs` becomes a candidate. `select_signals` keeps the **most
  recent** buy per `(market, outcome)` (freshest ⇒ lowest entry drift).
- **Entry**: freshness-gated (`max_drift`) taker **FAK BUY of the trade's exact
  `asset`** (the token the trader bought), sized by per-position / capital / gross
  caps and the 5-share venue floor.
- **Exit sweep (every poll)**: (a) **follow-exit** — the source trader SOLD
  (their tape is fetched even if they left the whitelist); (b) **stop-loss** —
  marked down ≥ `stop_loss_pct`; (c) **resolution** — redeem via the M6 relayer.
- **Restart-safe**: open positions persist to the `copy_positions` table and are
  **reloaded on startup** (`reload_positions`) — re-registering the token on the
  venue, rebuilding `open`, and seeding inventory — so a restart resumes
  management instead of orphaning + double-deploying.

## Key files

- `crates/app/src/strategy/copy.rs` — the strategy (signal select, entry, exit
  sweep, on-demand resolve, position reload, persistence).
- `crates/app/src/main.rs` — wiring: whitelist-driven universe, `build_copy_tradeable`
  (**ASSET-keyed** map), venue/relayer, capital carve, service startup.
- `crates/execution/src/live.rs` — `LiveVenue` (CLOB REST/WS, order submit,
  `ensure_token` for on-demand token registration).
- `crates/execution/src/sign.rs` — order signing; `clob_amounts` (limit) and
  `clob_market_amounts` (market/taker rounding).
- `crates/store/{lib,writer,read}.rs` — SQLite persistence: `copy_positions`
  (open positions + source trader), `fills`, `day_realized` (loss cap ledger).
- `crates/ingestion/src/{data_api,smart_money,sync,confluence}.rs` — Data API,
  ranking, universe sync.
- `crates/backtest/*` — offline copy-edge validation (`pm-backtest`).

## Critical fixes made during the live canary (do NOT regress)

1. **Complement mispricing.** The token was resolved by `(condition, outcome_index)
   → registry yes/no`, which mis-mapped and BOUGHT THE OPPOSITE SIDE of the smart
   money. Fixed by keying the tradeable map on the trade's `asset` (venue token
   id) — `build_copy_tradeable`, `TradeTokenInfo`, and the entry lookup all use
   `asset`. **Never** reintroduce an outcome-index→yes/no assumption.
2. **Market-order amount precision.** Taker (market) orders must round the USDC
   leg to 2 decimals and the shares leg to 4, or the CLOB rejects with
   "invalid amounts". See `clob_market_amounts` (taker path only — limit/maker
   orders keep the µ-exact `clob_amounts`).
3. **On-demand market sync.** Unsynced signals trade immediately via
   `resolve_ondemand` + `LiveVenue::ensure_token` (session-local synthetic
   `TokenId` above the registry range).
4. **Latest-buy selection.** `select_signals` keeps the most-recent buy (was the
   earliest → stale reference → high drift → everything rejected).
5. **Position reload / persistence.** `copy_positions` table + `reload_positions`
   make restarts safe.

## Config (`mm-live-copy-canary.toml`)

- `[capital]` bankroll_usd; `[strategies.copy]` enabled/live/capital_usd (carved
  out of the bankroll — Σ strategy capital ≤ bankroll, enforced in
  `wiring::strategy_envelopes`).
- `[copy]` per_position_usd, max_concurrent_positions, max_gross_usd,
  stop_loss_pct, max_drift, reaction_window_secs, min_bets, top_n,
  whitelist_refresh_secs, signal_poll_secs, follow_exit.
- `[inventory]` / `[live]` — stop-loss + daily/session loss latches (persist
  across restart via `day_realized`).
- `auto_restart_secs = 0` — OFF for copy (a periodic process re-exec would churn;
  the position reload wasn't designed to be triggered on that cadence here).
- `[strategies.mm].enabled = false`, arb neutered via `[edges]`, confluence
  superseded by the copy whitelist-driven universe.

## Deployment (`deploy/` + README "24/7 deployment")

- `deploy/copybot.service` — systemd unit: auto-restart, boot-start, journald.
  `ExecStart` pipes the confirmation phrase (headless has no interactive stdin).
- `deploy/setup.sh` — idempotent on-box setup. Build deps: `gcc`, **`cmake`** and
  **`clang`/libclang** (both required by the `highs-sys` LP-solver crate), plus a
  2 GB swapfile (Rust release build OOMs on a 1 GB box), Rust, release build,
  install+enable the service.
- `deploy/status.sh` — DB status (positions, deployed, fills, day P&L).
- Persistent DB at `~/copybot/data/copy-canary.sqlite` — the reload depends on it;
  never point `--db` at `/tmp`.
- **One bot per wallet** — never run two instances against the same deposit
  wallet (they double-trade and blow past caps).

## Known gaps / good next patches

- **On-chain reconcile guard**: the reload trusts the DB. A crash mid-close could
  leave a stale `copy_positions` row → a phantom sell attempt (rejected + retried,
  not catastrophic). Add a best-effort Data-API positions check on reload to drop
  rows the wallet no longer holds.
- **On-chain adoption of orphans**: positions opened before the reload feature (or
  bought manually) are NOT managed. Add startup adoption of on-chain holdings
  (stop-loss + resolution only; follow-exit is impossible — the source trader
  isn't recoverable from chain). NOTE: a handful of such orphans exist on the
  wallet from early laptop runs; they ride to resolution unmanaged.
- **Remote dashboard**: only `journalctl` + `status.sh` today. The rich TUI is
  in-process; a stream-to-laptop dashboard (e.g. NDJSON over an SSH tunnel) is
  unbuilt.
- **Live merge**: `NotSupportedLive` — redeem (via the relayer) works; complete-set
  merge is paper-simulated only.
- **Latency/stability**: WS reconnects were frequent from a non-US client — hence
  the US-East EC2 box. Keep the bot US-hosted.

## Safety model

Capital carve (Σ strategy capital ≤ bankroll) · per-position / gross / concurrency
caps · stop-loss · **persistent** UTC-day + session loss latches (survive
restart) · freshness/drift gate · live gating (typed phrase headless, `l` key in
TUI) · start-paused when live is held.

## Build / test

- `cargo build --bin arb`; `cargo test --workspace`; `cargo clippy` is strict —
  fix all warnings.
- Sandbox note: the `libsqlite3-sys` build script can panic under a restricted
  sandbox (no network / redirected target). Run builds & tests **outside** the
  sandbox (allowlist) or with a writable `CARGO_TARGET_DIR`.

## Operate / debug

- `pnl` (→ `deploy/status.sh`) — open positions, deployed capital, fills, day P&L.
- `botlogs` (→ `journalctl -u copybot -f`) — watch the `copy: entry funnel` line:
  `candidates`, `entered`, `skip_drift`, `skip_at_cap`, `skip_untradeable`,
  `skip_no_size`, `already_holding`. No funnel line ⇒ no fresh whitelisted buys.
- DB tables of interest: `copy_positions` (open + source trader), `fills`,
  `day_realized`.
