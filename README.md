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

## M4: TUI dashboard

`pm-tui` adds a full-screen Ratatui dashboard to the `arb` binary. The TUI
is active whenever stdout is a real terminal and `--headless` is not given;
piped/cron invocations stay headless automatically.

### Dashboard layout

The screen is divided into a header bar and five panels:

- **Opportunities** (upper-left, 62%) — rolling feed of detected arb
  opportunities: age, class (1–4), market name, edge bps, size, estimated
  dollar value, and a `*` marker when the basket was dispatched.
- **Positions** (upper-right, 38%) — open positions marked to the current
  live bid: market, quantity, basis cost, and live mark value.
- **Fills & Orders** (lower-left, 62%) — two sub-tables: recent fills (age,
  market, side, price, quantity, cash) and recent orders (age, order ID,
  state, detail).
- **Health** (lower-right, 38%) — live gauges (markets tracked, books live,
  supervisors, WS frames, reconnects, opportunities, baskets dispatched) and
  detect/dispatch latency percentiles (p50/p99 in µs).
- **Log** (bottom strip) — scrolling ring-buffer of structured log lines from
  the session; ↑/↓ to scroll, auto-follows tail at offset 0.

The header bar shows: mode badge (`PAPER` / `LIVE`), session uptime, optional
status badge (`PAUSED` / `HALT:<reason>` / `KILLED`), and two equity figures:

- **`equity(bid)`** — cash + open positions marked at the best live bid.
  Conservative and durable: used for the human-readable P&L report.
- **`risk-equity(mid)`** — cash + positions marked at mid. This is the feed
  the drawdown-halt logic watches. The M3 live-run artifact fix (spread of an
  open basket no longer trips the halt) applies here: mid-marking an
  open-basket spread was causing false daily-drawdown halts; the fix ensures
  quiet books on a live delta feed count as current before computing the
  mid-mark.

A footer line repeats the key hints at all times.

### Key bindings

| Key | Action |
|---|---|
| `p` | Toggle pause dispatch (coordinator stops admitting new baskets; ingestion continues) |
| `l` | Go-live modal — type `live` and press Enter to confirm; answers "live venue unavailable until M5 — staying in paper" (M5 stub) |
| `k` | Kill switch — y/N confirm modal; `y` trips the kill flag cleanly |
| `q` | Quit the dashboard and end the session |
| `↑` / `↓` | Scroll the log panel (↑ = back in history; auto-follow resumes at offset 0) |
| `Ctrl-C` | Quit (works in any modal state) |

### Config defaults (`[tui]` section)

| Key | Default | Minimum |
|---|---|---|
| `refresh_ms` | 100 | 50 |
| `feed_rows` | 50 | 1 |
| `fills_rows` | 20 | 1 |
| `log_lines` | 200 | 1 |

### `--headless` flag

Pass `--headless` to suppress the TUI and fall back to M3-style structured
`tracing` output to stdout. The binary also detects a non-tty stdout
automatically (pipe, file redirect, cron), so no flag is needed in CI or
scheduled jobs.

### Startup

In TUI mode tracing goes to the in-screen log ring, so before the dashboard
appears `arb` prints a plain stdout notice while the universe sync runs:

    arb: assembling market universe (rate-limited CLOB sync) ...
    arb: the dashboard will start when the sync completes.

The CLOB metadata fetch is bounded: markets are visited in registry order and
fetching stops once `max_markets` would-be-accepted markets are in hand
(`fetch_clob_bounded`; the accept/skip gate is shared with
`assemble_registry`, so the assembled registry is identical to one built from
an exhaustive fetch). Before this bound, every member market on the fetched
gamma keyset page (~1,100 on page one) was fetched at the rate-limited
5 req/s — ~4 minutes of silent black screen before the dashboard. Now
`--max-markets 20` starts in roughly 7 s.

### pty smoke (Apple Silicon dev machine, 2026-06-13, post fetch-bound fix)

    script -q /tmp/m4-tui-smoke.out \
      sh -c 'stty rows 40 cols 140; exec ./target/release/arb \
        --duration-secs 20 --max-markets 20 --db /tmp/m4-tui-smoke.sqlite'

The `stty` matters: a non-interactive `script` pty reports a 0×0 window and
Ratatui renders nothing into a zero-area terminal. The earlier smoke (12 KB
capture, no panel titles) verified only the alt-screen lifecycle, not actual
rendering — its claim that missing panel titles are "expected for any Ratatui
pty recording" was wrong.

| Check | Result |
|---|---|
| exit code | 0 |
| total wall time (20 s session) | 31 s (≈7 s sync + session + shutdown) |
| capture size | 44,792 bytes |
| startup notice lines on stdout | yes |
| alt-screen enter / leave (`[?1049h` / `[?1049l`) | both present |
| panel titles (Opportunities, Positions, Fills, Health) | all present |
| header (PAPER badge, equity) | present |
| `arb session result: healthy=true` | 1 |
| SQLite DB created | yes |

Interactive acceptance (live scrolling, key-binding exercise, visual layout
verification) is performed by the operator.

## M5: live execution

`arb` can trade real money on the Polymarket CLOB behind a hard ladder of
gates. Recon ground truth lives in `docs/RECON-M5.md`; the signing and auth
recipes are vector-verified against `py-clob-client` (fixtures in
`crates/execution/tests/fixtures/`).

### Mode ladder

| Mode | Flags | What happens |
|---|---|---|
| paper | (none) | M3/M4 behavior, byte-identical. Simulated fills. |
| shadow | `--live --shadow` | Real env secrets, real auth (API key derive), real sized baskets, real EIP-712 signatures — the final submit is logged (`SHADOW: signed, not submitted`), never sent. No confirmation needed; no money can move. Cyan `SHADOW` badge. |
| live | `--live` | Real orders. Headless: the confirmation phrase must be typed on stdin at startup. TUI: session starts `LIVE·HELD` (yellow); pressing `l` and typing `live` releases dispatch — badge turns red `LIVE`. |

Live is a start-time decision: a session is all paper or all live, never
mixed. Mid-session, `p`/`k`/kill-file still pause/stop dispatch.

### Env (live/shadow only — never in config, git, logs, or the DB)

| Var | Meaning |
|---|---|
| `PM_PRIVATE_KEY` | Wallet key exported from Polymarket settings (hex, `0x` ok) |
| `PM_PROXY_ADDRESS` | Your Polymarket proxy wallet (profile page) — the order `maker` |
| `PM_API_KEY` / `PM_API_SECRET` / `PM_API_PASSPHRASE` | Optional; all three or none. Absent → derived at startup via L1 ClobAuth |

### Live gates (`[live]` config; defaults are the canary values)

- `basket_cap_usd = 10.0` — per-basket basis cap; over-cap baskets are
  rejected whole (`live_rej` counter in the health panel).
- `min_leg_shares = 5.0` — venue minimum (RECON-pinned); a basket with any
  leg under it is rejected whole, never resized upward.
- `session_loss_usd = 25.0` — bid-marked realized+unrealized below −$25
  trips a latched `SessionLoss` halt (badge `HALT:SessionLoss`); restart to
  clear. Armed only in real live (not shadow/paper).
- **Pure-buy-only dispatch**: sell/split classes (e.g. C1Short) are
  live-rejected (visible counter) until M6's on-chain split path. Unwind
  sells of tokens we own still work (gasless CLOB orders).
- Live forces `redeem = hold`: a filled C1Long keeps its complete set
  (manual redeem via the UI until M6); on-chain merge is never attempted.

### Canary sizing (`canary.toml`)

The engine sizes every basket up to `per_market_usd` ($1000). The $10 live cap
**rejects** whole baskets over the cap — it never shrinks them — so at default
sizing every pure-buy candidate is rejected and a live session never fills.
`canary.toml` sizes baskets to ~$8 (changing sizing only; all safety guards
stay at their defaults) so genuine pure-buy arbs fit under the cap. Use it for
both the shadow rehearsal and the funded canary.

### Shadow rehearsal (operator runbook)

```bash
export PM_PRIVATE_KEY=<exported key>
export PM_PROXY_ADDRESS=<proxy wallet from profile>
cargo build --release --bin arb
./target/release/arb --live --shadow --headless --config canary.toml \
  --duration-secs 600 --db /tmp/m5-shadow.sqlite 2>&1 | tee /tmp/m5-shadow.log
# checks:
grep -c "live venue armed" /tmp/m5-shadow.log               # 1 — auth derive ok
grep -c "SHADOW: signed, not submitted" /tmp/m5-shadow.log  # ≥1 — signing path exercised
grep "session result" /tmp/m5-shadow.log                    # healthy=true
sqlite3 /tmp/m5-shadow.sqlite "select count(*) from fills"  # 0 — shadow never fills
```

Without `canary.toml` (default $1000 sizing) the run logs only
`basket over canary cap` / `rejected non-pure-buy` and **zero** signs — auth is
proven but the signing path is never reached. With it, expect at least one
`SHADOW: signed, not submitted` when a pure-buy arb appears (still
opportunity-dependent; re-run if the market is quiet).

### Shadow rehearsal results

_Pending operator run._

### Funded canary results

_Pending operator run (≤$10 basket, ~$50 funded; see the M5 plan Task 14
for the reconciliation checklist)._
