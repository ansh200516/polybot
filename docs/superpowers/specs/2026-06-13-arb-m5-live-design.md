# M5 — Live Execution Design

Date: 2026-06-13. Parent spec: `2026-06-12-polymarket-arb-bot-v2-design.md`
(§14 execution, §17 TUI, §22 milestones). Status: user-approved design;
implementation plan to follow.

## Goal

Replace the M4 go-live stub with a real Polymarket CLOB trading path, gated
hard enough that the first funded session risks tens of dollars, not the
bankroll. Exit gate (parent spec M5 row): EIP-712 signatures verified against
Polymarket's published vectors, plus one funded canary round-trip — at least
one pure-buy basket ≤ $10 filled live, reconciled end-to-end (venue records ↔
store rows ↔ P&L report).

## Decisions locked with user (2026-06-13)

| Decision | Value |
|---|---|
| Account type | Email/Magic login → CLOB `signature_type 1` (POLY_PROXY): funds in Polymarket proxy wallet, signer = EOA exported from Magic settings |
| Secrets | Environment variables only (`PM_PRIVATE_KEY`, optional `PM_API_KEY` / `PM_API_SECRET` / `PM_API_PASSPHRASE`). Never in config, git, logs, or SQLite |
| Canary caps | $10 per-basket basis cap, $25 hard session loss cap (bid-marked, no auto-resume) |
| Funding | User funds ~$50 during the build; funded round-trip is the final task |
| Approach | Recon-first, staged: recon doc → safety pre-reqs → signing → auth → LiveVenue → wiring → shadow → funded canary |

## Recon first (RECON-M5.md)

All read-only / signature-only, before any code relies on the answers:

1. `signature_type 1` semantics: exact `maker` (proxy) / `signer` (EOA)
   fields, proxy address derivation or lookup for a Magic account.
2. API key derivation/creation via L1 `ClobAuth` EIP-712 signature; L2 HMAC
   header recipe (cross-check `py-clob-client`).
3. Allowances: what a Magic/proxy account needs (likely pre-approved via the
   proxy; verify) — and what to do if not.
4. **Minimum order sizes** and tick/size constraints per market — the $10
   canary must clear per-leg minimums or be rejected whole.
5. FAK (Fill-and-Kill) semantics: partial-fill reporting, terminal states,
   order-id correlation, trades query shape.
6. Current fee schedule re-verification (parent spec §6 caveat).
7. Trading-endpoint rate limits (separate bucket from market-data).
8. Record live request/response fixtures (sanitized) for the mock-server
   tests.

## Architecture

New/extended units, all behind existing seams. The coordinator, basket
executor, store schema, and TUI keep their shapes.

- **`pm-execution/src/sign.rs`** — EIP-712 signing of the CLOB `Order`
  struct via `alloy`, `signature_type 1`. Pure (no I/O). Unit-tested against
  Polymarket's published example vectors; vectors land in the repo as
  fixtures.
- **`pm-execution/src/auth.rs`** — L1 `ClobAuth` signature → derive/create
  API key at startup (skipped when `PM_API_*` env vars provide one); L2 HMAC
  headers on every trading request, timestamped via the existing
  `server_time` offset. Secrets are read once from env into a non-`Debug`
  newtype that never implements `Display`.
- **`pm-execution/src/live.rs`** — `LiveVenue: ExecutionVenue` (parent spec
  §14): per-leg FAK limit orders, order-id correlation in `SubmitOutcome`,
  fills collapsed to terminal outcomes (venue.rs M5 note), open-orders +
  recent-trades queries for restart reconciliation, its own deterministic
  rate-limiter bucket. Split/merge venue calls return a `NotSupportedLive`
  failure variant — unreachable in practice because live dispatch is
  pure-buy-only (below), but the trait stays total.
- **`pm-app` wiring** — `--live` and `--shadow` flags; typed confirmation
  phrase on stdin for headless `--live`; TUI `l` modal completes (replaces
  the M4 "unavailable until M5" stub); live gates in the coordinator; canary
  config; safety pre-reqs (below).
- **Safety pre-reqs** (flagged in code "before real money"):
  - Capped mid-mark for the drawdown feed (coordinator.rs:496): mid is
    clamped to `bid + mid_spread_cap_ticks` so a wide/stale ask cannot delay
    the halt. Applies in all modes — the artifact exists in paper too.
  - `ws_connected` heartbeat (publisher.rs:200 proper fix).
  - Per-feed staleness display in the TUI health panel.

## Mode ladder & live gates

`paper` (default, byte-identical to M4) → `shadow` (`--live --shadow`: real
auth, real signing, real sized baskets; the final submit is logged, not
sent) → `live`. Live is a **start-time decision** — a session is either all
paper or all live, never mixed. `--live` arms the binary (env secrets
required, auth runs at startup). Headless: the confirmation phrase must be
typed on stdin at startup or the binary exits. TUI: the session starts with
live dispatch held, and the `l` modal's typed `live` releases it; without
`--live` the modal refuses as in M4 (message now: "start with --live to
arm").

Live-mode-only gates in the coordinator, on top of all existing risk checks:

- **Pure-buy-only dispatch.** Every leg `Buy` and `splits` empty, else the
  basket is rejected with a visible `live_rej` counter in the health panel —
  never paper-filled (no mixing of real and simulated fills in one session).
  Buy-only still covers C1Long, C2 NO-set buys, buy-formulated C3, and
  buy-only C4 worlds. **Exception: unwind sells of owned tokens are
  permitted** — selling a held balance is a gasless CLOB order, so the M3
  repair-or-unwind path works live for partially-filled buy baskets.
- **Canary caps.** Basket basis ≤ `live_basket_cap` ($10 default). If a
  basket cannot both respect the cap and clear venue per-leg minimums it is
  rejected whole, never resized upward. Session loss halt: bid-marked
  realized+unrealized < −`live_session_loss_cap` ($25 default) trips a new
  latched `SessionLoss` halt — dispatch stops for the session, TUI badge
  shows it, kill-file and `k` remain the harder stops.

Redeeming winners stays manual (Hold strategy) until M6.

## Errors & reconciliation

FAK means nothing rests on the book: every submit reaches a terminal state
(filled / part-filled / killed) within the request—poll window. Rejects map
to the existing persisted order state machine (write-ahead rows before
network calls, UUIDv7 idempotency keys — unchanged from M3). Restart
reconciliation extends M3's open-orders sweep with `LiveVenue`'s venue-side
open-orders + recent-trades queries; mismatches are logged and expire the
local row (conservative: never resubmit on ambiguity). Rate-limit and 5xx
responses retry within the fill window only; past it the leg is treated as
killed and the basket falls into repair/unwind.

## Config & env

```toml
[live]                      # all defaults are the canary values
basket_cap_usd    = 10.0    # live per-basket basis cap
session_loss_usd  = 25.0    # latched dispatch halt, bid-marked
confirm_phrase    = "I understand this trades real money"

[risk]                      # addition — applies in ALL modes (the M3
mid_spread_cap_ticks = 5    # artifact exists in paper too): drawdown-feed
                            # mid is clamped to bid + cap
```

Env: `PM_PRIVATE_KEY` (hex, `0x` prefix accepted and stripped),
`PM_API_KEY` / `PM_API_SECRET` / `PM_API_PASSPHRASE` (optional, skip
derivation), `PM_PROXY_ADDRESS` (optional override; otherwise derived/looked
up per recon).

## Testing

In increasing realism; the existing 332-test suite stays green throughout
(paper behavior must not change):

1. Signature unit tests vs Polymarket's published vectors (fixtures in
   repo).
2. Auth-header vectors cross-checked against `py-clob-client` outputs.
3. `LiveVenue` vs an in-process mock CLOB server replaying recon-recorded
   fixtures: accept, reject, partial fill, timeout, malformed body, rate
   limit.
4. Coordinator gate units: pure-buy filter, cap/minimum interplay,
   `SessionLoss` latching, typed-confirm and shadow gating.
5. Shadow session on live market data — the dress rehearsal; verifies
   auth + signing + sizing against the real API with zero submission.
6. Funded canary session — the exit gate, operator-run.

## Out of scope (M5)

Sell-leg/split classes live (C1Short et al.), automated redemption/merging,
relationship auto-suggester, deployment docs (M6), raising caps beyond
canary values (operator decision after canary results).
