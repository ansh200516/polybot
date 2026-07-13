# BTC "Up or Down 5m" — Trading Strategy Design

**Date:** 2026-07-13
**Status:** Draft for review
**Author:** brainstormed with Claude (research + 42-day backtest)
**Venue:** Polymarket **international CLOB** (eligibility confirmed — same entity as the live copy trader)

---

## 1. Goal & context

Design a trading strategy for Polymarket's **"BTC Up or Down 5m"** market: a recurring binary that locks a strike (the "Price to Beat") at each 5-minute window's open and resolves **UP if the close ≥ the strike**, else DOWN. $1 payout per correct share. Parallel markets exist for ETH/SOL/XRP/DOGE.

We already operate a live Polymarket executor (Rust, CLOB v2, sigType-3 signing) with risk, persistence, fills, and EC2/systemd deployment. This project adds a **new strategy** on that platform — it does **not** rebuild the plumbing. It is a **feature addition that runs in parallel** with the existing copy bot (same `StrategyHost` process, same wallet, **config-gated**); the copy bot is **not modified or replaced**, and the new bot is observable through the **same `pnl` command**. See §6.1.

**The deliverable of this strategy is disciplined, evidence-gated capital deployment — not a forecasting bot.** The research below is unambiguous that naive prediction loses here; the edge (if any) is microstructural and must be *measured live before it is traded*.

---

## 2. Load-bearing market mechanics (verified against Gamma + CLOB API, docs, Chainlink)

| Aspect | Fact | Confidence |
|---|---|---|
| **Resolution oracle** | **Chainlink BTC/USD Data Streams** — a multi-exchange *aggregate*, signed ~every 200 ms; settled automatically by Chainlink Automation at the window-end timestamp. **Not** UMA, **not** a single exchange, **not** Pyth. | High |
| **Strike / close** | Single-timestamp snapshots of the Chainlink aggregate at window open and window close. Ties (close == strike) → **UP** (structural micro-bias). | High |
| **Trading window** | Continuous, **no pre-close freeze**; practical last-execution ≈ **T-15–30 s** due to ~2–5 s relay/settlement latency. | Med-High |
| **Taker fee** | `fee = shares × 0.07 × p × (1−p)` → **3.5% of notional at 50¢, 0.7% at 90¢, ~0 at the extremes.** | High |
| **Maker fee** | **Zero.** Makers additionally earn (a) **Liquidity Rewards** for resting near mid and (b) **Maker Rebates** = 20% of taker fees in that market. | High |
| **Liquidity Rewards** | Score `S = ((v−s)/v)² × size`, `v = max_spread = 4.5¢`, `min_size = 50 shares`; two-sided required outside 10–90% (÷3 single-side penalty inside); book sampled ~1/min; paid `your_score / total_score × daily_pool`. **Per-window pool size unknown — must measure live.** | High mechanics / Low pool $ |
| **Tick size** | **Market-specific (0.01 or 0.001)** — must be read live per window; never hard-code. | High |
| **Min order** | 5 shares (but ≥50 to earn rewards). Order types: GTC, GTD, FOK, FAK. | High |
| **Data access** | Gamma API (metadata/outcomes), CLOB `/book` + `/prices-history` (**coarse ≥12 h for closed markets — no fine history for expired windows**), Data API/subgraph for fills. Fine historical order books need a paid vendor or **our own live logging**. | High |
| **Downtime** | Matching-engine restarts → HTTP **425**, then ~2-min **post-only (503)** window — can black out a full 5-min cycle. Must be handled. | Med-High |
| **Hosting** | Matching engine on **AWS eu-west-2 (London)** (triangulated, not officially published). | Med |

---

## 3. The edge thesis (physics + the honest caveat)

**Backtest:** 42 days of real Binance 1-second BTC data, 12,059 clean clock-aligned 5-min windows, lookahead-guarded.

### What is bankable science
5-min BTC is ~**driftless** (drift ≈ −0.009σ, negligible) with **$94 std** and **fat tails** (excess kurtosis +18). This makes **terminal convergence** a mechanical fact — a small lead late in the window is nearly decisive:

| τ-to-go | P(leader wins), lead $10–25 | $25–50 | $50–100 |
|---|---|---|---|
| 60s | 0.745 | 0.875 | 0.962 |
| 30s | 0.795 | 0.927 | 0.988 |
| **15s** | **0.857** | **0.958** | **0.996** |

At **T-15 s a $9 lead ⇒ 95%**; $32 ⇒ 99%. Rule "buy leader at T-15 s when |z|≥1.5" wins **98.0%** and fires on **65%** of windows. Symmetric (UP-leader 0.916 vs DOWN 0.909), regime-independent (split-sample 0.984/0.976), stable across all UTC hours.

A second, independent **model edge**: because returns are leptokurtic, the true leader wins ~**3–5¢ more often** than a Gaussian/Black-Scholes-pricing market-maker would quote. Real vs any counterparty pricing on a normal distribution.

### The caveat that governs everything
That 98% only *pays* if the book still offers the leader **below ~97.5¢ (breakeven)** that late. But depth **contracts ~6% per 10× fewer seconds-to-close**, spreads blow to 5–10¢, and **we have no live Polymarket order-book data** — so the size of that gap is **unmeasured**. Every rigorous external source warns the gap is small and contested:
- Academic (222M trades): above-random forecasters earn **negative** returns; profit is execution quality; makers capture +2.5¢/contract.
- Live postmortems: **25–27% win rate**, "522× in sim → −49.5% live."
- Polymarket **actively defends** this: 250–500 ms maker speed-bump, dynamic taker fee that peaks at 50/50.

**Conclusion:** the strategy's core job in Phase 1 is to *measure the gap* before risking capital on it. Micro-momentum at entry (tested) is **dead** after costs and is a non-goal.

---

## 4. Non-goals (explicitly not betting on these)

- **Forecasting alpha** ("predict the next 5 min"). The data says this loses.
- **Latency pickoff / stale-quote racing.** Owned by colocated London/Dublin HFT; taxed and speed-bumped. Dead for us.
- **Rewards farming as the thesis.** Ecosystem crypto rebate pool ≈ $40k/day across *all* makers; a solo's 1–3% share ≈ $400–1,300/day *at peak volume* (far less now). Rewards are a **bonus**, sized only after §5 Phase-3.
- **Micro-momentum entry signals.** No edge after a 1¢ spread.

---

## 5. Staged plan ("prove-then-scale") with hard go/no-go gates

The sequencing is the strategy. Each phase must pass its gate before the next begins.

### Phase 0 — Preconditions (blocking)
- **Eligibility:** confirmed (intl CLOB, existing entity). ✅
- **Latency baseline (measured 2026-07-13):** from the Pune / Azure Central India box, the Cloudflare edge is ~5 ms away, but **origin (London) round-trips show up as ~150–350 ms TTFB** on dynamic CLOB/Gamma endpoints — well above the ~200 ms cancel-replace bar for making. ⇒ Phases 1–2 are fine from India; **Phase 3 making requires the London VM** (§6.1).
- **Reward-eligibility check:** pull the live 5-min market's `rewards.rates` / Rewards tab to confirm Liquidity Rewards actually fund these ultra-short windows and record the per-market daily pool (flagged unconfirmed in research).
- **Market rotation:** implement discovery of the current window's `conditionId` + token IDs via Gamma (`slug = btc-updown-5m-<unix>`), plus the roll to the next window every 5 min, and per-window tick-size read.

### Phase 1 — Shadow / measure (ZERO capital, current India box OK)
Run the spot feed + fair-value model **read-only**. For every window, log to SQLite:
- strike, our composite spot, Chainlink value (if obtainable), the full book top-N, and the model's fair P(up), at 1 Hz — **densely in the last 60 s**.
- the resolved outcome.

**Compute the decisive metric:** at T-{30,15,10} s, for windows where a leader has |z| ≥ threshold, the distribution of **(fair − best available offer for the leader)** net of the 7% fee, and how often it clears a buffer (e.g. ≥2¢).

**Gate 1→2 (GO only if):** a harvestable gap exists — e.g. median net edge ≥ **2¢** on ≥ **N windows/day** at T-15 s. **If the book is already efficient (gap ≈ 0) → STOP. Do not trade.** This is the kill criterion that the "522×→−49.5%" bots skipped.

### Phase 2 — Micro taker (Approach B; latency-tolerant, India box OK)
Only if Gate 1 passes. Never rests a quote (no adverse selection). Logic per window:
- Maintain fair P(up) from spot + τ.
- At **T ≤ 20 s**, if |z| ≥ threshold **and** the leader is offered ≤ (fair − fee − buffer), send a **marketable FAK** for a **micro** size (e.g. $5–25 notional), operating near price extremes where the fee ≈ 0.
- Flatten/settle passively (contract self-resolves).
- Hard caps: per-window notional, daily loss (reuse `InventoryRisk` daily-loss + `RiskEngine` session-loss + sticky kill).

**Gate 2→3 (GO only if):** realized net PnL over ≥ **K** trades has **lower confidence bound > 0**, realized win rate ≈ modeled, and markout (+5 s/+30 s) shows we're not being adversely selected. **AND** the executor has been **relocated to London** (Phase-3 prerequisite).

### Phase 3 — Maker overlay (Approach A; requires London colocation)
Only if Gate 2 passes and colocated. Two-sided model-priced quoting:
- Quote both sides within **4.5¢ of mid**, ≥ **50 shares**, tick-aligned, to earn Liquidity Rewards + Rebates + spread — at **zero fee**.
- Re-price continuously off the composite spot + τ; quotes converge toward 0/1 as a leader emerges (capturing the leptokurtic edge vs Gaussian MMs).
- **Adverse-selection controls:** markout-driven widening/size-cut on fast spot moves; **pull quotes in the last ~10 s** (pin/manipulation risk); inventory skew; **self-trade prevention** (no self-cross → wash-trade ban risk).
- Handle **425/503** with post-only backoff.

**Kill criteria (all phases):** daily-loss floor, consecutive-error breaker, session-loss halt, sticky kill switch — all already in `RiskEngine`/`InventoryRisk`.

---

## 6. Architecture — reuse map (~70% exists)

**Reuse as-is:** CLOB v2 sigType-3 signing (`crates/execution/src/sign.rs`), order verbs — FAK market, GTC/GTD limit, cancel, best bid/ask, rate limiter (`crates/execution/src/live.rs`), the `Strategy` trait + `StrategyHost` (`crates/app/src/strategy/mod.rs`, `host.rs`), risk (`crates/risk/src/lib.rs`, `inventory.rs`), SQLite store, fills (user WS + `/data/trades`), heartbeat, TUI, config, systemd/EC2 deploy.

**Build new (only these):**
1. **BTC spot feed** — `crates/ingestion/src/spot.rs`: multi-exchange composite (Coinbase/Binance/Kraken/Bitstamp median or VWAP) to approximate the **Chainlink aggregate** (NOT the Polymarket UI feed — that's a pickoff vector); expose a `watch`-able latest price. Investigate consuming **Chainlink Data Streams BTC/USD directly** for settlement fidelity; Phase 1 measures the basis between composite and observed strikes.
2. **Fair-value model** — `p_up = Φ(d / σ_τ)`, `d = S_t − K`, `σ_τ = σ_5min·√(τ/300)`, with **causal** EWMA(1-min squared $-returns, ~120-min half-life) vol (√-time exact from 1-min up; 1-s deflated ~23%). Empirical leptokurtic surface as the ground-truth pricer. Snap to per-window `TickSize`.
3. **5-min market discovery/rotation** — Gamma-based current-window resolver + roll + tick read.
4. **Strategy loop** — `crates/app/src/strategy/btc5m.rs`, modeled on `strategy/copy.rs`; implements the Phase-1/2/3 logic behind config flags.
5. **Shadow logger** (Phase 1) — trivial given the book feed already exists; writes the §5 metrics to SQLite.

**Config:** add `[strategies.btc5m]` + `[btc5m]` to `crates/config/src/lib.rs` (mirror `CopyCfg`/`CopyParamsCfg`); wire in `main.rs` + `wiring.rs`.

**Top 5 files touched:** `strategy/mod.rs`, `config/lib.rs`, `main.rs`, `wiring.rs`, new `strategy/btc5m.rs` (+ new `ingestion/spot.rs`).

---

## 6.1 Parallel operation, deployment & `pnl` (feature-addition wiring)

**This bot runs in parallel with the copy bot — it does not replace it.** It is added as a second `Strategy` under the existing `StrategyHost` in the **same `arb` process**, gated by `[strategies.btc5m].enabled` (default **false**). Deploying the new binary therefore does **not** change copy-bot behavior until btc5m is explicitly enabled — and even then it starts read-only (Phase 1 shadow).

**Why same-process, not a second service:** both strategies trade the **same Polymarket wallet**. A second independent process on that wallet would race on collateral and, worse, each process's on-chain reconcile would treat the other's positions as foreign/stale and try to prune them. One process = one reconcile view, one collateral manager, coordinated caps, one kill switch — exactly what `StrategyHost` is for (copy + mm already coexist this way).

**Integration guardrail (reconcile scoping):** position management must be per-strategy. Copy owns `copy_positions` (its trader/markets); btc5m gets a new **`btc5m_positions`** table (BTC 5-min `conditionId`s). Verify copy's prune-stale / settle-resolved paths never touch btc5m rows and vice-versa. Capital is a separate carve (`[strategies.btc5m]` capital + its own `RiskConfig`/`InventoryConfig`); copy's caps are untouched.

**`pnl` parity:** `pnl` runs `deploy/status.sh` (locally via `ssh -i $COPYBOT_KEY $COPYBOT_HOST`, on-box directly). Realized PnL is already strategy-tagged (`day_realized.strategy`). Extend `status.sh` with a **"BTC 5M BOT"** section — current-window exposure, fills, `realized P&L today (btc5m)` from `day_realized WHERE strategy='btc5m'`, live marks from the Data-API — keeping the shared ACCOUNT block. The `pnl` alias is **unchanged**; one command shows both bots (optionally `pnl copy|btc` to filter).

**London (Phase 3) implication:** the maker leg needs a London VM. Default: relocate the shared process to London (the copy bot comes along — it's signal-driven, not latency-sensitive, so London is fine), keeping one wallet + one `pnl`. Fallback if copy must stay in India: run the Phase-3 maker as a **separate London service with its own wallet** (full isolation), and extend `pnl` to aggregate both hosts. Decide at Phase-3 entry.

---

## 7. Risk & correctness controls

- **Settlement fidelity:** key all decisions off the Chainlink aggregate proxy, never the Polymarket UI/display feed. Log basis; alert if composite ↔ strike basis drifts.
- **Per-window isolation:** positions self-resolve in ≤5 min → bounded inventory; but carry **pin risk** at the exact close — Phase-3 pulls quotes in the last ~10 s.
- **Reuse** `InventoryRisk` (net/gross caps, stop-loss, daily-loss) + `RiskEngine` (session-loss, drawdown, kill).
- **Downtime:** explicit 425/503 handling with post-only backoff; treat a blacked-out window as no-trade.
- **Self-trade prevention** in Phase 3 (ban-risk).
- **Reward-program volatility:** must remain net-positive with rebates → 0 (rebate regime is new and discretionary).

---

## 8. Instrumentation & success metrics

Phase 1 logs feed a daily report: for each τ-bucket, (fair − best-offer) net edge distribution, harvestable-window count, composite↔strike basis. Phase 2+: realized vs modeled win rate, EV ¢/trade, markout at +5/+30 s (adverse selection), live-vs-quote slippage, per-window fill rate. Gates in §5 are defined on these.

---

## 9. Open questions / risks

- **Is the last-15-s gap real and sized?** The whole thesis. Phase 1 answers it; if no, we stop.
- **Do Liquidity Rewards fund 5-min windows, and how large is the pool?** Unconfirmed; Phase 0 measures.
- **Chainlink Data Streams direct access** — feasible/affordable, or composite-proxy only?
- **Relocation ROI** — India→London ~120–135 ms; is a London VM worth it *before* Phase-3? (Phases 1–2 don't need it.)
- **Capacity** — thin windows (~$273) cap size; is aggregate across 5 assets enough to matter?

---

## 10. Concrete next actions

1. **Latency probe (read-only, on user's go):**
   `ssh arnab@135.235.139.216 'for h in clob.polymarket.com; do curl -o /dev/null -s -w "connect=%{time_connect}s ttfb=%{time_starttransfer}s\n" https://$h/; done'`
2. Spec **approved 2026-07-13** → **writing-plans** for the Phase-0/1 implementation plan: market discovery/rotation + spot feed + fair-value model + shadow logger + `[strategies.btc5m]` config gate (default off) + `btc5m_positions` table + `status.sh` `pnl` extension. Phase-0/1 deploy ships the new binary with **btc5m in shadow — copy-bot behavior unchanged**.

---

*Backtest scripts:* `…/scratchpad/{download,parse,lib,s2_dynamics,s3_model,s4_backtest,s5_robust}.py` (42d 1-s BTC, reproducible).
