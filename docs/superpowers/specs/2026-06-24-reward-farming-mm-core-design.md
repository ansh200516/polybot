# Reward-Farming Market Maker — Spec 1: Core Engine

Date: 2026-06-24
Status: Approved (design); ready for implementation planning
Scope: Spec 1 of a 3-spec program (see Roadmap). This document fully specifies
Spec 1 and sketches Specs 2–3 so the decomposition is reviewable.

## 1. Problem

The live market maker is consistently losing money. The current strategy is
**spread capture**: post a (single, confluence-favored) quote at a fixed
`spread_bps = 80` (0.8¢), re-quote every 2.5s, profit by buying below / selling
above fair. Evidence from live fills shows the opposite happening — buying at
89–90¢ and selling at 82–88¢ on a falling market — i.e. **adverse selection**:
a resting quote only gets hit when an informed trader is on the other side, so
the 0.8¢ captured is dwarfed by the 7–8¢ the market then moves.

This is structural, not a bug:
- Polymarket pays **no maker rebate on the spread itself** for most markets, so
  spread capture's only edge is mean-reversion that does not hold in trending
  (especially in-play sports) markets.
- The markets selected (smart-money / live sports) are exactly those with the
  most informed, trending flow — worst case for a passive maker.
- The loss caps don't bind: `daily_loss_usd` / `session_loss_usd` are
  session-relative and `auto_restart_secs = 1800` resets them every 30 min, so
  the bot re-arms and bleeds again each cycle (observed: ~−$29/day behind a
  nominal "$6" cap).

## 2. The actual edge: Polymarket Liquidity Rewards

Polymarket runs a **Liquidity Rewards** program (>$5M/month; World Cup 2026
incentives live Jun 11 – Jul 19, up to $52,000/game for the final). It pays
makers **daily (00:00 UTC)** for resting limit orders **near the midpoint** —
orders do **not** need to fill.

Scoring (from Polymarket docs), per one-minute sample:
- Order score `S(v, s) = ((v − s)/v)² · b`, where `v` = market `max_spread`
  (cents from mid), `s` = order's distance from the size-cutoff-adjusted
  midpoint (cents), `b` = in-game multiplier.
- `Q₁` (one side) sums `S · size` over bids on the token + asks on its
  complement; `Q₂` sums the opposite. For single-token two-sided quoting,
  `Q₁` = bid contributions and `Q₂` = ask contributions.
- `Q_min = max(min(Q₁, Q₂), max(Q₁, Q₂)/c)` for mid ∈ [0.10, 0.90]
  (`c = 3.0`); `Q_min = min(Q₁, Q₂)` outside that band (must be two-sided).
- Per sample, normalize `Q_min` across makers; sum across the epoch; the final
  normalized share × the market's reward pool = the maker's payout.

Implications that define the strategy:
- **Tightness wins quadratically** — 1¢ off mid ≈ 96% of max; 5¢ ≈ <40%.
- **Two-sided, balanced** is near-mandatory (single-sided pays 1/3 or zero).
- Express a view by skewing **sizes**, never prices.
- **Frequent cancels** reset the time-weighted score → quote **sticky**.
- Profit is the **daily reward**; fills are an incidental **cost** (inventory
  risk) to be minimized, not the goal.

Reward magnitude scales with deployed capital (~$10–200 per $10K/day in active
markets). At ~$23 the absolute payout is cents/day; the point of Spec 1 is to
**prove the machine** (delta-neutral, net-positive estimated reward, zero
bleed), then scale capital.

## 3. Goal & success criteria

Build a reward-farming quoting engine and validate it before risking
meaningful money.

Success (paper):
- Quotes are two-sided, in-band, tight, and balanced on reward-eligible markets.
- Cancel rate stays below a flicker threshold (sticky re-quoting works).
- Net inventory (delta) stays within caps; no phantom-ask rejects.
- The local score estimator reports a **positive `Q_min`** and a positive
  rough $/day estimate.

Success (tiny live, after paper):
- Real midnight-UTC reward payouts land and are within the estimator's
  ballpark; realized PnL (rewards − fill losses) is ≥ 0 over a multi-day run.
- The persistent daily loss cap halts the day at the configured limit across
  auto-restarts.

## 4. Roadmap (3 specs)

1. **Spec 1 — Core engine** (this doc): reward data layer, eligible-market
   selection, two-sided tight sticky quoting, local score estimator,
   delta-neutral inventory, persistent daily loss cap, instrumentation hooks.
2. **Spec 2 — Pro alpha layer**: adverse-selection avoidance (microprice fair
   value, book-imbalance/momentum quote-pull) and complement hedging
   (offload inventory via the YES+NO=1 relationship / NegRisk).
3. **Spec 3 — Adaptive tuning** (sketch in §13): offline/contextual-bandit
   tuning of a few guarded knobs from logged `(state, action, outcome)`.

Build order: 1 → paper-validate → tiny live → 2 → 3 → scale. Specs 2 and 3
each get their own design pass; Spec 1 lays their groundwork (the `QuotePolicy`
seam and the instrumentation tables).

## 5. Architecture

Reuse the existing MM rails unchanged: `QuoteManager` (resting-order
bookkeeping/reconcile), `InventoryRisk` (signed inventory + caps), the
live/paper `MakerVenue` + `UserFillSource`, the `run_mm_loop`, and store
persistence.

Introduce a **`QuotePolicy` seam** that the loop consults for the only two
decisions that differ between strategies:
- `select_markets(universe, book_snapshots, budget) -> Vec<MarketId>`
- `desired_quotes(market, book, inventory, reward_cfg) -> Vec<MakerOrder>`

Two implementations:
- `SpreadCapture` — today's behavior, retained for A/B comparison.
- `RewardFarm` — new (this spec).

Selected via config `[strategies.mm] policy = "spread_capture" | "reward_farm"`.
In `reward_farm` mode, confluence is forced **off** (it is a taker signal); arb
is unaffected. The loop, fills, reconcile, persistence, and venue code are
untouched — isolation by construction.

## 6. Data layer

The CLOB `/markets` response already carries a `rewards` object (present in our
own fixtures); we simply do not deserialize it:

```json
"rewards": { "rates": [{ "rewards_daily_rate": 50 }], "min_size": 100, "max_spread": 3 }
```

Changes:
- Add to `ClobMarket` (registry `gamma.rs`) a `rewards` field deserialized into
  `RewardConfig { min_size: f64, max_spread_cents: f64, daily_rate_usd: f64 }`,
  where `daily_rate_usd` = sum of `rates[].rewards_daily_rate` (0 if `rates`
  null/empty).
- Carry `RewardConfig` through `sync` into the registry `MarketMetrics`, so the
  strategy can read per-market `(min_incentive_size, max_incentive_spread,
  daily_rate)`.
- A market with `max_spread_cents == 0` or `daily_rate_usd == 0` is
  **not reward-eligible**.

Backward-compatible: the field is `#[serde(default)]`; markets without rewards
deserialize to zeros and are simply ineligible.

## 7. Market selection (reward-farm mode)

Universe = reward-eligible markets (`max_spread_cents > 0 && daily_rate_usd > 0`),
ranked by an **edge proxy**:

```
edge ≈ daily_rate_usd / max(competing_in_band_depth_usd, eps)
```

i.e. reward dollars per unit of competition, where `competing_in_band_depth_usd`
is estimated from the live book (resting size within `max_spread_cents` of the
adjusted mid, excluding our own orders). Rank descending, then **cap to the
cash budget**: each market parks ~2 sides × `min_incentive_size` × price, so the
number of funded markets = `floor(budget / per_market_cost)`. This replaces the
smart-money/liquidity ranking for this mode only.

## 8. Quoting policy (RewardFarm.desired_quotes)

Per market, each cycle:
1. **Adjusted mid** — midpoint of the book after dropping resting orders below
   `min_incentive_size` (mirrors Polymarket's size-cutoff-adjusted midpoint).
2. **Tight, non-crossing, two-sided** —
   - `bid_price` = highest tick `≤ adj_mid` and `≤ best_ask − 1 tick`.
   - `ask_price` = lowest tick `≥ adj_mid` and `≥ best_bid + 1 tick`.
   - Skip a side only if even that tightest tick is outside `max_spread_cents`
     (it would score ≈ 0).
3. **Balanced sizes** ≥ `min_incentive_size`. Base size from the per-market
   capital allocation. Express inventory lean by skewing **sizes** within a
   capped ratio (≤ 2:1) — bigger on the reducing side; **prices stay tight**.
4. **Sticky re-quote** — replace a resting side only when (a) it has drifted
   out of the reward band (|order − adj_mid| would exceed a re-quote threshold),
   or (b) the size imbalance exceeds a rebalance threshold. No fixed timer; this
   retires the 2.5s churn that resets the time-weighted score.

## 9. Local score estimator (paper-proof)

A module that every ~60s (mirroring Polymarket's sampling) computes our score
on our own resting quotes:
- per order `s = |price − adj_mid|` cents; `S = ((v − s)/v)²` (0 if `s > v`);
  contribution `S × size` (`v = max_spread_cents`).
- `Q₁ = Σ bid contributions`, `Q₂ = Σ ask contributions`;
  `Q_min = max(min(Q₁,Q₂), max(Q₁,Q₂)/3)` in [0.10,0.90], else `min(Q₁,Q₂)`.
- Accumulate `Q_min` per market across the session.
- Rough **$/day estimate** = `daily_rate_usd × our_in_band_depth /
  (our_in_band_depth + competing_in_band_depth)`. **Explicitly labeled an
  estimate** — true payout needs the epoch-wide maker totals only Polymarket
  has.
- Surface to the dashboard/logs: per-market est. reward/day, in-band ✓/✗,
  balance ratio (`min(Q₁,Q₂)/max(Q₁,Q₂)`), session cumulative estimate.

On paper it runs the policy against the live book and reports what we would
score (no spend). On live it runs alongside and is reconciled against the real
midnight payout. Spec 1 scopes single-token two-sided quoting; cross-complement
quoting is a Spec-2 hedging concern.

## 10. Delta-neutral inventory

Target net ≈ 0 per token. Fills move inventory; the size-skew (§8.3) leans new
quotes to mean-revert it. Reuse `InventoryRisk` hard caps (net
`max_inventory_usd`, gross `max_gross_inventory_usd`); when a cap is hit, quote
only the **reducing** side rather than skewing price. Active flatten and
complement hedging are deferred to Spec 2.

## 11. Persistent daily loss cap

Checkpoint cumulative **UTC-day** PnL (realized + marked) to the store, keyed by
UTC date. On startup/auto-restart, reload the current day's PnL and **stay
halted if it is already ≤ −`daily_loss_usd`**. The cap thus bounds the *day*,
not each 30-min session — closing the hole that produced the ~−$29 bleed behind
a "$6" cap. On halt: cancel all quotes and stop quoting; re-arm only at UTC day
rollover or an explicit manual reset. (Reuses the existing halt mechanism; adds
day-keyed persistence + reload.)

## 12. Instrumentation hooks (feeds Spec 3)

Two append-only store tables, written each cycle:
- `rf_decisions(id, ts, market, state_json, action_json)` — state features
  (adjusted mid, book imbalance, short-term vol, inventory, competing in-band
  depth) and the action taken (quote offsets, sizes, pull/no-pull).
- `rf_outcomes(decision_id, ts, reward_score_delta, rebate, adverse_pnl,
  inv_penalty)` — the realized components of the reward signal.

Cheap and isolated; this is the training corpus Spec 3 consumes. No learning is
performed in Spec 1.

## 13. Spec 3 sketch — Adaptive tuning (designed, built later)

- **Learns** only a few interpretable knobs: pull/skew threshold, size-skew
  gain, per-market capital weight. **Not** quote price (closed-form) and **not**
  an end-to-end black-box policy.
- **Method:** Thompson-sampling contextual bandit over a discrete, pre-vetted
  action set per state bucket; reward = `Δ(estimated reward $) + rebate −
  adverse_selection_pnl − inventory_penalty`, computed from §9 + fills + marks.
- **Safety:** learns from logged `(state, action, outcome)` (§12) — offline
  batch by default; optional online incremental updates only inside hard
  guardrails (§10–11 caps, vetted action ranges), exploration restricted to
  paper or a capped capital sleeve. The deterministic policy stays in control;
  the learner only nudges knobs inside safe bounds.
- **Output:** a small learned-parameter table the RewardFarm policy
  loads/hot-reloads.

Spec 3 gets a full design pass once Spec 1+2 have logged enough data to show
what is actually learnable.

## 14. Config (additions)

```toml
[strategies.mm]
policy = "reward_farm"        # "spread_capture" (default) | "reward_farm"

[reward_farm]
requote_band_ticks   = 1      # re-quote when an order drifts this many ticks past the target
size_skew_max_ratio  = 2.0    # cap on bid:ask size lean
sample_interval_ms   = 60000  # estimator sampling cadence (mirror Polymarket)
min_markets          = 1
# min_incentive_size / max_incentive_spread come per-market from the CLOB rewards object
```

Reuse existing `[inventory]` caps and `daily_loss_usd`; the persistence/reload
(§11) makes `daily_loss_usd` bind across restarts.

## 15. Testing

- **Unit:**
  - Estimator vs the docs' worked example (adj mid 0.50, `v = 3`):
    `Q₁ = ((3−1)/3)²·100 + ((3−2)/3)²·200 + ((3−1)/3)²·100 = 111.1` (golden).
  - Adjusted-mid with sub-`min_size` orders filtered.
  - Tight non-crossing bid/ask derivation (tight book and wide book).
  - Size-skew respects the ≤ 2:1 ratio and `min_incentive_size` floor.
  - Sticky re-quote: no replace while in-band; replace on band exit.
  - Persistent day-PnL halt: simulated restart reloads the day's loss and stays
    halted below the cap; re-arms at UTC rollover.
- **Integration (paper):** RewardFarm on a recorded/live book → assert
  two-sided in-band quotes, balanced sizes, cancel-rate under threshold, delta
  within caps, positive `Q_min`, zero phantom-ask rejects.
- **A/B:** identical book, SpreadCapture vs RewardFarm — report estimated reward
  and realized-PnL delta.

## 16. Out of scope (Spec 1)

- Microprice fair value, book-imbalance/momentum quote-pull (Spec 2).
- Complement / NegRisk hedging (Spec 2).
- Any learning/parameter adaptation (Spec 3) — only the logging hooks land here.
- Cross-complement (m and m') two-sided quoting (Spec 2).

## 17. Risks & honest caveats

- **Capital.** Absolute rewards scale with size; at ~$23 the payout is
  cents/day. Spec 1 proves mechanics; profit requires scaling capital later.
- **Estimator $ is approximate.** Without epoch-wide maker totals we can show
  score quality and a rough share, not exact $. Live reconciliation calibrates
  it.
- **Adverse selection persists in Spec 1.** Tight quotes still fill; the loss
  cap + delta caps bound the damage, but net edge over fills is not guaranteed
  until Spec 2's avoidance/hedging. The paper estimator + tiny-live gate exist
  precisely to catch this before scaling.
- **Reward-program changes.** Parameters (`c`, pools, eligibility) are
  Polymarket's to change; they are read per-market from the API, so the engine
  adapts, but the strategy's profitability is program-dependent.
