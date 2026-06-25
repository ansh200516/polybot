# Reward-Farming Market Maker — Spec 2: Pro Alpha Layer

Date: 2026-06-25
Status: Design — awaiting user review before planning
Scope: Spec 2 of the 3-spec program (Spec 1 = core engine, shipped; Spec 3 =
adaptive tuning, designed/deferred). Spec 2 refines Spec 1's quoting + inventory
so reward farming is net-positive in *live* (not just optimistic paper).
Sequencing (user-approved): **Phase A (adverse-selection avoidance) first, then
Phase B (complement hedging).**

## 1. Why Spec 2

Spec 1 farms liquidity rewards with tight, balanced, sticky two-sided quotes and
caps the downside (inventory caps + persistent day-loss cap). But it still rests
near the mid and takes fills, and on a real venue those fills are *adversely
selected* — your quote gets hit precisely when an informed trader is about to
move the price against you. The Spec-1 paper sim has **no** adverse selection
(it showed +$222), so paper overstates live edge. Spec 2 adds the alpha that
keeps reward farming net-positive once real adverse flow is present:

- **Phase A — avoid the bad fills** (microprice fair value + book-imbalance /
  momentum quote-pull + size rebalance). Also closes Spec-1 deferrals I2 (raw
  mid → size-cutoff-adjusted microprice) and §8.4(b) (size-imbalance requote).
- **Phase B — hedge the inventory you do take** via the complement token
  (YES+NO=1 / NegRisk), which also lets the live MM quote two-sided from flat
  (closes Spec-1 M3, the long-only bid-only bootstrap).

Each phase is independently shippable and testable; Phase A delivers most of the
live edge, so it ships first.

## 2. Goal & success criteria

- Quotes step aside from imminent adverse moves instead of resting into them.
- Fair value used for quoting + scoring is the **microprice** over the
  size-cutoff-adjusted book (not the raw mid).
- On a synthetic *trending/imbalanced* book, the Spec-2 MM takes materially
  fewer adverse fills than Spec-1 at comparable reward score (measured by the
  A/B harness: lower realized adverse-PnL per unit of estimated reward).
- Phase B: a long position is neutralized by a complement buy within inventory
  caps; live MM quotes two-sided from flat without naked-short rejects.
- SpreadCapture and arb remain unchanged; all new behavior gated to RewardFarm.

## 3. Architecture

Extends the existing `QuotePolicy` seam and the MM loop — no new strategy. New
pure, unit-testable functions live in `crates/app/src/strategy/quote_policy.rs`
(microprice, imbalance, pull decision) and a new
`crates/app/src/strategy/signals.rs` for the rolling momentum/volatility signal
state. The MM loop (`mm.rs`) consults them in the RewardFarm branch only. Config
extends `[reward_farm]`. Hedging (Phase B) adds a venue capability (buy the
complement / NegRisk convert) behind the existing `MakerVenue`/live path.

## 4. Phase A — Adverse-selection avoidance

### 4.1 Microprice fair value (closes I2)
Replace `adjusted_mid = (best_bid + best_ask)/2` with the **microprice**, the
size-weighted fair value that leans toward the heavier side:

```
microprice = (best_bid · ask_qty + best_ask · bid_qty) / (bid_qty + ask_qty)
```

computed over the **size-cutoff-adjusted** book — i.e. after dropping resting
levels below `min_incentive_size` (this is the §8.1 deferral, done here because
microprice needs per-level sizes anyway). Used for both the quoting reference
(`reward_quote_prices`) and the reward-score estimator's `adj_mid`, so they stay
consistent. Falls back to the plain mid when sizes are unavailable/zero.

### 4.2 Book-imbalance + momentum signal
A pure signal over the top-N (config) levels and a short rolling window:
- `imbalance = (bid_depth − ask_depth) / (bid_depth + ask_depth)` ∈ [−1, 1]
  (positive = buy pressure → price likely to tick up).
- `momentum = sign/size of the microprice change over `signal_window_ms`.
Rolling state lives in `signals.rs` (per token), updated each book event;
pure-function scored so it's testable without I/O.

### 4.3 Quote-pull policy (the key decision)
When the signal predicts an imminent move against a resting side beyond
`pull_threshold`, **pull that one side** (cancel it) and suppress re-quoting it
for `pull_cooldown_ms`; the other side keeps resting. Rationale and the
trade-off we are accepting:
- Pulling forfeits that side's time-weighted reward score for the cooldown, but
  avoids a fill that (in adverse flow) costs far more than the foregone reward.
- We pull **only on a strong signal** (threshold tuned high) so calm-market
  stickiness — and thus reward score — is preserved; weak signals do nothing.
- We pull rather than *widen* because a widened post-only quote still rests in
  the path of the move (just scores less) and can still be hit; pulling removes
  the exposure. (Widen-instead-of-pull is recorded as a tunable alternative for
  Spec 3 to learn.)
Both the bid and ask are evaluated independently each cycle.

### 4.4 Size-rebalance requote (closes §8.4(b))
Add a second requote trigger to the sticky logic: re-place a side when the
inventory-implied size lean has drifted beyond `size_rebalance_pct` from the
resting size (not only on price drift). Keeps quotes delta-neutral without
re-leaning every tick (which would reintroduce flicker).

### 4.5 Config (`[reward_farm]` additions)
```toml
microprice_levels      = 3      # book levels for microprice + imbalance
signal_window_ms       = 3000   # rolling window for momentum
pull_threshold         = 0.6    # |signal| above this pulls the endangered side
pull_cooldown_ms       = 5000   # suppress re-quoting the pulled side this long
size_rebalance_pct     = 0.25   # re-place a side when size lean drifts > 25%
```
All default to "off-ish"/conservative so enabling Spec 2 is incremental and
SpreadCapture is never affected.

### 4.6 Instrumentation
Fold `microprice`, `imbalance`, `momentum`, and the pull decision into the
existing `rf_decisions.state_json` / `action_json` (Spec-1 tables) so Spec 3 can
learn the pull threshold from logged `(signal, outcome)` pairs.

## 5. Phase B — Complement-pair quoting + merge/redeem (after Phase A)

> Design correction (vs the original §5 draft): Polymarket has **no naked
> short** — you cannot sell a YES token you don't hold, so an ask cannot be
> placed from flat and the "sell-then-cover" framing is infeasible. The correct,
> all-maker mechanism is to **bid the complement pair**. This is also exactly
> what the reward formula scores (its `Q₁` combines the `m`/YES book, `Q₂` the
> `m'`/NO book), so bidding both outcomes IS the two-sided structure.

### 5.1 Complement-pair quoting (closes M3, two-sided-from-flat)
When `hedging_enabled` (and required on the live, no-naked-short path), the
reward-farm MM quotes a **bid on YES and a bid on NO** (both buys) instead of
Spec-1's single-token bid+ask. From flat this is genuinely two-sided for the
reward score with zero naked-short risk. As fills land: a YES-bid fill → long
YES; a NO-bid fill → long NO (≡ short YES). A held YES+NO pair is
**delta-neutral** (resolves to $1/pair regardless of outcome). This refines I1:
single-token (bid+ask) stays the default for paper/no-hedging; complement-pair
(bid+bid) is used when `hedging_enabled`.

### 5.2 Delta-neutral sizing & the Phase-A pull, across the pair
Size-skew leans **between the two bids** by net delta: long YES ⇒ smaller
YES-bid, larger NO-bid (and vice versa), capped by `size_skew_max_ratio`. The
Phase-A signal maps to the pair: strong UP pressure on YES (≡ DOWN on NO)
endangers the **NO bid** (you'd buy NO just before it falls), so that bid is
pulled; strong DOWN endangers the YES bid. (Equivalent to Phase A's
"pull the endangered side," re-expressed for two bids.)

### 5.3 Merge / redeem to recycle capital
A held complete set (`min(yes_qty, no_qty)`) merges back to collateral ($1/set)
to free locked capital; resolved markets redeem. **Live constraint:**
`LiveVenue::merge`/`split` are `NotSupportedLive` (on-chain ops deferred to "M6",
exactly like the arb side), so on **live** the bot cannot merge programmatically
— pairs lock until market resolution (redeemed out-of-band), and the
`max_gross_inventory_usd` cap is the live capital control. The **paper** venue
*simulates* merge (reduce both legs by the matched amount, credit cash) so the
capital-recycling economics are validatable now; the live on-chain merge is a
documented M6 follow-up. Merge (paper) runs once the matched pair exceeds
`merge_threshold` so we don't churn tiny amounts.

### 5.4 Capital & risk
A YES+NO pair locks ~$1/share until merge/redeem. Phase B therefore (a) counts
**both** bid legs in the cash budget, (b) caps total pairs by
`max_gross_inventory_usd`, and (c) merges complete sets (5.3) to recycle capital
so the gross cap doesn't permanently throttle quoting.

### 5.5 Config (`[reward_farm]` additions, Phase B)
```toml
hedging_enabled    = false  # opt-in; off keeps Spec-1 single-token behavior
                            # (required for live two-sided-from-flat)
merge_threshold_usd = 5.0   # merge a complete YES+NO set once the matched pair
                            # exceeds this (recycles locked collateral)
```

## 6. Testing

**Phase A (pure + integration):**
- `microprice`: weights toward the heavier side; equals mid when sizes equal;
  size-cutoff filtering drops sub-min_size levels; zero-size fallback to mid.
- `imbalance`/`momentum`: sign + magnitude on crafted books/series.
- pull decision: pulls the endangered side above threshold, holds below, both
  sides evaluated independently, cooldown suppresses re-quote.
- size-rebalance: requote triggers past `size_rebalance_pct`, not before.
- integration (paper): on a synthetic *imbalanced/trending* book, assert the
  endangered side is pulled and the A/B harness shows lower adverse-PnL per unit
  reward vs Spec 1 on the same feed.

**Phase B:**
- complement-pair quoting: with `hedging_enabled`, a flat MM quotes a bid on YES
  AND a bid on NO (both buys, no naked-short reject); single-token bid+ask
  remains when hedging is off.
- pair delta-neutral sizing: long YES ⇒ smaller YES-bid / larger NO-bid (capped
  by `size_skew_max_ratio`); flat ⇒ balanced.
- pull mapping: strong UP-on-YES pulls the NO bid (and vice versa).
- merge/redeem: a held complete set above `merge_threshold` merges back to
  collateral (paper sim credits cash + reduces both legs); resolved markets
  redeem.
- capital: both bid legs count against the budget; total pairs respect the gross
  cap; merging recycles locked collateral.

## 7. Out of scope
- The adaptive/learning layer (Spec 3) — Spec 2 only *logs* the signals for it.
- Cross-venue or non-Polymarket hedging.
- Queue-position modelling (the paper sim stays fill-pct based).

## 8. Risks & honest notes
- **Microprice/threshold tuning is empirical** — defaults are conservative;
  Spec 3 is meant to learn `pull_threshold`. Until then, the A/B harness + paper
  signal logs guide manual tuning.
- **Pulling costs reward score**; the design only pulls on strong signals to
  protect calm-market stickiness. If live data shows the pulls are too frequent,
  raise `pull_threshold` (or switch to widen — logged as the Spec-3 alternative).
- **Hedging adds capital usage and complexity**; it is opt-in (`hedging_enabled
  = false` default) so Phase A can be validated live before Phase B is armed.
- Paper still lacks true adverse selection, so Phase A's benefit is proven on
  *synthetic adverse* feeds + the signal logs, then confirmed in tiny-live.
