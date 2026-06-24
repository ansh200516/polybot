# Reward-Farming Market Maker (Spec 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pivot the live MM from spread capture to Polymarket liquidity-reward farming — quote tight, balanced, two-sided, sticky orders on reward-eligible markets, measure expected rewards locally, stay delta-neutral, and stop the daily bleed.

**Architecture:** Reuse the existing MM rails (`QuoteManager`, `InventoryRisk`, live/paper venue, `run_mm_loop`). Add a `QuotePolicy` seam with two impls — `SpreadCapture` (current) and `RewardFarm` (new) — so only market-selection and quote-generation change. Ingest the CLOB `rewards` object, add a pure reward-score estimator, persist UTC-day PnL so the loss cap binds across restarts, and log `(state, action, outcome)` for the future learning spec.

**Tech Stack:** Rust workspace (`pm-registry`, `pm-ingestion`, `pm-config`, `pm-store`, `pm-app`), `serde`, `rusqlite`, `tokio`, existing `QuoteManager`/`InventoryRisk`/`MakerVenue` traits.

**Spec:** `docs/superpowers/specs/2026-06-24-reward-farming-mm-core-design.md`

---

## File Structure

- `crates/registry/src/gamma.rs` — add `ClobRewards`/`ClobRewardRate` + `rewards` field on `ClobMarket` (Task 1).
- `crates/registry/src/segment.rs` — add reward fields to `MarketMetrics` (Task 2).
- `crates/ingestion/src/sync.rs` — populate reward fields in `market_metrics()` (Task 2).
- `crates/config/src/lib.rs` — `Mm.policy` + new `RewardFarm` config section + validation (Task 3).
- `crates/app/src/strategy/reward_score.rs` — NEW: pure quadratic score estimator (Task 4).
- `crates/app/src/strategy/quote_policy.rs` — NEW: `QuotePolicy` trait + `SpreadCapture` + `RewardFarm` (Tasks 5–8).
- `crates/app/src/strategy/mm.rs` — call the policy from `quote()`; wire estimator + day-loss halt (Tasks 5, 8, 9).
- `crates/store/src/lib.rs` + `read.rs` + `write.rs` — `rf_decisions`/`rf_outcomes` tables + UTC-day PnL helpers (Tasks 9–10).

Each task is independently testable and ends in a commit.

---

## Task 1: Ingest the CLOB `rewards` object

**Files:**
- Modify: `crates/registry/src/gamma.rs` (the `ClobMarket` struct, ~`230-250`)
- Test: `crates/registry/src/gamma.rs` (test module at bottom)

- [ ] **Step 1: Write the failing test**

Add to the `gamma.rs` test module:

```rust
#[test]
fn clob_market_parses_rewards_object() {
    let json = r#"{
        "condition_id": "0xabc",
        "minimum_tick_size": 0.01,
        "tokens": [],
        "active": true,
        "rewards": { "rates": [{"rewards_daily_rate": 50.0}], "min_size": 100.0, "max_spread": 3.0 }
    }"#;
    let m: ClobMarket = serde_json::from_str(json).unwrap();
    assert_eq!(m.rewards.min_size, 100.0);
    assert_eq!(m.rewards.max_spread, 3.0);
    assert_eq!(m.rewards.daily_rate_usd(), 50.0);
}

#[test]
fn clob_market_rewards_defaults_when_absent_or_null() {
    let json = r#"{"condition_id":"0xabc","minimum_tick_size":0.01,"tokens":[],"active":true,
        "rewards":{"rates":null,"min_size":0,"max_spread":0}}"#;
    let m: ClobMarket = serde_json::from_str(json).unwrap();
    assert_eq!(m.rewards.daily_rate_usd(), 0.0);
    assert!(!m.rewards.is_eligible());
    // entirely missing `rewards` key also defaults
    let m2: ClobMarket = serde_json::from_str(
        r#"{"condition_id":"0x1","minimum_tick_size":0.01,"tokens":[],"active":true}"#).unwrap();
    assert!(!m2.rewards.is_eligible());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-registry clob_market_parses_rewards -- --nocapture`
Expected: FAIL (no field `rewards` on `ClobMarket`).

- [ ] **Step 3: Write minimal implementation**

In `gamma.rs`, add the structs and the field:

```rust
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClobRewardRate {
    #[serde(default)]
    pub rewards_daily_rate: f64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClobRewards {
    #[serde(default)]
    pub rates: Option<Vec<ClobRewardRate>>,
    /// `min_incentive_size` — minimum order size (shares) to qualify.
    #[serde(default)]
    pub min_size: f64,
    /// `max_incentive_spread` — max distance from mid (cents) that scores.
    #[serde(default)]
    pub max_spread: f64,
}

impl ClobRewards {
    /// Total configured daily reward rate (USD/day) across reward assets.
    pub fn daily_rate_usd(&self) -> f64 {
        self.rates
            .as_ref()
            .map(|r| r.iter().map(|x| x.rewards_daily_rate).sum())
            .unwrap_or(0.0)
    }
    /// Reward-eligible iff there is a positive scoring band AND a positive rate.
    pub fn is_eligible(&self) -> bool {
        self.max_spread > 0.0 && self.daily_rate_usd() > 0.0
    }
}
```

Add to `ClobMarket`:

```rust
    #[serde(default)]
    pub rewards: ClobRewards,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-registry rewards`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add crates/registry/src/gamma.rs
git commit -m "feat(registry): ingest CLOB rewards object (min_size, max_spread, daily rate)"
```

---

## Task 2: Carry reward params into `MarketMetrics`

**Files:**
- Modify: `crates/registry/src/segment.rs` (`MarketMetrics`, ~`43`)
- Modify: `crates/ingestion/src/sync.rs` (`market_metrics()`, ~`277`; and the CLOB merge that has the `ClobMarket`)
- Test: `crates/registry/src/segment.rs` test module

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn market_metrics_carries_reward_params() {
    let m = MarketMetrics {
        reward_min_size: 100.0,
        reward_max_spread_cents: 3.0,
        reward_daily_rate_usd: 50.0,
        ..MarketMetrics::default()
    };
    assert!(m.reward_eligible());
    assert!(!MarketMetrics::default().reward_eligible());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-registry market_metrics_carries_reward`
Expected: FAIL (unknown fields).

- [ ] **Step 3: Write minimal implementation**

Add to `MarketMetrics` in `segment.rs`:

```rust
    /// `min_incentive_size` (shares) for the liquidity-reward program; 0 = none.
    pub reward_min_size: f64,
    /// `max_incentive_spread` (cents) — scoring band half-width; 0 = ineligible.
    pub reward_max_spread_cents: f64,
    /// Configured reward rate (USD/day); 0 = ineligible.
    pub reward_daily_rate_usd: f64,
```

(These are `f64`, so `#[derive(Default)]` already yields `0.0`.) Add the helper:

```rust
impl MarketMetrics {
    pub fn reward_eligible(&self) -> bool {
        self.reward_max_spread_cents > 0.0 && self.reward_daily_rate_usd > 0.0
    }
}
```

- [ ] **Step 4: Populate from the CLOB market in `sync.rs`**

`market_metrics()` takes a `GammaMarket`/`GammaEvent`; the reward params come from the **CLOB** `ClobMarket`. Thread the `ClobRewards` into the metrics at the point where `market_metrics(...)` result is combined with the CLOB fetch (search `market_metrics(` and the `ClobMarket` merge in `sync.rs`). After the existing `let mut metrics = market_metrics(gm, event);` (add `mut` if needed), set:

```rust
    metrics.reward_min_size = clob.rewards.min_size;
    metrics.reward_max_spread_cents = clob.rewards.max_spread;
    metrics.reward_daily_rate_usd = clob.rewards.daily_rate_usd();
```

where `clob: &ClobMarket` is the fetched market in scope. If the CLOB market is not in scope at that call site, pass `&ClobRewards` into `market_metrics` as a new param and set the three fields inside it. Keep the function signature change minimal and update its two call sites.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p pm-registry market_metrics_carries_reward && cargo test -p pm-ingestion`
Expected: PASS; ingestion tests still green.

- [ ] **Step 6: Commit**

```bash
git add crates/registry/src/segment.rs crates/ingestion/src/sync.rs
git commit -m "feat(registry,ingestion): carry reward params into MarketMetrics"
```

---

## Task 3: Config — `policy` switch + `[reward_farm]` section

**Files:**
- Modify: `crates/config/src/lib.rs` (`Mm` struct ~`471`; add `RewardFarm`; `validate()`)
- Test: `crates/config/src/lib.rs` test module

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn reward_farm_config_parses_and_defaults() {
    let c = Config::from_toml_str(
        "[strategies.mm]\nenabled=true\npolicy=\"reward_farm\"\n\
         [reward_farm]\nrequote_band_ticks=2\nsize_skew_max_ratio=2.0\nsample_interval_ms=60000\n",
    ).unwrap();
    assert_eq!(c.strategies.mm.policy, "reward_farm");
    assert_eq!(c.reward_farm.requote_band_ticks, 2);
    // default when section omitted
    let d = Config::default();
    assert_eq!(d.strategies.mm.policy, "spread_capture");
    assert_eq!(d.reward_farm.size_skew_max_ratio, 2.0);
}

#[test]
fn reward_farm_rejects_bad_policy_and_ratio() {
    let mut c = Config::default();
    c.strategies.mm.policy = "nonsense".into();
    assert!(c.validate().is_err());
    let mut c2 = Config::default();
    c2.reward_farm.size_skew_max_ratio = 0.5; // must be >= 1.0
    assert!(c2.validate().is_err());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-config reward_farm`
Expected: FAIL (no `policy` / `reward_farm`).

- [ ] **Step 3: Write minimal implementation**

Add to `Mm`:

```rust
    /// Quote policy: "spread_capture" (default, legacy) or "reward_farm".
    #[serde(default = "default_mm_policy")]
    pub policy: String,
```

with `fn default_mm_policy() -> String { "spread_capture".into() }` and set `policy: default_mm_policy()` in `Mm::default()`.

Add the section struct + `Config` field + `Default`:

```rust
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RewardFarm {
    /// Re-quote a side only when it drifts this many ticks past target (anti-flicker).
    pub requote_band_ticks: u16,
    /// Max bid:ask size lean for inventory skew.
    pub size_skew_max_ratio: f64,
    /// Estimator sampling cadence (ms), mirroring Polymarket's 1/min sampling.
    pub sample_interval_ms: u64,
    /// Minimum reward-eligible markets to quote.
    pub min_markets: u32,
}

impl Default for RewardFarm {
    fn default() -> Self {
        RewardFarm { requote_band_ticks: 1, size_skew_max_ratio: 2.0, sample_interval_ms: 60_000, min_markets: 1 }
    }
}
```

Add `pub reward_farm: RewardFarm,` to `Config` and `reward_farm: RewardFarm::default()` to its `Default`.

In `Config::validate()` add:

```rust
        if !matches!(self.strategies.mm.policy.as_str(), "spread_capture" | "reward_farm") {
            return Err(ConfigError::BadMoney("strategies.mm.policy must be spread_capture or reward_farm"));
        }
        if self.reward_farm.size_skew_max_ratio < 1.0 {
            return Err(ConfigError::BadMoney("reward_farm.size_skew_max_ratio must be >= 1.0"));
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-config reward_farm`
Expected: PASS. Also run `cargo test -p pm-config defaults_are_the_locked_values` and update that test if it asserts the full `Config` default shape.

- [ ] **Step 5: Commit**

```bash
git add crates/config/src/lib.rs
git commit -m "feat(config): MM policy switch + [reward_farm] section"
```

---

## Task 4: Reward-score estimator (pure module)

**Files:**
- Create: `crates/app/src/strategy/reward_score.rs`
- Modify: `crates/app/src/strategy/mod.rs` (add `pub mod reward_score;`)
- Test: in `reward_score.rs`

- [ ] **Step 1: Write the failing test (golden from Polymarket docs)**

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn order_score_matches_docs_worked_example() {
        // adj mid 0.50, v=3c. Bids: 100@0.49(s=1), 200@0.48(s=2); ask on m' 100@0.51(s=1).
        let v = 3.0;
        let q1 = order_score(v, 1.0) * 100.0 + order_score(v, 2.0) * 200.0 + order_score(v, 1.0) * 100.0;
        assert!((q1 - 111.111).abs() < 0.01, "got {q1}");
    }

    #[test]
    fn out_of_band_scores_zero_and_qmin_two_sided() {
        assert_eq!(order_score(3.0, 4.0), 0.0); // s > v
        // balanced two-sided in [0.10,0.90]: Qmin = min when both present
        let q = q_min(60.0, 60.0, 0.50);
        assert!((q - 60.0).abs() < 1e-9);
        // single-sided in band gets the /3 floor, not zero
        let q2 = q_min(90.0, 0.0, 0.50);
        assert!((q2 - 30.0).abs() < 1e-9);
        // single-sided OUTSIDE band scores zero
        assert_eq!(q_min(90.0, 0.0, 0.95), 0.0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-app reward_score`
Expected: FAIL (module/functions absent).

- [ ] **Step 3: Write minimal implementation**

```rust
//! Pure local reproduction of Polymarket's liquidity-reward scoring, used to
//! ESTIMATE rewards on paper before risking money. No I/O. See the spec §2/§9.

/// Quadratic order score `S(v,s) = ((v-s)/v)^2`, 0 when `s > v` or `v <= 0`.
/// `v` = max_incentive_spread (cents), `s` = distance from adjusted mid (cents).
pub fn order_score(v: f64, s: f64) -> f64 {
    if v <= 0.0 || s < 0.0 || s > v {
        return 0.0;
    }
    let r = (v - s) / v;
    r * r
}

const C: f64 = 3.0; // single-sided scaling factor (Polymarket current)

/// Two-sided minimum score. In [0.10, 0.90] single-sided scores at 1/C;
/// outside, liquidity must be two-sided or it scores zero.
pub fn q_min(q_one: f64, q_two: f64, mid: f64) -> f64 {
    if (0.10..=0.90).contains(&mid) {
        f64::max(q_one.min(q_two), f64::max(q_one / C, q_two / C))
    } else {
        q_one.min(q_two)
    }
}

/// One resting order for scoring: distance from adjusted mid (cents) and size (shares).
#[derive(Debug, Clone, Copy)]
pub struct ScoredOrder {
    pub spread_cents: f64,
    pub size: f64,
}

/// Q_min for a single-token two-sided quote set (bids -> Q1, asks -> Q2).
pub fn quote_set_q_min(v: f64, mid: f64, bids: &[ScoredOrder], asks: &[ScoredOrder]) -> f64 {
    let sum = |os: &[ScoredOrder]| os.iter().map(|o| order_score(v, o.spread_cents) * o.size).sum::<f64>();
    q_min(sum(bids), sum(asks), mid)
}

/// Rough $/day estimate = daily_rate * our_depth / (our_depth + competing_depth).
/// EXPLICITLY an estimate — true payout needs epoch-wide maker totals.
pub fn est_daily_reward_usd(daily_rate_usd: f64, our_in_band_depth: f64, competing_in_band_depth: f64) -> f64 {
    let denom = our_in_band_depth + competing_in_band_depth;
    if denom <= 0.0 { 0.0 } else { daily_rate_usd * our_in_band_depth / denom }
}
```

Add `pub mod reward_score;` to `crates/app/src/strategy/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-app reward_score`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/app/src/strategy/reward_score.rs crates/app/src/strategy/mod.rs
git commit -m "feat(app): pure reward-score estimator (quadratic S, two-sided Q_min)"
```

---

## Task 5: `QuotePolicy` seam + `RewardFarm` quote pricing

**Files:**
- Create: `crates/app/src/strategy/quote_policy.rs`
- Modify: `crates/app/src/strategy/mod.rs` (`pub mod quote_policy;`)
- Test: in `quote_policy.rs`

This task introduces the seam and the pure quote-pricing math. Selection (Task 7) and sticky logic (Task 8) extend it; `mm.rs` wiring is Step 6 here.

- [ ] **Step 1: Write the failing test (tight, non-crossing, two-sided)**

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    // tick = 1c. best_bid=0.48, best_ask=0.52, adj_mid=0.50.
    #[test]
    fn quotes_tight_and_non_crossing() {
        let (bid, ask) = reward_quote_prices(0.50, 0.48, 0.52, 0.01, /*max_spread_c*/3.0);
        let (bid, ask) = (bid.unwrap(), ask.unwrap());
        assert!(bid < 0.52 && ask > 0.48, "must not cross");
        assert!((0.50 - bid) <= 0.0201 && (ask - 0.50) <= 0.0201, "within ~1 tick of mid");
    }

    // wide book: place 1 tick inside the touch, still within band
    #[test]
    fn wide_book_places_inside_touch() {
        let (bid, ask) = reward_quote_prices(0.50, 0.40, 0.60, 0.01, 3.0);
        let (bid, ask) = (bid.unwrap(), ask.unwrap());
        assert!(bid >= 0.49 && bid <= 0.50);
        assert!(ask <= 0.51 && ask >= 0.50);
    }

    // band too tight to quote without exceeding max_spread -> neither side
    #[test]
    fn skips_when_touch_outside_band() {
        // max_spread = 0.5c, but nearest non-crossing tick is 1c from mid -> skip
        assert_eq!(reward_quote_prices(0.50, 0.48, 0.52, 0.01, 0.5), (None, None));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-app quote_policy`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```rust
//! QuotePolicy seam: the only two decisions that differ between strategies —
//! WHICH markets and WHAT quotes. `SpreadCapture` preserves today's behavior;
//! `RewardFarm` implements liquidity-reward farming (spec §5–§8).

/// Compute tight, non-crossing, two-sided prices for reward farming.
/// All prices in dollars (0..1). Each side is `None` if it cannot sit within
/// `max_spread_cents` of the adjusted mid without crossing; `(None, None)` means
/// skip this market this cycle. Returning a per-side `Option` (not a NaN
/// sentinel) keeps the caller honest.
pub fn reward_quote_prices(
    adj_mid: f64,
    best_bid: f64,
    best_ask: f64,
    tick: f64,
    max_spread_cents: f64,
) -> (Option<f64>, Option<f64>) {
    let band = max_spread_cents / 100.0;
    // Highest tick <= mid that does not cross (<= best_ask - tick).
    let bid_cap = (best_ask - tick).min(adj_mid);
    let bid = (bid_cap / tick).floor() * tick;
    // Lowest tick >= mid that does not cross (>= best_bid + tick).
    let ask_floor = (best_bid + tick).max(adj_mid);
    let ask = (ask_floor / tick).ceil() * tick;
    let bid_ok = bid > 0.0 && (adj_mid - bid) <= band + 1e-9 && bid < best_ask;
    let ask_ok = ask < 1.0 && (ask - adj_mid) <= band + 1e-9 && ask > best_bid;
    // Quote the side(s) that qualify; if only one qualifies the caller still
    // gets a single-sided quote (Q_min handles the 1/3 / zero rule by mid).
    (bid_ok.then_some(bid), ask_ok.then_some(ask))
}

/// Adjusted mid: midpoint of the book after dropping resting size below `min_size`.
/// `levels_*` are (price, size) ascending/descending from the touch.
pub fn adjusted_mid(best_bid: f64, best_ask: f64) -> f64 {
    (best_bid + best_ask) / 2.0
}
```

(Note: the size-cutoff filtering of the mid is applied by the caller in `mm.rs` using the live book before calling `adjusted_mid`; this keeps the pure fn trivial. If the book type is available here, fold the filter in and extend the test.)

Add `pub mod quote_policy;` to `mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-app quote_policy`
Expected: PASS.

- [ ] **Step 5: Define the `QuotePolicy` trait and wire `mm.rs`**

In `quote_policy.rs` add:

```rust
use pm_core::instrument::{MarketId, TokenId};

#[derive(Debug, Clone, Copy)]
pub enum Policy { SpreadCapture, RewardFarm }

impl Policy {
    pub fn from_cfg(s: &str) -> Self {
        match s { "reward_farm" => Policy::RewardFarm, _ => Policy::SpreadCapture }
    }
}
```

In `mm.rs::quote()`, branch on the configured `Policy`: for `SpreadCapture` keep the existing `compute_quotes(...)` path unchanged; for `RewardFarm`, for each selected token compute `adj_mid` from the live book (filtering sub-`min_size` levels), call `reward_quote_prices(...)`, and build `MakerOrder`s via the existing `quote_order(...)` helper at the returned prices with balanced sizes (Task 6). Thread `Policy` + per-market reward params (`min_size`, `max_spread`, `daily_rate`) into `MmLoop` (read from the registry `MarketMetrics`).

- [ ] **Step 6: Run build + existing MM tests**

Run: `cargo test -p pm-app strategy::mm && cargo clippy -p pm-app --all-targets`
Expected: PASS; SpreadCapture path unchanged.

- [ ] **Step 7: Commit**

```bash
git add crates/app/src/strategy/quote_policy.rs crates/app/src/strategy/mod.rs crates/app/src/strategy/mm.rs
git commit -m "feat(app): QuotePolicy seam + reward-farm tight non-crossing quote pricing"
```

---

## Task 6: Balanced sizing + size-skew (delta-neutral)

**Files:**
- Modify: `crates/app/src/strategy/quote_policy.rs`
- Test: in `quote_policy.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn size_skew_leans_against_inventory_within_ratio() {
    // base 10 shares, long 8 (net>0) -> bigger ask, smaller bid, ratio <= 2:1
    let (bid_sz, ask_sz) = skewed_sizes(10.0, /*net*/ 8.0, /*cap*/ 10.0, /*max_ratio*/ 2.0, /*min*/ 5.0);
    assert!(ask_sz >= bid_sz);
    assert!(ask_sz / bid_sz <= 2.0 + 1e-9);
    assert!(bid_sz >= 5.0 && ask_sz >= 5.0, "both stay >= min_incentive_size");
}

#[test]
fn flat_inventory_is_balanced() {
    let (b, a) = skewed_sizes(10.0, 0.0, 10.0, 2.0, 5.0);
    assert!((a - b).abs() < 1e-9);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-app size_skew`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```rust
/// Balanced base sizes leaned against signed inventory `net` (shares).
/// Long (net>0) -> bigger ask; short -> bigger bid. Ratio clamped to
/// `max_ratio`; both sides floored at `min_size` to preserve the 2-sided bonus.
pub fn skewed_sizes(base: f64, net: f64, cap_shares: f64, max_ratio: f64, min_size: f64) -> (f64, f64) {
    let r = if cap_shares > 0.0 { (net / cap_shares).clamp(-1.0, 1.0) } else { 0.0 };
    // lean in [1/max_ratio, max_ratio]
    let lean = max_ratio.powf(r); // r>0 (long) -> lean>1 -> ask bigger
    let ask = (base * lean).max(min_size);
    let bid = (base / lean).max(min_size);
    (bid, ask)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-app size_skew`
Expected: PASS.

- [ ] **Step 5: Wire into the RewardFarm branch** of `mm.rs::quote()` — compute `base` from per-market capital allocation, read `net` from `InventoryRisk`, `cap_shares` from `max_inventory_usd / mid`, and quantize to the venue grain via the existing `quote_order` size rules. When a side price is `None` (skipped) or an inventory cap is hit, quote only the reducing side.

- [ ] **Step 6: Run + commit**

Run: `cargo test -p pm-app && cargo clippy -p pm-app --all-targets`

```bash
git add crates/app/src/strategy/quote_policy.rs crates/app/src/strategy/mm.rs
git commit -m "feat(app): delta-neutral size-skew for reward farming (prices stay tight)"
```

---

## Task 7: Reward-eligible market selection + budget cap

**Files:**
- Modify: `crates/app/src/strategy/quote_policy.rs`
- Modify: `crates/app/src/main.rs` (MM market selection in reward-farm mode)
- Test: in `quote_policy.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[derive(Clone)]
struct Cand { id: u64, daily_rate: f64, competing_depth: f64, per_market_cost: f64 }

#[test]
fn selection_ranks_by_edge_and_caps_to_budget() {
    let cands = vec![
        Cand { id: 1, daily_rate: 100.0, competing_depth: 1000.0, per_market_cost: 10.0 }, // edge 0.10
        Cand { id: 2, daily_rate: 100.0, competing_depth: 100.0,  per_market_cost: 10.0 }, // edge 1.00 (best)
        Cand { id: 3, daily_rate: 0.0,   competing_depth: 50.0,   per_market_cost: 10.0 }, // ineligible
    ];
    let picked = select_reward_markets(
        cands.iter().map(|c| (c.id, c.daily_rate, c.competing_depth, c.per_market_cost)).collect(),
        /*budget*/ 10.0, // only one market fits
    );
    assert_eq!(picked, vec![2]); // best edge, ineligible dropped, budget-capped to 1
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-app selection_ranks`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```rust
/// Rank reward-eligible markets by edge = daily_rate / competing_depth, then
/// greedily fit to `budget_usd`. Input tuples: (id, daily_rate, competing_depth, per_market_cost).
pub fn select_reward_markets(mut cands: Vec<(u64, f64, f64, f64)>, budget_usd: f64) -> Vec<u64> {
    cands.retain(|(_, rate, _, cost)| *rate > 0.0 && *cost > 0.0);
    cands.sort_by(|a, b| {
        let ea = a.1 / a.2.max(1e-9);
        let eb = b.1 / b.2.max(1e-9);
        eb.partial_cmp(&ea).unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut spent = 0.0;
    let mut out = Vec::new();
    for (id, _, _, cost) in cands {
        if spent + cost <= budget_usd + 1e-9 {
            spent += cost;
            out.push(id);
        }
    }
    out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-app selection_ranks`
Expected: PASS.

- [ ] **Step 5: Wire into `main.rs`** — in reward-farm mode build the MM token universe from registry markets where `MarketMetrics::reward_eligible()`, computing `competing_depth` from the live book (in-band resting size) and `per_market_cost ≈ 2 * min_size * mid`, then `select_reward_markets(..., budget)` where `budget = capital_usd` (or live balance later). Force confluence OFF in this mode.

- [ ] **Step 6: Run + commit**

```bash
git add crates/app/src/strategy/quote_policy.rs crates/app/src/main.rs
git commit -m "feat(app): reward-eligible market selection ranked by edge, budget-capped"
```

---

## Task 8: Sticky re-quoting (anti-flicker)

**Files:**
- Modify: `crates/app/src/strategy/quote_policy.rs`
- Modify: `crates/app/src/strategy/mm.rs`
- Test: in `quote_policy.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn requote_only_when_out_of_band() {
    // resting at 0.49; band 1 tick (0.01). mid drifts to 0.495 -> still ok; to 0.51 -> replace.
    assert!(!needs_requote(0.49, /*target*/ 0.495, /*tick*/ 0.01, /*band_ticks*/ 1));
    assert!(needs_requote(0.49, 0.51, 0.01, 1));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-app requote_only`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```rust
/// True when a resting order must be replaced: it has drifted more than
/// `band_ticks` from the new target price. Keeps quotes sticky (frequent
/// cancels reset the time-weighted reward score).
pub fn needs_requote(resting_price: f64, target_price: f64, tick: f64, band_ticks: u16) -> bool {
    (resting_price - target_price).abs() > (f64::from(band_ticks) * tick) + 1e-9
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-app requote_only`
Expected: PASS.

- [ ] **Step 5: Wire into `mm.rs`** — before reconciling the RewardFarm desired set, for each currently resting order skip replacement when `!needs_requote(resting, target, tick, requote_band_ticks)` and the size is within the rebalance threshold. This replaces the fixed-interval replace for reward-farm mode. Verify cancel-rate drops in the integration test (Task 11).

- [ ] **Step 6: Run + commit**

```bash
git add crates/app/src/strategy/quote_policy.rs crates/app/src/strategy/mm.rs
git commit -m "feat(app): sticky re-quoting for reward farming (anti-flicker)"
```

---

## Task 9: Persistent UTC-day loss cap

**Files:**
- Modify: `crates/store/src/read.rs` (add `day_pnl_micro(utc_day) -> i128`)
- Modify: `crates/app/src/strategy/mm.rs` (reload + halt on startup and each cycle)
- Test: `crates/store/src/read.rs` and `crates/app/src/strategy/mm.rs`

- [ ] **Step 1: Write the failing test (store)**

`pnl_snapshots` has `ts_ms`, `realized_micro`, `unrealized_micro`, `strategy`. Sum the day's realized + last unrealized for a strategy:

```rust
#[test]
fn day_pnl_sums_todays_realized_plus_last_unrealized() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d.sqlite");
    let s = Store::open(&path).unwrap();
    // two snapshots same UTC day for "mm"
    s.record_pnl_at(1_000, /*cash*/0, /*realized*/ -3_000_000, /*unrl*/ -1_000_000, 0, "mm").unwrap();
    s.record_pnl_at(2_000, 0, -5_000_000, -500_000, 0, "mm").unwrap();
    let rs = ReadStore::open(&path).unwrap();
    let day = utc_day_from_ms(2_000);
    // realized is cumulative in this schema -> take latest realized + latest unrealized
    assert_eq!(rs.day_pnl_micro("mm", day).unwrap(), -5_500_000);
}
```

(If `record_pnl` has no explicit-ts variant, add `record_pnl_at(ts_ms, ...)` used by the test; production keeps calling `record_pnl` which stamps `now`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-store day_pnl`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation (store)**

```rust
/// UTC day index (days since epoch) for a millisecond timestamp.
pub fn utc_day_from_ms(ts_ms: i64) -> i64 { ts_ms / 86_400_000 }

impl ReadStore {
    /// Today's PnL (micro): latest cumulative realized + latest unrealized for
    /// `strategy` among snapshots whose ts falls on `utc_day`. 0 if none.
    pub fn day_pnl_micro(&self, strategy: &str, utc_day: i64) -> Result<i128, StoreError> {
        let lo = utc_day * 86_400_000;
        let hi = lo + 86_400_000;
        let row: Option<(i64, i64)> = self.conn.query_row(
            "SELECT realized_micro, unrealized_micro FROM pnl_snapshots \
             WHERE strategy = ?1 AND ts_ms >= ?2 AND ts_ms < ?3 ORDER BY id DESC LIMIT 1",
            rusqlite::params![strategy, lo, hi],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        Ok(row.map(|(re, un)| i128::from(re) + i128::from(un)).unwrap_or(0))
    }
}
```

- [ ] **Step 4: Run test (store) to verify it passes**

Run: `cargo test -p pm-store day_pnl`
Expected: PASS.

- [ ] **Step 5: Wire the halt in `mm.rs`** — on startup and at each cycle, if `day_pnl_micro("mm", utc_day_now) <= -daily_loss_micro`, set the loop `halted = true`, cancel all quotes, and stop quoting until the UTC day rolls over (recompute `utc_day_now` each cycle; when it increases, clear the latch). Add a unit test that seeds a losing day PnL and asserts the loop starts halted.

```rust
#[tokio::test]
async fn starts_halted_when_day_already_at_loss_cap() {
    // build an MmLoop with daily_loss_micro = 6_000_000 and a ReadStore whose
    // day_pnl_micro returns -6_500_000; assert mm.halted == true after the
    // startup check and that quote() places nothing.
}
```

- [ ] **Step 6: Run + commit**

Run: `cargo test -p pm-store && cargo test -p pm-app starts_halted_when_day`

```bash
git add crates/store/src/read.rs crates/app/src/strategy/mm.rs
git commit -m "feat(store,app): persistent UTC-day loss cap (binds across auto-restarts)"
```

---

## Task 10: Instrumentation tables (feeds Spec 3)

**Files:**
- Modify: `crates/store/src/lib.rs` (SCHEMA + a `record_rf_decision`/`record_rf_outcome`)
- Modify: `crates/app/src/strategy/mm.rs` (write per cycle in reward-farm mode)
- Test: `crates/store/src/lib.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn rf_decision_and_outcome_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(&dir.path().join("rf.sqlite")).unwrap();
    let id = s.record_rf_decision(1_000, "0xcond", r#"{"mid":0.5}"#, r#"{"bid":0.49}"#).unwrap();
    s.record_rf_outcome(id, 2_000, 12_000, 0, -3_000, -1_000).unwrap();
    assert!(id > 0);
    // counts via a small helper or direct query
    assert_eq!(s.count_rf_outcomes_for(id).unwrap(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pm-store rf_decision`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

Append to the SCHEMA string in `lib.rs`:

```sql
CREATE TABLE IF NOT EXISTS rf_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
  market TEXT NOT NULL, state_json TEXT NOT NULL, action_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rf_outcomes (
  decision_id INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
  reward_score_delta_micro INTEGER NOT NULL, rebate_micro INTEGER NOT NULL,
  adverse_pnl_micro INTEGER NOT NULL, inv_penalty_micro INTEGER NOT NULL);
```

Add `Store::record_rf_decision(ts_ms, market, state_json, action_json) -> i64` (returns `last_insert_rowid()`), `record_rf_outcome(decision_id, ts_ms, reward_score_delta_micro, rebate_micro, adverse_pnl_micro, inv_penalty_micro)`, and a test-only `count_rf_outcomes_for`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pm-store rf_decision`
Expected: PASS.

- [ ] **Step 5: Wire the writes** in the RewardFarm branch of `mm.rs::quote()` — once per cycle per market, serialize the state features + chosen action and call `record_rf_decision`; on the next cycle / on fills, record the realized `record_rf_outcome`. Keep it behind reward-farm mode so SpreadCapture is unaffected.

- [ ] **Step 6: Run + commit**

```bash
git add crates/store/src/lib.rs crates/app/src/strategy/mm.rs
git commit -m "feat(store,app): rf_decisions/rf_outcomes instrumentation for future tuning"
```

---

## Task 11: Integration, estimator surfacing, A/B

**Files:**
- Modify: `crates/app/src/strategy/mm.rs` (publish estimator fields to `StrategyStatus`)
- Modify: `crates/tui/src/state.rs` + `render.rs` (show est. reward/day, in-band, balance)
- Test: `crates/app/tests/` (paper integration)

- [ ] **Step 1: Write the failing paper integration test**

```rust
// Drive a RewardFarm MmLoop over a PaperMakerVenue with a synthetic mid-0.50
// book and reward params (min_size=5, max_spread=3c, rate=$100/day). Assert:
//  - both bid and ask are placed within the band (two-sided)
//  - sizes are balanced when flat (|bid-ask| small)
//  - cancel count stays 0 across cycles with a static book (sticky)
//  - estimator reports Q_min > 0 and est_daily_reward_usd > 0
//  - no naked-ask / phantom rejects
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p pm-app reward_farm_paper`
Expected: FAIL.

- [ ] **Step 3: Implement the wiring** to make it pass — publish `RewardFarmStatus { est_reward_usd_day, q_min, in_band, balance_ratio, cumulative_est }` on `StrategyStatus`; compute each estimator sample in the loop using `reward_score`. Render a small "Rewards" line per market in the TUI.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p pm-app reward_farm_paper`
Expected: PASS.

- [ ] **Step 5: A/B sanity** — add a test/example that runs both policies over the same book and logs estimated reward + realized PnL so the difference is visible. Not a hard assert (depends on book), just observability.

- [ ] **Step 6: Full verification + commit**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets`
Expected: PASS, no warnings.

```bash
git add -A
git commit -m "feat(app,tui): reward-farming estimator surfaced + paper integration + A/B"
```

---

## Final verification

- [ ] `cargo test --workspace` green
- [ ] `cargo clippy --workspace --all-targets` clean
- [ ] Paper run: `RUST_LOG=info cargo run --release --bin arb -- --headless --config mm-paper.toml` with `[strategies.mm] policy="reward_farm"` → dashboard shows two-sided in-band quotes, positive est. reward, delta within caps, low cancel rate.
- [ ] Then a tiny live canary to confirm a real midnight-UTC payout lands (Spec 1 exit criterion).

## Notes for the implementer
- The MM rails are in `crates/app/src/strategy/mm.rs`; do NOT change `QuoteManager`/`InventoryRisk`/venue behavior — only branch quote-generation/selection by `Policy`.
- Keep `SpreadCapture` byte-for-byte unchanged (existing tests must stay green).
- All money is integer micro-USDC on the venue path; the estimator works in f64 cents/shares for scoring only — never round money through f64.
- Confluence MUST be off in reward-farm mode (it is a taker signal).
