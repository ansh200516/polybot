# Reward-Farming MM — Spec 2 Phase A (Adverse-Selection Avoidance) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make reward farming net-positive in live by stepping aside from adverse fills — microprice fair value, book-imbalance/momentum quote-pull, and a size-rebalance requote — all RewardFarm-gated, SpreadCapture/arb untouched.

**Architecture:** Extend the existing `QuotePolicy` seam. New pure fns in `quote_policy.rs` (microprice, imbalance, pull decision); rolling signal state in a new `signals.rs`; the MM loop (`mm.rs`) consults them in the RewardFarm branch. Config extends `[reward_farm]` (threaded via the existing `MmParams::from_config(&mm, &rf)`).

**Tech Stack:** Rust (`pm-app`, `pm-config`, `pm-core` `Book`/`Ladder`), existing reward-farm scaffolding (`reward_quote_prices`, `reward_compute_quotes`, sticky `needs_requote`, `quote_order`).

**Spec:** `docs/superpowers/specs/2026-06-25-reward-farming-mm-spec2-design.md` (Phase A = §4).

**Book API:** `book.bids`/`book.asks` are `Ladder`; `Ladder::best() -> Option<Px>`, `Ladder::qty_at(px: Px) -> Qty` (`Qty(u…)`), `Px::microusdc(ts)`, `ts.unit_microusdc()`. Iterate N ticks from `best` via `qty_at` for depth.

**Scope note:** Phase B (complement hedging) is planned separately after Phase A validates (the spec phases are independently shippable).

---

## Task A1: Config — `[reward_farm]` Phase-A knobs

**Files:** Modify `crates/config/src/lib.rs` (`RewardFarm` struct + `Default` + tests). Modify `crates/app/src/strategy/mm.rs` (`MmParams` fields + `from_config`).

- [ ] **Step 1 — failing test** (config tests):
```rust
#[test]
fn reward_farm_phase_a_knobs_parse_and_default() {
    let c = Config::from_toml_str(
        "[reward_farm]\nmicroprice_levels=3\nsignal_window_ms=3000\npull_threshold=0.6\npull_cooldown_ms=5000\nsize_rebalance_pct=0.25\n",
    ).unwrap();
    assert_eq!(c.reward_farm.microprice_levels, 3);
    assert_eq!(c.reward_farm.pull_threshold, 0.6);
    let d = Config::default();
    assert_eq!(d.reward_farm.microprice_levels, 3);
    assert_eq!(d.reward_farm.pull_cooldown_ms, 5000);
}

#[test]
fn reward_farm_phase_a_validation() {
    let mut c = Config::default();
    c.reward_farm.pull_threshold = 1.5; // must be in [0,1]
    assert!(c.validate().is_err());
    let mut c2 = Config::default();
    c2.reward_farm.microprice_levels = 0; // must be >= 1
    assert!(c2.validate().is_err());
}
```
- [ ] **Step 2** Run `cargo test -p pm-config reward_farm_phase_a` → FAIL.
- [ ] **Step 3 — implement.** Add to `RewardFarm`:
```rust
    /// Book levels used for microprice + imbalance.
    pub microprice_levels: u16,
    /// Rolling window (ms) for the momentum signal.
    pub signal_window_ms: u64,
    /// |signal| above this pulls the endangered side ([0,1]).
    pub pull_threshold: f64,
    /// Suppress re-quoting a pulled side this long (ms).
    pub pull_cooldown_ms: u64,
    /// Re-place a side when its size lean drifts more than this fraction.
    pub size_rebalance_pct: f64,
```
Add to `RewardFarm::default()`: `microprice_levels: 3, signal_window_ms: 3000, pull_threshold: 0.6, pull_cooldown_ms: 5000, size_rebalance_pct: 0.25`. In `Config::validate()` add (match the file's `ConfigError::BadMoney` style + the `is_finite()` convention):
```rust
        if self.reward_farm.microprice_levels < 1 {
            return Err(ConfigError::BadMoney("reward_farm.microprice_levels must be >= 1"));
        }
        if !(0.0..=1.0).contains(&self.reward_farm.pull_threshold) || !self.reward_farm.pull_threshold.is_finite() {
            return Err(ConfigError::BadMoney("reward_farm.pull_threshold must be in [0,1]"));
        }
        if !(0.0..=1.0).contains(&self.reward_farm.size_rebalance_pct) || !self.reward_farm.size_rebalance_pct.is_finite() {
            return Err(ConfigError::BadMoney("reward_farm.size_rebalance_pct must be in [0,1]"));
        }
```
- [ ] **Step 4** Thread into `MmParams`: add the five fields, set them in `from_config(&mm, &rf)` from `rf.*` (mirror the existing `size_skew_max_ratio`/`requote_band_ticks` threading). Update `MmParams` test constructors.
- [ ] **Step 5** `cargo test -p pm-config && cargo test -p pm-app strategy::mm && cargo clippy -p pm-config -p pm-app --all-targets -- -D warnings` → green.
- [ ] **Step 6** Commit: `git add crates/config/src/lib.rs crates/app/src/strategy/mm.rs && git commit -m "feat(config,app): [reward_farm] Phase-A knobs (microprice/signal/pull/rebalance)"`

---

## Task A2: Microprice fair value (closes I2/§8.1)

**Files:** `crates/app/src/strategy/quote_policy.rs` (pure fn + test); `crates/app/src/strategy/mm.rs` (`reward_compute_quotes` uses it).

- [ ] **Step 1 — failing test:**
```rust
#[test]
fn microprice_leans_to_heavier_side_and_falls_back_to_mid() {
    // equal sizes -> mid
    assert!((microprice(0.50, 0.52, 100.0, 100.0) - 0.51).abs() < 1e-9);
    // more bid size -> price pulled UP toward the ask (heavier bid = buy pressure)
    let mp = microprice(0.50, 0.52, 300.0, 100.0);
    assert!(mp > 0.51 && mp < 0.52, "got {mp}");
    // zero sizes -> mid fallback
    assert!((microprice(0.50, 0.52, 0.0, 0.0) - 0.51).abs() < 1e-9);
}
```
- [ ] **Step 2** `cargo test -p pm-app microprice` → FAIL.
- [ ] **Step 3 — implement** (standard microprice: bid price weighted by ASK qty, ask price by BID qty; heavier bid pulls it toward the ask):
```rust
/// Size-weighted fair value. `bid`/`ask` are top-of-book prices ($), `bid_qty`/
/// `ask_qty` the resting sizes there. Weights the bid price by ask qty and vice
/// versa, so a heavier bid (buy pressure) pulls fair value UP toward the ask.
/// Falls back to the midpoint when both sizes are 0.
pub fn microprice(bid: f64, ask: f64, bid_qty: f64, ask_qty: f64) -> f64 {
    let denom = bid_qty + ask_qty;
    if denom <= 0.0 {
        return (bid + ask) / 2.0;
    }
    (bid * ask_qty + ask * bid_qty) / denom
}
```
- [ ] **Step 4** `cargo test -p pm-app microprice` → PASS.
- [ ] **Step 5 — wire into `reward_compute_quotes` (mm.rs):** replace the `adjusted_mid(bb, ba)` call with `microprice(bb, ba, bid_qty, ask_qty)` where `bid_qty = book.bids.qty_at(best_bid).0 as f64`, `ask_qty = book.asks.qty_at(best_ask).0 as f64`. Size-cutoff: when computing the top-of-book sizes, ignore a level whose qty (in shares) is below `min_size` (the reward `min_incentive_size`) — i.e. if `qty_at(best) < min_size_shares`, walk one tick inward for that side's representative size, or treat it as 0 (fallback to mid). Keep it simple: use `qty_at(best)`; if it's below `min_size`, fall back that side's qty to 0 (microprice still defined via the other side / mid). Document the choice. Pass the resulting microprice as `adj_mid` into `reward_quote_prices` AND into the estimator sampling so both use the same fair value.
- [ ] **Step 6** `cargo test -p pm-app && cargo clippy -p pm-app --all-targets -- -D warnings` → green. (Existing reward-farm tests may need their expected mid updated where they used the raw mid; update them to microprice where book sizes are set, or set equal sizes so microprice == mid.)
- [ ] **Step 7** Commit: `git add crates/app/src/strategy/quote_policy.rs crates/app/src/strategy/mm.rs && git commit -m "feat(app): microprice fair value for reward farming (size-weighted, closes I2)"`

---

## Task A3: Book-imbalance signal

**Files:** `quote_policy.rs` (pure fn + test).

- [ ] **Step 1 — failing test:**
```rust
#[test]
fn imbalance_sign_and_bounds() {
    assert!((imbalance(100.0, 100.0)).abs() < 1e-9);     // balanced -> 0
    assert!(imbalance(300.0, 100.0) > 0.0);              // bid-heavy -> positive
    assert!(imbalance(100.0, 300.0) < 0.0);              // ask-heavy -> negative
    assert!(imbalance(100.0, 0.0) <= 1.0 && imbalance(0.0, 100.0) >= -1.0);
    assert_eq!(imbalance(0.0, 0.0), 0.0);                // empty -> 0
}
```
- [ ] **Step 2** `cargo test -p pm-app imbalance` → FAIL.
- [ ] **Step 3 — implement:**
```rust
/// Order-book imbalance over summed depths: (bid - ask)/(bid + ask) in [-1,1].
/// Positive = buy pressure (price likely to tick up). 0 when both are 0.
pub fn imbalance(bid_depth: f64, ask_depth: f64) -> f64 {
    let denom = bid_depth + ask_depth;
    if denom <= 0.0 { 0.0 } else { (bid_depth - ask_depth) / denom }
}

/// Sum resting share-qty over up to `levels` ticks inward from `best` on a ladder.
pub fn ladder_depth(ladder: &pm_core::book::Ladder, levels: u16) -> f64 { /* iterate best..levels via qty_at */ }
```
For `ladder_depth`, iterate from `best` toward mid `levels` ticks summing `qty_at(px).0 as f64` (skip empty ticks). (Read `pm_core::book::Ladder` for the exact `Px` stepping; `Px::get()` is the tick index.)
- [ ] **Step 4** Tests pass; add a `ladder_depth` test on a crafted `Book` (set a few levels, assert summed depth over 2 levels).
- [ ] **Step 5** `cargo clippy` clean.
- [ ] **Step 6** Commit: `git add crates/app/src/strategy/quote_policy.rs && git commit -m "feat(app): order-book imbalance + ladder depth signal"`

---

## Task A4: Momentum / rolling signal state

**Files:** Create `crates/app/src/strategy/signals.rs` (+ `pub mod signals;` in `mod.rs`); test inside it.

- [ ] **Step 1 — failing test:**
```rust
#[test]
fn momentum_tracks_recent_microprice_change_in_window() {
    let mut s = SignalState::new(std::time::Duration::from_millis(3000));
    s.observe(0, 0.50);
    s.observe(1000, 0.51);
    s.observe(2000, 0.53);
    // upward move within window -> positive, normalized roughly to magnitude
    assert!(s.momentum(2000) > 0.0);
    // a sample older than the window is dropped
    s.observe(6000, 0.53);
    assert!((s.momentum(6000)).abs() < 1e-9, "stale samples evicted");
}
```
- [ ] **Step 2** `cargo test -p pm-app momentum` → FAIL.
- [ ] **Step 3 — implement** a small per-token `SignalState` holding `(ts_ms, microprice)` samples in a `VecDeque`, evicting samples older than `window`; `momentum(now_ms)` = `(latest - oldest_in_window) / oldest_in_window` (relative change), 0 when <2 samples in window. Pure (time passed in). Add `pub mod signals;` to `strategy/mod.rs`.
- [ ] **Step 4** Tests pass; `cargo clippy` clean.
- [ ] **Step 5** Commit: `git add crates/app/src/strategy/signals.rs crates/app/src/strategy/mod.rs && git commit -m "feat(app): rolling momentum signal state (windowed microprice change)"`

---

## Task A5: Pull decision + wire into the RewardFarm loop

**Files:** `quote_policy.rs` (pure fn + test); `mm.rs` (per-token `SignalState` map, compute signals each cycle, pull endangered side + cooldown).

- [ ] **Step 1 — failing test:**
```rust
#[test]
fn pull_endangered_side_on_strong_signal() {
    // strong buy pressure (imb+mom high positive) endangers the ASK (about to be lifted)
    let s = combined_signal(0.7, 0.3); // imbalance, momentum -> blended in [-1,1]
    assert!(should_pull(Side::Ask, s, 0.6));   // ask endangered, |s|>=thresh
    assert!(!should_pull(Side::Bid, s, 0.6));  // bid safe
    // weak signal -> no pull either side
    let w = combined_signal(0.2, 0.1);
    assert!(!should_pull(Side::Ask, w, 0.6) && !should_pull(Side::Bid, w, 0.6));
}
```
- [ ] **Step 2** `cargo test -p pm-app pull_endangered` → FAIL.
- [ ] **Step 3 — implement:**
```rust
/// Blend imbalance + momentum into one signed pressure in [-1,1] (positive = up).
pub fn combined_signal(imbalance: f64, momentum: f64) -> f64 {
    ((imbalance + momentum.clamp(-1.0, 1.0)) / 2.0).clamp(-1.0, 1.0)
}

/// Pull the side about to be run over: strong UP pressure endangers the ASK
/// (it will be lifted into a falling-for-us position), strong DOWN endangers the BID.
pub fn should_pull(side: Side, signal: f64, threshold: f64) -> bool {
    match side {
        Side::Ask => signal >= threshold,
        Side::Bid => signal <= -threshold,
    }
}
```
- [ ] **Step 4** `cargo test -p pm-app pull_endangered` → PASS.
- [ ] **Step 5 — wire into `mm.rs` (RewardFarm only):** add `signals: HashMap<TokenId, SignalState>` to `MmLoop`; each cycle, for each quoted token: update its `SignalState` with the current microprice, compute `imbalance(ladder_depth(bids,N), ladder_depth(asks,N))` and `momentum`, blend via `combined_signal`. If `should_pull(side, signal, pull_threshold)`, DROP that side from `desired` (don't place it) and record a `pulled_until = now + pull_cooldown_ms` in a per-`(token,side)` map; while `now < pulled_until`, keep that side pulled. Gate entirely on `Policy::RewardFarm`. Fold the signal values into the `rf_decisions` `state_json` (extend the existing logging). Add an mm-level test: on a crafted strongly-bid-imbalanced book, the ask side is omitted from the placed orders (and restored after cooldown on a balanced book).
- [ ] **Step 6** `cargo test -p pm-app && cargo clippy -p pm-app --all-targets -- -D warnings` → green; SpreadCapture unchanged.
- [ ] **Step 7** Commit: `git add crates/app/src/strategy/quote_policy.rs crates/app/src/strategy/mm.rs && git commit -m "feat(app): quote-pull on strong adverse signal (imbalance+momentum), RewardFarm-gated"`

---

## Task A6: Size-rebalance requote (closes §8.4(b))

**Files:** `quote_policy.rs` (extend the sticky decision) + `mm.rs`.

- [ ] **Step 1 — failing test:**
```rust
#[test]
fn requote_on_size_drift_even_when_price_in_band() {
    // price in band (no price-drift requote) but size lean drifted > pct -> requote
    assert!(needs_requote_size(/*resting*/100.0, /*target*/130.0, /*pct*/0.25)); // +30% > 25%
    assert!(!needs_requote_size(100.0, 115.0, 0.25)); // +15% < 25%
}
```
- [ ] **Step 2** `cargo test -p pm-app requote_on_size` → FAIL.
- [ ] **Step 3 — implement:**
```rust
/// True when a resting side's size has drifted more than `pct` from the new
/// target size (relative), so it should be re-placed to restore the inventory lean.
pub fn needs_requote_size(resting_size: f64, target_size: f64, pct: f64) -> bool {
    if resting_size <= 0.0 { return target_size > 0.0; }
    ((resting_size - target_size).abs() / resting_size) > pct
}
```
- [ ] **Step 4** PASS.
- [ ] **Step 5 — wire into the sticky keep in `mm.rs`:** the existing sticky logic keeps a resting side when `!needs_requote(price...)`. Extend it: keep only when `!needs_requote(price...) && !needs_requote_size(resting_size, target_size, size_rebalance_pct)`. When the size drifted past `pct`, allow the replace (re-leans size). Add an mm test: a static (in-band) book where inventory shifted enough that the target size leans > pct → the side IS re-placed (id re-keys on paper venue).
- [ ] **Step 6** `cargo test -p pm-app && cargo clippy` → green.
- [ ] **Step 7** Commit: `git add crates/app/src/strategy/quote_policy.rs crates/app/src/strategy/mm.rs && git commit -m "feat(app): size-rebalance requote trigger (closes spec §8.4b)"`

---

## Task A7: Integration + A/B on a synthetic adverse feed

**Files:** `crates/app/tests/` or an `mm.rs` integration test; `publisher.rs`/`render.rs` if surfacing the signal.

- [ ] **Step 1 — failing test** `reward_farm_pulls_on_adverse_feed_lowers_adverse_pnl`: drive a RewardFarm `MmLoop` over a scripted book that (a) rests balanced, then (b) becomes strongly bid-imbalanced + trending up; assert the ask side is pulled during the adverse phase (no ask placed / id absent), and that across the run the realized adverse-PnL per unit of estimated reward is **lower** than the same feed run with `pull_threshold = 1.0` (pull effectively off). Reuse the A/B harness from Spec-1 Task 11.
- [ ] **Step 2** Run → FAIL (pull not effective until wired/threshold).
- [ ] **Step 3 — make it pass** by confirming the Task-A5 wiring + threshold plumbing; if needed, surface `signal`/`pulled` in `RewardFarmStatus` for the TUI "rew" line (optional; keep minimal).
- [ ] **Step 4** `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` → green/clean.
- [ ] **Step 5** Commit: `git add -A && git commit -m "test(app): adverse-feed A/B shows quote-pull lowers adverse-PnL per reward"` (run `git status` first; stage explicitly if anything unrelated).

---

## Final verification
- [ ] `cargo test --workspace` green; `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] Paper smoke (`policy="reward_farm"`): logs/DB show the signal in `rf_decisions.state_json`; on an imbalanced market a side is pulled; no errors.
- [ ] Final whole-Phase-A code review (subagent) for integration coherence + SpreadCapture isolation.

## Notes for the implementer
- All new behavior is **RewardFarm-gated**; SpreadCapture/arb must stay byte-for-byte.
- Money stays integer micro on the venue path; signals/microprice are f64 (fair-value math only).
- Reuse `reward_score`/`reward_quote_prices`/`quote_order`/sticky `needs_requote`; do not reimplement.
- Phase B (complement hedging) is a separate plan after Phase A validates.
