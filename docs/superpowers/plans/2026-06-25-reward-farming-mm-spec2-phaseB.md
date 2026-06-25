# Reward-Farming MM — Spec 2 Phase B (Complement-Pair Quoting + Merge) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Let the live reward-farm MM quote two-sided **from flat** with no naked short by bidding the **complement pair** (BID-YES + BID-NO) — closing Spec-1 M3 — and recycle locked collateral by merging complete YES+NO sets (paper-simulated; live on-chain merge is deferred per `LiveVenue` `NotSupportedLive`).

**Architecture:** `hedging_enabled` (opt-in) switches the reward-farm quoting *unit* from "one token, bid+ask" (Spec-1/Phase-A) to "one market, bid-YES + bid-NO". The estimator, budget, size-skew, and the Phase-A pull all become **market-pair-aware** on that path. Merge is a paper-venue simulation + a clear live-deferral. All RewardFarm-gated; SpreadCapture/arb untouched.

**Tech Stack:** `pm-app` (`mm.rs`, `quote_policy.rs`, `wiring.rs`), `pm-config`, `pm-core` (`Book`, `TokenId`), `pm-execution` paper venue. Reuses Phase-A `reward_fair_value`/signals/pull and Spec-1 `reward_quote_prices`/`quote_order`/estimator.

**Spec:** `docs/superpowers/specs/2026-06-25-reward-farming-mm-spec2-design.md` §5.

**Hard live constraint (confirmed):** `LiveVenue::merge`/`split` return `VenueError::NotSupportedLive` (`crates/execution/src/live.rs:735`). So merge runs in PAPER only; on live, pairs hold to resolution and `max_gross_inventory_usd` is the capital control. Do NOT attempt live merge — guard it.

---

## Task B1: Config + complement map plumbing

**Files:** `crates/config/src/lib.rs` (`RewardFarm` + `Default` + validate + tests); `crates/app/src/strategy/mm.rs` (`MmParams` fields).

- [ ] **Step 1 — failing test (config):**
```rust
#[test]
fn reward_farm_phase_b_knobs() {
    let c = Config::from_toml_str("[reward_farm]\nhedging_enabled=true\nmerge_threshold_usd=5.0\n").unwrap();
    assert!(c.reward_farm.hedging_enabled);
    assert_eq!(c.reward_farm.merge_threshold_usd, 5.0);
    let d = Config::default();
    assert!(!d.reward_farm.hedging_enabled);            // opt-in, default OFF
    assert_eq!(d.reward_farm.merge_threshold_usd, 5.0);
}
#[test]
fn reward_farm_merge_threshold_must_be_finite_nonneg() {
    let mut c = Config::default();
    c.reward_farm.merge_threshold_usd = -1.0;
    assert!(c.validate().is_err());
}
```
- [ ] **Step 2** Run → FAIL.
- [ ] **Step 3** Add to `RewardFarm`: `pub hedging_enabled: bool,` and `pub merge_threshold_usd: f64,`; defaults `false` / `5.0`. Validate `merge_threshold_usd >= 0.0 && is_finite()` (mirror existing checks). Thread both into `MmParams` via `from_config(&mm,&rf)`.
- [ ] **Step 4** `cargo test -p pm-config && cargo test -p pm-app strategy::mm && cargo clippy -p pm-config -p pm-app --all-targets -- -D warnings` (clippy: `CARGO_TARGET_DIR=/Users/ansh.singh/test/target` if sandbox libsqlite3-sys error).
- [ ] **Step 5** Commit: `feat(config,app): [reward_farm] Phase-B knobs (hedging_enabled, merge_threshold_usd)`

---

## Task B2: Complement-pair selection + bid-only-both quoting (closes M3)

**Files:** `crates/app/src/wiring.rs` (`mm_quote_tokens`); `crates/app/src/main.rs` (per-market complement map); `crates/app/src/strategy/mm.rs` (quoting).

- [ ] **Step 1 — failing test (wiring):**
```rust
#[test]
fn mm_quote_tokens_hedging_returns_complement_pair() {
    // hedging_enabled -> BOTH tokens (yes,no); else single token (Spec-1/I1)
    let toks = mm_quote_tokens_hedged(&reg, &m, /*reward_farm*/ true, /*hedging*/ true);
    assert!(toks.contains(&m.yes) && toks.contains(&m.no));
    let single = mm_quote_tokens_hedged(&reg, &m, true, false);
    assert_eq!(single, vec![m.yes]);
}
```
(Extend the existing `mm_quote_tokens` signature with a `hedging: bool` param, or add a sibling; keep SpreadCapture path = `directional_quote_tokens`.)
- [ ] **Step 2** FAIL.
- [ ] **Step 3** Implement: in reward-farm mode, `hedging` ⇒ `vec![m.yes, m.no]`, else `vec![m.yes]` (I1). Thread `hedging_enabled` to the `main.rs` selection loop. Build a `complement: HashMap<TokenId, TokenId>` (yes↔no) and a `token_is_yes: HashMap<TokenId,bool>` (or store market+side) so the loop/estimator can pair the two bids; pass into `MmStrategy`/`MmLoop`.
- [ ] **Step 4 — quoting (mm.rs):** in the RewardFarm branch, when `hedging_enabled`, each quoted token emits a **BID only** (no ask) — reuse `reward_quote_prices` but take only the bid side (the ask requires inventory / is the wrong leg here). When NOT hedging, keep Spec-1 single-token bid+ask. Both legs flow through `quote_order` + the existing gating. Budget: count BOTH bid legs of a market (Task B3 refines).
- [ ] **Step 5** mm-level test `rewardfarm_hedging_quotes_bid_on_both_complement_tokens`: a RewardFarm+hedging loop over a market with both books → asserts a BID is placed on YES and on NO, and NO ASK on either (two-sided-from-flat, no naked short). Non-hedging stays single-token bid+ask.
- [ ] **Step 6** `cargo test -p pm-app && cargo clippy …` green; SpreadCapture untouched.
- [ ] **Step 7** Commit: `feat(app): complement-pair bid quoting under hedging (two-sided-from-flat, closes M3)`

---

## Task B3: Pair-aware estimator + budget

**Files:** `crates/app/src/strategy/mm.rs` (estimator `sample_reward_estimate`, budget in `main.rs` selection / loop).

- [ ] **Step 1 — failing test:** `rewardfarm_hedging_estimator_pairs_yes_no_into_qmin` — with a bid-YES (1¢ off mid) + bid-NO (1¢ off mid) on the same market, the published `RewardFarmStatus.q_min` equals the two-sided `q_min(Q_bidYES, Q_bidNO, mid)` (NOT each token scored single-sided / penalized 1/3). Compare to the non-hedging single-token case to lock the difference.
- [ ] **Step 2** FAIL (current estimator groups per token; bid-only-per-token would score single-sided).
- [ ] **Step 3** In hedging mode, the estimator must combine a market's bid-YES into `Q₁` and bid-NO into `Q₂` (per the reward formula's m/m′) → `q_min` per **market**, using the `complement` map. Keep the non-hedging per-token path unchanged. Use `reward_score::q_min`/`order_score` (don't reimplement).
- [ ] **Step 4 — budget (main.rs):** `per_market_cost` for a hedging market = bid-YES notional + bid-NO notional (≈ `min_size × (p + (1−p)) = min_size`/share of the pair, plus both legs' sizes) — count BOTH legs so the budget funds the right number of pairs. Update the selection cost accordingly; document.
- [ ] **Step 5** `cargo test -p pm-app && cargo clippy …` green.
- [ ] **Step 6** Commit: `feat(app): pair-aware reward estimator + budget for complement-pair hedging`

---

## Task B4: Pair delta-neutral sizing + pull mapping

**Files:** `crates/app/src/strategy/mm.rs`; reuse `quote_policy::skewed_sizes`/`combined_signal`/`should_pull`.

- [ ] **Step 1 — failing test:** `rewardfarm_hedging_skews_bids_by_net_delta` — net long YES (yes_qty > no_qty) ⇒ the YES bid is sized SMALLER and the NO bid LARGER (rebalancing toward delta-neutral), ratio ≤ `size_skew_max_ratio`; flat ⇒ balanced. And `rewardfarm_hedging_pull_maps_up_yes_to_no_bid` — strong UP signal on YES pulls the **NO** bid (keeps the YES bid); strong DOWN pulls the YES bid.
- [ ] **Step 2** FAIL.
- [ ] **Step 3** Sizing: delta `= yes_qty_micro − no_qty_micro`; feed it to `skewed_sizes` so the *bid on the heavier side shrinks* and the *bid on the lighter side grows* (re-express the existing skew for two bids). Pull: compute the per-market signal once (microprice/imbalance/momentum on the YES book as today); map `should_pull(Bid-on-NO)` ⇐ strong UP (signal ≥ +thr), `should_pull(Bid-on-YES)` ⇐ strong DOWN (signal ≤ −thr); reuse the cooldown map keyed by `(token, Side::Bid)`. Gate all on `hedging_enabled` within RewardFarm.
- [ ] **Step 4** `cargo test -p pm-app && cargo clippy …` green.
- [ ] **Step 5** Commit: `feat(app): complement-pair delta-neutral sizing + pull mapping`

---

## Task B5: Merge complete sets (paper sim; live deferred)

**Files:** `crates/execution/src/paper_maker.rs` (or wherever the paper MM venue/inventory lives) for the sim; `crates/app/src/strategy/mm.rs` (merge decision); guard live.

- [ ] **Step 1 — failing test:** `merge_recycles_complete_set_in_paper` — after the loop accumulates `yes_qty` and `no_qty` with `min(yes,no) > merge_threshold`, a merge reduces BOTH legs by `min` and credits cash by `min × $1` (a complete set = $1); gross inventory drops; on a live venue `merge` is `NotSupportedLive` so the decision is a no-op (logged), pairs untouched.
- [ ] **Step 2** FAIL.
- [ ] **Step 3** Implement a `maybe_merge_sets()` in the MM loop: compute `matched = min(yes_inv, no_inv)` per market from `InventoryRisk`; if `matched_usd > merge_threshold` AND the venue supports merge (paper), reduce both `InventoryRisk` legs by `matched` and credit cash/realized by `matched × unit` (a merged set returns $1/set); record a store fill/conversion row. On the **live** venue, `merge` is `NotSupportedLive` → skip with a one-time `warn!` (pairs hold to resolution; gross cap is the control). Paper venue: simulate (mirror `basket.rs` `Ledger::merge`). Run `maybe_merge_sets` each cycle (cheap) or on fills.
- [ ] **Step 4** `cargo test -p pm-app && cargo test -p pm-execution && cargo clippy …` green.
- [ ] **Step 5** Commit: `feat(app,execution): merge complete YES+NO sets to recycle capital (paper; live deferred M6)`

---

## Task B6: Integration + final Phase-B review

- [ ] **Step 1 — integration test** `reward_farm_hedging_two_sided_from_flat_and_merges`: a RewardFarm+hedging loop over a market with both books + paper taker fills → assert (a) from flat it places bids on BOTH tokens (no ask, no naked-short reject), (b) fills accumulate toward a delta-neutral pair (|yes−no| bounded by the skew), (c) once a complete set exceeds `merge_threshold` it is merged (both legs drop, cash credited), (d) `RewardFarmStatus` shows two-sided `q_min > 0`. A/B vs non-hedging single-token for observability.
- [ ] **Step 2** `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` (pinned target dir) → green/clean.
- [ ] **Step 3** Commit: `test(app): complement-pair two-sided-from-flat + merge integration`
- [ ] **Step 4** Final whole-Phase-B review subagent: integration coherence, SpreadCapture/arb isolation, no-naked-short correctness, live-merge guard, money integer-µ.

## Notes for the implementer
- `hedging_enabled = false` (default) MUST keep Spec-1/Phase-A behavior byte-for-byte (single-token bid+ask). All complement-pair logic is gated on `hedging_enabled` within `Policy::RewardFarm`.
- Live merge is `NotSupportedLive` — never call it on live; guard + log. Paper simulates it.
- Reuse `reward_score`, `reward_quote_prices`, `skewed_sizes`, `combined_signal`/`should_pull`, `reward_fair_value` — do not reimplement.
- Money stays integer-µUSDC on the venue path; a complete set merges to exactly $1/set.
