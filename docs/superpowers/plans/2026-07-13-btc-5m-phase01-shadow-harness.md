# BTC 5m — Phase 0/1 Shadow-Measurement Harness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a config-gated, **read-only** `btc5m` strategy that runs in parallel with the copy bot, prices a fair P(up) for the current Polymarket "BTC Up or Down 5m" window from a composite BTC spot feed, and logs (fair value vs live book) to SQLite every window — so we can *measure* whether the terminal-convergence edge is harvestable before risking any capital. Plus `pnl` parity and a gate-metric report.

**Architecture:** A new `Btc5mStrategy` implements the existing `Strategy` trait and runs under `StrategyHost` in the same `arb` process (same wallet — avoids collateral/reconcile collisions). It reads books via `ctx.fetcher`, spot via a new `pm_ingestion::spot` composite feed, computes fair value in a pure `model` module, discovers/rotates the 5-min market via a new `pm_ingestion::gamma` client, and writes shadow rows via `StoreMsg`. **It places NO orders in Phase 0/1.** Default `[strategies.btc5m].enabled = false`, so shipping the binary does not change copy-bot behavior.

**Tech Stack:** Rust (edition 2024, workspace `pm-*` crates), tokio, reqwest (rustls), rusqlite, serde, watch/mpsc channels. Tests: `#[test]` / `#[tokio::test]` + proptest. Deploy: systemd on the existing box; `pnl` = `deploy/status.sh`.

**Conventions:**
- Cargo is not on PATH by default → prefix every cargo command with `export PATH="$HOME/.cargo/bin:$PATH" &&`.
- Every test module starts with `#[cfg(test)] mod tests { #![allow(clippy::unwrap_used, clippy::expect_used)] use super::*; … }` (workspace denies `unwrap`/`expect` outside tests).
- Money is µUSDC (`i64`/`i128`); shares are µshares (`Qty(u64)`); prices are `Px` (private field — construct via `Px::new(tick, ts)`).
- Commit after every green task.

---

## File structure

| File | New/Mod | Responsibility |
|---|---|---|
| `crates/app/src/strategy/btc5m/model.rs` | New | Pure fair-value math: `norm_cdf`, `EwmaVol`, `fair_p_up`, `snap_prob_to_px`. |
| `crates/app/src/strategy/btc5m/market.rs` | New | `Window` value + rotation state machine (which 5-min market is live now). |
| `crates/app/src/strategy/btc5m/shadow.rs` | New | `ShadowSample` record + conversion to `StoreMsg`. |
| `crates/app/src/strategy/btc5m/mod.rs` | New | `Btc5mStrategy` (impl `Strategy`) — the read-only shadow loop. |
| `crates/app/src/strategy/mod.rs` | Mod | `pub mod btc5m;`. |
| `crates/ingestion/src/spot.rs` | New | Composite BTC spot feed (multi-exchange REST poll → median mid) + 1-min bars. |
| `crates/ingestion/src/gamma.rs` | New | Gamma `/events` client + parse for the current 5-min market's tokens/tick/window. |
| `crates/ingestion/src/lib.rs` | Mod | `pub mod spot; pub mod gamma;`. |
| `crates/store/src/lib.rs` | Mod | `btc5m_shadow` table + `add_btc5m_shadow`. |
| `crates/store/src/writer.rs` | Mod | `StoreMsg::Btc5mShadow` variant + writer arm. |
| `crates/store/src/read.rs` | Mod | `ReadStore::btc5m_shadow_rows`. |
| `crates/config/src/lib.rs` | Mod | `Btc5mCfg` (toggle) + `Btc5mParamsCfg` (knobs) + validate. |
| `crates/app/src/wiring.rs` | Mod | `PlatformEnvelopes.btc5m` + capital carve. |
| `crates/app/src/main.rs` | Mod | Register `btc5m` when enabled (mirror the copy block). |
| `deploy/status.sh` | Mod | Add a "BTC 5M BOT" section to `pnl`. |
| `deploy/btc5m_report.py` | New | Phase-1 gate metric (median net edge at T-15s). |
| `mm-live-copy-canary.toml` | Mod | `[strategies.btc5m]` (disabled) + `[btc5m]`. |

---

## Task 1: Fair-value model (pure)

**Files:**
- Create: `crates/app/src/strategy/btc5m/model.rs`
- Modify: `crates/app/src/strategy/mod.rs` (add `pub mod btc5m;`) and create `crates/app/src/strategy/btc5m/mod.rs` with `pub mod model;` (temporary until Task 6 fills it).

- [ ] **Step 1: Create the module tree so `model` compiles.**

Add to `crates/app/src/strategy/mod.rs` after the existing `pub mod` lines (near line 24-31):
```rust
pub mod btc5m;
```
Create `crates/app/src/strategy/btc5m/mod.rs`:
```rust
//! BTC "Up or Down 5m" strategy (spec 2026-07-13). Phase 0/1 is READ-ONLY:
//! it prices a fair P(up) and logs it against the live book; it emits NO orders.
pub mod model;
```

- [ ] **Step 2: Write the failing test** in `crates/app/src/strategy/btc5m/model.rs`:

```rust
//! Pure fair-value math for the BTC 5m binary. Driftless-normal digital:
//! `p_up = Φ((spot − strike) / σ_τ)`, σ from a causal EWMA of 1-minute $-returns
//! (√-time to the remaining horizon). No I/O, no lookahead — callers pass only
//! data available at decision time.

/// Standard normal CDF (Abramowitz & Stegun 26.2.17; |error| < 7.5e-8).
pub fn norm_cdf(x: f64) -> f64 {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool { (a - b).abs() < tol }

    #[test]
    fn norm_cdf_known_values() {
        assert!(approx(norm_cdf(0.0), 0.5, 1e-9));
        assert!(approx(norm_cdf(1.0), 0.8413447, 1e-6));
        assert!(approx(norm_cdf(-1.0), 0.1586553, 1e-6));
        assert!(approx(norm_cdf(1.96), 0.9750021, 1e-6));
        assert!(approx(norm_cdf(-1.96), 0.0249979, 1e-6));
    }
}
```

- [ ] **Step 3: Run it — expect FAIL (panic `unimplemented`).**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::model::tests::norm_cdf_known_values`
Expected: FAIL (panics at `unimplemented!()`).

- [ ] **Step 4: Implement `norm_cdf`.**

```rust
pub fn norm_cdf(x: f64) -> f64 {
    const B0: f64 = 0.231_641_9;
    const B: [f64; 5] = [0.319_381_530, -0.356_563_782, 1.781_477_937, -1.821_255_978, 1.330_274_429];
    let t = 1.0 / (1.0 + B0 * x.abs());
    let pdf = (-x * x / 2.0).exp() / (2.0 * std::f64::consts::PI).sqrt();
    let poly = t * (B[0] + t * (B[1] + t * (B[2] + t * (B[3] + t * B[4]))));
    let upper_tail = pdf * poly; // ≈ 1 − Φ(|x|)
    if x >= 0.0 { 1.0 - upper_tail } else { upper_tail }
}
```

- [ ] **Step 5: Run it — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::model::tests::norm_cdf_known_values`
Expected: PASS.

- [ ] **Step 6: Write the failing test for `EwmaVol` + `fair_p_up`.** Append to the `tests` module:

```rust
    #[test]
    fn ewma_vol_warms_up_and_scales_sqrt_time() {
        let mut v = EwmaVol::new(120.0, 3);
        assert!(!v.ready());
        for r in [40.0, -30.0, 50.0] { v.update(r); }
        assert!(v.ready());
        let s1 = v.sigma_1min();
        assert!(s1 > 0.0);
        // √-time: 5-min σ = 1-min σ × √5; 4× the seconds ⇒ 2× the σ.
        assert!(approx(v.sigma_tau(300.0), s1 * 5f64.sqrt(), 1e-9));
        assert!(approx(v.sigma_tau(240.0) / v.sigma_tau(60.0), 2.0, 1e-9));
    }

    #[test]
    fn fair_p_up_is_half_at_strike_and_monotone() {
        // At the strike, driftless ⇒ 0.5.
        assert!(approx(fair_p_up(100_000.0, 100_000.0, 60.0, 42.0).unwrap(), 0.5, 1e-9));
        // z = +1 (d = σ_τ) ⇒ Φ(1).
        let sigma_1min = 42.0;
        let sigma_tau = sigma_1min * (15.0f64 / 60.0).sqrt();
        let up = fair_p_up(100_000.0 + sigma_tau, 100_000.0, 15.0, sigma_1min).unwrap();
        assert!(approx(up, norm_cdf(1.0), 1e-9));
        // τ ≤ 0 collapses to the terminal indicator (ties → UP).
        assert_eq!(fair_p_up(100_000.0, 100_000.0, 0.0, 42.0).unwrap(), 1.0);
        assert_eq!(fair_p_up(99_999.0, 100_000.0, 0.0, 42.0).unwrap(), 0.0);
    }
```

- [ ] **Step 7: Run — expect FAIL (types/fns undefined).**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::model::tests`
Expected: FAIL to compile (`EwmaVol`, `fair_p_up` not found).

- [ ] **Step 8: Implement `EwmaVol` + `fair_p_up`.** Add above the tests:

```rust
/// Causal EWMA of 1-minute squared $-returns → per-1-minute $ volatility.
/// `half_life_min` sets the decay (λ = 0.5^(1/HL)); `warmup` samples must
/// accrue before [`ready`](Self::ready). √-time gives σ over any horizon.
#[derive(Debug, Clone)]
pub struct EwmaVol { lambda: f64, var: f64, samples: u32, warmup: u32 }

impl EwmaVol {
    pub fn new(half_life_min: f64, warmup: u32) -> Self {
        EwmaVol { lambda: 0.5f64.powf(1.0 / half_life_min), var: 0.0, samples: 0, warmup }
    }
    /// Feed one 1-minute $-return (close−close).
    pub fn update(&mut self, ret_usd: f64) {
        if !ret_usd.is_finite() { return; }
        let sq = ret_usd * ret_usd;
        self.var = if self.samples == 0 { sq } else { self.lambda * self.var + (1.0 - self.lambda) * sq };
        self.samples = self.samples.saturating_add(1);
    }
    pub fn ready(&self) -> bool { self.samples >= self.warmup }
    pub fn sigma_1min(&self) -> f64 { self.var.sqrt() }
    /// σ over `tau_secs` seconds of remaining horizon (√-time from 1-minute).
    pub fn sigma_tau(&self, tau_secs: f64) -> f64 { self.sigma_1min() * (tau_secs / 60.0).sqrt() }
}

/// Fair P(up) for a driftless normal digital. `spot`/`strike` in $, `tau_secs`
/// remaining, `sigma_1min` the per-1-minute $ vol. Ties (spot == strike) and
/// τ ≤ 0 resolve UP (venue rule ≥). Returns `None` on non-finite inputs.
pub fn fair_p_up(spot: f64, strike: f64, tau_secs: f64, sigma_1min: f64) -> Option<f64> {
    if !(spot.is_finite() && strike.is_finite() && sigma_1min.is_finite()) { return None; }
    if tau_secs <= 0.0 { return Some(if spot >= strike { 1.0 } else { 0.0 }); }
    let sigma_tau = sigma_1min * (tau_secs / 60.0).sqrt();
    if sigma_tau <= 0.0 { return Some(if spot >= strike { 1.0 } else { 0.0 }); }
    Some(norm_cdf((spot - strike) / sigma_tau))
}
```

- [ ] **Step 9: Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::model::tests`
Expected: PASS (3 tests).

- [ ] **Step 10: Write the failing test for `snap_prob_to_px`.** Append:

```rust
    use pm_core::num::TickSize;

    #[test]
    fn snap_prob_to_px_rounds_to_tick_and_guards_extremes() {
        // 0.564 on a Cent grid → tick 56 (56¢).
        let px = snap_prob_to_px(0.564, TickSize::Cent).unwrap();
        assert_eq!(px.get(), 56);
        // Milli grid keeps more resolution: 0.5643 → tick 564.
        assert_eq!(snap_prob_to_px(0.5643, TickSize::Milli).unwrap().get(), 564);
        // Degenerate probabilities have no interior tick.
        assert!(snap_prob_to_px(0.0, TickSize::Cent).is_none());
        assert!(snap_prob_to_px(1.0, TickSize::Cent).is_none());
        assert!(snap_prob_to_px(0.999_9, TickSize::Cent).is_none()); // rounds to 100 → out of range
    }
```

- [ ] **Step 11: Run — expect FAIL (fn undefined).**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::model::tests::snap_prob_to_px`
Expected: FAIL to compile.

- [ ] **Step 12: Implement `snap_prob_to_px`.** Add (and `use pm_core::num::{Px, TickSize};` at the top of the file):

```rust
use pm_core::num::{Px, TickSize};

/// Snap a probability in (0,1) to the market's tick as a [`Px`]. Rounds to the
/// nearest tick; returns `None` for degenerate probs that land on 0 or the top
/// (no valid interior tick — matches `Px::new`'s 1..levels-1 domain).
pub fn snap_prob_to_px(p: f64, ts: TickSize) -> Option<Px> {
    if !p.is_finite() || p <= 0.0 || p >= 1.0 { return None; }
    let unit = ts.unit_microusdc() as f64;          // Cent=10_000, Milli=1_000
    let micro = (p * 1_000_000.0).round();
    let ticks = (micro / unit).round() as i64;
    if ticks < 1 || ticks >= i64::from(ts.levels()) { return None; }
    Px::new(ticks as u16, ts).ok()
}
```

- [ ] **Step 13: Run all model tests — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::model`
Expected: PASS (4 tests).

- [ ] **Step 14: Commit.**

```bash
git add crates/app/src/strategy/mod.rs crates/app/src/strategy/btc5m/mod.rs crates/app/src/strategy/btc5m/model.rs
git commit -m "feat(btc5m): pure fair-value model (norm_cdf, EWMA vol, p_up, tick snap)"
```

---

## Task 2: Config — `[strategies.btc5m]` toggle + `[btc5m]` knobs

**Files:**
- Modify: `crates/config/src/lib.rs` (add `Btc5mCfg`, `Btc5mParamsCfg`, wire into `Strategies`/`Config`, validate).

- [ ] **Step 1: Write the failing test.** In the `tests` mod of `crates/config/src/lib.rs`, add:

```rust
    #[test]
    fn btc5m_defaults_and_validation() {
        let c = Config::default();
        assert!(!c.strategies.btc5m.enabled);          // OFF by default
        assert_eq!(c.btc5m_params.vol_half_life_min, 120.0);

        // Parses from TOML and round-trips the toggle + a knob.
        let c = Config::from_toml_str(
            "[strategies.btc5m]\nenabled = true\ncapital_usd = 50.0\n\n[btc5m]\nz_threshold = 1.5\n"
        ).unwrap();
        assert!(c.strategies.btc5m.enabled);
        assert_eq!(c.strategies.btc5m.capital_usd, 50.0);
        assert_eq!(c.btc5m_params.z_threshold, 1.5);

        // capital > bankroll when enabled is rejected.
        let bad = Config::from_toml_str(
            "[capital]\nbankroll_usd = 10.0\n[strategies.btc5m]\nenabled = true\ncapital_usd = 999.0\n"
        );
        assert!(bad.is_err());
    }
```

- [ ] **Step 2: Run — expect FAIL (fields don't exist).**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-config -- btc5m_defaults_and_validation`
Expected: FAIL to compile (`strategies.btc5m` / `btc5m_params` unknown).

- [ ] **Step 3: Add the structs.** Near `CopyCfg` (after line ~523) add:

```rust
/// Toggle + capital for the BTC 5m strategy (mirrors [`CopyCfg`]). The tuning
/// knobs live in the top-level `[btc5m]` section ([`Btc5mParamsCfg`]).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Btc5mCfg { pub enabled: bool, pub live: bool, pub capital_usd: f64 }

impl Default for Btc5mCfg {
    fn default() -> Self { Btc5mCfg { enabled: false, live: false, capital_usd: 25.0 } }
}
```
Near `CopyParamsCfg` (after line ~861) add:

```rust
/// Tuning knobs for the BTC 5m strategy (TOML section `[btc5m]`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Btc5mParamsCfg {
    /// EWMA half-life (minutes) for the 1-minute $-return variance.
    pub vol_half_life_min: f64,
    /// 1-minute vol samples required before the model is "ready".
    pub vol_warmup_samples: u32,
    /// |z| = |d/σ_τ| a side must clear to be counted a "leader" in shadow stats.
    pub z_threshold: f64,
    /// Shadow-sample cadence within a window (ms).
    pub sample_interval_ms: u64,
    /// Comma-free list of spot sources to poll (e.g. "coinbase,kraken,binance").
    pub spot_sources: Vec<String>,
    /// Spot poll cadence (ms).
    pub spot_poll_ms: u64,
    /// Seconds-to-close at/under which sampling switches to dense cadence.
    pub dense_window_secs: i64,
}

impl Default for Btc5mParamsCfg {
    fn default() -> Self {
        Btc5mParamsCfg {
            vol_half_life_min: 120.0,
            vol_warmup_samples: 180,
            z_threshold: 1.5,
            sample_interval_ms: 1000,
            spot_sources: vec!["coinbase".into(), "kraken".into()],
            spot_poll_ms: 1000,
            dense_window_secs: 60,
        }
    }
}
```

- [ ] **Step 4: Wire into `Strategies` and `Config`.** In `pub struct Strategies { pub mm: Mm, pub copy: CopyCfg }` (line ~482) add the field:
```rust
    pub btc5m: Btc5mCfg,
```
(If `Strategies` has an explicit `impl Default`, add `btc5m: Btc5mCfg::default(),` to it; if it `#[derive(Default)]`, nothing else is needed since `Btc5mCfg: Default`.)
In `Config` (after the `copy_params` field, line ~28-32) add:
```rust
    #[serde(rename = "btc5m")]
    pub btc5m_params: Btc5mParamsCfg,
```
(`Config` derives `Default`, so the new field defaults automatically.)

- [ ] **Step 5: Add the validate block.** Inside `Config::validate` (near the copy block, ~line 1261-1319) add:

```rust
        if self.strategies.btc5m.capital_usd < 0.0 || !self.strategies.btc5m.capital_usd.is_finite() {
            return Err(ConfigError::BadMoney("strategies.btc5m.capital_usd must be finite and ≥ 0"));
        }
        if self.strategies.btc5m.enabled
            && self.strategies.btc5m.capital_usd > self.capital.bankroll_usd
        {
            return Err(ConfigError::BadMoney("strategies.btc5m.capital_usd must be ≤ capital.bankroll_usd when enabled"));
        }
        let bp = &self.btc5m_params;
        if !(bp.vol_half_life_min.is_finite() && bp.vol_half_life_min > 0.0) {
            return Err(ConfigError::BadMoney("btc5m.vol_half_life_min must be > 0"));
        }
        if !(bp.z_threshold.is_finite() && bp.z_threshold >= 0.0) {
            return Err(ConfigError::BadMoney("btc5m.z_threshold must be finite and ≥ 0"));
        }
        if bp.sample_interval_ms == 0 || bp.spot_poll_ms == 0 {
            return Err(ConfigError::BadMoney("btc5m.sample_interval_ms and spot_poll_ms must be > 0"));
        }
        if bp.spot_sources.is_empty() {
            return Err(ConfigError::BadMoney("btc5m.spot_sources must list at least one source"));
        }
```

- [ ] **Step 6: Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-config`
Expected: PASS (new test + existing config tests, incl. `config_smoke`, still green).

- [ ] **Step 7: Commit.**

```bash
git add crates/config/src/lib.rs
git commit -m "feat(btc5m): config — [strategies.btc5m] toggle + [btc5m] knobs + validate"
```

---

## Task 3: Store — `btc5m_shadow` table + write/read path

**Files:**
- Modify: `crates/store/src/lib.rs` (SCHEMA + `add_btc5m_shadow`), `crates/store/src/writer.rs` (`StoreMsg::Btc5mShadow` + arm), `crates/store/src/read.rs` (`btc5m_shadow_rows`).

- [ ] **Step 1: Write the failing round-trip test.** In the `tests` mod of `crates/store/src/lib.rs`:

```rust
    #[test]
    fn btc5m_shadow_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let mut s = Store::open(&path).unwrap();
        s.add_btc5m_shadow(&Btc5mShadowRow {
            ts_ms: 1_700_000_000_000, condition_id: "0xabc".into(), secs_to_go: 15,
            strike: 62_922.41, spot: 62_931.0, sigma_tau: 40.0, p_up: 0.58,
            best_bid_micro: 550_000, best_ask_micro: 560_000, tick_decimals: 2,
        }).unwrap();
        s.add_btc5m_shadow(&Btc5mShadowRow {
            ts_ms: 1_700_000_001_000, condition_id: "0xabc".into(), ..Default::default()
        }).unwrap();
        let rs = pm_store::read::ReadStore::open(&path).unwrap();
        let rows = rs.btc5m_shadow_rows(10).unwrap();          // newest-first
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts_ms, 1_700_000_001_000);
        assert_eq!(rows[1].secs_to_go, 15);
    }
```

> Note: `ReadStore::open` takes a *path*, so the round-trip uses the tempfile idiom (matches store lib.rs:1020). `Btc5mShadowRow` derives `Default` (Step 3) for the `..Default::default()` above.

- [ ] **Step 2: Run — expect FAIL (type/method undefined).**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-store -- btc5m_shadow_roundtrip`
Expected: FAIL to compile.

- [ ] **Step 3: Add the row type + table + insert.** In `crates/store/src/lib.rs`:

Add the row struct near `CopyPositionRow` (line ~165):
```rust
/// One shadow observation of the BTC 5m book vs the model's fair value.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Btc5mShadowRow {
    pub ts_ms: i64,
    pub condition_id: String,
    pub secs_to_go: i64,
    pub strike: f64,
    pub spot: f64,
    pub sigma_tau: f64,
    pub p_up: f64,
    pub best_bid_micro: i64,   // µUSDC of the YES/UP token best bid; 0 if none
    pub best_ask_micro: i64,   // µUSDC of the YES/UP token best ask; 0 if none
    pub tick_decimals: i64,    // 2 = Cent, 3 = Milli
}
```
Append to the `SCHEMA` const (after `copy_positions`, line ~302):
```sql
CREATE TABLE IF NOT EXISTS btc5m_shadow (
  ts_ms INTEGER NOT NULL,
  condition_id TEXT NOT NULL,
  secs_to_go INTEGER NOT NULL,
  strike REAL NOT NULL,
  spot REAL NOT NULL,
  sigma_tau REAL NOT NULL,
  p_up REAL NOT NULL,
  best_bid_micro INTEGER NOT NULL,
  best_ask_micro INTEGER NOT NULL,
  tick_decimals INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_btc5m_shadow_cond_ts ON btc5m_shadow (condition_id, ts_ms);
```
Add the insert method near `add_day_realized` (line ~767):
```rust
    pub fn add_btc5m_shadow(&mut self, r: &Btc5mShadowRow) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO btc5m_shadow
             (ts_ms, condition_id, secs_to_go, strike, spot, sigma_tau, p_up,
              best_bid_micro, best_ask_micro, tick_decimals)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            rusqlite::params![
                r.ts_ms, r.condition_id, r.secs_to_go, r.strike, r.spot, r.sigma_tau,
                r.p_up, r.best_bid_micro, r.best_ask_micro, r.tick_decimals
            ],
        )?;
        Ok(())
    }
```

- [ ] **Step 4: Add the read query.** In `crates/store/src/read.rs` near `copy_open_positions` (line ~300):
```rust
    pub fn btc5m_shadow_rows(&self, limit: usize) -> Result<Vec<crate::Btc5mShadowRow>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT ts_ms, condition_id, secs_to_go, strike, spot, sigma_tau, p_up,
                    best_bid_micro, best_ask_micro, tick_decimals
             FROM btc5m_shadow ORDER BY ts_ms DESC LIMIT ?1")?;
        let rows = stmt.query_map([limit as i64], |r| Ok(crate::Btc5mShadowRow {
            ts_ms: r.get(0)?, condition_id: r.get(1)?, secs_to_go: r.get(2)?,
            strike: r.get(3)?, spot: r.get(4)?, sigma_tau: r.get(5)?, p_up: r.get(6)?,
            best_bid_micro: r.get(7)?, best_ask_micro: r.get(8)?, tick_decimals: r.get(9)?,
        }))?.collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
```

- [ ] **Step 5: Add the async `StoreMsg` variant + writer arm.** In `crates/store/src/writer.rs`, add to the `StoreMsg` enum (near `DayRealized`):
```rust
    Btc5mShadow(crate::Btc5mShadowRow),
```
And in `run_writer`'s match (mirror the `DayRealized` arm):
```rust
        StoreMsg::Btc5mShadow(row) => {
            if let Err(e) = store.add_btc5m_shadow(&row) { store.note_write_error(&e); }
        }
```
(If the existing arms use a different error-noting helper name, match it exactly — grep `write_errors`/`note_write_error` in `writer.rs`.)

- [ ] **Step 6: Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-store`
Expected: PASS (round-trip + existing store tests, incl. legacy-migration tests, still green — the change is additive `CREATE TABLE IF NOT EXISTS`).

- [ ] **Step 7: Commit.**

```bash
git add crates/store/src/lib.rs crates/store/src/read.rs crates/store/src/writer.rs
git commit -m "feat(btc5m): store — btc5m_shadow table + write/read path"
```

---

## Task 4: Composite BTC spot feed

**Files:**
- Create: `crates/ingestion/src/spot.rs`
- Modify: `crates/ingestion/src/lib.rs` (`pub mod spot;`)

Design: a REST-poll MVP (simple, robust, no WS reconnection burden for Phase 1). Each poll fetches the last-trade price from each configured exchange, and the feed publishes the **median** mid on a `watch` channel; a 1-minute bar aggregator emits close-to-close $-returns to drive `EwmaVol`. Parse functions are split from I/O for unit tests.

- [ ] **Step 1: Write the failing test** in `crates/ingestion/src/spot.rs`:

```rust
//! Composite BTC/USD spot feed for the btc5m strategy. Polls several exchanges'
//! last-trade REST endpoints and publishes the MEDIAN price (a cheap proxy for
//! the Chainlink Data Streams multi-venue aggregate that actually settles the
//! market — NOT any single exchange, and NOT the Polymarket UI feed). A 1-minute
//! bar aggregator turns the tape into close-to-close $-returns for vol.

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn parse_coinbase_and_kraken_last_price() {
        // Coinbase: GET /products/BTC-USD/ticker → {"price":"62931.12", ...}
        assert!((parse_coinbase(r#"{"price":"62931.12","time":"..."}"#).unwrap() - 62931.12).abs() < 1e-6);
        // Kraken: GET /0/public/Ticker?pair=XBTUSD → {"result":{"XXBTZUSD":{"c":["62930.50","0.01"]}}}
        let k = r#"{"error":[],"result":{"XXBTZUSD":{"c":["62930.50","0.01"]}}}"#;
        assert!((parse_kraken(k).unwrap() - 62930.50).abs() < 1e-6);
    }

    #[test]
    fn median_of_prices() {
        assert!((median(&[100.0, 102.0, 101.0]) - 101.0).abs() < 1e-9);
        assert!((median(&[100.0, 102.0]) - 101.0).abs() < 1e-9);
        assert!(median(&[]).is_nan());
    }

    #[test]
    fn one_minute_bars_emit_close_to_close_returns() {
        let mut agg = MinuteBars::new();
        assert_eq!(agg.push(60_000, 100.0), None);      // first bar opens, no return yet
        assert_eq!(agg.push(90_000, 105.0), None);      // same minute (60_000..120_000)
        // Crossing into the next minute closes the prior bar (last price 105) and
        // returns the close-to-close $ move vs the previous bar's close.
        assert_eq!(agg.push(120_000, 107.0), None);     // first close has no prior → None
        let r = agg.push(181_000, 110.0).unwrap();      // prior bar close 107 vs 105 ⇒ +2.0
        assert!((r - 2.0).abs() < 1e-9);
    }
}
```

- [ ] **Step 2: Run — expect FAIL.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-ingestion -- spot::tests`
Expected: FAIL to compile.

- [ ] **Step 3: Implement parse fns, `median`, `MinuteBars`.** Prepend to the file:

```rust
use crate::IngestError;

/// Parse Coinbase `/products/BTC-USD/ticker` → last price.
pub fn parse_coinbase(body: &str) -> Result<f64, IngestError> {
    #[derive(serde::Deserialize)] struct T { price: String }
    let t: T = serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("coinbase: {e}")))?;
    t.price.parse::<f64>().map_err(|e| IngestError::Parse(format!("coinbase price: {e}")))
}

/// Parse Kraken `/0/public/Ticker?pair=XBTUSD` → last trade price (`c[0]`).
pub fn parse_kraken(body: &str) -> Result<f64, IngestError> {
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("kraken: {e}")))?;
    let result = v.get("result").and_then(|r| r.as_object())
        .ok_or_else(|| IngestError::Parse("kraken: no result".into()))?;
    let pair = result.values().next().ok_or_else(|| IngestError::Parse("kraken: empty result".into()))?;
    let last = pair.get("c").and_then(|c| c.get(0)).and_then(|s| s.as_str())
        .ok_or_else(|| IngestError::Parse("kraken: no c[0]".into()))?;
    last.parse::<f64>().map_err(|e| IngestError::Parse(format!("kraken last: {e}")))
}

/// Median of a slice ($). `NaN` for an empty slice.
pub fn median(xs: &[f64]) -> f64 {
    if xs.is_empty() { return f64::NAN; }
    let mut v: Vec<f64> = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

/// Close-to-close 1-minute bar aggregator. `push(ts_ms, price)` returns
/// `Some($-return)` when a bar boundary is crossed and a prior close exists.
#[derive(Debug, Default)]
pub struct MinuteBars { cur_min: Option<i64>, last_price: f64, prev_close: Option<f64> }

impl MinuteBars {
    pub fn new() -> Self { MinuteBars::default() }
    pub fn push(&mut self, ts_ms: i64, price: f64) -> Option<f64> {
        let minute = ts_ms.div_euclid(60_000);
        let mut ret = None;
        match self.cur_min {
            Some(m) if m == minute => {}
            Some(_) => {
                // Close the prior bar at its last price; emit return vs prev close.
                if let Some(pc) = self.prev_close { ret = Some(self.last_price - pc); }
                self.prev_close = Some(self.last_price);
            }
            None => {}
        }
        self.cur_min = Some(minute);
        self.last_price = price;
        ret
    }
}
```

- [ ] **Step 4: Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-ingestion -- spot::tests`
Expected: PASS (3 tests).

- [ ] **Step 5: Add the live feed handle (network; not unit-tested).** Append:

```rust
use std::sync::Arc;
use tokio::sync::watch;

/// Latest composite spot + vol readiness, published for the strategy loop.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpotSnapshot { pub ts_ms: i64, pub price: f64, pub sigma_1min: f64, pub vol_ready: bool }

/// Handle to the running feed. Clone-cheap; `latest()` reads the watch value.
#[derive(Clone)]
pub struct SpotFeed { rx: watch::Receiver<SpotSnapshot> }
impl SpotFeed { pub fn latest(&self) -> SpotSnapshot { *self.rx.borrow() } }

/// Spawn the poller. `sources` ∈ {"coinbase","kraken"} (others ignored with a
/// warning). Returns the handle; the task ends when `kill` is set.
pub fn spawn(
    http: reqwest::Client,
    sources: Vec<String>,
    poll_ms: u64,
    half_life_min: f64,
    warmup: u32,
    kill: Arc<std::sync::atomic::AtomicBool>,
) -> SpotFeed {
    let (tx, rx) = watch::channel(SpotSnapshot::default());
    tokio::spawn(async move {
        use std::sync::atomic::Ordering;
        // EWMA lives in the app model crate; duplicate the tiny recurrence here to
        // avoid an ingestion→app dependency (ingestion must not depend on app).
        let lambda = 0.5f64.powf(1.0 / half_life_min);
        let (mut var, mut n) = (0.0f64, 0u32);
        let mut bars = MinuteBars::new();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(poll_ms));
        loop {
            if kill.load(Ordering::Relaxed) { break; }
            tick.tick().await;
            let mut prices = Vec::new();
            for s in &sources {
                let (url, which) = match s.as_str() {
                    "coinbase" => ("https://api.exchange.coinbase.com/products/BTC-USD/ticker", 0),
                    "kraken"   => ("https://api.kraken.com/0/public/Ticker?pair=XBTUSD", 1),
                    _ => continue,
                };
                if let Ok(resp) = http.get(url).send().await {
                    if let Ok(body) = resp.text().await {
                        let p = if which == 0 { parse_coinbase(&body) } else { parse_kraken(&body) };
                        if let Ok(px) = p { if px.is_finite() && px > 0.0 { prices.push(px); } }
                    }
                }
            }
            let price = median(&prices);
            if !price.is_finite() { continue; }
            let now_ms = chrono_now_ms();
            if let Some(r) = bars.push(now_ms, price) {
                let sq = r * r;
                var = if n == 0 { sq } else { lambda * var + (1.0 - lambda) * sq };
                n = n.saturating_add(1);
            }
            let _ = tx.send(SpotSnapshot { ts_ms: now_ms, price, sigma_1min: var.sqrt(), vol_ready: n >= warmup });
        }
    });
    SpotFeed { rx }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
```

> Verify `IngestError` has `Parse(String)` and `Http(String)` variants (data_api.rs uses both). If `IngestError::Parse` differs, match the real variant name.

- [ ] **Step 6: Declare the module.** In `crates/ingestion/src/lib.rs` add `pub mod spot;` alongside the other `pub mod` lines.

- [ ] **Step 7: Build + test the crate — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-ingestion`
Expected: PASS (compiles with the live `spawn`; unit tests green).

- [ ] **Step 8: Commit.**

```bash
git add crates/ingestion/src/spot.rs crates/ingestion/src/lib.rs
git commit -m "feat(btc5m): composite BTC spot feed (median of exchanges) + 1-min vol"
```

---

## Task 5: 5-minute market discovery + rotation

**Files:**
- Create: `crates/ingestion/src/gamma.rs` (Gamma events client + parse)
- Create: `crates/app/src/strategy/btc5m/market.rs` (`Window` + rotation)
- Modify: `crates/ingestion/src/lib.rs` (`pub mod gamma;`), `crates/app/src/strategy/btc5m/mod.rs` (`pub mod market;`)

- [ ] **Step 1: Write the failing parse test** in `crates/ingestion/src/gamma.rs`:

```rust
//! Minimal Gamma API client for the current BTC "Up or Down 5m" market: given a
//! series slug it returns the live window's conditionId, YES/NO token ids, tick
//! size, and open/close timestamps. Parse split from I/O for unit testing.

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn parse_events_extracts_window() {
        // Trimmed shape of gamma-api /events?slug=... → [{ markets: [{ ... }] }]
        let body = r#"[{"markets":[{
            "conditionId":"0xCOND",
            "clobTokenIds":"[\"111\",\"222\"]",
            "orderPriceMinTickSize":"0.01",
            "startDate":"2026-07-13T07:50:00Z",
            "endDate":"2026-07-13T07:55:00Z",
            "closed":false
        }]}]"#;
        let w = parse_current_window(body).unwrap().unwrap();
        assert_eq!(w.condition_id, "0xCOND");
        assert_eq!(w.yes_token, "111");
        assert_eq!(w.no_token, "222");
        assert_eq!(w.tick_decimals, 2);          // 0.01 → Cent
        assert_eq!(w.t_open_ms, 1_783_929_000_000);   // 2026-07-13T07:50:00Z
        assert_eq!(w.t_close_ms, 1_783_929_300_000);  // 2026-07-13T07:55:00Z (+300s)
    }

    #[test]
    fn parse_events_skips_closed_and_missing() {
        assert!(parse_current_window("[]").unwrap().is_none());
        let closed = r#"[{"markets":[{"conditionId":"x","clobTokenIds":"[\"1\",\"2\"]","orderPriceMinTickSize":"0.01","startDate":"2026-07-13T07:50:00Z","endDate":"2026-07-13T07:55:00Z","closed":true}]}]"#;
        assert!(parse_current_window(closed).unwrap().is_none());
    }
}
```

- [ ] **Step 2: Run — expect FAIL.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-ingestion -- gamma::tests`
Expected: FAIL to compile.

- [ ] **Step 3: Implement the DTO + parse + client.** Prepend:

```rust
use crate::IngestError;

/// A resolved 5-min window: identity, both token ids, tick, and its time range.
#[derive(Debug, Clone, PartialEq)]
pub struct GammaWindow {
    pub condition_id: String,
    pub yes_token: String,
    pub no_token: String,
    pub tick_decimals: i64,   // 2 = Cent (0.01), 3 = Milli (0.001)
    pub t_open_ms: i64,
    pub t_close_ms: i64,
}

fn tick_decimals_from_str(s: &str) -> i64 { if s.trim() == "0.001" { 3 } else { 2 } }

fn rfc3339_to_ms(s: &str) -> Option<i64> {
    // Minimal: "YYYY-MM-DDTHH:MM:SSZ" → epoch ms (UTC). Avoids a chrono dep here.
    let b = s.as_bytes();
    if b.len() < 20 { return None; }
    let num = |a: usize, z: usize| s.get(a..z)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0,4)?, num(5,7)?, num(8,10)?);
    let (h, mi, se) = (num(11,13)?, num(14,16)?, num(17,19)?);
    // days from civil (Howard Hinnant's algorithm)
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let doy = (153 * (if mo > 2 { mo - 3 } else { mo + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + h * 3600 + mi * 60 + se) * 1000) as i64)
}

/// Pick the current (open, not closed) window from a Gamma `/events` body.
pub fn parse_current_window(body: &str) -> Result<Option<GammaWindow>, IngestError> {
    let events: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| IngestError::Parse(format!("gamma events: {e}")))?;
    let arr = match events.as_array() { Some(a) => a, None => return Ok(None) };
    for ev in arr {
        let markets = match ev.get("markets").and_then(|m| m.as_array()) { Some(m) => m, None => continue };
        for m in markets {
            if m.get("closed").and_then(|c| c.as_bool()).unwrap_or(false) { continue; }
            let cond = match m.get("conditionId").and_then(|c| c.as_str()) { Some(c) => c, None => continue };
            let toks_raw = match m.get("clobTokenIds").and_then(|t| t.as_str()) { Some(t) => t, None => continue };
            let toks: Vec<String> = serde_json::from_str(toks_raw).unwrap_or_default();
            if toks.len() != 2 { continue; }
            let tick = m.get("orderPriceMinTickSize").and_then(|t| t.as_str()).unwrap_or("0.01");
            let (open, close) = match (
                m.get("startDate").and_then(|s| s.as_str()).and_then(rfc3339_to_ms),
                m.get("endDate").and_then(|s| s.as_str()).and_then(rfc3339_to_ms),
            ) { (Some(o), Some(c)) => (o, c), _ => continue };
            return Ok(Some(GammaWindow {
                condition_id: cond.to_string(),
                yes_token: toks[0].clone(), no_token: toks[1].clone(),
                tick_decimals: tick_decimals_from_str(tick),
                t_open_ms: open, t_close_ms: close,
            }));
        }
    }
    Ok(None)
}

/// Keyless Gamma client (mirrors `DataApiClient`).
pub struct GammaClient { http: reqwest::Client, base: String }
impl GammaClient {
    pub fn new(http: reqwest::Client, base: Option<&str>) -> Self {
        GammaClient { http, base: base.unwrap_or("https://gamma-api.polymarket.com").trim_end_matches('/').to_string() }
    }
    /// Fetch the live window for a series slug (e.g. `btc-updown-5m-<unix>`), if any.
    pub async fn current_window(&self, slug: &str) -> Result<Option<GammaWindow>, IngestError> {
        let url = format!("{}/events?slug={}", self.base, slug);
        let body = self.http.get(&url).send().await.map_err(|e| IngestError::Http(e.to_string()))?
            .error_for_status().map_err(|e| IngestError::Http(e.to_string()))?
            .text().await.map_err(|e| IngestError::Http(e.to_string()))?;
        parse_current_window(&body)
    }
}
```

> The exact Gamma field names (`clobTokenIds` as a JSON-encoded string, `orderPriceMinTickSize`, `startDate`/`endDate`, `closed`) were confirmed on live market objects in the spec's research. If a live probe shows a different discovery path (e.g. the series exposes a "current" endpoint), adjust `current_window`'s URL only — `parse_current_window` stays.

- [ ] **Step 4: Add `pub mod gamma;` to `crates/ingestion/src/lib.rs`. Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-ingestion -- gamma::tests`
Expected: PASS (2 tests).

- [ ] **Step 5: Write the failing rotation test** in `crates/app/src/strategy/btc5m/market.rs`:

```rust
//! Which 5-min window is live right now, and the per-window state the shadow
//! loop needs (tokens, tick, strike snapshot). Rotation is time-driven; the
//! Gamma refresh is the strategy loop's job (this is the pure decision logic).

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use pm_ingestion::gamma::GammaWindow;

    fn gw(cond: &str, open: i64, close: i64) -> GammaWindow {
        GammaWindow { condition_id: cond.into(), yes_token: "1".into(), no_token: "2".into(),
            tick_decimals: 2, t_open_ms: open, t_close_ms: close }
    }

    #[test]
    fn adopts_window_and_snapshots_strike_once() {
        let mut r = Rotation::default();
        // Adopt a fresh window; strike is snapshotted from spot at adoption.
        let changed = r.adopt(gw("A", 0, 300_000), 62_900.0);
        assert!(changed);
        assert_eq!(r.current().unwrap().condition_id, "A");
        assert_eq!(r.current().unwrap().strike, 62_900.0);
        // Re-adopting the SAME window must not re-snapshot the strike.
        assert!(!r.adopt(gw("A", 0, 300_000), 63_000.0));
        assert_eq!(r.current().unwrap().strike, 62_900.0);
        // A new conditionId rotates and re-snapshots.
        assert!(r.adopt(gw("B", 300_000, 600_000), 63_010.0));
        assert_eq!(r.current().unwrap().strike, 63_010.0);
    }

    #[test]
    fn secs_to_go_clamps_at_zero() {
        let mut r = Rotation::default();
        r.adopt(gw("A", 0, 300_000), 100.0);
        assert_eq!(r.current().unwrap().secs_to_go(150_000), 150);
        assert_eq!(r.current().unwrap().secs_to_go(400_000), 0);
    }
}
```

- [ ] **Step 6: Run — expect FAIL.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::market::tests`
Expected: FAIL to compile.

- [ ] **Step 7: Implement `Window` + `Rotation`.** Prepend (and add `pub mod market;` to `strategy/btc5m/mod.rs`):

```rust
use pm_ingestion::gamma::GammaWindow;

/// A window the loop is actively shadowing: its Gamma identity + the strike we
/// snapshotted (our composite spot at adoption — a proxy for the Chainlink
/// open; the shadow report reconciles against the venue outcome later).
#[derive(Debug, Clone, PartialEq)]
pub struct Window { pub gamma: GammaWindow, pub strike: f64 }

impl Window {
    /// Seconds remaining to close (clamped at 0).
    pub fn secs_to_go(&self, now_ms: i64) -> i64 { ((self.gamma.t_close_ms - now_ms).max(0)) / 1000 }
}

/// Tracks the currently-adopted window and rotates on conditionId change.
#[derive(Debug, Default)]
pub struct Rotation { current: Option<Window> }

impl Rotation {
    /// Adopt `gw` as current. If it's a new conditionId, snapshot `spot_now` as
    /// the strike and return `true` (rotated); if it's the same window, keep the
    /// existing strike and return `false`.
    pub fn adopt(&mut self, gw: GammaWindow, spot_now: f64) -> bool {
        let same = self.current.as_ref().map(|w| w.gamma.condition_id == gw.condition_id).unwrap_or(false);
        if same { return false; }
        self.current = Some(Window { gamma: gw, strike: spot_now });
        true
    }
    pub fn current(&self) -> Option<&Window> { self.current.as_ref() }
}
```

- [ ] **Step 8: Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::market::tests`
Expected: PASS (2 tests).

- [ ] **Step 9: Commit.**

```bash
git add crates/ingestion/src/gamma.rs crates/ingestion/src/lib.rs crates/app/src/strategy/btc5m/market.rs crates/app/src/strategy/btc5m/mod.rs
git commit -m "feat(btc5m): 5-min market discovery (gamma) + window rotation"
```

---

## Task 6: Read-only shadow strategy loop

**Files:**
- Modify: `crates/app/src/strategy/btc5m/mod.rs` (add `Btc5mStrategy`)
- Create: `crates/app/src/strategy/btc5m/shadow.rs` (`ShadowSample` → `StoreMsg`)

The loop mirrors `HeartbeatStrategy` (kill + `ctl_rx` + tick), and additionally each tick: reads the YES-token book via `ctx.fetcher.fetch`, reads spot via the injected `SpotFeed`, computes fair `p_up`, and (when a window is live and vol is ready) sends a `StoreMsg::Btc5mShadow`. **It never constructs an `Order` — Phase 1 is read-only.** Gamma refresh runs on a slower sub-interval inside the loop.

- [ ] **Step 1: Write the failing test** in `crates/app/src/strategy/btc5m/shadow.rs`:

```rust
//! The shadow sample the loop records each tick and its mapping to a store row.
use pm_store::Btc5mShadowRow;

/// A single (fair vs book) observation, pre-persistence.
#[derive(Debug, Clone, PartialEq)]
pub struct ShadowSample {
    pub ts_ms: i64,
    pub condition_id: String,
    pub secs_to_go: i64,
    pub strike: f64,
    pub spot: f64,
    pub sigma_tau: f64,
    pub p_up: f64,
    pub best_bid_micro: i64,
    pub best_ask_micro: i64,
    pub tick_decimals: i64,
}

impl ShadowSample {
    pub fn into_row(self) -> Btc5mShadowRow {
        Btc5mShadowRow {
            ts_ms: self.ts_ms, condition_id: self.condition_id, secs_to_go: self.secs_to_go,
            strike: self.strike, spot: self.spot, sigma_tau: self.sigma_tau, p_up: self.p_up,
            best_bid_micro: self.best_bid_micro, best_ask_micro: self.best_ask_micro,
            tick_decimals: self.tick_decimals,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    #[test]
    fn sample_maps_to_row() {
        let s = ShadowSample { ts_ms: 1, condition_id: "c".into(), secs_to_go: 15, strike: 1.0,
            spot: 2.0, sigma_tau: 3.0, p_up: 0.6, best_bid_micro: 550_000, best_ask_micro: 560_000, tick_decimals: 2 };
        let r = s.clone().into_row();
        assert_eq!(r.condition_id, "c");
        assert_eq!(r.best_ask_micro, 560_000);
        assert_eq!(r.p_up, 0.6);
    }
}
```

- [ ] **Step 2: Run — expect FAIL.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::shadow::tests`
Expected: FAIL to compile.

- [ ] **Step 3: Implement (the code above is the implementation). Add `pub mod shadow;` to `strategy/btc5m/mod.rs`. Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::shadow::tests`
Expected: PASS.

- [ ] **Step 4: Write the failing loop test** in `crates/app/src/strategy/btc5m/mod.rs`. This mirrors the heartbeat test: build an inert `StrategyCtx`, inject a live window + a spot snapshot, run one tick, and assert a shadow row was queued on `store_tx` and NO order path was touched (there is no venue in the struct, so "no orders" is structural — we assert the store receives a `Btc5mShadow`).

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::{mpsc, watch};
    use pm_store::writer::StoreMsg;
    use crate::strategy::{StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};
    use crate::wiring::BookFetcher;
    use super::*;

    #[tokio::test]
    async fn shadow_loop_writes_a_row_and_no_orders() {
        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let (store_tx, mut store_rx) = mpsc::channel::<StoreMsg>(16);
        let ctx = StrategyCtx {
            registry: Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap()),
            fetcher: BookFetcher::new(HashMap::new()),   // unknown token → fetch None → book fields 0
            store_tx, kill: Arc::clone(&kill), ctl_rx, status_tx,
        };
        // A strategy pre-seeded with a live window + a fixed spot (no network).
        let strat = Btc5mStrategy::new_for_test(
            /* window */ pm_ingestion::gamma::GammaWindow {
                condition_id: "C".into(), yes_token: "999".into(), no_token: "998".into(),
                tick_decimals: 2, t_open_ms: 0, t_close_ms: 300_000 },
            /* strike */ 62_900.0, /* spot */ 62_940.0, /* sigma_1min */ 40.0,
            /* sample_ms */ 5,
        );
        let run = tokio::spawn(Box::new(strat).run(ctx));
        // The first store message must be a shadow row (never an order — this
        // strategy has no venue and constructs no Order in Phase 1).
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), store_rx.recv())
            .await.expect("no store message within timeout").expect("store sender dropped");
        match msg {
            StoreMsg::Btc5mShadow(r) => {
                assert_eq!(r.condition_id, "C");
                assert!(r.p_up > 0.5, "spot above strike ⇒ p_up > 0.5, got {}", r.p_up);
                assert_eq!(r.best_ask_micro, 0, "unknown token ⇒ no book");
            }
            other => panic!("expected Btc5mShadow, got {other:?}"),
        }
        kill.store(true, Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_secs(5), run).await.unwrap().unwrap();
    }
}
```

- [ ] **Step 5: Run — expect FAIL (`Btc5mStrategy` undefined).**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::tests::shadow_loop_writes_a_row_and_no_orders`
Expected: FAIL to compile.

- [ ] **Step 6: Implement `Btc5mStrategy`.** In `crates/app/src/strategy/btc5m/mod.rs` (above the tests, after the `pub mod` lines):

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pm_ingestion::gamma::{GammaClient, GammaWindow};
use pm_ingestion::spot::SpotFeed;
use pm_store::writer::StoreMsg;
use tokio::time::MissedTickBehavior;

use super::{Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};
use crate::strategy::btc5m::market::Rotation;
use crate::strategy::btc5m::model::{fair_p_up};
use crate::strategy::btc5m::shadow::ShadowSample;
use pm_core::instrument::TokenId;

/// Read-only Phase-0/1 shadow strategy. Holds either a live `GammaClient` +
/// `SpotFeed` (production) or a pre-seeded window+spot (tests).
pub struct Btc5mStrategy {
    id: StrategyId,
    sample_ms: u64,
    // production inputs (None in tests)
    gamma: Option<GammaClient>,
    slug_fn: Option<Box<dyn Fn(i64) -> String + Send>>,
    spot: Option<SpotFeed>,
    // test seed (None in production)
    seed: Option<(GammaWindow, f64, f64)>, // (window, strike, spot); sigma via seed_sigma
    seed_sigma: f64,
}

impl Btc5mStrategy {
    /// Production constructor.
    pub fn new(
        gamma: GammaClient,
        slug_fn: Box<dyn Fn(i64) -> String + Send>,
        spot: SpotFeed,
        sample_ms: u64,
    ) -> Self {
        Btc5mStrategy {
            id: StrategyId("btc5m"), sample_ms,
            gamma: Some(gamma), slug_fn: Some(slug_fn), spot: Some(spot),
            seed: None, seed_sigma: 0.0,
        }
    }

    /// Test constructor: no network; a fixed window/strike/spot/sigma.
    pub fn new_for_test(window: GammaWindow, strike: f64, spot: f64, sigma_1min: f64, sample_ms: u64) -> Self {
        Btc5mStrategy {
            id: StrategyId("btc5m"), sample_ms,
            gamma: None, slug_fn: None, spot: None,
            seed: Some((window, strike, spot)), seed_sigma: sigma_1min,
        }
    }

    fn now_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
    }
}

impl Strategy for Btc5mStrategy {
    fn id(&self) -> StrategyId { self.id }
    fn make_on_apply(&self) -> Option<pm_ingestion::supervisor::OnApplyFn> { None }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let StrategyCtx { fetcher, store_tx, kill, mut ctl_rx, status_tx, .. } = ctx;
            let mut paused = false;
            let mut rot = Rotation::default();
            let me = *self;

            // Seed path (tests): adopt the fixed window once.
            if let Some((w, strike, _spot)) = me.seed.clone() {
                rot.adopt(w, strike);
            }

            let mut tick = tokio::time::interval(Duration::from_millis(me.sample_ms.max(1)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut gamma_poll = tokio::time::interval(Duration::from_millis(1000));
            gamma_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                if kill.load(Ordering::Relaxed) { break; }
                tokio::select! {
                    _ = gamma_poll.tick() => {
                        // Production: refresh the current window from Gamma.
                        if let (Some(g), Some(sf), Some(slug_fn)) = (me.gamma.as_ref(), me.spot.as_ref(), me.slug_fn.as_ref()) {
                            let now = Self::now_ms();
                            let slug = slug_fn(now);
                            if let Ok(Some(w)) = g.current_window(&slug).await {
                                rot.adopt(w, sf.latest().price);
                            }
                        }
                    }
                    _ = tick.tick() => {
                        if paused { continue; }
                        let now = Self::now_ms();
                        let win = match rot.current() { Some(w) => w.clone(), None => continue };
                        // Spot + vol: live feed in prod, seed in tests.
                        let (spot, sigma_1min, vol_ready) = match (me.spot.as_ref(), &me.seed) {
                            (Some(sf), _) => { let s = sf.latest(); (s.price, s.sigma_1min, s.vol_ready) }
                            (None, Some((_, _, seed_spot))) => (*seed_spot, me.seed_sigma, true),
                            _ => continue,
                        };
                        if !vol_ready || !spot.is_finite() { continue; }
                        let secs = win.secs_to_go(now);
                        let sigma_tau = sigma_1min * ((secs.max(0) as f64) / 60.0).sqrt();
                        let p_up = match fair_p_up(spot, win.strike, secs as f64, sigma_1min) { Some(p) => p, None => continue };

                        // YES/UP token book (best bid/ask in µUSDC; 0 if unknown/stale).
                        let (mut bid_micro, mut ask_micro) = (0i64, 0i64);
                        if let Ok(tok) = win.gamma.yes_token.parse::<u128>() {
                            let ts = if win.gamma.tick_decimals == 3 { pm_core::num::TickSize::Milli } else { pm_core::num::TickSize::Cent };
                            if let Some((book, true)) = fetcher.fetch(TokenId(tok)).await {
                                if let Some(px) = book.bids.best() { bid_micro = px.microusdc(ts) as i64; }
                                if let Some(px) = book.asks.best() { ask_micro = px.microusdc(ts) as i64; }
                            }
                        }

                        let sample = ShadowSample {
                            ts_ms: now, condition_id: win.gamma.condition_id.clone(), secs_to_go: secs,
                            strike: win.strike, spot, sigma_tau, p_up,
                            best_bid_micro: bid_micro, best_ask_micro: ask_micro,
                            tick_decimals: win.gamma.tick_decimals,
                        };
                        // Best-effort; never block the loop.
                        let _ = store_tx.try_send(StoreMsg::Btc5mShadow(sample.into_row()));

                        // Publish a light status (display-only; no accounting in shadow).
                        let _ = status_tx.send(StrategyStatus {
                            paused,
                            open_positions: 0,
                            ..Default::default()
                        });
                    }
                    cmd = ctl_rx.recv() => match cmd {
                        Some(StrategyCommand::SetPaused(p)) => paused = p,
                        Some(StrategyCommand::VetoQuote { .. }) => {}
                        None => break,
                    },
                }
            }
        })
    }
}
```

> `TokenId` is `pm_core::instrument::TokenId`. Confirm its constructor shape — the extraction shows `TokenId(u128)`-style usage; if it wraps a `String`/newtype differently, adjust the `win.gamma.yes_token.parse()` + `TokenId(..)` line only. `Px::microusdc(ts)` returns `u64` µUSDC/share (num.rs).

- [ ] **Step 7: Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- strategy::btc5m::tests::shadow_loop_writes_a_row_and_no_orders`
Expected: PASS (a `Btc5mShadow` row is queued; `best_ask_micro == 0` since the token is unknown to the inert fetcher; `p_up > 0.5`).

- [ ] **Step 8: Grep-guard: no order path in btc5m.** Run:

`grep -rnE 'submit_fak|Order::new|ExecutionVenue|\.place\(' crates/app/src/strategy/btc5m/ || echo "OK: no order path (read-only)"`
Expected: prints `OK: no order path (read-only)`.

- [ ] **Step 9: Commit.**

```bash
git add crates/app/src/strategy/btc5m/mod.rs crates/app/src/strategy/btc5m/shadow.rs
git commit -m "feat(btc5m): read-only shadow loop (book vs fair value → store)"
```

---

## Task 7: Capital carve + registration (gated, default off)

**Files:**
- Modify: `crates/app/src/wiring.rs` (`PlatformEnvelopes.btc5m` + carve), `crates/app/src/main.rs` (register when enabled).

- [ ] **Step 1: Write the failing carve test.** In the `tests` mod of `crates/app/src/wiring.rs`:

```rust
    #[test]
    fn btc5m_envelope_carved_only_when_enabled() {
        let mut cfg = pm_config::Config::default();
        cfg.capital.bankroll_usd = 100.0;
        let risk = risk_config(&cfg, None).unwrap();
        // Disabled ⇒ no envelope.
        let env = strategy_envelopes(&cfg, &risk, pm_core::num::Usdc(100_000_000)).unwrap();
        assert!(env.btc5m.is_none());
        // Enabled ⇒ envelope with the carved capital.
        cfg.strategies.btc5m.enabled = true;
        cfg.strategies.btc5m.capital_usd = 20.0;
        let env = strategy_envelopes(&cfg, &risk, pm_core::num::Usdc(100_000_000)).unwrap();
        assert_eq!(env.btc5m.as_ref().unwrap().capital.0, 20_000_000);
    }
```

- [ ] **Step 2: Run — expect FAIL (`env.btc5m` missing).**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- wiring::tests::btc5m_envelope_carved_only_when_enabled`
Expected: FAIL to compile.

- [ ] **Step 3: Extend `PlatformEnvelopes` + carve.** In `crates/app/src/wiring.rs`:
Add the field to the struct (near line 89):
```rust
    pub btc5m: Option<StrategyEnvelope>,
```
Inside `strategy_envelopes` mirror the `copy` arm (near lines 148-162) — carve `btc5m` capital, include it in the combined-carve overflow check, and build the envelope only when enabled:
```rust
    let btc5m = if config.strategies.btc5m.enabled {
        let cap = pm_config::usd_to_microusdc(config.strategies.btc5m.capital_usd)?;
        Some(StrategyEnvelope::new(StrategyId("btc5m"), Usdc(cap), risk_cfg.clone()))
    } else { None };
    // include btc5m in the sum that must not exceed bankroll (mirror the copy line)
    let carved = /* existing arb+mm+copy carve sum */ + btc5m.as_ref().map(|e| e.capital.0).unwrap_or(0);
```
Return it in the `PlatformEnvelopes { … btc5m }` literal.

> Read the exact existing carve arithmetic (wiring.rs ~148-162) and add `btc5m` symmetrically to whatever accumulator variable it uses. The envelope uses `risk_cfg.clone()` (btc5m runs read-only in Phase 1, so the arb risk envelope is a safe conservative bound; a dedicated `Btc5mRiskConfig` arrives with Phase 2).

- [ ] **Step 4: Run — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-app -- wiring::tests::btc5m_envelope_carved_only_when_enabled`
Expected: PASS.

- [ ] **Step 5: Register in `main.rs` (behind the enabled gate).** After the copy registration block (near main.rs:2297), mirror it:

```rust
    if let Some(btc5m_envelope) = btc5m_envelope {
        use pm_ingestion::gamma::GammaClient;
        use pm_ingestion::spot;
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .user_agent("pm-arb-bot/1.0")
            .build()
            .unwrap_or_else(|e| fatal(format!("btc5m http client: {e}")));
        let spot_feed = spot::spawn(
            http.clone(),
            config.btc5m_params.spot_sources.clone(),
            config.btc5m_params.spot_poll_ms,
            config.btc5m_params.vol_half_life_min,
            config.btc5m_params.vol_warmup_samples,
            Arc::clone(&kill),                       // the process kill flag used elsewhere in main
        );
        let gamma = GammaClient::new(http, None);
        // Series slug for the live 5-min window: aligned to the current 5-min
        // boundary (unix seconds). Confirm the exact slug scheme with a live probe
        // (spec §7: `btc-updown-5m-<unix>`); this closure is the only thing to tweak.
        let slug_fn: Box<dyn Fn(i64) -> String + Send> = Box::new(|now_ms: i64| {
            let boundary = (now_ms / 1000) / 300 * 300;
            format!("btc-updown-5m-{boundary}")
        });
        let btc5m = pm_app::strategy::btc5m::Btc5mStrategy::new(
            gamma, slug_fn, spot_feed,
            config.btc5m_params.sample_interval_ms,
        );
        host.add(Box::new(btc5m), btc5m_envelope);
    }
```
And destructure `btc5m` out of `PlatformEnvelopes { …, btc5m: btc5m_envelope, .. }` at main.rs:1534.

> The `kill: Arc<AtomicBool>` used by `spot::spawn` must be the same process kill flag `main` already owns and hands to `HostShared`/`StrategyCtx`. Find it near the host build (main.rs ~1534-1578) and pass a clone.

- [ ] **Step 6: Build the app — expect PASS.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo build -p pm-app`
Expected: compiles.

- [ ] **Step 7: Commit.**

```bash
git add crates/app/src/wiring.rs crates/app/src/main.rs
git commit -m "feat(btc5m): capital carve + gated registration (default off)"
```

---

## Task 8: `pnl` parity + Phase-1 gate report

**Files:**
- Modify: `deploy/status.sh` (add a "BTC 5M BOT" section)
- Create: `deploy/btc5m_report.py`

- [ ] **Step 1: Add the BTC section to `status.sh`.** Append after the existing copy Python block (the script already computes `DB`/`WALLET`). Add a second `python3` block:

```bash
python3 - "$DB" <<'PY'
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db)
try:
    n = con.execute("SELECT COUNT(*) FROM btc5m_shadow").fetchone()[0]
except sqlite3.OperationalError:
    print("=== BTC 5M BOT ===\n  (no btc5m_shadow table yet — strategy not deployed/enabled)\n")
    raise SystemExit(0)
r = con.execute("SELECT realized_micro FROM day_realized WHERE strategy='btc5m' ORDER BY utc_day DESC LIMIT 1").fetchone()
realized = (r[0] if r else 0) / 1e6
last = con.execute("SELECT condition_id, secs_to_go, spot, strike, p_up, best_bid_micro, best_ask_micro FROM btc5m_shadow ORDER BY ts_ms DESC LIMIT 1").fetchone()
print("=== BTC 5M BOT — shadow measurement ===")
print(f"  shadow samples: {n}    realized P&L today (btc5m): ${realized:+.2f}")
if last:
    cid, secs, spot, strike, p_up, bid, ask = last
    print(f"  latest: {cid[:10]}…  T-{secs}s  spot ${spot:,.2f} vs strike ${strike:,.2f}  fair(up)={p_up:.3f}  book {bid/1e6:.3f}/{ask/1e6:.3f}")
print()
PY
```

- [ ] **Step 2: Create `deploy/btc5m_report.py`** — the Phase-1 gate metric:

```python
#!/usr/bin/env python3
"""Phase-1 gate: is the terminal-convergence edge harvestable?
For late buckets, among windows where a leader exists (|z|>=Z), report how often
the leader's best offer is below fair (i.e. a taker could buy the ~sure side
cheap). Usage: python3 deploy/btc5m_report.py [db_path] [z_threshold]"""
import sqlite3, sys, statistics
db = sys.argv[1] if len(sys.argv) > 1 else __import__("os").path.expanduser("~/copybot/data/copy-canary.sqlite")
Z = float(sys.argv[2]) if len(sys.argv) > 2 else 1.5
con = sqlite3.connect(db)
rows = con.execute(
    "SELECT secs_to_go, spot, strike, sigma_tau, p_up, best_bid_micro, best_ask_micro "
    "FROM btc5m_shadow WHERE sigma_tau > 0").fetchall()
BUCKETS = [(0,10),(10,20),(20,45),(45,90)]
print(f"btc5m Phase-1 gate report  (Z={Z})  samples={len(rows)}")
print(f"{'bucket(s)':>10} {'n_leader':>9} {'edge>=2c%':>9} {'med_net_edge_c':>15}")
for lo, hi in BUCKETS:
    nets = []
    n_leader = 0
    for secs, spot, strike, sig, p_up, bid, ask in rows:
        if not (lo <= secs < hi):
            continue
        z = (spot - strike) / sig if sig else 0.0
        if abs(z) < Z:
            continue
        n_leader += 1
        up_leads = z > 0
        # Leader token's ASK in cents; UP uses YES-ask, DOWN uses (1 - YES-bid).
        if up_leads:
            offer = ask / 1e6
            fair = p_up
        else:
            offer = 1.0 - (bid / 1e6) if bid else None
            fair = 1.0 - p_up
        if not offer or offer <= 0:
            continue
        fee = 0.07 * offer * (1 - offer)          # taker fee $/share
        net_edge_c = (fair - offer - fee) * 100.0  # cents/contract before spread
        nets.append(net_edge_c)
    if nets:
        pct = 100.0 * sum(1 for x in nets if x >= 2.0) / len(nets)
        print(f"{f'[{lo},{hi})':>10} {n_leader:>9} {pct:>8.1f}% {statistics.median(nets):>15.2f}")
    else:
        print(f"{f'[{lo},{hi})':>10} {n_leader:>9} {'-':>9} {'-':>15}")
print("\nGATE: proceed to Phase 2 only if the [0,20)s buckets show a positive median")
print("net edge on a meaningful n_leader (see spec §5, Gate 1→2).")
```

- [ ] **Step 3: Smoke-test the report against a seeded DB.**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
python3 - <<'PY'
import sqlite3
con = sqlite3.connect("/tmp/btc5m_test.sqlite")
con.execute("""CREATE TABLE btc5m_shadow (ts_ms INTEGER, condition_id TEXT, secs_to_go INTEGER,
 strike REAL, spot REAL, sigma_tau REAL, p_up REAL, best_bid_micro INTEGER, best_ask_micro INTEGER, tick_decimals INTEGER)""")
# a T-15s window, UP leads by ~2σ, leader ask 0.90 while fair ~0.98 → net edge > 2c
con.execute("INSERT INTO btc5m_shadow VALUES (1,'C',15,62900,62980,40,0.977,880000,900000,2)")
con.commit()
PY
python3 deploy/btc5m_report.py /tmp/btc5m_test.sqlite 1.5
```
Expected: the `[10,20)` bucket shows `n_leader=1` and a positive `med_net_edge_c` (~+5–6c).

- [ ] **Step 4: Verify `pnl` renders the section against the same seeded DB.**

```bash
bash deploy/status.sh /tmp/btc5m_test.sqlite /dev/null
```
Expected: prints an `=== ACCOUNT ===` note (no journal), the copy section (empty), and a `=== BTC 5M BOT — shadow measurement ===` section with `shadow samples: 1` and the latest line.

- [ ] **Step 5: Commit.**

```bash
git add deploy/status.sh deploy/btc5m_report.py
git commit -m "feat(btc5m): pnl parity (status.sh BTC section) + Phase-1 gate report"
```

---

## Task 9: Canary config + full-workspace verification

**Files:**
- Modify: `mm-live-copy-canary.toml`

- [ ] **Step 1: Add the (disabled) sections to `mm-live-copy-canary.toml`.** Append:

```toml
[strategies.btc5m]
enabled = false     # Phase 1 flips this to true (still read-only — no orders)
live = false
capital_usd = 25.0

[btc5m]
vol_half_life_min = 120.0
vol_warmup_samples = 180
z_threshold = 1.5
sample_interval_ms = 1000
spot_sources = ["coinbase", "kraken"]
spot_poll_ms = 1000
dense_window_secs = 60
```

- [ ] **Step 2: Confirm the shipped config still parses + validates.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test -p pm-config -- config_smoke`
Expected: PASS (the smoke test parses the shipped TOML; `deny_unknown_fields` means the new sections must be recognized — they are, via Task 2).

- [ ] **Step 3: Full workspace test + release build.**

Run: `export PATH="$HOME/.cargo/bin:$PATH" && cargo test --workspace && cargo build --release --bin arb`
Expected: all tests pass; `arb` builds. (No behavior change with `enabled = false`.)

- [ ] **Step 4: Manual shadow smoke (optional, read-only, ~2 min).** Enable btc5m in a scratch config pointed at a scratch DB and run briefly to confirm it discovers a window and writes rows — WITHOUT touching the live copy DB:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cp mm-live-copy-canary.toml /tmp/btc5m-shadow.toml
# in /tmp/btc5m-shadow.toml set [strategies.btc5m].enabled = true and [strategies.copy].enabled=false, [strategies.mm].enabled=false
timeout 120 ./target/release/arb --config /tmp/btc5m-shadow.toml --db /tmp/btc5m-shadow.sqlite || true
python3 deploy/btc5m_report.py /tmp/btc5m-shadow.sqlite 1.5
```
Expected: some `shadow samples` accumulate; the report renders (numbers will be sparse over 2 min). This is a live-network read-only check — no orders, scratch DB.

- [ ] **Step 5: Commit.**

```bash
git add mm-live-copy-canary.toml
git commit -m "feat(btc5m): ship [strategies.btc5m] (disabled) + [btc5m] knobs; Phase 0/1 complete"
```

---

## Deployment note (Phase 1 go-live)

On the box, `cd ~/copybot && git pull && bash deploy/setup.sh` rebuilds `arb` and restarts `copybot` (same service — btc5m is in-process). To start shadowing, flip `[strategies.btc5m].enabled = true` in `~/copybot/mm-live-copy-canary.toml` and restart; **the copy bot is unaffected** (its own toggles unchanged, and btc5m places no orders). Watch with `pnl` (now shows the BTC section) and, after a day, `ssh … 'python3 ~/copybot/deploy/btc5m_report.py'` to read the Gate 1→2 metric. No London move is needed for Phase 1.

**Phase-0 reward check (one-off, manual):** confirm whether Liquidity Rewards actually fund these 5-min windows (spec §5 open question — informs Phase 3, not Phase 1): `curl -s 'https://clob.polymarket.com/markets/<conditionId>' | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('rewards'))"`.

## What this plan intentionally defers (later plans)
- **Phase 2** (micro taker near price extremes): a `Btc5mVenue`/order path, `btc5m_positions`, `DayRealized{strategy:"btc5m"}` writes, per-window flatten, stop-loss — a separate plan gated on the Gate 1→2 metric.
- **Phase 3** (maker): two-sided model quoting, rewards/rebate capture, adverse-selection controls, and the **London relocation** — a separate plan gated on Phase-2 results.
- **Chainlink-direct feed** (vs the composite proxy): evaluate once Phase-1 basis (composite vs venue strike) is measured.
