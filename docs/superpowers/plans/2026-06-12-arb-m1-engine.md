# M1 — Core Math + Detection Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `pm-core` (exact numeric types, ladder books, fees) and `pm-engine` (depth walker, arb detectors for classes 1–3, per-component LP detector, dedup) with full tests and criterion benchmarks — no I/O anywhere.

**Architecture:** Integer-exact money math (spec §4): prices are native-tick `u16`, sizes micro-share `u64`, cash signed micro-USDC `i128`, rounding always against us. Dense per-tick ladders (spec §5). A generalized basket walker (spec §7) does all sizing; classes 1–3 are thin adapters over it; the LP (spec §10) enumerates relationship-pruned worlds per component, solves with HiGHS, and re-validates every solution in exact integer math.

**Tech Stack:** Rust stable (2024 edition), cargo workspace; `highs` (LP), `proptest` (property tests), `criterion` (benches). No async, no network, no DB in M1.

**Spec:** `docs/superpowers/specs/2026-06-12-polymarket-arb-bot-v2-design.md` (the section numbers cited below refer to it).

---

## Conventions for every task

- **PATH:** cargo is not on PATH on this machine. Prefix every cargo invocation:
  `export PATH="$HOME/.cargo/bin:$PATH"`
- **Branch:** all work happens on `feat/m1-engine` (Task 1 creates it). Never commit to `main`.
- **Commit trailer:** end every commit message with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- **Crate naming:** directories are `crates/core`, `crates/engine`, `crates/config`; package names are `pm-core`, `pm-engine`, `pm-config` (a crate literally named `core` would collide with Rust's built-in core).
- **Lint policy:** `clippy::unwrap_used` / `clippy::expect_used` are denied workspace-wide. Test modules open with `#![allow(clippy::unwrap_used)]` as their first line.
- **`highs` note:** `highs-sys` builds vendored HiGHS via CMake; Task 1 verifies `cmake` exists before anything else.

## File map (what exists when M1 is done)

```
Cargo.toml                       workspace: members, shared deps, lints
rust-toolchain.toml              stable channel pin
README.md                        build/test/bench instructions + recorded baselines (Task 15)
crates/core/Cargo.toml           pm-core: no runtime deps
crates/core/src/lib.rs           module re-exports
crates/core/src/num.rs           TickSize, Px, Qty, Usdc, Bps, buy_cost, sell_proceeds, edge_bps
crates/core/src/book.rs          Side, Ladder (dense, best-pointer), Book, LadderIter
crates/core/src/fees.rs          fee_microusdc (venue formula, ceil)
crates/core/src/instrument.rs    TokenId, MarketId, EventId, Market, Partition, Relationship
crates/engine/Cargo.toml         pm-engine: deps pm-core, highs
crates/engine/src/lib.rs         ArbClass, Action, LegFill, Opportunity, GasTable, EngineParams
crates/engine/src/walker.rs      LegSpec, BasketSpec, WalkResult, walk()
crates/engine/src/class1.rs      binary complete-set detector (long+short)
crates/engine/src/class2.rs      NegRisk partition detector (long+short via NO-set)
crates/engine/src/class3.rs      implies / mutually-exclusive / equivalent detectors
crates/engine/src/lp.rs          ComponentSpec, World, enumerate_worlds, exact reval, solve_component
crates/engine/src/dedup.rs       Fingerprint, Cooldown
crates/engine/benches/hot_path.rs  criterion suite (Task 15)
crates/config/Cargo.toml         pm-config: serde, toml
crates/config/src/lib.rs         typed config skeleton, defaults = spec §2
```

Boundary rule (spec §3): `pm-core` and `pm-engine` do no I/O and have no async. Registry/component *computation* is M2; M1's engine receives prepared `Partition`/`Relationship`/`ComponentSpec` values (tests construct them by hand). `Book` carries no seq/staleness fields in M1 — those belong to M2's ingestion wrapper.

---

### Task 1: Workspace scaffold

**Files:**
- Create: `Cargo.toml`, `rust-toolchain.toml`, `crates/core/Cargo.toml`, `crates/core/src/lib.rs`

- [ ] **Step 1: Verify toolchain and cmake exist**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo --version && rustc --version && cmake --version
```

Expected: three version lines. If `cmake` is missing, stop and report (it's required by Task 12's `highs` dependency; `brew install cmake`).

- [ ] **Step 2: Create branch**

```bash
git switch -c feat/m1-engine
```

- [ ] **Step 3: Write workspace files**

`Cargo.toml`:
```toml
[workspace]
resolver = "3"
members = ["crates/core", "crates/engine", "crates/config"]

[workspace.package]
version = "0.1.0"
edition = "2024"

[workspace.dependencies]
pm-core = { path = "crates/core" }
highs = "1"
serde = { version = "1", features = ["derive"] }
toml = "0.8"
proptest = "1"
criterion = "0.5"

[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
```

`rust-toolchain.toml`:
```toml
[toolchain]
channel = "stable"
```

`crates/core/Cargo.toml`:
```toml
[package]
name = "pm-core"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dev-dependencies]
proptest.workspace = true
```

`crates/core/src/lib.rs`:
```rust
pub mod num;
```

`crates/core/src/num.rs` (placeholder module so the crate compiles; Task 2 fills it):
```rust
// Numeric core: filled in by Task 2.
```

Note: `crates/engine` and `crates/config` are listed as members but don't exist yet — cargo would fail. Create them in Task 6 and Task 14 instead; for now the members line reads `members = ["crates/core"]` and later tasks extend it. Write it that way:

```toml
members = ["crates/core"]
```

- [ ] **Step 4: Verify build**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build && cargo test
```

Expected: compiles; `running 0 tests`.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "chore: workspace scaffold with pm-core skeleton"
```

---

### Task 2: `pm-core::num` — exact numeric types

**Files:**
- Modify: `crates/core/src/num.rs`
- Test: same file, `#[cfg(test)] mod tests`

Spec §4. Everything here is `Copy`, exact, and branch-light.

- [ ] **Step 1: Write the failing tests**

Replace `crates/core/src/num.rs` with the types *declared but unimplemented last*; in TDD order, write tests first inside the module skeleton:

```rust
//! Exact numeric core (spec §4). Prices are native-tick integers, sizes are
//! micro-shares, cash is signed micro-USDC. Rounding is always against us.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TickSize {
    /// 0.01 markets — 100 levels per dollar.
    Cent,
    /// 0.001 markets — 1000 levels per dollar.
    Milli,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Px(u16);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Qty(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Usdc(pub i128);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Bps(pub i32);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NumError {
    OutOfRange,
}

pub const ONE_SHARE_MICRO: u64 = 1_000_000;
pub const ONE_USDC_MICRO: u64 = 1_000_000;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn ticksize_levels_and_units() {
        assert_eq!(TickSize::Cent.levels(), 100);
        assert_eq!(TickSize::Milli.levels(), 1000);
        assert_eq!(TickSize::Cent.unit_microusdc(), 10_000);
        assert_eq!(TickSize::Milli.unit_microusdc(), 1_000);
    }

    #[test]
    fn px_rejects_boundary_ticks() {
        assert!(Px::new(0, TickSize::Cent).is_err());
        assert!(Px::new(100, TickSize::Cent).is_err());
        assert!(Px::new(1000, TickSize::Milli).is_err());
        assert_eq!(Px::new(46, TickSize::Cent).unwrap().get(), 46);
    }

    #[test]
    fn px_microusdc_value() {
        // 0.46 on a cent market = 460_000 µUSDC/share
        assert_eq!(Px::new(46, TickSize::Cent).unwrap().microusdc(TickSize::Cent), 460_000);
        // 0.046 on a milli market
        assert_eq!(Px::new(46, TickSize::Milli).unwrap().microusdc(TickSize::Milli), 46_000);
    }

    #[test]
    fn buy_cost_rounds_up_sell_proceeds_round_down() {
        // 3 micro-shares at 0.46: true value 1.38 µUSDC
        assert_eq!(buy_cost(460_000, Qty(3)).0, 2);
        assert_eq!(sell_proceeds(460_000, Qty(3)).0, 1);
        // exact multiples don't round
        assert_eq!(buy_cost(460_000, Qty(1_000_000)).0, 460_000);
        assert_eq!(sell_proceeds(460_000, Qty(1_000_000)).0, 460_000);
    }

    #[test]
    fn edge_bps_floors_and_rejects_nonpositive_basis() {
        assert_eq!(edge_bps(Usdc(2_000_000), Usdc(98_000_000)), Some(Bps(204))); // 204.08… → 204
        assert_eq!(edge_bps(Usdc(-1), Usdc(100)), Some(Bps(-100)));
        assert_eq!(edge_bps(Usdc(1), Usdc(0)), None);
        assert_eq!(edge_bps(Usdc(1), Usdc(-5)), None);
    }

    proptest! {
        #[test]
        fn rounding_is_against_us(px in 1u64..1_000_000, q in 0u64..10_000_000_000) {
            let true_num = px as i128 * q as i128; // value × 1e6
            let b = buy_cost(px, Qty(q)).0;
            let s = sell_proceeds(px, Qty(q)).0;
            prop_assert!(b * 1_000_000 >= true_num);
            prop_assert!(s * 1_000_000 <= true_num);
            prop_assert!(b - s <= 1); // they bracket the true value within one µUSDC
        }
    }
}
```

- [ ] **Step 2: Run tests, verify they fail to compile**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo test -p pm-core
```

Expected: compile errors — `levels`, `unit_microusdc`, `Px::new`, `buy_cost`, etc. not found.

- [ ] **Step 3: Implement**

Add above the tests module:

```rust
impl TickSize {
    pub const fn levels(self) -> u16 {
        match self {
            TickSize::Cent => 100,
            TickSize::Milli => 1000,
        }
    }
    pub const fn unit_microusdc(self) -> u64 {
        match self {
            TickSize::Cent => 10_000,
            TickSize::Milli => 1_000,
        }
    }
}

impl Px {
    /// Interior ticks only: 1 ..= levels-1. Prices 0 and 1 are not representable.
    pub const fn new(tick: u16, ts: TickSize) -> Result<Self, NumError> {
        if tick >= 1 && tick < ts.levels() {
            Ok(Px(tick))
        } else {
            Err(NumError::OutOfRange)
        }
    }
    pub const fn get(self) -> u16 {
        self.0
    }
    /// Price in µUSDC per share.
    pub const fn microusdc(self, ts: TickSize) -> u64 {
        self.0 as u64 * ts.unit_microusdc()
    }
}

/// ceil(n/d) for n >= 0, d > 0.
const fn div_ceil_i128(n: i128, d: i128) -> i128 {
    (n + d - 1) / d
}

/// Cash to BUY `qty` micro-shares at `px_micro` µUSDC/share. Rounds UP (against us).
pub fn buy_cost(px_micro: u64, qty: Qty) -> Usdc {
    Usdc(div_ceil_i128(px_micro as i128 * qty.0 as i128, ONE_SHARE_MICRO as i128))
}

/// Cash received SELLING `qty` micro-shares at `px_micro`. Rounds DOWN (against us).
pub fn sell_proceeds(px_micro: u64, qty: Qty) -> Usdc {
    Usdc((px_micro as i128 * qty.0 as i128) / ONE_SHARE_MICRO as i128)
}

/// Net edge in bps of basis, floored toward −∞. None unless basis > 0.
pub fn edge_bps(net: Usdc, basis: Usdc) -> Option<Bps> {
    if basis.0 <= 0 {
        return None;
    }
    Some(Bps((net.0 * 10_000).div_euclid(basis.0) as i32))
}
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cargo test -p pm-core
```

Expected: all tests pass (5 unit + 1 proptest).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): exact numeric types with against-us rounding"
```

---

### Task 3: `pm-core::book` — dense ladders

**Files:**
- Create: `crates/core/src/book.rs`
- Modify: `crates/core/src/lib.rs` (add `pub mod book;`)

Spec §5: dense per-tick arrays, O(1) set, incremental best-pointer, iteration from best outward. The proptest pits `Ladder` against a `BTreeMap` reference model.

- [ ] **Step 1: Write the failing tests**

`crates/core/src/book.rs`:

```rust
//! Dense per-tick order-book ladders (spec §5).

use crate::num::{Px, Qty, TickSize};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Bid,
    Ask,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn px(t: u16) -> Px {
        Px::new(t, TickSize::Cent).unwrap()
    }

    #[test]
    fn empty_ladder_has_no_best() {
        let l = Ladder::new(TickSize::Cent, Side::Ask);
        assert_eq!(l.best(), None);
        assert_eq!(l.qty_at(px(50)), Qty(0));
    }

    #[test]
    fn best_tracks_inserts_per_side() {
        let mut asks = Ladder::new(TickSize::Cent, Side::Ask);
        asks.set(px(60), Qty(5));
        asks.set(px(40), Qty(5));
        asks.set(px(50), Qty(5));
        assert_eq!(asks.best(), Some(px(40))); // lowest ask is best

        let mut bids = Ladder::new(TickSize::Cent, Side::Bid);
        bids.set(px(40), Qty(5));
        bids.set(px(60), Qty(5));
        bids.set(px(50), Qty(5));
        assert_eq!(bids.best(), Some(px(60))); // highest bid is best
    }

    #[test]
    fn zeroing_best_rescans_to_next_level() {
        let mut asks = Ladder::new(TickSize::Cent, Side::Ask);
        asks.set(px(40), Qty(5));
        asks.set(px(55), Qty(7));
        asks.set(px(40), Qty(0));
        assert_eq!(asks.best(), Some(px(55)));
        asks.set(px(55), Qty(0));
        assert_eq!(asks.best(), None);
    }

    #[test]
    fn iter_from_best_orders_correctly_and_skips_zeros() {
        let mut bids = Ladder::new(TickSize::Cent, Side::Bid);
        bids.set(px(30), Qty(1));
        bids.set(px(70), Qty(2));
        bids.set(px(50), Qty(3));
        let got: Vec<(u16, u64)> = bids.iter_from_best().map(|(p, q)| (p.get(), q.0)).collect();
        assert_eq!(got, vec![(70, 2), (50, 3), (30, 1)]);

        let mut asks = Ladder::new(TickSize::Cent, Side::Ask);
        asks.set(px(70), Qty(2));
        asks.set(px(30), Qty(1));
        let got: Vec<(u16, u64)> = asks.iter_from_best().map(|(p, q)| (p.get(), q.0)).collect();
        assert_eq!(got, vec![(30, 1), (70, 2)]);
    }

    #[test]
    fn book_apply_routes_sides() {
        let mut b = Book::new(TickSize::Cent);
        b.apply(Side::Bid, px(45), Qty(10));
        b.apply(Side::Ask, px(55), Qty(20));
        assert_eq!(b.bids.best(), Some(px(45)));
        assert_eq!(b.asks.best(), Some(px(55)));
    }

    proptest! {
        /// Ladder behaves identically to a BTreeMap<u16, u64> reference model.
        #[test]
        fn matches_reference_model(
            ops in proptest::collection::vec((1u16..100, 0u64..1_000_000), 1..200),
            is_bid in proptest::bool::ANY,
        ) {
            let side = if is_bid { Side::Bid } else { Side::Ask };
            let mut ladder = Ladder::new(TickSize::Cent, side);
            let mut model: BTreeMap<u16, u64> = BTreeMap::new();
            for (t, q) in ops {
                ladder.set(px(t), Qty(q));
                if q == 0 { model.remove(&t); } else { model.insert(t, q); }

                let model_best = match side {
                    Side::Bid => model.keys().next_back().copied(),
                    Side::Ask => model.keys().next().copied(),
                };
                prop_assert_eq!(ladder.best().map(|p| p.get()), model_best);

                let want: Vec<(u16, u64)> = match side {
                    Side::Bid => model.iter().rev().map(|(k, v)| (*k, *v)).collect(),
                    Side::Ask => model.iter().map(|(k, v)| (*k, *v)).collect(),
                };
                let got: Vec<(u16, u64)> =
                    ladder.iter_from_best().map(|(p, q)| (p.get(), q.0)).collect();
                prop_assert_eq!(got, want);
            }
        }
    }
}
```

- [ ] **Step 2: Run tests, verify compile failure**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo test -p pm-core
```

Expected: `Ladder`/`Book` not found. (Remember to add `pub mod book;` to `lib.rs` now.)

- [ ] **Step 3: Implement**

Insert between the `Side` enum and the tests module:

```rust
#[derive(Clone, Debug)]
pub struct Ladder {
    ts: TickSize,
    side: Side,
    lvls: Box<[u64]>, // index = tick; 0 and levels() unused by Px's invariant
    best: Option<u16>,
}

impl Ladder {
    pub fn new(ts: TickSize, side: Side) -> Self {
        Ladder {
            ts,
            side,
            lvls: vec![0u64; ts.levels() as usize + 1].into_boxed_slice(),
            best: None,
        }
    }

    pub fn ts(&self) -> TickSize {
        self.ts
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn qty_at(&self, px: Px) -> Qty {
        Qty(self.lvls[px.get() as usize])
    }

    pub fn best(&self) -> Option<Px> {
        // Invariant: `best` only ever stores in-range interior ticks.
        self.best.and_then(|t| Px::new(t, self.ts).ok())
    }

    /// Replace the resting quantity at `px` (deltas arrive as absolute levels).
    pub fn set(&mut self, px: Px, qty: Qty) {
        let t = px.get();
        self.lvls[t as usize] = qty.0;
        let improves = |cand: u16, cur: u16| match self.side {
            Side::Bid => cand > cur,
            Side::Ask => cand < cur,
        };
        if qty.0 > 0 {
            if self.best.is_none_or(|b| improves(t, b)) {
                self.best = Some(t);
            }
        } else if self.best == Some(t) {
            self.best = self.rescan_from(t);
        }
    }

    /// Find the next non-empty level strictly worse than `from`.
    fn rescan_from(&self, from: u16) -> Option<u16> {
        match self.side {
            Side::Bid => (1..from).rev().find(|&i| self.lvls[i as usize] > 0),
            Side::Ask => ((from + 1)..self.ts.levels()).find(|&i| self.lvls[i as usize] > 0),
        }
    }

    /// Non-empty levels from best toward worse.
    pub fn iter_from_best(&self) -> impl Iterator<Item = (Px, Qty)> + '_ {
        let (start, ascending) = match (self.best, self.side) {
            (Some(b), Side::Ask) => (b, true),
            (Some(b), Side::Bid) => (b, false),
            (None, _) => (0, true), // empty range below
        };
        LadderIter { ladder: self, cur: if self.best.is_some() { Some(start) } else { None }, ascending }
    }
}

struct LadderIter<'a> {
    ladder: &'a Ladder,
    cur: Option<u16>,
    ascending: bool,
}

impl<'a> Iterator for LadderIter<'a> {
    type Item = (Px, Qty);
    fn next(&mut self) -> Option<Self::Item> {
        let mut t = self.cur?;
        loop {
            let q = self.ladder.lvls[t as usize];
            let item = if q > 0 {
                Px::new(t, self.ladder.ts).ok().map(|p| (p, Qty(q)))
            } else {
                None
            };
            // advance
            let next = if self.ascending {
                if t + 1 < self.ladder.ts.levels() { Some(t + 1) } else { None }
            } else if t > 1 {
                Some(t - 1)
            } else {
                None
            };
            self.cur = next;
            if let Some(it) = item {
                return Some(it);
            }
            t = next?;
        }
    }
}

#[derive(Clone, Debug)]
pub struct Book {
    pub bids: Ladder,
    pub asks: Ladder,
}

impl Book {
    pub fn new(ts: TickSize) -> Self {
        Book { bids: Ladder::new(ts, Side::Bid), asks: Ladder::new(ts, Side::Ask) }
    }

    pub fn ts(&self) -> TickSize {
        self.bids.ts()
    }

    pub fn apply(&mut self, side: Side, px: Px, qty: Qty) {
        match side {
            Side::Bid => self.bids.set(px, qty),
            Side::Ask => self.asks.set(px, qty),
        }
    }
}
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cargo test -p pm-core
```

Expected: all pass, including `matches_reference_model`.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): dense ladder books with incremental best tracking"
```

---

### Task 4: `pm-core::fees`

**Files:**
- Create: `crates/core/src/fees.rs`
- Modify: `crates/core/src/lib.rs` (add `pub mod fees;`)

Spec §6: `fee_µ = ceil(rate_bps · min(p, 1−p) · q / (10⁴ · 10⁶))`. Symmetric in price, ceil against us. The live schedule/levy asset gets re-verified in M2; the formula is parametrized here.

- [ ] **Step 1: Write the failing tests**

`crates/core/src/fees.rs`:

```rust
//! Venue fee formula (spec §6). Symmetric in price, rounded up (against us).

use crate::num::{Bps, Qty, Usdc};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn zero_rate_is_free() {
        assert_eq!(fee_microusdc(Bps(0), 460_000, Qty(10_000_000)), Usdc(0));
    }

    #[test]
    fn golden_value() {
        // 200 bps on 10 shares at 0.40: 0.02 × 0.40 × 10 = $0.08 = 80_000 µUSDC
        assert_eq!(fee_microusdc(Bps(200), 400_000, Qty(10_000_000)), Usdc(80_000));
    }

    #[test]
    fn symmetric_in_price() {
        for (p, q) in [(10_000u64, 3_333_333u64), (250_000, 1), (990_000, 7_777_777)] {
            assert_eq!(
                fee_microusdc(Bps(150), p, Qty(q)),
                fee_microusdc(Bps(150), 1_000_000 - p, Qty(q))
            );
        }
    }

    #[test]
    fn ceil_rounds_against_us() {
        // 1 bp on 1 micro-share at 0.50: true fee = 0.5×1×1e-4 µ = 0.00005 µ → ceil → 1 µ
        assert_eq!(fee_microusdc(Bps(1), 500_000, Qty(1)), Usdc(1));
    }

    proptest! {
        #[test]
        fn monotone_in_qty_and_rate(
            rate in 0i32..500, p in 1u64..1_000_000, q in 0u64..100_000_000_000
        ) {
            let f = fee_microusdc(Bps(rate), p, Qty(q)).0;
            let f_more_q = fee_microusdc(Bps(rate), p, Qty(q + 1_000_000)).0;
            let f_more_r = fee_microusdc(Bps(rate + 1), p, Qty(q)).0;
            prop_assert!(f_more_q >= f);
            prop_assert!(f_more_r >= f);
            prop_assert!(f >= 0);
        }
    }
}
```

- [ ] **Step 2: Run tests, verify compile failure**

```bash
cargo test -p pm-core
```

Expected: `fee_microusdc` not found.

- [ ] **Step 3: Implement**

Insert above the tests module:

```rust
/// Fee in µUSDC for a fill of `qty` micro-shares at `px_micro` µUSDC/share.
/// Polymarket schedule shape: rate · min(p, 1−p) · size, levied on the
/// output asset. Rounded UP. Negative rates are treated as zero.
pub fn fee_microusdc(rate: Bps, px_micro: u64, qty: Qty) -> Usdc {
    let rate = i128::from(rate.0.max(0));
    let base = i128::from(px_micro.min(1_000_000 - px_micro));
    let num = rate * base * i128::from(qty.0);
    const DEN: i128 = 10_000 * 1_000_000;
    Usdc((num + DEN - 1) / DEN)
}
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cargo test -p pm-core
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): venue fee formula, ceil-rounded"
```

---

### Task 5: `pm-core::instrument` — metadata types

**Files:**
- Create: `crates/core/src/instrument.rs`
- Modify: `crates/core/src/lib.rs` (add `pub mod instrument;`)

Spec §3/§9 types used by the engine. Ids are *interned handles* (`u64`/`u32`) — M2's registry owns the mapping from venue uint256 ids; the hot path never touches strings.

- [ ] **Step 1: Write the file (types + tests together — pure data, single step)**

`crates/core/src/instrument.rs`:

```rust
//! Instrument metadata handles. Venue-id interning happens in the registry (M2).

use crate::num::{Bps, TickSize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TokenId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct MarketId(pub u32);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EventId(pub u32);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Market {
    pub id: MarketId,
    pub yes: TokenId,
    pub no: TokenId,
    pub tick: TickSize,
    pub fee_bps: Bps,
    pub neg_risk: bool,
}

/// A mutually-exclusive outcome set (spec §8 class 2). `yes_tokens[i]` and
/// `no_tokens[i]` belong to the same member market. Only sets with
/// `verified_exhaustive == true` are tradable by class 2.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Partition {
    pub event: EventId,
    pub markets: Vec<MarketId>,
    pub yes_tokens: Vec<TokenId>,
    pub no_tokens: Vec<TokenId>,
    pub verified_exhaustive: bool,
}

/// Approved logical relationships (spec §9), stated about market YES outcomes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Relationship {
    /// a true ⇒ b true.
    Implies { a: MarketId, b: MarketId },
    /// a and b cannot both be true.
    MutuallyExclusive { a: MarketId, b: MarketId },
    /// a true ⇔ b true.
    Equivalent { a: MarketId, b: MarketId },
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn partition_lanes_stay_parallel() {
        let p = Partition {
            event: EventId(1),
            markets: vec![MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(20)],
            no_tokens: vec![TokenId(11), TokenId(21)],
            verified_exhaustive: true,
        };
        assert_eq!(p.markets.len(), p.yes_tokens.len());
        assert_eq!(p.markets.len(), p.no_tokens.len());
    }
}
```

- [ ] **Step 2: Build, test, commit**

```bash
cargo test -p pm-core
git add -A && git commit -m "feat(core): instrument metadata handles and relationship types"
```

---

### Task 6: `pm-engine` skeleton — opportunity types and params

**Files:**
- Create: `crates/engine/Cargo.toml`, `crates/engine/src/lib.rs`
- Modify: root `Cargo.toml` (members)

- [ ] **Step 1: Add the crate**

Root `Cargo.toml`: `members = ["crates/core", "crates/engine"]`.

`crates/engine/Cargo.toml`:
```toml
[package]
name = "pm-engine"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dependencies]
pm-core.workspace = true

[dev-dependencies]
proptest.workspace = true
```

(The `highs` dependency is added in Task 12, criterion in Task 15.)

- [ ] **Step 2: Write types + tests**

`crates/engine/src/lib.rs`:

```rust
//! Detection engine: sizing walker, arb classes 1–3, LP detector, dedup.

pub mod walker;

use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ArbClass {
    C1Long,
    C1Short,
    C2Long,
    C2Short,
    C3Implies,
    C3MutEx,
    C3Equiv,
    C4Lp,
}

/// Our action on a book (we buy from asks, sell into bids).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Action {
    Buy,
    Sell,
}

/// One leg of a sized opportunity. `cash` is signed: negative = out (cost +
/// fee for buys), positive = in (proceeds − fee for sells).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LegFill {
    pub token: TokenId,
    pub action: Action,
    pub ts: TickSize,
    pub limit_px: Px,
    pub qty: Qty,
    pub cash: Usdc,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Opportunity {
    pub class: ArbClass,
    pub fills: Vec<LegFill>,
    /// Basket units in micro-shares (each unit = 1 micro-share of every leg).
    pub units: Qty,
    pub net: Usdc,
    pub basis: Usdc,
    pub edge: Bps,
    /// Complete-set splits execution must perform first (market, units).
    /// Empty for pure-buy baskets.
    pub splits: Vec<(MarketId, Qty)>,
}

/// Per-operation Polygon gas estimates, µUSDC (spec §6; refined in M5).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct GasTable {
    pub split: u64,
    pub merge: u64,
    pub redeem: u64,
    pub negrisk_convert: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RedeemStrategy {
    Merge,
    Hold,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EngineParams {
    pub floor_c12: Bps,
    pub floor_c3: Bps,
    pub min_profit: Usdc,
    pub gas: GasTable,
    pub redeem: RedeemStrategy,
    /// Per-basket cash cap, µUSDC (spec §2 per-market cap).
    pub max_basis: Usdc,
    pub max_worlds: usize,
    pub cooldown_ms: u64,
    pub reemit_improvement_pct: u32,
}

impl Default for EngineParams {
    fn default() -> Self {
        EngineParams {
            floor_c12: Bps(30),
            floor_c3: Bps(100),
            min_profit: Usdc(1_000_000), // $1 dust filter
            gas: GasTable { split: 10_000, merge: 10_000, redeem: 15_000, negrisk_convert: 20_000 },
            redeem: RedeemStrategy::Merge,
            max_basis: Usdc(1_000_000_000), // $1k
            max_worlds: 4096,
            cooldown_ms: 2_000,
            reemit_improvement_pct: 20,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn defaults_match_spec_section_2_and_6() {
        let p = EngineParams::default();
        assert_eq!(p.floor_c12, Bps(30));
        assert_eq!(p.floor_c3, Bps(100));
        assert_eq!(p.min_profit, Usdc(1_000_000));
        assert_eq!(p.max_basis, Usdc(1_000_000_000));
        assert_eq!(p.max_worlds, 4096);
        assert_eq!(p.cooldown_ms, 2_000);
        assert_eq!(p.reemit_improvement_pct, 20);
        assert_eq!(p.redeem, RedeemStrategy::Merge);
    }
}
```

`crates/engine/src/walker.rs` for now:
```rust
// Filled in by Task 7.
```

- [ ] **Step 3: Build, test, commit**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo test -p pm-engine
git add -A && git commit -m "feat(engine): opportunity types and engine params"
```

---

### Task 7: `pm-engine::walker` — the sizing core

**Files:**
- Modify: `crates/engine/src/walker.rs`

Spec §7. One generalized basket model serves every detector:

- A basket *unit* = 1 micro-share of every leg simultaneously.
- `payout_per_share` µUSDC arrives per unit at resolution/merge (e.g. 1_000_000 for class 1 long; `(n−1)·1_000_000` for a class-2 NO set; 0 for sell baskets).
- `collateral_per_share` µUSDC is paid per unit up front (1_000_000 when splitting; else 0).
- Buy legs walk asks (cost + fee out); sell legs walk bids (proceeds − fee in).
- `gas` µUSDC is charged once per basket.

The walk decision uses **exact per-share marginals scaled by 10⁴** so fee bps stay integral: a buy leg at price `p` costs `10⁴·p + rate·min(p, 10⁶−p)` scaled units per share; a sell leg yields `10⁴·p − rate·min(p, 10⁶−p)`. March levels while marginal net > 0, capped by depth and by `max_basis`. Final accounting re-prices every (leg, level) fill with the rounded `buy_cost`/`sell_proceeds`/`fee_microusdc` — the exact numbers are authoritative for the floor gates.

- [ ] **Step 1: Write the failing tests**

`crates/engine/src/walker.rs`:

```rust
//! Generalized depth walker (spec §7): exact sizing for multi-leg baskets.

use crate::{Action, LegFill};
use pm_core::book::Ladder;
use pm_core::fees::fee_microusdc;
use pm_core::instrument::TokenId;
use pm_core::num::{buy_cost, edge_bps, sell_proceeds, Bps, Px, Qty, Usdc};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::num::TickSize;
    use proptest::prelude::*;

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    fn ladder(side: Side, lvls: &[(u16, u64)]) -> Ladder {
        let mut l = Ladder::new(TS, side);
        for &(t, q) in lvls {
            l.set(px(t), Qty(q));
        }
        l
    }

    fn buy_leg(token: u64, l: &Ladder, fee: i32) -> LegSpec<'_> {
        LegSpec { token: TokenId(token), action: Action::Buy, ladder: l, fee_bps: Bps(fee) }
    }

    fn sell_leg(token: u64, l: &Ladder, fee: i32) -> LegSpec<'_> {
        LegSpec { token: TokenId(token), action: Action::Sell, ladder: l, fee_bps: Bps(fee) }
    }

    /// Independent exact recompute of basket net at `units`, for cross-checking.
    fn brute_net(spec: &BasketSpec, units: u64) -> i128 {
        let mut cash: i128 = 0;
        for leg in &spec.legs {
            let mut remaining = units;
            for (p, q) in leg.ladder.iter_from_best() {
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(q.0);
                let pm = p.microusdc(leg.ladder.ts());
                let fee = fee_microusdc(leg.fee_bps, pm, Qty(take)).0;
                match leg.action {
                    Action::Buy => cash -= buy_cost(pm, Qty(take)).0 + fee,
                    Action::Sell => cash += sell_proceeds(pm, Qty(take)).0 - fee,
                }
                remaining -= take;
            }
            assert_eq!(remaining, 0, "brute_net called beyond depth");
        }
        let payout = (spec.payout_per_share as i128 * units as i128) / 1_000_000;
        let collateral = (spec.collateral_per_share as i128 * units as i128 + 999_999) / 1_000_000;
        cash + payout - collateral - spec.gas as i128
    }

    #[test]
    fn class1_long_shape_full_depth() {
        // YES asks 0.46×100sh, NO asks 0.52×100sh, no fees/gas → 2¢/unit, 100 sh.
        let yes = ladder(Side::Ask, &[(46, 100_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(100_000_000));
        assert_eq!(w.net, Usdc(2_000_000)); // $2
        assert_eq!(w.basis, Usdc(98_000_000)); // $98
        assert_eq!(w.edge, Bps(204));
        assert_eq!(w.fills.len(), 2);
        assert_eq!(w.fills[0].limit_px, px(46));
        assert_eq!(w.fills[1].limit_px, px(52));
    }

    #[test]
    fn stops_at_unprofitable_level() {
        // Second YES level pushes the sum past $1 → only first level taken.
        let yes = ladder(Side::Ask, &[(46, 50_000_000), (49, 50_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(50_000_000));
        assert_eq!(w.net, Usdc(1_000_000)); // 2¢ × 50
    }

    #[test]
    fn sell_basket_split_and_dump() {
        // Bids: YES 0.55×40sh, NO 0.50×40sh → split $1, sell at $1.05 → 5¢/unit.
        let yes = ladder(Side::Bid, &[(55, 40_000_000)]);
        let no = ladder(Side::Bid, &[(50, 40_000_000)]);
        let spec = BasketSpec {
            legs: vec![sell_leg(1, &yes, 0), sell_leg(2, &no, 0)],
            payout_per_share: 0,
            collateral_per_share: 1_000_000,
            gas: 0,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(40_000_000));
        assert_eq!(w.net, Usdc(2_000_000)); // 5¢ × 40
        assert_eq!(w.basis, Usdc(40_000_000)); // collateral $40
        assert!(w.fills.iter().all(|f| f.cash.0 > 0)); // sells bring cash in
    }

    #[test]
    fn fees_and_gas_reduce_net_exactly() {
        let yes = ladder(Side::Ask, &[(46, 100_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 100), buy_leg(2, &no, 100)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 123_456,
        };
        let w = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).unwrap();
        // fees: 1% × min(p,1−p) per share per leg = (0.46+0.48)¢ ×100 sh = $0.94
        let expected_fees = 460_000 + 480_000;
        assert_eq!(w.net, Usdc(2_000_000 - expected_fees - 123_456));
    }

    #[test]
    fn respects_max_basis_cap() {
        let yes = ladder(Side::Ask, &[(46, 100_000_000)]);
        let no = ladder(Side::Ask, &[(52, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        // $9.80 cap → exactly 10 shares (98¢ basis each).
        let w = walk(&spec, Usdc(9_800_000), Usdc(0), Bps(0)).unwrap();
        assert_eq!(w.units, Qty(10_000_000));
        assert!(w.basis <= Usdc(9_800_000));
    }

    #[test]
    fn gates_min_profit_and_floor() {
        let yes = ladder(Side::Ask, &[(49, 100_000_000)]);
        let no = ladder(Side::Ask, &[(50, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        // 1¢/unit on 99¢ basis ≈ 101 bps: passes 30, fails 150.
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(30)).is_some());
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(150)).is_none());
        // min_profit above $1 total → rejected.
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(1_100_000), Bps(0)).is_none());
    }

    #[test]
    fn no_edge_returns_none() {
        let yes = ladder(Side::Ask, &[(50, 100_000_000)]);
        let no = ladder(Side::Ask, &[(51, 100_000_000)]);
        let spec = BasketSpec {
            legs: vec![buy_leg(1, &yes, 0), buy_leg(2, &no, 0)],
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: 0,
        };
        assert!(walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0)).is_none());
    }

    proptest! {
        /// Walker's chosen extent is optimal among level boundaries and its
        /// accounting matches the independent recompute.
        #[test]
        fn optimal_at_level_boundaries(
            ya in 30u16..70, yq1 in 1u64..30_000_000, yq2 in 1u64..30_000_000,
            na in 30u16..70, nq1 in 1u64..30_000_000, nq2 in 1u64..30_000_000,
            fee in 0i32..200, gas in 0u64..50_000,
        ) {
            let yes = ladder(Side::Ask, &[(ya, yq1), (ya + 20, yq2)]);
            let no = ladder(Side::Ask, &[(na, nq1), (na + 20, nq2)]);
            let spec = BasketSpec {
                legs: vec![buy_leg(1, &yes, fee), buy_leg(2, &no, fee)],
                payout_per_share: 1_000_000,
                collateral_per_share: 0,
                gas,
            };
            let res = walk(&spec, Usdc(i128::MAX), Usdc(0), Bps(0));
            // candidate extents: every level-boundary prefix
            let depths = [yq1, yq1 + yq2, nq1, nq1 + nq2];
            let max_units = (yq1 + yq2).min(nq1 + nq2);
            let mut best: i128 = 0;
            for &d in depths.iter() {
                let u = d.min(max_units);
                let n = brute_net(&spec, u);
                if n > best { best = n; }
            }
            match res {
                Some(w) => {
                    prop_assert_eq!(w.net.0, brute_net(&spec, w.units.0));
                    prop_assert!(w.net.0 >= best,
                        "walker {} < best boundary {}", w.net.0, best);
                }
                None => prop_assert!(best <= 0, "walker missed profit {}", best),
            }
        }
    }
}
```

- [ ] **Step 2: Run tests, verify compile failure**

```bash
cargo test -p pm-engine
```

Expected: `LegSpec`, `BasketSpec`, `walk` not found.

- [ ] **Step 3: Implement**

Insert above the tests module:

```rust
#[derive(Clone, Copy, Debug)]
pub struct LegSpec<'a> {
    pub token: TokenId,
    pub action: Action,
    pub ladder: &'a Ladder,
    pub fee_bps: Bps,
}

#[derive(Clone, Debug)]
pub struct BasketSpec<'a> {
    pub legs: Vec<LegSpec<'a>>,
    /// µUSDC received per whole share-unit at resolution/merge.
    pub payout_per_share: u64,
    /// µUSDC paid per whole share-unit up front (splits).
    pub collateral_per_share: u64,
    /// Flat µUSDC per basket.
    pub gas: u64,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WalkResult {
    pub units: Qty,
    pub net: Usdc,
    pub basis: Usdc,
    pub edge: Bps,
    pub fills: Vec<LegFill>,
}

/// Marginal value of one share of `leg` at price `pm`, scaled ×10⁴ so the fee
/// term stays integral. Positive contributions reduce profit for buys.
fn scaled_leg_cost(action: Action, fee_bps: Bps, pm: u64) -> i128 {
    let p = i128::from(pm);
    let fee = i128::from(fee_bps.0.max(0)) * i128::from(pm.min(1_000_000 - pm));
    match action {
        Action::Buy => 10_000 * p + fee,
        Action::Sell => -(10_000 * p - fee),
    }
}

struct Cursor<'a, I: Iterator<Item = (Px, Qty)>> {
    leg: LegSpec<'a>,
    levels: I,
    cur: Option<(Px, u64)>, // price, remaining at level
    segs: Vec<(Px, u64)>,   // (price, filled) per touched level
}

/// Size the basket against depth. Returns None when nothing clears the gates.
pub fn walk(
    spec: &BasketSpec,
    max_basis: Usdc,
    min_profit: Usdc,
    floor: Bps,
) -> Option<WalkResult> {
    if spec.legs.is_empty() {
        return None;
    }
    let mut cursors: Vec<Cursor<_>> = spec
        .legs
        .iter()
        .map(|leg| {
            let mut levels = leg.ladder.iter_from_best();
            let cur = levels.next().map(|(p, q)| (p, q.0));
            Cursor { leg: *leg, levels, cur, segs: Vec::new() }
        })
        .collect();

    let fixed_scaled =
        10_000 * (i128::from(spec.payout_per_share) - i128::from(spec.collateral_per_share));
    let mut units: u64 = 0;
    let mut basis_used: i128 = 0; // µUSDC committed (collateral + buy notional), unrounded ceiling proxy

    loop {
        // Marginal scaled net per share at the current level combo.
        let mut marginal = fixed_scaled;
        let mut chunk = u64::MAX;
        let mut basis_per_share_scaled: i128 = 10_000 * i128::from(spec.collateral_per_share);
        let mut exhausted = false;
        for c in &cursors {
            match c.cur {
                Some((p, rem)) => {
                    let pm = p.microusdc(c.leg.ladder.ts());
                    let cost = scaled_leg_cost(c.leg.action, c.leg.fee_bps, pm);
                    marginal -= cost;
                    if c.leg.action == Action::Buy {
                        basis_per_share_scaled += cost;
                    }
                    chunk = chunk.min(rem);
                }
                None => {
                    exhausted = true;
                }
            }
        }
        if exhausted || marginal <= 0 || chunk == 0 || chunk == u64::MAX {
            break;
        }
        // Cap by remaining basis: shares ≤ remaining·10⁴·10⁶ / basis_per_share_scaled.
        if basis_per_share_scaled > 0 {
            let remaining = max_basis.0.saturating_sub(basis_used);
            if remaining <= 0 {
                break;
            }
            // micro-shares allowed = remaining µUSDC × 10⁴ × 10⁶ / scaled
            // basis per share. Clamp `remaining` so the multiply can't
            // overflow i128 even when max_basis is i128::MAX in tests.
            let rem = remaining.min(1_000_000_000_000_000_000);
            let cap = (rem * 10_000_000_000 / basis_per_share_scaled)
                .min(i128::from(u64::MAX)) as u64;
            if cap == 0 {
                break;
            }
            chunk = chunk.min(cap);
        }
        // Take the chunk on every leg.
        for c in &mut cursors {
            if let Some((p, rem)) = c.cur {
                let new_rem = rem - chunk;
                match c.segs.last_mut() {
                    Some(last) if last.0 == p => last.1 += chunk,
                    _ => c.segs.push((p, chunk)),
                }
                c.cur = if new_rem == 0 { c.levels.next().map(|(p, q)| (p, q.0)) } else { Some((p, new_rem)) };
            }
        }
        units += chunk;
        basis_used += basis_per_share_scaled * i128::from(chunk) / (10_000 * 1_000_000);
    }

    if units == 0 {
        return None;
    }

    // Exact accounting over recorded segments (authoritative).
    let mut cash: i128 = 0;
    let mut buy_outlay: i128 = 0;
    let mut fills = Vec::with_capacity(cursors.len());
    for c in &cursors {
        let ts = c.leg.ladder.ts();
        let mut leg_cash: i128 = 0;
        let mut worst: Option<Px> = None;
        let mut qty: u64 = 0;
        for &(p, q) in &c.segs {
            let pm = p.microusdc(ts);
            let fee = fee_microusdc(c.leg.fee_bps, pm, Qty(q)).0;
            match c.leg.action {
                Action::Buy => {
                    let cost = buy_cost(pm, Qty(q)).0 + fee;
                    leg_cash -= cost;
                    buy_outlay += cost;
                }
                Action::Sell => leg_cash += sell_proceeds(pm, Qty(q)).0 - fee,
            }
            worst = Some(p); // segments are walked best→worst
            qty += q;
        }
        cash += leg_cash;
        let worst = worst?;
        fills.push(LegFill {
            token: c.leg.token,
            action: c.leg.action,
            ts,
            limit_px: worst,
            qty: Qty(qty),
            cash: Usdc(leg_cash),
        });
    }
    // Payout floors (income), collateral ceils (cost) — both ÷10⁶ exact-ish.
    let payout = i128::from(spec.payout_per_share) * i128::from(units) / 1_000_000;
    let collateral =
        (i128::from(spec.collateral_per_share) * i128::from(units) + 999_999) / 1_000_000;
    let net = Usdc(cash + payout - collateral - i128::from(spec.gas));
    let basis = Usdc(buy_outlay + collateral);

    if net < min_profit {
        return None;
    }
    let edge = edge_bps(net, basis)?;
    if edge < floor {
        return None;
    }
    Some(WalkResult { units: Qty(units), net, basis, edge, fills })
}
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cargo test -p pm-engine
```

Expected: all unit tests + `optimal_at_level_boundaries` pass. If the proptest finds a counterexample, minimize and fix the walker (most likely suspects: basis-cap arithmetic, marginal sign conventions) — do not weaken the test.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): generalized depth walker with exact accounting"
```

---

### Task 8: `pm-engine::class1` — binary complete-set detector

**Files:**
- Create: `crates/engine/src/class1.rs`
- Modify: `crates/engine/src/lib.rs` (add `pub mod class1;`)
- Modify: `crates/engine/src/walker.rs` (add `WalkResult::into_opportunity`)

Spec §8 class 1. Long: buy YES asks + NO asks, $1 payout per unit, gas per `redeem` strategy. Short: $1 collateral per unit (split), sell into both bids, split gas.

- [ ] **Step 1: Add the shared adapter to `walker.rs`** (after the `WalkResult` struct):

```rust
impl WalkResult {
    pub fn into_opportunity(self, class: crate::ArbClass) -> crate::Opportunity {
        crate::Opportunity {
            class,
            fills: self.fills,
            units: self.units,
            net: self.net,
            basis: self.basis,
            edge: self.edge,
            splits: Vec::new(),
        }
    }
}
```

- [ ] **Step 2: Write the failing tests**

`crates/engine/src/class1.rs`:

```rust
//! Class 1: binary complete-set arbitrage (spec §8).

use crate::walker::{walk, BasketSpec, LegSpec};
use crate::{Action, ArbClass, EngineParams, Opportunity, RedeemStrategy};
use pm_core::book::Book;
use pm_core::instrument::Market;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::instrument::{MarketId, TokenId};
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    fn market(fee: i32) -> Market {
        Market {
            id: MarketId(1),
            yes: TokenId(10),
            no: TokenId(11),
            tick: TS,
            fee_bps: Bps(fee),
            neg_risk: false,
        }
    }

    fn books(yes_ask: u16, no_ask: u16, yes_bid: u16, no_bid: u16, q: u64) -> (Book, Book) {
        let mut yes = Book::new(TS);
        let mut no = Book::new(TS);
        yes.apply(Side::Ask, px(yes_ask), Qty(q));
        no.apply(Side::Ask, px(no_ask), Qty(q));
        yes.apply(Side::Bid, px(yes_bid), Qty(q));
        no.apply(Side::Bid, px(no_bid), Qty(q));
        (yes, no)
    }

    fn zero_gas_params() -> EngineParams {
        EngineParams {
            gas: crate::GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[test]
    fn finds_long_when_set_is_cheap() {
        // asks 0.46+0.52 = 0.98; bids fair.
        let (yes, no) = books(46, 52, 44, 50, 100_000_000);
        let ops = detect(&market(0), &yes, &no, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C1Long);
        assert_eq!(ops[0].net, Usdc(2_000_000));
        assert_eq!(ops[0].fills[0].action, Action::Buy);
    }

    #[test]
    fn finds_short_when_set_is_rich() {
        // bids 0.55+0.50 = 1.05; asks fair.
        let (yes, no) = books(57, 52, 55, 50, 100_000_000);
        let ops = detect(&market(0), &yes, &no, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C1Short);
        assert_eq!(ops[0].net, Usdc(5_000_000)); // 5¢ × 100
        assert_eq!(ops[0].basis, Usdc(100_000_000)); // split collateral
        assert_eq!(ops[0].splits, vec![(MarketId(1), Qty(100_000_000))]);
    }

    #[test]
    fn fair_books_yield_nothing() {
        let (yes, no) = books(51, 50, 49, 48, 100_000_000);
        assert!(detect(&market(0), &yes, &no, &zero_gas_params()).is_empty());
    }

    #[test]
    fn thirty_bps_floor_filters_thin_edges() {
        // asks sum to 0.998 on a Milli book → ~20 bps gross: below the 30 floor.
        let ts = TickSize::Milli;
        let p = |t: u16| Px::new(t, ts).unwrap();
        let mut yes = Book::new(ts);
        let mut no = Book::new(ts);
        yes.apply(Side::Ask, p(499), Qty(100_000_000));
        no.apply(Side::Ask, p(499), Qty(100_000_000));
        let m = Market { tick: ts, ..market(0) };
        assert!(detect(&m, &yes, &no, &zero_gas_params()).is_empty());
    }

    #[test]
    fn redeem_strategy_picks_gas() {
        let (yes, no) = books(46, 52, 44, 50, 100_000_000);
        let mut p = zero_gas_params();
        p.gas.merge = 500_000; // $0.50
        p.gas.redeem = 1_500_000;
        p.redeem = RedeemStrategy::Merge;
        let merge_net = detect(&market(0), &yes, &no, &p)[0].net;
        p.redeem = RedeemStrategy::Hold;
        let hold_net = detect(&market(0), &yes, &no, &p)[0].net;
        assert_eq!(merge_net, Usdc(1_500_000));
        assert_eq!(hold_net, Usdc(500_000));
    }

    #[test]
    fn both_sides_can_fire_on_a_crossed_market() {
        // Degenerate books: cheap asks AND rich bids (won't happen live; math
        // must still be independent per side).
        let (yes, no) = books(46, 50, 55, 52, 100_000_000);
        let ops = detect(&market(0), &yes, &no, &zero_gas_params());
        assert_eq!(ops.len(), 2);
    }
}
```

- [ ] **Step 3: Run tests, verify compile failure**

```bash
cargo test -p pm-engine class1
```

Expected: `detect` not found.

- [ ] **Step 4: Implement**

Insert above the tests module in `class1.rs`:

```rust
/// Detect long (cheap set) and short (rich set) complete-set arbs.
pub fn detect(m: &Market, yes: &Book, no: &Book, p: &EngineParams) -> Vec<Opportunity> {
    let mut out = Vec::new();
    let long_gas = match p.redeem {
        RedeemStrategy::Merge => p.gas.merge,
        RedeemStrategy::Hold => p.gas.redeem,
    };
    let long = BasketSpec {
        legs: vec![
            LegSpec { token: m.yes, action: Action::Buy, ladder: &yes.asks, fee_bps: m.fee_bps },
            LegSpec { token: m.no, action: Action::Buy, ladder: &no.asks, fee_bps: m.fee_bps },
        ],
        payout_per_share: 1_000_000,
        collateral_per_share: 0,
        gas: long_gas,
    };
    if let Some(w) = walk(&long, p.max_basis, p.min_profit, p.floor_c12) {
        out.push(w.into_opportunity(ArbClass::C1Long));
    }
    let short = BasketSpec {
        legs: vec![
            LegSpec { token: m.yes, action: Action::Sell, ladder: &yes.bids, fee_bps: m.fee_bps },
            LegSpec { token: m.no, action: Action::Sell, ladder: &no.bids, fee_bps: m.fee_bps },
        ],
        payout_per_share: 0,
        collateral_per_share: 1_000_000,
        gas: p.gas.split,
    };
    if let Some(w) = walk(&short, p.max_basis, p.min_profit, p.floor_c12) {
        let units = w.units;
        let mut op = w.into_opportunity(ArbClass::C1Short);
        op.splits = vec![(m.id, units)]; // execution must split before selling
        out.push(op);
    }
    out
}
```

- [ ] **Step 5: Run tests, verify pass, commit**

```bash
cargo test -p pm-engine
git add -A && git commit -m "feat(engine): class-1 complete-set detector"
```

---

### Task 9: `pm-engine::class2` — NegRisk partition detector

**Files:**
- Create: `crates/engine/src/class2.rs`
- Modify: `crates/engine/src/lib.rs` (add `pub mod class2;`)

Spec §8 class 2. Long: buy every YES ask; exactly one pays $1. Short (rich set): buy every NO ask; the NO set pays $(n−1) per unit via the NegRisk identity. **Hard gate on `verified_exhaustive`.** Books arrive as `&HashMap<TokenId, Book>`; per-market fee/tick come from the `Market` slice.

- [ ] **Step 1: Write the failing tests**

`crates/engine/src/class2.rs`:

```rust
//! Class 2: NegRisk multi-outcome arbitrage (spec §8).

use std::collections::HashMap;

use crate::walker::{walk, BasketSpec, LegSpec};
use crate::{Action, ArbClass, EngineParams, Opportunity};
use pm_core::book::Book;
use pm_core::instrument::{Market, Partition, TokenId};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::instrument::{EventId, MarketId};
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    /// 3-outcome partition; per outcome: (yes_ask, no_ask) at 100 shares deep.
    fn fixture(quotes: &[(u16, u16)], verified: bool) -> (Partition, Vec<Market>, HashMap<TokenId, Book>) {
        let n = quotes.len() as u32;
        let mut markets = Vec::new();
        let mut books = HashMap::new();
        let mut yes_tokens = Vec::new();
        let mut no_tokens = Vec::new();
        let mut market_ids = Vec::new();
        for (i, &(ya, na)) in quotes.iter().enumerate() {
            let i = i as u32;
            let yes = TokenId(u64::from(i) * 2 + 10);
            let no = TokenId(u64::from(i) * 2 + 11);
            markets.push(Market {
                id: MarketId(i),
                yes,
                no,
                tick: TS,
                fee_bps: Bps(0),
                neg_risk: true,
            });
            let mut yb = Book::new(TS);
            yb.apply(Side::Ask, px(ya), Qty(100_000_000));
            let mut nb = Book::new(TS);
            nb.apply(Side::Ask, px(na), Qty(100_000_000));
            books.insert(yes, yb);
            books.insert(no, nb);
            yes_tokens.push(yes);
            no_tokens.push(no);
            market_ids.push(MarketId(i));
        }
        let part = Partition {
            event: EventId(n),
            markets: market_ids,
            yes_tokens,
            no_tokens,
            verified_exhaustive: verified,
        };
        (part, markets, books)
    }

    fn zero_gas_params() -> EngineParams {
        EngineParams {
            gas: crate::GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[test]
    fn finds_long_when_yes_set_sums_below_one() {
        // 0.30 + 0.30 + 0.35 = 0.95 → 5¢/unit, NO asks fair (sum 2.10 > 2).
        let (part, markets, books) = fixture(&[(30, 72), (30, 72), (35, 66)], true);
        let ops = detect(&part, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C2Long);
        assert_eq!(ops[0].net, Usdc(5_000_000));
        assert_eq!(ops[0].fills.len(), 3);
    }

    #[test]
    fn finds_short_via_cheap_no_set() {
        // NO asks 0.62+0.62+0.70 = 1.94 < n−1 = 2 → 6¢/unit. YES asks fair.
        let (part, markets, books) = fixture(&[(35, 62), (35, 62), (32, 70)], true);
        let ops = detect(&part, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C2Short);
        assert_eq!(ops[0].net, Usdc(6_000_000));
    }

    #[test]
    fn unverified_partition_is_untouchable() {
        let (part, markets, books) = fixture(&[(30, 72), (30, 72), (35, 66)], false);
        assert!(detect(&part, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn fair_partition_yields_nothing() {
        // YES sums to 1.00, NO sums to 2.00 exactly.
        let (part, markets, books) = fixture(&[(33, 67), (33, 67), (34, 66)], true);
        assert!(detect(&part, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn missing_book_skips_cleanly() {
        let (part, markets, mut books) = fixture(&[(30, 72), (30, 72), (35, 66)], true);
        books.remove(&part.yes_tokens[2]);
        assert!(detect(&part, &markets, &books, &zero_gas_params()).is_empty());
    }
}
```

- [ ] **Step 2: Run, verify compile failure**

```bash
cargo test -p pm-engine class2
```

- [ ] **Step 3: Implement**

Insert above the tests module:

```rust
fn fee_of(markets: &[Market], token: TokenId) -> Option<pm_core::num::Bps> {
    markets.iter().find(|m| m.yes == token || m.no == token).map(|m| m.fee_bps)
}

/// Detect underpriced YES sets (long) and underpriced NO sets (short, via the
/// NegRisk identity: a full NO set pays $(n−1) per unit).
pub fn detect(
    part: &Partition,
    markets: &[Market],
    books: &HashMap<TokenId, Book>,
    p: &EngineParams,
) -> Vec<Opportunity> {
    let mut out = Vec::new();
    if !part.verified_exhaustive || part.yes_tokens.len() < 2 {
        return out;
    }
    let n = part.yes_tokens.len() as u64;

    let build = |tokens: &[TokenId]| -> Option<Vec<LegSpec<'_>>> {
        tokens
            .iter()
            .map(|&t| {
                Some(LegSpec {
                    token: t,
                    action: Action::Buy,
                    ladder: &books.get(&t)?.asks,
                    fee_bps: fee_of(markets, t)?,
                })
            })
            .collect()
    };

    if let Some(legs) = build(&part.yes_tokens) {
        let spec = BasketSpec {
            legs,
            payout_per_share: 1_000_000,
            collateral_per_share: 0,
            gas: p.gas.redeem,
        };
        if let Some(w) = walk(&spec, p.max_basis, p.min_profit, p.floor_c12) {
            out.push(w.into_opportunity(ArbClass::C2Long));
        }
    }
    if let Some(legs) = build(&part.no_tokens) {
        let spec = BasketSpec {
            legs,
            payout_per_share: (n - 1) * 1_000_000,
            collateral_per_share: 0,
            gas: p.gas.negrisk_convert,
        };
        if let Some(w) = walk(&spec, p.max_basis, p.min_profit, p.floor_c12) {
            out.push(w.into_opportunity(ArbClass::C2Short));
        }
    }
    out
}
```

- [ ] **Step 4: Run tests, verify pass, commit**

```bash
cargo test -p pm-engine
git add -A && git commit -m "feat(engine): class-2 NegRisk partition detector"
```

---

### Task 10: `pm-engine::class3` — cross-market logical detector

**Files:**
- Create: `crates/engine/src/class3.rs`
- Modify: `crates/engine/src/lib.rs` (add `pub mod class3;`)

Spec §8 class 3, buy-only formulations, 100 bps floor:
- `Implies(a→b)` violated → buy `NO_a` + buy `YES_b` (min payout $1/unit).
- `MutuallyExclusive(a,b)` violated → buy `NO_a` + buy `NO_b` (min payout $1/unit).
- `Equivalent(a,b)` → both implication directions.

- [ ] **Step 1: Write the failing tests**

`crates/engine/src/class3.rs`:

```rust
//! Class 3: cross-market logical arbitrage (spec §8). Buy-only, 100 bps floor.

use std::collections::HashMap;

use crate::walker::{walk, BasketSpec, LegSpec};
use crate::{Action, ArbClass, EngineParams, Opportunity};
use pm_core::book::Book;
use pm_core::instrument::{Market, MarketId, Relationship, TokenId};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    /// Two binary markets with the given (yes_ask, no_ask) quotes, 100sh deep.
    fn fixture(a: (u16, u16), b: (u16, u16)) -> (Vec<Market>, HashMap<TokenId, Book>) {
        let mut markets = Vec::new();
        let mut books = HashMap::new();
        for (i, &(ya, na)) in [a, b].iter().enumerate() {
            let i = i as u32;
            let yes = TokenId(u64::from(i) * 2 + 10);
            let no = TokenId(u64::from(i) * 2 + 11);
            markets.push(Market {
                id: MarketId(i),
                yes,
                no,
                tick: TS,
                fee_bps: Bps(0),
                neg_risk: false,
            });
            let mut yb = Book::new(TS);
            yb.apply(Side::Ask, px(ya), Qty(100_000_000));
            let mut nb = Book::new(TS);
            nb.apply(Side::Ask, px(na), Qty(100_000_000));
            books.insert(yes, yb);
            books.insert(no, nb);
        }
        (markets, books)
    }

    fn zero_gas_params() -> EngineParams {
        EngineParams {
            gas: crate::GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[test]
    fn implies_violation_is_tradable() {
        // P(A)≈0.65 (NO_a ask 0.35), P(B) ask 0.55 < P(A): A⇒B violated.
        // Basket NO_a + YES_b = 0.90 → 10¢/unit ≥ 100 bps. NO_b at 0.50 keeps
        // the mutex direction quiet.
        let (markets, books) = fixture((68, 35), (55, 50));
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C3Implies);
        assert_eq!(ops[0].net, Usdc(10_000_000));
        let toks: Vec<TokenId> = ops[0].fills.iter().map(|f| f.token).collect();
        assert_eq!(toks, vec![TokenId(11), TokenId(12)]); // NO_a, YES_b
    }

    #[test]
    fn coherent_implication_is_quiet() {
        // P(A) low vs P(B) high: NO_a 0.70 + YES_b 0.75 = 1.45 → no arb.
        let (markets, books) = fixture((32, 70), (75, 27));
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        assert!(detect(&rel, &markets, &books, &zero_gas_params()).is_empty());
    }

    #[test]
    fn mutex_violation_is_tradable() {
        // NO_a 0.55 + NO_b 0.40 = 0.95 → 5¢/unit ≥ 100 bps.
        let (markets, books) = fixture((47, 55), (62, 40));
        let rel = Relationship::MutuallyExclusive { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C3MutEx);
        assert_eq!(ops[0].net, Usdc(5_000_000));
    }

    #[test]
    fn hundred_bps_floor_bites() {
        let rel = Relationship::Implies { a: MarketId(0), b: MarketId(1) };
        // NO_a 0.50 + YES_b 0.50 = 1.00 → no profit at all.
        let (markets, books) = fixture((68, 50), (50, 51));
        assert!(detect(&rel, &markets, &books, &zero_gas_params()).is_empty());
        // NO_a 0.49 + YES_b 0.50 = 0.99 → 1¢ on 0.99 ≈ 101 bps ≥ 100 → trades.
        let (markets, books) = fixture((68, 49), (50, 52));
        assert_eq!(detect(&rel, &markets, &books, &zero_gas_params()).len(), 1);
    }

    #[test]
    fn equivalent_checks_both_directions() {
        // A cheap vs B rich: YES_a 0.40, NO_b ask 0.45 → buy YES_a + NO_b = 0.85.
        // (Equivalent(a,b): direction b⇒a buys NO_b + YES_a.)
        let (markets, books) = fixture((40, 62), (57, 45));
        let rel = Relationship::Equivalent { a: MarketId(0), b: MarketId(1) };
        let ops = detect(&rel, &markets, &books, &zero_gas_params());
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].class, ArbClass::C3Equiv);
        assert_eq!(ops[0].net, Usdc(15_000_000));
    }
}
```

- [ ] **Step 2: Run, verify compile failure**

```bash
cargo test -p pm-engine class3
```

- [ ] **Step 3: Implement**

Insert above the tests module:

```rust
fn find(markets: &[Market], id: MarketId) -> Option<&Market> {
    markets.iter().find(|m| m.id == id)
}

/// Buy `first` + buy `second`, guaranteed ≥ $1/unit payout, class-3 floor.
fn pair_basket(
    class: ArbClass,
    first: (TokenId, &Book, pm_core::num::Bps),
    second: (TokenId, &Book, pm_core::num::Bps),
    p: &EngineParams,
) -> Option<Opportunity> {
    let spec = BasketSpec {
        legs: vec![
            LegSpec { token: first.0, action: Action::Buy, ladder: &first.1.asks, fee_bps: first.2 },
            LegSpec { token: second.0, action: Action::Buy, ladder: &second.1.asks, fee_bps: second.2 },
        ],
        payout_per_share: 1_000_000,
        collateral_per_share: 0,
        gas: p.gas.redeem,
    };
    walk(&spec, p.max_basis, p.min_profit, p.floor_c3).map(|w| w.into_opportunity(class))
}

/// Detect Dutch books across one approved relationship.
pub fn detect(
    rel: &Relationship,
    markets: &[Market],
    books: &HashMap<TokenId, Book>,
    p: &EngineParams,
) -> Vec<Opportunity> {
    let mut out = Vec::new();
    let leg = |token: TokenId, m: &Market| -> Option<(TokenId, &Book, pm_core::num::Bps)> {
        Some((token, books.get(&token)?, m.fee_bps))
    };
    let implies = |a: MarketId, b: MarketId, class: ArbClass, out: &mut Vec<Opportunity>| {
        let (Some(ma), Some(mb)) = (find(markets, a), find(markets, b)) else { return };
        let (Some(no_a), Some(yes_b)) = (leg(ma.no, ma), leg(mb.yes, mb)) else { return };
        if let Some(op) = pair_basket(class, no_a, yes_b, p) {
            out.push(op);
        }
    };
    match *rel {
        Relationship::Implies { a, b } => implies(a, b, ArbClass::C3Implies, &mut out),
        Relationship::MutuallyExclusive { a, b } => {
            let (Some(ma), Some(mb)) = (find(markets, a), find(markets, b)) else {
                return out;
            };
            let (Some(no_a), Some(no_b)) = (leg(ma.no, ma), leg(mb.no, mb)) else {
                return out;
            };
            if let Some(op) = pair_basket(ArbClass::C3MutEx, no_a, no_b, p) {
                out.push(op);
            }
        }
        Relationship::Equivalent { a, b } => {
            implies(a, b, ArbClass::C3Equiv, &mut out);
            implies(b, a, ArbClass::C3Equiv, &mut out);
        }
    }
    out
}
```

- [ ] **Step 4: Run tests, verify pass, commit**

```bash
cargo test -p pm-engine
git add -A && git commit -m "feat(engine): class-3 logical-relationship detector"
```

---

### Task 11: `pm-engine::lp` part A — worlds, pruning, exact re-validation

**Files:**
- Create: `crates/engine/src/lp.rs`
- Modify: `crates/engine/src/lib.rs` (add `pub mod lp;`)

Spec §10. The LP's action set in M1 is **buys, sells, and splits** — that is complete for detection across classes 1–3: a NO-set purchase already realizes the class-2-short payout in world accounting (each losing market's NO pays $1), and splits are what enable sells. NegRisk-convert as an LP action is an execution-path optimization deferred to M2+. The LP objective is gas-less; the exact re-validation charges flat gas (redeem if positions are held, split if splits are used) and is the only authority on profit.

- [ ] **Step 1: Write the failing tests**

`crates/engine/src/lp.rs`:

```rust
//! Class 4: unified LP detector (spec §10). Part A: worlds + exact reval.

use std::collections::HashMap;

use crate::{Action, EngineParams, LegFill, Opportunity};
use pm_core::book::Book;
use pm_core::fees::fee_microusdc;
use pm_core::instrument::{Market, MarketId, Partition, Relationship, TokenId};
use pm_core::num::{buy_cost, edge_bps, sell_proceeds, Qty, Usdc};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::book::Side;
    use pm_core::instrument::EventId;
    use pm_core::num::{Bps, Px, TickSize};

    const TS: TickSize = TickSize::Cent;

    fn px(t: u16) -> Px {
        Px::new(t, TS).unwrap()
    }

    fn mk(i: u32) -> Market {
        Market {
            id: MarketId(i),
            yes: TokenId(u64::from(i) * 2 + 10),
            no: TokenId(u64::from(i) * 2 + 11),
            tick: TS,
            fee_bps: Bps(0),
            neg_risk: false,
        }
    }

    fn quote(books: &mut HashMap<TokenId, Book>, t: TokenId, ask: u16, bid: u16, q: u64) {
        let mut b = Book::new(TS);
        b.apply(Side::Ask, px(ask), Qty(q));
        b.apply(Side::Bid, px(bid), Qty(q));
        books.insert(t, b);
    }

    #[test]
    fn two_free_binaries_make_four_worlds() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        assert_eq!(enumerate_worlds(&spec, 4096).unwrap().len(), 4);
    }

    #[test]
    fn implies_prunes_a_and_not_b() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::Implies { a: MarketId(0), b: MarketId(1) }],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        assert_eq!(worlds.len(), 3);
        assert!(worlds.iter().all(|w| {
            let a = token_pays(&spec, w, TokenId(10)).unwrap();
            let b = token_pays(&spec, w, TokenId(12)).unwrap();
            !a || b
        }));
    }

    #[test]
    fn mutex_and_equivalent_prune() {
        let books = HashMap::new();
        let spec_mutex = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::MutuallyExclusive { a: MarketId(0), b: MarketId(1) }],
            books: &books,
        };
        assert_eq!(enumerate_worlds(&spec_mutex, 4096).unwrap().len(), 3);
        let spec_eq = ComponentSpec {
            relationships: vec![Relationship::Equivalent { a: MarketId(0), b: MarketId(1) }],
            ..spec_mutex.clone()
        };
        assert_eq!(enumerate_worlds(&spec_eq, 4096).unwrap().len(), 2);
    }

    #[test]
    fn partition_contributes_n_outcomes() {
        let books = HashMap::new();
        let part = Partition {
            event: EventId(0),
            markets: vec![MarketId(0), MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(12), TokenId(14)],
            no_tokens: vec![TokenId(11), TokenId(13), TokenId(15)],
            verified_exhaustive: true,
        };
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1), mk(2), mk(3)], // 3 in partition + 1 free
            partitions: vec![part],
            relationships: vec![],
            books: &books,
        };
        // 3 partition outcomes × 2 for the free market.
        assert_eq!(enumerate_worlds(&spec, 4096).unwrap().len(), 6);
        // exactly one partition YES pays per world
        for w in enumerate_worlds(&spec, 4096).unwrap() {
            let paying = [TokenId(10), TokenId(12), TokenId(14)]
                .iter()
                .filter(|&&t| token_pays(&spec, &w, t).unwrap())
                .count();
            assert_eq!(paying, 1);
        }
    }

    #[test]
    fn world_cap_applies_to_preprune_product() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: (0..3).map(mk).collect(), // 2^3 = 8 pre-prune
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        assert!(enumerate_worlds(&spec, 7).is_none());
        assert!(enumerate_worlds(&spec, 8).is_some());
    }

    #[test]
    fn exact_reval_of_hedged_class1_basket() {
        // Buy YES@0.46 + NO@0.52 ×100sh: payoff $100 in both worlds, cost $98.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![
                LegFill {
                    token: TokenId(10),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(46),
                    qty: Qty(100_000_000),
                    cash: Usdc(-46_000_000),
                },
                LegFill {
                    token: TokenId(11),
                    action: Action::Buy,
                    ts: TS,
                    limit_px: px(52),
                    qty: Qty(100_000_000),
                    cash: Usdc(-52_000_000),
                },
            ],
            splits: vec![],
        };
        let (worst, basis) = exact_worst_net(&spec, &worlds, &sol, 0).unwrap();
        assert_eq!(worst, Usdc(2_000_000));
        assert_eq!(basis, Usdc(98_000_000));
    }

    #[test]
    fn exact_reval_split_and_sell() {
        // Split 100 sets, sell YES@0.55 + NO@0.50: $5 risk-free.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 57, 55, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![
                LegFill {
                    token: TokenId(10),
                    action: Action::Sell,
                    ts: TS,
                    limit_px: px(55),
                    qty: Qty(100_000_000),
                    cash: Usdc(55_000_000),
                },
                LegFill {
                    token: TokenId(11),
                    action: Action::Sell,
                    ts: TS,
                    limit_px: px(50),
                    qty: Qty(100_000_000),
                    cash: Usdc(50_000_000),
                },
            ],
            splits: vec![(MarketId(0), Qty(100_000_000))],
        };
        let (worst, basis) = exact_worst_net(&spec, &worlds, &sol, 0).unwrap();
        assert_eq!(worst, Usdc(5_000_000));
        assert_eq!(basis, Usdc(100_000_000));
    }

    #[test]
    fn unhedged_position_has_negative_worst_world() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let worlds = enumerate_worlds(&spec, 4096).unwrap();
        let sol = LpSolution {
            fills: vec![LegFill {
                token: TokenId(10),
                action: Action::Buy,
                ts: TS,
                limit_px: px(46),
                qty: Qty(100_000_000),
                cash: Usdc(-46_000_000),
            }],
            splits: vec![],
        };
        let (worst, _) = exact_worst_net(&spec, &worlds, &sol, 0).unwrap();
        assert_eq!(worst, Usdc(-46_000_000)); // YES loses world
    }
}
```

- [ ] **Step 2: Run, verify compile failure**

```bash
cargo test -p pm-engine lp
```

- [ ] **Step 3: Implement**

Insert above the tests module:

```rust
#[derive(Clone, Debug)]
pub struct ComponentSpec<'a> {
    pub markets: Vec<Market>,
    pub partitions: Vec<Partition>,
    pub relationships: Vec<Relationship>,
    pub books: &'a HashMap<TokenId, Book>,
}

/// One resolution world: truth value of every market's YES, indexed in
/// `ComponentSpec::markets` order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct World {
    yes_true: Vec<bool>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LpSolution {
    pub fills: Vec<LegFill>,
    pub splits: Vec<(MarketId, Qty)>,
}

fn market_index(spec: &ComponentSpec, id: MarketId) -> Option<usize> {
    spec.markets.iter().position(|m| m.id == id)
}

/// Does `token` pay $1/share in world `w`?
pub fn token_pays(spec: &ComponentSpec, w: &World, token: TokenId) -> Option<bool> {
    let (i, is_yes) = spec.markets.iter().enumerate().find_map(|(i, m)| {
        if m.yes == token {
            Some((i, true))
        } else if m.no == token {
            Some((i, false))
        } else {
            None
        }
    })?;
    Some(w.yes_true[i] == is_yes)
}

fn consistent(spec: &ComponentSpec, yes_true: &[bool]) -> bool {
    spec.relationships.iter().all(|r| match *r {
        Relationship::Implies { a, b } => {
            match (market_index(spec, a), market_index(spec, b)) {
                (Some(a), Some(b)) => !yes_true[a] || yes_true[b],
                _ => true,
            }
        }
        Relationship::MutuallyExclusive { a, b } => {
            match (market_index(spec, a), market_index(spec, b)) {
                (Some(a), Some(b)) => !(yes_true[a] && yes_true[b]),
                _ => true,
            }
        }
        Relationship::Equivalent { a, b } => {
            match (market_index(spec, a), market_index(spec, b)) {
                (Some(a), Some(b)) => yes_true[a] == yes_true[b],
                _ => true,
            }
        }
    })
}

/// Enumerate relationship-consistent worlds. None ⇒ pre-prune product
/// exceeds `max_worlds` (caller skips the component).
pub fn enumerate_worlds(spec: &ComponentSpec, max_worlds: usize) -> Option<Vec<World>> {
    let m = spec.markets.len();
    // Which markets belong to a partition (index into partitions) vs free.
    let mut owner: Vec<Option<usize>> = vec![None; m];
    for (pi, p) in spec.partitions.iter().enumerate() {
        for mid in &p.markets {
            if let Some(i) = market_index(spec, *mid) {
                owner[i] = Some(pi);
            }
        }
    }
    let free: Vec<usize> = (0..m).filter(|&i| owner[i].is_none()).collect();

    // Pre-prune world count.
    let mut count: u128 = 1;
    for p in &spec.partitions {
        count = count.saturating_mul(p.markets.len() as u128);
    }
    count = count.saturating_mul(1u128 << free.len().min(127));
    if count > max_worlds as u128 {
        return None;
    }

    // Cartesian product: choice of winner per partition × bools for free.
    let mut worlds = Vec::new();
    let part_sizes: Vec<usize> = spec.partitions.iter().map(|p| p.markets.len()).collect();
    let mut choice = vec![0usize; part_sizes.len()];
    loop {
        // For this partition choice, enumerate free-market bools.
        for mask in 0u64..(1u64 << free.len()) {
            let mut yes_true = vec![false; m];
            for (pi, &c) in choice.iter().enumerate() {
                if let Some(i) = market_index(spec, spec.partitions[pi].markets[c]) {
                    yes_true[i] = true;
                }
            }
            for (bit, &i) in free.iter().enumerate() {
                yes_true[i] = mask & (1 << bit) != 0;
            }
            if consistent(spec, &yes_true) {
                worlds.push(World { yes_true });
            }
        }
        // Odometer over partition choices.
        let mut k = 0;
        loop {
            if k == choice.len() {
                return Some(worlds);
            }
            choice[k] += 1;
            if choice[k] < part_sizes[k] {
                break;
            }
            choice[k] = 0;
            k += 1;
        }
    }
}

/// Exact integer profit of `sol` in its worst world, plus cash basis.
/// `gas_micro` is charged flat. None on bookkeeping violations (selling more
/// than held, unknown token).
pub fn exact_worst_net(
    spec: &ComponentSpec,
    worlds: &[World],
    sol: &LpSolution,
    gas_micro: u64,
) -> Option<(Usdc, Usdc)> {
    // Net position per token and cash, exact.
    let mut pos: HashMap<TokenId, i128> = HashMap::new();
    let mut cash: i128 = 0;
    let mut basis: i128 = 0;
    for f in &sol.fills {
        let pm = f.limit_px.microusdc(f.ts);
        // Recompute cash bounds from scratch (don't trust `f.cash`):
        let m = spec.markets.iter().find(|m| m.yes == f.token || m.no == f.token)?;
        let fee = fee_microusdc(m.fee_bps, pm, f.qty).0;
        match f.action {
            Action::Buy => {
                let out = buy_cost(pm, f.qty).0 + fee;
                cash -= out;
                basis += out;
                *pos.entry(f.token).or_default() += i128::from(f.qty.0);
            }
            Action::Sell => {
                cash += sell_proceeds(pm, f.qty).0 - fee;
                *pos.entry(f.token).or_default() -= i128::from(f.qty.0);
            }
        }
    }
    for &(mid, q) in &sol.splits {
        let i = market_index(spec, mid)?;
        let m = spec.markets[i];
        let coll = (i128::from(q.0) * 1_000_000 + 999_999) / 1_000_000; // $1/share, ceil
        cash -= coll;
        basis += coll;
        *pos.entry(m.yes).or_default() += i128::from(q.0);
        *pos.entry(m.no).or_default() += i128::from(q.0);
    }
    if pos.values().any(|&p| p < 0) {
        return None; // naked short — not expressible on the venue
    }
    let mut worst: Option<i128> = None;
    for w in worlds {
        let mut payoff: i128 = 0;
        for (&t, &p) in &pos {
            if p > 0 && token_pays(spec, w, t)? {
                payoff += p; // $1/share = 1 µUSDC per micro-share
            }
        }
        let profit = cash + payoff - i128::from(gas_micro);
        worst = Some(worst.map_or(profit, |x: i128| x.min(profit)));
    }
    Some((Usdc(worst?), Usdc(basis)))
}
```

- [ ] **Step 4: Run tests, verify pass, commit**

```bash
cargo test -p pm-engine
git add -A && git commit -m "feat(engine): LP worlds, relationship pruning, exact reval"
```

---

### Task 12: `pm-engine::lp` part B — HiGHS solve

**Files:**
- Modify: `crates/engine/src/lp.rs`, `crates/engine/Cargo.toml` (add `highs.workspace = true`)

Variables: buy per (token, ask level), sell per (token, bid level), split per market, and free `t`. Maximize `t` s.t. every world's profit ≥ `t`, holdings per token ≥ 0, budget ≤ `max_basis`. Split columns have zero coefficient in world rows (a split pays out exactly its $1 cost in every world) — they exist to fund sells via the holdings rows. Solutions are floored to micro-shares and re-validated exactly; the floor is 30 bps for relationship-free components, 100 bps otherwise (spec §10).

- [ ] **Step 1: Write the failing tests** (append to the tests module in `lp.rs`):

```rust
    // ---- part B: solver ----

    fn solver_params() -> EngineParams {
        EngineParams {
            gas: crate::GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[test]
    fn lp_recovers_class1_long() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 46, 44, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let LpResult::Found(op) = solve_component(&spec, &solver_params()) else {
            panic!("expected Found");
        };
        assert_eq!(op.net, Usdc(2_000_000));
        assert_eq!(op.basis, Usdc(98_000_000));
        assert!(op.splits.is_empty());
        assert_eq!(op.fills.len(), 2);
        assert!(op.fills.iter().all(|f| f.action == Action::Buy));
    }

    #[test]
    fn lp_recovers_class1_short_via_split() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 57, 55, 100_000_000);
        quote(&mut books, TokenId(11), 52, 50, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        let LpResult::Found(op) = solve_component(&spec, &solver_params()) else {
            panic!("expected Found");
        };
        assert_eq!(op.net, Usdc(5_000_000));
        assert_eq!(op.splits, vec![(MarketId(0), Qty(100_000_000))]);
        assert!(op.fills.iter().all(|f| f.action == Action::Sell));
    }

    #[test]
    fn lp_recovers_class2_long() {
        let mut books = HashMap::new();
        // YES asks 0.30/0.30/0.35; NOs fair-rich so they don't dominate.
        for (i, ya) in [(0u32, 30u16), (1, 30), (2, 35)] {
            quote(&mut books, TokenId(u64::from(i) * 2 + 10), ya, ya.saturating_sub(2), 100_000_000);
            quote(&mut books, TokenId(u64::from(i) * 2 + 11), 100 - ya + 2, 100 - ya, 100_000_000);
        }
        let part = Partition {
            event: EventId(0),
            markets: vec![MarketId(0), MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(12), TokenId(14)],
            no_tokens: vec![TokenId(11), TokenId(13), TokenId(15)],
            verified_exhaustive: true,
        };
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1), mk(2)],
            partitions: vec![part],
            relationships: vec![],
            books: &books,
        };
        let LpResult::Found(op) = solve_component(&spec, &solver_params()) else {
            panic!("expected Found");
        };
        assert_eq!(op.net, Usdc(5_000_000)); // 1 − 0.95 per unit × 100
    }

    #[test]
    fn lp_recovers_class3_implies_via_pruning() {
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 68, 64, 100_000_000); // YES_a
        quote(&mut books, TokenId(11), 35, 33, 100_000_000); // NO_a ask 0.35
        quote(&mut books, TokenId(12), 55, 53, 100_000_000); // YES_b ask 0.55
        quote(&mut books, TokenId(13), 48, 44, 100_000_000); // NO_b
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::Implies { a: MarketId(0), b: MarketId(1) }],
            books: &books,
        };
        let p = EngineParams { floor_c3: Bps(100), ..solver_params() };
        let LpResult::Found(op) = solve_component(&spec, &p) else {
            panic!("expected Found");
        };
        // NO_a + YES_b = 0.90 → ≥ $10 on 100sh (LP may find more; never less).
        assert!(op.net >= Usdc(10_000_000), "net was {:?}", op.net);
        // Without the relationship the same books are no-arb:
        let spec_free = ComponentSpec { relationships: vec![], ..spec.clone() };
        assert!(matches!(solve_component(&spec_free, &solver_params()), LpResult::NoEdge));
    }

    #[test]
    fn floor_rule_uses_c3_floor_when_relationships_present() {
        // NO_a 0.49 + YES_b 0.50 = 0.99 → ≈101 bps. With a relationship in
        // the component, the class-3 floor applies: raise it to 150 and the
        // same edge must be rejected; at 100 it must pass.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 68, 64, 100_000_000);
        quote(&mut books, TokenId(11), 49, 45, 100_000_000); // NO_a 0.49
        quote(&mut books, TokenId(12), 50, 48, 100_000_000); // YES_b 0.50
        quote(&mut books, TokenId(13), 52, 47, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0), mk(1)],
            partitions: vec![],
            relationships: vec![Relationship::Implies { a: MarketId(0), b: MarketId(1) }],
            books: &books,
        };
        let p = EngineParams { floor_c3: Bps(150), ..solver_params() };
        assert!(matches!(solve_component(&spec, &p), LpResult::NoEdge));
        let p = EngineParams { floor_c3: Bps(100), ..solver_params() };
        assert!(matches!(solve_component(&spec, &p), LpResult::Found(_)));
    }

    #[test]
    fn solver_tolerance_cannot_fake_an_edge() {
        // Perfectly fair books: asks sum to exactly 1 everywhere. Any t* the
        // solver reports is numerical noise; exact reval must kill it.
        let mut books = HashMap::new();
        quote(&mut books, TokenId(10), 50, 48, 100_000_000);
        quote(&mut books, TokenId(11), 50, 48, 100_000_000);
        let spec = ComponentSpec {
            markets: vec![mk(0)],
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        assert!(matches!(solve_component(&spec, &solver_params()), LpResult::NoEdge));
    }

    #[test]
    fn too_many_worlds_skips() {
        let books = HashMap::new();
        let spec = ComponentSpec {
            markets: (0..13).map(mk).collect(), // 8192 > 4096
            partitions: vec![],
            relationships: vec![],
            books: &books,
        };
        assert!(matches!(
            solve_component(&spec, &solver_params()),
            LpResult::Skipped(SkipReason::TooManyWorlds)
        ));
    }
```

- [ ] **Step 2: Run, verify compile failure**

```bash
cargo test -p pm-engine lp
```

Expected: `LpResult`, `SkipReason`, `solve_component` not found. Add `highs.workspace = true` to `crates/engine/Cargo.toml` dependencies now — first build will compile vendored HiGHS via cmake (slow once, then cached).

- [ ] **Step 3: Implement**

Append to `lp.rs` (above tests):

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SkipReason {
    TooManyWorlds,
    SolverFailed,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LpResult {
    Found(Opportunity),
    NoEdge,
    Skipped(SkipReason),
}

enum VarKind {
    Buy { token: TokenId, px: pm_core::num::Px, ts: pm_core::num::TickSize, fee: pm_core::num::Bps, depth_micro: u64 },
    Sell { token: TokenId, px: pm_core::num::Px, ts: pm_core::num::TickSize, fee: pm_core::num::Bps, depth_micro: u64 },
    Split { market: MarketId },
}

/// Solve one component. Gas-less objective; exact reval applies gas and floors.
pub fn solve_component(spec: &ComponentSpec, p: &EngineParams) -> LpResult {
    use highs::{HighsModelStatus, RowProblem, Sense};

    let Some(worlds) = enumerate_worlds(spec, p.max_worlds) else {
        return LpResult::Skipped(SkipReason::TooManyWorlds);
    };
    if worlds.is_empty() {
        return LpResult::NoEdge;
    }

    let mut pb = RowProblem::default();
    let t = pb.add_column(1.0, f64::NEG_INFINITY..f64::INFINITY);

    // Collect variables.
    let mut vars: Vec<(VarKind, highs::Col)> = Vec::new();
    for m in &spec.markets {
        for token in [m.yes, m.no] {
            let Some(book) = spec.books.get(&token) else { continue };
            for (px, q) in book.asks.iter_from_best() {
                let col = pb.add_column(0.0, 0.0..(q.0 as f64 / 1e6));
                vars.push((VarKind::Buy { token, px, ts: book.ts(), fee: m.fee_bps, depth_micro: q.0 }, col));
            }
            for (px, q) in book.bids.iter_from_best() {
                let col = pb.add_column(0.0, 0.0..(q.0 as f64 / 1e6));
                vars.push((VarKind::Sell { token, px, ts: book.ts(), fee: m.fee_bps, depth_micro: q.0 }, col));
            }
        }
        let col = pb.add_column(0.0, 0.0..(p.max_basis.0 as f64 / 1e6));
        vars.push((VarKind::Split { market: m.id }, col));
    }

    let unit_cash = |k: &VarKind| -> f64 {
        // Cash flow per SHARE (dollars): negative = outflow.
        match *k {
            VarKind::Buy { px, ts, fee, .. } => {
                let pm = px.microusdc(ts);
                let fee_d = f64::from(fee.0.max(0)) * pm.min(1_000_000 - pm) as f64 / 1e10;
                -(pm as f64 / 1e6 + fee_d)
            }
            VarKind::Sell { px, ts, fee, .. } => {
                let pm = px.microusdc(ts);
                let fee_d = f64::from(fee.0.max(0)) * pm.min(1_000_000 - pm) as f64 / 1e10;
                pm as f64 / 1e6 - fee_d
            }
            VarKind::Split { .. } => -1.0,
        }
    };

    // World rows: cash + payoff − t ≥ 0.
    for w in &worlds {
        let mut row: Vec<(highs::Col, f64)> = vec![(t, -1.0)];
        for (k, col) in &vars {
            let mut coef = unit_cash(k);
            match *k {
                VarKind::Buy { token, .. } => {
                    if token_pays(spec, w, token) == Some(true) {
                        coef += 1.0;
                    }
                }
                VarKind::Sell { token, .. } => {
                    if token_pays(spec, w, token) == Some(true) {
                        coef -= 1.0;
                    }
                }
                VarKind::Split { .. } => coef += 1.0, // YES+NO pays $1 always
            }
            if coef != 0.0 {
                row.push((*col, coef));
            }
        }
        pb.add_row(0.0..f64::INFINITY, row);
    }

    // Holdings per token: buys + splits − sells ≥ 0.
    let mut tokens: Vec<TokenId> = Vec::new();
    for m in &spec.markets {
        tokens.push(m.yes);
        tokens.push(m.no);
    }
    for tok in tokens {
        let mut row: Vec<(highs::Col, f64)> = Vec::new();
        for (k, col) in &vars {
            match *k {
                VarKind::Buy { token, .. } if token == tok => row.push((*col, 1.0)),
                VarKind::Sell { token, .. } if token == tok => row.push((*col, -1.0)),
                VarKind::Split { market } => {
                    let m = spec.markets.iter().find(|m| m.id == market);
                    if m.is_some_and(|m| m.yes == tok || m.no == tok) {
                        row.push((*col, 1.0));
                    }
                }
                _ => {}
            }
        }
        if !row.is_empty() {
            pb.add_row(0.0..f64::INFINITY, row);
        }
    }

    // Budget: Σ outflows ≤ max_basis.
    let mut row: Vec<(highs::Col, f64)> = Vec::new();
    for (k, col) in &vars {
        let c = unit_cash(k);
        if c < 0.0 {
            row.push((*col, -c));
        }
    }
    if !row.is_empty() {
        pb.add_row(0.0..(p.max_basis.0 as f64 / 1e6), row);
    }

    let solved = pb.optimise(Sense::Maximise).solve();
    if solved.status() != HighsModelStatus::Optimal {
        return LpResult::Skipped(SkipReason::SolverFailed);
    }
    let sol = solved.get_solution();
    let cols = sol.columns();

    // Extract (skip col 0 = t), floor to micro-shares, clamp by depth.
    let mut fills: Vec<LegFill> = Vec::new();
    let mut splits: Vec<(MarketId, Qty)> = Vec::new();
    for (i, (k, _)) in vars.iter().enumerate() {
        let shares = cols[i + 1].max(0.0);
        let micro = (shares * 1e6).floor() as u64;
        if micro == 0 {
            continue;
        }
        match *k {
            VarKind::Buy { token, px, ts, fee, depth_micro } => {
                let q = Qty(micro.min(depth_micro));
                let pm = px.microusdc(ts);
                let cash = Usdc(-(buy_cost(pm, q).0 + fee_microusdc(fee, pm, q).0));
                fills.push(LegFill { token, action: Action::Buy, ts, limit_px: px, qty: q, cash });
            }
            VarKind::Sell { token, px, ts, fee, depth_micro } => {
                let q = Qty(micro.min(depth_micro));
                let pm = px.microusdc(ts);
                let cash = Usdc(sell_proceeds(pm, q).0 - fee_microusdc(fee, pm, q).0);
                fills.push(LegFill { token, action: Action::Sell, ts, limit_px: px, qty: q, cash });
            }
            VarKind::Split { market } => splits.push((market, Qty(micro))),
        }
    }
    if fills.is_empty() && splits.is_empty() {
        return LpResult::NoEdge;
    }

    // Flooring buys/splits below sells could create phantom shorts; clamp
    // sells to holdings per token before reval.
    let mut holdings: HashMap<TokenId, u64> = HashMap::new();
    for f in &fills {
        if f.action == Action::Buy {
            *holdings.entry(f.token).or_default() += f.qty.0;
        }
    }
    for &(mid, q) in &splits {
        if let Some(i) = market_index(spec, mid) {
            *holdings.entry(spec.markets[i].yes).or_default() += q.0;
            *holdings.entry(spec.markets[i].no).or_default() += q.0;
        }
    }
    for f in &mut fills {
        if f.action == Action::Sell {
            let h = holdings.entry(f.token).or_default();
            let q = f.qty.0.min(*h);
            *h -= q;
            let pm = f.limit_px.microusdc(f.ts);
            let m = spec.markets.iter().find(|m| m.yes == f.token || m.no == f.token);
            let fee = m.map_or(Usdc(0), |m| fee_microusdc(m.fee_bps, pm, Qty(q)));
            f.qty = Qty(q);
            f.cash = Usdc(sell_proceeds(pm, Qty(q)).0 - fee.0);
        }
    }
    fills.retain(|f| f.qty.0 > 0);

    let gas = if splits.is_empty() { p.gas.redeem } else { p.gas.split + p.gas.redeem };
    let lp_sol = LpSolution { fills, splits };
    let Some((worst, basis)) = exact_worst_net(spec, &worlds, &lp_sol, gas) else {
        return LpResult::Skipped(SkipReason::SolverFailed);
    };
    if worst < p.min_profit {
        return LpResult::NoEdge;
    }
    let floor = if spec.relationships.is_empty() { p.floor_c12 } else { p.floor_c3 };
    let Some(edge) = edge_bps(worst, basis) else {
        return LpResult::NoEdge;
    };
    if edge < floor {
        return LpResult::NoEdge;
    }
    LpResult::Found(Opportunity {
        class: crate::ArbClass::C4Lp,
        fills: lp_sol.fills,
        units: Qty(0), // heterogeneous basket; per-leg qtys are authoritative
        net: worst,
        basis,
        edge,
        splits: lp_sol.splits,
    })
}
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cargo test -p pm-engine
```

Expected: all LP tests pass. First run compiles vendored HiGHS (minutes). If `highs` 1.x API names differ (e.g. `optimise` vs `optimize`, solution accessors), consult `cargo doc -p highs --open` and adapt the call sites only — keep the model structure identical.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(engine): HiGHS LP detector with exact integer re-validation"
```

---

### Task 13: `pm-engine::dedup`

**Files:**
- Create: `crates/engine/src/dedup.rs`
- Modify: `crates/engine/src/lib.rs` (add `pub mod dedup;`)

Spec §11: fingerprint = class + sorted (token, action, limit px); size excluded. Cooldown suppresses re-emits inside the window unless net improves by more than the configured percentage. `now` is always passed in — no hidden clock, fully testable.

- [ ] **Step 1: Write the failing tests**

`crates/engine/src/dedup.rs`:

```rust
//! Opportunity dedup: price-shape fingerprint + cooldown window (spec §11).

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use crate::Opportunity;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{Action, ArbClass, LegFill};
    use pm_core::instrument::TokenId;
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

    fn op(net: i128, px: u16, qty: u64) -> Opportunity {
        Opportunity {
            class: ArbClass::C1Long,
            fills: vec![LegFill {
                token: TokenId(10),
                action: Action::Buy,
                ts: TickSize::Cent,
                limit_px: Px::new(px, TickSize::Cent).unwrap(),
                qty: Qty(qty),
                cash: Usdc(-1),
            }],
            units: Qty(qty),
            net: Usdc(net),
            basis: Usdc(net * 50),
            edge: Bps(200),
            splits: vec![],
        }
    }

    #[test]
    fn fingerprint_ignores_size_but_not_price_or_class() {
        let a = op(1_000_000, 46, 100);
        let b = op(2_000_000, 46, 999_999); // same shape, bigger
        let c = op(1_000_000, 47, 100); // different price
        let mut d = op(1_000_000, 46, 100);
        d.class = ArbClass::C1Short;
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert_ne!(a.fingerprint(), c.fingerprint());
        assert_ne!(a.fingerprint(), d.fingerprint());
    }

    #[test]
    fn fingerprint_is_leg_order_invariant() {
        let mut a = op(1_000_000, 46, 100);
        a.fills.push(LegFill {
            token: TokenId(11),
            action: Action::Buy,
            ts: TickSize::Cent,
            limit_px: Px::new(52, TickSize::Cent).unwrap(),
            qty: Qty(100),
            cash: Usdc(-1),
        });
        let mut b = a.clone();
        b.fills.reverse();
        assert_eq!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn cooldown_suppresses_then_readmits() {
        let mut cd = Cooldown::new(Duration::from_millis(2000), 20);
        let t0 = Instant::now();
        assert!(cd.admit(t0, &op(1_000_000, 46, 100)));
        assert!(!cd.admit(t0 + Duration::from_millis(500), &op(1_000_000, 46, 100)));
        assert!(cd.admit(t0 + Duration::from_millis(2500), &op(1_000_000, 46, 100)));
    }

    #[test]
    fn improvement_beats_the_window() {
        let mut cd = Cooldown::new(Duration::from_millis(2000), 20);
        let t0 = Instant::now();
        assert!(cd.admit(t0, &op(1_000_000, 46, 100)));
        // +10% — not enough.
        assert!(!cd.admit(t0 + Duration::from_millis(100), &op(1_100_000, 46, 150)));
        // +25% — re-emit.
        assert!(cd.admit(t0 + Duration::from_millis(200), &op(1_250_000, 46, 200)));
        // and the recorded bar moved: +10% over 1.25M now fails.
        assert!(!cd.admit(t0 + Duration::from_millis(300), &op(1_300_000, 46, 210)));
    }

    #[test]
    fn purge_keeps_the_map_bounded() {
        let mut cd = Cooldown::new(Duration::from_millis(1), 20);
        let t0 = Instant::now();
        for i in 0..2000u16 {
            let _ = cd.admit(t0, &op(1_000_000, 1 + (i % 98), u64::from(i) + 1));
        }
        // Everything is expired by now; the next admit triggers a purge.
        assert!(cd.admit(t0 + Duration::from_millis(10), &op(1_000_000, 46, 100)));
        assert!(cd.tracked() < 2000);
    }
}
```

- [ ] **Step 2: Run, verify compile failure**

```bash
cargo test -p pm-engine dedup
```

- [ ] **Step 3: Implement**

Insert above the tests module:

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Fingerprint(u64);

impl Opportunity {
    /// Price-shape identity: class + sorted (token, action, limit px).
    /// Size deliberately excluded (spec §11).
    pub fn fingerprint(&self) -> Fingerprint {
        let mut legs: Vec<(u64, bool, u16)> = self
            .fills
            .iter()
            .map(|f| (f.token.0, matches!(f.action, crate::Action::Buy), f.limit_px.get()))
            .collect();
        legs.sort_unstable();
        let mut h = DefaultHasher::new();
        self.class.hash(&mut h);
        legs.hash(&mut h);
        Fingerprint(h.finish())
    }
}

pub struct Cooldown {
    window: Duration,
    improvement_pct: u32,
    seen: HashMap<Fingerprint, (Instant, i128)>,
}

const PURGE_THRESHOLD: usize = 1024;

impl Cooldown {
    pub fn new(window: Duration, improvement_pct: u32) -> Self {
        Cooldown { window, improvement_pct, seen: HashMap::new() }
    }

    pub fn tracked(&self) -> usize {
        self.seen.len()
    }

    /// True ⇒ emit the opportunity (and start/refresh its cooldown).
    pub fn admit(&mut self, now: Instant, op: &Opportunity) -> bool {
        if self.seen.len() > PURGE_THRESHOLD {
            let window = self.window;
            self.seen.retain(|_, (t, _)| now.duration_since(*t) < window);
        }
        let fp = op.fingerprint();
        let admit = match self.seen.get(&fp) {
            Some(&(t0, net0)) if now.duration_since(t0) < self.window => {
                let bar = net0 + net0 * i128::from(self.improvement_pct) / 100;
                op.net.0 > bar
            }
            _ => true,
        };
        if admit {
            self.seen.insert(fp, (now, op.net.0));
        }
        admit
    }
}
```

- [ ] **Step 4: Run tests, verify pass, commit**

```bash
cargo test -p pm-engine
git add -A && git commit -m "feat(engine): fingerprint dedup with cooldown and improvement re-emit"
```

---

### Task 14: `pm-config` skeleton

**Files:**
- Create: `crates/config/Cargo.toml`, `crates/config/src/lib.rs`
- Modify: root `Cargo.toml` (`members = ["crates/core", "crates/engine", "crates/config"]`)

Spec §18 skeleton: typed sections, defaults = the locked §2 values, `deny_unknown_fields`, checked dollar→µUSDC conversion. Consumed by the runtime in M3 — nothing depends on it yet.

- [ ] **Step 1: Add crate**

`crates/config/Cargo.toml`:
```toml
[package]
name = "pm-config"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dependencies]
serde.workspace = true
toml.workspace = true
```

- [ ] **Step 2: Write the failing tests + types**

`crates/config/src/lib.rs`:

```rust
//! Typed configuration skeleton (spec §18). Defaults are the spec §2 locked
//! values. Secrets never live here — env vars only (M3+).

use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub capital: Capital,
    pub edges: Edges,
    pub gas: Gas,
    pub lp: Lp,
    pub dedup: Dedup,
    pub mode: Mode,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Capital {
    pub bankroll_usd: f64,
    pub per_market_usd: f64,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Edges {
    pub min_edge_class12_bps: i32,
    pub min_edge_class3_bps: i32,
    pub min_profit_usd: f64,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Gas {
    pub split_microusdc: u64,
    pub merge_microusdc: u64,
    pub redeem_microusdc: u64,
    pub negrisk_convert_microusdc: u64,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Lp {
    pub max_worlds: usize,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Dedup {
    pub cooldown_ms: u64,
    pub reemit_improvement_pct: u32,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Mode {
    pub paper: bool,
}

#[derive(Debug, PartialEq)]
pub enum ConfigError {
    Parse(String),
    BadMoney(&'static str),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn defaults_are_the_locked_values() {
        let c = Config::default();
        assert_eq!(c.capital.bankroll_usd, 10_000.0);
        assert_eq!(c.capital.per_market_usd, 1_000.0);
        assert_eq!(c.edges.min_edge_class12_bps, 30);
        assert_eq!(c.edges.min_edge_class3_bps, 100);
        assert_eq!(c.edges.min_profit_usd, 1.0);
        assert_eq!(c.lp.max_worlds, 4096);
        assert_eq!(c.dedup.cooldown_ms, 2000);
        assert_eq!(c.dedup.reemit_improvement_pct, 20);
        assert!(c.mode.paper);
    }

    #[test]
    fn empty_toml_is_all_defaults() {
        assert_eq!(Config::from_toml_str("").unwrap(), Config::default());
    }

    #[test]
    fn partial_override_parses() {
        let c = Config::from_toml_str("[capital]\nbankroll_usd = 500.0\n").unwrap();
        assert_eq!(c.capital.bankroll_usd, 500.0);
        assert_eq!(c.capital.per_market_usd, 1_000.0);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(Config::from_toml_str("[capital]\nbankrol = 1.0\n").is_err());
        assert!(Config::from_toml_str("[typo_section]\nx = 1\n").is_err());
    }

    #[test]
    fn money_conversion_is_checked() {
        assert_eq!(usd_to_microusdc(10_000.0).unwrap(), 10_000_000_000);
        assert_eq!(usd_to_microusdc(0.000001).unwrap(), 1);
        assert!(usd_to_microusdc(-1.0).is_err());
        assert!(usd_to_microusdc(f64::NAN).is_err());
        assert!(usd_to_microusdc(f64::INFINITY).is_err());
    }
}
```

- [ ] **Step 3: Run (compile failure), then implement**

Add above the tests module:

```rust
impl Default for Capital {
    fn default() -> Self {
        Capital { bankroll_usd: 10_000.0, per_market_usd: 1_000.0 }
    }
}
impl Default for Edges {
    fn default() -> Self {
        Edges { min_edge_class12_bps: 30, min_edge_class3_bps: 100, min_profit_usd: 1.0 }
    }
}
impl Default for Gas {
    fn default() -> Self {
        Gas {
            split_microusdc: 10_000,
            merge_microusdc: 10_000,
            redeem_microusdc: 15_000,
            negrisk_convert_microusdc: 20_000,
        }
    }
}
impl Default for Lp {
    fn default() -> Self {
        Lp { max_worlds: 4096 }
    }
}
impl Default for Dedup {
    fn default() -> Self {
        Dedup { cooldown_ms: 2000, reemit_improvement_pct: 20 }
    }
}
impl Default for Mode {
    fn default() -> Self {
        Mode { paper: true }
    }
}
impl Default for Config {
    fn default() -> Self {
        Config {
            capital: Capital::default(),
            edges: Edges::default(),
            gas: Gas::default(),
            lp: Lp::default(),
            dedup: Dedup::default(),
            mode: Mode::default(),
        }
    }
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))
    }
}

/// Checked dollars → µUSDC (round-to-nearest). Rejects NaN/∞/negative/overflow.
pub fn usd_to_microusdc(usd: f64) -> Result<i128, ConfigError> {
    if !usd.is_finite() || usd < 0.0 || usd > 1e18 {
        return Err(ConfigError::BadMoney("must be finite, non-negative, sane"));
    }
    Ok((usd * 1e6).round() as i128)
}
```

- [ ] **Step 4: Run tests, verify pass, commit**

```bash
cargo test -p pm-config
git add -A && git commit -m "feat(config): typed config skeleton with spec defaults"
```

---

### Task 15: Criterion benches, gates, README, tag

**Files:**
- Create: `crates/engine/benches/hot_path.rs`, `README.md`
- Modify: `crates/engine/Cargo.toml`

- [ ] **Step 1: Register the bench**

Append to `crates/engine/Cargo.toml`:

```toml
[dev-dependencies]
proptest.workspace = true
criterion.workspace = true

[[bench]]
name = "hot_path"
harness = false
```

(keep the existing proptest line; just add criterion.)

- [ ] **Step 2: Write the bench suite**

`crates/engine/benches/hot_path.rs`:

```rust
//! Hot-path benchmarks vs spec §20 gates.
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use pm_core::book::{Book, Ladder, Side};
use pm_core::instrument::{EventId, Market, MarketId, Partition, Relationship, TokenId};
use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};
use pm_engine::{class1, class2, lp, EngineParams, GasTable};

const TS: TickSize = TickSize::Cent;

fn px(t: u16) -> Px {
    Px::new(t, TS).unwrap()
}

fn params() -> EngineParams {
    EngineParams {
        gas: GasTable { split: 0, merge: 0, redeem: 0, negrisk_convert: 0 },
        min_profit: Usdc(0),
        ..EngineParams::default()
    }
}

fn mk(i: u32) -> Market {
    Market {
        id: MarketId(i),
        yes: TokenId(u64::from(i) * 2 + 10),
        no: TokenId(u64::from(i) * 2 + 11),
        tick: TS,
        fee_bps: Bps(0),
        neg_risk: false,
    }
}

fn arb_books() -> (Book, Book) {
    let mut yes = Book::new(TS);
    let mut no = Book::new(TS);
    for (i, q) in [(46u16, 50_000_000u64), (48, 30_000_000), (51, 20_000_000)] {
        yes.apply(Side::Ask, px(i), Qty(q));
    }
    for (i, q) in [(52u16, 50_000_000u64), (53, 30_000_000), (55, 20_000_000)] {
        no.apply(Side::Ask, px(i), Qty(q));
    }
    yes.apply(Side::Bid, px(44), Qty(50_000_000));
    no.apply(Side::Bid, px(50), Qty(50_000_000));
    (yes, no)
}

fn bench_ladder_apply(c: &mut Criterion) {
    // Gate: ≤ 1 µs p99 (spec §20). Pre-generated delta cycle.
    let deltas: Vec<(Px, Qty)> = (0..1024)
        .map(|i| (px(1 + (i * 7) % 98), Qty(u64::from((i * 13) % 5) * 1_000_000)))
        .collect();
    c.bench_function("ladder_apply_delta", |b| {
        b.iter_batched_ref(
            || Ladder::new(TS, Side::Ask),
            |l| {
                for &(p, q) in &deltas {
                    l.set(black_box(p), black_box(q));
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_class1_detect(c: &mut Criterion) {
    // Gate: ≤ 20 µs p99 post-apply (spec §20).
    let (yes, no) = arb_books();
    let m = mk(0);
    let p = params();
    c.bench_function("class1_detect", |b| {
        b.iter(|| black_box(class1::detect(black_box(&m), black_box(&yes), black_box(&no), &p)))
    });
}

fn bench_class2_scan_n16(c: &mut Criterion) {
    // Gate: ≤ 50 µs p99 for n ≤ 16 (spec §20).
    let n = 16u32;
    let mut markets = Vec::new();
    let mut books = HashMap::new();
    let mut yes_tokens = Vec::new();
    let mut no_tokens = Vec::new();
    let mut market_ids = Vec::new();
    for i in 0..n {
        let m = mk(i);
        let mut yb = Book::new(TS);
        yb.apply(Side::Ask, px(5), Qty(100_000_000)); // 16×0.05 = 0.80 < 1 → arb
        let mut nb = Book::new(TS);
        nb.apply(Side::Ask, px(97), Qty(100_000_000));
        books.insert(m.yes, yb);
        books.insert(m.no, nb);
        yes_tokens.push(m.yes);
        no_tokens.push(m.no);
        market_ids.push(m.id);
        markets.push(m);
    }
    let part = Partition {
        event: EventId(0),
        markets: market_ids,
        yes_tokens,
        no_tokens,
        verified_exhaustive: true,
    };
    let p = params();
    c.bench_function("class2_scan_n16", |b| {
        b.iter(|| black_box(class2::detect(black_box(&part), &markets, &books, &p)))
    });
}

fn bench_lp_8_markets(c: &mut Criterion) {
    // Gate: ≤ 10 ms p99 (spec §20): 8 binaries, 2 relationships, planted arb.
    let markets: Vec<Market> = (0..8).map(mk).collect();
    let mut books = HashMap::new();
    for m in &markets {
        let (ya, na) = if m.id == MarketId(0) { (46, 52) } else { (50, 52) };
        let mut yb = Book::new(TS);
        yb.apply(Side::Ask, px(ya), Qty(50_000_000));
        yb.apply(Side::Bid, px(ya - 2), Qty(50_000_000));
        let mut nb = Book::new(TS);
        nb.apply(Side::Ask, px(na), Qty(50_000_000));
        nb.apply(Side::Bid, px(na - 2), Qty(50_000_000));
        books.insert(m.yes, yb);
        books.insert(m.no, nb);
    }
    let spec = lp::ComponentSpec {
        markets,
        partitions: vec![],
        relationships: vec![
            Relationship::Implies { a: MarketId(1), b: MarketId(2) },
            Relationship::MutuallyExclusive { a: MarketId(3), b: MarketId(4) },
        ],
        books: &books,
    };
    let p = params();
    c.bench_function("lp_solve_8_markets", |b| {
        b.iter(|| black_box(lp::solve_component(black_box(&spec), &p)))
    });
}

criterion_group!(
    benches,
    bench_ladder_apply,
    bench_class1_detect,
    bench_class2_scan_n16,
    bench_lp_8_markets
);
criterion_main!(benches);
```

Note `ladder_apply_delta` times 1024 sets per iteration — divide the reported time by 1024 when comparing to the 1 µs gate.

- [ ] **Step 3: Run benches and record**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo bench -p pm-engine 2>&1 | tee /tmp/m1-bench.txt
```

Compare against spec §20 gates (remember the ÷1024 for ladder_apply). All four must clear. If a gate fails, profile before optimizing; do not loosen gates.

- [ ] **Step 4: Write README.md with measured numbers**

```markdown
# Polymarket Arbitrage Bot

Rust workspace implementing depth-aware, fee-aware arbitrage detection for
Polymarket. M1 (math core + detection engine) is complete; ingestion,
execution, TUI follow in M2–M6. Design: `docs/superpowers/specs/2026-06-12-polymarket-arb-bot-v2-design.md`.

## Build & test

cargo is installed via rustup; if not on PATH: `export PATH="$HOME/.cargo/bin:$PATH"`.
The LP detector compiles vendored HiGHS — `cmake` must be installed.

    cargo test --workspace      # full suite
    cargo bench -p pm-engine    # criterion hot-path suite

## M1 measured baselines (this machine, <date>)

| Benchmark | Gate (p99) | Measured |
|---|---|---|
| ladder_apply_delta (per set) | ≤ 1 µs | <fill> |
| class1_detect | ≤ 20 µs | <fill> |
| class2_scan_n16 | ≤ 50 µs | <fill> |
| lp_solve_8_markets | ≤ 10 ms | <fill> |

Deployment note: co-location dominates language-level speed — measure RTT to
Polymarket endpoints from candidate regions before choosing a host (full
guidance lands with M6).
```

Replace every `<fill>`/`<date>` with the real measured numbers from Step 3 — leaving them is a task failure.

- [ ] **Step 5: Full verification**

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

Expected: fmt makes no diff (or commit it), zero clippy warnings, all tests green (≈ 45+ across the workspace).

- [ ] **Step 6: Commit and tag**

```bash
git add -A && git commit -m "feat(engine): criterion hot-path suite, baselines, README"
git tag m1-engine
git log --oneline
```

---

## M1 completion criteria (spec §22)

- [ ] All 15 tasks committed on `feat/m1-engine`; working tree clean.
- [ ] `cargo test --workspace` green; includes walker-vs-brute proptest, ladder reference-model proptest, LP recovering classes 1–3, solver-tolerance rejection.
- [ ] `cargo clippy --all-targets -- -D warnings` clean; no `unwrap`/`expect` outside tests/benches.
- [ ] All four §20 bench gates met and recorded in README with real numbers.
- [ ] Tag `m1-engine` exists.
- [ ] Then: superpowers:finishing-a-development-branch (integration choice is the user's).

