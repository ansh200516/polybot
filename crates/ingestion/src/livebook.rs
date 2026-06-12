//! LiveBook wraps pm_core::Book with staleness, hash integrity, and off-tick
//! price counting (spec §5 deferred fields).

use pm_core::book::{Book, Side};
use pm_core::num::{Px, Qty, TickSize};
use std::time::{Duration, Instant};

/// A single level from a REST snapshot or WS frame, as raw micro-unit values.
pub struct RawLevel {
    pub price_micro: u64,
    pub size_micro: u64,
}

/// A single delta from a WS change message, as raw micro-unit values.
#[derive(Clone, Copy)]
pub struct RawChange {
    /// true = bid side, false = ask side.
    pub side_buy: bool,
    pub price_micro: u64,
    pub size_micro: u64,
}

/// Why a resnapshot is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResnapshotReason {
    CrossedBook,
    PersistentOffTick,
    /// Changes arrived for a token not yet known to the shard.
    UnknownToken,
    /// Connection loss / forced staleness — not an integrity failure.
    FeedLost,
}

/// Outcome of applying a snapshot or a set of changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Ok,
    NeedsResnapshot(ResnapshotReason),
}

/// Number of cumulative off-tick prices that triggers a resnapshot demand.
pub const OFF_TICK_RESNAPSHOT_THRESHOLD: u32 = 16;

/// Book + integrity + staleness (spec §5).
pub struct LiveBook {
    book: Book,
    ts: TickSize,
    last_update: Option<Instant>,
    hash: Option<Box<str>>,
    valid: bool,
    off_tick: u32,
    /// Reason the book was last invalidated, used to replay the pending demand
    /// idempotently when deltas arrive on an invalid book.
    invalid_reason: Option<ResnapshotReason>,
}

impl LiveBook {
    pub fn new(ts: TickSize) -> Self {
        Self {
            book: Book::new(ts),
            ts,
            last_update: None,
            hash: None,
            valid: false,
            off_tick: 0,
            invalid_reason: None,
        }
    }

    /// Read-only access to the inner book.
    pub fn book(&self) -> &Book {
        &self.book
    }

    /// Current venue hash (set by last apply call that provided one).
    pub fn hash(&self) -> Option<&str> {
        self.hash.as_deref()
    }

    /// Whether the book is valid (not crossed, not explicitly invalidated).
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Cumulative count of off-tick price events seen.
    pub fn off_tick_count(&self) -> u32 {
        self.off_tick
    }

    /// When last a successful apply_snapshot or apply_changes stamped the book.
    pub fn last_update(&self) -> Option<Instant> {
        self.last_update
    }

    /// True if the book is stale: invalid, never stamped, or stamp age ≥ window.
    ///
    /// NOTE (delta-only feed): per-book age staleness is meaningful only when the
    /// feed itself is alive and pushing deltas; the supervisor gates staleness at
    /// FEED level — a quiet book on a live connection is current. M3's detection
    /// gate must combine `valid` with feed liveness, not raw book age.
    pub fn is_stale(&self, now: Instant, window: Duration) -> bool {
        !self.valid
            || self
                .last_update
                .is_none_or(|t| now.duration_since(t) >= window)
    }

    /// Mark this book stale/invalid (used by the shard on WS reconnect).
    /// Clears `last_update` and sets valid = false so `is_stale` returns true.
    pub fn force_stale(&mut self) {
        self.valid = false;
        self.last_update = None;
        self.invalid_reason = Some(ResnapshotReason::FeedLost);
    }

    /// Convert a raw micro-USDC price to a valid `Px`, or None if off-tick.
    ///
    /// A price is on-tick iff:
    /// - it is strictly between 0 and 1_000_000 (exclusive),
    /// - it is exactly divisible by the tick's unit in µUSDC,
    /// - the resulting tick falls in the valid range for `Px::new`.
    fn price_to_px(&self, micro: u64) -> Option<Px> {
        let unit = self.ts.unit_microusdc();
        if micro == 0 || micro >= 1_000_000 || !micro.is_multiple_of(unit) {
            return None;
        }
        Px::new((micro / unit) as u16, self.ts).ok()
    }

    /// Replace the entire book from a snapshot.
    ///
    /// Off-tick levels are counted but skipped. After the build:
    /// - If the resulting book is crossed (best_bid ≥ best_ask both present),
    ///   the book is kept empty (invalid), and `NeedsResnapshot(CrossedBook)` is
    ///   returned — the venue sent garbage.
    /// - Otherwise: valid = true, last_update stamped, hash set, off_tick reset
    ///   to the count of off-tick levels in THIS snapshot.
    pub fn apply_snapshot(
        &mut self,
        now: Instant,
        bids: &[RawLevel],
        asks: &[RawLevel],
        hash: &str,
    ) -> ApplyOutcome {
        // Rebuild fresh.
        self.book = Book::new(self.ts);
        let mut off_tick_this = 0u32;

        for level in bids {
            if let Some(px) = self.price_to_px(level.price_micro) {
                self.book.apply(Side::Bid, px, Qty(level.size_micro));
            } else {
                off_tick_this += 1;
            }
        }
        for level in asks {
            if let Some(px) = self.price_to_px(level.price_micro) {
                self.book.apply(Side::Ask, px, Qty(level.size_micro));
            } else {
                off_tick_this += 1;
            }
        }

        // Check for crossed state.
        if let (Some(bb), Some(ba)) = (self.book.bids.best(), self.book.asks.best())
            && bb >= ba
        {
            self.valid = false;
            self.invalid_reason = Some(ResnapshotReason::CrossedBook);
            self.book = Book::new(self.ts);
            return ApplyOutcome::NeedsResnapshot(ResnapshotReason::CrossedBook);
        }

        // Commit.
        self.off_tick = off_tick_this;
        self.hash = Some(hash.into());
        self.last_update = Some(now);
        self.valid = true;
        self.invalid_reason = None;
        ApplyOutcome::Ok
    }

    /// Apply a batch of WS delta changes.
    ///
    /// If the book is currently invalid, returns the pending reason immediately
    /// without touching the book (idempotent demand — caller must resnapshot
    /// before applying further deltas).
    ///
    /// Otherwise: off-tick prices are counted and skipped. After the batch:
    /// - `off_tick >= THRESHOLD` → invalidate + `NeedsResnapshot(PersistentOffTick)`.
    /// - best_bid ≥ best_ask → invalidate + `NeedsResnapshot(CrossedBook)`.
    /// - Else: stamp `last_update`, update hash if `Some`.
    pub fn apply_changes(
        &mut self,
        now: Instant,
        changes: &[RawChange],
        hash: Option<&str>,
    ) -> ApplyOutcome {
        // Idempotent demand on invalid books.
        if !self.valid {
            let reason = self
                .invalid_reason
                .unwrap_or(ResnapshotReason::CrossedBook);
            return ApplyOutcome::NeedsResnapshot(reason);
        }

        for ch in changes {
            let side = if ch.side_buy { Side::Bid } else { Side::Ask };
            if let Some(px) = self.price_to_px(ch.price_micro) {
                self.book.apply(side, px, Qty(ch.size_micro));
            } else {
                self.off_tick += 1;
            }
        }

        // Persistent off-tick check.
        if self.off_tick >= OFF_TICK_RESNAPSHOT_THRESHOLD {
            self.valid = false;
            self.invalid_reason = Some(ResnapshotReason::PersistentOffTick);
            return ApplyOutcome::NeedsResnapshot(ResnapshotReason::PersistentOffTick);
        }

        // Crossed-book check.
        if let (Some(bb), Some(ba)) = (self.book.bids.best(), self.book.asks.best())
            && bb >= ba
        {
            self.valid = false;
            self.invalid_reason = Some(ResnapshotReason::CrossedBook);
            return ApplyOutcome::NeedsResnapshot(ResnapshotReason::CrossedBook);
        }

        // Commit.
        self.last_update = Some(now);
        if let Some(h) = hash {
            self.hash = Some(h.into());
        }
        ApplyOutcome::Ok
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::TickSize;
    use std::time::{Duration, Instant};

    fn lvl(p: &str, s: &str) -> RawLevel {
        RawLevel {
            price_micro: crate::decimal::parse_micro(p).unwrap(),
            size_micro: crate::decimal::parse_micro(s).unwrap(),
        }
    }

    fn snapshot(now: Instant) -> LiveBook {
        let mut lb = LiveBook::new(TickSize::Cent);
        let out = lb.apply_snapshot(
            now,
            &[lvl("0.44", "100"), lvl("0.43", "50")],
            &[lvl("0.46", "80"), lvl("0.47", "20")],
            "hash-1",
        );
        assert_eq!(out, ApplyOutcome::Ok);
        lb
    }

    #[test]
    fn snapshot_replaces_and_stamps() {
        let t0 = Instant::now();
        let lb = snapshot(t0);
        assert!(lb.valid());
        assert_eq!(lb.book().bids.best().unwrap().get(), 44);
        assert_eq!(lb.book().asks.best().unwrap().get(), 46);
        assert!(!lb.is_stale(t0 + Duration::from_millis(100), Duration::from_millis(1500)));
        assert!(lb.is_stale(t0 + Duration::from_millis(2000), Duration::from_millis(1500)));
    }

    #[test]
    fn delta_updates_levels_and_hash() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        let out = lb.apply_changes(
            t0 + Duration::from_millis(10),
            &[RawChange { side_buy: true, price_micro: 440_000, size_micro: 0 }],
            Some("hash-2"),
        );
        assert_eq!(out, ApplyOutcome::Ok);
        assert_eq!(lb.book().bids.best().unwrap().get(), 43);
        assert_eq!(lb.hash(), Some("hash-2"));
    }

    #[test]
    fn off_tick_price_is_counted_and_skipped() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        // 0.445 is off-tick on a Cent market
        let out = lb.apply_changes(
            t0,
            &[RawChange { side_buy: true, price_micro: 445_000, size_micro: 5_000_000 }],
            None,
        );
        assert_eq!(out, ApplyOutcome::Ok);
        assert_eq!(lb.off_tick_count(), 1);
        assert_eq!(lb.book().bids.best().unwrap().get(), 44); // unchanged
    }

    #[test]
    fn persistent_off_tick_demands_resnapshot() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        let bad = RawChange { side_buy: true, price_micro: 445_000, size_micro: 5_000_000 };
        for _ in 0..OFF_TICK_RESNAPSHOT_THRESHOLD - 1 {
            assert_eq!(lb.apply_changes(t0, &[bad], None), ApplyOutcome::Ok);
        }
        assert_eq!(
            lb.apply_changes(t0, &[bad], None),
            ApplyOutcome::NeedsResnapshot(ResnapshotReason::PersistentOffTick)
        );
    }

    #[test]
    fn crossed_book_demands_resnapshot_and_invalidates() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        let out = lb.apply_changes(
            t0,
            &[RawChange { side_buy: true, price_micro: 470_000, size_micro: 5_000_000 }],
            None,
        );
        assert_eq!(out, ApplyOutcome::NeedsResnapshot(ResnapshotReason::CrossedBook));
        assert!(!lb.valid());
        // a fresh snapshot restores validity
        let out = lb.apply_snapshot(t0, &[lvl("0.44", "10")], &[lvl("0.46", "10")], "hash-3");
        assert_eq!(out, ApplyOutcome::Ok);
        assert!(lb.valid());
    }

    #[test]
    fn price_at_or_beyond_bounds_is_off_tick() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        for p in [0u64, 1_000_000, 1_010_000] {
            let out = lb.apply_changes(
                t0,
                &[RawChange { side_buy: false, price_micro: p, size_micro: 1 }],
                None,
            );
            assert!(matches!(out, ApplyOutcome::Ok | ApplyOutcome::NeedsResnapshot(_)));
        }
        assert_eq!(lb.off_tick_count(), 3);
    }

    #[test]
    fn crossed_snapshot_is_rejected_and_stays_invalid() {
        let t0 = Instant::now();
        let mut lb = LiveBook::new(TickSize::Cent);
        // Best bid (0.60) ≥ best ask (0.40) → crossed snapshot
        let out = lb.apply_snapshot(
            t0,
            &[lvl("0.60", "10")],
            &[lvl("0.40", "10")],
            "bad-hash",
        );
        assert_eq!(out, ApplyOutcome::NeedsResnapshot(ResnapshotReason::CrossedBook));
        assert!(!lb.valid());
    }

    #[test]
    fn deltas_after_invalidation_keep_demanding_resnapshot_and_do_not_stamp() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        // Invalidate via crossed book
        let _ = lb.apply_changes(
            t0,
            &[RawChange { side_buy: true, price_micro: 470_000, size_micro: 5_000_000 }],
            None,
        );
        assert!(!lb.valid());
        let last_update_before = lb.last_update();
        // Applying more changes on an invalid book must return the pending reason
        let out = lb.apply_changes(
            t0 + Duration::from_millis(50),
            &[RawChange { side_buy: true, price_micro: 440_000, size_micro: 10_000_000 }],
            Some("hash-new"),
        );
        assert_eq!(out, ApplyOutcome::NeedsResnapshot(ResnapshotReason::CrossedBook));
        // last_update must not advance
        assert_eq!(lb.last_update(), last_update_before);
    }
}
