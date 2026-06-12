//! Single-writer shard: owns a HashMap<TokenId, LiveBook> (spec §5 / §12).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use pm_core::instrument::TokenId;
use pm_core::num::TickSize;

use crate::livebook::{ApplyOutcome, LiveBook, RawChange, RawLevel, ResnapshotReason};

/// Aggregate stats for one shard — useful for probes and health metrics.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShardStats {
    pub snapshots: u64,
    pub deltas: u64,
    pub off_tick: u64,
    pub resnapshots_requested: u64,
}

/// Owns an exclusive HashMap<TokenId, LiveBook>. Never shared across threads
/// (single-writer — the task owns it).
#[derive(Default)]
pub struct Shard {
    books: HashMap<TokenId, LiveBook>,
    stats: ShardStats,
}

impl Shard {
    /// Ensure a book slot exists for `token` with the given tick size.
    /// No-op if it already exists (tick size changes are not supported in-band).
    pub fn ensure_book(&mut self, token: TokenId, tick: TickSize) {
        self.books.entry(token).or_insert_with(|| LiveBook::new(tick));
    }

    /// Apply a REST snapshot to `token`. Creates the book slot if absent.
    /// Increments snapshot stats; if the outcome requests a resnapshot, also
    /// increments that counter (crossed snapshot).
    pub fn apply_snapshot(
        &mut self,
        now: Instant,
        token: TokenId,
        tick: TickSize,
        bids: &[RawLevel],
        asks: &[RawLevel],
        hash: &str,
    ) -> ApplyOutcome {
        self.stats.snapshots += 1;
        let book = self.books.entry(token).or_insert_with(|| LiveBook::new(tick));
        let outcome = book.apply_snapshot(now, bids, asks, hash);
        if matches!(outcome, ApplyOutcome::NeedsResnapshot(_)) {
            self.stats.resnapshots_requested += 1;
        }
        outcome
    }

    /// Apply WS delta changes to `token`.
    ///
    /// If the token is unknown (no book slot), returns
    /// `NeedsResnapshot(UnknownToken)` — the supervisor must fetch a snapshot
    /// before deltas can be accepted. Does NOT create a slot (snapshot-only path
    /// creates slots).
    ///
    /// Increments delta stats and off-tick stats accordingly.
    pub fn apply_changes(
        &mut self,
        now: Instant,
        token: TokenId,
        changes: &[RawChange],
        hash: Option<&str>,
    ) -> ApplyOutcome {
        let Some(book) = self.books.get_mut(&token) else {
            self.stats.resnapshots_requested += 1;
            return ApplyOutcome::NeedsResnapshot(ResnapshotReason::UnknownToken);
        };

        let off_tick_before = book.off_tick_count();
        self.stats.deltas += 1;
        let outcome = book.apply_changes(now, changes, hash);

        // Account for any new off-tick prices seen during this apply.
        let off_tick_after = book.off_tick_count();
        // off_tick_after may be less than off_tick_before if it just reset (it
        // doesn't reset on changes — only on snapshot).  Guard the delta.
        if off_tick_after > off_tick_before {
            self.stats.off_tick += u64::from(off_tick_after - off_tick_before);
        }

        if matches!(outcome, ApplyOutcome::NeedsResnapshot(_)) {
            self.stats.resnapshots_requested += 1;
        }
        outcome
    }

    /// Whether a specific token's book is stale.
    pub fn is_stale(&self, token: TokenId, now: Instant, window: Duration) -> bool {
        self.books
            .get(&token)
            .is_none_or(|b| b.is_stale(now, window))
    }

    /// Tokens whose books are stale (invalid or stamp age ≥ window).
    pub fn stale_tokens(&self, now: Instant, window: Duration) -> Vec<TokenId> {
        self.books
            .iter()
            .filter(|(_, b)| b.is_stale(now, window))
            .map(|(&t, _)| t)
            .collect()
    }

    /// Mark every book as stale (used on WS reconnect).
    /// Clears `last_update` by invalidating each book's staleness — we
    /// accomplish this by setting `valid = false` via a forced apply_changes
    /// path... but since we can't mutate valid directly we use a dedicated
    /// method on LiveBook.
    pub fn mark_all_stale(&mut self) {
        for book in self.books.values_mut() {
            book.force_stale();
        }
    }

    /// Read-only access to a book by token.
    pub fn book(&self, token: TokenId) -> Option<&LiveBook> {
        self.books.get(&token)
    }

    /// Current stats snapshot.
    pub fn stats(&self) -> ShardStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::livebook::{ApplyOutcome, RawChange, RawLevel, ResnapshotReason};
    use pm_core::instrument::TokenId;
    use pm_core::num::TickSize;
    use std::time::{Duration, Instant};

    fn lvl(p: &str, s: &str) -> RawLevel {
        RawLevel {
            price_micro: crate::decimal::parse_micro(p).unwrap(),
            size_micro: crate::decimal::parse_micro(s).unwrap(),
        }
    }

    fn seed_snapshot(shard: &mut Shard, token: TokenId, now: Instant) -> ApplyOutcome {
        shard.apply_snapshot(
            now,
            token,
            TickSize::Cent,
            &[lvl("0.44", "100"), lvl("0.43", "50")],
            &[lvl("0.46", "80"), lvl("0.47", "20")],
            "hash-1",
        )
    }

    #[test]
    fn routing_independence_two_tokens() {
        let mut shard = Shard::default();
        let t0 = Instant::now();
        let tok_a = TokenId(0);
        let tok_b = TokenId(1);

        assert_eq!(seed_snapshot(&mut shard, tok_a, t0), ApplyOutcome::Ok);
        assert_eq!(seed_snapshot(&mut shard, tok_b, t0), ApplyOutcome::Ok);

        // Modify tok_a only
        let out = shard.apply_changes(
            t0,
            tok_a,
            &[RawChange { side_buy: true, price_micro: 440_000, size_micro: 0 }],
            None,
        );
        assert_eq!(out, ApplyOutcome::Ok);

        // tok_a best bid moved to 43
        assert_eq!(shard.book(tok_a).unwrap().book().bids.best().unwrap().get(), 43);
        // tok_b unchanged
        assert_eq!(shard.book(tok_b).unwrap().book().bids.best().unwrap().get(), 44);
    }

    #[test]
    fn stale_tokens_lists_only_stale() {
        let mut shard = Shard::default();
        let t0 = Instant::now();
        let tok_a = TokenId(0);
        let tok_b = TokenId(1);

        // Snapshot both at t0
        seed_snapshot(&mut shard, tok_a, t0);
        // tok_b snapshot at a later time
        shard.apply_snapshot(
            t0 + Duration::from_millis(1000),
            tok_b,
            TickSize::Cent,
            &[lvl("0.44", "10")],
            &[lvl("0.56", "10")],
            "hash-b",
        );

        let window = Duration::from_millis(1500);
        // At t0 + 1600ms: tok_a is stale (1600 > 1500), tok_b is fresh (600 < 1500)
        let stale = shard.stale_tokens(t0 + Duration::from_millis(1600), window);
        assert!(stale.contains(&tok_a));
        assert!(!stale.contains(&tok_b));
    }

    #[test]
    fn mark_all_stale_invalidates_everything() {
        let mut shard = Shard::default();
        let t0 = Instant::now();
        let tok_a = TokenId(0);
        let tok_b = TokenId(1);

        seed_snapshot(&mut shard, tok_a, t0);
        seed_snapshot(&mut shard, tok_b, t0);

        shard.mark_all_stale();

        let window = Duration::from_millis(1500);
        // Both should appear stale immediately after mark_all_stale
        let stale = shard.stale_tokens(t0, window);
        assert!(stale.contains(&tok_a));
        assert!(stale.contains(&tok_b));
    }

    #[test]
    fn stats_counters_increment() {
        let mut shard = Shard::default();
        let t0 = Instant::now();
        let tok = TokenId(0);

        assert_eq!(shard.stats().snapshots, 0);
        seed_snapshot(&mut shard, tok, t0);
        assert_eq!(shard.stats().snapshots, 1);

        assert_eq!(shard.stats().deltas, 0);
        let ok = shard.apply_changes(
            t0,
            tok,
            &[RawChange { side_buy: true, price_micro: 440_000, size_micro: 0 }],
            None,
        );
        assert_eq!(ok, ApplyOutcome::Ok);
        assert_eq!(shard.stats().deltas, 1);

        // Off-tick change — stats.off_tick increments
        shard.apply_changes(
            t0,
            tok,
            &[RawChange { side_buy: true, price_micro: 445_000, size_micro: 1_000_000 }],
            None,
        );
        assert_eq!(shard.stats().off_tick, 1);
    }

    #[test]
    fn resnapshot_stat_increments_on_crossed_book() {
        let mut shard = Shard::default();
        let t0 = Instant::now();
        let tok = TokenId(0);
        seed_snapshot(&mut shard, tok, t0);

        // Cross the book
        let out = shard.apply_changes(
            t0,
            tok,
            &[RawChange { side_buy: true, price_micro: 470_000, size_micro: 5_000_000 }],
            None,
        );
        assert_eq!(out, ApplyOutcome::NeedsResnapshot(ResnapshotReason::CrossedBook));
        assert_eq!(shard.stats().resnapshots_requested, 1);
    }

    #[test]
    fn unknown_token_change_requests_snapshot() {
        let mut shard = Shard::default();
        let t0 = Instant::now();
        let tok = TokenId(99);

        let out = shard.apply_changes(
            t0,
            tok,
            &[RawChange { side_buy: true, price_micro: 440_000, size_micro: 10_000_000 }],
            None,
        );
        assert_eq!(out, ApplyOutcome::NeedsResnapshot(ResnapshotReason::UnknownToken));
        assert_eq!(shard.stats().resnapshots_requested, 1);
    }
}
