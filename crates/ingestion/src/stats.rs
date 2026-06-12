//! Aggregate ingestion statistics: HDR histograms for stage latencies,
//! rolled-up counters from supervisors, and formatted probe output lines.
//!
//! # Design
//!
//! [`ProbeStats`] owns two [`hdrhistogram::Histogram`]s for µs-resolution
//! stage latencies, plus gauge fields that track rolled-up supervisor
//! counters and book health.
//!
//! Gauge fields are **replaced** on each print cycle:
//! 1. Call [`ProbeStats::reset_gauges`] to zero all gauge fields.
//! 2. Call [`ProbeStats::absorb_supervisor`] once per supervisor (or per
//!    [`StatsCell`] snapshot) — this **adds** to the gauge fields.
//! 3. Call [`ProbeStats::line`] to produce the formatted output string.
//!
//! Histograms are cumulative across the lifetime of the `ProbeStats` —
//! they are never reset unless the caller drops and recreates the struct.
//!
//! # StatsCell
//!
//! [`StatsCell`] is a low-overhead shared-stats handle for supervisors that
//! run inside async tasks. It carries `AtomicU64` fields mirroring [`SupStats`]
//! plus books/stale gauges, and two `std::sync::Mutex<Histogram<u64>>`s for
//! per-frame latency recording (µs-scale work, ~100 frames/s — negligible lock
//! contention). The supervisor calls [`StatsCell::refresh`] once per handled
//! frame and once per sweep tick. The probe drains/merges per print.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use hdrhistogram::Histogram;

use crate::supervisor::SupStats;

// ---------------------------------------------------------------------------
// StageHistos — HDR histograms for two pipeline stages
// ---------------------------------------------------------------------------

/// HDR histograms for ingestion pipeline stage latencies (µs, 3 sig figs).
pub struct StageHistos {
    /// Time from frame receipt to last parse_frame completion (µs).
    pub recv_to_parsed: Histogram<u64>,
    /// Time from parse completion to last apply_changes/apply_snapshot call (µs).
    pub parsed_to_applied: Histogram<u64>,
}

impl StageHistos {
    fn new() -> Self {
        // auto_resize = true: histograms grow dynamically to accommodate any value.
        // Histogram::new(3) only fails for sig_figs outside [1,5]; 3 is always valid.
        #[allow(clippy::expect_used)]
        let mut h1 = Histogram::<u64>::new(3).expect("histogram alloc");
        h1.auto(true);
        #[allow(clippy::expect_used)]
        let mut h2 = Histogram::<u64>::new(3).expect("histogram alloc");
        h2.auto(true);
        StageHistos { recv_to_parsed: h1, parsed_to_applied: h2 }
    }
}

// ---------------------------------------------------------------------------
// StatsCell — lightweight shared stats handle for supervisors
// ---------------------------------------------------------------------------

/// Shared-stats handle written by a [`Supervisor`] task and read by the probe.
///
/// Atomics carry cumulative counters (frames, events, etc.) plus the current
/// books/stale gauges. The two histograms live behind a `std::sync::Mutex`
/// so the supervisor can record µs latencies inline (the lock is held for
/// ~100 ns — negligible at ~100 frames/s).
///
/// # Refresh protocol
///
/// The supervisor calls [`StatsCell::refresh`] at the end of every
/// `handle_frame` call and at the start of every sweep tick, supplying:
/// - a snapshot of [`SupStats`] from `supervisor.stats()`,
/// - the current book count from `shard.book_count()`,
/// - the current stale count from `shard.stale_tokens(now, staleness).len()`,
/// - optional parse and apply latencies in µs (from `Instant` measurements
///   taken in `handle_frame`).
pub struct StatsCell {
    // Cumulative counters (mirrors SupStats fields).
    pub frames: AtomicU64,
    pub events: AtomicU64,
    pub parse_errors: AtomicU64,
    pub reconnects: AtomicU64,
    pub resnapshots: AtomicU64,
    pub resnapshot_errors: AtomicU64,
    pub unknown_token_changes: AtomicU64,

    // Current gauges (overwritten on every refresh).
    pub books: AtomicU64,
    pub stale: AtomicU64,

    // Per-frame latency histograms (Mutex — held for ~100 ns per frame).
    pub recv_to_parsed_us: Mutex<Histogram<u64>>,
    pub parsed_to_applied_us: Mutex<Histogram<u64>>,
}

impl StatsCell {
    /// Allocate a fresh `StatsCell` with zeroed counters and empty histograms.
    pub fn new() -> Arc<Self> {
        // Histogram::new(3) only fails for sig_figs outside [1,5]; 3 is always valid.
        #[allow(clippy::expect_used)]
        let mut h1 = Histogram::<u64>::new(3).expect("histogram alloc");
        h1.auto(true);
        #[allow(clippy::expect_used)]
        let mut h2 = Histogram::<u64>::new(3).expect("histogram alloc");
        h2.auto(true);
        Arc::new(Self {
            frames: AtomicU64::new(0),
            events: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            resnapshots: AtomicU64::new(0),
            resnapshot_errors: AtomicU64::new(0),
            unknown_token_changes: AtomicU64::new(0),
            books: AtomicU64::new(0),
            stale: AtomicU64::new(0),
            recv_to_parsed_us: Mutex::new(h1),
            parsed_to_applied_us: Mutex::new(h2),
        })
    }

    /// Refresh all fields from a supervisor + shard snapshot.
    ///
    /// `parse_us` and `apply_us` are optional per-frame latencies in
    /// microseconds; when supplied they are recorded into the histograms.
    pub fn refresh(
        &self,
        sup_stats: &SupStats,
        books: usize,
        stale: usize,
        parse_us: Option<u64>,
        apply_us: Option<u64>,
    ) {
        // Relaxed stores: these are read by a separate thread (the probe) on a
        // best-effort basis — we don't need strict ordering relative to other
        // memory operations. The probe takes periodic snapshots; a torn read
        // here is acceptable for monitoring purposes.
        self.frames.store(sup_stats.frames, Ordering::Relaxed);
        self.events.store(sup_stats.events, Ordering::Relaxed);
        self.parse_errors.store(sup_stats.parse_errors, Ordering::Relaxed);
        self.reconnects.store(sup_stats.reconnects, Ordering::Relaxed);
        self.resnapshots.store(sup_stats.resnapshots, Ordering::Relaxed);
        self.resnapshot_errors.store(sup_stats.resnapshot_errors, Ordering::Relaxed);
        self.unknown_token_changes
            .store(sup_stats.unknown_token_changes, Ordering::Relaxed);
        self.books.store(books as u64, Ordering::Relaxed);
        self.stale.store(stale as u64, Ordering::Relaxed);

        if let Some(us) = parse_us
            && let Ok(mut h) = self.recv_to_parsed_us.lock()
        {
            let _ = h.record(us); // ignore saturate errors
        }
        if let Some(us) = apply_us
            && let Ok(mut h) = self.parsed_to_applied_us.lock()
        {
            let _ = h.record(us);
        }
    }

    /// Snapshot all counter fields (relaxed loads).
    pub fn snapshot_stats(&self) -> SupStats {
        SupStats {
            frames: self.frames.load(Ordering::Relaxed),
            events: self.events.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            resnapshots: self.resnapshots.load(Ordering::Relaxed),
            resnapshot_errors: self.resnapshot_errors.load(Ordering::Relaxed),
            unknown_token_changes: self.unknown_token_changes.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// ProbeStats — the aggregate rolling-up struct used by the probe
// ---------------------------------------------------------------------------

/// Aggregated ingestion statistics for one print cycle.
///
/// Gauge fields (books, stale, frames, etc.) are replaced each cycle via
/// [`reset_gauges`] + [`absorb_supervisor`]. Histogram fields are cumulative.
///
/// [`reset_gauges`]: ProbeStats::reset_gauges
/// [`absorb_supervisor`]: ProbeStats::absorb_supervisor
pub struct ProbeStats {
    // Stage histograms — cumulative.
    histos: StageHistos,

    // Rolled-up gauge fields — replaced each print cycle.
    pub frames: u64,
    pub events: u64,
    pub parse_errors: u64,
    pub reconnects: u64,
    pub resnapshots: u64,
    pub resnapshot_errors: u64,
    pub unknown_token_changes: u64,
    pub books: usize,
    pub stale: usize,
}

impl ProbeStats {
    /// Create a new `ProbeStats` with zeroed counters and empty histograms.
    pub fn new() -> Self {
        ProbeStats {
            histos: StageHistos::new(),
            frames: 0,
            events: 0,
            parse_errors: 0,
            reconnects: 0,
            resnapshots: 0,
            resnapshot_errors: 0,
            unknown_token_changes: 0,
            books: 0,
            stale: 0,
        }
    }

    /// Record one recv→parsed latency sample (µs).
    pub fn record_recv_to_parsed_us(&mut self, us: u64) {
        let _ = self.histos.recv_to_parsed.record(us);
    }

    /// Record one parsed→applied latency sample (µs).
    pub fn record_parsed_to_applied_us(&mut self, us: u64) {
        let _ = self.histos.parsed_to_applied.record(us);
    }

    /// Zero all gauge fields in preparation for a fresh absorb cycle.
    ///
    /// Does **not** touch the histograms — those are cumulative.
    pub fn reset_gauges(&mut self) {
        self.frames = 0;
        self.events = 0;
        self.parse_errors = 0;
        self.reconnects = 0;
        self.resnapshots = 0;
        self.resnapshot_errors = 0;
        self.unknown_token_changes = 0;
        self.books = 0;
        self.stale = 0;
    }

    /// Add one supervisor's stats snapshot to the gauge fields.
    ///
    /// Supervisor stats are **cumulative** (monotonically increasing counters).
    /// Call [`reset_gauges`] before the first `absorb_supervisor` call in a
    /// print cycle, then call `absorb_supervisor` once per supervisor.
    ///
    /// [`reset_gauges`]: ProbeStats::reset_gauges
    pub fn absorb_supervisor(&mut self, sup_stats: &SupStats, books: usize, stale: usize) {
        self.frames = self.frames.saturating_add(sup_stats.frames);
        self.events = self.events.saturating_add(sup_stats.events);
        self.parse_errors = self.parse_errors.saturating_add(sup_stats.parse_errors);
        self.reconnects = self.reconnects.saturating_add(sup_stats.reconnects);
        self.resnapshots = self.resnapshots.saturating_add(sup_stats.resnapshots);
        self.resnapshot_errors =
            self.resnapshot_errors.saturating_add(sup_stats.resnapshot_errors);
        self.unknown_token_changes =
            self.unknown_token_changes.saturating_add(sup_stats.unknown_token_changes);
        self.books = self.books.saturating_add(books);
        self.stale = self.stale.saturating_add(stale);
    }

    /// Format a one-line status string for the probe output.
    ///
    /// Example:
    /// `"up=120s books=400 stale=3 frames=12345 events=23456 parse_err=0 reconn=0 resnap=412 p50/p99 parse=8µs/40µs apply=2µs/19µs"`
    pub fn line(&self, uptime: Duration) -> String {
        let p50_parse = self.histos.recv_to_parsed.value_at_quantile(0.50);
        let p99_parse = self.histos.recv_to_parsed.value_at_quantile(0.99);
        let p50_apply = self.histos.parsed_to_applied.value_at_quantile(0.50);
        let p99_apply = self.histos.parsed_to_applied.value_at_quantile(0.99);

        format!(
            "up={up}s books={books} stale={stale} frames={frames} events={events} \
             parse_err={parse_err} reconn={reconn} resnap={resnap} \
             p50/p99 parse={p50_parse}µs/{p99_parse}µs apply={p50_apply}µs/{p99_apply}µs",
            up = uptime.as_secs(),
            books = self.books,
            stale = self.stale,
            frames = self.frames,
            events = self.events,
            parse_err = self.parse_errors,
            reconn = self.reconnects,
            resnap = self.resnapshots,
            p50_parse = p50_parse,
            p99_parse = p99_parse,
            p50_apply = p50_apply,
            p99_apply = p99_apply,
        )
    }

    /// Health predicate.
    ///
    /// Returns `true` iff:
    /// - `books > 0`
    /// - `stale * 5 <= books` (at most 20% stale)
    /// - `parse_errors * 100 <= max(1, frames)` (≤ 1% parse error rate)
    pub fn healthy(&self) -> bool {
        if self.books == 0 {
            return false;
        }
        if self.stale * 5 > self.books {
            return false;
        }
        let denom = self.frames.max(1);
        self.parse_errors * 100 <= denom
    }
}

impl Default for ProbeStats {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::supervisor::SupStats;
    use std::time::Duration;

    // ------------------------------------------------------------------
    // Percentile sanity
    // ------------------------------------------------------------------

    #[test]
    fn percentile_sanity_parse_stage() {
        let mut ps = ProbeStats::new();
        for us in 1u64..=1000 {
            ps.record_recv_to_parsed_us(us);
        }
        let p50 = ps.histos.recv_to_parsed.value_at_quantile(0.50);
        let p99 = ps.histos.recv_to_parsed.value_at_quantile(0.99);
        assert!(
            (400..=600).contains(&p50),
            "p50 of 1..1000µs should be in [400,600], got {p50}"
        );
        assert!(p99 >= 900, "p99 of 1..1000µs should be ≥ 900, got {p99}");
    }

    #[test]
    fn percentile_sanity_apply_stage() {
        let mut ps = ProbeStats::new();
        for us in 1u64..=1000 {
            ps.record_parsed_to_applied_us(us);
        }
        let p50 = ps.histos.parsed_to_applied.value_at_quantile(0.50);
        let p99 = ps.histos.parsed_to_applied.value_at_quantile(0.99);
        assert!(
            (400..=600).contains(&p50),
            "p50 of 1..1000µs should be in [400,600], got {p50}"
        );
        assert!(p99 >= 900, "p99 of 1..1000µs should be ≥ 900, got {p99}");
    }

    // ------------------------------------------------------------------
    // line() format
    // ------------------------------------------------------------------

    #[test]
    fn line_contains_required_substrings() {
        let mut ps = ProbeStats::new();
        ps.books = 400;
        ps.stale = 3;
        ps.frames = 12345;
        ps.events = 23456;
        ps.record_recv_to_parsed_us(8);
        ps.record_parsed_to_applied_us(2);

        let s = ps.line(Duration::from_secs(120));
        assert!(s.contains("books="), "missing books= in: {s}");
        assert!(s.contains("stale="), "missing stale= in: {s}");
        assert!(s.contains("p50"), "missing p50 in: {s}");
        assert!(s.contains("up=120s"), "missing up= in: {s}");
        assert!(s.contains("frames=12345"), "missing frames= in: {s}");
    }

    // ------------------------------------------------------------------
    // healthy() boundaries
    // ------------------------------------------------------------------

    #[test]
    fn healthy_zero_books_is_false() {
        let ps = ProbeStats::new(); // books = 0
        assert!(!ps.healthy());
    }

    #[test]
    fn healthy_stale_boundary_20pct() {
        let mut ps = ProbeStats::new();
        ps.books = 100;

        // Exactly 20% stale → healthy (stale*5 == books)
        ps.stale = 20;
        assert!(ps.healthy(), "20% stale should be healthy (boundary)");

        // 21% stale → unhealthy (stale*5 > books)
        ps.stale = 21;
        assert!(!ps.healthy(), "21% stale should be unhealthy");
    }

    #[test]
    fn healthy_parse_error_rate_1pct_boundary() {
        let mut ps = ProbeStats::new();
        ps.books = 10;
        ps.frames = 100;

        // Exactly 1% parse errors → healthy (parse_errors*100 == frames)
        ps.parse_errors = 1;
        assert!(ps.healthy(), "1% parse error rate should be healthy (boundary)");

        // 2% parse errors → unhealthy
        ps.parse_errors = 2;
        assert!(!ps.healthy(), "2% parse error rate should be unhealthy");
    }

    #[test]
    fn healthy_zero_frames_uses_max_1_denom() {
        let mut ps = ProbeStats::new();
        ps.books = 10;
        ps.stale = 0;
        ps.frames = 0;
        ps.parse_errors = 0;
        // 0 parse errors * 100 <= max(1, 0) = 1 → healthy
        assert!(ps.healthy(), "zero frames and zero errors should be healthy");

        ps.parse_errors = 1;
        // 1 * 100 = 100 > 1 → unhealthy
        assert!(!ps.healthy(), "parse error with 0 frames should be unhealthy");
    }

    // ------------------------------------------------------------------
    // reset_gauges + absorb_supervisor accumulation
    // ------------------------------------------------------------------

    #[test]
    fn absorb_two_supervisors_sums_correctly() {
        let mut ps = ProbeStats::new();

        let sup1 = SupStats {
            frames: 1000,
            events: 2000,
            parse_errors: 5,
            reconnects: 1,
            resnapshots: 50,
            resnapshot_errors: 2,
            unknown_token_changes: 3,
        };
        let sup2 = SupStats {
            frames: 500,
            events: 1000,
            parse_errors: 0,
            reconnects: 0,
            resnapshots: 20,
            resnapshot_errors: 0,
            unknown_token_changes: 1,
        };

        // First print cycle
        ps.reset_gauges();
        ps.absorb_supervisor(&sup1, 200, 10);
        ps.absorb_supervisor(&sup2, 100, 5);

        assert_eq!(ps.frames, 1500);
        assert_eq!(ps.events, 3000);
        assert_eq!(ps.parse_errors, 5);
        assert_eq!(ps.reconnects, 1);
        assert_eq!(ps.resnapshots, 70);
        assert_eq!(ps.resnapshot_errors, 2);
        assert_eq!(ps.unknown_token_changes, 4);
        assert_eq!(ps.books, 300);
        assert_eq!(ps.stale, 15);

        // Second print cycle: simulate progress — same sups, just call reset+absorb again
        ps.reset_gauges();
        ps.absorb_supervisor(&sup1, 200, 10);
        ps.absorb_supervisor(&sup2, 100, 5);

        // Should still equal the same values (gauges replaced not accumulated)
        assert_eq!(ps.frames, 1500);
        assert_eq!(ps.books, 300);
    }

    // ------------------------------------------------------------------
    // StatsCell
    // ------------------------------------------------------------------

    #[test]
    fn stats_cell_refresh_and_snapshot() {
        let cell = StatsCell::new();
        let stats = SupStats {
            frames: 42,
            events: 84,
            parse_errors: 1,
            reconnects: 0,
            resnapshots: 3,
            resnapshot_errors: 0,
            unknown_token_changes: 2,
        };
        cell.refresh(&stats, 100, 5, Some(10), Some(3));

        let snap = cell.snapshot_stats();
        assert_eq!(snap.frames, 42);
        assert_eq!(snap.events, 84);
        assert_eq!(snap.parse_errors, 1);
        assert_eq!(cell.books.load(Ordering::Relaxed), 100);
        assert_eq!(cell.stale.load(Ordering::Relaxed), 5);

        // Check histogram entries
        let p50 = cell.recv_to_parsed_us.lock().unwrap().value_at_quantile(0.50);
        assert_eq!(p50, 10);
    }
}
