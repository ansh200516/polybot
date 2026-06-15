//! App-level counters + the two M3 latency stages (spec §20):
//! applied→detected (detector) and detected→submitted (coordinator).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use hdrhistogram::Histogram;

fn histo() -> Histogram<u64> {
    // auto_resize=true: grows dynamically; Histogram::new(3) always succeeds for sig_figs=3.
    // Mirrors ingestion's StageHistos idiom.
    #[allow(clippy::expect_used)]
    let mut h = Histogram::<u64>::new(3).expect("histogram alloc");
    h.auto(true);
    h
}

pub struct AppStats {
    pub opps_emitted: AtomicU64,
    pub opps_dropped: AtomicU64,
    pub lp_jobs: AtomicU64,
    pub lp_solved: AtomicU64,
    pub lp_skips: AtomicU64,
    pub lp_dropped_full: AtomicU64,
    pub admitted: AtomicU64,
    pub suppressed_cooldown: AtomicU64,
    pub suppressed_busy: AtomicU64,
    pub expired_age: AtomicU64,
    pub rejected_risk: AtomicU64,
    /// Opps suppressed by the plausibility ceiling (edges.max_edge_bps) — almost
    /// always stale/dead books or a NegRisk set wrongly assumed exhaustive.
    pub suppressed_implausible: AtomicU64,
    pub live_rej: AtomicU64,
    pub live_held: AtomicU64,
    pub dispatched: AtomicU64,
    pub baskets_clean: AtomicU64,
    pub baskets_repaired: AtomicU64,
    pub baskets_unwound: AtomicU64,
    pub baskets_nofill: AtomicU64,
    pub exec_errors: AtomicU64,
    pub detect_us: Mutex<Histogram<u64>>,
    pub dispatch_us: Mutex<Histogram<u64>>,
}

impl AppStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            opps_emitted: AtomicU64::new(0),
            opps_dropped: AtomicU64::new(0),
            lp_jobs: AtomicU64::new(0),
            lp_solved: AtomicU64::new(0),
            lp_skips: AtomicU64::new(0),
            lp_dropped_full: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            suppressed_cooldown: AtomicU64::new(0),
            suppressed_busy: AtomicU64::new(0),
            expired_age: AtomicU64::new(0),
            rejected_risk: AtomicU64::new(0),
            suppressed_implausible: AtomicU64::new(0),
            live_rej: AtomicU64::new(0),
            live_held: AtomicU64::new(0),
            dispatched: AtomicU64::new(0),
            baskets_clean: AtomicU64::new(0),
            baskets_repaired: AtomicU64::new(0),
            baskets_unwound: AtomicU64::new(0),
            baskets_nofill: AtomicU64::new(0),
            exec_errors: AtomicU64::new(0),
            detect_us: Mutex::new(histo()),
            dispatch_us: Mutex::new(histo()),
        })
    }

    pub fn record_detect_us(&self, us: u64) {
        if let Ok(mut h) = self.detect_us.lock() {
            let _ = h.record(us.max(1));
        }
    }

    pub fn record_dispatch_us(&self, us: u64) {
        if let Ok(mut h) = self.dispatch_us.lock() {
            let _ = h.record(us.max(1));
        }
    }

    pub fn line(&self) -> String {
        let (d_p50, d_p99) = if let Ok(h) = self.detect_us.lock() {
            (h.value_at_quantile(0.50), h.value_at_quantile(0.99))
        } else {
            (0, 0)
        };
        let (dp_p50, dp_p99) = if let Ok(h) = self.dispatch_us.lock() {
            (h.value_at_quantile(0.50), h.value_at_quantile(0.99))
        } else {
            (0, 0)
        };
        format!(
            "opps={opps} dropped={dropped} lp_jobs={lp_jobs} lp_solved={lp_solved} \
             lp_skips={lp_skips} \
             lp_dropped_full={lp_dropped_full} \
             admitted={admitted} cool={cool} busy={busy} expired={expired} \
             risk_rej={risk_rej} implausible={implausible} live_rej={live_rej} live_held={live_held} dispatched={dispatched} \
             baskets_clean={b_clean} repaired={b_rep} unwound={b_unw} nofill={b_nof} \
             exec_err={exec_err} \
             detect_p50={d_p50}µs detect_p99={d_p99}µs \
             dispatch_p50={dp_p50}µs dispatch_p99={dp_p99}µs",
            opps = self.opps_emitted.load(Ordering::Relaxed),
            dropped = self.opps_dropped.load(Ordering::Relaxed),
            lp_jobs = self.lp_jobs.load(Ordering::Relaxed),
            lp_solved = self.lp_solved.load(Ordering::Relaxed),
            lp_skips = self.lp_skips.load(Ordering::Relaxed),
            lp_dropped_full = self.lp_dropped_full.load(Ordering::Relaxed),
            admitted = self.admitted.load(Ordering::Relaxed),
            cool = self.suppressed_cooldown.load(Ordering::Relaxed),
            busy = self.suppressed_busy.load(Ordering::Relaxed),
            expired = self.expired_age.load(Ordering::Relaxed),
            risk_rej = self.rejected_risk.load(Ordering::Relaxed),
            implausible = self.suppressed_implausible.load(Ordering::Relaxed),
            live_rej = self.live_rej.load(Ordering::Relaxed),
            live_held = self.live_held.load(Ordering::Relaxed),
            dispatched = self.dispatched.load(Ordering::Relaxed),
            b_clean = self.baskets_clean.load(Ordering::Relaxed),
            b_rep = self.baskets_repaired.load(Ordering::Relaxed),
            b_unw = self.baskets_unwound.load(Ordering::Relaxed),
            b_nof = self.baskets_nofill.load(Ordering::Relaxed),
            exec_err = self.exec_errors.load(Ordering::Relaxed),
            d_p50 = d_p50,
            d_p99 = d_p99,
            dp_p50 = dp_p50,
            dp_p99 = dp_p99,
        )
    }
}

impl Default for AppStats {
    fn default() -> Self {
        // AppStats::new() returns Arc<Self>; for Default we build a plain Self.
        Self {
            opps_emitted: AtomicU64::new(0),
            opps_dropped: AtomicU64::new(0),
            lp_jobs: AtomicU64::new(0),
            lp_solved: AtomicU64::new(0),
            lp_skips: AtomicU64::new(0),
            lp_dropped_full: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            suppressed_cooldown: AtomicU64::new(0),
            suppressed_busy: AtomicU64::new(0),
            expired_age: AtomicU64::new(0),
            rejected_risk: AtomicU64::new(0),
            suppressed_implausible: AtomicU64::new(0),
            live_rej: AtomicU64::new(0),
            live_held: AtomicU64::new(0),
            dispatched: AtomicU64::new(0),
            baskets_clean: AtomicU64::new(0),
            baskets_repaired: AtomicU64::new(0),
            baskets_unwound: AtomicU64::new(0),
            baskets_nofill: AtomicU64::new(0),
            exec_errors: AtomicU64::new(0),
            detect_us: Mutex::new(histo()),
            dispatch_us: Mutex::new(histo()),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn stats_line_contains_expected_fields_and_quantiles() {
        let s = AppStats::new();

        // Record detect latencies: 100 samples at 50µs → p50 = 50, p99 = 50
        for _ in 0..100 {
            s.record_detect_us(50);
        }
        // Record dispatch latencies: 100 samples at 200µs → p50 = 200, p99 = 200
        for _ in 0..100 {
            s.record_dispatch_us(200);
        }

        s.opps_emitted.fetch_add(42, Ordering::Relaxed);
        s.lp_jobs.fetch_add(7, Ordering::Relaxed);
        s.dispatched.fetch_add(10, Ordering::Relaxed);

        let line = s.line();

        // Field presence
        assert!(line.contains("opps=42"), "missing opps=42 in: {line}");
        assert!(line.contains("lp_jobs=7"), "missing lp_jobs=7 in: {line}");
        assert!(
            line.contains("dispatched=10"),
            "missing dispatched=10 in: {line}"
        );

        // Quantiles must be ≥ recorded minimums
        let d_p50 = s.detect_us.lock().unwrap().value_at_quantile(0.50);
        let d_p99 = s.detect_us.lock().unwrap().value_at_quantile(0.99);
        assert!(d_p50 >= 50, "detect p50 {d_p50} < recorded min 50");
        assert!(d_p99 >= 50, "detect p99 {d_p99} < recorded min 50");

        let dp_p50 = s.dispatch_us.lock().unwrap().value_at_quantile(0.50);
        assert!(dp_p50 >= 200, "dispatch p50 {dp_p50} < recorded min 200");

        // Histogram substrings appear in the line
        assert!(
            line.contains("detect_p50="),
            "missing detect_p50= in: {line}"
        );
        assert!(
            line.contains("dispatch_p50="),
            "missing dispatch_p50= in: {line}"
        );
    }
}
