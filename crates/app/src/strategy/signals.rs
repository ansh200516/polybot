//! Per-token rolling signal state for Spec-2 adverse-selection avoidance.
//! Holds recent (ts_ms, microprice) samples within a window and derives a
//! short-term momentum signal. Pure (time is passed in); no I/O.

use std::collections::VecDeque;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SignalState {
    window_ms: u128,
    samples: VecDeque<(i64, f64)>, // (ts_ms, microprice), oldest..newest
}

impl SignalState {
    pub fn new(window: Duration) -> Self {
        SignalState { window_ms: window.as_millis(), samples: VecDeque::new() }
    }

    /// Record a microprice sample at `ts_ms` and evict samples older than the window.
    pub fn observe(&mut self, ts_ms: i64, microprice: f64) {
        self.samples.push_back((ts_ms, microprice));
        self.evict(ts_ms);
    }

    fn evict(&mut self, now_ms: i64) {
        while let Some(&(ts, _)) = self.samples.front() {
            if (now_ms - ts) as u128 > self.window_ms {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Relative microprice change over the in-window samples: (newest-oldest)/oldest.
    /// 0.0 when fewer than 2 samples are in the window or oldest is non-positive.
    pub fn momentum(&self, now_ms: i64) -> f64 {
        // Consider only samples within the window relative to now_ms.
        let in_win: Vec<f64> = self.samples.iter()
            .filter(|(ts, _)| (now_ms - *ts) as u128 <= self.window_ms)
            .map(|(_, p)| *p).collect();
        if in_win.len() < 2 { return 0.0; }
        let oldest = in_win[0];
        // `len() >= 2` guaranteed above, so the last index is valid (avoids
        // `.unwrap()`, which is denied crate-wide via `clippy::unwrap_used`).
        let newest = in_win[in_win.len() - 1];
        if oldest <= 0.0 { 0.0 } else { (newest - oldest) / oldest }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn momentum_tracks_recent_microprice_change_in_window() {
        let mut s = SignalState::new(std::time::Duration::from_millis(3000));
        s.observe(0, 0.50);
        s.observe(1000, 0.51);
        s.observe(2000, 0.53);
        assert!(s.momentum(2000) > 0.0); // upward move within window
        // a sample older than the window is dropped: at t=6000 only the 6000 sample remains
        s.observe(6000, 0.53);
        assert!((s.momentum(6000)).abs() < 1e-9, "stale samples evicted -> <2 in window -> 0");
    }

    #[test]
    fn momentum_negative_on_down_move_and_zero_for_single_sample() {
        // Steady down-move across in-window samples -> negative momentum.
        let mut s = SignalState::new(std::time::Duration::from_millis(3000));
        s.observe(0, 0.60);
        s.observe(1000, 0.55);
        s.observe(2000, 0.50);
        assert!(s.momentum(2000) < 0.0, "downward move within window -> negative");

        // A single in-window sample is <2 -> momentum is 0.0.
        let mut one = SignalState::new(std::time::Duration::from_millis(3000));
        one.observe(0, 0.50);
        assert!((one.momentum(0)).abs() < 1e-9, "single sample -> 0");
    }
}
