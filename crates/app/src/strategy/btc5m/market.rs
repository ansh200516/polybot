//! Which 5-min window is live right now, and the per-window state the shadow
//! loop needs (tokens, tick, strike snapshot). Rotation is time-driven; the
//! Gamma refresh is the strategy loop's job (this is the pure decision logic).

use pm_ingestion::gamma::GammaWindow;

/// A window the loop is actively shadowing: its Gamma identity + the strike we
/// snapshotted (our composite spot at adoption — a proxy for the Chainlink open).
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
    /// Adopt `gw` as current. New conditionId → snapshot `spot_now` as strike and
    /// return `true` (rotated); same window → keep existing strike, return `false`.
    pub fn adopt(&mut self, gw: GammaWindow, spot_now: f64) -> bool {
        let same = self.current.as_ref().map(|w| w.gamma.condition_id == gw.condition_id).unwrap_or(false);
        if same { return false; }
        self.current = Some(Window { gamma: gw, strike: spot_now });
        true
    }
    pub fn current(&self) -> Option<&Window> { self.current.as_ref() }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use pm_ingestion::gamma::GammaWindow;

    fn gw(cond: &str, open: i64, close: i64) -> GammaWindow {
        GammaWindow {
            condition_id: cond.into(),
            yes_token: "1".into(),
            no_token: "2".into(),
            tick_decimals: 2,
            t_open_ms: open,
            t_close_ms: close,
        }
    }

    #[test]
    fn adopts_window_and_snapshots_strike_once() {
        let mut r = Rotation::default();
        let changed = r.adopt(gw("A", 0, 300_000), 62_900.0);
        assert!(changed);
        assert_eq!(r.current().unwrap().gamma.condition_id, "A");
        assert_eq!(r.current().unwrap().strike, 62_900.0);
        assert!(!r.adopt(gw("A", 0, 300_000), 63_000.0));
        assert_eq!(r.current().unwrap().strike, 62_900.0);
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
