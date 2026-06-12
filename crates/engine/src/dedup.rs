//! Opportunity dedup: price-shape fingerprint + cooldown window (spec §11).

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use crate::Opportunity;

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
