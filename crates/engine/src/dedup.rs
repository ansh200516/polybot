//! Opportunity dedup: price-shape fingerprint + cooldown window (spec §11).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::Opportunity;

/// FNV-1a 64-bit over a canonical byte encoding. Deliberately hand-rolled:
/// the value is persisted (spec §16 opportunities table) and must be stable
/// across processes, architectures, and Rust releases — std's DefaultHasher
/// guarantees none of those.
fn fnv1a(bytes: impl IntoIterator<Item = u8>) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Fingerprint(u64);

impl Opportunity {
    /// Price-shape identity: class + sorted (token, action, limit px).
    /// Size deliberately excluded (spec §11). Stable across processes and
    /// releases — safe to persist.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut legs: Vec<(u64, bool, u16)> = self
            .fills
            .iter()
            .map(|f| (f.token.0, matches!(f.action, crate::Action::Buy), f.limit_px.get()))
            .collect();
        legs.sort_unstable();
        let class_byte = match self.class {
            crate::ArbClass::C1Long => 1u8,
            crate::ArbClass::C1Short => 2,
            crate::ArbClass::C2Long => 3,
            crate::ArbClass::C2Short => 4,
            crate::ArbClass::C3Implies => 5,
            crate::ArbClass::C3MutEx => 6,
            crate::ArbClass::C3Equiv => 7,
            crate::ArbClass::C4Lp => 8,
        };
        let bytes = std::iter::once(class_byte).chain(legs.iter().flat_map(|(t, buy, px)| {
            t.to_le_bytes()
                .into_iter()
                .chain(std::iter::once(u8::from(*buy)))
                .chain(px.to_le_bytes())
        }));
        Fingerprint(fnv1a(bytes))
    }
}

/// Windowed suppression of repeat opportunities, keyed by fingerprint.
pub struct Cooldown {
    window: Duration,
    improvement_pct: u32,
    seen: HashMap<Fingerprint, (Instant, i128)>,
}

const PURGE_THRESHOLD: usize = 1024;

impl Cooldown {
    /// `window` = suppression window; `improvement_pct` = integer percent (20 = 20%) by which net must improve to re-emit inside the window. Note: for net below 100/pct µUSDC the integer bar truncates to "any improvement" — irrelevant above the $1 min_profit dust filter.
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

    fn op_with_class(net: i128, px: u16, qty: u64, class: ArbClass) -> Opportunity {
        Opportunity {
            class,
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
        // Add many unique entries to trigger PURGE_THRESHOLD (1024).
        // Vary class and px (and action) to create > 1024 distinct fingerprints.
        // px must be in range [1, 99] for TickSize::Cent.
        // We need 8 classes × 99 px × 2 actions = 1584 combinations.
        let classes = [
            ArbClass::C1Long, ArbClass::C1Short, ArbClass::C2Long, ArbClass::C2Short,
            ArbClass::C3Implies, ArbClass::C3MutEx, ArbClass::C3Equiv, ArbClass::C4Lp,
        ];
        let mut idx = 0u16;
        for class in &classes {
            for px in 1u16..=99 {
                for action_flag in 0..2 {
                    let mut op_inner = op_with_class(1_000_000, px, u64::from(idx) + 1, *class);
                    if action_flag == 1 && !op_inner.fills.is_empty() {
                        op_inner.fills[0].action = crate::Action::Sell;
                    }
                    let _ = cd.admit(t0, &op_inner);
                    idx += 1;
                    if idx > 1500 {
                        break;
                    }
                }
                if idx > 1500 {
                    break;
                }
            }
            if idx > 1500 {
                break;
            }
        }
        // All entries are now expired (10ms > 1ms window).
        // Next admit triggers the purge, which removes all expired entries.
        assert!(cd.admit(t0 + Duration::from_millis(10), &op(1_000_000, 46, 100)));
        assert!(cd.tracked() <= 1);
    }

    #[test]
    fn fingerprint_is_a_stable_function_of_shape() {
        // Pinned value: if this changes, persisted fingerprints (spec §16)
        // would be orphaned — bump consciously with a store migration.
        let fp = op(1_000_000, 46, 100).fingerprint();
        assert_eq!(fp, op(2_000_000, 46, 999).fingerprint());
        let crate::dedup::Fingerprint(raw) = fp;
        assert_eq!(raw, 0x43f68069009a5663_u64);
    }
}
