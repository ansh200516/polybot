//! Pure taker-entry decision for the btc5m micro-taker (Phase 2). No I/O. Given
//! the leader (by z), its own token's ask, and the fair value, decide whether to
//! buy the near-certain leader as a marketable FAK, and at what size.
use pm_core::num::{Px, Qty, TickSize};

/// Parameters governing a Phase-2 entry (from config).
#[derive(Debug, Clone, Copy)]
pub struct EntryParams {
    pub entry_window_secs: i64,
    pub z_threshold: f64,
    pub edge_buffer: f64, // probability units
    pub fee_rate: f64,    // 0.07
    pub notional_usd: f64,
}

/// A decided entry: buy `up ? YES : NO` at `limit_px` (marketable = the ask), `qty` µshares.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Entry {
    pub up: bool,
    pub limit_px: Px,
    pub qty: Qty,
}

/// Decide a taker entry on the leader. `secs` = seconds-to-go; `z` = normalized
/// deviation (sign = leader: >0 UP, <0 DOWN); `fair_leader` = fair P(leader wins);
/// `leader_ask_micro` = the LEADER token's best ask in µUSDC (0 = no book). Returns
/// the buy on the leader's own token if it's offered ≥ `edge_buffer` below fair
/// net of the fee, sized to `notional_usd`. Pure; no I/O.
pub fn decide_entry(secs: i64, z: f64, fair_leader: f64, leader_ask_micro: i64, ts: TickSize, p: EntryParams) -> Option<Entry> {
    if secs <= 0 || secs > p.entry_window_secs { return None; }
    if !z.is_finite() || z.abs() < p.z_threshold { return None; }
    if leader_ask_micro <= 0 { return None; }
    let offer = leader_ask_micro as f64 / 1_000_000.0;
    if !(offer > 0.0 && offer < 1.0) { return None; }
    // Polymarket crypto (crypto_fees_v2) taker fee = rate·p·(1−p) — per the live
    // Gamma feeSchedule (rate 0.07) + docs. This DELIBERATELY differs from
    // pm_core::fees::fee_microusdc (rate·min(p,1−p), a different/older schedule);
    // do not "unify" them — these 5-min markets use p·(1−p). Matches btc5m_report.py.
    let fee = p.fee_rate * offer * (1.0 - offer);
    if !fair_leader.is_finite() || fair_leader - offer - fee < p.edge_buffer { return None; }
    let unit = ts.unit_microusdc();
    let ticks = (leader_ask_micro as u64) / unit;
    let limit_px = Px::new(ticks as u16, ts).ok()?;
    let shares = (p.notional_usd / offer).floor();
    if !(shares >= 1.0 && shares.is_finite()) { return None; }
    let qty = Qty((shares * 1_000_000.0) as u64);
    Some(Entry { up: z > 0.0, limit_px, qty })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn enters_a_cheap_late_leader() {
        let p = EntryParams { entry_window_secs: 20, z_threshold: 1.5, edge_buffer: 0.02, fee_rate: 0.07, notional_usd: 10.0 };
        let e = decide_entry(15, 2.0, 0.98, 900_000, TickSize::Cent, p).unwrap();
        assert!(e.up);
        assert_eq!(e.limit_px.get(), 90);
        assert_eq!(e.qty, Qty(11_000_000));
    }

    #[test]
    fn rejects_outside_window_thin_edge_and_no_book() {
        let p = EntryParams { entry_window_secs: 20, z_threshold: 1.5, edge_buffer: 0.02, fee_rate: 0.07, notional_usd: 10.0 };
        assert!(decide_entry(25, 2.0, 0.98, 900_000, TickSize::Cent, p).is_none()); // too early
        assert!(decide_entry(0, 2.0, 0.98, 900_000, TickSize::Cent, p).is_none());  // expired
        assert!(decide_entry(15, 1.0, 0.98, 900_000, TickSize::Cent, p).is_none()); // |z| < thresh
        assert!(decide_entry(15, 2.0, 0.995, 990_000, TickSize::Cent, p).is_none());// net edge < buffer
        assert!(decide_entry(15, 2.0, 0.98, 0, TickSize::Cent, p).is_none());       // no book
    }

    #[test]
    fn down_leader_when_z_negative() {
        let p = EntryParams { entry_window_secs: 20, z_threshold: 1.5, edge_buffer: 0.02, fee_rate: 0.07, notional_usd: 10.0 };
        // z<0 → DOWN leads; leader is the NO token, whose ask we pass as leader_ask_micro.
        let e = decide_entry(15, -2.0, 0.98, 900_000, TickSize::Cent, p).unwrap();
        assert!(!e.up);
        assert_eq!(e.qty, Qty(11_000_000));
    }
}
