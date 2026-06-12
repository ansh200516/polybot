//! Venue fee formula (spec §6). Symmetric in price, rounded up (against us).

use crate::num::{Bps, ONE_USDC_MICRO, Qty, Usdc};

/// Fee in µUSDC for a fill of `qty` micro-shares at `px_micro` µUSDC/share.
/// Polymarket's documented schedule shape: rate · min(p, 1−p) · size. Rounded UP.
/// Negative rates are treated as zero. Returns µUSDC; the levy asset (USDC vs.
/// shares) must be re-verified against live docs in M2 before reliance.
/// Callers must pass px_micro ≤ 1_000_000 (the only intended producer is Px::microusdc).
pub fn fee_microusdc(rate: Bps, px_micro: u64, qty: Qty) -> Usdc {
    debug_assert!(px_micro <= ONE_USDC_MICRO);
    let rate = i128::from(rate.0.max(0));
    let base = i128::from(px_micro.min(ONE_USDC_MICRO - px_micro));
    let num = rate * base * i128::from(qty.0);
    const DEN: i128 = 10_000 * ONE_USDC_MICRO as i128;
    Usdc((num + DEN - 1) / DEN)
}

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
    fn negative_rate_is_free() {
        assert_eq!(fee_microusdc(Bps(-1), 500_000, Qty(1_000_000)), Usdc(0));
    }

    #[test]
    fn golden_value() {
        // 200 bps on 10 shares at 0.40: 0.02 × 0.40 × 10 = $0.08 = 80_000 µUSDC
        assert_eq!(
            fee_microusdc(Bps(200), 400_000, Qty(10_000_000)),
            Usdc(80_000)
        );
    }

    #[test]
    fn symmetric_in_price() {
        for (p, q) in [
            (10_000u64, 3_333_333u64),
            (250_000, 1),
            (990_000, 7_777_777),
        ] {
            assert_eq!(
                fee_microusdc(Bps(150), p, Qty(q)),
                fee_microusdc(Bps(150), 1_000_000 - p, Qty(q))
            );
        }
    }

    #[test]
    fn ceil_rounds_against_us() {
        // 1 bp on 1 micro-share at 0.50: true fee = 0.00005 µ → ceil → 1 µ
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

        #[test]
        fn symmetric_in_price_proptest(rate in 0i32..500, p in 1u64..500_000, q in 0u64..100_000_000_000) {
            prop_assert_eq!(
                fee_microusdc(Bps(rate), p, Qty(q)),
                fee_microusdc(Bps(rate), 1_000_000 - p, Qty(q))
            );
        }
    }
}
