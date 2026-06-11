//! Exact numeric core (spec §4). Prices are native-tick integers, sizes are
//! micro-shares, cash is signed micro-USDC. Rounding is always against us.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TickSize {
    /// 0.01 markets — 100 levels per dollar.
    Cent,
    /// 0.001 markets — 1000 levels per dollar.
    Milli,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Px(u16);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Qty(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Usdc(pub i128);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Bps(pub i32);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NumError {
    OutOfRange,
}

pub const ONE_SHARE_MICRO: u64 = 1_000_000;
pub const ONE_USDC_MICRO: u64 = 1_000_000;

impl TickSize {
    pub const fn levels(self) -> u16 {
        match self {
            TickSize::Cent => 100,
            TickSize::Milli => 1000,
        }
    }
    pub const fn unit_microusdc(self) -> u64 {
        match self {
            TickSize::Cent => 10_000,
            TickSize::Milli => 1_000,
        }
    }
}

impl Px {
    /// Interior ticks only: 1 ..= levels-1. Prices 0 and 1 are not representable.
    pub const fn new(tick: u16, ts: TickSize) -> Result<Self, NumError> {
        if tick >= 1 && tick < ts.levels() {
            Ok(Px(tick))
        } else {
            Err(NumError::OutOfRange)
        }
    }
    pub const fn get(self) -> u16 {
        self.0
    }
    /// Price in µUSDC per share.
    pub const fn microusdc(self, ts: TickSize) -> u64 {
        self.0 as u64 * ts.unit_microusdc()
    }
}

/// ceil(n/d) for n >= 0, d > 0.
const fn div_ceil_i128(n: i128, d: i128) -> i128 {
    (n + d - 1) / d
}

/// Cash to BUY `qty` micro-shares at `px_micro` µUSDC/share. Rounds UP (against us).
/// Callers must pass px_micro ≤ 1_000_000 (the only intended producer is Px::microusdc).
pub fn buy_cost(px_micro: u64, qty: Qty) -> Usdc {
    debug_assert!(px_micro <= ONE_USDC_MICRO);
    Usdc(div_ceil_i128(px_micro as i128 * qty.0 as i128, ONE_SHARE_MICRO as i128))
}

/// Cash received SELLING `qty` micro-shares at `px_micro`. Rounds DOWN (against us).
/// Callers must pass px_micro ≤ 1_000_000 (the only intended producer is Px::microusdc).
pub fn sell_proceeds(px_micro: u64, qty: Qty) -> Usdc {
    debug_assert!(px_micro <= ONE_USDC_MICRO);
    Usdc((px_micro as i128 * qty.0 as i128) / ONE_SHARE_MICRO as i128)
}

/// Net edge in bps of basis, floored toward −∞. None unless basis > 0.
pub fn edge_bps(net: Usdc, basis: Usdc) -> Option<Bps> {
    if basis.0 <= 0 {
        return None;
    }
    let bps = (net.0 * 10_000).div_euclid(basis.0);
    Some(Bps(bps.clamp(i128::from(i32::MIN), i128::from(i32::MAX)) as i32))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn ticksize_levels_and_units() {
        assert_eq!(TickSize::Cent.levels(), 100);
        assert_eq!(TickSize::Milli.levels(), 1000);
        assert_eq!(TickSize::Cent.unit_microusdc(), 10_000);
        assert_eq!(TickSize::Milli.unit_microusdc(), 1_000);
    }

    #[test]
    fn px_rejects_boundary_ticks() {
        assert!(Px::new(0, TickSize::Cent).is_err());
        assert!(Px::new(100, TickSize::Cent).is_err());
        assert!(Px::new(1000, TickSize::Milli).is_err());
        assert_eq!(Px::new(46, TickSize::Cent).unwrap().get(), 46);
        assert!(Px::new(99, TickSize::Cent).is_ok());
        assert!(Px::new(999, TickSize::Milli).is_ok());
    }

    #[test]
    fn px_microusdc_value() {
        // 0.46 on a cent market = 460_000 µUSDC/share
        assert_eq!(Px::new(46, TickSize::Cent).unwrap().microusdc(TickSize::Cent), 460_000);
        // 0.046 on a milli market
        assert_eq!(Px::new(46, TickSize::Milli).unwrap().microusdc(TickSize::Milli), 46_000);
    }

    #[test]
    fn buy_cost_rounds_up_sell_proceeds_round_down() {
        // 3 micro-shares at 0.46: true value 1.38 µUSDC
        assert_eq!(buy_cost(460_000, Qty(3)).0, 2);
        assert_eq!(sell_proceeds(460_000, Qty(3)).0, 1);
        // exact multiples don't round
        assert_eq!(buy_cost(460_000, Qty(1_000_000)).0, 460_000);
        assert_eq!(sell_proceeds(460_000, Qty(1_000_000)).0, 460_000);
    }

    #[test]
    fn edge_bps_floors_and_rejects_nonpositive_basis() {
        assert_eq!(edge_bps(Usdc(2_000_000), Usdc(98_000_000)), Some(Bps(204))); // 204.08… → 204
        assert_eq!(edge_bps(Usdc(-1), Usdc(100)), Some(Bps(-100)));
        assert_eq!(edge_bps(Usdc(1), Usdc(0)), None);
        assert_eq!(edge_bps(Usdc(1), Usdc(-5)), None);
        // dust basis with huge net saturates instead of wrapping negative
        assert_eq!(edge_bps(Usdc(10_000_000_000_000), Usdc(1)), Some(Bps(i32::MAX)));
    }

    proptest! {
        #[test]
        fn rounding_is_against_us(px in 1u64..1_000_000, q in 0u64..10_000_000_000) {
            let true_num = px as i128 * q as i128; // value × 1e6
            let b = buy_cost(px, Qty(q)).0;
            let s = sell_proceeds(px, Qty(q)).0;
            prop_assert!(b * 1_000_000 >= true_num);
            prop_assert!(s * 1_000_000 <= true_num);
            prop_assert!(b - s <= 1); // they bracket the true value within one µUSDC
        }
    }
}
