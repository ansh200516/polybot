//! Detection engine: sizing walker, arb classes 1–3, LP detector, dedup.

pub mod class1;
pub mod class2;
pub mod class3;
pub mod dedup;
pub mod lp;
pub mod walker;

use pm_core::instrument::{Market, MarketId, TokenId};
use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};

pub(crate) fn find_market(markets: &[Market], id: MarketId) -> Option<&Market> {
    markets.iter().find(|m| m.id == id)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ArbClass {
    C1Long,
    C1Short,
    C2Long,
    C2Short,
    C3Implies,
    C3MutEx,
    C3Equiv,
    C4Lp,
}

/// Our action on a book (we buy from asks, sell into bids).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Action {
    Buy,
    Sell,
}

/// One leg of a sized opportunity. `cash` is signed: negative = out (cost +
/// fee for buys), positive = in (proceeds − fee for sells).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LegFill {
    pub token: TokenId,
    pub action: Action,
    pub ts: TickSize,
    pub limit_px: Px,
    pub qty: Qty,
    pub cash: Usdc,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Opportunity {
    pub class: ArbClass,
    pub fills: Vec<LegFill>,
    /// Uniform basket size for classes 1–3 (each unit = 1 micro-share of every
    /// leg; equals every fill's qty). `Qty(0)` for `C4Lp`, whose legs are
    /// non-uniform — per-leg `fills[i].qty` are authoritative there.
    pub units: Qty,
    pub net: Usdc,
    pub basis: Usdc,
    pub edge: Bps,
    /// Complete-set splits execution MUST perform before the fills (market,
    /// units) — required for sell legs (e.g. C1Short). Empty for pure-buy
    /// baskets. Note C2Short needs no entry here: a full NO set redeems
    /// $(n−1) at resolution by itself; NegRisk conversion is an optional
    /// early-realization step (its gas is already charged conservatively).
    pub splits: Vec<(MarketId, Qty)>,
}

/// Per-operation Polygon gas estimates, µUSDC (spec §6; refined in M5).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct GasTable {
    pub split: u64,
    pub merge: u64,
    pub redeem: u64,
    pub negrisk_convert: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RedeemStrategy {
    Merge,
    Hold,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EngineParams {
    pub floor_c12: Bps,
    pub floor_c3: Bps,
    pub min_profit: Usdc,
    pub gas: GasTable,
    pub redeem: RedeemStrategy,
    /// Per-basket cash cap, µUSDC (spec §2 per-market cap).
    pub max_basis: Usdc,
    pub max_worlds: usize,
    pub cooldown_ms: u64,
    pub reemit_improvement_pct: u32,
}

impl Default for EngineParams {
    fn default() -> Self {
        EngineParams {
            floor_c12: Bps(30),
            floor_c3: Bps(100),
            min_profit: Usdc(1_000_000), // $1 dust filter (1_000_000 µUSDC = $1)
            gas: GasTable { split: 10_000, merge: 10_000, redeem: 15_000, negrisk_convert: 20_000 },
            redeem: RedeemStrategy::Merge,
            max_basis: Usdc(1_000_000_000), // $1k per-market cap, in µUSDC
            max_worlds: 4096,
            cooldown_ms: 2_000,
            reemit_improvement_pct: 20,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn defaults_match_spec_section_2_and_6() {
        let p = EngineParams::default();
        assert_eq!(p.floor_c12, Bps(30));
        assert_eq!(p.floor_c3, Bps(100));
        assert_eq!(p.min_profit, Usdc(1_000_000));
        assert_eq!(p.max_basis, Usdc(1_000_000_000));
        assert_eq!(p.max_worlds, 4096);
        assert_eq!(p.cooldown_ms, 2_000);
        assert_eq!(p.reemit_improvement_pct, 20);
        assert_eq!(p.redeem, RedeemStrategy::Merge);
        assert_eq!(p.gas.split, 10_000);
        assert_eq!(p.gas.merge, 10_000);
        assert_eq!(p.gas.redeem, 15_000);
        assert_eq!(p.gas.negrisk_convert, 20_000);
    }
}
