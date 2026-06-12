//! Instrument metadata handles. Venue-id interning happens in the registry (M2).

use crate::num::{Bps, TickSize};

/// Dense intern index assigned by the registry (M2). This is NOT the venue's
/// uint256 token id — those never enter the hot path.
/// u64 handle space: token count mirrors venue listings (a few thousand), but u64 keeps headroom trivially free.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TokenId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct MarketId(pub u32);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EventId(pub u32);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Market {
    pub id: MarketId,
    pub yes: TokenId,
    pub no: TokenId,
    pub tick: TickSize,
    pub fee_bps: Bps,
    pub neg_risk: bool,
}

/// A mutually-exclusive outcome set (spec §8 class 2). `yes_tokens[i]` and
/// `no_tokens[i]` belong to the same member market. Only sets with
/// `verified_exhaustive == true` are tradable by class 2.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Partition {
    pub event: EventId,
    pub markets: Vec<MarketId>,
    pub yes_tokens: Vec<TokenId>,
    pub no_tokens: Vec<TokenId>,
    pub verified_exhaustive: bool,
}

impl Partition {
    /// Structural sanity: ≥2 outcomes and parallel lanes of equal length.
    /// The registry (M2) is the sole production constructor and enforces this
    /// at build time; detectors must skip partitions that fail it.
    pub fn is_well_formed(&self) -> bool {
        let n = self.markets.len();
        n >= 2 && self.yes_tokens.len() == n && self.no_tokens.len() == n
    }
}

/// Approved logical relationships (spec §9), stated about market YES outcomes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Relationship {
    /// a true ⇒ b true.
    Implies { a: MarketId, b: MarketId },
    /// a and b cannot both be true.
    MutuallyExclusive { a: MarketId, b: MarketId },
    /// a true ⇔ b true.
    Equivalent { a: MarketId, b: MarketId },
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn part(n_markets: usize, n_yes: usize, n_no: usize) -> Partition {
        Partition {
            event: EventId(1),
            markets: (0..n_markets as u32).map(MarketId).collect(),
            yes_tokens: (0..n_yes as u64).map(TokenId).collect(),
            no_tokens: (100..100 + n_no as u64).map(TokenId).collect(),
            verified_exhaustive: true,
        }
    }

    #[test]
    fn well_formed_partition_passes() {
        assert!(part(2, 2, 2).is_well_formed());
        assert!(part(5, 5, 5).is_well_formed());
    }

    #[test]
    fn malformed_partitions_fail() {
        assert!(!part(2, 1, 2).is_well_formed()); // yes lane short
        assert!(!part(2, 2, 3).is_well_formed()); // no lane long
        assert!(!part(1, 1, 1).is_well_formed()); // single outcome
        assert!(!part(0, 0, 0).is_well_formed()); // empty
    }
}
