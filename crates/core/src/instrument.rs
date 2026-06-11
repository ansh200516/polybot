//! Instrument metadata handles. Venue-id interning happens in the registry (M2).

use crate::num::{Bps, TickSize};

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

/// Approved logical relationships (spec §9), stated about market YES outcomes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

    #[test]
    fn partition_lanes_stay_parallel() {
        let p = Partition {
            event: EventId(1),
            markets: vec![MarketId(1), MarketId(2)],
            yes_tokens: vec![TokenId(10), TokenId(20)],
            no_tokens: vec![TokenId(11), TokenId(21)],
            verified_exhaustive: true,
        };
        assert_eq!(p.markets.len(), p.yes_tokens.len());
        assert_eq!(p.markets.len(), p.no_tokens.len());
    }
}
