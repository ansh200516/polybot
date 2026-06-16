//! Dense per-tick order-book ladders (spec §5).

use crate::num::{Px, Qty, TickSize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Debug)]
pub struct Ladder {
    ts: TickSize,
    side: Side,
    lvls: Box<[u64]>, // index = tick; 0 and levels() unused by Px's invariant
    best: Option<Px>,
}

impl Ladder {
    pub fn new(ts: TickSize, side: Side) -> Self {
        Ladder {
            ts,
            side,
            lvls: vec![0u64; ts.levels() as usize + 1].into_boxed_slice(),
            best: None,
        }
    }

    pub fn ts(&self) -> TickSize {
        self.ts
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn qty_at(&self, px: Px) -> Qty {
        Qty(self.lvls[px.get() as usize])
    }

    pub fn best(&self) -> Option<Px> {
        self.best
    }

    /// Replace the resting quantity at `px` (deltas arrive as absolute levels).
    pub fn set(&mut self, px: Px, qty: Qty) {
        let t = px.get();
        self.lvls[t as usize] = qty.0;
        let improves = |cand: u16, cur: u16| match self.side {
            Side::Bid => cand > cur,
            Side::Ask => cand < cur,
        };
        if qty.0 > 0 {
            if self.best.is_none_or(|b| improves(t, b.get())) {
                self.best = Some(px);
            }
        } else if self.best.map(Px::get) == Some(t) {
            self.best = self.rescan_from(t);
        }
    }

    /// Find the next non-empty level strictly worse than `from`.
    /// Worst case O(levels) — a cache-friendly sequential scan over ≤8 KB —
    /// but amortized O(1): a level only triggers a rescan once per time it
    /// became best, which required an insert that paid for it.
    fn rescan_from(&self, from: u16) -> Option<Px> {
        let t = match self.side {
            Side::Bid => (1..from).rev().find(|&i| self.lvls[i as usize] > 0),
            Side::Ask => ((from + 1)..self.ts.levels()).find(|&i| self.lvls[i as usize] > 0),
        };
        t.and_then(|i| Px::new(i, self.ts).ok())
    }

    /// Non-empty levels from best toward worse.
    pub fn iter_from_best(&self) -> impl Iterator<Item = (Px, Qty)> + '_ {
        let start = self.best.map(Px::get);
        let ascending = matches!(self.side, Side::Ask);
        LadderIter {
            ladder: self,
            cur: start,
            ascending,
        }
    }
}

struct LadderIter<'a> {
    ladder: &'a Ladder,
    cur: Option<u16>,
    ascending: bool,
}

impl<'a> Iterator for LadderIter<'a> {
    type Item = (Px, Qty);
    fn next(&mut self) -> Option<Self::Item> {
        let mut t = self.cur?;
        loop {
            let q = self.ladder.lvls[t as usize];
            let item = if q > 0 {
                Px::new(t, self.ladder.ts).ok().map(|p| (p, Qty(q)))
            } else {
                None
            };
            // advance
            let next = if self.ascending {
                if t + 1 < self.ladder.ts.levels() {
                    Some(t + 1)
                } else {
                    None
                }
            } else if t > 1 {
                Some(t - 1)
            } else {
                None
            };
            self.cur = next;
            if let Some(it) = item {
                return Some(it);
            }
            t = next?;
        }
    }
}

/// Two sides of one market token's book.
///
/// TODO(M2): ingestion wraps this with seq/hash integrity and `last_update`
/// staleness per spec §5; once those land, `bids`/`asks` should become
/// private with `apply()` as the sole mutation path.
#[derive(Clone, Debug)]
pub struct Book {
    pub bids: Ladder,
    pub asks: Ladder,
}

impl Book {
    pub fn new(ts: TickSize) -> Self {
        Book {
            bids: Ladder::new(ts, Side::Bid),
            asks: Ladder::new(ts, Side::Ask),
        }
    }

    pub fn ts(&self) -> TickSize {
        self.bids.ts()
    }

    pub fn apply(&mut self, side: Side, px: Px, qty: Qty) {
        match side {
            Side::Bid => self.bids.set(px, qty),
            Side::Ask => self.asks.set(px, qty),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn px(t: u16) -> Px {
        Px::new(t, TickSize::Cent).unwrap()
    }

    #[test]
    fn empty_ladder_has_no_best() {
        let l = Ladder::new(TickSize::Cent, Side::Ask);
        assert_eq!(l.best(), None);
        assert_eq!(l.qty_at(px(50)), Qty(0));
    }

    #[test]
    fn best_tracks_inserts_per_side() {
        let mut asks = Ladder::new(TickSize::Cent, Side::Ask);
        asks.set(px(60), Qty(5));
        asks.set(px(40), Qty(5));
        asks.set(px(50), Qty(5));
        assert_eq!(asks.best(), Some(px(40))); // lowest ask is best

        let mut bids = Ladder::new(TickSize::Cent, Side::Bid);
        bids.set(px(40), Qty(5));
        bids.set(px(60), Qty(5));
        bids.set(px(50), Qty(5));
        assert_eq!(bids.best(), Some(px(60))); // highest bid is best
    }

    #[test]
    fn zeroing_best_rescans_to_next_level() {
        let mut asks = Ladder::new(TickSize::Cent, Side::Ask);
        asks.set(px(40), Qty(5));
        asks.set(px(55), Qty(7));
        asks.set(px(40), Qty(0));
        assert_eq!(asks.best(), Some(px(55)));
        asks.set(px(55), Qty(0));
        assert_eq!(asks.best(), None);
    }

    #[test]
    fn iter_from_best_orders_correctly_and_skips_zeros() {
        let mut bids = Ladder::new(TickSize::Cent, Side::Bid);
        bids.set(px(30), Qty(1));
        bids.set(px(70), Qty(2));
        bids.set(px(50), Qty(3));
        let got: Vec<(u16, u64)> = bids.iter_from_best().map(|(p, q)| (p.get(), q.0)).collect();
        assert_eq!(got, vec![(70, 2), (50, 3), (30, 1)]);

        let mut asks = Ladder::new(TickSize::Cent, Side::Ask);
        asks.set(px(70), Qty(2));
        asks.set(px(30), Qty(1));
        let got: Vec<(u16, u64)> = asks.iter_from_best().map(|(p, q)| (p.get(), q.0)).collect();
        assert_eq!(got, vec![(30, 1), (70, 2)]);
    }

    #[test]
    fn book_apply_routes_sides() {
        let mut b = Book::new(TickSize::Cent);
        b.apply(Side::Bid, px(45), Qty(10));
        b.apply(Side::Ask, px(55), Qty(20));
        assert_eq!(b.bids.best(), Some(px(45)));
        assert_eq!(b.asks.best(), Some(px(55)));
    }

    #[test]
    fn qty_at_reflects_zeroing() {
        let mut l = Ladder::new(TickSize::Cent, Side::Ask);
        l.set(px(40), Qty(7));
        assert_eq!(l.qty_at(px(40)), Qty(7));
        l.set(px(40), Qty(0));
        assert_eq!(l.qty_at(px(40)), Qty(0));
    }

    #[test]
    fn clone_is_deep() {
        let mut a = Ladder::new(TickSize::Cent, Side::Ask);
        a.set(px(40), Qty(7));
        let b = a.clone();
        a.set(px(40), Qty(0));
        assert_eq!(b.qty_at(px(40)), Qty(7));
        assert_eq!(b.best(), Some(px(40)));
    }

    #[test]
    fn boundary_ticks_iterate() {
        let mut asks = Ladder::new(TickSize::Cent, Side::Ask);
        asks.set(px(1), Qty(1));
        asks.set(px(99), Qty(2));
        let got: Vec<u16> = asks.iter_from_best().map(|(p, _)| p.get()).collect();
        assert_eq!(got, vec![1, 99]);
        let mut bids = Ladder::new(TickSize::Cent, Side::Bid);
        bids.set(px(1), Qty(1));
        bids.set(px(99), Qty(2));
        let got: Vec<u16> = bids.iter_from_best().map(|(p, _)| p.get()).collect();
        assert_eq!(got, vec![99, 1]);
    }

    proptest! {
        /// Ladder behaves identically to a BTreeMap<u16, u64> reference model.
        #[test]
        fn matches_reference_model(
            ops in proptest::collection::vec((1u16..100, 0u64..1_000_000), 1..200),
            is_bid in proptest::bool::ANY,
        ) {
            let side = if is_bid { Side::Bid } else { Side::Ask };
            let mut ladder = Ladder::new(TickSize::Cent, side);
            let mut model: BTreeMap<u16, u64> = BTreeMap::new();
            for (t, q) in ops {
                ladder.set(px(t), Qty(q));
                if q == 0 { model.remove(&t); } else { model.insert(t, q); }

                let model_best = match side {
                    Side::Bid => model.keys().next_back().copied(),
                    Side::Ask => model.keys().next().copied(),
                };
                prop_assert_eq!(ladder.best().map(|p| p.get()), model_best);

                let want: Vec<(u16, u64)> = match side {
                    Side::Bid => model.iter().rev().map(|(k, v)| (*k, *v)).collect(),
                    Side::Ask => model.iter().map(|(k, v)| (*k, *v)).collect(),
                };
                let got: Vec<(u16, u64)> =
                    ladder.iter_from_best().map(|(p, q)| (p.get(), q.0)).collect();
                prop_assert_eq!(got, want);
            }
        }
    }
}
