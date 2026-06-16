//! `SplitVenue` (Task 4.6): pair a [`MakerVenue`] with a SEPARATE
//! [`UserFillSource`] behind ONE value that is both.
//!
//! Live market-making's low-latency path places + manages resting orders on the
//! live CLOB ([`LiveVenue`](crate::live::LiveVenue) as the `MakerVenue`) but
//! sources fills from the user WS ([`LiveUserWsFills`](crate::user_ws::LiveUserWsFills)
//! as the `UserFillSource`) instead of the REST poll. The generic MM loop
//! (`run_mm_loop<V: MakerVenue + UserFillSource>`) needs a SINGLE `V` that is
//! both; `LiveVenue` is both (REST fills), but the WS feed is a distinct object.
//! `SplitVenue` glues the two together: `place`/`cancel`/`replace`/`open_orders`
//! delegate to the maker, `poll` delegates to the fills source — so the SAME
//! loop runs with `V = SplitVenue<LiveVenue, LiveUserWsFills>` (live maker orders
//! + WS fills) with zero loop changes.
//!
//! It is a generic, transport-agnostic adapter: any `MakerVenue` paired with any
//! `UserFillSource` (incl. test mocks) composes, which is exactly how the
//! delegation tests exercise it.

use crate::fills::{MakerFill, UserFillSource};
use crate::maker::{MakerOrder, MakerVenue, OpenOrder, OrderId};
use crate::venue::VenueError;

/// Pairs a `MakerVenue` `maker` with a `UserFillSource` `fills`. Implements BOTH
/// traits by delegation, so it is a drop-in `V` for the generic MM loop.
pub struct SplitVenue<M: MakerVenue, F: UserFillSource> {
    /// Handles place / cancel / replace / open_orders (the live CLOB).
    pub maker: M,
    /// Handles fill polling (the user-WS feed).
    pub fills: F,
}

impl<M: MakerVenue, F: UserFillSource> SplitVenue<M, F> {
    /// Pair a maker venue with a fills source.
    pub fn new(maker: M, fills: F) -> Self {
        SplitVenue { maker, fills }
    }
}

impl<M: MakerVenue, F: UserFillSource> MakerVenue for SplitVenue<M, F> {
    async fn place(&mut self, o: &MakerOrder) -> Result<OrderId, VenueError> {
        self.maker.place(o).await
    }

    async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError> {
        self.maker.cancel(id).await
    }

    async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError> {
        self.maker.replace(id, o).await
    }

    async fn open_orders(&mut self) -> Result<Vec<OpenOrder>, VenueError> {
        self.maker.open_orders().await
    }
}

impl<M: MakerVenue, F: UserFillSource> UserFillSource for SplitVenue<M, F> {
    async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError> {
        self.fills.poll().await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::fills::MockUserFills;
    use crate::maker::{MockMakerVenue, OrderType};
    use pm_core::book::Side;
    use pm_core::instrument::TokenId;
    use pm_core::num::{Px, Qty, TickSize};

    fn bid(tick: u16, size: u64) -> MakerOrder {
        MakerOrder {
            token: TokenId(7),
            side: Side::Bid,
            price: Px::new(tick, TickSize::Cent).unwrap(),
            size: Qty(size),
            order_type: OrderType::Gtc,
            post_only: true,
        }
    }

    fn fill(order_id: &str) -> MakerFill {
        MakerFill {
            order_id: OrderId(order_id.into()),
            token: TokenId(7),
            qty: Qty(1_000_000),
            px: Px::new(33, TickSize::Cent).unwrap(),
            trade_id: "t1".into(),
        }
    }

    /// place / cancel / replace / open_orders hit the MAKER mock; poll hits the
    /// FILLS mock — proving the two halves are wired to their own backends.
    #[tokio::test]
    async fn split_venue_delegates() {
        let maker = MockMakerVenue::new();
        // Two scripted fill batches for the fills side.
        let fills = MockUserFills::new(vec![vec![fill("0xa"), fill("0xb")], vec![]]);
        let mut sv = SplitVenue::new(maker, fills);

        // place → maker
        let id = sv.place(&bid(44, 100_000_000)).await.unwrap();
        assert_eq!(sv.maker.placed.len(), 1, "place recorded on the maker mock");
        assert_eq!(sv.maker.placed[0].price.get(), 44);

        // open_orders → maker (the one resting order)
        let open = MakerVenue::open_orders(&mut sv).await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id);

        // replace → maker (re-keys), then cancel → maker.
        let id2 = sv.replace(&id, &bid(46, 50_000_000)).await.unwrap();
        assert_eq!(sv.maker.replaced.len(), 1, "replace recorded on the maker mock");
        sv.cancel(&id2).await.unwrap();
        assert_eq!(sv.maker.cancelled, vec![id2], "cancel recorded on the maker mock");

        // poll → fills (the scripted batch), NOT the maker.
        let got = sv.poll().await.unwrap();
        assert_eq!(got.len(), 2, "poll drains the fills source");
        assert_eq!(got[0].order_id, OrderId("0xa".into()));
        assert_eq!(got[1].order_id, OrderId("0xb".into()));
        // Second poll: the scripted empty batch.
        assert!(sv.poll().await.unwrap().is_empty(), "fills source exhausted");
    }
}
