//! Resting maker-order types + the `MakerVenue` trait (spec §6, Phase 3).
//!
//! This is the **foundation only** (Task 3.1): the data model
//! ([`MakerOrder`]/[`OpenOrder`]), the order-id newtype, the [`OrderType`]
//! time-in-force, and the async [`MakerVenue`] trait for resting
//! place/cancel/replace/open-orders. No live wire (Task 3.3), no fills
//! (Task 3.4), no `QuoteManager` (Task 3.2). Nothing here is exercised by the
//! running app until Phase 4 wires up market-making — it is inert.
//!
//! [`MakerVenue`] is a SEPARATE trait from the taker [`crate::venue::ExecutionVenue`]:
//! the taker path sends marketable fill-and-kill baskets, whereas this rests
//! `postOnly` GTC/GTD orders that sit on the book. They share [`VenueError`].

use pm_core::book::Side;
use pm_core::instrument::TokenId;
use pm_core::num::{Px, Qty};

use crate::venue::VenueError;

/// Time-in-force for a resting maker order (CLOB V2 `orderType`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OrderType {
    /// Good-til-cancelled: rests until explicitly cancelled.
    Gtc,
    /// Good-til-date: auto-expires at `expiry_ms` (unix epoch milliseconds).
    Gtd { expiry_ms: u64 },
}

/// The venue's order id for a resting order (CLOB `orderID`). An opaque,
/// venue-assigned string; we never parse it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct OrderId(pub String);

/// A resting maker order to place on the venue (spec §6). `post_only` rejects
/// any order that would cross (take) on arrival — makers pay 0 fees on CLOB V2,
/// so a maker strategy never wants to accidentally take.
#[derive(Clone, Debug)]
pub struct MakerOrder {
    pub token: TokenId,
    pub side: Side,
    pub price: Px,
    pub size: Qty,
    pub order_type: OrderType,
    pub post_only: bool,
}

/// A resting order as the venue reports it (`open_orders`). Carries the
/// venue-assigned [`OrderId`]; the rest mirrors the placed order's identity.
#[derive(Clone, Debug)]
pub struct OpenOrder {
    pub id: OrderId,
    pub token: TokenId,
    pub side: Side,
    pub price: Px,
    pub size: Qty,
}

/// Resting maker-order venue: place / cancel / replace `postOnly` GTC/GTD
/// orders and list the open book (spec §6). SEPARATE from the taker
/// [`crate::venue::ExecutionVenue`] (marketable FAK baskets).
///
/// `#[allow(async_fn_in_trait)]` + `: Send` mirror `ExecutionVenue`: known
/// in-crate implementors (the test mock now; the live CLOB V2 venue and the
/// Phase-4 paper sim later), driven from per-strategy async loops.
#[allow(async_fn_in_trait)]
pub trait MakerVenue: Send {
    /// Place a resting order; returns the venue-assigned order id.
    async fn place(&mut self, o: &MakerOrder) -> Result<OrderId, VenueError>;

    /// Cancel a resting order by id.
    async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError>;

    /// Replace `id` with a new resting order (cancel + repost). Returns the NEW
    /// id — the venue re-keys a replaced order.
    async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError>;

    /// List every currently-resting order.
    async fn open_orders(&self) -> Result<Vec<OpenOrder>, VenueError>;
}

// ---------------------------------------------------------------------------
// Test mock
// ---------------------------------------------------------------------------

/// In-memory [`MakerVenue`] for tests: records every call and assigns
/// monotonically-incrementing [`OrderId`]s (`"mock-1"`, `"mock-2"`, …), so
/// tests (and Task 3.2's `QuoteManager`) can drive resting-order flows
/// deterministically without a live venue.
///
/// `cancel`/`replace` of an unknown id are no-ops on the open book (idempotent,
/// matching the reconnect-idempotency QuoteManager will rely on); every call is
/// still recorded.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct MockMakerVenue {
    /// Currently-resting orders, in placement order.
    open: Vec<OpenOrder>,
    /// Monotonic id counter; pre-incremented so the first minted id is `mock-1`.
    next_id: u64,
    /// Every `place` call (orders as submitted), in call order.
    pub placed: Vec<MakerOrder>,
    /// Every `cancel` call (ids), in call order.
    pub cancelled: Vec<OrderId>,
    /// Every `replace` call (old id, new order), in call order.
    pub replaced: Vec<(OrderId, MakerOrder)>,
}

#[cfg(test)]
impl MockMakerVenue {
    pub fn new() -> Self {
        Self::default()
    }

    fn mint_id(&mut self) -> OrderId {
        self.next_id += 1;
        OrderId(format!("mock-{}", self.next_id))
    }

    fn rest(&mut self, id: OrderId, o: &MakerOrder) {
        self.open.push(OpenOrder {
            id,
            token: o.token,
            side: o.side,
            price: o.price,
            size: o.size,
        });
    }
}

#[cfg(test)]
impl MakerVenue for MockMakerVenue {
    async fn place(&mut self, o: &MakerOrder) -> Result<OrderId, VenueError> {
        self.placed.push(o.clone());
        let id = self.mint_id();
        self.rest(id.clone(), o);
        Ok(id)
    }

    async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError> {
        self.cancelled.push(id.clone());
        self.open.retain(|o| &o.id != id);
        Ok(())
    }

    async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError> {
        self.replaced.push((id.clone(), o.clone()));
        self.open.retain(|existing| &existing.id != id);
        let new_id = self.mint_id();
        self.rest(new_id.clone(), o);
        Ok(new_id)
    }

    async fn open_orders(&self) -> Result<Vec<OpenOrder>, VenueError> {
        Ok(self.open.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::TickSize;

    fn px(tick: u16) -> Px {
        Px::new(tick, TickSize::Cent).unwrap()
    }

    fn gtc_bid(token: u64, tick: u16, size: u64) -> MakerOrder {
        MakerOrder {
            token: TokenId(token),
            side: Side::Bid,
            price: px(tick),
            size: Qty(size),
            order_type: OrderType::Gtc,
            post_only: true,
        }
    }

    #[tokio::test]
    async fn maker_mock_records_place_cancel_replace() {
        let mut v = MockMakerVenue::new();

        // 1. Place a postOnly GTC bid: 100 shares @ 0.44 on token 7.
        let o = gtc_bid(7, 44, 100_000_000);
        let id = v.place(&o).await.unwrap();
        assert_eq!(id, OrderId("mock-1".into()));

        // open_orders lists exactly the placed order under the assigned id.
        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id);
        assert_eq!(open[0].token, TokenId(7));
        assert_eq!(open[0].side, Side::Bid);
        assert_eq!(open[0].price, px(44));
        assert_eq!(open[0].size, Qty(100_000_000));

        // 2. Replace it with new params (price 0.46, size 50 shares): the venue
        //    re-keys to a new id and the old order is gone.
        let o2 = gtc_bid(7, 46, 50_000_000);
        let id2 = v.replace(&id, &o2).await.unwrap();
        assert_ne!(id2, id);
        assert_eq!(id2, OrderId("mock-2".into()));

        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id2);
        assert_eq!(open[0].price, px(46));
        assert_eq!(open[0].size, Qty(50_000_000));

        // 3. Cancel the replacement → the book is empty.
        v.cancel(&id2).await.unwrap();
        assert!(v.open_orders().await.unwrap().is_empty());

        // 4. The recorded call log is precise.
        assert_eq!(v.placed.len(), 1);
        assert_eq!(v.placed[0].order_type, OrderType::Gtc);
        assert!(v.placed[0].post_only);
        assert_eq!(v.placed[0].price, px(44));

        assert_eq!(v.replaced.len(), 1);
        assert_eq!(v.replaced[0].0, id);
        assert_eq!(v.replaced[0].1.price, px(46));
        assert_eq!(v.replaced[0].1.size, Qty(50_000_000));

        assert_eq!(v.cancelled, vec![id2]);
    }
}
