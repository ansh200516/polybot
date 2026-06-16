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

#[cfg(test)]
use std::collections::VecDeque;

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
///
/// `PartialEq`/`Eq` (all fields are `Eq`): Task 3.2's `QuoteManager` diffs the
/// desired quote against what is resting to decide place/cancel/replace, mirroring
/// the value-type convention of `venue::Fill`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MakerOrder {
    pub token: TokenId,
    pub side: Side,
    pub price: Px,
    pub size: Qty,
    pub order_type: OrderType,
    pub post_only: bool,
}

/// A resting order as the venue reports it (`open_orders`). Carries the
/// venue-assigned [`OrderId`]; the remaining fields mirror the placed order's
/// identity. There is intentionally no `order_type`/`post_only` here: the
/// open-book view reports what is *resting now*, not how it was submitted.
///
/// `PartialEq`/`Eq`: lets `QuoteManager` reconciliation compare reported state
/// directly in tests + dedup logic.
#[derive(Clone, PartialEq, Eq, Debug)]
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
/// **All four methods take `&mut self`**, `open_orders` included: the live impl
/// (Task 3.3) routes every REST call — reads as well as writes — through a
/// `&mut self` rate limiter (`self.limiter.acquire()`), so a shared-`&self` read
/// would force interior mutability there. Keeping the read uniform with the
/// writes avoids that.
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

    /// List every currently-resting order. `&mut self` for parity with the
    /// rate-limited live read path (see the trait-level note).
    async fn open_orders(&mut self) -> Result<Vec<OpenOrder>, VenueError>;
}

// ---------------------------------------------------------------------------
// Test mock
// ---------------------------------------------------------------------------

/// In-memory [`MakerVenue`] for tests: records every accepted call and assigns
/// monotonically-incrementing [`OrderId`]s (`"mock-1"`, `"mock-2"`, …), so tests
/// (and Task 3.2's `QuoteManager`) can drive resting-order flows deterministically
/// without a live venue.
///
/// # Semantics
/// - **`cancel` of an unknown id** is a clean, idempotent no-op returning
///   `Ok(())` — it supports the reconnect path where an order the manager still
///   thinks is open was already filled/expired venue-side. The call is recorded.
/// - **`replace` of an unknown id** returns `Err(VenueError::Live(_))` and makes
///   NO change: a replace must hand back the new resting id, so minting one for an
///   order that isn't there would leave a phantom (the bug this guards against).
/// - **Error injection:** push a [`VenueError`] onto the matching `fail_*` queue
///   to force the next call to that method to fail *before* any state change or
///   recording. Queues drain front-first (one error per call), so a test can script
///   "fail once, then recover" reconnect behaviour deterministically.
/// - **Recorders** (`placed`/`cancelled`/`replaced`) capture only calls that
///   returned `Ok`; injected errors and the unknown-id `replace` error record nothing.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct MockMakerVenue {
    /// Currently-resting orders, in placement order.
    open: Vec<OpenOrder>,
    /// Monotonic id counter; pre-incremented so the first minted id is `mock-1`.
    next_id: u64,
    /// Every accepted `place` (orders as submitted), in call order.
    pub placed: Vec<MakerOrder>,
    /// Every accepted `cancel` (ids), in call order.
    pub cancelled: Vec<OrderId>,
    /// Every accepted `replace` (old id, new order), in call order.
    pub replaced: Vec<(OrderId, MakerOrder)>,
    /// Errors to inject into upcoming `place` calls (popped front-first).
    pub fail_place: VecDeque<VenueError>,
    /// Errors to inject into upcoming `cancel` calls.
    pub fail_cancel: VecDeque<VenueError>,
    /// Errors to inject into upcoming `replace` calls.
    pub fail_replace: VecDeque<VenueError>,
    /// Errors to inject into upcoming `open_orders` calls.
    pub fail_open_orders: VecDeque<VenueError>,
}

#[cfg(test)]
impl MockMakerVenue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a resting order as if left by a PRIOR session — the orphaned quotes
    /// that Task 3.5's startup/reconnect reconciliation discovers via
    /// [`open_orders`](MakerVenue::open_orders) and then cancels or adopts. Mints
    /// and returns a fresh venue id and rests the order, but records NOTHING in
    /// `placed` (this process did not place it), so a reconcile test can still
    /// assert "no venue calls" against the recorders.
    pub fn seed_open(&mut self, token: TokenId, side: Side, price: Px, size: Qty) -> OrderId {
        let id = self.mint_id();
        self.open.push(OpenOrder {
            id: id.clone(),
            token,
            side,
            price,
            size,
        });
        id
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
        if let Some(e) = self.fail_place.pop_front() {
            return Err(e);
        }
        self.placed.push(o.clone());
        let id = self.mint_id();
        self.rest(id.clone(), o);
        Ok(id)
    }

    async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError> {
        if let Some(e) = self.fail_cancel.pop_front() {
            return Err(e);
        }
        // Idempotent: cancelling an id that isn't resting is a clean no-op.
        self.open.retain(|o| &o.id != id);
        self.cancelled.push(id.clone());
        Ok(())
    }

    async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError> {
        if let Some(e) = self.fail_replace.pop_front() {
            return Err(e);
        }
        // A replace re-keys an EXISTING resting order; refuse unknown ids rather
        // than minting a phantom order with no predecessor on the book.
        if !self.open.iter().any(|existing| &existing.id == id) {
            return Err(VenueError::Live(format!(
                "replace: no resting order with id {}",
                id.0
            )));
        }
        self.open.retain(|existing| &existing.id != id);
        let new_id = self.mint_id();
        self.rest(new_id.clone(), o);
        self.replaced.push((id.clone(), o.clone()));
        Ok(new_id)
    }

    async fn open_orders(&mut self) -> Result<Vec<OpenOrder>, VenueError> {
        if let Some(e) = self.fail_open_orders.pop_front() {
            return Err(e);
        }
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

    #[tokio::test]
    async fn maker_mock_error_injection_and_unknown_id_semantics() {
        let mut v = MockMakerVenue::new();

        // --- Injected errors fail the next call BEFORE any state change or
        //     recording, then the queue drains and the venue recovers. ---
        v.fail_place.push_back(VenueError::Live("place rejected".into()));
        assert!(matches!(
            v.place(&gtc_bid(7, 44, 100_000_000)).await,
            Err(VenueError::Live(_))
        ));
        assert!(v.placed.is_empty());
        assert!(v.open_orders().await.unwrap().is_empty());

        // Queue drained → place now succeeds and rests.
        let id = v.place(&gtc_bid(7, 44, 100_000_000)).await.unwrap();
        assert_eq!(v.open_orders().await.unwrap().len(), 1);

        v.fail_cancel.push_back(VenueError::Live("cancel rejected".into()));
        assert!(matches!(v.cancel(&id).await, Err(VenueError::Live(_))));
        assert!(v.cancelled.is_empty());
        assert_eq!(v.open_orders().await.unwrap().len(), 1); // unchanged

        v.fail_replace
            .push_back(VenueError::Live("replace rejected".into()));
        assert!(matches!(
            v.replace(&id, &gtc_bid(7, 46, 50_000_000)).await,
            Err(VenueError::Live(_))
        ));
        assert!(v.replaced.is_empty());

        v.fail_open_orders
            .push_back(VenueError::Live("read failed".into()));
        assert!(matches!(v.open_orders().await, Err(VenueError::Live(_))));

        // --- Unknown-id semantics. ---
        let unknown = OrderId("nope".into());

        // replace of an unknown id errors and mints NO phantom order.
        assert!(matches!(
            v.replace(&unknown, &gtc_bid(7, 46, 50_000_000)).await,
            Err(VenueError::Live(_))
        ));
        assert!(v.replaced.is_empty());
        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id); // original still resting, untouched

        // cancel of an unknown id is a clean idempotent no-op (Ok), recorded.
        v.cancel(&unknown).await.unwrap();
        assert_eq!(v.open_orders().await.unwrap().len(), 1);
        assert_eq!(v.cancelled, vec![unknown]);
    }
}
