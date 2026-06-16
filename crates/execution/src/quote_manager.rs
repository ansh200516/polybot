//! Per-strategy resting-quote bookkeeping + reconcile over [`MakerVenue`]
//! (spec §6, Phase 3 — Task 3.2).
//!
//! A [`QuoteManager`] is the venue-agnostic memory of "what this strategy
//! currently has resting on the book", keyed by `(TokenId, Side)` — one resting
//! quote per side per token, matching the market-making model of a single bid +
//! single ask per market. [`QuoteManager::reconcile`] diffs a *desired* set of
//! quotes against that memory and drives the venue (`place`/`cancel`/`replace`)
//! to converge, **idempotently**: re-reconciling an unchanged desired set issues
//! no venue calls, so we never churn the book (nor burn rate-limit budget).
//!
//! # Scope (YAGNI)
//! Bookkeeping + reconcile + `cancel_all`/`forget_all`/`tracked` ONLY. There is
//! deliberately **no** timing/cadence here (the strategy decides WHEN to
//! reconcile — spec §7 `quote_refresh_ms`), **no** rate limiting (the live venue
//! owns that — Task 3.3), and no fills / strategy loop. Nothing here runs until
//! the Phase-4 market-making strategy wires it up — it is inert.
//!
//! # On-error consistency contract
//! Any [`VenueError`] aborts the pass and propagates (`Err`), leaving the
//! tracking map in a state a subsequent `reconcile`/`cancel_all` can resume
//! from, because the map mutates ONLY after the venue call succeeds:
//! - a **failed `place`** records nothing — the key stays untracked;
//! - a **failed `replace`** keeps the OLD id tracked — the prior quote is still
//!   (believed) resting, so the next pass re-attempts the replace;
//! - a **failed `cancel`** keeps the id tracked — the next pass re-cancels it.
//!
//! Retry / reconnect orchestration lives in the MM strategy (Task 3.5).

use std::collections::{HashMap, HashSet};

use pm_core::book::Side;
use pm_core::instrument::TokenId;

use crate::maker::{MakerOrder, MakerVenue, OrderId};
use crate::venue::VenueError;

/// What we believe is resting for one `(TokenId, Side)` key: the venue-assigned
/// [`OrderId`] plus the [`MakerOrder`] we last submitted under it. The order is
/// retained so [`QuoteManager::reconcile`] can decide place / replace / no-op by
/// value-comparing the desired quote against it (`MakerOrder: Eq`).
#[derive(Clone, Debug)]
struct Resting {
    id: OrderId,
    order: MakerOrder,
}

/// Per-strategy tracker of resting maker quotes — one per `(TokenId, Side)`.
/// Venue-agnostic: every method is generic over any [`MakerVenue`] (the test
/// mock now; the live CLOB V2 venue and the Phase-4 paper sim later).
///
/// See the module docs for the reconcile algorithm and the on-error
/// consistency contract.
#[derive(Debug, Default)]
pub struct QuoteManager {
    resting: HashMap<(TokenId, Side), Resting>,
}

impl QuoteManager {
    /// A fresh tracker with nothing resting.
    pub fn new() -> Self {
        Self::default()
    }

    /// Drive `venue` until the resting book matches `desired` (one quote per
    /// `(token, side)`), updating the tracking map as calls succeed.
    /// **Idempotent**: a `desired` identical to what is tracked issues no venue
    /// calls.
    ///
    /// For each `desired` quote keyed by `(token, side)`:
    /// - untracked → [`place`](MakerVenue::place); record the returned id;
    /// - tracked but differing in price/size/type/post_only →
    ///   [`replace`](MakerVenue::replace) the old id; record the new id;
    /// - tracked and identical → no-op (don't churn the book).
    ///
    /// Every tracked key absent from `desired` is
    /// [`cancel`](MakerVenue::cancel)led and dropped.
    ///
    /// On any [`VenueError`] the pass aborts and returns `Err`, leaving the map
    /// per the module-level on-error contract.
    pub async fn reconcile<V: MakerVenue>(
        &mut self,
        venue: &mut V,
        desired: &[MakerOrder],
    ) -> Result<(), VenueError> {
        let desired_keys: HashSet<(TokenId, Side)> =
            desired.iter().map(|o| (o.token, o.side)).collect();

        // 1. Cancel + drop everything tracked that is no longer desired. Snapshot
        //    (key, id) first so no borrow of `self.resting` crosses an await.
        let stale: Vec<((TokenId, Side), OrderId)> = self
            .resting
            .iter()
            .filter(|(key, _)| !desired_keys.contains(*key))
            .map(|(key, resting)| (*key, resting.id.clone()))
            .collect();
        for (key, id) in stale {
            venue.cancel(&id).await?; // err → key stays tracked; next pass re-cancels
            self.resting.remove(&key);
        }

        // 2. Place / replace / no-op each desired quote.
        for o in desired {
            let key = (o.token, o.side);
            let old_id = match self.resting.get(&key) {
                // Identical to what's resting → idempotent no-op.
                Some(existing) if &existing.order == o => continue,
                // Tracked but changed → replace, re-keying to the new id.
                Some(existing) => Some(existing.id.clone()),
                // Nothing tracked → place fresh.
                None => None,
            };
            let new_id = match old_id {
                Some(old) => venue.replace(&old, o).await?, // err → OLD id retained
                None => venue.place(o).await?,              // err → nothing recorded
            };
            self.resting.insert(
                key,
                Resting {
                    id: new_id,
                    order: o.clone(),
                },
            );
        }

        Ok(())
    }

    /// Cancel every tracked order and clear the map — the flatten directive
    /// (spec §5). Issues one [`cancel`](MakerVenue::cancel) per tracked order,
    /// dropping each only after it succeeds.
    ///
    /// On a [`VenueError`] the pass aborts and returns `Err`; already-cancelled
    /// keys are gone while the failing key and any remaining keys stay tracked,
    /// so a retry re-cancels exactly those. After a full success the map is
    /// empty, so a second call is a no-op (double-cancel safe).
    pub async fn cancel_all<V: MakerVenue>(&mut self, venue: &mut V) -> Result<(), VenueError> {
        let all: Vec<((TokenId, Side), OrderId)> = self
            .resting
            .iter()
            .map(|(key, resting)| (*key, resting.id.clone()))
            .collect();
        for (key, id) in all {
            venue.cancel(&id).await?;
            self.resting.remove(&key);
        }
        Ok(())
    }

    /// Forget all tracked orders WITHOUT touching the venue. For the reconnect
    /// path (Task 3.5), where resting state is re-derived from a fresh
    /// `open_orders()` read, or to drop tracking after an out-of-band flatten.
    pub fn forget_all(&mut self) {
        self.resting.clear();
    }

    /// The current `(token, side) → OrderId` view, for tests and Task 3.5
    /// startup reconciliation against the venue's `open_orders()`.
    pub fn tracked(&self) -> HashMap<(TokenId, Side), OrderId> {
        self.resting
            .iter()
            .map(|(key, resting)| (*key, resting.id.clone()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::maker::{MockMakerVenue, OrderType};
    use pm_core::num::{Px, Qty, TickSize};

    fn px(tick: u16) -> Px {
        Px::new(tick, TickSize::Cent).unwrap()
    }

    fn quote(token: u64, side: Side, tick: u16, size: u64) -> MakerOrder {
        MakerOrder {
            token: TokenId(token),
            side,
            price: px(tick),
            size: Qty(size),
            order_type: OrderType::Gtc,
            post_only: true,
        }
    }

    fn bid(token: u64, tick: u16, size: u64) -> MakerOrder {
        quote(token, Side::Bid, tick, size)
    }

    fn ask(token: u64, tick: u16, size: u64) -> MakerOrder {
        quote(token, Side::Ask, tick, size)
    }

    fn tracked_id(qm: &QuoteManager, token: u64, side: Side) -> Option<OrderId> {
        qm.tracked().get(&(TokenId(token), side)).cloned()
    }

    #[tokio::test]
    async fn reconcile_places_then_idempotent() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();

        // First reconcile: a fresh bid + ask are both placed and tracked.
        let desired = vec![bid(7, 44, 100_000_000), ask(7, 46, 100_000_000)];
        qm.reconcile(&mut v, &desired).await.unwrap();
        assert_eq!(v.placed.len(), 2);
        assert_eq!(qm.tracked().len(), 2);
        assert_eq!(tracked_id(&qm, 7, Side::Bid), Some(OrderId("mock-1".into())));
        assert_eq!(tracked_id(&qm, 7, Side::Ask), Some(OrderId("mock-2".into())));

        // Reconciling the SAME desired set is a pure no-op: no new venue calls.
        qm.reconcile(&mut v, &desired).await.unwrap();
        assert_eq!(v.placed.len(), 2, "idempotent reconcile must not re-place");
        assert!(v.replaced.is_empty(), "idempotent reconcile must not replace");
        assert!(v.cancelled.is_empty(), "idempotent reconcile must not cancel");
        // Tracked ids are unchanged (book untouched).
        assert_eq!(tracked_id(&qm, 7, Side::Bid), Some(OrderId("mock-1".into())));
        assert_eq!(tracked_id(&qm, 7, Side::Ask), Some(OrderId("mock-2".into())));
    }

    #[tokio::test]
    async fn reconcile_replaces_changed_and_cancels_dropped() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();

        qm.reconcile(&mut v, &[bid(7, 44, 100_000_000), ask(7, 46, 100_000_000)])
            .await
            .unwrap();
        let ask_id = tracked_id(&qm, 7, Side::Ask).unwrap();

        // New desired: the bid's price changed (→ replace) and the ask is gone
        // (→ cancel). Nothing else.
        qm.reconcile(&mut v, &[bid(7, 45, 100_000_000)])
            .await
            .unwrap();

        // Bid was replaced: exactly one replace, the old id handed in, and the
        // tracked id moved to the venue's new id.
        assert_eq!(v.replaced.len(), 1);
        assert_eq!(v.replaced[0].0, OrderId("mock-1".into()));
        assert_eq!(v.replaced[0].1.price, px(45));
        assert_eq!(tracked_id(&qm, 7, Side::Bid), Some(OrderId("mock-3".into())));

        // Ask was cancelled and dropped from tracking.
        assert_eq!(v.cancelled, vec![ask_id]);
        assert_eq!(tracked_id(&qm, 7, Side::Ask), None);
        assert_eq!(qm.tracked().len(), 1);
    }

    #[tokio::test]
    async fn cancel_all_clears() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();

        qm.reconcile(&mut v, &[bid(7, 44, 100_000_000), ask(7, 46, 100_000_000)])
            .await
            .unwrap();
        assert_eq!(qm.tracked().len(), 2);

        // cancel_all issues a cancel per tracked order and empties the map.
        qm.cancel_all(&mut v).await.unwrap();
        assert_eq!(v.cancelled.len(), 2);
        assert!(v.cancelled.contains(&OrderId("mock-1".into())));
        assert!(v.cancelled.contains(&OrderId("mock-2".into())));
        assert!(qm.tracked().is_empty());

        // Calling it again is a no-op — no further venue calls (double-cancel safe).
        qm.cancel_all(&mut v).await.unwrap();
        assert_eq!(v.cancelled.len(), 2, "second cancel_all must be a no-op");
        assert!(qm.tracked().is_empty());
    }

    #[tokio::test]
    async fn forget_all_drops_tracking_without_touching_venue() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();
        qm.reconcile(&mut v, &[bid(7, 44, 100_000_000), ask(7, 46, 100_000_000)])
            .await
            .unwrap();
        assert_eq!(qm.tracked().len(), 2);

        // forget_all clears local tracking but issues NO cancels (unlike
        // cancel_all) — for the reconnect path that re-derives state from
        // open_orders().
        qm.forget_all();
        assert!(qm.tracked().is_empty());
        assert!(v.cancelled.is_empty(), "forget_all must not call the venue");
    }

    #[tokio::test]
    async fn reconcile_propagates_venue_error_and_keeps_consistent_state() {
        // --- Failed place records NOTHING. ---
        {
            let mut qm = QuoteManager::new();
            let mut v = MockMakerVenue::new();
            v.fail_place
                .push_back(VenueError::Live("place rejected".into()));

            let r = qm.reconcile(&mut v, &[bid(7, 44, 100_000_000)]).await;
            assert!(matches!(r, Err(VenueError::Live(_))));
            assert!(v.placed.is_empty());
            assert!(qm.tracked().is_empty(), "failed place must record nothing");
        }

        // --- Failed replace keeps the OLD id tracked. ---
        {
            let mut qm = QuoteManager::new();
            let mut v = MockMakerVenue::new();
            qm.reconcile(&mut v, &[bid(7, 44, 100_000_000)])
                .await
                .unwrap();
            let old_id = tracked_id(&qm, 7, Side::Bid).unwrap();

            v.fail_replace
                .push_back(VenueError::Live("replace rejected".into()));
            let r = qm.reconcile(&mut v, &[bid(7, 45, 100_000_000)]).await;
            assert!(matches!(r, Err(VenueError::Live(_))));
            assert!(v.replaced.is_empty());
            assert_eq!(
                tracked_id(&qm, 7, Side::Bid),
                Some(old_id),
                "failed replace must retain the OLD id"
            );
        }

        // --- Failed cancel keeps the id tracked (so a retry re-cancels). ---
        {
            let mut qm = QuoteManager::new();
            let mut v = MockMakerVenue::new();
            qm.reconcile(&mut v, &[bid(7, 44, 100_000_000)])
                .await
                .unwrap();
            let old_id = tracked_id(&qm, 7, Side::Bid).unwrap();

            v.fail_cancel
                .push_back(VenueError::Live("cancel rejected".into()));
            // Empty desired drops the bid → triggers the (failing) cancel.
            let r = qm.reconcile(&mut v, &[]).await;
            assert!(matches!(r, Err(VenueError::Live(_))));
            assert_eq!(
                tracked_id(&qm, 7, Side::Bid),
                Some(old_id),
                "failed cancel must keep the order tracked for retry"
            );
        }
    }
}
