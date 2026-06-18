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
//! [`reconcile`](QuoteManager::reconcile) does NOT abort on the first
//! [`VenueError`]: a single rejected order must not block every OTHER market's
//! quote. It CONTINUES past each per-order failure and returns the FIRST error
//! at the end (`cancel_all` still aborts — flatten is all-or-nothing). The map
//! mutates ONLY after the venue call succeeds, so each failed call leaves
//! consistent, resumable state:
//! - a **failed `place`** records nothing — the key stays untracked;
//! - a **failed `replace`** keeps the OLD id tracked — the prior quote is still
//!   (believed) resting, so the next pass re-attempts the replace;
//! - a **failed `cancel`** keeps the id tracked — the next pass re-cancels it.
//!
//! So a returned `Err` means "≥1 order failed (state still consistent), and the
//! orders that COULD go up did". Retry / reconnect orchestration lives in the MM
//! strategy (Task 3.5).

use std::collections::{HashMap, HashSet};

use pm_core::book::Side;
use pm_core::instrument::TokenId;
use pm_core::num::{Px, Qty};

use crate::maker::{MakerOrder, MakerVenue, OrderId, OrderType};
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
    /// A [`VenueError`] on any single order is logged-by-skipping: the pass
    /// CONTINUES (other orders still place/cancel) and returns the FIRST error,
    /// leaving each failed order's tracking consistent per the module-level
    /// on-error contract. So `Err` means "≥1 order failed; the rest still went up".
    pub async fn reconcile<V: MakerVenue>(
        &mut self,
        venue: &mut V,
        desired: &[MakerOrder],
    ) -> Result<(), VenueError> {
        let desired_keys: HashSet<(TokenId, Side)> =
            desired.iter().map(|o| (o.token, o.side)).collect();

        // A single order's rejection (balance/allowance, a transient venue error,
        // a momentarily-crossing post-only) must NOT block every OTHER market's
        // quote — so we CONTINUE past per-order failures and return the FIRST one
        // at the end. Per-order state stays consistent exactly as before (a failed
        // place records nothing; a failed replace/cancel keeps the id tracked), so
        // the next pass retries precisely the failed orders while the rest stay up.
        let mut first_err: Option<VenueError> = None;

        // 1. Cancel + drop everything tracked that is no longer desired. Snapshot
        //    (key, id) first so no borrow of `self.resting` crosses an await.
        let stale: Vec<((TokenId, Side), OrderId)> = self
            .resting
            .iter()
            .filter(|(key, _)| !desired_keys.contains(*key))
            .map(|(key, resting)| (*key, resting.id.clone()))
            .collect();
        for (key, id) in stale {
            match venue.cancel(&id).await {
                Ok(()) => {
                    self.resting.remove(&key);
                }
                // err → key stays tracked; next pass re-cancels. Don't drop it.
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
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
            let result = match &old_id {
                Some(old) => venue.replace(old, o).await, // err → OLD id retained
                None => venue.place(o).await,             // err → nothing recorded
            };
            match result {
                Ok(new_id) => {
                    self.resting.insert(
                        key,
                        Resting {
                            id: new_id,
                            order: o.clone(),
                        },
                    );
                }
                // Per-contract on failure: a fresh place recorded nothing, and a
                // replace leaves the OLD id tracked (we never touched `self.resting`
                // for `key`). Skip this order and keep going.
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
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

    /// Adopt an order discovered resting on the venue into local tracking
    /// WITHOUT any venue call, so the manager believes that `(token, side)` quote
    /// is already on the book and a subsequent [`reconcile`](Self::reconcile)
    /// won't double-place it. Inserts (overwriting any existing entry for the
    /// key) the venue-assigned `id` plus a reconstructed [`MakerOrder`].
    ///
    /// The venue's open-book view ([`OpenOrder`](crate::maker::OpenOrder)) reports
    /// only token/side/price/size — never `order_type`/`post_only` — so the
    /// adopted order is reconstructed with the market-maker's standard shape
    /// ([`OrderType::Gtc`], `post_only = true`). If the live desired quote later
    /// differs from this reconstruction (a different price/size, or a
    /// non-standard shape), the next [`reconcile`](Self::reconcile) simply
    /// [`replace`](MakerVenue::replace)s it — safe normalization at the cost of a
    /// single replace. Used by the startup/reconnect reconciliation primitive
    /// (`crate::reconcile`) and by a strategy resuming after a bare reconnect.
    pub fn adopt(&mut self, token: TokenId, side: Side, id: OrderId, price: Px, size: Qty) {
        self.resting.insert(
            (token, side),
            Resting {
                id,
                order: MakerOrder {
                    token,
                    side,
                    price,
                    size,
                    order_type: OrderType::Gtc,
                    post_only: true,
                },
            },
        );
    }

    /// Sync tracking with a [`MakerFill`](crate::fills::MakerFill) the strategy
    /// just booked: decrement the matching resting order's remaining size by
    /// `filled`, and DROP the `(token, side)` entry entirely once fully filled.
    ///
    /// The manager is otherwise UNAWARE of fills — they arrive out-of-band via
    /// [`UserFillSource::poll`](crate::fills::UserFillSource), and the venue
    /// removes a fully-filled resting order. Without this, the manager would keep
    /// "tracking" a now-gone id, so [`reconcile`](Self::reconcile) would NO-OP an
    /// identical next desired forever (it believes the quote is still resting) —
    /// the tracked set drifts from the venue's real resting set and the strategy
    /// STOPS re-quoting a filled market. Calling `note_fill` per booked fill
    /// keeps the two in sync: a fully-filled side is dropped so the next
    /// `reconcile` re-places it; a partial fill leaves the side tracked with a
    /// reduced size so the next `reconcile` tops it back up (a replace).
    ///
    /// Matches `id` against the tracked orders (one per `(token, side)`); a pure
    /// no-op if `id` isn't tracked (already replaced / cancelled / consumed), so
    /// a duplicate or late fill is harmless.
    pub fn note_fill(&mut self, id: &OrderId, filled: Qty) {
        let Some(key) = self
            .resting
            .iter()
            .find(|(_, r)| &r.id == id)
            .map(|(key, _)| *key)
        else {
            return; // not tracked → nothing to sync (idempotent on dup/late fills)
        };
        if let Some(r) = self.resting.get_mut(&key) {
            let remaining = r.order.size.0.saturating_sub(filled.0);
            if remaining == 0 {
                self.resting.remove(&key);
            } else {
                r.order.size = Qty(remaining);
            }
        }
    }

    /// The current `(token, side) → OrderId` view, for tests and Task 3.5
    /// startup reconciliation against the venue's `open_orders()`.
    pub fn tracked(&self) -> HashMap<(TokenId, Side), OrderId> {
        self.resting
            .iter()
            .map(|(key, resting)| (*key, resting.id.clone()))
            .collect()
    }

    /// A snapshot of every resting quote with its FULL detail (the
    /// [`MakerOrder`] — token/side/price/size) plus its venue [`OrderId`].
    /// Unlike [`tracked`](Self::tracked) (ids only) this carries the price/size
    /// the dashboard's open-orders panel renders. Order is unspecified.
    pub fn resting_orders(&self) -> Vec<(OrderId, MakerOrder)> {
        self.resting
            .values()
            .map(|r| (r.id.clone(), r.order.clone()))
            .collect()
    }

    /// Cancel the single resting quote at `(token, side)`, dropping it only after
    /// the venue confirms. A no-op `Ok(())` when nothing is tracked there (so a
    /// double-cancel or a cancel of an already-filled order is harmless). Used by
    /// the dashboard's per-order cancel: it pulls exactly that one quote without
    /// disturbing the others (vs [`cancel_all`](Self::cancel_all)).
    pub async fn cancel_one<V: MakerVenue>(
        &mut self,
        venue: &mut V,
        token: TokenId,
        side: Side,
    ) -> Result<(), VenueError> {
        if let Some(r) = self.resting.get(&(token, side)) {
            let id = r.id.clone();
            venue.cancel(&id).await?;
            self.resting.remove(&(token, side));
        }
        Ok(())
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
    async fn cancel_one_pulls_only_that_side_and_resting_orders_snapshots_detail() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();
        qm.reconcile(&mut v, &[bid(7, 44, 100_000_000), ask(7, 46, 100_000_000)])
            .await
            .unwrap();

        // resting_orders() carries the FULL detail (price/size), not just ids.
        let snap = qm.resting_orders();
        assert_eq!(snap.len(), 2);
        let bid_row = snap.iter().find(|(_, o)| o.side == Side::Bid).unwrap();
        assert_eq!(bid_row.1.price, px(44));
        assert_eq!(bid_row.1.size, Qty(100_000_000));

        // cancel_one pulls ONLY the bid; the ask stays resting.
        let bid_id = tracked_id(&qm, 7, Side::Bid).unwrap();
        qm.cancel_one(&mut v, TokenId(7), Side::Bid).await.unwrap();
        assert_eq!(v.cancelled, vec![bid_id], "exactly the bid was cancelled");
        assert_eq!(tracked_id(&qm, 7, Side::Bid), None);
        assert_eq!(
            tracked_id(&qm, 7, Side::Ask),
            Some(OrderId("mock-2".into())),
            "the ask is untouched"
        );

        // Cancelling a side that isn't tracked is a no-op Ok (idempotent).
        qm.cancel_one(&mut v, TokenId(7), Side::Bid).await.unwrap();
        assert_eq!(v.cancelled.len(), 1, "no extra cancel for an absent side");
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
    async fn note_fill_decrements_partial_then_drops_full_so_reconcile_requotes() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();

        // Rest a 100-share bid.
        let desired = vec![bid(7, 44, 100_000_000)];
        qm.reconcile(&mut v, &desired).await.unwrap();
        let id1 = tracked_id(&qm, 7, Side::Bid).unwrap();

        // A PARTIAL fill (40 sh) leaves the side tracked under the SAME id with a
        // reduced size — so re-reconciling the FULL desired tops it back up (a
        // replace, not a no-op).
        qm.note_fill(&id1, Qty(40_000_000));
        assert_eq!(
            tracked_id(&qm, 7, Side::Bid),
            Some(id1.clone()),
            "a partial fill keeps the side tracked"
        );
        qm.reconcile(&mut v, &desired).await.unwrap();
        assert_eq!(v.replaced.len(), 1, "a partial fill tops the quote back to full size");
        let id2 = tracked_id(&qm, 7, Side::Bid).unwrap();
        assert_ne!(id2, id1, "the top-up replace re-keyed to a new id");

        // A FULL fill DROPS the side from tracking entirely...
        qm.note_fill(&id2, Qty(100_000_000));
        assert!(
            tracked_id(&qm, 7, Side::Bid).is_none(),
            "a full fill drops the side from tracking"
        );

        // ...so the next reconcile RE-PLACES it (the drift fix: a filled market is
        // re-quoted, never no-oped forever against a now-gone resting order).
        let placed_before = v.placed.len();
        qm.reconcile(&mut v, &desired).await.unwrap();
        assert_eq!(
            v.placed.len(),
            placed_before + 1,
            "the fully-filled side must be re-placed, not no-oped"
        );
        assert!(tracked_id(&qm, 7, Side::Bid).is_some(), "re-quoted and tracked again");

        // note_fill for an UNKNOWN id (already replaced / cancelled / a dup or
        // late fill) is a pure no-op — never panics, never touches other keys.
        let before = qm.tracked();
        qm.note_fill(&OrderId("nope".into()), Qty(1));
        qm.note_fill(&id1, Qty(1)); // the old (replaced-away) id
        assert_eq!(qm.tracked(), before, "an untracked id is a no-op");
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

    /// The "resumable" guarantee across MULTIPLE keys: when an earlier venue
    /// call in a pass succeeds and a LATER one fails, the earlier mutation must
    /// persist (the pass is partially applied, not rolled back), so the next
    /// pass converges from there. Here the stale ask is cancelled (succeeds)
    /// and then the changed bid's replace fails — the cancel must stick.
    #[tokio::test]
    async fn reconcile_partial_progress_persists_when_later_call_fails() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();
        qm.reconcile(&mut v, &[bid(7, 44, 100_000_000), ask(7, 46, 100_000_000)])
            .await
            .unwrap();
        let ask_id = tracked_id(&qm, 7, Side::Ask).unwrap();
        let bid_old = tracked_id(&qm, 7, Side::Bid).unwrap();

        // New desired: ask dropped (→ cancel, succeeds first), bid changed
        // (→ replace, made to fail). reconcile cancels stale keys before
        // placing/replacing, so the cancel lands before the failing replace.
        v.fail_replace
            .push_back(VenueError::Live("replace rejected".into()));
        let r = qm
            .reconcile(&mut v, &[bid(7, 45, 100_000_000)])
            .await;
        assert!(matches!(r, Err(VenueError::Live(_))));

        // Earlier success persisted: the ask really was cancelled and dropped.
        assert_eq!(v.cancelled, vec![ask_id]);
        assert_eq!(tracked_id(&qm, 7, Side::Ask), None);
        // Later failure left the bid's OLD id tracked for the next pass.
        assert!(v.replaced.is_empty());
        assert_eq!(tracked_id(&qm, 7, Side::Bid), Some(bid_old));
        assert_eq!(qm.tracked().len(), 1);
    }

    /// RESILIENCE: a single rejected order must NOT block the others. The first
    /// place fails, but the SECOND market's quote still goes up; reconcile returns
    /// the (first) error so the caller still knows, and per-order state is
    /// consistent (the failed order is untracked, the placed one is tracked).
    #[tokio::test]
    async fn reconcile_continues_past_a_failed_order() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();
        v.fail_place
            .push_back(VenueError::Live("token-7 place rejected".into()));

        // Two markets desired; the first place (token 7) fails — the second
        // (token 8) must STILL be placed (the old code aborted the whole pass).
        let r = qm
            .reconcile(&mut v, &[bid(7, 44, 100_000_000), bid(8, 30, 100_000_000)])
            .await;
        assert!(matches!(r, Err(VenueError::Live(_))), "the first error is surfaced");
        assert_eq!(tracked_id(&qm, 7, Side::Bid), None, "the failed order is not tracked");
        assert!(
            tracked_id(&qm, 8, Side::Bid).is_some(),
            "a later order still goes up despite the earlier failure"
        );
        assert_eq!(v.placed.len(), 1, "only the order that succeeded was placed");
    }

    // ── Task 3.5: adopt (startup / reconnect reconciliation) ──────────────────

    /// Adopting a resting order then reconciling an IDENTICAL desired quote must
    /// be a pure no-op: adoption made the manager believe the quote is already
    /// resting, so it suppresses a redundant place.
    #[tokio::test]
    async fn adopt_then_identical_reconcile_issues_no_venue_calls() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();
        // The order really rests on the venue (prior session); adopt its real id.
        let id = v.seed_open(TokenId(7), Side::Bid, px(44), Qty(100_000_000));
        qm.adopt(TokenId(7), Side::Bid, id.clone(), px(44), Qty(100_000_000));
        assert_eq!(tracked_id(&qm, 7, Side::Bid), Some(id.clone()));

        // Desired quote identical to the adopted (reconstructed) Gtc/post_only
        // shape → reconcile recognizes it as already resting: NO venue calls.
        qm.reconcile(&mut v, &[bid(7, 44, 100_000_000)]).await.unwrap();
        assert!(v.placed.is_empty(), "adoption must suppress a redundant place");
        assert!(v.replaced.is_empty());
        assert!(v.cancelled.is_empty());
        assert_eq!(tracked_id(&qm, 7, Side::Bid), Some(id), "adopted id retained");
    }

    /// Adopting a resting order then reconciling a DIFFERENT price must issue
    /// exactly one replace against the adopted id (no place, no cancel) — the
    /// adopted quote is normalized to the live desired one.
    #[tokio::test]
    async fn adopt_then_reconcile_different_price_replaces_once() {
        let mut qm = QuoteManager::new();
        let mut v = MockMakerVenue::new();
        let id = v.seed_open(TokenId(7), Side::Bid, px(44), Qty(100_000_000));
        qm.adopt(TokenId(7), Side::Bid, id.clone(), px(44), Qty(100_000_000));

        qm.reconcile(&mut v, &[bid(7, 45, 100_000_000)]).await.unwrap();
        assert_eq!(v.replaced.len(), 1, "exactly one replace");
        assert_eq!(v.replaced[0].0, id, "the adopted id is handed to the venue");
        assert_eq!(v.replaced[0].1.price, px(45));
        assert!(v.placed.is_empty(), "a changed adopted quote replaces, not places");
        assert!(v.cancelled.is_empty());
        // Tracking moved to the venue's new id (the mock re-keys on replace).
        assert_eq!(tracked_id(&qm, 7, Side::Bid), Some(OrderId("mock-2".into())));
    }
}
