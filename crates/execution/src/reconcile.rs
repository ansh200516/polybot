//! Startup / reconnect reconciliation of resting maker orders (spec §6, Phase 3
//! — Task 3.5). The reusable venue-side primitive a restarting or reconnecting
//! market-maker runs BEFORE it resumes quoting, to deal with orders left
//! resting on the book by a previous session.
//!
//! Under the taker (FAK) path nothing rests, so a stray open order is an
//! anomaly. Under the MAKER path resting orders are normal, so a restart must
//! actively reconcile them against fresh local state via one of two policies:
//!
//! - [`ReconcilePolicy::CancelAll`] — the DEFAULT, safe policy. A restarting
//!   market-maker has lost the in-memory intent behind any orphaned quote (how
//!   stale it is, what mid it was quoted around, whether it still fits the
//!   current risk caps), so the safe move is a clean slate: cancel every resting
//!   order, then quote fresh from the current book. Idempotent cancels (Task
//!   3.3) make this robust even if an order fills/expires between the read and
//!   the cancel.
//! - [`ReconcilePolicy::Adopt`] — for a reconnect WITHOUT a restart, where the
//!   strategy's in-memory desired quotes survived the disconnect. Re-attach the
//!   discovered orders to the [`QuoteManager`] (NO venue calls) so it knows they
//!   are resting and won't double-place; the next `reconcile` converges any that
//!   drifted from the live desired quote.
//!
//! This is the execution/venue half of Task 3.5 and carries NO risk dependency
//! (`pm-execution` does not depend on `pm-risk`). The inventory half — seeding
//! `InventoryRisk` from `store.position(token)` so caps/mark-to-market resume
//! from the real position — lives in `pm-risk` as `InventoryRisk::seed`. Phase-4
//! wiring composes the two; nothing here runs until then — it is inert.

use crate::maker::MakerVenue;
use crate::quote_manager::QuoteManager;
use crate::venue::VenueError;

/// How [`reconcile_open_orders`] treats the orders found resting on the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcilePolicy {
    /// Cancel every resting order, leaving the [`QuoteManager`] empty. The
    /// DEFAULT, safe choice for a process RESTART: in-memory intent is lost, so
    /// take a clean slate and re-quote fresh.
    CancelAll,
    /// Adopt every resting order into the [`QuoteManager`] without touching the
    /// venue. For a RECONNECT where the strategy's in-memory desired quotes
    /// survived the disconnect.
    Adopt,
}

/// What [`reconcile_open_orders`] did: how many resting orders were `found`, and
/// how many were `cancelled` (CancelAll) vs `adopted` (Adopt). On success
/// exactly one of `cancelled`/`adopted` is non-zero (the other policy's counter
/// stays 0) and that active counter equals `found`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Resting orders the venue reported.
    pub found: usize,
    /// Orders cancelled at the venue (non-zero only under `CancelAll`).
    pub cancelled: usize,
    /// Orders adopted into the `QuoteManager` (non-zero only under `Adopt`).
    pub adopted: usize,
}

/// Reconcile the orders currently resting on `venue` against fresh local state,
/// per `policy` (spec §6, Task 3.5). Reads the open book once, then either
/// cancels every order ([`CancelAll`](ReconcilePolicy::CancelAll)) or adopts
/// every order into `qm` ([`Adopt`](ReconcilePolicy::Adopt)), returning a
/// [`ReconcileReport`] of the counts.
///
/// - **CancelAll** issues one [`cancel`](MakerVenue::cancel) per resting order
///   and leaves `qm` untouched (empty, for a fresh startup). Cancels are
///   idempotent (Task 3.3), so an order that vanishes (fills/expires) between
///   the `open_orders()` read and its cancel is a clean no-op.
/// - **Adopt** issues NO venue calls: each order is loaded into `qm` via
///   [`QuoteManager::adopt`], which reconstructs the maker's standard shape
///   (`Gtc`, `post_only`); a later `reconcile` normalizes any that differ from
///   the live desired quote.
///
/// On any [`VenueError`] (the `open_orders()` read, or a `cancel`) the pass
/// aborts and returns `Err`. Under CancelAll a prefix of orders may already be
/// cancelled; because cancels are idempotent the caller can simply re-run this
/// to converge.
pub async fn reconcile_open_orders<V: MakerVenue>(
    venue: &mut V,
    qm: &mut QuoteManager,
    policy: ReconcilePolicy,
) -> Result<ReconcileReport, VenueError> {
    let open = venue.open_orders().await?;
    let found = open.len();
    let mut cancelled = 0;
    let mut adopted = 0;

    match policy {
        ReconcilePolicy::CancelAll => {
            for o in &open {
                // Idempotent (Task 3.3): a no-longer-resting id is a clean no-op,
                // so a read/cancel race is safe. Count only after the venue acks.
                venue.cancel(&o.id).await?;
                cancelled += 1;
            }
        }
        ReconcilePolicy::Adopt => {
            for o in open {
                qm.adopt(o.token, o.side, o.id, o.price, o.size);
                adopted += 1;
            }
        }
    }

    Ok(ReconcileReport {
        found,
        cancelled,
        adopted,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::maker::{MakerOrder, MockMakerVenue, OrderType};
    use pm_core::book::Side;
    use pm_core::instrument::TokenId;
    use pm_core::num::{Px, Qty, TickSize};

    fn px(tick: u16) -> Px {
        Px::new(tick, TickSize::Cent).unwrap()
    }

    /// A desired quote in the maker's standard shape (Gtc, post_only) — what
    /// `QuoteManager::adopt` reconstructs, so an adopted order re-quoted with
    /// identical price/size compares equal.
    fn gtc(token: u64, side: Side, tick: u16, size: u64) -> MakerOrder {
        MakerOrder {
            token: TokenId(token),
            side,
            price: px(tick),
            size: Qty(size),
            order_type: OrderType::Gtc,
            post_only: true,
        }
    }

    #[tokio::test]
    async fn reconcile_cancel_all_clears_resting() {
        // Two orders left resting by a crashed session.
        let mut v = MockMakerVenue::new();
        let id1 = v.seed_open(TokenId(7), Side::Bid, px(44), Qty(100_000_000));
        let id2 = v.seed_open(TokenId(7), Side::Ask, px(46), Qty(100_000_000));
        let mut qm = QuoteManager::new();

        let report = reconcile_open_orders(&mut v, &mut qm, ReconcilePolicy::CancelAll)
            .await
            .unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                found: 2,
                cancelled: 2,
                adopted: 0
            }
        );
        // Both cancelled at the venue and the book is now empty.
        assert_eq!(v.cancelled.len(), 2);
        assert!(v.cancelled.contains(&id1));
        assert!(v.cancelled.contains(&id2));
        assert!(v.open_orders().await.unwrap().is_empty());
        // qm left empty (clean slate, ready to quote fresh).
        assert!(qm.tracked().is_empty());
    }

    #[tokio::test]
    async fn reconcile_adopt_loads_into_manager() {
        let mut v = MockMakerVenue::new();
        let id1 = v.seed_open(TokenId(7), Side::Bid, px(44), Qty(100_000_000));
        let id2 = v.seed_open(TokenId(7), Side::Ask, px(46), Qty(100_000_000));
        let mut qm = QuoteManager::new();

        let report = reconcile_open_orders(&mut v, &mut qm, ReconcilePolicy::Adopt)
            .await
            .unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                found: 2,
                cancelled: 0,
                adopted: 2
            }
        );
        // No venue cancels; qm now tracks both REAL venue ids.
        assert!(v.cancelled.is_empty());
        let tracked = qm.tracked();
        assert_eq!(tracked.len(), 2);
        assert_eq!(tracked.get(&(TokenId(7), Side::Bid)), Some(&id1));
        assert_eq!(tracked.get(&(TokenId(7), Side::Ask)), Some(&id2));

        // Re-quoting the SAME prices/sizes (matching the adopted Gtc/post_only
        // reconstruction) issues no venue calls — adoption suppressed the
        // re-place that a fresh manager would have done.
        qm.reconcile(
            &mut v,
            &[
                gtc(7, Side::Bid, 44, 100_000_000),
                gtc(7, Side::Ask, 46, 100_000_000),
            ],
        )
        .await
        .unwrap();
        assert!(v.placed.is_empty(), "adopted quotes must not be re-placed");
        assert!(v.replaced.is_empty());
        assert!(v.cancelled.is_empty());
    }

    #[tokio::test]
    async fn reconcile_empty_is_noop() {
        let mut v = MockMakerVenue::new();
        let mut qm = QuoteManager::new();

        // No resting orders → zero counts and no venue mutations, under EITHER
        // policy.
        let cancel = reconcile_open_orders(&mut v, &mut qm, ReconcilePolicy::CancelAll)
            .await
            .unwrap();
        assert_eq!(
            cancel,
            ReconcileReport {
                found: 0,
                cancelled: 0,
                adopted: 0
            }
        );
        let adopt = reconcile_open_orders(&mut v, &mut qm, ReconcilePolicy::Adopt)
            .await
            .unwrap();
        assert_eq!(
            adopt,
            ReconcileReport {
                found: 0,
                cancelled: 0,
                adopted: 0
            }
        );

        assert!(v.cancelled.is_empty());
        assert!(v.placed.is_empty());
        assert!(v.replaced.is_empty());
        assert!(qm.tracked().is_empty());
    }
}
