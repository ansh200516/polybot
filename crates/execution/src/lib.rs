//! Order state machine + write-ahead persistence (spec §14).
//!
//! # Design notes
//!
//! ## Write-ahead contract
//! `persist_transition` validates the transition, sends an `OrderEvent` row to
//! the store writer, and waits for the ack BEFORE mutating `order.state`. If
//! the ack channel is dropped (write failure), the function returns
//! `ExecError::StoreClosed` and the in-memory state is left unchanged — both
//! the store and the caller see the same pre-transition state.
//!
//! ## `ExecError::Venue(String)` — design decision
//! The venue module does not exist until Task 8. Rather than introducing a
//! temporary `VenueError` placeholder or creating a circular dependency, we use
//! `Venue(String)` permanently. Basket/venue callers map their errors via
//! `.map_err(|e| ExecError::Venue(e.to_string()))`. This is simple, avoids
//! the need to re-export a downstream error type, and the string representation
//! is sufficient for logging and human-readable diagnostics.
//!
//! Tasks 8–10 call `Order::new`, `order.to_row(ts_ms)`, `persist_transition`,
//! `can_transition`, `OrderState`, and `ExecError` exactly as declared below.

pub mod auth;
pub mod basket;
pub mod live;
pub mod secrets;
pub mod sign;
pub mod venue;

use pm_core::instrument::TokenId;
use pm_core::num::{Bps, Px, Qty, TickSize};
use pm_engine::Action;
use pm_store::writer::StoreMsg;
use pm_store::{OrderEventRow, OrderRow};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// OrderState
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    Draft,
    Signed,
    Submitted,
    Live,
    Filled,
    PartFilled,
    Cancelled,
    Rejected,
    Expired,
}

impl OrderState {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderState::Draft => "Draft",
            OrderState::Signed => "Signed",
            OrderState::Submitted => "Submitted",
            OrderState::Live => "Live",
            OrderState::Filled => "Filled",
            OrderState::PartFilled => "PartFilled",
            OrderState::Cancelled => "Cancelled",
            OrderState::Rejected => "Rejected",
            OrderState::Expired => "Expired",
        }
    }
}

impl std::fmt::Display for OrderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Transition table
// ---------------------------------------------------------------------------

/// Legal transitions (spec §14):
/// Draft → Signed → Submitted → Live → {Filled, PartFilled, Cancelled, Rejected, Expired};
/// Submitted may go straight to Rejected or Expired; PartFilled may finish,
/// cancel, or expire.
pub fn can_transition(from: OrderState, to: OrderState) -> bool {
    use OrderState::*;
    matches!(
        (from, to),
        (Draft, Signed)
            | (Signed, Submitted)
            | (Submitted, Live)
            | (Submitted, Rejected)
            | (Submitted, Expired)
            | (Live, Filled)
            | (Live, PartFilled)
            | (Live, Cancelled)
            | (Live, Rejected)
            | (Live, Expired)
            | (PartFilled, Filled)
            | (PartFilled, Cancelled)
            | (PartFilled, Expired)
    )
}

// ---------------------------------------------------------------------------
// ExecError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ExecError {
    BadTransition(OrderState, OrderState),
    /// Store channel closed or write failed (ack dropped) — execution cannot
    /// proceed without durability (write-ahead contract).
    StoreClosed,
    /// Venue-layer error stringified; see module doc for the design rationale.
    Venue(String),
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::BadTransition(a, b) => write!(f, "illegal transition {a} -> {b}"),
            ExecError::StoreClosed => write!(f, "store writer unavailable"),
            ExecError::Venue(e) => write!(f, "venue error: {e}"),
        }
    }
}

impl std::error::Error for ExecError {}

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Order {
    pub id: Uuid,
    pub fingerprint: String,
    pub token: TokenId,
    pub action: Action,
    pub ts: TickSize,
    pub limit_px: Px,
    pub qty: Qty,
    pub fee_bps: Bps,
    pub state: OrderState,
}

impl Order {
    /// Construct a new order in `Draft` state with a UUIDv7 client order id
    /// (idempotency key — spec §14).
    pub fn new(
        fingerprint: String,
        token: TokenId,
        action: Action,
        ts: TickSize,
        limit_px: Px,
        qty: Qty,
        fee_bps: Bps,
    ) -> Self {
        Order {
            id: Uuid::now_v7(),
            fingerprint,
            token,
            action,
            ts,
            limit_px,
            qty,
            fee_bps,
            state: OrderState::Draft,
        }
    }

    pub fn action_str(&self) -> &'static str {
        match self.action {
            Action::Buy => "Buy",
            Action::Sell => "Sell",
        }
    }

    /// Serialise to an `OrderRow` for insertion into the store.
    ///
    /// # Safety invariants (by construction)
    /// - `qty.0` is bounded by per-market position caps well below `i64::MAX`.
    /// - `token.0` is a dense intern index that fits comfortably in `i64`.
    pub fn to_row(&self, ts_ms: i64) -> OrderRow {
        debug_assert!(self.qty.0 <= i64::MAX as u64, "qty exceeds i64::MAX");
        debug_assert!(self.token.0 <= i64::MAX as u64, "token id exceeds i64::MAX");
        OrderRow {
            id: self.id.to_string(),
            ts_ms,
            fingerprint: self.fingerprint.clone(),
            token: self.token.0 as i64,
            action: self.action_str().into(),
            limit_ticks: i64::from(self.limit_px.get()),
            tick_levels: i64::from(self.ts.levels()),
            qty_micro: self.qty.0 as i64,
            strategy: "arb".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Write-ahead transition
// ---------------------------------------------------------------------------

/// Validate the transition, persist the `OrderEvent` row (acked by the store
/// writer), then mutate in-memory state.
///
/// The state mutation happens ONLY after the ack — if the store writer drops
/// the ack channel the function returns `ExecError::StoreClosed` and
/// `order.state` is unchanged, keeping memory and store consistent.
pub async fn persist_transition(
    store: &mpsc::Sender<StoreMsg>,
    order: &mut Order,
    to: OrderState,
    detail: &str,
    ts_ms: i64,
) -> Result<(), ExecError> {
    if !can_transition(order.state, to) {
        return Err(ExecError::BadTransition(order.state, to));
    }
    let (ack_tx, ack_rx) = oneshot::channel();
    store
        .send(StoreMsg::OrderEvent(
            OrderEventRow {
                order_id: order.id.to_string(),
                ts_ms,
                state: to.as_str().into(),
                detail: detail.into(),
            },
            Some(ack_tx),
        ))
        .await
        .map_err(|_| ExecError::StoreClosed)?;
    ack_rx.await.map_err(|_| ExecError::StoreClosed)?;
    // Write-ahead: state mutates only after durable commit confirmed.
    order.state = to;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::instrument::TokenId;
    use pm_core::num::{Px, Qty, TickSize};
    use pm_engine::Action;

    fn order() -> Order {
        Order::new(
            "fp".into(),
            TokenId(7),
            Action::Buy,
            TickSize::Cent,
            Px::new(44, TickSize::Cent).unwrap(),
            Qty(100_000_000),
            pm_core::num::Bps(0),
        )
    }

    #[test]
    fn new_orders_are_draft_with_v7_ids() {
        let a = order();
        let b = order();
        assert_eq!(a.state, OrderState::Draft);
        assert_ne!(a.id, b.id);
        assert_eq!(a.id.get_version_num(), 7);
    }

    #[test]
    fn legal_paper_lifecycle() {
        use OrderState::*;
        for path in [
            vec![Draft, Signed, Submitted, Live, Filled],
            vec![Draft, Signed, Submitted, Live, PartFilled, Cancelled],
            vec![Draft, Signed, Submitted, Live, Cancelled],
            vec![Draft, Signed, Submitted, Rejected],
            vec![Draft, Signed, Submitted, Expired],
            vec![Draft, Signed, Submitted, Live, Expired],
        ] {
            for w in path.windows(2) {
                assert!(can_transition(w[0], w[1]), "{:?} -> {:?}", w[0], w[1]);
            }
        }
    }

    #[test]
    fn illegal_transitions_rejected() {
        use OrderState::*;
        for (from, to) in [
            (Draft, Submitted),
            (Draft, Filled),
            (Signed, Live),
            (Filled, Cancelled),
            (Cancelled, Live),
            (Rejected, Signed),
            (Expired, Filled),
            (Live, Draft),
            (Filled, Filled),
        ] {
            assert!(
                !can_transition(from, to),
                "{from:?} -> {to:?} must be illegal"
            );
        }
    }

    #[tokio::test]
    async fn persist_transition_is_write_ahead_and_validated() {
        use pm_store::writer::{StoreMsg, run_writer};
        let store = pm_store::Store::open_in_memory().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let writer = tokio::spawn(run_writer(store, rx));

        let mut o = order();
        // insert the order row first (acked)
        let (ack, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(StoreMsg::OrderInsert(o.to_row(1), Some(ack)))
            .await
            .unwrap();
        ack_rx.await.unwrap();

        persist_transition(&tx, &mut o, OrderState::Signed, "", 2)
            .await
            .unwrap();
        assert_eq!(o.state, OrderState::Signed);

        // illegal: Signed -> Filled — state must not change, nothing persisted
        let err = persist_transition(&tx, &mut o, OrderState::Filled, "", 3).await;
        assert!(matches!(
            err,
            Err(ExecError::BadTransition(
                OrderState::Signed,
                OrderState::Filled
            ))
        ));
        assert_eq!(o.state, OrderState::Signed);

        drop(tx);
        let store = writer.await.unwrap();
        assert_eq!(
            store.open_orders().unwrap(),
            vec![(o.id.to_string(), "Signed".to_string())]
        );
    }

    #[tokio::test]
    async fn failed_store_write_returns_store_closed_and_leaves_state() {
        use pm_store::writer::run_writer;
        let store = pm_store::Store::open_in_memory().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let writer = tokio::spawn(run_writer(store, rx));

        // No OrderInsert: the OrderEvent hits a foreign-key violation in the store,
        // the writer drops the ack, and persist_transition must surface StoreClosed
        // while leaving in-memory state untouched.
        let mut o = order();
        let err = persist_transition(&tx, &mut o, OrderState::Signed, "", 1).await;
        assert!(matches!(err, Err(ExecError::StoreClosed)));
        assert_eq!(o.state, OrderState::Draft);

        drop(tx);
        let store = writer.await.unwrap();
        assert_eq!(store.write_errors, 1);
        assert!(store.open_orders().unwrap().is_empty());
    }
}
