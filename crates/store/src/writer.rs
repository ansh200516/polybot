//! Single writer task: owns the rusqlite connection (spec §16), fed by a
//! bounded mpsc. Acked messages (orders/fills/conversions) fire their oneshot
//! only after commit — the write-ahead contract for spec §14.

use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::{
    ConversionRow, FillRow, HaltRow, MarketRow, OppRow, OrderEventRow, OrderRow, PnlRow, RelRow,
    Store,
};

pub type Ack = Option<oneshot::Sender<()>>;

pub enum StoreMsg {
    MarketUpsert(MarketRow),
    RelationshipUpsert(RelRow),
    Opportunity(OppRow),
    OrderInsert(OrderRow, Ack),
    OrderEvent(OrderEventRow, Ack),
    Fill(FillRow, Ack),
    Conversion(ConversionRow, Ack),
    PnlSnapshot(PnlRow),
    Halt(HaltRow),
}

/// Run until the channel closes; returns the store for final inspection
/// (session summary, tests).
///
/// Blocking note: store calls are synchronous sqlite I/O executed directly on
/// the runtime worker. This is deliberate for M3: write rates are bounded
/// (paper trading, bounded channel) and `block_in_place` would panic under the
/// current-thread test runtimes that pair this writer with `tokio::time::pause`.
/// Revisit (dedicated thread) if M5 live write rates demand it.
pub async fn run_writer(mut store: Store, mut rx: mpsc::Receiver<StoreMsg>) -> Store {
    while let Some(msg) = rx.recv().await {
        let (result, ack, op): (Result<(), crate::StoreError>, Ack, &'static str) = match msg {
            StoreMsg::MarketUpsert(r) => (store.upsert_market(&r), None, "market_upsert"),
            StoreMsg::RelationshipUpsert(r) => {
                (store.upsert_relationship(&r), None, "relationship_upsert")
            }
            StoreMsg::Opportunity(r) => (store.insert_opportunity(&r), None, "opportunity"),
            StoreMsg::OrderInsert(r, ack) => (store.insert_order(&r), ack, "order_insert"),
            StoreMsg::OrderEvent(r, ack) => (store.insert_order_event(&r), ack, "order_event"),
            StoreMsg::Fill(r, ack) => (store.insert_fill(&r).map(|_| ()), ack, "fill"),
            StoreMsg::Conversion(r, ack) => {
                (store.apply_conversion(&r).map(|_| ()), ack, "conversion")
            }
            StoreMsg::PnlSnapshot(r) => (store.insert_pnl_snapshot(&r), None, "pnl_snapshot"),
            StoreMsg::Halt(r) => (store.insert_halt(&r), None, "halt"),
        };
        match result {
            Ok(()) => {
                if let Some(a) = ack {
                    let _ = a.send(());
                }
            }
            Err(e) => {
                store.write_errors += 1;
                error!(op, "store write failed: {e}");
                // ack (if any) is dropped here → awaiting side sees RecvError.
                drop(ack);
            }
        }
    }
    store
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{FillRow, OrderEventRow, OrderRow, Store};

    fn order_row(id: &str) -> OrderRow {
        OrderRow {
            id: id.into(),
            ts_ms: 1,
            fingerprint: "fp".into(),
            token: 7,
            action: "Buy".into(),
            limit_ticks: 44,
            tick_levels: 100,
            qty_micro: 1_000_000,
            strategy: "arb".into(),
        }
    }

    #[tokio::test]
    async fn acked_order_insert_round_trips() {
        let store = Store::open_in_memory().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let writer = tokio::spawn(run_writer(store, rx));

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(StoreMsg::OrderInsert(order_row("o1"), Some(ack_tx)))
            .await
            .unwrap();
        ack_rx.await.unwrap(); // ack fires only after commit

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(StoreMsg::OrderEvent(
            OrderEventRow {
                order_id: "o1".into(),
                ts_ms: 2,
                state: "Signed".into(),
                detail: String::new(),
            },
            Some(ack_tx),
        ))
        .await
        .unwrap();
        ack_rx.await.unwrap();

        drop(tx); // close channel → writer drains and returns the store
        let store = writer.await.unwrap();
        assert_eq!(
            store.open_orders().unwrap(),
            vec![("o1".to_string(), "Signed".to_string())]
        );
        assert_eq!(store.write_errors, 0);
    }

    #[tokio::test]
    async fn failed_acked_write_drops_ack_and_counts() {
        let store = Store::open_in_memory().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let writer = tokio::spawn(run_writer(store, rx));

        // Sell with no lots → Oversell → write error; ack sender dropped.
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(StoreMsg::OrderInsert(order_row("o1"), None))
            .await
            .unwrap();
        tx.send(StoreMsg::Fill(
            FillRow {
                order_id: "o1".into(),
                ts_ms: 1,
                token: 7,
                action: "Sell".into(),
                px_ticks: 50,
                tick_levels: 100,
                qty_micro: 1_000_000,
                cash_micro: 500_000,
                fee_micro: 0,
                strategy: "arb".into(),
            },
            Some(ack_tx),
        ))
        .await
        .unwrap();
        assert!(ack_rx.await.is_err()); // dropped, not fired

        drop(tx);
        let store = writer.await.unwrap();
        assert_eq!(store.write_errors, 1);
        assert_eq!(store.count_fills().unwrap(), 0);
    }
}
