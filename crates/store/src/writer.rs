//! Single writer task: owns the rusqlite connection (spec §16), fed by a
//! bounded mpsc. Acked messages (orders/fills/conversions) fire their oneshot
//! only after commit — the write-ahead contract for spec §14.

use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::{
    ConversionRow, FillRow, HaltRow, MarketRow, OppRow, OrderEventRow, OrderRow, PnlRow, RelRow,
    RfDecisionRow, RfOutcomeRow, Store,
};

pub type Ack = Option<oneshot::Sender<()>>;

pub enum StoreMsg {
    MarketUpsert(MarketRow),
    RelationshipUpsert(RelRow),
    Opportunity(OppRow),
    OrderInsert(OrderRow, Ack),
    OrderEvent(OrderEventRow, Ack),
    /// LONG-ONLY (arb) fill: routed to `Store::insert_fill` (strict; a Sell
    /// beyond long holdings is an `Oversell` write error + rollback).
    Fill(FillRow, Ack),
    /// SIGNED (market-making) fill: routed to `Store::insert_fill_signed` so a
    /// sell-to-open SHORT (or a buy that covers one) persists instead of being
    /// `Oversell`-dropped. Set by inventory-bearing strategies (e.g. MM).
    FillSigned(FillRow, Ack),
    Conversion(ConversionRow, Ack),
    PnlSnapshot(PnlRow),
    Halt(HaltRow),
    /// RewardFarm DECISION telemetry (Task 10, spec §12): best-effort, un-acked
    /// (like `Opportunity` / `PnlSnapshot`). A failed write only bumps
    /// `write_errors` — it never blocks the trading path.
    RfDecision(RfDecisionRow),
    /// RewardFarm OUTCOME telemetry (Task 10, spec §12): the writer correlates it
    /// to the most-recent decision for its market key; an outcome with no matching
    /// decision is dropped (NOT an error), so the un-acked, best-effort contract
    /// holds even right after a restart.
    RfOutcome(RfOutcomeRow),
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
            StoreMsg::FillSigned(r, ack) => {
                (store.insert_fill_signed(&r).map(|_| ()), ack, "fill_signed")
            }
            StoreMsg::Conversion(r, ack) => {
                (store.apply_conversion(&r).map(|_| ()), ack, "conversion")
            }
            StoreMsg::PnlSnapshot(r) => (store.insert_pnl_snapshot(&r), None, "pnl_snapshot"),
            StoreMsg::Halt(r) => (store.insert_halt(&r), None, "halt"),
            StoreMsg::RfDecision(r) => (
                store
                    .record_rf_decision(r.ts_ms, &r.market, &r.state_json, &r.action_json)
                    .map(|_| ()),
                None,
                "rf_decision",
            ),
            StoreMsg::RfOutcome(r) => (
                store
                    .record_rf_outcome_for_latest(
                        &r.market,
                        r.ts_ms,
                        r.reward_score_delta_micro,
                        r.rebate_micro,
                        r.adverse_pnl_micro,
                        r.inv_penalty_micro,
                    )
                    .map(|_| ()),
                None,
                "rf_outcome",
            ),
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
    use crate::{FillRow, OrderEventRow, OrderRow, RfDecisionRow, RfOutcomeRow, Store};

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

    #[tokio::test]
    async fn fill_signed_opens_short_without_oversell() {
        // The SIGNED route persists a sell-to-open SHORT (no long holdings)
        // instead of Oversell-dropping it the way the strict `Fill` route does
        // in the test above — the ack fires (commit) and no write error counts.
        let store = Store::open_in_memory().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let writer = tokio::spawn(run_writer(store, rx));

        tx.send(StoreMsg::OrderInsert(order_row("o1"), None))
            .await
            .unwrap();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(StoreMsg::FillSigned(
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
                strategy: "mm".into(),
            },
            Some(ack_tx),
        ))
        .await
        .unwrap();
        ack_rx.await.unwrap(); // signed commit → ack fires (NOT dropped)

        drop(tx);
        let store = writer.await.unwrap();
        assert_eq!(store.write_errors, 0, "the signed short-open must not error");
        assert_eq!(store.count_fills().unwrap(), 1, "the short fill persisted");
        assert_eq!(store.position(7).unwrap(), (-1_000_000, -500_000));
    }

    #[tokio::test]
    async fn rf_decision_then_outcome_correlates_via_writer() {
        // Task 10: the RewardFarm telemetry variants flow through the writer like
        // any other message. An outcome with NO prior decision is silently DROPPED
        // (best-effort), while one AFTER a decision correlates to it — neither
        // bumps `write_errors` (telemetry is never a trading-path failure).
        let store = Store::open_in_memory().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let writer = tokio::spawn(run_writer(store, rx));

        // Outcome before any decision for market "7" → dropped, not an error.
        tx.send(StoreMsg::RfOutcome(RfOutcomeRow {
            market: "7".into(),
            ts_ms: 1,
            reward_score_delta_micro: 0,
            rebate_micro: 0,
            adverse_pnl_micro: 0,
            inv_penalty_micro: 0,
        }))
        .await
        .unwrap();

        // Decision (id 1 on a fresh DB) then an outcome → correlated.
        tx.send(StoreMsg::RfDecision(RfDecisionRow {
            ts_ms: 2,
            market: "7".into(),
            state_json: "{}".into(),
            action_json: "{}".into(),
        }))
        .await
        .unwrap();
        tx.send(StoreMsg::RfOutcome(RfOutcomeRow {
            market: "7".into(),
            ts_ms: 3,
            reward_score_delta_micro: 0,
            rebate_micro: 9,
            adverse_pnl_micro: -2,
            inv_penalty_micro: 0,
        }))
        .await
        .unwrap();

        drop(tx);
        let store = writer.await.unwrap();
        assert_eq!(store.write_errors, 0, "dropped + correlated writes never error");
        assert_eq!(
            store.count_rf_outcomes_for(1).unwrap(),
            1,
            "exactly the correlated outcome attached to decision id 1"
        );
    }
}
