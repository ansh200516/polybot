//! User-fills source (Task 3.4): tells the (future Phase-4) market-making
//! strategy when its RESTING maker orders fill, so it can update inventory and
//! re-quote. INERT until Phase 4 wires it.
//!
//! Phase 3 sources maker fills by polling the auth'd `GET /data/trades`
//! ([`LiveVenue`](crate::live::LiveVenue)'s [`UserFillSource`] impl): it reuses
//! the proven, rate-limited, cursor-paginated `data_rows("trades")` walk the
//! taker fill path already uses, and is fully testable against the in-crate HTTP
//! mock. The pure [`parse_maker_fills`] core does the row→fill mapping + dedup
//! with no I/O.
//!
//! ## Maker vs taker rows
//! The auth'd trades feed returns THIS account's trades. We emit a fill only for
//! rows where we were the MAKER (`trader_side == "MAKER"`): those carry our
//! resting-order fills in `maker_orders[]`. A row where we were the TAKER lists
//! the COUNTERPARTIES' resting orders in `maker_orders[]` (the taker
//! `poll_fills` path consumes those differently), so emitting from it would
//! report fills for orders we never placed — we skip it.
//!
//! ## Side is intentionally absent
//! A maker fill is keyed by `order_id` and carries NO side: the trade row's
//! `side` is the TAKER's perspective, not the resting order's, so it is not a
//! reliable maker side. The Phase-4 consumer resolves side from its own quote
//! tracking — it placed the order, so it knows whether the resting order was a
//! bid or an ask (see [`MakerFill`]'s on-fill mapping).
//!
//! ## Dedup
//! Trade rows recur across polls until they scroll off the cursor window, so the
//! source dedups on `"{trade_id}:{order_id}"` and emits each maker fill exactly
//! once. The dedup state lives on the source; the caller just owns the loop.
//!
//! ## Lower-latency upgrade (future)
//! The user WS `wss://ws-subscriptions-clob.polymarket.com/ws/user`
//! (auth `{apiKey, secret, passphrase}`, subscribe by `condition_id`) pushes
//! `trade` events (which fire MATCHED then CONFIRMED — so they need the SAME
//! dedup) plus `order` events, at lower latency. It is a drop-in replacement
//! behind this SAME [`UserFillSource`] trait; Phase 3 stays on the REST poll
//! because it reuses proven infra and is fully testable offline.

use std::collections::HashSet;

use tracing::warn;

use pm_core::instrument::TokenId;
use pm_core::num::{Px, Qty, TickSize};

use crate::live::{decimal_to_micro, px_from_decimal};
use crate::maker::OrderId;
use crate::venue::VenueError;

#[cfg(test)]
use std::collections::VecDeque;

/// One fill of one of OUR resting maker orders (Task 3.4).
///
/// Keyed by [`OrderId`] (the resting order's venue id) and carrying the trade's
/// `trade_id` for traceability / cross-feed idempotency. `qty` is the maker
/// entry's `matched_amount` in µshares; `px` is its `price` typed to the token's
/// tick size. There is deliberately NO side field — see the module docs.
///
/// # Consumer on-fill mapping (Phase 4)
/// The market-making strategy turns a `MakerFill` into an
/// `InventoryRisk::on_fill(token, signed_qty, cash)` call using the side it
/// recorded when it PLACED `order_id` (which this event does not carry):
/// - `signed_qty` = `qty.0 as i128`, with the sign from the resting order's
///   side: `+qty` if it was a bid (buy), `−qty` if it was an ask (sell).
/// - `px_micro` = `px.microusdc(ts)` — the strategy knows the token's `ts`.
/// - `cash` (signed µUSDC; makers pay 0 fee on CLOB V2): a bid fill is
///   `Usdc(-buy_cost(px_micro, qty).0)`, an ask fill is
///   `Usdc(sell_proceeds(px_micro, qty).0)`.
///
/// The `trade_id` lets the consumer dedup at its own layer too, should it ever
/// merge a WS feed (which double-fires MATCHED then CONFIRMED) with this poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MakerFill {
    /// The resting order that filled (our CLOB `orderID`).
    pub order_id: OrderId,
    /// Our interned token for the trade's `asset_id`.
    pub token: TokenId,
    /// Filled size in µshares (the maker entry's `matched_amount`).
    pub qty: Qty,
    /// Fill price as a tick index for the token's tick size.
    pub px: Px,
    /// The venue trade id (`id`), for traceability + cross-feed dedup.
    pub trade_id: String,
}

/// Source of NEW (deduped) maker fills since the last poll. The CALLER owns the
/// loop + sleep cadence; the SOURCE owns the dedup state, so each maker fill is
/// returned exactly once even though trade rows recur across polls.
///
/// `#[allow(async_fn_in_trait)]` + `: Send` mirror [`crate::maker::MakerVenue`]
/// and [`crate::venue::ExecutionVenue`]: in-crate implementors (the live CLOB
/// venue now; a test double; a future user-WS client) driven from per-strategy
/// async loops.
#[allow(async_fn_in_trait)]
pub trait UserFillSource: Send {
    /// Return the maker fills observed since the previous call (deduped).
    async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError>;
}

/// Pure, I/O-free core: map a page of `GET /data/trades` rows to NEW maker
/// fills. This is the fully-unit-testable heart of the live source.
///
/// For every row where we were the MAKER (`trader_side == "MAKER"`) and each of
/// its `maker_orders[]` entries: build the dedup key `"{trade_id}:{order_id}"`,
/// skip it when already in `seen`, resolve the row's `asset_id` to our
/// `(TokenId, TickSize)` via `resolve` (skip the row with a `warn!` when
/// unregistered), parse `matched_amount`→µshares and `price`→[`Px`] (skip the
/// entry with a `warn!` on a missing / bad / tick-unaligned value), and emit a
/// [`MakerFill`]. Only EMITTED keys are inserted into `seen`, so a transiently
/// malformed entry is retried on a later poll rather than silently swallowed.
///
/// Rows where we were the TAKER are skipped (quietly — they are normal): their
/// `maker_orders[]` are the counterparties' orders, not ours (see the module
/// docs). Robust to missing / malformed fields — a bad entry is skipped, never a
/// panic.
pub(crate) fn parse_maker_fills(
    rows: &[serde_json::Value],
    resolve: impl Fn(&str) -> Option<(TokenId, TickSize)>,
    seen: &mut HashSet<String>,
) -> Vec<MakerFill> {
    let mut out = Vec::new();
    for row in rows {
        // Only OUR maker-side trades carry our resting-order fills. A TAKER row
        // (or one with no/unknown role) lists counterparties in `maker_orders[]`
        // — emitting from it would report fills for orders we never placed.
        // Quiet skip: taker rows are normal and frequent.
        if row.get("trader_side").and_then(|v| v.as_str()) != Some("MAKER") {
            continue;
        }
        let Some(trade_id) = row.get("id").and_then(|v| v.as_str()) else {
            warn!("maker-fill row missing trade id; skipping (cannot dedup)");
            continue;
        };
        let Some(makers) = row.get("maker_orders").and_then(|v| v.as_array()) else {
            // A maker-side trade with no maker_orders array has nothing to emit.
            continue;
        };
        let Some(asset_id) = row.get("asset_id").and_then(|v| v.as_str()) else {
            warn!(trade_id, "maker-fill row missing asset_id; skipping");
            continue;
        };
        // Row-level asset → token + tick size (every maker entry in a trade
        // shares the trade's asset). Unregistered → skip the whole row, warn so
        // the operator notices a token they forgot to register.
        let Some((token, ts)) = resolve(asset_id) else {
            warn!(trade_id, asset_id, "maker fill for unregistered token; skipping");
            continue;
        };
        for entry in makers {
            let Some(order_id) = entry.get("order_id").and_then(|v| v.as_str()) else {
                warn!(trade_id, "maker_orders entry missing order_id; skipping");
                continue;
            };
            let key = format!("{trade_id}:{order_id}");
            if seen.contains(&key) {
                continue;
            }
            let Some(qty_micro) = entry
                .get("matched_amount")
                .and_then(|v| v.as_str())
                .and_then(decimal_to_micro)
            else {
                warn!(trade_id, order_id, "maker fill missing/bad matched_amount; skipping");
                continue;
            };
            let Some(px) = entry
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|p| px_from_decimal(p, ts))
            else {
                warn!(trade_id, order_id, "maker fill missing/bad/unaligned price; skipping");
                continue;
            };
            // Emit exactly once: record the key only now that we have a real fill.
            seen.insert(key);
            out.push(MakerFill {
                order_id: OrderId(order_id.to_string()),
                token,
                qty: Qty(qty_micro),
                px,
                trade_id: trade_id.to_string(),
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Test double
// ---------------------------------------------------------------------------

/// Scripted [`UserFillSource`] for tests (incl. the Phase-4 strategy tests):
/// returns the pre-loaded batches in order, then empty once exhausted. Gated
/// `#[cfg(test)]` like [`crate::maker::MockMakerVenue`].
#[cfg(test)]
#[derive(Debug, Default)]
pub struct MockUserFills {
    batches: VecDeque<Vec<MakerFill>>,
}

#[cfg(test)]
impl MockUserFills {
    /// Build from a script of per-poll batches (front = first poll).
    pub fn new(batches: Vec<Vec<MakerFill>>) -> Self {
        MockUserFills {
            batches: batches.into(),
        }
    }
}

#[cfg(test)]
impl UserFillSource for MockUserFills {
    async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError> {
        Ok(self.batches.pop_front().unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Tests (pure unit tests over parse_maker_fills + the mock)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    const TRADES_MAKER_FILLS: &str =
        include_str!("../tests/fixtures/clob_responses/trades_maker_fills.json");

    /// Resolve only the fixture's registered asset (token 7, Cent ticks).
    fn resolve(aid: &str) -> Option<(TokenId, TickSize)> {
        (aid == "123456789").then_some((TokenId(7), TickSize::Cent))
    }

    /// The `data` array of a `/data/trades` page as owned `Value` rows.
    fn rows_of(page: &str) -> Vec<serde_json::Value> {
        let v: serde_json::Value = serde_json::from_str(page).unwrap();
        v.get("data").unwrap().as_array().unwrap().clone()
    }

    #[test]
    fn parse_extracts_maker_fills_and_dedups() {
        let rows = rows_of(TRADES_MAKER_FILLS);
        let mut seen = HashSet::new();
        let fills = parse_maker_fills(&rows, resolve, &mut seen);

        // Two maker fills from the MAKER row's two entries; the TAKER row (whose
        // maker_orders are a counterparty's resting order) contributes nothing.
        assert_eq!(fills.len(), 2, "the TAKER row must be skipped");
        assert_eq!(fills[0].order_id, OrderId("0xresting-A".into()));
        assert_eq!(fills[0].token, TokenId(7));
        assert_eq!(fills[0].qty, Qty(10_000_000));
        assert_eq!(fills[0].px.get(), 33);
        assert_eq!(fills[0].trade_id, "trade-aaa");
        assert_eq!(fills[1].order_id, OrderId("0xresting-B".into()));
        assert_eq!(fills[1].qty, Qty(5_000_000));
        assert_eq!(fills[1].px.get(), 34);
        assert_eq!(fills[1].trade_id, "trade-aaa");

        // Re-running with the SAME rows and the SAME `seen` yields nothing.
        let again = parse_maker_fills(&rows, resolve, &mut seen);
        assert!(again.is_empty(), "dedup across polls: {again:?}");
    }

    #[test]
    fn parse_skips_unregistered_and_malformed() {
        let rows = vec![
            // Unregistered asset → the whole row is skipped.
            json!({
                "id": "t-unreg",
                "asset_id": "999999999",
                "trader_side": "MAKER",
                "maker_orders": [
                    {"order_id": "0xunreg", "matched_amount": "3", "price": "0.20"}
                ]
            }),
            // Registered asset, mixed entries: missing matched_amount, then a
            // tick-unaligned price (0.335 on a Cent market), then one good entry.
            // Only the good entry survives; the bad ones are skipped, not fatal.
            json!({
                "id": "t-mixed",
                "asset_id": "123456789",
                "trader_side": "MAKER",
                "maker_orders": [
                    {"order_id": "0xno-amount", "price": "0.33"},
                    {"order_id": "0xbad-price", "matched_amount": "4", "price": "0.335"},
                    {"order_id": "0xgood", "matched_amount": "4", "price": "0.33"}
                ]
            }),
        ];
        let mut seen = HashSet::new();
        let fills = parse_maker_fills(&rows, resolve, &mut seen);
        assert_eq!(fills.len(), 1, "only the one good entry survives");
        assert_eq!(fills[0].order_id, OrderId("0xgood".into()));
        assert_eq!(fills[0].token, TokenId(7));
        assert_eq!(fills[0].qty, Qty(4_000_000));
        assert_eq!(fills[0].px.get(), 33);
    }

    #[test]
    fn parse_maps_qty_and_px_exactly() {
        // A single known entry: 5 shares @ 0.34 on a Cent market → 5e6 µshares,
        // tick 34 (= 340_000 µUSDC/share). Pins the exact µshare + Px mapping.
        let rows = vec![json!({
            "id": "t-exact",
            "asset_id": "123456789",
            "trader_side": "MAKER",
            "maker_orders": [
                {"order_id": "0xexact", "matched_amount": "5", "price": "0.34"}
            ]
        })];
        let mut seen = HashSet::new();
        let fills = parse_maker_fills(&rows, resolve, &mut seen);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, Qty(5_000_000), "5 shares → 5e6 µshares");
        assert_eq!(fills[0].px.get(), 34, "0.34 → tick 34 on Cent");
        assert_eq!(fills[0].px.microusdc(TickSize::Cent), 340_000);
    }

    #[tokio::test]
    async fn mock_user_fills_scripts_batches() {
        let f1 = MakerFill {
            order_id: OrderId("0x1".into()),
            token: TokenId(7),
            qty: Qty(1_000_000),
            px: Px::new(33, TickSize::Cent).unwrap(),
            trade_id: "t1".into(),
        };
        let f2 = MakerFill {
            order_id: OrderId("0x2".into()),
            token: TokenId(7),
            qty: Qty(2_000_000),
            px: Px::new(34, TickSize::Cent).unwrap(),
            trade_id: "t2".into(),
        };
        let mut mock = MockUserFills::new(vec![vec![f1.clone(), f2.clone()], vec![]]);

        assert_eq!(mock.poll().await.unwrap(), vec![f1, f2], "first scripted batch");
        // A scripted empty batch, then exhausted → both empty.
        assert!(mock.poll().await.unwrap().is_empty(), "scripted empty batch");
        assert!(mock.poll().await.unwrap().is_empty(), "exhausted → empty");
    }
}
