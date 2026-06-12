//! ExecutionVenue trait + PaperVenue (spec §14). The paper venue fills
//! against the LIVE book re-read after `paper_latency` — no midpoints, no
//! infinite depth.

use std::time::Duration;

use pm_core::book::Book;
use pm_core::fees::fee_microusdc;
use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{ONE_SHARE_MICRO, Px, Qty, Usdc, buy_cost, sell_proceeds};
use pm_engine::{Action, GasTable};

use crate::Order;

#[derive(Debug)]
pub enum VenueError {
    BookUnavailable(TokenId),
    /// A live CLOB request failed: HTTP error (status + body) or a 200 response
    /// with `success:false` (the venue's `errorMsg`). Live-only.
    Live(String),
    /// An operation the live venue does not support in this milestone
    /// (split/merge on-chain ops are deferred). Live-only.
    NotSupportedLive(&'static str),
}

impl std::fmt::Display for VenueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VenueError::BookUnavailable(t) => write!(f, "no live book for token {}", t.0),
            VenueError::Live(e) => write!(f, "live venue error: {e}"),
            VenueError::NotSupportedLive(op) => write!(f, "operation not supported live: {op}"),
        }
    }
}

impl std::error::Error for VenueError {}

/// One executed price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fill {
    pub px: Px,
    pub qty: Qty,
    /// Signed cash net of fee: negative = out (buy cost + fee), positive = in.
    pub cash: Usdc,
    pub fee: Usdc,
}

/// FAK result: fills plus total filled quantity; the remainder was killed.
#[derive(Debug, Clone, Default)]
pub struct SubmitOutcome {
    pub fills: Vec<Fill>,
    pub filled: Qty,
    /// Venue-assigned order id (CLOB `orderID`). Live only; `None` in paper.
    pub venue_order_id: Option<String>,
}

/// Read access to current books. The app adapts the supervisor command
/// channel into this; tests use in-memory maps. Returns None when the book is
/// unknown OR integrity-invalid (callers treat both as unavailable).
#[allow(async_fn_in_trait)]
pub trait BookSource: Send {
    async fn book(&mut self, token: TokenId) -> Option<Book>;
}

/// Venue abstraction (spec §14). LiveVenue arrives in M5 with identical
/// semantics to the caller.
///
/// M5 notes: LiveVenue will need order-id correlation in SubmitOutcome, async fill reporting collapsed to a terminal outcome for FAK, and failure variants for split/merge.
#[allow(async_fn_in_trait)]
pub trait ExecutionVenue: Send {
    /// Fill-and-kill at the order's limit. Never blocks beyond venue latency.
    async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError>;

    /// Submit a whole basket "concurrently": implementations must price all
    /// orders against the same instant in time. Default = sequential calls
    /// (correct for mocks; PaperVenue overrides with single-latency batch).
    ///
    /// Returns exactly one result per input order, in the same order —
    /// implementations must preserve this (the basket coordinator zips results
    /// against orders).
    async fn submit_all(&mut self, orders: &[Order]) -> Vec<Result<SubmitOutcome, VenueError>> {
        let mut out = Vec::with_capacity(orders.len());
        for o in orders {
            out.push(self.submit_fak(o).await);
        }
        out
    }

    /// Split collateral into a complete set: returns signed cash (negative:
    /// collateral + gas out). On-chain ops are assumed to succeed in paper.
    async fn split(&mut self, market: MarketId, units: Qty) -> Result<Usdc, VenueError>;

    /// Merge a complete set back to collateral: returns signed cash (positive:
    /// collateral in, net of gas).
    async fn merge(&mut self, market: MarketId, units: Qty) -> Result<Usdc, VenueError>;
}

pub struct PaperVenue<B> {
    books: B,
    latency: Duration,
    gas: GasTable,
}

impl<B: BookSource> PaperVenue<B> {
    pub fn new(books: B, latency: Duration, gas: GasTable) -> Self {
        PaperVenue {
            books,
            latency,
            gas,
        }
    }

    /// Walk the current book for one order (no latency — callers slept already).
    async fn fill_now(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
        let book = self
            .books
            .book(order.token)
            .await
            .ok_or(VenueError::BookUnavailable(order.token))?;
        let ladder = match order.action {
            Action::Buy => &book.asks,
            Action::Sell => &book.bids,
        };
        let mut remaining = order.qty.0;
        let mut fills = Vec::new();
        for (px, avail) in ladder.iter_from_best() {
            let within = match order.action {
                Action::Buy => px.get() <= order.limit_px.get(),
                Action::Sell => px.get() >= order.limit_px.get(),
            };
            if !within || remaining == 0 {
                break;
            }
            let take = remaining.min(avail.0);
            if take == 0 {
                continue;
            }
            let px_micro = px.microusdc(order.ts);
            let fee = fee_microusdc(order.fee_bps, px_micro, Qty(take));
            let cash = match order.action {
                Action::Buy => Usdc(-(buy_cost(px_micro, Qty(take)).0 + fee.0)),
                Action::Sell => Usdc(sell_proceeds(px_micro, Qty(take)).0 - fee.0),
            };
            fills.push(Fill {
                px,
                qty: Qty(take),
                cash,
                fee,
            });
            remaining -= take;
        }
        Ok(SubmitOutcome {
            filled: Qty(order.qty.0 - remaining),
            fills,
            venue_order_id: None,
        })
    }
}

impl<B: BookSource> ExecutionVenue for PaperVenue<B> {
    async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
        tokio::time::sleep(self.latency).await;
        self.fill_now(order).await
    }

    async fn submit_all(&mut self, orders: &[Order]) -> Vec<Result<SubmitOutcome, VenueError>> {
        // One shared latency: all legs hit the venue at the same simulated instant.
        tokio::time::sleep(self.latency).await;
        let mut out = Vec::with_capacity(orders.len());
        for o in orders {
            out.push(self.fill_now(o).await);
        }
        out
    }

    // Complete-set collateral is always $1/share by construction; the market id is retained for the M5 LiveVenue trait shape.
    async fn split(&mut self, _market: MarketId, units: Qty) -> Result<Usdc, VenueError> {
        // $1 collateral per whole share; ceil for safety on sub-share dust.
        let collateral = buy_cost(ONE_SHARE_MICRO, units);
        Ok(Usdc(-(collateral.0 + i128::from(self.gas.split))))
    }

    async fn merge(&mut self, _market: MarketId, units: Qty) -> Result<Usdc, VenueError> {
        let collateral = sell_proceeds(ONE_SHARE_MICRO, units);
        Ok(Usdc(collateral.0 - i128::from(self.gas.merge)))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::Order;
    use pm_core::book::{Book, Side};
    use pm_core::instrument::TokenId;
    use pm_core::num::{Bps, Px, Qty, TickSize};
    use pm_engine::{Action, GasTable};
    use std::collections::HashMap;

    /// Static in-memory BookSource for tests.
    struct MapBooks(HashMap<TokenId, Book>);

    impl BookSource for MapBooks {
        async fn book(&mut self, token: TokenId) -> Option<Book> {
            self.0.get(&token).cloned()
        }
    }

    fn cent_book(asks: &[(u16, u64)], bids: &[(u16, u64)]) -> Book {
        let mut b = Book::new(TickSize::Cent);
        for &(t, q) in asks {
            b.apply(Side::Ask, Px::new(t, TickSize::Cent).unwrap(), Qty(q));
        }
        for &(t, q) in bids {
            b.apply(Side::Bid, Px::new(t, TickSize::Cent).unwrap(), Qty(q));
        }
        b
    }

    fn gas() -> GasTable {
        GasTable {
            split: 10_000,
            merge: 10_000,
            redeem: 15_000,
            negrisk_convert: 20_000,
        }
    }

    fn buy(token: u64, limit: u16, qty: u64) -> Order {
        Order::new(
            "fp".into(),
            TokenId(token),
            Action::Buy,
            TickSize::Cent,
            Px::new(limit, TickSize::Cent).unwrap(),
            Qty(qty),
            Bps(0),
        )
    }

    #[tokio::test]
    async fn buy_walks_asks_up_to_limit_only() {
        // asks: 100sh @44, 50sh @46, 50sh @48; limit 46, want 200sh
        let books = MapBooks(HashMap::from([(
            TokenId(1),
            cent_book(
                &[(44, 100_000_000), (46, 50_000_000), (48, 50_000_000)],
                &[(40, 1_000_000)],
            ),
        )]));
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        let out = v.submit_fak(&buy(1, 46, 200_000_000)).await.unwrap();
        assert_eq!(out.filled, Qty(150_000_000)); // 48s untouched
        // cash: −(44_000_000 + 23_000_000) = −67_000_000
        let cash: i128 = out.fills.iter().map(|f| f.cash.0).sum();
        assert_eq!(cash, -67_000_000);
        assert_eq!(out.fills.len(), 2);
    }

    #[tokio::test]
    async fn fees_are_charged_and_rounded_against_us() {
        // 200 bps fee, 10 shares @ 0.44
        let books = MapBooks(HashMap::from([(
            TokenId(1),
            cent_book(&[(44, 10_000_000)], &[(40, 1)]),
        )]));
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        let mut o = buy(1, 44, 10_000_000);
        o.fee_bps = Bps(200);
        let out = v.submit_fak(&o).await.unwrap();
        let expected_fee = pm_core::fees::fee_microusdc(Bps(200), 440_000, Qty(10_000_000));
        assert_eq!(out.fills[0].fee, expected_fee);
        assert_eq!(out.fills[0].cash.0, -(4_400_000 + expected_fee.0));
    }

    #[tokio::test]
    async fn sell_walks_bids_down_to_limit_only() {
        let books = MapBooks(HashMap::from([(
            TokenId(1),
            cent_book(
                &[(60, 1)],
                &[(50, 30_000_000), (48, 30_000_000), (45, 30_000_000)],
            ),
        )]));
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        let mut o = buy(1, 48, 90_000_000);
        o.action = Action::Sell;
        let out = v.submit_fak(&o).await.unwrap();
        assert_eq!(out.filled, Qty(60_000_000)); // 45s below limit
        let cash: i128 = out.fills.iter().map(|f| f.cash.0).sum();
        assert_eq!(cash, 15_000_000 + 14_400_000);
    }

    #[tokio::test]
    async fn book_moved_between_detect_and_fill_fills_less() {
        // Honesty test (spec §21): the venue reads the CURRENT book.
        let books = MapBooks(HashMap::from([(
            TokenId(1),
            cent_book(&[(44, 5_000_000)], &[(40, 1)]),
        )]));
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        // detector thought 100sh were there; only 5sh remain
        let out = v.submit_fak(&buy(1, 44, 100_000_000)).await.unwrap();
        assert_eq!(out.filled, Qty(5_000_000));
    }

    #[tokio::test]
    async fn missing_book_is_venue_error() {
        let books = MapBooks(HashMap::new());
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        assert!(matches!(
            v.submit_fak(&buy(1, 44, 1)).await,
            Err(VenueError::BookUnavailable(TokenId(1)))
        ));
    }

    #[tokio::test]
    async fn split_and_merge_cash_includes_gas_against_us() {
        let books = MapBooks(HashMap::new());
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        use pm_core::instrument::MarketId;
        // split 100 units: −(100_000_000 + 10_000)
        let c = v.split(MarketId(0), Qty(100_000_000)).await.unwrap();
        assert_eq!(c.0, -100_010_000);
        // merge 100 units: +(100_000_000 − 10_000)
        let p = v.merge(MarketId(0), Qty(100_000_000)).await.unwrap();
        assert_eq!(p.0, 99_990_000);
    }

    #[tokio::test]
    async fn submit_all_shares_one_latency_and_reads_all_books() {
        tokio::time::pause();
        let books = MapBooks(HashMap::from([
            (TokenId(1), cent_book(&[(44, 100_000_000)], &[(40, 1)])),
            (TokenId(2), cent_book(&[(50, 100_000_000)], &[(40, 1)])),
        ]));
        let mut v = PaperVenue::new(books, std::time::Duration::from_millis(200), gas());
        let orders = vec![buy(1, 44, 100_000_000), buy(2, 50, 100_000_000)];
        let fut = v.submit_all(&orders);
        let outs = fut.await; // auto-advanced paused time: one 200ms sleep total
        assert_eq!(outs.len(), 2);
        assert_eq!(outs[0].as_ref().unwrap().filled, Qty(100_000_000));
        assert_eq!(outs[1].as_ref().unwrap().filled, Qty(100_000_000));
    }

    #[tokio::test]
    async fn ask_exactly_at_limit_fills() {
        let books = MapBooks(HashMap::from([(
            TokenId(1),
            cent_book(&[(46, 50_000_000)], &[(40, 1)]),
        )]));
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        let out = v.submit_fak(&buy(1, 46, 50_000_000)).await.unwrap();
        assert_eq!(out.filled, Qty(50_000_000)); // <= is inclusive: exactly-at-limit fills
    }

    #[tokio::test]
    async fn empty_side_book_fills_nothing_without_error() {
        // book with bids only; buying finds no asks → zero fill, not an error
        let books = MapBooks(HashMap::from([(
            TokenId(1),
            cent_book(&[], &[(40, 1_000_000)]),
        )]));
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        let out = v.submit_fak(&buy(1, 50, 1_000_000)).await.unwrap();
        assert_eq!(out.filled, Qty(0));
        assert!(out.fills.is_empty());
    }

    #[tokio::test]
    async fn dust_sell_with_fee_can_have_negative_cash() {
        // Honest venue: sell 1 µshare @ tick 1, fee 200 bps → proceeds floor 0,
        // fee ceil 1 → cash = −1. Downstream ledgers must accept negative sell cash.
        let books = MapBooks(HashMap::from([(
            TokenId(1),
            cent_book(&[(60, 1)], &[(1, 1_000_000)]),
        )]));
        let mut v = PaperVenue::new(books, std::time::Duration::ZERO, gas());
        let mut o = buy(1, 1, 1);
        o.action = Action::Sell;
        o.fee_bps = Bps(200);
        let out = v.submit_fak(&o).await.unwrap();
        assert_eq!(out.filled, Qty(1));
        assert_eq!(out.fills[0].cash.0, -1);
        assert_eq!(out.fills[0].fee.0, 1);
    }
}
