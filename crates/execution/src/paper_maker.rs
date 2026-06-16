//! PAPER maker-fill simulator (Task 4.1): a [`PaperMakerVenue`] that lets the
//! (Task 4.2) market-making strategy run end-to-end with **zero live orders**.
//!
//! It implements BOTH the resting-order [`MakerVenue`] (place/cancel/replace/
//! open-orders) and the [`UserFillSource`] (poll for fills) over the SAME
//! [`BookSource`] the taker [`crate::venue::PaperVenue`] reads, so a strategy
//! can quote, get filled, and re-quote against the live book without ever
//! touching the wire. It is the maker dual of `PaperVenue::fill_now`: that walks
//! the opposing ladder to TAKE liquidity; this watches the opposing ladder for
//! prices that cross BACK to a resting quote.
//!
//! INERT until Task 4.2 wires it: nothing here is exercised by the running app
//! yet.
//!
//! # Trade-through fill model (the heart)
//! A resting quote fills when the opposing best price crosses to (or through)
//! our limit. The fill quantity is capped by the opposing liquidity visible
//! **at or through our price** — a deliberately CONSERVATIVE proxy, because the
//! paper sim has the book but NOT the trade tape:
//!
//! - A resting **Bid** at `price` is hit when `best_ask <= price` (a seller
//!   crossed down to us). `crossing_liquidity` = total ask size at ticks
//!   `<= price`; `filled = min(remaining, crossing_liquidity)`.
//! - A resting **Ask** at `price` is hit when `best_bid >= price` (a buyer
//!   crossed up to us). `crossing_liquidity` = total bid size at ticks
//!   `>= price`; `filled = min(remaining, crossing_liquidity)`.
//!
//! Decisions baked into this model:
//! - **Fill at our OWN price.** The maker gets its posted limit (price-time
//!   priority), NOT the crossing taker's price. The emitted [`MakerFill::px`] is
//!   always the resting `price`.
//! - **Crossing-liquidity cap.** Without a trade tape we cannot know how much of
//!   the crossing volume was OURS, so we cap a fill by the opposing depth at/through
//!   our price. This never over-fills (and may under-fill vs. a real venue —
//!   honest in our favour for a paper sim).
//! - **`crossing_liquidity == 0` ⇔ not crossed**, so the same walk handles the
//!   no-fill case: no opposing size at/through our price means `filled == 0`.
//! - **Partial fills rest.** When `crossing_liquidity < remaining` the order
//!   stays resting with a reduced `remaining`; a later poll with more crossing
//!   liquidity fills the rest.
//! - **Emitted once.** A fill is emitted exactly once because `remaining` is
//!   decremented (and the order removed at zero) — no separate dedup set is
//!   needed (unlike the live [`UserFillSource`], which dedups recurring trade
//!   rows).
//!
//! # postOnly crossing rejection
//! [`place`](MakerVenue::place) mirrors the live `INVALID_POST_ONLY_ORDER`
//! behaviour: a `post_only` order that is marketable against the CURRENT book (a
//! Bid with `price >= best_ask`, or an Ask with `price <= best_bid`) is rejected
//! with [`VenueError::Live`]. If the book is unavailable we cannot PROVE a cross,
//! so we allow the place (and likewise skip — never fill — a resting order whose
//! book is unavailable on [`poll`](UserFillSource::poll)).
//!
//! # YAGNI / out of scope
//! No latency (the taker `PaperVenue` models latency; the maker sim omits it for
//! simplicity), no queue-position modelling, no GTC/GTD expiry, no inventory, no
//! strategy loop — those live in Task 4.2 and beyond.

use std::collections::HashMap;

use pm_core::book::{Book, Side};
use pm_core::instrument::TokenId;
use pm_core::num::{Px, Qty};

use crate::fills::{MakerFill, UserFillSource};
use crate::maker::{MakerOrder, MakerVenue, OpenOrder, OrderId};
use crate::venue::{BookSource, VenueError};

/// One resting paper order. The map key is its [`OrderId`]; `seq` is the
/// monotonic placement index (== the paper id's number) used to iterate resting
/// orders in a deterministic, insertion-ordered way for `open_orders`/`poll`.
#[derive(Debug, Clone)]
struct RestingPaperOrder {
    token: TokenId,
    side: Side,
    price: Px,
    remaining: Qty,
    seq: u64,
}

/// PAPER maker venue + fill simulator over a [`BookSource`] (Task 4.1). See the
/// module docs for the trade-through fill model and the postOnly-cross rule.
pub struct PaperMakerVenue<B: BookSource> {
    books: B,
    /// Currently-resting orders, keyed by their minted paper id.
    resting: HashMap<OrderId, RestingPaperOrder>,
    /// Monotonic order-id counter; pre-incremented so the first id is `paper-1`.
    next_id: u64,
    /// Monotonic fill counter; pre-incremented so the first trade id is
    /// `paper-fill-1`.
    next_fill: u64,
}

impl<B: BookSource> PaperMakerVenue<B> {
    pub fn new(books: B) -> Self {
        PaperMakerVenue {
            books,
            resting: HashMap::new(),
            next_id: 0,
            next_fill: 0,
        }
    }

    /// Resting orders in deterministic placement order (`seq` ascending), as
    /// `(seq, id)` so callers don't re-borrow the map in a sort closure.
    fn ordered_ids(&self) -> Vec<(u64, OrderId)> {
        let mut ids: Vec<(u64, OrderId)> = self
            .resting
            .iter()
            .map(|(id, r)| (r.seq, id.clone()))
            .collect();
        ids.sort_by_key(|(seq, _)| *seq);
        ids
    }
}

/// Opposing liquidity visible AT OR THROUGH a resting order's `price` — the
/// conservative cap on a single poll's fill (see module docs).
///
/// - A resting **Bid** is crossed by sellers: sum ask size at ticks `<= price`.
/// - A resting **Ask** is crossed by buyers: sum bid size at ticks `>= price`.
///
/// `iter_from_best()` is monotonic (asks ascend, bids descend from best), so
/// `take_while` stops at the first level beyond our price. A `0` result means
/// the book is NOT crossed for this order.
fn crossing_liquidity(book: &Book, side: Side, price: Px) -> u64 {
    match side {
        Side::Bid => book
            .asks
            .iter_from_best()
            .take_while(|(ask_px, _)| ask_px.get() <= price.get())
            .map(|(_, q)| q.0)
            .sum(),
        Side::Ask => book
            .bids
            .iter_from_best()
            .take_while(|(bid_px, _)| bid_px.get() >= price.get())
            .map(|(_, q)| q.0)
            .sum(),
    }
}

impl<B: BookSource> MakerVenue for PaperMakerVenue<B> {
    /// Mint a paper id (`paper-{n}`) and rest the order. A `post_only` order
    /// that would cross the CURRENT book is rejected (live
    /// `INVALID_POST_ONLY_ORDER` parity); an unavailable book can't prove a
    /// cross, so the place is allowed.
    async fn place(&mut self, o: &MakerOrder) -> Result<OrderId, VenueError> {
        if o.post_only {
            // Book unavailable → cannot prove a cross → allow (documented).
            if let Some(book) = self.books.book(o.token).await {
                let would_cross = match o.side {
                    Side::Bid => book.asks.best().is_some_and(|ask| o.price.get() >= ask.get()),
                    Side::Ask => book.bids.best().is_some_and(|bid| o.price.get() <= bid.get()),
                };
                if would_cross {
                    return Err(VenueError::Live("paper: post-only would cross".into()));
                }
            }
        }
        self.next_id += 1;
        let seq = self.next_id;
        let id = OrderId(format!("paper-{seq}"));
        self.resting.insert(
            id.clone(),
            RestingPaperOrder {
                token: o.token,
                side: o.side,
                price: o.price,
                remaining: o.size,
                seq,
            },
        );
        Ok(id)
    }

    /// Idempotent: cancelling an id that isn't resting is a clean `Ok(())`
    /// (matches the live + mock contract).
    async fn cancel(&mut self, id: &OrderId) -> Result<(), VenueError> {
        self.resting.remove(id);
        Ok(())
    }

    /// Cancel-then-place (same as live; CLOB V2 has no native amend). Returns
    /// the NEW id.
    async fn replace(&mut self, id: &OrderId, o: &MakerOrder) -> Result<OrderId, VenueError> {
        self.cancel(id).await?;
        self.place(o).await
    }

    /// The currently-resting orders (remaining size), in placement order.
    async fn open_orders(&mut self) -> Result<Vec<OpenOrder>, VenueError> {
        Ok(self
            .ordered_ids()
            .into_iter()
            .filter_map(|(_, id)| {
                self.resting.get(&id).map(|r| OpenOrder {
                    id: id.clone(),
                    token: r.token,
                    side: r.side,
                    price: r.price,
                    size: r.remaining,
                })
            })
            .collect())
    }
}

impl<B: BookSource> UserFillSource for PaperMakerVenue<B> {
    /// Apply the trade-through fill model to every resting order against its
    /// token's CURRENT book (see module docs). Orders whose book is unavailable
    /// are skipped (no fill this poll). Each produced fill decrements
    /// `remaining`; a fully-filled order is removed.
    async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError> {
        let mut fills = Vec::new();
        for (_, id) in self.ordered_ids() {
            // Copy the order's params out so the immutable borrow ends before
            // the async book read + later mutation.
            let Some((token, side, price, remaining)) = self
                .resting
                .get(&id)
                .map(|r| (r.token, r.side, r.price, r.remaining))
            else {
                continue;
            };
            // Book unavailable → skip (cannot price a fill this poll).
            let Some(book) = self.books.book(token).await else {
                continue;
            };
            let filled = crossing_liquidity(&book, side, price).min(remaining.0);
            if filled == 0 {
                continue;
            }
            self.next_fill += 1;
            fills.push(MakerFill {
                order_id: id.clone(),
                token,
                // Maker fills at ITS OWN price (price-time priority), not the
                // crossing taker's.
                px: price,
                qty: Qty(filled),
                trade_id: format!("paper-fill-{}", self.next_fill),
            });
            let new_remaining = remaining.0 - filled;
            if new_remaining == 0 {
                self.resting.remove(&id);
            } else if let Some(r) = self.resting.get_mut(&id) {
                r.remaining = Qty(new_remaining);
            }
        }
        Ok(fills)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::maker::OrderType;
    use pm_core::book::{Book, Side};
    use pm_core::instrument::TokenId;
    use pm_core::num::{Px, Qty, TickSize};
    use std::collections::HashMap;

    /// Mutable in-memory [`BookSource`]: tests rewrite the map between polls to
    /// drive the book toward/away from a cross.
    struct MockBooks(HashMap<TokenId, Book>);

    impl BookSource for MockBooks {
        async fn book(&mut self, token: TokenId) -> Option<Book> {
            self.0.get(&token).cloned()
        }
    }

    const SH: u64 = 1_000_000; // one share in µshares

    fn px(tick: u16) -> Px {
        Px::new(tick, TickSize::Cent).unwrap()
    }

    /// Build a Cent book from `(tick, qty)` ask and bid levels.
    fn cent_book(asks: &[(u16, u64)], bids: &[(u16, u64)]) -> Book {
        let mut b = Book::new(TickSize::Cent);
        for &(t, q) in asks {
            b.apply(Side::Ask, px(t), Qty(q));
        }
        for &(t, q) in bids {
            b.apply(Side::Bid, px(t), Qty(q));
        }
        b
    }

    fn maker(side: Side, tick: u16, size: u64) -> MakerOrder {
        MakerOrder {
            token: TokenId(1),
            side,
            price: px(tick),
            size: Qty(size),
            order_type: OrderType::Gtc,
            post_only: true,
        }
    }

    fn bid(tick: u16, size: u64) -> MakerOrder {
        maker(Side::Bid, tick, size)
    }

    fn ask(tick: u16, size: u64) -> MakerOrder {
        maker(Side::Ask, tick, size)
    }

    fn venue(book: Book) -> PaperMakerVenue<MockBooks> {
        PaperMakerVenue::new(MockBooks(HashMap::from([(TokenId(1), book)])))
    }

    #[tokio::test]
    async fn resting_bid_fills_when_ask_crosses() {
        // best ask 0.50 → a postOnly bid @0.48 does not cross at place.
        let mut v = venue(cent_book(&[(50, 100 * SH)], &[(40, 100 * SH)]));
        let id = v.place(&bid(48, 100 * SH)).await.unwrap();

        // First poll: best ask 0.50 > 0.48 → no cross → no fill, still resting.
        assert!(v.poll().await.unwrap().is_empty());
        assert_eq!(v.open_orders().await.unwrap().len(), 1);

        // Seller crosses down to 0.48.
        v.books
            .0
            .insert(TokenId(1), cent_book(&[(48, 100 * SH)], &[(40, 100 * SH)]));
        let fills = v.poll().await.unwrap();
        assert_eq!(fills.len(), 1);
        let f = &fills[0];
        assert_eq!(f.order_id, id);
        assert_eq!(f.token, TokenId(1));
        assert_eq!(f.px, px(48), "maker fills at its OWN price");
        assert_eq!(f.qty, Qty(100 * SH), "capped by ask liquidity at <=0.48");
        assert_eq!(f.trade_id, "paper-fill-1");
        // Fully filled → gone.
        assert!(v.open_orders().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn resting_ask_fills_when_bid_crosses() {
        // best bid 0.50 → a postOnly ask @0.52 does not cross at place.
        let mut v = venue(cent_book(&[(60, 100 * SH)], &[(50, 100 * SH)]));
        let id = v.place(&ask(52, 100 * SH)).await.unwrap();

        // First poll: best bid 0.50 < 0.52 → no cross → no fill.
        assert!(v.poll().await.unwrap().is_empty());

        // Buyer crosses up to 0.52.
        v.books
            .0
            .insert(TokenId(1), cent_book(&[(60, 100 * SH)], &[(52, 100 * SH)]));
        let fills = v.poll().await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, id);
        assert_eq!(fills[0].px, px(52), "maker fills at its OWN price");
        assert_eq!(fills[0].qty, Qty(100 * SH), "capped by bid liquidity at >=0.52");
        assert_eq!(fills[0].trade_id, "paper-fill-1");
        assert!(v.open_orders().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_fill_when_not_crossed() {
        let mut v = venue(cent_book(&[(50, 100 * SH)], &[(40, 100 * SH)]));
        let id = v.place(&bid(45, 100 * SH)).await.unwrap();
        // Book never crosses to 0.45.
        assert!(v.poll().await.unwrap().is_empty());
        assert!(v.poll().await.unwrap().is_empty());
        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id);
        assert_eq!(open[0].size, Qty(100 * SH), "remaining unchanged");
    }

    #[tokio::test]
    async fn partial_fill_capped_by_available_size() {
        let mut v = venue(cent_book(&[(50, 100 * SH)], &[(40, 100 * SH)]));
        let id = v.place(&bid(48, 100 * SH)).await.unwrap();

        // Cross with only 30sh at <=0.48; the 0.49 ask (huge) must be EXCLUDED
        // (49 > 48), proving the at-or-through-our-price cap.
        v.books.0.insert(
            TokenId(1),
            cent_book(&[(48, 30 * SH), (49, 1000 * SH)], &[(40, 100 * SH)]),
        );
        let fills = v.poll().await.unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, Qty(30 * SH), "capped by crossing liquidity");
        assert_eq!(fills[0].px, px(48));
        assert_eq!(fills[0].trade_id, "paper-fill-1");

        // Order stays resting with reduced remaining.
        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id);
        assert_eq!(open[0].size, Qty(70 * SH));

        // A later poll with more liquidity fills the rest; trade id advances.
        v.books
            .0
            .insert(TokenId(1), cent_book(&[(48, 1000 * SH)], &[(40, 100 * SH)]));
        let fills2 = v.poll().await.unwrap();
        assert_eq!(fills2.len(), 1);
        assert_eq!(fills2[0].qty, Qty(70 * SH));
        assert_eq!(fills2[0].px, px(48));
        assert_eq!(fills2[0].trade_id, "paper-fill-2");
        assert!(v.open_orders().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn postonly_place_that_would_cross_is_rejected() {
        // best ask 0.49 → a postOnly bid @0.50 is marketable → reject.
        let mut v = venue(cent_book(&[(49, 100 * SH)], &[(40, 100 * SH)]));
        match v.place(&bid(50, 100 * SH)).await {
            Err(VenueError::Live(msg)) => assert!(msg.contains("post-only would cross"), "{msg}"),
            other => panic!("expected Live cross-rejection, got {other:?}"),
        }
        assert!(v.open_orders().await.unwrap().is_empty(), "nothing rested");

        // Symmetric: postOnly ask @0.40 <= best bid 0.40 → reject.
        assert!(matches!(
            v.place(&ask(40, 100 * SH)).await,
            Err(VenueError::Live(_))
        ));

        // A NON-postOnly crossing order is NOT rejected (the gate is postOnly-only).
        let mut crossing = bid(50, 100 * SH);
        crossing.post_only = false;
        v.place(&crossing).await.unwrap();
        assert_eq!(v.open_orders().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancel_is_idempotent_and_open_orders_reflects_state() {
        let mut v = venue(cent_book(&[(50, 100 * SH)], &[(40, 100 * SH)]));
        let id1 = v.place(&bid(45, 100 * SH)).await.unwrap();
        let id2 = v.place(&bid(44, 50 * SH)).await.unwrap();
        assert_ne!(id1, id2);
        assert_eq!(v.open_orders().await.unwrap().len(), 2);

        // Cancel id1, then double-cancel, then cancel an unknown id — all Ok.
        v.cancel(&id1).await.unwrap();
        v.cancel(&id1).await.unwrap();
        v.cancel(&OrderId("paper-999".into())).await.unwrap();

        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id2);
        assert_eq!(open[0].token, TokenId(1));
        assert_eq!(open[0].side, Side::Bid);
        assert_eq!(open[0].price, px(44));
        assert_eq!(open[0].size, Qty(50 * SH));
    }

    #[tokio::test]
    async fn fill_emitted_once() {
        // Place uncrossed, then move the book to a full cross.
        let mut v = venue(cent_book(&[(50, 100 * SH)], &[(40, 100 * SH)]));
        let id = v.place(&bid(48, 100 * SH)).await.unwrap();
        v.books
            .0
            .insert(TokenId(1), cent_book(&[(48, 1000 * SH)], &[(40, 100 * SH)]));

        let f1 = v.poll().await.unwrap();
        assert_eq!(f1.len(), 1);
        assert_eq!(f1[0].order_id, id);
        assert_eq!(f1[0].qty, Qty(100 * SH));

        // The order is gone; a second poll (book still crossed) emits nothing.
        let f2 = v.poll().await.unwrap();
        assert!(f2.is_empty(), "fully-filled order must not re-emit: {f2:?}");
        assert!(v.open_orders().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn replace_rekeys_and_repositions() {
        let mut v = venue(cent_book(&[(50, 100 * SH)], &[(40, 100 * SH)]));
        let id1 = v.place(&bid(45, 100 * SH)).await.unwrap();
        let id2 = v.replace(&id1, &bid(46, 50 * SH)).await.unwrap();
        assert_ne!(id1, id2);
        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id2);
        assert_eq!(open[0].price, px(46));
        assert_eq!(open[0].size, Qty(50 * SH));
    }

    #[tokio::test]
    async fn book_unavailable_allows_place_and_skips_fill() {
        // No books registered at all.
        let mut v = PaperMakerVenue::new(MockBooks(HashMap::new()));
        // postOnly place with no book → cannot prove a cross → allowed.
        let id = v.place(&bid(48, 100 * SH)).await.unwrap();
        assert_eq!(v.open_orders().await.unwrap().len(), 1);
        // poll with no book → skip → no fill, order remains.
        assert!(v.poll().await.unwrap().is_empty());
        let open = v.open_orders().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, id);
        assert_eq!(open[0].size, Qty(100 * SH));
    }
}
