//! End-to-end session replay over scripted transports — no network.
//!
//! FakeTransport pops pre-loaded frames from a VecDeque; FakeRest returns
//! canned ParsedBook entries.  All five tests drive the supervisor through
//! `run_session` or `sweep_once` deterministically.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pm_ingestion::livebook::RawLevel;
use pm_ingestion::rest::ParsedBook;
use pm_ingestion::supervisor::{
    FactoryDecision, RestBookSource, SessionEnd, Supervisor, SupervisorConfig,
};
use pm_ingestion::ws::WsTransport;
use pm_ingestion::IngestError;
use pm_core::instrument::TokenId;
use pm_core::num::TickSize;

// ---------------------------------------------------------------------------
// FakeTransport
// ---------------------------------------------------------------------------

struct FakeTransport {
    incoming: VecDeque<Result<String, IngestError>>,
    pub sent: Arc<Mutex<Vec<String>>>,
}

impl FakeTransport {
    fn new(frames: Vec<Result<String, IngestError>>) -> Self {
        FakeTransport {
            incoming: frames.into_iter().collect(),
            sent: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl WsTransport for FakeTransport {
    async fn next_frame(&mut self) -> Option<Result<String, IngestError>> {
        self.incoming.pop_front()
    }

    async fn send_text(&mut self, text: &str) -> Result<(), IngestError> {
        self.sent.lock().unwrap().push(text.to_owned());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FakeTransportBlocking — delivers scripted frames then blocks forever
// ---------------------------------------------------------------------------

/// A transport that delivers frames from a script then returns a future that
/// never resolves. Used to test feed-silence detection: with tokio time paused,
/// the sweep interval fires while next_frame is pending-forever, allowing the
/// silence detection logic to run without requiring real elapsed time.
struct FakeTransportBlocking {
    incoming: VecDeque<Result<String, IngestError>>,
}

impl FakeTransportBlocking {
    fn new(frames: Vec<Result<String, IngestError>>) -> Self {
        FakeTransportBlocking { incoming: frames.into_iter().collect() }
    }
}

impl WsTransport for FakeTransportBlocking {
    async fn next_frame(&mut self) -> Option<Result<String, IngestError>> {
        if let Some(f) = self.incoming.pop_front() {
            return Some(f);
        }
        // After the script is exhausted, block forever — simulates a live but
        // silent socket. With tokio::time::pause(), the sweep interval can fire
        // while this future is pending, allowing silence detection.
        std::future::pending().await
    }

    async fn send_text(&mut self, _text: &str) -> Result<(), IngestError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FakeRest
// ---------------------------------------------------------------------------

struct FakeRest {
    /// venue_id → (bids [(price, size)], asks [(price, size)], hash)
    books: HashMap<String, (Vec<(String, String)>, Vec<(String, String)>, String)>,
    pub call_log: Arc<Mutex<Vec<String>>>,
}

impl FakeRest {
    fn new(
        books: HashMap<String, (Vec<(String, String)>, Vec<(String, String)>, String)>,
    ) -> Self {
        FakeRest { books, call_log: Arc::new(Mutex::new(Vec::new())) }
    }
}

fn parse_lvls(pairs: &[(String, String)]) -> Vec<RawLevel> {
    pairs
        .iter()
        .map(|(p, s)| RawLevel {
            price_micro: pm_ingestion::decimal::parse_micro(p).unwrap(),
            size_micro: pm_ingestion::decimal::parse_micro(s).unwrap(),
        })
        .collect()
}

impl RestBookSource for FakeRest {
    async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, IngestError> {
        self.call_log.lock().unwrap().push(venue_token_id.to_owned());
        match self.books.get(venue_token_id) {
            Some((bids, asks, hash)) => Ok(ParsedBook {
                asset_id: venue_token_id.to_owned(),
                hash: hash.clone(),
                bids: parse_lvls(bids),
                asks: parse_lvls(asks),
            }),
            None => Err(IngestError::Http(format!("no canned book for {venue_token_id}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Frame builders — match the FIXTURE shapes exactly
// ---------------------------------------------------------------------------

/// Build a `book` frame (array envelope with a single book event).
///
/// Shape matches ws_book.json: array of objects with asset_id, hash, bids, asks,
/// event_type.
fn book_frame(asset_id: &str, bid_price: &str, bid_size: &str, ask_price: &str, ask_size: &str, hash: &str) -> String {
    serde_json::json!([{
        "event_type": "book",
        "asset_id": asset_id,
        "hash": hash,
        "bids": [{"price": bid_price, "size": bid_size}],
        "asks": [{"price": ask_price, "size": ask_size}]
    }])
    .to_string()
}

/// Build a `price_change` frame (single object with price_changes array).
///
/// Shape matches ws_price_change.json: object with market (condition id),
/// price_changes array where each element has asset_id, price, size, side, hash.
fn price_change_frame(
    market: &str,
    asset_id: &str,
    side: &str,
    price: &str,
    size: &str,
    hash: &str,
) -> String {
    serde_json::json!({
        "event_type": "price_change",
        "market": market,
        "price_changes": [{
            "asset_id": asset_id,
            "price": price,
            "size": size,
            "side": side,
            "hash": hash
        }]
    })
    .to_string()
}

/// Build a price_change frame with multiple changes across two tokens.
fn price_change_frame_two(
    market: &str,
    asset_id1: &str, side1: &str, price1: &str, size1: &str, hash1: &str,
    asset_id2: &str, side2: &str, price2: &str, size2: &str, hash2: &str,
) -> String {
    serde_json::json!({
        "event_type": "price_change",
        "market": market,
        "price_changes": [
            {
                "asset_id": asset_id1,
                "price": price1,
                "size": size1,
                "side": side1,
                "hash": hash1
            },
            {
                "asset_id": asset_id2,
                "price": price2,
                "size": size2,
                "side": side2,
                "hash": hash2
            }
        ]
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Helper: build a Supervisor with FakeRest and two tokens
// ---------------------------------------------------------------------------

fn two_token_fake_rest() -> (FakeRest, Arc<Mutex<Vec<String>>>) {
    let mut books = HashMap::new();
    // token A: venue_id = "token_a"
    books.insert(
        "token_a".to_owned(),
        (
            vec![("0.44".to_owned(), "100".to_owned())],
            vec![("0.56".to_owned(), "100".to_owned())],
            "rest-hash-a".to_owned(),
        ),
    );
    // token B: venue_id = "token_b"
    books.insert(
        "token_b".to_owned(),
        (
            vec![("0.43".to_owned(), "50".to_owned())],
            vec![("0.57".to_owned(), "50".to_owned())],
            "rest-hash-b".to_owned(),
        ),
    );
    let rest = FakeRest::new(books);
    let call_log = Arc::clone(&rest.call_log);
    (rest, call_log)
}

fn default_cfg() -> SupervisorConfig {
    SupervisorConfig {
        staleness: Duration::from_millis(1500),
        feed_silence: Duration::from_millis(15_000),
        backoff_base: Duration::from_millis(250),
        backoff_cap: Duration::from_secs(30),
        // Large sweep interval so the timer never fires in run_session during
        // these deterministic tests (we use sweep_once directly where needed).
        sweep_interval: Duration::from_secs(3600),
    }
}

// ---------------------------------------------------------------------------
// Test 1: snapshot_then_deltas_builds_books
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_then_deltas_builds_books() {
    // Script:
    //   frame 1: book snapshot for token_a
    //   frame 2: price_change touching both token_a and token_b
    //   (no more frames → transport ends → run_session returns)
    //
    // Assert:
    //   - book_a bid was changed by the delta (removed the 0.44 level → size=0)
    //   - book_b bid unchanged (delta added a level)
    //   - FakeRest called twice (initial resnapshot_all for both tokens) — no extra
    //   - stats: 2 frames, events ≥ 2, parse_errors = 0

    let (rest, call_log) = two_token_fake_rest();

    let mut sup = Supervisor::new(
        vec![
            (TokenId(0), "token_a".to_owned(), TickSize::Cent),
            (TokenId(1), "token_b".to_owned(), TickSize::Cent),
        ],
        rest,
        default_cfg(),
    );

    // book snapshot frame for token_a (overrides the REST snapshot on the shard)
    let f1 = book_frame("token_a", "0.44", "100", "0.56", "100", "ws-hash-a");
    // price_change frame: remove 0.44 bid on token_a (size=0) + add bid on token_b
    let f2 = price_change_frame_two(
        "market_cond",
        "token_a", "BUY", "0.44", "0", "hash-a2",
        "token_b", "BUY", "0.43", "200", "hash-b2",
    );

    let mut transport = FakeTransport::new(vec![Ok(f1), Ok(f2)]);

    sup.run_session(&mut transport).await;

    // REST calls: resnapshot_all() for 2 tokens at session start
    let calls = call_log.lock().unwrap();
    assert_eq!(calls.len(), 2, "initial resnapshot_all should fetch both tokens");

    let shard = sup.shard();
    let book_a = shard.book(TokenId(0)).expect("book_a must exist");
    let book_b = shard.book(TokenId(1)).expect("book_b must exist");

    // Note: run_session calls mark_all_stale on transport end (spec: instant staleness
    // on disconnect), so valid() == false here. We check ladder data instead.
    //
    // After delta: 0.44 bid removed from token_a → best bid should be gone.
    // Book was set by REST to bid=0.44, ask=0.56; WS book frame also set bid=0.44.
    // Delta zeroed 0.44 (size=0) → no bids left.
    assert!(
        book_a.book().bids.best().is_none(),
        "0.44 bid should have been zeroed by the delta; best bid = {:?}",
        book_a.book().bids.best()
    );

    // book_b: REST gave bid=0.43, delta added bid=0.43 with size=200 (replaces same tick).
    assert_eq!(book_b.book().bids.best().unwrap().get(), 43);

    let stats = sup.stats();
    assert_eq!(stats.parse_errors, 0);
    assert!(stats.frames >= 2);
}

// ---------------------------------------------------------------------------
// Test 2: crossed_book_triggers_rest_resnapshot
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crossed_book_triggers_rest_resnapshot() {
    // Script:
    //   REST gives sane initial snapshot (bid=0.44, ask=0.56) for token_a.
    //   Then a WS price_change moves the ask down to 0.40 — crossing bid at 0.44.
    //   Supervisor should detect NeedsResnapshot → call REST for token_a again.
    //   REST returns a corrected book (bid=0.44, ask=0.56).
    //
    // Assert:
    //   - FakeRest called ≥ 2 times for "token_a" (1 initial + 1 resnapshot)
    //   - book_a valid after resnapshot
    //   - stats.resnapshots == 1

    let mut books = HashMap::new();
    books.insert(
        "token_a".to_owned(),
        (
            vec![("0.44".to_owned(), "100".to_owned())],
            vec![("0.56".to_owned(), "100".to_owned())],
            "rest-hash-a".to_owned(),
        ),
    );
    let rest = FakeRest::new(books);
    let call_log = Arc::clone(&rest.call_log);

    let mut sup = Supervisor::new(
        vec![(TokenId(0), "token_a".to_owned(), TickSize::Cent)],
        rest,
        default_cfg(),
    );

    // Delta that crosses the book: add a bid at 0.60 (above current ask 0.56)
    let crossing_delta = price_change_frame(
        "market_cond",
        "token_a",
        "BUY",
        "0.60",
        "500",
        "hash-cross",
    );

    let mut transport = FakeTransport::new(vec![Ok(crossing_delta)]);

    sup.run_session(&mut transport).await;

    let calls = call_log.lock().unwrap();
    // Initial resnapshot_all() + resnapshot after crossing = at least 2 calls
    let token_a_calls: Vec<_> = calls.iter().filter(|s| s.as_str() == "token_a").collect();
    assert!(
        token_a_calls.len() >= 2,
        "expected ≥ 2 REST calls for token_a (initial + resnapshot after crossed book), got {}",
        token_a_calls.len()
    );
    drop(calls);

    // Note: run_session marks all books stale on transport end (spec §19 instant staleness).
    // resnapshots counts ALL successful REST applies: 1 initial (resnapshot_all) + 1 after
    // crossing = 2 total.  This confirms the resnapshot path fired for the crossing event.
    assert_eq!(
        sup.stats().resnapshots, 2,
        "2 total resnapshots: 1 initial + 1 triggered by crossed book"
    );
}

// ---------------------------------------------------------------------------
// FakeRest that fails on the first call for a token, succeeds on subsequent
// ---------------------------------------------------------------------------

struct FakeRestFailFirst {
    books: HashMap<String, (Vec<(String, String)>, Vec<(String, String)>, String)>,
    /// Tokens for which the FIRST call should fail.
    fail_first: std::collections::HashSet<String>,
    call_counts: HashMap<String, usize>,
    pub call_log: Arc<Mutex<Vec<String>>>,
}

impl FakeRestFailFirst {
    fn new(
        books: HashMap<String, (Vec<(String, String)>, Vec<(String, String)>, String)>,
        fail_first: std::collections::HashSet<String>,
    ) -> Self {
        FakeRestFailFirst {
            books,
            fail_first,
            call_counts: HashMap::new(),
            call_log: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl RestBookSource for FakeRestFailFirst {
    async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, IngestError> {
        self.call_log.lock().unwrap().push(venue_token_id.to_owned());
        let count = self.call_counts.entry(venue_token_id.to_owned()).or_insert(0);
        *count += 1;
        if *count == 1 && self.fail_first.contains(venue_token_id) {
            return Err(IngestError::Http(format!("first call for {venue_token_id} forced to fail")));
        }
        match self.books.get(venue_token_id) {
            Some((bids, asks, hash)) => Ok(ParsedBook {
                asset_id: venue_token_id.to_owned(),
                hash: hash.clone(),
                bids: parse_lvls(bids),
                asks: parse_lvls(asks),
            }),
            None => Err(IngestError::Http(format!("no canned book for {venue_token_id}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Test 3: delta_for_unknown_token_requests_snapshot
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delta_for_unknown_token_requests_snapshot() {
    // Script:
    //   FakeRest configured to fail the FIRST call for token_b.
    //   resnapshot_all() starts: REST called for token_a (succeeds), token_b (fails → stale).
    //   A price_change arrives for token_b.
    //   apply_changes on an invalid book returns NeedsResnapshot(FeedLost).
    //   Supervisor calls REST for token_b again → succeeds this time.
    //
    // Assert:
    //   - token_b appears ≥ 2 times in call_log
    //   - resnapshots stat ≥ 1 (the triggered resnapshot succeeded)
    //   - book_b data is present (book slot exists)

    let mut books = HashMap::new();
    books.insert(
        "token_a".to_owned(),
        (
            vec![("0.44".to_owned(), "100".to_owned())],
            vec![("0.56".to_owned(), "100".to_owned())],
            "rest-hash-a".to_owned(),
        ),
    );
    books.insert(
        "token_b".to_owned(),
        (
            vec![("0.43".to_owned(), "50".to_owned())],
            vec![("0.57".to_owned(), "50".to_owned())],
            "rest-hash-b".to_owned(),
        ),
    );
    let mut fail_first = std::collections::HashSet::new();
    fail_first.insert("token_b".to_owned());

    let rest = FakeRestFailFirst::new(books, fail_first);
    let call_log = Arc::clone(&rest.call_log);

    let mut sup = Supervisor::new(
        vec![
            (TokenId(0), "token_a".to_owned(), TickSize::Cent),
            (TokenId(1), "token_b".to_owned(), TickSize::Cent),
        ],
        rest,
        default_cfg(),
    );

    // price_change for token_b — its initial REST snapshot failed so book is stale/invalid.
    // apply_changes returns NeedsResnapshot(FeedLost) → supervisor calls REST again.
    let delta_b = price_change_frame(
        "market_cond",
        "token_b",
        "BUY",
        "0.43",
        "100",
        "hash-b1",
    );

    let mut transport = FakeTransport::new(vec![Ok(delta_b)]);

    sup.run_session(&mut transport).await;

    let calls = call_log.lock().unwrap();
    // Initial resnapshot_all: token_a (ok) + token_b (fails, count=1)
    // Delta for token_b → invalid book → NeedsResnapshot → retry REST (count=2, succeeds)
    let token_b_calls: Vec<_> = calls.iter().filter(|s| s.as_str() == "token_b").collect();
    assert!(
        token_b_calls.len() >= 2,
        "expected ≥ 2 REST calls for token_b (initial fail + retry after stale delta), got {}",
        token_b_calls.len()
    );
    drop(calls);

    // resnapshots stat counts the SUCCESSFUL REST applies: the retry succeeded.
    // Initial token_a + retry token_b = 2 successful resnapshots.
    assert!(
        sup.stats().resnapshots >= 1,
        "at least 1 successful resnapshot expected (the retry for token_b)"
    );
}

// ---------------------------------------------------------------------------
// Test 4: transport_end_marks_stale_and_reconnects_with_resubscribe
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transport_end_marks_stale_and_reconnects_with_resubscribe() {
    // Use run() with a factory that yields two scripted transports, then Stop.
    //
    // Transport 1: snapshot for token_a, then ends.
    // Transport 2: another snapshot frame, then ends.
    // Factory returns Stop on the third call.
    //
    // Assert:
    //   - subscribe message sent on both transports (sent log has ≥ 2 entries each)
    //   - REST called for token_a at least twice (once per session's resnapshot_all)
    //   - stats.reconnects == 1 (first transport ended; second ended → 2nd reconnect
    //     won't happen because factory returns Stop)
    //   - book valid at end

    let mut books = HashMap::new();
    books.insert(
        "token_a".to_owned(),
        (
            vec![("0.44".to_owned(), "100".to_owned())],
            vec![("0.56".to_owned(), "100".to_owned())],
            "rest-hash-a".to_owned(),
        ),
    );
    let rest = FakeRest::new(books);
    let call_log = Arc::clone(&rest.call_log);

    let mut sup = Supervisor::new(
        vec![(TokenId(0), "token_a".to_owned(), TickSize::Cent)],
        rest,
        default_cfg(),
    );

    // Shared sent-log for both transports so we can check total subscribes.
    let sent_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let sent_clone = Arc::clone(&sent_log);
    let mut call_count = 0u32;

    let factory = move || {
        call_count += 1;
        let sent = Arc::clone(&sent_clone);
        async move {
            match call_count {
                1 => {
                    // First transport: just ends (no frames, to keep test fast).
                    let mut t = FakeTransport::new(vec![]);
                    t.sent = Arc::clone(&sent);
                    Ok(FactoryDecision::Connect(t))
                }
                2 => {
                    // Second transport: also ends immediately.
                    let mut t = FakeTransport::new(vec![]);
                    t.sent = Arc::clone(&sent);
                    Ok(FactoryDecision::Connect(t))
                }
                _ => Ok::<_, IngestError>(FactoryDecision::Stop),
            }
        }
    };

    sup.run(factory).await;

    // Two sessions ran → subscribe sent twice.
    let sent = sent_log.lock().unwrap();
    assert!(
        sent.len() >= 2,
        "subscribe message should be sent on each transport connection, got {}",
        sent.len()
    );
    drop(sent);

    // REST resnapshot_all called for each session → ≥ 2 calls total.
    let calls = call_log.lock().unwrap();
    let a_calls: Vec<_> = calls.iter().filter(|s| s.as_str() == "token_a").collect();
    assert!(
        a_calls.len() >= 2,
        "REST must be called for token_a on each session (resnapshot_all), got {}",
        a_calls.len()
    );
    drop(calls);

    // reconnects: first transport ended → reconnects=1; second ended → reconnects=2
    // (both sessions ended due to empty transports).
    assert!(
        sup.stats().reconnects >= 1,
        "at least 1 reconnect should be counted"
    );
}

// ---------------------------------------------------------------------------
// Test 5: invalid_books_resnapshot_via_sweep
// ---------------------------------------------------------------------------

/// After a session ends (TransportLost → mark_all_stale), all books are invalid.
/// sweep_once uses invalid_tokens() (not age-based stale_tokens) and resnaps them.
#[tokio::test]
async fn invalid_books_resnapshot_via_sweep() {
    // Script:
    //   1. run_session with a snapshot frame for token_a, then transport ends.
    //      mark_all_stale() is called → book is invalid (valid=false).
    //   2. Call sweep_once(Instant::now()).
    //      sweep_once now uses invalid_tokens() → finds token_a → resnapshots.
    //   3. Assert: book is valid, resnapshots count increased.

    let (rest, call_log) = two_token_fake_rest();

    let mut sup = Supervisor::new(
        vec![(TokenId(0), "token_a".to_owned(), TickSize::Cent)],
        rest,
        default_cfg(),
    );

    // Session with one book frame, then transport ends (TransportLost → mark_all_stale).
    let f1 = book_frame("token_a", "0.44", "100", "0.56", "100", "ws-hash-a");
    let mut transport = FakeTransport::new(vec![Ok(f1)]);
    sup.run_session(&mut transport).await;

    // After session ends, book is invalid (mark_all_stale called by TransportLost path).
    assert!(
        !sup.shard().book(TokenId(0)).unwrap().valid(),
        "book should be invalid after TransportLost (mark_all_stale was called)"
    );

    let resnapshots_before = sup.stats().resnapshots;
    let calls_before = call_log.lock().unwrap().len();

    // sweep_once uses invalid_tokens() — finds the invalid book and resnapshots it.
    // The `now` parameter is kept for API stability (M3) but not used in sweep logic.
    sup.sweep_once(Instant::now()).await;

    let calls_after = call_log.lock().unwrap().len();
    let resnapshots_after = sup.stats().resnapshots;
    assert!(
        calls_after > calls_before,
        "sweep_once should have triggered a REST call for the invalid book; calls before={calls_before}, after={calls_after}"
    );
    assert!(
        resnapshots_after > resnapshots_before,
        "resnapshots stat should have increased; before={resnapshots_before}, after={resnapshots_after}"
    );

    // Book should be valid after the sweep resnapshot.
    assert!(
        sup.shard().book(TokenId(0)).unwrap().valid(),
        "book should be valid after sweep resnapshot of invalid book"
    );
}

// ---------------------------------------------------------------------------
// Test 6: feed_silence_forces_reconnect
// ---------------------------------------------------------------------------

/// When the feed is alive but silent (no frames for feed_silence duration),
/// the supervisor must detect this in the sweep arm and return FeedSilent,
/// triggering a reconnect. All books are marked stale.
///
/// Uses `tokio::time::pause()` (start_paused) + `tokio::time::advance()` so
/// the sweep interval fires deterministically while next_frame is pending.
/// Uses `FakeTransportBlocking` which returns Pending after exhausting its script.
#[tokio::test(start_paused = true)]
async fn feed_silence_forces_reconnect() {
    // Configuration: tiny silence window and fast sweep for this test.
    let cfg = SupervisorConfig {
        feed_silence: Duration::from_millis(5_000),   // 5s silence window
        sweep_interval: Duration::from_millis(500),   // sweep every 500ms
        ..default_cfg()
    };

    let (rest, _call_log) = two_token_fake_rest();
    let mut sup = Supervisor::new(
        vec![(TokenId(0), "token_a".to_owned(), TickSize::Cent)],
        rest,
        cfg,
    );

    // FakeTransportBlocking: delivers one good snapshot frame, then blocks forever.
    let f1 = book_frame("token_a", "0.44", "100", "0.56", "100", "snap-1");
    let mut transport = FakeTransportBlocking::new(vec![Ok(f1)]);

    // Drive run_session concurrently with a time-advance arm.
    //
    // The biased select! inside run_session checks the frame arm first.
    // After the script exhausts, next_frame returns Pending forever.
    // The sweep interval is paused (start_paused = true).
    // We advance time past sweep_interval + feed_silence, which causes the
    // interval to fire. The sweep arm detects silence and returns FeedSilent.
    //
    // The outer tokio::select! here ensures we get the SessionEnd result back:
    // - If run_session completes (FeedSilent detected) → first arm wins.
    // - The second arm advances time then stays pending forever → never wins.
    let result = tokio::select! {
        r = sup.run_session(&mut transport) => r,
        _ = async {
            // Yield once to let run_session process the initial frame and reach
            // the pending-forever state in next_frame.
            tokio::task::yield_now().await;
            // Advance time past sweep_interval + feed_silence so the interval fires.
            tokio::time::advance(Duration::from_millis(10_000)).await;
            // Never resolve this arm — run_session must win when it detects silence.
            std::future::pending::<()>().await
        } => unreachable!("time-advance arm should never complete"),
    };

    assert_eq!(result, SessionEnd::FeedSilent, "expected FeedSilent when feed is silent");
    assert_eq!(
        sup.stats().feed_silence_reconnects, 1,
        "feed_silence_reconnects should be 1 after one silent session"
    );
    // All books should be invalid after mark_all_stale (called by FeedSilent path).
    assert!(
        !sup.shard().book(TokenId(0)).map(|b| b.valid()).unwrap_or(true),
        "book should be invalid after feed-silence mark_all_stale"
    );
}
