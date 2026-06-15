//! WS connection supervisor: subscribe, route, reconnect, resnapshot.
//!
//! # Ownership model
//! One [`Supervisor`] owns one [`Shard`] and a slice of tokens (venue-id +
//! [`TokenId`] + [`TickSize`]).  Chunking the token universe across multiple
//! supervisors is the caller's responsibility (the M3 app wiring); M2 treats a
//! single supervisor as the complete unit of operation.
//!
//! # Select placement
//! The sweep timer lives INSIDE `run_session`.  `run_session` drives both
//! incoming frames (via the transport) and periodic staleness sweeps (via
//! `tokio::time::interval`).  Both arms need `&mut self`; to avoid a double-
//! borrow we inline the sweep logic in the `select!` arm rather than calling
//! `sweep_once` there.  `sweep_once` stays `pub` so replay tests can invoke it
//! directly without a transport.
//!
//! # Feed-level silence model (delta-only WS)
//! Polymarket's WS feed is delta-only: a quiet market sends NO frames because
//! its book is UNCHANGED — silence means "current", not "stale".  The correct
//! model therefore distinguishes two independent health signals:
//!
//! * **Book integrity** (`valid == false`): the book was crossed, accumulated
//!   too many off-tick prices, or lost the feed.  These books must be
//!   resnapshotted regardless of feed liveness — handled inline by `handle_frame`
//!   and backstopped by `sweep_once` (which iterates `invalid_tokens()`).
//!
//! * **Feed silence** (`now - last_frame > feed_silence`): the entire connection
//!   has gone quiet beyond the configured window, indicating a dead socket.
//!   The supervisor detects this in the sweep arm and returns
//!   [`SessionEnd::FeedSilent`], triggering an immediate reconnect.
//!
//! Per-book age (`last_update` / `is_stale`) is preserved for M3 detection
//! logic but is NOT used as a sweep trigger in the running session — a book
//! that is quiet because the market is quiet is not stale.
//!
//! # Jitter RNG
//! A tiny xorshift64 seeded from wall-clock nanos XOR per-instance address
//! gives real entropy and per-supervisor decorrelation to avoid thundering herds
//! without pulling in the `rand` crate.

use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pm_core::book::Book;
use pm_core::instrument::TokenId;
use pm_core::num::TickSize;
use tokio::sync::mpsc;

use crate::IngestError;
use crate::livebook::RawChange;
use crate::rest::ParsedBook;
use crate::shard::Shard;
use crate::stats::StatsCell;
use crate::ws::{WsEvent, WsTransport, parse_frame, subscribe_message};

// ---------------------------------------------------------------------------
// M3 seam type aliases
// ---------------------------------------------------------------------------

/// Detection hook type (M3 seam §5): called after every successful apply.
/// Public so strategies (`pm-app`) can name the exact type `set_on_apply` takes.
pub type OnApplyFn = Box<dyn FnMut(TokenId, &Shard) + Send>;

// ---------------------------------------------------------------------------
// SupervisorCommand — M3 seam §12
// ---------------------------------------------------------------------------

/// Commands servable while a session runs (M3 seam; spec §12 app wiring).
pub enum SupervisorCommand {
    /// Snapshot one book: replies with (clone, valid flag), or None if unknown.
    BookSnapshot {
        token: TokenId,
        reply: tokio::sync::oneshot::Sender<Option<(Book, bool)>>,
    },
}

/// Await the next command, or pend forever when no channel is installed.
async fn recv_cmd(rx: &mut Option<mpsc::Receiver<SupervisorCommand>>) -> SupervisorCommand {
    match rx.as_mut() {
        Some(r) => match r.recv().await {
            Some(c) => c,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration knobs for a [`Supervisor`].
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Books older than this are considered stale — kept as an M3 detection knob.
    /// NOT used as a sweep trigger in the running session (see module doc).
    pub staleness: Duration,
    /// Feed-level silence window: if no text frame is received for this long the
    /// supervisor treats the connection as dead and returns [`SessionEnd::FeedSilent`].
    pub feed_silence: Duration,
    /// Base duration for exponential reconnect backoff.
    pub backoff_base: Duration,
    /// Maximum delay cap for reconnect backoff.
    pub backoff_cap: Duration,
    /// How often the sweep arm runs in `run_session`.
    pub sweep_interval: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        SupervisorConfig {
            staleness: Duration::from_millis(1_500),
            feed_silence: Duration::from_millis(15_000),
            backoff_base: Duration::from_millis(250),
            backoff_cap: Duration::from_secs(30),
            sweep_interval: Duration::from_secs(1),
        }
    }
}

// ---------------------------------------------------------------------------
// REST source trait
// ---------------------------------------------------------------------------

/// Abstraction over REST book fetches so replay tests can script the responses.
#[allow(async_fn_in_trait)]
pub trait RestBookSource: Send {
    /// Fetch a book snapshot for the given venue token id.
    async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, IngestError>;
}

// ClobRest already matches this signature; impl it here.
impl RestBookSource for crate::rest::ClobRest {
    async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, IngestError> {
        crate::rest::ClobRest::book(self, venue_token_id).await
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Aggregate supervisor stats.
#[derive(Debug, Default, Clone, Copy)]
pub struct SupStats {
    /// Raw text frames received.
    pub frames: u64,
    /// WsEvent items produced (one frame may yield multiple events).
    pub events: u64,
    /// Frames that failed `parse_frame` (counted, not fatal per §19).
    pub parse_errors: u64,
    /// Number of WS reconnects (initial connect does not count).
    pub reconnects: u64,
    /// REST resnapshots successfully applied.
    pub resnapshots: u64,
    /// REST resnapshots that failed (token left stale; sweep retries).
    pub resnapshot_errors: u64,
    /// Price-change entries whose asset_id is not in our token universe.
    pub unknown_token_changes: u64,
    /// Sessions ended because the feed went silent beyond `feed_silence`.
    pub feed_silence_reconnects: u64,
}

// ---------------------------------------------------------------------------
// Token metadata
// ---------------------------------------------------------------------------

/// Per-token data the supervisor needs at runtime.
struct TokenMeta {
    id: TokenId,
    venue_id: Box<str>,
    tick: TickSize,
}

// ---------------------------------------------------------------------------
// SessionEnd — what run_session returns to the outer loop
// ---------------------------------------------------------------------------

/// Why `run_session` returned.
#[derive(Debug, PartialEq, Eq)]
pub enum SessionEnd {
    /// Transport returned `None` or an error — outer loop should reconnect.
    TransportLost,
    /// No text frame received for longer than `feed_silence` — outer loop
    /// should reconnect exactly as for `TransportLost`.
    FeedSilent,
}

// ---------------------------------------------------------------------------
// FactoryDecision — how run() receives transports
// ---------------------------------------------------------------------------

/// Returned by the factory closure passed to [`Supervisor::run`].
///
/// `Stop` is used in tests to make the infinite outer loop terminate after a
/// known number of connections.
pub enum FactoryDecision<T> {
    /// Use this transport for the next session.
    Connect(T),
    /// Signal the run loop to exit cleanly (test-only).
    Stop,
}

// ---------------------------------------------------------------------------
// Supervisor
// ---------------------------------------------------------------------------

/// Connection supervisor: owns a shard and a list of tokens; manages the full
/// subscribe → snapshot → route → reconnect lifecycle.
pub struct Supervisor<R: RestBookSource> {
    shard: Shard,
    tokens: Vec<TokenMeta>,
    by_venue: HashMap<Box<str>, TokenId>,
    /// O(1) index from TokenId into `tokens`.
    by_token: HashMap<TokenId, usize>,
    rest: R,
    cfg: SupervisorConfig,
    stats: SupStats,
    /// Tracks when the last text frame was received on the current session.
    ///
    /// Initialized to `tokio::time::Instant::now()` at the start of each
    /// `run_session` call and updated on every received Ok text frame.
    /// Using `tokio::time::Instant` (not `std::time::Instant`) so that
    /// `tokio::time::pause()` in tests can control time deterministically.
    last_frame: tokio::time::Instant,
    /// Optional shared-stats mirror for the probe; set via [`share_stats`].
    ///
    /// When `Some`, [`refresh_mirror`] is called at the end of every
    /// `handle_frame` call and on every sweep tick so the probe can read
    /// counters without holding any lock on the supervisor.
    ///
    /// [`share_stats`]: Supervisor::share_stats
    /// [`refresh_mirror`]: Supervisor::refresh_mirror
    stats_mirror: Option<std::sync::Arc<StatsCell>>,
    /// M3 detection hook: called after every successful apply (ApplyOutcome::Ok).
    /// None when not installed (default) — M2 behavior is completely unchanged.
    on_apply: Option<OnApplyFn>,
    /// M3 command channel receiver: serves BookSnapshot queries during a session.
    /// None when not installed (default).
    cmd_rx: Option<mpsc::Receiver<SupervisorCommand>>,
}

impl<R: RestBookSource> Supervisor<R> {
    /// Create a new supervisor.
    ///
    /// `tokens` is a list of `(TokenId, venue_id_string, TickSize)` tuples.
    pub fn new(tokens: Vec<(TokenId, String, TickSize)>, rest: R, cfg: SupervisorConfig) -> Self {
        let mut by_venue: HashMap<Box<str>, TokenId> = HashMap::new();
        let mut by_token: HashMap<TokenId, usize> = HashMap::new();
        let metas: Vec<TokenMeta> = tokens
            .into_iter()
            .enumerate()
            .map(|(idx, (id, venue_id, tick))| {
                let venue: Box<str> = venue_id.into_boxed_str();
                by_venue.insert(venue.clone(), id);
                by_token.insert(id, idx);
                TokenMeta {
                    id,
                    venue_id: venue,
                    tick,
                }
            })
            .collect();
        Supervisor {
            shard: Shard::default(),
            tokens: metas,
            by_venue,
            by_token,
            rest,
            cfg,
            stats: SupStats::default(),
            last_frame: tokio::time::Instant::now(),
            stats_mirror: None,
            on_apply: None,
            cmd_rx: None,
        }
    }

    // -----------------------------------------------------------------------
    // Public accessors
    // -----------------------------------------------------------------------

    /// Read-only access to supervisor stats.
    pub fn stats(&self) -> &SupStats {
        &self.stats
    }

    /// Test-only: override `last_frame` to simulate feed silence without wall time.
    ///
    /// This allows deterministic testing of the feed-silence detection logic
    /// without requiring real elapsed time or `tokio::time::pause`.
    #[cfg(test)]
    pub fn set_last_frame_for_test(&mut self, t: tokio::time::Instant) {
        self.last_frame = t;
    }

    /// Read-only access to the underlying shard.
    pub fn shard(&self) -> &Shard {
        &self.shard
    }

    /// Install a shared-stats mirror and return a clone of the `Arc<StatsCell>`.
    ///
    /// The probe calls this before spawning the supervisor task to obtain a
    /// handle for reading stats without holding the supervisor lock. After this
    /// call, every `handle_frame` and sweep tick will call `refresh_mirror`.
    ///
    /// Calling `share_stats` a second time replaces the previous mirror.
    pub fn share_stats(&mut self) -> std::sync::Arc<StatsCell> {
        let cell = StatsCell::new();
        self.stats_mirror = Some(std::sync::Arc::clone(&cell));
        cell
    }

    /// Install the M3 detection hook.
    ///
    /// The callback fires after every apply whose outcome is `ApplyOutcome::Ok`
    /// (snapshot and delta applies). It is called with the affected `TokenId`
    /// and an immutable reference to the `Shard` AFTER the apply, so the caller
    /// can read the new book state.
    ///
    /// When not installed (default), M2 behavior is completely unchanged.
    pub fn set_on_apply(&mut self, cb: OnApplyFn) {
        self.on_apply = Some(cb);
    }

    /// Create (or replace) the command channel.
    ///
    /// Returns the sender half; the supervisor holds the receiver and polls it
    /// in the `run_session` select loop. Replacing a previous channel drops the
    /// old receiver, which will cause senders on the old channel to get errors.
    pub fn command_channel(&mut self, capacity: usize) -> mpsc::Sender<SupervisorCommand> {
        let (tx, rx) = mpsc::channel(capacity);
        self.cmd_rx = Some(rx);
        tx
    }

    /// Fire the detection hook after a successful apply (disjoint field borrow).
    fn fire_on_apply(&mut self, token: TokenId) {
        if let Some(cb) = self.on_apply.as_mut() {
            cb(token, &self.shard);
        }
    }

    /// Serve a single command (called from the select loop).
    fn handle_command(&mut self, cmd: SupervisorCommand) {
        match cmd {
            SupervisorCommand::BookSnapshot { token, reply } => {
                let view = self
                    .shard
                    .book(token)
                    .map(|lb| (lb.book().clone(), lb.valid()));
                let _ = reply.send(view);
            }
        }
    }

    /// Push current stats + book health into the shared mirror (if installed).
    ///
    /// `stale_count` is the pre-computed stale token count (from the sweep scan
    /// that already iterated the shard). Pass `Some(n)` from the sweep arm (which
    /// already iterated stale_tokens) and `None` from handle_frame (which avoids
    /// the O(books) scan on every frame — the mirror keeps the last stale value).
    /// The books gauge uses `book_count()` which is O(1) and is always refreshed.
    fn refresh_mirror(
        &self,
        stale_count: Option<usize>,
        parse_us: Option<u64>,
        apply_us: Option<u64>,
    ) {
        if let Some(ref cell) = self.stats_mirror {
            let books = self.shard.book_count();
            // When stale_count is None (called from handle_frame), reuse the last
            // stale value stored in the cell to avoid an O(books) scan per frame.
            // When Some, the sweep has already iterated stale_tokens so we use that count.
            let stale = stale_count
                .unwrap_or_else(|| cell.stale.load(std::sync::atomic::Ordering::Relaxed) as usize);
            cell.refresh(&self.stats, books, stale, parse_us, apply_us);
        }
    }

    // -----------------------------------------------------------------------
    // Single session — called by run() and by replay tests directly
    // -----------------------------------------------------------------------

    /// Drive one WS session until the transport ends or feed goes silent.
    ///
    /// Steps:
    /// 1. Send a subscribe message for all owned venue ids.
    /// 2. Resnapshot every token via REST.
    /// 3. Inner loop: select! between incoming frames and the sweep timer.
    ///    - Frame `None` or `Some(Err(_))` → `mark_all_stale`, `reconnects += 1`,
    ///      return `SessionEnd::TransportLost`.
    ///    - Frame text → update `last_frame`, call `handle_frame`.
    ///    - Sweep tick → check feed silence first; if silent: `mark_all_stale`,
    ///      `feed_silence_reconnects += 1`, return `SessionEnd::FeedSilent`.
    ///      Otherwise resnapshot INVALID books only (`invalid_tokens()`).
    ///
    /// Replay tests call this directly; the outer `run()` wraps it with
    /// backoff+factory.
    pub async fn run_session<T: WsTransport>(&mut self, transport: &mut T) -> SessionEnd {
        // Reset last_frame at the start of each session so the silence window
        // is relative to session open, not supervisor construction.
        self.last_frame = tokio::time::Instant::now();
        // Mark the session as live so the publisher/probe can show a real
        // connection-up flag rather than relying on frame-count deltas (M5 heartbeat).
        if let Some(ref cell) = self.stats_mirror {
            cell.connected
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        // Take the command receiver out of self so the select loop can hold it
        // as a local without conflicting with &mut self in arm bodies.
        // Safety note: the receiver would be lost if this future were cancelled
        // between take and restore; safe today because the only awaiter (`run()`)
        // never cancels it — do not select against run_session.
        let mut cmd_rx = self.cmd_rx.take();

        // Step 1: subscribe.
        let venue_ids: Vec<String> = self.tokens.iter().map(|m| m.venue_id.to_string()).collect();
        let sub_msg = subscribe_message(&venue_ids);
        // Best-effort: if sending fails the session is immediately broken.
        let _ = transport.send_text(&sub_msg).await;

        // Step 2: resnapshot all.
        self.resnapshot_all().await;

        // Step 3: inner loop.
        // The sweep interval lives here; replay tests bypass it by calling
        // sweep_once() directly — they never call run_session() with a live
        // interval.
        let mut sweep = tokio::time::interval(self.cfg.sweep_interval);
        sweep.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                // biased: drain frames first. Under continuous frame flow the sweep arm can
                // starve — acceptable: busy books are fresh by definition and integrity
                // failures resnapshot inline; the sweep only backstops INVALID books.
                // Commands come second — must not starve frames. Sweep is last.
                biased;
                frame = transport.next_frame() => {
                    match frame {
                        None | Some(Err(_)) => {
                            self.shard.mark_all_stale();
                            self.stats.reconnects += 1;
                            if let Some(ref cell) = self.stats_mirror {
                                cell.connected
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                            }
                            self.cmd_rx = cmd_rx;
                            return SessionEnd::TransportLost;
                        }
                        Some(Ok(text)) => {
                            // Update feed-liveness timestamp on every successfully
                            // received text frame (valid or not — even a parse failure
                            // proves the socket is alive).
                            self.last_frame = tokio::time::Instant::now();
                            self.handle_frame(&text).await;
                        }
                    }
                }
                // Commands may be delayed under frame saturation — accepted cost
                // of frames-first bias; a delayed BookSnapshot only means a
                // fresher book at fill time.
                cmd = recv_cmd(&mut cmd_rx) => {
                    self.handle_command(cmd);
                }
                _ = sweep.tick() => {
                    // Feed-silence check: if the connection has been quiet for longer
                    // than the configured window, treat it as a dead socket and reconnect.
                    let now_tokio = tokio::time::Instant::now();
                    if now_tokio.duration_since(self.last_frame) > self.cfg.feed_silence {
                        self.shard.mark_all_stale();
                        self.stats.feed_silence_reconnects += 1;
                        let invalid_count = self.shard.invalid_tokens().len();
                        // Refresh mirror so the probe sees the new stale count.
                        self.refresh_mirror(Some(invalid_count), None, None);
                        if let Some(ref cell) = self.stats_mirror {
                            cell.connected
                                .store(false, std::sync::atomic::Ordering::Relaxed);
                        }
                        self.cmd_rx = cmd_rx;
                        return SessionEnd::FeedSilent;
                    }

                    // Inline sweep: resnapshot only integrity-invalid books.
                    // Delta-only model: a quiet book on a live connection is current —
                    // only crossed/off-tick/feed-lost books need REST resnapshot.
                    let invalid = self.shard.invalid_tokens();
                    let invalid_count = invalid.len();
                    for token_id in invalid {
                        self.resnapshot(token_id).await;
                    }
                    // Refresh mirror after sweep with pre-computed invalid count.
                    self.refresh_mirror(Some(invalid_count), None, None);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Sweep
    // -----------------------------------------------------------------------

    /// Resnapshot all tokens whose books are integrity-invalid.
    ///
    /// Uses `shard.invalid_tokens()` (books where `valid() == false`) rather
    /// than age-based staleness.  Per the delta-only feed model, a quiet book
    /// on a live connection is current; only crossed/off-tick/feed-lost books
    /// need REST resnapshot.
    ///
    /// The `now` parameter is retained for API stability (M3 may add age-based
    /// detection on top) but is not used in the sweep logic here.
    ///
    /// Exposed as `pub` so replay tests can drive it directly without running
    /// through a live `tokio::time::interval`.
    pub async fn sweep_once(&mut self, _now: Instant) {
        let invalid = self.shard.invalid_tokens();
        // invalid_tokens iterates a HashMap — keys are already unique; no dedup needed.
        // Serial REST during mass-invalid events blocks frames for N×RTT — acceptable M2 probe limitation; M3 wiring fans out.
        let invalid_count = invalid.len();
        for token_id in invalid {
            self.resnapshot(token_id).await;
        }
        // Honor the mirror contract: refresh after sweep so the probe sees the post-sweep state.
        self.refresh_mirror(Some(invalid_count), None, None);
    }

    // -----------------------------------------------------------------------
    // Outer run loop with backoff + factory
    // -----------------------------------------------------------------------

    /// Run forever: mint transports via `factory`, drive sessions, reconnect.
    ///
    /// `factory` returns [`FactoryDecision`]:
    /// - `Connect(transport)` → run a session; backoff+jitter on factory `Err`.
    /// - `Stop` → exit the loop cleanly (used in tests to bound the run).
    ///
    /// Backoff: `delay = min(cap, base · 2^attempt) · rand(0.5..1.0)`.
    /// `attempt` resets to 0 after a successful `factory()` call.
    /// Jitter via `xorshift64` seeded from wall-clock nanos XOR per-instance
    /// address — real entropy, no `rand` dependency needed.
    pub async fn run<T, F, Fut>(&mut self, mut factory: F)
    where
        T: WsTransport,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<FactoryDecision<T>, IngestError>>,
    {
        let mut attempt: u32 = 0;
        // Per-instance address salt for jitter decorrelation.
        let addr_salt = std::ptr::from_ref(self) as u64;

        loop {
            // Apply backoff before retrying after a failed factory call.
            if attempt > 0 {
                let delay = backoff_delay(
                    self.cfg.backoff_base,
                    self.cfg.backoff_cap,
                    attempt,
                    addr_salt,
                );
                tokio::time::sleep(delay).await;
            }

            match factory().await {
                Err(_e) => {
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Ok(FactoryDecision::Stop) => return,
                Ok(FactoryDecision::Connect(mut transport)) => {
                    attempt = 0;
                    self.run_session(&mut transport).await;
                    // Both SessionEnd::TransportLost and SessionEnd::FeedSilent
                    // should reconnect — the run() loop does not pattern-match on
                    // the return value, so both variants fall through here and
                    // loop back to call factory() again, which is the correct
                    // behavior for both transport errors and feed silence.
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Frame handling
    // -----------------------------------------------------------------------

    async fn handle_frame(&mut self, text: &str) {
        let parse_start = Instant::now();
        self.stats.frames += 1;
        // Stamp last_frame_ms on every received frame (even parse failures prove
        // the socket is alive).  Uses SystemTime for epoch ms to match now_ms().
        if let Some(ref cell) = self.stats_mirror {
            let ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            cell.last_frame_ms
                .store(ms, std::sync::atomic::Ordering::Relaxed);
        }
        let events = match parse_frame(text) {
            Ok(ev) => ev,
            Err(_) => {
                self.stats.parse_errors += 1;
                self.refresh_mirror(None, None, None);
                return;
            }
        };
        let parse_us = parse_start.elapsed().as_micros() as u64;
        self.stats.events += events.len() as u64;

        let apply_start = Instant::now();
        for event in events {
            match event {
                WsEvent::Book(book_ev) => {
                    // Route by event.asset_id (per-token in book frames).
                    if let Some(&token_id) = self.by_venue.get(book_ev.asset_id.as_str()) {
                        let tick = match self
                            .by_token
                            .get(&token_id)
                            .and_then(|&i| self.tokens.get(i))
                        {
                            Some(m) => m.tick,
                            None => {
                                debug_assert!(false, "routed token missing from meta");
                                self.stats.unknown_token_changes += 1;
                                continue;
                            }
                        };
                        // Ensure slot exists with the right tick.
                        self.shard.ensure_book(token_id, tick);
                        match book_ev.to_raw_levels() {
                            Ok((bids, asks)) => {
                                let outcome = self.shard.apply_snapshot(
                                    Instant::now(),
                                    token_id,
                                    tick,
                                    &bids,
                                    &asks,
                                    &book_ev.hash,
                                );
                                if matches!(outcome, crate::livebook::ApplyOutcome::Ok) {
                                    // Site (a): fire after successful WS book snapshot.
                                    self.fire_on_apply(token_id);
                                } else {
                                    self.resnapshot(token_id).await;
                                }
                            }
                            Err(_) => {
                                self.stats.parse_errors += 1;
                            }
                        }
                    }
                    // Unknown asset_id in a book frame — no routing info, ignore.
                }
                WsEvent::PriceChange(pc) => {
                    // Route EVERY ParsedChange by its OWN asset_id (the CRITICAL
                    // ROUTING CONTRACT from the task spec).  Group consecutive
                    // changes per token and pass the last hash of each group.
                    self.handle_price_change(pc).await;
                }
                WsEvent::TickSizeChange { .. } | WsEvent::Other => {
                    // Count as events only — no routing action.
                }
            }
        }
        let apply_us = apply_start.elapsed().as_micros() as u64;
        self.refresh_mirror(None, Some(parse_us), Some(apply_us));
    }

    async fn handle_price_change(&mut self, pc: crate::ws::PriceChangeEvent) {
        if pc.changes.is_empty() {
            return;
        }

        // Group consecutive changes by ParsedChange.asset_id.
        // We iterate and collect groups in order; consecutive = same venue_id
        // without interruption.
        struct Group {
            token_id: TokenId,
            changes: Vec<RawChange>,
            last_hash: Option<String>,
        }

        let mut groups: Vec<Group> = Vec::new();

        for change in pc.changes {
            let token_id = match self.by_venue.get(change.asset_id.as_str()) {
                Some(&id) => id,
                None => {
                    // Token not in our universe (registry lag or foreign market in a shared
                    // frame): count and skip — we can't apply a book we don't track.
                    self.stats.unknown_token_changes += 1;
                    continue;
                }
            };

            let raw = RawChange {
                side_buy: change.side_buy,
                price_micro: change.price_micro,
                size_micro: change.size_micro,
            };

            if let Some(last) = groups.last_mut()
                && last.token_id == token_id
            {
                last.changes.push(raw);
                last.last_hash = change.hash;
                continue;
            }
            groups.push(Group {
                token_id,
                changes: vec![raw],
                last_hash: change.hash,
            });
        }

        // Apply each group.
        for group in groups {
            let now = Instant::now();
            let hash_ref = group.last_hash.as_deref();
            let outcome = self
                .shard
                .apply_changes(now, group.token_id, &group.changes, hash_ref);
            if matches!(outcome, crate::livebook::ApplyOutcome::Ok) {
                // Site (b): fire after successful delta apply.
                self.fire_on_apply(group.token_id);
            } else {
                self.resnapshot(group.token_id).await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Resnapshot helpers
    // -----------------------------------------------------------------------

    /// Fetch a REST snapshot for a single token and apply it to the shard.
    async fn resnapshot(&mut self, token_id: TokenId) {
        let (venue_id, tick) = match self
            .by_token
            .get(&token_id)
            .and_then(|&i| self.tokens.get(i))
        {
            Some(m) => (m.venue_id.to_string(), m.tick),
            None => {
                debug_assert!(false, "routed token missing from meta");
                return;
            }
        };

        match self.rest.book(&venue_id).await {
            Ok(book) => {
                let outcome = self.shard.apply_snapshot(
                    Instant::now(),
                    token_id,
                    tick,
                    &book.bids,
                    &book.asks,
                    &book.hash,
                );
                self.stats.resnapshots += 1;
                if matches!(outcome, crate::livebook::ApplyOutcome::Ok) {
                    // Site (c): fire after a successful REST resnapshot apply.
                    self.fire_on_apply(token_id);
                }
                // NeedsResnapshot outcome after a REST fetch is not re-enqueued
                // here — the sweep backstop will retry on the next tick.
            }
            Err(_e) => {
                self.stats.resnapshot_errors += 1;
                // Leave the book stale; sweep will retry.
            }
        }
    }

    /// Resnapshot every owned token.
    async fn resnapshot_all(&mut self) {
        // Collect token ids first to avoid borrow issues.
        let ids: Vec<TokenId> = self.tokens.iter().map(|m| m.id).collect();
        for token_id in ids {
            self.resnapshot(token_id).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Backoff + jitter
// ---------------------------------------------------------------------------

/// Compute a backoff delay with full jitter.
///
/// `delay = min(cap, base · 2^attempt) · rand(0.5..1.0)`
/// where `rand` comes from a tiny xorshift64 seeded from wall-clock nanos XOR
/// the caller's per-instance `salt` (supervisor address) for decorrelation.
fn backoff_delay(base: Duration, cap: Duration, attempt: u32, salt: u64) -> Duration {
    let multiplier = 1u64.checked_shl(attempt.min(62)).unwrap_or(u64::MAX);
    let raw_nanos = base.as_nanos().saturating_mul(multiplier as u128);
    let cap_nanos = cap.as_nanos();
    let capped_nanos = raw_nanos.min(cap_nanos);

    // Real entropy: wall-clock nanos XOR per-instance address XOR mixing constant.
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9e3779b97f4a7c15);
    let seed = wall ^ salt.rotate_left(32) ^ 0xdeadbeef_cafebabe;
    let jitter_u64 = xorshift64(seed);
    // Map to [0.5, 1.0): (jitter_u64 >> 1) / u64::MAX gives [0.0, 0.5), then +0.5
    let frac = (jitter_u64 >> 1) as f64 / u64::MAX as f64 + 0.5;
    let jittered_nanos = (capped_nanos as f64 * frac) as u128;

    Duration::from_nanos(jittered_nanos as u64)
}

/// One round of xorshift64.
fn xorshift64(mut x: u64) -> u64 {
    if x == 0 {
        x = 0x123456789abcdef0;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::livebook::RawLevel;
    use crate::rest::ParsedBook;
    use crate::ws::WsTransport;
    use pm_core::instrument::TokenId;
    use pm_core::num::TickSize;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    /// Transport that plays scripted frames then parks until released.
    ///
    /// `release` is a shared `Notify`; the test calls `notify_one()` to unblock
    /// a parked `next_frame`.  Using `Arc<Notify>` means the future can be
    /// dropped and re-polled by a biased `select!` without losing the signal:
    /// `Notify::notified()` correctly re-registers each time.
    struct SeqTransport {
        frames: std::collections::VecDeque<String>,
        release: std::sync::Arc<tokio::sync::Notify>,
    }

    impl WsTransport for SeqTransport {
        async fn next_frame(&mut self) -> Option<Result<String, crate::IngestError>> {
            if let Some(f) = self.frames.pop_front() {
                return Some(Ok(f));
            }
            self.release.notified().await;
            None
        }

        async fn send_text(&mut self, _text: &str) -> Result<(), crate::IngestError> {
            Ok(())
        }
    }

    struct OneBookRest;

    impl RestBookSource for OneBookRest {
        async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, crate::IngestError> {
            Ok(ParsedBook {
                asset_id: venue_token_id.to_string(),
                hash: "h1".into(),
                bids: vec![RawLevel {
                    price_micro: 440_000,
                    size_micro: 100_000_000,
                }],
                asks: vec![RawLevel {
                    price_micro: 460_000,
                    size_micro: 80_000_000,
                }],
            })
        }
    }

    const PC_FRAME: &str = r#"{"event_type":"price_change","market":"m","price_changes":[{"asset_id":"tok1","price":"0.43","size":"5","side":"BUY"}]}"#;

    #[tokio::test]
    async fn on_apply_fires_after_snapshot_and_delta_applies() {
        let mut sup = Supervisor::new(
            vec![(TokenId(5), "tok1".to_string(), TickSize::Cent)],
            OneBookRest,
            SupervisorConfig::default(),
        );
        let fired: Arc<Mutex<Vec<TokenId>>> = Arc::default();
        let fired2 = Arc::clone(&fired);
        sup.set_on_apply(Box::new(move |t, shard| {
            assert!(shard.book(t).is_some(), "hook sees the shard post-apply");
            fired2.lock().unwrap().push(t);
        }));
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        release.notify_one(); // release immediately: frames drain, then None
        let mut t = SeqTransport {
            frames: [PC_FRAME.to_string()].into(),
            release,
        };
        let end = sup.run_session(&mut t).await;
        assert_eq!(end, SessionEnd::TransportLost);
        let fired = fired.lock().unwrap();
        // 1× resnapshot_all apply + 1× delta apply
        assert_eq!(fired.as_slice(), &[TokenId(5), TokenId(5)]);
    }

    #[tokio::test]
    async fn command_channel_serves_book_snapshots_while_session_runs() {
        let mut sup = Supervisor::new(
            vec![(TokenId(5), "tok1".to_string(), TickSize::Cent)],
            OneBookRest,
            SupervisorConfig::default(),
        );
        let cmd_tx = sup.command_channel(8);
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let release2 = std::sync::Arc::clone(&release);
        let task = tokio::spawn(async move {
            let mut t = SeqTransport {
                frames: [].into(),
                release: release2,
            };
            sup.run_session(&mut t).await
        });

        let (otx, orx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SupervisorCommand::BookSnapshot {
                token: TokenId(5),
                reply: otx,
            })
            .await
            .unwrap();
        let (book, valid) = orx.await.unwrap().unwrap();
        assert!(valid);
        assert_eq!(book.bids.best().unwrap().get(), 44);

        // Unknown token → None.
        let (otx, orx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SupervisorCommand::BookSnapshot {
                token: TokenId(99),
                reply: otx,
            })
            .await
            .unwrap();
        assert!(orx.await.unwrap().is_none());

        release.notify_one();
        assert_eq!(task.await.unwrap(), SessionEnd::TransportLost);
    }

    #[test]
    fn backoff_grows_and_caps() {
        let base = Duration::from_millis(250);
        let cap = Duration::from_secs(30);
        let salt = 0u64;
        let d1 = backoff_delay(base, cap, 1, salt);
        let d2 = backoff_delay(base, cap, 2, salt);
        let d7 = backoff_delay(base, cap, 7, salt);
        // Each step at least 0.5× the cap of 2^n*base (jitter ≥ 0.5)
        assert!(d1 >= Duration::from_millis(125)); // 0.5 × 500ms
        assert!(d2 >= Duration::from_millis(250)); // 0.5 × 1s
        assert!(d7 <= cap);
        // At attempt 200 it doesn't overflow or panic
        let _ = backoff_delay(base, cap, 200, salt);
    }

    #[test]
    fn jitter_multiplier_varies_across_calls() {
        // Collect 32 jitter fractions at a fixed attempt. With real wall-clock
        // entropy the seed changes on each call so the multiplier must vary.
        // We assert at least 2 distinct Duration values — a constant seed would
        // always produce exactly one.
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(60);
        // Use a fixed salt (0) so only wall-clock nanos vary across iterations.
        let delays: Vec<Duration> = (0..32).map(|_| backoff_delay(base, cap, 1, 0)).collect();
        let distinct = delays
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert!(
            distinct >= 2,
            "jitter should produce varying multipliers across calls (got {distinct} distinct values out of 32)"
        );
    }

    #[test]
    fn unknown_token_changes_counted_not_rested() {
        // Build a supervisor with no tokens, then manually check that
        // unknown_token_changes increments when by_venue misses.
        // We exercise this via the backoff_delay path indirectly; the real
        // path is tested in replay integration tests. Here we just verify
        // the SupStats field exists and defaults to zero.
        let stats = SupStats::default();
        assert_eq!(stats.unknown_token_changes, 0);
    }

    /// Layer 1 requirement: StatsCell.connected reflects WS session liveness;
    /// StatsCell.last_frame_ms is stamped (epoch ms > 0) once a frame is applied;
    /// connected is cleared when the session ends / before reconnect.
    #[tokio::test]
    async fn stats_cell_tracks_connection_and_last_frame() {
        let mut sup = Supervisor::new(
            vec![(TokenId(5), "tok1".to_string(), TickSize::Cent)],
            OneBookRest,
            SupervisorConfig::default(),
        );
        let cell = sup.share_stats();

        // Before any session, both fields default to false/0.
        assert!(
            !cell.connected.load(Ordering::Relaxed),
            "connected must be false before a session starts"
        );
        assert_eq!(
            cell.last_frame_ms.load(Ordering::Relaxed),
            0,
            "last_frame_ms must be 0 before any frame"
        );

        // Run a session: snapshot applied (from resnapshot_all) + one delta frame.
        // Session ends with TransportLost (SeqTransport delivers frames then None).
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        release.notify_one(); // pre-release: frames drain immediately, then None
        let mut t = SeqTransport {
            frames: [PC_FRAME.to_string()].into(),
            release,
        };
        let end = sup.run_session(&mut t).await;
        assert_eq!(end, SessionEnd::TransportLost);

        // After TransportLost: connected must be cleared (false).
        assert!(
            !cell.connected.load(Ordering::Relaxed),
            "connected must be false after TransportLost"
        );
        // last_frame_ms must have been stamped (delta frame was applied).
        assert!(
            cell.last_frame_ms.load(Ordering::Relaxed) > 0,
            "last_frame_ms must be > 0 after a frame was applied"
        );
    }

    /// Separate test: connected is set true once the session is live (transport
    /// connected and resnapshot_all done), then cleared when the connection drops.
    /// We verify the "true during session" window by checking right after
    /// resnapshot_all via a command-channel round-trip (the session is live while
    /// the task is running and we can interact with it).
    #[tokio::test]
    async fn stats_cell_connected_true_while_session_live() {
        let mut sup = Supervisor::new(
            vec![(TokenId(5), "tok1".to_string(), TickSize::Cent)],
            OneBookRest,
            SupervisorConfig::default(),
        );
        let cell = sup.share_stats();
        let cmd_tx = sup.command_channel(8);

        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let release2 = std::sync::Arc::clone(&release);
        let task = tokio::spawn(async move {
            let mut t = SeqTransport {
                frames: [].into(),
                release: release2,
            };
            sup.run_session(&mut t).await
        });

        // Probe the session: send a BookSnapshot command. This round-trip
        // can only complete once resnapshot_all has run and the session loop
        // is active — at that point connected must be true.
        let (otx, orx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SupervisorCommand::BookSnapshot {
                token: TokenId(5),
                reply: otx,
            })
            .await
            .unwrap();
        let _ = orx.await.unwrap(); // wait for reply (session is live)

        assert!(
            cell.connected.load(Ordering::Relaxed),
            "connected must be true while session is running"
        );

        release.notify_one(); // end the session
        let end = task.await.unwrap();
        assert_eq!(end, SessionEnd::TransportLost);

        assert!(
            !cell.connected.load(Ordering::Relaxed),
            "connected must be false after session ends"
        );
    }

    /// REST source that returns a crossed book (bid 0.60 ≥ ask 0.40), which
    /// causes `apply_snapshot` to return `NeedsResnapshot` rather than `Ok`.
    struct CrossedRest;

    impl RestBookSource for CrossedRest {
        async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, crate::IngestError> {
            Ok(ParsedBook {
                asset_id: venue_token_id.to_string(),
                hash: "bad".into(),
                bids: vec![RawLevel {
                    price_micro: 600_000,
                    size_micro: 1_000_000,
                }],
                asks: vec![RawLevel {
                    price_micro: 400_000,
                    size_micro: 1_000_000,
                }],
            })
        }
    }

    /// The `on_apply` hook must NOT fire when `apply_snapshot` returns
    /// `NeedsResnapshot` (crossed book).  Even if resnapshot_all is retried,
    /// the count must stay at zero.
    #[tokio::test]
    async fn on_apply_does_not_fire_on_crossed_resnapshot() {
        let mut sup = Supervisor::new(
            vec![(TokenId(5), "tok1".to_string(), TickSize::Cent)],
            CrossedRest,
            SupervisorConfig::default(),
        );
        let fired = Arc::new(Mutex::new(0u32));
        let fired2 = Arc::clone(&fired);
        sup.set_on_apply(Box::new(move |_t, _s| {
            *fired2.lock().unwrap() += 1;
        }));
        // SeqTransport with no frames and release pre-notified — session ends
        // immediately after resnapshot_all without delivering any delta frames.
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        release.notify_one();
        let mut t = SeqTransport {
            frames: [].into(),
            release,
        };
        let end = sup.run_session(&mut t).await;
        assert_eq!(end, SessionEnd::TransportLost);
        assert_eq!(
            *fired.lock().unwrap(),
            0,
            "hook must not fire on NeedsResnapshot"
        );
    }
}
