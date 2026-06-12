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
//! # Jitter RNG
//! A tiny xorshift64 seeded from `Instant::now().elapsed()` nanos gives enough
//! jitter to avoid thundering herds without pulling in the `rand` crate.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::{Duration, Instant};

use pm_core::instrument::TokenId;
use pm_core::num::TickSize;

use crate::livebook::RawChange;
use crate::rest::ParsedBook;
use crate::shard::Shard;
use crate::ws::{parse_frame, subscribe_message, WsEvent, WsTransport};
use crate::IngestError;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration knobs for a [`Supervisor`].
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Books older than this are considered stale and trigger a resnapshot.
    pub staleness: Duration,
    /// Base duration for exponential reconnect backoff.
    pub backoff_base: Duration,
    /// Maximum delay cap for reconnect backoff.
    pub backoff_cap: Duration,
    /// How often the staleness sweep runs in `run_session`.
    pub sweep_interval: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        SupervisorConfig {
            staleness: Duration::from_millis(1500),
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
    rest: R,
    cfg: SupervisorConfig,
    stats: SupStats,
}

impl<R: RestBookSource> Supervisor<R> {
    /// Create a new supervisor.
    ///
    /// `tokens` is a list of `(TokenId, venue_id_string, TickSize)` tuples.
    pub fn new(tokens: Vec<(TokenId, String, TickSize)>, rest: R, cfg: SupervisorConfig) -> Self {
        let mut by_venue: HashMap<Box<str>, TokenId> = HashMap::new();
        let metas: Vec<TokenMeta> = tokens
            .into_iter()
            .map(|(id, venue_id, tick)| {
                let venue: Box<str> = venue_id.into_boxed_str();
                by_venue.insert(venue.clone(), id);
                TokenMeta { id, venue_id: venue, tick }
            })
            .collect();
        Supervisor {
            shard: Shard::default(),
            tokens: metas,
            by_venue,
            rest,
            cfg,
            stats: SupStats::default(),
        }
    }

    // -----------------------------------------------------------------------
    // Public accessors
    // -----------------------------------------------------------------------

    /// Read-only access to supervisor stats.
    pub fn stats(&self) -> &SupStats {
        &self.stats
    }

    /// Read-only access to the underlying shard.
    pub fn shard(&self) -> &Shard {
        &self.shard
    }

    // -----------------------------------------------------------------------
    // Single session — called by run() and by replay tests directly
    // -----------------------------------------------------------------------

    /// Drive one WS session until the transport ends.
    ///
    /// Steps:
    /// 1. Send a subscribe message for all owned venue ids.
    /// 2. Resnapshot every token via REST.
    /// 3. Inner loop: select! between incoming frames and the sweep timer.
    ///    - Frame `None` or `Some(Err(_))` → `mark_all_stale`, `reconnects += 1`,
    ///      return `SessionEnd::TransportLost`.
    ///    - Frame text → `handle_frame`.
    ///    - Sweep tick → inline sweep (same logic as `sweep_once` but avoids
    ///      double-borrow of `&mut self`; see module doc).
    ///
    /// Replay tests call this directly; the outer `run()` wraps it with
    /// backoff+factory.
    pub async fn run_session<T: WsTransport>(
        &mut self,
        transport: &mut T,
    ) -> SessionEnd {
        // Step 1: subscribe.
        let venue_ids: Vec<String> =
            self.tokens.iter().map(|m| m.venue_id.to_string()).collect();
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
                biased;
                frame = transport.next_frame() => {
                    match frame {
                        None | Some(Err(_)) => {
                            self.shard.mark_all_stale();
                            self.stats.reconnects += 1;
                            return SessionEnd::TransportLost;
                        }
                        Some(Ok(text)) => {
                            self.handle_frame(&text).await;
                        }
                    }
                }
                _ = sweep.tick() => {
                    // Inline sweep to avoid double-borrow of &mut self.
                    let now = Instant::now();
                    let stale = self.shard.stale_tokens(now, self.cfg.staleness);
                    let mut seen: HashSet<TokenId> = HashSet::new();
                    for token_id in stale {
                        if seen.insert(token_id) {
                            self.resnapshot(token_id).await;
                        }
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Sweep
    // -----------------------------------------------------------------------

    /// Resnapshot all tokens whose books are stale at `now`.
    ///
    /// Exposed as `pub` so replay tests can drive it directly without running
    /// through a live `tokio::time::interval`.
    pub async fn sweep_once(&mut self, now: Instant) {
        let stale = self.shard.stale_tokens(now, self.cfg.staleness);
        let mut seen: HashSet<TokenId> = HashSet::new();
        for token_id in stale {
            if seen.insert(token_id) {
                self.resnapshot(token_id).await;
            }
        }
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
    /// Jitter via `xorshift64` seeded from `Instant::now()` nanos — no `rand`
    /// dependency needed.
    pub async fn run<T, F, Fut>(&mut self, mut factory: F)
    where
        T: WsTransport,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<FactoryDecision<T>, IngestError>>,
    {
        let mut attempt: u32 = 0;

        loop {
            // Apply backoff before retrying after a failed factory call.
            if attempt > 0 {
                let delay = backoff_delay(
                    self.cfg.backoff_base,
                    self.cfg.backoff_cap,
                    attempt,
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
                    // SessionEnd::TransportLost → loop and reconnect.
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Frame handling
    // -----------------------------------------------------------------------

    async fn handle_frame(&mut self, text: &str) {
        self.stats.frames += 1;
        let events = match parse_frame(text) {
            Ok(ev) => ev,
            Err(_) => {
                self.stats.parse_errors += 1;
                return;
            }
        };
        self.stats.events += events.len() as u64;

        for event in events {
            match event {
                WsEvent::Book(book_ev) => {
                    // Route by event.asset_id (per-token in book frames).
                    if let Some(&token_id) = self.by_venue.get(book_ev.asset_id.as_str()) {
                        let tick = self.tokens
                            .iter()
                            .find(|m| m.id == token_id)
                            .map(|m| m.tick)
                            .unwrap_or(TickSize::Cent);
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
                                if matches!(outcome, crate::livebook::ApplyOutcome::NeedsResnapshot(_)) {
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
                    // Unknown token — resnapshot by venue_id.
                    // We can't look up the TokenId, but the venue_id is in the change.
                    // We need to request a snapshot. Since we don't have a TokenId for
                    // this venue_id, we skip routing — the caller should have pre-loaded
                    // all tokens. Per the contract: NeedsResnapshot path.
                    // Find if there's already a matching token via by_venue (there isn't),
                    // so we use shard.apply_changes on a dummy to get NeedsResnapshot.
                    // Instead: directly call REST with the venue_id string.
                    let venue_id = change.asset_id.clone();
                    if let Err(_e) = self.rest.book(&venue_id).await {
                        self.stats.resnapshot_errors += 1;
                    }
                    // We can't apply without a TokenId, so skip.
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
            let outcome =
                self.shard.apply_changes(now, group.token_id, &group.changes, hash_ref);
            if matches!(outcome, crate::livebook::ApplyOutcome::NeedsResnapshot(_)) {
                self.resnapshot(group.token_id).await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Resnapshot helpers
    // -----------------------------------------------------------------------

    /// Fetch a REST snapshot for a single token and apply it to the shard.
    async fn resnapshot(&mut self, token_id: TokenId) {
        let venue_id = match self.tokens.iter().find(|m| m.id == token_id) {
            Some(m) => m.venue_id.to_string(),
            None => return, // unknown token — shouldn't happen
        };
        let tick = self
            .tokens
            .iter()
            .find(|m| m.id == token_id)
            .map(|m| m.tick)
            .unwrap_or(TickSize::Cent);

        match self.rest.book(&venue_id).await {
            Ok(book) => {
                self.shard.apply_snapshot(
                    Instant::now(),
                    token_id,
                    tick,
                    &book.bids,
                    &book.asks,
                    &book.hash,
                );
                self.stats.resnapshots += 1;
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
/// where `rand` comes from a tiny xorshift64 seeded from `Instant::now()` nanos.
fn backoff_delay(base: Duration, cap: Duration, attempt: u32) -> Duration {
    let multiplier = 1u64.checked_shl(attempt.min(62)).unwrap_or(u64::MAX);
    let raw_nanos = base.as_nanos().saturating_mul(multiplier as u128);
    let cap_nanos = cap.as_nanos();
    let capped_nanos = raw_nanos.min(cap_nanos);

    // xorshift64 for jitter — seed from current time nanos.
    let seed = Instant::now().elapsed().as_nanos() as u64 ^ 0xdeadbeef_cafebabe;
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
    use std::time::Duration;

    #[test]
    fn backoff_grows_and_caps() {
        let base = Duration::from_millis(250);
        let cap = Duration::from_secs(30);
        let d1 = backoff_delay(base, cap, 1);
        let d2 = backoff_delay(base, cap, 2);
        let d7 = backoff_delay(base, cap, 7);
        // Each step at least 0.5× the cap of 2^n*base (jitter ≥ 0.5)
        assert!(d1 >= Duration::from_millis(125)); // 0.5 × 500ms
        assert!(d2 >= Duration::from_millis(250)); // 0.5 × 1s
        assert!(d7 <= cap);
        // At attempt 200 it doesn't overflow or panic
        let _ = backoff_delay(base, cap, 200);
    }
}
