//! User-WS maker-fill source (Task 4.6): a LOW-LATENCY live maker-fill feed for
//! fast scalping, behind the SAME [`UserFillSource`] trait as the Task-3.4 REST
//! poll ([`LiveVenue`](crate::live::LiveVenue)). It is the drop-in latency
//! upgrade the `fills.rs` module docs foreshadow.
//!
//! ## What it does
//! Connects to the CLOB user channel
//! (`wss://ws-subscriptions-clob.polymarket.com/ws/user`), subscribes by
//! **condition_id** (market) with the L2 `{apiKey, secret, passphrase}` auth,
//! and reads `trade` events. The user channel returns only OUR trades; each
//! `trade` carries the resting orders of ours that filled in `maker_orders[]`.
//! A spawned background task pushes the parsed [`MakerFill`]s onto an
//! `mpsc` channel; [`LiveUserWsFills::poll`] drains it non-blockingly, so the
//! generic `run_mm_loop` sees fills with WS latency instead of a REST poll's.
//!
//! ## Dedup across the trade lifecycle
//! A `trade` fires repeatedly through its lifecycle (`MATCHED` → `MINED` →
//! `CONFIRMED`) with the SAME `id`, so we DEDUP on the SAME key the REST path
//! uses — `"{trade_id}:{order_id}"` — and emit each maker fill exactly ONCE, on
//! FIRST observation (we do NOT wait for `CONFIRMED`: inventory must update
//! fast). The `seen` set lives on the background task and is carried ACROSS
//! reconnects so a trade re-sent after a reconnect is not double-counted.
//!
//! ## Lifecycle (mirrors `ingestion::supervisor`)
//! connect → send the subscribe message → read frames (`parse_ws_trade` → push)
//! → on transport end/error, reconnect with capped backoff and RE-SUBSCRIBE,
//! carrying `seen`. The client sends a `PING` text frame every ~10 s; the server
//! replies `PONG` (which `recv` skips like any non-text frame).
//!
//! ## Why a copied transport (not a `pm-ingestion` dependency)
//! `pm-execution` must not depend on `pm-ingestion`. So this mirrors ingestion's
//! [`WsTransport`] abstraction with a minimal in-crate copy — exactly as
//! `live.rs` copies the REST rate limiter and the decimal helpers. Tests inject
//! a MOCK transport via the [`LiveUserWsFills::with_transport_factory`]
//! constructor, so the whole feed is exercised with ZERO network.
//!
//! ## UNVERIFIABLE offline (needs canary confirmation)
//! The exact WS frame field names, the `PING`/`PONG` keepalive format, the auth
//! handshake, and the MATCHED-first emit semantics are from the docs spike, not
//! a live capture. The pure parse + the mock-transport task lifecycle ARE tested
//! here; the wire contract is confirmed only in the live canary.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::warn;

use pm_core::instrument::TokenId;
use pm_core::num::{Qty, TickSize};

use crate::fills::{MakerFill, UserFillSource};
use crate::live::{decimal_to_micro, px_from_decimal};
use crate::maker::OrderId;
use crate::secrets::ApiCreds;
use crate::venue::VenueError;

/// How often the client sends a keepalive `PING` text frame (docs: ~10 s).
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// Reconnect backoff base / cap (mirrors `SupervisorConfig` defaults).
const BACKOFF_BASE: Duration = Duration::from_millis(250);
const BACKOFF_CAP: Duration = Duration::from_secs(30);
/// Fill channel capacity. Generous: `poll()` drains fully every quote cycle, so
/// it only ever buffers a single burst; bounded (not unbounded) caps memory if
/// the consumer stalls.
const FILL_CHANNEL_CAP: usize = 4096;

// ---------------------------------------------------------------------------
// A) Pure, testable pieces (no I/O)
// ---------------------------------------------------------------------------

/// Build the EXACT user-channel subscribe message (Task 4.6, spike-confirmed):
/// `{"auth":{"apiKey":..,"secret":..,"passphrase":..},"markets":[<condition_id>,..],"type":"user"}`.
///
/// Subscribe is by **condition_id** (market), NOT token/asset id. The same
/// message is sent on the initial connect AND on every reconnect (resubscribe).
/// The secret + passphrase go on the wire here exactly as they do in the REST
/// L2 auth header — this is the one authorized place the resolved secret leaves
/// memory.
pub fn user_subscribe_message(creds: &ApiCreds, condition_ids: &[String]) -> String {
    let markets: Vec<serde_json::Value> = condition_ids
        .iter()
        .map(|c| serde_json::Value::String(c.clone()))
        .collect();
    serde_json::json!({
        "auth": {
            "apiKey": creds.key.as_str(),
            "secret": creds.secret.expose(),
            "passphrase": creds.passphrase.expose(),
        },
        "markets": markets,
        "type": "user",
    })
    .to_string()
}

/// Pure, I/O-free core: map ONE user-channel `trade` frame to NEW (deduped)
/// maker fills. The maker dual of [`parse_maker_fills`](crate::fills::parse_maker_fills),
/// reusing the SAME dedup key (`"{trade_id}:{order_id}"`), the SAME
/// `resolve(asset_id) -> (TokenId, TickSize)` mapping, and the SAME
/// `matched_amount`→µshares / `price`→[`Px`] helpers.
///
/// Only `event_type == "trade"` frames produce fills; any other frame (an
/// `order` event, a subscribe ack, etc.) returns empty. The user channel
/// returns only OUR trades, so — unlike the REST path — there is NO
/// `trader_side` filter: every `maker_orders[]` entry is one of OUR resting
/// orders that filled. For each entry: build the key, skip if `seen`, map
/// `matched_amount`→µshares and `price`→[`Px`] for the trade's token, emit a
/// [`MakerFill`], and record the key (only on emit, so a transiently malformed
/// entry is retried on a later frame). Robust to missing/malformed fields — a
/// bad entry is skipped, never a panic.
///
/// Emits on FIRST observation regardless of `status` (`MATCHED`/`MINED`/
/// `CONFIRMED`): the dedup guarantees once-only across the lifecycle, and
/// inventory must update fast (we do not wait for `CONFIRMED`).
pub(crate) fn parse_ws_trade(
    value: &serde_json::Value,
    resolve: impl Fn(&str) -> Option<(TokenId, TickSize)>,
    seen: &mut HashSet<String>,
) -> Vec<MakerFill> {
    // Only `trade` frames carry fills. `order` events / non-trade frames → none.
    if value.get("event_type").and_then(|v| v.as_str()) != Some("trade") {
        return Vec::new();
    }
    let Some(trade_id) = value.get("id").and_then(|v| v.as_str()) else {
        warn!("ws trade frame missing id; skipping (cannot dedup)");
        return Vec::new();
    };
    let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) else {
        warn!(trade_id, "ws trade frame missing asset_id; skipping");
        return Vec::new();
    };
    // asset_id (the venue token id) → our (TokenId, TickSize). Unregistered →
    // skip the whole frame with a warn (an operator forgot to register a token).
    let Some((token, ts)) = resolve(asset_id) else {
        warn!(trade_id, asset_id, "ws trade for unregistered token; skipping");
        return Vec::new();
    };
    let Some(makers) = value.get("maker_orders").and_then(|v| v.as_array()) else {
        // A trade with no maker_orders array has nothing of OURS to emit.
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in makers {
        let Some(order_id) = entry.get("order_id").and_then(|v| v.as_str()) else {
            warn!(trade_id, "ws maker_orders entry missing order_id; skipping");
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
            warn!(trade_id, order_id, "ws maker fill missing/bad matched_amount; skipping");
            continue;
        };
        let Some(px) = entry
            .get("price")
            .and_then(|v| v.as_str())
            .and_then(|p| px_from_decimal(p, ts))
        else {
            warn!(trade_id, order_id, "ws maker fill missing/bad/unaligned price; skipping");
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
    out
}

// ---------------------------------------------------------------------------
// B) Transport abstraction (minimal in-crate copy of ingestion's WsTransport)
// ---------------------------------------------------------------------------

/// Abstract WebSocket transport for the user channel. Mirrors
/// `pm_ingestion::ws::WsTransport` (text frames passed through; Ping/Pong/Binary
/// skipped; clean close / stream end = `None`), copied here so `pm-execution`
/// stays independent of `pm-ingestion`.
///
/// The methods are declared `-> impl Future + Send` (not bare `async fn`) so the
/// futures are provably `Send` for ANY implementor: the background reader is
/// `tokio::spawn`ed from a GENERIC constructor ([`LiveUserWsFills::with_transport_factory`]),
/// which requires the spawned future to be `Send` regardless of the concrete
/// transport. (Implementors may still use `async fn` bodies.)
pub trait WsTransport: Send {
    /// Read the next text frame. `None` = clean close / stream ended;
    /// `Some(Err(_))` = protocol / I/O error.
    fn recv(&mut self) -> impl Future<Output = Option<Result<String, VenueError>>> + Send;

    /// Send a UTF-8 text frame.
    fn send(&mut self, text: &str) -> impl Future<Output = Result<(), VenueError>> + Send;
}

// ---------------------------------------------------------------------------
// Production transport — tokio-tungstenite (mirrors ingestion::TungsteniteTransport)
// ---------------------------------------------------------------------------

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};

/// Production user-WS transport backed by `tokio-tungstenite`. Text frames pass
/// through; Binary / Ping / Pong are skipped; Close / stream end return `None`.
pub struct TungsteniteUserTransport<S> {
    stream: WebSocketStream<S>,
}

impl TungsteniteUserTransport<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    /// Connect to the given user-WS URL.
    pub async fn connect(url: &str) -> Result<Self, VenueError> {
        let (stream, _response) = connect_async(url)
            .await
            .map_err(|e| VenueError::Live(e.to_string()))?;
        Ok(Self { stream })
    }
}

impl<S> WsTransport for TungsteniteUserTransport<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    async fn recv(&mut self) -> Option<Result<String, VenueError>> {
        loop {
            match self.stream.next().await {
                None => return None,
                Some(Err(e)) => return Some(Err(VenueError::Live(e.to_string()))),
                Some(Ok(msg)) => match msg {
                    Message::Text(t) => return Some(Ok(t.to_string())),
                    Message::Close(_) => return None,
                    // Ping / Pong / Binary — skip silently (server PONGs land here).
                    _ => continue,
                },
            }
        }
    }

    async fn send(&mut self, text: &str) -> Result<(), VenueError> {
        self.stream
            .send(Message::Text(text.to_owned()))
            .await
            .map_err(|e| VenueError::Live(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// C) LiveUserWsFills — the UserFillSource
// ---------------------------------------------------------------------------

/// Low-latency live maker-fill source over the user WS (Task 4.6). Behind the
/// SAME [`UserFillSource`] trait as the REST poll, so the generic `run_mm_loop`
/// drives it unchanged (wrapped with the live `MakerVenue` by
/// [`SplitVenue`](crate::split_venue::SplitVenue)).
///
/// Holds the consumer end of an `mpsc` channel fed by a spawned background task
/// (the connect→subscribe→read→reconnect loop). [`poll`](UserFillSource::poll)
/// drains the channel non-blockingly, so it never waits on the socket.
pub struct LiveUserWsFills {
    rx: mpsc::Receiver<MakerFill>,
}

impl LiveUserWsFills {
    /// Production constructor: spawn the background task connecting to `url` with
    /// the real [`TungsteniteUserTransport`], subscribing to `condition_ids`
    /// (markets) with `creds`, resolving each trade's `asset_id` via `resolve`
    /// (venue token id → `(TokenId, TickSize)`).
    pub fn connect(
        url: String,
        creds: ApiCreds,
        condition_ids: Vec<String>,
        resolve: HashMap<String, (TokenId, TickSize)>,
    ) -> Self {
        Self::with_transport_factory(creds, condition_ids, resolve, move || {
            let url = url.clone();
            async move { TungsteniteUserTransport::connect(&url).await }
        })
    }

    /// Testable constructor: spawn the background task driven by an arbitrary
    /// [`WsTransport`] `factory` (called once per (re)connect). Tests pass a mock
    /// factory feeding scripted frames, so the whole feed runs with ZERO network.
    pub fn with_transport_factory<T, F, Fut>(
        creds: ApiCreds,
        condition_ids: Vec<String>,
        resolve: HashMap<String, (TokenId, TickSize)>,
        factory: F,
    ) -> Self
    where
        T: WsTransport + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, VenueError>> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(FILL_CHANNEL_CAP);
        tokio::spawn(run_user_ws(tx, creds, condition_ids, resolve, factory));
        LiveUserWsFills { rx }
    }
}

impl UserFillSource for LiveUserWsFills {
    /// Drain every fill the background task has pushed since the last call
    /// (non-blocking `try_recv` loop). A disconnected channel (task gone) simply
    /// returns what is buffered, then empties — the MM loop tolerates an empty
    /// poll and re-quotes on its own cadence.
    async fn poll(&mut self) -> Result<Vec<MakerFill>, VenueError> {
        let mut out = Vec::new();
        // Drain every buffered fill. `try_recv` returns `Err` on BOTH an empty
        // channel and a disconnected one (task gone) — either ends the drain,
        // returning whatever is buffered (an empty poll is fine for the MM loop).
        while let Ok(fill) = self.rx.try_recv() {
            out.push(fill);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Background task: connect → subscribe → read → reconnect (carrying `seen`)
// ---------------------------------------------------------------------------

/// The spawned reader loop: mint a transport via `factory`, run one session,
/// then reconnect with capped backoff and resubscribe — forever, until the
/// receiver (the [`LiveUserWsFills`]) is dropped. The `seen` dedup set persists
/// ACROSS sessions so a trade re-sent after a reconnect is not double-emitted.
async fn run_user_ws<T, F, Fut>(
    tx: mpsc::Sender<MakerFill>,
    creds: ApiCreds,
    condition_ids: Vec<String>,
    resolve: HashMap<String, (TokenId, TickSize)>,
    mut factory: F,
) where
    T: WsTransport,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, VenueError>>,
{
    // Serialize the subscribe message ONCE: identical on connect + every resub.
    let subscribe_msg = user_subscribe_message(&creds, &condition_ids);
    let mut seen: HashSet<String> = HashSet::new();
    let mut attempt: u32 = 0;
    loop {
        // Receiver dropped → nobody will read fills → stop the task.
        if tx.is_closed() {
            return;
        }
        if attempt > 0 {
            tokio::time::sleep(backoff_delay(BACKOFF_BASE, BACKOFF_CAP, attempt)).await;
        }
        match factory().await {
            Ok(mut transport) => {
                // Connected. Session ends on clean close, transport error, or
                // receiver-gone.
                let _ = run_one_session(&mut transport, &tx, &subscribe_msg, &resolve, &mut seen)
                    .await;
                // A base backoff before reconnecting avoids hot-looping if the
                // server drops us right after subscribe. This ALSO resets the
                // counter after a successful connect: repeated CONNECT failures
                // (the `Err` arm) grow `attempt` exponentially, but a connect
                // that worked drops it back to the 1-step base here.
                attempt = 1;
            }
            Err(_e) => {
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// Drive ONE user-WS session: send the subscribe message, then read frames and
/// push parsed maker fills, sending a `PING` every [`PING_INTERVAL`]. Returns
/// `Ok(())` on a clean close (or the receiver being dropped) and `Err` on a
/// transport / send error — both make the caller reconnect.
async fn run_one_session<T: WsTransport>(
    transport: &mut T,
    tx: &mpsc::Sender<MakerFill>,
    subscribe_msg: &str,
    resolve: &HashMap<String, (TokenId, TickSize)>,
    seen: &mut HashSet<String>,
) -> Result<(), VenueError> {
    // Subscribe FIRST (by condition_id). A send failure here breaks the session.
    transport.send(subscribe_msg).await?;

    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            // Frames first: a live feed proves liveness, so a starved PING under
            // heavy frame flow is harmless (the PING only matters when WE are silent).
            biased;
            frame = transport.recv() => {
                match frame {
                    None => return Ok(()),            // clean close → reconnect
                    Some(Err(e)) => return Err(e),    // transport error → reconnect
                    Some(Ok(text)) => {
                        // Unparseable JSON is ignored (a malformed frame is not fatal).
                        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                            continue;
                        };
                        let fills = parse_ws_trade(&value, |aid| resolve.get(aid).copied(), seen);
                        for fill in fills {
                            // Receiver dropped → the LiveUserWsFills is gone; stop.
                            if tx.send(fill).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            _ = ping.tick() => {
                // Keepalive PING (text). A send failure means the socket is dead.
                transport.send("PING").await?;
            }
        }
    }
}

/// Capped exponential reconnect backoff (no jitter — a single user-WS
/// connection per process, so there is no thundering herd to decorrelate).
/// `attempt` is 1 for the first backoff: `delay = min(cap, base · 2^(attempt−1))`.
fn backoff_delay(base: Duration, cap: Duration, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(32);
    let multiplier = 1u128 << shift;
    let nanos = base
        .as_nanos()
        .saturating_mul(multiplier)
        .min(cap.as_nanos());
    Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

// ---------------------------------------------------------------------------
// Tests (pure parse + the mock-transport task lifecycle; ZERO network)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::secrets::Secret;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    fn creds() -> ApiCreds {
        ApiCreds {
            key: "test-key".into(),
            secret: Secret::new("test-secret".into()),
            passphrase: Secret::new("test-pass".into()),
        }
    }

    /// Resolve only the fixture's registered asset (token 7, Cent ticks) — the
    /// same convention as `fills::tests::resolve`.
    fn resolve(aid: &str) -> Option<(TokenId, TickSize)> {
        (aid == "123456789").then_some((TokenId(7), TickSize::Cent))
    }

    fn resolve_map() -> HashMap<String, (TokenId, TickSize)> {
        HashMap::from([("123456789".to_string(), (TokenId(7), TickSize::Cent))])
    }

    /// One user-channel `trade` frame: `n` maker_orders, the given `status`.
    fn trade_frame(id: &str, status: &str, makers: serde_json::Value) -> serde_json::Value {
        json!({
            "event_type": "trade",
            "id": id,
            "market": "0xcondition",
            "asset_id": "123456789",
            "side": "BUY",
            "size": "15",
            "price": "0.33",
            "status": status,
            "maker_orders": makers,
            "type": "TRADE",
        })
    }

    // ── A) pure pieces ─────────────────────────────────────────────────────

    #[test]
    fn user_subscribe_message_shape() {
        let msg = user_subscribe_message(&creds(), &["0xcond1".into(), "0xcond2".into()]);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        // type = "user" (the user channel), subscribe by condition_id (market).
        assert_eq!(v["type"], "user");
        // auth carries apiKey + secret + passphrase (the L2 creds, raw).
        assert_eq!(v["auth"]["apiKey"], "test-key");
        assert_eq!(v["auth"]["secret"], "test-secret");
        assert_eq!(v["auth"]["passphrase"], "test-pass");
        // markets is the condition-id array (NOT token/asset ids), in order.
        let markets = v["markets"].as_array().unwrap();
        assert_eq!(markets.len(), 2);
        assert_eq!(markets[0], "0xcond1");
        assert_eq!(markets[1], "0xcond2");
    }

    #[test]
    fn parse_ws_trade_emits_once_and_dedups() {
        let mut seen = HashSet::new();
        // A MATCHED trade with TWO maker_orders → two fills.
        let matched = trade_frame(
            "trade-aaa",
            "MATCHED",
            json!([
                {"order_id": "0xresting-A", "matched_amount": "10", "price": "0.33"},
                {"order_id": "0xresting-B", "matched_amount": "5", "price": "0.34"},
            ]),
        );
        let fills = parse_ws_trade(&matched, resolve, &mut seen);
        assert_eq!(fills.len(), 2, "both maker_orders emit on first observation");
        assert_eq!(fills[0].order_id, OrderId("0xresting-A".into()));
        assert_eq!(fills[0].trade_id, "trade-aaa");
        assert_eq!(fills[1].order_id, OrderId("0xresting-B".into()));

        // The SAME trade re-sent as CONFIRMED (same id, same orders) → 0 fills:
        // the lifecycle double-fire is deduped on "{trade_id}:{order_id}".
        let confirmed = trade_frame(
            "trade-aaa",
            "CONFIRMED",
            json!([
                {"order_id": "0xresting-A", "matched_amount": "10", "price": "0.33"},
                {"order_id": "0xresting-B", "matched_amount": "5", "price": "0.34"},
            ]),
        );
        let again = parse_ws_trade(&confirmed, resolve, &mut seen);
        assert!(again.is_empty(), "MATCHED→CONFIRMED dedup: {again:?}");
    }

    #[test]
    fn parse_ws_trade_skips_unregistered_and_malformed() {
        let mut seen = HashSet::new();

        // Unregistered asset → the whole frame is skipped.
        let unreg = json!({
            "event_type": "trade",
            "id": "t-unreg",
            "asset_id": "999999999",
            "maker_orders": [{"order_id": "0xx", "matched_amount": "3", "price": "0.20"}],
        });
        assert!(parse_ws_trade(&unreg, resolve, &mut seen).is_empty());

        // Registered asset, mixed entries: missing matched_amount, then a
        // tick-unaligned price (0.335 on a Cent market), then one good entry —
        // only the good one survives; the bad ones are skipped, not fatal.
        let mixed = trade_frame(
            "t-mixed",
            "MATCHED",
            json!([
                {"order_id": "0xno-amount", "price": "0.33"},
                {"order_id": "0xbad-price", "matched_amount": "4", "price": "0.335"},
                {"order_id": "0xgood", "matched_amount": "4", "price": "0.33"},
            ]),
        );
        let fills = parse_ws_trade(&mixed, resolve, &mut seen);
        assert_eq!(fills.len(), 1, "only the one good entry survives");
        assert_eq!(fills[0].order_id, OrderId("0xgood".into()));

        // A non-trade frame (an `order` event) → empty, never a fill.
        let order_evt = json!({"event_type": "order", "id": "o-1", "asset_id": "123456789"});
        assert!(parse_ws_trade(&order_evt, resolve, &mut seen).is_empty());
        // A frame missing event_type → empty.
        assert!(parse_ws_trade(&json!({"id": "x"}), resolve, &mut seen).is_empty());
    }

    #[test]
    fn parse_ws_trade_maps_qty_px() {
        // 5 shares @ 0.34 on a Cent market → 5e6 µshares, tick 34. Pins the exact
        // µshare + Px mapping (shared with the REST path's helpers).
        let mut seen = HashSet::new();
        let frame = trade_frame(
            "t-exact",
            "MATCHED",
            json!([{"order_id": "0xexact", "matched_amount": "5", "price": "0.34"}]),
        );
        let fills = parse_ws_trade(&frame, resolve, &mut seen);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, Qty(5_000_000), "5 shares → 5e6 µshares");
        assert_eq!(fills[0].px.get(), 34, "0.34 → tick 34 on Cent");
        assert_eq!(fills[0].px.microusdc(TickSize::Cent), 340_000);
    }

    #[test]
    fn backoff_grows_and_caps() {
        let d1 = backoff_delay(BACKOFF_BASE, BACKOFF_CAP, 1);
        let d2 = backoff_delay(BACKOFF_BASE, BACKOFF_CAP, 2);
        let d3 = backoff_delay(BACKOFF_BASE, BACKOFF_CAP, 3);
        assert_eq!(d1, Duration::from_millis(250));
        assert_eq!(d2, Duration::from_millis(500));
        assert_eq!(d3, Duration::from_millis(1000));
        // Large attempt caps at BACKOFF_CAP and never overflows / panics.
        assert_eq!(backoff_delay(BACKOFF_BASE, BACKOFF_CAP, 200), BACKOFF_CAP);
    }

    // ── B) the WS source via a MOCK transport (ZERO network) ───────────────

    /// Scripted mock transport: replays `frames` in order, RECORDS every sent
    /// message in `sent`, then — once frames are exhausted — fires `drained`
    /// (so the test knows all frames have been read + their fills pushed) and
    /// PARKS forever (so the task never reconnects, keeping `seen` stable).
    struct MockTransport {
        frames: VecDeque<String>,
        sent: Arc<Mutex<Vec<String>>>,
        drained: Option<oneshot::Sender<()>>,
    }

    impl WsTransport for MockTransport {
        async fn recv(&mut self) -> Option<Result<String, VenueError>> {
            if let Some(f) = self.frames.pop_front() {
                return Some(Ok(f));
            }
            // Exhausted: the task has read + pushed every prior frame's fills
            // before this call, so signalling here proves the channel is loaded.
            if let Some(tx) = self.drained.take() {
                let _ = tx.send(());
            }
            std::future::pending().await
        }

        async fn send(&mut self, text: &str) -> Result<(), VenueError> {
            self.sent.lock().unwrap().push(text.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn live_user_ws_fills_via_mock_transport() {
        let sent: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (drained_tx, drained_rx) = oneshot::channel();
        // Script: an ignored (subscribe-ack) frame, a MATCHED trade (2 makers),
        // then the SAME trade re-sent as CONFIRMED (a lifecycle dup → deduped).
        let frames: VecDeque<String> = [
            json!({"event_type": "subscriptions", "status": "ok"}).to_string(),
            trade_frame(
                "trade-xyz",
                "MATCHED",
                json!([
                    {"order_id": "0xresting-A", "matched_amount": "10", "price": "0.33"},
                    {"order_id": "0xresting-B", "matched_amount": "5", "price": "0.34"},
                ]),
            )
            .to_string(),
            trade_frame(
                "trade-xyz",
                "CONFIRMED",
                json!([
                    {"order_id": "0xresting-A", "matched_amount": "10", "price": "0.33"},
                    {"order_id": "0xresting-B", "matched_amount": "5", "price": "0.34"},
                ]),
            )
            .to_string(),
        ]
        .into();

        let sent_for_factory = Arc::clone(&sent);
        let mut once = Some((frames, drained_tx));
        let mut fills_src = LiveUserWsFills::with_transport_factory(
            creds(),
            vec!["0xcondition".into()],
            resolve_map(),
            move || {
                // First call returns the scripted mock; the mock parks after its
                // frames, so the task never asks for a second transport.
                let taken = once.take();
                let sent = Arc::clone(&sent_for_factory);
                async move {
                    match taken {
                        Some((frames, drained)) => Ok(MockTransport {
                            frames,
                            sent,
                            drained: Some(drained),
                        }),
                        None => Err(VenueError::Live("mock exhausted".into())),
                    }
                }
            },
        );

        // Wait until the task has read every scripted frame and pushed its fills.
        drained_rx.await.unwrap();

        let fills = fills_src.poll().await.unwrap();
        assert_eq!(fills.len(), 2, "MATCHED emits two fills; CONFIRMED dup deduped");
        assert_eq!(fills[0].order_id, OrderId("0xresting-A".into()));
        assert_eq!(fills[0].qty, Qty(10_000_000));
        assert_eq!(fills[0].px.get(), 33);
        assert_eq!(fills[0].token, TokenId(7));
        assert_eq!(fills[1].order_id, OrderId("0xresting-B".into()));
        assert_eq!(fills[1].qty, Qty(5_000_000));
        assert_eq!(fills[1].px.get(), 34);

        // A second poll yields nothing more (the dup was deduped, not buffered).
        assert!(fills_src.poll().await.unwrap().is_empty());

        // The SUBSCRIBE message was sent FIRST, before any frame was read. (Taken
        // last so the mutex guard is never held across an await.)
        let sent = sent.lock().unwrap();
        assert!(!sent.is_empty(), "a subscribe message was sent");
        let sub: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(sub["type"], "user", "first send is the user subscribe");
        assert_eq!(sub["markets"][0], "0xcondition");
        assert_eq!(sub["auth"]["apiKey"], "test-key");
    }

    #[tokio::test]
    async fn poll_is_empty_when_no_fills_yet() {
        // A transport that sends nothing and parks: poll() returns empty, never
        // blocks (non-blocking try_recv drain).
        struct Idle;
        impl WsTransport for Idle {
            async fn recv(&mut self) -> Option<Result<String, VenueError>> {
                std::future::pending().await
            }
            async fn send(&mut self, _text: &str) -> Result<(), VenueError> {
                Ok(())
            }
        }
        let mut fills_src = LiveUserWsFills::with_transport_factory(
            creds(),
            vec!["0xcond".into()],
            resolve_map(),
            || async { Ok(Idle) },
        );
        assert!(fills_src.poll().await.unwrap().is_empty());
    }
}
