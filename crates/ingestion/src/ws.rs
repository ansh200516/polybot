//! WebSocket frame models and transport abstraction for the Polymarket CLOB
//! market feed.
//!
//! # Frame envelopes
//! - `book` events arrive as a **JSON array** of objects.
//! - `price_change` arrives as a **single JSON object** whose `price_changes`
//!   field is an array; each element carries its own `asset_id` and `hash`.
//! - `last_trade_price` and all other event types are mapped to `WsEvent::Other`.
//!
//! `parse_frame` handles both the array and single-object envelopes defensively.

use serde::Deserialize;

use crate::decimal::parse_micro;
use crate::livebook::RawLevel;
use crate::IngestError;

// ---------------------------------------------------------------------------
// Raw serde shapes (direct JSON model)
// ---------------------------------------------------------------------------

/// A single bid/ask level as it arrives on the wire (string fields).
#[derive(Debug, Deserialize)]
pub struct WireLevel {
    pub price: String,
    pub size: String,
}

/// Raw book event as it arrives on the wire.
#[derive(Debug, Deserialize)]
pub struct BookEvent {
    pub asset_id: String,
    pub hash: String,
    #[serde(default)]
    pub bids: Vec<WireLevel>,
    #[serde(default)]
    pub asks: Vec<WireLevel>,
}

impl BookEvent {
    /// Convert wire levels to `RawLevel` pairs, mirroring `parse_book_response`.
    ///
    /// Zero-size levels are skipped (they signal removal in snapshots; the WS
    /// feed should not send them in a full `book` frame, but be defensive).
    pub fn to_raw_levels(&self) -> Result<(Vec<RawLevel>, Vec<RawLevel>), IngestError> {
        let mut bids = Vec::with_capacity(self.bids.len());
        for lvl in &self.bids {
            let price_micro = parse_micro(&lvl.price).map_err(IngestError::Decimal)?;
            let size_micro = parse_micro(&lvl.size).map_err(IngestError::Decimal)?;
            if size_micro == 0 {
                continue;
            }
            bids.push(RawLevel { price_micro, size_micro });
        }
        let mut asks = Vec::with_capacity(self.asks.len());
        for lvl in &self.asks {
            let price_micro = parse_micro(&lvl.price).map_err(IngestError::Decimal)?;
            let size_micro = parse_micro(&lvl.size).map_err(IngestError::Decimal)?;
            if size_micro == 0 {
                continue;
            }
            asks.push(RawLevel { price_micro, size_micro });
        }
        Ok((bids, asks))
    }
}

/// A single price-change entry within a `price_change` frame.
/// Size 0 is MEANINGFUL here (level removal) — do NOT skip.
#[derive(Debug)]
pub struct ParsedChange {
    /// Token (asset) this change applies to.
    pub asset_id: String,
    /// Optional per-change hash from the venue.
    pub hash: Option<String>,
    /// true = BUY (bid) side, false = SELL (ask) side.
    pub side_buy: bool,
    /// Price in micro-units (×10⁶).
    pub price_micro: u64,
    /// Size in micro-units (×10⁶). 0 = level removal.
    pub size_micro: u64,
}

/// Parsed `price_change` event. `asset_id` is the market (condition) identifier
/// from the top-level `market` field; individual token IDs are on each `change`.
#[derive(Debug)]
pub struct PriceChangeEvent {
    /// The market (condition) ID from the `market` field.
    pub asset_id: String,
    /// Optional top-level hash (absent in observed fixtures).
    pub hash: Option<String>,
    /// One entry per changed price level, across all tokens in the market.
    pub changes: Vec<ParsedChange>,
}

// ---------------------------------------------------------------------------
// WsEvent discriminated union
// ---------------------------------------------------------------------------

/// A fully-parsed WebSocket frame.
#[derive(Debug)]
pub enum WsEvent {
    /// Full book snapshot (one per token, from array envelope).
    Book(BookEvent),
    /// Price-level changes for one or more tokens in a market.
    PriceChange(PriceChangeEvent),
    /// Tick-size change notification.
    TickSizeChange {
        asset_id: String,
        new_tick: String,
    },
    /// Any event type not explicitly handled (e.g. `last_trade_price`).
    Other,
}

// ---------------------------------------------------------------------------
// Frame parser
// ---------------------------------------------------------------------------

/// Parse a raw WebSocket text frame into zero or more `WsEvent`s.
///
/// Handles both array envelopes (`book`) and single-object envelopes
/// (`price_change`, `last_trade_price`, etc.).
pub fn parse_frame(text: &str) -> Result<Vec<WsEvent>, IngestError> {
    let v: serde_json::Value =
        serde_json::from_str(text).map_err(|e| IngestError::Parse(e.to_string()))?;

    match &v {
        serde_json::Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(parse_one(item)?);
            }
            Ok(out)
        }
        serde_json::Value::Object(_) => Ok(vec![parse_one(&v)?]),
        other => Err(IngestError::Parse(format!("unexpected frame shape: {other}"))),
    }
}

/// Parse a single JSON object into one `WsEvent`.
fn parse_one(v: &serde_json::Value) -> Result<WsEvent, IngestError> {
    let event_type = v
        .get("event_type")
        .and_then(|x| x.as_str())
        .unwrap_or("");

    match event_type {
        "book" => {
            let ev: BookEvent = serde_json::from_value(v.clone())
                .map_err(|e| IngestError::Parse(e.to_string()))?;
            Ok(WsEvent::Book(ev))
        }
        "price_change" => {
            // Top-level `market` field is used as asset_id (no top-level asset_id in fixture).
            let asset_id = v
                .get("market")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_owned();
            let hash = v.get("hash").and_then(|x| x.as_str()).map(str::to_owned);

            let changes_raw = v
                .get("price_changes")
                .and_then(|x| x.as_array())
                .ok_or_else(|| {
                    IngestError::Parse("price_change frame missing price_changes array".into())
                })?;

            let mut changes = Vec::with_capacity(changes_raw.len());
            for item in changes_raw {
                let change_asset_id = item
                    .get("asset_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_owned();
                let change_hash =
                    item.get("hash").and_then(|x| x.as_str()).map(str::to_owned);
                let price_str = item
                    .get("price")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| IngestError::Parse("price_changes entry missing price".into()))?;
                let size_str = item
                    .get("size")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| IngestError::Parse("price_changes entry missing size".into()))?;
                let side_str = item
                    .get("side")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| IngestError::Parse("price_changes entry missing side".into()))?;

                let side_buy = match side_str {
                    "BUY" => true,
                    "SELL" => false,
                    other => {
                        return Err(IngestError::Parse(format!(
                            "unknown side value in price_changes: {other:?}"
                        )))
                    }
                };
                let price_micro =
                    parse_micro(price_str).map_err(|e| IngestError::Parse(e.to_string()))?;
                let size_micro =
                    parse_micro(size_str).map_err(|e| IngestError::Parse(e.to_string()))?;

                changes.push(ParsedChange {
                    asset_id: change_asset_id,
                    hash: change_hash,
                    side_buy,
                    price_micro,
                    size_micro,
                });
            }

            Ok(WsEvent::PriceChange(PriceChangeEvent { asset_id, hash, changes }))
        }
        "tick_size_change" => {
            let asset_id = v
                .get("asset_id")
                .and_then(|x| x.as_str())
                .ok_or_else(|| {
                    IngestError::Parse("tick_size_change missing asset_id".into())
                })?
                .to_owned();
            let new_tick = v
                .get("new_tick_size")
                .and_then(|x| x.as_str())
                .ok_or_else(|| {
                    IngestError::Parse("tick_size_change missing new_tick_size".into())
                })?
                .to_owned();
            Ok(WsEvent::TickSizeChange { asset_id, new_tick })
        }
        // last_trade_price and anything else
        _ => Ok(WsEvent::Other),
    }
}

// ---------------------------------------------------------------------------
// Subscribe message builder
// ---------------------------------------------------------------------------

/// Build a JSON subscription message for the given asset IDs.
///
/// Shape: `{"type":"market","assets_ids":["id1","id2",...]}`.
pub fn subscribe_message(asset_ids: &[String]) -> String {
    let ids: Vec<serde_json::Value> =
        asset_ids.iter().map(|s| serde_json::Value::String(s.clone())).collect();
    serde_json::json!({
        "type": "market",
        "assets_ids": ids,
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Transport abstraction
// ---------------------------------------------------------------------------

/// Abstract WebSocket transport used by the ingestion supervisor.
///
/// Implemented by `TungsteniteTransport` for production and by test doubles
/// for replay-based tests (T11).
#[allow(async_fn_in_trait)]
pub trait WsTransport {
    /// Read the next text frame from the connection.
    ///
    /// Returns `None` when the connection has cleanly closed or the stream has
    /// ended; returns `Some(Err(_))` on protocol or I/O errors.
    async fn next_frame(&mut self) -> Option<Result<String, IngestError>>;

    /// Send a UTF-8 text frame.
    async fn send_text(&mut self, text: &str) -> Result<(), IngestError>;
}

// ---------------------------------------------------------------------------
// Production transport — tokio-tungstenite
// ---------------------------------------------------------------------------

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    WebSocketStream,
};

/// Production WebSocket transport backed by `tokio-tungstenite`.
///
/// Text frames are passed through directly. Binary, Ping, and Pong frames are
/// silently skipped. Close frames and stream exhaustion return `None`.
/// I/O or protocol errors surface as `Some(Err(IngestError::Ws(_)))`.
pub struct TungsteniteTransport<S> {
    stream: WebSocketStream<S>,
}

impl TungsteniteTransport<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    /// Connect to the given WebSocket URL and return a transport.
    pub async fn connect(url: &str) -> Result<Self, IngestError> {
        let (stream, _response) =
            connect_async(url).await.map_err(|e| IngestError::Ws(e.to_string()))?;
        Ok(Self { stream })
    }
}

impl<S> WsTransport for TungsteniteTransport<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    async fn next_frame(&mut self) -> Option<Result<String, IngestError>> {
        loop {
            match self.stream.next().await {
                None => return None,
                Some(Err(e)) => return Some(Err(IngestError::Ws(e.to_string()))),
                Some(Ok(msg)) => match msg {
                    Message::Text(t) => return Some(Ok(t.to_string())),
                    Message::Close(_) => return None,
                    // Ping / Pong / Binary — skip silently
                    _ => continue,
                },
            }
        }
    }

    async fn send_text(&mut self, text: &str) -> Result<(), IngestError> {
        self.stream
            .send(Message::Text(text.to_owned()))
            .await
            .map_err(|e| IngestError::Ws(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("../registry/tests/fixtures/{name}")).unwrap()
    }

    #[test]
    fn parses_book_event_fixture() {
        let evs = parse_frame(&fixture("ws_book.json")).unwrap();
        assert!(!evs.is_empty());
        let WsEvent::Book(b) = &evs[0] else { panic!("expected Book, got {:?}", evs[0]) };
        assert!(!b.asset_id.is_empty());
        assert!(!b.hash.is_empty());
        assert!(!b.bids.is_empty() || !b.asks.is_empty());
    }

    #[test]
    fn parses_price_change_fixture() {
        let evs = parse_frame(&fixture("ws_price_change.json")).unwrap();
        let WsEvent::PriceChange(pc) = &evs[0] else { panic!("expected PriceChange") };
        assert!(!pc.asset_id.is_empty());
        assert!(!pc.changes.is_empty());
        for c in &pc.changes {
            assert!(c.price_micro > 0 && c.price_micro < 1_000_000);
        }
        assert!(pc.changes.iter().any(|c| c.side_buy) || pc.changes.iter().any(|c| !c.side_buy));
    }

    #[test]
    fn parses_last_trade_price_fixture_as_other() {
        let evs = parse_frame(&fixture("ws_last_trade_price.json")).unwrap();
        assert!(matches!(evs[0], WsEvent::Other));
    }

    #[test]
    fn unknown_event_types_are_tolerated() {
        let evs = parse_frame(r#"{"event_type":"sandwich_alert","asset_id":"1"}"#).unwrap();
        assert!(matches!(evs[0], WsEvent::Other));
    }

    #[test]
    fn array_and_single_object_frames_both_parse() {
        let single = r#"{"event_type":"sandwich_alert"}"#;
        let array = r#"[{"event_type":"sandwich_alert"},{"event_type":"sandwich_alert"}]"#;
        assert_eq!(parse_frame(single).unwrap().len(), 1);
        assert_eq!(parse_frame(array).unwrap().len(), 2);
    }

    #[test]
    fn bad_change_prices_fail_loudly_per_event() {
        // a malformed price inside price_changes is an error (caller counts it)
        let frame = r#"{"event_type":"price_change","asset_id":"1","price_changes":[{"price":"abc","size":"1","side":"BUY"}]}"#;
        assert!(parse_frame(frame).is_err());
    }

    #[test]
    fn tick_size_change_parses() {
        let frame = r#"{"event_type":"tick_size_change","asset_id":"1","old_tick_size":"0.01","new_tick_size":"0.001"}"#;
        let evs = parse_frame(frame).unwrap();
        assert!(matches!(&evs[0], WsEvent::TickSizeChange { asset_id, new_tick } if asset_id == "1" && new_tick == "0.001"));
    }

    #[test]
    fn subscribe_message_shape() {
        let msg = subscribe_message(&["111".into(), "222".into()]);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["type"], "market");
        assert_eq!(v["assets_ids"].as_array().unwrap().len(), 2);
    }
}
