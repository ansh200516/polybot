//! Gamma / CLOB metadata models. Shapes are fixture-verified (Task 1 + RECON.md).
//!
//! Reconciliations vs. prompt template:
//! - GammaEvent.id: fixture has `"30615"` (string) — plain String, no custom deserializer needed.
//! - GammaMarket: added `maker_base_fee` / `taker_base_fee` (int, RECON §2); `neg_risk_request_id`
//!   is present in fixture but not consumed by M2 — excluded to keep model minimal.
//! - ClobMarket: added `maker_base_fee` / `taker_base_fee` (int, RECON §4/8); ClobToken got
//!   `price` (f64) and `winner` (bool) as seen in clob_markets.json.
//! - ClobMarketsPage: added `limit` and `count` (both default) to absorb envelope fields without
//!   deny_unknown_fields; only `data` and `next_cursor` are consumed downstream.
//! - ClobBook: fully specified from clob_book.json — `market`, `timestamp` (String, milliseconds),
//!   `min_order_size` (String), `tick_size` (String), `neg_risk` (bool), `last_trade_price` (String).
//!   NOTE: `tick_size` here is a String; CLOB /markets `minimum_tick_size` is f64 — per RECON §11.

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GammaError {
    /// `clobTokenIds` field absent or null.
    MissingTokenIds,
    /// `clobTokenIds` field present but not valid JSON.
    MalformedTokenIds,
}

// ---------------------------------------------------------------------------
// Flexible numeric metric deserializer
// ---------------------------------------------------------------------------

/// Deserialize a Gamma numeric metric that may arrive as a JSON number OR a
/// stringified number.
///
/// Venue quirk (fixture-verified): at the **market** level `volume` and
/// `liquidity` are JSON *strings* (e.g. `"824997.24"`, `"21274.49"`) while
/// `volume24hr` is a bare float; at the **event** level all three are bare
/// floats. This accepts either shape.
///
/// Returns `None` for JSON `null`, an empty/whitespace string, or a string that
/// does not parse — an advisory liquidity metric must never fail the whole
/// parse (and a missing metric is treated conservatively as 0 downstream).
fn de_flexible_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct FlexF64;

    impl serde::de::Visitor<'_> for FlexF64 {
        type Value = Option<f64>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a float, a stringified float, or null")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
            Ok(Some(v as f64))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v as f64))
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
            let t = v.trim();
            Ok(if t.is_empty() { None } else { t.parse::<f64>().ok() })
        }

        // JSON `null` arrives here via `deserialize_any`.
        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }

    deserializer.deserialize_any(FlexF64)
}

// ---------------------------------------------------------------------------
// Gamma market
// ---------------------------------------------------------------------------

/// A single Gamma market entry.
///
/// Venue quirk: `clobTokenIds` is a STRINGIFIED JSON array of two uint256
/// decimal strings — deserialise with [`GammaMarket::clob_token_ids`].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    #[serde(default)]
    /// May be "" for legacy markets (real entries in the committed fixture). Sync must not key a map on it without filtering empties.
    pub condition_id: String,
    /// Raw stringified JSON array, e.g. `"[\"1234\", \"5678\"]"`.
    /// Use [`clob_token_ids()`] to parse it.
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    // default false → a market missing this field is treated as inactive and excluded by sync (safe direction: missed market, never a wrongly-included one)
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub question: Option<String>,
    /// Protocol fee in basis points (Gamma reports 1000 = 100 bps for most markets).
    /// RECON §2: integer, not float.
    #[serde(default)]
    pub maker_base_fee: i64,
    /// Protocol fee in basis points.
    #[serde(default)]
    pub taker_base_fee: i64,
    /// Lifetime traded volume, USD. Venue quirk: at the market level Gamma sends
    /// this as a *stringified* float (e.g. `"824997.24"`); [`de_flexible_f64`]
    /// also accepts a bare number. `None` when the field is absent. Captured for
    /// Phase-5 market segmentation only — never gates/orders the universe.
    #[serde(default, deserialize_with = "de_flexible_f64")]
    pub volume: Option<f64>,
    /// Trailing-24h traded volume, USD (`volume24hr`). Market-level Gamma reports
    /// this as a bare float; `None` when absent. Phase-5 segmentation signal.
    #[serde(default, rename = "volume24hr", deserialize_with = "de_flexible_f64")]
    pub volume_24hr: Option<f64>,
    /// Resting book liquidity, USD. Market-level `liquidity` is a *stringified*
    /// float; `None` when absent (e.g. a resolved/closed market — see the Iran
    /// market in the events fixture). Phase-5 segmentation signal.
    #[serde(default, deserialize_with = "de_flexible_f64")]
    pub liquidity: Option<f64>,
    /// Optional category / tag signal. NOTE: the committed Gamma fixtures carry
    /// NO explicit `category` (or `tags`) field at the market or event level, so
    /// this is `None` in practice today. Kept as forward-compatible capture so a
    /// future Gamma response that includes it threads through without a schema
    /// change. The v1 classifier does not consume it (it uses volume+liquidity).
    #[serde(default)]
    pub category: Option<String>,
}

impl GammaMarket {
    /// Parse the stringified `clobTokenIds` field into a `Vec<String>`.
    ///
    /// Returns `Err(GammaError::MissingTokenIds)` if the field was absent/null,
    /// `Err(GammaError::MalformedTokenIds)` if present but not valid JSON.
    pub fn clob_token_ids(&self) -> Result<Vec<String>, GammaError> {
        let raw = self
            .clob_token_ids
            .as_deref()
            .ok_or(GammaError::MissingTokenIds)?;
        serde_json::from_str(raw).map_err(|_| GammaError::MalformedTokenIds)
    }
}

// ---------------------------------------------------------------------------
// Gamma event
// ---------------------------------------------------------------------------

/// A Gamma event (parent of member markets).
///
/// Reconciliation: `id` is a numeric string in the fixture (`"30615"`) — plain
/// String handles both quoted and…well, it must be quoted because JSON; no
/// custom deserialiser needed.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaEvent {
    #[serde(default)]
    /// May deserialize to "" if absent; sync treats empty as "no event grouping".
    pub id: String,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    pub markets: Vec<GammaMarket>,
    #[serde(default)]
    pub title: Option<String>,
}

// ---------------------------------------------------------------------------
// CLOB markets page
// ---------------------------------------------------------------------------

/// Envelope returned by `GET /markets` on the CLOB API.
///
/// Fixture fields: `next_cursor`, `limit`, `count`, `data`.
/// Only `data` and `next_cursor` are consumed by M2; `limit`/`count` are
/// modelled with defaults to avoid parse failures on future envelope changes.
#[derive(Debug, Clone, Deserialize)]
pub struct ClobMarketsPage {
    pub data: Vec<ClobMarket>,
    #[serde(default)]
    pub next_cursor: String,
    /// Page size limit; informational only for M2.
    #[serde(default)]
    pub limit: u32,
    /// Markets returned in this page; informational only for M2.
    #[serde(default)]
    pub count: u32,
}

// ---------------------------------------------------------------------------
// CLOB market
// ---------------------------------------------------------------------------

/// A single market record from the CLOB `/markets` endpoint.
///
/// `minimum_tick_size` is a JSON **float** (0.01, 0.001, or legacy 0.04).
/// All three values MUST parse; supported-ness is Task 12's policy, not here.
#[derive(Debug, Clone, Deserialize)]
pub struct ClobMarket {
    #[serde(default)]
    pub condition_id: String,
    /// JSON float — RECON §4/7: 0.01, 0.001, or legacy 0.04.
    pub minimum_tick_size: f64,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    pub tokens: Vec<ClobToken>,
    #[serde(default)]
    // default false → a market missing this field is treated as inactive and excluded by sync (safe direction: missed market, never a wrongly-included one)
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    /// CLOB maker fee in basis points (typically 0). RECON §4/8.
    #[serde(default)]
    pub maker_base_fee: i64,
    /// CLOB taker fee in basis points (0 or 200 on legacy entries). RECON §4/8.
    #[serde(default)]
    pub taker_base_fee: i64,
}

// ---------------------------------------------------------------------------
// CLOB token (within a market)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ClobToken {
    #[serde(default)]
    pub token_id: String,
    #[serde(default)]
    pub outcome: String,
    /// Settlement price (0 or 1 for resolved; float for active). RECON §4.
    // informational settlement/last price — never used in arithmetic
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub winner: bool,
}

// ---------------------------------------------------------------------------
// CLOB order book
// ---------------------------------------------------------------------------

/// Full order-book snapshot from `GET /book?token_id=<ID>`.
///
/// RECON §5: `timestamp` and `last_trade_price` are strings (milliseconds and
/// decimal price respectively). `tick_size` is a string here, unlike the float
/// `minimum_tick_size` in [`ClobMarket`] — per RECON §11 cross-reference.
#[derive(Debug, Clone, Deserialize)]
pub struct ClobBook {
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub asset_id: String,
    /// Unix milliseconds as string. RECON §5.
    #[serde(default)]
    pub timestamp: String,
    /// 40-char hex, no 0x prefix. RECON §5.
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub bids: Vec<ClobLevel>,
    #[serde(default)]
    pub asks: Vec<ClobLevel>,
    /// Minimum order size as string (e.g. `"5"`). RECON §5.
    #[serde(default)]
    pub min_order_size: String,
    /// Tick size as string (e.g. `"0.001"`). RECON §5 / §11.
    #[serde(default)]
    pub tick_size: String,
    #[serde(default)]
    pub neg_risk: bool,
    /// Last trade price as decimal string. RECON §5.
    #[serde(default)]
    pub last_trade_price: String,
}

// ---------------------------------------------------------------------------
// CLOB order book level
// ---------------------------------------------------------------------------

/// A single price level in the order book.
///
/// Both `price` and `size` are decimal strings per RECON §5.
#[derive(Debug, Clone, Deserialize)]
pub struct ClobLevel {
    pub price: String,
    pub size: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("tests/fixtures/{name}")).unwrap()
    }

    #[test]
    fn parses_gamma_markets_fixture() {
        let markets: Vec<GammaMarket> =
            serde_json::from_str(&fixture("gamma_markets.json")).unwrap();
        assert!(!markets.is_empty());
        for m in &markets {
            assert!(!m.condition_id.is_empty());
            let toks = m.clob_token_ids().unwrap();
            assert_eq!(
                toks.len(),
                2,
                "binary market must have YES and NO token ids"
            );
            assert!(
                toks.iter().all(|t| t.chars().all(|c| c.is_ascii_digit())),
                "token ids must be pure decimal"
            );
        }
    }

    #[test]
    fn parses_gamma_events_fixture() {
        let events: Vec<GammaEvent> = serde_json::from_str(&fixture("gamma_events.json")).unwrap();
        assert!(!events.is_empty());
        assert!(
            events.iter().any(|e| e.neg_risk),
            "fixture must contain a negRisk event"
        );
        assert!(events.iter().any(|e| !e.markets.is_empty()));
    }

    /// The market-level liquidity metrics are captured from the committed
    /// fixtures despite the venue's string-vs-number quirk: market-level
    /// `volume`/`liquidity` are stringified floats, `volume24hr` is a bare float.
    #[test]
    fn captures_market_metrics_from_markets_fixture() {
        let markets: Vec<GammaMarket> =
            serde_json::from_str(&fixture("gamma_markets.json")).unwrap();
        // First fixture market: "New Rihanna Album before GTA VI?"
        let m = &markets[0];
        // volume / liquidity arrive as JSON strings — must parse to floats.
        assert!((m.volume.unwrap() - 824_997.240_332_004_8).abs() < 1e-6);
        assert!((m.liquidity.unwrap() - 21_274.491_1).abs() < 1e-6);
        // volume24hr arrives as a bare float.
        assert!((m.volume_24hr.unwrap() - 1_159.799_728).abs() < 1e-6);
        // No category field in the fixtures.
        assert!(m.category.is_none());
    }

    /// Inside the events fixture, member markets carry the same metrics, and a
    /// closed/resolved market (US x Iran) is missing `liquidity`/`volume24hr`
    /// entirely — which must deserialize to `None`, never a parse error.
    #[test]
    fn captures_member_market_metrics_and_tolerates_missing() {
        let events: Vec<GammaEvent> = serde_json::from_str(&fixture("gamma_events.json")).unwrap();

        let spain = events
            .iter()
            .flat_map(|e| &e.markets)
            .find(|m| m.condition_id.contains("7976b8db"))
            .unwrap();
        assert!((spain.volume.unwrap() - 39_975_679.106_692_575).abs() < 1.0);
        assert!((spain.liquidity.unwrap() - 3_645_429.866_74).abs() < 1.0);
        assert!(spain.volume_24hr.is_some());

        let iran = events
            .iter()
            .flat_map(|e| &e.markets)
            .find(|m| m.condition_id.contains("bbc6689d"))
            .unwrap();
        // volume present (string), but liquidity & volume24hr absent → None.
        assert!(iran.volume.is_some());
        assert!(
            iran.liquidity.is_none(),
            "missing liquidity must be None, not an error"
        );
        assert!(iran.volume_24hr.is_none());
    }

    /// A stringified empty value or an unparseable string yields `None` rather
    /// than failing the parse (advisory metric, conservative downstream).
    #[test]
    fn flexible_metric_tolerates_empty_and_garbage_strings() {
        let m: GammaMarket = serde_json::from_str(
            r#"{"conditionId":"0xa","volume":"","liquidity":"n/a","volume24hr":null}"#,
        )
        .unwrap();
        assert!(m.volume.is_none());
        assert!(m.liquidity.is_none());
        assert!(m.volume_24hr.is_none());
    }

    #[test]
    fn parses_clob_markets_fixture_including_legacy_ticks() {
        let page: ClobMarketsPage = serde_json::from_str(&fixture("clob_markets.json")).unwrap();
        assert!(!page.data.is_empty());
        assert!(!page.next_cursor.is_empty());
        // all tick sizes PARSE (incl. legacy 0.04); supported-ness is Task 12's policy
        let ticks: Vec<f64> = page.data.iter().map(|m| m.minimum_tick_size).collect();
        assert!(
            ticks
                .iter()
                .any(|t| (*t - 0.01).abs() < 1e-9 || (*t - 0.001).abs() < 1e-9),
            "fixture must contain at least one supported tick size"
        );
        for m in &page.data {
            assert!(m.minimum_tick_size > 0.0);
        }
    }

    #[test]
    fn parses_clob_book_fixture() {
        let book: ClobBook = serde_json::from_str(&fixture("clob_book.json")).unwrap();
        assert!(!book.bids.is_empty() || !book.asks.is_empty());
        assert!(!book.hash.is_empty());
        assert!(!book.asset_id.is_empty());
    }

    #[test]
    fn parses_clob_time_fixture() {
        let t: u64 = serde_json::from_str(&fixture("clob_time.json")).unwrap();
        assert!(t > 1_700_000_000);
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        let m: GammaMarket = serde_json::from_str(
            r#"{"conditionId":"0xa","clobTokenIds":"[\"1\",\"2\"]","negRisk":false,
                "active":true,"closed":false,"some_future_field":42}"#,
        )
        .unwrap();
        assert_eq!(
            m.clob_token_ids().unwrap(),
            vec!["1".to_string(), "2".to_string()]
        );
    }

    #[test]
    fn missing_token_ids_is_a_clean_error() {
        let m: GammaMarket = serde_json::from_str(r#"{"conditionId":"0xa"}"#).unwrap();
        assert_eq!(m.clob_token_ids(), Err(GammaError::MissingTokenIds));
        let m: GammaMarket =
            serde_json::from_str(r#"{"conditionId":"0xa","clobTokenIds":"not json"}"#).unwrap();
        assert_eq!(m.clob_token_ids(), Err(GammaError::MalformedTokenIds));
    }

    #[test]
    fn missing_active_defaults_to_excluded() {
        let m: GammaMarket = serde_json::from_str(r#"{"conditionId":"0xa"}"#).unwrap();
        assert!(!m.active);
    }
}
