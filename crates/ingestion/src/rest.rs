//! CLOB REST client with deterministic token-bucket rate limiting.
//!
//! Two halves:
//! (a) `TokenBucket` — pure, clock-injected; no async, no I/O.
//! (b) `ClobRest` — thin reqwest wrapper; all response parsing is pure and
//!     fixture-tested via `parse_book_response`.

use std::time::{Duration, Instant};

use crate::livebook::RawLevel;
use crate::IngestError;

// ---------------------------------------------------------------------------
// Token bucket (pure, clock injected)
// ---------------------------------------------------------------------------

/// Result of a single `try_acquire` call.
#[derive(Debug, PartialEq, Eq)]
pub enum Ready {
    /// A token was consumed — proceed immediately.
    Now,
    /// No token available yet — wait at least this long before retrying.
    After(Duration),
}

/// Deterministic token bucket rate limiter.
///
/// `capacity` is the burst ceiling; `rate_per_sec` is the refill rate.
/// Clock is injected via `now: Instant` so tests never sleep.
pub struct TokenBucket {
    capacity: u32,
    tokens: f64,
    rate_per_sec: f64,
    last: Option<Instant>,
}

impl TokenBucket {
    /// Create a full bucket (starts at capacity).
    pub fn new(capacity: u32, rate_per_sec: f64) -> Self {
        TokenBucket { capacity, tokens: f64::from(capacity), rate_per_sec, last: None }
    }

    /// Try to consume one token.
    ///
    /// Uses `saturating_duration_since` so time going backwards produces zero
    /// elapsed and mints no tokens.
    pub fn try_acquire(&mut self, now: Instant) -> Ready {
        if let Some(last) = self.last {
            let dt = now.saturating_duration_since(last).as_secs_f64();
            self.tokens = (self.tokens + dt * self.rate_per_sec).min(f64::from(self.capacity));
        }
        self.last = Some(now);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ready::Now
        } else {
            let need = 1.0 - self.tokens;
            Ready::After(Duration::from_secs_f64(need / self.rate_per_sec))
        }
    }
}

// ---------------------------------------------------------------------------
// Pure book response parsing
// ---------------------------------------------------------------------------

/// A parsed order-book snapshot in exact micro-unit integers.
pub struct ParsedBook {
    pub asset_id: String,
    pub hash: String,
    pub bids: Vec<RawLevel>,
    pub asks: Vec<RawLevel>,
}

/// Parse the raw JSON body of `GET /book?token_id=<ID>` into exact-integer levels.
///
/// Uses `pm_registry::gamma::ClobBook` as the serde shape (fixture-verified).
/// Zero-size levels are skipped silently (they indicate removal in deltas;
/// a snapshot should not contain them, but the venue may slip one in).
/// Off-range prices are NOT filtered here — `livebook` owns tick policy.
pub fn parse_book_response(body: &str) -> Result<ParsedBook, IngestError> {
    let raw: pm_registry::gamma::ClobBook =
        serde_json::from_str(body).map_err(|e| IngestError::Parse(e.to_string()))?;

    let mut bids = Vec::with_capacity(raw.bids.len());
    for lvl in &raw.bids {
        let price_micro =
            crate::decimal::parse_micro(&lvl.price).map_err(IngestError::Decimal)?;
        let size_micro =
            crate::decimal::parse_micro(&lvl.size).map_err(IngestError::Decimal)?;
        if size_micro == 0 {
            continue; // silently skip zero-size levels
        }
        bids.push(RawLevel { price_micro, size_micro });
    }

    let mut asks = Vec::with_capacity(raw.asks.len());
    for lvl in &raw.asks {
        let price_micro =
            crate::decimal::parse_micro(&lvl.price).map_err(IngestError::Decimal)?;
        let size_micro =
            crate::decimal::parse_micro(&lvl.size).map_err(IngestError::Decimal)?;
        if size_micro == 0 {
            continue;
        }
        asks.push(RawLevel { price_micro, size_micro });
    }

    Ok(ParsedBook { asset_id: raw.asset_id, hash: raw.hash, bids, asks })
}

// ---------------------------------------------------------------------------
// CLOB REST client
// ---------------------------------------------------------------------------

/// Thin reqwest client for the Polymarket CLOB REST API.
///
/// All methods call `acquire()` before issuing the HTTP request so throughput
/// is bounded by the token bucket.
pub struct ClobRest {
    http: reqwest::Client,
    base: String,
    bucket: TokenBucket,
}

impl ClobRest {
    /// Create a new client.
    ///
    /// `base` should be the CLOB base URL, e.g. `"https://clob.polymarket.com"`.
    ///
    /// # Errors
    /// Returns `IngestError::Http` if the underlying TLS stack fails to
    /// initialise (extremely unlikely with the default rustls config).
    pub fn new(
        base: &str,
        capacity: u32,
        rate_per_sec: f64,
    ) -> Result<Self, IngestError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| IngestError::Http(e.to_string()))?;
        Ok(ClobRest { http, base: base.to_owned(), bucket: TokenBucket::new(capacity, rate_per_sec) })
    }

    /// Sleep until the bucket has a token.
    async fn acquire(&mut self) {
        loop {
            match self.bucket.try_acquire(Instant::now()) {
                Ready::Now => return,
                Ready::After(d) => tokio::time::sleep(d).await,
            }
        }
    }

    /// Fetch a full order-book snapshot for one token.
    pub async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, IngestError> {
        self.acquire().await;
        let url = format!("{}/book?token_id={}", self.base, venue_token_id);
        let body = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| IngestError::Http(e.to_string()))?
            .error_for_status()
            .map_err(|e| IngestError::Http(e.to_string()))?
            .text()
            .await
            .map_err(|e| IngestError::Http(e.to_string()))?;
        parse_book_response(&body)
    }

    /// Walk all CLOB markets pages and return accumulated records.
    ///
    /// Pagination per RECON.md: cursor starts as empty string; terminal value
    /// is `"LTE="` (base64 of "-1"). Guard: abort after 200 pages.
    pub async fn all_markets(
        &mut self,
    ) -> Result<Vec<pm_registry::gamma::ClobMarket>, IngestError> {
        const TERMINAL: &str = "LTE=";
        const MAX_PAGES: usize = 200;

        let mut cursor = String::new();
        let mut all = Vec::new();

        for _ in 0..MAX_PAGES {
            self.acquire().await;
            let url = format!("{}/markets?next_cursor={}", self.base, cursor);
            let body = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| IngestError::Http(e.to_string()))?
                .error_for_status()
                .map_err(|e| IngestError::Http(e.to_string()))?
                .text()
                .await
                .map_err(|e| IngestError::Http(e.to_string()))?;

            let page: pm_registry::gamma::ClobMarketsPage =
                serde_json::from_str(&body).map_err(|e| IngestError::Parse(e.to_string()))?;

            let done = page.next_cursor == TERMINAL || page.next_cursor.is_empty();
            all.extend(page.data);
            if done {
                return Ok(all);
            }
            cursor = page.next_cursor;
        }

        Err(IngestError::Http("pagination runaway".into()))
    }

    /// Fetch the server's current Unix timestamp in seconds.
    pub async fn server_time(&mut self) -> Result<u64, IngestError> {
        self.acquire().await;
        let url = format!("{}/time", self.base);
        let body = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| IngestError::Http(e.to_string()))?
            .error_for_status()
            .map_err(|e| IngestError::Http(e.to_string()))?
            .text()
            .await
            .map_err(|e| IngestError::Http(e.to_string()))?;
        body.trim()
            .parse::<u64>()
            .map_err(|e| IngestError::Parse(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn bucket_allows_burst_then_throttles() {
        let mut tb = TokenBucket::new(5, 10.0); // cap 5, 10 tokens/sec
        let t0 = Instant::now();
        for _ in 0..5 {
            assert_eq!(tb.try_acquire(t0), Ready::Now);
        }
        match tb.try_acquire(t0) {
            Ready::After(d) => assert!(d > Duration::ZERO && d <= Duration::from_millis(100)),
            Ready::Now => panic!("bucket should be empty"),
        }
        // refill after 100ms → one token
        assert_eq!(tb.try_acquire(t0 + Duration::from_millis(100)), Ready::Now);
    }

    #[test]
    fn bucket_caps_refill() {
        let mut tb = TokenBucket::new(2, 1000.0);
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(60);
        assert_eq!(tb.try_acquire(later), Ready::Now);
        assert_eq!(tb.try_acquire(later), Ready::Now);
        assert!(matches!(tb.try_acquire(later), Ready::After(_)));
    }

    #[test]
    fn bucket_time_going_backwards_is_safe() {
        let mut tb = TokenBucket::new(1, 1.0);
        let t0 = Instant::now();
        assert_eq!(tb.try_acquire(t0 + Duration::from_secs(5)), Ready::Now);
        // a now BEFORE last seen must not panic or mint tokens
        assert!(matches!(tb.try_acquire(t0), Ready::After(_)));
    }

    #[test]
    fn book_response_parses_to_raw_levels() {
        let raw =
            std::fs::read_to_string("../registry/tests/fixtures/clob_book.json").unwrap();
        let parsed = parse_book_response(&raw).unwrap();
        assert!(!parsed.bids.is_empty() || !parsed.asks.is_empty());
        assert!(!parsed.hash.is_empty());
        assert!(!parsed.asset_id.is_empty());
        for l in parsed.bids.iter().chain(parsed.asks.iter()) {
            assert!(l.price_micro > 0 && l.price_micro < 1_000_000);
            assert!(l.size_micro > 0);
        }
    }

    #[test]
    fn malformed_book_is_a_parse_error() {
        assert!(parse_book_response("{").is_err());
        assert!(parse_book_response(
            r#"{"asset_id":"1","hash":"h","bids":[{"price":"abc","size":"1"}],"asks":[]}"#
        )
        .is_err());
    }
}
