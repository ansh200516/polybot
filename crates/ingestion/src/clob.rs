//! Minimal CLOB REST `/book` poller for the BTC 5m shadow strategy. The 5-min
//! window's YES token is discovered dynamically via Gamma, so it is in NEITHER
//! the shared registry NOR any WS supervisor the arb/mm/copy `BookFetcher`
//! sees — this samples the public `/book` endpoint directly. READ-ONLY: no
//! auth, no orders. Parse split from I/O for unit testing (mirrors `gamma`).
//!
//! Shape matches what `execution::live`'s `best_ask`/`best_bid` parse off the
//! SAME public endpoint: a top-level object with `bids`/`asks` level arrays,
//! each level a `{ "price": "<decimal string>", .. }`; best ask is the MIN ask
//! price and best bid the MAX bid price (iterate all levels, sort-agnostic).

use crate::IngestError;

/// One order-book level. Prices are venue DECIMAL STRINGS ("0.34"); extra
/// fields (e.g. `size`) are ignored.
#[derive(serde::Deserialize)]
struct Level {
    price: String,
}

/// The `/book` response: two level arrays. Either side may be absent or empty.
#[derive(serde::Deserialize)]
struct BookResp {
    #[serde(default)]
    bids: Vec<Level>,
    #[serde(default)]
    asks: Vec<Level>,
}

/// Venue price decimal string → µUSDC/share (`price × 1_000_000`, rounded).
/// `None` for a non-finite / unparseable price so that level is SKIPPED (mirrors
/// `live.rs`'s `filter_map`), never failing the whole book.
fn price_to_micro(s: &str) -> Option<i64> {
    let p = s.trim().parse::<f64>().ok()?;
    if !p.is_finite() {
        return None;
    }
    Some((p * 1_000_000.0).round() as i64)
}

/// Pure parse of a `/book` body → `(best_bid_micro, best_ask_micro)` in µUSDC.
/// Best bid = the MAX bid price, best ask = the MIN ask price, over ALL levels
/// so the result is independent of the venue's sort order. A side that is
/// empty/absent yields `None`. Malformed top-level JSON → [`IngestError::Parse`].
pub fn parse_book_best(body: &str) -> Result<(Option<i64>, Option<i64>), IngestError> {
    let book: BookResp =
        serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("clob /book: {e}")))?;
    let best_bid = book.bids.iter().filter_map(|l| price_to_micro(&l.price)).max();
    let best_ask = book.asks.iter().filter_map(|l| price_to_micro(&l.price)).min();
    Ok((best_bid, best_ask))
}

/// Fetch + parse the public CLOB `/book` for `token_id` (the venue's decimal
/// uint256 id) → `(best_bid_micro, best_ask_micro)`. Public read: no auth, no
/// limiter (sampling cadence is seconds). Transport / non-2xx →
/// [`IngestError::Http`]; a malformed body → [`IngestError::Parse`].
pub async fn fetch_book_best(
    http: &reqwest::Client,
    base: &str,
    token_id: &str,
) -> Result<(Option<i64>, Option<i64>), IngestError> {
    let url = format!("{base}/book?token_id={token_id}");
    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| IngestError::Http(e.to_string()))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| IngestError::Http(e.to_string()))?;
    if !status.is_success() {
        return Err(IngestError::Http(format!("clob /book {status}: {body}")));
    }
    parse_book_best(&body)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn parse_book_best_picks_max_bid_min_ask_in_micro() {
        // Real Polymarket /book shape (per execution::live): string prices,
        // deliberately UNSORTED levels, extra fields (size) ignored.
        let body = r#"{
            "market": "0xabc",
            "bids": [
                {"price": "0.33", "size": "100"},
                {"price": "0.34", "size": "50"},
                {"price": "0.30", "size": "200"}
            ],
            "asks": [
                {"price": "0.37", "size": "80"},
                {"price": "0.35", "size": "20"},
                {"price": "0.40", "size": "10"}
            ]
        }"#;
        let (bid, ask) = parse_book_best(body).unwrap();
        assert_eq!(bid, Some(340_000), "max bid 0.34 -> 340000 uUSDC");
        assert_eq!(ask, Some(350_000), "min ask 0.35 -> 350000 uUSDC");
    }

    #[test]
    fn parse_book_best_handles_empty_and_absent_sides() {
        let (bid, ask) = parse_book_best(r#"{"bids": [], "asks": []}"#).unwrap();
        assert_eq!((bid, ask), (None, None));
        // Absent sides (serde default) → None, not an error.
        let (bid2, ask2) = parse_book_best("{}").unwrap();
        assert_eq!((bid2, ask2), (None, None));
    }

    #[test]
    fn parse_book_best_rejects_non_json() {
        assert!(matches!(
            parse_book_best("not json"),
            Err(IngestError::Parse(_))
        ));
    }
}
