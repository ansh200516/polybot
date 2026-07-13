//! Minimal Gamma API client for the current BTC "Up or Down 5m" market: given a
//! series slug it returns the live window's conditionId, YES/NO token ids, tick
//! size, and open/close timestamps. Parse split from I/O for unit testing.

use crate::IngestError;

/// A resolved 5-min window: identity, both token ids, tick, and its time range.
#[derive(Debug, Clone, PartialEq)]
pub struct GammaWindow {
    pub condition_id: String,
    pub yes_token: String,
    pub no_token: String,
    pub tick_decimals: i64,   // 2 = Cent (0.01), 3 = Milli (0.001)
    pub t_open_ms: i64,
    pub t_close_ms: i64,
}

fn tick_decimals_from_str(s: &str) -> i64 { if s.trim() == "0.001" { 3 } else { 2 } }

fn rfc3339_to_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20 { return None; }
    let num = |a: usize, z: usize| s.get(a..z)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0,4)?, num(5,7)?, num(8,10)?);
    let (h, mi, se) = (num(11,13)?, num(14,16)?, num(17,19)?);
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let doy = (153 * (if mo > 2 { mo - 3 } else { mo + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + h * 3600 + mi * 60 + se) * 1000) as i64)
}

/// Pick the current (open, not closed) window from a Gamma `/events` body.
pub fn parse_current_window(body: &str) -> Result<Option<GammaWindow>, IngestError> {
    let events: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| IngestError::Parse(format!("gamma events: {e}")))?;
    let arr = match events.as_array() { Some(a) => a, None => return Ok(None) };
    for ev in arr {
        let markets = match ev.get("markets").and_then(|m| m.as_array()) { Some(m) => m, None => continue };
        for m in markets {
            if m.get("closed").and_then(|c| c.as_bool()).unwrap_or(false) { continue; }
            let cond = match m.get("conditionId").and_then(|c| c.as_str()) { Some(c) => c, None => continue };
            let toks_raw = match m.get("clobTokenIds").and_then(|t| t.as_str()) { Some(t) => t, None => continue };
            let toks: Vec<String> = serde_json::from_str(toks_raw).unwrap_or_default();
            if toks.len() != 2 { continue; }
            let tick = m.get("orderPriceMinTickSize").and_then(|t| t.as_str()).unwrap_or("0.01");
            let (open, close) = match (
                m.get("startDate").and_then(|s| s.as_str()).and_then(rfc3339_to_ms),
                m.get("endDate").and_then(|s| s.as_str()).and_then(rfc3339_to_ms),
            ) { (Some(o), Some(c)) => (o, c), _ => continue };
            return Ok(Some(GammaWindow {
                condition_id: cond.to_string(),
                yes_token: toks[0].clone(), no_token: toks[1].clone(),
                tick_decimals: tick_decimals_from_str(tick),
                t_open_ms: open, t_close_ms: close,
            }));
        }
    }
    Ok(None)
}

/// Keyless Gamma client (mirrors `DataApiClient`).
pub struct GammaClient { http: reqwest::Client, base: String }
impl GammaClient {
    pub fn new(http: reqwest::Client, base: Option<&str>) -> Self {
        GammaClient { http, base: base.unwrap_or("https://gamma-api.polymarket.com").trim_end_matches('/').to_string() }
    }
    /// Fetch the live window for a series slug (e.g. `btc-updown-5m-<unix>`), if any.
    pub async fn current_window(&self, slug: &str) -> Result<Option<GammaWindow>, IngestError> {
        let url = format!("{}/events?slug={}", self.base, slug);
        let body = self.http.get(&url).send().await.map_err(|e| IngestError::Http(e.to_string()))?
            .error_for_status().map_err(|e| IngestError::Http(e.to_string()))?
            .text().await.map_err(|e| IngestError::Http(e.to_string()))?;
        parse_current_window(&body)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn parse_events_extracts_window() {
        let body = r#"[{"markets":[{
            "conditionId":"0xCOND",
            "clobTokenIds":"[\"111\",\"222\"]",
            "orderPriceMinTickSize":"0.01",
            "startDate":"2026-07-13T07:50:00Z",
            "endDate":"2026-07-13T07:55:00Z",
            "closed":false
        }]}]"#;
        let w = parse_current_window(body).unwrap().unwrap();
        assert_eq!(w.condition_id, "0xCOND");
        assert_eq!(w.yes_token, "111");
        assert_eq!(w.no_token, "222");
        assert_eq!(w.tick_decimals, 2);
        assert_eq!(w.t_open_ms, 1_783_929_000_000);
        assert_eq!(w.t_close_ms, 1_783_929_300_000);
    }

    #[test]
    fn parse_events_skips_closed_and_missing() {
        assert!(parse_current_window("[]").unwrap().is_none());
        let closed = r#"[{"markets":[{"conditionId":"x","clobTokenIds":"[\"1\",\"2\"]","orderPriceMinTickSize":"0.01","startDate":"2026-07-13T07:50:00Z","endDate":"2026-07-13T07:55:00Z","closed":true}]}]"#;
        assert!(parse_current_window(closed).unwrap().is_none());
    }
}
