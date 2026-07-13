//! Composite BTC/USD spot feed for the btc5m strategy. Polls several exchanges'
//! last-trade REST endpoints and publishes the MEDIAN price (a cheap proxy for
//! the Chainlink Data Streams multi-venue aggregate that actually settles the
//! market — NOT any single exchange, and NOT the Polymarket UI feed). A 1-minute
//! bar aggregator turns the tape into close-to-close $-returns for vol.

use crate::IngestError;

/// Parse Coinbase `/products/BTC-USD/ticker` → last price.
pub fn parse_coinbase(body: &str) -> Result<f64, IngestError> {
    #[derive(serde::Deserialize)] struct T { price: String }
    let t: T = serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("coinbase: {e}")))?;
    t.price.parse::<f64>().map_err(|e| IngestError::Parse(format!("coinbase price: {e}")))
}

/// Parse Kraken `/0/public/Ticker?pair=XBTUSD` → last trade price (`c[0]`).
pub fn parse_kraken(body: &str) -> Result<f64, IngestError> {
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("kraken: {e}")))?;
    let result = v.get("result").and_then(|r| r.as_object())
        .ok_or_else(|| IngestError::Parse("kraken: no result".into()))?;
    let pair = result.values().next().ok_or_else(|| IngestError::Parse("kraken: empty result".into()))?;
    let last = pair.get("c").and_then(|c| c.get(0)).and_then(|s| s.as_str())
        .ok_or_else(|| IngestError::Parse("kraken: no c[0]".into()))?;
    last.parse::<f64>().map_err(|e| IngestError::Parse(format!("kraken last: {e}")))
}

/// Median of a slice ($). `NaN` for an empty slice.
pub fn median(xs: &[f64]) -> f64 {
    if xs.is_empty() { return f64::NAN; }
    let mut v: Vec<f64> = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

/// Close-to-close 1-minute bar aggregator. `push(ts_ms, price)` returns
/// `Some($-return)` when a bar boundary is crossed and a prior close exists.
#[derive(Debug, Default)]
pub struct MinuteBars { cur_min: Option<i64>, last_price: f64, prev_close: Option<f64> }

impl MinuteBars {
    pub fn new() -> Self { MinuteBars::default() }
    pub fn push(&mut self, ts_ms: i64, price: f64) -> Option<f64> {
        let minute = ts_ms.div_euclid(60_000);
        let mut ret = None;
        match self.cur_min {
            Some(m) if m == minute => {}
            Some(_) => {
                if let Some(pc) = self.prev_close { ret = Some(self.last_price - pc); }
                self.prev_close = Some(self.last_price);
            }
            None => {}
        }
        self.cur_min = Some(minute);
        self.last_price = price;
        ret
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn parse_coinbase_and_kraken_last_price() {
        assert!((parse_coinbase(r#"{"price":"62931.12","time":"..."}"#).unwrap() - 62931.12).abs() < 1e-6);
        let k = r#"{"error":[],"result":{"XXBTZUSD":{"c":["62930.50","0.01"]}}}"#;
        assert!((parse_kraken(k).unwrap() - 62930.50).abs() < 1e-6);
    }

    #[test]
    fn median_of_prices() {
        assert!((median(&[100.0, 102.0, 101.0]) - 101.0).abs() < 1e-9);
        assert!((median(&[100.0, 102.0]) - 101.0).abs() < 1e-9);
        assert!(median(&[]).is_nan());
    }

    #[test]
    fn one_minute_bars_emit_close_to_close_returns() {
        let mut agg = MinuteBars::new();
        assert_eq!(agg.push(60_000, 100.0), None);
        assert_eq!(agg.push(90_000, 105.0), None);
        assert_eq!(agg.push(120_000, 107.0), None);
        let r = agg.push(181_000, 110.0).unwrap();
        assert!((r - 2.0).abs() < 1e-9);
    }
}

use std::sync::Arc;
use tokio::sync::watch;

/// Latest composite spot + vol readiness, published for the strategy loop.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpotSnapshot { pub ts_ms: i64, pub price: f64, pub sigma_1min: f64, pub vol_ready: bool }

/// Handle to the running feed. Clone-cheap; `latest()` reads the watch value.
#[derive(Clone)]
pub struct SpotFeed { rx: watch::Receiver<SpotSnapshot> }
impl SpotFeed { pub fn latest(&self) -> SpotSnapshot { *self.rx.borrow() } }

/// Spawn the poller. `sources` ∈ {"coinbase","kraken"} (others ignored). Task ends when `kill` is set.
pub fn spawn(
    http: reqwest::Client,
    sources: Vec<String>,
    poll_ms: u64,
    half_life_min: f64,
    warmup: u32,
    kill: Arc<std::sync::atomic::AtomicBool>,
) -> SpotFeed {
    let (tx, rx) = watch::channel(SpotSnapshot::default());
    tokio::spawn(async move {
        use std::sync::atomic::Ordering;
        let lambda = 0.5f64.powf(1.0 / half_life_min);
        let (mut var, mut n) = (0.0f64, 0u32);
        let mut bars = MinuteBars::new();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(poll_ms));
        loop {
            if kill.load(Ordering::Relaxed) { break; }
            tick.tick().await;
            let mut prices = Vec::new();
            for s in &sources {
                let (url, which) = match s.as_str() {
                    "coinbase" => ("https://api.exchange.coinbase.com/products/BTC-USD/ticker", 0),
                    "kraken"   => ("https://api.kraken.com/0/public/Ticker?pair=XBTUSD", 1),
                    _ => continue,
                };
                if let Ok(resp) = http.get(url).send().await {
                    if let Ok(body) = resp.text().await {
                        let p = if which == 0 { parse_coinbase(&body) } else { parse_kraken(&body) };
                        if let Ok(px) = p { if px.is_finite() && px > 0.0 { prices.push(px); } }
                    }
                }
            }
            let price = median(&prices);
            if !price.is_finite() { continue; }
            let now_ms = chrono_now_ms();
            if let Some(r) = bars.push(now_ms, price) {
                let sq = r * r;
                var = if n == 0 { sq } else { lambda * var + (1.0 - lambda) * sq };
                n = n.saturating_add(1);
            }
            let _ = tx.send(SpotSnapshot { ts_ms: now_ms, price, sigma_1min: var.sqrt(), vol_ready: n >= warmup });
        }
    });
    SpotFeed { rx }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
