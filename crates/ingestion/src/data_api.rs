//! Polymarket **Data API** client (`https://data-api.polymarket.com`) — PUBLIC,
//! keyless, read-only. Source for the "top-trader confluence" market signal: the
//! leaderboard's top performers + their OPEN positions decide which markets (and
//! which side) the market maker follows.
//!
//! Two endpoints (shapes RECON-confirmed against live responses):
//! - `GET /v1/leaderboard?orderBy=PNL|VOL&timePeriod=DAY|WEEK|MONTH|ALL&limit=N`
//!   → `[{rank, proxyWallet, userName, vol, pnl, …}]`.
//! - `GET /positions?user=<proxyWallet>&sizeThreshold=<f>&limit=500`
//!   → `[{conditionId, asset, size, outcome, outcomeIndex, curPrice, redeemable,
//!      oppositeAsset, …}]`. `redeemable = true` ⇒ the market RESOLVED (not
//!   tradeable) — callers must drop those.

use serde::Deserialize;

use crate::IngestError;

/// Default Data API base (no auth).
pub const DEFAULT_DATA_API_BASE: &str = "https://data-api.polymarket.com";

/// Leaderboard ranking metric (`orderBy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBy {
    /// Profit and loss — "top performers".
    Pnl,
    /// Trading volume — "most active".
    Vol,
}

impl OrderBy {
    fn as_str(self) -> &'static str {
        match self {
            OrderBy::Pnl => "PNL",
            OrderBy::Vol => "VOL",
        }
    }
}

/// Leaderboard time window (`timePeriod`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimePeriod {
    Day,
    Week,
    Month,
    All,
}

impl TimePeriod {
    fn as_str(self) -> &'static str {
        match self {
            TimePeriod::Day => "DAY",
            TimePeriod::Week => "WEEK",
            TimePeriod::Month => "MONTH",
            TimePeriod::All => "ALL",
        }
    }
}

/// One leaderboard row. Only the fields the confluence uses are kept; the rest
/// of the response (rank, xUsername, profileImage, …) is ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct LeaderboardEntry {
    /// The trader's on-chain Polymarket address (the funder/proxy wallet) — the
    /// key for `positions(user=…)`.
    #[serde(rename = "proxyWallet")]
    pub proxy_wallet: String,
    #[serde(rename = "userName", default)]
    pub user_name: String,
    #[serde(default)]
    pub pnl: f64,
    #[serde(default)]
    pub vol: f64,
}

/// One open position. `redeemable = true` means the market already RESOLVED (so
/// it is NOT a live market to make a market in) — see [`Position::is_open`].
#[derive(Debug, Clone, Deserialize)]
pub struct Position {
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    /// The CLOB-tradeable token id of the outcome this trader HOLDS (their side).
    pub asset: String,
    #[serde(default)]
    pub size: f64,
    #[serde(default)]
    pub outcome: String,
    #[serde(rename = "outcomeIndex", default)]
    pub outcome_index: i64,
    #[serde(rename = "curPrice", default)]
    pub cur_price: f64,
    /// `true` ⇒ the market resolved and the position can be redeemed — i.e. it is
    /// NOT a live, tradeable market.
    #[serde(default)]
    pub redeemable: bool,
}

impl Position {
    /// A live, tradeable position: the market has NOT resolved (`!redeemable`)
    /// and the outcome still has a non-degenerate mark (`0 < curPrice < 1`).
    pub fn is_open(&self) -> bool {
        !self.redeemable && self.cur_price > 0.0 && self.cur_price < 1.0 && self.size > 0.0
    }
}

/// Keyless client for the Data API.
pub struct DataApiClient {
    http: reqwest::Client,
    base: String,
}

impl DataApiClient {
    /// Build a client; `base` defaults to [`DEFAULT_DATA_API_BASE`] when `None`.
    ///
    /// Venue quirk: the Data API actively 403s some default agents (verified:
    /// `Python-urllib/*` is blocked) while an absent/curl/browser agent is fine.
    /// `reqwest` sends no User-Agent by default, so we set an explicit innocuous
    /// one to stay clear of that bot filter.
    pub fn new(base: Option<&str>) -> Result<Self, IngestError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .user_agent("pm-arb-bot/1.0")
            .build()
            .map_err(|e| IngestError::Http(e.to_string()))?;
        Ok(DataApiClient {
            http,
            base: base.unwrap_or(DEFAULT_DATA_API_BASE).trim_end_matches('/').to_string(),
        })
    }

    /// Top `limit` (clamped 1..=50) leaderboard entries for `order_by`/`period`.
    pub async fn leaderboard(
        &self,
        order_by: OrderBy,
        period: TimePeriod,
        limit: usize,
    ) -> Result<Vec<LeaderboardEntry>, IngestError> {
        let url = format!(
            "{}/v1/leaderboard?orderBy={}&timePeriod={}&limit={}",
            self.base,
            order_by.as_str(),
            period.as_str(),
            limit.clamp(1, 50),
        );
        let body = self.get(&url).await?;
        parse_leaderboard(&body)
    }

    /// All positions for `proxy_wallet` with size ≥ `size_threshold`. Includes
    /// resolved (redeemable) positions — filter with [`Position::is_open`].
    pub async fn positions(
        &self,
        proxy_wallet: &str,
        size_threshold: f64,
    ) -> Result<Vec<Position>, IngestError> {
        let url = format!(
            "{}/positions?user={}&sizeThreshold={}&limit=500",
            self.base, proxy_wallet, size_threshold,
        );
        let body = self.get(&url).await?;
        parse_positions(&body)
    }

    async fn get(&self, url: &str) -> Result<String, IngestError> {
        self.http
            .get(url)
            .send()
            .await
            .map_err(|e| IngestError::Http(e.to_string()))?
            .error_for_status()
            .map_err(|e| IngestError::Http(e.to_string()))?
            .text()
            .await
            .map_err(|e| IngestError::Http(e.to_string()))
    }
}

/// Parse a leaderboard JSON array (separated for unit-testing without I/O).
pub fn parse_leaderboard(body: &str) -> Result<Vec<LeaderboardEntry>, IngestError> {
    serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("leaderboard: {e}")))
}

/// Parse a positions JSON array (separated for unit-testing without I/O).
pub fn parse_positions(body: &str) -> Result<Vec<Position>, IngestError> {
    serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("positions: {e}")))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn parses_leaderboard_shape() {
        // Trimmed real /v1/leaderboard response.
        let body = r#"[
            {"rank":"1","proxyWallet":"0x96cfcb0c30942cfcd1cdf76c7d408794d66b1acb",
             "userName":"mintblade","xUsername":"","verifiedBadge":false,
             "vol":17759922.23,"pnl":9238344.62,"profileImage":""},
            {"rank":"2","proxyWallet":"0xED64A7bf029040aa331ABC87902434d815eF217d",
             "userName":"fishalive","vol":13281460.37,"pnl":9063378.17}
        ]"#;
        let rows = parse_leaderboard(body).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].proxy_wallet, "0x96cfcb0c30942cfcd1cdf76c7d408794d66b1acb");
        assert_eq!(rows[0].user_name, "mintblade");
        assert!((rows[0].pnl - 9238344.62).abs() < 1.0);
    }

    #[test]
    fn parses_positions_and_open_filter() {
        // Two positions: a RESOLVED one (redeemable, curPrice 0) and a LIVE one.
        let body = r#"[
            {"proxyWallet":"0xabc","asset":"2144...763","conditionId":"0x3774...ae61",
             "size":3821328.678,"avgPrice":0.3087,"currentValue":0,"cashPnl":-1179938.4,
             "curPrice":0,"redeemable":true,"title":"USA vs PAR draw?","outcome":"Yes","outcomeIndex":0},
            {"proxyWallet":"0xabc","asset":"9999","conditionId":"0xLIVE",
             "size":1000.0,"avgPrice":0.42,"curPrice":0.55,"redeemable":false,
             "title":"Some live market","outcome":"No","outcomeIndex":1}
        ]"#;
        let ps = parse_positions(body).unwrap();
        assert_eq!(ps.len(), 2);
        // The resolved (redeemable) position is NOT open; the live one is.
        assert!(!ps[0].is_open(), "redeemable/curPrice=0 → resolved, not open");
        assert!(ps[1].is_open(), "non-redeemable with 0<curPrice<1 → open");
        assert_eq!(ps[1].condition_id, "0xLIVE");
        assert_eq!(ps[1].asset, "9999");
        assert_eq!(ps[1].outcome, "No");
    }

    #[test]
    fn order_and_period_strings() {
        assert_eq!(OrderBy::Pnl.as_str(), "PNL");
        assert_eq!(OrderBy::Vol.as_str(), "VOL");
        assert_eq!(TimePeriod::Month.as_str(), "MONTH");
        assert_eq!(TimePeriod::All.as_str(), "ALL");
    }
}
