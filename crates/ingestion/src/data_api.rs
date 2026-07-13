//! Polymarket **Data API** client (`https://data-api.polymarket.com`) — PUBLIC,
//! keyless, read-only. Source for the "top-trader confluence" market signal: the
//! leaderboard's top performers + their OPEN positions decide which markets (and
//! which side) the market maker follows.
//!
//! Endpoints (shapes RECON-confirmed against live responses):
//! - `GET /v1/leaderboard?orderBy=PNL|VOL&timePeriod=DAY|WEEK|MONTH|ALL&limit=N`
//!   → `[{rank, proxyWallet, userName, vol, pnl, …}]`.
//! - `GET /positions?user=<proxyWallet>&sizeThreshold=<f>&limit=500`
//!   → `[{conditionId, asset, size, outcome, outcomeIndex, curPrice, redeemable,
//!      oppositeAsset, …}]`. `redeemable = true` ⇒ the market RESOLVED (not
//!   tradeable) — callers must drop those.
//! - `GET /trades?user=<w>|market=<conditionId>&limit=&offset=` → timestamped
//!   fills `[{proxyWallet, side, asset, conditionId, size, price, timestamp,
//!   outcome, outcomeIndex, …}]`. Paginated; the backtest's market tape + the
//!   per-trader follow signals.
//! - `GET /closed-positions?user=<w>` → resolved track record (like `/positions`
//!   plus realized `avgPrice`/`cashPnl`; `curPrice ≈ 1/0` ⇒ won/lost).

use serde::{Deserialize, Serialize};

use crate::IngestError;

/// Default Data API base (no auth).
pub const DEFAULT_DATA_API_BASE: &str = "https://data-api.polymarket.com";

/// `/trades` pagination page size — fetched per request, paged via `offset`.
const TRADES_PAGE: usize = 100;

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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// Average entry price the trader paid for this side (`avgPrice`). Used by the
    /// backtest's edge-per-bet ranking.
    #[serde(rename = "avgPrice", default)]
    pub avg_price: f64,
    /// Realized + unrealized cash P&L on the position (`cashPnl`).
    #[serde(rename = "cashPnl", default)]
    pub cash_pnl: f64,
    /// `true` ⇒ the market resolved and the position can be redeemed — i.e. it is
    /// NOT a live, tradeable market.
    #[serde(default)]
    pub redeemable: bool,
    /// `true` ⇒ a NEGATIVE-RISK (multi-outcome) market — redemption/merge must
    /// target the NegRisk adapter, not the standard CTF one. Sourced from the Data
    /// API's `negativeRisk` so the reconcile redeem-sweep routes it correctly.
    #[serde(rename = "negativeRisk", default)]
    pub neg_risk: bool,
}

impl Position {
    /// A live, tradeable position: the market has NOT resolved (`!redeemable`)
    /// and the outcome still has a non-degenerate mark (`0 < curPrice < 1`).
    pub fn is_open(&self) -> bool {
        !self.redeemable && self.cur_price > 0.0 && self.cur_price < 1.0 && self.size > 0.0
    }
}

/// Side of a fill on the CLOB, as reported by `/trades` (`"BUY"`/`"SELL"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

impl<'de> Deserialize<'de> for TradeSide {
    /// Lenient on case (`BUY`/`buy`/`Buy` all parse) but strict on meaning: an
    /// unrecognized side is a hard error rather than a silent default, because a
    /// misclassified Buy/Sell would invert a copy signal in the backtest.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_ascii_uppercase().as_str() {
            "BUY" => Ok(TradeSide::Buy),
            "SELL" => Ok(TradeSide::Sell),
            other => Err(serde::de::Error::custom(format!(
                "unknown trade side: {other:?} (expected BUY or SELL)"
            ))),
        }
    }
}

impl Serialize for TradeSide {
    /// Emits the wire form (`"BUY"`/`"SELL"`) so cached `/trades` JSON round-trips
    /// through the lenient [`Deserialize`] above (used by the backtest cache).
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        })
    }
}

/// One timestamped fill from `GET /trades`. Only the fields the backtest reads
/// are kept; the rest (transactionHash, pseudonym, bio, profileImage, …) is
/// ignored.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Trade {
    /// The trader's proxy wallet (the `user` key for `/trades?user=`).
    #[serde(rename = "proxyWallet")]
    pub proxy_wallet: String,
    /// The market this fill belongs to.
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    /// The CLOB token id of the outcome that was traded.
    pub asset: String,
    /// BUY or SELL.
    pub side: TradeSide,
    /// Filled size in shares.
    #[serde(default)]
    pub size: f64,
    /// Fill price in `[0, 1]`.
    #[serde(default)]
    pub price: f64,
    /// Unix epoch SECONDS of the fill.
    #[serde(default)]
    pub timestamp: i64,
    /// Index of the traded outcome within the market (`0` = first, e.g. "Yes").
    #[serde(rename = "outcomeIndex", default)]
    pub outcome_index: i64,
    /// Human market title (used by the sports/non-sports split + logging).
    #[serde(default)]
    pub title: String,
    /// Market slug.
    #[serde(default)]
    pub slug: String,
}

/// One resolved/closed position from `GET /closed-positions` — the shape of
/// `/positions` plus realized fields. Source for the track-record ranking.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClosedPos {
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    /// CLOB token id of the outcome the trader held.
    pub asset: String,
    /// Average entry price the trader paid for this side (`avgPrice`).
    #[serde(rename = "avgPrice", default)]
    pub avg_price: f64,
    #[serde(rename = "outcomeIndex", default)]
    pub outcome_index: i64,
    /// Resolved mark of the held outcome: `≈ 1.0` if it won, `≈ 0.0` if it lost.
    #[serde(rename = "curPrice", default)]
    pub cur_price: f64,
    /// Realized cash P&L on the position (`cashPnl`).
    #[serde(rename = "cashPnl", default)]
    pub cash_pnl: f64,
    #[serde(default)]
    pub size: f64,
    #[serde(default)]
    pub title: String,
}

impl ClosedPos {
    /// Whether the held outcome resolved as the winner. A resolved winner marks
    /// at `curPrice ≈ 1.0` and a loser at `≈ 0.0`, so `0.5` cleanly splits them.
    pub fn won(&self) -> bool {
        self.cur_price >= 0.5
    }
}

/// What a `/trades` query is scoped to: a single trader or a single market.
#[derive(Debug, Clone, Copy)]
pub enum TradesFilter<'a> {
    /// A trader's own fills (`?user=<proxyWallet>`) — the follow signals.
    User(&'a str),
    /// A market's full trade tape (`?market=<conditionId>`) — the entry/exit price.
    Market(&'a str),
}

impl TradesFilter<'_> {
    /// The query-string fragment (`user=…` or `market=…`).
    fn query(&self) -> String {
        match self {
            TradesFilter::User(w) => format!("user={w}"),
            TradesFilter::Market(c) => format!("market={c}"),
        }
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

    /// Up to `limit` timestamped fills for `filter` (a trader or a market), in
    /// the order the API returns them. Pages by `offset` in blocks of
    /// [`TRADES_PAGE`] until a short page (history exhausted) or `limit` is
    /// reached, then truncates to exactly `limit`. A short inter-page sleep keeps
    /// us comfortably under the `/trades` budget (~200 req / 10 s).
    pub async fn trades(
        &self,
        filter: TradesFilter<'_>,
        limit: usize,
    ) -> Result<Vec<Trade>, IngestError> {
        let mut out: Vec<Trade> = Vec::new();
        let mut offset = 0usize;
        while out.len() < limit {
            let url = format!(
                "{}/trades?{}&limit={}&offset={}",
                self.base,
                filter.query(),
                TRADES_PAGE,
                offset,
            );
            let page = parse_trades(&self.get(&url).await?)?;
            let got = page.len();
            out.extend(page);
            // A short (or empty) page means there is no more history to fetch.
            if got < TRADES_PAGE {
                break;
            }
            offset += TRADES_PAGE;
            // Polite throttle between pages.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        out.truncate(limit);
        Ok(out)
    }

    /// All resolved/closed positions for `user` (their realized track record).
    pub async fn closed_positions(&self, user: &str) -> Result<Vec<ClosedPos>, IngestError> {
        let url = format!("{}/closed-positions?user={}", self.base, user);
        let body = self.get(&url).await?;
        parse_closed_positions(&body)
    }

    /// The account's total OPEN-positions value in µUSDC via `/value?user=` — the
    /// "Portfolio" positions figure the Polymarket UI shows (ALL of the wallet's
    /// positions, not just ones our bot opened). This is the positions leg of the
    /// copy strategy's live account equity (paired with the CLOB cash balance).
    /// `Ok(0)` for an empty portfolio; errors only on transport/parse failure.
    pub async fn portfolio_value_micro(&self, user: &str) -> Result<i128, IngestError> {
        let url = format!("{}/value?user={}", self.base, user);
        let body = self.get(&url).await?;
        parse_portfolio_value_micro(&body)
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

/// Parse a `/trades` JSON array (separated for unit-testing without I/O).
pub fn parse_trades(body: &str) -> Result<Vec<Trade>, IngestError> {
    serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("trades: {e}")))
}

/// Parse a `/closed-positions` JSON array (separated for unit-testing without I/O).
pub fn parse_closed_positions(body: &str) -> Result<Vec<ClosedPos>, IngestError> {
    serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("closed-positions: {e}")))
}

/// Parse `/value` (`[{"user":..,"value":<usd float>}]`) to µUSDC (value × 1e6).
/// An empty array (no positions) ⇒ 0. Errors only on unparseable JSON.
pub fn parse_portfolio_value_micro(body: &str) -> Result<i128, IngestError> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| IngestError::Parse(format!("value: {e}")))?;
    let usd = v
        .as_array()
        .and_then(|a| a.first())
        .and_then(|o| o.get("value"))
        .and_then(|x| x.as_f64())
        .unwrap_or(0.0);
    Ok((usd * 1_000_000.0).round() as i128)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn parses_portfolio_value_to_micro() {
        // Real /value shape: an array with one {user, value} object (USD float).
        assert_eq!(
            parse_portfolio_value_micro(r#"[{"user":"0x26fe","value":34.9483}]"#).unwrap(),
            34_948_300
        );
        // Empty portfolio ⇒ 0 (no positions), not an error.
        assert_eq!(parse_portfolio_value_micro("[]").unwrap(), 0);
        // Garbage ⇒ error (caller falls back to static caps / prior equity).
        assert!(parse_portfolio_value_micro("nope").is_err());
    }

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
        // The realized fields (already in the response) now parse too.
        assert!((ps[0].avg_price - 0.3087).abs() < 1e-6);
        assert!((ps[0].cash_pnl - -1179938.4).abs() < 1.0);
        assert!((ps[1].avg_price - 0.42).abs() < 1e-6);
    }

    #[test]
    fn trade_side_parses_buy_sell() {
        let buy: TradeSide = serde_json::from_str("\"BUY\"").unwrap();
        let sell: TradeSide = serde_json::from_str("\"SELL\"").unwrap();
        assert_eq!(buy, TradeSide::Buy);
        assert_eq!(sell, TradeSide::Sell);
        // Lenient on case, strict on meaning: an unknown side errors rather than
        // silently defaulting (a wrong Buy/Sell would corrupt the backtest).
        assert_eq!(
            serde_json::from_str::<TradeSide>("\"buy\"").unwrap(),
            TradeSide::Buy
        );
        assert!(serde_json::from_str::<TradeSide>("\"HODL\"").is_err());
    }

    #[test]
    fn parses_trades_shape() {
        // Trimmed real /trades array: one BUY, one SELL, with the fields the
        // backtest reads (the rest — transactionHash, pseudonym, … — is ignored).
        let body = r#"[
            {"proxyWallet":"0x96cfcb0c30942cfcd1cdf76c7d408794d66b1acb","side":"BUY",
             "asset":"71321045679252212594626385532706912750332728571942532289631379312455583992563",
             "conditionId":"0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
             "size":1500.0,"price":0.62,"timestamp":1718901234,
             "title":"Will BTC hit 100k?","slug":"will-btc-hit-100k","outcome":"Yes",
             "outcomeIndex":0,"transactionHash":"0xabc123"},
            {"proxyWallet":"0xED64A7bf029040aa331ABC87902434d815eF217d","side":"SELL",
             "asset":"52114319501245915516055106046884209969926127482827954674443846427813813222426",
             "conditionId":"0x3774d3f9d68f94e3d3a4f6f5e9b8c7a6b5d4c3b2a1000000000000000000ae61",
             "size":250.5,"price":0.48,"timestamp":1718905678,
             "title":"USA vs PAR draw?","slug":"usa-vs-par-draw","outcome":"No",
             "outcomeIndex":1,"transactionHash":"0xdef456"}
        ]"#;
        let ts = parse_trades(body).unwrap();
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].side, TradeSide::Buy);
        assert!((ts[0].price - 0.62).abs() < 1e-9);
        assert_eq!(ts[0].timestamp, 1718901234);
        assert_eq!(ts[0].outcome_index, 0);
        assert_eq!(
            ts[0].condition_id,
            "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af"
        );
        assert_eq!(ts[0].proxy_wallet, "0x96cfcb0c30942cfcd1cdf76c7d408794d66b1acb");
        assert_eq!(ts[0].slug, "will-btc-hit-100k");
        assert_eq!(ts[1].side, TradeSide::Sell);
        assert!((ts[1].size - 250.5).abs() < 1e-9);
        assert_eq!(ts[1].outcome_index, 1);
        assert_eq!(ts[1].title, "USA vs PAR draw?");
    }

    #[test]
    fn parses_closed_positions_and_won() {
        // Trimmed real /closed-positions array: a resolved WINNER (curPrice~1)
        // and a resolved LOSER (curPrice~0), with realized avgPrice/cashPnl.
        let body = r#"[
            {"proxyWallet":"0xabc","asset":"111","conditionId":"0xWIN",
             "size":1000.0,"avgPrice":0.42,"curPrice":1.0,"cashPnl":580.0,
             "title":"Won market","outcome":"Yes","outcomeIndex":0},
            {"proxyWallet":"0xabc","asset":"222","conditionId":"0xLOSE",
             "size":500.0,"avgPrice":0.31,"curPrice":0.0,"cashPnl":-155.0,
             "title":"Lost market","outcome":"No","outcomeIndex":1}
        ]"#;
        let cps = parse_closed_positions(body).unwrap();
        assert_eq!(cps.len(), 2);
        // Winner: curPrice ≈ 1.0.
        assert!((cps[0].avg_price - 0.42).abs() < 1e-9);
        assert!((cps[0].cur_price - 1.0).abs() < 1e-9);
        assert!((cps[0].cash_pnl - 580.0).abs() < 1e-9);
        assert_eq!(cps[0].condition_id, "0xWIN");
        assert_eq!(cps[0].outcome_index, 0);
        assert!(cps[0].won(), "curPrice≈1 → winner");
        // Loser: curPrice ≈ 0.0.
        assert!((cps[1].cur_price - 0.0).abs() < 1e-9);
        assert!(!cps[1].won(), "curPrice≈0 → loser");
    }

    #[test]
    fn order_and_period_strings() {
        assert_eq!(OrderBy::Pnl.as_str(), "PNL");
        assert_eq!(OrderBy::Vol.as_str(), "VOL");
        assert_eq!(TimePeriod::Month.as_str(), "MONTH");
        assert_eq!(TimePeriod::All.as_str(), "ALL");
    }
}
