//! Typed configuration skeleton (spec §18). Defaults are the spec §2 locked
//! values. Secrets never live here — env vars only (M3+).

use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub capital: Capital,
    pub edges: Edges,
    pub gas: Gas,
    pub lp: Lp,
    pub dedup: Dedup,
    pub mode: Mode,
    pub endpoints: Endpoints,
    pub universe: Universe,
    pub ingestion: Ingestion,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Capital {
    pub bankroll_usd: f64,
    pub per_market_usd: f64,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Edges {
    pub min_edge_class12_bps: i32,
    pub min_edge_class3_bps: i32,
    pub min_profit_usd: f64,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Gas {
    pub split_microusdc: u64,
    pub merge_microusdc: u64,
    pub redeem_microusdc: u64,
    pub negrisk_convert_microusdc: u64,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Lp {
    pub max_worlds: usize,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Dedup {
    pub cooldown_ms: u64,
    pub reemit_improvement_pct: u32,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Mode {
    pub paper: bool,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Endpoints {
    pub gamma_base: String,
    pub clob_base: String,
    pub ws_market_url: String,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Universe {
    pub max_markets: usize,
    pub require_active: bool,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Ingestion {
    pub staleness_ms: u64,
    pub ws_chunk_size: usize,
    pub resync_interval_s: u64,
    pub sweep_interval_ms: u64,
    pub rest_rate_capacity: u32,
    pub rest_rate_per_sec: f64,
    pub backoff_base_ms: u64,
    pub backoff_cap_ms: u64,
    pub relationships_path: String,
}

#[derive(Debug, PartialEq)]
pub enum ConfigError {
    Parse(String),
    BadMoney(&'static str),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Parse(s) => write!(f, "config parse error: {s}"),
            ConfigError::BadMoney(s) => write!(f, "bad money value: {s}"),
        }
    }
}
impl std::error::Error for ConfigError {}

impl Default for Capital {
    fn default() -> Self {
        Capital {
            bankroll_usd: 10_000.0,
            per_market_usd: 1_000.0,
        }
    }
}
impl Default for Edges {
    fn default() -> Self {
        Edges {
            min_edge_class12_bps: 30,
            min_edge_class3_bps: 100,
            min_profit_usd: 1.0,
        }
    }
}
impl Default for Gas {
    fn default() -> Self {
        Gas {
            split_microusdc: 10_000,
            merge_microusdc: 10_000,
            redeem_microusdc: 15_000,
            negrisk_convert_microusdc: 20_000,
        }
    }
}
impl Default for Lp {
    fn default() -> Self {
        Lp { max_worlds: 4096 }
    }
}
impl Default for Dedup {
    fn default() -> Self {
        Dedup {
            cooldown_ms: 2000,
            reemit_improvement_pct: 20,
        }
    }
}
impl Default for Mode {
    fn default() -> Self {
        Mode { paper: true }
    }
}
impl Default for Endpoints {
    fn default() -> Self {
        Endpoints {
            gamma_base: "https://gamma-api.polymarket.com".to_string(),
            clob_base: "https://clob.polymarket.com".to_string(),
            ws_market_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
        }
    }
}
impl Default for Universe {
    fn default() -> Self {
        Universe {
            max_markets: 200,
            require_active: true,
        }
    }
}
impl Default for Ingestion {
    fn default() -> Self {
        Ingestion {
            staleness_ms: 30_000,
            ws_chunk_size: 50,
            resync_interval_s: 300,
            sweep_interval_ms: 1000,
            rest_rate_capacity: 10,
            rest_rate_per_sec: 5.0,
            backoff_base_ms: 250,
            backoff_cap_ms: 30_000,
            relationships_path: "relationships.toml".to_string(),
        }
    }
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Sanity checks beyond shape: positive capital, per-market ≤ bankroll,
    /// percentage domains.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.capital.bankroll_usd <= 0.0 || !self.capital.bankroll_usd.is_finite() {
            return Err(ConfigError::BadMoney("bankroll_usd must be > 0"));
        }
        if self.capital.per_market_usd <= 0.0
            || !self.capital.per_market_usd.is_finite()
            || self.capital.per_market_usd > self.capital.bankroll_usd
        {
            return Err(ConfigError::BadMoney(
                "per_market_usd must be in (0, bankroll]",
            ));
        }
        if self.dedup.reemit_improvement_pct > 100 {
            return Err(ConfigError::BadMoney(
                "reemit_improvement_pct must be ≤ 100",
            ));
        }
        // Ingestion validation
        if self.ingestion.staleness_ms < 100 {
            return Err(ConfigError::BadMoney("staleness_ms must be ≥ 100"));
        }
        if self.ingestion.ws_chunk_size < 1 {
            return Err(ConfigError::BadMoney("ws_chunk_size must be ≥ 1"));
        }
        if self.ingestion.rest_rate_per_sec <= 0.0 {
            return Err(ConfigError::BadMoney("rest_rate_per_sec must be > 0.0"));
        }
        if self.ingestion.rest_rate_capacity < 1 {
            return Err(ConfigError::BadMoney("rest_rate_capacity must be ≥ 1"));
        }
        if self.ingestion.backoff_base_ms > self.ingestion.backoff_cap_ms {
            return Err(ConfigError::BadMoney("backoff_base_ms must be ≤ backoff_cap_ms"));
        }
        // Endpoints validation
        if self.endpoints.gamma_base.is_empty() {
            return Err(ConfigError::BadMoney("gamma_base must be non-empty"));
        }
        if !self.endpoints.gamma_base.starts_with("https://") {
            return Err(ConfigError::BadMoney("gamma_base must start with https://"));
        }
        if self.endpoints.clob_base.is_empty() {
            return Err(ConfigError::BadMoney("clob_base must be non-empty"));
        }
        if !self.endpoints.clob_base.starts_with("https://") {
            return Err(ConfigError::BadMoney("clob_base must start with https://"));
        }
        if self.endpoints.ws_market_url.is_empty() {
            return Err(ConfigError::BadMoney("ws_market_url must be non-empty"));
        }
        if !self.endpoints.ws_market_url.starts_with("wss://") {
            return Err(ConfigError::BadMoney("ws_market_url must start with wss://"));
        }
        if self.ingestion.relationships_path.is_empty() {
            return Err(ConfigError::BadMoney("relationships_path must be non-empty"));
        }
        Ok(())
    }
}

/// Checked dollars → µUSDC (round-to-nearest). Rejects NaN/∞/negative/overflow.
pub fn usd_to_microusdc(usd: f64) -> Result<i128, ConfigError> {
    if !usd.is_finite() || !(0.0..=1e18).contains(&usd) {
        return Err(ConfigError::BadMoney("must be finite, non-negative, sane"));
    }
    Ok((usd * 1e6).round() as i128)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::float_cmp)]
    use super::*;

    #[test]
    fn defaults_are_the_locked_values() {
        let c = Config::default();
        assert_eq!(c.capital.bankroll_usd, 10_000.0);
        assert_eq!(c.capital.per_market_usd, 1_000.0);
        assert_eq!(c.edges.min_edge_class12_bps, 30);
        assert_eq!(c.edges.min_edge_class3_bps, 100);
        assert_eq!(c.edges.min_profit_usd, 1.0);
        assert_eq!(c.gas.split_microusdc, 10_000);
        assert_eq!(c.gas.merge_microusdc, 10_000);
        assert_eq!(c.gas.redeem_microusdc, 15_000);
        assert_eq!(c.gas.negrisk_convert_microusdc, 20_000);
        assert_eq!(c.lp.max_worlds, 4096);
        assert_eq!(c.dedup.cooldown_ms, 2000);
        assert_eq!(c.dedup.reemit_improvement_pct, 20);
        assert!(c.mode.paper);
        assert_eq!(c.endpoints.gamma_base, "https://gamma-api.polymarket.com");
        assert_eq!(c.endpoints.clob_base, "https://clob.polymarket.com");
        assert_eq!(c.endpoints.ws_market_url, "wss://ws-subscriptions-clob.polymarket.com/ws/market");
        assert_eq!(c.universe.max_markets, 200);
        assert!(c.universe.require_active);
        assert_eq!(c.ingestion.staleness_ms, 30_000);
        assert_eq!(c.ingestion.ws_chunk_size, 50);
        assert_eq!(c.ingestion.resync_interval_s, 300);
        assert_eq!(c.ingestion.sweep_interval_ms, 1000);
        assert_eq!(c.ingestion.rest_rate_capacity, 10);
        assert_eq!(c.ingestion.rest_rate_per_sec, 5.0);
        assert_eq!(c.ingestion.backoff_base_ms, 250);
        assert_eq!(c.ingestion.backoff_cap_ms, 30_000);
        assert_eq!(c.ingestion.relationships_path, "relationships.toml");
    }

    #[test]
    fn empty_toml_is_all_defaults() {
        assert_eq!(Config::from_toml_str("").unwrap(), Config::default());
    }

    #[test]
    fn partial_override_parses() {
        // bankroll override must be ≥ per_market default (1_000); use a value that passes.
        let c = Config::from_toml_str("[capital]\nbankroll_usd = 5000.0\n").unwrap();
        assert_eq!(c.capital.bankroll_usd, 5000.0);
        assert_eq!(c.capital.per_market_usd, 1_000.0);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(Config::from_toml_str("[capital]\nbankrol = 1.0\n").is_err());
        assert!(Config::from_toml_str("[typo_section]\nx = 1\n").is_err());
    }

    #[test]
    fn validate_rejects_insane_values() {
        assert!(Config::from_toml_str("[capital]\nbankroll_usd = -5.0\n").is_err());
        assert!(Config::from_toml_str("[capital]\nper_market_usd = 99999.0\n").is_err());
        assert!(Config::from_toml_str("[dedup]\nreemit_improvement_pct = 150\n").is_err());
    }

    #[test]
    fn money_conversion_is_checked() {
        assert_eq!(usd_to_microusdc(10_000.0).unwrap(), 10_000_000_000);
        assert_eq!(usd_to_microusdc(0.000001).unwrap(), 1);
        assert!(usd_to_microusdc(-1.0).is_err());
        assert!(usd_to_microusdc(f64::NAN).is_err());
        assert!(usd_to_microusdc(f64::INFINITY).is_err());
    }

    #[test]
    fn endpoints_override_parses() {
        let c = Config::from_toml_str("[endpoints]\ngamma_base = \"https://custom-gamma.com\"\n").unwrap();
        assert_eq!(c.endpoints.gamma_base, "https://custom-gamma.com");
        assert_eq!(c.endpoints.clob_base, "https://clob.polymarket.com");
        assert_eq!(c.endpoints.ws_market_url, "wss://ws-subscriptions-clob.polymarket.com/ws/market");
    }

    #[test]
    fn universe_override_parses() {
        let c = Config::from_toml_str("[universe]\nmax_markets = 100\n").unwrap();
        assert_eq!(c.universe.max_markets, 100);
        assert!(c.universe.require_active);
    }

    #[test]
    fn ingestion_override_parses() {
        let c = Config::from_toml_str("[ingestion]\nstaleness_ms = 2000\n").unwrap();
        assert_eq!(c.ingestion.staleness_ms, 2000);
        assert_eq!(c.ingestion.ws_chunk_size, 50);
    }

    #[test]
    fn validate_rejects_bad_ingestion_values() {
        // staleness_ms < 100
        assert!(Config::from_toml_str("[ingestion]\nstaleness_ms = 50\n").is_err());
        // ws_chunk_size < 1
        assert!(Config::from_toml_str("[ingestion]\nws_chunk_size = 0\n").is_err());
        // rest_rate_per_sec <= 0.0
        assert!(Config::from_toml_str("[ingestion]\nrest_rate_per_sec = 0.0\n").is_err());
        // rest_rate_capacity < 1
        assert!(Config::from_toml_str("[ingestion]\nrest_rate_capacity = 0\n").is_err());
        // backoff_base_ms > backoff_cap_ms
        assert!(Config::from_toml_str("[ingestion]\nbackoff_base_ms = 40000\nbackoff_cap_ms = 30000\n").is_err());
    }

    #[test]
    fn validate_rejects_bad_endpoint_urls() {
        // gamma_base empty
        assert!(Config::from_toml_str("[endpoints]\ngamma_base = \"\"\n").is_err());
        // gamma_base with http:// instead of https://
        assert!(Config::from_toml_str("[endpoints]\ngamma_base = \"http://gamma.com\"\n").is_err());
        // clob_base empty
        assert!(Config::from_toml_str("[endpoints]\nclob_base = \"\"\n").is_err());
        // clob_base with http:// instead of https://
        assert!(Config::from_toml_str("[endpoints]\nclob_base = \"http://clob.com\"\n").is_err());
        // ws_market_url empty
        assert!(Config::from_toml_str("[endpoints]\nws_market_url = \"\"\n").is_err());
        // ws_market_url with http:// instead of wss://
        assert!(Config::from_toml_str("[endpoints]\nws_market_url = \"http://ws.com\"\n").is_err());
    }

    #[test]
    fn validate_rejects_empty_relationships_path() {
        assert!(Config::from_toml_str("[ingestion]\nrelationships_path = \"\"\n").is_err());
    }
}
