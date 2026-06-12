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

#[derive(Debug, PartialEq)]
pub enum ConfigError {
    Parse(String),
    BadMoney(&'static str),
}

impl Default for Capital {
    fn default() -> Self {
        Capital { bankroll_usd: 10_000.0, per_market_usd: 1_000.0 }
    }
}
impl Default for Edges {
    fn default() -> Self {
        Edges { min_edge_class12_bps: 30, min_edge_class3_bps: 100, min_profit_usd: 1.0 }
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
        Dedup { cooldown_ms: 2000, reemit_improvement_pct: 20 }
    }
}
impl Default for Mode {
    fn default() -> Self {
        Mode { paper: true }
    }
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))
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
    }

    #[test]
    fn empty_toml_is_all_defaults() {
        assert_eq!(Config::from_toml_str("").unwrap(), Config::default());
    }

    #[test]
    fn partial_override_parses() {
        let c = Config::from_toml_str("[capital]\nbankroll_usd = 500.0\n").unwrap();
        assert_eq!(c.capital.bankroll_usd, 500.0);
        assert_eq!(c.capital.per_market_usd, 1_000.0);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(Config::from_toml_str("[capital]\nbankrol = 1.0\n").is_err());
        assert!(Config::from_toml_str("[typo_section]\nx = 1\n").is_err());
    }

    #[test]
    fn money_conversion_is_checked() {
        assert_eq!(usd_to_microusdc(10_000.0).unwrap(), 10_000_000_000);
        assert_eq!(usd_to_microusdc(0.000001).unwrap(), 1);
        assert!(usd_to_microusdc(-1.0).is_err());
        assert!(usd_to_microusdc(f64::NAN).is_err());
        assert!(usd_to_microusdc(f64::INFINITY).is_err());
    }
}
