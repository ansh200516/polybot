//! Market segmentation (Phase 5, Task 5.1).
//!
//! Classifies each market into a [`MarketSegment`] from its static Gamma
//! liquidity metrics ([`MarketMetrics`]) so that Phase 5.2 can route strategies
//! per segment (arb runs on every market; the market maker only quotes the
//! liquid segments).
//!
//! # Design
//!
//! - [`classify`] is a **pure** function of `(metrics, thresholds)` — no I/O, no
//!   registry state — so it is trivially testable and deterministic.
//! - Metrics are **optional**: not every Gamma market reports volume/liquidity
//!   (resolved/closed markets often omit them). A missing metric is treated as
//!   `0`, so an unknown-liquidity market falls to [`MarketSegment::Illiquid`]
//!   and is therefore NOT eligible for market making — the conservative default.
//! - This is **opt-in / forward-only**: producing a classification changes no
//!   existing gating, capping, or ordering. Defaults are tuned so nothing is
//!   forced into a new segment unless an operator wires the routing in 5.2.
//!
//! # Future refinement (NOT in v1)
//!
//! A runtime price-volatility / spread-stability signal (e.g. mid stddev from
//! the live books) would sharpen the "stable" distinction. That is deliberately
//! out of scope here: v1 uses only the static volume + liquidity that Gamma
//! already publishes. The thresholds live in config so they can be tuned later.

use std::collections::HashMap;

use pm_core::instrument::MarketId;

use crate::Registry;

// ---------------------------------------------------------------------------
// Per-market metrics
// ---------------------------------------------------------------------------

/// Static, per-market liquidity metrics captured from Gamma (Task 5.1, §A).
///
/// All fields are optional because not every market reports them (the venue
/// omits `liquidity`/`volume24hr` on resolved/closed markets). Captured into the
/// [`Registry`] alongside `question`; consumed by [`classify`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MarketMetrics {
    /// Lifetime traded volume, USD.
    pub volume: Option<f64>,
    /// Trailing-24h traded volume, USD.
    pub volume_24hr: Option<f64>,
    /// Resting book liquidity, USD.
    pub liquidity: Option<f64>,
    /// Optional category / tag signal (today always `None`: the Gamma fixtures
    /// carry no explicit category — see `gamma::GammaMarket::category`). Exposed
    /// so 5.2 routing can branch on it if a future feed provides it.
    pub category: Option<String>,
}

// ---------------------------------------------------------------------------
// Segment
// ---------------------------------------------------------------------------

/// Liquidity tier a market is classified into (Task 5.1, §B).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketSegment {
    /// Deep, high-turnover market: clears the HIGH volume AND liquidity bars.
    /// Eligible for the most aggressive routing (e.g. MM with full size).
    LiquidStable,
    /// Tradable market: clears the LOW volume AND liquidity bars but not the
    /// high ones. Eligible for market making.
    Liquid,
    /// Thin or unknown-liquidity market: below the low bar on volume and/or
    /// liquidity, or missing the metric entirely. Arb only — never quoted by MM.
    Illiquid,
}

// ---------------------------------------------------------------------------
// Thresholds
// ---------------------------------------------------------------------------

/// Cutoffs for [`classify`] (Task 5.1, §B). Bounds are **inclusive**: a metric
/// exactly equal to a threshold clears that bar (`metric >= threshold`).
///
/// Defaults (USD), chosen as reasonable starting points to be tuned later:
/// - `LiquidStable`: volume ≥ 100_000 AND liquidity ≥ 50_000
/// - `Liquid`:       volume ≥ 10_000  AND liquidity ≥ 5_000
/// - otherwise → `Illiquid`
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentThresholds {
    /// Minimum lifetime volume (USD) for [`MarketSegment::LiquidStable`].
    pub liquid_stable_min_volume: f64,
    /// Minimum resting liquidity (USD) for [`MarketSegment::LiquidStable`].
    pub liquid_stable_min_liquidity: f64,
    /// Minimum lifetime volume (USD) for [`MarketSegment::Liquid`].
    pub liquid_min_volume: f64,
    /// Minimum resting liquidity (USD) for [`MarketSegment::Liquid`].
    pub liquid_min_liquidity: f64,
}

impl Default for SegmentThresholds {
    fn default() -> Self {
        SegmentThresholds {
            liquid_stable_min_volume: 100_000.0,
            liquid_stable_min_liquidity: 50_000.0,
            liquid_min_volume: 10_000.0,
            liquid_min_liquidity: 5_000.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Classifier (pure)
// ---------------------------------------------------------------------------

/// Classify a market from its metrics under `t`. PURE.
///
/// Both volume AND liquidity must clear a tier's bars (a market that is deep on
/// one axis but thin on the other does not qualify for that tier). Bars are
/// inclusive (`>=`). A missing metric (`None`) is treated as `0`, so an
/// unknown-liquidity market classifies as [`MarketSegment::Illiquid`] — the
/// conservative choice, since it must not become MM-eligible by default.
pub fn classify(metrics: &MarketMetrics, t: &SegmentThresholds) -> MarketSegment {
    let volume = metrics.volume.unwrap_or(0.0);
    let liquidity = metrics.liquidity.unwrap_or(0.0);

    if volume >= t.liquid_stable_min_volume && liquidity >= t.liquid_stable_min_liquidity {
        MarketSegment::LiquidStable
    } else if volume >= t.liquid_min_volume && liquidity >= t.liquid_min_liquidity {
        MarketSegment::Liquid
    } else {
        MarketSegment::Illiquid
    }
}

/// Classify every market in `reg` under `t`, returning a `MarketId → segment`
/// map. Convenience for 5.2 wiring; equivalent to calling [`Registry::segment`]
/// per market. A market with no captured metrics classifies as
/// [`MarketSegment::Illiquid`] (via the `None`-as-0 rule in [`classify`]).
pub fn classify_registry(
    reg: &Registry,
    t: &SegmentThresholds,
) -> HashMap<MarketId, MarketSegment> {
    reg.markets()
        .iter()
        .map(|m| (m.id, reg.segment(m.id, t)))
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn metrics(volume: Option<f64>, liquidity: Option<f64>) -> MarketMetrics {
        MarketMetrics {
            volume,
            liquidity,
            volume_24hr: None,
            category: None,
        }
    }

    #[test]
    fn high_volume_and_liquidity_is_liquid_stable() {
        let t = SegmentThresholds::default();
        let m = metrics(Some(500_000.0), Some(250_000.0));
        assert_eq!(classify(&m, &t), MarketSegment::LiquidStable);
    }

    #[test]
    fn mid_volume_and_liquidity_is_liquid() {
        let t = SegmentThresholds::default();
        // Above the low bar, below the high bar on both axes.
        let m = metrics(Some(50_000.0), Some(20_000.0));
        assert_eq!(classify(&m, &t), MarketSegment::Liquid);
    }

    #[test]
    fn thin_market_is_illiquid() {
        let t = SegmentThresholds::default();
        let m = metrics(Some(100.0), Some(50.0));
        assert_eq!(classify(&m, &t), MarketSegment::Illiquid);
    }

    #[test]
    fn one_axis_thin_does_not_qualify() {
        let t = SegmentThresholds::default();
        // Huge volume but no liquidity → cannot be LiquidStable or Liquid.
        let deep_vol_thin_liq = metrics(Some(10_000_000.0), Some(10.0));
        assert_eq!(classify(&deep_vol_thin_liq, &t), MarketSegment::Illiquid);
        // Deep book but ~no turnover → also Illiquid.
        let thin_vol_deep_liq = metrics(Some(10.0), Some(10_000_000.0));
        assert_eq!(classify(&thin_vol_deep_liq, &t), MarketSegment::Illiquid);
    }

    #[test]
    fn missing_metrics_are_illiquid() {
        let t = SegmentThresholds::default();
        // Fully unknown → Illiquid.
        assert_eq!(classify(&metrics(None, None), &t), MarketSegment::Illiquid);
        // Known huge volume but UNKNOWN liquidity → Illiquid (conservative: an
        // unknown-liquidity market must not be MM-eligible).
        assert_eq!(
            classify(&metrics(Some(10_000_000.0), None), &t),
            MarketSegment::Illiquid
        );
    }

    #[test]
    fn exact_threshold_is_inclusive() {
        let t = SegmentThresholds::default();
        // Exactly on the HIGH bar on both axes → LiquidStable (inclusive `>=`).
        let on_high = metrics(
            Some(t.liquid_stable_min_volume),
            Some(t.liquid_stable_min_liquidity),
        );
        assert_eq!(classify(&on_high, &t), MarketSegment::LiquidStable);

        // Exactly on the LOW bar → Liquid.
        let on_low = metrics(Some(t.liquid_min_volume), Some(t.liquid_min_liquidity));
        assert_eq!(classify(&on_low, &t), MarketSegment::Liquid);

        // One cent below the low bar on volume → drops to Illiquid.
        let just_below = metrics(
            Some(t.liquid_min_volume - 0.01),
            Some(t.liquid_min_liquidity),
        );
        assert_eq!(classify(&just_below, &t), MarketSegment::Illiquid);
    }

    #[test]
    fn custom_thresholds_are_respected() {
        let t = SegmentThresholds {
            liquid_stable_min_volume: 1_000.0,
            liquid_stable_min_liquidity: 1_000.0,
            liquid_min_volume: 100.0,
            liquid_min_liquidity: 100.0,
        };
        let m = metrics(Some(1_500.0), Some(1_200.0));
        assert_eq!(classify(&m, &t), MarketSegment::LiquidStable);
    }
}
