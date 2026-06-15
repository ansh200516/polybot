//! Strategy identity, per-strategy capital envelope, and the startup capital
//! allocator (multi-strategy platform, Task 1.2). The allocator is a pure
//! startup guard: Σ per-strategy capital must not exceed the bankroll. No
//! `StrategyHost` and no strategy trait yet — those arrive in later tasks.

use pm_core::num::Usdc;
use pm_risk::RiskConfig;

/// Stable identity for a strategy (e.g. `"arb"`, `"mm"`). A `&'static str`
/// keeps it copyable and cheap to use as a label/map key.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct StrategyId(pub &'static str);

/// One strategy's slice of the platform: its identity, the capital carved out
/// for it, and the risk envelope it runs under.
#[derive(Debug, Clone)]
pub struct StrategyEnvelope {
    pub id: StrategyId,
    pub capital: Usdc,
    pub risk: RiskConfig,
}

impl StrategyEnvelope {
    pub fn new(id: StrategyId, capital: Usdc, risk: RiskConfig) -> Self {
        StrategyEnvelope { id, capital, risk }
    }
}

/// Startup guard: the sum of per-strategy capital must not exceed `bankroll`.
/// Sums in i128 (the `Usdc` width) so a long list of envelopes can't overflow
/// a narrower accumulator. Returns `Err` naming the total vs the bankroll when
/// over-allocated, else `Ok(())`.
pub fn allocate(envs: &[StrategyEnvelope], bankroll: Usdc) -> Result<(), String> {
    let total: i128 = envs.iter().map(|e| e.capital.0).sum();
    if total > bankroll.0 {
        return Err(format!(
            "capital over-allocation: strategies sum to {total} µUSDC, exceeding bankroll {} µUSDC",
            bankroll.0
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn arb_risk() -> RiskConfig {
        crate::wiring::risk_config(&pm_config::Config::default(), None).unwrap()
    }

    #[test]
    fn allocator_rejects_overallocation() {
        let envs = vec![
            StrategyEnvelope::new(StrategyId("arb"), Usdc(6_000_000), arb_risk()),
            StrategyEnvelope::new(StrategyId("mm"), Usdc(5_000_000), arb_risk()),
        ];
        assert!(allocate(&envs, Usdc(10_000_000)).is_err()); // 6+5 > 10
        assert!(allocate(&envs, Usdc(11_000_000)).is_ok());
    }
}
