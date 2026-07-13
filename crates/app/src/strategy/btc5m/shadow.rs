//! The shadow sample the loop records each tick and its mapping to a store row.
use pm_store::Btc5mShadowRow;

/// A single (fair vs book) observation, pre-persistence.
#[derive(Debug, Clone, PartialEq)]
pub struct ShadowSample {
    pub ts_ms: i64,
    pub condition_id: String,
    pub secs_to_go: i64,
    pub strike: f64,
    pub spot: f64,
    pub sigma_tau: f64,
    pub p_up: f64,
    pub best_bid_micro: i64,
    pub best_ask_micro: i64,
    pub tick_decimals: i64,
}

impl ShadowSample {
    pub fn into_row(self) -> Btc5mShadowRow {
        Btc5mShadowRow {
            ts_ms: self.ts_ms,
            condition_id: self.condition_id,
            secs_to_go: self.secs_to_go,
            strike: self.strike,
            spot: self.spot,
            sigma_tau: self.sigma_tau,
            p_up: self.p_up,
            best_bid_micro: self.best_bid_micro,
            best_ask_micro: self.best_ask_micro,
            tick_decimals: self.tick_decimals,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    #[test]
    fn sample_maps_to_row() {
        let s = ShadowSample {
            ts_ms: 1,
            condition_id: "c".into(),
            secs_to_go: 15,
            strike: 1.0,
            spot: 2.0,
            sigma_tau: 3.0,
            p_up: 0.6,
            best_bid_micro: 550_000,
            best_ask_micro: 560_000,
            tick_decimals: 2,
        };
        let r = s.clone().into_row();
        assert_eq!(r.condition_id, "c");
        assert_eq!(r.best_ask_micro, 560_000);
        assert_eq!(r.p_up, 0.6);
    }
}
