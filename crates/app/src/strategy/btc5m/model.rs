//! Pure fair-value math for the BTC 5m binary. Driftless-normal digital:
//! `p_up = Φ((spot − strike) / σ_τ)`, σ from a causal EWMA of 1-minute $-returns
//! (√-time to the remaining horizon). No I/O, no lookahead — callers pass only
//! data available at decision time.

use pm_core::num::{Px, TickSize};

/// Standard normal CDF (Abramowitz & Stegun 26.2.17; |error| < 7.5e-8).
pub fn norm_cdf(x: f64) -> f64 {
    const B0: f64 = 0.231_641_9;
    const B: [f64; 5] = [0.319_381_530, -0.356_563_782, 1.781_477_937, -1.821_255_978, 1.330_274_429];
    let t = 1.0 / (1.0 + B0 * x.abs());
    let pdf = (-x * x / 2.0).exp() / (2.0 * std::f64::consts::PI).sqrt();
    let poly = t * (B[0] + t * (B[1] + t * (B[2] + t * (B[3] + t * B[4]))));
    let upper_tail = pdf * poly; // ≈ 1 − Φ(|x|)
    if x >= 0.0 { 1.0 - upper_tail } else { upper_tail }
}

/// Causal EWMA of 1-minute squared $-returns → per-1-minute $ volatility.
#[derive(Debug, Clone)]
pub struct EwmaVol { lambda: f64, var: f64, samples: u32, warmup: u32 }

impl EwmaVol {
    pub fn new(half_life_min: f64, warmup: u32) -> Self {
        EwmaVol { lambda: 0.5f64.powf(1.0 / half_life_min), var: 0.0, samples: 0, warmup }
    }
    pub fn update(&mut self, ret_usd: f64) {
        if !ret_usd.is_finite() { return; }
        let sq = ret_usd * ret_usd;
        self.var = if self.samples == 0 { sq } else { self.lambda * self.var + (1.0 - self.lambda) * sq };
        self.samples = self.samples.saturating_add(1);
    }
    pub fn ready(&self) -> bool { self.samples >= self.warmup }
    pub fn sigma_1min(&self) -> f64 { self.var.sqrt() }
    pub fn sigma_tau(&self, tau_secs: f64) -> f64 { self.sigma_1min() * (tau_secs / 60.0).sqrt() }
}

/// Fair P(up) for a driftless normal digital. Ties (spot == strike) and τ ≤ 0 → UP.
pub fn fair_p_up(spot: f64, strike: f64, tau_secs: f64, sigma_1min: f64) -> Option<f64> {
    if !(spot.is_finite() && strike.is_finite() && tau_secs.is_finite() && sigma_1min.is_finite()) { return None; }
    if tau_secs <= 0.0 { return Some(if spot >= strike { 1.0 } else { 0.0 }); }
    let sigma_tau = sigma_1min * (tau_secs / 60.0).sqrt();
    if sigma_tau <= 0.0 { return Some(if spot >= strike { 1.0 } else { 0.0 }); }
    Some(norm_cdf((spot - strike) / sigma_tau))
}

/// Snap a probability in (0,1) to the market's tick as a `Px`. Rounds to nearest
/// tick; `None` for degenerate probs that land on 0 or the top.
pub fn snap_prob_to_px(p: f64, ts: TickSize) -> Option<Px> {
    if !p.is_finite() || p <= 0.0 || p >= 1.0 { return None; }
    let unit = ts.unit_microusdc() as f64;
    let micro = (p * 1_000_000.0).round();
    let ticks = (micro / unit).round() as i64;
    if ticks < 1 || ticks >= i64::from(ts.levels()) { return None; }
    Px::new(ticks as u16, ts).ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool { (a - b).abs() < tol }

    #[test]
    fn norm_cdf_known_values() {
        assert!(approx(norm_cdf(0.0), 0.5, 1e-9));
        assert!(approx(norm_cdf(1.0), 0.8413447, 1e-6));
        assert!(approx(norm_cdf(-1.0), 0.1586553, 1e-6));
        assert!(approx(norm_cdf(1.96), 0.9750021, 1e-6));
        assert!(approx(norm_cdf(-1.96), 0.0249979, 1e-6));
    }

    #[test]
    fn ewma_vol_warms_up_and_scales_sqrt_time() {
        let mut v = EwmaVol::new(120.0, 3);
        assert!(!v.ready());
        for r in [40.0, -30.0, 50.0] { v.update(r); }
        assert!(v.ready());
        let s1 = v.sigma_1min();
        assert!(s1 > 0.0);
        assert!(approx(v.sigma_tau(300.0), s1 * 5f64.sqrt(), 1e-9));
        assert!(approx(v.sigma_tau(240.0) / v.sigma_tau(60.0), 2.0, 1e-9));
    }

    #[test]
    fn fair_p_up_is_half_at_strike_and_monotone() {
        assert!(approx(fair_p_up(100_000.0, 100_000.0, 60.0, 42.0).unwrap(), 0.5, 1e-9));
        let sigma_1min = 42.0;
        let sigma_tau = sigma_1min * (15.0f64 / 60.0).sqrt();
        let up = fair_p_up(100_000.0 + sigma_tau, 100_000.0, 15.0, sigma_1min).unwrap();
        assert!(approx(up, norm_cdf(1.0), 1e-9));
        assert_eq!(fair_p_up(100_000.0, 100_000.0, 0.0, 42.0).unwrap(), 1.0);
        assert_eq!(fair_p_up(99_999.0, 100_000.0, 0.0, 42.0).unwrap(), 0.0);
        assert!(fair_p_up(100_000.0, 100_000.0, f64::NAN, 42.0).is_none());
        assert!(fair_p_up(100_000.0, 100_000.0, f64::INFINITY, 42.0).is_none());
    }

    #[test]
    fn snap_prob_to_px_rounds_to_tick_and_guards_extremes() {
        let px = snap_prob_to_px(0.564, TickSize::Cent).unwrap();
        assert_eq!(px.get(), 56);
        assert_eq!(snap_prob_to_px(0.5643, TickSize::Milli).unwrap().get(), 564);
        assert!(snap_prob_to_px(0.0, TickSize::Cent).is_none());
        assert!(snap_prob_to_px(1.0, TickSize::Cent).is_none());
        assert!(snap_prob_to_px(0.999_9, TickSize::Cent).is_none());
    }
}
