//! Pure risk engine (spec §15): caps, halts, kill switch. No I/O, no async,
//! no engine types — callers convert baskets to plain `BasketCheck` data.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use pm_core::instrument::MarketId;
use pm_core::num::Usdc;

#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub bankroll: Usdc,
    pub per_market_cap: Usdc,
    pub max_unhedged: Usdc,
    pub max_open_orders: usize,
    pub max_basket_legs: usize,
    /// Drawdown halt threshold in bps of bankroll (200 = 2%), peak-to-trough on session equity.
    pub daily_drawdown_bps: i128,
    pub error_halt_count: u32,
    pub error_halt_window: Duration,
    /// Threshold only — the storm WINDOW is applied by the store's session-start
    /// query; see note_session_starts_in_window.
    pub restart_storm_count: u32,
}

/// Plain-data view of a basket for risk checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasketCheck {
    /// Worst-case cash out for the whole basket (cost basis incl. fees/gas/splits).
    pub total_cost: Usdc,
    /// Largest single-leg cash out — the worst-case unhedged exposure if only
    /// that leg fills (spec §14 "enforced before submission").
    pub max_leg_cost: Usdc,
    pub legs: usize,
    pub per_market: Vec<(MarketId, Usdc)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaltReason {
    DailyDrawdown,
    ConsecutiveErrors,
    RestartStorm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    Bankroll,
    PerMarketCap,
    MaxOpenOrders,
    MaxBasketLegs,
    Unhedged,
    Paused,
    Halted(HaltReason),
    KillSwitch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskVerdict {
    Approved,
    Rejected(RejectReason),
}

pub struct RiskEngine {
    cfg: RiskConfig,
    global_exposure: i128,
    per_market: HashMap<MarketId, i128>,
    open_orders: usize,
    paused: bool,
    halted: Option<HaltReason>,
    killed: bool,
    peak_equity: i128,
    errors: VecDeque<Instant>,
}

impl RiskEngine {
    pub fn new(cfg: RiskConfig) -> Self {
        RiskEngine {
            cfg,
            global_exposure: 0,
            per_market: HashMap::new(),
            open_orders: 0,
            paused: false,
            halted: None,
            killed: false,
            peak_equity: 0,
            errors: VecDeque::new(),
        }
    }

    /// Synchronous pre-submit check (spec §15). Order matters: kill/halt/pause
    /// dominate; structural caps (MaxBasketLegs, MaxOpenOrders, Unhedged) next;
    /// money caps (Bankroll, PerMarketCap) last.
    pub fn pre_check(&self, b: &BasketCheck) -> RiskVerdict {
        use RejectReason as R;
        if self.killed {
            return RiskVerdict::Rejected(R::KillSwitch);
        }
        if let Some(h) = self.halted {
            return RiskVerdict::Rejected(R::Halted(h));
        }
        if self.paused {
            return RiskVerdict::Rejected(R::Paused);
        }
        if b.legs > self.cfg.max_basket_legs {
            return RiskVerdict::Rejected(R::MaxBasketLegs);
        }
        if self.open_orders + b.legs > self.cfg.max_open_orders {
            return RiskVerdict::Rejected(R::MaxOpenOrders);
        }
        if b.max_leg_cost.0 > self.cfg.max_unhedged.0 {
            return RiskVerdict::Rejected(R::Unhedged);
        }
        if self.global_exposure + b.total_cost.0 > self.cfg.bankroll.0 {
            return RiskVerdict::Rejected(R::Bankroll);
        }
        for &(m, cost) in &b.per_market {
            let existing = self.per_market.get(&m).copied().unwrap_or(0);
            if existing + cost.0 > self.cfg.per_market_cap.0 {
                return RiskVerdict::Rejected(R::PerMarketCap);
            }
        }
        RiskVerdict::Approved
    }

    /// Reserve a dispatched basket's worst-case exposure + its leg count.
    pub fn reserve(&mut self, b: &BasketCheck) {
        self.global_exposure += b.total_cost.0;
        for &(m, cost) in &b.per_market {
            *self.per_market.entry(m).or_insert(0) += cost.0;
        }
        self.open_orders += b.legs;
    }

    /// Release a completed basket's reservation (counterpart of `reserve`).
    pub fn release(&mut self, b: &BasketCheck) {
        self.global_exposure -= b.total_cost.0;
        for &(m, cost) in &b.per_market {
            *self.per_market.entry(m).or_insert(0) -= cost.0;
        }
        self.open_orders = self.open_orders.saturating_sub(b.legs);
    }

    /// Commit an actual position cost-basis delta for a market (signed; from
    /// the coordinator's position book after an execution report).
    pub fn commit(&mut self, market: MarketId, delta: Usdc) {
        self.global_exposure += delta.0;
        *self.per_market.entry(market).or_insert(0) += delta.0;
    }

    pub fn set_paused(&mut self, p: bool) {
        self.paused = p;
    }

    /// Kill switch (spec §15): trips dispatch off; requires manual clear.
    pub fn trip_kill(&mut self) {
        self.killed = true;
    }

    pub fn clear_kill(&mut self) {
        self.killed = false;
    }

    pub fn is_killed(&self) -> bool {
        self.killed
    }

    pub fn halted(&self) -> Option<HaltReason> {
        self.halted
    }

    /// Record an order error. Consecutive errors (no intervening success)
    /// within the window trip the halt.
    pub fn record_error(&mut self, now: Instant) -> Option<HaltReason> {
        self.errors.push_back(now);
        while let Some(&front) = self.errors.front() {
            if now.duration_since(front) > self.cfg.error_halt_window {
                self.errors.pop_front();
            } else {
                break;
            }
        }
        if u32::try_from(self.errors.len()).unwrap_or(u32::MAX) >= self.cfg.error_halt_count {
            self.halted = Some(HaltReason::ConsecutiveErrors);
        }
        self.halted
    }

    /// A successful basket resets the consecutive-error chain.
    pub fn record_success(&mut self) {
        self.errors.clear();
    }

    /// Update session equity (µUSDC, relative to session start = 0). Trips the
    /// drawdown halt when peak − equity ≥ bankroll · dd_bps / 10⁴.
    pub fn update_equity(&mut self, equity: Usdc) -> Option<HaltReason> {
        self.peak_equity = self.peak_equity.max(equity.0);
        let dd = self.peak_equity - equity.0;
        if dd >= self.cfg.bankroll.0 * self.cfg.daily_drawdown_bps / 10_000 {
            self.halted = Some(HaltReason::DailyDrawdown);
        }
        self.halted
    }

    /// Feed the store-derived count of session starts inside the storm window
    /// (spec §15 restart-storm detector → trips at startup). The window itself
    /// is applied by the store's session-start query (`record_session_start`);
    /// only the count threshold lives in RiskConfig.
    pub fn note_session_starts_in_window(&mut self, count: usize) -> Option<HaltReason> {
        if u32::try_from(count).unwrap_or(u32::MAX) >= self.cfg.restart_storm_count {
            self.halted = Some(HaltReason::RestartStorm);
        }
        self.halted
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::instrument::MarketId;
    use pm_core::num::Usdc;
    use std::time::Duration;
    use std::time::Instant;

    fn cfg() -> RiskConfig {
        RiskConfig {
            bankroll: Usdc(10_000_000_000),      // $10k
            per_market_cap: Usdc(1_000_000_000), // $1k
            max_unhedged: Usdc(200_000_000),     // $200
            max_open_orders: 4,
            max_basket_legs: 3,
            daily_drawdown_bps: 200, // 2%
            error_halt_count: 3,
            error_halt_window: Duration::from_secs(60),
            restart_storm_count: 5,
        }
    }

    fn basket(total: i128, max_leg: i128, markets: &[(u32, i128)]) -> BasketCheck {
        BasketCheck {
            total_cost: Usdc(total),
            max_leg_cost: Usdc(max_leg),
            legs: markets.len().max(1),
            per_market: markets
                .iter()
                .map(|&(m, c)| (MarketId(m), Usdc(c)))
                .collect(),
        }
    }

    #[test]
    fn approves_within_all_caps() {
        let r = RiskEngine::new(cfg());
        let b = basket(100_000_000, 100_000_000, &[(0, 100_000_000)]);
        assert_eq!(r.pre_check(&b), RiskVerdict::Approved);
    }

    #[test]
    fn per_market_cap_counts_existing_exposure() {
        let mut r = RiskEngine::new(cfg());
        let b1 = basket(900_000_000, 100_000_000, &[(0, 900_000_000)]);
        assert_eq!(r.pre_check(&b1), RiskVerdict::Approved);
        r.reserve(&b1);
        // 900 + 200 > 1000 per-market cap
        let b2 = basket(200_000_000, 100_000_000, &[(0, 200_000_000)]);
        assert_eq!(
            r.pre_check(&b2),
            RiskVerdict::Rejected(RejectReason::PerMarketCap)
        );
        // a different market is fine
        let b3 = basket(200_000_000, 100_000_000, &[(1, 200_000_000)]);
        assert_eq!(r.pre_check(&b3), RiskVerdict::Approved);
    }

    #[test]
    fn bankroll_cap_is_global() {
        let mut c = cfg();
        c.max_open_orders = 100; // isolate the bankroll cap from the order-count cap
        let mut r = RiskEngine::new(c);
        for m in 0..9u32 {
            let b = basket(1_000_000_000, 100_000_000, &[(m, 1_000_000_000)]);
            assert_eq!(r.pre_check(&b), RiskVerdict::Approved, "market {m}");
            r.reserve(&b);
        }
        // 9k reserved; 1.5k more blows the 10k bankroll even though market 9 is fresh
        let b = basket(1_500_000_000, 100_000_000, &[(9, 1_500_000_000)]);
        assert_eq!(
            r.pre_check(&b),
            RiskVerdict::Rejected(RejectReason::Bankroll)
        );
    }

    #[test]
    fn leg_and_order_count_caps() {
        let mut r = RiskEngine::new(cfg());
        let b = basket(
            10_000_000,
            10_000_000,
            &[
                (0, 10_000_000),
                (1, 10_000_000),
                (2, 10_000_000),
                (3, 10_000_000),
            ],
        );
        assert_eq!(
            r.pre_check(&b),
            RiskVerdict::Rejected(RejectReason::MaxBasketLegs)
        ); // 4 > 3
        let b2 = basket(10_000_000, 10_000_000, &[(0, 10_000_000), (1, 10_000_000)]);
        r.reserve(&b2); // 2 open orders
        let b3 = basket(
            10_000_000,
            10_000_000,
            &[(2, 10_000_000), (3, 10_000_000), (4, 10_000_000)],
        );
        assert_eq!(
            r.pre_check(&b3),
            RiskVerdict::Rejected(RejectReason::MaxOpenOrders)
        ); // 2+3 > 4
    }

    #[test]
    fn single_leg_baskets_also_count_against_open_orders() {
        let mut r = RiskEngine::new(cfg()); // max_open_orders: 4
        for m in 0..4u32 {
            let b = basket(10_000_000, 10_000_000, &[(m, 10_000_000)]);
            assert_eq!(r.pre_check(&b), RiskVerdict::Approved);
            r.reserve(&b);
        }
        let b = basket(10_000_000, 10_000_000, &[(9, 10_000_000)]);
        assert_eq!(
            r.pre_check(&b),
            RiskVerdict::Rejected(RejectReason::MaxOpenOrders)
        );
        // releasing frees the slots again
        r.release(&basket(10_000_000, 10_000_000, &[(0, 10_000_000)]));
        assert_eq!(r.pre_check(&b), RiskVerdict::Approved);
    }

    #[test]
    fn unhedged_pre_check_uses_max_leg() {
        let r = RiskEngine::new(cfg());
        let b = basket(300_000_000, 250_000_000, &[(0, 300_000_000)]); // leg $250 > $200
        assert_eq!(
            r.pre_check(&b),
            RiskVerdict::Rejected(RejectReason::Unhedged)
        );
    }

    #[test]
    fn release_then_commit_replaces_reservation_with_actuals() {
        let mut r = RiskEngine::new(cfg());
        let b = basket(900_000_000, 100_000_000, &[(0, 900_000_000)]);
        r.reserve(&b);
        r.release(&b);
        // basket filled only $300 worth and holds it as a position
        r.commit(MarketId(0), Usdc(300_000_000));
        let b2 = basket(600_000_000, 100_000_000, &[(0, 600_000_000)]);
        assert_eq!(r.pre_check(&b2), RiskVerdict::Approved); // 300+600 ≤ 1000
        let b3 = basket(800_000_000, 100_000_000, &[(0, 800_000_000)]);
        assert_eq!(
            r.pre_check(&b3),
            RiskVerdict::Rejected(RejectReason::PerMarketCap)
        );
        // commit can also reduce exposure (positions closed)
        r.commit(MarketId(0), Usdc(-300_000_000));
        assert_eq!(r.pre_check(&b3), RiskVerdict::Approved);
    }

    // ── Task 6: halts, kill switch, pause ─────────────────────────────────────

    #[test]
    fn pause_and_kill_dominate() {
        let mut r = RiskEngine::new(cfg());
        let b = basket(1_000_000, 1_000_000, &[(0, 1_000_000)]);
        r.set_paused(true);
        assert_eq!(r.pre_check(&b), RiskVerdict::Rejected(RejectReason::Paused));
        r.set_paused(false);
        r.trip_kill();
        assert!(r.is_killed());
        assert_eq!(
            r.pre_check(&b),
            RiskVerdict::Rejected(RejectReason::KillSwitch)
        );
        r.clear_kill(); // manual clear (spec §15)
        assert_eq!(r.pre_check(&b), RiskVerdict::Approved);
    }

    #[test]
    fn consecutive_errors_within_window_halt() {
        let mut r = RiskEngine::new(cfg()); // 3 errors / 60s
        let t0 = Instant::now();
        assert_eq!(r.record_error(t0), None);
        assert_eq!(r.record_error(t0 + Duration::from_secs(10)), None);
        // a success resets the consecutive count
        r.record_success();
        assert_eq!(r.record_error(t0 + Duration::from_secs(20)), None);
        assert_eq!(r.record_error(t0 + Duration::from_secs(30)), None);
        assert_eq!(
            r.record_error(t0 + Duration::from_secs(40)),
            Some(HaltReason::ConsecutiveErrors)
        );
        assert_eq!(r.halted(), Some(HaltReason::ConsecutiveErrors));
    }

    #[test]
    fn errors_outside_window_do_not_halt() {
        let mut r = RiskEngine::new(cfg());
        let t0 = Instant::now();
        assert_eq!(r.record_error(t0), None);
        assert_eq!(r.record_error(t0 + Duration::from_secs(61)), None);
        assert_eq!(r.record_error(t0 + Duration::from_secs(122)), None);
        assert_eq!(r.halted(), None);
    }

    #[test]
    fn drawdown_halt_is_peak_to_trough() {
        let mut r = RiskEngine::new(cfg()); // 200 bps of $10k = $200
        assert_eq!(r.update_equity(Usdc(150_000_000)), None); // peak $150
        assert_eq!(r.update_equity(Usdc(0)), None); // dd $150 < $200
        assert_eq!(
            r.update_equity(Usdc(-60_000_000)),
            Some(HaltReason::DailyDrawdown)
        ); // dd $210
    }

    #[test]
    fn restart_storm_halts() {
        let mut r = RiskEngine::new(cfg()); // mut: note_session_starts_in_window takes &mut self
        assert_eq!(r.note_session_starts_in_window(4), None);
        assert_eq!(
            r.note_session_starts_in_window(5),
            Some(HaltReason::RestartStorm)
        );
    }

    #[test]
    fn caps_are_inclusive_at_exact_boundary() {
        let r = RiskEngine::new(cfg());
        // exactly at per-market cap: approved; one µUSDC over: rejected
        let at = basket(1_000_000_000, 100_000_000, &[(0, 1_000_000_000)]);
        assert_eq!(r.pre_check(&at), RiskVerdict::Approved);
        let over = basket(1_000_000_001, 100_000_000, &[(0, 1_000_000_001)]);
        assert!(matches!(r.pre_check(&over), RiskVerdict::Rejected(_)));
        // fill to exactly the bankroll across markets: approved at boundary
        let mut c = cfg();
        c.max_open_orders = 100;
        let mut r = RiskEngine::new(c);
        for m in 0..10u32 {
            let b = basket(1_000_000_000, 100_000_000, &[(m, 1_000_000_000)]);
            assert_eq!(r.pre_check(&b), RiskVerdict::Approved, "market {m}");
            r.reserve(&b);
        }
        // bankroll fully consumed: even $1 more is rejected globally
        let one = basket(1_000_000, 1_000_000, &[(10, 1_000_000)]);
        assert_eq!(
            r.pre_check(&one),
            RiskVerdict::Rejected(RejectReason::Bankroll)
        );
    }
}
