//! Coordinator (spec §12 pipeline hub): coalesce/dedup detected opportunities,
//! risk pre-check, single-in-flight dispatch to the execution task, position
//! book + periodic P&L snapshots, and kill-switch tripping.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pm_config::{Config, ConfigError};
use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{Bps, Usdc, sell_proceeds};
use pm_engine::dedup::Cooldown;
use pm_engine::{Action, EngineParams, Opportunity};
use pm_execution::basket::{BasketOutcome, BasketReport, ExecParams};
use pm_risk::{BasketCheck, RiskConfig, RiskEngine, RiskVerdict};
use pm_store::usdc_to_i64;
use pm_store::writer::StoreMsg;
use pm_store::{HaltRow, OppRow, PnlRow};
use tokio::sync::mpsc;
use tracing::warn;

use crate::detector::DetectedOpp;
use crate::stats::AppStats;
use crate::wiring::BookFetcher;

/// A risk-approved basket handed to the execution task.
pub struct ExecRequest {
    pub opp: Opportunity,
    pub check: BasketCheck,
    pub at: Instant,
}

/// The execution task's reply for one basket.
pub struct ExecReport {
    pub check: BasketCheck,
    pub result: Result<BasketReport, String>,
}

#[derive(Debug, Clone, Copy)]
pub struct CoordinatorSummary {
    pub cash: Usdc,
    pub equity: Usdc,
    pub open_positions: usize,
}

/// Wall-clock milliseconds since the Unix epoch (0 on clock error).
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Build the risk pre-check for an opportunity: per-market buy-leg cash plus
/// 1:1 µshare→µUSDC collateral for any splits, summed per market.
pub fn basket_check(opp: &Opportunity, token_market: &HashMap<TokenId, MarketId>) -> BasketCheck {
    let mut per_market_map: HashMap<MarketId, i128> = HashMap::new();
    let mut max_leg: i128 = 0;
    for f in &opp.fills {
        let abs_cash = f.cash.0.abs();
        max_leg = max_leg.max(abs_cash);
        if f.action == Action::Buy
            && let Some(&m) = token_market.get(&f.token)
        {
            *per_market_map.entry(m).or_insert(0) += abs_cash;
        }
    }
    // Split collateral: µshares 1:1 µUSDC, per market.
    for &(m, units) in &opp.splits {
        *per_market_map.entry(m).or_insert(0) += units.0 as i128;
    }
    let mut per_market: Vec<(MarketId, Usdc)> = per_market_map
        .into_iter()
        .map(|(m, c)| (m, Usdc(c)))
        .collect();
    per_market.sort_by_key(|(m, _)| *m);
    let total: i128 = per_market.iter().map(|(_, c)| c.0).sum();
    BasketCheck {
        total_cost: Usdc(total),
        max_leg_cost: Usdc(max_leg),
        legs: opp.fills.len(),
        per_market,
    }
}

pub struct Coordinator {
    risk: RiskEngine,
    cooldown: Cooldown,
    token_market: HashMap<TokenId, MarketId>,
    fetcher: BookFetcher,
    positions: crate::positions::PositionBook,

    opp_rx: mpsc::Receiver<DetectedOpp>,
    exec_tx: mpsc::Sender<ExecRequest>,
    report_rx: mpsc::Receiver<ExecReport>,
    store_tx: mpsc::Sender<StoreMsg>,

    kill: Arc<AtomicBool>,
    stats: Arc<AppStats>,

    max_age: Duration,
    pnl_interval: Duration,
    dispatch_enabled: bool,

    busy: bool,
    in_flight: Option<BasketCheck>,
    report_closed: bool,
    kill_logged: bool,
    halt_logged: bool,
}

impl Coordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: &Config,
        risk_cfg: RiskConfig,
        params: EngineParams,
        token_market: HashMap<TokenId, MarketId>,
        fetcher: BookFetcher,
        opp_rx: mpsc::Receiver<DetectedOpp>,
        exec_tx: mpsc::Sender<ExecRequest>,
        report_rx: mpsc::Receiver<ExecReport>,
        store_tx: mpsc::Sender<StoreMsg>,
        kill: Arc<AtomicBool>,
        stats: Arc<AppStats>,
    ) -> Result<Self, ConfigError> {
        let cooldown = Cooldown::new(
            Duration::from_millis(params.cooldown_ms),
            params.reemit_improvement_pct,
        );
        Ok(Self {
            risk: RiskEngine::new(risk_cfg),
            cooldown,
            token_market,
            fetcher,
            positions: crate::positions::PositionBook::default(),
            opp_rx,
            exec_tx,
            report_rx,
            store_tx,
            kill,
            stats,
            max_age: Duration::from_millis(cfg.risk.max_opportunity_age_ms),
            pnl_interval: Duration::from_secs(10),
            // Paper mode still dispatches (to PaperVenue); flag reserved for a
            // future dry-run toggle. Wired to mode.paper for explicitness.
            dispatch_enabled: cfg.mode.paper,
            busy: false,
            in_flight: None,
            report_closed: false,
            kill_logged: false,
            halt_logged: false,
        })
    }

    pub fn note_session_starts(&mut self, n: usize) {
        if self.risk.note_session_starts_in_window(n).is_some() {
            self.maybe_log_halt_blocking();
        }
    }

    pub async fn run(mut self) -> CoordinatorSummary {
        let mut pnl_tick = tokio::time::interval(self.pnl_interval);
        pnl_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        pnl_tick.tick().await; // consume immediate tick
        loop {
            self.check_kill().await;
            tokio::select! {
                maybe = self.opp_rx.recv() => match maybe {
                    None => break,
                    Some(first) => self.handle_opps(first).await,
                },
                maybe = self.report_rx.recv(), if !self.report_closed => match maybe {
                    Some(rep) => self.handle_report(rep).await,
                    None => self.report_closed = true,
                },
                _ = pnl_tick.tick() => self.snapshot_pnl().await,
            }
        }
        // Drain: one in-flight basket may still report.
        if self.busy
            && !self.report_closed
            && let Some(rep) = self.report_rx.recv().await
        {
            self.handle_report(rep).await;
        }
        self.snapshot_pnl().await; // final durable snapshot
        let marks = self.marks().await;
        let pnl = self.positions.pnl(&marks);
        CoordinatorSummary {
            cash: pnl.cash,
            equity: pnl.equity,
            open_positions: self.positions.holdings().len(),
        }
    }

    async fn handle_opps(&mut self, first: DetectedOpp) {
        // Drain-and-coalesce by fingerprint, keeping the max-net candidate.
        let mut coalesced: HashMap<u64, DetectedOpp> = HashMap::new();
        Self::coalesce(&mut coalesced, first);
        while let Ok(d) = self.opp_rx.try_recv() {
            Self::coalesce(&mut coalesced, d);
        }

        for (_, d) in coalesced {
            self.process_opp(d).await;
        }
    }

    fn coalesce(map: &mut HashMap<u64, DetectedOpp>, d: DetectedOpp) {
        let fp = d.opp.fingerprint().as_u64();
        match map.get(&fp) {
            Some(existing) if existing.opp.net.0 >= d.opp.net.0 => {}
            _ => {
                map.insert(fp, d);
            }
        }
    }

    async fn process_opp(&mut self, d: DetectedOpp) {
        use Ordering::Relaxed;
        if d.at.elapsed() > self.max_age {
            self.stats.expired_age.fetch_add(1, Relaxed);
            return;
        }
        if !self.cooldown.admit(Instant::now(), &d.opp) {
            self.stats.suppressed_cooldown.fetch_add(1, Relaxed);
            return;
        }
        self.stats.admitted.fetch_add(1, Relaxed);

        let check = basket_check(&d.opp, &self.token_market);
        let verdict = self.risk.pre_check(&check);
        let approved = matches!(verdict, RiskVerdict::Approved);
        let dispatch = approved && !self.busy && self.dispatch_enabled;

        self.log_opportunity(&d.opp, dispatch);

        if !approved {
            self.stats.rejected_risk.fetch_add(1, Relaxed);
            self.maybe_log_halt().await;
            return;
        }
        if !dispatch {
            self.stats.suppressed_busy.fetch_add(1, Relaxed);
            return;
        }

        self.risk.reserve(&check);
        self.busy = true;
        self.in_flight = Some(check.clone());
        self.stats.dispatched.fetch_add(1, Relaxed);
        self.stats
            .record_dispatch_us(d.at.elapsed().as_micros() as u64);
        let req = ExecRequest {
            opp: d.opp,
            check,
            at: d.at,
        };
        if self.exec_tx.send(req).await.is_err() {
            // Execution task gone: undo the reservation.
            if let Some(c) = self.in_flight.take() {
                self.risk.release(&c);
            }
            self.busy = false;
        }
    }

    fn log_opportunity(&self, opp: &Opportunity, dispatched: bool) {
        let legs_json = legs_json(opp);
        let row = OppRow {
            ts_ms: now_ms(),
            class: format!("{:?}", opp.class),
            fingerprint: format!("{:016x}", opp.fingerprint().as_u64()),
            edge_bps: opp.edge.0 as i64,
            units_micro: opp.units.0 as i64,
            net_micro: usdc_to_i64(opp.net).unwrap_or(i64::MAX),
            basis_micro: usdc_to_i64(opp.basis).unwrap_or(i64::MAX),
            legs_json,
            dispatched,
        };
        // Fire-and-forget (spec §16 allows coalescing under back-pressure).
        let _ = self.store_tx.try_send(StoreMsg::Opportunity(row));
    }

    async fn handle_report(&mut self, rep: ExecReport) {
        use Ordering::Relaxed;
        self.busy = false;
        self.in_flight = None;
        self.risk.release(&rep.check);
        match rep.result {
            Ok(report) => {
                let deltas =
                    self.positions
                        .apply(&report.positions, report.cash_delta, &self.token_market);
                for (m, c) in deltas {
                    self.risk.commit(m, c);
                }
                match report.outcome {
                    BasketOutcome::FilledClean => {
                        self.stats.baskets_clean.fetch_add(1, Relaxed);
                        self.risk.record_success();
                    }
                    BasketOutcome::Repaired => {
                        self.stats.baskets_repaired.fetch_add(1, Relaxed);
                        self.risk.record_success();
                    }
                    BasketOutcome::Unwound => {
                        self.stats.baskets_unwound.fetch_add(1, Relaxed);
                        let _ = self.risk.record_error(Instant::now());
                    }
                    BasketOutcome::NoFill | BasketOutcome::RejectedUnhedged => {
                        self.stats.baskets_nofill.fetch_add(1, Relaxed);
                    }
                }
                for _ in 0..report.order_errors {
                    let _ = self.risk.record_error(Instant::now());
                }
            }
            Err(e) => {
                self.stats.exec_errors.fetch_add(1, Relaxed);
                warn!("basket execution error: {e}");
                let _ = self.risk.record_error(Instant::now());
            }
        }
        self.maybe_log_halt().await;
    }

    async fn check_kill(&mut self) {
        if self.kill.load(Ordering::Acquire) && !self.kill_logged {
            self.risk.trip_kill();
            self.kill_logged = true;
            warn!("kill switch tripped: blocking all dispatch");
            let row = HaltRow {
                ts_ms: now_ms(),
                reason: "KillSwitch".into(),
                detail: String::new(),
            };
            let _ = self.store_tx.send(StoreMsg::Halt(row)).await;
        }
    }

    async fn maybe_log_halt(&mut self) {
        if let Some(h) = self.risk.halted()
            && !self.halt_logged
        {
            self.halt_logged = true;
            warn!("risk halt: {h:?}");
            let row = HaltRow {
                ts_ms: now_ms(),
                reason: format!("{h:?}"),
                detail: String::new(),
            };
            let _ = self.store_tx.send(StoreMsg::Halt(row)).await;
        }
    }

    /// Synchronous halt log for `note_session_starts` (called before `run`).
    fn maybe_log_halt_blocking(&mut self) {
        if let Some(h) = self.risk.halted()
            && !self.halt_logged
        {
            self.halt_logged = true;
            warn!("risk halt: {h:?}");
            let row = HaltRow {
                ts_ms: now_ms(),
                reason: format!("{h:?}"),
                detail: String::new(),
            };
            let _ = self.store_tx.try_send(StoreMsg::Halt(row));
        }
    }

    /// Conservative bid-side marks for every held token; missing/invalid → 0.
    async fn marks(&self) -> HashMap<TokenId, Usdc> {
        let mut out = HashMap::new();
        for (t, q, _) in self.positions.holdings() {
            let mark = match self.fetcher.fetch(t).await {
                Some((book, true)) => book
                    .bids
                    .best()
                    .map(|bid| sell_proceeds(bid.microusdc(book.ts()), q))
                    .unwrap_or(Usdc(0)),
                _ => Usdc(0),
            };
            out.insert(t, mark);
        }
        out
    }

    async fn snapshot_pnl(&mut self) {
        let marks = self.marks().await;
        let pnl = self.positions.pnl(&marks);
        if self.risk.update_equity(pnl.equity).is_some() {
            self.maybe_log_halt().await;
        }
        let row = PnlRow {
            ts_ms: now_ms(),
            cash_micro: usdc_to_i64(pnl.cash).unwrap_or(i64::MAX),
            realized_micro: usdc_to_i64(pnl.realized).unwrap_or(i64::MAX),
            unrealized_micro: usdc_to_i64(pnl.unrealized).unwrap_or(i64::MAX),
            equity_micro: usdc_to_i64(pnl.equity).unwrap_or(i64::MAX),
        };
        let _ = self.store_tx.send(StoreMsg::PnlSnapshot(row)).await;
    }
}

fn legs_json(opp: &Opportunity) -> String {
    let mut s = String::from("[");
    for (i, f) in opp.fills.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let action = match f.action {
            Action::Buy => "Buy",
            Action::Sell => "Sell",
        };
        s.push_str(&format!(
            "{{\"token\":{},\"action\":\"{}\",\"px\":{},\"qty\":{}}}",
            f.token.0,
            action,
            f.limit_px.get(),
            f.qty.0
        ));
    }
    s.push(']');
    s
}

/// The execution task: pull requests, run the basket, ship the report back.
#[allow(clippy::too_many_arguments)]
pub async fn run_execution<V: pm_execution::venue::ExecutionVenue>(
    mut venue: V,
    mut rx: mpsc::Receiver<ExecRequest>,
    report_tx: mpsc::Sender<ExecReport>,
    store_tx: mpsc::Sender<StoreMsg>,
    token_market: HashMap<TokenId, MarketId>,
    market_tokens: HashMap<MarketId, (TokenId, TokenId)>,
    token_fee: HashMap<TokenId, Bps>,
    params: ExecParams,
) {
    while let Some(req) = rx.recv().await {
        let result = pm_execution::basket::execute_basket(
            &mut venue,
            &store_tx,
            &req.opp,
            &token_market,
            &market_tokens,
            &token_fee,
            &params,
            now_ms(),
        )
        .await
        .map_err(|e| e.to_string());
        if report_tx
            .send(ExecReport {
                check: req.check,
                result,
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::{Px, Qty, TickSize};
    use pm_engine::{ArbClass, LegFill};
    use std::time::Duration;

    struct Harness {
        opp_tx: mpsc::Sender<DetectedOpp>,
        exec_rx: mpsc::Receiver<ExecRequest>,
        report_tx: mpsc::Sender<ExecReport>,
        store_rx: mpsc::Receiver<StoreMsg>,
        stats: Arc<AppStats>,
        handle: tokio::task::JoinHandle<CoordinatorSummary>,
    }

    fn token_market() -> HashMap<TokenId, MarketId> {
        HashMap::from([(TokenId(1), MarketId(0)), (TokenId(2), MarketId(0))])
    }

    /// C1Long: buy tok1@44¢ + buy tok2@50¢, 100 µshares, net 5_990_000,
    /// basis 94_000_000. Optionally nudge tok2's price for a distinct fingerprint.
    fn opp_fixture(tok2_px: u16) -> Opportunity {
        let f1 = LegFill {
            token: TokenId(1),
            action: Action::Buy,
            ts: TickSize::Cent,
            limit_px: Px::new(44, TickSize::Cent).unwrap(),
            qty: Qty(100),
            cash: Usdc(-44_000_000),
        };
        let f2 = LegFill {
            token: TokenId(2),
            action: Action::Buy,
            ts: TickSize::Cent,
            limit_px: Px::new(tok2_px, TickSize::Cent).unwrap(),
            qty: Qty(100),
            cash: Usdc(-50_000_000),
        };
        Opportunity {
            class: ArbClass::C1Long,
            fills: vec![f1, f2],
            units: Qty(100),
            net: Usdc(5_990_000),
            basis: Usdc(94_000_000),
            edge: Bps(637),
            splits: vec![],
        }
    }

    fn spawn(kill_preset: bool) -> Harness {
        let (opp_tx, opp_rx) = mpsc::channel(64);
        let (exec_tx, exec_rx) = mpsc::channel(64);
        let (report_tx, report_rx) = mpsc::channel(64);
        let (store_tx, store_rx) = mpsc::channel(64);
        let stats = AppStats::new();
        let kill = Arc::new(AtomicBool::new(kill_preset));
        let cfg = Config::default();
        let coord = Coordinator::new(
            &cfg,
            crate::wiring::risk_config(&cfg).unwrap(),
            crate::wiring::engine_params(&cfg).unwrap(),
            token_market(),
            BookFetcher::new(HashMap::new()),
            opp_rx,
            exec_tx,
            report_rx,
            store_tx,
            Arc::clone(&kill),
            Arc::clone(&stats),
        )
        .unwrap();
        let handle = tokio::spawn(coord.run());
        Harness {
            opp_tx,
            exec_rx,
            report_tx,
            store_rx,
            stats,
            handle,
        }
    }

    fn drain_store(rx: &mut mpsc::Receiver<StoreMsg>) -> (usize, usize, usize) {
        let (mut opps, mut pnls, mut halts) = (0, 0, 0);
        while let Ok(m) = rx.try_recv() {
            match m {
                StoreMsg::Opportunity(_) => opps += 1,
                StoreMsg::PnlSnapshot(_) => pnls += 1,
                StoreMsg::Halt(_) => halts += 1,
                _ => {}
            }
        }
        (opps, pnls, halts)
    }

    #[tokio::test]
    async fn dispatches_approved_then_busy_suppresses_then_report_frees() {
        let mut h = spawn(false);

        h.opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(50),
                at: Instant::now(),
            })
            .await
            .unwrap();

        // Coordinator dispatches the approved basket.
        let req = tokio::time::timeout(Duration::from_secs(2), h.exec_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(req.check.total_cost, Usdc(94_000_000));

        // A differently-shaped opp while busy → suppressed, no second dispatch.
        h.opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(49),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            h.exec_rx.try_recv().is_err(),
            "must not dispatch while busy"
        );
        assert!(h.stats.suppressed_busy.load(Ordering::Relaxed) >= 1);

        // Report frees the slot; cash flows in.
        h.report_tx
            .send(ExecReport {
                check: req.check,
                result: Ok(BasketReport {
                    outcome: BasketOutcome::FilledClean,
                    cash_delta: Usdc(5_990_000),
                    positions: vec![],
                    order_errors: 0,
                }),
            })
            .await
            .unwrap();

        drop(h.opp_tx);
        drop(h.report_tx);

        let summary = tokio::time::timeout(Duration::from_secs(2), h.handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.cash, Usdc(5_990_000));
        assert_eq!(h.stats.dispatched.load(Ordering::Relaxed), 1);

        let (opps, pnls, _) = drain_store(&mut h.store_rx);
        assert!(opps >= 2, "expected ≥2 opportunity rows, got {opps}");
        assert!(pnls >= 1, "expected ≥1 pnl snapshot, got {pnls}");
    }

    #[tokio::test]
    async fn aged_opportunities_are_discarded() {
        let mut h = spawn(false);
        h.opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(50),
                at: Instant::now() - Duration::from_secs(5),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(h.exec_rx.try_recv().is_err(), "aged opp must not dispatch");
        assert_eq!(h.stats.expired_age.load(Ordering::Relaxed), 1);

        drop(h.opp_tx);
        drop(h.report_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), h.handle).await;
    }

    #[tokio::test]
    async fn kill_flag_blocks_dispatch_and_logs_halt() {
        let mut h = spawn(true);
        h.opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            h.exec_rx.try_recv().is_err(),
            "kill flag must block dispatch"
        );

        drop(h.opp_tx);
        drop(h.report_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), h.handle).await;

        let (_, _, halts) = drain_store(&mut h.store_rx);
        assert_eq!(halts, 1, "expected exactly 1 halt row, got {halts}");
    }
}
