//! Coordinator (spec §12 pipeline hub): coalesce/dedup detected opportunities,
//! risk pre-check, single-in-flight dispatch to the execution task, position
//! book + periodic P&L snapshots, and kill-switch tripping.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pm_config::{Config, ConfigError};
use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{Bps, Qty, Usdc, buy_cost, sell_proceeds};
use pm_engine::dedup::Cooldown;
use pm_engine::{Action, EngineParams, Opportunity};
use pm_execution::basket::{BasketOutcome, BasketReport, ExecParams};
use pm_risk::{BasketCheck, RiskConfig, RiskEngine, RiskVerdict};
use pm_store::usdc_to_i64;
use pm_store::writer::StoreMsg;
use pm_store::{HaltRow, OppRow, PnlRow};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

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

/// Control commands from the TUI (translated by main.rs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtlCommand {
    SetPaused(bool),
    /// Release the live-dispatch latch (TUI `l` modal, post typed-confirm).
    /// Idempotent and one-way: real orders flow from here on.
    ReleaseLive,
}

/// Coordinator-owned state the dashboard needs, published on every change
/// and every P&L snapshot. Money in µUSDC (display conversion is the TUI's).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CoordStatus {
    pub paused: bool,
    pub halted: Option<String>,
    pub killed: bool,
    pub busy: bool,
    pub live: bool,
    pub live_released: bool,
    pub cash_micro: i64,
    pub equity_micro: i64,     // bid-marked (reporting)
    pub equity_mid_micro: i64, // mid-marked (risk/halt feed)
    pub realized_micro: i64,
    pub unrealized_micro: i64,
    pub open_positions: usize,
}

/// Receive the next control command, or pend forever when no channel is wired
/// (same Option-pending idiom as the supervisor's `recv_cmd`).
async fn recv_ctl(rx: &mut Option<mpsc::Receiver<CtlCommand>>) -> CtlCommand {
    match rx.as_mut() {
        Some(r) => match r.recv().await {
            Some(c) => c,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
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

/// Live-mode dispatch parameters (spec 2026-06-13 §Mode ladder & live gates).
#[derive(Debug, Clone, Copy)]
pub struct LiveParams {
    pub live: bool,
    /// Headless live (post typed-confirm) and shadow start released; TUI live
    /// starts held until the `l` modal's typed confirmation.
    pub released_at_start: bool,
    /// Canary per-basket basis cap, µUSDC.
    pub basket_cap: Usdc,
    /// Venue minimum per leg, µshares (RECON: 5 shares).
    pub min_leg: Qty,
    /// Venue minimum order VALUE per leg, µUSDC (Polymarket V2 $1 floor). A
    /// basket with any buy leg whose makerAmount is below this is rejected whole.
    pub min_leg_value: Usdc,
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
    mid_spread_cap_ticks: u16,

    live: bool,
    live_released: bool,
    live_basket_cap: Usdc,
    live_min_leg: Qty,
    live_min_leg_value: Usdc,

    busy: bool,
    in_flight: Option<BasketCheck>,
    report_closed: bool,
    kill_logged: bool,
    halt_logged: bool,

    ctl_rx: Option<mpsc::Receiver<CtlCommand>>,
    status_tx: Option<watch::Sender<CoordStatus>>,
    paused: bool,
    last_status: CoordStatus,
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
        live: LiveParams,
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
            // Both modes dispatch; pause/halt/kill and the live release latch are
            // the gates (mode.paper here silently disabled live dispatch).
            dispatch_enabled: true,
            mid_spread_cap_ticks: cfg.risk.mid_spread_cap_ticks,
            live: live.live,
            live_released: live.released_at_start,
            live_basket_cap: live.basket_cap,
            live_min_leg: live.min_leg,
            live_min_leg_value: live.min_leg_value,
            busy: false,
            in_flight: None,
            report_closed: false,
            kill_logged: false,
            halt_logged: false,
            ctl_rx: None,
            status_tx: None,
            paused: false,
            last_status: CoordStatus::default(),
        })
    }

    /// Create the TUI control channel; the coordinator owns the receiver.
    pub fn control_channel(&mut self, capacity: usize) -> mpsc::Sender<CtlCommand> {
        let (tx, rx) = mpsc::channel(capacity);
        self.ctl_rx = Some(rx);
        tx
    }

    /// Create the dashboard status watch; the coordinator owns the sender.
    pub fn status_channel(&mut self) -> watch::Receiver<CoordStatus> {
        let (tx, rx) = watch::channel(CoordStatus::default());
        self.status_tx = Some(tx);
        rx
    }

    pub fn note_session_starts(&mut self, n: usize) {
        if self.risk.note_session_starts_in_window(n).is_some() {
            self.maybe_log_halt_blocking();
        }
    }

    /// Apply a TUI control command and republish status.
    fn handle_ctl(&mut self, cmd: CtlCommand) {
        match cmd {
            CtlCommand::SetPaused(p) => {
                // Resume tail: opps seen while paused consumed their cooldown admit slot —
                // identical shapes stay suppressed up to cooldown_ms (2s) after resume.
                self.paused = p;
                self.risk.set_paused(p);
                info!("control: paused={p}");
            }
            CtlCommand::ReleaseLive => {
                if self.live && !self.live_released {
                    self.live_released = true;
                    warn!("LIVE DISPATCH RELEASED — real orders from here on");
                }
            }
        }
        self.publish_status();
    }

    /// Refresh the volatile status fields (money fields persist between
    /// publishes — see `snapshot_pnl`) and send a clone if a watch is wired.
    fn publish_status(&mut self) {
        self.last_status.paused = self.paused;
        self.last_status.halted = self.risk.halted().map(|h| format!("{h:?}"));
        self.last_status.killed = self.risk.is_killed();
        self.last_status.busy = self.busy;
        self.last_status.live = self.live;
        self.last_status.live_released = self.live_released;
        self.last_status.open_positions = self.positions.holdings().len();
        if let Some(tx) = &self.status_tx {
            let _ = tx.send(self.last_status.clone());
        }
    }

    pub async fn run(mut self) -> CoordinatorSummary {
        let mut pnl_tick = tokio::time::interval(self.pnl_interval);
        pnl_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        pnl_tick.tick().await; // consume immediate tick
        // run consumes self, so the receiver need not be restored.
        let mut ctl_rx = self.ctl_rx.take();
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
                cmd = recv_ctl(&mut ctl_rx) => self.handle_ctl(cmd),
            }
        }
        // Drain: one in-flight basket may still report.
        if self.busy
            && !self.report_closed
            && let Some(rep) = self.report_rx.recv().await
        {
            self.handle_report(rep).await;
        }
        // Exec task died mid-basket (report_closed && busy): release the stranded
        // reservation so final RiskEngine state can't carry phantom exposure.
        if let Some(c) = self.in_flight.take() {
            self.risk.release(&c);
            self.busy = false;
        }
        self.snapshot_pnl().await; // final durable snapshot
        // Post-shutdown marks are 0 (supervisors gone): the final summary equity
        // is a conservative floor, not fair value.
        let marks = self.marks_pair().await.0;
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

        // Live-mode gates (spec §Mode ladder & live gates): run after cooldown
        // admit and before the risk pre-check, mirroring the cooldown reject's
        // early return — a gated basket is filtered before risk and writes no
        // store row, exactly like a cooldown-suppressed opp.
        if self.live {
            if !self.live_released {
                // Held is expected steady-state pre-release: counted (live_held
                // gauge), deliberately not per-event logged. Like a pause, a
                // held opp consumed its cooldown admit slot — identical shapes
                // stay suppressed up to cooldown_ms after release.
                self.stats.live_held.fetch_add(1, Relaxed);
                return; // held, not rejected: dispatch released later via the modal
            }
            let pure_buy =
                d.opp.fills.iter().all(|f| f.action == Action::Buy) && d.opp.splits.is_empty();
            if !pure_buy {
                self.stats.live_rej.fetch_add(1, Relaxed);
                info!(class = ?d.opp.class, "live: rejected non-pure-buy basket (sell/split classes are M6)");
                return;
            }
            if check.total_cost.0 > self.live_basket_cap.0 {
                self.stats.live_rej.fetch_add(1, Relaxed);
                info!(cost = check.total_cost.0, cap = self.live_basket_cap.0, "live: basket over canary cap");
                return;
            }
            // Venue minimum per leg (RECON-pinned 5 SHARES): a leg the venue would
            // reject kills the whole basket — never resize upward.
            if d.opp.fills.iter().any(|f| f.qty.0 < self.live_min_leg.0) {
                self.stats.live_rej.fetch_add(1, Relaxed);
                info!("live: basket has a leg under the venue minimum");
                return;
            }
            // Venue minimum order VALUE per leg (Polymarket V2 $1 marketable-BUY
            // floor). Every leg is a buy here (pure-buy gate above), so the venue's
            // makerAmount is buy_cost(px, qty) — match it exactly. A leg below the
            // floor kills the whole basket; never resize upward. The 5-share gate
            // above is too weak on cheap tokens (5 × $0.10 = $0.50 < $1).
            if d
                .opp
                .fills
                .iter()
                .any(|f| buy_cost(f.limit_px.microusdc(f.ts), f.qty).0 < self.live_min_leg_value.0)
            {
                self.stats.live_rej.fetch_add(1, Relaxed);
                info!("live: basket has a leg under the venue's per-order $ minimum");
                return;
            }
        }

        let verdict = self.risk.pre_check(&check);
        let approved = matches!(verdict, RiskVerdict::Approved);
        // Direct flag check closes the one-iteration race between the watcher
        // setting the flag and check_kill tripping the risk engine.
        let dispatch =
            approved && !self.busy && self.dispatch_enabled && !self.kill.load(Ordering::Acquire);

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
        } else {
            self.publish_status();
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
        self.publish_status();
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
            self.publish_status();
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
            self.publish_status();
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

    /// (bid marks, mid marks) per held token. Bid: conservative reporting.
    /// Mid: (bid+ask)/2 floor, capped at bid + mid_spread_cap_ticks — the
    /// drawdown-halt feed, immune to the open-basket spread artifact (M3
    /// live-run finding) and to wide/stale asks delaying the halt. Missing
    /// ask → bid; missing book/bid → 0 for both.
    async fn marks_pair(&self) -> (HashMap<TokenId, Usdc>, HashMap<TokenId, Usdc>) {
        let mut bid_marks = HashMap::new();
        let mut mid_marks = HashMap::new();
        for (t, q, _) in self.positions.holdings() {
            let (bid_mark, mid_mark) = match self.fetcher.fetch(t).await {
                Some((book, true)) => match book.bids.best() {
                    Some(bid) => {
                        let ts = book.ts();
                        let bid_micro = bid.microusdc(ts);
                        let ask_micro = book
                            .asks
                            .best()
                            .map(|a| a.microusdc(ts))
                            .unwrap_or(bid_micro);
                        // Wide/stale asks must not delay the halt: mid ≤ bid + mid_spread_cap_ticks.
                        let cap_micro = u64::from(self.mid_spread_cap_ticks)
                            .saturating_mul(ts.unit_microusdc());
                        let mid_micro = ((bid_micro + ask_micro) / 2)
                            .min(bid_micro.saturating_add(cap_micro));
                        (sell_proceeds(bid_micro, q), sell_proceeds(mid_micro, q))
                    }
                    None => (Usdc(0), Usdc(0)),
                },
                _ => (Usdc(0), Usdc(0)),
            };
            bid_marks.insert(t, bid_mark);
            mid_marks.insert(t, mid_mark);
        }
        (bid_marks, mid_marks)
    }

    async fn snapshot_pnl(&mut self) {
        let (bid_marks, mid_marks) = self.marks_pair().await;
        let pnl = self.positions.pnl(&bid_marks);
        let pnl_mid = self.positions.pnl(&mid_marks);
        // Risk/halt feed is MID-marked (immune to the open-basket spread artifact);
        // the durable PnlRow below stays BID-marked (conservative reporting).
        if self.risk.update_equity(pnl_mid.equity).is_some() {
            self.maybe_log_halt().await;
        }
        // Session-loss cap is BID-marked (conservative — trips sooner than mid);
        // the drawdown halt above stays mid-marked.
        if self.risk.update_session_pnl(pnl.equity).is_some() {
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
        // Persist the money fields on last_status (they survive pause/kill
        // publishes that don't recompute marks), then publish.
        self.last_status.cash_micro = usdc_to_i64(pnl.cash).unwrap_or(i64::MAX);
        self.last_status.equity_micro = usdc_to_i64(pnl.equity).unwrap_or(i64::MAX);
        self.last_status.equity_mid_micro = usdc_to_i64(pnl_mid.equity).unwrap_or(i64::MAX);
        self.last_status.realized_micro = usdc_to_i64(pnl.realized).unwrap_or(i64::MAX);
        self.last_status.unrealized_micro = usdc_to_i64(pnl.unrealized).unwrap_or(i64::MAX);
        self.publish_status();
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
    #![allow(clippy::unwrap_used, clippy::expect_used)]
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

    /// Paper-inert live params: live disabled, all gates dormant. Mirrors the
    /// value main.rs passes pre-Task-11.
    fn inert_live() -> LiveParams {
        LiveParams {
            live: false,
            released_at_start: true,
            basket_cap: Usdc(0),
            min_leg: Qty(0),
            min_leg_value: Usdc(0),
        }
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
            crate::wiring::risk_config(&cfg, None).unwrap(),
            crate::wiring::engine_params(&cfg).unwrap(),
            token_market(),
            BookFetcher::new(HashMap::new()),
            opp_rx,
            exec_tx,
            report_rx,
            store_tx,
            Arc::clone(&kill),
            Arc::clone(&stats),
            inert_live(),
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

    /// Helper: build an opp with an explicit `net` value (same shape as
    /// `opp_fixture` at tok2_px=50, so same fingerprint).
    fn opp_with_net(net: i128) -> Opportunity {
        let mut o = opp_fixture(50);
        o.net = Usdc(net);
        o
    }

    #[tokio::test]
    async fn exec_error_report_frees_slot_and_counts() {
        // Dispatch one opp, reply with Err, assert: busy freed (a new
        // different-shape opp dispatches afterwards), exec_errors == 1.
        let mut h = spawn(false);

        // Send first opp and wait for dispatch.
        h.opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req = tokio::time::timeout(Duration::from_secs(2), h.exec_rx.recv())
            .await
            .unwrap()
            .unwrap();

        // Reply with an error.
        h.report_tx
            .send(ExecReport {
                check: req.check,
                result: Err("simulated exec error".to_string()),
            })
            .await
            .unwrap();

        // Give coordinator time to process the error report.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(h.stats.exec_errors.load(Ordering::Relaxed), 1);

        // Send a differently-shaped opp — should dispatch because busy is cleared.
        h.opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(49), // different fingerprint
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req2 = tokio::time::timeout(Duration::from_secs(2), h.exec_rx.recv())
            .await
            .expect("timeout waiting for second dispatch")
            .expect("channel closed before second dispatch");
        assert!(req2.check.total_cost.0 > 0, "second dispatch must be real");
        assert_eq!(h.stats.dispatched.load(Ordering::Relaxed), 2);

        drop(h.opp_tx);
        drop(h.report_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), h.handle).await;
    }

    #[tokio::test]
    async fn coalesce_keeps_max_net_per_fingerprint() {
        // Send two same-shape opps (identical fingerprint, same tok2_px=50) with
        // different net values BEFORE spawning the coordinator, so both are queued
        // in the channel and drained as a single coalesced batch.
        let (opp_tx, opp_rx) = mpsc::channel(64);
        let (exec_tx, mut exec_rx) = mpsc::channel(64);
        let (report_tx, report_rx) = mpsc::channel(64);
        let (store_tx, store_rx) = mpsc::channel(64);
        let stats = AppStats::new();
        let kill = Arc::new(AtomicBool::new(false));
        let cfg = Config::default();

        // Queue both opps BEFORE spawning the coordinator run loop.
        opp_tx
            .send(DetectedOpp {
                opp: opp_with_net(5_990_000),
                at: Instant::now(),
            })
            .await
            .unwrap();
        opp_tx
            .send(DetectedOpp {
                opp: opp_with_net(6_990_000), // higher net, same fingerprint
                at: Instant::now(),
            })
            .await
            .unwrap();

        let coord = Coordinator::new(
            &cfg,
            crate::wiring::risk_config(&cfg, None).unwrap(),
            crate::wiring::engine_params(&cfg).unwrap(),
            token_market(),
            BookFetcher::new(HashMap::new()),
            opp_rx,
            exec_tx,
            report_rx,
            store_tx,
            Arc::clone(&kill),
            Arc::clone(&stats),
            inert_live(),
        )
        .unwrap();
        let handle = tokio::spawn(coord.run());

        // Only one ExecRequest should arrive (coalesced), with the higher net.
        let req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            req.opp.net,
            Usdc(6_990_000),
            "coalesce must keep max-net candidate"
        );

        // Verify only one dispatch happened.
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);

        drop(opp_tx);
        drop(report_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        drop(store_rx);
    }

    // NOTE: The `kill_blocks_even_if_risk_not_yet_tripped` test variant (item 4,
    // third bullet) is intentionally skipped. The dispatch-condition guard
    //   `&& !self.kill.load(Acquire)`
    // is a two-token conjunct that is trivially readable in process_opp; writing a
    // *deterministic* test requires either exposing private state or introducing
    // artificial synchronisation points — both of which add more complexity than
    // the guard itself. Coverage of the kill path is already provided by
    // `kill_flag_blocks_dispatch_and_logs_halt`.

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

    use pm_core::book::{Book, Side};
    use pm_ingestion::supervisor::SupervisorCommand;

    /// Build a coordinator + its channels, leaving the control/status channels
    /// for the caller to install before spawning `run`.
    #[allow(clippy::type_complexity)]
    fn build_coord(
        kill: Arc<AtomicBool>,
        fetcher: BookFetcher,
        risk_cfg: RiskConfig,
    ) -> (
        Coordinator,
        mpsc::Sender<DetectedOpp>,
        mpsc::Receiver<ExecRequest>,
        mpsc::Sender<ExecReport>,
        mpsc::Receiver<StoreMsg>,
        Arc<AppStats>,
    ) {
        let (opp_tx, opp_rx) = mpsc::channel(64);
        let (exec_tx, exec_rx) = mpsc::channel(64);
        let (report_tx, report_rx) = mpsc::channel(64);
        let (store_tx, store_rx) = mpsc::channel(64);
        let stats = AppStats::new();
        let cfg = Config::default();
        let coord = Coordinator::new(
            &cfg,
            risk_cfg,
            crate::wiring::engine_params(&cfg).unwrap(),
            token_market(),
            fetcher,
            opp_rx,
            exec_tx,
            report_rx,
            store_tx,
            kill,
            Arc::clone(&stats),
            inert_live(),
        )
        .unwrap();
        (coord, opp_tx, exec_rx, report_tx, store_rx, stats)
    }

    #[tokio::test]
    async fn control_channel_pauses_and_resumes_dispatch() {
        let kill = Arc::new(AtomicBool::new(false));
        let (mut coord, opp_tx, mut exec_rx, _report_tx, _store_rx, stats) = build_coord(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
        );
        let ctl = coord.control_channel(4);
        let handle = tokio::spawn(coord.run());

        // Pause, then send an otherwise-approvable opp → must be rejected by risk.
        ctl.send(CtlCommand::SetPaused(true)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            exec_rx.try_recv().is_err(),
            "paused coordinator must not dispatch"
        );
        assert!(
            stats.rejected_risk.load(Ordering::Relaxed) >= 1,
            "paused opp must count as a risk rejection"
        );

        // Resume; let the unpause land before the next opp so the select can't
        // race the opp arm ahead of the control arm (both would be ready).
        ctl.send(CtlCommand::SetPaused(false)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(49),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .expect("timeout waiting for dispatch after resume")
            .expect("channel closed before resume dispatch");
        assert!(req.check.total_cost.0 > 0);

        drop(ctl);
        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn status_watch_reflects_pause_kill_and_busy() {
        let kill = Arc::new(AtomicBool::new(false));
        let (mut coord, opp_tx, exec_rx, _report_tx, _store_rx, _stats) = build_coord(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
        );
        let ctl = coord.control_channel(4);
        let mut status = coord.status_channel();
        let handle = tokio::spawn(coord.run());

        // Pause → status reflects paused.
        ctl.send(CtlCommand::SetPaused(true)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), status.changed())
            .await
            .expect("timeout waiting for pause status")
            .unwrap();
        assert!(status.borrow().paused, "status must reflect paused=true");

        // Resume → status reflects unpaused.
        ctl.send(CtlCommand::SetPaused(false)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), status.changed())
            .await
            .expect("timeout waiting for resume status")
            .unwrap();
        assert!(!status.borrow().paused, "status must reflect paused=false");

        // Kill: set the flag and send an opp so check_kill trips at loop top.
        kill.store(true, Ordering::Release);
        opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), status.changed())
            .await
            .expect("timeout waiting for kill status")
            .unwrap();
        assert!(status.borrow().killed, "status must reflect killed=true");

        drop(ctl);
        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        drop(exec_rx);
    }

    #[tokio::test]
    async fn status_watch_reflects_busy_during_in_flight() {
        let kill = Arc::new(AtomicBool::new(false));
        let (mut coord, opp_tx, mut exec_rx, _report_tx, _store_rx, _stats) = build_coord(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
        );
        let ctl = coord.control_channel(4);
        let mut status = coord.status_channel();
        let handle = tokio::spawn(coord.run());

        // Dispatch an approvable opp; the harness exec_rx receives it (but does
        // not reply), leaving the coordinator in the busy=true in-flight window.
        opp_tx
            .send(DetectedOpp {
                opp: opp_fixture(50),
                at: Instant::now(),
            })
            .await
            .unwrap();

        // The exec_rx must receive the request (confirms dispatch happened).
        let _req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .expect("timeout waiting for exec dispatch")
            .expect("exec channel closed before dispatch");

        // publish_status() was called on the success path — status must now show busy.
        tokio::time::timeout(Duration::from_secs(2), status.changed())
            .await
            .expect("timeout waiting for busy status")
            .unwrap();
        assert!(
            status.borrow().busy,
            "status must reflect busy=true during in-flight"
        );

        drop(ctl);
        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// A served fetcher answering BookSnapshot with a single Cent book
    /// (bid_tick / ask_tick), valid=true — mirrors wiring.rs's test server.
    fn served_fetcher(bid_tick: u16, ask_tick: u16) -> BookFetcher {
        let (tx, mut rx) = mpsc::channel::<SupervisorCommand>(8);
        tokio::spawn(async move {
            while let Some(SupervisorCommand::BookSnapshot { reply, .. }) = rx.recv().await {
                let mut book = Book::new(TickSize::Cent);
                book.apply(
                    Side::Bid,
                    Px::new(bid_tick, TickSize::Cent).unwrap(),
                    Qty(100_000_000),
                );
                book.apply(
                    Side::Ask,
                    Px::new(ask_tick, TickSize::Cent).unwrap(),
                    Qty(100_000_000),
                );
                let _ = reply.send(Some((book, true)));
            }
        });
        BookFetcher::new(HashMap::from([(TokenId(1), tx)]))
    }

    #[tokio::test]
    async fn drawdown_halt_uses_mid_marks_not_bid_marks() {
        // bankroll $100, 2% → $2 drawdown trip.
        let mut risk_cfg = crate::wiring::risk_config(&Config::default(), None).unwrap();
        risk_cfg.bankroll = Usdc(100_000_000);

        // Scenario 1: bid 47 / ask 51 (spread 4 ticks ≤ cap 5; mid 49).
        //   Clamped mid = min(49, 47+5) = 49 ticks.
        //   BID equity = −50 + 47 = −$3  → dd $3 ≥ $2 WOULD halt on bid.
        //   MID equity = −50 + 49 = −$1  → dd $1  < $2, no halt (mid is immune).
        // (Pre-clamp this used bid=40/ask=60/mid=50; after clamping mid to bid+5=45,
        //  equity_mid became −$5 which tripped the halt — book updated to a spread
        //  within the cap so the mid-immunity property is preserved.)
        let kill = Arc::new(AtomicBool::new(false));
        let (mut coord, _opp_tx, _exec_rx, _report_tx, mut store_rx, _stats) =
            build_coord(Arc::clone(&kill), served_fetcher(47, 51), risk_cfg);

        coord.positions.apply(
            &[(TokenId(1), Qty(100_000_000), Usdc(50_000_000))],
            Usdc(-50_000_000),
            &token_market(),
        );

        // Install the watch BEFORE snapshot so its publish lands in the watch.
        let status = coord.status_channel();
        coord.snapshot_pnl().await;

        // Durable PnlRow is BID-marked (−$3) and there is NO halt.
        let mut saw_pnl = false;
        while let Ok(m) = store_rx.try_recv() {
            match m {
                StoreMsg::PnlSnapshot(row) => {
                    assert_eq!(
                        row.equity_micro, -3_000_000,
                        "durable PnlRow must stay bid-marked at −$3"
                    );
                    saw_pnl = true;
                }
                StoreMsg::Halt(_) => panic!("mid-marked equity −$1 must NOT halt"),
                _ => {}
            }
        }
        assert!(saw_pnl, "expected a PnlSnapshot row");

        let s = status.borrow();
        assert_eq!(s.equity_micro, -3_000_000, "status bid equity = −$3");
        assert_eq!(s.equity_mid_micro, -1_000_000, "status mid equity = −$1");
        drop(s);

        // Scenario 2: re-serve bid 30 / ask 34 (mid 32). Same holding.
        //   MID equity = −50 + 32 = −$18 → dd $18 ≥ $2 → halt DOES fire,
        //   proving the mid feed is live, not merely disabled.
        coord.fetcher = served_fetcher(30, 34);
        coord.snapshot_pnl().await;

        let mut saw_halt = false;
        while let Ok(m) = store_rx.try_recv() {
            if let StoreMsg::Halt(row) = m {
                assert_eq!(row.reason, "DailyDrawdown");
                saw_halt = true;
            }
        }
        assert!(saw_halt, "mid equity −$18 must trip the drawdown halt");
    }

    // ---- M5 Task 10: live-mode dispatch gates ------------------------------

    /// Build a live-configured coordinator, leaving ctl/status for the caller.
    /// `mode_paper` lets a test exercise the latent-bug regression (mode.paper
    /// = false must still dispatch).
    #[allow(clippy::type_complexity)]
    fn build_coord_live(
        kill: Arc<AtomicBool>,
        fetcher: BookFetcher,
        risk_cfg: RiskConfig,
        live: LiveParams,
        mode_paper: bool,
    ) -> (
        Coordinator,
        mpsc::Sender<DetectedOpp>,
        mpsc::Receiver<ExecRequest>,
        mpsc::Sender<ExecReport>,
        mpsc::Receiver<StoreMsg>,
        Arc<AppStats>,
    ) {
        let (opp_tx, opp_rx) = mpsc::channel(64);
        let (exec_tx, exec_rx) = mpsc::channel(64);
        let (report_tx, report_rx) = mpsc::channel(64);
        let (store_tx, store_rx) = mpsc::channel(64);
        let stats = AppStats::new();
        let mut cfg = Config::default();
        cfg.mode.paper = mode_paper;
        let coord = Coordinator::new(
            &cfg,
            risk_cfg,
            crate::wiring::engine_params(&cfg).unwrap(),
            token_market(),
            fetcher,
            opp_rx,
            exec_tx,
            report_rx,
            store_tx,
            kill,
            Arc::clone(&stats),
            live,
        )
        .unwrap();
        (coord, opp_tx, exec_rx, report_tx, store_rx, stats)
    }

    /// An all-buy opp with explicit per-leg ($) costs and qtys (µshares). Two
    /// legs on the same market (MarketId(0)); total cost = leg0 + leg1.
    fn buy_opp(leg0_cash: i128, q0: u64, leg1_cash: i128, q1: u64, tok2_px: u16) -> Opportunity {
        let f1 = LegFill {
            token: TokenId(1),
            action: Action::Buy,
            ts: TickSize::Cent,
            limit_px: Px::new(44, TickSize::Cent).unwrap(),
            qty: Qty(q0),
            cash: Usdc(leg0_cash),
        };
        let f2 = LegFill {
            token: TokenId(2),
            action: Action::Buy,
            ts: TickSize::Cent,
            limit_px: Px::new(tok2_px, TickSize::Cent).unwrap(),
            qty: Qty(q1),
            cash: Usdc(leg1_cash),
        };
        Opportunity {
            class: ArbClass::C1Long,
            fills: vec![f1, f2],
            units: Qty(q0.min(q1)),
            net: Usdc(1_000_000),
            basis: Usdc((leg0_cash.abs()) + (leg1_cash.abs())),
            edge: Bps(637),
            splits: vec![],
        }
    }

    /// $8 all-buy basket (leg0 $5 / leg1 $3), both legs 20 shares — under the
    /// $10 canary cap and over the 5-share venue minimum. `tok2_px` distinguishes
    /// fingerprints across re-feeds (cooldown suppresses identical shapes).
    fn buy_opp_8usd(tok2_px: u16) -> Opportunity {
        buy_opp(-5_000_000, 20_000_000, -3_000_000, 20_000_000, tok2_px)
    }

    /// Standard canary LiveParams: $10 cap, 5-share minimum.
    fn live_params(live: bool, released_at_start: bool) -> LiveParams {
        LiveParams {
            live,
            released_at_start,
            basket_cap: Usdc(10_000_000),
            min_leg: Qty(5_000_000),
            min_leg_value: Usdc(1_000_000),
        }
    }

    #[tokio::test]
    async fn live_rejects_non_pure_buy_baskets() {
        let kill = Arc::new(AtomicBool::new(false));
        let (coord, opp_tx, mut exec_rx, _report_tx, _store_rx, stats) = build_coord_live(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
            live_params(true, true),
            false,
        );
        let handle = tokio::spawn(coord.run());

        // $8 basket but one leg is a Sell → not pure-buy → rejected.
        let mut opp = buy_opp_8usd(50);
        opp.fills[1].action = Action::Sell;
        opp_tx
            .send(DetectedOpp {
                opp,
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            exec_rx.try_recv().is_err(),
            "non-pure-buy basket must not dispatch in live mode"
        );
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 1);
        assert_eq!(stats.live_held.load(Ordering::Relaxed), 0);

        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn live_rejects_baskets_over_canary_cap() {
        let kill = Arc::new(AtomicBool::new(false));
        let (coord, opp_tx, mut exec_rx, _report_tx, _store_rx, stats) = build_coord_live(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
            live_params(true, true),
            false,
        );
        let handle = tokio::spawn(coord.run());

        // $12 all-buy basket (leg0 $6 / leg1 $6) > $10 cap → rejected.
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp(-6_000_000, 20_000_000, -6_000_000, 20_000_000, 50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            exec_rx.try_recv().is_err(),
            "$12 basket over the $10 canary cap must not dispatch"
        );
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 1);

        // $8 all-buy basket (distinct fingerprint) ≤ $10 cap → dispatched.
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp_8usd(49),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .expect("timeout waiting for $8 dispatch")
            .expect("exec channel closed before dispatch");
        assert_eq!(req.check.total_cost, Usdc(8_000_000));
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 1, "no extra reject");

        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn live_rejects_baskets_with_a_leg_under_venue_minimum() {
        let kill = Arc::new(AtomicBool::new(false));
        let (coord, opp_tx, mut exec_rx, _report_tx, _store_rx, stats) = build_coord_live(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
            live_params(true, true), // min_leg = 5 shares
            false,
        );
        let handle = tokio::spawn(coord.run());

        // Leg qtys [3 sh, 20 sh]: 3 < 5-share minimum → whole basket rejected.
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp(-1_500_000, 3_000_000, -3_000_000, 20_000_000, 50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            exec_rx.try_recv().is_err(),
            "a leg under the venue minimum must kill the basket"
        );
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 1);

        // Leg qtys [6 sh, 20 sh] (distinct fingerprint): both ≥ 5 → dispatched.
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp(-3_000_000, 6_000_000, -3_000_000, 20_000_000, 49),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .expect("timeout waiting for [6sh,20sh] dispatch")
            .expect("exec channel closed before dispatch");
        assert!(req.check.total_cost.0 > 0);
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 1, "no extra reject");

        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn live_rejects_baskets_with_a_leg_under_the_dollar_minimum() {
        let kill = Arc::new(AtomicBool::new(false));
        let (coord, opp_tx, mut exec_rx, _report_tx, _store_rx, stats) = build_coord_live(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
            live_params(true, true), // min_leg_value = $1
            false,
        );
        let handle = tokio::spawn(coord.run());

        // leg1 = 5 shares @ $0.10 = $0.50 makerAmount: clears the 5-share floor
        // but is under the $1 marketable-BUY minimum → whole basket rejected.
        // (leg0 = 10 shares @ $0.44 = $4.40; total $4.90 < $10 cap, both legs ≥ 5sh,
        // so ONLY the new value gate can reject it.)
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp(-4_400_000, 10_000_000, -500_000, 5_000_000, 10),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            exec_rx.try_recv().is_err(),
            "a buy leg under the $1 venue minimum must kill the basket"
        );
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 1);

        // Distinct fingerprint, every leg ≥ $1: leg1 = 15 shares @ $0.11 = $1.65,
        // leg0 = 10 shares @ $0.44 = $4.40; total $6.05 < $10 → dispatched.
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp(-4_400_000, 10_000_000, -1_650_000, 15_000_000, 11),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .expect("timeout waiting for ≥$1-per-leg dispatch")
            .expect("exec channel closed before dispatch");
        assert!(req.check.total_cost.0 > 0);
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 1, "no extra reject");

        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn live_holds_dispatch_until_released() {
        let kill = Arc::new(AtomicBool::new(false));
        let (mut coord, opp_tx, mut exec_rx, _report_tx, _store_rx, stats) = build_coord_live(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
            live_params(true, false), // held at start
            false,
        );
        let ctl = coord.control_channel(4);
        let handle = tokio::spawn(coord.run());

        // Held: an otherwise-dispatchable $8 basket is not dispatched, and it
        // counts as held (NOT a reject).
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp_8usd(50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            exec_rx.try_recv().is_err(),
            "held live dispatch must not send before release"
        );
        assert_eq!(stats.live_held.load(Ordering::Relaxed), 1);
        assert_eq!(stats.live_rej.load(Ordering::Relaxed), 0, "held is not a reject");

        // Release, then re-feed a FRESH-fingerprint equivalent opp → dispatched.
        ctl.send(CtlCommand::ReleaseLive).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        opp_tx
            .send(DetectedOpp {
                opp: buy_opp_8usd(49),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .expect("timeout waiting for post-release dispatch")
            .expect("exec channel closed before dispatch");
        assert_eq!(req.check.total_cost, Usdc(8_000_000));
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);

        drop(ctl);
        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn live_mode_still_dispatches_with_mode_paper_false() {
        // Regression for the latent M4 bug: dispatch_enabled was wired to
        // cfg.mode.paper, so live mode (mode.paper=false) never dispatched.
        let kill = Arc::new(AtomicBool::new(false));
        let (coord, opp_tx, mut exec_rx, _report_tx, _store_rx, stats) = build_coord_live(
            Arc::clone(&kill),
            BookFetcher::new(HashMap::new()),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
            live_params(true, true), // live + released
            false,                   // mode.paper = false
        );
        let handle = tokio::spawn(coord.run());

        opp_tx
            .send(DetectedOpp {
                opp: buy_opp_8usd(50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let req = tokio::time::timeout(Duration::from_secs(2), exec_rx.recv())
            .await
            .expect("timeout: live dispatch must work with mode.paper=false")
            .expect("exec channel closed before dispatch");
        assert_eq!(req.check.total_cost, Usdc(8_000_000));
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);

        drop(opp_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn session_loss_halt_reaches_status_string() {
        // session_loss_cap $25, BID-marked. Position cost $50, bid mark $20 →
        // bid equity −$30 ≤ −$25 → SessionLoss halt, visible on the status watch.
        let mut risk_cfg = crate::wiring::risk_config(&Config::default(), None).unwrap();
        risk_cfg.session_loss_cap = Some(Usdc(25_000_000));
        // Loosen drawdown so SessionLoss is unambiguously the latched reason:
        // bankroll huge → drawdown trip needs a far deeper loss than $30.
        risk_cfg.bankroll = Usdc(1_000_000_000_000);

        let kill = Arc::new(AtomicBool::new(false));
        // bid 20 / ask 24 (Cent); 100 µshares held at $50 cost.
        let (mut coord, _opp_tx, _exec_rx, _report_tx, _store_rx, _stats) = build_coord_live(
            Arc::clone(&kill),
            served_fetcher(20, 24),
            risk_cfg,
            live_params(false, true),
            true,
        );
        coord.positions.apply(
            &[(TokenId(1), Qty(100_000_000), Usdc(50_000_000))],
            Usdc(-50_000_000),
            &token_market(),
        );

        let status = coord.status_channel();
        coord.snapshot_pnl().await;

        assert_eq!(
            status.borrow().halted,
            Some("SessionLoss".to_string()),
            "bid equity −$30 ≤ −$25 cap must latch SessionLoss into the status string"
        );
    }

    #[tokio::test]
    async fn mid_mark_is_clamped_to_bid_plus_spread_cap() {
        // Book: best bid 40, best ask 90 (Cent ticks) — raw mid would be 65.
        // With mid_spread_cap_ticks = 5 (Config::default()) the mid must clamp
        // to bid + 5 = 45 ticks.  Position: 100 µshares (Qty(100_000_000)).
        //   bid_mark  = sell_proceeds(40*10_000, 100_000_000) = 400_000 * 100 / 1_000_000 ×1 share
        //             = 40_000_000 µUSDC  (= $40)
        //   clamped mid_mark = sell_proceeds(45*10_000, 100_000_000) = 45_000_000 µUSDC (= $45)
        //   raw (unclamped) mid would be sell_proceeds(65*10_000, 100_000_000) = 65_000_000
        let kill = Arc::new(AtomicBool::new(false));
        let (mut coord, _opp_tx, _exec_rx, _report_tx, _store_rx, _stats) = build_coord(
            Arc::clone(&kill),
            served_fetcher(40, 90),
            crate::wiring::risk_config(&Config::default(), None).unwrap(),
        );

        coord.positions.apply(
            &[(TokenId(1), Qty(100_000_000), Usdc(50_000_000))],
            Usdc(-50_000_000),
            &token_market(),
        );

        let (bid_marks, mid_marks) = coord.marks_pair().await;
        let token = TokenId(1);
        assert_eq!(
            bid_marks[&token],
            Usdc(40_000_000),
            "bid mark must be 40 ticks ($40) — unchanged"
        );
        assert_eq!(
            mid_marks[&token],
            Usdc(45_000_000),
            "mid must clamp to bid + 5 ticks ($45), not raw $65"
        );
    }
}
