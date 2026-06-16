//! `ArbStrategy` — the existing arbitrage pipeline (per-supervisor `Detector`
//! inline hook + `Coordinator` + `run_execution` + LP pool) wrapped behind the
//! `Strategy` trait with **byte-identical behavior** (multi-strategy platform,
//! Task 1.4).
//!
//! This is wiring only: nothing in `coordinator.rs`/`detector.rs`/`lp_pool.rs`
//! or `run_execution` changes. `ArbStrategy` owns the arb-internal channels
//! (`opp`/`lp`/`exec`/`report`) and the pipeline's construction inputs;
//! `make_on_apply` builds a fresh `Detector` per supervisor exactly as `main.rs`
//! does today, and `run` spawns the LP pool + execution task and the
//! `Coordinator`, bridging the generic per-strategy control/status channels onto
//! the coordinator's own `control_channel`/`status_channel`.
//!
//! **Why the venue arrives as a builder closure, not a generic `V` field.**
//! `run_execution` is generic over the `ExecutionVenue`, whose methods are
//! `async fn`-in-trait. Those returned futures are *not* automatically `Send`,
//! so the future built from `run_execution::<V>(..)` is only provably `Send`
//! when `V` is **concrete** (the compiler checks the venue's actual async
//! bodies). `main.rs`/`e2e_paper` `tokio::spawn(run_execution(..))` only because
//! they pass a concrete venue in a non-generic function. A generic
//! `ArbStrategy<V>` (or a generic `new::<V>`) is type-checked once over an
//! *abstract* `V`, where that `Send` proof is unavailable — so neither could
//! spawn `run_execution`. The fix: the caller (which holds a concrete venue in a
//! non-generic context — `main.rs::main`, a test) hands `new` an
//! [`ExecTaskBuilder`]: a `Send` closure that, given the arb's `exec`/`report`
//! channel halves and the per-run `store_tx`, returns the boxed `Send`
//! `run_execution` future. `ArbStrategy` stays non-generic and `run_execution`
//! runs verbatim. (This is the spec's "the execution venue **or a way to build
//! it**".)

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pm_config::Config;
use pm_core::instrument::{MarketId, TokenId};
use pm_engine::EngineParams;
use pm_ingestion::supervisor::OnApplyFn;
use pm_risk::RiskConfig;
use pm_store::writer::StoreMsg;
use tokio::sync::{mpsc, watch};
use tracing::error;

use crate::coordinator::{
    CoordStatus, Coordinator, CtlCommand, ExecReport, ExecRequest, LiveParams,
};
use crate::detector::{DetectedOpp, Detector, SolveJob};
use crate::lp_pool::run_lp_pool;
use crate::stats::AppStats;
use crate::wiring::ComponentIndex;

use super::{Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};

/// Channel capacities, lifted verbatim from `main.rs` so the wrapped pipeline
/// back-pressures identically (opp 1024 / lp 64 / exec 4 / report 4).
const OPP_CAP: usize = 1024;
const LP_CAP: usize = 64;
const EXEC_CAP: usize = 4;
const REPORT_CAP: usize = 4;
/// Same capacity `main.rs` passes to `Coordinator::control_channel`.
const CTL_CAP: usize = 8;

/// Builds the execution task (`run_execution`) when `run` hands it the arb's
/// `exec`/`report` channel halves and the per-run `store_tx`. The concrete
/// execution venue (and run_execution's other inputs — the token/market maps,
/// fee map, `ExecParams`) are captured by the *caller* of [`ArbStrategy::new`],
/// where the venue type is concrete; see the module docs for why this can't live
/// inside a generic `new`/`run`. Typically:
///
/// ```ignore
/// let token_market_exec = token_market.clone();
/// let build: ExecTaskBuilder = Box::new(move |exec_rx, report_tx, store_tx| {
///     Box::pin(run_execution(
///         venue, exec_rx, report_tx, store_tx,
///         token_market_exec, market_tokens, token_fee, exec_params,
///     ))
/// });
/// ```
pub type ExecTaskBuilder = Box<
    dyn FnOnce(
            mpsc::Receiver<ExecRequest>,
            mpsc::Sender<ExecReport>,
            mpsc::Sender<StoreMsg>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send,
>;

/// The arbitrage pipeline wrapped as a [`Strategy`]. It owns the arb-internal
/// channels (`opp`/`lp`/`exec`/`report`) plus every input `main.rs` passes when
/// constructing the `Detector`/`Coordinator`; the execution venue arrives as the
/// [`ExecTaskBuilder`] closure (see the module docs). The shared handles
/// (`registry`, `fetcher`, `store_tx`, `kill`) arrive per-run via [`StrategyCtx`].
pub struct ArbStrategy {
    // ---- Coordinator construction inputs ----
    cfg: Config,
    risk_cfg: RiskConfig,
    params: EngineParams,
    token_market: HashMap<TokenId, MarketId>,
    live: LiveParams,
    /// Session-start count for the coordinator's restart-storm guard — a
    /// process-level value `main.rs` reads from the store and forwards via
    /// `note_session_starts` (see the Task-1.8 concern in the PR notes).
    session_starts: usize,

    // ---- Detector (make_on_apply) inputs ----
    index: Arc<ComponentIndex>,
    lp_min_interval: Duration,

    // ---- LP pool input ----
    lp_concurrency: usize,

    // ---- shared ----
    stats: Arc<AppStats>,

    // ---- arb-internal channels (created in `new`) ----
    opp_tx: mpsc::Sender<DetectedOpp>,
    opp_rx: mpsc::Receiver<DetectedOpp>,
    lp_tx: mpsc::Sender<SolveJob>,
    lp_rx: mpsc::Receiver<SolveJob>,
    exec_tx: mpsc::Sender<ExecRequest>,
    exec_rx: mpsc::Receiver<ExecRequest>,
    report_tx: mpsc::Sender<ExecReport>,
    report_rx: mpsc::Receiver<ExecReport>,

    /// The execution task (concrete venue captured by the caller). `run` feeds it
    /// the `exec_rx`/`report_tx` halves + `ctx.store_tx`, then spawns the result.
    exec_builder: ExecTaskBuilder,

    // ---- arb-specific live-release bridge (Task 1.8 routes ReleaseLive here) ----
    live_ctl_tx: mpsc::Sender<CtlCommand>,
    live_ctl_rx: mpsc::Receiver<CtlCommand>,

    // ---- arb-process status watch (Task 1.8) ----
    /// The coordinator's `CoordStatus` republished on a watch created UP FRONT
    /// (in `new`) so the publisher can read the arb-process gates the host
    /// aggregate drops (`live_released`/`busy`) BEFORE `run` consumes the
    /// strategy. `run`'s status bridge forwards every coordinator status onto
    /// `arb_status_tx`; `arb_status_rx()` hands out receivers.
    arb_status_tx: watch::Sender<CoordStatus>,
    arb_status_rx: watch::Receiver<CoordStatus>,

    // ---- coordinator-death health signal (Task 1.8 follow-fix) ----
    /// Set to `true` by `run` ONLY when the coordinator task ends abnormally
    /// (panic/cancel). `ArbStrategy::run` swallows the coordinator `JoinError`
    /// and returns `Ok`, so a host task-outcome check alone cannot see a
    /// mid-session coordinator panic — this flag surfaces it to `main.rs` for
    /// the health / exit-code signal. Created up front so `coordinator_aborted()`
    /// is callable before `run` consumes the strategy. Untouched on the normal
    /// shutdown path (stays `false`), keeping that path byte-identical.
    coordinator_aborted: Arc<AtomicBool>,
}

impl ArbStrategy {
    /// Build the arb strategy. The execution venue is captured (along with
    /// run_execution's other inputs) inside `exec_builder` by the caller — see
    /// [`ExecTaskBuilder`] and the module docs. `ArbStrategy` owns all four
    /// arb-internal channel pairs, created here.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Config,
        risk_cfg: RiskConfig,
        params: EngineParams,
        token_market: HashMap<TokenId, MarketId>,
        index: Arc<ComponentIndex>,
        stats: Arc<AppStats>,
        lp_min_interval: Duration,
        lp_concurrency: usize,
        live: LiveParams,
        session_starts: usize,
        exec_builder: ExecTaskBuilder,
    ) -> Self {
        let (opp_tx, opp_rx) = mpsc::channel(OPP_CAP);
        let (lp_tx, lp_rx) = mpsc::channel(LP_CAP);
        let (exec_tx, exec_rx) = mpsc::channel(EXEC_CAP);
        let (report_tx, report_rx) = mpsc::channel(REPORT_CAP);
        let (live_ctl_tx, live_ctl_rx) = mpsc::channel(CTL_CAP);
        // Created up front so `arb_status_rx()` is callable before `run` consumes
        // the strategy; `run`'s status bridge forwards the coordinator's status
        // onto `arb_status_tx`.
        let (arb_status_tx, arb_status_rx) = watch::channel(CoordStatus::default());
        let coordinator_aborted = Arc::new(AtomicBool::new(false));
        ArbStrategy {
            cfg,
            risk_cfg,
            params,
            token_market,
            live,
            session_starts,
            index,
            lp_min_interval,
            lp_concurrency,
            stats,
            opp_tx,
            opp_rx,
            lp_tx,
            lp_rx,
            exec_tx,
            exec_rx,
            report_tx,
            report_rx,
            exec_builder,
            live_ctl_tx,
            live_ctl_rx,
            arb_status_tx,
            arb_status_rx,
            coordinator_aborted,
        }
    }

    /// Arb-specific live-release control. `StrategyCommand` deliberately omits
    /// the arb/live `ReleaseLive` latch (it is not a generic per-strategy
    /// concern); Task 1.8's main-wiring routes a live-release by sending
    /// `CtlCommand::ReleaseLive` on this sender, which `run` forwards into the
    /// coordinator's own control channel alongside the bridged pause command.
    /// Obtain it BEFORE `run` consumes the strategy.
    pub fn live_release_sender(&self) -> mpsc::Sender<CtlCommand> {
        self.live_ctl_tx.clone()
    }

    /// Arb-process status watch. `StrategyStatus` (the host aggregate) drops the
    /// process-wide gates `live_released`/`busy`, so the publisher reads them
    /// from arb's coordinator `CoordStatus` here. Created up front in `new` and
    /// fed by `run`'s status bridge, so it is obtainable BEFORE `run` consumes
    /// the strategy (Task 1.8). Defaults to `CoordStatus::default()` until the
    /// coordinator publishes its first status.
    pub fn arb_status_rx(&self) -> watch::Receiver<CoordStatus> {
        self.arb_status_rx.clone()
    }

    /// A clone of the coordinator-death flag (Task 1.8 follow-fix). `main.rs`
    /// obtains this BEFORE `run` consumes the strategy and, after the host joins,
    /// folds `coordinator_aborted.load(Acquire)` into the session health signal
    /// (a dead arb coordinator ⇒ `healthy = false` / non-zero exit). Set ONLY on
    /// the coordinator-abort path inside `run`, so it is `false` after a normal
    /// shutdown.
    pub fn coordinator_aborted(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.coordinator_aborted)
    }
}

/// Map the coordinator's `CoordStatus` onto the per-strategy `StrategyStatus`
/// 1:1 on the money/halt/pause fields. The process-wide gates `CoordStatus`
/// carries (`killed`/`busy`/`live`/`live_released`) are intentionally NOT
/// forwarded — they stay on the host/coordinator (see `StrategyStatus`).
fn coord_status_to_strategy(cs: &CoordStatus) -> StrategyStatus {
    StrategyStatus {
        paused: cs.paused,
        halted: cs.halted.clone(),
        cash_micro: cs.cash_micro,
        equity_micro: cs.equity_micro,
        equity_mid_micro: cs.equity_mid_micro,
        realized_micro: cs.realized_micro,
        unrealized_micro: cs.unrealized_micro,
        open_positions: cs.open_positions,
        // Arb harvests no maker rebate — the estimate is MM-specific (Task 4.4).
        rebate_micro: 0,
    }
}

/// Forward the generic per-strategy pause command (and the arb-specific
/// live-release passthrough) into the coordinator's own `CtlCommand` channel.
/// `SetPaused` is translated 1:1; `live_ctl_rx` carries already-`CtlCommand`
/// messages (e.g. `ReleaseLive`) straight through. Ends when both inputs close;
/// a closed `coord_ctl` (coordinator gone) also stops it.
async fn bridge_control(
    mut ctl_rx: mpsc::Receiver<StrategyCommand>,
    mut live_ctl_rx: mpsc::Receiver<CtlCommand>,
    coord_ctl: mpsc::Sender<CtlCommand>,
) {
    let mut ctl_open = true;
    let mut live_open = true;
    while ctl_open || live_open {
        tokio::select! {
            cmd = ctl_rx.recv(), if ctl_open => match cmd {
                Some(StrategyCommand::SetPaused(p)) => {
                    if coord_ctl.send(CtlCommand::SetPaused(p)).await.is_err() {
                        break;
                    }
                }
                None => ctl_open = false,
            },
            cmd = live_ctl_rx.recv(), if live_open => match cmd {
                Some(c) => {
                    if coord_ctl.send(c).await.is_err() {
                        break;
                    }
                }
                None => live_open = false,
            },
        }
    }
}

/// Republish the coordinator's `CoordStatus` watch two ways: the mapped
/// per-strategy `StrategyStatus` for the host to aggregate, and the RAW
/// `CoordStatus` onto `arb_status_tx` for the publisher's arb-process gates
/// (`live_released`/`busy`, which the aggregate intentionally drops — Task 1.8).
/// Forwards the current value, then every change; on coordinator shutdown it
/// forwards the final retained value on BOTH so the discarded
/// `CoordinatorSummary`'s state is never lost. Stops when the host drops the
/// per-strategy receiver (the primary sink); the raw forward is best-effort.
async fn bridge_status(
    mut coord_status: watch::Receiver<CoordStatus>,
    status_tx: watch::Sender<StrategyStatus>,
    arb_status_tx: watch::Sender<CoordStatus>,
) {
    loop {
        {
            let cs = coord_status.borrow_and_update();
            // Raw CoordStatus → publisher (best-effort: a dropped arb_status_rx
            // just means the publisher is gone). Mapped → host aggregate.
            let _ = arb_status_tx.send(cs.clone());
            if status_tx.send(coord_status_to_strategy(&cs)).is_err() {
                return; // host dropped the StrategyStatus receiver
            }
        }
        if coord_status.changed().await.is_err() {
            // Coordinator dropped its sender after its final publish; the watch
            // retains that last value — forward it on both, then stop.
            let cs = coord_status.borrow();
            let _ = arb_status_tx.send(cs.clone());
            let _ = status_tx.send(coord_status_to_strategy(&cs));
            return;
        }
    }
}

impl Strategy for ArbStrategy {
    fn id(&self) -> StrategyId {
        StrategyId("arb")
    }

    /// Build a FRESH `Detector` (per supervisor) capturing clones of the arb
    /// `opp`/`lp` senders + index/params/stats — byte-identical to the
    /// `Detector::new` + `set_on_apply` block `main.rs` runs for each supervisor.
    fn make_on_apply(&self) -> Option<OnApplyFn> {
        let mut det = Detector::new(
            Arc::clone(&self.index),
            self.params,
            self.opp_tx.clone(),
            self.lp_tx.clone(),
            self.lp_min_interval,
            Arc::clone(&self.stats),
        );
        Some(Box::new(move |t, shard| det.on_apply(t, shard)))
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let ArbStrategy {
                cfg,
                risk_cfg,
                params,
                token_market,
                live,
                session_starts,
                index: _,
                lp_min_interval: _,
                lp_concurrency,
                stats,
                opp_tx,
                opp_rx,
                lp_tx,
                lp_rx,
                exec_tx,
                exec_rx,
                report_tx,
                report_rx,
                exec_builder,
                live_ctl_tx: _,
                live_ctl_rx,
                arb_status_tx,
                arb_status_rx: _,
                coordinator_aborted,
            } = *self;

            // ---- Coordinator FIRST: construct it (and handle its `Result`)
            // before spawning ANY task, so a construction failure can't strand /
            // leak the LP-pool or execution tasks. The constructor is infallible
            // today (its only path is `Ok`), but the `Err` arm `return`s, which in
            // a long-running process would detach already-spawned tasks — so the
            // order is a guard, not cosmetics. Constructed exactly as main.rs,
            // with ctx.kill / ctx.store_tx / ctx.fetcher. Spawn order is otherwise
            // behavior-irrelevant: the pipeline is wired by the channels `new`
            // already created.
            let mut coord = match Coordinator::new(
                &cfg,
                risk_cfg,
                params,
                token_market,
                ctx.fetcher,
                opp_rx,
                exec_tx,
                report_rx,
                ctx.store_tx.clone(),
                ctx.kill,
                Arc::clone(&stats),
                live,
            ) {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "ArbStrategy: coordinator construction failed");
                    return;
                }
            };
            coord.note_session_starts(session_starts);
            let coord_ctl = coord.control_channel(CTL_CAP);
            let coord_status = coord.status_channel();

            // ---- LP pool: clone the opp sender BEFORE dropping main's copies,
            // then drop the detector-side senders this strategy held only so
            // `make_on_apply` could clone them — the cascade then mirrors main.rs
            // (the live detectors clone live inside the supervisor tasks).
            let opp_tx_lp = opp_tx.clone();
            drop(opp_tx);
            drop(lp_tx);
            let lp_handle = tokio::spawn(run_lp_pool(
                lp_rx,
                opp_tx_lp,
                params,
                lp_concurrency,
                Arc::clone(&stats),
            ));

            // ---- Execution task: the caller-built `run_execution` future, fed
            // the arb's exec/report halves + ctx.store_tx (see ExecTaskBuilder).
            let exec_handle = tokio::spawn(exec_builder(exec_rx, report_tx, ctx.store_tx.clone()));

            // ---- Coordinator run loop (spawned last, after its peer tasks are up).
            let coord_handle = tokio::spawn(coord.run());

            // ---- Bridges: pause/live-release → coordinator ctl; CoordStatus →
            // per-strategy StrategyStatus.
            // Retain sender clones so the coordinator-abort path can stamp a
            // terminal "coordinator aborted" status AFTER the bridge has forwarded
            // the coordinator's last value (so it isn't overwritten). On the
            // normal path these clones are never written and just drop with `run`.
            let status_tx_marker = ctx.status_tx.clone();
            let arb_status_tx_marker = arb_status_tx.clone();
            let ctl_bridge = tokio::spawn(bridge_control(ctx.ctl_rx, live_ctl_rx, coord_ctl));
            let status_bridge =
                tokio::spawn(bridge_status(coord_status, ctx.status_tx, arb_status_tx));

            // ---- Return when the coordinator loop ends. The `CoordinatorSummary`
            // is discarded (final state already surfaced via status_tx, per the
            // trait doc). A JoinError (panic/cancel) means the coordinator died:
            // record it on `coordinator_aborted` (the health signal `main.rs`
            // folds into the exit code, since this `run` still returns `Ok`) and
            // log at `error!`. Set ONLY here, so the normal path stays untouched.
            if let Err(e) = coord_handle.await {
                coordinator_aborted.store(true, Ordering::Release);
                error!(error = %e, "ArbStrategy: coordinator task ended abnormally");
            }
            let _ = lp_handle.await;
            let _ = exec_handle.await;
            // The coordinator has dropped its status sender; let the status bridge
            // forward that final value before we return.
            let _ = status_bridge.await;
            // The control bridge only lingers if its inputs are still open; with
            // the coordinator gone it is inert, so stop it.
            ctl_bridge.abort();
            let _ = ctl_bridge.await;
            // Failure path ONLY: stamp a terminal "coordinator aborted" marker so
            // the TUI surfaces the dead coordinator (header HALT + per-strategy
            // line). Done AFTER the status bridge so it is the LAST published value
            // (not overwritten by the bridge's final forward); `send_modify`
            // preserves the coordinator's last money fields and only sets `halted`.
            if coordinator_aborted.load(Ordering::Acquire) {
                status_tx_marker
                    .send_modify(|s| s.halted = Some("coordinator aborted".to_string()));
                arb_status_tx_marker
                    .send_modify(|cs| cs.halted = Some("coordinator aborted".to_string()));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use pm_config::Config;
    use pm_core::instrument::{MarketId, TokenId};
    use pm_core::num::{Bps, Px, Qty, TickSize, Usdc, buy_cost, sell_proceeds};
    use pm_engine::{Action, ArbClass, LegFill, Opportunity};
    use pm_execution::Order;
    use pm_execution::basket::ExecParams;
    use pm_execution::venue::{ExecutionVenue, Fill, SubmitOutcome, VenueError};
    use pm_ingestion::supervisor::OnApplyFn;
    use pm_store::Store;
    use pm_store::writer::run_writer;
    use tokio::sync::{mpsc, oneshot, watch};

    use super::{ArbStrategy, ExecTaskBuilder};
    use crate::coordinator::{LiveParams, run_execution};
    use crate::detector::DetectedOpp;
    use crate::stats::AppStats;
    use crate::strategy::host::{HostShared, StrategyHost};
    use crate::strategy::stub::HeartbeatStrategy;
    use crate::strategy::{
        Strategy, StrategyCommand, StrategyCtx, StrategyEnvelope, StrategyId, StrategyStatus,
    };
    use crate::wiring::{BookFetcher, ComponentIndex, engine_params, risk_config};

    /// A venue that records every dispatched basket and blocks the FIRST
    /// `submit_all` on a one-shot gate — so the coordinator stays in the
    /// busy/in-flight window long enough to prove busy-suppression — then fills
    /// every leg clean (mirrors the basket-test `will_fill`) and merges at the
    /// default 10k-µUSDC gas, so a clean C1Long nets the canonical 5_990_000.
    struct GatedVenue {
        dispatched: mpsc::UnboundedSender<Vec<(TokenId, Action, u16, u64)>>,
        gate: Option<oneshot::Receiver<()>>,
    }

    impl GatedVenue {
        fn fill(order: &Order) -> SubmitOutcome {
            let px_micro = order.limit_px.microusdc(order.ts);
            let cash = match order.action {
                Action::Buy => Usdc(-buy_cost(px_micro, order.qty).0),
                Action::Sell => Usdc(sell_proceeds(px_micro, order.qty).0),
            };
            SubmitOutcome {
                fills: vec![Fill {
                    px: order.limit_px,
                    qty: order.qty,
                    cash,
                    fee: Usdc(0),
                }],
                filled: order.qty,
                venue_order_id: None,
            }
        }
    }

    impl ExecutionVenue for GatedVenue {
        async fn submit_fak(&mut self, order: &Order) -> Result<SubmitOutcome, VenueError> {
            Ok(Self::fill(order))
        }

        async fn submit_all(&mut self, orders: &[Order]) -> Vec<Result<SubmitOutcome, VenueError>> {
            let rec: Vec<_> = orders
                .iter()
                .map(|o| (o.token, o.action, o.limit_px.get(), o.qty.0))
                .collect();
            let _ = self.dispatched.send(rec);
            if let Some(gate) = self.gate.take() {
                let _ = gate.await; // block until the test releases the slot
            }
            orders.iter().map(|o| Ok(Self::fill(o))).collect()
        }

        async fn split(&mut self, _market: MarketId, units: Qty) -> Result<Usdc, VenueError> {
            Ok(Usdc(-(units.0 as i128 + 10_000)))
        }

        async fn merge(&mut self, _market: MarketId, units: Qty) -> Result<Usdc, VenueError> {
            Ok(Usdc(units.0 as i128 - 10_000))
        }
    }

    /// C1Long: buy tok1@44¢ + buy tok2@`tok2_ticks`¢, 100 shares each. At
    /// `tok2_ticks=50` the basket costs 94_000_000 µUSDC and a clean fill + merge
    /// nets 5_990_000 — the same numbers `coordinator::tests` and `e2e_paper` use.
    fn c1long_opp(tok2_ticks: u16) -> Opportunity {
        let yes = LegFill {
            token: TokenId(1),
            action: Action::Buy,
            ts: TickSize::Cent,
            limit_px: Px::new(44, TickSize::Cent).unwrap(),
            qty: Qty(100_000_000),
            cash: Usdc(-44_000_000),
        };
        let no = LegFill {
            token: TokenId(2),
            action: Action::Buy,
            ts: TickSize::Cent,
            limit_px: Px::new(tok2_ticks, TickSize::Cent).unwrap(),
            qty: Qty(100_000_000),
            cash: Usdc(-(i128::from(tok2_ticks) * 1_000_000)),
        };
        Opportunity {
            class: ArbClass::C1Long,
            fills: vec![yes, no],
            units: Qty(100_000_000),
            net: Usdc(5_990_000),
            basis: Usdc(94_000_000),
            edge: Bps(637),
            splits: vec![],
        }
    }

    fn maps() -> (
        HashMap<TokenId, MarketId>,
        HashMap<MarketId, (TokenId, TokenId)>,
    ) {
        (
            HashMap::from([(TokenId(1), MarketId(0)), (TokenId(2), MarketId(0))]),
            HashMap::from([(MarketId(0), (TokenId(1), TokenId(2)))]),
        )
    }

    fn empty_index() -> Arc<ComponentIndex> {
        Arc::new(ComponentIndex {
            by_token: HashMap::new(),
            entries: HashMap::new(),
        })
    }

    fn empty_registry() -> Arc<pm_registry::Registry> {
        Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap())
    }

    /// Build an `ArbStrategy` over a `GatedVenue` with the same inert/test inputs
    /// the coordinator tests use (default config, inert live params, empty index).
    /// The `GatedVenue` is concrete here, so the `run_execution` future captured
    /// by the `ExecTaskBuilder` is provably `Send` (see the module docs) — this
    /// is exactly the shape Task 1.8's `main.rs` wiring will use.
    fn build_arb(venue: GatedVenue) -> ArbStrategy {
        let cfg = Config::default();
        let params = engine_params(&cfg).unwrap();
        let risk_cfg = risk_config(&cfg, None).unwrap();
        let (token_market, market_tokens) = maps();
        let token_fee: HashMap<TokenId, Bps> = HashMap::new();
        let exec_params = ExecParams {
            fill_window: Duration::from_millis(500),
            max_unhedged: risk_cfg.max_unhedged,
            redeem: params.redeem,
        };
        // Capture the concrete venue + run_execution's static inputs (the spec's
        // "a way to build it"); `new` owns the channels and feeds them in.
        let token_market_exec = token_market.clone();
        let exec_builder: ExecTaskBuilder = Box::new(move |exec_rx, report_tx, store_tx| {
            Box::pin(run_execution(
                venue,
                exec_rx,
                report_tx,
                store_tx,
                token_market_exec,
                market_tokens,
                token_fee,
                exec_params,
            ))
        });
        ArbStrategy::new(
            cfg,
            risk_cfg,
            params,
            token_market,
            empty_index(),
            AppStats::new(),
            Duration::from_millis(500),
            1,
            LiveParams {
                live: false,
                released_at_start: true,
                basket_cap: Usdc(0),
                min_leg: Qty(0),
                min_leg_value: Usdc(0),
            },
            1,
            exec_builder,
        )
    }

    #[tokio::test]
    async fn arb_make_on_apply_builds_a_detector_hook() {
        let (dispatched, _rx) = mpsc::unbounded_channel();
        let arb = build_arb(GatedVenue {
            dispatched,
            gate: None,
        });
        assert_eq!(arb.id(), StrategyId("arb"));
        assert!(
            arb.make_on_apply().is_some(),
            "arb provides an inline per-supervisor detector hook"
        );
    }

    /// Mirrors `coordinator::tests::dispatches_approved_then_busy_suppresses_then_report_frees`,
    /// but driven through `ArbStrategy::run`: an approvable C1Long reaches the
    /// execution venue as the expected 94_000_000-µUSDC basket; a second
    /// different-shape opp arriving while busy is suppressed; releasing the slot
    /// settles the basket clean and the 5_990_000 final cash is surfaced on the
    /// per-strategy `status_tx` (the discarded `CoordinatorSummary`'s stand-in).
    #[tokio::test]
    async fn arb_strategy_dispatches_like_coordinator() {
        let (dispatched_tx, mut dispatched_rx) = mpsc::unbounded_channel();
        let (gate_tx, gate_rx) = oneshot::channel();
        let arb = build_arb(GatedVenue {
            dispatched: dispatched_tx,
            gate: Some(gate_rx),
        });

        // The detector feeds this exact channel; injecting here is what a
        // supervisor's on_apply hook does (the coordinator reads its rx half).
        let opp_tx = arb.opp_tx.clone();
        let stats = Arc::clone(&arb.stats);

        let kill = Arc::new(AtomicBool::new(false));
        let store = Store::open_in_memory().unwrap();
        let (store_tx, store_rx) = mpsc::channel(256);
        let writer = tokio::spawn(run_writer(store, store_rx));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, status_rx) = watch::channel(StrategyStatus::default());
        let ctx = StrategyCtx {
            registry: empty_registry(),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx,
            kill: Arc::clone(&kill),
            ctl_rx,
            status_tx,
        };
        let run_handle = tokio::spawn(Box::new(arb).run(ctx));

        // 1. Approvable opp → dispatched to the execution venue as the basket.
        opp_tx
            .send(DetectedOpp {
                opp: c1long_opp(50),
                at: Instant::now(),
            })
            .await
            .unwrap();
        let mut legs = tokio::time::timeout(Duration::from_secs(5), dispatched_rx.recv())
            .await
            .expect("timeout waiting for dispatch to the execution venue")
            .expect("dispatched channel closed before dispatch");
        legs.sort_by_key(|&(t, _, _, _)| t.0);
        assert_eq!(
            legs,
            vec![
                (TokenId(1), Action::Buy, 44, 100_000_000),
                (TokenId(2), Action::Buy, 50, 100_000_000),
            ],
            "ArbStrategy must dispatch the approved C1Long basket to the execution venue"
        );
        let total: i128 = legs
            .iter()
            .map(|&(_, _, ticks, qty)| {
                buy_cost(Px::new(ticks, TickSize::Cent).unwrap().microusdc(TickSize::Cent), Qty(qty)).0
            })
            .sum();
        assert_eq!(total, 94_000_000, "basket cost mirrors the coordinator test");
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);

        // 2. A differently-shaped opp while busy → suppressed, no second dispatch.
        opp_tx
            .send(DetectedOpp {
                opp: c1long_opp(49),
                at: Instant::now(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            dispatched_rx.try_recv().is_err(),
            "must not dispatch a second basket while busy"
        );
        assert!(stats.suppressed_busy.load(Ordering::Relaxed) >= 1);
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1, "still one dispatch");

        // 3. Release the slot → basket fills clean and merges back.
        let _ = gate_tx.send(());
        let deadline = Instant::now() + Duration::from_secs(5);
        while stats.baskets_clean.load(Ordering::Relaxed) < 1 {
            assert!(Instant::now() < deadline, "basket never settled clean");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // 4. Shutdown: dropping the opp sender closes opp_rx → coordinator drains,
        //    takes its final snapshot, and `run` returns.
        drop(opp_tx);
        tokio::time::timeout(Duration::from_secs(5), run_handle)
            .await
            .expect("timeout waiting for ArbStrategy::run to return")
            .expect("ArbStrategy::run task panicked");

        // 5. Final state is surfaced via status_tx (CoordinatorSummary discarded).
        let final_status = status_rx.borrow().clone();
        assert_eq!(
            final_status.cash_micro, 5_990_000,
            "final cash bridged onto the per-strategy StrategyStatus"
        );
        assert_eq!(final_status.equity_micro, 5_990_000, "flat after merge → equity == cash");
        assert_eq!(final_status.open_positions, 0);

        drop(writer);
    }

    // ====================================================================
    // Task 1.9 — StrategyHost integration: the REAL `ArbStrategy` (#1) and
    // `HeartbeatStrategy` (#2) running together under the host, proving
    // parallelism + status aggregation + fault isolation.
    //
    // These live in arb.rs's test module (not a `tests/` integration crate or a
    // sibling `#[cfg(test)]` module) ON PURPOSE: the arb harness they reuse —
    // `GatedVenue`, `build_arb`, `c1long_opp`, `empty_registry` — is
    // crate-private to THIS module, so a sibling/external test could not see it.
    // Opps are driven straight into the arb's `opp_tx` exactly as
    // `arb_strategy_dispatches_like_coordinator` does (the channel a supervisor's
    // `on_apply` hook feeds), so no production signature changes are needed.
    // ====================================================================

    /// A per-strategy envelope with the default test risk config. The host's
    /// startup capital guard only sums `capital`, so the risk config is inert
    /// here (the strategies carry their own).
    fn strat_envelope(id: StrategyId, capital_micro: i128) -> StrategyEnvelope {
        StrategyEnvelope::new(
            id,
            Usdc(capital_micro),
            risk_config(&Config::default(), None).unwrap(),
        )
    }

    /// Build the shared host handles backed by a REAL in-memory store + writer
    /// (the arb's coordinator/execution path writes opp/pnl/halt rows, so unlike
    /// the host's fake-strategy tests this must drain a live writer rather than a
    /// dropped receiver). Returns the kill flag and the writer join handle to
    /// keep alive for the test's duration.
    fn host_shared_with_store() -> (
        HostShared,
        Arc<AtomicBool>,
        tokio::task::JoinHandle<Store>,
    ) {
        let kill = Arc::new(AtomicBool::new(false));
        let store = Store::open_in_memory().unwrap();
        let (store_tx, store_rx) = mpsc::channel(256);
        let writer = tokio::spawn(run_writer(store, store_rx));
        let shared = HostShared {
            registry: empty_registry(),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx,
            kill: Arc::clone(&kill),
        };
        (shared, kill, writer)
    }

    /// The legs the approved `c1long_opp(50)` basket must reach the recording
    /// venue as (token-sorted): tok1 buy @44¢ + tok2 buy @50¢, 100 shares each.
    fn expected_basket_legs() -> Vec<(TokenId, Action, u16, u64)> {
        vec![
            (TokenId(1), Action::Buy, 44, 100_000_000),
            (TokenId(2), Action::Buy, 50, 100_000_000),
        ]
    }

    /// A sibling that panics the instant it runs — the "failing strategy" half of
    /// the isolation test. (`host::tests::PanicStrategy` is private to that
    /// module, so this defines its own minimal panicker to run alongside the real
    /// arb.)
    struct BoomStrategy {
        id: StrategyId,
    }

    impl Strategy for BoomStrategy {
        fn id(&self) -> StrategyId {
            self.id
        }

        fn make_on_apply(&self) -> Option<OnApplyFn> {
            None
        }

        fn run(self: Box<Self>, _ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                panic!("BoomStrategy::run panics to exercise host fault isolation beside the real arb");
            })
        }
    }

    /// **Parallelism + aggregation.** Register the REAL `ArbStrategy` and a real
    /// `HeartbeatStrategy` in one `StrategyHost` and assert, all bounded by
    /// `tokio::time::timeout`:
    ///
    /// (a) the arb dispatches the approved C1Long basket to its recording venue;
    /// (b) the heartbeat is genuinely live and publishing in parallel — pausing
    ///     it flips `paused` in the host aggregate (a seeded-but-dead heartbeat
    ///     could never process the command and republish), its money at zero;
    /// (c) the host aggregate (`RunningHost::status()`) lists BOTH strategies with
    ///     the arb's settled 5_990_000 cash reflected and the heartbeat at zero.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_runs_real_arb_and_heartbeat_in_parallel_and_aggregates() {
        // Real arb over a recording venue; no gate → the basket settles clean
        // immediately (the standalone arb test gates only to prove busy
        // suppression, which the coordinator tests already cover).
        let (dispatched_tx, mut dispatched_rx) = mpsc::unbounded_channel();
        let arb = build_arb(GatedVenue {
            dispatched: dispatched_tx,
            gate: None,
        });
        // Grab the arb's opp sender (what a supervisor's on_apply hook feeds) +
        // its stats BEFORE the host consumes the strategy.
        let opp_tx = arb.opp_tx.clone();
        let stats = Arc::clone(&arb.stats);

        let mut host = StrategyHost::new(Usdc(10_000_000));
        // allocate: arb gets the whole bankroll, heartbeat nothing → Σ == bankroll.
        host.add(Box::new(arb), strat_envelope(StrategyId("arb"), 10_000_000));
        host.add(
            Box::new(HeartbeatStrategy::with_interval(
                StrategyId("heartbeat"),
                Duration::from_millis(5),
            )),
            strat_envelope(StrategyId("heartbeat"), 0),
        );

        // The startup capital guard passes (arb=bankroll + heartbeat=0 ≤ bankroll).
        host.validate_capital()
            .expect("arb (=bankroll) + heartbeat (0) must allocate within the bankroll");

        // The combined per-supervisor hook builds with the arb's detector hook
        // present (the heartbeat contributes none). We drive opps directly down
        // the channel below (like the arb test), so DROP the hook — holding it
        // would pin the arb's lp_tx/opp_tx clones and block clean shutdown.
        let on_apply = host.make_on_apply();
        assert!(
            on_apply.is_some(),
            "combined on_apply must be Some — the real arb installs a per-supervisor detector hook"
        );
        drop(on_apply);

        let (shared, kill, writer) = host_shared_with_store();
        let running = host.run(shared).expect("capital validates → run spawns the strategies");
        let mut status = running.status();

        // (a) The real arb dispatches the approved basket to its recording venue.
        opp_tx
            .send(DetectedOpp {
                opp: c1long_opp(50),
                at: Instant::now(),
            })
            .await
            .expect("arb opp channel must be open");
        let mut legs = tokio::time::timeout(Duration::from_secs(5), dispatched_rx.recv())
            .await
            .expect("timeout waiting for the real arb to dispatch under the host")
            .expect("dispatch channel closed before the arb dispatched");
        legs.sort_by_key(|&(t, _, _, _)| t.0);
        assert_eq!(
            legs,
            expected_basket_legs(),
            "the arb must dispatch the approved basket while the heartbeat runs in parallel"
        );
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);

        // (b) The heartbeat is live + publishing in parallel: pause it and watch
        // `paused` flip in the aggregate, money still zero.
        assert!(
            running.pause(StrategyId("heartbeat"), true).await,
            "the heartbeat's control channel must be live"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let hb_alive = {
                    let view = status.borrow_and_update();
                    view.iter().any(|(id, s)| {
                        *id == StrategyId("heartbeat")
                            && s.paused
                            && s.equity_micro == 0
                            && s.cash_micro == 0
                    })
                };
                if hb_alive {
                    break;
                }
                if status.changed().await.is_err() {
                    panic!("aggregate closed before the heartbeat reflected its pause");
                }
            }
        })
        .await
        .expect("timeout waiting for the heartbeat's (zero) status to reach the aggregate");

        // (c) Close the arb's opp channel so its coordinator drains and takes the
        // final P&L snapshot (the only sub-pnl-interval one) — its 5_990_000
        // settled cash then reaches the aggregate. The heartbeat keeps running.
        drop(opp_tx);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let both = {
                    let view = status.borrow_and_update();
                    view.len() == 2
                        && view.iter().any(|(id, s)| {
                            *id == StrategyId("arb")
                                && s.cash_micro == 5_990_000
                                && s.equity_micro == 5_990_000
                                && s.open_positions == 0
                        })
                        && view.iter().any(|(id, s)| {
                            *id == StrategyId("heartbeat")
                                && s.equity_micro == 0
                                && s.cash_micro == 0
                        })
                };
                if both {
                    break;
                }
                if status.changed().await.is_err() {
                    panic!("aggregate closed before the arb's settled cash reached it");
                }
            }
        })
        .await
        .expect("timeout waiting for the aggregate to reflect arb=5_990_000 + heartbeat=0");
        assert!(
            stats.baskets_clean.load(Ordering::Relaxed) >= 1,
            "the dispatched basket must have settled clean"
        );

        // Clean shutdown: the global kill stops the heartbeat (the arb already
        // finished when its opp channel closed); join is bounded.
        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), running.join())
            .await
            .expect("host did not finish after the global kill");
        drop(writer);
    }

    /// **Fault isolation.** A panicking sibling registered ALONGSIDE the real
    /// arb: the host catches the panic at the task boundary, and the arb must
    /// keep running — it still dispatches its basket AND its settled cash still
    /// reaches the aggregate. (The existing `host::tests` panic test proves the
    /// same for a *fake* survivor; this proves it for the real arb pipeline.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_isolates_a_failing_sibling_from_the_real_arb() {
        let (dispatched_tx, mut dispatched_rx) = mpsc::unbounded_channel();
        let arb = build_arb(GatedVenue {
            dispatched: dispatched_tx,
            gate: None,
        });
        let opp_tx = arb.opp_tx.clone();
        let stats = Arc::clone(&arb.stats);

        let mut host = StrategyHost::new(Usdc(10_000_000));
        host.add(
            Box::new(BoomStrategy {
                id: StrategyId("boom"),
            }),
            strat_envelope(StrategyId("boom"), 0),
        );
        host.add(Box::new(arb), strat_envelope(StrategyId("arb"), 10_000_000));

        let on_apply = host.make_on_apply();
        assert!(
            on_apply.is_some(),
            "the real arb still installs its detector hook beside the panicking sibling"
        );
        drop(on_apply);

        let (shared, kill, writer) = host_shared_with_store();
        let running = host.run(shared).expect("capital validates → run spawns the strategies");
        let mut status = running.status();

        // The real arb dispatches its basket despite the sibling having panicked.
        opp_tx
            .send(DetectedOpp {
                opp: c1long_opp(50),
                at: Instant::now(),
            })
            .await
            .expect("arb opp channel must be open");
        let mut legs = tokio::time::timeout(Duration::from_secs(5), dispatched_rx.recv())
            .await
            .expect("timeout: the arb must dispatch even though its sibling panicked")
            .expect("dispatch channel closed before the arb dispatched");
        legs.sort_by_key(|&(t, _, _, _)| t.0);
        assert_eq!(
            legs,
            expected_basket_legs(),
            "the surviving real arb must still dispatch the approved basket"
        );
        assert_eq!(stats.dispatched.load(Ordering::Relaxed), 1);

        // ...and the aggregate still updates for the arb (the host's status
        // pipeline survives the sibling's panic). Close the opp channel to force
        // the final snapshot.
        drop(opp_tx);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let arb_settled = {
                    let view = status.borrow_and_update();
                    view.iter().any(|(id, s)| {
                        *id == StrategyId("arb")
                            && s.cash_micro == 5_990_000
                            && s.open_positions == 0
                    })
                };
                if arb_settled {
                    break;
                }
                if status.changed().await.is_err() {
                    panic!("aggregate closed before the surviving arb's cash reached it");
                }
            }
        })
        .await
        .expect("timeout: the aggregate must still update for the arb after the sibling panicked");
        assert!(stats.baskets_clean.load(Ordering::Relaxed) >= 1);

        // Standard shutdown signal (the arb already drained via its closed opp
        // channel; the panicked sibling is long gone); join is bounded.
        kill.store(true, Ordering::Release);
        let _ = tokio::time::timeout(Duration::from_secs(5), running.join()).await;
        drop(writer);
    }
}
