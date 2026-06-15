//! Strategy identity, per-strategy capital envelope, the startup capital
//! allocator, and the `Strategy` boundary — the trait plus its runtime
//! `StrategyCtx` and per-strategy `StrategyStatus` (multi-strategy platform,
//! Tasks 1.2–1.3). The allocator is a pure startup guard: Σ per-strategy
//! capital must not exceed the bankroll. No `StrategyHost` yet, and arb is not
//! wrapped as a `Strategy` yet — those arrive in later tasks.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use pm_core::num::Usdc;
use pm_ingestion::supervisor::OnApplyFn;
use pm_registry::Registry;
use pm_risk::RiskConfig;
use pm_store::writer::StoreMsg;
use tokio::sync::{mpsc, watch};

use crate::wiring::BookFetcher;

pub mod arb;
pub mod host;

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

/// One strategy's slice of the dashboard state, published on a `watch` channel
/// for the host to aggregate. The per-strategy analogue of the coordinator's
/// `CoordStatus`: the money fields are µUSDC (display conversion is the TUI's),
/// mirroring the columns `publisher.rs` reads — `cash_micro`, `equity_micro`
/// (bid-marked, reporting), `equity_mid_micro` (mid-marked, risk/halt feed),
/// `realized_micro`, `unrealized_micro` — plus the latched halt reason and the
/// pause flag. Field order matches `CoordStatus` (minus the process-wide gates
/// `killed`/`live`/`busy`, which stay on the host/coordinator) so the host's
/// Task-1.7 aggregation maps 1:1.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StrategyStatus {
    pub paused: bool,
    pub halted: Option<String>,
    pub cash_micro: i64,
    pub equity_micro: i64,
    pub equity_mid_micro: i64,
    pub realized_micro: i64,
    pub unrealized_micro: i64,
    pub open_positions: usize,
}

/// Neutral per-strategy control command from the host (TUI-translated),
/// decoupled from the coordinator. Pause is the only control for now;
/// per-strategy and global kill flow through `StrategyCtx.kill`, not here.
/// Arb-specific controls (e.g. the coordinator's live-release latch) stay on
/// arb's own path — Task 1.4 bridges `SetPaused` into the coordinator's pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyCommand {
    SetPaused(bool),
}

/// The runtime handles a strategy's `run` loop owns: shared read-only ingestion
/// (`registry`, `fetcher`), the durable-store sender, the global kill flag, its
/// own control-command stream, and its status publisher. The host builds one
/// per strategy; the `ctl_rx`/`status_tx` pair is the per-strategy control and
/// dashboard channel (the same shape the coordinator exposes via
/// `control_channel`/`status_channel`).
pub struct StrategyCtx {
    pub registry: Arc<Registry>,
    pub fetcher: BookFetcher,
    pub store_tx: mpsc::Sender<StoreMsg>,
    pub kill: Arc<AtomicBool>,
    pub ctl_rx: mpsc::Receiver<StrategyCommand>,
    pub status_tx: watch::Sender<StrategyStatus>,
}

/// A self-contained trading unit the `StrategyHost` runs in parallel. `Send`
/// (not `Sync`): the host owns each unit and drives it on its own task. A unit
/// gets market data two ways, either or both: an inline per-supervisor
/// `on_apply` hook (arb's hot path) and/or its owned async loop reading via
/// `ctx.fetcher` on its own cadence. `run` returns a boxed future (rather than
/// being an `async fn`) so the trait stays dyn-compatible — the host holds
/// units as `Box<dyn Strategy>`.
pub trait Strategy: Send {
    /// Stable identity (status-map key / log label).
    fn id(&self) -> StrategyId;
    /// Per-supervisor inline hook (arb uses this); `None` if the strategy reads
    /// books on its own cadence. `OnApplyFn` is exactly the type
    /// `Supervisor::set_on_apply` consumes.
    fn make_on_apply(&self) -> Option<OnApplyFn>;
    /// The strategy's owned async loop; resolves when the strategy ends. Final
    /// state is reported out-of-band via `ctx.status_tx` (the host reads the
    /// last published `StrategyStatus`), so the future's `Output` is `()` by
    /// design rather than the spec's `StrategySummary`.
    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>>;
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

    /// Minimal `Strategy`: a stable id, no inline hook, and a `run` loop that
    /// observes the kill flag once and returns. Lives in the test module — the
    /// host's real heartbeat stub arrives with `StrategyHost` (Task 1.5).
    struct NoopStrategy {
        id: StrategyId,
    }

    impl NoopStrategy {
        fn new(id: StrategyId) -> Self {
            NoopStrategy { id }
        }
    }

    impl Strategy for NoopStrategy {
        fn id(&self) -> StrategyId {
            self.id
        }

        fn make_on_apply(&self) -> Option<OnApplyFn> {
            None
        }

        fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                // A real strategy loops until kill/ctl close; the noop reads the
                // kill flag once (proving ctx is wired) and returns immediately.
                let _ = ctx.kill.load(std::sync::atomic::Ordering::Relaxed);
            })
        }
    }

    #[tokio::test]
    async fn strategy_trait_object_reports_id_and_status() {
        let s: Box<dyn Strategy> = Box::new(NoopStrategy::new(StrategyId("noop")));
        assert_eq!(s.id(), StrategyId("noop"));
        assert!(s.make_on_apply().is_none());
    }
}
