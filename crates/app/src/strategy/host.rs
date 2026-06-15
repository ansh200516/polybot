//! `StrategyHost` — runs N strategies in parallel over the shared ingestion /
//! store / kill handles, with **fault isolation**, **status aggregation**, and
//! **per-strategy control** (multi-strategy platform, Task 1.5).
//!
//! The host owns the set of registered strategies plus their capital envelopes
//! and is responsible for:
//!
//! 1. **Registration** — [`StrategyHost::add`] stores each `Box<dyn Strategy>`
//!    alongside its [`StrategyEnvelope`].
//! 2. **Capital allocation** — [`StrategyHost::validate_capital`] (called by
//!    [`StrategyHost::run`]) runs the startup guard `allocate(Σcapital ≤
//!    bankroll)`; over-allocation is a fatal startup error (the `Err` propagates
//!    out of `run` before any task is spawned).
//! 3. **Combined `on_apply`** — [`StrategyHost::make_on_apply`] builds each
//!    registered strategy's inline hook (the `Some` ones) and returns a single
//!    closure that invokes them in sequence on every `(token, shard)`. It is
//!    rebuilt fresh per call so Task-1.8's main wiring can install one hook per
//!    supervisor (each strategy's `make_on_apply` builds fresh per-supervisor
//!    state, exactly as `main.rs` does for the arb detector today).
//! 4. **Run** — [`StrategyHost::run`] spawns each strategy's `run` future as its
//!    OWN task and returns a [`RunningHost`] handle.
//!
//! ## Fault isolation
//!
//! Each strategy runs in its own [`tokio::spawn`]ed task. Tokio catches a panic
//! at the task-poll boundary: a panicking (or returning) strategy task neither
//! aborts the process nor disturbs its siblings — its [`JoinHandle`] simply
//! resolves to `Err(JoinError)` (with `is_panic()` set). The host's join task
//! awaits every strategy handle and, on a panic/abnormal exit, emits a `warn!`
//! tagged with the [`StrategyId`]; the other strategy tasks keep running and a
//! finished strategy's last-published [`StrategyStatus`] stays visible in the
//! aggregate (its `watch` channel retains the final value).
//!
//! ## Status aggregation (single writer)
//!
//! Every strategy publishes its [`StrategyStatus`] on its own `watch::Sender`
//! (the `status_tx` in its [`StrategyCtx`]). The host exposes one aggregated
//! `watch::Receiver<`[`StrategyStatusView`]`>` for the publisher (Task 1.7/1.8),
//! fed by a strictly single-writer pipeline:
//!
//! * one **forwarder** task per strategy turns that strategy's `watch` changes
//!   into `(`[`StrategyId`]`, `[`StrategyStatus`]`)` deltas on one shared `mpsc`
//!   channel, then
//! * one **aggregator** task — the SOLE writer of the aggregate `watch` — owns
//!   the authoritative vec (seeded with every registered strategy), applies each
//!   delta to the matching slot in place, and republishes the updated vec.
//!
//! Because only the aggregator ever writes the aggregate `watch`, a slow
//! forwarder can never clobber a fresher value with a staler snapshot — the
//! last-writer race a multi-writer (snapshot-per-forwarder) design would have. A
//! forwarder exits when its strategy's `status_tx` drops (the strategy finished);
//! once every forwarder has exited, the delta channel closes, the aggregator
//! returns and drops the aggregate sender, and the aggregate `watch` closes — the
//! same shutdown signal the coordinator's status watch gives the publisher today.
//! A finished strategy keeps its last status in the view (the aggregator never
//! removes a slot).
//!
//! ## Per-strategy control & shutdown
//!
//! Each strategy gets its own `mpsc::Receiver<`[`StrategyCommand`]`>`; the host
//! keeps the senders keyed by [`StrategyId`] and exposes
//! [`RunningHost::pause`] / [`RunningHost::control_sender`]. The **global kill**
//! is the shared `Arc<AtomicBool>` in [`HostShared`] that every strategy already
//! observes — the host adds no extra kill path. `run` "completes" (via
//! [`RunningHost::join`]) once every strategy task has finished; as in `main.rs`
//! today, that is driven by the global kill plus the ingestion/channel cascade,
//! not by the host forcing anything.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use pm_core::num::Usdc;
use pm_ingestion::supervisor::OnApplyFn;
use pm_registry::Registry;
use pm_store::writer::StoreMsg;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::wiring::BookFetcher;

use super::{
    Strategy, StrategyCommand, StrategyCtx, StrategyEnvelope, StrategyId, StrategyStatus, allocate,
};

/// Per-strategy control-channel capacity. Pause commands are rare, so a small
/// fixed buffer is plenty.
const CTL_CAP: usize = 8;

/// Capacity of the shared status-delta channel that feeds the single aggregator.
/// Per-strategy updates coalesce (each forwarder forwards only its strategy's
/// latest), so a modest buffer absorbs bursts; forwarders back-pressure if the
/// aggregator ever lags.
const DELTA_CAP: usize = 64;

/// The aggregated dashboard view: every strategy's id paired with its latest
/// [`StrategyStatus`]. Published on a `watch` channel for the TUI publisher
/// (header equity = Σ per-strategy equity; plus the per-strategy breakdown).
pub type StrategyStatusView = Vec<(StrategyId, StrategyStatus)>;

/// The shared, global handles every strategy receives (the read-mostly ingestion
/// surface, the durable-store sender, and the global kill flag). The host clones
/// these into each strategy's [`StrategyCtx`]; it never owns the underlying
/// singletons (supervisors / store / registry live in `main.rs`).
pub struct HostShared {
    pub registry: Arc<Registry>,
    pub fetcher: BookFetcher,
    pub store_tx: mpsc::Sender<StoreMsg>,
    pub kill: Arc<AtomicBool>,
}

/// Owns the registered strategies + their capital envelopes until [`run`] spawns
/// them. Build with [`new`], register units with [`add`], then consume with
/// [`run`].
///
/// [`new`]: StrategyHost::new
/// [`add`]: StrategyHost::add
/// [`run`]: StrategyHost::run
pub struct StrategyHost {
    bankroll: Usdc,
    entries: Vec<(Box<dyn Strategy>, StrategyEnvelope)>,
}

impl StrategyHost {
    /// Create an empty host with the platform bankroll the capital allocator
    /// validates against.
    pub fn new(bankroll: Usdc) -> Self {
        StrategyHost {
            bankroll,
            entries: Vec::new(),
        }
    }

    /// Register a strategy and its capital/risk envelope. Both are stored; the
    /// envelope feeds the startup capital guard, the strategy is spawned by
    /// [`run`](StrategyHost::run).
    pub fn add(&mut self, strategy: Box<dyn Strategy>, envelope: StrategyEnvelope) {
        self.entries.push((strategy, envelope));
    }

    /// Startup capital guard: Σ per-strategy capital must not exceed the
    /// bankroll. Delegates to [`allocate`]; returns its `Err` (naming the total
    /// vs the bankroll) when over-allocated. Called by [`run`](StrategyHost::run)
    /// before any task is spawned, but exposed so callers can pre-validate.
    pub fn validate_capital(&self) -> Result<(), String> {
        let envelopes: Vec<StrategyEnvelope> =
            self.entries.iter().map(|(_, env)| env.clone()).collect();
        allocate(&envelopes, self.bankroll)
    }

    /// Build the combined per-supervisor inline hook: each registered strategy's
    /// [`make_on_apply`](Strategy::make_on_apply) is built (the `Some` ones), and
    /// a single closure invokes them in sequence on every `(token, shard)`.
    /// Returns `None` when no strategy installs a hook.
    ///
    /// Rebuilt fresh on each call: every invocation re-runs each strategy's
    /// `make_on_apply` (which constructs fresh per-supervisor state, e.g. arb's
    /// `Detector`), so Task-1.8's main wiring calls this once per supervisor.
    pub fn make_on_apply(&self) -> Option<OnApplyFn> {
        let mut hooks: Vec<OnApplyFn> = self
            .entries
            .iter()
            .filter_map(|(strategy, _)| strategy.make_on_apply())
            .collect();
        if hooks.is_empty() {
            return None;
        }
        Some(Box::new(move |token, shard| {
            for hook in hooks.iter_mut() {
                hook(token, shard);
            }
        }))
    }

    /// Spawn every registered strategy as its own fault-isolated task and return
    /// a [`RunningHost`] handle (the aggregated status receiver + the
    /// per-strategy control senders + the completion handle).
    ///
    /// Validates capital first: an over-allocation returns `Err` *before* any
    /// task is spawned (fatal at startup). Must be called from within a Tokio
    /// runtime (it calls [`tokio::spawn`], mirroring `publisher::spawn_publisher`).
    pub fn run(self, shared: HostShared) -> Result<RunningHost, String> {
        // Capital allocation guard FIRST — fatal at startup, before any spawn.
        self.validate_capital()?;
        let entries = self.entries;

        // One shared status-delta channel feeds the single aggregator below.
        let (delta_tx, delta_rx) = mpsc::channel::<(StrategyId, StrategyStatus)>(DELTA_CAP);

        let mut handles: Vec<(StrategyId, JoinHandle<()>)> = Vec::with_capacity(entries.len());
        let mut ctl: HashMap<StrategyId, mpsc::Sender<StrategyCommand>> =
            HashMap::with_capacity(entries.len());
        // Seed the aggregate with every registered strategy (default status) so
        // the view lists all strategies from the very first publish.
        let mut seed: StrategyStatusView = Vec::with_capacity(entries.len());

        for (strategy, _envelope) in entries {
            let id = strategy.id();
            let (status_tx, status_rx) = watch::channel(StrategyStatus::default());
            let (ctl_tx, ctl_rx) = mpsc::channel(CTL_CAP);
            let ctx = StrategyCtx {
                registry: Arc::clone(&shared.registry),
                fetcher: shared.fetcher.clone(),
                store_tx: shared.store_tx.clone(),
                kill: Arc::clone(&shared.kill),
                ctl_rx,
                status_tx,
            };
            // tokio::spawn isolates the strategy: a panic in `run` is caught at
            // the task boundary (handle resolves to Err), never aborting the
            // process or the sibling tasks.
            handles.push((id, tokio::spawn(strategy.run(ctx))));
            ctl.insert(id, ctl_tx);
            seed.push((id, StrategyStatus::default()));
            // One forwarder per strategy turns its watch changes into deltas.
            spawn_forwarder(id, status_rx, delta_tx.clone());
        }
        // Drop the host's delta sender so the channel (and then the aggregate
        // watch) closes once every forwarder — i.e. every strategy — has exited.
        drop(delta_tx);

        // The single aggregator is the ONLY writer of the aggregate watch.
        let (agg_tx, agg_rx) = watch::channel(seed.clone());
        spawn_aggregator(seed, delta_rx, agg_tx);

        // Join task: await every strategy, logging panics/abnormal exits with the
        // StrategyId and tracking whether ANY task ended abnormally. Completes
        // when ALL strategy tasks have finished; its `bool` result surfaces to
        // [`RunningHost::join`] so the caller can fold a dead strategy task into
        // the session health signal (Task 1.8 follow-fix).
        let join_handle = tokio::spawn(async move {
            let mut any_abnormal = false;
            for (id, handle) in handles {
                match handle.await {
                    Ok(()) => {}
                    Err(e) if e.is_panic() => {
                        any_abnormal = true;
                        warn!(
                            strategy = id.0,
                            "strategy task panicked — fault isolated; other strategies keep running"
                        );
                    }
                    Err(_) => {
                        any_abnormal = true;
                        warn!(strategy = id.0, "strategy task ended abnormally (cancelled)");
                    }
                }
            }
            any_abnormal
        });

        Ok(RunningHost {
            status_rx: agg_rx,
            ctl,
            join_handle,
        })
    }
}

/// A live, running host: the aggregated status receiver, the per-strategy control
/// senders, and the completion handle. Returned by [`StrategyHost::run`].
pub struct RunningHost {
    status_rx: watch::Receiver<StrategyStatusView>,
    ctl: HashMap<StrategyId, mpsc::Sender<StrategyCommand>>,
    /// Resolves to `true` if any strategy task ended abnormally (panic/cancel).
    join_handle: JoinHandle<bool>,
}

impl RunningHost {
    /// A fresh receiver of the aggregated per-strategy status view (for the
    /// publisher). Each `borrow()` returns the latest snapshot of every strategy.
    pub fn status(&self) -> watch::Receiver<StrategyStatusView> {
        self.status_rx.clone()
    }

    /// The control sender for one strategy, or `None` if no such strategy is
    /// registered. The clone lets a caller (e.g. the TUI loop) send
    /// [`StrategyCommand`]s on its own cadence.
    pub fn control_sender(&self, id: StrategyId) -> Option<mpsc::Sender<StrategyCommand>> {
        self.ctl.get(&id).cloned()
    }

    /// Convenience: send `SetPaused(paused)` to one strategy. Returns `false` if
    /// the strategy is unknown or its control channel is closed (task gone).
    pub async fn pause(&self, id: StrategyId, paused: bool) -> bool {
        match self.ctl.get(&id) {
            Some(tx) => tx.send(StrategyCommand::SetPaused(paused)).await.is_ok(),
            None => false,
        }
    }

    /// Await completion: resolves once every strategy task has finished (driven
    /// by the global kill + the channel cascade). Drops the control senders (and
    /// the aggregate receiver) BEFORE awaiting, so any strategy that shuts down
    /// on a closed control channel — e.g. the heartbeat when the global kill flag
    /// is not set (a duration / quit / ctrl-c shutdown) — unblocks here. Awaiting
    /// `join_handle` while still holding `ctl` would deadlock such a strategy (and
    /// thus the host); destructuring up front guarantees `ctl` drops first.
    ///
    /// Returns `true` if any strategy TASK ended abnormally (panicked or was
    /// cancelled) — the caller folds this into the session health signal. (A join
    /// task panic is itself treated as abnormal.) Note: a strategy whose `run`
    /// catches an inner failure and returns `Ok` looks normal HERE; such cases
    /// (e.g. arb's coordinator panic) report via their own channel.
    pub async fn join(self) -> bool {
        let RunningHost {
            status_rx,
            ctl,
            join_handle,
        } = self;
        drop(ctl);
        drop(status_rx);
        join_handle.await.unwrap_or(true)
    }
}

/// Spawn one per-strategy forwarder: each `status_rx` change becomes a
/// `(StrategyId, StrategyStatus)` delta on the shared channel carrying that
/// strategy's latest status. Exits when the strategy's `status_tx` drops (it
/// finished) or the aggregator is gone (`send` errors). Forwarders never write
/// the aggregate watch — only the aggregator does.
fn spawn_forwarder(
    id: StrategyId,
    mut status_rx: watch::Receiver<StrategyStatus>,
    delta_tx: mpsc::Sender<(StrategyId, StrategyStatus)>,
) {
    tokio::spawn(async move {
        loop {
            match status_rx.changed().await {
                Ok(()) => {
                    // `borrow_and_update` reads the latest and marks it seen, so a
                    // burst of updates coalesces into the freshest delta.
                    let status = status_rx.borrow_and_update().clone();
                    if delta_tx.send((id, status)).await.is_err() {
                        return; // aggregator gone
                    }
                }
                Err(_) => return, // strategy's status_tx dropped → it finished
            }
        }
    });
}

/// Spawn the single aggregator — the SOLE writer of the aggregate watch. It owns
/// the authoritative view (seeded with every strategy), applies each incoming
/// delta to that strategy's slot in place, and republishes the updated vec, so no
/// staler snapshot can ever clobber a fresher one. When every forwarder has
/// dropped its delta sender (all strategies finished), `recv` returns `None`, the
/// task returns, and dropping `agg_tx` closes the aggregate watch (the
/// publisher's shutdown signal).
fn spawn_aggregator(
    mut view: StrategyStatusView,
    mut delta_rx: mpsc::Receiver<(StrategyId, StrategyStatus)>,
    agg_tx: watch::Sender<StrategyStatusView>,
) {
    // O(1) slot lookup; every strategy is present in the seed.
    let index: HashMap<StrategyId, usize> =
        view.iter().enumerate().map(|(i, (id, _))| (*id, i)).collect();
    tokio::spawn(async move {
        while let Some((id, status)) = delta_rx.recv().await {
            if let Some(&i) = index.get(&id) {
                view[i].1 = status;
                if agg_tx.send(view.clone()).is_err() {
                    return; // publisher dropped the aggregate receiver
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use pm_config::Config;

    use super::*;
    use crate::wiring::risk_config;

    // ---- test fakes -------------------------------------------------------

    /// Publishes one or more `equity_micro` values back-to-back (yielding between
    /// each so a forwarder can observe them), then idles until the global kill
    /// flag is set or its control channel closes. No inline hook.
    struct FixedEquityStrategy {
        id: StrategyId,
        equities: Vec<i64>,
    }

    impl FixedEquityStrategy {
        fn new(id: StrategyId, equity_micro: i64) -> Self {
            FixedEquityStrategy {
                id,
                equities: vec![equity_micro],
            }
        }

        /// Publish a sequence of equity values rapidly (the last is the final,
        /// "true" value) to prove the aggregate settles on the LATEST.
        fn with_sequence(id: StrategyId, equities: Vec<i64>) -> Self {
            FixedEquityStrategy { id, equities }
        }
    }

    impl Strategy for FixedEquityStrategy {
        fn id(&self) -> StrategyId {
            self.id
        }

        fn make_on_apply(&self) -> Option<OnApplyFn> {
            None
        }

        fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                let StrategyCtx {
                    kill,
                    mut ctl_rx,
                    status_tx,
                    ..
                } = ctx;
                let n = self.equities.len();
                for (i, equity_micro) in self.equities.iter().enumerate() {
                    let _ = status_tx.send(StrategyStatus {
                        equity_micro: *equity_micro,
                        ..Default::default()
                    });
                    if i + 1 < n {
                        // Yield so the forwarder can observe this update before the
                        // next one overwrites it (no fixed sleep).
                        tokio::task::yield_now().await;
                    }
                }
                // Idle: poll the global kill (the real shutdown signal) on a short
                // cadence, and react to control commands / channel close. No long
                // sleeps — tests drive shutdown via the kill flag + a timeout.
                loop {
                    if kill.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::select! {
                        cmd = ctl_rx.recv() => match cmd {
                            Some(StrategyCommand::SetPaused(p)) => {
                                status_tx.send_modify(|s| s.paused = p);
                            }
                            None => break, // host dropped the control sender
                        },
                        _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                    }
                }
            })
        }
    }

    /// Panics immediately in `run` to exercise the host's fault isolation.
    struct PanicStrategy {
        id: StrategyId,
    }

    impl PanicStrategy {
        fn new(id: StrategyId) -> Self {
            PanicStrategy { id }
        }
    }

    impl Strategy for PanicStrategy {
        fn id(&self) -> StrategyId {
            self.id
        }

        fn make_on_apply(&self) -> Option<OnApplyFn> {
            None
        }

        fn run(self: Box<Self>, _ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                panic!("PanicStrategy::run panics to exercise host fault isolation");
            })
        }
    }

    // ---- helpers ----------------------------------------------------------

    fn test_risk() -> pm_risk::RiskConfig {
        risk_config(&Config::default(), None).unwrap()
    }

    fn envelope(id: StrategyId, capital_micro: i128) -> StrategyEnvelope {
        StrategyEnvelope::new(id, Usdc(capital_micro), test_risk())
    }

    fn empty_registry() -> Arc<Registry> {
        Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap())
    }

    fn test_shared(kill: Arc<AtomicBool>) -> HostShared {
        // The fakes never touch store_tx; a dropped receiver is harmless.
        let (store_tx, _store_rx) = mpsc::channel(16);
        HostShared {
            registry: empty_registry(),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx,
            kill,
        }
    }

    // ---- tests ------------------------------------------------------------

    /// Two strategies run in parallel and the aggregated view eventually reports
    /// BOTH, with their `equity_micro` summing to 7_000_000.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_runs_two_strategies_and_aggregates() {
        let kill = Arc::new(AtomicBool::new(false));
        let shared = test_shared(Arc::clone(&kill));

        let mut host = StrategyHost::new(Usdc(10_000_000));
        host.add(
            Box::new(FixedEquityStrategy::new(StrategyId("a"), 7_000_000)),
            envelope(StrategyId("a"), 7_000_000),
        );
        host.add(
            Box::new(FixedEquityStrategy::new(StrategyId("b"), 0)),
            envelope(StrategyId("b"), 0),
        );

        let host = host.run(shared).expect("capital validates");
        let mut status = host.status();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = {
                    let view = status.borrow_and_update();
                    view.len() == 2
                        && view.iter().any(|(id, _)| *id == StrategyId("a"))
                        && view.iter().any(|(id, _)| *id == StrategyId("b"))
                        && view.iter().map(|(_, s)| s.equity_micro).sum::<i64>() == 7_000_000
                };
                if done {
                    break;
                }
                if status.changed().await.is_err() {
                    panic!("aggregate view closed before both strategies reported");
                }
            }
        })
        .await
        .expect("timed out waiting for both strategies to aggregate to 7_000_000");

        // Clean shutdown via the global kill, then join (bounded).
        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), host.join())
            .await
            .expect("host did not finish after global kill");
    }

    /// A panicking strategy must NOT bring down the host or its sibling: the
    /// FixedEquity strategy still publishes its status (visible in the aggregate)
    /// and the process stays alive (no propagated panic).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_isolates_a_panicking_strategy() {
        let kill = Arc::new(AtomicBool::new(false));
        let shared = test_shared(Arc::clone(&kill));

        let mut host = StrategyHost::new(Usdc(10_000_000));
        host.add(
            Box::new(PanicStrategy::new(StrategyId("boom"))),
            envelope(StrategyId("boom"), 0),
        );
        host.add(
            Box::new(FixedEquityStrategy::new(StrategyId("ok"), 7_000_000)),
            envelope(StrategyId("ok"), 7_000_000),
        );

        let host = host.run(shared).expect("capital validates");
        let mut status = host.status();

        // The surviving strategy reports despite the sibling's panic.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = {
                    let view = status.borrow_and_update();
                    view.iter()
                        .any(|(id, s)| *id == StrategyId("ok") && s.equity_micro == 7_000_000)
                };
                if done {
                    break;
                }
                if status.changed().await.is_err() {
                    panic!("aggregate view closed before the surviving strategy reported");
                }
            }
        })
        .await
        .expect("panic took down the host or the surviving strategy");

        // We reached here ⇒ the process is alive and no panic propagated.
        kill.store(true, Ordering::Release);
        let _ = tokio::time::timeout(Duration::from_secs(5), host.join()).await;
    }

    /// The single-writer aggregator must settle on the LATEST status when a
    /// strategy emits two rapid updates — a slower sibling forwarder can never
    /// clobber the newer value with a staler snapshot (the last-writer race the
    /// old per-forwarder snapshot writer was prone to).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_aggregate_reflects_latest_of_rapid_updates() {
        let kill = Arc::new(AtomicBool::new(false));
        let shared = test_shared(Arc::clone(&kill));

        let mut host = StrategyHost::new(Usdc(10_000_000));
        // "a" emits a stale value then its final value back-to-back; "b" is a
        // steady sibling whose forwarder is the other source of deltas.
        host.add(
            Box::new(FixedEquityStrategy::with_sequence(
                StrategyId("a"),
                vec![1_000_000, 7_000_000],
            )),
            envelope(StrategyId("a"), 7_000_000),
        );
        host.add(
            Box::new(FixedEquityStrategy::new(StrategyId("b"), 0)),
            envelope(StrategyId("b"), 0),
        );

        let host = host.run(shared).expect("capital validates");
        let mut status = host.status();

        // Converge to "a" == 7_000_000 (its latest, NOT the stale 1_000_000),
        // with "b" present at 0.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = {
                    let view = status.borrow_and_update();
                    view.iter()
                        .any(|(id, s)| *id == StrategyId("a") && s.equity_micro == 7_000_000)
                        && view
                            .iter()
                            .any(|(id, s)| *id == StrategyId("b") && s.equity_micro == 0)
                };
                if done {
                    break;
                }
                if status.changed().await.is_err() {
                    panic!("aggregate view closed before settling on the latest status");
                }
            }
        })
        .await
        .expect("aggregate did not settle on the latest of the rapid updates");

        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), host.join())
            .await
            .expect("host did not finish after global kill");
    }
}
