//! `HeartbeatStrategy` — the trivial production "strategy #2" that proves the
//! `StrategyHost` runs a second strategy in parallel with arb, fully isolated
//! (multi-strategy platform, Task 1.6).
//!
//! It observes NO market data (`make_on_apply` is `None`, so it emits no orders
//! and touches no execution) and reads no books. Its entire job is to republish
//! a zero-valued [`StrategyStatus`] on a fixed interval so the host's status
//! aggregation always sees a live second strategy, and to honor the generic
//! `SetPaused` control (reflected in the published status — a heartbeat has no
//! orders to actually pause, but the field mirrors the control). It exits
//! cleanly when the global kill flag is set or its control channel closes.
//!
//! YAGNI: no execution, no book reads, no config beyond an optional tick
//! interval. It exists purely as a harness-validation strategy.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pm_ingestion::supervisor::OnApplyFn;
use tokio::time::MissedTickBehavior;

use super::{Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};

/// Default tick cadence. A heartbeat only needs to publish on a steady, slow
/// beat — slow enough to be negligible in production, while tests inject a short
/// interval via [`HeartbeatStrategy::with_interval`].
const DEFAULT_INTERVAL: Duration = Duration::from_millis(250);

/// A minimal [`Strategy`] that publishes a zero-valued [`StrategyStatus`] on a
/// fixed interval and otherwise does nothing. Its only state is the `paused`
/// flag toggled by [`StrategyCommand::SetPaused`], reflected in each published
/// status. Used to validate that the host runs strategies in parallel and in
/// isolation.
pub struct HeartbeatStrategy {
    id: StrategyId,
    interval: Duration,
}

impl HeartbeatStrategy {
    /// A heartbeat with the [`DEFAULT_INTERVAL`] tick cadence.
    pub fn new(id: StrategyId) -> Self {
        HeartbeatStrategy {
            id,
            interval: DEFAULT_INTERVAL,
        }
    }

    /// A heartbeat with a caller-chosen tick interval (tests use a short one).
    pub fn with_interval(id: StrategyId, interval: Duration) -> Self {
        HeartbeatStrategy { id, interval }
    }
}

impl Strategy for HeartbeatStrategy {
    fn id(&self) -> StrategyId {
        self.id
    }

    /// The heartbeat observes no market data, so it installs no per-supervisor
    /// inline hook — and therefore emits no orders.
    fn make_on_apply(&self) -> Option<OnApplyFn> {
        None
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let interval = self.interval;
        Box::pin(async move {
            let StrategyCtx {
                kill,
                mut ctl_rx,
                status_tx,
                ..
            } = ctx;
            let mut paused = false;
            let mut tick = tokio::time::interval(interval);
            // A heartbeat wants a steady beat, not a catch-up burst after a
            // stall, so skip missed ticks rather than firing them back-to-back.
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                // The global kill is the real shutdown signal; observe it each
                // iteration (the immediate first `tick()` makes the cadence the
                // poll interval — short in tests).
                if kill.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = tick.tick() => {
                        // Republish a zero-valued status carrying only the live
                        // `paused` flag. A send error means the host dropped the
                        // receiver — nothing left to report to, so stop.
                        let status = StrategyStatus {
                            paused,
                            ..Default::default()
                        };
                        if status_tx.send(status).is_err() {
                            break;
                        }
                    }
                    cmd = ctl_rx.recv() => match cmd {
                        Some(StrategyCommand::SetPaused(p)) => paused = p,
                        Some(StrategyCommand::VetoQuote { .. }) => {}
                        None => break, // host dropped the control sender
                    },
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use tokio::sync::{mpsc, watch};

    use super::*;
    use crate::wiring::BookFetcher;

    fn empty_registry() -> Arc<pm_registry::Registry> {
        Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap())
    }

    /// Build a minimal [`StrategyCtx`] for the heartbeat: it never touches the
    /// registry/fetcher/store, so those are inert (a dropped store receiver is
    /// harmless). The caller owns the kill flag and the control/status channels.
    fn test_ctx(
        kill: Arc<AtomicBool>,
        ctl_rx: mpsc::Receiver<StrategyCommand>,
        status_tx: watch::Sender<StrategyStatus>,
    ) -> StrategyCtx {
        let (store_tx, _store_rx) = mpsc::channel(16);
        StrategyCtx {
            registry: empty_registry(),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx,
            kill,
            ctl_rx,
            status_tx,
        }
    }

    /// The heartbeat installs no inline hook (no orders) and publishes a
    /// zero-valued status on its `watch` sender within a bounded timeout.
    #[tokio::test]
    async fn heartbeat_publishes_status_and_has_no_hook() {
        let hb = HeartbeatStrategy::with_interval(StrategyId("hb"), Duration::from_millis(5));
        assert_eq!(hb.id(), StrategyId("hb"));
        assert!(
            hb.make_on_apply().is_none(),
            "heartbeat observes no market data → no inline hook"
        );

        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, mut status_rx) = watch::channel(StrategyStatus::default());
        // Mark the channel's initial value seen so `changed()` only fires on the
        // heartbeat's own publish (not the creation-time default).
        status_rx.borrow_and_update();
        let ctx = test_ctx(Arc::clone(&kill), ctl_rx, status_tx);

        let run = tokio::spawn(Box::new(hb).run(ctx));

        tokio::time::timeout(Duration::from_secs(5), status_rx.changed())
            .await
            .expect("heartbeat did not publish a status within the timeout")
            .expect("heartbeat status sender dropped without publishing");
        assert_eq!(
            *status_rx.borrow(),
            StrategyStatus::default(),
            "heartbeat publishes a zero-valued status (no orders, not paused)"
        );

        // Clean shutdown via the global kill (bounded).
        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("heartbeat did not exit after kill")
            .expect("heartbeat run task panicked");
    }

    /// Setting the global kill makes `run` return within a bounded timeout (no
    /// hang).
    #[tokio::test]
    async fn heartbeat_exits_on_kill() {
        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let hb = HeartbeatStrategy::with_interval(StrategyId("hb"), Duration::from_millis(5));
        let ctx = test_ctx(Arc::clone(&kill), ctl_rx, status_tx);

        let run = tokio::spawn(Box::new(hb).run(ctx));
        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("heartbeat did not exit within the timeout after kill")
            .expect("heartbeat run task panicked");
    }
}
