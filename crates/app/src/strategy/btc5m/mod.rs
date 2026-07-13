//! BTC "Up or Down 5m" strategy (spec 2026-07-13). Phase 0/1 is READ-ONLY:
//! it prices a fair P(up) and logs it against the live book; it emits NO orders.
pub mod entry;
pub mod market;
pub mod model;
pub mod settle;
pub mod shadow;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pm_ingestion::gamma::{GammaClient, GammaWindow};
use pm_ingestion::spot::SpotFeed;
use pm_store::writer::StoreMsg;
use tokio::time::MissedTickBehavior;

use super::{Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};
use crate::strategy::btc5m::market::Rotation;
use crate::strategy::btc5m::model::fair_p_up;
use crate::strategy::btc5m::shadow::ShadowSample;

/// Read-only Phase-0/1 shadow strategy. Holds either a live `GammaClient` +
/// `SpotFeed` (production) or a pre-seeded window+spot (tests). It emits NO
/// orders: each tick it prices a fair `p_up`, reads the YES-token book, and
/// records a [`ShadowSample`] to the store — nothing else.
pub struct Btc5mStrategy {
    id: StrategyId,
    sample_ms: u64,
    /// HTTP client + CLOB REST base for polling the current window's YES-token
    /// `/book` directly (the dynamically-discovered 5m token is in neither the
    /// shared registry nor any WS supervisor, so `ctx.fetcher` can't see it).
    book_http: reqwest::Client,
    clob_base: String,
    gamma: Option<GammaClient>,
    slug_fn: Option<Box<dyn Fn(i64) -> String + Send>>,
    spot: Option<SpotFeed>,
    seed: Option<(GammaWindow, f64, f64)>, // (window, strike, spot); sigma via seed_sigma
    seed_sigma: f64,
}

impl Btc5mStrategy {
    pub fn new(
        gamma: GammaClient,
        slug_fn: Box<dyn Fn(i64) -> String + Send>,
        spot: SpotFeed,
        sample_ms: u64,
        book_http: reqwest::Client,
        clob_base: String,
    ) -> Self {
        Btc5mStrategy {
            id: StrategyId("btc5m"),
            sample_ms,
            book_http,
            clob_base,
            gamma: Some(gamma),
            slug_fn: Some(slug_fn),
            spot: Some(spot),
            seed: None,
            seed_sigma: 0.0,
        }
    }

    pub fn new_for_test(
        window: GammaWindow,
        strike: f64,
        spot: f64,
        sigma_1min: f64,
        sample_ms: u64,
    ) -> Self {
        Btc5mStrategy {
            id: StrategyId("btc5m"),
            sample_ms,
            // Never used in tests: the seed=Some guard below skips the book poll.
            book_http: reqwest::Client::new(),
            clob_base: String::new(),
            gamma: None,
            slug_fn: None,
            spot: None,
            seed: Some((window, strike, spot)),
            seed_sigma: sigma_1min,
        }
    }

    fn now_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

impl Strategy for Btc5mStrategy {
    fn id(&self) -> StrategyId {
        self.id
    }

    fn make_on_apply(&self) -> Option<pm_ingestion::supervisor::OnApplyFn> {
        None
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // `registry`/`fetcher` intentionally dropped: they only know the
            // static arb/mm/copy token universe + its WS supervisors, never the
            // dynamically-discovered 5m window token (see the CLOB poll below).
            let StrategyCtx {
                store_tx,
                kill,
                mut ctl_rx,
                status_tx,
                ..
            } = ctx;
            let mut paused = false;
            let mut rot = Rotation::default();
            let me = *self;

            if let Some((w, strike, _spot)) = me.seed.clone() {
                rot.adopt(w, strike);
            }

            let mut tick = tokio::time::interval(Duration::from_millis(me.sample_ms.max(1)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut gamma_poll = tokio::time::interval(Duration::from_millis(1000));
            gamma_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                if kill.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = gamma_poll.tick() => {
                        if let (Some(g), Some(sf), Some(slug_fn)) = (me.gamma.as_ref(), me.spot.as_ref(), me.slug_fn.as_ref()) {
                            let now = Self::now_ms();
                            let slug = slug_fn(now);
                            if let Ok(Some(w)) = g.current_window(&slug).await {
                                rot.adopt(w, sf.latest().price);
                            }
                        }
                    }
                    _ = tick.tick() => {
                        if paused { continue; }
                        let now = Self::now_ms();
                        let win = match rot.current() { Some(w) => w.clone(), None => continue };
                        let (spot, sigma_1min, vol_ready) = match (me.spot.as_ref(), &me.seed) {
                            (Some(sf), _) => { let s = sf.latest(); (s.price, s.sigma_1min, s.vol_ready) }
                            (None, Some((_, _, seed_spot))) => (*seed_spot, me.seed_sigma, true),
                            _ => continue,
                        };
                        if !vol_ready || !spot.is_finite() { continue; }
                        let secs = win.secs_to_go(now);
                        let sigma_tau = sigma_1min * ((secs.max(0) as f64) / 60.0).sqrt();
                        let p_up = match fair_p_up(spot, win.strike, secs as f64, sigma_1min) { Some(p) => p, None => continue };

                        // Poll the public CLOB /book directly for the rotating
                        // window's YES token — it is discovered dynamically via
                        // Gamma, so `ctx.fetcher`/`registry` (static universe + WS
                        // supervisors) can't see it. µUSDC comes straight from the
                        // parse. READ-ONLY sampling. Tests (seed=Some) skip the
                        // network so the book stays 0; the row still logs fair value.
                        let (mut bid_micro, mut ask_micro) = (0i64, 0i64);
                        if me.seed.is_none()
                            && let Ok((bid, ask)) = pm_ingestion::clob::fetch_book_best(
                                &me.book_http,
                                &me.clob_base,
                                &win.gamma.yes_token,
                            )
                            .await
                        {
                            bid_micro = bid.unwrap_or(0);
                            ask_micro = ask.unwrap_or(0);
                        }

                        let sample = ShadowSample {
                            ts_ms: now, condition_id: win.gamma.condition_id.clone(), secs_to_go: secs,
                            strike: win.strike, spot, sigma_tau, p_up,
                            best_bid_micro: bid_micro, best_ask_micro: ask_micro,
                            tick_decimals: win.gamma.tick_decimals,
                        };
                        let _ = store_tx.try_send(StoreMsg::Btc5mShadow(sample.into_row()));
                        let _ = status_tx.send(StrategyStatus { paused, open_positions: 0, ..Default::default() });
                    }
                    cmd = ctl_rx.recv() => match cmd {
                        Some(StrategyCommand::SetPaused(p)) => paused = p,
                        Some(StrategyCommand::VetoQuote { .. }) => {}
                        None => break,
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::{mpsc, watch};
    use pm_store::writer::StoreMsg;
    use crate::strategy::{StrategyCommand, StrategyCtx, StrategyStatus};
    use crate::wiring::BookFetcher;
    use super::*;

    #[tokio::test]
    async fn shadow_loop_writes_a_row_and_no_orders() {
        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let (store_tx, mut store_rx) = mpsc::channel::<StoreMsg>(16);
        let ctx = StrategyCtx {
            registry: Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap()),
            fetcher: BookFetcher::new(HashMap::new()),
            store_tx, kill: Arc::clone(&kill), ctl_rx, status_tx,
        };
        let strat = Btc5mStrategy::new_for_test(
            pm_ingestion::gamma::GammaWindow {
                condition_id: "C".into(), yes_token: "999".into(), no_token: "998".into(),
                tick_decimals: 2, t_open_ms: 0, t_close_ms: 300_000 },
            62_900.0, 62_940.0, 40.0, 5,
        );
        let run = tokio::spawn(Box::new(strat).run(ctx));
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), store_rx.recv())
            .await.expect("no store message within timeout").expect("store sender dropped");
        match msg {
            StoreMsg::Btc5mShadow(r) => {
                assert_eq!(r.condition_id, "C");
                assert!(r.p_up > 0.5, "spot above strike ⇒ p_up > 0.5, got {}", r.p_up);
                assert_eq!(r.best_ask_micro, 0, "unknown token ⇒ no book");
            }
            _ => panic!("expected StoreMsg::Btc5mShadow, got a different variant"),
        }
        kill.store(true, Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_secs(5), run).await.unwrap().unwrap();
    }
}
