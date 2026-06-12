//! AppState publisher (spec §17): the ~10 Hz task that assembles the dashboard
//! snapshot from the read-only store, the coordinator status watch, the
//! ingestion stats cells, the app stats, and the tracing ring buffer.
//!
//! ## Money rule
//!
//! Every µUSDC→USD conversion in this module is **display only** (`f64`). None
//! of these values is ever fed back into accounting — the durable money math
//! lives in the coordinator/positions/store in integer µUSDC.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pm_core::instrument::TokenId;
use pm_core::num::{Qty, sell_proceeds};
use pm_registry::Registry;
use pm_store::read::ReadStore;
use pm_tui::state::{AppState, FillLine, Health, OppLine, OrderLine, PositionLine};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::coordinator::{CoordStatus, now_ms};
use crate::logbuf::LogBuffer;
use crate::stats::AppStats;
use crate::wiring::BookFetcher;

/// Display-only µUSDC → USD (`micro / 1e6`). Never fed back into accounting.
pub fn usd(micro: i64) -> f64 {
    micro as f64 / 1e6
}

/// Resolve a token id to a display name: `"Question YES"` / `"Question NO"`.
/// Falls back to the truncated condition id, then to `"token N"`.
pub fn market_display(reg: &Registry, token_i64: i64) -> String {
    let token = TokenId(token_i64 as u64);
    match reg.market_of_token(token) {
        Some(m) => {
            let side = if m.yes == token { "YES" } else { "NO" };
            match reg.question(m.id) {
                Some(q) => format!("{q} {side}"),
                None => {
                    // No Gamma question — fall back to a truncated condition id.
                    let cond = reg.market_condition(m.id).unwrap_or("?");
                    let short: String = cond.chars().take(10).collect();
                    format!("{short} {side}")
                }
            }
        }
        None => format!("token {token_i64}"),
    }
}

/// Summarize a legs JSON array: the first leg's market name plus `" (+k)"` for
/// the `k` remaining legs. Returns `"?"` if the JSON does not parse.
pub fn legs_market_summary(reg: &Registry, legs_json: &str) -> String {
    let legs: serde_json::Value = match serde_json::from_str(legs_json) {
        Ok(v) => v,
        Err(_) => return "?".to_string(),
    };
    let Some(arr) = legs.as_array() else {
        return "?".to_string();
    };
    let Some(first) = arr.first() else {
        return "?".to_string();
    };
    let token = match first.get("token").and_then(|t| t.as_i64()) {
        Some(t) => t,
        None => return "?".to_string(),
    };
    let base = market_display(reg, token);
    let extra = arr.len().saturating_sub(1);
    if extra > 0 {
        format!("{base} (+{extra})")
    } else {
        base
    }
}

/// Everything the publisher reads each tick. Owns the read store, the stats
/// handles, the status watch receiver, and the display helpers' inputs.
pub struct PublisherCtx {
    pub read: ReadStore,
    pub stats: Arc<AppStats>,
    pub cells: Vec<Arc<pm_ingestion::stats::StatsCell>>,
    pub status_rx: watch::Receiver<CoordStatus>,
    pub registry: Arc<Registry>,
    pub logbuf: Arc<LogBuffer>,
    pub fetcher: BookFetcher,
    pub feed_rows: usize,
    pub fills_rows: usize,
    pub log_lines: usize,
    pub mode_paper: bool,
    pub start: Instant,
    pub last_frames: u64,
    pub last_at: Instant,
}

/// Assemble one dashboard snapshot. Store-read failures degrade panels to empty
/// (never panic, never block the producer path — see `ReadStore`'s rationale).
pub async fn assemble(ctx: &mut PublisherCtx) -> AppState {
    let now = now_ms();
    let status = ctx.status_rx.borrow().clone();

    let age_s = |ts_ms: i64| -> u64 { ((now - ts_ms).max(0) / 1000) as u64 };

    // --- Opportunities -----------------------------------------------------
    let opportunities = ctx
        .read
        .recent_opportunities(ctx.feed_rows)
        .unwrap_or_default()
        .into_iter()
        .map(|o| OppLine {
            age_s: age_s(o.ts_ms),
            class: o.class,
            market: legs_market_summary(&ctx.registry, &o.legs_json),
            edge_bps: o.edge_bps,
            size_shares: o.units_micro as f64 / 1e6,
            est_profit_usd: usd(o.net_micro),
            dispatched: o.dispatched,
        })
        .collect();

    // --- Fills -------------------------------------------------------------
    let fills = ctx
        .read
        .recent_fills(ctx.fills_rows)
        .unwrap_or_default()
        .into_iter()
        .map(|f| FillLine {
            ago_s: age_s(f.ts_ms),
            market: market_display(&ctx.registry, f.token),
            action: f.action,
            // Display-only price: ticks / tick_levels (e.g. 44/100 = "0.44").
            px: format!("{:.2}", f.px_ticks as f64 / f.tick_levels.max(1) as f64),
            qty_shares: f.qty_micro as f64 / 1e6,
            cash_usd: usd(f.cash_micro),
        })
        .collect();

    // --- Orders (durable order ledger) -------------------------------------
    let orders = ctx
        .read
        .recent_orders(ctx.feed_rows)
        .unwrap_or_default()
        .into_iter()
        .map(|o| OrderLine {
            ago_s: age_s(o.ts_ms),
            order_id_short: o.order_id.chars().take(8).collect(),
            state: o.state,
            detail: o.detail,
        })
        .collect();

    // --- Positions (marked at the current bid) -----------------------------
    let mut positions = Vec::new();
    for (token, qty_micro, cost_micro) in ctx.read.open_positions().unwrap_or_default() {
        let mark_usd = match ctx.fetcher.fetch(TokenId(token as u64)).await {
            Some((book, true)) => match book.bids.best() {
                Some(bid) => {
                    let proceeds = sell_proceeds(bid.microusdc(book.ts()), Qty(qty_micro as u64));
                    usd(i64::try_from(proceeds.0).unwrap_or(i64::MAX))
                }
                None => 0.0,
            },
            _ => 0.0,
        };
        positions.push(PositionLine {
            market: market_display(&ctx.registry, token),
            qty_shares: qty_micro as f64 / 1e6,
            basis_usd: usd(cost_micro),
            mark_usd,
        });
    }

    // --- Ingestion gauges (summed over all stats cells) --------------------
    let mut books = 0u64;
    let mut stale = 0u64;
    let mut frames = 0u64;
    let mut reconnects = 0u64;
    let mut parse_errors = 0u64;
    for cell in &ctx.cells {
        books += cell.books.load(Ordering::Relaxed);
        stale += cell.stale.load(Ordering::Relaxed);
        frames += cell.frames.load(Ordering::Relaxed);
        reconnects += cell.reconnects.load(Ordering::Relaxed);
        parse_errors += cell.parse_errors.load(Ordering::Relaxed);
    }
    let dt = ctx.last_at.elapsed().as_secs_f64();
    let frames_per_s = if dt > 0.0 {
        (frames.saturating_sub(ctx.last_frames)) as f64 / dt
    } else {
        0.0
    };
    // Quiet-universe flicker limitation: on a live-but-silent delta feed no new
    // frames arrive, so `frames > last_frames` reads false and ws_connected
    // flickers off even though the socket is healthy. We accept this for M4 —
    // the empty-cells case (no ingestion wired) reports connected so unit/dev
    // setups don't show a spurious disconnect. A heartbeat-based liveness signal
    // is the proper fix (M5).
    let ws_connected = frames > ctx.last_frames || ctx.cells.is_empty();
    ctx.last_frames = frames;
    ctx.last_at = Instant::now();

    // --- Latency histograms ------------------------------------------------
    let (detect_p50_us, detect_p99_us) = match ctx.stats.detect_us.lock() {
        Ok(h) => (h.value_at_quantile(0.50), h.value_at_quantile(0.99)),
        Err(_) => (0, 0),
    };
    let (dispatch_p50_us, dispatch_p99_us) = match ctx.stats.dispatch_us.lock() {
        Ok(h) => (h.value_at_quantile(0.50), h.value_at_quantile(0.99)),
        Err(_) => (0, 0),
    };

    let lp_jobs = ctx.stats.lp_jobs.load(Ordering::Relaxed);
    let lp_solved = ctx.stats.lp_solved.load(Ordering::Relaxed);
    let health = Health {
        ws_connected,
        books,
        stale,
        frames,
        frames_per_s,
        reconnects,
        parse_errors,
        detect_p50_us,
        detect_p99_us,
        dispatch_p50_us,
        dispatch_p99_us,
        opps_emitted: ctx.stats.opps_emitted.load(Ordering::Relaxed),
        admitted: ctx.stats.admitted.load(Ordering::Relaxed),
        dispatched: ctx.stats.dispatched.load(Ordering::Relaxed),
        baskets_clean: ctx.stats.baskets_clean.load(Ordering::Relaxed),
        baskets_repaired: ctx.stats.baskets_repaired.load(Ordering::Relaxed),
        baskets_unwound: ctx.stats.baskets_unwound.load(Ordering::Relaxed),
        solver_queue: lp_jobs.saturating_sub(lp_solved),
        lp_solved,
    };

    AppState {
        uptime_s: ctx.start.elapsed().as_secs(),
        mode_paper: ctx.mode_paper,
        paused: status.paused,
        halted: status.halted,
        killed: status.killed,
        busy: status.busy,
        cash_usd: usd(status.cash_micro),
        equity_usd: usd(status.equity_micro),
        equity_mid_usd: usd(status.equity_mid_micro),
        realized_usd: usd(status.realized_micro),
        unrealized_usd: usd(status.unrealized_micro),
        opportunities,
        positions,
        fills,
        orders,
        health,
        log: ctx.logbuf.tail(ctx.log_lines),
    }
}

/// Spawn the publisher task. Returns a watch receiver seeded with a default
/// snapshot and the task handle. The loop exits when the coordinator's status
/// sender is dropped (`has_changed().is_err()`) or all snapshot receivers are
/// dropped (`send().is_err()`).
pub fn spawn_publisher(
    mut ctx: PublisherCtx,
    refresh: Duration,
) -> (watch::Receiver<Arc<AppState>>, JoinHandle<()>) {
    let (tx, rx) = watch::channel(Arc::new(AppState::default()));
    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(refresh);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            // Coordinator gone → nothing left to publish.
            if ctx.status_rx.has_changed().is_err() {
                break;
            }
            let state = assemble(&mut ctx).await;
            if tx.send(Arc::new(state)).is_err() {
                break; // no receivers left
            }
        }
    });
    (rx, handle)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::TickSize;
    use pm_registry::RegistryBuilder;

    fn reg() -> std::sync::Arc<pm_registry::Registry> {
        let mut b = RegistryBuilder::default();
        b.add_market(
            "0xa",
            "ya",
            "na",
            TickSize::Cent,
            0,
            false,
            Some("Will X win?".into()),
            true,
            false,
            None,
        );
        std::sync::Arc::new(b.finish("").unwrap())
    }

    #[test]
    fn market_display_resolves_question_and_side() {
        let r = reg();
        let m = r.markets()[0];
        assert_eq!(market_display(&r, m.yes.0 as i64), "Will X win? YES");
        assert_eq!(market_display(&r, m.no.0 as i64), "Will X win? NO");
        assert_eq!(market_display(&r, 999), "token 999");
    }

    #[test]
    fn legs_market_summary_uses_first_leg_plus_count() {
        let r = reg();
        let m = r.markets()[0];
        let json = format!(
            "[{{\"token\":{},\"action\":\"Buy\",\"px\":44,\"qty\":1}},{{\"token\":{},\"action\":\"Buy\",\"px\":50,\"qty\":1}}]",
            m.yes.0, m.no.0
        );
        assert_eq!(legs_market_summary(&r, &json), "Will X win? YES (+1)");
        assert_eq!(legs_market_summary(&r, "not json"), "?");
    }

    #[test]
    fn usd_conversion_is_display_only_micro_over_1e6() {
        assert_eq!(usd(5_990_000), 5.99);
        assert_eq!(usd(-44_000_000), -44.0);
    }

    #[tokio::test]
    async fn assemble_produces_display_state_from_store_and_status() {
        let r = reg();
        let m = r.markets()[0];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sqlite");
        {
            let mut s = pm_store::Store::open(&path).unwrap();
            s.insert_opportunity(&pm_store::OppRow {
                ts_ms: crate::coordinator::now_ms(),
                class: "C1Long".into(),
                fingerprint: "f".into(),
                edge_bps: 637,
                units_micro: 100_000_000,
                net_micro: 5_990_000,
                basis_micro: 94_000_000,
                legs_json: format!(
                    "[{{\"token\":{},\"action\":\"Buy\",\"px\":44,\"qty\":1}}]",
                    m.yes.0
                ),
                dispatched: true,
            })
            .unwrap();
            s.insert_order(&pm_store::OrderRow {
                id: "0192abcd-rest-of-uuid".into(),
                ts_ms: 1,
                fingerprint: "f".into(),
                token: m.yes.0 as i64,
                action: "Buy".into(),
                limit_ticks: 44,
                tick_levels: 100,
                qty_micro: 100_000_000,
            })
            .unwrap();
            s.insert_fill(&pm_store::FillRow {
                order_id: "0192abcd-rest-of-uuid".into(),
                ts_ms: crate::coordinator::now_ms(),
                token: m.yes.0 as i64,
                action: "Buy".into(),
                px_ticks: 44,
                tick_levels: 100,
                qty_micro: 100_000_000,
                cash_micro: -44_000_000,
                fee_micro: 0,
            })
            .unwrap();
        }
        let read = pm_store::read::ReadStore::open(&path).unwrap();

        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            use pm_core::book::{Book, Side};
            use pm_core::num::{Px, Qty};
            while let Some(pm_ingestion::supervisor::SupervisorCommand::BookSnapshot {
                reply,
                ..
            }) = cmd_rx.recv().await
            {
                let mut b = Book::new(TickSize::Cent);
                b.apply(
                    Side::Bid,
                    Px::new(42, TickSize::Cent).unwrap(),
                    Qty(200_000_000),
                );
                b.apply(
                    Side::Ask,
                    Px::new(46, TickSize::Cent).unwrap(),
                    Qty(200_000_000),
                );
                let _ = reply.send(Some((b, true)));
            }
        });
        let fetcher =
            crate::wiring::BookFetcher::new(std::collections::HashMap::from([(m.yes, cmd_tx)]));

        let stats = crate::stats::AppStats::new();
        stats
            .opps_emitted
            .fetch_add(7, std::sync::atomic::Ordering::Relaxed);
        let logbuf = crate::logbuf::LogBuffer::new(10);
        {
            use tracing_subscriber::layer::SubscriberExt;
            let sub = tracing_subscriber::registry().with(crate::logbuf::RingLayer::new(
                std::sync::Arc::clone(&logbuf),
            ));
            tracing::subscriber::with_default(sub, || tracing::info!("hello dashboard"));
        }
        let (_status_tx, status_rx) =
            tokio::sync::watch::channel(crate::coordinator::CoordStatus {
                equity_micro: 5_990_000,
                equity_mid_micro: 6_100_000,
                ..Default::default()
            });

        let mut ctx = PublisherCtx {
            read,
            stats,
            cells: Vec::new(),
            status_rx,
            registry: r,
            logbuf,
            fetcher,
            feed_rows: 50,
            fills_rows: 20,
            log_lines: 50,
            mode_paper: true,
            start: std::time::Instant::now(),
            last_frames: 0,
            last_at: std::time::Instant::now(),
        };
        let state = assemble(&mut ctx).await;
        assert_eq!(state.opportunities.len(), 1);
        assert_eq!(state.opportunities[0].market, "Will X win? YES");
        assert!((state.fills[0].cash_usd + 44.0).abs() < 1e-9);
        assert_eq!(state.positions.len(), 1);
        assert!((state.positions[0].mark_usd - 42.0).abs() < 1e-9);
        assert!((state.equity_usd - 5.99).abs() < 1e-9);
        assert!((state.equity_mid_usd - 6.10).abs() < 1e-9);
        assert_eq!(state.health.opps_emitted, 7);
        assert_eq!(state.orders.len(), 1);
        assert_eq!(state.orders[0].order_id_short, "0192abcd");
        assert!(state.log.iter().any(|(_, l)| l.contains("hello dashboard")));
        // _status_tx must outlive assemble (watch borrow); keep the binding.
        drop(_status_tx);
    }
}
