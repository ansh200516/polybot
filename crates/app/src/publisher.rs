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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pm_core::book::Side;
use pm_core::instrument::TokenId;
use pm_core::num::{Qty, sell_proceeds};
use pm_registry::Registry;
use pm_store::read::ReadStore;
use pm_tui::state::{
    AppState, FillLine, Health, OpenOrderLine, OppLine, OrderLine, PositionLine, StrategyLine,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::coordinator::{CoordStatus, now_ms};
use crate::logbuf::LogBuffer;
use crate::stats::AppStats;
use crate::strategy::host::StrategyStatusView;
use crate::strategy::{RestingOrderSnapshot, StrategyId, StrategyStatus};
use crate::wiring::BookFetcher;

/// Display-only µUSDC → USD (`micro / 1e6`). Never fed back into accounting.
pub fn usd(micro: i64) -> f64 {
    micro as f64 / 1e6
}

/// Summed per-strategy money in µUSDC, produced by [`aggregate_money`] when the
/// publisher is driven by the `StrategyHost`'s aggregated view: the dashboard
/// header equity/cash/etc. become the SUM of every strategy's micros. Held as
/// integer µUSDC only to defer rounding — the µUSDC→USD step still happens at
/// the `AppState` boundary via [`usd`]; like every value here it is display
/// only and never fed back into accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AggregatedMoney {
    pub cash_micro: i64,
    pub equity_micro: i64,
    pub equity_mid_micro: i64,
    pub realized_micro: i64,
    pub unrealized_micro: i64,
    /// Σ per-strategy maker-rebate ESTIMATE (Task 4.4), µUSDC. Summed like the
    /// other money fields but kept DISTINCT — it is an unverified, out-of-band
    /// estimate and is never folded into `equity`/`cash`/`realized` (which would
    /// inflate position P&L). Only the MM strategy contributes; arb/heartbeat
    /// leave it 0.
    pub rebate_micro: i64,
}

/// Sum every strategy's money fields across the host's aggregated view. Pure.
/// Saturating adds so a pathological strategy can't panic the publisher in a
/// debug build (realistic totals sit far below i64's µUSDC range).
pub fn aggregate_money(view: &[(StrategyId, StrategyStatus)]) -> AggregatedMoney {
    let mut m = AggregatedMoney::default();
    for (_, s) in view {
        m.cash_micro = m.cash_micro.saturating_add(s.cash_micro);
        m.equity_micro = m.equity_micro.saturating_add(s.equity_micro);
        m.equity_mid_micro = m.equity_mid_micro.saturating_add(s.equity_mid_micro);
        m.realized_micro = m.realized_micro.saturating_add(s.realized_micro);
        m.unrealized_micro = m.unrealized_micro.saturating_add(s.unrealized_micro);
        // The maker-rebate estimate sums alongside the others but stays a
        // SEPARATE field (never added into equity/cash/realized).
        m.rebate_micro = m.rebate_micro.saturating_add(s.rebate_micro);
    }
    m
}

/// One display-only [`StrategyLine`] per strategy in the host's aggregated view,
/// in view order. Pure: µUSDC→USD via [`usd`], id via `StrategyId.0`, and the
/// per-strategy `paused`/`halted` carried through verbatim. The per-strategy
/// analogue of the header money — display only, never fed back into accounting.
pub fn strategy_lines(view: &[(StrategyId, StrategyStatus)]) -> Vec<StrategyLine> {
    view.iter()
        .map(|(id, s)| StrategyLine {
            id: id.0.to_string(),
            equity_usd: usd(s.equity_micro),
            cash_usd: usd(s.cash_micro),
            realized_usd: usd(s.realized_micro),
            unrealized_usd: usd(s.unrealized_micro),
            open_positions: s.open_positions,
            paused: s.paused,
            halted: s.halted.clone(),
        })
        .collect()
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

/// Turn one strategy's [`RestingOrderSnapshot`] into a display [`OpenOrderLine`].
/// The `key` is the opaque `"<token>:<b|a>"` handle the cancel/un-veto command
/// carries back (decoded in `main.rs`); a vetoed slot shows no live price/size.
fn open_order_line(strategy: &str, r: &RestingOrderSnapshot, reg: &Registry) -> OpenOrderLine {
    let (side_label, side_char) = match r.side {
        Side::Bid => ("Bid", 'b'),
        Side::Ask => ("Ask", 'a'),
    };
    let px = if r.vetoed {
        "—".to_string()
    } else {
        format!(
            "{:.2}",
            f64::from(r.px_ticks) / f64::from(r.tick_levels.max(1))
        )
    };
    OpenOrderLine {
        strategy: strategy.to_string(),
        market: market_display(reg, r.token.0 as i64),
        side: side_label.to_string(),
        px,
        qty_shares: r.qty_micro as f64 / 1e6,
        vetoed: r.vetoed,
        key: format!("{}:{}", r.token.0, side_char),
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
    /// Optional aggregated per-strategy status from the `StrategyHost` (multi-
    /// strategy platform, Task 1.7/1.8). When `Some` (production wiring, Task
    /// 1.8), [`assemble`] sources the header money from the SUM of every
    /// strategy's micros, fills `AppState.per_strategy`, and reconciles the
    /// header badges from the aggregate + `kill` + `arb_status_rx` (see below).
    /// When `None` (legacy single-strategy wiring) the publisher behaves exactly
    /// as before, sourcing both money and badges from the single `CoordStatus`.
    pub strategy_status_rx: Option<watch::Receiver<StrategyStatusView>>,
    /// Global kill flag (multi-strategy platform, Task 1.8). On the wired path
    /// (`strategy_status_rx` is `Some`) the `killed` header badge is sourced from
    /// this flag directly — the host aggregate's per-strategy `StrategyStatus`
    /// has no process-wide `killed` gate. Ignored on the `None` path (badges
    /// still come from `CoordStatus`).
    pub kill: Arc<AtomicBool>,
    /// Arb's coordinator `CoordStatus` watch (multi-strategy platform, Task 1.8).
    /// On the wired path it supplies the arb-process gates the aggregate drops —
    /// `live_released` + `busy` — for the header badges (default `false` when
    /// `None`). Ignored on the `None` path.
    pub arb_status_rx: Option<watch::Receiver<CoordStatus>>,
    pub registry: Arc<Registry>,
    pub logbuf: Arc<LogBuffer>,
    pub fetcher: BookFetcher,
    pub feed_rows: usize,
    pub fills_rows: usize,
    pub log_lines: usize,
    pub mode_paper: bool,
    /// `--live --shadow`: signs but never submits. Display only — forwarded into
    /// AppState so the header shows the distinct (non-red) SHADOW badge.
    pub shadow: bool,
    pub start: Instant,
    pub last_frames: u64,
    pub last_at: Instant,
}

/// Assemble one dashboard snapshot. Store-read failures degrade panels to empty
/// (never panic, never block the producer path — see `ReadStore`'s rationale).
pub async fn assemble(ctx: &mut PublisherCtx) -> AppState {
    let now = now_ms();

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
            strategy: f.strategy,
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

    // --- Positions (per (token, strategy); SIGNED net incl. shorts) --------
    // Sign/mark convention: a positive net is a LONG, marked at the best BID
    // (what we'd sell into) → a positive mark. A negative net is a SHORT,
    // marked at the best ASK (what it'd cost to buy back) → a negative mark (a
    // liability). Both `qty_shares` and `mark_usd` carry the sign so the panel
    // shows e.g. `-5.0` shares with a negative mark. Longs are byte-identical
    // to before (bid-marked via `sell_proceeds`).
    let mut positions = Vec::new();
    for (token, strategy, net_micro, cost_micro) in ctx.read.open_positions().unwrap_or_default() {
        let mark_usd = match ctx.fetcher.fetch(TokenId(token as u64)).await {
            Some((book, true)) => {
                if net_micro >= 0 {
                    match book.bids.best() {
                        Some(bid) => {
                            let proceeds =
                                sell_proceeds(bid.microusdc(book.ts()), Qty(net_micro as u64));
                            usd(i64::try_from(proceeds.0).unwrap_or(i64::MAX))
                        }
                        None => 0.0,
                    }
                } else {
                    match book.asks.best() {
                        // Cost to buy back the short → a negative mark.
                        Some(ask) => {
                            let cost = sell_proceeds(
                                ask.microusdc(book.ts()),
                                Qty(net_micro.unsigned_abs()),
                            );
                            -usd(i64::try_from(cost.0).unwrap_or(i64::MAX))
                        }
                        None => 0.0,
                    }
                }
            }
            _ => 0.0,
        };
        positions.push(PositionLine {
            strategy,
            market: market_display(&ctx.registry, token),
            qty_shares: net_micro as f64 / 1e6,
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
    let mut feeds_up: u64 = 0;
    let mut oldest_frame_age_s: u64 = 0;
    for cell in &ctx.cells {
        books += cell.books.load(Ordering::Relaxed);
        stale += cell.stale.load(Ordering::Relaxed);
        frames += cell.frames.load(Ordering::Relaxed);
        reconnects += cell.reconnects.load(Ordering::Relaxed);
        parse_errors += cell.parse_errors.load(Ordering::Relaxed);

        if cell.connected.load(Ordering::Relaxed) {
            feeds_up += 1;
        }

        // Cells that have never received a frame (last_frame_ms == 0) are
        // treated as age 0 — a brand-new session should not inflate the gauge.
        let last_ms = cell.last_frame_ms.load(Ordering::Relaxed);
        if last_ms > 0 {
            let age_s = ((now - last_ms).max(0) / 1000) as u64;
            if age_s > oldest_frame_age_s {
                oldest_frame_age_s = age_s;
            }
        }
    }
    let feeds_total = ctx.cells.len() as u64;
    // Truthful liveness only: zero supervisors is never "up" (main exits at
    // startup when no supervisors spawn, so cells are non-empty in production).
    let ws_connected = feeds_up > 0;

    let dt = ctx.last_at.elapsed().as_secs_f64();
    let frames_per_s = if dt > 0.0 {
        (frames.saturating_sub(ctx.last_frames)) as f64 / dt
    } else {
        0.0
    };
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
        feeds_up,
        feeds_total,
        oldest_frame_age_s,
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
        live_rej: ctx.stats.live_rej.load(Ordering::Relaxed),
        live_held: ctx.stats.live_held.load(Ordering::Relaxed),
    };

    // --- Header money + per-strategy breakdown + badges --------------------
    // Wired path (Task 1.8, `strategy_status_rx` is `Some`): header money is the
    // SUM of every strategy's micros, `per_strategy` lists each strategy, and the
    // badges are reconciled across sources — `killed` from the global kill flag,
    // `paused` from ANY strategy paused, `halted` from the first halted strategy,
    // and `live_released`/`busy` from arb's `CoordStatus` (the aggregate drops
    // these process-wide gates). Legacy path (`None`): money, `per_strategy`
    // empty, and ALL badges come from the single `CoordStatus`, exactly as before.
    let (money, per_strategy, live_released, paused, halted, killed, busy) =
        match &ctx.strategy_status_rx {
            Some(rx) => {
                let view = rx.borrow();
                let money = aggregate_money(view.as_slice());
                let per_strategy = strategy_lines(view.as_slice());
                let paused = view.iter().any(|(_, s)| s.paused);
                let halted = view.iter().find_map(|(_, s)| s.halted.clone());
                drop(view);
                let killed = ctx.kill.load(Ordering::Acquire);
                let (live_released, busy) = match &ctx.arb_status_rx {
                    Some(a) => {
                        let cs = a.borrow();
                        (cs.live_released, cs.busy)
                    }
                    None => (false, false),
                };
                (money, per_strategy, live_released, paused, halted, killed, busy)
            }
            None => {
                // Legacy single-strategy path: money + all badges from the single
                // CoordStatus. Borrowed only here, since the wired path above does
                // not use it.
                let status = ctx.status_rx.borrow().clone();
                let money = AggregatedMoney {
                    cash_micro: status.cash_micro,
                    equity_micro: status.equity_micro,
                    equity_mid_micro: status.equity_mid_micro,
                    realized_micro: status.realized_micro,
                    unrealized_micro: status.unrealized_micro,
                    // `CoordStatus` (arb's single-strategy feed) has no rebate
                    // estimate — that is MM-specific. 0 on the legacy path.
                    rebate_micro: 0,
                };
                (
                    money,
                    Vec::new(),
                    status.live_released,
                    status.paused,
                    status.halted.clone(),
                    status.killed,
                    status.busy,
                )
            }
        };

    // Open-orders panel: flatten every strategy's resting + vetoed quotes into
    // display rows (MM is the only strategy with a resting book today). SORTED by
    // the opaque key so the selectable list is STABLE across ticks (the MM's
    // snapshot order is otherwise HashMap-arbitrary, which would make the cursor
    // jump between unrelated orders).
    let open_orders: Vec<OpenOrderLine> = match &ctx.strategy_status_rx {
        Some(rx) => {
            let view = rx.borrow();
            let mut rows: Vec<OpenOrderLine> = view
                .iter()
                .flat_map(|(id, s)| {
                    s.resting_orders
                        .iter()
                        .map(|r| open_order_line(id.0, r, &ctx.registry))
                })
                .collect();
            rows.sort_by(|a, b| a.key.cmp(&b.key));
            rows
        }
        None => Vec::new(),
    };

    AppState {
        uptime_s: ctx.start.elapsed().as_secs(),
        mode_paper: ctx.mode_paper,
        shadow: ctx.shadow,
        live_released,
        paused,
        halted,
        killed,
        busy,
        cash_usd: usd(money.cash_micro),
        equity_usd: usd(money.equity_micro),
        equity_mid_usd: usd(money.equity_mid_micro),
        realized_usd: usd(money.realized_micro),
        unrealized_usd: usd(money.unrealized_micro),
        opportunities,
        positions,
        fills,
        orders,
        open_orders,
        health,
        log: ctx.logbuf.tail(ctx.log_lines),
        per_strategy,
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

    /// Spawn a book responder serving a single (bid, ask) snapshot for one
    /// token's `BookSnapshot` requests — the test analogue of a live feed, used
    /// to mark positions. Returns the command sender to register in a
    /// [`BookFetcher`].
    fn spawn_book(
        bid_tick: u16,
        ask_tick: u16,
    ) -> tokio::sync::mpsc::Sender<pm_ingestion::supervisor::SupervisorCommand> {
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
                    Px::new(bid_tick, TickSize::Cent).unwrap(),
                    Qty(200_000_000),
                );
                b.apply(
                    Side::Ask,
                    Px::new(ask_tick, TickSize::Cent).unwrap(),
                    Qty(200_000_000),
                );
                let _ = reply.send(Some((b, true)));
            }
        });
        cmd_tx
    }

    /// Build a legacy-path (`strategy_status_rx: None`) `PublisherCtx` over the
    /// given store + fetcher + registry. Returns the ctx and the `CoordStatus`
    /// sender (kept by the caller so the watch borrow inside `assemble` stays
    /// valid). Defaults mirror the other publisher tests.
    fn legacy_ctx(
        read: ReadStore,
        fetcher: crate::wiring::BookFetcher,
        registry: std::sync::Arc<pm_registry::Registry>,
    ) -> (PublisherCtx, watch::Sender<crate::coordinator::CoordStatus>) {
        let (status_tx, status_rx) =
            watch::channel(crate::coordinator::CoordStatus::default());
        let ctx = PublisherCtx {
            read,
            stats: crate::stats::AppStats::new(),
            cells: Vec::new(),
            status_rx,
            strategy_status_rx: None,
            kill: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            arb_status_rx: None,
            registry,
            logbuf: crate::logbuf::LogBuffer::new(10),
            fetcher,
            feed_rows: 10,
            fills_rows: 10,
            log_lines: 10,
            mode_paper: true,
            shadow: false,
            start: std::time::Instant::now(),
            last_frames: 0,
            last_at: std::time::Instant::now(),
        };
        (ctx, status_tx)
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

    /// Layer 2 requirement: ws_connected, feeds_up, feeds_total, and
    /// oldest_frame_age_s must come from StatsCell.connected / last_frame_ms,
    /// not from the old frame-count-delta heuristic.
    #[tokio::test]
    async fn health_ws_flag_and_staleness_come_from_cells() {
        use pm_ingestion::stats::StatsCell;
        use std::sync::atomic::Ordering;

        // Cell 0: connected, last_frame_ms = now (age ≈ 0 s).
        let cell0 = StatsCell::new();
        cell0
            .connected
            .store(true, Ordering::Relaxed);
        cell0.last_frame_ms.store(
            crate::coordinator::now_ms(),
            Ordering::Relaxed,
        );

        // Cell 1: disconnected, last_frame_ms = now - 30_000 ms (age ≈ 30 s).
        let cell1 = StatsCell::new();
        cell1
            .connected
            .store(false, Ordering::Relaxed);
        cell1.last_frame_ms.store(
            crate::coordinator::now_ms() - 30_000,
            Ordering::Relaxed,
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sqlite");
        // ReadStore requires the file to exist; create it via Store::open first.
        let _ = pm_store::Store::open(&path).unwrap();
        let read = pm_store::read::ReadStore::open(&path).unwrap();

        let stats = crate::stats::AppStats::new();
        let logbuf = crate::logbuf::LogBuffer::new(10);
        let (_status_tx, status_rx) =
            tokio::sync::watch::channel(crate::coordinator::CoordStatus::default());
        let r = reg();
        let fetcher =
            crate::wiring::BookFetcher::new(std::collections::HashMap::new());

        let mut ctx = PublisherCtx {
            read,
            stats,
            cells: vec![cell0, cell1],
            status_rx,
            strategy_status_rx: None,
            kill: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            arb_status_rx: None,
            registry: r,
            logbuf,
            fetcher,
            feed_rows: 10,
            fills_rows: 10,
            log_lines: 10,
            mode_paper: true,
            shadow: false,
            start: std::time::Instant::now(),
            last_frames: 0,
            last_at: std::time::Instant::now(),
        };

        let state = assemble(&mut ctx).await;

        assert!(state.health.ws_connected, "any live feed → ws_connected true");
        assert_eq!(state.health.feeds_up, 1, "only cell0 is connected");
        assert_eq!(state.health.feeds_total, 2, "two cells total");
        // Cell1 has last_frame_ms = now - 30_000, so oldest age >= 30 s.
        assert!(
            state.health.oldest_frame_age_s >= 30,
            "oldest frame age must be >= 30 s, got {}",
            state.health.oldest_frame_age_s
        );
        drop(_status_tx);
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
                strategy: "arb".into(),
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
                strategy: "arb".into(),
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
                strategy: "arb".into(),
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
            strategy_status_rx: None,
            kill: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            arb_status_rx: None,
            registry: r,
            logbuf,
            fetcher,
            feed_rows: 50,
            fills_rows: 20,
            log_lines: 50,
            mode_paper: true,
            shadow: false,
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

    /// live_released comes from CoordStatus; live_rej and live_held come from
    /// AppStats — verify the publisher maps both correctly.
    #[tokio::test]
    async fn live_fields_map_from_status_and_stats() {
        use pm_ingestion::stats::StatsCell;
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sqlite");
        let _ = pm_store::Store::open(&path).unwrap();
        let read = pm_store::read::ReadStore::open(&path).unwrap();

        let stats = crate::stats::AppStats::new();
        stats.live_rej.store(5, Ordering::Relaxed);
        stats.live_held.store(3, Ordering::Relaxed);

        let logbuf = crate::logbuf::LogBuffer::new(10);
        let r = reg();
        let fetcher = crate::wiring::BookFetcher::new(std::collections::HashMap::new());

        let (_status_tx, status_rx) =
            tokio::sync::watch::channel(crate::coordinator::CoordStatus {
                live: true,
                live_released: true,
                ..Default::default()
            });

        let mut ctx = PublisherCtx {
            read,
            stats,
            cells: vec![StatsCell::new()],
            status_rx,
            strategy_status_rx: None,
            kill: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            arb_status_rx: None,
            registry: r,
            logbuf,
            fetcher,
            feed_rows: 10,
            fills_rows: 10,
            log_lines: 10,
            mode_paper: false,
            shadow: true,
            start: std::time::Instant::now(),
            last_frames: 0,
            last_at: std::time::Instant::now(),
        };

        let state = assemble(&mut ctx).await;
        assert!(state.live_released, "live_released must come from CoordStatus");
        assert!(state.shadow, "shadow must map from PublisherCtx into AppState");
        assert_eq!(state.health.live_rej, 5, "live_rej from AppStats");
        assert_eq!(state.health.live_held, 3, "live_held from AppStats");
        drop(_status_tx);
    }

    /// Task 4.4: `aggregate_money` sums the maker-rebate estimate across
    /// strategies alongside the other money — but as a SEPARATE field, never
    /// folded into equity/cash/realized. Only MM contributes; arb leaves it 0.
    #[test]
    fn aggregate_money_sums_rebate_separately() {
        let view: Vec<(StrategyId, StrategyStatus)> = vec![
            (
                StrategyId("arb"),
                StrategyStatus {
                    equity_micro: 7_000_000,
                    cash_micro: 1_000_000,
                    realized_micro: 2_000_000,
                    // arb earns no maker rebate.
                    rebate_micro: 0,
                    ..Default::default()
                },
            ),
            (
                StrategyId("mm"),
                StrategyStatus {
                    equity_micro: 3_000_000,
                    cash_micro: 500_000,
                    realized_micro: 1_000_000,
                    rebate_micro: 25_000,
                    ..Default::default()
                },
            ),
        ];
        let m = aggregate_money(&view);
        assert_eq!(m.equity_micro, 10_000_000, "equity sums the two strategies");
        assert_eq!(m.cash_micro, 1_500_000);
        assert_eq!(m.realized_micro, 3_000_000);
        // The rebate is summed on its OWN field and is NOT mixed into equity etc.
        assert_eq!(m.rebate_micro, 25_000, "rebate summed across strategies");
        assert_eq!(
            m.equity_micro, 10_000_000,
            "rebate must not inflate the summed equity"
        );
    }

    /// Task 1.7: fed the host's aggregated per-strategy view, the publisher
    /// produces the header money as the SUM of every strategy's micros
    /// (overriding the single CoordStatus) and fills `per_strategy` with one
    /// display-only line per strategy (ids + usd values + paused flag).
    #[tokio::test]
    async fn aggregated_view_sums_header_money_and_fills_per_strategy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sqlite");
        let _ = pm_store::Store::open(&path).unwrap();
        let read = pm_store::read::ReadStore::open(&path).unwrap();

        let stats = crate::stats::AppStats::new();
        let logbuf = crate::logbuf::LogBuffer::new(10);
        let r = reg();
        let fetcher = crate::wiring::BookFetcher::new(std::collections::HashMap::new());

        // CoordStatus money is deliberately non-zero so the test proves the
        // aggregated view OVERRIDES it (header equity is the per-strategy sum,
        // not 999). On the `Some` (wired) path the badges are RECONCILED, not
        // taken from this CoordStatus: killed ← kill flag, paused ← any strategy,
        // halted ← first halted strategy, live_released/busy ← arb_status_rx
        // (`None` here ⇒ false). The dedicated badge test below asserts those.
        let (_status_tx, status_rx) =
            tokio::sync::watch::channel(crate::coordinator::CoordStatus {
                equity_micro: 999_000_000,
                cash_micro: 999_000_000,
                ..Default::default()
            });

        // Two strategies: "arb" (equity 7.0, cash 1.0, realized 2.0, unreal
        // -0.5, paused) and "mm" (all zeros). Header equity must sum to 7.0.
        let view: StrategyStatusView = vec![
            (
                StrategyId("arb"),
                StrategyStatus {
                    equity_micro: 7_000_000,
                    cash_micro: 1_000_000,
                    realized_micro: 2_000_000,
                    unrealized_micro: -500_000,
                    open_positions: 4,
                    paused: true,
                    ..Default::default()
                },
            ),
            (StrategyId("mm"), StrategyStatus::default()),
        ];
        let (_strat_tx, strat_rx) = tokio::sync::watch::channel(view);

        let mut ctx = PublisherCtx {
            read,
            stats,
            cells: Vec::new(),
            status_rx,
            strategy_status_rx: Some(strat_rx),
            kill: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            arb_status_rx: None,
            registry: r,
            logbuf,
            fetcher,
            feed_rows: 10,
            fills_rows: 10,
            log_lines: 10,
            mode_paper: true,
            shadow: false,
            start: std::time::Instant::now(),
            last_frames: 0,
            last_at: std::time::Instant::now(),
        };

        let state = assemble(&mut ctx).await;

        // Header money is the SUM of the per-strategy micros, NOT the CoordStatus.
        assert!(
            (state.equity_usd - 7.0).abs() < 1e-9,
            "header equity must be the per-strategy sum 7.0, got {}",
            state.equity_usd
        );
        assert!(
            (state.cash_usd - 1.0).abs() < 1e-9,
            "header cash sum = {}",
            state.cash_usd
        );
        assert!((state.realized_usd - 2.0).abs() < 1e-9);
        assert!((state.unrealized_usd + 0.5).abs() < 1e-9);

        // per_strategy lists BOTH ids with correct display-only usd values.
        assert_eq!(state.per_strategy.len(), 2);
        let arb = state
            .per_strategy
            .iter()
            .find(|l| l.id == "arb")
            .unwrap();
        assert!((arb.equity_usd - 7.0).abs() < 1e-9, "arb equity_usd");
        assert!((arb.cash_usd - 1.0).abs() < 1e-9, "arb cash_usd");
        assert!((arb.realized_usd - 2.0).abs() < 1e-9, "arb realized_usd");
        assert!((arb.unrealized_usd + 0.5).abs() < 1e-9, "arb unrealized_usd");
        assert_eq!(arb.open_positions, 4, "arb open-position count carried through");
        assert!(arb.paused, "arb paused flag carried through");
        let mm = state
            .per_strategy
            .iter()
            .find(|l| l.id == "mm")
            .unwrap();
        assert!((mm.equity_usd - 0.0).abs() < 1e-9, "mm equity_usd");
        assert!(!mm.paused, "mm not paused");

        drop(_status_tx);
        drop(_strat_tx);
    }

    /// Task 1.8: on the `Some` (wired) path the header badges are RECONCILED from
    /// multiple sources, NOT from the single `CoordStatus` (`status_rx`). With
    /// `status_rx` left all-false, every badge that comes out `true` must have
    /// come from its real source: `killed` ← the global kill flag; `paused` ←
    /// ANY strategy paused; `halted` ← the first halted strategy; and
    /// `live_released`/`busy` ← arb's `CoordStatus` (`arb_status_rx`).
    #[tokio::test]
    async fn wired_path_reconciles_header_badges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sqlite");
        let _ = pm_store::Store::open(&path).unwrap();
        let read = pm_store::read::ReadStore::open(&path).unwrap();

        let stats = crate::stats::AppStats::new();
        let logbuf = crate::logbuf::LogBuffer::new(10);
        let r = reg();
        let fetcher = crate::wiring::BookFetcher::new(std::collections::HashMap::new());

        // The single CoordStatus is deliberately all-false (default), so any badge
        // that ends up true MUST have come from its reconciled source — proving the
        // wired path does not read badges from `status_rx`.
        let (_status_tx, status_rx) =
            tokio::sync::watch::channel(crate::coordinator::CoordStatus::default());

        // View: "arb" paused (not halted), "mm" halted (not paused).
        let view: StrategyStatusView = vec![
            (
                StrategyId("arb"),
                StrategyStatus {
                    paused: true,
                    ..Default::default()
                },
            ),
            (
                StrategyId("mm"),
                StrategyStatus {
                    halted: Some("DailyDrawdown".to_string()),
                    ..Default::default()
                },
            ),
        ];
        let (_strat_tx, strat_rx) = tokio::sync::watch::channel(view);

        // arb's CoordStatus supplies the process-wide gates the aggregate drops.
        let (_arb_status_tx, arb_status_rx) =
            tokio::sync::watch::channel(crate::coordinator::CoordStatus {
                live_released: true,
                busy: true,
                ..Default::default()
            });

        // Global kill flag is set.
        let kill = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

        let mut ctx = PublisherCtx {
            read,
            stats,
            cells: Vec::new(),
            status_rx,
            strategy_status_rx: Some(strat_rx),
            kill: std::sync::Arc::clone(&kill),
            arb_status_rx: Some(arb_status_rx),
            registry: r,
            logbuf,
            fetcher,
            feed_rows: 10,
            fills_rows: 10,
            log_lines: 10,
            mode_paper: true,
            shadow: false,
            start: std::time::Instant::now(),
            last_frames: 0,
            last_at: std::time::Instant::now(),
        };

        let state = assemble(&mut ctx).await;

        assert!(state.killed, "killed must come from the global kill flag");
        assert!(state.paused, "paused must be true when ANY strategy is paused");
        assert_eq!(
            state.halted,
            Some("DailyDrawdown".to_string()),
            "halted must be the first halted strategy's reason"
        );
        assert!(
            state.live_released,
            "live_released must come from arb_status_rx (CoordStatus)"
        );
        assert!(state.busy, "busy must come from arb_status_rx (CoordStatus)");

        drop(_status_tx);
        drop(_strat_tx);
        drop(_arb_status_tx);
    }

    /// A DB with one arb fill (strict long Buy) and one mm fill (signed
    /// Sell-to-open) → `AppState.fills` carries BOTH, each tagged with the
    /// strategy that traded it, so the dashboard can show who did what.
    #[tokio::test]
    async fn assemble_tags_fills_by_strategy() {
        let r = reg();
        let m = r.markets()[0];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sqlite");
        {
            let mut s = pm_store::Store::open(&path).unwrap();
            s.insert_order(&pm_store::OrderRow {
                id: "arb1".into(),
                ts_ms: 1,
                fingerprint: "f".into(),
                token: m.yes.0 as i64,
                action: "Buy".into(),
                limit_ticks: 44,
                tick_levels: 100,
                qty_micro: 100_000_000,
                strategy: "arb".into(),
            })
            .unwrap();
            s.insert_fill(&pm_store::FillRow {
                order_id: "arb1".into(),
                ts_ms: 10,
                token: m.yes.0 as i64,
                action: "Buy".into(),
                px_ticks: 44,
                tick_levels: 100,
                qty_micro: 100_000_000,
                cash_micro: -44_000_000,
                fee_micro: 0,
                strategy: "arb".into(),
            })
            .unwrap();
            s.insert_order(&pm_store::OrderRow {
                id: "mm1".into(),
                ts_ms: 2,
                fingerprint: "f".into(),
                token: m.no.0 as i64,
                action: "Sell".into(),
                limit_ticks: 40,
                tick_levels: 100,
                qty_micro: 50_000_000,
                strategy: "mm".into(),
            })
            .unwrap();
            // Signed Sell-to-open: no longs → a pure mm short (never Oversells).
            s.insert_fill_signed(&pm_store::FillRow {
                order_id: "mm1".into(),
                ts_ms: 20,
                token: m.no.0 as i64,
                action: "Sell".into(),
                px_ticks: 40,
                tick_levels: 100,
                qty_micro: 50_000_000,
                cash_micro: 20_000_000,
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        }
        let read = pm_store::read::ReadStore::open(&path).unwrap();
        // Fills don't need a book (only positions are marked) → empty fetcher.
        let fetcher = crate::wiring::BookFetcher::new(std::collections::HashMap::new());
        let (mut ctx, _status_tx) = legacy_ctx(read, fetcher, r);

        let state = assemble(&mut ctx).await;

        assert_eq!(state.fills.len(), 2, "both fills surface");
        let arb = state
            .fills
            .iter()
            .find(|f| f.strategy == "arb")
            .unwrap();
        assert_eq!(arb.action, "Buy", "the arb fill is the long Buy");
        let mm = state
            .fills
            .iter()
            .find(|f| f.strategy == "mm")
            .unwrap();
        assert_eq!(mm.action, "Sell", "the mm fill is the short-open Sell");
        drop(_status_tx);
    }

    /// An mm SHORT position surfaces as a `PositionLine` tagged `"mm"` with a
    /// NEGATIVE signed qty, marked at the best ASK (what it'd cost to buy back)
    /// → a negative mark. Proves the signed/short convention end to end.
    #[tokio::test]
    async fn assemble_marks_mm_short_at_ask_tagged_mm() {
        let r = reg();
        let m = r.markets()[0];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sqlite");
        {
            let mut s = pm_store::Store::open(&path).unwrap();
            s.insert_order(&pm_store::OrderRow {
                id: "mm1".into(),
                ts_ms: 1,
                fingerprint: "f".into(),
                token: m.no.0 as i64,
                action: "Sell".into(),
                limit_ticks: 40,
                tick_levels: 100,
                qty_micro: 50_000_000,
                strategy: "mm".into(),
            })
            .unwrap();
            // Short 50 NO @ $0.40 → proceeds $20 → basis −$20, net −50.
            s.insert_fill_signed(&pm_store::FillRow {
                order_id: "mm1".into(),
                ts_ms: 2,
                token: m.no.0 as i64,
                action: "Sell".into(),
                px_ticks: 40,
                tick_levels: 100,
                qty_micro: 50_000_000,
                cash_micro: 20_000_000,
                fee_micro: 0,
                strategy: "mm".into(),
            })
            .unwrap();
        }
        let read = pm_store::read::ReadStore::open(&path).unwrap();
        // Book for the NO token: bid 38, ask 41. The short marks at the ASK.
        let fetcher = crate::wiring::BookFetcher::new(std::collections::HashMap::from([(
            m.no,
            spawn_book(38, 41),
        )]));
        let (mut ctx, _status_tx) = legacy_ctx(read, fetcher, r);

        let state = assemble(&mut ctx).await;

        assert_eq!(state.positions.len(), 1, "one open mm short");
        let p = &state.positions[0];
        assert_eq!(p.strategy, "mm", "position tagged with its strategy");
        assert!(
            (p.qty_shares + 50.0).abs() < 1e-9,
            "signed (negative) short qty, got {}",
            p.qty_shares
        );
        assert!((p.basis_usd + 20.0).abs() < 1e-9, "short basis −$20, got {}", p.basis_usd);
        // Marked at the ASK (0.41 × 50 = $20.50) as a liability → −$20.50, NOT
        // the bid (0.38 × 50 = $19.00). A bid-mark would give −19.00, so this
        // also proves the short uses the ask side.
        assert!(
            (p.mark_usd + 20.5).abs() < 1e-9,
            "short marked at the ask as a negative liability, got {}",
            p.mark_usd
        );
        drop(_status_tx);
    }
}
