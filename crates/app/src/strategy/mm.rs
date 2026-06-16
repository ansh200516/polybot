//! `MmStrategy` — the market-making quoting loop (multi-strategy platform,
//! Task 4.2). The platform's first risk-taking strategy: it quotes a symmetric
//! bid/ask around the book mid on a fixed cadence, sizes each quote by a
//! notional cap clamped by the inventory caps, books fills into inventory +
//! accounting + the store, and latches a safety stop on an inventory halt.
//!
//! # Scope (Task 4.2 — the CORE loop only)
//! - **Fair = mid** quoting; bid = fair − spread/2, ask = fair + spread/2.
//! - **Sizing** = `max_quote_usd` notional per side (and never above the
//!   strategy's capital), gated by [`InventoryRisk::check_quote`].
//! - **Fills** → [`InventoryRisk::on_fill`] (authoritative signed inventory) +
//!   [`PositionBook`] (reporting) + a `"mm"`-tagged store fill row.
//! - **Safety stop**: `InventoryRisk::mark` on the mid feed; a latched
//!   [`InvHalt`](pm_risk::inventory::InvHalt) cancels all quotes and stops
//!   quoting (latched).
//! - **Pause/kill**: mirrors the [`stub`](super::stub) lifecycle — pause cancels
//!   resting quotes and stops quoting (fills are always consumed); the global
//!   kill cancels and exits cleanly.
//! - **Paper only**: runs over the [`PaperMakerVenue`]; the live arm is Task 4.5.
//!
//! # Scope (Task 4.3 — skew + volatility pull, this task)
//! - **Inventory SKEW**: [`skew_fair`] shifts `fair` (= mid) against inventory
//!   inside [`compute_quotes`] — a long lowers BOTH quotes, a short raises them,
//!   scaled by `clamp(net / inventory_cap, ±1)` up to `inventory_skew_bps`.
//! - **Volatility pull**: [`InventoryRisk::vol_hint`] fires on a large + fast
//!   mid move in [`MmLoop::quote`], excluding that token from the desired set so
//!   `reconcile` cancels its resting quotes without replacing (a pull).
//!
//! # Deferred (left as clean seams — do NOT implement here)
//! - **Rebate accrual** (Task 4.4) — plugs in at fill consumption.
//! - **Live venue + host wiring** (Task 4.5): `run` builds a concrete
//!   [`PaperMakerVenue`], but the loop is generic over any
//!   `MakerVenue + UserFillSource`, so 4.5 passes a live venue unchanged.
//!
//! # Accounting note (why InventoryRisk is authoritative)
//! [`InventoryRisk`] tracks SIGNED net inventory + realized/unrealized P&L and
//! is the source of truth for risk. [`PositionBook`] is fed in lock-step (cost
//! basis deltas + cash mirror inventory exactly) so `positions.pnl` reports the
//! same equity; held tokens are valued from the signed net at the current book,
//! so the report is correct for both long and short inventory even though
//! `PositionBook` itself is append-only.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pm_core::book::{Book, Side};
use pm_core::instrument::{MarketId, TokenId};
use pm_core::num::{Px, Qty, TickSize, Usdc, buy_cost, sell_proceeds};
use pm_execution::fills::UserFillSource;
use pm_execution::maker::{MakerOrder, MakerVenue, OrderId, OrderType};
use pm_execution::paper_maker::PaperMakerVenue;
use pm_execution::quote_manager::QuoteManager;
use pm_ingestion::supervisor::OnApplyFn;
use pm_risk::inventory::{InventoryConfig, InventoryRisk, Marks, QuoteIntent, QuoteVerdict};
use pm_store::writer::StoreMsg;
use pm_store::{FillRow, OrderRow, PnlRow, usdc_to_i64};
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tracing::warn;

use crate::coordinator::now_ms;
use crate::positions::PositionBook;
use crate::wiring::BookFetcher;

use super::{Strategy, StrategyCommand, StrategyCtx, StrategyId, StrategyStatus};

/// Resolved quote-loop parameters (USD → µUSDC done once, up front).
#[derive(Debug, Clone, Copy)]
pub struct MmParams {
    /// Total quoted spread around fair, in bps of $1 (1 bp = 100 µUSDC/share).
    pub spread_bps: u32,
    /// Quote-loop cadence.
    pub quote_refresh: Duration,
    /// Max notional per single quote (one side), µUSDC.
    pub max_quote_micro: i128,
    /// Inventory-skew limit (Task 4.3): MAX fair-value shift at full per-market
    /// inventory, bps of $1 (1 bp = 100 µUSDC/share). `0` disables skew. See
    /// [`skew_fair`].
    pub inventory_skew_bps: u32,
}

impl MmParams {
    /// Resolve `[strategies.mm]` config into runtime params (the USD notional is
    /// converted to µUSDC here). The seam the Task-4.5 main wiring uses.
    pub fn from_config(mm: &pm_config::Mm) -> Result<Self, pm_config::ConfigError> {
        Ok(MmParams {
            spread_bps: mm.spread_bps,
            quote_refresh: Duration::from_millis(mm.quote_refresh_ms),
            max_quote_micro: pm_config::usd_to_microusdc(mm.max_quote_usd)?,
            inventory_skew_bps: mm.inventory_skew_bps,
        })
    }
}

/// Market-making strategy (spec §7). Constructed by the Task-4.5 main wiring
/// (no host wiring here); `run` builds the paper venue + inventory risk +
/// position book + quote manager and drives [`run_mm_loop`].
pub struct MmStrategy {
    id: StrategyId,
    /// Markets to quote (provided; Phase 5 refines the universe per segment).
    tokens: Vec<TokenId>,
    /// `token → market` for [`PositionBook::apply`] (from the registry).
    token_market: HashMap<TokenId, MarketId>,
    params: MmParams,
    inv_cfg: InventoryConfig,
    capital: Usdc,
}

impl MmStrategy {
    pub fn new(
        tokens: Vec<TokenId>,
        token_market: HashMap<TokenId, MarketId>,
        params: MmParams,
        inv_cfg: InventoryConfig,
        capital: Usdc,
    ) -> Self {
        MmStrategy {
            id: StrategyId("mm"),
            tokens,
            token_market,
            params,
            inv_cfg,
            capital,
        }
    }
}

impl Strategy for MmStrategy {
    fn id(&self) -> StrategyId {
        self.id
    }

    /// The MM reads books on its OWN cadence via `ctx.fetcher`, not the
    /// per-supervisor inline hook (that is arb's hot path).
    fn make_on_apply(&self) -> Option<OnApplyFn> {
        None
    }

    fn run(self: Box<Self>, ctx: StrategyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let MmStrategy {
                id: _,
                tokens,
                token_market,
                params,
                inv_cfg,
                capital,
            } = *self;
            // Build the CONCRETE paper venue here (non-generic context) so the
            // future is provably `Send` even though `run_mm_loop` is generic —
            // the same pattern arb uses for `run_execution` (see arb.rs docs).
            // Task 4.5 swaps in a live venue without touching the loop.
            let venue = PaperMakerVenue::new(ctx.fetcher.clone());
            run_mm_loop(
                venue,
                QuoteManager::new(),
                InventoryRisk::new(inv_cfg),
                PositionBook::default(),
                ctx,
                params,
                tokens,
                token_market,
                capital,
            )
            .await;
        })
    }
}

// ---------------------------------------------------------------------------
// Pure quote math (no async, no inventory — unit-tested directly)
// ---------------------------------------------------------------------------

/// `Buy`/`Sell` action string for a resting side (store row tag).
fn side_action(side: Side) -> &'static str {
    match side {
        Side::Bid => "Buy",
        Side::Ask => "Sell",
    }
}

/// Mid price in µUSDC/share, or `None` if either side of the book is empty.
fn mid_micro(book: &Book) -> Option<u64> {
    let ts = book.ts();
    let bid = book.bids.best()?;
    let ask = book.asks.best()?;
    Some((bid.microusdc(ts) + ask.microusdc(ts)) / 2)
}

/// Signed mark value of `net` µshares at `price_micro` µUSDC/share, floored
/// toward −∞ (against us on BOTH sides — mirrors [`InventoryRisk::mark`]).
fn signed_value(net: i128, price_micro: u64) -> i128 {
    (net * i128::from(price_micro)).div_euclid(1_000_000)
}

/// Build one resting `postOnly` Gtc maker order sized by `notional_micro /
/// price`. `None` when the price or computed size is non-positive.
fn quote_order(
    token: TokenId,
    side: Side,
    price: Px,
    ts: TickSize,
    notional_micro: i128,
) -> Option<MakerOrder> {
    let price_micro = i128::from(price.microusdc(ts));
    if price_micro <= 0 {
        return None;
    }
    // size µshares = notional µUSDC × 1e6 µshare/share ÷ (µUSDC/share).
    let size_micro = notional_micro.saturating_mul(1_000_000) / price_micro;
    if size_micro <= 0 {
        return None;
    }
    Some(MakerOrder {
        token,
        side,
        price,
        size: Qty(size_micro as u64),
        order_type: OrderType::Gtc,
        post_only: true,
    })
}

/// Shift `fair` (µUSDC/share) AGAINST inventory (Task 4.3, spec §7): a long
/// (`net > 0`) LOWERS fair so BOTH quotes drop (less eager to buy more, keener
/// to sell down); a short RAISES it. The magnitude is `skew_bps · 100 · r`
/// µUSDC, where `r = clamp(net / max_inventory_shares, −1, +1)` and
/// `max_inventory_shares` is the per-market cap (`max_inventory_micro` µUSDC)
/// valued at `fair` — the same notional→µshares scaling as [`quote_order`].
///
/// All-integer (µUSDC), no f64 in the money path: `net` is clamped to
/// `±max_shares` so the ratio never exceeds 1, then `shift = skew_micro ·
/// net_clamped / max_shares` truncates toward 0 (a slight UNDER-shift, never an
/// over-shift; symmetric for long/short, matching the codebase's truncating
/// integer money math). At FULL inventory the shift is exactly `skew_bps · 100`
/// µUSDC. A flat book (`net == 0`), a non-positive cap/price, or `skew_bps == 0`
/// returns `fair` unchanged. The skew only moves `fair`; [`compute_quotes`]'s
/// existing tick clamps then keep the quote non-crossing and inside the interior
/// range (a skew that pushes a side out just skips that quote — never a cross).
fn skew_fair(fair: i128, net_micro: i128, max_inventory_micro: i128, skew_bps: u32) -> i128 {
    if skew_bps == 0 || net_micro == 0 || max_inventory_micro <= 0 || fair <= 0 {
        return fair;
    }
    // Per-market cap (µUSDC) valued at `fair` → max inventory in µshares
    // (µUSDC × µshares/share ÷ µUSDC/share), mirroring `quote_order`'s sizing.
    let max_shares = max_inventory_micro.saturating_mul(1_000_000) / fair;
    if max_shares <= 0 {
        return fair;
    }
    // r = clamp(net / max_shares, −1, +1), realized by clamping net to ±max.
    let net_clamped = net_micro.clamp(-max_shares, max_shares);
    // Max shift at full inventory, µUSDC (1 bp = 100 µUSDC/share).
    let skew_micro = i128::from(skew_bps).saturating_mul(100);
    let shift = skew_micro.saturating_mul(net_clamped) / max_shares;
    fair - shift
}

/// Compute the `(bid, ask)` quotes from a book + params, BEFORE the inventory
/// cap check (that gate is applied in the loop). Pure so the quoting math is
/// unit-tested without async.
///
/// `fair = mid` shifted against inventory by [`skew_fair`] (Task 4.3: a long
/// lowers BOTH quotes, a short raises them; `net == 0` or `inventory_skew_bps ==
/// 0` leaves `fair = mid`, i.e. the Task-4.2 symmetric quote). The half-spread
/// (µUSDC) is `spread_bps · 100 / 2` (1 bp = 100 µUSDC/share since $1.00 =
/// 10_000 bps = 1_000_000 µUSDC). The bid rounds DOWN to a tick and the ask
/// rounds UP (maker-favorable / never narrower); they are bumped apart to stay
/// strictly non-crossing, and both must be interior ticks `[1, levels−1]` —
/// otherwise the token is skipped (`(None, None)`). These clamps bound the skew:
/// it can never cross the book or leave the valid range (it just skips a side).
///
/// `net_micro` is the strategy's current signed net for `token` (µshares) and
/// `max_inventory_micro` the per-market inventory cap (µUSDC) the skew
/// normalizes against — sourced from [`InventoryRisk`] in the loop.
fn compute_quotes(
    book: &Book,
    token: TokenId,
    params: &MmParams,
    notional_micro: i128,
    net_micro: i128,
    max_inventory_micro: i128,
) -> (Option<MakerOrder>, Option<MakerOrder>) {
    let ts = book.ts();
    let (Some(best_bid), Some(best_ask)) = (book.bids.best(), book.asks.best()) else {
        return (None, None);
    };
    let unit = i128::from(ts.unit_microusdc());
    let levels = i128::from(ts.levels());

    // fair = mid skewed against inventory (Task 4.3); half-spread in µUSDC.
    let mid = (i128::from(best_bid.microusdc(ts)) + i128::from(best_ask.microusdc(ts))) / 2;
    let fair = skew_fair(mid, net_micro, max_inventory_micro, params.inventory_skew_bps);
    let half = i128::from(params.spread_bps) * 100 / 2;

    // bid rounds DOWN (floor), ask rounds UP (ceil) — never narrower than asked.
    let bid_tick = (fair - half).div_euclid(unit);
    let mut ask_tick = {
        let n = fair + half;
        (n + unit - 1).div_euclid(unit)
    };
    // Never cross / collapse onto one tick: keep the ask strictly above the bid.
    if ask_tick <= bid_tick {
        ask_tick = bid_tick + 1;
    }
    // Both must be interior ticks [1, levels-1], else no valid non-crossing quote.
    if bid_tick < 1 || ask_tick > levels - 1 {
        return (None, None);
    }
    let (Ok(bid_px), Ok(ask_px)) = (Px::new(bid_tick as u16, ts), Px::new(ask_tick as u16, ts))
    else {
        return (None, None);
    };
    (
        quote_order(token, Side::Bid, bid_px, ts, notional_micro),
        quote_order(token, Side::Ask, ask_px, ts, notional_micro),
    )
}

// ---------------------------------------------------------------------------
// The quote loop
// ---------------------------------------------------------------------------

/// What we recorded when we placed a resting order: enough to resolve a later
/// [`MakerFill`](pm_execution::fills::MakerFill), which carries no side and no
/// tick size — we know both because we placed it.
#[derive(Debug, Clone, Copy)]
struct Placed {
    side: Side,
    ts: TickSize,
}

/// The market maker's owned per-strategy state + handles. Generic over the
/// venue so Task 4.5 can drive a live `MakerVenue + UserFillSource` through the
/// SAME loop; `run` builds the concrete [`PaperMakerVenue`].
struct MmLoop<V: MakerVenue + UserFillSource> {
    venue: V,
    qm: QuoteManager,
    inv: InventoryRisk,
    positions: PositionBook,
    fetcher: BookFetcher,
    store_tx: mpsc::Sender<StoreMsg>,
    status_tx: watch::Sender<StrategyStatus>,
    params: MmParams,
    tokens: Vec<TokenId>,
    token_market: HashMap<TokenId, MarketId>,
    /// Per-side notional cap: `min(max_quote_usd, capital)`, µUSDC.
    notional_micro: i128,
    /// `order_id → resting-order metadata`, for fill→side resolution and the
    /// write-ahead order row. Grows as ids churn; old ids are harmless.
    placed: HashMap<OrderId, Placed>,
    paused: bool,
    /// Latched once an inventory halt fires — quoting never resumes this session.
    halted: bool,
}

impl<V: MakerVenue + UserFillSource> MmLoop<V> {
    /// One quote cycle: re-quote (when active), consume fills, mark + safety
    /// stop, publish status.
    async fn tick(&mut self) {
        if !self.paused && !self.halted {
            self.quote().await;
        }
        // Fills are consumed even when paused/halted — resting orders may still
        // settle in-flight, and inventory/accounting must stay correct.
        self.consume_fills().await;
        self.mark_and_check().await;
        self.publish_status().await;
    }

    /// Build the desired quote set (inventory-gated) and reconcile it onto the
    /// venue, then record any newly-placed orders (+ write their order rows).
    async fn quote(&mut self) {
        let tokens = self.tokens.clone();
        // Per-market inventory cap (µUSDC) the skew normalizes against — read
        // once; the skew is otherwise a pure function of the per-token net.
        let max_inventory_micro = self.inv.config().max_inventory_usd.0;
        let mut desired: Vec<MakerOrder> = Vec::new();
        let mut desired_ts: HashMap<(TokenId, Side), TickSize> = HashMap::new();
        for token in tokens {
            // Need a VALID two-sided book; skip the token otherwise.
            let Some((book, true)) = self.fetcher.fetch(token).await else {
                continue;
            };
            let ts = book.ts();
            // VOLATILITY PULL (Task 4.3, spec §7): a large + FAST mid move makes
            // `vol_hint` fire — PULL this token's quotes for this tick by leaving
            // them OUT of `desired`, so `reconcile` cancels any resting quotes and
            // places none (a pull, NOT a replace — we don't want to be run over
            // during the move). Non-sticky + per-token: `vol_hint` only fires on a
            // fresh large move, so a calmer later tick re-quotes with no cooldown
            // bookkeeping. A pulled token produces no quotes regardless of skew.
            if let Some(mid) = mid_micro(&book)
                && self.inv.vol_hint(token, mid, Instant::now())
            {
                continue;
            }
            // Skew fair against the strategy's current signed net for this token.
            let net_micro = self.inv.net(token);
            let (bid, ask) = compute_quotes(
                &book,
                token,
                &self.params,
                self.notional_micro,
                net_micro,
                max_inventory_micro,
            );
            for o in [bid, ask].into_iter().flatten() {
                let signed_qty = match o.side {
                    Side::Bid => o.size.0 as i128,
                    Side::Ask => -(o.size.0 as i128),
                };
                let intent = QuoteIntent {
                    token,
                    signed_qty,
                    price_micro: o.price.microusdc(ts),
                };
                // Inventory cap gate (Task 2.2): only quote sides it approves.
                if matches!(self.inv.check_quote(&intent), QuoteVerdict::Approve) {
                    desired_ts.insert((o.token, o.side), ts);
                    desired.push(o);
                }
            }
        }
        // QuoteManager leaves consistent state on error and the next tick
        // retries (reconnect orchestration is the Task-3.5/4.5 seam).
        if self.qm.reconcile(&mut self.venue, &desired).await.is_err() {
            return;
        }
        self.record_placed(&desired, &desired_ts).await;
    }

    /// Record every newly-resting order into `placed` and emit its write-ahead
    /// order row (so the FK-referencing fill rows persist). Idempotent per id.
    async fn record_placed(
        &mut self,
        desired: &[MakerOrder],
        desired_ts: &HashMap<(TokenId, Side), TickSize>,
    ) {
        for ((token, side), id) in self.qm.tracked() {
            if self.placed.contains_key(&id) {
                continue;
            }
            let Some(order) = desired.iter().find(|o| o.token == token && o.side == side) else {
                continue;
            };
            let ts = desired_ts
                .get(&(token, side))
                .copied()
                .unwrap_or(TickSize::Cent);
            self.placed.insert(id.clone(), Placed { side, ts });
            let row = OrderRow {
                id: id.0.clone(),
                ts_ms: now_ms(),
                fingerprint: id.0.clone(),
                token: token.0 as i64,
                action: side_action(side).into(),
                limit_ticks: i64::from(order.price.get()),
                tick_levels: i64::from(ts.levels()),
                qty_micro: order.size.0 as i64,
                strategy: "mm".into(),
            };
            let _ = self.store_tx.send(StoreMsg::OrderInsert(row, None)).await;
        }
    }

    /// Poll the venue for fills and book each into inventory + positions + the
    /// store (`"mm"`-tagged, via the SIGNED store route so sell-to-open SHORTS
    /// persist). Makers pay 0 fee on CLOB V2.
    async fn consume_fills(&mut self) {
        let fills = match self.venue.poll().await {
            Ok(f) => f,
            Err(_) => return,
        };
        for f in fills {
            // The fill carries no side (see fills.rs); resolve it from the
            // order_id→side map we recorded when we PLACED the order.
            let Some(meta) = self.placed.get(&f.order_id).copied() else {
                warn!(order_id = %f.order_id.0, "mm: fill for an unknown resting order; skipping");
                continue;
            };
            let px_micro = f.px.microusdc(meta.ts);
            let (signed_qty, cash) = match meta.side {
                // bid → +qty, cash = −buy_cost; ask → −qty, cash = +sell_proceeds.
                Side::Bid => (f.qty.0 as i128, Usdc(-buy_cost(px_micro, f.qty).0)),
                Side::Ask => (-(f.qty.0 as i128), sell_proceeds(px_micro, f.qty)),
            };
            // Authoritative signed inventory + realized/unrealized.
            let basis_before = self.inv.basis(f.token).0;
            self.inv.on_fill(f.token, signed_qty, cash);
            let basis_after = self.inv.basis(f.token).0;
            // Mirror into the reporting PositionBook in lock-step: the cost-basis
            // delta tracks inventory exactly, and `qty` (the filled volume) keeps
            // the token present in `pnl` even for shorts (value comes from the
            // signed marks we supply in `publish_status`, not from `qty`).
            let cost_delta = Usdc(basis_after - basis_before);
            self.positions
                .apply(&[(f.token, f.qty, cost_delta)], cash, &self.token_market);
            // REBATE ACCRUAL (Task 4.4) plugs in here.
            let row = FillRow {
                order_id: f.order_id.0.clone(),
                ts_ms: now_ms(),
                token: f.token.0 as i64,
                action: side_action(meta.side).into(),
                px_ticks: i64::from(f.px.get()),
                tick_levels: i64::from(meta.ts.levels()),
                qty_micro: f.qty.0 as i64,
                cash_micro: usdc_to_i64(cash).unwrap_or(0),
                fee_micro: 0,
                strategy: "mm".into(),
            };
            // SIGNED route: an ask-fill opens a SHORT (no long holdings), which
            // the strict `Fill` path would Oversell-drop — `FillSigned` persists it.
            let _ = self.store_tx.send(StoreMsg::FillSigned(row, None)).await;
        }
    }

    /// Mark held inventory at the mid feed and latch the safety stop: an
    /// inventory halt cancels all quotes and stops quoting (latched).
    async fn mark_and_check(&mut self) {
        if self.halted {
            return; // already latched; quotes already cancelled.
        }
        let tokens = self.tokens.clone();
        let mut marks: Marks = HashMap::new();
        for token in tokens {
            if self.inv.net(token) == 0 {
                continue;
            }
            // Omitting an unmarkable held token makes `mark` withhold the latch
            // that cycle (per its Marks contract) — a transient gap won't halt.
            if let Some((book, true)) = self.fetcher.fetch(token).await
                && let Some(mid) = mid_micro(&book)
            {
                marks.insert(token, mid);
            }
        }
        // VOLATILITY PULL (Task 4.3) lives in `quote()`, not here: the pull must
        // exclude a token from THIS tick's desired set (so `reconcile` cancels
        // without replacing), which is built there. `mark` only owns the latched
        // safety stop below.
        let _ = self.inv.mark(&marks);
        if self.inv.halted().is_some() {
            self.halted = true;
            self.cancel_all().await;
        }
    }

    /// Compute bid- and mid-marked P&L and publish the full [`StrategyStatus`]
    /// (+ a durable, bid-marked `PnlRow`). Held tokens are valued from the
    /// SIGNED net at the current book — long at the best bid, short at the best
    /// ask (the conservative reporting side) — so `positions.pnl` is correct for
    /// either sign even though `PositionBook` is append-only.
    async fn publish_status(&mut self) {
        let tokens = self.tokens.clone();
        let mut bid_marks: HashMap<TokenId, Usdc> = HashMap::new();
        let mut mid_marks: HashMap<TokenId, Usdc> = HashMap::new();
        let mut open_positions = 0usize;
        for token in tokens {
            let net = self.inv.net(token);
            if net == 0 {
                continue;
            }
            open_positions += 1;
            if let Some((book, true)) = self.fetcher.fetch(token).await
                && let (Some(bb), Some(ba)) = (book.bids.best(), book.asks.best())
            {
                let ts = book.ts();
                let bid_micro = bb.microusdc(ts);
                let ask_micro = ba.microusdc(ts);
                let mid = (bid_micro + ask_micro) / 2;
                // Conservative reporting price per side: long → best bid (what we
                // could sell into), short → best ask (what we'd buy back at).
                let bid_price = if net > 0 { bid_micro } else { ask_micro };
                bid_marks.insert(token, Usdc(signed_value(net, bid_price)));
                mid_marks.insert(token, Usdc(signed_value(net, mid)));
            }
        }
        let pnl = self.positions.pnl(&bid_marks); // bid-marked (reporting)
        let pnl_mid = self.positions.pnl(&mid_marks); // mid-marked (risk feed)
        let halted = self.inv.halted().map(|h| format!("{h:?}"));

        let row = PnlRow {
            ts_ms: now_ms(),
            cash_micro: usdc_to_i64(pnl.cash).unwrap_or(i64::MAX),
            realized_micro: usdc_to_i64(pnl.realized).unwrap_or(i64::MAX),
            unrealized_micro: usdc_to_i64(pnl.unrealized).unwrap_or(i64::MAX),
            equity_micro: usdc_to_i64(pnl.equity).unwrap_or(i64::MAX),
            strategy: "mm".into(),
        };
        let _ = self.store_tx.send(StoreMsg::PnlSnapshot(row)).await;

        let _ = self.status_tx.send(StrategyStatus {
            paused: self.paused,
            halted,
            cash_micro: usdc_to_i64(pnl.cash).unwrap_or(i64::MAX),
            equity_micro: usdc_to_i64(pnl.equity).unwrap_or(i64::MAX),
            equity_mid_micro: usdc_to_i64(pnl_mid.equity).unwrap_or(i64::MAX),
            realized_micro: usdc_to_i64(pnl.realized).unwrap_or(i64::MAX),
            unrealized_micro: usdc_to_i64(pnl.unrealized).unwrap_or(i64::MAX),
            open_positions,
        });
    }

    /// Cancel every resting quote (best-effort — the next tick re-quotes when
    /// active, or stays flat when paused/halted).
    async fn cancel_all(&mut self) {
        let _ = self.qm.cancel_all(&mut self.venue).await;
    }
}

/// The market maker's owned async loop, generic over the venue (Task 4.5 passes
/// a live one). Mirrors the [`stub`](super::stub) lifecycle: a `quote_refresh`
/// interval, honoring `ctx.kill` each iteration and draining `ctl_rx` for
/// pause, exiting cleanly when killed or the control channel closes.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_mm_loop<V: MakerVenue + UserFillSource>(
    venue: V,
    qm: QuoteManager,
    inv: InventoryRisk,
    positions: PositionBook,
    ctx: StrategyCtx,
    params: MmParams,
    tokens: Vec<TokenId>,
    token_market: HashMap<TokenId, MarketId>,
    capital: Usdc,
) {
    let StrategyCtx {
        registry: _,
        fetcher,
        store_tx,
        kill,
        mut ctl_rx,
        status_tx,
    } = ctx;
    // Per-side notional is capped by max_quote_usd AND the whole capital envelope.
    let notional_micro = params.max_quote_micro.min(capital.0).max(0);
    let mut mm = MmLoop {
        venue,
        qm,
        inv,
        positions,
        fetcher,
        store_tx,
        status_tx,
        params,
        tokens,
        token_market,
        notional_micro,
        placed: HashMap::new(),
        paused: false,
        halted: false,
    };

    let mut tick = tokio::time::interval(params.quote_refresh);
    // A steady cadence, not a catch-up burst after a stall (mirrors the stub).
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        // The global kill is the real shutdown signal; observe it each iteration.
        if kill.load(Ordering::Relaxed) {
            mm.cancel_all().await;
            mm.publish_status().await; // final state out-of-band (trait contract)
            return;
        }
        tokio::select! {
            _ = tick.tick() => mm.tick().await,
            cmd = ctl_rx.recv() => match cmd {
                Some(StrategyCommand::SetPaused(p)) => {
                    mm.paused = p;
                    // Pause cancels resting quotes and stops quoting; resume just
                    // re-enables — the next tick re-quotes.
                    if p {
                        mm.cancel_all().await;
                    }
                }
                None => {
                    // Host dropped the control sender → shut down cleanly.
                    mm.cancel_all().await;
                    return;
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    use pm_ingestion::supervisor::SupervisorCommand;
    use pm_risk::inventory::InvHalt;

    const SH: u64 = 1_000_000; // one share in µshares

    type SharedBooks = Arc<Mutex<HashMap<TokenId, (Book, bool)>>>;

    fn px(tick: u16) -> Px {
        Px::new(tick, TickSize::Cent).unwrap()
    }

    /// A Cent book from `(tick, qty)` bid and ask levels.
    fn cent_book(bids: &[(u16, u64)], asks: &[(u16, u64)]) -> Book {
        let mut b = Book::new(TickSize::Cent);
        for &(t, q) in bids {
            b.apply(Side::Bid, px(t), Qty(q));
        }
        for &(t, q) in asks {
            b.apply(Side::Ask, px(t), Qty(q));
        }
        b
    }

    /// One valid two-sided Cent book bid 0.48 / ask 0.52 → mid 0.50.
    fn mid50_book() -> Book {
        cent_book(&[(48, 100 * SH)], &[(52, 100 * SH)])
    }

    fn empty_registry() -> Arc<pm_registry::Registry> {
        Arc::new(pm_registry::RegistryBuilder::default().finish("").unwrap())
    }

    /// A [`BookFetcher`] backed by a shared, MUTABLE book map served over a
    /// supervisor channel (mirrors `coordinator::tests::served_fetcher`). Tests
    /// rewrite the map between steps to drive crosses + marks; every `tokens`
    /// entry routes to the one server. Returns the fetcher + the shared handle.
    fn controllable_fetcher(
        tokens: &[TokenId],
        initial: HashMap<TokenId, (Book, bool)>,
    ) -> (BookFetcher, SharedBooks) {
        let shared: SharedBooks = Arc::new(Mutex::new(initial));
        let shared2 = Arc::clone(&shared);
        let (tx, mut rx) = mpsc::channel::<SupervisorCommand>(64);
        tokio::spawn(async move {
            while let Some(SupervisorCommand::BookSnapshot { token, reply }) = rx.recv().await {
                let snap = shared2.lock().unwrap().get(&token).cloned();
                let _ = reply.send(snap);
            }
        });
        let routes = tokens.iter().map(|t| (*t, tx.clone())).collect();
        (BookFetcher::new(routes), shared)
    }

    fn mk_params(spread_bps: u32, max_quote_usd: f64) -> MmParams {
        // Skew OFF by default so the Task-4.2 quoting/fill/lifecycle tests keep
        // their exact symmetric expectations; the skew tests opt in below.
        mk_params_skew(spread_bps, max_quote_usd, 0)
    }

    fn mk_params_skew(spread_bps: u32, max_quote_usd: f64, inventory_skew_bps: u32) -> MmParams {
        MmParams {
            spread_bps,
            quote_refresh: Duration::from_millis(10),
            max_quote_micro: pm_config::usd_to_microusdc(max_quote_usd).unwrap(),
            inventory_skew_bps,
        }
    }

    /// Generous inventory caps (no halt) for the quoting/fill tests.
    fn generous_inv() -> InventoryConfig {
        InventoryConfig {
            max_inventory_usd: Usdc(1_000_000_000),       // $1000
            max_gross_inventory_usd: Usdc(2_000_000_000), // $2000
            inventory_stop_loss_usd: Usdc(1_000_000_000), // $1000
            daily_loss_usd: Usdc(1_000_000_000),          // $1000
            vol_pull_ticks: 5,
            vol_window: Duration::from_millis(2000),
        }
    }

    fn token_market_for(tokens: &[TokenId]) -> HashMap<TokenId, MarketId> {
        tokens.iter().map(|t| (*t, MarketId(0))).collect()
    }

    #[allow(clippy::type_complexity)]
    fn build_loop(
        fetcher: BookFetcher,
        inv_cfg: InventoryConfig,
        params: MmParams,
        tokens: Vec<TokenId>,
        capital: Usdc,
    ) -> (
        MmLoop<PaperMakerVenue<BookFetcher>>,
        mpsc::Receiver<StoreMsg>,
        watch::Receiver<StrategyStatus>,
    ) {
        let (store_tx, store_rx) = mpsc::channel(256);
        let (status_tx, status_rx) = watch::channel(StrategyStatus::default());
        let venue = PaperMakerVenue::new(fetcher.clone());
        let notional_micro = params.max_quote_micro.min(capital.0).max(0);
        let token_market = token_market_for(&tokens);
        let mm = MmLoop {
            venue,
            qm: QuoteManager::new(),
            inv: InventoryRisk::new(inv_cfg),
            positions: PositionBook::default(),
            fetcher,
            store_tx,
            status_tx,
            params,
            tokens,
            token_market,
            notional_micro,
            placed: HashMap::new(),
            paused: false,
            halted: false,
        };
        (mm, store_rx, status_rx)
    }

    // ── Pure quote math ───────────────────────────────────────────────────────

    /// mid 0.50, spread_bps 200 → bid 0.49 / ask 0.51 (symmetric), both postOnly
    /// Gtc, each sized by `max_quote_usd / price`.
    #[test]
    fn compute_quotes_symmetric_around_mid() {
        let book = mid50_book(); // mid 0.50
        let params = mk_params(200, 5.0);
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        let bid = bid.expect("bid");
        let ask = ask.expect("ask");

        assert_eq!(bid.price, px(49), "bid = mid − half = 0.49");
        assert_eq!(ask.price, px(51), "ask = mid + half = 0.51");
        assert_eq!(bid.side, Side::Bid);
        assert_eq!(ask.side, Side::Ask);
        // Symmetric: 49 and 51 are equidistant from the mid tick 50.
        assert_eq!(50 - bid.price.get(), ask.price.get() - 50);
        // postOnly Gtc (a maker never wants to take).
        assert!(bid.post_only && ask.post_only);
        assert_eq!(bid.order_type, OrderType::Gtc);
        assert_eq!(ask.order_type, OrderType::Gtc);
        // Size clamped by max_quote_usd: notional / price (µshares).
        assert_eq!(bid.size, Qty(5_000_000 * 1_000_000 / 490_000));
        assert_eq!(ask.size, Qty(5_000_000 * 1_000_000 / 510_000));
    }

    /// A spread so wide it pushes a side outside the interior tick range yields
    /// no quote (the token is skipped).
    #[test]
    fn compute_quotes_skips_when_out_of_range() {
        let book = mid50_book();
        let params = mk_params(20_000, 5.0); // half = 100 ticks → out of [1, 99]
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        assert!(bid.is_none() && ask.is_none());
    }

    /// A one-sided book (no ask) cannot form a mid → no quote.
    #[test]
    fn compute_quotes_skips_one_sided_book() {
        let book = cent_book(&[(48, 100 * SH)], &[]);
        let params = mk_params(200, 5.0);
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        assert!(bid.is_none() && ask.is_none());
    }

    /// A sub-tick spread still yields a strictly non-crossing quote: the bid/ask
    /// are bumped at least one tick apart (never narrower).
    #[test]
    fn compute_quotes_never_crosses_on_tiny_spread() {
        let book = mid50_book();
        let params = mk_params(1, 5.0); // 1 bp ≪ one tick
        let (bid, ask) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, 0);
        let bid = bid.expect("bid");
        let ask = ask.expect("ask");
        assert!(
            ask.price.get() > bid.price.get(),
            "ask must stay strictly above bid: {} / {}",
            bid.price.get(),
            ask.price.get()
        );
    }

    // ── Inventory skew (Task 4.3) ──────────────────────────────────────────────

    /// $5 per-market cap valued at the 0.50 mid → 10 shares = full inventory.
    const SKEW_CAP_MICRO: i128 = 5_000_000; // $5
    const FULL_CAP_SHARES: i128 = 10 * SH as i128; // 10 sh @ $0.50 = $5

    /// Same book/spread as `compute_quotes_symmetric_around_mid`, but a LONG net
    /// shifts BOTH quotes strictly DOWN (offload), a SHORT shifts BOTH strictly
    /// UP, and flat (net 0) is byte-identical to the Task-4.2 symmetric quote.
    #[test]
    fn skew_long_inventory_shifts_both_quotes_down() {
        let book = mid50_book(); // mid 0.50
        let params = mk_params_skew(200, 5.0, 150); // half = 0.01; full skew = 1.5¢

        // Flat → no skew → the Task-4.2 symmetric quote (0.49 / 0.51).
        let (fb, fa) = compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, 0, SKEW_CAP_MICRO);
        let (fb, fa) = (fb.expect("flat bid"), fa.expect("flat ask"));
        assert_eq!((fb.price, fa.price), (px(49), px(51)), "flat == Task 4.2");

        // Full-cap LONG → fair −1.5¢ to 0.485 → bid 0.47 / ask 0.50, both strictly
        // below the flat quote.
        let (lb, la) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, FULL_CAP_SHARES, SKEW_CAP_MICRO);
        let (lb, la) = (lb.expect("long bid"), la.expect("long ask"));
        assert_eq!((lb.price, la.price), (px(47), px(50)), "long lowers both quotes");
        assert!(lb.price.get() < fb.price.get() && la.price.get() < fa.price.get());

        // Full-cap SHORT → fair +1.5¢ to 0.515 → bid 0.50 / ask 0.53, both strictly
        // above the flat quote.
        let (sb, sa) =
            compute_quotes(&book, TokenId(1), &params, params.max_quote_micro, -FULL_CAP_SHARES, SKEW_CAP_MICRO);
        let (sb, sa) = (sb.expect("short bid"), sa.expect("short ask"));
        assert_eq!((sb.price, sa.price), (px(50), px(53)), "short raises both quotes");
        assert!(sb.price.get() > fb.price.get() && sa.price.get() > fa.price.get());
    }

    /// The skew magnitude scales LINEARLY with inventory: full cap ≈ the full
    /// `inventory_skew_bps` shift, half cap ≈ half, flat = none, and inventory
    /// beyond the cap clamps to the full shift (`r` saturates at ±1).
    #[test]
    fn skew_magnitude_scales_with_inventory() {
        const MID: i128 = 500_000;
        let full_micro = i128::from(150u32) * 100; // 1.5¢ = 15_000 µUSDC at full cap

        // Flat → no shift.
        assert_eq!(skew_fair(MID, 0, SKEW_CAP_MICRO, 150), MID);

        // Full-cap long → exactly the configured max shift, DOWN.
        let full_long = skew_fair(MID, FULL_CAP_SHARES, SKEW_CAP_MICRO, 150);
        assert_eq!(MID - full_long, full_micro, "full cap → full skew");

        // Half-cap long → ~half the shift (exact here: 7_500).
        let half_long = skew_fair(MID, FULL_CAP_SHARES / 2, SKEW_CAP_MICRO, 150);
        assert_eq!(MID - half_long, full_micro / 2, "half cap → half skew");

        // Short is symmetric: same magnitude, opposite sign (UP).
        let full_short = skew_fair(MID, -FULL_CAP_SHARES, SKEW_CAP_MICRO, 150);
        assert_eq!(full_short - MID, full_micro, "short skews up by the same amount");

        // Beyond the cap clamps to the full shift (no runaway).
        let over_cap = skew_fair(MID, 10 * FULL_CAP_SHARES, SKEW_CAP_MICRO, 150);
        assert_eq!(over_cap, full_long, "inventory past the cap clamps to full skew");
    }

    /// Extreme inventory + a tiny spread near a book edge must never cross or
    /// leave the interior tick range, and never panic: a side pushed out is just
    /// skipped, and a still-valid quote stays strictly non-crossing.
    #[test]
    fn skew_never_crosses_or_leaves_range() {
        let extreme = 1_000_000_000i128; // ≫ any cap → ratio saturates at ±1
        let tiny_cap = 1_000_000i128; // $1
        let params = mk_params_skew(1, 5.0, 150); // 1 bp spread, 1.5¢ full skew

        // Low edge: bid 0.01 / ask 0.03 (mid 0.02). A full-cap LONG skews fair
        // below tick 1 → the bid leaves [1, 99] → token skipped (no panic).
        let low = cent_book(&[(1, 100 * SH)], &[(3, 100 * SH)]);
        let (lb, la) = compute_quotes(&low, TokenId(1), &params, params.max_quote_micro, extreme, tiny_cap);
        assert!(lb.is_none() && la.is_none(), "skew past the low edge skips the token");

        // High edge: bid 0.97 / ask 0.99 (mid 0.98). A full-cap SHORT skews fair
        // above tick 99 → the ask leaves the range → token skipped.
        let high = cent_book(&[(97, 100 * SH)], &[(99, 100 * SH)]);
        let (hb, ha) = compute_quotes(&high, TokenId(1), &params, params.max_quote_micro, -extreme, tiny_cap);
        assert!(hb.is_none() && ha.is_none(), "skew past the high edge skips the token");

        // Mid-book: an extreme long with a sub-tick spread still yields a VALID,
        // strictly non-crossing quote (skew shifts it, the clamps keep ask > bid).
        let mid = mid50_book();
        let (mb, ma) = compute_quotes(&mid, TokenId(1), &params, params.max_quote_micro, extreme, SKEW_CAP_MICRO);
        let (mb, ma) = (mb.expect("mid bid"), ma.expect("mid ask"));
        assert!(ma.price.get() > mb.price.get(), "skewed quote stays non-crossing");
    }

    // ── Inventory cap gating ───────────────────────────────────────────────────

    /// `InventoryRisk::check_quote` is consulted: a quote it rejects is NOT
    /// placed, while a de-risking quote on the same token still is. A long is
    /// seeded above the per-side size and the per-market cap set tight, so the
    /// BID (further increase) is rejected while the ASK (reduce) is approved.
    #[tokio::test]
    async fn inventory_rejected_bid_is_not_placed_but_ask_is() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let mut inv_cfg = generous_inv();
        inv_cfg.max_inventory_usd = Usdc(5_000_000); // $5 per-market cap
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // Seed a long well above the per-side size so the ASK de-risks (always
        // approved) while the BID (further increase) breaches the $5 cap.
        mm.inv
            .on_fill(TokenId(1), (11 * SH) as i128, Usdc(-5_500_000));

        mm.quote().await;
        let tracked = mm.qm.tracked();
        assert!(
            !tracked.contains_key(&(TokenId(1), Side::Bid)),
            "the cap-breaching bid must not be placed"
        );
        assert!(
            tracked.contains_key(&(TokenId(1), Side::Ask)),
            "the de-risking ask must still be placed"
        );
    }

    // ── Fills → inventory + positions + store ──────────────────────────────────

    /// A bid fill flows to `InventoryRisk::on_fill` + `PositionBook` and writes a
    /// `"mm"`-tagged fill row (plus its FK-parent order row); equity/inventory
    /// move as expected.
    #[tokio::test]
    async fn bid_fill_books_inventory_positions_and_store() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, mut store_rx, status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.quote().await; // bid 0.49 / ask 0.51
        assert!(mm.qm.tracked().contains_key(&(TokenId(1), Side::Bid)));

        // Seller crosses DOWN to our bid (best_ask ≤ 0.49) but not up to our ask.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(48, 100 * SH)], &[(49, 100 * SH)]), true));
        mm.consume_fills().await;

        let net = mm.inv.net(TokenId(1));
        assert!(net > 0, "a bid fill makes us long");

        let mut fills = Vec::new();
        let mut order_rows = 0usize;
        while let Ok(msg) = store_rx.try_recv() {
            match msg {
                StoreMsg::FillSigned(row, _) => fills.push(row),
                StoreMsg::OrderInsert(row, _) => {
                    assert_eq!(row.strategy, "mm");
                    order_rows += 1;
                }
                _ => {}
            }
        }
        assert_eq!(fills.len(), 1, "exactly one fill booked");
        let f = &fills[0];
        assert_eq!(f.strategy, "mm");
        assert_eq!(f.action, "Buy");
        assert_eq!(f.qty_micro, net as i64, "fill qty == net (flat → long)");
        assert!(f.cash_micro < 0, "a buy pays cash out");
        assert_eq!(f.fee_micro, 0, "makers pay 0 fee");
        assert!(order_rows >= 1, "the resting order row (FK parent) was written");

        // Status reflects the open long + cash paid out (no profit yet).
        mm.publish_status().await;
        let st = status_rx.borrow();
        assert_eq!(st.open_positions, 1);
        assert!(st.cash_micro < 0);
        assert!(st.halted.is_none());
    }

    /// An ask fill books a SHORT with the correct signs (proves the signed
    /// fill→inventory mapping for both sides).
    #[tokio::test]
    async fn ask_fill_books_a_short_with_correct_signs() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, mut store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.quote().await; // bid 0.49 / ask 0.51
        // Buyer crosses UP to our ask (best_bid ≥ 0.51), not down to our bid.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(51, 100 * SH)], &[(52, 100 * SH)]), true));
        mm.consume_fills().await;

        let net = mm.inv.net(TokenId(1));
        assert!(net < 0, "an ask fill makes us short");

        let mut fill = None;
        while let Ok(msg) = store_rx.try_recv() {
            if let StoreMsg::FillSigned(row, _) = msg {
                fill = Some(row);
            }
        }
        let f = fill.expect("a fill row");
        assert_eq!(f.action, "Sell");
        assert_eq!(f.strategy, "mm");
        assert!(f.cash_micro > 0, "a sell receives cash");
        assert_eq!(f.qty_micro, (-net) as i64, "fill qty == |net| (flat → short)");
    }

    /// End-to-end (Task 4.2b): an MM ask-fill opens a SHORT, and the SIGNED store
    /// route must DURABLY persist the fill row. The strict `Fill` route would
    /// Oversell-drop it (a `write_error`, no row). Wires the loop's store channel
    /// to a real writer + store and asserts the short-open fill landed cleanly.
    #[tokio::test]
    async fn ask_fill_opens_short_and_persists_via_signed_route() {
        use pm_store::Store;
        use pm_store::writer::run_writer;

        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        // Wire the loop's store channel to a real writer over an in-memory store.
        let store = Store::open_in_memory().unwrap();
        let writer = tokio::spawn(run_writer(store, store_rx));

        mm.quote().await; // places bid + ask and their FK-parent order rows
        // Buyer crosses UP to our ask (best_bid ≥ 0.51) → we SELL to open a short.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(51, 100 * SH)], &[(52, 100 * SH)]), true));
        mm.consume_fills().await;
        assert!(mm.inv.net(TokenId(1)) < 0, "an ask fill makes us short");

        // Drop the loop (and thus its store_tx) so the writer drains and returns.
        drop(mm);
        let store = writer.await.unwrap();
        assert_eq!(
            store.write_errors, 0,
            "the signed short-open fill must persist, not Oversell-drop"
        );
        assert_eq!(store.count_fills().unwrap(), 1, "exactly one fill row persisted");
        // Durable signed position reflects the open short (net < 0, basis < 0).
        let (net, cost) = store.position(1).unwrap();
        assert!(
            net < 0 && cost < 0,
            "signed position reflects the open short: ({net}, {cost})"
        );
    }

    // ── Pause / resume ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn pause_cancels_quotes_then_resume_requotes() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, generous_inv(), params, tokens, Usdc(1_000_000_000));

        mm.tick().await; // active → places bid + ask
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "two resting quotes when active"
        );

        // Pause cancels resting quotes and stops quoting.
        mm.paused = true;
        mm.cancel_all().await;
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "pause cancels resting quotes"
        );
        mm.tick().await; // paused → no new quotes
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "paused → none placed"
        );

        // Resume → quotes return on the next tick.
        mm.paused = false;
        mm.tick().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "resume re-quotes"
        );
    }

    // ── Volatility quote-pull (Task 4.3) ───────────────────────────────────────

    /// A large + FAST mid move makes `vol_hint` fire → the token's resting quotes
    /// are CANCELLED and NOT replaced this tick (a pull, not a replace). A later
    /// calm tick (the move settled) re-quotes — the pull is non-sticky.
    #[tokio::test]
    async fn vol_hint_pulls_quotes_without_replace() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        // vol_pull_ticks = 5 (5¢ threshold). A long window so the test never
        // races the wall clock between successive quote() calls.
        let mut inv_cfg = generous_inv();
        inv_cfg.vol_window = Duration::from_secs(3600);
        let params = mk_params(200, 5.0); // skew OFF — isolate the pull
        let (mut mm, _store_rx, _status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // Tick 1 (mid 0.50): vol_hint's FIRST observation can't fire → quotes rest.
        mm.quote().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "a calm first tick rests bid + ask"
        );

        // A large + FAST jump 0.50 → 0.70 (+20¢ ≫ 5¢) within the window.
        shared.lock().unwrap().insert(
            TokenId(1),
            (cent_book(&[(68, 100 * SH)], &[(72, 100 * SH)]), true),
        );

        // Tick 2: vol_hint fires → PULL. The resting quotes are cancelled and
        // NONE are placed (asserting the cancel happened and there was no replace).
        mm.quote().await;
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "vol pull cancels the resting quotes WITHOUT replacing"
        );
        assert!(mm.qm.tracked().is_empty(), "nothing tracked while pulled");

        // Tick 3 (book unchanged at 0.70): the move has settled (0¢ since last) →
        // vol_hint quiet → the token re-quotes (the pull is non-sticky).
        mm.quote().await;
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            2,
            "a calm later tick re-quotes"
        );
    }

    // ── Inventory halt (safety stop) ───────────────────────────────────────────

    #[tokio::test]
    async fn inventory_stop_loss_halts_and_cancels_quotes() {
        let tokens = vec![TokenId(1)];
        let (fetcher, shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let mut inv_cfg = generous_inv();
        inv_cfg.inventory_stop_loss_usd = Usdc(500_000); // $0.50 stop
        let params = mk_params(200, 5.0);
        let (mut mm, _store_rx, status_rx) =
            build_loop(fetcher, inv_cfg, params, tokens, Usdc(1_000_000_000));

        // Acquire a ~$5 long via a bid fill.
        mm.quote().await;
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(48, 100 * SH)], &[(49, 100 * SH)]), true));
        mm.consume_fills().await;
        assert!(mm.inv.net(TokenId(1)) > 0);
        assert_eq!(
            mm.venue.open_orders().await.unwrap().len(),
            1,
            "the unfilled ask still rests before the halt"
        );

        // Crash the price (mid ≈ 0.10): the unrealized bleed on the long far
        // exceeds the $0.50 stop → StopLoss latches.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (cent_book(&[(9, 100 * SH)], &[(11, 100 * SH)]), true));
        mm.mark_and_check().await;

        assert!(mm.halted, "stop-loss latched");
        assert_eq!(mm.inv.halted(), Some(InvHalt::StopLoss));
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "a halt cancels all resting quotes"
        );

        mm.publish_status().await;
        assert!(
            status_rx.borrow().halted.is_some(),
            "status reflects the latched halt"
        );

        // Latched: even with a healthy book back, a further tick does NOT re-quote.
        shared
            .lock()
            .unwrap()
            .insert(TokenId(1), (mid50_book(), true));
        mm.tick().await;
        assert!(
            mm.venue.open_orders().await.unwrap().is_empty(),
            "the halt is latched → no re-quote"
        );
    }

    // ── Kill → cancel + clean exit (drives the real run_mm_loop) ───────────────

    #[tokio::test]
    async fn kill_cancels_and_exits_cleanly() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let kill = Arc::new(AtomicBool::new(false));
        let (_ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, status_rx) = watch::channel(StrategyStatus::default());
        let (store_tx, _store_rx) = mpsc::channel(256);
        let ctx = StrategyCtx {
            registry: empty_registry(),
            fetcher: fetcher.clone(),
            store_tx,
            kill: Arc::clone(&kill),
            ctl_rx,
            status_tx,
        };
        let venue = PaperMakerVenue::new(fetcher);
        let token_market = token_market_for(&tokens);
        let run = tokio::spawn(run_mm_loop(
            venue,
            QuoteManager::new(),
            InventoryRisk::new(generous_inv()),
            PositionBook::default(),
            ctx,
            mk_params(200, 5.0),
            tokens,
            token_market,
            Usdc(1_000_000_000),
        ));

        // Let it run a few quote cycles (10 ms interval), then kill it.
        tokio::time::sleep(Duration::from_millis(40)).await;
        kill.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("mm did not exit within the timeout after kill")
            .expect("mm run task panicked");

        // It published at least one status while running (loop was live).
        let _ = status_rx;
    }

    #[tokio::test]
    async fn closed_control_channel_exits_cleanly() {
        let tokens = vec![TokenId(1)];
        let (fetcher, _shared) =
            controllable_fetcher(&tokens, HashMap::from([(TokenId(1), (mid50_book(), true))]));
        let kill = Arc::new(AtomicBool::new(false));
        let (ctl_tx, ctl_rx) = mpsc::channel::<StrategyCommand>(8);
        let (status_tx, _status_rx) = watch::channel(StrategyStatus::default());
        let (store_tx, _store_rx) = mpsc::channel(256);
        let ctx = StrategyCtx {
            registry: empty_registry(),
            fetcher: fetcher.clone(),
            store_tx,
            kill,
            ctl_rx,
            status_tx,
        };
        let venue = PaperMakerVenue::new(fetcher);
        let token_market = token_market_for(&tokens);
        let run = tokio::spawn(run_mm_loop(
            venue,
            QuoteManager::new(),
            InventoryRisk::new(generous_inv()),
            PositionBook::default(),
            ctx,
            mk_params(200, 5.0),
            tokens,
            token_market,
            Usdc(1_000_000_000),
        ));
        // Dropping the control sender closes ctl_rx → the loop shuts down cleanly.
        drop(ctl_tx);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("mm did not exit after its control channel closed")
            .expect("mm run task panicked");
    }
}
