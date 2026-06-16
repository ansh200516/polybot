//! Per-strategy inventory risk (spec §5, Phase 2). A NEW risk module for
//! risk-taking, **inventory-bearing** strategies (market-making first). It is
//! SEPARATE from the risk-free-basket `RiskEngine` in `lib.rs` — arb keeps using
//! that one untouched. This module is inert until a strategy opts in (Phase 4).
//!
//! Implemented so far: inventory STATE + `on_fill` accounting (Task 2.1), the
//! pre-fill cap check `check_quote` (Task 2.2), and mark-to-market `mark` with
//! the latched stop-loss (Task 2.3). The flatten directive, the volatility
//! hint, the daily-loss check, and config-file parsing (Task 2.4) are
//! intentionally absent here.

use std::collections::HashMap;

use pm_core::instrument::TokenId;
use pm_core::num::{div_ceil_i128, Usdc, ONE_SHARE_MICRO};

/// Per-strategy inventory caps (spec §5, all µUSDC). Defined now so the type is
/// stable across Phase 2, but this task only STORES it — the cap CHECKS land in
/// Tasks 2.2–2.4. Per-strategy and conservative by config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryConfig {
    /// Per-market net exposure cap (`|net·mark|` per market). Enforced by
    /// `check_quote` (Task 2.2).
    pub max_inventory_usd: Usdc,
    /// Gross inventory cap, summed across markets. Enforced by `check_quote`
    /// (Task 2.2).
    pub max_gross_inventory_usd: Usdc,
    /// Mark-to-market loss that latches an inventory halt + flatten. Task 2.4.
    pub inventory_stop_loss_usd: Usdc,
    /// Per-strategy daily realized+unrealized floor. Task 2.4.
    pub daily_loss_usd: Usdc,
}

/// One token's signed-net + average-cost accounting (the per-token view).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenInventory {
    /// Signed net inventory in µshares (1 share = 1e6 µshares). `>0` long (net
    /// of buys), `<0` short (net of sells), `0` flat. (`Qty` is unsigned, so
    /// fills arrive signed.)
    pub net: i128,
    /// Signed cost basis of the OPEN position, in µUSDC (the net cash invested:
    /// `basis -= cash` per fill). Equivalently `basis = net · avg_price / 1e6`
    /// (`net` µshares × `avg_price` µUSDC/share ÷ 1e6 µshares/share = µUSDC).
    /// `>0` for longs (cash paid in), `<0` for shorts (cash taken in), `0` flat.
    ///
    /// Mark-to-market (Task 2.3) values `net` at a per-share µUSDC `mark` and
    /// MUST apply the same ÷1e6 share scaling:
    /// `unrealized = net·mark/1e6 − basis` (µUSDC, uniform for longs & shorts).
    pub basis: Usdc,
    /// Realized P&L booked on the closed/reduced size so far, µUSDC.
    pub realized: Usdc,
}

/// Plain-data view of a prospective quote for `check_quote` (Task 2.2).
///
/// Mirrors the `BasketCheck` convention in `lib.rs`: `pm-risk` must NOT depend
/// on `pm-execution`, so this is the risk-side intent that the Phase-4
/// market-making strategy converts its `MakerOrder` into — `pm-risk` never sees
/// the engine/execution types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteIntent {
    /// Token the quote would trade.
    pub token: TokenId,
    /// Signed fill size this quote could take, in µshares: `+` buy, `−` sell
    /// (same sign convention as `on_fill`'s `signed_qty`).
    pub signed_qty: i128,
    /// Quote price in µUSDC per share, `0..=1_000_000` (= $0.00..=$1.00). Used
    /// as the best-available mark for this token's projected notional.
    pub price_micro: u64,
}

/// Why `check_quote` rejected a quote (mirrors `lib.rs`'s `RejectReason`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvReason {
    /// Projected per-token notional would exceed `max_inventory_usd`.
    PerMarketInventory,
    /// Projected gross exposure (Σ across tokens) would exceed
    /// `max_gross_inventory_usd`.
    GrossInventory,
}

/// Verdict for a pre-fill quote check (mirrors `lib.rs`'s `RiskVerdict`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteVerdict {
    Approve,
    Reject(InvReason),
}

/// Per-token mark prices for `mark`: µUSDC per share, each `0..=1_000_000`
/// (= $0.00..=$1.00). The **caller** chooses the conservative price for each
/// side it holds — e.g. the best BID for longs (what it could sell into) and
/// the best ASK for shorts (what it would have to buy back at) — `mark` just
/// applies whatever price is supplied per token. (See the coordinator's
/// `marks_pair` for how the app already produces conservative per-token marks,
/// mapping a missing book → 0.)
pub type Marks = HashMap<TokenId, u64>;

/// Why an inventory halt latched. Sticky once set (spec §5: "like
/// `SessionLoss`") — mirrors `lib.rs`'s `HaltReason`: there is no clear path,
/// the strategy restarts to clear it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvHalt {
    /// Mark-to-market P&L (`realized + unrealized`) fell to
    /// `−inventory_stop_loss_usd` or lower. (The `daily_loss_usd` check — which
    /// needs a daily baseline this module does not yet track — is DEFERRED to
    /// Task 2.4, so there is no `DailyLoss` variant yet.)
    StopLoss,
}

/// Snapshot returned by `mark`: the marked P&L split plus any latched halt.
/// Plain data (no engine types), like `QuoteVerdict` and `lib.rs`'s verdicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryStatus {
    /// Signed net µshares for every token with recorded state, sorted by
    /// `TokenId` (deterministic; includes tokens that have round-tripped back
    /// to flat — `net` 0 — whose `realized` still counts in the sum below).
    pub net_by_token: Vec<(TokenId, i128)>,
    /// Σ realized P&L across all tokens, µUSDC.
    pub realized: Usdc,
    /// Σ unrealized (marked) P&L across all tokens, µUSDC. Each token's
    /// `net·mark/1e6` value term is rounded against us (see `mark`).
    pub unrealized: Usdc,
    /// `realized + unrealized`, µUSDC — the measure the stop-loss keys off.
    pub mtm_pnl: Usdc,
    /// The latched inventory halt, if any (sticky once set).
    pub halted: Option<InvHalt>,
}

/// Per-strategy inventory state for one inventory-bearing strategy.
///
/// Accounting model — **average cost** (chosen for simplicity and a clean
/// mark-to-market seam for Task 2.3):
/// - **Open / increase** the current side: add the fill's outlay/proceeds to
///   basis (`basis -= cash`, since `cash` is `−paid` on a buy / `+received` on a
///   sell). The running average price is `basis / net`.
/// - **Reduce / close** the current side: release basis pro-rata to the fraction
///   closed (`basis ← basis · new_net / old_net`) and book
///   `realized += cash − released_basis`.
/// - **Flip** through flat (e.g. sell more than the long): close ALL of the old
///   side (realizing on it at the fill price), then open the remainder on the
///   opposite side at that same fill price.
///
/// This yields the exact, rounding-independent invariant
/// `realized − basis = Σ cash` per token, so `equity = Σ cash + mark_value` and
/// Task 2.3's realized/unrealized split is internally consistent.
pub struct InventoryRisk {
    cfg: InventoryConfig,
    by_token: HashMap<TokenId, TokenInventory>,
    /// Latched inventory halt (sticky): set by `mark`, never auto-cleared.
    halted: Option<InvHalt>,
}

impl InventoryRisk {
    pub fn new(cfg: InventoryConfig) -> Self {
        InventoryRisk {
            cfg,
            by_token: HashMap::new(),
            halted: None,
        }
    }

    /// The stored caps. The cap CHECKS that consume these land in Tasks 2.2–2.4.
    pub fn config(&self) -> &InventoryConfig {
        &self.cfg
    }

    /// Apply a fill to this token's inventory (average-cost model; see the type
    /// docs for the full model and invariant).
    ///
    /// - `signed_qty`: fill size in µshares, SIGNED — `+` buy, `−` sell (`Qty`
    ///   itself is unsigned, so the caller carries the side as the sign here).
    /// - `cash`: signed µUSDC, consistent with the rest of the money path —
    ///   `−` paid out on a buy, `+` received on a sell.
    ///
    /// A `signed_qty == 0` fill is a no-op (no inventory or basis meaning).
    pub fn on_fill(&mut self, token: TokenId, signed_qty: i128, cash: Usdc) {
        if signed_qty == 0 {
            return;
        }
        let e = self.by_token.entry(token).or_default();
        let old_net = e.net;
        let new_net = old_net + signed_qty;
        let dc = cash.0;

        if old_net == 0 || (old_net > 0) == (signed_qty > 0) {
            // Open from flat, or increase the existing side: the fill's whole
            // outlay/proceeds joins the average-cost basis.
            e.basis.0 -= dc;
        } else if signed_qty.unsigned_abs() <= old_net.unsigned_abs() {
            // Reduce/close the existing side without flipping (new_net keeps the
            // old sign, or hits 0): release basis pro-rata, book realized.
            let new_basis = e.basis.0 * new_net / old_net;
            let released = e.basis.0 - new_basis;
            e.realized.0 += dc - released;
            e.basis.0 = new_basis;
        } else {
            // Flip: close ALL of old_net (realize on it), then open the
            // remainder on the opposite side — split this fill's cash by shares.
            let dc_close = dc * old_net.abs() / signed_qty.abs();
            let dc_open = dc - dc_close;
            e.realized.0 += dc_close - e.basis.0;
            e.basis.0 = -dc_open;
        }
        e.net = new_net;
    }

    /// Pre-fill inventory cap check for a single quote (spec §5, Task 2.2).
    /// Read-only and INERT until a strategy calls it (Phase 4). Mirrors the
    /// plain-data, inclusive-boundary style of `RiskEngine::pre_check`.
    ///
    /// Units & rounding (all i128, µUSDC): a token's `net` is µshares and a
    /// `notional` is `ceil(|net|·price / 1e6)` µUSDC (µshares × µUSDC/share ÷
    /// 1e6 µshares/share), the same scaling as the `basis = net·avg_price/1e6`
    /// identity on `TokenInventory`. The `/1e6` rounds UP (against us, per
    /// `num.rs`'s `div_ceil_i128`) so a cap OVERSTATES exposure and binds on the
    /// safe side; the overstatement is sub-µUSDC.
    ///
    /// Policy:
    /// - **De-risking is never blocked.** If the quote does not grow the token's
    ///   net magnitude (`|proj_net| ≤ |net|`) it is always `Approve`, even when
    ///   the position is already over a cap — moving toward flat must not gate.
    /// - Otherwise the quote INCREASES exposure and both caps apply:
    ///   - Per-market: projected token notional `ceil(|proj_net|·price/1e6)`.
    ///     Over `max_inventory_usd` → `Reject(PerMarketInventory)`.
    ///   - Gross: that projected notional + Σ `|basis|` of every OTHER token.
    ///     Other tokens use their cost-basis magnitude as the exposure proxy —
    ///     this module holds no live marks for them, and the quote carries the
    ///     only fresh price. Over `max_gross_inventory_usd` →
    ///     `Reject(GrossInventory)`. (Reducing quotes already returned early, so
    ///     gross is only ever checked on increasing quotes.)
    ///
    /// Boundaries are INCLUSIVE (`> cap` rejects, `== cap` approves), matching
    /// `RiskEngine::pre_check`.
    pub fn check_quote(&self, q: &QuoteIntent) -> QuoteVerdict {
        let net = self.net(q.token);
        let proj_net = net + q.signed_qty;

        // De-risking (toward flat) is always allowed, even when over a cap.
        if proj_net.abs() <= net.abs() {
            return QuoteVerdict::Approve;
        }

        // Increasing exposure: value the quote's token at its own quote price,
        // rounding the notional UP (against us) so the cap binds on the safe
        // side. This same `proj_notional` also feeds the gross sum below.
        let proj_notional = div_ceil_i128(
            proj_net.abs() * i128::from(q.price_micro),
            i128::from(ONE_SHARE_MICRO),
        );
        if proj_notional > self.cfg.max_inventory_usd.0 {
            return QuoteVerdict::Reject(InvReason::PerMarketInventory);
        }

        // Gross = this token's projected notional + every OTHER token's
        // cost-basis magnitude (no live marks for them here).
        let mut gross = proj_notional;
        for (other, ti) in &self.by_token {
            if *other != q.token {
                gross += ti.basis.0.abs();
            }
        }
        if gross > self.cfg.max_gross_inventory_usd.0 {
            return QuoteVerdict::Reject(InvReason::GrossInventory);
        }

        QuoteVerdict::Approve
    }

    /// Mark-to-market the whole book at the supplied per-token `marks`, returning
    /// the realized/unrealized split, the combined `mtm_pnl`, and any latched
    /// halt. INERT until a strategy calls it (Phase 4); the Phase-4 MM strategy
    /// feeds the marks (conservative per side — see [`Marks`]).
    ///
    /// **Unrealized** (µUSDC) sums each token's `floor(net·mark/1e6) − basis` —
    /// the same ÷1e6 share scaling as the `basis = net·avg_price/1e6` identity
    /// on [`TokenInventory`]. The `net·mark/1e6` value term is rounded **toward
    /// −∞** with `i128::div_euclid` (a true floor here, since the 1e6 divisor is
    /// positive — `div_ceil_i128` only handles non-negative numerators, so it
    /// can't value shorts). Flooring is "against us" on BOTH sides: a long's
    /// value floors DOWN, and a short's negative value floors to a LARGER
    /// buy-back liability — so a marked loss is never understated and the
    /// stop-loss binds on the safe side. `realized` sums every token's booked
    /// P&L; `mtm_pnl = realized + unrealized`.
    ///
    /// **Missing marks** (token absent from `marks`) are valued at the side's
    /// WORST case: a long at mark 0 (worth nothing), a short at mark 1_000_000
    /// ($1.00 — the maximal buy-back cost). This is the conservative, fail-safe
    /// choice and generalizes the app's existing "missing book → 0" mark
    /// (`marks_pair`), which is worst-case for the longs arb holds. Because the
    /// latch is STICKY, callers should supply a mark for EVERY held token (0
    /// when a book is unavailable, exactly as `marks_pair` does) so a transient
    /// data gap can't permanently halt; a token genuinely absent here is treated
    /// as unmarkable and fails safe toward the halt.
    ///
    /// **Latch:** if not already halted and `mtm_pnl ≤ −inventory_stop_loss_usd`,
    /// the `StopLoss` halt latches. First halt wins and recovery never clears it
    /// (sticky), mirroring `RiskEngine::update_session_pnl`; the boundary is
    /// inclusive (exactly `−cap` trips), matching the session-loss cap.
    pub fn mark(&mut self, marks: &Marks) -> InventoryStatus {
        let mut realized: i128 = 0;
        let mut unrealized: i128 = 0;
        let mut net_by_token: Vec<(TokenId, i128)> = Vec::with_capacity(self.by_token.len());

        for (&token, ti) in &self.by_token {
            realized += ti.realized.0;
            net_by_token.push((token, ti.net));

            // Conservative price for a missing token: worst case per side — a
            // long at $0.00 (worthless), a short at $1.00 (maximal buy-back).
            let mark_price = match marks.get(&token) {
                Some(&m) => i128::from(m),
                None if ti.net < 0 => i128::from(ONE_SHARE_MICRO),
                None => 0,
            };
            // floor(net·mark/1e6): against us on BOTH sides (div_euclid floors
            // toward −∞ because the 1e6 divisor is positive).
            let value = (ti.net * mark_price).div_euclid(i128::from(ONE_SHARE_MICRO));
            unrealized += value - ti.basis.0;
        }
        net_by_token.sort_by_key(|&(t, _)| t);

        let mtm = realized + unrealized;
        // Sticky stop-loss latch: first halt wins, recovery never clears it
        // (mirrors update_session_pnl). daily_loss_usd is DEFERRED to Task 2.4.
        if self.halted.is_none() && mtm <= -self.cfg.inventory_stop_loss_usd.0 {
            self.halted = Some(InvHalt::StopLoss);
        }

        InventoryStatus {
            net_by_token,
            realized: Usdc(realized),
            unrealized: Usdc(unrealized),
            mtm_pnl: Usdc(mtm),
            halted: self.halted,
        }
    }

    /// The latched inventory halt, if any. Sticky: once `mark` sets it there is
    /// no clear path — the strategy restarts to clear it (mirrors
    /// `RiskEngine::halted`).
    pub fn halted(&self) -> Option<InvHalt> {
        self.halted
    }

    /// Signed net inventory for `token` in µshares (`0` if untouched).
    pub fn net(&self, token: TokenId) -> i128 {
        self.by_token.get(&token).map_or(0, |e| e.net)
    }

    /// Signed cost basis for `token` in µUSDC (`0` if untouched).
    pub fn basis(&self, token: TokenId) -> Usdc {
        self.by_token.get(&token).map_or(Usdc(0), |e| e.basis)
    }

    /// Realized P&L booked for `token` in µUSDC (`0` if untouched).
    pub fn realized(&self, token: TokenId) -> Usdc {
        self.by_token.get(&token).map_or(Usdc(0), |e| e.realized)
    }

    /// Full per-token view (net + basis + realized); `None` if untouched.
    pub fn token(&self, token: TokenId) -> Option<TokenInventory> {
        self.by_token.get(&token).copied()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::instrument::TokenId;
    use pm_core::num::Usdc;

    /// Caps are inert this task; any values work for constructing the state.
    fn cfg() -> InventoryConfig {
        InventoryConfig {
            max_inventory_usd: Usdc(500_000_000),       // $500
            max_gross_inventory_usd: Usdc(2_000_000_000), // $2k
            inventory_stop_loss_usd: Usdc(100_000_000), // $100
            daily_loss_usd: Usdc(200_000_000),          // $200
        }
    }

    const SHARE: i128 = 1_000_000; // 1 share = 1e6 µshares
    /// $1.00/share. At this price a token's notional in µUSDC equals its net in
    /// µshares numerically, so cap boundaries land on round numbers.
    const PRICE_1USD: u64 = 1_000_000;

    #[test]
    fn new_token_is_flat() {
        let inv = InventoryRisk::new(cfg());
        assert_eq!(inv.net(TokenId(7)), 0);
        assert_eq!(inv.basis(TokenId(7)), Usdc(0));
        assert_eq!(inv.realized(TokenId(7)), Usdc(0));
        assert_eq!(inv.token(TokenId(7)), None);
    }

    #[test]
    fn config_is_stored() {
        let inv = InventoryRisk::new(cfg());
        assert_eq!(*inv.config(), cfg());
    }

    #[test]
    fn on_fill_accumulates_signed_net_and_basis() {
        let t = TokenId(1);
        let mut inv = InventoryRisk::new(cfg());

        // 1) Buy +100 sh @ $0.40 → outlay $40 (cash paid out, negative).
        inv.on_fill(t, 100 * SHARE, Usdc(-40_000_000));
        assert_eq!(inv.net(t), 100 * SHARE, "net long after buy");
        assert_eq!(inv.basis(t), Usdc(40_000_000), "basis = outlay");
        assert_eq!(inv.realized(t), Usdc(0), "no realized on an open");

        // 2) Sell −40 sh @ $0.50 → proceeds $20 (cash received, positive).
        //    Reduce long 100→60; avg-cost basis releases pro-rata: 40·60/100=24.
        inv.on_fill(t, -40 * SHARE, Usdc(20_000_000));
        assert_eq!(inv.net(t), 60 * SHARE, "net reduced");
        assert_eq!(inv.basis(t), Usdc(24_000_000), "basis released pro-rata");
        // realized = proceeds 20 − released basis (40−24=16) = +4.
        assert_eq!(inv.realized(t), Usdc(4_000_000), "realized on the 40 sold");

        // 3) Sell −100 sh @ $0.55 → proceeds $55. This FLIPS long 60 → short 40.
        //    Close 60 (proceeds 60·0.55=$33, basis $24 → realize +9), then open
        //    short 40 at $0.55 (received 40·0.55=$22 → basis −22).
        inv.on_fill(t, -100 * SHARE, Usdc(55_000_000));
        assert_eq!(inv.net(t), -40 * SHARE, "flipped to short");
        assert_eq!(inv.basis(t), Usdc(-22_000_000), "short basis = −proceeds");
        assert_eq!(inv.realized(t), Usdc(13_000_000), "4 + 9 realized total");

        // Cash-conservation invariant: realized − basis = Σ cash.
        let sum_cash = -40_000_000 + 20_000_000 + 55_000_000;
        assert_eq!(inv.realized(t).0 - inv.basis(t).0, sum_cash);

        // The per-token view matches the accessors.
        assert_eq!(
            inv.token(t),
            Some(TokenInventory {
                net: -40 * SHARE,
                basis: Usdc(-22_000_000),
                realized: Usdc(13_000_000),
            })
        );
    }

    #[test]
    fn sell_to_exactly_flat_zeroes_basis() {
        let t = TokenId(2);
        let mut inv = InventoryRisk::new(cfg());
        inv.on_fill(t, 100 * SHARE, Usdc(-30_000_000)); // buy 100 @ $0.30
        inv.on_fill(t, -100 * SHARE, Usdc(35_000_000)); // sell 100 @ $0.35
        assert_eq!(inv.net(t), 0, "flat");
        assert_eq!(inv.basis(t), Usdc(0), "basis fully released");
        assert_eq!(inv.realized(t), Usdc(5_000_000), "35 − 30 = +5 realized");
    }

    #[test]
    fn short_then_cover_accounts_symmetrically() {
        let t = TokenId(3);
        let mut inv = InventoryRisk::new(cfg());
        // Open short: sell 50 @ $0.60 → received $30 → basis −30.
        inv.on_fill(t, -50 * SHARE, Usdc(30_000_000));
        assert_eq!(inv.net(t), -50 * SHARE);
        assert_eq!(inv.basis(t), Usdc(-30_000_000), "short basis is negative");
        assert_eq!(inv.realized(t), Usdc(0));
        // Cover 20 @ $0.45 → paid $9. Reduce short −50→−30; basis −30·(−30/−50)=−18.
        inv.on_fill(t, 20 * SHARE, Usdc(-9_000_000));
        assert_eq!(inv.net(t), -30 * SHARE, "short reduced");
        assert_eq!(inv.basis(t), Usdc(-18_000_000), "short basis released pro-rata");
        // released = −30 − (−18) = −12; realized = cash(−9) − (−12) = +3.
        assert_eq!(inv.realized(t), Usdc(3_000_000), "bought back 20 cheaper");
    }

    #[test]
    fn on_fill_is_independent_per_token() {
        let (a, b) = (TokenId(10), TokenId(20));
        let mut inv = InventoryRisk::new(cfg());

        inv.on_fill(a, 50 * SHARE, Usdc(-15_000_000)); // buy A 50 @ $0.30
        inv.on_fill(b, 20 * SHARE, Usdc(-12_000_000)); // buy B 20 @ $0.60

        // Closing A out at a loss must not perturb B.
        inv.on_fill(a, -50 * SHARE, Usdc(10_000_000)); // sell A 50 @ $0.20
        assert_eq!(inv.net(a), 0);
        assert_eq!(inv.basis(a), Usdc(0));
        assert_eq!(inv.realized(a), Usdc(-5_000_000), "10 − 15 = −5 on A");

        assert_eq!(inv.net(b), 20 * SHARE, "B untouched");
        assert_eq!(inv.basis(b), Usdc(12_000_000), "B untouched");
        assert_eq!(inv.realized(b), Usdc(0), "B untouched");
    }

    #[test]
    fn zero_qty_fill_is_a_noop() {
        let t = TokenId(4);
        let mut inv = InventoryRisk::new(cfg());
        inv.on_fill(t, 10 * SHARE, Usdc(-4_000_000));
        inv.on_fill(t, 0, Usdc(999)); // ignored
        assert_eq!(inv.net(t), 10 * SHARE);
        assert_eq!(inv.basis(t), Usdc(4_000_000));
        assert_eq!(inv.realized(t), Usdc(0));
    }

    #[test]
    fn on_fill_flips_short_to_long() {
        // Mirror image of the long→short flip in
        // `on_fill_accumulates_signed_net_and_basis`: open short, reduce it, then
        // buy through flat into a net LONG (basis goes from negative to positive).
        let t = TokenId(5);
        let mut inv = InventoryRisk::new(cfg());

        // 1) Sell-to-open −100 sh @ $0.40 → received $40 (cash positive).
        inv.on_fill(t, -100 * SHARE, Usdc(40_000_000));
        assert_eq!(inv.net(t), -100 * SHARE, "net short after sell-to-open");
        assert_eq!(inv.basis(t), Usdc(-40_000_000), "short basis = −proceeds");
        assert_eq!(inv.realized(t), Usdc(0), "no realized on an open");

        // 2) Buy +40 sh @ $0.30 → paid $12 (cash negative). Reduce short −100→−60;
        //    avg-cost basis releases pro-rata: −40·(−60/−100) = −24.
        inv.on_fill(t, 40 * SHARE, Usdc(-12_000_000));
        assert_eq!(inv.net(t), -60 * SHARE, "short reduced");
        assert_eq!(inv.basis(t), Usdc(-24_000_000), "basis released pro-rata");
        // released = −40 − (−24) = −16; realized = cash(−12) − (−16) = +4.
        assert_eq!(inv.realized(t), Usdc(4_000_000), "realized on the 40 covered");

        // 3) Buy +100 sh @ $0.45 → paid $45. This FLIPS short 60 → long 40.
        //    Close 60 (paid 60·0.45=$27 to cover basis −$24 → realize −3), then
        //    open long 40 at $0.45 (paid 40·0.45=$18 → basis +18, now POSITIVE).
        inv.on_fill(t, 100 * SHARE, Usdc(-45_000_000));
        assert_eq!(inv.net(t), 40 * SHARE, "flipped to long");
        assert_eq!(inv.basis(t), Usdc(18_000_000), "long basis = outlay, positive");
        assert_eq!(inv.realized(t), Usdc(1_000_000), "4 + (−3) realized total");

        // Cash-conservation invariant: realized − basis = Σ cash.
        let sum_cash = 40_000_000 - 12_000_000 - 45_000_000;
        assert_eq!(inv.realized(t).0 - inv.basis(t).0, sum_cash);

        assert_eq!(
            inv.token(t),
            Some(TokenInventory {
                net: 40 * SHARE,
                basis: Usdc(18_000_000),
                realized: Usdc(1_000_000),
            })
        );
    }

    /// Deterministic xorshift64 PRNG with a FIXED seed. `pm-risk` has no
    /// `proptest` dev-dependency (checked `Cargo.toml`), so the rounding
    /// invariant is exercised with this hand-rolled, reproducible loop instead.
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Rng(seed)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        /// Uniform-ish integer in `[lo, hi]` (inclusive).
        fn range(&mut self, lo: i128, hi: i128) -> i128 {
            let span = (hi - lo + 1) as u128;
            lo + (u128::from(self.next_u64()) % span) as i128
        }
        fn coin(&mut self) -> bool {
            self.next_u64() & 1 == 1
        }
    }

    #[test]
    fn rounding_never_breaks_cash_conservation() {
        // Randomized invariant (fixed seed → reproducible). ODD, non-divisible
        // quantities and prices force the pro-rata basis division
        // (`basis·new_net/old_net`) and the flip cash split to truncate; the
        // invariant `realized − basis = Σ cash` must STILL hold EXACTLY after
        // every fill — rounding only shifts dust between the realized and
        // unrealized buckets, never their sum (so total equity is never wrong).
        let t = TokenId(99);
        let mut inv = InventoryRisk::new(cfg());
        let mut rng = Rng::new(0x5DEE_CE66_D123_4567);
        let mut sum_cash: i128 = 0;
        let mut reduce_or_flip = 0u32;

        for _ in 0..500 {
            let old_net = inv.net(t);
            let qty = rng.range(1, 12_345_678) | 1; // `| 1` → always odd µshares
            let price = rng.range(1, 999_983); // µUSDC/share, large-prime ceiling
            let buy = rng.coin();
            let signed_qty = if buy { qty } else { -qty };
            // cash = ∓ price·qty/1e6, sign per the money path (− buy / + sell).
            let mag = price * qty / SHARE;
            let cash = Usdc(if buy { -mag } else { mag });

            // Count the fills that hit the rounding-prone reduce/flip branches.
            if old_net != 0 && (old_net > 0) != buy {
                reduce_or_flip += 1;
            }

            inv.on_fill(t, signed_qty, cash);
            sum_cash += cash.0;
            assert_eq!(
                inv.realized(t).0 - inv.basis(t).0,
                sum_cash,
                "cash conservation must hold exactly after every fill"
            );
        }
        assert!(
            reduce_or_flip > 0,
            "sequence must exercise the reduce/flip rounding branches"
        );
    }

    // ── Task 2.2: check_quote pre-fill cap enforcement ────────────────────────

    #[test]
    fn check_quote_rejects_increase_over_per_market_cap() {
        let inv = InventoryRisk::new(cfg()); // max_inventory_usd = $500
        let t = TokenId(1);
        // Flat → buy 550 sh @ $1.00 = $550 notional > $500 cap → reject.
        let over = QuoteIntent {
            token: t,
            signed_qty: 550 * SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(
            inv.check_quote(&over),
            QuoteVerdict::Reject(InvReason::PerMarketInventory)
        );
        // Flat → buy 450 sh @ $1.00 = $450 < $500 → approve.
        let under = QuoteIntent {
            token: t,
            signed_qty: 450 * SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(inv.check_quote(&under), QuoteVerdict::Approve);
    }

    #[test]
    fn check_quote_per_market_boundary_is_inclusive() {
        // Mirrors RiskEngine's `caps_are_inclusive_at_exact_boundary`: exactly
        // at the cap approves, one µUSDC over rejects.
        let inv = InventoryRisk::new(cfg());
        let t = TokenId(1);
        let cap = cfg().max_inventory_usd.0; // 500_000_000 µUSDC
        // At $1.00/share, notional (µUSDC) == net (µshares), so net = cap is exact.
        let at = QuoteIntent {
            token: t,
            signed_qty: cap,
            price_micro: PRICE_1USD,
        };
        assert_eq!(
            inv.check_quote(&at),
            QuoteVerdict::Approve,
            "exactly at the per-market cap approves"
        );
        let over = QuoteIntent {
            token: t,
            signed_qty: cap + 1,
            price_micro: PRICE_1USD,
        };
        assert_eq!(
            inv.check_quote(&over),
            QuoteVerdict::Reject(InvReason::PerMarketInventory)
        );
    }

    #[test]
    fn check_quote_approves_any_reducing_quote_even_over_cap() {
        let mut inv = InventoryRisk::new(cfg());
        let t = TokenId(1);
        // Build a position WELL over the $500 per-market cap: long 2000 sh @ $1.00.
        inv.on_fill(t, 2000 * SHARE, Usdc(-2_000_000_000)); // net +2000, basis $2000
        // A sell that shrinks the long is approved despite being over-cap.
        let reduce = QuoteIntent {
            token: t,
            signed_qty: -500 * SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(inv.check_quote(&reduce), QuoteVerdict::Approve);
        // A sell that overshoots through flat to a SMALLER short still reduces
        // magnitude (|−100| < |+2000|) → approved.
        let flip_smaller = QuoteIntent {
            token: t,
            signed_qty: -2100 * SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(inv.check_quote(&flip_smaller), QuoteVerdict::Approve);
        // But ADDING even one share to the already over-cap long is rejected.
        let add = QuoteIntent {
            token: t,
            signed_qty: SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(
            inv.check_quote(&add),
            QuoteVerdict::Reject(InvReason::PerMarketInventory)
        );
    }

    #[test]
    fn check_quote_gross_cap_sums_other_tokens_basis() {
        // per-market $500, gross $2000. Token A carries $1700 of basis; a fresh
        // quote on token B is within ITS per-market cap but pushes Σ over gross.
        let mut inv = InventoryRisk::new(cfg());
        let (a, b) = (TokenId(10), TokenId(20));
        inv.on_fill(a, 1700 * SHARE, Usdc(-1_700_000_000)); // A basis $1700
        // B: buy 400 sh @ $1.00 = $400 ≤ $500 per-market, but gross 1700+400 =
        // $2100 > $2000.
        let q = QuoteIntent {
            token: b,
            signed_qty: 400 * SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(
            inv.check_quote(&q),
            QuoteVerdict::Reject(InvReason::GrossInventory)
        );
        // The SAME B quote with A absent (gross = $400) is approved — proving the
        // rejection above was the gross cap, not B's own per-market cap.
        let inv_solo = InventoryRisk::new(cfg());
        assert_eq!(inv_solo.check_quote(&q), QuoteVerdict::Approve);
    }

    #[test]
    fn check_quote_gross_boundary_is_inclusive() {
        // Σ exactly == gross cap approves; one µUSDC over rejects.
        let mut inv = InventoryRisk::new(cfg()); // gross $2000
        let (a, b) = (TokenId(10), TokenId(20));
        inv.on_fill(a, 1500 * SHARE, Usdc(-1_500_000_000)); // A basis $1500
        // B $500 (== its per-market cap, inclusive) → gross 1500+500 = $2000 ==
        // cap → approve.
        let at = QuoteIntent {
            token: b,
            signed_qty: 500 * SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(
            inv.check_quote(&at),
            QuoteVerdict::Approve,
            "gross exactly at the cap approves"
        );
        // Add $1 to A's basis → gross $2001 > $2000 → reject (B still within its
        // own per-market cap, so the gross cap is what trips).
        inv.on_fill(a, SHARE, Usdc(-1_000_000)); // A basis $1501
        assert_eq!(
            inv.check_quote(&at),
            QuoteVerdict::Reject(InvReason::GrossInventory)
        );
    }

    #[test]
    fn check_quote_rejects_flip_to_larger_opposite() {
        // Holding a small long, a sell big enough to flip THROUGH flat into a
        // LARGER short (|proj_net| > |net|) is an INCREASE in exposure, not a
        // reduce — so it must face the per-market cap, NOT be auto-approved by
        // the reduce rule.
        let mut inv = InventoryRisk::new(cfg()); // max_inventory_usd = $500
        let t = TokenId(1);
        inv.on_fill(t, 100 * SHARE, Usdc(-100_000_000)); // net +100 sh, basis $100
        // Sell 700 sh → proj_net = −600 sh; |−600| > |+100|, notional 600·$1 =
        // $600 > $500.
        let flip_bigger = QuoteIntent {
            token: t,
            signed_qty: -700 * SHARE,
            price_micro: PRICE_1USD,
        };
        assert_eq!(
            inv.check_quote(&flip_bigger),
            QuoteVerdict::Reject(InvReason::PerMarketInventory)
        );
    }

    #[test]
    fn check_quote_gross_excludes_quoted_tokens_own_basis() {
        // The quoted token ALREADY holds inventory. The gross sum must value it
        // by its PROJECTED notional (not its stale |basis|) and add only OTHER
        // tokens' |basis| — its own basis must not be double-counted.
        let mut inv = InventoryRisk::new(cfg()); // per-market $500, gross $2000
        let (b, a) = (TokenId(1), TokenId(2));
        inv.on_fill(b, 200 * SHARE, Usdc(-200_000_000)); // quoted B: net +200, basis $200
        inv.on_fill(a, 1600 * SHARE, Usdc(-1_600_000_000)); // other A: basis $1600
        // Buy 100 more of B → proj_net_B +300 sh, proj_notional $300 (≤ $500).
        let q = QuoteIntent {
            token: b,
            signed_qty: 100 * SHARE,
            price_micro: PRICE_1USD,
        };
        // Correct gross = 300 (B projected) + 1600 (A basis) = $1900 ≤ $2000 →
        // Approve. Double-counting B's own $200 basis would give $2100 > $2000 →
        // Reject; asserting Approve proves the quoted token's basis is excluded.
        assert_eq!(
            inv.check_quote(&q),
            QuoteVerdict::Approve,
            "quoted token's own basis must be excluded from the gross sum"
        );
    }

    // ── Task 2.3: mark-to-market + latched stop-loss ──────────────────────────

    #[test]
    fn mark_computes_mtm_and_latches_stop_loss() {
        // cfg() has inventory_stop_loss_usd = $100.
        let t = TokenId(1);
        let mut inv = InventoryRisk::new(cfg());
        // Long 1000 sh @ $0.50 → net +1000 sh, basis $500, realized 0.
        inv.on_fill(t, 1000 * SHARE, Usdc(-500_000_000));

        // Mark $0.39: value = 1000·0.39 = $390; unrealized = 390 − 500 = −$110.
        // mtm = 0 + (−110) = −$110 ≤ −$100 → StopLoss latches.
        let st = inv.mark(&Marks::from([(t, 390_000)]));
        assert_eq!(st.realized, Usdc(0));
        assert_eq!(st.unrealized, Usdc(-110_000_000));
        assert_eq!(st.mtm_pnl, Usdc(-110_000_000));
        assert_eq!(st.halted, Some(InvHalt::StopLoss));
        assert_eq!(inv.halted(), Some(InvHalt::StopLoss));

        // Recovery: mark $0.60 → unrealized = 600 − 500 = +$100, mtm = +$100.
        // The latch is STICKY — it must STAY StopLoss despite the recovery.
        let st2 = inv.mark(&Marks::from([(t, 600_000)]));
        assert_eq!(st2.mtm_pnl, Usdc(100_000_000), "mtm recovered to +$100");
        assert_eq!(
            st2.halted,
            Some(InvHalt::StopLoss),
            "stop-loss is sticky: recovery never clears it"
        );
        assert_eq!(inv.halted(), Some(InvHalt::StopLoss));
    }

    #[test]
    fn mark_above_stop_is_not_halted() {
        // A long AND a short, each marked away from basis; mtm well above −$100.
        let (a, b) = (TokenId(1), TokenId(2));
        let mut inv = InventoryRisk::new(cfg());

        // A: buy 100 @ $0.40, then sell 40 @ $0.50 → net +60 sh, basis $24,
        //    realized +$4 (proceeds $20 − released basis $16).
        inv.on_fill(a, 100 * SHARE, Usdc(-40_000_000));
        inv.on_fill(a, -40 * SHARE, Usdc(20_000_000));
        // B: sell-to-open 50 @ $0.60 → net −50 sh, basis −$30, realized 0.
        inv.on_fill(b, -50 * SHARE, Usdc(30_000_000));

        // Marks: A $0.45, B $0.55 (the short's mark ≠ its $0.60 basis price).
        //   value_A = 60·0.45 = $27;       unrealized_A = 27 − 24       = +$3.00.
        //   value_B = −50·0.55 = −$27.50;  unrealized_B = −27.5 − (−30) = +$2.50.
        let st = inv.mark(&Marks::from([(a, 450_000), (b, 550_000)]));

        assert_eq!(st.realized, Usdc(4_000_000), "Σ realized = $4 (all from A)");
        assert_eq!(
            st.unrealized,
            Usdc(5_500_000),
            "Σ unrealized = $3 (long A) + $2.50 (short B)"
        );
        assert_eq!(
            st.mtm_pnl,
            Usdc(9_500_000),
            "mtm = realized $4 + unrealized $5.50"
        );
        assert_eq!(st.halted, None, "mtm $9.50 ≫ −$100 stop → no halt");
        // net_by_token is sorted by id: A (TokenId 1) then B (TokenId 2).
        assert_eq!(st.net_by_token, vec![(a, 60 * SHARE), (b, -50 * SHARE)]);
    }

    #[test]
    fn mark_value_rounds_against_us_on_both_sides() {
        // Zero-cash opens leave basis 0, isolating the value term so the
        // assertion is purely about how net·mark/1e6 rounds. 3 µshares @ $0.46
        // has the exact (fractional) value 1.38 µUSDC.
        let (long, short) = (TokenId(1), TokenId(2));
        let mut inv = InventoryRisk::new(cfg());
        inv.on_fill(long, 3, Usdc(0)); // net +3 µshares, basis 0
        inv.on_fill(short, -3, Usdc(0)); // net −3 µshares, basis 0

        let st = inv.mark(&Marks::from([(long, 460_000), (short, 460_000)]));
        // Long:  floor(+1.38) = 1  (value rounds DOWN — against us).
        // Short: floor(−1.38) = −2 (buy-back liability magnitude rounds UP — against us).
        // unrealized = (1 − 0) + (−2 − 0) = −1.
        assert_eq!(st.unrealized, Usdc(-1));
        assert_eq!(st.realized, Usdc(0));
        assert_eq!(st.mtm_pnl, Usdc(-1));
    }

    #[test]
    fn mark_missing_token_is_valued_at_side_worst_case() {
        // Documented policy: a token absent from `marks` is valued at its side's
        // worst case — a long at mark 0 ($0.00), a short at mark 1_000_000
        // ($1.00). An empty marks map → BOTH are valued worst-case.
        let (long, short) = (TokenId(1), TokenId(2));
        let mut inv = InventoryRisk::new(cfg());
        inv.on_fill(long, 100 * SHARE, Usdc(-40_000_000)); // long: net +100, basis $40
        inv.on_fill(short, -50 * SHARE, Usdc(30_000_000)); // short: net −50, basis −$30

        let st = inv.mark(&Marks::new());
        // Long missing  → value 0      → unrealized = 0 − 40       = −$40.
        // Short missing → value −50·$1 = −$50 → unrealized = −50 − (−30) = −$20.
        // (Skipping missing tokens would give 0; marking the short at 0 would
        //  give +$30 — asserting −$60 pins the documented worst-case policy.)
        assert_eq!(st.unrealized, Usdc(-60_000_000));
        assert_eq!(st.realized, Usdc(0));
        assert_eq!(st.mtm_pnl, Usdc(-60_000_000));
        assert_eq!(st.halted, None, "−$60 is above the −$100 stop");
    }
}
