//! Per-strategy inventory risk (spec §5, Phase 2). A NEW risk module for
//! risk-taking, **inventory-bearing** strategies (market-making first). It is
//! SEPARATE from the risk-free-basket `RiskEngine` in `lib.rs` — arb keeps using
//! that one untouched. This module is inert until a strategy opts in (Phase 4).
//!
//! Implemented so far: inventory STATE + `on_fill` accounting (Task 2.1) and
//! the pre-fill cap check `check_quote` (Task 2.2). Mark-to-market (`mark`),
//! the stop-loss latch, and the flatten directive (Tasks 2.3–2.4) are
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
}

impl InventoryRisk {
    pub fn new(cfg: InventoryConfig) -> Self {
        InventoryRisk {
            cfg,
            by_token: HashMap::new(),
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
}
