//! Per-strategy inventory risk (spec §5, Phase 2). A NEW risk module for
//! risk-taking, **inventory-bearing** strategies (market-making first). It is
//! SEPARATE from the risk-free-basket `RiskEngine` in `lib.rs` — arb keeps using
//! that one untouched. This module is inert until a strategy opts in (Phase 4).
//!
//! Task 2.1 scope: inventory STATE + `on_fill` accounting ONLY. The cap checks
//! (`check_quote`), mark-to-market (`mark`), stop-loss latch, and flatten
//! directive are Tasks 2.2–2.4 and are intentionally absent here.

use std::collections::HashMap;

use pm_core::instrument::TokenId;
use pm_core::num::Usdc;

/// Per-strategy inventory caps (spec §5, all µUSDC). Defined now so the type is
/// stable across Phase 2, but this task only STORES it — the cap CHECKS land in
/// Tasks 2.2–2.4. Per-strategy and conservative by config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryConfig {
    /// Per-market net exposure cap (`|net·mark|` per market). Enforced in Task 2.2.
    pub max_inventory_usd: Usdc,
    /// Gross inventory cap, summed across markets. Enforced in Task 2.2.
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
}
