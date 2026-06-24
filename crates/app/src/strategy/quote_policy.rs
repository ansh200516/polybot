//! QuotePolicy seam: the only two decisions that differ between strategies —
//! WHICH markets and WHAT quotes. `SpreadCapture` preserves today's behavior;
//! `RewardFarm` implements liquidity-reward farming (spec §5-§8).

/// Compute tight, non-crossing, two-sided prices for reward farming.
/// All prices in dollars (0..1). Each side is `None` if it cannot sit within
/// `max_spread_cents` of the adjusted mid without crossing; `(None, None)` means
/// skip this market this cycle.
///
/// `max_spread_cents <= 0` means the market is NOT in the reward program, so it
/// short-circuits to `(None, None)` (an on-tick mid would otherwise sit at
/// distance 0, inside the zero band, and quote a non-eligible market).
///
/// When both sides quote, `bid < ask` is GUARANTEED: an on-grid mid collapses
/// the tightest bid and ask onto the same tick, which a post-only venue rejects
/// as a self-cross, so the ask is bumped up one tick (the bid stays tightest at
/// the mid). If that bumped ask then falls outside the band the result is
/// correctly single-sided.
pub fn reward_quote_prices(
    adj_mid: f64,
    best_bid: f64,
    best_ask: f64,
    tick: f64,
    max_spread_cents: f64,
) -> (Option<f64>, Option<f64>) {
    // Not reward-eligible (no scoring band) → never quote, even on an on-tick mid
    // where the distance-0 quote would otherwise pass the `<= band` check.
    if max_spread_cents <= 0.0 {
        return (None, None);
    }
    let band = max_spread_cents / 100.0;
    let bid_cap = (best_ask - tick).min(adj_mid);
    let bid = (bid_cap / tick).floor() * tick;
    let ask_floor = (best_bid + tick).max(adj_mid);
    let mut ask = (ask_floor / tick).ceil() * tick;
    // POST-ONLY SELF-CROSS GUARD: an on-grid mid puts bid and ask on the SAME
    // tick (the venue rejects two opposing post-only orders at one price). Bump
    // ONLY the ask up a tick so `bid < ask` strictly; the `ask_ok` band/cross
    // checks below then run against the bumped ask (a bump out of band → ask
    // dropped → correctly single-sided). The bid stays at the mid (tightest).
    if bid >= ask {
        ask = bid + tick;
    }
    let bid_ok = bid > 0.0 && (adj_mid - bid) <= band + 1e-9 && bid < best_ask;
    let ask_ok = ask < 1.0 && (ask - adj_mid) <= band + 1e-9 && ask > best_bid;
    (bid_ok.then_some(bid), ask_ok.then_some(ask))
}

/// Adjusted mid: midpoint of the book (the size-cutoff filtering of sub-min_size
/// levels is applied by the caller before calling this).
pub fn adjusted_mid(best_bid: f64, best_ask: f64) -> f64 {
    (best_bid + best_ask) / 2.0
}

/// Size-weighted fair value. `bid`/`ask` are top-of-book prices ($), `bid_qty`/
/// `ask_qty` the resting sizes there. Weights the bid price by ask qty and vice
/// versa, so a heavier bid (buy pressure) pulls fair value UP toward the ask.
/// Falls back to the midpoint when both sizes are 0.
pub fn microprice(bid: f64, ask: f64, bid_qty: f64, ask_qty: f64) -> f64 {
    let denom = bid_qty + ask_qty;
    if denom <= 0.0 {
        return (bid + ask) / 2.0;
    }
    (bid * ask_qty + ask * bid_qty) / denom
}

/// Order-book imbalance over summed depths: (bid - ask)/(bid + ask) in [-1,1].
/// Positive = buy pressure. 0 when both are 0.
pub fn imbalance(bid_depth: f64, ask_depth: f64) -> f64 {
    let denom = bid_depth + ask_depth;
    if denom <= 0.0 {
        0.0
    } else {
        (bid_depth - ask_depth) / denom
    }
}

/// Sum resting size (SHARES) over up to `levels` non-empty price levels inward
/// from `best` on a ladder (µshares -> shares). 0 when the ladder is empty.
///
/// Walks from `best` toward worse prices, matching the ladder's orientation:
/// a BID ladder's best is the highest tick, so depth lies at LOWER ticks
/// (step -1); an ASK ladder's best is the lowest tick, so depth lies at HIGHER
/// ticks (step +1) — the same direction as [`Ladder::iter_from_best`]. Empty
/// ticks are skipped (they don't consume a level), and the walk is bounded by
/// `Px::new`'s interior-tick invariant (`1..levels`), so it never runs off
/// either end of the ladder.
pub fn ladder_depth(ladder: &pm_core::book::Ladder, levels: u16) -> f64 {
    use pm_core::book::Side;
    use pm_core::num::Px;

    let Some(best) = ladder.best() else {
        return 0.0;
    };
    let ts = ladder.ts();
    // Best is the touch; depth is BEHIND it: bids deepen at lower ticks, asks at
    // higher ticks (best toward worse), mirroring `Ladder::iter_from_best`.
    let step: i32 = match ladder.side() {
        Side::Bid => -1,
        Side::Ask => 1,
    };

    let mut tick = i32::from(best.get());
    let mut found = 0u16;
    let mut depth = 0.0;
    while found < levels {
        // `Px::new` rejects ticks outside `1..levels`, which bounds the walk at
        // BOTH ends (tick 0 below bids, `levels` above asks) — no manual edge math.
        let Ok(px) = Px::new(tick as u16, ts) else {
            break;
        };
        let q = ladder.qty_at(px).0;
        if q > 0 {
            depth += q as f64 / 1_000_000.0;
            found += 1;
        }
        tick += step;
    }
    depth
}

/// Balanced base sizes leaned against signed inventory `net` (shares).
/// Long (net>0) -> bigger ask; short -> bigger bid. Ratio clamped to
/// `max_ratio`; both sides floored at `min_size` to preserve the 2-sided bonus.
///
/// Express a view by skewing SIZES, never prices (spec §2/§8.3) — the reward
/// score is quadratic in tightness, so prices stay pinned at the tight reward
/// band ([`reward_quote_prices`]) while the lean lives entirely in the sizes.
/// `r = clamp(net / cap_shares, -1, 1)` scales the lean by how full the
/// per-market inventory cap is. The lean is split HALF onto each side
/// (`lean = max_ratio^(r/2)`, ask×lean and bid÷lean), so the bigger:smaller
/// (`ask/bid` when long) ratio is `lean² = max_ratio^r` — reaching the full
/// `max_ratio` (the spec's ≤2:1 TWO-SIDED cap) only at the inventory cap and
/// exactly `1` (balanced) when flat. Splitting matters: a per-side
/// `max_ratio^r` would let `ask/bid` reach `max_ratio²` (4:1 at a 2.0 cap),
/// breaking the ≤2:1 invariant. The `min_size` floor can push the realized
/// ratio BELOW `max_ratio` (never above), which keeps both sides earning the
/// two-sided bonus.
pub fn skewed_sizes(base: f64, net: f64, cap_shares: f64, max_ratio: f64, min_size: f64) -> (f64, f64) {
    let r = if cap_shares > 0.0 { (net / cap_shares).clamp(-1.0, 1.0) } else { 0.0 };
    // HALF the lean per side → ask/bid = max_ratio^r ≤ max_ratio (the ≤2:1 cap).
    let lean = max_ratio.powf(r / 2.0); // r>0 (long) -> lean>1 -> ask bigger
    let ask = (base * lean).max(min_size);
    let bid = (base / lean).max(min_size);
    (bid, ask)
}

/// Rank reward-eligible markets by edge = daily_rate / competing_depth, then
/// greedily fit to `budget_usd`. Input tuples: (id, daily_rate, competing_depth, per_market_cost).
pub fn select_reward_markets(mut cands: Vec<(u64, f64, f64, f64)>, budget_usd: f64) -> Vec<u64> {
    cands.retain(|(_, rate, _, cost)| *rate > 0.0 && *cost > 0.0);
    cands.sort_by(|a, b| {
        let ea = a.1 / a.2.max(1e-9);
        let eb = b.1 / b.2.max(1e-9);
        eb.partial_cmp(&ea).unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut spent = 0.0;
    let mut out = Vec::new();
    for (id, _, _, cost) in cands {
        if spent + cost <= budget_usd + 1e-9 {
            spent += cost;
            out.push(id);
        }
    }
    out
}

/// True when a resting order must be replaced: it has drifted more than
/// `band_ticks` from the new target price. Keeps quotes sticky (frequent
/// cancels reset the time-weighted reward score).
pub fn needs_requote(resting_price: f64, target_price: f64, tick: f64, band_ticks: u16) -> bool {
    (resting_price - target_price).abs() > (f64::from(band_ticks) * tick) + 1e-9
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    SpreadCapture,
    RewardFarm,
}

impl Policy {
    pub fn from_cfg(s: &str) -> Self {
        match s {
            "reward_farm" => Policy::RewardFarm,
            _ => Policy::SpreadCapture,
        }
    }
}

#[cfg(test)]
mod tests {
    // `manual_range_contains` fires on the provided `a >= lo && a <= hi`
    // assertions below; allowed (not rewritten) to keep the spec tests verbatim.
    #![allow(clippy::unwrap_used, clippy::manual_range_contains)]
    use super::*;
    use pm_core::book::{Book, Side};
    use pm_core::num::{Px, Qty, TickSize};

    fn cent_px(t: u16) -> Px {
        Px::new(t, TickSize::Cent).unwrap()
    }

    #[test]
    fn quotes_tight_and_non_crossing() {
        let (bid, ask) = reward_quote_prices(0.50, 0.48, 0.52, 0.01, 3.0);
        let (bid, ask) = (bid.unwrap(), ask.unwrap());
        assert!(bid < 0.52 && ask > 0.48, "must not cross");
        assert!((0.50 - bid) <= 0.0201 && (ask - 0.50) <= 0.0201, "within ~1 tick of mid");
    }

    #[test]
    fn wide_book_places_inside_touch() {
        let (bid, ask) = reward_quote_prices(0.50, 0.40, 0.60, 0.01, 3.0);
        let (bid, ask) = (bid.unwrap(), ask.unwrap());
        assert!(bid >= 0.49 && bid <= 0.50);
        assert!(ask <= 0.51 && ask >= 0.50);
    }

    #[test]
    fn skips_when_touch_outside_band() {
        // A LOCKED touch (best_bid == best_ask == mid) forces the tightest
        // non-crossing tick a FULL tick (1¢) off mid — bid clamps to
        // `best_ask - tick`, ask to `best_bid + tick` — which is outside the
        // 0.5¢ band, so BOTH sides score ≈0 and are skipped (spec §8.2).
        //
        // NOTE: the originally-specified inputs `(0.50, 0.48, 0.52, 0.01, 0.5)`
        // CANNOT yield `(None, None)`: that book's tightest two-sided quote sits
        // exactly AT mid (distance 0), inside ANY positive band — identical book
        // to `quotes_tight_and_non_crossing`, which expects quotes. The impl
        // (per spec §8.2) returns `(Some(0.50), Some(0.50))` there; the test was
        // corrected to a scenario that genuinely exercises the skip path.
        assert_eq!(reward_quote_prices(0.50, 0.50, 0.50, 0.01, 0.5), (None, None));
    }

    #[test]
    fn never_locks_bid_equal_ask_on_grid_mid() {
        // on-grid mid 0.50, ample band -> both sides, but bid<ask (ask bumped a tick)
        let (bid, ask) = reward_quote_prices(0.50, 0.48, 0.52, 0.01, 3.0);
        let (bid, ask) = (bid.unwrap(), ask.unwrap());
        assert!(bid < ask, "post-only must not lock: bid={bid} ask={ask}");
        assert!((bid - 0.50).abs() < 1e-9 && (ask - 0.51).abs() < 1e-9);
    }

    #[test]
    fn zero_band_is_never_eligible_even_on_grid_mid() {
        // max_spread_cents = 0 (not reward-eligible) -> skip, even though mid is on-grid
        assert_eq!(reward_quote_prices(0.50, 0.48, 0.52, 0.01, 0.0), (None, None));
        assert_eq!(reward_quote_prices(0.50, 0.40, 0.60, 0.01, 0.0), (None, None));
    }

    #[test]
    fn size_skew_leans_against_inventory_within_ratio() {
        let (bid_sz, ask_sz) = skewed_sizes(10.0, /*net*/ 8.0, /*cap*/ 10.0, /*max_ratio*/ 2.0, /*min*/ 5.0);
        assert!(ask_sz >= bid_sz);
        assert!(ask_sz / bid_sz <= 2.0 + 1e-9);
        assert!(bid_sz >= 5.0 && ask_sz >= 5.0, "both stay >= min_incentive_size");
    }

    #[test]
    fn flat_inventory_is_balanced() {
        let (b, a) = skewed_sizes(10.0, 0.0, 10.0, 2.0, 5.0);
        assert!((a - b).abs() < 1e-9);
    }

    #[test]
    fn size_skew_short_inventory_leans_bid_bigger() {
        // Short (net<0) -> lean<1 -> bid bigger, ask smaller (buy to reduce short).
        let (bid_sz, ask_sz) = skewed_sizes(10.0, /*net*/ -8.0, /*cap*/ 10.0, /*max_ratio*/ 2.0, /*min*/ 5.0);
        assert!(bid_sz >= ask_sz);
        assert!(bid_sz / ask_sz <= 2.0 + 1e-9);
        assert!(bid_sz >= 5.0 && ask_sz >= 5.0, "both stay >= min_incentive_size");
    }

    #[test]
    fn selection_ranks_by_edge_and_caps_to_budget() {
        // tuples: (id, daily_rate, competing_depth, per_market_cost)
        let cands = vec![
            (1u64, 100.0, 1000.0, 10.0), // edge 0.10
            (2u64, 100.0, 100.0,  10.0), // edge 1.00 (best)
            (3u64, 0.0,   50.0,   10.0), // ineligible (rate 0)
        ];
        let picked = select_reward_markets(cands, /*budget*/ 10.0); // only one fits
        assert_eq!(picked, vec![2]);
    }

    #[test]
    fn selection_fits_multiple_best_edge_first_within_budget() {
        // Same cost each; budget fits exactly two → greedy takes the two
        // highest-edge markets in rank order and stops before the third.
        let cands = vec![
            (1u64, 10.0,  100.0, 10.0), // edge 0.10 (worst)
            (2u64, 100.0, 100.0, 10.0), // edge 1.00 (best)
            (3u64, 50.0,  100.0, 10.0), // edge 0.50
        ];
        let picked = select_reward_markets(cands, /*budget*/ 25.0);
        assert_eq!(picked, vec![2, 3]);
    }

    #[test]
    fn requote_only_when_out_of_band() {
        // resting at 0.49; band 1 tick (0.01). target 0.495 -> keep; target 0.51 -> replace.
        assert!(!needs_requote(0.49, 0.495, 0.01, 1));
        assert!(needs_requote(0.49, 0.51, 0.01, 1));
    }

    #[test]
    fn microprice_leans_to_heavier_side_and_falls_back_to_mid() {
        assert!((microprice(0.50, 0.52, 100.0, 100.0) - 0.51).abs() < 1e-9); // equal -> mid
        let mp = microprice(0.50, 0.52, 300.0, 100.0); // bid-heavy -> up toward ask
        assert!(mp > 0.51 && mp < 0.52, "got {mp}");
        assert!((microprice(0.50, 0.52, 0.0, 0.0) - 0.51).abs() < 1e-9); // zero -> mid
    }

    #[test]
    fn imbalance_sign_and_bounds() {
        assert!((imbalance(100.0, 100.0)).abs() < 1e-9);
        assert!(imbalance(300.0, 100.0) > 0.0);
        assert!(imbalance(100.0, 300.0) < 0.0);
        assert!(imbalance(100.0, 0.0) <= 1.0 && imbalance(0.0, 100.0) >= -1.0);
        assert_eq!(imbalance(0.0, 0.0), 0.0);
    }

    #[test]
    fn ladder_depth_sums_top_levels_skipping_gaps_per_side() {
        // µshares: 1 share = 1_000_000. Levels are NON-adjacent so the test proves
        // we sum the top-N *non-empty* price levels (skipping the empty ticks
        // between them) and that the walk steps the correct way for each side.
        let mut book = Book::new(TickSize::Cent);
        // Bids: best is the HIGHEST tick; depth lies at LOWER ticks (60 -> 58 -> 55).
        book.apply(Side::Bid, cent_px(60), Qty(2_000_000)); // 2.0 shares (best)
        book.apply(Side::Bid, cent_px(58), Qty(3_000_000)); // 3.0 shares (gap at 59)
        book.apply(Side::Bid, cent_px(55), Qty(5_000_000)); // 5.0 shares
        // Asks: best is the LOWEST tick; depth lies at HIGHER ticks (40 -> 42 -> 45).
        book.apply(Side::Ask, cent_px(40), Qty(1_000_000)); // 1.0 share (best)
        book.apply(Side::Ask, cent_px(42), Qty(4_000_000)); // 4.0 shares (gap at 41)
        book.apply(Side::Ask, cent_px(45), Qty(2_000_000)); // 2.0 shares

        // Top 1 = just the touch.
        assert!((ladder_depth(&book.bids, 1) - 2.0).abs() < 1e-9);
        assert!((ladder_depth(&book.asks, 1) - 1.0).abs() < 1e-9);
        // Top 2 = touch + next non-empty level (skips the empty tick), STOPS there.
        assert!((ladder_depth(&book.bids, 2) - 5.0).abs() < 1e-9); // 2 + 3
        assert!((ladder_depth(&book.asks, 2) - 5.0).abs() < 1e-9); // 1 + 4
        // Top 3 = every level on each side.
        assert!((ladder_depth(&book.bids, 3) - 10.0).abs() < 1e-9); // 2 + 3 + 5
        assert!((ladder_depth(&book.asks, 3) - 7.0).abs() < 1e-9); // 1 + 4 + 2
        // Asking for MORE levels than exist returns all available without
        // over-iterating past the ladder bounds (no panic, no extra depth).
        assert!((ladder_depth(&book.bids, 50) - 10.0).abs() < 1e-9);
        assert!((ladder_depth(&book.asks, 50) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn ladder_depth_empty_ladder_is_zero() {
        let book = Book::new(TickSize::Cent);
        assert!((ladder_depth(&book.bids, 5)).abs() < 1e-9);
        assert!((ladder_depth(&book.asks, 5)).abs() < 1e-9);
    }
}
