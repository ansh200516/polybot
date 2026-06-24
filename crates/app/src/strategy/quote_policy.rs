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
}
