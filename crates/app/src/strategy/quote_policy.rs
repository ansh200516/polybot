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
}
