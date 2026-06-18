//! Top-trader **confluence**: turn the Data-API leaderboard + the top performers'
//! OPEN positions into a market-selection (and directional) signal.
//!
//! Flow: pull the leaderboard (by PnL/volume, over a window), walk it in rank
//! order, and for each trader WITH open positions ([`Position::is_open`])
//! accumulate their holdings — until `top_traders` such traders are collected.
//! Per market (`conditionId`) the FAVORED side is the outcome token the most of
//! those traders hold (size as the tiebreak), so the MM can lean directionally
//! toward the smart money. The pure [`aggregate`] core is unit-tested without
//! any I/O; [`top_trader_markets`] is the thin async wrapper that fetches.

use std::collections::HashMap;

use crate::data_api::{DataApiClient, OrderBy, Position, TimePeriod};
use crate::IngestError;

/// One market the top traders are collectively in, plus the side they favor.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfluenceMarket {
    /// The market's condition id (drives universe selection).
    pub condition_id: String,
    /// The CLOB token id of the FAVORED outcome (the side most top traders hold)
    /// — what the MM quotes for a directional lean.
    pub favored_token: String,
    /// Human label of the favored outcome ("Yes"/"No"), for logging.
    pub favored_outcome: String,
    /// How many of the selected top traders hold the favored side.
    pub trader_count: usize,
    /// Total favored-side size (shares) across those traders.
    pub total_size: f64,
}

/// Inputs for [`top_trader_markets`], sourced from `[confluence]` config.
#[derive(Debug, Clone)]
pub struct ConfluenceParams {
    pub order_by: OrderBy,
    pub period: TimePeriod,
    /// Number of traders WITH open positions to include.
    pub top_traders: usize,
    /// How deep to scan the leaderboard to find that many open-position traders.
    pub scan_limit: usize,
    /// Drop positions below this many shares (the API `sizeThreshold`).
    pub size_threshold: f64,
}

/// Pure aggregation core: given each selected trader's OPEN positions, pick the
/// favored outcome token per market (most holders, size as tiebreak). Sorted by
/// trader_count desc, then total_size desc, then condition_id (stable).
pub fn aggregate(open_positions_per_trader: &[Vec<Position>]) -> Vec<ConfluenceMarket> {
    // (condition_id, asset) -> (#traders, total_size, outcome_label)
    let mut by_side: HashMap<(String, String), (usize, f64, String)> = HashMap::new();
    for trader in open_positions_per_trader {
        for pos in trader {
            let e = by_side
                .entry((pos.condition_id.clone(), pos.asset.clone()))
                .or_insert((0, 0.0, pos.outcome.clone()));
            e.0 += 1;
            e.1 += pos.size;
        }
    }
    // Per market, keep the side with the most holders (then most size).
    let mut by_market: HashMap<String, ConfluenceMarket> = HashMap::new();
    for ((cid, asset), (count, size, outcome)) in by_side {
        let take = match by_market.get(&cid) {
            None => true,
            Some(cur) => {
                count > cur.trader_count
                    || (count == cur.trader_count && size > cur.total_size)
            }
        };
        if take {
            by_market.insert(
                cid.clone(),
                ConfluenceMarket {
                    condition_id: cid,
                    favored_token: asset,
                    favored_outcome: outcome,
                    trader_count: count,
                    total_size: size,
                },
            );
        }
    }
    let mut out: Vec<ConfluenceMarket> = by_market.into_values().collect();
    out.sort_by(|a, b| {
        b.trader_count
            .cmp(&a.trader_count)
            .then(
                b.total_size
                    .partial_cmp(&a.total_size)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
            .then(a.condition_id.cmp(&b.condition_id))
    });
    out
}

/// Fetch the leaderboard, collect the first `top_traders` entries that have OPEN
/// positions, and aggregate them into per-market favored sides. A trader whose
/// positions fail to load is skipped (best-effort); a trader with zero open
/// positions doesn't count toward `top_traders`.
pub async fn top_trader_markets(
    client: &DataApiClient,
    params: &ConfluenceParams,
) -> Result<Vec<ConfluenceMarket>, IngestError> {
    let board = client
        .leaderboard(params.order_by, params.period, params.scan_limit)
        .await?;
    let mut per_trader: Vec<Vec<Position>> = Vec::new();
    for entry in &board {
        if per_trader.len() >= params.top_traders {
            break;
        }
        let positions = match client.positions(&entry.proxy_wallet, params.size_threshold).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(trader = %entry.user_name, "confluence: positions fetch failed: {e}");
                continue;
            }
        };
        let open: Vec<Position> = positions.into_iter().filter(Position::is_open).collect();
        if open.is_empty() {
            continue; // "top traders WHO HAVE open trades"
        }
        tracing::debug!(
            trader = %entry.user_name,
            pnl = entry.pnl,
            open_positions = open.len(),
            "confluence: including top trader"
        );
        per_trader.push(open);
    }
    Ok(aggregate(&per_trader))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn pos(cid: &str, asset: &str, outcome: &str, size: f64) -> Position {
        // Construct an OPEN position via JSON (Position has private-ish defaults).
        let body = format!(
            r#"[{{"conditionId":"{cid}","asset":"{asset}","size":{size},
                 "outcome":"{outcome}","outcomeIndex":0,"curPrice":0.5,"redeemable":false}}]"#
        );
        crate::data_api::parse_positions(&body).unwrap().pop().unwrap()
    }

    #[test]
    fn favored_side_is_most_held_then_largest() {
        // M1: traders A,B hold YES (asset "Y"); trader C holds NO (asset "N").
        //     → favored YES (2 holders > 1).
        // M2: trader A holds NO (asset "n2"); only one holder.
        let a = vec![pos("M1", "Y", "Yes", 10.0), pos("M2", "n2", "No", 5.0)];
        let b = vec![pos("M1", "Y", "Yes", 3.0)];
        let c = vec![pos("M1", "N", "No", 100.0)]; // bigger size, but fewer holders
        let out = aggregate(&[a, b, c]);

        assert_eq!(out.len(), 2, "two distinct markets");
        // M1 first (2 holders), favored YES token, NOT the bigger-size NO side.
        assert_eq!(out[0].condition_id, "M1");
        assert_eq!(out[0].favored_token, "Y");
        assert_eq!(out[0].favored_outcome, "Yes");
        assert_eq!(out[0].trader_count, 2);
        assert!((out[0].total_size - 13.0).abs() < 1e-9);
        // M2 second (1 holder).
        assert_eq!(out[1].condition_id, "M2");
        assert_eq!(out[1].favored_token, "n2");
        assert_eq!(out[1].trader_count, 1);
    }

    #[test]
    fn size_breaks_holder_ties() {
        // Both sides have 1 holder → the larger total size wins.
        let a = vec![pos("M", "Y", "Yes", 5.0)];
        let b = vec![pos("M", "N", "No", 50.0)];
        let out = aggregate(&[a, b]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].favored_token, "N", "tie on holders → larger size wins");
    }

    #[test]
    fn empty_input_yields_no_markets() {
        assert!(aggregate(&[]).is_empty());
        assert!(aggregate(&[vec![], vec![]]).is_empty());
    }
}
