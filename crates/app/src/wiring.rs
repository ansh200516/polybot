//! Config → engine/risk parameter conversion, component indexing, shard
//! packing, and the BookFetcher adapter (spec §12 app wiring).

use std::collections::HashMap;
use std::sync::Arc;

use pm_config::{Config, ConfigError, usd_to_microusdc};
use pm_core::book::Book;
use pm_core::instrument::{Market, MarketId, Partition, Relationship, TokenId};
use pm_core::num::{Bps, Usdc};
use pm_engine::{EngineParams, GasTable, RedeemStrategy};
use pm_ingestion::supervisor::SupervisorCommand;
use pm_registry::Registry;
use pm_registry::components::ComponentId;
use pm_risk::RiskConfig;
use tokio::sync::{mpsc, oneshot};

pub fn engine_params(cfg: &Config) -> Result<EngineParams, ConfigError> {
    Ok(EngineParams {
        floor_c12: Bps(cfg.edges.min_edge_class12_bps),
        floor_c3: Bps(cfg.edges.min_edge_class3_bps),
        min_profit: Usdc(usd_to_microusdc(cfg.edges.min_profit_usd)?),
        gas: GasTable {
            split: cfg.gas.split_microusdc,
            merge: cfg.gas.merge_microusdc,
            redeem: cfg.gas.redeem_microusdc,
            negrisk_convert: cfg.gas.negrisk_convert_microusdc,
        },
        redeem: match cfg.execution.redeem_strategy.as_str() {
            "hold" => RedeemStrategy::Hold,
            _ => RedeemStrategy::Merge,
        },
        max_basis: Usdc(usd_to_microusdc(cfg.capital.per_market_usd)?),
        max_worlds: cfg.lp.max_worlds,
        cooldown_ms: cfg.dedup.cooldown_ms,
        reemit_improvement_pct: cfg.dedup.reemit_improvement_pct,
    })
}

pub fn risk_config(cfg: &Config) -> Result<RiskConfig, ConfigError> {
    Ok(RiskConfig {
        bankroll: Usdc(usd_to_microusdc(cfg.capital.bankroll_usd)?),
        per_market_cap: Usdc(usd_to_microusdc(cfg.capital.per_market_usd)?),
        max_unhedged: Usdc(usd_to_microusdc(cfg.risk.max_unhedged_usd)?),
        max_open_orders: cfg.risk.max_open_orders,
        max_basket_legs: cfg.risk.max_basket_legs,
        daily_drawdown_bps: (cfg.risk.daily_drawdown_pct * 100.0).round() as i128,
        error_halt_count: cfg.risk.error_halt_count,
        error_halt_window: std::time::Duration::from_secs(cfg.risk.error_halt_window_s),
        restart_storm_count: cfg.risk.restart_storm_count,
    })
}

/// Everything a detector needs about one connected component.
pub struct ComponentEntry {
    pub markets: Vec<Market>,
    pub partitions: Vec<Partition>,
    pub relationships: Vec<Relationship>,
    pub tokens: Vec<TokenId>,
}

pub struct ComponentIndex {
    pub by_token: HashMap<TokenId, ComponentId>,
    pub entries: HashMap<ComponentId, ComponentEntry>,
}

pub fn build_component_index(reg: &Registry) -> ComponentIndex {
    let mut entries: HashMap<ComponentId, ComponentEntry> = HashMap::new();
    let mut by_token = HashMap::new();
    for m in reg.markets() {
        let cid = reg.component_of(m.id);
        let e = entries.entry(cid).or_insert_with(|| ComponentEntry {
            markets: Vec::new(),
            partitions: Vec::new(),
            relationships: Vec::new(),
            tokens: Vec::new(),
        });
        e.markets.push(*m);
        e.tokens.push(m.yes);
        e.tokens.push(m.no);
        by_token.insert(m.yes, cid);
        by_token.insert(m.no, cid);
    }
    for p in reg.partitions() {
        if let Some(&first) = p.markets.first() {
            let cid = reg.component_of(first);
            // Registry construction guarantees it.
            debug_assert!(entries.contains_key(&cid), "partition member missing from market entries");
            if let Some(e) = entries.get_mut(&cid) {
                e.partitions.push(p.clone());
            }
        }
    }
    for r in reg.approved_relationships() {
        let a = match *r {
            Relationship::Implies { a, .. }
            | Relationship::MutuallyExclusive { a, .. }
            | Relationship::Equivalent { a, .. } => a,
        };
        let cid = reg.component_of(a);
        if let Some(e) = entries.get_mut(&cid) {
            e.relationships.push(*r);
        }
    }
    ComponentIndex { by_token, entries }
}

/// First-fit-decreasing: pack whole components into chunks of ≤ `max_tokens`
/// tokens. An oversized component gets its own oversized chunk (caller warns).
pub fn pack_components(reg: &Registry, max_tokens: usize) -> Vec<Vec<TokenId>> {
    let idx = build_component_index(reg);
    let mut comps: Vec<&ComponentEntry> = idx.entries.values().collect();
    comps.sort_by_key(|e| std::cmp::Reverse(e.tokens.len()));
    let mut chunks: Vec<Vec<TokenId>> = Vec::new();
    for e in comps {
        if let Some(chunk) = chunks
            .iter_mut()
            .find(|c| c.len() + e.tokens.len() <= max_tokens)
        {
            chunk.extend(e.tokens.iter().copied());
        } else {
            chunks.push(e.tokens.clone());
        }
    }
    chunks
}

/// (token → market, market → (yes, no)) maps for execution/risk conversion.
pub fn token_maps(
    reg: &Registry,
) -> (
    HashMap<TokenId, MarketId>,
    HashMap<MarketId, (TokenId, TokenId)>,
) {
    let mut tm = HashMap::new();
    let mut mt = HashMap::new();
    for m in reg.markets() {
        tm.insert(m.yes, m.id);
        tm.insert(m.no, m.id);
        mt.insert(m.id, (m.yes, m.no));
    }
    (tm, mt)
}

/// Per-token venue fee rates from the registry sync (spec §6: fee rates are
/// fetched live and must never default to zero silently).
pub fn fee_map(reg: &Registry) -> HashMap<TokenId, Bps> {
    reg.markets()
        .iter()
        .flat_map(|m| [(m.yes, m.fee_bps), (m.no, m.fee_bps)])
        .collect()
}

/// Routes book-snapshot queries to the supervisor owning each token.
#[derive(Clone)]
pub struct BookFetcher {
    routes: Arc<HashMap<TokenId, mpsc::Sender<SupervisorCommand>>>,
}

impl BookFetcher {
    pub fn new(routes: HashMap<TokenId, mpsc::Sender<SupervisorCommand>>) -> Self {
        BookFetcher {
            routes: Arc::new(routes),
        }
    }

    /// Raw fetch: (book, valid) or None (unknown token / dead supervisor).
    // M4: distinguish dead-supervisor from unknown-token if metrics require it.
    pub async fn fetch(&self, token: TokenId) -> Option<(Book, bool)> {
        let tx = self.routes.get(&token)?;
        let (otx, orx) = oneshot::channel();
        tx.send(SupervisorCommand::BookSnapshot { token, reply: otx })
            .await
            .ok()?;
        orx.await.ok()?
    }
}

/// PaperVenue's view: only VALID books exist (spec §5 amendment — invalid
/// books block fills exactly like missing ones).
impl pm_execution::venue::BookSource for BookFetcher {
    async fn book(&mut self, token: TokenId) -> Option<Book> {
        match self.fetch(token).await {
            Some((book, true)) => Some(book),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::TickSize;
    use pm_registry::RegistryBuilder;

    /// 4 markets: a+d linked by relationship; b+c in NegRisk event ev1; all Cent.
    fn reg() -> pm_registry::Registry {
        let mut b = RegistryBuilder::default();
        b.add_market(
            "0xa",
            "ya",
            "na",
            TickSize::Cent,
            0,
            false,
            None,
            true,
            false,
            None,
        );
        b.add_market(
            "0xb",
            "yb",
            "nb",
            TickSize::Cent,
            0,
            true,
            Some("B?".into()),
            true,
            false,
            Some("ev1"),
        );
        b.add_market(
            "0xc",
            "yc",
            "nc",
            TickSize::Cent,
            0,
            true,
            Some("C?".into()),
            true,
            false,
            Some("ev1"),
        );
        b.add_market(
            "0xd",
            "yd",
            "nd",
            TickSize::Cent,
            0,
            false,
            None,
            true,
            false,
            None,
        );
        let toml = "[[relationship]]\nkind = \"implies\"\na = \"0xa\"\nb = \"0xd\"\nstatus = \"approved\"\n";
        b.finish(toml).unwrap()
    }

    #[test]
    fn engine_params_reflect_locked_config() {
        let cfg = pm_config::Config::default();
        let p = engine_params(&cfg).unwrap();
        assert_eq!(p.floor_c12, Bps(30));
        assert_eq!(p.floor_c3, Bps(100));
        assert_eq!(p.min_profit, Usdc(1_000_000));
        assert_eq!(p.max_basis, Usdc(1_000_000_000));
        assert_eq!(p.redeem, RedeemStrategy::Merge);
        assert_eq!(p.max_worlds, 4096);
        assert_eq!(p.cooldown_ms, 2000);
    }

    #[test]
    fn risk_config_reflects_locked_config() {
        let cfg = pm_config::Config::default();
        let r = risk_config(&cfg).unwrap();
        assert_eq!(r.bankroll, Usdc(10_000_000_000));
        assert_eq!(r.per_market_cap, Usdc(1_000_000_000));
        assert_eq!(r.max_unhedged, Usdc(200_000_000));
        assert_eq!(r.daily_drawdown_bps, 200);
        assert_eq!(r.restart_storm_count, 5);
    }

    #[test]
    fn component_index_groups_partitions_and_relationships() {
        let r = reg();
        let idx = build_component_index(&r);
        assert_eq!(idx.entries.len(), 2); // {a,d} via relationship, {b,c} via partition
        let a = r.market_by_condition("0xa").unwrap().id;
        let d = r.market_by_condition("0xd").unwrap().id;
        let ca = r.component_of(a);
        assert_eq!(r.component_of(d), ca);
        let e = &idx.entries[&ca];
        assert_eq!(e.markets.len(), 2);
        assert_eq!(e.relationships.len(), 1);
        assert!(e.partitions.is_empty());
        assert_eq!(e.tokens.len(), 4);
        for t in &e.tokens {
            assert_eq!(idx.by_token[t], ca);
        }
        let b = r.market_by_condition("0xb").unwrap().id;
        let eb = &idx.entries[&r.component_of(b)];
        assert_eq!(eb.partitions.len(), 1);
    }

    #[test]
    fn pack_components_keeps_components_whole() {
        let r = reg();
        let chunks = pack_components(&r, 4);
        assert_eq!(chunks.len(), 2);
        let idx = build_component_index(&r);
        for chunk in &chunks {
            let cid = idx.by_token[&chunk[0]];
            assert!(
                chunk.iter().all(|t| idx.by_token[t] == cid),
                "chunk spans components"
            );
        }
        // oversized component still ships whole (warn, don't split)
        let chunks = pack_components(&r, 2);
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.len() == 4));
    }

    #[test]
    fn token_maps_cover_both_sides() {
        let r = reg();
        let (tm, mt) = token_maps(&r);
        assert_eq!(tm.len(), 8);
        assert_eq!(mt.len(), 4);
        let m = r.market_by_condition("0xa").unwrap();
        assert_eq!(tm[&m.yes], m.id);
        assert_eq!(mt[&m.id], (m.yes, m.no));
    }

    #[test]
    fn fee_map_carries_registry_rates() {
        let mut b = RegistryBuilder::default();
        b.add_market(
            "0xf",
            "yf",
            "nf",
            TickSize::Cent,
            200,
            false,
            None,
            true,
            false,
            None,
        );
        let r = b.finish("").unwrap();
        let fees = fee_map(&r);
        let m = r.market_by_condition("0xf").unwrap();
        assert_eq!(fees[&m.yes], Bps(200));
        assert_eq!(fees[&m.no], Bps(200));
    }

    #[tokio::test]
    async fn book_fetcher_routes_and_filters_validity() {
        // Serve the command channel manually: a task answering BookSnapshot.
        let (tx, mut rx) = mpsc::channel::<SupervisorCommand>(4);
        tokio::spawn(async move {
            while let Some(SupervisorCommand::BookSnapshot { token, reply }) = rx.recv().await {
                let mut book = Book::new(TickSize::Cent);
                use pm_core::book::Side;
                use pm_core::num::{Px, Qty};
                book.apply(Side::Bid, Px::new(40, TickSize::Cent).unwrap(), Qty(1));
                // token 1 → valid book; token 2 → invalid book; others handled by router
                let _ = reply.send(Some((book, token == TokenId(1))));
            }
        });
        let routes = HashMap::from([(TokenId(1), tx.clone()), (TokenId(2), tx)]);
        let f = BookFetcher::new(routes);

        // raw fetch returns both
        assert!(f.fetch(TokenId(1)).await.is_some());
        assert_eq!(f.fetch(TokenId(2)).await.map(|(_, v)| v), Some(false));
        // unknown token → None without panic
        assert!(f.fetch(TokenId(9)).await.is_none());

        // BookSource filters invalid books out
        use pm_execution::venue::BookSource;
        let mut bs = f.clone();
        assert!(bs.book(TokenId(1)).await.is_some());
        assert!(bs.book(TokenId(2)).await.is_none());
        assert!(bs.book(TokenId(9)).await.is_none());
    }
}
