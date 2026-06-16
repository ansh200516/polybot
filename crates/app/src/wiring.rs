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

use crate::strategy::{StrategyEnvelope, StrategyId};

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

pub fn risk_config(
    cfg: &Config,
    session_loss_cap: Option<Usdc>,
) -> Result<RiskConfig, ConfigError> {
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
        session_loss_cap,
    })
}

/// Per-strategy inventory-risk caps (spec §5, Phase 2). Mirrors `risk_config`:
/// converts the `[inventory]` config money (USD) to µUSDC via `usd_to_microusdc`
/// and the volatility window (ms) to a `Duration`. INERT — there is no caller
/// yet; the Phase-4 market-making strategy wires it when it opts into inventory
/// risk. Parse-time bounds (positive money, `daily ≥ stop`, `vol_window_ms ≥ 1`)
/// are enforced by `Config::validate`.
pub fn inventory_config(cfg: &Config) -> Result<pm_risk::inventory::InventoryConfig, ConfigError> {
    Ok(pm_risk::inventory::InventoryConfig {
        max_inventory_usd: Usdc(usd_to_microusdc(cfg.inventory.max_inventory_usd)?),
        max_gross_inventory_usd: Usdc(usd_to_microusdc(cfg.inventory.max_gross_inventory_usd)?),
        inventory_stop_loss_usd: Usdc(usd_to_microusdc(cfg.inventory.inventory_stop_loss_usd)?),
        daily_loss_usd: Usdc(usd_to_microusdc(cfg.inventory.daily_loss_usd)?),
        vol_pull_ticks: cfg.inventory.vol_pull_ticks,
        vol_window: std::time::Duration::from_millis(cfg.inventory.vol_window_ms),
    })
}

/// The platform's per-strategy capital envelopes plus the RiskConfig arb's
/// `RiskEngine` enforces — the pure capital carve-out (Task 4.4b), factored out
/// of `main`'s host-build block so it is unit-testable.
///
/// `mm` is `Some` only when `[strategies.mm] enabled`. The risk field on each
/// envelope is allocator/record metadata (the host only sums `capital`); MM's
/// real risk enforcement is its [`InventoryConfig`](pm_risk::inventory::InventoryConfig),
/// not `mm`'s envelope `RiskConfig`.
pub struct PlatformEnvelopes {
    /// Arb's envelope: the WHOLE bankroll when MM is off, else `bankroll −
    /// mm_capital`. Its `risk` is [`arb_risk`](Self::arb_risk).
    pub arb: StrategyEnvelope,
    /// The heartbeat's envelope: always zero capital (it takes no risk).
    pub heartbeat: StrategyEnvelope,
    /// The MM's envelope — `Some` only when the MM is enabled.
    pub mm: Option<StrategyEnvelope>,
    /// The RiskConfig arb's `RiskEngine` enforces. Byte-identical to the input
    /// `risk_cfg` when MM is off; when MM is on its `bankroll` is REDUCED to
    /// arb's slice so arb genuinely trades within its reduced capital (the crux
    /// of sharing real funds without overlap).
    pub arb_risk: RiskConfig,
}

/// Carve the platform `bankroll` into per-strategy envelopes (Task 4.4b). Pure
/// (no I/O), so it is unit-tested directly.
///
/// * **MM disabled (default):** byte-identical to pre-4.4b — arb claims the
///   WHOLE bankroll (its `RiskEngine` cap stays `risk_cfg.bankroll`), the
///   heartbeat claims zero, and there is no MM envelope. Σ capital == bankroll.
/// * **MM enabled:** `mm_capital = usd→µUSDC(mm.capital_usd)` is carved OUT, so
///   `arb_capital = bankroll − mm_capital`. Arb's enforced `RiskConfig.bankroll`
///   is reduced to `arb_capital` ([`arb_risk`](PlatformEnvelopes::arb_risk)) so
///   the two strategies SHARE the bankroll without overlapping real funds. Σ
///   capital (arb + mm + heartbeat 0) == bankroll, so the startup allocator
///   passes exactly.
///
/// `bankroll` is the platform bankroll the host validates against (the caller
/// passes `risk_cfg.bankroll`). Errors if `mm.capital_usd` is unconvertible or
/// (when enabled) exceeds the bankroll — the latter is also rejected at config
/// validation, but guarded here too since this fn is the real-money carve.
pub fn strategy_envelopes(
    config: &Config,
    risk_cfg: &RiskConfig,
    bankroll: Usdc,
) -> Result<PlatformEnvelopes, ConfigError> {
    let mm = &config.strategies.mm;
    // The heartbeat always claims zero capital; its risk is record-only.
    let heartbeat = StrategyEnvelope::new(StrategyId("heartbeat"), Usdc(0), risk_cfg.clone());

    if !mm.enabled {
        // DEFAULT path — change NOTHING: arb claims the whole bankroll and its
        // enforced risk cap is the unmodified `risk_cfg`.
        return Ok(PlatformEnvelopes {
            arb: StrategyEnvelope::new(StrategyId("arb"), bankroll, risk_cfg.clone()),
            heartbeat,
            mm: None,
            arb_risk: risk_cfg.clone(),
        });
    }

    // MM ON: carve its slice out of the bankroll (real funds — no overlap).
    let mm_capital = Usdc(usd_to_microusdc(mm.capital_usd)?);
    if mm_capital.0 > bankroll.0 {
        return Err(ConfigError::BadMoney(
            "strategies.mm.capital_usd exceeds the platform bankroll",
        ));
    }
    let arb_capital = Usdc(bankroll.0 - mm_capital.0);
    // Arb's RiskEngine cap shrinks to its slice so it trades within it.
    let arb_risk = RiskConfig {
        bankroll: arb_capital,
        ..risk_cfg.clone()
    };
    // MM's envelope risk records its slice; MM enforces via InventoryConfig.
    let mm_risk = RiskConfig {
        bankroll: mm_capital,
        ..risk_cfg.clone()
    };
    Ok(PlatformEnvelopes {
        arb: StrategyEnvelope::new(StrategyId("arb"), arb_capital, arb_risk.clone()),
        heartbeat,
        mm: Some(StrategyEnvelope::new(StrategyId("mm"), mm_capital, mm_risk)),
        arb_risk,
    })
}

/// Task 4.5 — the LIVE-gating predicate for the market maker. MM trades REAL
/// maker orders ONLY when BOTH hold:
///  * `process_live` — the PROCESS is in real-money mode (`--live`). This is also
///    what forces the typed `confirm_phrase` at startup, so a `true` here means
///    the live confirmation already ran (main blocks startup until it is typed
///    when `--live`). MM therefore CANNOT reach a live venue without it.
///  * `mm_live` — the operator opted the STRATEGY in (`[strategies.mm].live`).
///
/// Pure + total (just `process_live && mm_live`) so the truth table is
/// unit-tested directly. main builds MM's live venue iff this returns `true`;
/// EVERY other combination uses the paper maker venue, so paper is the default
/// and the live path requires deliberate opt-in at BOTH the process and the
/// strategy level (plus the confirmation the process gate enforces).
pub fn mm_use_live(process_live: bool, mm_live: bool) -> bool {
    process_live && mm_live
}

/// Task 4.6 — derive the user-channel WS URL from the configured MARKET WS URL.
///
/// The market feed lives at `…/ws/market`; the user (private fills) feed is its
/// sibling `…/ws/user` (spike-confirmed:
/// `wss://ws-subscriptions-clob.polymarket.com/ws/user`). Deriving it from the
/// existing `endpoints.ws_market_url` avoids a second config field while keeping
/// a custom/staging host working. If the configured market URL is NOT the
/// expected `…/market` shape, fall back to the spike-confirmed absolute URL
/// (the user feed is on the production host regardless).
///
/// Pure + total, so the derivation is unit-tested directly.
pub fn user_ws_url(market_ws_url: &str) -> String {
    match market_ws_url.strip_suffix("/market") {
        Some(base) => format!("{base}/user"),
        None => "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_string(),
    }
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

pub fn build_component_index(reg: &Registry, include_nonexhaustive_negrisk: bool) -> ComponentIndex {
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
        // Well-formedness is mandatory. Beyond that:
        //  - verified-exhaustive sets always enter (class 2 + LP exactly-one).
        //  - mutually-exclusive-only NegRisk sets enter ONLY when opted in; the
        //    LP then models them as at-most-one-winner (k+1 worlds, see
        //    enumerate_worlds). class 2 still gates on verified_exhaustive, so it
        //    never trades them as complete sets.
        //  - a non-NegRisk grouping is never mutually exclusive → never entered;
        //    its markets stay free binary vars (the conservative fallback).
        if !p.is_well_formed() {
            continue;
        }
        let include = p.verified_exhaustive || (include_nonexhaustive_negrisk && p.neg_risk);
        if !include {
            continue;
        }
        if let Some(&first) = p.markets.first() {
            let cid = reg.component_of(first);
            // Registry construction guarantees it.
            debug_assert!(
                entries.contains_key(&cid),
                "partition member missing from market entries"
            );
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
    // Token→component grouping is independent of which partitions enter LP
    // entries, so the gate is irrelevant here.
    let idx = build_component_index(reg, false);
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
        let r = risk_config(&cfg, None).unwrap();
        assert_eq!(r.bankroll, Usdc(10_000_000_000));
        assert_eq!(r.per_market_cap, Usdc(1_000_000_000));
        assert_eq!(r.max_unhedged, Usdc(200_000_000));
        assert_eq!(r.daily_drawdown_bps, 200);
        assert_eq!(r.restart_storm_count, 5);
    }

    #[test]
    fn inventory_config_reflects_conservative_defaults() {
        let cfg = pm_config::Config::default();
        let inv = inventory_config(&cfg).unwrap();
        assert_eq!(inv.max_inventory_usd, Usdc(50_000_000)); // $50
        assert_eq!(inv.max_gross_inventory_usd, Usdc(100_000_000)); // $100
        assert_eq!(inv.inventory_stop_loss_usd, Usdc(25_000_000)); // $25
        assert_eq!(inv.daily_loss_usd, Usdc(50_000_000)); // $50
        assert_eq!(inv.vol_pull_ticks, 5);
        assert_eq!(inv.vol_window, std::time::Duration::from_millis(2000));
    }

    // ── Capital carve-out (Task 4.4b) ──────────────────────────────────────

    /// MM OFF (default): arb claims the WHOLE bankroll, there is no MM envelope,
    /// arb's enforced risk cap is unchanged, and Σ capital == bankroll — i.e.
    /// byte-identical to pre-4.4b (the live arb path the user runs today).
    #[test]
    fn strategy_envelopes_default_off_keeps_arb_whole_bankroll() {
        let cfg = Config::default(); // mm.enabled == false
        let risk = risk_config(&cfg, None).unwrap();
        let bankroll = risk.bankroll;
        let env = strategy_envelopes(&cfg, &risk, bankroll).unwrap();

        assert!(env.mm.is_none(), "no MM envelope when disabled");
        assert_eq!(env.arb.id, StrategyId("arb"));
        assert_eq!(env.arb.capital, bankroll, "arb claims the whole bankroll");
        assert_eq!(env.heartbeat.capital, Usdc(0), "heartbeat takes no capital");
        assert_eq!(
            env.arb_risk.bankroll, bankroll,
            "arb's enforced risk cap is unchanged when MM is off"
        );
        // Σ capital == bankroll → the startup allocator passes.
        let all = [env.arb.clone(), env.heartbeat.clone()];
        assert_eq!(all.iter().map(|e| e.capital.0).sum::<i128>(), bankroll.0);
        assert!(crate::strategy::allocate(&all, bankroll).is_ok());
    }

    /// MM ON: the bankroll is carved between arb and MM. The host gains an "mm"
    /// envelope, arb's envelope (and enforced risk cap) drop to `bankroll −
    /// mm_capital`, the heartbeat stays zero, and Σ envelopes == bankroll so the
    /// allocator passes with no overlapping real funds.
    #[test]
    fn strategy_envelopes_mm_on_carves_capital_and_reduces_arb() {
        let mut cfg = Config::default();
        cfg.strategies.mm.enabled = true;
        cfg.strategies.mm.capital_usd = 25.0;
        let risk = risk_config(&cfg, None).unwrap();
        let bankroll = risk.bankroll; // $10_000 → 10_000_000_000 µUSDC
        let mm_capital = Usdc(usd_to_microusdc(25.0).unwrap());

        let env = strategy_envelopes(&cfg, &risk, bankroll).unwrap();
        let mm = env.mm.clone().unwrap(); // present when enabled
        assert_eq!(mm.id, StrategyId("mm"));
        assert_eq!(mm.capital, mm_capital, "MM gets exactly its configured slice");

        let arb_slice = Usdc(bankroll.0 - mm_capital.0);
        assert_eq!(env.arb.capital, arb_slice, "arb's envelope = bankroll − mm");
        assert_eq!(
            env.arb_risk.bankroll, arb_slice,
            "arb's RiskEngine cap is REDUCED to its slice (genuinely shares funds)"
        );
        assert_eq!(env.heartbeat.capital, Usdc(0));

        // Σ envelopes == bankroll EXACTLY → allocator passes, no overlap.
        let all = [env.arb.clone(), env.heartbeat.clone(), mm];
        assert_eq!(
            all.iter().map(|e| e.capital.0).sum::<i128>(),
            bankroll.0,
            "arb + mm + heartbeat == bankroll"
        );
        assert!(crate::strategy::allocate(&all, bankroll).is_ok());
    }

    /// An enabled MM whose slice exceeds the bankroll is a fatal carve error
    /// (can't share funds that don't exist) — guarded in the helper, not just at
    /// config validation.
    #[test]
    fn strategy_envelopes_rejects_mm_capital_over_bankroll() {
        let mut cfg = Config::default();
        cfg.strategies.mm.enabled = true;
        cfg.strategies.mm.capital_usd = 20_000.0; // > $10_000 default bankroll
        let risk = risk_config(&cfg, None).unwrap();
        let bankroll = risk.bankroll;
        assert!(strategy_envelopes(&cfg, &risk, bankroll).is_err());
    }

    // ── Live gating (Task 4.5) ─────────────────────────────────────────────

    /// The LIVE predicate is the conjunction of the PROCESS `--live` flag and the
    /// STRATEGY `[strategies.mm].live` opt-in: ONLY `(true, true)` is live; all
    /// three other combinations are paper. The startup confirmation is enforced
    /// at the process level (main blocks until `confirm_phrase` is typed when
    /// `--live`), so a `true` result here necessarily means the confirmation ran
    /// — MM cannot select a live venue without it.
    #[test]
    fn mm_use_live_truth_table() {
        assert!(mm_use_live(true, true), "process --live AND mm.live → LIVE");
        assert!(!mm_use_live(true, false), "process --live, mm paper → paper");
        assert!(!mm_use_live(false, true), "mm.live but process paper → paper");
        assert!(!mm_use_live(false, false), "neither → paper");
    }

    /// Task 4.6: the user-WS URL is the sibling of the configured market WS URL
    /// (`…/ws/market` → `…/ws/user`), and a non-`/market` host falls back to the
    /// spike-confirmed absolute user URL.
    #[test]
    fn user_ws_url_derives_sibling_user_feed() {
        assert_eq!(
            user_ws_url("wss://ws-subscriptions-clob.polymarket.com/ws/market"),
            "wss://ws-subscriptions-clob.polymarket.com/ws/user",
            "the default market URL yields the sibling /ws/user feed"
        );
        // The default config's market URL maps to the spike-confirmed user URL.
        assert_eq!(
            user_ws_url(&Config::default().endpoints.ws_market_url),
            "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        );
        // A non-/market endpoint falls back to the spike-confirmed absolute URL.
        assert_eq!(
            user_ws_url("wss://staging.example/feed"),
            "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        );
    }

    #[test]
    fn component_index_groups_partitions_and_relationships() {
        let r = reg();
        let idx = build_component_index(&r, false);
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

    /// By DEFAULT (gate off) build_component_index must NOT leak unverified or
    /// ill-formed partitions into entries — preserving the conservative M5
    /// behavior (their markets fall back to free binary vars). The opt-in path
    /// is covered by `component_index_includes_nonexhaustive_negrisk_when_opted_in`.
    #[test]
    fn component_index_excludes_unverified_partitions() {
        let mut b = RegistryBuilder::default();
        // Verified 2-member NegRisk event ev1 (b + c).
        b.add_market("0xb", "yb", "nb", TickSize::Cent, 0, true, Some("B?".into()), true, false, Some("ev1"));
        b.add_market("0xc", "yc", "nc", TickSize::Cent, 0, true, Some("C?".into()), true, false, Some("ev1"));
        // Unverified single-member event ev2 (TooFewMembers → verified_exhaustive=false).
        b.add_market("0xe", "ye", "ne", TickSize::Cent, 0, true, Some("E?".into()), true, false, Some("ev2"));
        let r = b.finish("").unwrap();

        // Sanity: the fixture really does contain an unverified partition.
        assert!(
            r.partitions().iter().any(|p| !p.verified_exhaustive),
            "fixture must contain an unverified partition"
        );

        let idx = build_component_index(&r, false);
        // No entry may carry a partition that violates enumerate_worlds' contract.
        for e in idx.entries.values() {
            assert!(
                e.partitions.iter().all(|p| p.verified_exhaustive && p.is_well_formed()),
                "build_component_index leaked an unverified/ill-formed partition"
            );
        }
        // The verified ev1 partition is still retained.
        assert!(
            idx.entries.values().any(|e| e.partitions.len() == 1),
            "verified partition must be retained"
        );
    }

    #[test]
    fn component_index_includes_nonexhaustive_negrisk_when_opted_in() {
        let mut b = RegistryBuilder::default();
        // 2-member NegRisk event where one outcome is a placeholder ("Other") →
        // mutually exclusive (neg_risk) yet NOT verified-exhaustive, still
        // well-formed (2 members).
        b.add_market("0xf", "yf", "nf", TickSize::Cent, 0, true, Some("Will F win?".into()), true, false, Some("ev9"));
        b.add_market("0xg", "yg", "ng", TickSize::Cent, 0, true, Some("Other".into()), true, false, Some("ev9"));
        let r = b.finish("").unwrap();

        // Fixture sanity: a well-formed, non-exhaustive, NegRisk partition exists.
        assert!(
            r.partitions()
                .iter()
                .any(|p| p.neg_risk && !p.verified_exhaustive && p.is_well_formed()),
            "fixture must contain a well-formed non-exhaustive NegRisk partition"
        );

        // Gate OFF (default): the non-exhaustive partition is excluded.
        let off = build_component_index(&r, false);
        assert!(
            off.entries.values().all(|e| e.partitions.is_empty()),
            "non-exhaustive partition must be excluded by default"
        );

        // Gate ON: it is included, and it is the mutually-exclusive-only one.
        let on = build_component_index(&r, true);
        let included: Vec<_> = on.entries.values().flat_map(|e| &e.partitions).collect();
        assert_eq!(included.len(), 1, "opt-in must include the NegRisk partition");
        assert!(
            !included[0].verified_exhaustive && included[0].neg_risk,
            "the included partition is mutually-exclusive-only"
        );
    }

    #[test]
    fn pack_components_keeps_components_whole() {
        let r = reg();
        let chunks = pack_components(&r, 4);
        assert_eq!(chunks.len(), 2);
        let idx = build_component_index(&r, false);
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
