pub mod components;
pub mod gamma;
pub mod intern;
pub mod partitions;
pub mod relationships;

use std::collections::HashMap;

use pm_core::instrument::{EventId, Market, MarketId, Partition, Relationship, TokenId};
use pm_core::num::{Bps, TickSize};

use crate::components::{ComponentId, Components};
use crate::intern::Interner;
use crate::partitions::{ExclusionReason, MemberMarket, derive_partition};
use crate::relationships::{LoadedRelationships, RelationshipError, load_relationships};

// ---------------------------------------------------------------------------
// Public error type
// ---------------------------------------------------------------------------

/// Errors that can occur when building a Registry.
#[derive(Debug)]
pub enum RegistryError {
    Relationship(RelationshipError),
}

impl From<RelationshipError> for RegistryError {
    fn from(e: RelationshipError) -> Self {
        RegistryError::Relationship(e)
    }
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Relationship(e) => write!(f, "registry build error: {e}"),
        }
    }
}

impl std::error::Error for RegistryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RegistryError::Relationship(e) => Some(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Registry (immutable snapshot)
// ---------------------------------------------------------------------------

/// Immutable snapshot of all market/partition/relationship metadata.
/// Published as `Arc<Registry>` via `tokio::sync::watch` from the ingestion
/// sync task. The registry crate itself stays I/O-free.
pub struct Registry {
    /// Dense list of markets, indexed by MarketId.
    markets: Vec<Market>,
    /// TokenId → MarketId (for quick market lookups by token).
    token_market: HashMap<TokenId, MarketId>,
    /// The interner — kept for venue-string lookups and condition-id resolution.
    interner: Interner,
    /// All partitions (verified or not; the engine gates on the flag).
    partitions: Vec<Partition>,
    /// Approved relationships.
    approved_relationships: Vec<Relationship>,
    /// Components (union-find over all markets).
    components: Components,
    /// Log of (event, reason) exclusions from verification.
    exclusion_log: Vec<(EventId, ExclusionReason)>,
    /// (kind, a, b) from relationship TOML that couldn't be resolved.
    unresolved_relationships: Vec<(String, String, String)>,
    /// Number of pending (not yet approved or rejected) relationships.
    pending_relationship_count: usize,
}

// Compile-time Send + Sync assertion.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn check() {
        assert_send_sync::<Registry>();
    }
    let _ = check;
};

impl Registry {
    /// All markets in intern order.
    pub fn markets(&self) -> &[Market] {
        &self.markets
    }

    /// Look up a market by its venue condition-id string (e.g. `"0xabc..."`).
    pub fn market_by_condition(&self, condition_id: &str) -> Option<&Market> {
        self.interner.find_market(condition_id).and_then(|mid| self.markets.get(mid.0 as usize))
    }

    /// Which market owns this token?
    pub fn market_of_token(&self, token: TokenId) -> Option<&Market> {
        let &mid = self.token_market.get(&token)?;
        self.markets.get(mid.0 as usize)
    }

    /// Tick size for a token.
    pub fn tick_of(&self, token: TokenId) -> Option<TickSize> {
        Some(self.market_of_token(token)?.tick)
    }

    /// Fee in bps for a token.
    pub fn fee_of(&self, token: TokenId) -> Option<Bps> {
        Some(self.market_of_token(token)?.fee_bps)
    }

    /// All partitions (verified and unverified).
    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    /// Approved relationships loaded from the relationship TOML.
    pub fn approved_relationships(&self) -> &[Relationship] {
        &self.approved_relationships
    }

    /// Which component does this market belong to?
    pub fn component_of(&self, market: MarketId) -> ComponentId {
        self.components.component_of(market)
    }

    /// All markets in a component.
    pub fn component_members(&self, component: ComponentId) -> Vec<MarketId> {
        self.components.members(component)
    }

    /// All token ids interned (yes and no for every market).
    pub fn all_tokens(&self) -> Vec<TokenId> {
        self.markets
            .iter()
            .flat_map(|m| [m.yes, m.no])
            .collect()
    }

    /// Venue token id string for an interned token handle.
    pub fn token_venue_id(&self, token: TokenId) -> Option<&str> {
        self.interner.token_str(token)
    }

    /// Find a token by venue id string without inserting (returns None if not interned).
    pub fn venue_token_id(&self, venue_id: &str) -> Option<TokenId> {
        self.interner.find_token(venue_id)
    }

    /// Log of (event, reason) pairs explaining why an event's partition was
    /// excluded from `verified_exhaustive`.
    pub fn exclusion_log(&self) -> &[(EventId, ExclusionReason)] {
        &self.exclusion_log
    }

    /// Relationship TOML entries whose condition ids weren't found in the interned
    /// market set — (kind, a, b).
    pub fn unresolved_relationships(&self) -> &[(String, String, String)] {
        &self.unresolved_relationships
    }

    /// Number of `status = "pending"` entries in the relationship TOML.
    pub fn pending_relationship_count(&self) -> usize {
        self.pending_relationship_count
    }
}

// ---------------------------------------------------------------------------
// Per-market builder metadata (not part of the final Market struct)
// ---------------------------------------------------------------------------

struct MarketMeta {
    question: Option<String>,
    active: bool,
    closed: bool,
}

// ---------------------------------------------------------------------------
// RegistryBuilder
// ---------------------------------------------------------------------------

/// Builds a [`Registry`] by ingesting venue metadata then finalising with the
/// relationship TOML. The builder itself is mutable; the resulting `Registry`
/// is immutable and `Send + Sync`.
#[derive(Default)]
pub struct RegistryBuilder {
    interner: Interner,
    markets: Vec<Market>,
    meta: Vec<MarketMeta>,
    /// event → list of member MarketIds (in insertion order).
    event_members: HashMap<EventId, Vec<MarketId>>,
}

impl RegistryBuilder {
    /// Register one binary market. The builder interns `condition_id` → `MarketId`,
    /// `yes_venue_id` / `no_venue_id` → `TokenId`, and (if provided) `event_key` → `EventId`.
    ///
    /// # Parameters
    /// - `condition_id`: venue condition-id hex string (e.g. `"0xabc..."`).
    /// - `yes_venue_id` / `no_venue_id`: venue uint256 decimal token id strings.
    /// - `tick`: tick size.
    /// - `fee_bps`: maker fee in basis points.
    /// - `neg_risk`: whether this market is part of a NegRisk event.
    /// - `question`: display question text (used for placeholder screening).
    /// - `active` / `closed`: market state flags.
    /// - `event_key`: optional event grouping key (intern → EventId).
    #[allow(clippy::too_many_arguments)]
    pub fn add_market(
        &mut self,
        condition_id: &str,
        yes_venue_id: &str,
        no_venue_id: &str,
        tick: TickSize,
        fee_bps: i32,
        neg_risk: bool,
        question: Option<String>,
        active: bool,
        closed: bool,
        event_key: Option<&str>,
    ) {
        let mid = self.interner.market(condition_id);
        // Dense: the intern id must match the vec index.
        debug_assert_eq!(mid.0 as usize, self.markets.len());

        let yes = self.interner.token(yes_venue_id);
        let no = self.interner.token(no_venue_id);

        self.markets.push(Market {
            id: mid,
            yes,
            no,
            tick,
            fee_bps: Bps(fee_bps),
            neg_risk,
        });

        let event_id = event_key.map(|k| self.interner.event(k));

        if let Some(eid) = event_id {
            self.event_members.entry(eid).or_default().push(mid);
        }

        self.meta.push(MarketMeta { question, active, closed });
    }

    /// Finalise the registry with the relationship TOML source.
    ///
    /// - Groups markets by `EventId` and calls `derive_partition` for each group.
    /// - Loads relationships using a resolver over the condition-id intern table.
    /// - Builds the union-find components.
    /// - Returns the immutable [`Registry`].
    pub fn finish(self, relationship_toml: &str) -> Result<Registry, RegistryError> {
        let RegistryBuilder { interner, markets, meta, event_members } = self;

        // ---- 1. Derive partitions per event --------------------------------
        let mut partitions: Vec<Partition> = Vec::new();
        let mut exclusion_log: Vec<(EventId, ExclusionReason)> = Vec::new();

        for (event_id, member_ids) in &event_members {
            // Build MemberMarket slice from the per-market metadata.
            let members: Vec<MemberMarket> = member_ids
                .iter()
                .map(|&mid| {
                    let m = &markets[mid.0 as usize];
                    let mx = &meta[mid.0 as usize];
                    MemberMarket {
                        market: mid,
                        yes: m.yes,
                        no: m.no,
                        question: mx.question.clone(),
                        active: mx.active,
                        closed: mx.closed,
                    }
                })
                .collect();

            // neg_risk = true if any member's flag is set (venue marks all
            // members; we take the logical OR just in case of partial data).
            let neg_risk = members
                .iter()
                .any(|mm| markets[mm.market.0 as usize].neg_risk);

            let (partition, reasons) = derive_partition(*event_id, neg_risk, &members);

            for reason in &reasons {
                exclusion_log.push((*event_id, *reason));
            }

            // Collect ALL partitions (engine gates on verified_exhaustive).
            partitions.push(partition);
        }

        // ---- 2. Load relationships -----------------------------------------
        let resolver = |cond_id: &str| -> Option<MarketId> { interner.find_market(cond_id) };

        let LoadedRelationships {
            approved: approved_relationships,
            pending_count: pending_relationship_count,
            unresolved: unresolved_relationships,
        } = load_relationships(relationship_toml, &resolver)?;

        // ---- 3. Build components -------------------------------------------
        let n_markets = markets.len() as u32;
        let components = Components::build(n_markets, &partitions, &approved_relationships);

        // ---- 4. Build token → market index ---------------------------------
        let mut token_market: HashMap<TokenId, MarketId> =
            HashMap::with_capacity(markets.len() * 2);
        for m in &markets {
            token_market.insert(m.yes, m.id);
            token_market.insert(m.no, m.id);
        }

        Ok(Registry {
            markets,
            token_market,
            interner,
            partitions,
            approved_relationships,
            components,
            exclusion_log,
            unresolved_relationships,
            pending_relationship_count,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::TickSize;

    fn sample() -> Registry {
        let mut b = RegistryBuilder::default();
        b.add_market("0xaaa", "tokyes_a", "tokno_a", TickSize::Cent, 0, false, Some("Will A win?".into()), true, false, None);
        b.add_market("0xbbb", "tokyes_b", "tokno_b", TickSize::Milli, 0, true, Some("Will B win?".into()), true, false, Some("ev1"));
        b.add_market("0xccc", "tokyes_c", "tokno_c", TickSize::Milli, 0, true, Some("Will C win?".into()), true, false, Some("ev1"));
        b.finish("").unwrap()
    }

    #[test]
    fn builder_interns_and_indexes() {
        let r = sample();
        assert_eq!(r.markets().len(), 3);
        let m = r.market_by_condition("0xbbb").unwrap();
        let yes = m.yes;
        assert_eq!(r.market_of_token(yes).unwrap().id, m.id);
        assert_eq!(r.tick_of(yes).unwrap(), TickSize::Milli);
    }

    #[test]
    fn negrisk_event_becomes_partition() {
        let r = sample();
        assert_eq!(r.partitions().len(), 1);
        let p = &r.partitions()[0];
        assert!(p.verified_exhaustive);
        assert_eq!(p.markets.len(), 2);
    }

    #[test]
    fn components_span_partitions() {
        let r = sample();
        let b = r.market_by_condition("0xbbb").unwrap().id;
        let c = r.market_by_condition("0xccc").unwrap().id;
        let a = r.market_by_condition("0xaaa").unwrap().id;
        assert_eq!(r.component_of(b), r.component_of(c));
        assert_ne!(r.component_of(a), r.component_of(b));
    }

    #[test]
    fn relationships_wire_into_components() {
        let mut b = RegistryBuilder::default();
        b.add_market("0xaaa", "ya", "na", TickSize::Cent, 0, false, None, true, false, None);
        b.add_market("0xbbb", "yb", "nb", TickSize::Cent, 0, false, None, true, false, None);
        let toml = "[[relationship]]\nkind = \"implies\"\na = \"0xaaa\"\nb = \"0xbbb\"\nstatus = \"approved\"\n";
        let r = b.finish(toml).unwrap();
        assert_eq!(r.approved_relationships().len(), 1);
        let a = r.market_by_condition("0xaaa").unwrap().id;
        let bb = r.market_by_condition("0xbbb").unwrap().id;
        assert_eq!(r.component_of(a), r.component_of(bb));
    }

    #[test]
    fn all_tokens_enumerates_both_sides() {
        let r = sample();
        assert_eq!(r.all_tokens().len(), 6);
    }

    #[test]
    fn market_by_condition_unknown_returns_none() {
        let r = sample();
        assert!(r.market_by_condition("0xunknown").is_none());
    }

    #[test]
    fn component_members_round_trips_component_of() {
        let r = sample();
        let b_id = r.market_by_condition("0xbbb").unwrap().id;
        let comp = r.component_of(b_id);
        let members = r.component_members(comp);
        assert!(members.contains(&b_id));
        // Every member maps back to the same component.
        for m in &members {
            assert_eq!(r.component_of(*m), comp);
        }
    }

    #[test]
    fn fee_of_returns_correct_bps() {
        let mut b = RegistryBuilder::default();
        b.add_market("0xfee", "fy", "fn", TickSize::Cent, 200, false, None, true, false, None);
        let r = b.finish("").unwrap();
        let m = r.market_by_condition("0xfee").unwrap();
        assert_eq!(r.fee_of(m.yes), Some(Bps(200)));
        assert_eq!(r.fee_of(m.no), Some(Bps(200)));
    }

    #[test]
    fn venue_token_id_find_without_insert() {
        let r = sample();
        // known venue string → returns the interned TokenId
        let tid = r.venue_token_id("tokyes_a");
        assert!(tid.is_some());
        assert_eq!(tid.unwrap(), r.markets()[0].yes);
        // unknown venue string → None (no side-effect insertion)
        assert!(r.venue_token_id("nothere").is_none());
    }

    #[test]
    fn token_venue_id_round_trips() {
        let r = sample();
        let m = r.market_by_condition("0xbbb").unwrap();
        let venue = r.token_venue_id(m.yes).unwrap();
        assert_eq!(venue, "tokyes_b");
    }
}
