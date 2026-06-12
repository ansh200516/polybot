use pm_core::instrument::{MarketId, Partition, Relationship};

/// Connected components over markets: partition members ∪ approved
/// relationship endpoints (spec §9). Rebuilt on every registry sync.
#[derive(Debug, Clone)]
pub struct Components {
    parent: Vec<u32>, // union-find, indexed by MarketId
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ComponentId(pub u32);

impl Components {
    pub fn build(n_markets: u32, partitions: &[Partition], relationships: &[Relationship]) -> Self {
        let mut c = Components { parent: (0..n_markets).collect() };
        for p in partitions {
            for w in p.markets.windows(2) {
                c.union(w[0], w[1]);
            }
        }
        for r in relationships {
            let (a, b) = match *r {
                Relationship::Implies { a, b }
                | Relationship::MutuallyExclusive { a, b }
                | Relationship::Equivalent { a, b } => (a, b),
            };
            c.union(a, b);
        }
        c
    }

    fn find(&self, i: u32) -> u32 {
        let mut i = i;
        while self.parent[i as usize] != i {
            i = self.parent[i as usize];
        }
        i
    }

    fn union(&mut self, a: MarketId, b: MarketId) {
        debug_assert!((a.0 as usize) < self.parent.len());
        debug_assert!((b.0 as usize) < self.parent.len());
        let (ra, rb) = (self.find(a.0), self.find(b.0));
        if ra != rb {
            self.parent[rb as usize] = ra;
        }
    }

    pub fn component_of(&self, m: MarketId) -> ComponentId {
        debug_assert!((m.0 as usize) < self.parent.len());
        ComponentId(self.find(m.0))
    }

    pub fn members(&self, c: ComponentId) -> Vec<MarketId> {
        (0..self.parent.len() as u32)
            .filter(|&i| self.find(i) == c.0)
            .map(MarketId)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::instrument::{EventId, MarketId, Partition, Relationship, TokenId};

    fn part(event: u32, members: &[u32]) -> Partition {
        Partition {
            event: EventId(event),
            markets: members.iter().map(|&i| MarketId(i)).collect(),
            yes_tokens: members.iter().map(|&i| TokenId(u64::from(i) * 2)).collect(),
            no_tokens: members.iter().map(|&i| TokenId(u64::from(i) * 2 + 1)).collect(),
            verified_exhaustive: true,
        }
    }

    #[test]
    fn partitions_and_relationships_union() {
        let parts = vec![part(0, &[0, 1, 2])];
        let rels = vec![Relationship::Implies { a: MarketId(3), b: MarketId(0) }];
        let c = Components::build(5, &parts, &rels); // markets 0..5
        assert_eq!(c.component_of(MarketId(0)), c.component_of(MarketId(2)));
        assert_eq!(c.component_of(MarketId(3)), c.component_of(MarketId(1)));
        assert_ne!(c.component_of(MarketId(4)), c.component_of(MarketId(0)));
        // membership listing
        let comp = c.members(c.component_of(MarketId(0)));
        assert_eq!(comp.len(), 4);
        assert!(!comp.contains(&MarketId(4)));
    }

    #[test]
    fn singleton_markets_are_their_own_component() {
        let c = Components::build(3, &[], &[]);
        let ids: std::collections::HashSet<_> = (0..3).map(|i| c.component_of(MarketId(i))).collect();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn chain_of_relationships_merges_transitively() {
        let rels = vec![
            Relationship::Implies { a: MarketId(0), b: MarketId(1) },
            Relationship::Implies { a: MarketId(1), b: MarketId(2) },
        ];
        let c = Components::build(4, &[], &rels);
        assert_eq!(c.component_of(MarketId(0)), c.component_of(MarketId(2)));
        assert_ne!(c.component_of(MarketId(0)), c.component_of(MarketId(3)));
    }
}
