//! Venue-id interning: the only home of venue strings (spec §3 ids-are-handles).
// each string lives once in the vec (handle-indexed) and once as the map key; ~2× memory, fine at ≤10k entries

use std::collections::HashMap;

use pm_core::instrument::{EventId, MarketId, TokenId};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn intern_is_idempotent_and_dense() {
        let mut it = Interner::default();
        let a = it
            .token("11015470973684177829729219287262166995141465048508201953575582100565462316560");
        let b = it.token("4");
        let a2 = it
            .token("11015470973684177829729219287262166995141465048508201953575582100565462316560");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(a, TokenId(0));
        assert_eq!(b, TokenId(1));
        assert_eq!(
            it.token_str(a).unwrap(),
            "11015470973684177829729219287262166995141465048508201953575582100565462316560"
        );
        assert!(it.token_str(TokenId(99)).is_none());
    }

    #[test]
    fn lookup_without_insert() {
        let mut it = Interner::default();
        assert!(it.find_token("42").is_none());
        let t = it.token("42");
        assert_eq!(it.find_token("42"), Some(t));
    }

    #[test]
    fn markets_and_events_intern_separately() {
        let mut it = Interner::default();
        let m = it.market("0xabc123");
        let e = it.event("141414");
        assert_eq!(m, MarketId(0));
        assert_eq!(e, EventId(0));
        assert_eq!(it.market_str(m).unwrap(), "0xabc123");
        assert_eq!(it.event_str(e).unwrap(), "141414");
        assert_eq!(it.find_market("0xabc123"), Some(m));
        assert_eq!(it.find_event("141414"), Some(e));
        assert!(it.find_market("0xnope").is_none());
    }
}

#[derive(Default, Debug)]
pub struct Interner {
    tokens: Vec<Box<str>>,
    token_idx: HashMap<Box<str>, TokenId>,
    markets: Vec<Box<str>>,
    market_idx: HashMap<Box<str>, MarketId>,
    events: Vec<Box<str>>,
    event_idx: HashMap<Box<str>, EventId>,
}

impl Interner {
    pub fn with_capacity(tokens: usize, markets: usize, events: usize) -> Self {
        Self {
            tokens: Vec::with_capacity(tokens),
            token_idx: HashMap::with_capacity(tokens),
            markets: Vec::with_capacity(markets),
            market_idx: HashMap::with_capacity(markets),
            events: Vec::with_capacity(events),
            event_idx: HashMap::with_capacity(events),
        }
    }

    pub fn token(&mut self, venue_id: &str) -> TokenId {
        if let Some(&t) = self.token_idx.get(venue_id) {
            return t;
        }
        let t = TokenId(self.tokens.len() as u64);
        self.tokens.push(venue_id.into());
        self.token_idx.insert(venue_id.into(), t);
        t
    }

    #[must_use]
    pub fn find_token(&self, venue_id: &str) -> Option<TokenId> {
        self.token_idx.get(venue_id).copied()
    }

    #[must_use]
    pub fn token_str(&self, t: TokenId) -> Option<&str> {
        self.tokens
            .get(usize::try_from(t.0).ok()?)
            .map(AsRef::as_ref)
    }

    pub fn market(&mut self, venue_id: &str) -> MarketId {
        if let Some(&m) = self.market_idx.get(venue_id) {
            return m;
        }
        debug_assert!(self.markets.len() < u32::MAX as usize);
        let m = MarketId(self.markets.len() as u32);
        self.markets.push(venue_id.into());
        self.market_idx.insert(venue_id.into(), m);
        m
    }

    #[must_use]
    pub fn find_market(&self, venue_id: &str) -> Option<MarketId> {
        self.market_idx.get(venue_id).copied()
    }

    #[must_use]
    pub fn market_str(&self, m: MarketId) -> Option<&str> {
        self.markets
            .get(usize::try_from(m.0).ok()?)
            .map(AsRef::as_ref)
    }

    pub fn event(&mut self, venue_id: &str) -> EventId {
        if let Some(&e) = self.event_idx.get(venue_id) {
            return e;
        }
        debug_assert!(self.events.len() < u32::MAX as usize);
        let e = EventId(self.events.len() as u32);
        self.events.push(venue_id.into());
        self.event_idx.insert(venue_id.into(), e);
        e
    }

    #[must_use]
    pub fn find_event(&self, venue_id: &str) -> Option<EventId> {
        self.event_idx.get(venue_id).copied()
    }

    #[must_use]
    pub fn event_str(&self, e: EventId) -> Option<&str> {
        self.events
            .get(usize::try_from(e.0).ok()?)
            .map(AsRef::as_ref)
    }
}
