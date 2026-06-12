//! NegRisk partition derivation + conservative exhaustiveness verification
//! (spec §8/§9): verified_exhaustive only when every check passes; every
//! failure is reported so the probe can log why a set was excluded.

use pm_core::instrument::{EventId, MarketId, Partition, TokenId};

pub struct MemberMarket {
    pub market: MarketId,
    pub yes: TokenId,
    pub no: TokenId,
    pub question: Option<String>,
    pub active: bool,
    pub closed: bool,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn member(i: u32, q: &str) -> MemberMarket {
        MemberMarket {
            market: MarketId(i),
            yes: TokenId(u64::from(i) * 2),
            no: TokenId(u64::from(i) * 2 + 1),
            question: Some(q.to_string()),
            active: true,
            closed: false,
        }
    }

    #[test]
    fn clean_negrisk_event_verifies() {
        let members = vec![member(0, "Will Alice win?"), member(1, "Will Bob win?"), member(2, "Will Carol win?")];
        let (p, reasons) = derive_partition(EventId(7), true, &members);
        assert!(reasons.is_empty());
        assert!(p.verified_exhaustive);
        assert!(p.is_well_formed());
        assert_eq!(p.yes_tokens.len(), 3);
    }

    #[test]
    fn non_negrisk_event_never_verifies() {
        let members = vec![member(0, "A?"), member(1, "B?")];
        let (p, reasons) = derive_partition(EventId(7), false, &members);
        assert!(!p.verified_exhaustive);
        assert!(reasons.contains(&ExclusionReason::NotNegRisk));
    }

    #[test]
    fn placeholder_outcomes_block_verification() {
        for bad in ["Will another candidate win?", "Other", "None of the above wins", "Will any other person win?"] {
            let members = vec![member(0, "Will Alice win?"), member(1, bad)];
            let (p, reasons) = derive_partition(EventId(7), true, &members);
            assert!(!p.verified_exhaustive, "{bad:?} should block");
            assert!(
                reasons.iter().any(|r| matches!(r, ExclusionReason::PlaceholderOutcome { .. })),
                "{bad:?} should produce PlaceholderOutcome"
            );
        }
        // Assert the payload carries the bad member's id
        let members = vec![member(0, "Will Alice win?"), member(1, "Other")];
        let (_, reasons) = derive_partition(EventId(7), true, &members);
        assert!(reasons.iter().any(|r| matches!(r, ExclusionReason::PlaceholderOutcome { market } if *market == MarketId(1))));
    }

    #[test]
    fn missing_question_blocks_verification() {
        // Conservative: can't screen for placeholders without text.
        let mut m = member(1, "B?");
        m.question = None;
        let members = vec![member(0, "A?"), m];
        let (p, reasons) = derive_partition(EventId(7), true, &members);
        assert!(!p.verified_exhaustive);
        assert!(reasons.iter().any(|r| matches!(r, ExclusionReason::PlaceholderOutcome { .. })));
    }

    #[test]
    fn inactive_or_closed_members_block_verification() {
        // closed = true arm
        let mut m = member(1, "B?");
        m.closed = true;
        let members = vec![member(0, "A?"), m];
        let (p, reasons) = derive_partition(EventId(7), true, &members);
        assert!(!p.verified_exhaustive);
        assert!(reasons.iter().any(|r| matches!(r, ExclusionReason::InactiveMember { market } if *market == MarketId(1))));

        // active = false arm
        let mut m2 = member(1, "B?");
        m2.active = false;
        let members2 = vec![member(0, "A?"), m2];
        let (p2, reasons2) = derive_partition(EventId(7), true, &members2);
        assert!(!p2.verified_exhaustive);
        assert!(reasons2.iter().any(|r| matches!(r, ExclusionReason::InactiveMember { market } if *market == MarketId(1))));
    }

    #[test]
    fn fewer_than_two_members_block_verification() {
        let (p, reasons) = derive_partition(EventId(7), true, &[member(0, "A?")]);
        assert!(!p.verified_exhaustive);
        assert!(!p.is_well_formed());
        assert!(reasons.contains(&ExclusionReason::TooFewMembers));
    }

    #[test]
    fn multiple_reasons_accumulate() {
        let (p, reasons) = derive_partition(EventId(7), false, &[member(0, "Other")]);
        assert!(!p.verified_exhaustive);
        assert!(reasons.contains(&ExclusionReason::NotNegRisk));
        assert!(reasons.contains(&ExclusionReason::TooFewMembers));
        assert!(reasons.iter().any(|r| matches!(r, ExclusionReason::PlaceholderOutcome { market } if *market == MarketId(0))));
        let _ = p;
    }

    #[test]
    fn word_boundaries_protect_proper_nouns() {
        let members = vec![member(0, "Will Motherwell win the league?"), member(1, "Will Celtic win the league?")];
        let (p, reasons) = derive_partition(EventId(7), true, &members);
        assert!(reasons.is_empty(), "{reasons:?}");
        assert!(p.verified_exhaustive);
        // but a real standalone "other" still blocks
        let members = vec![member(0, "Will Motherwell win?"), member(1, "Will some other team win?")];
        let (_, reasons) = derive_partition(EventId(7), true, &members);
        assert!(reasons.iter().any(|r| matches!(r, ExclusionReason::PlaceholderOutcome { market } if *market == MarketId(1))));
    }

    #[test]
    fn empty_member_slice_blocks() {
        let (p, reasons) = derive_partition(EventId(7), true, &[]);
        assert!(!p.verified_exhaustive);
        assert!(reasons.contains(&ExclusionReason::TooFewMembers));
    }
}

/// Member-scoped reasons carry the first offending MarketId so the exclusion
/// log is actionable (spec §8) — the probe resolves it to question text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionReason {
    NotNegRisk,
    PlaceholderOutcome { market: MarketId },
    InactiveMember { market: MarketId },
    TooFewMembers,
}

const PLACEHOLDER_MARKERS: &[&str] = &["other", "another", "none of the above", "any other"];

/// `needle` matches only at word boundaries (non-alphabetic or string edge on
/// both sides) — "other" must not match inside "Motherwell".
fn word_boundary_contains(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let abs = start + pos;
        let before_ok = abs == 0 || !h[abs - 1].is_ascii_alphabetic();
        let end = abs + needle.len();
        let after_ok = end >= h.len() || !h[end].is_ascii_alphabetic();
        if before_ok && after_ok {
            return true;
        }
        start = abs + 1;
    }
    false
}

/// Derive a Partition from one event's member markets. `verified_exhaustive`
/// is set ONLY when every conservative check passes; every failed check is
/// returned for the exclusion log (spec §8). A member with NO question text
/// is treated as a placeholder (can't screen what we can't read).
pub fn derive_partition(
    event: EventId,
    neg_risk: bool,
    members: &[MemberMarket],
) -> (Partition, Vec<ExclusionReason>) {
    let mut reasons = Vec::new();
    if !neg_risk {
        reasons.push(ExclusionReason::NotNegRisk);
    }
    if members.len() < 2 {
        reasons.push(ExclusionReason::TooFewMembers);
    }
    if let Some(m) = members.iter().find(|m| !m.active || m.closed) {
        reasons.push(ExclusionReason::InactiveMember { market: m.market });
    }
    if let Some(m) = members.iter().find(|m| match m.question.as_deref() {
        None => true,
        Some(q) => {
            let q = q.to_lowercase();
            PLACEHOLDER_MARKERS.iter().any(|p| word_boundary_contains(&q, p))
        }
    }) {
        reasons.push(ExclusionReason::PlaceholderOutcome { market: m.market });
    }
    let partition = Partition {
        event,
        markets: members.iter().map(|m| m.market).collect(),
        yes_tokens: members.iter().map(|m| m.yes).collect(),
        no_tokens: members.iter().map(|m| m.no).collect(),
        verified_exhaustive: reasons.is_empty(),
    };
    (partition, reasons)
}
