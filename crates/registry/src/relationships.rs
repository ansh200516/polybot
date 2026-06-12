use pm_core::instrument::{MarketId, Relationship};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RelFile {
    #[serde(default)]
    relationship: Vec<RelEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelEntry {
    kind: String,
    a: String,
    b: String,
    status: String,
    #[serde(default)]
    #[allow(dead_code)] // populated by serde deserialization only; kept so deny_unknown_fields accepts it
    note: Option<String>,
}

#[derive(Debug)]
pub struct LoadedRelationships {
    pub approved: Vec<Relationship>,
    pub pending_count: usize,
    /// (kind, a, b) entries whose condition ids didn't resolve to tracked markets.
    pub unresolved: Vec<(String, String, String)>,
}

#[derive(Debug)]
pub enum RelationshipError {
    Parse(String),
    UnknownKind(String),
    UnknownStatus(String),
    SelfReferential { a: String },
    Duplicate { a: MarketId, b: MarketId },
}

impl std::fmt::Display for RelationshipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelationshipError::Parse(s) => write!(f, "relationship parse error: {s}"),
            RelationshipError::UnknownKind(s) => write!(f, "unknown relationship kind: {s}"),
            RelationshipError::UnknownStatus(s) => write!(f, "unknown relationship status: {s}"),
            RelationshipError::SelfReferential { a } => {
                write!(f, "self-referential relationship: {a}")
            }
            RelationshipError::Duplicate { a, b } => {
                write!(f, "duplicate relationship after canonicalization: ({a:?}, {b:?})")
            }
        }
    }
}

impl std::error::Error for RelationshipError {}

pub fn load_relationships(
    toml_src: &str,
    resolve: &impl Fn(&str) -> Option<MarketId>,
) -> Result<LoadedRelationships, RelationshipError> {
    let file: RelFile =
        toml::from_str(toml_src).map_err(|e| RelationshipError::Parse(e.to_string()))?;
    let mut approved = Vec::new();
    let mut pending_count = 0usize;
    let mut unresolved = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for e in &file.relationship {
        match e.status.as_str() {
            "approved" => {}
            "pending" | "rejected" => {
                pending_count += usize::from(e.status == "pending");
                continue;
            }
            other => return Err(RelationshipError::UnknownStatus(other.to_string())),
        }
        // Validate kind eagerly — a typo in a hand-edited file must be loud
        // regardless of whether the condition ids happen to be tracked.
        match e.kind.as_str() {
            "implies" | "mutually_exclusive" | "equivalent" => {}
            other => return Err(RelationshipError::UnknownKind(other.to_string())),
        }
        if e.a == e.b {
            return Err(RelationshipError::SelfReferential { a: e.a.clone() });
        }
        let (Some(a), Some(b)) = (resolve(&e.a), resolve(&e.b)) else {
            unresolved.push((e.kind.clone(), e.a.clone(), e.b.clone()));
            continue;
        };
        if a == b {
            return Err(RelationshipError::SelfReferential { a: e.a.clone() });
        }
        let rel = match e.kind.as_str() {
            "implies" => Relationship::Implies { a, b },
            "mutually_exclusive" => {
                let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
                Relationship::MutuallyExclusive { a: lo, b: hi }
            }
            "equivalent" => {
                let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
                Relationship::Equivalent { a: lo, b: hi }
            }
            // Already validated above; unreachable but needed for exhaustiveness.
            _ => unreachable!(),
        };
        // Duplicate detection on the canonical form (Implies is directional:
        // Implies{a,b} and Implies{b,a} are DIFFERENT constraints, both legal).
        if !seen.insert(rel) {
            let (a, b) = match rel {
                Relationship::Implies { a, b }
                | Relationship::MutuallyExclusive { a, b }
                | Relationship::Equivalent { a, b } => (a, b),
            };
            return Err(RelationshipError::Duplicate { a, b });
        }
        approved.push(rel);
    }
    Ok(LoadedRelationships { approved, pending_count, unresolved })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::instrument::{MarketId, Relationship};

    fn resolver<'a>(pairs: &'a [(&'a str, u32)]) -> impl Fn(&str) -> Option<MarketId> + 'a {
        move |s: &str| pairs.iter().find(|(k, _)| *k == s).map(|(_, v)| MarketId(*v))
    }

    const SAMPLE: &str = r#"
[[relationship]]
kind = "implies"
a = "0xaaa"
b = "0xbbb"
status = "approved"
note = "A winning implies B qualifies"

[[relationship]]
kind = "mutually_exclusive"
a = "0xccc"
b = "0xbbb"
status = "approved"

[[relationship]]
kind = "equivalent"
a = "0xddd"
b = "0xeee"
status = "pending"
"#;

    #[test]
    fn parses_approves_and_canonicalizes() {
        let r = resolver(&[("0xaaa", 5), ("0xbbb", 1), ("0xccc", 9), ("0xddd", 2), ("0xeee", 3)]);
        let out = load_relationships(SAMPLE, &r).unwrap();
        // pending entry excluded from tradable set
        assert_eq!(out.approved.len(), 2);
        assert!(out.approved.contains(&Relationship::Implies { a: MarketId(5), b: MarketId(1) }));
        // mutex canonicalized a ≤ b: (9,1) → (1,9)
        assert!(out.approved.contains(&Relationship::MutuallyExclusive { a: MarketId(1), b: MarketId(9) }));
        assert_eq!(out.pending_count, 1);
    }

    #[test]
    fn implies_direction_is_preserved_not_canonicalized() {
        let r = resolver(&[("0xaaa", 9), ("0xbbb", 1)]);
        let toml = r#"
[[relationship]]
kind = "implies"
a = "0xaaa"
b = "0xbbb"
status = "approved"
"#;
        let out = load_relationships(toml, &r).unwrap();
        assert_eq!(out.approved[0], Relationship::Implies { a: MarketId(9), b: MarketId(1) });
    }

    #[test]
    fn self_referential_is_an_error() {
        let r = resolver(&[("0xaaa", 5)]);
        let toml = r#"
[[relationship]]
kind = "mutually_exclusive"
a = "0xaaa"
b = "0xaaa"
status = "approved"
"#;
        assert!(matches!(load_relationships(toml, &r), Err(RelationshipError::SelfReferential { .. })));
    }

    #[test]
    fn duplicates_after_canonicalization_are_an_error() {
        let r = resolver(&[("0xaaa", 5), ("0xbbb", 1)]);
        let toml = r#"
[[relationship]]
kind = "equivalent"
a = "0xaaa"
b = "0xbbb"
status = "approved"

[[relationship]]
kind = "equivalent"
a = "0xbbb"
b = "0xaaa"
status = "approved"
"#;
        assert!(matches!(load_relationships(toml, &r), Err(RelationshipError::Duplicate { .. })));
    }

    #[test]
    fn unknown_condition_ids_are_skipped_and_reported() {
        let r = resolver(&[("0xaaa", 5)]);
        let toml = r#"
[[relationship]]
kind = "implies"
a = "0xaaa"
b = "0xunknown"
status = "approved"
"#;
        let out = load_relationships(toml, &r).unwrap();
        assert!(out.approved.is_empty());
        assert_eq!(out.unresolved.len(), 1);
    }

    #[test]
    fn unknown_kind_or_status_is_an_error() {
        let r = resolver(&[]);
        assert!(load_relationships("[[relationship]]\nkind = \"causes\"\na = \"x\"\nb = \"y\"\nstatus = \"approved\"\n", &r).is_err());
        assert!(load_relationships("[[relationship]]\nkind = \"implies\"\na = \"x\"\nb = \"y\"\nstatus = \"blessed\"\n", &r).is_err());
    }
}
