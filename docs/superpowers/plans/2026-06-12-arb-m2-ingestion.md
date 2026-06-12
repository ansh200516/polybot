# M2 — Registry + Live Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `pm-registry` (venue-id interning, market/partition metadata from Gamma, relationship registry with components) and `pm-ingestion` (CLOB REST + WS read-only client, live single-writer book shards with integrity/staleness/resync) plus a probe binary that demonstrates stable live books — spec §22 M2 exit criteria.

**Architecture:** Reality-first: Task 1 captures live API payloads as committed fixtures; every parser is tested against those fixtures, never against guessed shapes. Registry snapshots publish via `tokio::sync::watch<Arc<Registry>>` (lock-free reads). Books live in N shard tasks (single writer, zero locks, per spec §5/§12); the WS supervisor routes events by token and triggers REST resnapshots on reconnect/integrity failure. Prices/sizes parse from decimal strings straight to exact integers — no f64 anywhere in the money path. Detection dispatch is M3; M2 shards expose a hook and prove book stability.

**Tech Stack:** tokio (rt-multi-thread), reqwest (rustls), tokio-tungstenite (rustls), serde/serde_json, hdrhistogram, tracing + tracing-subscriber. No database, no signing, no order placement — read-only.

**Spec:** `docs/superpowers/specs/2026-06-12-polymarket-arb-bot-v2-design.md` (§5 book integrity, §9 registry, §12 pipeline shape, §13 ingestion, §18 config, §19 errors, §22 exit criteria).

---

## Conventions for every task

- **PATH:** `export PATH="$HOME/.cargo/bin:$PATH"` before every cargo invocation.
- **Branch:** all work on `feat/m2-ingestion` (Task 1 creates it from `main`). Never commit to `main`.
- **Commit trailer:** `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- **Crate naming:** dirs `crates/registry`, `crates/ingestion`; packages `pm-registry`, `pm-ingestion`.
- **Lint policy:** workspace denies `unwrap_used`/`expect_used`/`unsafe_code`; test modules open with `#![allow(clippy::unwrap_used)]`.
- **TDD:** tests first, watch them fail, implement, green, commit.
- **Fixture reconciliation rule:** the serde structs and field names in this plan are templates written from the spec. The committed fixtures from Task 1 are the contract. Where a fixture disagrees with a plan code block, FOLLOW THE FIXTURE, adjust the struct, and note the delta in your report. Inventing fields not present in fixtures is a task failure.
- **No network in `cargo test`.** Unit/replay tests run on fixtures and fake transports only. The probe binary is the only thing that touches the live API.
- **Sketched I/O shells:** a few code blocks for thin HTTP/WS plumbing are signatures + behavior sketches rather than full bodies (they can't be honestly pre-written before Task 1's fixtures exist). For those, THE TESTS IN THAT TASK ARE THE BINDING CONTRACT — write the body to make the specified tests pass, keep the shell ≤ ~15 lines per endpoint, and note any deviation.
- **Clock injection:** anything time-dependent takes `now: Instant` as a parameter (the M1 `Cooldown` pattern) so tests never sleep.

## File map (what exists when M2 is done)

```
crates/registry/Cargo.toml          pm-registry: deps pm-core, serde, serde_json, toml
crates/registry/src/lib.rs          Registry, RegistrySnapshot watch types, errors
crates/registry/src/intern.rs       venue-string ↔ TokenId/MarketId/EventId intern tables
crates/registry/src/gamma.rs        Gamma + CLOB market-metadata models (fixture-shaped)
crates/registry/src/partitions.rs   NegRisk partition derivation + exhaustiveness verification
crates/registry/src/relationships.rs TOML relationship file: parse, validate, canonicalize
crates/registry/src/components.rs   union-find over markets (partitions ∪ approved relationships)
crates/registry/tests/fixtures/     committed sanitized live captures (Task 1)
crates/ingestion/Cargo.toml         pm-ingestion: deps pm-core, pm-registry, pm-config, tokio, reqwest, tokio-tungstenite, serde_json, hdrhistogram, tracing
crates/ingestion/src/lib.rs         module exports, IngestError
crates/ingestion/src/decimal.rs     exact decimal-string → µUSDC / µshares parsers
crates/ingestion/src/livebook.rs    LiveBook (Book + integrity + staleness), apply outcomes
crates/ingestion/src/shard.rs       Shard: token→LiveBook map, event application, stats
crates/ingestion/src/rest.rs        CLOB REST client (book snapshot, markets, time) + token-bucket rate limiter
crates/ingestion/src/ws.rs          WS frame models, subscribe builder, WsTransport trait, session handler
crates/ingestion/src/supervisor.rs  connection supervisor: chunked subs, backoff+jitter, resnapshot triggers
crates/ingestion/src/sync.rs        Gamma full sync + periodic resync → registry watch publisher
crates/ingestion/src/stats.rs       counters + hdrhistogram stages, snapshot formatting
crates/ingestion/src/bin/probe.rs   the M2 acceptance instrument
crates/ingestion/tests/replay.rs    end-to-end replay test: snapshot→deltas→gap→resnapshot on fake transport
docs/recon/RECON.md                 Task 1 findings: endpoints, shapes, limits, quirks
```

Boundary rules: `pm-registry` does no I/O (parsing pure functions + data structures; the HTTP fetch lives in ingestion's sync task). `pm-ingestion` never computes edges — detection is M3; shards expose `BookEvent` hooks.

---

### Task 1: Live API reconnaissance + committed fixtures

**Files:**
- Create: `docs/recon/RECON.md`, `crates/registry/tests/fixtures/*.json`, scratch capture code (deleted after)

This task runs against the live public API (no auth needed for market data). If the sandbox denies network, report BLOCKED — the controller will run the captures and hand you the files.

- [ ] **Step 1: Branch**

```bash
git switch -c feat/m2-ingestion
```

- [ ] **Step 2: REST captures via curl**

```bash
mkdir -p crates/registry/tests/fixtures docs/recon
# Gamma: a page of active markets and events (NegRisk included)
curl -s "https://gamma-api.polymarket.com/markets?limit=5&active=true&closed=false" -o crates/registry/tests/fixtures/gamma_markets.json
curl -s "https://gamma-api.polymarket.com/events?limit=3&active=true&closed=false" -o crates/registry/tests/fixtures/gamma_events.json
# REQUIREMENT: at least one captured event must have negRisk=true (Task 12's
# partition-assembly test depends on it). Inspect; if absent, re-fetch with a
# larger limit or a category known to be NegRisk (e.g. an election event) and
# keep one such event in the fixture.
# CLOB: market metadata page, one order book, server time
curl -s "https://clob.polymarket.com/markets?next_cursor=" -o /tmp/clob_markets_raw.json
curl -s "https://clob.polymarket.com/time" -o crates/registry/tests/fixtures/clob_time.json
```

Inspect `/tmp/clob_markets_raw.json`; copy the envelope plus the FIRST 3 entries (at least one with `neg_risk: true`) into `crates/registry/tests/fixtures/clob_markets.json` (keep the real envelope keys: cursor fields etc.). Pick a liquid token id from it, then:

```bash
curl -s "https://clob.polymarket.com/book?token_id=<TOKEN_ID>" -o crates/registry/tests/fixtures/clob_book.json
```

- [ ] **Step 3: WS capture via scratch binary**

Create a temporary `crates/ingestion` skeleton ONLY if needed — preferred: a standalone scratch dir OUTSIDE the workspace is not allowed; instead add a throwaway example under `examples/` of a tiny new `pm-ingestion` crate created in this task with deps tokio/tokio-tungstenite/futures-util (this crate persists; the example is deleted at the end of the task). The example connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market`, sends the subscribe message for 2–3 captured token ids, prints every raw frame for ~60 seconds, then exits:

```rust
// crates/ingestion/examples/ws_capture.rs (THROWAWAY — deleted in step 6)
use futures_util::{SinkExt, StreamExt};

#[tokio::main]
async fn main() {
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("connect");
    let assets: Vec<String> = std::env::args().skip(1).collect();
    let sub = serde_json::json!({ "type": "market", "assets_ids": assets }).to_string();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(sub.into())).await.expect("send");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(msg))) => println!("{msg}"),
            Ok(Some(Err(e))) => { eprintln!("ws error: {e}"); break; }
            Ok(None) => break,
            Err(_) => println!("(no frame in 5s)"),
        }
    }
}
```

(`expect` is fine here: examples are dev-only; add `#![allow(clippy::expect_used)]` at the top.) Run it:

```bash
cargo run -p pm-ingestion --example ws_capture -- <TOKEN_ID_1> <TOKEN_ID_2> | tee /tmp/ws_frames.txt
```

From `/tmp/ws_frames.txt` extract into fixtures: one full `book` event (`crates/registry/tests/fixtures/ws_book.json`), one `price_change` event (`ws_price_change.json`), and one of each other event type observed (`ws_tick_size_change.json`, `ws_last_trade_price.json` — if not observed in 60s, note that in RECON.md and capture what did appear).

- [ ] **Step 4: Write `docs/recon/RECON.md`** documenting, with evidence:
  - Exact endpoints used and base URLs; pagination mechanics for CLOB `/markets` (cursor field names, terminal cursor value).
  - Field names/types for: Gamma market (esp. the CLOB token ids field and its encoding — historically a STRINGIFIED JSON array inside JSON — condition id, negRisk flag, closed/active), Gamma event→market nesting, CLOB market (tick size field name + values seen, fee fields and values, token pair structure), CLOB book (bids/asks level shape, hash, timestamp — string or number), WS event envelope (single object vs array of events per frame, event_type values, price_change inner shape incl. side encoding, hash presence per event type).
  - Decimal formats observed (max decimal places on price and size).
  - Any rate-limit response headers seen.
  - Tick sizes observed and whether 0.01/0.001 assumption (spec §4) holds; FLAG loudly if other tick sizes exist.
- [ ] **Step 5: Sanitize fixtures** — keep them small (≤ a few KB each: truncate long book level arrays to ~10 levels per side, keep 2–3 markets per list fixture), pretty-print, no secrets (there are none in public data).
- [ ] **Step 6: Delete the ws_capture example**, keep the `pm-ingestion` crate skeleton (Cargo.toml + empty lib.rs with `// modules land in later tasks`), add `crates/ingestion` and `crates/registry` to workspace members (create `crates/registry` skeleton too: Cargo.toml + lib.rs stub).

`crates/registry/Cargo.toml`:
```toml
[package]
name = "pm-registry"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dependencies]
pm-core.workspace = true
serde.workspace = true
serde_json = "1"
toml.workspace = true

[dev-dependencies]
proptest.workspace = true
```

`crates/ingestion/Cargo.toml`:
```toml
[package]
name = "pm-ingestion"
version.workspace = true
edition.workspace = true

[lints]
workspace = true

[dependencies]
pm-core.workspace = true
pm-registry = { path = "../registry" }
pm-config = { path = "../config" }
serde.workspace = true
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "sync", "net", "io-util"] }
tokio-tungstenite = { version = "0.24", features = ["rustls-tls-webpki-roots"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
futures-util = "0.3"
hdrhistogram = "7"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
proptest.workspace = true
```

Add `serde_json = "1"` etc. to `[workspace.dependencies]` where shared (serde_json used by both new crates — put serde_json, tokio, tracing in workspace.dependencies and reference with `.workspace = true`; keep this consistent).

- [ ] **Step 7: Verify + commit**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --workspace && cargo test --workspace
git add -A && git commit -m "feat(m2): live API recon — fixtures, RECON.md, crate skeletons"
```

Expected: builds; 90 tests still green; fixtures + RECON.md committed.

---

### Task 2: `pm-registry::intern` — venue-id interning

**Files:**
- Create: `crates/registry/src/intern.rs`; Modify: `crates/registry/src/lib.rs`

Venue token ids are uint256 decimal strings (up to 78 digits); condition ids are 0x-hex strings. The hot path uses `TokenId(u64)`/`MarketId(u32)`/`EventId(u32)` handles (M1 contract). The intern table is the ONLY place venue strings live.

- [ ] **Step 1: Write the failing tests**

```rust
//! Venue-id interning: the only home of venue strings (spec §3 ids-are-handles).

use std::collections::HashMap;

use pm_core::instrument::{EventId, MarketId, TokenId};

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn intern_is_idempotent_and_dense() {
        let mut it = Interner::default();
        let a = it.token("11015470973684177829729219287262166995141465048508201953575582100565462316560");
        let b = it.token("4");
        let a2 = it.token("11015470973684177829729219287262166995141465048508201953575582100565462316560");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(a, TokenId(0));
        assert_eq!(b, TokenId(1));
        assert_eq!(it.token_str(a).unwrap(), "11015470973684177829729219287262166995141465048508201953575582100565462316560");
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
    }
}
```

- [ ] **Step 2: Run (compile fail), implement**

```rust
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
    pub fn token(&mut self, venue_id: &str) -> TokenId {
        if let Some(&t) = self.token_idx.get(venue_id) {
            return t;
        }
        let t = TokenId(self.tokens.len() as u64);
        self.tokens.push(venue_id.into());
        self.token_idx.insert(venue_id.into(), t);
        t
    }
    pub fn find_token(&self, venue_id: &str) -> Option<TokenId> {
        self.token_idx.get(venue_id).copied()
    }
    pub fn token_str(&self, t: TokenId) -> Option<&str> {
        self.tokens.get(usize::try_from(t.0).ok()?).map(AsRef::as_ref)
    }
    // market()/find_market()/market_str() and event()/find_event()/event_str()
    // follow the identical pattern with MarketId(u32)/EventId(u32) and
    // `self.markets.len() as u32`.
}
```

Write the market/event methods out in full (mirror the token trio exactly).

- [ ] **Step 3: Green + commit**

```bash
cargo test -p pm-registry
git add -A && git commit -m "feat(registry): venue-id interning tables"
```

---

### Task 3: `pm-registry::gamma` — metadata models against fixtures

**Files:**
- Create: `crates/registry/src/gamma.rs`; Modify: lib.rs

Parse the Task-1 fixtures into typed models. FIXTURE RECONCILIATION RULE APPLIES: field names below are templates; the fixtures win.

- [ ] **Step 1: Write the failing tests** (these load the committed fixtures):

```rust
//! Gamma / CLOB metadata models. Shapes are fixture-verified (Task 1).

use serde::Deserialize;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("tests/fixtures/{name}")).unwrap()
    }

    #[test]
    fn parses_gamma_markets_fixture() {
        let markets: Vec<GammaMarket> = serde_json::from_str(&fixture("gamma_markets.json")).unwrap();
        assert!(!markets.is_empty());
        for m in &markets {
            assert!(!m.condition_id.is_empty());
            let toks = m.clob_token_ids().unwrap();
            assert_eq!(toks.len(), 2, "binary market must have YES and NO token ids");
            assert!(toks.iter().all(|t| t.chars().all(|c| c.is_ascii_digit())));
        }
    }

    #[test]
    fn parses_gamma_events_fixture() {
        let events: Vec<GammaEvent> = serde_json::from_str(&fixture("gamma_events.json")).unwrap();
        assert!(!events.is_empty());
        assert!(events.iter().any(|e| !e.markets.is_empty()));
    }

    #[test]
    fn parses_clob_markets_fixture() {
        let page: ClobMarketsPage = serde_json::from_str(&fixture("clob_markets.json")).unwrap();
        assert!(!page.data.is_empty());
        for m in &page.data {
            assert!(m.minimum_tick_size == "0.01" || m.minimum_tick_size == "0.001",
                "unexpected tick size {} — spec §4 assumption violated, STOP and report",
                m.minimum_tick_size);
            assert_eq!(m.tokens.len(), 2);
        }
    }

    #[test]
    fn parses_clob_book_fixture() {
        let book: ClobBook = serde_json::from_str(&fixture("clob_book.json")).unwrap();
        assert!(!book.bids.is_empty() || !book.asks.is_empty());
        assert!(!book.hash.is_empty());
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        // Venue adds fields freely; models must be open (no deny_unknown_fields).
        let m: GammaMarket = serde_json::from_str(
            r#"{"conditionId":"0xa","clobTokenIds":"[\"1\",\"2\"]","negRisk":false,
                "active":true,"closed":false,"some_future_field":42}"#,
        ).unwrap();
        assert_eq!(m.clob_token_ids().unwrap(), vec!["1".to_string(), "2".to_string()]);
    }
}
```

- [ ] **Step 2: Implement** (template — reconcile with fixtures):

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    pub condition_id: String,
    /// Venue quirk: a STRINGIFIED JSON array of two uint256 decimal strings.
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub question: Option<String>,
}

impl GammaMarket {
    pub fn clob_token_ids(&self) -> Result<Vec<String>, GammaError> {
        let raw = self.clob_token_ids.as_deref().ok_or(GammaError::MissingTokenIds)?;
        serde_json::from_str(raw).map_err(|_| GammaError::MalformedTokenIds)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaEvent {
    pub id: String,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    pub markets: Vec<GammaMarket>,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClobMarketsPage {
    pub data: Vec<ClobMarket>,
    #[serde(default)]
    pub next_cursor: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClobMarket {
    pub condition_id: String,
    pub minimum_tick_size: String,
    #[serde(default)]
    pub neg_risk: bool,
    pub tokens: Vec<ClobToken>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClobToken {
    pub token_id: String,
    #[serde(default)]
    pub outcome: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClobBook {
    pub asset_id: String,
    pub hash: String,
    pub bids: Vec<ClobLevel>,
    pub asks: Vec<ClobLevel>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClobLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GammaError {
    MissingTokenIds,
    MalformedTokenIds,
}
```

NOTE: if the CLOB fixture shows `minimum_tick_size` as a NUMBER not a string, model it with a custom deserializer into a string (or f64-rejecting exact reader) and document in the report. Tick-size→`TickSize` mapping happens in Task 11 (sync), not here.

- [ ] **Step 3: Green + commit** (`cargo test -p pm-registry`).

```bash
git add -A && git commit -m "feat(registry): fixture-verified Gamma/CLOB metadata models"
```

---

### Task 4: `pm-registry::partitions` — NegRisk partitions + exhaustiveness

**Files:**
- Create: `crates/registry/src/partitions.rs`; Modify: lib.rs

Spec §8/§9: a NegRisk event's member markets form a candidate `Partition`. `verified_exhaustive` only when conservative checks pass; exclusion reasons are recorded (probe logs them).

- [ ] **Step 1: Write the failing tests**

```rust
//! NegRisk partition derivation + conservative exhaustiveness verification.

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
            assert!(reasons.contains(&ExclusionReason::PlaceholderOutcome));
        }
    }

    #[test]
    fn inactive_or_closed_members_block_verification() {
        let mut m = member(1, "B?");
        m.closed = true;
        let members = vec![member(0, "A?"), m];
        let (p, reasons) = derive_partition(EventId(7), true, &members);
        assert!(!p.verified_exhaustive);
        assert!(reasons.contains(&ExclusionReason::InactiveMember));
    }

    #[test]
    fn fewer_than_two_members_block_verification() {
        let (p, reasons) = derive_partition(EventId(7), true, &[member(0, "A?")]);
        assert!(!p.verified_exhaustive);
        assert!(!p.is_well_formed());
        assert!(reasons.contains(&ExclusionReason::TooFewMembers));
    }
}
```

- [ ] **Step 2: Implement**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionReason {
    NotNegRisk,
    PlaceholderOutcome,
    InactiveMember,
    TooFewMembers,
}

const PLACEHOLDER_MARKERS: &[&str] = &["other", "another", "none of the above", "any other"];

/// Derive a Partition from one event's member markets. `verified_exhaustive`
/// is set ONLY when every conservative check passes; every failed check is
/// returned so the probe can log why a set was excluded (spec §8).
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
    if members.iter().any(|m| !m.active || m.closed) {
        reasons.push(ExclusionReason::InactiveMember);
    }
    if members.iter().any(|m| {
        m.question
            .as_deref()
            .map(str::to_lowercase)
            .is_some_and(|q| PLACEHOLDER_MARKERS.iter().any(|p| q.contains(p)))
    }) {
        reasons.push(ExclusionReason::PlaceholderOutcome);
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
```

- [ ] **Step 3: Green + commit** — `feat(registry): NegRisk partition derivation with conservative verification`.

---

### Task 5: `pm-registry::relationships` + `components`

**Files:**
- Create: `crates/registry/src/relationships.rs`, `crates/registry/src/components.rs`; Modify: lib.rs

Spec §9 + the registry-load validation amendment: parse the TOML relationship file; only `status = "approved"` entries become `Relationship` values; symmetric kinds canonicalize `a ≤ b`; self-referential and duplicate entries are rejected with errors; unknown condition ids resolve to None and are reported (not fatal — market may not be tracked). Components: union-find over MarketIds joining partition members and approved-relationship endpoints.

- [ ] **Step 1: Write the failing tests**

```rust
// relationships.rs tests
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::instrument::{MarketId, Relationship};

    fn resolver(pairs: &[(&str, u32)]) -> impl Fn(&str) -> Option<MarketId> + '_ {
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

// components.rs tests
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
}
```

- [ ] **Step 2: Implement**

relationships.rs:

```rust
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

pub fn load_relationships(
    toml_src: &str,
    resolve: &impl Fn(&str) -> Option<MarketId>,
) -> Result<LoadedRelationships, RelationshipError> {
    let file: RelFile = toml::from_str(toml_src).map_err(|e| RelationshipError::Parse(e.to_string()))?;
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
            other => return Err(RelationshipError::UnknownKind(other.to_string())),
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
```

(`Relationship` derives Hash? It derives `Clone, Copy, PartialEq, Eq, Debug` — ADD `Hash` to its derive list in `pm-core::instrument` as part of this task; one-line change, note it.)

components.rs:

```rust
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
        let (ra, rb) = (self.find(a.0), self.find(b.0));
        if ra != rb {
            self.parent[rb as usize] = ra;
        }
    }

    pub fn component_of(&self, m: MarketId) -> ComponentId {
        ComponentId(self.find(m.0))
    }

    pub fn members(&self, c: ComponentId) -> Vec<MarketId> {
        (0..self.parent.len() as u32)
            .filter(|&i| self.find(i) == c.0)
            .map(MarketId)
            .collect()
    }
}
```

(Out-of-range MarketId would panic on index — acceptable: registry constructs both; add `debug_assert!((m.0 as usize) < self.parent.len())`.)

- [ ] **Step 3: Green + commit** — `feat(registry): relationship TOML with §9 validation; market components`.

---

### Task 6: `pm-registry::lib` — the Registry aggregate

**Files:**
- Modify: `crates/registry/src/lib.rs` (assemble), plus module exports

- [ ] **Step 1: Write the failing tests**

```rust
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
}
```

- [ ] **Step 2: Implement** — `RegistryBuilder` (wraps Interner; `add_market(condition_id, yes_venue_id, no_venue_id, tick, fee_bps_i32, neg_risk, question, active, closed, event_key: Option<&str>)`; `finish(relationship_toml) -> Result<Registry, RegistryError>` derives partitions per event via Task 4, loads relationships via Task 5 with a resolver over the intern table, builds Components). `Registry` (immutable): `markets() -> &[Market]`, `market_by_condition(&str)`, `market_of_token(TokenId)`, `tick_of(TokenId)`, `fee_of(TokenId)`, `partitions() -> &[Partition]`, `approved_relationships() -> &[Relationship]`, `component_of(MarketId) -> ComponentId`, `component_members(ComponentId)`, `all_tokens() -> Vec<TokenId>`, `token_venue_id(TokenId) -> Option<&str>`, plus `exclusion_log() -> &[(EventId, ExclusionReason)]`. Keep `Registry` `Send + Sync` (plain owned data) — it is published as `Arc<Registry>` via `tokio::sync::watch` from ingestion's sync task (the watch lives in ingestion; registry stays I/O-free).

- [ ] **Step 3: Green + commit** — `feat(registry): immutable Registry aggregate with builder`.

---

### Task 7: `pm-ingestion::decimal` — exact string → integer parsing

**Files:**
- Create: `crates/ingestion/src/decimal.rs`; Modify: `crates/ingestion/src/lib.rs` (`pub mod decimal;`)

No f64 anywhere in the money path: venue decimal strings parse straight to µ integers. Six decimal places max (µ resolution); reject malformed, signed, overlong, overflowing.

- [ ] **Step 1: Write the failing tests**

```rust
//! Exact decimal-string parsing. "0.46" → 460_000 µ. Never touches f64.

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn golden_values() {
        assert_eq!(parse_micro("0.46").unwrap(), 460_000);
        assert_eq!(parse_micro("0.001").unwrap(), 1_000);
        assert_eq!(parse_micro("1").unwrap(), 1_000_000);
        assert_eq!(parse_micro("0").unwrap(), 0);
        assert_eq!(parse_micro("12.5").unwrap(), 12_500_000);
        assert_eq!(parse_micro("0.000001").unwrap(), 1);
        assert_eq!(parse_micro("123456.654321").unwrap(), 123_456_654_321);
        assert_eq!(parse_micro(".5").unwrap(), 500_000); // venue sometimes omits leading zero
        assert_eq!(parse_micro("7.").unwrap(), 7_000_000);
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", ".", "-1", "+1", "1e3", "0.0000001", "1.2.3", "abc", "0x10", " 1", "1 "] {
            assert!(parse_micro(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_micro("18446744073709551616").is_err()); // > u64::MAX integer part scaled
        assert!(parse_micro("99999999999999999999.0").is_err());
    }

    proptest! {
        #[test]
        fn roundtrips_canonical(micro in 0u64..1_000_000_000_000) {
            let int = micro / 1_000_000;
            let frac = micro % 1_000_000;
            let s = if frac == 0 { format!("{int}") } else {
                let f = format!("{frac:06}");
                format!("{int}.{}", f.trim_end_matches('0'))
            };
            prop_assert_eq!(parse_micro(&s).unwrap(), micro);
        }
    }
}
```

- [ ] **Step 2: Implement**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalError {
    Empty,
    BadChar,
    TooManyDecimals,
    Overflow,
}

/// Parse a non-negative decimal string to µ units (×10⁶), exactly.
pub fn parse_micro(s: &str) -> Result<u64, DecimalError> {
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(DecimalError::Empty);
    }
    if frac_part.len() > 6 {
        return Err(DecimalError::TooManyDecimals);
    }
    let mut value: u64 = 0;
    if !int_part.is_empty() {
        if !int_part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(DecimalError::BadChar);
        }
        for b in int_part.bytes() {
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(b - b'0')))
                .ok_or(DecimalError::Overflow)?;
        }
    }
    value = value.checked_mul(1_000_000).ok_or(DecimalError::Overflow)?;
    if !frac_part.is_empty() {
        if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(DecimalError::BadChar);
        }
        let mut frac: u64 = 0;
        for b in frac_part.bytes() {
            frac = frac * 10 + u64::from(b - b'0'); // ≤ 6 digits: cannot overflow
        }
        frac *= 10u64.pow(6 - frac_part.len() as u32);
        value = value.checked_add(frac).ok_or(DecimalError::Overflow)?;
    }
    Ok(value)
}
```

- [ ] **Step 3: Green + commit** — `feat(ingestion): exact decimal parsing, no f64 in the money path`.

---

### Task 8: `pm-ingestion::livebook` + `shard`

**Files:**
- Create: `crates/ingestion/src/livebook.rs`, `crates/ingestion/src/shard.rs`; Modify: lib.rs

Spec §5's deferred fields land here: `LiveBook` wraps `pm_core::Book` with `last_update`, venue `hash`, and validity. `Shard` owns `HashMap<TokenId, LiveBook>` (single writer). Apply outcomes drive resnapshots (spec §19): off-tick prices are counted and the level skipped; persistent failures, crossed books, or unknown semantics → `NeedsResnapshot`.

- [ ] **Step 1: Write the failing tests** (livebook.rs; representative — write all of these)

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::TickSize;
    use std::time::{Duration, Instant};

    fn lvl(p: &str, s: &str) -> RawLevel {
        RawLevel { price_micro: crate::decimal::parse_micro(p).unwrap(), size_micro: crate::decimal::parse_micro(s).unwrap() }
    }

    fn snapshot(now: Instant) -> LiveBook {
        let mut lb = LiveBook::new(TickSize::Cent);
        let out = lb.apply_snapshot(
            now,
            &[lvl("0.44", "100"), lvl("0.43", "50")],
            &[lvl("0.46", "80"), lvl("0.47", "20")],
            "hash-1",
        );
        assert_eq!(out, ApplyOutcome::Ok);
        lb
    }

    #[test]
    fn snapshot_replaces_and_stamps() {
        let t0 = Instant::now();
        let lb = snapshot(t0);
        assert!(lb.valid());
        assert_eq!(lb.book().bids.best().unwrap().get(), 44);
        assert_eq!(lb.book().asks.best().unwrap().get(), 46);
        assert!(!lb.is_stale(t0 + Duration::from_millis(100), Duration::from_millis(1500)));
        assert!(lb.is_stale(t0 + Duration::from_millis(2000), Duration::from_millis(1500)));
    }

    #[test]
    fn delta_updates_levels_and_hash() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        let out = lb.apply_changes(
            t0 + Duration::from_millis(10),
            &[RawChange { side_buy: true, price_micro: 440_000, size_micro: 0 }],
            Some("hash-2"),
        );
        assert_eq!(out, ApplyOutcome::Ok);
        assert_eq!(lb.book().bids.best().unwrap().get(), 43);
        assert_eq!(lb.hash(), Some("hash-2"));
    }

    #[test]
    fn off_tick_price_is_counted_and_skipped() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        // 0.445 is off-tick on a Cent market
        let out = lb.apply_changes(t0, &[RawChange { side_buy: true, price_micro: 445_000, size_micro: 5_000_000 }], None);
        assert_eq!(out, ApplyOutcome::Ok);
        assert_eq!(lb.off_tick_count(), 1);
        assert_eq!(lb.book().bids.best().unwrap().get(), 44); // unchanged
    }

    #[test]
    fn persistent_off_tick_demands_resnapshot() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        let bad = RawChange { side_buy: true, price_micro: 445_000, size_micro: 5_000_000 };
        for _ in 0..OFF_TICK_RESNAPSHOT_THRESHOLD - 1 {
            assert_eq!(lb.apply_changes(t0, &[bad], None), ApplyOutcome::Ok);
        }
        assert_eq!(lb.apply_changes(t0, &[bad], None), ApplyOutcome::NeedsResnapshot(ResnapshotReason::PersistentOffTick));
    }

    #[test]
    fn crossed_book_demands_resnapshot_and_invalidates() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        let out = lb.apply_changes(
            t0,
            &[RawChange { side_buy: true, price_micro: 470_000, size_micro: 5_000_000 }],
            None,
        );
        assert_eq!(out, ApplyOutcome::NeedsResnapshot(ResnapshotReason::CrossedBook));
        assert!(!lb.valid());
        // a fresh snapshot restores validity
        let out = lb.apply_snapshot(t0, &[lvl("0.44", "10")], &[lvl("0.46", "10")], "hash-3");
        assert_eq!(out, ApplyOutcome::Ok);
        assert!(lb.valid());
    }

    #[test]
    fn price_at_or_beyond_bounds_is_off_tick() {
        let t0 = Instant::now();
        let mut lb = snapshot(t0);
        for p in [0u64, 1_000_000, 1_010_000] {
            let out = lb.apply_changes(t0, &[RawChange { side_buy: false, price_micro: p, size_micro: 1 }], None);
            assert!(matches!(out, ApplyOutcome::Ok | ApplyOutcome::NeedsResnapshot(_)));
        }
        assert_eq!(lb.off_tick_count(), 3);
    }
}
```

shard.rs tests: route by token (two books in one shard stay independent); `stale_tokens(now, staleness)` lists only stale ones; `mark_all_stale()` (used on WS reconnect) invalidates everything; stats counters increment (`applied_deltas`, `snapshots`, `off_tick`, `resnapshots_requested`).

- [ ] **Step 2: Implement**

livebook.rs:

```rust
use pm_core::book::{Book, Side};
use pm_core::num::{Px, Qty, TickSize};
use std::time::{Duration, Instant};

pub struct RawLevel { pub price_micro: u64, pub size_micro: u64 }
pub struct RawChange { pub side_buy: bool, pub price_micro: u64, pub size_micro: u64 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResnapshotReason { CrossedBook, PersistentOffTick }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome { Ok, NeedsResnapshot(ResnapshotReason) }

pub const OFF_TICK_RESNAPSHOT_THRESHOLD: u32 = 16;

pub struct LiveBook {
    book: Book,
    ts: TickSize,
    last_update: Option<Instant>,
    hash: Option<Box<str>>,
    valid: bool,
    off_tick: u32,
}
```

Key mechanics:
- `price_to_px(&self, micro) -> Option<Px>`: `micro % unit == 0` and within `(0, 1e6)` exclusive → `Px::new((micro / unit) as u16, ts).ok()`; else None (off-tick).
- `apply_snapshot`: rebuild `book` fresh (`Book::new(ts)` then set each level; off-tick levels in a SNAPSHOT are counted but skipped too), set hash/stamp/valid=true, reset `off_tick` to 0.
- `apply_changes`: for each change: off-tick → `off_tick += 1`, skip; else `book.apply(side, px, Qty(size))`. After applying: if `off_tick >= THRESHOLD` → invalidate + `NeedsResnapshot(PersistentOffTick)`. If best_bid ≥ best_ask (both present) → invalidate + `NeedsResnapshot(CrossedBook)`. Else stamp `last_update`, update hash if `Some`.
- `is_stale(now, window)`: `!valid || last_update.map_or(true, |t| now.duration_since(t) >= window)`.
- Size strings are micro-shares directly (venue sizes are in shares: size_micro from `parse_micro` = µshares ✓).

shard.rs:

```rust
pub struct ShardStats { pub snapshots: u64, pub deltas: u64, pub off_tick: u64, pub resnapshots_requested: u64 }

pub struct Shard {
    books: HashMap<TokenId, LiveBook>,
    stats: ShardStats,
}
```
Methods: `ensure_book(token, tick)`, `apply_snapshot(now, token, ...) -> ApplyOutcome`, `apply_changes(now, token, ...) -> ApplyOutcome` (unknown token → create-on-snapshot only; changes for unknown token → request snapshot), `is_stale`, `stale_tokens(now, window) -> Vec<TokenId>`, `mark_all_stale()`, `book(token) -> Option<&LiveBook>`, `stats()`.

- [ ] **Step 3: Green + commit** — `feat(ingestion): LiveBook integrity/staleness and single-writer shards`.

---

### Task 9: `pm-ingestion::rest` — CLOB REST client + rate limiter

**Files:**
- Create: `crates/ingestion/src/rest.rs`; Modify: lib.rs

Two halves: (a) a PURE token-bucket rate limiter (clock injected, fully tested), (b) a thin reqwest client whose RESPONSE PARSING is pure and fixture-tested; the HTTP plumbing itself stays ~10 lines per endpoint and is exercised by the probe, not unit tests.

- [ ] **Step 1: Write the failing tests** (rate limiter + parsing)

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn bucket_allows_burst_then_throttles() {
        let mut tb = TokenBucket::new(5, 10.0); // cap 5, 10 tokens/sec
        let t0 = Instant::now();
        for _ in 0..5 {
            assert_eq!(tb.try_acquire(t0), Ready::Now);
        }
        match tb.try_acquire(t0) {
            Ready::After(d) => assert!(d > Duration::ZERO && d <= Duration::from_millis(100)),
            Ready::Now => panic!("bucket should be empty"),
        }
        // refill after 100ms → one token
        assert_eq!(tb.try_acquire(t0 + Duration::from_millis(100)), Ready::Now);
    }

    #[test]
    fn bucket_caps_refill() {
        let mut tb = TokenBucket::new(2, 1000.0);
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(60);
        assert_eq!(tb.try_acquire(later), Ready::Now);
        assert_eq!(tb.try_acquire(later), Ready::Now);
        assert!(matches!(tb.try_acquire(later), Ready::After(_)));
    }

    #[test]
    fn book_response_parses_to_raw_levels() {
        let raw = std::fs::read_to_string("../registry/tests/fixtures/clob_book.json").unwrap();
        let parsed = parse_book_response(&raw).unwrap();
        assert!(!parsed.bids.is_empty() || !parsed.asks.is_empty());
        assert!(!parsed.hash.is_empty());
        // levels are exact micro integers
        for l in parsed.bids.iter().chain(parsed.asks.iter()) {
            assert!(l.price_micro < 1_000_000);
            assert!(l.size_micro > 0);
        }
    }
}
```

- [ ] **Step 2: Implement**

```rust
#[derive(Debug, PartialEq, Eq)]
pub enum Ready { Now, After(Duration) }

/// Deterministic token bucket (clock injected).
pub struct TokenBucket {
    capacity: u32,
    tokens: f64,
    rate_per_sec: f64,
    last: Option<Instant>,
}

impl TokenBucket {
    pub fn new(capacity: u32, rate_per_sec: f64) -> Self {
        TokenBucket { capacity, tokens: f64::from(capacity), rate_per_sec, last: None }
    }
    pub fn try_acquire(&mut self, now: Instant) -> Ready {
        if let Some(last) = self.last {
            let dt = now.saturating_duration_since(last).as_secs_f64();
            self.tokens = (self.tokens + dt * self.rate_per_sec).min(f64::from(self.capacity));
        }
        self.last = Some(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ready::Now
        } else {
            let need = 1.0 - self.tokens;
            Ready::After(Duration::from_secs_f64(need / self.rate_per_sec))
        }
    }
}

pub struct ParsedBook {
    pub asset_id: String,
    pub hash: String,
    pub bids: Vec<crate::livebook::RawLevel>,
    pub asks: Vec<crate::livebook::RawLevel>,
}

/// Pure: response body → exact-integer levels (via pm_registry::gamma::ClobBook).
pub fn parse_book_response(body: &str) -> Result<ParsedBook, IngestError> { /* serde → ClobBook → parse_micro each level; off-range prices are NOT filtered here (livebook owns tick policy) */ }

pub struct ClobRest {
    http: reqwest::Client,
    base: String,
    bucket: TokenBucket,
}

impl ClobRest {
    pub fn new(base: &str, capacity: u32, rate_per_sec: f64) -> Self { /* reqwest Client with 10s timeout */ }
    /// Awaits the bucket, GETs /book?token_id=, returns ParsedBook.
    pub async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, IngestError> { /* bucket → tokio::time::sleep on Ready::After → GET → parse_book_response */ }
    /// Paginated GET /markets walk until terminal cursor (per RECON.md mechanics).
    pub async fn all_markets(&mut self) -> Result<Vec<pm_registry::gamma::ClobMarket>, IngestError> { ... }
    pub async fn server_time(&mut self) -> Result<String, IngestError> { ... }
}
```

Write the async bodies in full (each ~10–15 lines; bucket-sleep loop is a small helper `acquire(&mut self)` using `tokio::time::sleep`). `IngestError` enum in lib.rs: `Http(String)`, `Parse(String)`, `Decimal(crate::decimal::DecimalError)`, `Ws(String)` — with Display + Error impls.

- [ ] **Step 3: Green + commit** — `feat(ingestion): CLOB REST client with deterministic rate limiting`.

---

### Task 10: `pm-ingestion::ws` — frame models + session handler

**Files:**
- Create: `crates/ingestion/src/ws.rs`; Modify: lib.rs

Frame parsing is pure and fixture-tested. The session handler is generic over a `WsTransport` trait so reconnect/subscribe/dispatch logic is tested with a scripted fake — tokio-tungstenite only appears in the one real impl. FIXTURE RECONCILIATION RULE APPLIES to every field name (notably: whether frames carry ONE event or an ARRAY of events — RECON.md decides; the parser below handles both defensively).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("../registry/tests/fixtures/{name}")).unwrap()
    }

    #[test]
    fn parses_book_event_fixture() {
        let evs = parse_frame(&fixture("ws_book.json")).unwrap();
        let WsEvent::Book(b) = &evs[0] else { panic!("expected Book, got {:?}", evs[0]) };
        assert!(!b.asset_id.is_empty());
        assert!(!b.hash.is_empty());
        assert!(!b.bids.is_empty() || !b.asks.is_empty());
    }

    #[test]
    fn parses_price_change_fixture() {
        let evs = parse_frame(&fixture("ws_price_change.json")).unwrap();
        let WsEvent::PriceChange(pc) = &evs[0] else { panic!("expected PriceChange") };
        assert!(!pc.asset_id.is_empty());
        assert!(!pc.changes.is_empty());
        for c in &pc.changes {
            assert!(c.price_micro < 1_000_000);
        }
    }

    #[test]
    fn unknown_event_types_are_tolerated() {
        let evs = parse_frame(r#"{"event_type":"sandwich_alert","asset_id":"1"}"#).unwrap();
        assert!(matches!(evs[0], WsEvent::Other));
    }

    #[test]
    fn array_and_single_object_frames_both_parse() {
        let single = r#"{"event_type":"sandwich_alert"}"#;
        let array = r#"[{"event_type":"sandwich_alert"},{"event_type":"sandwich_alert"}]"#;
        assert_eq!(parse_frame(single).unwrap().len(), 1);
        assert_eq!(parse_frame(array).unwrap().len(), 2);
    }

    #[test]
    fn subscribe_message_shape() {
        let msg = subscribe_message(&["111".into(), "222".into()]);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["type"], "market");
        assert_eq!(v["assets_ids"].as_array().unwrap().len(), 2);
    }
}
```

- [ ] **Step 2: Implement**

```rust
use serde::Deserialize;

#[derive(Debug)]
pub enum WsEvent {
    Book(BookEvent),
    PriceChange(PriceChangeEvent),
    TickSizeChange { asset_id: String, new_tick: String },
    Other,
}

#[derive(Debug, Deserialize)]
pub struct BookEvent {
    pub asset_id: String,
    pub hash: String,
    #[serde(default)]
    pub bids: Vec<pm_registry::gamma::ClobLevel>,
    #[serde(default)]
    pub asks: Vec<pm_registry::gamma::ClobLevel>,
}

#[derive(Debug)]
pub struct PriceChangeEvent {
    pub asset_id: String,
    pub hash: Option<String>,
    pub changes: Vec<ParsedChange>,
}

#[derive(Debug)]
pub struct ParsedChange {
    pub side_buy: bool,
    pub price_micro: u64,
    pub size_micro: u64,
}

/// One text frame → events. Handles both a single JSON object and an array
/// of objects (RECON.md documents which the venue actually sends; both are
/// accepted defensively). Unknown event types parse to `Other` — never an
/// error (spec §19: count and continue).
pub fn parse_frame(text: &str) -> Result<Vec<WsEvent>, IngestError> { /* serde_json::Value sniff: array → map each, object → one; dispatch on event_type; price levels through decimal::parse_micro; side string per RECON.md ("BUY"/"SELL" template) */ }

pub fn subscribe_message(asset_ids: &[String]) -> String {
    serde_json::json!({ "type": "market", "assets_ids": asset_ids }).to_string()
}

/// Transport abstraction: the real impl wraps tokio-tungstenite; tests script it.
pub trait WsTransport: Send {
    fn next_frame(&mut self) -> impl std::future::Future<Output = Option<Result<String, IngestError>>> + Send;
    fn send_text(&mut self, text: String) -> impl std::future::Future<Output = Result<(), IngestError>> + Send;
}

pub struct TungsteniteTransport { /* WebSocketStream<MaybeTlsStream<TcpStream>> */ }
impl TungsteniteTransport {
    pub async fn connect(url: &str) -> Result<Self, IngestError> { ... }
}
// impl WsTransport for TungsteniteTransport: text frames pass through; Ping →
// auto-Pong (tungstenite handles); Close/None → None; non-text frames skipped.
```

Write `parse_frame` and the tungstenite impl in full. If `event_type` field name differs in fixtures, reconcile.

- [ ] **Step 3: Green + commit** — `feat(ingestion): WS frame models and transport abstraction`.

---

### Task 11: `pm-ingestion::supervisor` — session loop, reconnect, resnapshot

**Files:**
- Create: `crates/ingestion/src/supervisor.rs`; Modify: lib.rs
- Test: `crates/ingestion/tests/replay.rs` (integration: full session over a fake transport)

The heart of M2's reliability story. One supervisor per WS connection (token set chunked by config). Responsibilities:
- subscribe on connect; route parsed events to the owning shard (by `asset_id` → TokenId via registry snapshot);
- on `NeedsResnapshot` outcomes or deltas-for-unknown-books: enqueue a REST resnapshot (deduped per token, rate-limited by the REST client's bucket);
- on transport end/error: mark ALL its tokens stale (`mark_all_stale`), reconnect with exponential backoff + full jitter (base 250ms, cap 30s), resubscribe, resnapshot everything it owns;
- staleness sweep each tick: tokens stale beyond `staleness_ms` get a resnapshot request (covers silent feeds);
- stats: frames, events, resnapshots, reconnects, parse errors (counted, sampled into tracing per spec §19).

Shard ownership model for M2: the supervisor OWNS its shard map directly (one `Shard` per supervisor; sharding across supervisors comes from chunking the token universe). The §12 multi-shard router belongs to M3's app wiring; this is documented in the module docs.

- [ ] **Step 1: Write the failing replay tests** (`tests/replay.rs`)

```rust
//! End-to-end session replay over a scripted transport — no network.
#![allow(clippy::unwrap_used)]

use pm_ingestion::supervisor::{Supervisor, SupervisorConfig, RestBookSource};
use pm_ingestion::ws::WsTransport;
use pm_ingestion::IngestError;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Scripted transport: pops pre-loaded frames; records sends; can be told to fail.
struct FakeTransport {
    incoming: VecDeque<Result<String, IngestError>>,
    sent: Arc<Mutex<Vec<String>>>,
}
// impl WsTransport for FakeTransport { next_frame pops front (None when empty); send_text records }

/// Scripted REST source: returns canned ParsedBook per token; records calls.
struct FakeRest { /* map venue_id → ParsedBook; Arc<Mutex<Vec<String>>> call log */ }
// impl RestBookSource for FakeRest

fn book_frame(asset: &str, bid: &str, ask: &str, hash: &str) -> String { /* build the ws_book.json shape with one level per side */ }
fn change_frame(asset: &str, side: &str, price: &str, size: &str) -> String { /* ws_price_change.json shape */ }

#[tokio::test]
async fn snapshot_then_deltas_builds_books() {
    // script: subscribe-ack implicit; book snapshot; two price changes; end.
    // assert: book state reflects deltas; no resnapshots requested; stats count 3 events.
}

#[tokio::test]
async fn crossed_book_triggers_rest_resnapshot() {
    // script: snapshot; delta that crosses the book; end.
    // assert: FakeRest called once for that token; book valid again afterward
    // (FakeRest returns a sane book); resnapshot stat == 1.
}

#[tokio::test]
async fn delta_for_unknown_token_requests_snapshot() {
    // script: price_change for a token never snapshotted.
    // assert: REST called for it; after the canned snapshot arrives the book exists.
}

#[tokio::test]
async fn transport_end_marks_stale_and_reconnects_with_resubscribe() {
    // Two-connection script: first transport ends after the snapshot; factory
    // hands out a second transport. assert: subscribe sent on BOTH transports;
    // all tokens resnapshotted after reconnect; reconnect stat == 1; books not
    // stale at the end.
}

#[tokio::test]
async fn silent_feed_goes_stale_and_sweep_resnapshots() {
    // script: snapshot then silence; drive the supervisor's sweep with a
    // mocked now (sweep_once(now) public hook). assert: token reported stale;
    // resnapshot requested by the sweep.
}
```

- [ ] **Step 2: Implement**

```rust
pub struct SupervisorConfig {
    pub staleness: Duration,
    pub backoff_base: Duration,   // 250ms
    pub backoff_cap: Duration,    // 30s
    pub sweep_interval: Duration, // 1s
}

/// REST dependency as a trait so replay tests can script it.
pub trait RestBookSource: Send {
    fn book(&mut self, venue_token_id: &str) -> impl Future<Output = Result<ParsedBook, IngestError>> + Send;
}

pub struct Supervisor<R: RestBookSource> {
    shard: Shard,
    tokens: Vec<(TokenId, Box<str>, TickSize)>, // handle, venue id, tick
    rest: R,
    cfg: SupervisorConfig,
    stats: SupStats,
}
```

Core loop (`run`, generic over a transport FACTORY `FnMut() -> Future<Output = Result<T, IngestError>>` so reconnect can mint a new transport):

```text
loop {
    transport = factory() (with backoff+jitter on failure: delay = min(cap, base·2^attempt) · rand(0.5..1.0); reset attempt counter on success)
    send subscribe_message(chunk venue ids)
    resnapshot_all()   // REST snapshot every owned token (initial + after reconnect)
    inner: loop {
        select! {
            frame = transport.next_frame() => match frame {
                None | Some(Err(_)) => { shard.mark_all_stale(); stats.reconnects += 1; break inner; }
                Some(Ok(text)) => for ev in parse_frame(&text) (parse error → count, continue):
                    route to shard.apply_*; on NeedsResnapshot or unknown-token → resnapshot(token)
            }
            _ = sweep tick => sweep_once(Instant::now())
        }
    }
}
```

`sweep_once(now)`: `shard.stale_tokens(now, cfg.staleness)` → resnapshot each (deduped: skip tokens already in-flight this sweep). Expose `sweep_once` and a `run_session(transport)` (single-connection inner loop) as `pub` so the replay tests drive them deterministically without the factory/backoff outer loop; the outer `run` composes them. Jitter via a tiny xorshift seeded from `Instant` — no rand dependency needed; document.

`resnapshot(token)`: REST book → `shard.apply_snapshot`; failure → count + leave stale (next sweep retries).

- [ ] **Step 3: Green + commit** — `feat(ingestion): WS supervisor with reconnect, staleness sweep, resnapshot`.

---

### Task 12: `pm-ingestion::sync` — Gamma sync → Registry publisher

**Files:**
- Create: `crates/ingestion/src/sync.rs`; Modify: lib.rs

Builds the `Registry` from live metadata and publishes `Arc<Registry>` on a `tokio::sync::watch` channel; a periodic resync rebuilds and re-publishes (registry is immutable; consumers grab the current Arc). Universe filtering happens here (spec §13).

- [ ] **Step 1: Write the failing tests** (pure parts: assembling a Registry from fixture-shaped inputs + universe filter)

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn assembles_registry_from_metadata() {
        let clob: pm_registry::gamma::ClobMarketsPage =
            serde_json::from_str(&std::fs::read_to_string("../registry/tests/fixtures/clob_markets.json").unwrap()).unwrap();
        let gamma: Vec<pm_registry::gamma::GammaEvent> =
            serde_json::from_str(&std::fs::read_to_string("../registry/tests/fixtures/gamma_events.json").unwrap()).unwrap();
        let reg = assemble_registry(&clob.data, &gamma, "", &UniverseFilter::default()).unwrap();
        assert!(!reg.markets().is_empty());
        assert_eq!(reg.all_tokens().len(), reg.markets().len() * 2);
    }

    #[test]
    fn universe_filter_caps_and_excludes_closed() {
        // synthetic ClobMarkets: 5 active + 1 closed; filter max_markets = 3
        // assert: 3 markets, closed one absent.
    }

    #[test]
    fn unknown_tick_size_is_skipped_with_reason() {
        // synthetic market with minimum_tick_size "0.1" → skipped, reason logged
        // (spec §4 supports only 0.01 / 0.001).
    }
}
```

- [ ] **Step 2: Implement**

```rust
pub struct UniverseFilter {
    pub max_markets: usize,        // default 200 for the probe
    pub require_active: bool,      // default true
}

/// Pure assembly: CLOB metadata (authoritative for tick/fee/tokens) joined
/// with Gamma events (authoritative for event grouping / NegRisk sets),
/// matched on condition id. Markets failing the filter or with unsupported
/// tick sizes are skipped with logged reasons.
pub fn assemble_registry(
    clob: &[ClobMarket],
    gamma_events: &[GammaEvent],
    relationship_toml: &str,
    filter: &UniverseFilter,
) -> Result<Registry, RegistryError> { /* RegistryBuilder loop: tick map "0.01"→Cent, "0.001"→Milli; fee from CLOB fields per RECON.md (default 0 if absent — NOT hardcoded: read the field; absence logged); event_key from gamma membership (condition id → event); finish(toml) */ }

pub struct SyncTask { /* gamma+clob fetch via ClobRest + reqwest for gamma; interval; watch::Sender<Arc<Registry>>; relationship file path + mtime tracking */ }
// run(): initial assemble + publish; loop every resync_interval: refetch,
// reassemble, publish; reload relationship file when mtime changes (hot
// reload, spec §9). New tokens vs previous registry → return a diff
// (added/removed tokens) so the caller (probe / M3 app) can adjust WS
// subscriptions; M2 probe logs the diff and keeps initial subscriptions.
```

Gamma fetch: `GET {gamma_base}/events?limit=…&active=true&closed=false` paginated per RECON.md (offset or cursor — whichever recon documented). Keep fetch functions thin; parsing already tested.

- [ ] **Step 3: Green + commit** — `feat(ingestion): Gamma sync, universe filter, registry watch publisher`.

---

### Task 13: `pm-config` — M2 sections

**Files:**
- Modify: `crates/config/src/lib.rs`

Add spec §18 sections consumed by M2 (defaults per spec; same deny_unknown_fields/default pattern as existing sections):

```rust
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Endpoints {
    pub gamma_base: String,   // "https://gamma-api.polymarket.com"
    pub clob_base: String,    // "https://clob.polymarket.com"
    pub ws_market_url: String,// "wss://ws-subscriptions-clob.polymarket.com/ws/market"
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Universe {
    pub max_markets: usize,        // 200
    pub require_active: bool,      // true
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Ingestion {
    pub staleness_ms: u64,         // 1500 (spec §5)
    pub ws_chunk_size: usize,      // 50 tokens per connection
    pub resync_interval_s: u64,    // 300
    pub sweep_interval_ms: u64,    // 1000
    pub rest_rate_capacity: u32,   // 10
    pub rest_rate_per_sec: f64,    // 5.0
    pub backoff_base_ms: u64,      // 250
    pub backoff_cap_ms: u64,       // 30_000
    pub relationships_path: String,// "relationships.toml"
}
```

Wire into `Config` (new fields `endpoints`, `universe`, `ingestion`), extend `validate()` (staleness_ms ≥ 100; ws_chunk_size ≥ 1; rates > 0; backoff base ≤ cap; URLs non-empty and scheme-prefixed), extend the defaults test, add a TOML-override test for one field per new section, and a validate-rejects test (zero rate, empty URL, base > cap).

- [ ] Implement TDD-style, then:

```bash
cargo test -p pm-config
git add -A && git commit -m "feat(config): endpoints/universe/ingestion sections for M2"
```

---

### Task 14: `pm-ingestion::stats` + the probe binary

**Files:**
- Create: `crates/ingestion/src/stats.rs`, `crates/ingestion/src/bin/probe.rs`; Modify: lib.rs

**stats.rs** — counters + latency histograms (spec §20 stages available in M2: ws-recv→parsed, parsed→applied):

```rust
pub struct StageHistos {
    pub recv_to_parsed: hdrhistogram::Histogram<u64>, // µs
    pub parsed_to_applied: hdrhistogram::Histogram<u64>,
}
pub struct ProbeStats { /* StageHistos + rolled-up supervisor stats + registry counts */ }
impl ProbeStats {
    pub fn record_recv_to_parsed(&mut self, us: u64) { /* saturating record */ }
    pub fn line(&self, uptime: Duration) -> String { /* one human line:
       books=NN stale=N msgs/s=NNN p50/p99 parse=Xµs/Yµs apply=Xµs/Yµs
       resnap=N reconn=N parse_err=N offtick=N */ }
    pub fn healthy(&self, tracked: usize) -> bool { /* stale fraction < 20% && parse_err rate < 1% */ }
}
```

Tests: recording + percentile extraction sanity; `healthy()` boundaries; `line()` contains the key fields (string-contains assertions, not exact format).

**probe.rs** — the M2 acceptance instrument (~150 lines):

```text
usage: probe [--config <path>] [--duration-secs N] [--max-markets N]

1. tracing_subscriber init (env-filter, default info).
2. Load Config (file if given, else defaults) + overrides from flags.
3. ClobRest::all_markets + gamma events fetch → assemble_registry
   (relationship file optional: missing file = no relationships, log it).
   Log: markets tracked, partitions (verified/unverified + exclusion
   reasons), components, tokens.
4. Spawn supervisors: chunk registry.all_tokens() venue ids by
   ws_chunk_size; each chunk gets a Supervisor with its own
   TungsteniteTransport factory against ws_market_url + a ClobRest handle.
5. Every 10s: print ProbeStats::line(); every resync_interval: SyncTask
   refresh (log universe diff; M2 keeps initial subscriptions).
6. On --duration-secs elapsed (or ctrl-c): final summary block + exit code
   0 if ProbeStats::healthy() else 2.
```

Implement with plain `std::env::args` parsing (clap lands with the M3 app). The probe is a `[[bin]]` in pm-ingestion: `src/bin/probe.rs`.

- [ ] Tests for stats.rs (pure); probe compiles (`cargo build -p pm-ingestion --bin probe`); commit — `feat(ingestion): stage stats and the live probe binary`.

---

### Task 15: Live verification, README, tag

**Files:**
- Modify: `README.md`; Create: `relationships.toml` (starter file with a commented example, all entries pending)

- [ ] **Step 1: Short live shakedown** (network):

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo run -p pm-ingestion --bin probe --release -- --duration-secs 120 --max-markets 50 2>&1 | tee /tmp/probe-shakedown.txt
```

Expect: registry sync logged (markets/partitions/components), stats lines flowing, exit 0. Debug anything broken NOW (this is the first full-system reality contact; fixture-tested parsers make surprises unlikely but reconcile any that appear and note them).

- [ ] **Step 2: The acceptance run** (longer):

```bash
cargo run -p pm-ingestion --bin probe --release -- --duration-secs 1800 --max-markets 200 2>&1 | tee /tmp/probe-30min.txt
```

30 minutes, 200 markets. Acceptance (spec §22 M2): exit code 0; stale fraction < 20% at every printed line after warmup; ≥ 1 successful resnapshot OR reconnect handled (if none occurred naturally, kill the network briefly mid-run — e.g. toggle wifi — OR accept and note that the path was proven by replay tests); parse-error rate < 1%; apply p99 < 200µs (spec §20 in-process row).

The full "hours-long" soak (spec exit wording) is documented in the README as a user-runnable command; the 30-minute run + replay-tested failure paths are the in-session evidence. State exactly this in the README — no overclaiming.

- [ ] **Step 3: README** — add an M2 section: crate layout additions, probe usage, the measured 30-minute results (books tracked, msg rates, p50/p99 parse/apply, resnapshots, reconnects), the soak command, and a `relationships.toml` pointer (how to approve an entry).

- [ ] **Step 4: Full verification + tag**

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --workspace
git add -A && git commit -m "feat(m2): probe acceptance run, README M2 section"
git tag m2-ingestion
```

Record the final test count in the README line you added. NO stray output files in the commit (check `git status` before `git add` — automation debris like `*_out.txt` is gitignored but verify).

---

## M2 completion criteria (spec §22 row 2)

- [ ] All 15 tasks committed on `feat/m2-ingestion`; tree clean; tag `m2-ingestion`.
- [ ] `cargo test --workspace` green (M1's 90 + new registry/ingestion/config tests; zero network in tests).
- [ ] `cargo clippy --all-targets -- -D warnings` clean; fmt clean.
- [ ] Committed fixtures + RECON.md document the real API shapes.
- [ ] Probe 30-minute live run: exit 0, healthy stats, results recorded in README; staleness/resync proven live or by replay + documented.
- [ ] Then: superpowers:finishing-a-development-branch (integration choice is the user's).

