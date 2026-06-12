//! Gamma keyset sync, universe assembly, and registry watch publisher.
//!
//! # Architecture
//!
//! [`assemble_registry`] is pure: given gamma events + CLOB market metadata +
//! a relationship TOML string it returns an [`AssembledUniverse`].  All I/O
//! lives in the async [`SyncTask`] shell.
//!
//! # NegRisk / yes-no token ordering
//!
//! For markets that have explicit "Yes"/"No" outcome labels (case-insensitive)
//! in the CLOB tokens list the yes-token is the one whose `outcome` matches
//! "yes", the no-token the one matching "no".
//!
//! For markets whose outcome labels are anything else (NegRisk member markets
//! label outcomes by candidate name — the venue's index order is the yes/no
//! convention per RECON §3), index 0 is yes and index 1 is no.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use pm_core::num::TickSize;
use pm_registry::gamma::{ClobMarket, GammaEvent};
use pm_registry::{RegistryBuilder, RegistryError};

use crate::IngestError;
use crate::rest::ClobRest;

// ---------------------------------------------------------------------------
// Universe filter
// ---------------------------------------------------------------------------

/// Parameters controlling which markets are included in the assembled universe.
pub struct UniverseFilter {
    /// Hard cap on the number of markets to add to the registry. Default: 200.
    pub max_markets: usize,
    /// When `true`, skip markets where CLOB reports `active == false` or
    /// `closed == true`. Default: `true`.
    pub require_active: bool,
}

impl Default for UniverseFilter {
    fn default() -> Self {
        UniverseFilter {
            max_markets: 200,
            require_active: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Skip reason
// ---------------------------------------------------------------------------

/// Why a market was excluded from the assembled universe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkippedReason {
    /// `minimum_tick_size` was not 0.01 or 0.001.
    UnsupportedTick,
    /// Gamma `conditionId` was empty.
    EmptyConditionId,
    /// Market is inactive or closed and `require_active` is `true`.
    InactiveOrClosed,
    /// CLOB tokens list had fewer than 2 entries or token_ids were empty.
    MissingTokens,
    /// CLOB lookup returned no record for this condition id.
    ClobLookupFailed,
}

// ---------------------------------------------------------------------------
// Assembled universe
// ---------------------------------------------------------------------------

/// Output of [`assemble_registry`]: the finished registry plus a log of
/// markets that were skipped and why.
pub struct AssembledUniverse {
    /// The constructed registry, ready for publication via `watch::Sender`.
    pub registry: Arc<pm_registry::Registry>,
    /// Markets that were skipped (condition id or event id) and the reason.
    pub skipped: Vec<(String, SkippedReason)>,
}

// ---------------------------------------------------------------------------
// Pure assembly
// ---------------------------------------------------------------------------

/// Classify `minimum_tick_size` into a supported [`TickSize`] or `None`.
///
/// Exact f64 comparison with 1 × 10⁻⁹ tolerance.
fn classify_tick(tick: f64) -> Option<TickSize> {
    if (tick - 0.01).abs() < 1e-9 {
        Some(TickSize::Cent)
    } else if (tick - 0.001).abs() < 1e-9 {
        Some(TickSize::Milli)
    } else {
        None
    }
}

/// Determine the yes/no venue token ids from a CLOB token list.
///
/// Strategy (per module doc-comment):
/// - If any token's `outcome` equals "yes" (case-insensitive), use that as
///   yes and find a "no" counterpart.
/// - Otherwise use index 0 as yes, index 1 as no.
///
/// Returns `None` if the tokens list has fewer than 2 entries or any
/// selected token has an empty `token_id`.
fn pick_yes_no(tokens: &[pm_registry::gamma::ClobToken]) -> Option<(String, String)> {
    if tokens.len() < 2 {
        return None;
    }

    let yes_id;
    let no_id;

    // Check whether the tokens carry explicit "Yes"/"No" labels.
    let yes_pos = tokens
        .iter()
        .position(|t| t.outcome.eq_ignore_ascii_case("yes"));
    let no_pos = tokens
        .iter()
        .position(|t| t.outcome.eq_ignore_ascii_case("no"));

    if let (Some(yi), Some(ni)) = (yes_pos, no_pos) {
        yes_id = tokens[yi].token_id.clone();
        no_id = tokens[ni].token_id.clone();
    } else {
        // Fall back to venue index order (NegRisk members, sports markets, etc.)
        yes_id = tokens[0].token_id.clone();
        no_id = tokens[1].token_id.clone();
    }

    if yes_id.is_empty() || no_id.is_empty() {
        return None;
    }

    Some((yes_id, no_id))
}

/// Per-market acceptance gate shared by [`assemble_registry`] and
/// [`fetch_clob_bounded`], so fetch bounding and assembly cannot drift.
///
/// `Ok` carries the parsed (tick, yes-token, no-token) triple assembly needs;
/// `Err` is the skip reason. [`SkippedReason::EmptyConditionId`] and
/// [`SkippedReason::ClobLookupFailed`] are decided by the callers — they
/// concern the gamma record and the lookup itself, not the CLOB data.
fn gate_market(
    clob: &ClobMarket,
    filter: &UniverseFilter,
) -> Result<(TickSize, String, String), SkippedReason> {
    if filter.require_active && (!clob.active || clob.closed) {
        return Err(SkippedReason::InactiveOrClosed);
    }
    let tick = classify_tick(clob.minimum_tick_size).ok_or(SkippedReason::UnsupportedTick)?;
    let (yes_id, no_id) = pick_yes_no(&clob.tokens).ok_or(SkippedReason::MissingTokens)?;
    Ok((tick, yes_id, no_id))
}

/// Assemble a [`Registry`] from gamma event groupings joined with CLOB market
/// metadata.
///
/// # Parameters
/// - `events`: gamma events in the order they should be iterated.
/// - `clob_by_condition`: CLOB market records keyed by `condition_id`.
/// - `relationship_toml`: raw TOML text for the relationship file (empty string
///   is valid — no relationships are loaded).
/// - `filter`: policy for which markets to include.
///
/// # Capping behaviour
///
/// Once `filter.max_markets` markets have been added the remaining gamma
/// markets are simply not visited — they do **not** produce skip entries.
pub fn assemble_registry(
    events: &[GammaEvent],
    clob_by_condition: &HashMap<String, ClobMarket>,
    relationship_toml: &str,
    filter: &UniverseFilter,
) -> Result<AssembledUniverse, RegistryError> {
    let mut builder = RegistryBuilder::default();
    let mut skipped: Vec<(String, SkippedReason)> = Vec::new();
    let mut count = 0usize;

    'outer: for event in events {
        for gm in &event.markets {
            if count >= filter.max_markets {
                break 'outer;
            }

            // ---- condition id check -----------------------------------------
            if gm.condition_id.is_empty() {
                skipped.push((gm.condition_id.clone(), SkippedReason::EmptyConditionId));
                continue;
            }

            // ---- CLOB lookup ------------------------------------------------
            let clob = match clob_by_condition.get(&gm.condition_id) {
                Some(c) => c,
                None => {
                    skipped.push((gm.condition_id.clone(), SkippedReason::ClobLookupFailed));
                    continue;
                }
            };

            // ---- shared gate: active/closed, tick size, tokens ---------------
            let (tick, yes_id, no_id) = match gate_market(clob, filter) {
                Ok(parsed) => parsed,
                Err(reason) => {
                    skipped.push((gm.condition_id.clone(), reason));
                    continue;
                }
            };

            // ---- fee (CLOB taker_base_fee is authoritative) ----------------
            let fee_bps = i32::try_from(clob.taker_base_fee).unwrap_or_else(|_| {
                if clob.taker_base_fee > i64::from(i32::MAX) {
                    i32::MAX
                } else {
                    i32::MIN
                }
            });

            // ---- negRisk (defensive or) ------------------------------------
            let neg_risk = clob.neg_risk || gm.neg_risk;

            // ---- event key (skip grouping when empty) ----------------------
            let event_key: Option<&str> = if event.id.is_empty() {
                None
            } else {
                Some(&event.id)
            };

            builder.add_market(
                &gm.condition_id,
                &yes_id,
                &no_id,
                tick,
                fee_bps,
                neg_risk,
                gm.question.clone(),
                clob.active,
                clob.closed,
                event_key,
            );

            count += 1;
        }
    }

    let registry = builder.finish(relationship_toml)?;
    Ok(AssembledUniverse {
        registry: Arc::new(registry),
        skipped,
    })
}

// ---------------------------------------------------------------------------
// Keyset envelope parser (pure, unit-tested)
// ---------------------------------------------------------------------------

/// Parse the Gamma `/events/keyset` response envelope.
///
/// The keyset endpoint wraps the array inside `{"events": [...], ...}` rather
/// than returning a bare array (unlike the deprecated `/events` endpoint).
/// This function is pure (serde only) and has a unit test that pins the shape.
pub fn events_keyset_envelope(body: &str) -> Result<Vec<GammaEvent>, IngestError> {
    #[derive(serde::Deserialize)]
    struct Envelope {
        events: Vec<GammaEvent>,
    }
    let env: Envelope =
        serde_json::from_str(body).map_err(|e| IngestError::Parse(e.to_string()))?;
    Ok(env.events)
}

// ---------------------------------------------------------------------------
// SyncTask (thin async shell — untested beyond the probe)
// ---------------------------------------------------------------------------

/// Thin async wrapper that drives gamma keyset pagination, per-condition CLOB
/// lookups, file-mtime-aware relationship reloads, and `watch::Sender` publication.
///
/// # Relationship file caching
///
/// The file contents are cached in `relationship_toml`.  [`sync_once`] calls
/// [`maybe_reload_relationships`] on every cycle: if the mtime has changed (or
/// this is the first call) the file is re-read and the cache is updated.  This
/// avoids redundant disk reads on cycles where the file has not changed.
///
/// [`sync_once`]: SyncTask::sync_once
/// [`maybe_reload_relationships`]: SyncTask::maybe_reload_relationships
pub struct SyncTask {
    clob: ClobRest,
    gamma_base: String,
    http: reqwest::Client,
    relationships_path: PathBuf,
    last_mtime: Option<SystemTime>,
    /// Cached contents of the relationships TOML file.  Populated on first call
    /// to `sync_once` and refreshed whenever the file's mtime changes.
    relationship_toml: String,
    filter: UniverseFilter,
    tx: tokio::sync::watch::Sender<Arc<pm_registry::Registry>>,
}

impl SyncTask {
    /// Create a new sync task.
    pub fn new(
        clob: ClobRest,
        gamma_base: &str,
        relationships_path: PathBuf,
        filter: UniverseFilter,
        tx: tokio::sync::watch::Sender<Arc<pm_registry::Registry>>,
    ) -> Result<Self, IngestError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| IngestError::Http(e.to_string()))?;
        Ok(SyncTask {
            clob,
            gamma_base: gamma_base.to_owned(),
            http,
            relationships_path,
            last_mtime: None,
            relationship_toml: String::new(),
            filter,
            tx,
        })
    }

    /// Walk the gamma `/events/keyset` endpoint collecting events until
    /// `filter.max_markets` worth of member markets have been seen, or
    /// `limit_pages` pages have been fetched (whichever comes first).
    ///
    /// The keyset cursor is threaded via the `next_cursor` field in the
    /// envelope.  An empty or absent cursor signals the final page.
    pub async fn fetch_gamma_events(
        &mut self,
        limit_pages: usize,
    ) -> Result<Vec<GammaEvent>, IngestError> {
        #[derive(serde::Deserialize)]
        struct KeysetEnvelope {
            events: Vec<GammaEvent>,
            #[serde(default)]
            next_cursor: String,
        }

        let mut cursor = String::new();
        let mut all_events: Vec<GammaEvent> = Vec::new();
        let mut market_count = 0usize;

        for _ in 0..limit_pages {
            let url = if cursor.is_empty() {
                format!(
                    "{}/events/keyset?limit=100&active=true&closed=false",
                    self.gamma_base
                )
            } else {
                format!(
                    "{}/events/keyset?limit=100&active=true&closed=false&next_cursor={}",
                    self.gamma_base, cursor
                )
            };

            let body = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| IngestError::Http(e.to_string()))?
                .error_for_status()
                .map_err(|e| IngestError::Http(e.to_string()))?
                .text()
                .await
                .map_err(|e| IngestError::Http(e.to_string()))?;

            let page: KeysetEnvelope =
                serde_json::from_str(&body).map_err(|e| IngestError::Parse(e.to_string()))?;

            for ev in &page.events {
                market_count += ev.markets.len();
            }
            all_events.extend(page.events);

            if page.next_cursor.is_empty() || market_count >= self.filter.max_markets {
                break;
            }
            cursor = page.next_cursor;
        }

        Ok(all_events)
    }

    /// Issue single-market CLOB lookups for the markets in `events`, bounded
    /// by `filter.max_markets` would-be-accepted markets (see
    /// [`fetch_clob_bounded`]), returning a map of condition id →
    /// [`ClobMarket`] plus any failures as `ClobLookupFailed` skip entries.
    pub async fn fetch_clob_for(
        &mut self,
        events: &[GammaEvent],
    ) -> (HashMap<String, ClobMarket>, Vec<(String, SkippedReason)>) {
        fetch_clob_bounded(&mut self.clob, events, &self.filter).await
    }

    /// Check whether the relationships file's mtime has changed and, if so,
    /// reload the file contents into `self.relationship_toml`.
    ///
    /// Returns `true` if the cache was refreshed (mtime changed or first call),
    /// `false` if the file is unchanged.
    ///
    /// On the first call `last_mtime` is `None`, so the file is always read
    /// regardless of its mtime.  On subsequent calls a re-read only occurs when
    /// the OS-reported mtime differs from the value stored in `last_mtime`.
    ///
    /// I/O errors during the re-read are logged at `info` level and the cached
    /// contents are reset to an empty string (equivalent to "no relationships").
    pub fn maybe_reload_relationships(&mut self) -> bool {
        let mtime = std::fs::metadata(&self.relationships_path)
            .and_then(|m| m.modified())
            .ok();
        if mtime != self.last_mtime {
            self.last_mtime = mtime;
            self.relationship_toml = match std::fs::read_to_string(&self.relationships_path) {
                Ok(s) => s,
                Err(e) => {
                    tracing::info!(
                        path = %self.relationships_path.display(),
                        error = %e,
                        "relationships file missing or unreadable; proceeding without relationships"
                    );
                    String::new()
                }
            };
            true
        } else {
            false
        }
    }

    /// Full sync cycle: fetch gamma events, fetch CLOB metadata, reload
    /// relationships if the file's mtime changed, assemble the registry, and
    /// publish via the watch sender.
    ///
    /// # Skip deduplication
    ///
    /// `fetch_clob_for` records a `ClobLookupFailed` entry for every condition
    /// id whose CLOB HTTP request fails.  Those conditions are simply absent from
    /// the resulting `clob_by_condition` map.  `assemble_registry` then
    /// independently emits its own `ClobLookupFailed` entry for every condition
    /// that is missing from that map.  The two sets are therefore identical: each
    /// failed condition appears exactly once in `universe.skipped` because
    /// `assemble_registry` is the sole recorder.  The failures vec returned by
    /// `fetch_clob_for` is intentionally discarded here to avoid double-counting.
    pub async fn sync_once(&mut self) -> Result<AssembledUniverse, IngestError> {
        let events = self.fetch_gamma_events(50).await?;
        // CLOB failures are dropped: assemble_registry will emit ClobLookupFailed
        // for each condition absent from the map, covering the same set exactly.
        let (clob_by_condition, _clob_failures) = self.fetch_clob_for(&events).await;

        // Reload relationships from disk only when mtime has changed (or first call).
        self.maybe_reload_relationships();

        let universe = assemble_registry(
            &events,
            &clob_by_condition,
            &self.relationship_toml,
            &self.filter,
        )
        .map_err(|e| IngestError::Parse(e.to_string()))?;

        let _ = self.tx.send(Arc::clone(&universe.registry));
        Ok(universe)
    }
}

// ---------------------------------------------------------------------------
// Bounded CLOB fetch
// ---------------------------------------------------------------------------

/// Fetch seam over the single-market CLOB lookup so the bounded fetch loop is
/// testable without a network client.
pub trait ClobFetch {
    /// Fetch a single CLOB market by condition id.
    fn market(
        &mut self,
        condition_id: &str,
    ) -> impl std::future::Future<Output = Result<ClobMarket, IngestError>>;
}

impl ClobFetch for ClobRest {
    async fn market(&mut self, condition_id: &str) -> Result<ClobMarket, IngestError> {
        ClobRest::market(self, condition_id).await
    }
}

/// Fetch CLOB metadata for the markets in `events`, visiting them in exactly
/// the order [`assemble_registry`] does and stopping once
/// `filter.max_markets` markets *would be accepted* by assembly.
///
/// Acceptance mirrors `assemble_registry`'s per-market gating (shared via
/// [`gate_market`]), so assembling from the bounded map yields a registry
/// identical to one assembled from an exhaustive fetch: assembly never visits
/// markets past its cap, and every market it does visit is fetched here.
///
/// This bound matters for startup latency: a single gamma keyset page can
/// carry 1000+ member markets, and the CLOB client is rate-limited — an
/// unbounded per-market fetch takes minutes when only `max_markets` (default
/// 200, smoke runs 20) are kept.
pub async fn fetch_clob_bounded<F: ClobFetch>(
    fetch: &mut F,
    events: &[GammaEvent],
    filter: &UniverseFilter,
) -> (HashMap<String, ClobMarket>, Vec<(String, SkippedReason)>) {
    let mut by_condition: HashMap<String, ClobMarket> = HashMap::new();
    let mut failures: Vec<(String, SkippedReason)> = Vec::new();
    let mut accepted = 0usize;

    'outer: for event in events {
        for gm in &event.markets {
            // Mirror assemble_registry's cap check: it breaks at the TOP of
            // the loop for the market AFTER the last accepted one, so nothing
            // past this point is ever visited by assembly either.
            if accepted >= filter.max_markets {
                break 'outer;
            }
            if gm.condition_id.is_empty() {
                continue; // assembly will record EmptyConditionId
            }
            if !by_condition.contains_key(&gm.condition_id) {
                match fetch.market(&gm.condition_id).await {
                    Ok(cm) => {
                        by_condition.insert(gm.condition_id.clone(), cm);
                    }
                    Err(e) => {
                        tracing::warn!(
                            condition_id = %gm.condition_id,
                            error = %e,
                            "CLOB single-market lookup failed; skipping"
                        );
                        failures.push((gm.condition_id.clone(), SkippedReason::ClobLookupFailed));
                        continue;
                    }
                }
            }
            // Count every visit assembly would accept — including a repeated
            // condition id (assembly does not dedup), which costs no refetch.
            if gate_market(&by_condition[&gm.condition_id], filter).is_ok() {
                accepted += 1;
            }
        }
    }

    (by_condition, failures)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_registry::gamma::{ClobMarket, ClobToken, GammaEvent};
    use std::collections::HashMap;

    // ---- helpers ------------------------------------------------------------

    /// Parameters for [`make_clob`].
    struct ClobSpec<'a> {
        condition_id: &'a str,
        tick: f64,
        taker_fee: i64,
        active: bool,
        closed: bool,
        neg_risk: bool,
        yes_token: &'a str,
        no_token: &'a str,
    }

    /// Build a minimal ClobMarket via serde for a binary market.
    fn make_clob(spec: ClobSpec<'_>) -> ClobMarket {
        let ClobSpec {
            condition_id,
            tick,
            taker_fee,
            active,
            closed,
            neg_risk,
            yes_token,
            no_token,
        } = spec;
        let json = format!(
            r#"{{
                "condition_id": {cid:?},
                "minimum_tick_size": {tick},
                "neg_risk": {neg_risk},
                "active": {active},
                "closed": {closed},
                "maker_base_fee": 0,
                "taker_base_fee": {taker_fee},
                "tokens": [
                    {{"token_id": {yes:?}, "outcome": "Yes", "price": 0.5, "winner": false}},
                    {{"token_id": {no:?},  "outcome": "No",  "price": 0.5, "winner": false}}
                ]
            }}"#,
            cid = condition_id,
            tick = tick,
            neg_risk = neg_risk,
            active = active,
            closed = closed,
            taker_fee = taker_fee,
            yes = yes_token,
            no = no_token,
        );
        serde_json::from_str(&json).unwrap()
    }

    /// Build a minimal GammaEvent containing one GammaMarket (serde construction).
    fn make_event(
        event_id: &str,
        condition_id: &str,
        yes_tok: &str,
        no_tok: &str,
        neg_risk: bool,
        active: bool,
    ) -> GammaEvent {
        let json = format!(
            r#"{{
                "id": {eid:?},
                "negRisk": {neg_risk},
                "markets": [{{
                    "conditionId": {cid:?},
                    "clobTokenIds": "[\"{yes}\", \"{no}\"]",
                    "negRisk": {neg_risk},
                    "active": {active},
                    "closed": false,
                    "question": "Test question?"
                }}]
            }}"#,
            eid = event_id,
            cid = condition_id,
            yes = yes_tok,
            no = no_tok,
            neg_risk = neg_risk,
            active = active,
        );
        serde_json::from_str(&json).unwrap()
    }

    /// Build a GammaEvent with multiple markets.
    fn make_event_multi(
        event_id: &str,
        neg_risk: bool,
        markets: &[(&str, &str, &str)], // (condition_id, yes_tok, no_tok)
    ) -> GammaEvent {
        let markets_json: Vec<String> = markets
            .iter()
            .map(|(cid, yes, no)| {
                format!(
                    r#"{{"conditionId": {cid:?}, "clobTokenIds": "[\"{yes}\", \"{no}\"]", "negRisk": {neg_risk}, "active": true, "closed": false, "question": "Q?"}}"#,
                    cid = cid,
                    yes = yes,
                    no = no,
                    neg_risk = neg_risk,
                )
            })
            .collect();
        let json = format!(
            r#"{{"id": {eid:?}, "negRisk": {neg_risk}, "markets": [{mlist}]}}"#,
            eid = event_id,
            neg_risk = neg_risk,
            mlist = markets_json.join(","),
        );
        serde_json::from_str(&json).unwrap()
    }

    // ---- Test 1: authoritative tick and fee ---------------------------------

    #[test]
    fn assembles_with_clob_authoritative_tick_and_fee() {
        // Event 1: single Cent market, taker fee 0
        let ev1 = make_event("ev1", "0xaaa", "ya", "na", false, true);
        // Event 2: two Milli negRisk markets, one with taker fee 200
        let ev2 = make_event_multi("ev2", true, &[("0xbbb", "yb", "nb"), ("0xccc", "yc", "nc")]);

        let mut clob = HashMap::new();
        clob.insert(
            "0xaaa".into(),
            make_clob(ClobSpec {
                condition_id: "0xaaa",
                tick: 0.01,
                taker_fee: 0,
                active: true,
                closed: false,
                neg_risk: false,
                yes_token: "ya",
                no_token: "na",
            }),
        );
        clob.insert(
            "0xbbb".into(),
            make_clob(ClobSpec {
                condition_id: "0xbbb",
                tick: 0.001,
                taker_fee: 200,
                active: true,
                closed: false,
                neg_risk: true,
                yes_token: "yb",
                no_token: "nb",
            }),
        );
        clob.insert(
            "0xccc".into(),
            make_clob(ClobSpec {
                condition_id: "0xccc",
                tick: 0.001,
                taker_fee: 0,
                active: true,
                closed: false,
                neg_risk: true,
                yes_token: "yc",
                no_token: "nc",
            }),
        );

        let events = vec![ev1, ev2];
        let universe = assemble_registry(&events, &clob, "", &UniverseFilter::default()).unwrap();
        let reg = &universe.registry;

        assert_eq!(reg.markets().len(), 3);
        assert!(universe.skipped.is_empty());

        let ma = reg.market_by_condition("0xaaa").unwrap();
        assert_eq!(ma.tick, TickSize::Cent);
        assert_eq!(ma.fee_bps.0, 0);
        assert!(!ma.neg_risk);

        let mb = reg.market_by_condition("0xbbb").unwrap();
        assert_eq!(mb.tick, TickSize::Milli);
        assert_eq!(mb.fee_bps.0, 200);
        assert!(mb.neg_risk);

        let mc = reg.market_by_condition("0xccc").unwrap();
        assert_eq!(mc.tick, TickSize::Milli);
        assert_eq!(mc.fee_bps.0, 0);
        assert!(mc.neg_risk);

        // ev2 has 2 negRisk members → partition should be derived
        let partitions = reg.partitions();
        assert!(
            !partitions.is_empty(),
            "ev2 negRisk event must produce a partition"
        );
    }

    // ---- Test 2: unsupported tick -------------------------------------------

    #[test]
    fn skips_unsupported_tick_with_reason() {
        let ev = make_event("ev1", "0xbad", "y1", "n1", false, true);
        let mut clob = HashMap::new();
        clob.insert(
            "0xbad".into(),
            make_clob(ClobSpec {
                condition_id: "0xbad",
                tick: 0.04,
                taker_fee: 0,
                active: true,
                closed: false,
                neg_risk: false,
                yes_token: "y1",
                no_token: "n1",
            }),
        );

        let universe = assemble_registry(&[ev], &clob, "", &UniverseFilter::default()).unwrap();

        assert_eq!(universe.registry.markets().len(), 0);
        assert_eq!(universe.skipped.len(), 1);
        assert_eq!(
            universe.skipped[0],
            ("0xbad".to_string(), SkippedReason::UnsupportedTick)
        );
    }

    // ---- Test 3: empty condition id + failed lookup --------------------------

    #[test]
    fn skips_empty_condition_and_failed_lookup() {
        // Empty conditionId market
        let ev_empty: GammaEvent = serde_json::from_str(
            r#"{"id":"evA","negRisk":false,"markets":[{"conditionId":"","clobTokenIds":"[\"1\",\"2\"]","active":true,"closed":false}]}"#,
        ).unwrap();

        // Missing CLOB record
        let ev_missing = make_event("evB", "0xmissing", "y", "n", false, true);

        let clob: HashMap<String, ClobMarket> = HashMap::new(); // empty — nothing will match

        let universe = assemble_registry(
            &[ev_empty, ev_missing],
            &clob,
            "",
            &UniverseFilter::default(),
        )
        .unwrap();

        assert_eq!(universe.registry.markets().len(), 0);
        assert_eq!(universe.skipped.len(), 2);

        let reasons: Vec<SkippedReason> = universe.skipped.iter().map(|(_, r)| *r).collect();
        assert!(reasons.contains(&SkippedReason::EmptyConditionId));
        assert!(reasons.contains(&SkippedReason::ClobLookupFailed));
    }

    // ---- Test 4: max_markets cap -------------------------------------------

    #[test]
    fn max_markets_caps_universe() {
        // 3 markets across 2 events; cap at 2
        let ev1 = make_event_multi("ev1", false, &[("0xa", "ya", "na"), ("0xb", "yb", "nb")]);
        let ev2 = make_event("ev2", "0xc", "yc", "nc", false, true);

        let mut clob = HashMap::new();
        clob.insert(
            "0xa".into(),
            make_clob(ClobSpec {
                condition_id: "0xa",
                tick: 0.01,
                taker_fee: 0,
                active: true,
                closed: false,
                neg_risk: false,
                yes_token: "ya",
                no_token: "na",
            }),
        );
        clob.insert(
            "0xb".into(),
            make_clob(ClobSpec {
                condition_id: "0xb",
                tick: 0.01,
                taker_fee: 0,
                active: true,
                closed: false,
                neg_risk: false,
                yes_token: "yb",
                no_token: "nb",
            }),
        );
        clob.insert(
            "0xc".into(),
            make_clob(ClobSpec {
                condition_id: "0xc",
                tick: 0.01,
                taker_fee: 0,
                active: true,
                closed: false,
                neg_risk: false,
                yes_token: "yc",
                no_token: "nc",
            }),
        );

        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
        };
        let universe = assemble_registry(&[ev1, ev2], &clob, "", &filter).unwrap();

        // exactly 2 added; 0xc was not visited — no skip entry for it
        assert_eq!(universe.registry.markets().len(), 2);
        // capped markets simply aren't visited — no skip entries expected
        assert_eq!(
            universe.skipped.len(),
            0,
            "capped markets produce no skip entries"
        );
    }

    // ---- Test 5: inactive skipped vs included when require_active=false ------

    #[test]
    fn inactive_skipped_when_required() {
        let ev = make_event("ev1", "0xclosed", "y", "n", false, true);
        let mut clob = HashMap::new();
        // closed=true → inactive under require_active
        clob.insert(
            "0xclosed".into(),
            make_clob(ClobSpec {
                condition_id: "0xclosed",
                tick: 0.01,
                taker_fee: 0,
                active: true,
                closed: true,
                neg_risk: false,
                yes_token: "y",
                no_token: "n",
            }),
        );

        // require_active = true → skip
        let filter_strict = UniverseFilter {
            max_markets: 200,
            require_active: true,
        };
        let strict =
            assemble_registry(std::slice::from_ref(&ev), &clob, "", &filter_strict).unwrap();
        assert_eq!(strict.registry.markets().len(), 0);
        assert_eq!(strict.skipped.len(), 1);
        assert_eq!(strict.skipped[0].1, SkippedReason::InactiveOrClosed);

        // require_active = false → include
        let filter_lax = UniverseFilter {
            max_markets: 200,
            require_active: false,
        };
        let lax = assemble_registry(&[ev], &clob, "", &filter_lax).unwrap();
        assert_eq!(lax.registry.markets().len(), 1);
        assert!(lax.skipped.is_empty());
    }

    // ---- Test 6: fixture events assemble ------------------------------------

    #[test]
    fn fixture_events_assemble() {
        let events_json =
            std::fs::read_to_string("../registry/tests/fixtures/gamma_events.json").unwrap();
        let events: Vec<GammaEvent> = serde_json::from_str(&events_json).unwrap();

        // Build a synthetic CLOB map for all member markets found in the fixture.
        // Use tick 0.001 (all fixture events are negRisk liquid markets) and
        // active=true, closed=false so they pass the activity filter.
        let mut clob_by_condition = HashMap::new();
        for event in &events {
            for gm in &event.markets {
                if gm.condition_id.is_empty() {
                    continue;
                }
                // Parse the gamma token ids to reuse in the synthetic CLOB record.
                let toks = gm.clob_token_ids().unwrap_or_default();
                let (yes_tok, no_tok) = if toks.len() >= 2 {
                    (toks[0].clone(), toks[1].clone())
                } else {
                    ("yes_tok".to_string(), "no_tok".to_string())
                };
                clob_by_condition.insert(
                    gm.condition_id.clone(),
                    make_clob(ClobSpec {
                        condition_id: &gm.condition_id,
                        tick: 0.001,
                        taker_fee: 0,
                        active: true,
                        closed: false,
                        neg_risk: gm.neg_risk,
                        yes_token: &yes_tok,
                        no_token: &no_tok,
                    }),
                );
            }
        }

        let universe =
            assemble_registry(&events, &clob_by_condition, "", &UniverseFilter::default()).unwrap();

        // At least some markets should have been assembled.
        assert!(
            !universe.registry.markets().is_empty(),
            "must have assembled at least one market"
        );

        // The fixture contains negRisk events with only 1 member market each.
        // The partition derivation will record TooFewMembers exclusions.
        // We just confirm partitions exist (even if not verified) and the
        // conservative TooFewMembers gate is visible in the exclusion log.
        let partitions = universe.registry.partitions();
        let exclusion_log = universe.registry.exclusion_log();

        // partitions should exist (one per event that has grouping)
        assert!(
            !partitions.is_empty(),
            "partitions must be derived from fixture events"
        );

        // All fixture negRisk events have 1 member, which hits TooFewMembers
        assert!(
            !exclusion_log.is_empty(),
            "TooFewMembers exclusions expected for single-member negRisk events"
        );
    }

    // ---- Test 7: keyset envelope parser ------------------------------------

    #[test]
    fn events_keyset_envelope_parses_object_wrapper() {
        // Build a synthetic envelope wrapping the first event from the fixture.
        let events_json =
            std::fs::read_to_string("../registry/tests/fixtures/gamma_events.json").unwrap();
        let raw_events: Vec<serde_json::Value> = serde_json::from_str(&events_json).unwrap();
        let first_event = raw_events.into_iter().next().unwrap();

        let envelope = serde_json::json!({
            "events": [first_event],
            "next_cursor": "someOpaqueBase64Cursor=="
        });
        let envelope_str = serde_json::to_string(&envelope).unwrap();

        let parsed = events_keyset_envelope(&envelope_str).unwrap();
        assert_eq!(
            parsed.len(),
            1,
            "should parse exactly 1 event from envelope"
        );
        assert!(!parsed[0].id.is_empty(), "event id should be non-empty");
    }

    #[test]
    fn events_keyset_envelope_rejects_malformed() {
        assert!(events_keyset_envelope("{").is_err());
        assert!(events_keyset_envelope(r#"{"not_events": []}"#).is_err());
    }

    // ---- pick_yes_no logic --------------------------------------------------

    #[test]
    fn pick_yes_no_uses_labels_when_present() {
        let tokens: Vec<ClobToken> = serde_json::from_str(
            r#"[{"token_id":"n_tok","outcome":"No","price":0.5,"winner":false},
               {"token_id":"y_tok","outcome":"Yes","price":0.5,"winner":false}]"#,
        )
        .unwrap();
        // No is index 0, Yes is index 1 — label-based should pick correctly
        let (yes, no) = pick_yes_no(&tokens).unwrap();
        assert_eq!(yes, "y_tok");
        assert_eq!(no, "n_tok");
    }

    #[test]
    fn pick_yes_no_falls_back_to_index_for_candidate_labels() {
        let tokens: Vec<ClobToken> = serde_json::from_str(
            r#"[{"token_id":"spain_tok","outcome":"Spain","price":0.17,"winner":false},
               {"token_id":"not_spain_tok","outcome":"Field","price":0.83,"winner":false}]"#,
        )
        .unwrap();
        let (yes, no) = pick_yes_no(&tokens).unwrap();
        assert_eq!(
            yes, "spain_tok",
            "index 0 is yes for non-Yes/No labelled markets"
        );
        assert_eq!(no, "not_spain_tok");
    }

    // ---- Test N: MissingTokens skipped at assemble level --------------------

    /// A market whose CLOB tokens list has fewer than 2 entries (or empty
    /// token_ids) must be recorded as [`SkippedReason::MissingTokens`] by
    /// [`assemble_registry`], which delegates to [`pick_yes_no`].
    #[test]
    fn assemble_skips_market_with_missing_tokens() {
        // Market with zero tokens in the CLOB record.
        let ev_zero = make_event("ev_zero", "0xzero", "yt", "nt", false, true);
        let clob_zero: ClobMarket = serde_json::from_str(
            r#"{
            "condition_id": "0xzero",
            "minimum_tick_size": 0.01,
            "neg_risk": false,
            "active": true,
            "closed": false,
            "maker_base_fee": 0,
            "taker_base_fee": 0,
            "tokens": []
        }"#,
        )
        .unwrap();

        // Market with only one token in the CLOB record.
        let ev_one = make_event("ev_one", "0xone", "yt", "nt", false, true);
        let clob_one: ClobMarket = serde_json::from_str(
            r#"{
            "condition_id": "0xone",
            "minimum_tick_size": 0.01,
            "neg_risk": false,
            "active": true,
            "closed": false,
            "maker_base_fee": 0,
            "taker_base_fee": 0,
            "tokens": [{"token_id": "only_tok", "outcome": "Yes", "price": 1.0, "winner": false}]
        }"#,
        )
        .unwrap();

        let mut clob = HashMap::new();
        clob.insert("0xzero".to_string(), clob_zero);
        clob.insert("0xone".to_string(), clob_one);

        let universe =
            assemble_registry(&[ev_zero, ev_one], &clob, "", &UniverseFilter::default()).unwrap();

        assert_eq!(
            universe.registry.markets().len(),
            0,
            "no market should be assembled"
        );
        assert_eq!(universe.skipped.len(), 2);
        let reasons: Vec<SkippedReason> = universe.skipped.iter().map(|(_, r)| *r).collect();
        assert!(
            reasons.iter().all(|r| *r == SkippedReason::MissingTokens),
            "both markets must be skipped as MissingTokens; got {reasons:?}"
        );
    }

    #[test]
    fn pick_yes_no_returns_none_for_empty_token_ids() {
        let tokens: Vec<ClobToken> = serde_json::from_str(
            r#"[{"token_id":"","outcome":"Yes","price":0,"winner":false},
               {"token_id":"","outcome":"No","price":0,"winner":false}]"#,
        )
        .unwrap();
        assert!(pick_yes_no(&tokens).is_none());
    }

    #[test]
    fn pick_yes_no_returns_none_for_too_few_tokens() {
        let tokens: Vec<ClobToken> = serde_json::from_str(
            r#"[{"token_id":"y","outcome":"Yes","price":0.5,"winner":false}]"#,
        )
        .unwrap();
        assert!(pick_yes_no(&tokens).is_none());
    }

    // ---- bounded CLOB fetch --------------------------------------------------

    /// In-memory [`ClobFetch`] recording every lookup; condition ids absent
    /// from `responses` fail like an HTTP error.
    struct StubClob {
        responses: HashMap<String, ClobMarket>,
        calls: Vec<String>,
    }

    impl StubClob {
        fn new(markets: Vec<ClobMarket>) -> Self {
            StubClob {
                responses: markets
                    .into_iter()
                    .map(|m| (m.condition_id.clone(), m))
                    .collect(),
                calls: Vec::new(),
            }
        }
    }

    impl ClobFetch for StubClob {
        async fn market(&mut self, condition_id: &str) -> Result<ClobMarket, IngestError> {
            self.calls.push(condition_id.to_string());
            self.responses
                .get(condition_id)
                .cloned()
                .ok_or_else(|| IngestError::Http("stub: lookup failed".into()))
        }
    }

    /// An acceptable (active, cent-tick, two-token) CLOB record.
    fn ok_clob(cid: &str, yes: &str, no: &str) -> ClobMarket {
        make_clob(ClobSpec {
            condition_id: cid,
            tick: 0.01,
            taker_fee: 0,
            active: true,
            closed: false,
            neg_risk: false,
            yes_token: yes,
            no_token: no,
        })
    }

    #[tokio::test]
    async fn bounded_fetch_stops_at_max_markets() {
        let events = vec![
            make_event_multi("e1", false, &[("c1", "y1", "n1"), ("c2", "y2", "n2")]),
            make_event_multi("e2", false, &[("c3", "y3", "n3"), ("c4", "y4", "n4")]),
        ];
        let mut stub = StubClob::new(vec![
            ok_clob("c1", "y1", "n1"),
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
            ok_clob("c4", "y4", "n4"),
        ]);
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
        };

        let (map, failures) = fetch_clob_bounded(&mut stub, &events, &filter).await;

        assert_eq!(
            stub.calls,
            vec!["c1", "c2"],
            "must stop fetching once max_markets acceptable markets are in hand"
        );
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("c1") && map.contains_key("c2"));
        assert!(failures.is_empty());
    }

    #[tokio::test]
    async fn bounded_fetch_gated_out_markets_do_not_count() {
        // c1 is inactive: fetched, but must not count toward the cap.
        let inactive = make_clob(ClobSpec {
            condition_id: "c1",
            tick: 0.01,
            taker_fee: 0,
            active: false,
            closed: false,
            neg_risk: false,
            yes_token: "y1",
            no_token: "n1",
        });
        let events = vec![make_event_multi(
            "e1",
            false,
            &[
                ("c1", "y1", "n1"),
                ("c2", "y2", "n2"),
                ("c3", "y3", "n3"),
                ("c4", "y4", "n4"),
            ],
        )];
        let mut stub = StubClob::new(vec![
            inactive,
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
            ok_clob("c4", "y4", "n4"),
        ]);
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
        };

        let (map, _failures) = fetch_clob_bounded(&mut stub, &events, &filter).await;

        assert_eq!(
            stub.calls,
            vec!["c1", "c2", "c3"],
            "inactive c1 must not count; c4 must never be fetched"
        );
        assert_eq!(map.len(), 3, "fetched records are kept even when gated out");
    }

    #[tokio::test]
    async fn bounded_fetch_failed_lookups_do_not_count() {
        // c1 is missing from the stub → lookup fails → must not count.
        let events = vec![make_event_multi(
            "e1",
            false,
            &[("c1", "y1", "n1"), ("c2", "y2", "n2"), ("c3", "y3", "n3")],
        )];
        let mut stub = StubClob::new(vec![
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
        ]);
        let filter = UniverseFilter {
            max_markets: 1,
            require_active: true,
        };

        let (map, failures) = fetch_clob_bounded(&mut stub, &events, &filter).await;

        assert_eq!(stub.calls, vec!["c1", "c2"]);
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("c2"));
        assert_eq!(
            failures,
            vec![("c1".to_string(), SkippedReason::ClobLookupFailed)]
        );
    }

    #[tokio::test]
    async fn bounded_fetch_duplicate_condition_counts_without_refetch() {
        // The same condition id in two events: assemble_registry would accept
        // it twice (it does not dedup), so the bounded fetch must count both
        // visits — without issuing a second HTTP lookup — and stop before c2.
        let events = vec![
            make_event("e1", "c1", "y1", "n1", false, true),
            make_event_multi("e2", false, &[("c1", "y1", "n1"), ("c2", "y2", "n2")]),
        ];
        let mut stub = StubClob::new(vec![
            ok_clob("c1", "y1", "n1"),
            ok_clob("c2", "y2", "n2"),
        ]);
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
        };

        let (map, _failures) = fetch_clob_bounded(&mut stub, &events, &filter).await;

        assert_eq!(stub.calls, vec!["c1"], "duplicate visit must not refetch");
        assert_eq!(map.len(), 1);
    }

    #[tokio::test]
    async fn bounded_fetch_assembles_identical_registry_to_exhaustive_fetch() {
        // c1 has an unsupported tick (gated out), c2..c4 acceptable, cap 2:
        // assembly from the bounded map must equal assembly from the full map.
        let bad_tick = make_clob(ClobSpec {
            condition_id: "c1",
            tick: 0.05,
            taker_fee: 0,
            active: true,
            closed: false,
            neg_risk: false,
            yes_token: "y1",
            no_token: "n1",
        });
        let events = vec![make_event_multi(
            "e1",
            false,
            &[
                ("c1", "y1", "n1"),
                ("c2", "y2", "n2"),
                ("c3", "y3", "n3"),
                ("c4", "y4", "n4"),
            ],
        )];
        let full: Vec<ClobMarket> = vec![
            bad_tick,
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
            ok_clob("c4", "y4", "n4"),
        ];
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
        };

        let full_map: HashMap<String, ClobMarket> = full
            .iter()
            .map(|m| (m.condition_id.clone(), m.clone()))
            .collect();
        let mut stub = StubClob::new(full);
        let (bounded_map, _) = fetch_clob_bounded(&mut stub, &events, &filter).await;

        let from_full = assemble_registry(&events, &full_map, "", &filter).unwrap();
        let from_bounded = assemble_registry(&events, &bounded_map, "", &filter).unwrap();

        let conditions = |u: &AssembledUniverse| -> Vec<String> {
            u.registry
                .markets()
                .iter()
                .map(|m| u.registry.market_condition(m.id).unwrap_or("").to_string())
                .collect()
        };
        assert_eq!(conditions(&from_full), vec!["c2", "c3"]);
        assert_eq!(conditions(&from_bounded), conditions(&from_full));
        assert_eq!(from_bounded.skipped, from_full.skipped);
    }
}
