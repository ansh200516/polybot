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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use futures_util::stream::StreamExt;

use pm_core::num::TickSize;
use pm_registry::gamma::{ClobMarket, GammaEvent, GammaMarket};
use pm_registry::segment::{MarketMetrics, MarketPriority, SegmentThresholds, classify, market_priority};
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
    /// Task 5.3 — opt-in universe prioritization. **Default `false`** ⇒ the
    /// universe cap keeps the first `max_markets` ACCEPTED markets in Gamma
    /// keyset order (historical, byte-identical behaviour). `true` ⇒ a candidate
    /// pool is gathered in keyset order and ranked by [`market_priority`]
    /// (segment tier, then liquidity, then volume); the top `max_markets`
    /// survive. The entire ranking path is guarded behind this flag, so the
    /// default code path is unchanged.
    pub prioritize_by_liquidity: bool,
    /// Task 5.3 — candidate pool size for prioritization. **Default `0`**, the
    /// sentinel for "= `max_markets`" (no extra fetching). A non-zero value must
    /// be ≥ `max_markets`; it is the number of ACCEPTED candidates gathered (in
    /// keyset order) before ranking. Clamped to [`MAX_CANDIDATE_POOL`] to bound
    /// CLOB API cost. Inert unless `prioritize_by_liquidity`.
    pub candidate_pool: usize,
    /// Task 5.3 — segment thresholds used to classify each candidate for the
    /// tier component of [`market_priority`]. Only consulted on the prioritized
    /// path; ignored entirely when `prioritize_by_liquidity` is `false`.
    pub segment_thresholds: SegmentThresholds,
}

impl Default for UniverseFilter {
    fn default() -> Self {
        UniverseFilter {
            max_markets: 200,
            require_active: true,
            // Task 5.3 defaults keep the historical keyset-order cap unchanged.
            prioritize_by_liquidity: false,
            candidate_pool: 0,
            segment_thresholds: SegmentThresholds::default(),
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
    /// Task 5.3 — a gated-OK candidate that ranked below the top `max_markets`
    /// by [`market_priority`] (segment tier, liquidity, volume) and was dropped.
    /// ONLY ever produced on the opt-in prioritized path; the default keyset
    /// path never emits it.
    BelowPriorityCut,
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

/// Hard ceiling on the prioritization candidate pool (Task 5.3). Caps the
/// number of CLOB lookups when scaling toward broad coverage so a large
/// `candidate_pool` can never make startup fetch an unbounded number of markets.
pub const MAX_CANDIDATE_POOL: usize = 5000;

/// Resolve the effective candidate-pool size for the prioritized path.
///
/// `candidate_pool == 0` is the sentinel for "= `max_markets`" (no extra
/// fetching). The result is clamped to [`MAX_CANDIDATE_POOL`] to bound API cost,
/// but never below `max_markets` (the operator asked to KEEP that many, so the
/// pool must be able to fill them even past the ceiling). Only meaningful when
/// `filter.prioritize_by_liquidity` is set.
fn effective_candidate_pool(filter: &UniverseFilter) -> usize {
    let requested = if filter.candidate_pool == 0 {
        filter.max_markets
    } else {
        filter.candidate_pool
    };
    requested.min(MAX_CANDIDATE_POOL).max(filter.max_markets)
}

/// Build one gated, accepted market into the registry builder.
///
/// Shared by [`assemble_keyset`] and [`assemble_prioritized`] so the two paths
/// cannot drift (mirrors the [`gate_market`] sharing). Reproduces the original
/// inline block exactly: CLOB `taker_base_fee` is authoritative, `neg_risk` is
/// the defensive OR of the CLOB and gamma flags, an empty event id means "no
/// grouping", and the Phase-5 metrics are captured for the accepted market.
fn add_accepted_market(
    builder: &mut RegistryBuilder,
    event: &GammaEvent,
    gm: &GammaMarket,
    clob: &ClobMarket,
    tick: TickSize,
    yes_id: &str,
    no_id: &str,
) {
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
        yes_id,
        no_id,
        tick,
        fee_bps,
        neg_risk,
        gm.question.clone(),
        clob.active,
        clob.closed,
        event_key,
    );
    // Phase-5 segmentation: capture the per-market Gamma liquidity metrics
    // (with the event-level fallback — see `market_metrics`). The liquidity-
    // reward params come from the CLOB market (`clob.rewards`), not Gamma, so
    // they are set here where the fetched `ClobMarket` is in scope.
    let mut metrics = market_metrics(gm, event);
    metrics.reward_min_size = clob.rewards.min_size;
    metrics.reward_max_spread_cents = clob.rewards.max_spread;
    metrics.reward_daily_rate_usd = clob.rewards.daily_rate_usd();
    builder.record_market_metrics(metrics);
}

/// Build the Phase-5 [`MarketMetrics`] for a market, falling back to its EVENT's
/// figures for any metric the market omits.
///
/// This fallback is load-bearing on LIVE data: the Gamma feed carries `volume`
/// per market but reports `liquidity` ONLY at the event level (the market's
/// `liquidity` is `null`). Without the fallback every live market scores
/// liquidity 0 → classifies [`Illiquid`](pm_registry::segment::MarketSegment::Illiquid)
/// → the market maker (which quotes only liquid segments) gets an EMPTY market
/// set and never quotes. Liquidity prefers the event's CLOB book depth
/// (`liquidity_clob`) over the broader `liquidity`. Per-market `volume` is kept
/// as the primary signal (so a thin outcome inside a liquid event — low own
/// volume — still classifies Illiquid and is not quoted).
fn market_metrics(gm: &GammaMarket, event: &GammaEvent) -> MarketMetrics {
    MarketMetrics {
        volume: gm.volume.or(event.volume),
        volume_24hr: gm.volume_24hr.or(event.volume_24hr),
        liquidity: gm
            .liquidity
            .or(event.liquidity_clob)
            .or(event.liquidity),
        category: gm.category.clone(),
        // Reward-program params are NOT in the Gamma feed; they come from the
        // CLOB market and are set by the caller (`add_accepted_market`), where
        // the fetched `ClobMarket` is in scope. Default to 0 (= ineligible) here.
        ..Default::default()
    }
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
/// **Default (`filter.prioritize_by_liquidity == false`):** markets are accepted
/// in gamma keyset order and once `filter.max_markets` have been added the
/// remaining gamma markets are simply not visited — they do **not** produce skip
/// entries. This is the historical behaviour, byte-identical to before Task 5.3.
///
/// **Prioritized (opt-in, `filter.prioritize_by_liquidity == true`):** candidates
/// are accepted in keyset order up to a pool of [`effective_candidate_pool`]
/// markets, ranked by [`market_priority`] (segment tier, then liquidity, then
/// volume, with condition id + keyset position as a stable tiebreak), and the top
/// `filter.max_markets` are kept. Candidates ranked below the cut are dropped and
/// recorded as [`SkippedReason::BelowPriorityCut`]. Kept markets are assembled in
/// keyset order, so event grouping is preserved — ranking only decides *which*
/// markets survive, not their assembly order. The whole ranking path is guarded
/// behind the flag, leaving the default code path unchanged.
pub fn assemble_registry(
    events: &[GammaEvent],
    clob_by_condition: &HashMap<String, ClobMarket>,
    relationship_toml: &str,
    filter: &UniverseFilter,
) -> Result<AssembledUniverse, RegistryError> {
    let mut builder = RegistryBuilder::default();
    let mut skipped: Vec<(String, SkippedReason)> = Vec::new();

    if filter.prioritize_by_liquidity {
        assemble_prioritized(&mut builder, &mut skipped, events, clob_by_condition, filter);
    } else {
        assemble_keyset(&mut builder, &mut skipped, events, clob_by_condition, filter);
    }

    let registry = builder.finish(relationship_toml)?;
    Ok(AssembledUniverse {
        registry: Arc::new(registry),
        skipped,
    })
}

/// DEFAULT capping path: accept in keyset order, stop at `filter.max_markets`.
/// Byte-identical in behaviour to the pre-Task-5.3 `assemble_registry` loop.
fn assemble_keyset(
    builder: &mut RegistryBuilder,
    skipped: &mut Vec<(String, SkippedReason)>,
    events: &[GammaEvent],
    clob_by_condition: &HashMap<String, ClobMarket>,
    filter: &UniverseFilter,
) {
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

            add_accepted_market(builder, event, gm, clob, tick, &yes_id, &no_id);
            count += 1;
        }
    }
}

/// OPT-IN prioritized capping path (Task 5.3). Gathers accepted candidates in
/// keyset order up to [`effective_candidate_pool`], ranks them by
/// [`market_priority`], keeps the top `filter.max_markets`, and records the rest
/// as [`SkippedReason::BelowPriorityCut`].
///
/// Phase 1 visits the SAME prefix of markets that [`fetch_clob_bounded`] fetches
/// (both share `effective_candidate_pool` + [`gate_market`]), so every CLOB
/// record assembly needs was fetched, and skip reasons for gate failures within
/// the visited prefix match the keyset path exactly.
fn assemble_prioritized<'a>(
    builder: &mut RegistryBuilder,
    skipped: &mut Vec<(String, SkippedReason)>,
    events: &'a [GammaEvent],
    clob_by_condition: &'a HashMap<String, ClobMarket>,
    filter: &UniverseFilter,
) {
    /// An accepted candidate: enough state to rank it and (if kept) build it.
    struct Candidate<'a> {
        event: &'a GammaEvent,
        gm: &'a GammaMarket,
        clob: &'a ClobMarket,
        tick: TickSize,
        yes_id: String,
        no_id: String,
        priority: MarketPriority,
    }

    let pool_cap = effective_candidate_pool(filter);
    let mut candidates: Vec<Candidate<'a>> = Vec::new();

    // ---- Phase 1: gather accepted candidates in keyset order, up to pool_cap.
    'outer: for event in events {
        for gm in &event.markets {
            if candidates.len() >= pool_cap {
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

            // ---- rank inputs: classify the segment, build the priority key ---
            // Event-level fallback (see `market_metrics`) so live markets — whose
            // own `liquidity` is null — inherit their event's liquidity and rank.
            let metrics = market_metrics(gm, event);
            let segment = classify(&metrics, &filter.segment_thresholds);
            let priority = market_priority(&metrics, segment);

            candidates.push(Candidate {
                event,
                gm,
                clob,
                tick,
                yes_id,
                no_id,
                priority,
            });
        }
    }

    // ---- Phase 2: rank best-first, keep the top max_markets ------------------
    let candidate_count = candidates.len();
    // Rank candidate indices: priority DESC, then condition id ASC, then keyset
    // position ASC — a total, deterministic order even for duplicate ids.
    let mut order: Vec<usize> = (0..candidate_count).collect();
    order.sort_by(|&a, &b| {
        candidates[b]
            .priority
            .cmp(&candidates[a].priority)
            .then_with(|| candidates[a].gm.condition_id.cmp(&candidates[b].gm.condition_id))
            .then(a.cmp(&b))
    });
    let keep = filter.max_markets.min(candidate_count);
    let kept: std::collections::HashSet<usize> = order[..keep].iter().copied().collect();

    // Build kept markets in keyset (candidate insertion) order so event grouping
    // is preserved; record dropped candidates as BelowPriorityCut.
    for (i, cand) in candidates.iter().enumerate() {
        if kept.contains(&i) {
            add_accepted_market(
                builder, cand.event, cand.gm, cand.clob, cand.tick, &cand.yes_id, &cand.no_id,
            );
        } else {
            skipped.push((cand.gm.condition_id.clone(), SkippedReason::BelowPriorityCut));
        }
    }

    tracing::info!(
        candidates = candidate_count,
        kept = keep,
        max_markets = filter.max_markets,
        candidate_pool = pool_cap,
        "universe prioritization: ranked {candidate_count} candidates → kept top {keep} by (segment, liquidity)"
    );
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
    /// Opt-in CONFLUENCE mode (Data-API "follow the smart money"): when `Some`,
    /// [`sync_once`] builds the universe from these specific market condition ids
    /// (the top traders' favored markets) via
    /// [`fetch_gamma_markets_by_condition`] instead of walking the Gamma keyset.
    /// `None` ⇒ the normal keyset universe.
    ///
    /// [`sync_once`]: SyncTask::sync_once
    /// [`fetch_gamma_markets_by_condition`]: SyncTask::fetch_gamma_markets_by_condition
    confluence_conditions: Option<Vec<String>>,
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
            confluence_conditions: None,
        })
    }

    /// Enable CONFLUENCE mode: build the universe from these market `condition_ids`
    /// (the top traders' favored markets) instead of the Gamma keyset. Both `None`
    /// and `Some(empty)` fall back to the keyset universe. Builder-style so the
    /// existing `new` call sites and tests are unaffected.
    pub fn with_confluence_conditions(mut self, conditions: Option<Vec<String>>) -> Self {
        self.confluence_conditions = conditions.filter(|c| !c.is_empty());
        self
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

    /// Fetch Gamma metadata for a SPECIFIC set of market `condition_ids`
    /// (CONFLUENCE mode), batching repeated `&condition_ids=` query params. Unlike
    /// [`fetch_gamma_events`] (which walks active events in keyset order) this
    /// targets the exact markets the top traders hold. Markets that are missing,
    /// inactive, or closed are simply ABSENT from the result (best-effort): the
    /// `active=true&closed=false` filter is applied server-side so a resolved
    /// smart-money pick never enters the universe.
    ///
    /// Each returned [`GammaMarket`] is wrapped in a synthetic single-market
    /// [`GammaEvent`] by [`confluence_events`] so the rest of the pipeline
    /// (`fetch_clob_for` + [`assemble_registry`], including the Phase-5 segment
    /// filter) is identical to the keyset path.
    ///
    /// [`fetch_gamma_events`]: SyncTask::fetch_gamma_events
    pub async fn fetch_gamma_markets_by_condition(
        &self,
        condition_ids: &[String],
    ) -> Result<Vec<GammaMarket>, IngestError> {
        let mut out: Vec<GammaMarket> = Vec::new();
        // Gamma caps the query-string length; 20 ids/request stays well under it
        // and keeps each response small. A batch failure is propagated (the caller
        // falls back to the keyset universe rather than running on a partial set).
        for chunk in condition_ids.chunks(20) {
            let params: String = chunk
                .iter()
                .map(|c| format!("&condition_ids={c}"))
                .collect();
            let url = format!(
                "{}/markets?active=true&closed=false&limit=100{}",
                self.gamma_base, params
            );
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
            let markets: Vec<GammaMarket> =
                serde_json::from_str(&body).map_err(|e| IngestError::Parse(e.to_string()))?;
            out.extend(markets);
        }
        Ok(out)
    }

    /// Issue single-market CLOB lookups for the markets in `events`, bounded
    /// by `filter.max_markets` would-be-accepted markets (see
    /// [`fetch_clob_bounded`]), returning a map of condition id →
    /// [`ClobMarket`] plus any failures as `ClobLookupFailed` skip entries.
    pub async fn fetch_clob_for(
        &mut self,
        events: &[GammaEvent],
    ) -> (HashMap<String, ClobMarket>, Vec<(String, SkippedReason)>) {
        fetch_clob_bounded(&self.clob, events, &self.filter).await
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
        // CONFLUENCE mode (opt-in): build the universe from the top traders'
        // favored markets (fetched by condition id) instead of the Gamma keyset.
        // Everything downstream (CLOB fetch + assembly + segment filter) is shared.
        let events = match self.confluence_conditions.clone() {
            Some(conditions) => {
                let markets = self.fetch_gamma_markets_by_condition(&conditions).await?;
                tracing::info!(
                    requested = conditions.len(),
                    resolved = markets.len(),
                    "sync: confluence universe (top-trader favored markets)"
                );
                confluence_events(markets)
            }
            None => self.fetch_gamma_events(50).await?,
        };
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

/// Wrap each CONFLUENCE [`GammaMarket`] in a synthetic single-market
/// [`GammaEvent`] so the confluence universe flows through the SAME
/// `fetch_clob_for` + [`assemble_registry`] path as the keyset universe. The
/// market's own `volume`/`liquidity` are lifted to the event level (the Phase-5
/// segmentation fallback reads event-level metrics when a market omits its own),
/// so the segment filter classifies confluence markets exactly as keyset ones.
///
/// Order is PRESERVED: callers pass markets best-confluence-first so the
/// non-prioritized assembly keeps the strongest signals under `max_markets`.
///
/// NOTE: a synthetic event holds ONE market, so a NegRisk member market loses
/// its event grouping here — acceptable for the MM (it quotes a single token per
/// market); the universal arb's NegRisk LP simply has no multi-leg partition to
/// solve over for a confluence-sourced market.
pub fn confluence_events(markets: Vec<GammaMarket>) -> Vec<GammaEvent> {
    markets
        .into_iter()
        .map(|m| GammaEvent {
            id: m.condition_id.clone(),
            neg_risk: m.neg_risk,
            title: m.question.clone(),
            volume: m.volume,
            volume_24hr: m.volume_24hr,
            liquidity: m.liquidity,
            liquidity_clob: None,
            markets: vec![m],
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Bounded CLOB fetch
// ---------------------------------------------------------------------------

/// Fetch seam over the single-market CLOB lookup so the bounded fetch loop is
/// testable without a network client.
///
/// `&self` (not `&mut self`): [`fetch_clob_bounded`] issues many lookups
/// CONCURRENTLY through one shared `&F`, so the method must be callable from
/// several in-flight futures at once. [`ClobRest`] satisfies this via its
/// interior-mutable (Mutex-guarded) rate limiter.
pub trait ClobFetch {
    /// Fetch a single CLOB market by condition id.
    fn market(
        &self,
        condition_id: &str,
    ) -> impl std::future::Future<Output = Result<ClobMarket, IngestError>>;
}

impl ClobFetch for ClobRest {
    async fn market(&self, condition_id: &str) -> Result<ClobMarket, IngestError> {
        ClobRest::market(self, condition_id).await
    }
}

/// Max CLOB single-market lookups [`fetch_clob_bounded`] keeps IN FLIGHT at once.
///
/// The shared token-bucket rate limiter (5 req/s default) still caps the issue
/// RATE; this only lets that many requests be outstanding so a slow tail of
/// `/markets` responses OVERLAPS instead of serialising. Sized so the rate limit
/// stays the binding constraint even when responses sit near the 10s client
/// timeout (≈ rate × timeout) — turning a pathological multi-minute sync into
/// tens of seconds WITHOUT raising the request rate.
const CLOB_FETCH_CONCURRENCY: usize = 32;

/// Fetch CLOB metadata for the markets in `events`, in assemble_registry's visit
/// order, stopping once the visit cap of would-be-accepted markets is reached —
/// but issuing the lookups in CONCURRENT waves (bounded by [`CLOB_FETCH_CONCURRENCY`]
/// and the shared rate limiter) so a slow `/markets` tail no longer serialises.
///
/// The cap depends on the prioritization mode (Task 5.3):
/// - **Default (`!prioritize_by_liquidity`):** `filter.max_markets` — assembly
///   never visits markets past its cap, so neither does the fetch.
/// - **Prioritized:** [`effective_candidate_pool`] — assembly ranks the whole
///   candidate pool down to `max_markets`, so the fetch must cover the pool.
///   This is the documented API-cost tradeoff: up to `candidate_pool` CLOB
///   lookups happen even though only `max_markets` end up in the registry. CLOB
///   metadata carries no ranking signal (liquidity/volume come from gamma), so
///   the pool cannot be pre-trimmed before fetching: a candidate must clear the
///   CLOB-backed [`gate_market`] to be rankable at all.
///
/// Acceptance mirrors `assemble_registry`'s per-market gating (shared via
/// [`gate_market`] + `effective_candidate_pool`). The fetched SET is identical to
/// a purely sequential walk: each wave scans at most `cap - accepted` further
/// VISITS (beyond that even an all-accept run would have hit the cap, so a
/// sequential fetch would not have looked further), so we never fetch past the
/// prefix assembly needs. Within a wave the lookups run concurrently; results are
/// applied in scan order so `by_condition`/`failures` stay deterministic.
///
/// This bound matters for startup latency: a single gamma keyset page can carry
/// 1000+ member markets, and the CLOB client is rate-limited.
pub async fn fetch_clob_bounded<F: ClobFetch>(
    fetch: &F,
    events: &[GammaEvent],
    filter: &UniverseFilter,
) -> (HashMap<String, ClobMarket>, Vec<(String, SkippedReason)>) {
    // Shared with assembly: prioritization fetches the whole candidate pool,
    // the default path fetches exactly up to max_markets.
    let cap = if filter.prioritize_by_liquidity {
        effective_candidate_pool(filter)
    } else {
        filter.max_markets
    };

    // Ordered visits in assemble_registry's exact order, empties skipped.
    // Duplicates are RETAINED — assembly counts a repeated condition id toward
    // the cap without a refetch, so we must too.
    let visits: Vec<&str> = events
        .iter()
        .flat_map(|e| e.markets.iter())
        .map(|gm| gm.condition_id.as_str())
        .filter(|c| !c.is_empty())
        .collect();

    let mut by_condition: HashMap<String, ClobMarket> = HashMap::new();
    let mut failed: HashSet<String> = HashSet::new();
    let mut failures: Vec<(String, SkippedReason)> = Vec::new();
    let mut accepted = 0usize;
    let mut gate_idx = 0usize;

    while gate_idx < visits.len() && accepted < cap {
        // Gate every already-resolved visit in order, counting acceptances, until
        // the cap is hit or we reach an unresolved (not-yet-fetched) condition.
        while gate_idx < visits.len() && accepted < cap {
            let cond = visits[gate_idx];
            if let Some(cm) = by_condition.get(cond) {
                if gate_market(cm, filter).is_ok() {
                    accepted += 1;
                }
                gate_idx += 1;
            } else if failed.contains(cond) {
                gate_idx += 1; // a failed lookup never counts toward the cap
            } else {
                break; // unresolved → fetch the next wave below
            }
        }
        if accepted >= cap || gate_idx >= visits.len() {
            break;
        }

        // Build the next wave: distinct, not-yet-resolved condition ids scanning
        // forward from gate_idx. Bounded by (a) `cap - accepted` VISITS scanned and
        // (b) CLOB_FETCH_CONCURRENCY distinct lookups in flight. The first
        // unresolved visit (at gate_idx) is always picked up, so each wave makes
        // progress and the loop terminates.
        let visit_budget = cap - accepted;
        let mut wave: Vec<String> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();
        let mut scanned = 0usize;
        let mut j = gate_idx;
        while j < visits.len() && scanned < visit_budget && wave.len() < CLOB_FETCH_CONCURRENCY {
            let cond = visits[j];
            j += 1;
            scanned += 1;
            if !by_condition.contains_key(cond) && !failed.contains(cond) && seen.insert(cond) {
                wave.push(cond.to_owned());
            }
        }

        // Fetch the wave concurrently; the shared rate limiter throttles the
        // issue rate, so this just overlaps slow responses.
        let mut results: HashMap<String, Result<ClobMarket, IngestError>> =
            futures_util::stream::iter(wave.iter().map(|cond| {
                let cond = cond.clone();
                async move {
                    let r = fetch.market(&cond).await;
                    (cond, r)
                }
            }))
            .buffer_unordered(CLOB_FETCH_CONCURRENCY)
            .collect()
            .await;

        // Apply in scan order so by_condition / failures are completion-order
        // independent (matches the old sequential output exactly).
        for cond in &wave {
            match results.remove(cond) {
                Some(Ok(cm)) => {
                    by_condition.insert(cond.clone(), cm);
                }
                Some(Err(e)) => {
                    tracing::warn!(
                        condition_id = %cond,
                        error = %e,
                        "CLOB single-market lookup failed; skipping"
                    );
                    failed.insert(cond.clone());
                    failures.push((cond.clone(), SkippedReason::ClobLookupFailed));
                }
                None => {
                    // Defensive: every wave entry yields a result, but never loop.
                    failed.insert(cond.clone());
                    failures.push((cond.clone(), SkippedReason::ClobLookupFailed));
                }
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
            ..UniverseFilter::default()
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
            ..UniverseFilter::default()
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
            ..UniverseFilter::default()
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

    // ---- Test 6b: Phase-5 metric capture is threaded + classified -----------

    /// Assembling the committed events fixture threads the per-market Gamma
    /// liquidity metrics into the registry and classifies them, WITHOUT changing
    /// which markets are accepted or their order (purely additive capture).
    #[test]
    fn fixture_metrics_are_threaded_and_classified() {
        use pm_registry::segment::{MarketSegment, SegmentThresholds};

        const SPAIN: &str = "0x7976b8dbacf9077eb1453a62bcefd6ab2df199acd28aad276ff0d920d6992892";
        const FED: &str = "0xdde06286a7b9464d344f410ab0b3d2ebc6469904e72c27fd982f65fdbf78768d";
        const IRAN: &str = "0xbbc6689d0f6d57ea42168836712237c7308b3e0118c8914d31b6126d0f3254c5";

        let events_json =
            std::fs::read_to_string("../registry/tests/fixtures/gamma_events.json").unwrap();
        let events: Vec<GammaEvent> = serde_json::from_str(&events_json).unwrap();

        // Synthetic, all-active CLOB for every member market (same approach as
        // `fixture_events_assemble`) so the activity gate accepts them all.
        let mut clob_by_condition = HashMap::new();
        for event in &events {
            for gm in &event.markets {
                if gm.condition_id.is_empty() {
                    continue;
                }
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
        let reg = &universe.registry;

        // Invariance: all three fixture markets assemble, in event order, none
        // skipped — metric capture changed nothing about acceptance/order.
        let conditions: Vec<&str> = reg
            .markets()
            .iter()
            .map(|m| reg.market_condition(m.id).unwrap())
            .collect();
        assert_eq!(conditions, vec![SPAIN, FED, IRAN]);
        assert!(universe.skipped.is_empty());

        let t = SegmentThresholds::default();

        // Spain: huge volume + liquidity (both arrive as JSON strings) → captured
        // non-None and classified LiquidStable.
        let spain = reg.market_by_condition(SPAIN).unwrap().id;
        let spain_metrics = reg.metrics(spain).unwrap();
        assert!(spain_metrics.volume.unwrap() > 1_000_000.0);
        assert!(spain_metrics.liquidity.unwrap() > 1_000_000.0);
        assert_eq!(reg.segment(spain, &t), MarketSegment::LiquidStable);

        // Iran: the MARKET-level liquidity is absent (null on the wire, as on the
        // LIVE feed), but the market now INHERITS its EVENT's liquidity (~1.8M)
        // via the `market_metrics` fallback — so it classifies LiquidStable
        // rather than being wrongly dropped to Illiquid. This is the fix for the
        // live bug where every market scored liquidity 0 and the MM never quoted.
        let iran = reg.market_by_condition(IRAN).unwrap().id;
        let iran_metrics = reg.metrics(iran).unwrap();
        assert!(iran_metrics.volume.is_some());
        assert!(
            iran_metrics.liquidity.unwrap() > 1_000_000.0,
            "market liquidity is null → must inherit the event's liquidity"
        );
        assert_eq!(reg.segment(iran, &t), MarketSegment::LiquidStable);
    }

    /// Regression for the LIVE Gamma shape: the market object carries `volume`
    /// but its `liquidity` is `null` (only the EVENT exposes liquidity). The
    /// market must inherit the event's liquidity — preferring `liquidityClob`
    /// (CLOB book depth) over the broader `liquidity` — so it does not collapse
    /// to Illiquid (the bug that left the market maker with an empty market set).
    #[test]
    fn live_shaped_market_inherits_event_liquidity() {
        let event: GammaEvent = serde_json::from_str(
            r#"{
                "id": "1",
                "negRisk": false,
                "volume": 1577270.29,
                "liquidity": 3163.22,
                "liquidityClob": 9000.0,
                "markets": [
                    {"conditionId":"0xabc","clobTokenIds":"[\"1\",\"2\"]",
                     "active":true,"closed":false,"volume":"494516.53","question":"Q"}
                ]
            }"#,
        )
        .unwrap();
        let gm = &event.markets[0];
        // The live market really does omit liquidity.
        assert_eq!(gm.liquidity, None, "live market object carries no liquidity");

        let m = market_metrics(gm, &event);
        assert_eq!(m.volume, Some(494516.53), "per-market volume stays primary");
        assert_eq!(
            m.liquidity,
            Some(9000.0),
            "inherits the event's liquidityClob (preferred over the broader liquidity)"
        );
    }

    /// End-to-end coverage of the CLOB→metrics REWARD mapping (Task 2): a
    /// reward-bearing CLOB market must thread `rewards.{min_size, max_spread,
    /// daily_rate}` into the registry's [`MarketMetrics`] with the CORRECT field
    /// correspondence. The three values are distinct, so a silent field-swap
    /// (e.g. `reward_max_spread_cents = clob.rewards.min_size`) fails an assert.
    #[test]
    fn assemble_records_reward_metrics_from_clob() {
        use pm_registry::gamma::{ClobRewardRate, ClobRewards};

        let ev = make_event("ev_rw", "0xrw", "y_rw", "n_rw", false, true);

        // A reward-bearing CLOB market. `make_clob` leaves `rewards` all-zero;
        // its `rewards` field is `pub`, so set it without touching `ClobSpec`.
        let mut clob = make_clob(ClobSpec {
            condition_id: "0xrw",
            tick: 0.01,
            taker_fee: 0,
            active: true,
            closed: false,
            neg_risk: false,
            yes_token: "y_rw",
            no_token: "n_rw",
        });
        clob.rewards = ClobRewards {
            max_spread: 3.0,
            min_size: 100.0,
            rates: Some(vec![ClobRewardRate {
                rewards_daily_rate: 50.0,
            }]),
        };

        let mut clob_by_condition = HashMap::new();
        clob_by_condition.insert("0xrw".to_string(), clob);

        let universe =
            assemble_registry(&[ev], &clob_by_condition, "", &UniverseFilter::default()).unwrap();
        let reg = &universe.registry;

        // Resolve condition → id → metrics (mirrors the SPAIN/FED/IRAN test).
        let id = reg.market_by_condition("0xrw").unwrap().id;
        let m = reg.metrics(id).unwrap();
        // Exact mapping with distinct values: a field-swap fails one of these.
        assert_eq!(m.reward_max_spread_cents, 3.0);
        assert_eq!(m.reward_min_size, 100.0);
        assert_eq!(m.reward_daily_rate_usd, 50.0);
        assert!(m.reward_eligible());
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
        // Interior-mutable: the bounded fetch now calls `market(&self)` from a
        // concurrent wave, so the recorder cannot take `&mut self`.
        calls: std::sync::Mutex<Vec<String>>,
    }

    impl StubClob {
        fn new(markets: Vec<ClobMarket>) -> Self {
            StubClob {
                responses: markets
                    .into_iter()
                    .map(|m| (m.condition_id.clone(), m))
                    .collect(),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        /// Recorded lookups, SORTED. The bounded fetch issues each wave of lookups
        /// concurrently, so the asserted contract is the SET fetched (the cost
        /// bound), not the (now nondeterministic) completion order.
        fn calls_sorted(&self) -> Vec<String> {
            let mut c = self.calls.lock().unwrap().clone();
            c.sort();
            c
        }
    }

    impl ClobFetch for StubClob {
        async fn market(&self, condition_id: &str) -> Result<ClobMarket, IngestError> {
            self.calls.lock().unwrap().push(condition_id.to_string());
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
        let stub = StubClob::new(vec![
            ok_clob("c1", "y1", "n1"),
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
            ok_clob("c4", "y4", "n4"),
        ]);
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
            ..UniverseFilter::default()
        };

        let (map, failures) = fetch_clob_bounded(&stub, &events, &filter).await;

        assert_eq!(
            stub.calls_sorted(),
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
        let stub = StubClob::new(vec![
            inactive,
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
            ok_clob("c4", "y4", "n4"),
        ]);
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
            ..UniverseFilter::default()
        };

        let (map, _failures) = fetch_clob_bounded(&stub, &events, &filter).await;

        assert_eq!(
            stub.calls_sorted(),
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
        let stub = StubClob::new(vec![
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
        ]);
        let filter = UniverseFilter {
            max_markets: 1,
            require_active: true,
            ..UniverseFilter::default()
        };

        let (map, failures) = fetch_clob_bounded(&stub, &events, &filter).await;

        assert_eq!(stub.calls_sorted(), vec!["c1", "c2"]);
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
        let stub = StubClob::new(vec![
            ok_clob("c1", "y1", "n1"),
            ok_clob("c2", "y2", "n2"),
        ]);
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
            ..UniverseFilter::default()
        };

        let (map, _failures) = fetch_clob_bounded(&stub, &events, &filter).await;

        assert_eq!(
            stub.calls_sorted(),
            vec!["c1"],
            "duplicate visit must not refetch"
        );
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
            ..UniverseFilter::default()
        };

        let full_map: HashMap<String, ClobMarket> = full
            .iter()
            .map(|m| (m.condition_id.clone(), m.clone()))
            .collect();
        let stub = StubClob::new(full);
        let (bounded_map, _) = fetch_clob_bounded(&stub, &events, &filter).await;

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

    // ---- Task 5.3: universe prioritization ---------------------------------

    /// Build a single-market gamma event carrying Phase-5 liquidity metrics.
    fn make_event_metrics(
        event_id: &str,
        cond: &str,
        yes: &str,
        no: &str,
        volume: f64,
        liquidity: f64,
    ) -> GammaEvent {
        let json = format!(
            r#"{{
                "id": {eid:?},
                "negRisk": false,
                "markets": [{{
                    "conditionId": {cid:?},
                    "clobTokenIds": "[\"{yes}\", \"{no}\"]",
                    "negRisk": false,
                    "active": true,
                    "closed": false,
                    "question": "Q?",
                    "volume": {volume},
                    "liquidity": {liquidity}
                }}]
            }}"#,
            eid = event_id,
            cid = cond,
            yes = yes,
            no = no,
            volume = volume,
            liquidity = liquidity,
        );
        serde_json::from_str(&json).unwrap()
    }

    /// 4 markets whose keyset order (c1, c2 thin Illiquid; then c3, c4 deep
    /// LiquidStable) is the OPPOSITE of liquidity order — so the default keyset
    /// cap keeps the thin pair while prioritization keeps the deep pair.
    fn priority_fixture() -> (Vec<GammaEvent>, HashMap<String, ClobMarket>) {
        let events = vec![
            make_event_metrics("e1", "c1", "y1", "n1", 100.0, 50.0), // Illiquid
            make_event_metrics("e2", "c2", "y2", "n2", 200.0, 60.0), // Illiquid
            make_event_metrics("e3", "c3", "y3", "n3", 500_000.0, 300_000.0), // LiquidStable
            make_event_metrics("e4", "c4", "y4", "n4", 400_000.0, 250_000.0), // LiquidStable
        ];
        let clob: HashMap<String, ClobMarket> = [
            ("c1", "y1", "n1"),
            ("c2", "y2", "n2"),
            ("c3", "y3", "n3"),
            ("c4", "y4", "n4"),
        ]
        .iter()
        .map(|&(c, y, n)| (c.to_string(), ok_clob(c, y, n)))
        .collect();
        (events, clob)
    }

    fn conditions_of(u: &AssembledUniverse) -> Vec<String> {
        u.registry
            .markets()
            .iter()
            .map(|m| u.registry.market_condition(m.id).unwrap_or("").to_string())
            .collect()
    }

    #[test]
    fn default_path_keeps_keyset_order_not_priority() {
        // DEFAULT (prioritize_by_liquidity = false): the cap keeps the FIRST
        // max_markets in keyset order — the THIN c1, c2 — byte-identically to
        // pre-Task-5.3. Capped markets are never visited, so there are no skips.
        let (events, clob) = priority_fixture();
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
            ..UniverseFilter::default()
        };
        let u = assemble_registry(&events, &clob, "", &filter).unwrap();
        assert_eq!(
            conditions_of(&u),
            vec!["c1", "c2"],
            "default keeps the first max_markets in keyset order"
        );
        assert!(
            u.skipped.is_empty(),
            "capped markets are never visited → no skip entries"
        );
    }

    #[test]
    fn prioritized_keeps_deep_markets_and_drops_thin() {
        // OPT-IN prioritization with a pool covering all 4 candidates keeps the
        // DEEP LiquidStable markets (c3, c4) and drops the thin c1, c2 — the
        // OPPOSITE of the keyset default — capped at max_markets.
        let (events, clob) = priority_fixture();
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
            prioritize_by_liquidity: true,
            candidate_pool: 4,
            ..UniverseFilter::default()
        };
        let u = assemble_registry(&events, &clob, "", &filter).unwrap();
        // Kept markets are assembled in keyset order; the deep pair wins.
        assert_eq!(
            conditions_of(&u),
            vec!["c3", "c4"],
            "prioritization keeps the deep LiquidStable markets"
        );
        // The thin markets are recorded as dropped below the priority cut.
        let dropped: Vec<&str> = u
            .skipped
            .iter()
            .filter(|(_, r)| *r == SkippedReason::BelowPriorityCut)
            .map(|(c, _)| c.as_str())
            .collect();
        assert_eq!(
            dropped,
            vec!["c1", "c2"],
            "thin markets dropped as BelowPriorityCut"
        );
    }

    #[test]
    fn candidate_pool_bounds_candidates_considered() {
        let (events, clob) = priority_fixture();
        // A wide pool (4) considers every candidate → keeps the two deepest.
        let wide = UniverseFilter {
            max_markets: 2,
            require_active: true,
            prioritize_by_liquidity: true,
            candidate_pool: 4,
            ..UniverseFilter::default()
        };
        let u_wide = assemble_registry(&events, &clob, "", &wide).unwrap();
        assert!(
            conditions_of(&u_wide).contains(&"c4".to_string()),
            "c4 is considered (and kept) with a wide pool"
        );

        // A narrow pool (3) stops gathering after c1, c2, c3 — c4 is NEVER even
        // considered, even though it is deep. The cap then keeps c3 + the best of
        // the thin pair (c2, liquidity 60 > c1 liquidity 50).
        let narrow = UniverseFilter {
            max_markets: 2,
            require_active: true,
            prioritize_by_liquidity: true,
            candidate_pool: 3,
            ..UniverseFilter::default()
        };
        let u_narrow = assemble_registry(&events, &clob, "", &narrow).unwrap();
        let kept = conditions_of(&u_narrow);
        assert!(
            !kept.contains(&"c4".to_string()),
            "c4 is beyond the candidate pool → never considered"
        );
        assert_eq!(
            kept,
            vec!["c2", "c3"],
            "kept (keyset order) = c2 (best thin in pool) + c3 (deep)"
        );
    }

    #[test]
    fn effective_candidate_pool_sentinel_clamp_and_floor() {
        // 0 sentinel → equals max_markets (no extra fetching).
        let f = UniverseFilter {
            max_markets: 50,
            candidate_pool: 0,
            ..UniverseFilter::default()
        };
        assert_eq!(effective_candidate_pool(&f), 50);
        // An explicit pool ≥ max_markets (under the ceiling) is used as-is.
        let f = UniverseFilter {
            max_markets: 50,
            candidate_pool: 500,
            ..UniverseFilter::default()
        };
        assert_eq!(effective_candidate_pool(&f), 500);
        // Clamped down to the ceiling to bound API cost.
        let f = UniverseFilter {
            max_markets: 50,
            candidate_pool: 10_000,
            ..UniverseFilter::default()
        };
        assert_eq!(effective_candidate_pool(&f), MAX_CANDIDATE_POOL);
        // Never below max_markets, even when max_markets exceeds the ceiling.
        let f = UniverseFilter {
            max_markets: MAX_CANDIDATE_POOL + 100,
            candidate_pool: 0,
            ..UniverseFilter::default()
        };
        assert_eq!(effective_candidate_pool(&f), MAX_CANDIDATE_POOL + 100);
    }

    #[tokio::test]
    async fn bounded_fetch_covers_candidate_pool_when_prioritizing() {
        // Prioritization must fetch the WHOLE candidate pool so assembly can rank
        // over it; the default path still stops at max_markets. This is the
        // CLOB-fetch consistency / API-cost tradeoff in action.
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
        let stub = StubClob::new(vec![
            ok_clob("c1", "y1", "n1"),
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
            ok_clob("c4", "y4", "n4"),
        ]);
        let filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
            prioritize_by_liquidity: true,
            candidate_pool: 4,
            ..UniverseFilter::default()
        };
        let (map, failures) = fetch_clob_bounded(&stub, &events, &filter).await;
        assert_eq!(
            stub.calls_sorted(),
            vec!["c1", "c2", "c3", "c4"],
            "fetch covers the whole candidate pool when prioritizing"
        );
        assert_eq!(map.len(), 4);
        assert!(failures.is_empty());

        // Contrast: the DEFAULT path still stops at max_markets.
        let stub_default = StubClob::new(vec![
            ok_clob("c1", "y1", "n1"),
            ok_clob("c2", "y2", "n2"),
            ok_clob("c3", "y3", "n3"),
            ok_clob("c4", "y4", "n4"),
        ]);
        let default_filter = UniverseFilter {
            max_markets: 2,
            require_active: true,
            ..UniverseFilter::default()
        };
        let (_m, _f) = fetch_clob_bounded(&stub_default, &events, &default_filter).await;
        assert_eq!(
            stub_default.calls_sorted(),
            vec!["c1", "c2"],
            "default fetch still stops at max_markets"
        );
    }

    #[tokio::test]
    async fn bounded_fetch_runs_lookups_concurrently_bounded_by_limit() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // A fetch seam that records the PEAK number of simultaneously in-flight
        // lookups. The counter is bumped at entry (before the await), so the peak
        // is observed deterministically regardless of the timer.
        struct Probe {
            responses: HashMap<String, ClobMarket>,
            in_flight: AtomicUsize,
            max_in_flight: AtomicUsize,
        }
        impl ClobFetch for Probe {
            async fn market(&self, cid: &str) -> Result<ClobMarket, IngestError> {
                let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(cur, Ordering::SeqCst);
                // Hold the "request" open so concurrent lookups actually overlap.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                self.responses
                    .get(cid)
                    .cloned()
                    .ok_or_else(|| IngestError::Http("probe miss".into()))
            }
        }

        // 40 distinct acceptable markets > the concurrency limit, so the first
        // wave must saturate (but not exceed) it.
        let specs: Vec<(String, String, String)> = (0..40)
            .map(|i| (format!("c{i}"), format!("y{i}"), format!("n{i}")))
            .collect();
        let pairs: Vec<(&str, &str, &str)> = specs
            .iter()
            .map(|(c, y, n)| (c.as_str(), y.as_str(), n.as_str()))
            .collect();
        let events = vec![make_event_multi("e1", false, &pairs)];
        let responses: HashMap<String, ClobMarket> = specs
            .iter()
            .map(|(c, y, n)| (c.clone(), ok_clob(c, y, n)))
            .collect();
        let probe = Probe {
            responses,
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        };
        // Prioritized so the cap covers the whole pool → all 40 are fetched.
        let filter = UniverseFilter {
            max_markets: 40,
            require_active: true,
            prioritize_by_liquidity: true,
            candidate_pool: 100,
            ..UniverseFilter::default()
        };

        let (map, failures) = fetch_clob_bounded(&probe, &events, &filter).await;

        assert_eq!(map.len(), 40, "all acceptable markets fetched");
        assert!(failures.is_empty());
        assert_eq!(
            probe.max_in_flight.load(Ordering::SeqCst),
            CLOB_FETCH_CONCURRENCY,
            "the first wave runs lookups concurrently, saturating but not exceeding the limit"
        );
    }

    #[test]
    fn confluence_events_wraps_each_market_and_lifts_metrics() {
        // Two markets exactly as Gamma `/markets?condition_ids=` returns them:
        // string `volume`/`liquidity`, the second omitting `liquidity`.
        let body = r#"[
          {"conditionId":"0xAAA","clobTokenIds":"[\"111\",\"222\"]","active":true,
           "closed":false,"negRisk":false,"question":"Q1?","volume":"50000.5",
           "volume24hr":1234.0,"liquidity":"9000.25"},
          {"conditionId":"0xBBB","clobTokenIds":"[\"333\",\"444\"]","active":true,
           "closed":false,"negRisk":true,"question":"Q2?","volume":2000.0}
        ]"#;
        let markets: Vec<GammaMarket> = serde_json::from_str(body).unwrap();
        let events = confluence_events(markets);

        assert_eq!(events.len(), 2, "one synthetic single-market event per market");
        // Order preserved; each event holds exactly its one market.
        assert_eq!(events[0].id, "0xAAA");
        assert_eq!(events[0].markets.len(), 1);
        assert_eq!(events[0].markets[0].condition_id, "0xAAA");
        // Market metrics lifted to the EVENT level for the Phase-5 segment fallback.
        assert_eq!(events[0].volume, Some(50000.5));
        assert_eq!(events[0].liquidity, Some(9000.25));
        assert!(!events[0].neg_risk);
        // Second market: neg_risk preserved, absent liquidity stays None.
        assert_eq!(events[1].id, "0xBBB");
        assert!(events[1].neg_risk);
        assert_eq!(events[1].volume, Some(2000.0));
        assert_eq!(events[1].liquidity, None);
    }
}
