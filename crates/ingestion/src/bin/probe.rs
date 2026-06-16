//! M2 acceptance probe: spins up supervisors, collects stats, reports health.
//!
//! # Usage
//!
//! ```text
//! probe [--config <path>] [--duration-secs N] [--max-markets N] [--relationships <path>]
//! ```
//!
//! Exits with code 0 when [`ProbeStats::healthy`] is true at the end of the
//! run, or code 2 when unhealthy. Code 1 is reserved for startup errors.
//!
//! # Network
//!
//! This binary makes live network calls (CLOB REST + WS). It must NOT be
//! invoked by `cargo test` — it is the M2 acceptance instrument only.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use pm_config::Config;
use pm_ingestion::{
    rest::ClobRest,
    stats::{ProbeStats, StatsCell},
    supervisor::{FactoryDecision, Supervisor, SupervisorConfig},
    sync::{AssembledUniverse, SyncTask, UniverseFilter},
    ws::TungsteniteTransport,
};
use pm_registry::segment::SegmentThresholds;

// ---------------------------------------------------------------------------
// CLI argument parsing (plain std::env::args)
// ---------------------------------------------------------------------------

struct Args {
    config_path: Option<PathBuf>,
    duration_secs: u64,
    max_markets: Option<usize>,
    relationships_path: Option<PathBuf>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut args = std::env::args().skip(1).peekable();
        let mut config_path: Option<PathBuf> = None;
        let mut duration_secs: u64 = 120;
        let mut max_markets: Option<usize> = None;
        let mut relationships_path: Option<PathBuf> = None;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--config requires a value".to_string())?;
                    config_path = Some(PathBuf::from(v));
                }
                "--duration-secs" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--duration-secs requires a value".to_string())?;
                    duration_secs = v
                        .parse::<u64>()
                        .map_err(|e| format!("--duration-secs: {e}"))?;
                }
                "--max-markets" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--max-markets requires a value".to_string())?;
                    max_markets = Some(
                        v.parse::<usize>()
                            .map_err(|e| format!("--max-markets: {e}"))?,
                    );
                }
                "--relationships" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--relationships requires a value".to_string())?;
                    relationships_path = Some(PathBuf::from(v));
                }
                other => {
                    return Err(format!("unknown argument: {other}"));
                }
            }
        }

        Ok(Args {
            config_path,
            duration_secs,
            max_markets,
            relationships_path,
        })
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // ---- tracing init -------------------------------------------------------
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    // ---- config load --------------------------------------------------------
    let mut config = match &args.config_path {
        Some(path) => {
            let src = match std::fs::read_to_string(path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: failed to read config {}: {e}", path.display());
                    std::process::exit(1);
                }
            };
            match Config::from_toml_str(&src) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: config parse failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        None => Config::default(),
    };

    // Apply CLI flag overrides.
    if let Some(mm) = args.max_markets {
        config.universe.max_markets = mm;
    }
    if let Some(ref rp) = args.relationships_path {
        config.ingestion.relationships_path = rp.display().to_string();
    }

    // ---- sync once: assemble universe ---------------------------------------
    let (tx, _rx) = tokio::sync::watch::channel(Arc::new({
        // Use a stub registry for the watch channel until sync completes.
        // We re-publish below after sync_once returns.
        pm_registry::RegistryBuilder::default()
            .finish("")
            .expect("empty registry")
    }));

    let clob_for_sync = match ClobRest::new(
        &config.endpoints.clob_base,
        config.ingestion.rest_rate_capacity,
        config.ingestion.rest_rate_per_sec,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: ClobRest init failed: {e}");
            std::process::exit(1);
        }
    };

    let filter = UniverseFilter {
        max_markets: config.universe.max_markets,
        require_active: config.universe.require_active,
        // Task 5.3 universe scaling knobs (opt-in; defaults keep keyset order).
        prioritize_by_liquidity: config.universe.prioritize_by_liquidity,
        candidate_pool: config.universe.candidate_pool,
        // Mirror `[segments]` thresholds (kept inline to avoid a pm-app dep).
        segment_thresholds: SegmentThresholds {
            liquid_stable_min_volume: config.segments.liquid_stable_min_volume,
            liquid_stable_min_liquidity: config.segments.liquid_stable_min_liquidity,
            liquid_min_volume: config.segments.liquid_min_volume,
            liquid_min_liquidity: config.segments.liquid_min_liquidity,
        },
    };

    let mut sync_task = match SyncTask::new(
        clob_for_sync,
        &config.endpoints.gamma_base,
        PathBuf::from(&config.ingestion.relationships_path),
        filter,
        tx,
    ) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: SyncTask init failed: {e}");
            std::process::exit(1);
        }
    };

    info!("running sync_once to assemble universe ...");
    let universe: AssembledUniverse = match sync_task.sync_once().await {
        Ok(u) => u,
        Err(e) => {
            eprintln!("error: sync_once failed: {e}");
            std::process::exit(1);
        }
    };

    let registry = &universe.registry;
    info!(
        markets = registry.markets().len(),
        tokens = registry.all_tokens().len(),
        partitions = registry.partitions().len(),
        "universe assembled"
    );

    // Log partitions verified vs total.
    let verified = registry
        .partitions()
        .iter()
        .filter(|p| p.verified_exhaustive)
        .count();
    info!(
        verified = verified,
        total = registry.partitions().len(),
        "partitions (verified/total)"
    );

    // Count distinct components.
    let component_ids: std::collections::HashSet<_> = registry
        .markets()
        .iter()
        .map(|m| registry.component_of(m.id))
        .collect();
    info!(
        components = component_ids.len(),
        "distinct market components"
    );

    // Log exclusion entries.
    for (event_id, reason) in registry.exclusion_log() {
        info!(event = ?event_id, reason = ?reason, "partition exclusion");
    }

    // Log skipped markets summary (count per reason).
    {
        use std::collections::HashMap;
        let mut counts: HashMap<String, usize> = HashMap::new();
        for (_, reason) in &universe.skipped {
            *counts.entry(format!("{reason:?}")).or_insert(0) += 1;
        }
        for (reason, count) in &counts {
            info!(reason = %reason, count = %count, "skipped markets");
        }
    }

    // Log unresolved relationships.
    for (kind, a, b) in registry.unresolved_relationships() {
        info!(kind = %kind, a = %a, b = %b, "unresolved relationship");
    }

    // ---- build supervisors -------------------------------------------------
    let all_tokens = registry.all_tokens();
    let chunk_size = config.ingestion.ws_chunk_size;

    info!(
        total_tokens = all_tokens.len(),
        chunk_size = chunk_size,
        "building supervisors"
    );

    let ws_url = config.endpoints.ws_market_url.clone();
    let clob_base = config.endpoints.clob_base.clone();
    let rest_rate_capacity = config.ingestion.rest_rate_capacity;
    let rest_rate_per_sec = config.ingestion.rest_rate_per_sec;
    let staleness = Duration::from_millis(config.ingestion.staleness_ms);
    let feed_silence = Duration::from_millis(config.ingestion.feed_silence_ms);
    let backoff_base = Duration::from_millis(config.ingestion.backoff_base_ms);
    let backoff_cap = Duration::from_millis(config.ingestion.backoff_cap_ms);
    let sweep_interval = Duration::from_millis(config.ingestion.sweep_interval_ms);

    // Shutdown flag: set to true after duration elapses.
    let shutdown = Arc::new(AtomicBool::new(false));

    let mut stat_cells: Vec<Arc<StatsCell>> = Vec::new();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for chunk in all_tokens.chunks(chunk_size) {
        // Build token triples for this chunk.
        let token_triples: Vec<_> = chunk
            .iter()
            .filter_map(|&tok| {
                let venue_id = registry.token_venue_id(tok)?.to_owned();
                let tick = registry.tick_of(tok)?;
                Some((tok, venue_id, tick))
            })
            .collect();

        if token_triples.is_empty() {
            continue;
        }

        let rest = match ClobRest::new(&clob_base, rest_rate_capacity, rest_rate_per_sec) {
            Ok(r) => r,
            Err(e) => {
                warn!("ClobRest init failed for chunk: {e}; skipping chunk");
                continue;
            }
        };

        let sup_cfg = SupervisorConfig {
            staleness,
            feed_silence,
            backoff_base,
            backoff_cap,
            sweep_interval,
        };

        let mut sup = Supervisor::new(token_triples, rest, sup_cfg);
        let cell = sup.share_stats();
        stat_cells.push(cell);

        let ws_url_clone = ws_url.clone();
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = tokio::spawn(async move {
            sup.run(move || {
                let url = ws_url_clone.clone();
                let is_shutdown = shutdown_clone.load(Ordering::Acquire);
                async move {
                    if is_shutdown {
                        return Ok(FactoryDecision::Stop);
                    }
                    match TungsteniteTransport::connect(&url).await {
                        Ok(t) => Ok(FactoryDecision::Connect(t)),
                        Err(e) => Err(e),
                    }
                }
            })
            .await;
        });
        handles.push(handle);
    }

    if handles.is_empty() {
        eprintln!("error: no supervisors started (empty universe?)");
        std::process::exit(1);
    }

    info!(supervisors = handles.len(), "supervisors spawned");

    // ---- main stats loop ---------------------------------------------------
    let start = Instant::now();
    let duration = Duration::from_secs(args.duration_secs);
    let mut probe_stats = ProbeStats::new();

    loop {
        // Drain histogram samples from each StatsCell into probe_stats.
        for cell in &stat_cells {
            // Drain parse histogram.
            if let Ok(mut h) = cell.recv_to_parsed_us.lock() {
                // Iterate recorded values and feed them into probe_stats histograms.
                for v in h.iter_recorded() {
                    for _ in 0..v.count_at_value() {
                        probe_stats.record_recv_to_parsed_us(v.value_iterated_to());
                    }
                }
                h.reset();
            }
            if let Ok(mut h) = cell.parsed_to_applied_us.lock() {
                for v in h.iter_recorded() {
                    for _ in 0..v.count_at_value() {
                        probe_stats.record_parsed_to_applied_us(v.value_iterated_to());
                    }
                }
                h.reset();
            }
        }

        // Aggregate gauge fields.
        probe_stats.reset_gauges();
        for cell in &stat_cells {
            let sup_snap = cell.snapshot_stats();
            let books = cell.books.load(Ordering::Relaxed) as usize;
            let stale = cell.stale.load(Ordering::Relaxed) as usize;
            probe_stats.absorb_supervisor(&sup_snap, books, stale);
        }

        let elapsed = start.elapsed();
        let line = probe_stats.line(elapsed);
        info!("{line}");

        if elapsed >= duration {
            break;
        }

        // Print every 10s.
        let remaining = duration.saturating_sub(elapsed);
        tokio::time::sleep(remaining.min(Duration::from_secs(10))).await;
    }

    // ---- shutdown -------------------------------------------------------
    shutdown.store(true, Ordering::Release);

    // Grace period: give supervisors 2s to notice the shutdown flag on next
    // reconnect, then abort.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for h in handles {
        h.abort();
    }

    // ---- final summary --------------------------------------------------
    // One final reset_gauges+absorb+histogram-drain cycle so the verdict reflects
    // the post-shutdown state rather than the last mid-run snapshot.
    {
        // Drain histograms.
        for cell in &stat_cells {
            if let Ok(mut h) = cell.recv_to_parsed_us.lock() {
                for v in h.iter_recorded() {
                    for _ in 0..v.count_at_value() {
                        probe_stats.record_recv_to_parsed_us(v.value_iterated_to());
                    }
                }
                h.reset();
            }
            if let Ok(mut h) = cell.parsed_to_applied_us.lock() {
                for v in h.iter_recorded() {
                    for _ in 0..v.count_at_value() {
                        probe_stats.record_parsed_to_applied_us(v.value_iterated_to());
                    }
                }
                h.reset();
            }
        }
        // Re-absorb gauges from the final cell state.
        probe_stats.reset_gauges();
        for cell in &stat_cells {
            let sup_snap = cell.snapshot_stats();
            let books = cell.books.load(Ordering::Acquire) as usize;
            let stale = cell.stale.load(Ordering::Acquire) as usize;
            probe_stats.absorb_supervisor(&sup_snap, books, stale);
        }
    }

    let final_line = probe_stats.line(start.elapsed());
    info!("FINAL: {final_line}");
    let healthy = probe_stats.healthy();
    info!(healthy = healthy, "probe result");

    if healthy {
        std::process::exit(0);
    } else {
        std::process::exit(2);
    }
}
