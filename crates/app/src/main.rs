//! arb — headless paper-trading session (M3, spec §22).
//! Usage: arb [--config <path>] [--duration-secs N] [--max-markets N]
//!            [--relationships <path>] [--db <path>]
//! duration-secs 0 (default) = run until SIGINT/kill switch.
//! Exit codes: 0 healthy session, 1 startup error, 2 unhealthy.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{info, warn};

use pm_app::coordinator::{Coordinator, now_ms, run_execution};
use pm_app::detector::Detector;
use pm_app::kill::spawn_kill_watch;
use pm_app::lp_pool::run_lp_pool;
use pm_app::stats::AppStats;
use pm_app::wiring::{
    BookFetcher, build_component_index, engine_params, fee_map, pack_components, risk_config,
    token_maps,
};
use pm_config::Config;
use pm_core::instrument::Relationship;
use pm_execution::basket::ExecParams;
use pm_execution::venue::PaperVenue;
use pm_ingestion::rest::ClobRest;
use pm_ingestion::stats::StatsCell;
use pm_ingestion::supervisor::{FactoryDecision, Supervisor, SupervisorCommand, SupervisorConfig};
use pm_ingestion::sync::{AssembledUniverse, SyncTask, UniverseFilter};
use pm_ingestion::ws::TungsteniteTransport;
use pm_store::writer::{StoreMsg, run_writer};
use pm_store::{MarketRow, RelRow, Store};

// ---------------------------------------------------------------------------
// CLI argument parsing (plain std::env::args, mirroring probe)
// ---------------------------------------------------------------------------

struct Args {
    config_path: Option<PathBuf>,
    duration_secs: u64,
    max_markets: Option<usize>,
    relationships_path: Option<PathBuf>,
    db: Option<PathBuf>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut args = std::env::args().skip(1).peekable();
        let mut config_path: Option<PathBuf> = None;
        let mut duration_secs: u64 = 0;
        let mut max_markets: Option<usize> = None;
        let mut relationships_path: Option<PathBuf> = None;
        let mut db: Option<PathBuf> = None;

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
                "--db" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--db requires a value".to_string())?;
                    db = Some(PathBuf::from(v));
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        Ok(Args {
            config_path,
            duration_secs,
            max_markets,
            relationships_path,
            db,
        })
    }
}

fn fatal(msg: impl std::fmt::Display) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(1);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => fatal(e),
    };

    // ---- config load + validate + CLI overrides ----------------------------
    let mut config = match &args.config_path {
        Some(path) => {
            let src = std::fs::read_to_string(path)
                .unwrap_or_else(|e| fatal(format!("failed to read config {}: {e}", path.display())));
            Config::from_toml_str(&src).unwrap_or_else(|e| fatal(format!("config parse failed: {e}")))
        }
        None => Config::default(),
    };
    if let Some(mm) = args.max_markets {
        config.universe.max_markets = mm;
    }
    if let Some(ref rp) = args.relationships_path {
        config.ingestion.relationships_path = rp.display().to_string();
    }
    if let Some(ref db) = args.db {
        config.store.path = db.display().to_string();
    }
    if let Err(e) = config.validate() {
        fatal(format!("config validation failed: {e}"));
    }

    // ---- store open + reconciliation (BEFORE anything else) ----------------
    let mut store = Store::open(Path::new(&config.store.path))
        .unwrap_or_else(|e| fatal(format!("store open {}: {e}", config.store.path)));

    let session_window_ms = (config.risk.restart_storm_window_s * 1000) as i64;
    let starts = store
        .record_session_start(now_ms(), session_window_ms)
        .unwrap_or_else(|e| fatal(format!("record_session_start: {e}")));
    info!(
        session_starts_in_window = starts,
        "session start recorded"
    );

    // Reconcile any orders left open by a previous crashed session.
    let open = store
        .open_orders()
        .unwrap_or_else(|e| fatal(format!("open_orders: {e}")));
    let reconciled = open.len();
    for (id, state) in &open {
        info!(order = %id, state = %state, "reconciling stranded open order → expired");
        store
            .expire_order(id, now_ms())
            .unwrap_or_else(|e| fatal(format!("expire_order {id}: {e}")));
    }
    info!(reconciled, "reconciliation complete");

    // ---- sync_once: assemble universe --------------------------------------
    let (tx, _rx) = tokio::sync::watch::channel(Arc::new(
        pm_registry::RegistryBuilder::default()
            .finish("")
            .expect("empty registry"),
    ));

    let clob_for_sync = ClobRest::new(
        &config.endpoints.clob_base,
        config.ingestion.rest_rate_capacity,
        config.ingestion.rest_rate_per_sec,
    )
    .unwrap_or_else(|e| fatal(format!("ClobRest init: {e}")));

    let filter = UniverseFilter {
        max_markets: config.universe.max_markets,
        require_active: config.universe.require_active,
    };
    let mut sync_task = SyncTask::new(
        clob_for_sync,
        &config.endpoints.gamma_base,
        PathBuf::from(&config.ingestion.relationships_path),
        filter,
        tx,
    )
    .unwrap_or_else(|e| fatal(format!("SyncTask init: {e}")));

    info!("running sync_once to assemble universe ...");
    let universe: AssembledUniverse = sync_task
        .sync_once()
        .await
        .unwrap_or_else(|e| fatal(format!("sync_once: {e}")));

    // AssembledUniverse.registry is Arc<Registry>; clone the Arc so it outlives
    // the supervisors / detectors that capture it.
    let reg = Arc::clone(&universe.registry);

    let component_ids: std::collections::HashSet<_> =
        reg.markets().iter().map(|m| reg.component_of(m.id)).collect();
    info!(
        markets = reg.markets().len(),
        tokens = reg.all_tokens().len(),
        partitions = reg.partitions().len(),
        components = component_ids.len(),
        exclusions = reg.exclusion_log().len(),
        skipped = universe.skipped.len(),
        unresolved = reg.unresolved_relationships().len(),
        "universe assembled"
    );

    // ---- store the universe (directly, BEFORE spawning the writer) ---------
    for m in reg.markets() {
        let condition_id = reg.market_condition(m.id).unwrap_or("").to_string();
        let row = MarketRow {
            id: m.id.0 as i64,
            condition_id,
            tick_levels: m.tick.levels() as i64,
            fee_bps: m.fee_bps.0 as i64,
            neg_risk: m.neg_risk,
        };
        if let Err(e) = store.upsert_market(&row) {
            warn!("upsert_market {}: {e}", m.id.0);
        }
    }
    for r in reg.approved_relationships() {
        let (kind, a, b) = match *r {
            Relationship::Implies { a, b } => ("implies", a, b),
            Relationship::MutuallyExclusive { a, b } => ("mutuallyexclusive", a, b),
            Relationship::Equivalent { a, b } => ("equivalent", a, b),
        };
        let row = RelRow {
            kind: kind.to_string(),
            a: a.0 as i64,
            b: b.0 as i64,
            status: "approved".to_string(),
        };
        if let Err(e) = store.upsert_relationship(&row) {
            warn!("upsert_relationship {kind}: {e}");
        }
    }

    // ---- spawn the writer ---------------------------------------------------
    let (store_tx, store_rx) = mpsc::channel::<StoreMsg>(4096);
    let writer = tokio::spawn(run_writer(store, store_rx));

    // ---- wiring -------------------------------------------------------------
    let params = engine_params(&config).unwrap_or_else(|e| fatal(format!("engine_params: {e}")));
    let risk_cfg = risk_config(&config).unwrap_or_else(|e| fatal(format!("risk_config: {e}")));
    let (token_market, market_tokens) = token_maps(&reg);
    let token_fee = fee_map(&reg);
    let index = Arc::new(build_component_index(&reg));
    let chunk_size = config.ingestion.ws_chunk_size;
    let chunks = pack_components(&reg, chunk_size);

    // ---- channels + shared state -------------------------------------------
    let (opp_tx, opp_rx) = mpsc::channel(1024);
    let (lp_tx, lp_rx) = mpsc::channel(64);
    let (exec_tx, exec_rx) = mpsc::channel(4);
    let (report_tx, report_rx) = mpsc::channel(4);
    let kill = Arc::new(AtomicBool::new(false));
    let stats = AppStats::new();

    // LP pool's opp sender, cloned BEFORE main's opp_tx is dropped.
    let opp_tx_lp = opp_tx.clone();

    // ---- supervisors per chunk ---------------------------------------------
    let ws_url = config.endpoints.ws_market_url.clone();
    let clob_base = config.endpoints.clob_base.clone();
    let rest_rate_capacity = config.ingestion.rest_rate_capacity;
    let rest_rate_per_sec = config.ingestion.rest_rate_per_sec;
    let sup_cfg = SupervisorConfig {
        staleness: Duration::from_millis(config.ingestion.staleness_ms),
        feed_silence: Duration::from_millis(config.ingestion.feed_silence_ms),
        backoff_base: Duration::from_millis(config.ingestion.backoff_base_ms),
        backoff_cap: Duration::from_millis(config.ingestion.backoff_cap_ms),
        sweep_interval: Duration::from_millis(config.ingestion.sweep_interval_ms),
    };
    let lp_min_interval = Duration::from_millis(config.lp.min_resolve_interval_ms);

    let shutdown = Arc::new(AtomicBool::new(false));
    let mut stat_cells: Vec<Arc<StatsCell>> = Vec::new();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut routes: HashMap<pm_core::instrument::TokenId, mpsc::Sender<SupervisorCommand>> =
        HashMap::new();

    for chunk in &chunks {
        if chunk.len() > chunk_size {
            warn!(
                chunk_tokens = chunk.len(),
                chunk_size, "oversized component chunk (single component exceeds ws_chunk_size)"
            );
        }

        let token_triples: Vec<_> = chunk
            .iter()
            .filter_map(|&tok| {
                let venue_id = reg.token_venue_id(tok)?.to_owned();
                let tick = reg.tick_of(tok)?;
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

        let mut sup = Supervisor::new(token_triples, rest, sup_cfg.clone());

        // Command channel: route every token in this chunk to this supervisor.
        let cmd_tx = sup.command_channel(32);
        for &tok in chunk {
            routes.insert(tok, cmd_tx.clone());
        }

        // Install the detector hook.
        let mut det = Detector::new(
            Arc::clone(&index),
            params,
            opp_tx.clone(),
            lp_tx.clone(),
            lp_min_interval,
            Arc::clone(&stats),
        );
        sup.set_on_apply(Box::new(move |t, shard| det.on_apply(t, shard)));

        stat_cells.push(sup.share_stats());

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

    // Drop main's detector-side senders so channel closure cascades on shutdown.
    // The LP pool holds opp_tx_lp (cloned above); the detectors hold their own
    // clones inside the supervisor tasks.
    drop(opp_tx);
    drop(lp_tx);

    if handles.is_empty() {
        eprintln!("error: no supervisors started (empty universe?)");
        // Tear down the writer cleanly before exiting.
        drop(store_tx);
        let _ = writer.await;
        std::process::exit(1);
    }
    let supervisors_started = handles.len();
    info!(supervisors = supervisors_started, "supervisors spawned");

    // ---- book fetcher + LP pool --------------------------------------------
    let fetcher = BookFetcher::new(routes);
    let lp_handle = tokio::spawn(run_lp_pool(
        lp_rx,
        opp_tx_lp,
        params,
        config.lp.solver_concurrency,
        Arc::clone(&stats),
    ));

    // ---- execution task -----------------------------------------------------
    let venue = PaperVenue::new(
        fetcher.clone(),
        Duration::from_millis(config.execution.paper_latency_ms),
        params.gas,
    );
    let exec_params = ExecParams {
        fill_window: Duration::from_millis(config.execution.fill_window_ms),
        max_unhedged: risk_cfg.max_unhedged,
        redeem: params.redeem,
    };
    let exec_handle = tokio::spawn(run_execution(
        venue,
        exec_rx,
        report_tx,
        store_tx.clone(),
        token_market.clone(),
        market_tokens,
        token_fee,
        exec_params,
    ));

    // ---- coordinator --------------------------------------------------------
    let mut coord = Coordinator::new(
        &config,
        risk_cfg,
        params,
        token_market,
        fetcher,
        opp_rx,
        exec_tx,
        report_rx,
        store_tx.clone(),
        Arc::clone(&kill),
        Arc::clone(&stats),
    )
    .unwrap_or_else(|e| fatal(format!("Coordinator::new: {e}")));
    coord.note_session_starts(starts);
    let coord_handle = tokio::spawn(coord.run());

    // ---- kill watch ---------------------------------------------------------
    let kill_handle = spawn_kill_watch(PathBuf::from(&config.risk.kill_file), Arc::clone(&kill));

    // RestartStorm at startup: a halt was logged synchronously by
    // note_session_starts. Detect by re-checking the restart count vs config.
    let restart_storm =
        starts >= config.risk.restart_storm_count as usize && config.risk.restart_storm_count > 0;
    if restart_storm {
        warn!(
            starts,
            limit = config.risk.restart_storm_count,
            "restart storm detected at startup"
        );
    }

    // ---- main loop ----------------------------------------------------------
    let start = Instant::now();
    let duration = Duration::from_secs(args.duration_secs);
    let run_forever = args.duration_secs == 0;
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let trigger;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                log_stats(&stats, &stat_cells, start.elapsed());
                if !run_forever && start.elapsed() >= duration {
                    trigger = "duration";
                    break;
                }
                if kill.load(Ordering::Acquire) {
                    trigger = "kill";
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                trigger = "ctrl_c";
                break;
            }
        }
    }
    info!(trigger, elapsed_s = start.elapsed().as_secs(), "shutdown initiated");

    // ---- shutdown cascade ---------------------------------------------------
    shutdown.store(true, Ordering::Release);
    // Grace: let supervisors notice the flag on next reconnect, then abort.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for h in &handles {
        h.abort();
    }
    for h in handles {
        let _ = h.await;
    }
    // Supervisors dropped → detectors dropped → opp/lp senders close → LP pool
    // exits → its opp_tx clone drops → coordinator's opp_rx closes → drains.
    let _ = lp_handle.await;
    let summary = match coord_handle.await {
        Ok(s) => s,
        Err(e) => fatal(format!("coordinator task join: {e}")),
    };
    // Coordinator dropped exec_tx → execution task's rx closes → it ends.
    let _ = exec_handle.await;
    kill_handle.abort();

    // Drop main's writer sender LAST so all StoreMsg producers are gone.
    drop(store_tx);
    let store = match writer.await {
        Ok(s) => s,
        Err(e) => fatal(format!("writer task join: {e}")),
    };

    // ---- final report -------------------------------------------------------
    let realized = store.realized_total().unwrap_or(0);
    let opportunities = store.count_opportunities().unwrap_or(0);
    let fills = store.count_fills().unwrap_or(0);
    let halts = store.count_halts().unwrap_or(0);
    let write_errors = store.write_errors;

    info!(
        cash_micro = summary.cash.0,
        equity_micro = summary.equity.0,
        open_positions = summary.open_positions,
        "session summary"
    );
    info!(
        realized_micro = realized,
        opportunities, fills, halts, write_errors, "session counts"
    );
    info!("FINAL stats: {}", stats.line());
    info!(duration_s = start.elapsed().as_secs(), "session ended");

    let healthy = write_errors == 0 && !restart_storm && supervisors_started > 0;
    info!(healthy, "arb session result");
    if healthy {
        std::process::exit(0);
    } else {
        std::process::exit(2);
    }
}

/// Log app stats plus aggregated ingest gauges across every supervisor cell.
fn log_stats(stats: &AppStats, cells: &[Arc<StatsCell>], elapsed: Duration) {
    let mut books = 0u64;
    let mut frames = 0u64;
    let mut reconnects = 0u64;
    for cell in cells {
        books += cell.books.load(Ordering::Relaxed);
        frames += cell.frames.load(Ordering::Relaxed);
        reconnects += cell.reconnects.load(Ordering::Relaxed);
    }
    info!(
        elapsed_s = elapsed.as_secs(),
        books, frames, reconnects, "{}",
        stats.line()
    );
}
