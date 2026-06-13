//! arb — paper-trading session (M3 engine + M4 live TUI dashboard, spec §22/§17).
//! Usage: arb [--config <path>] [--duration-secs N] [--max-markets N]
//!            [--relationships <path>] [--db <path>] [--headless]
//! duration-secs 0 (default) = run until SIGINT/kill switch.
//!
//! TUI: by default arb launches the interactive dashboard when stdout is a
//! terminal. `--headless` forces M3-style fmt logging to stdout. The TUI is
//! ALSO skipped automatically when stdout is not a terminal (piped/redirected),
//! so cron/CI invocations stay headless without the flag.
//!
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
use tracing::{error, info, warn};

use pm_app::coordinator::{Coordinator, LiveParams, now_ms, run_execution};
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
use pm_engine::RedeemStrategy;
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
    headless: bool,
    /// Live trading (real money). A START-TIME decision — paper and live fills
    /// never mix in one session. Forces `mode.paper = false`.
    live: bool,
    /// Dry-run live: sign every order but never submit (no network, no money).
    /// Requires `--live`.
    shadow: bool,
}

impl Args {
    fn parse() -> Result<Self, String> {
        Self::from_iter(std::env::args().skip(1))
    }

    /// Parse from an explicit argument iterator (testable core; `parse()` feeds
    /// it `std::env::args().skip(1)`).
    fn from_iter(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = args.peekable();
        let mut config_path: Option<PathBuf> = None;
        let mut duration_secs: u64 = 0;
        let mut max_markets: Option<usize> = None;
        let mut relationships_path: Option<PathBuf> = None;
        let mut db: Option<PathBuf> = None;
        let mut headless = false;
        let mut live = false;
        let mut shadow = false;

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
                "--headless" => {
                    headless = true;
                }
                "--live" => {
                    live = true;
                }
                "--shadow" => {
                    shadow = true;
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        // --shadow is a modifier on --live (sign-but-don't-submit); on its own it
        // is meaningless and almost certainly a mistake — refuse rather than
        // silently run paper.
        if shadow && !live {
            return Err("--shadow requires --live".to_string());
        }

        Ok(Args {
            config_path,
            duration_secs,
            max_markets,
            relationships_path,
            db,
            headless,
            live,
            shadow,
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
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => fatal(e),
    };

    // Mode decision: the TUI engages only when not forced headless AND stdout is
    // an actual terminal. A piped/redirected stdout (cron/CI) reports false here,
    // so the dashboard is skipped automatically without --headless.
    use std::io::IsTerminal;
    let tui_active = !args.headless && std::io::stdout().is_terminal();

    // Tracing init — the EnvFilter MUST be the FIRST layer so debug/trace events
    // are discarded before they ever reach the ring buffer's lock. In TUI mode
    // the fmt (stdout) layer is replaced by the ring layer: stdout writes would
    // corrupt the alternate screen.
    let logbuf = pm_app::logbuf::LogBuffer::new(512);
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if tui_active {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(env_filter)
            .with(pm_app::logbuf::RingLayer::new(std::sync::Arc::clone(
                &logbuf,
            )))
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    // ---- config load + validate + CLI overrides ----------------------------
    let mut config = match &args.config_path {
        Some(path) => {
            let src = std::fs::read_to_string(path).unwrap_or_else(|e| {
                fatal(format!("failed to read config {}: {e}", path.display()))
            });
            Config::from_toml_str(&src)
                .unwrap_or_else(|e| fatal(format!("config parse failed: {e}")))
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

    // ---- live arming (spec 2026-06-13 §Mode ladder) -------------------------
    // Live is a START-TIME decision: paper and live fills never mix in a
    // session. --shadow signs but never submits (no confirm: no money moves).
    // This block runs BEFORE the alternate screen (started much later) and
    // eprintln/stdin go to the controlling terminal, so the typed-confirm
    // prompt is safe here even on the TUI path (where it is skipped anyway).
    if args.live {
        config.mode.paper = false;
    }
    let live_rt = if args.live {
        let secrets = pm_execution::secrets::LiveSecrets::from_env()
            .unwrap_or_else(|e| fatal(format!("live secrets: {e}")));
        let signer: alloy_signer_local::PrivateKeySigner = secrets
            .private_key
            .expose_key_hex()
            .parse()
            .unwrap_or_else(|e| fatal(format!("PM_PRIVATE_KEY invalid: {e}")));
        let proxy: alloy_primitives::Address = match secrets.proxy_address.as_deref() {
            Some(p) => p
                .parse()
                .unwrap_or_else(|e| fatal(format!("PM_PROXY_ADDRESS invalid: {e}"))),
            None => fatal(
                "PM_PROXY_ADDRESS not set — copy your Polymarket proxy wallet \
                 address from the profile page (docs/RECON-M5.md §Magic/email)",
            ),
        };
        // V2 deposit-wallet flow (RECON-M5-V2-1271): new accounts trade via the
        // deposit wallet (the order maker, signatureType 3). Required in live.
        let deposit_wallet: alloy_primitives::Address = match secrets.deposit_wallet.as_deref() {
            Some(d) => d
                .parse()
                .unwrap_or_else(|e| fatal(format!("PM_DEPOSIT_WALLET invalid: {e}"))),
            None => fatal(
                "PM_DEPOSIT_WALLET not set — your Polymarket deposit-wallet \
                 address (the smart-contract wallet holding your funds; see \
                 docs/RECON-M5-V2-1271.md). New V2 accounts must trade via the \
                 deposit wallet.",
            ),
        };
        // Cross-check identities at startup. Addresses are public — no secrets.
        info!(eoa = %signer.address(), deposit_wallet = %deposit_wallet, "live identities");
        // Headless live trades real money on startup: demand the typed phrase.
        // The TUI path confirms via the `l` modal instead (release latch).
        if !args.shadow && !tui_active {
            eprintln!(
                "LIVE MODE — type the confirmation phrase to continue:\n  {}",
                config.live.confirm_phrase
            );
            let mut line = String::new();
            if std::io::stdin().read_line(&mut line).is_err()
                || line.trim() != config.live.confirm_phrase
            {
                fatal("confirmation phrase mismatch — refusing to trade live");
            }
        }
        Some((secrets, signer, proxy, deposit_wallet))
    } else {
        None
    };

    // ---- store open + reconciliation (BEFORE anything else) ----------------
    let mut store = Store::open(Path::new(&config.store.path))
        .unwrap_or_else(|e| fatal(format!("store open {}: {e}", config.store.path)));

    let session_window_ms = (config.risk.restart_storm_window_s * 1000) as i64;
    let starts = store
        .record_session_start(now_ms(), session_window_ms)
        .unwrap_or_else(|e| fatal(format!("record_session_start: {e}")));
    info!(session_starts_in_window = starts, "session start recorded");

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

    // In TUI mode tracing already goes to the ring buffer, so without this
    // line the user stares at a silent blank terminal for the whole sync
    // (rate-limited CLOB lookups — typically seconds, minutes if degraded).
    // The alternate screen is not entered yet, so plain stdout is safe.
    if tui_active {
        println!("arb: assembling market universe (rate-limited CLOB sync) ...");
        println!("arb: the dashboard will start when the sync completes.");
    }
    info!("running sync_once to assemble universe ...");
    let universe: AssembledUniverse = sync_task
        .sync_once()
        .await
        .unwrap_or_else(|e| fatal(format!("sync_once: {e}")));

    // AssembledUniverse.registry is Arc<Registry>; clone the Arc so it outlives
    // the supervisors / detectors that capture it.
    let reg = Arc::clone(&universe.registry);

    let component_ids: std::collections::HashSet<_> = reg
        .markets()
        .iter()
        .map(|m| reg.component_of(m.id))
        .collect();
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
    // Session-loss cap arms ONLY for real-money live (not shadow, not paper):
    // a hard daily-loss circuit-breaker has no meaning when no money moves.
    let session_loss_cap = if args.live && !args.shadow {
        Some(pm_core::num::Usdc(
            pm_config::usd_to_microusdc(config.live.session_loss_usd)
                .unwrap_or_else(|e| fatal(format!("live.session_loss_usd: {e}"))),
        ))
    } else {
        None
    };
    let risk_cfg = risk_config(&config, session_loss_cap)
        .unwrap_or_else(|e| fatal(format!("risk_config: {e}")));
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
    // Config-driven base; used as-is by the PAPER arm. The LIVE arm derives its
    // own params via live_exec_params (forces redeem = Hold — see that fn).
    let exec_params = ExecParams {
        fill_window: Duration::from_millis(config.execution.fill_window_ms),
        max_unhedged: risk_cfg.max_unhedged,
        redeem: params.redeem,
    };
    // The live arm needs market_tokens for BOTH venue registration and
    // run_execution (which moves it); clone before either arm takes ownership.
    let market_tokens_for_registration = market_tokens.clone();
    // Both arms produce the same JoinHandle<()> so the binding unifies.
    let exec_handle = if let Some((secrets, signer, proxy, deposit_wallet)) = live_rt {
        // Server time for the L1 timestamp comes from the venue clock; reuse the
        // ingestion REST client (token-bucket limited, same base).
        let mut rest = ClobRest::new(
            &config.endpoints.clob_base,
            config.ingestion.rest_rate_capacity,
            config.ingestion.rest_rate_per_sec,
        )
        .unwrap_or_else(|e| fatal(format!("ClobRest for server time: {e}")));
        // Pre-derived PM_API_* win; otherwise derive/create against the venue.
        let creds = match secrets.api {
            Some(c) => c,
            None => {
                let t = rest
                    .server_time()
                    .await
                    .unwrap_or_else(|e| fatal(format!("server_time: {e}")));
                let http = reqwest::Client::new();
                // V2 deposit-wallet flow: bind the API key to the DEPOSIT WALLET
                // (ClobAuth.address = deposit wallet, ERC-7739-wrapped L1 sig,
                // POLY_ADDRESS = deposit wallet) so order.signer = deposit wallet
                // matches the bound key (RECON-M5-V2-1271 "Auth binding").
                pm_execution::auth::derive_or_create_api_key(
                    &http,
                    &config.endpoints.clob_base,
                    &signer,
                    t,
                    Some(deposit_wallet),
                )
                .await
                .unwrap_or_else(|e| fatal(format!("API key derive/create: {e}")))
            }
        };
        // No secret fields on this line — keep it that way (creds/signer must
        // never be interpolated into logs).
        info!(shadow = args.shadow, "live venue armed (api key ready)");
        let mut venue = pm_execution::live::LiveVenue::new(pm_execution::live::LiveVenueCfg {
            base: config.endpoints.clob_base.clone(),
            creds,
            signer,
            proxy,
            deposit_wallet,
            fill_window: Duration::from_millis(config.execution.fill_window_ms),
            rate_per_sec: config.ingestion.rest_rate_per_sec,
            rate_capacity: config.ingestion.rest_rate_capacity,
            shadow: args.shadow,
        })
        .unwrap_or_else(|e| fatal(format!("LiveVenue: {e}")));
        // Startup reconciliation, venue side (spec §Errors): FAK leaves nothing
        // resting, so any open order is an anomaly worth a warning.
        match venue.open_orders().await {
            Ok(open) if !open.is_empty() => {
                warn!(count = open.len(), "venue reports open orders at startup (unexpected under FAK)")
            }
            Ok(_) => {}
            Err(e) => warn!("venue open-orders check failed at startup: {e}"),
        }
        // Register every universe token (venue id + market neg_risk).
        for m in reg.markets() {
            if let Some(&(yes, no)) = market_tokens_for_registration.get(&m.id) {
                for tok in [yes, no] {
                    if let Some(vid) = reg.token_venue_id(tok) {
                        venue.register_token(tok, vid.to_owned(), m.neg_risk);
                    }
                }
            }
        }
        tokio::spawn(run_execution(
            venue,
            exec_rx,
            report_tx,
            store_tx.clone(),
            token_market.clone(),
            market_tokens,
            token_fee,
            // Live never performs on-chain ops: a filled C1Long HOLDS its complete set
            // (manual redeem until M6). venue.merge would return NotSupportedLive and
            // fail the basket AFTER real money filled (integration-review catch).
            live_exec_params(&exec_params),
        ))
    } else {
        let venue = PaperVenue::new(
            fetcher.clone(),
            Duration::from_millis(config.execution.paper_latency_ms),
            params.gas,
        );
        tokio::spawn(run_execution(
            venue,
            exec_rx,
            report_tx,
            store_tx.clone(),
            token_market.clone(),
            market_tokens,
            token_fee,
            exec_params,
        ))
    };

    // Clone the fetcher for the publisher BEFORE the coordinator consumes it
    // (the publisher marks open positions at the live bid). None in headless.
    let fetcher_pub = if tui_active {
        Some(fetcher.clone())
    } else {
        None
    };

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
        // Live dispatch params (spec §Mode ladder). released_at_start is true
        // for paper (inert — no live venue), shadow (signs but no money moves),
        // and headless live (the typed phrase was demanded at startup); it is
        // HELD only for TUI live, where the `l` modal releases the latch.
        LiveParams {
            live: args.live,
            released_at_start: !args.live || args.shadow || !tui_active,
            basket_cap: pm_core::num::Usdc(
                pm_config::usd_to_microusdc(config.live.basket_cap_usd)
                    .unwrap_or_else(|e| fatal(format!("live.basket_cap_usd: {e}"))),
            ),
            min_leg: pm_core::num::Qty((config.live.min_leg_shares * 1e6).round() as u64),
        },
    )
    .unwrap_or_else(|e| fatal(format!("Coordinator::new: {e}")));
    coord.note_session_starts(starts);
    // Wire the dashboard channels BEFORE spawning coord.run() (both take &mut).
    // ctl_tx translates TuiCommands into coordinator control; status_rx feeds the
    // publisher. Held even in headless mode (cheap; keeps the spawn ordering one
    // shape), though only the TUI path uses them.
    let ctl_tx = coord.control_channel(8);
    let status_rx = coord.status_channel();
    let coord_handle = tokio::spawn(coord.run());

    // ---- kill watch ---------------------------------------------------------
    let kill_handle = spawn_kill_watch(PathBuf::from(&config.risk.kill_file), Arc::clone(&kill));
    info!(kill_file = %config.risk.kill_file, "kill switch sentinel path (resolved relative to cwd)");

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

    // ---- publisher + TUI startup -------------------------------------------
    // Built only when the TUI is active. Owns terminal lifecycle: raw mode +
    // alternate screen on enter; a panic hook restores the terminal BEFORE the
    // default hook prints so a panic stays readable; run_tui's task tears the
    // screen down on its own exit so the final report prints on the normal
    // screen even if the TUI exits first (q / Ctrl-C-as-key).
    let (mut tui_cmd_rx, tui_task) = if tui_active {
        let read = pm_store::read::ReadStore::open(Path::new(&config.store.path))
            .unwrap_or_else(|e| fatal(format!("ReadStore::open: {e}")));
        let ctx = pm_app::publisher::PublisherCtx {
            read,
            stats: Arc::clone(&stats),
            cells: stat_cells.clone(),
            status_rx: status_rx.clone(),
            registry: Arc::clone(&reg),
            logbuf: Arc::clone(&logbuf),
            fetcher: fetcher_pub.expect("fetcher_pub is Some when tui_active"),
            feed_rows: config.tui.feed_rows,
            fills_rows: config.tui.fills_rows,
            log_lines: config.tui.log_lines,
            mode_paper: config.mode.paper,
            shadow: args.shadow,
            start: Instant::now(),
            last_frames: 0,
            last_at: Instant::now(),
        };
        let (state_rx, _pub_handle) =
            pm_app::publisher::spawn_publisher(ctx, Duration::from_millis(config.tui.refresh_ms));

        crossterm::terminal::enable_raw_mode().unwrap_or_else(|e| fatal(format!("raw mode: {e}")));
        let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::EnterAlternateScreen);
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = crossterm::terminal::disable_raw_mode();
            let _ =
                crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
            default_hook(info);
        }));
        let backend = ratatui::backend::CrosstermBackend::new(std::io::stdout());
        let mut terminal = match ratatui::Terminal::new(backend) {
            Ok(t) => t,
            Err(e) => {
                let _ = crossterm::terminal::disable_raw_mode();
                let _ = crossterm::execute!(
                    std::io::stdout(),
                    crossterm::terminal::LeaveAlternateScreen
                );
                fatal(format!("terminal: {e}"));
            }
        };

        let key_rx = pm_tui::run::spawn_input_thread();
        let (cmd_tx, cmd_rx) = mpsc::channel::<pm_tui::state::TuiCommand>(8);
        let task = tokio::spawn(async move {
            let _ = pm_tui::run::run_tui(&mut terminal, state_rx, key_rx, cmd_tx).await;
            // Teardown here so the final report prints on the normal screen even
            // when the TUI exits first (q / Ctrl-C-as-key).
            let _ = crossterm::terminal::disable_raw_mode();
            let _ =
                crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
        });
        (Some(cmd_rx), Some(task))
    } else {
        (None, None)
    };

    // ---- main loop ----------------------------------------------------------
    // Two independent intervals:
    //   * 1s arm — checks kill flag and duration-elapsed; effective resolution
    //     is ~1s (1s watcher poll + 1s loop poll), so --duration-secs values as
    //     small as 1s are honoured correctly in smoke tests.
    //   * 10s arm — purely for periodic status logging; no kill/duration logic.
    let start = Instant::now();
    let duration = Duration::from_secs(args.duration_secs);
    let run_forever = args.duration_secs == 0;
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let trigger;
    loop {
        tokio::select! {
            _ = poll.tick() => {
                if kill.load(Ordering::Acquire) {
                    trigger = "kill";
                    break;
                }
                if !run_forever && start.elapsed() >= duration {
                    trigger = "duration";
                    break;
                }
            }
            _ = ticker.tick() => {
                log_stats(&stats, &stat_cells, start.elapsed());
            }
            _ = tokio::signal::ctrl_c() => {
                trigger = "ctrl_c";
                break;
            }
            cmd = recv_tui(&mut tui_cmd_rx) => match cmd {
                pm_tui::state::TuiCommand::SetPaused(p) => {
                    let _ = ctl_tx.send(pm_app::coordinator::CtlCommand::SetPaused(p)).await;
                }
                pm_tui::state::TuiCommand::Kill => kill.store(true, Ordering::Release),
                pm_tui::state::TuiCommand::GoLive => {
                    if args.live {
                        let _ = ctl_tx
                            .send(pm_app::coordinator::CtlCommand::ReleaseLive)
                            .await;
                    } else {
                        warn!("live not armed — restart with --live to trade real money");
                    }
                }
                pm_tui::state::TuiCommand::Quit => {
                    trigger = "quit";
                    break;
                }
            },
        }
    }
    info!(
        trigger,
        elapsed_s = start.elapsed().as_secs(),
        "shutdown initiated"
    );

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
    // On coordinator panic: log the error but do NOT exit immediately — continue
    // the shutdown sequence so the writer flushes and the store is cleanly closed.
    // Treat coordinator panic as unhealthy (exit 2 at the end).
    let coord_summary = match coord_handle.await {
        Ok(s) => Some(s),
        Err(e) => {
            error!(error = %e, "coordinator task panicked; continuing shutdown to flush writer");
            None
        }
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

    // Await the TUI task LAST so its terminal teardown is ordered before the
    // final report hits the (now normal) screen. The coordinator was awaited
    // above; dropping its status_tx closed the publisher's watch → the publisher
    // exits → its state_tx drops → run_tui's state_rx closes → run_tui returns →
    // the task runs disable_raw_mode + LeaveAlternateScreen. So a session ending
    // for NON-TUI reasons (duration/kill/sentinel/ctrl_c) still tears the screen
    // down here.
    if let Some(t) = tui_task {
        let _ = t.await;
    }

    // ---- final report -------------------------------------------------------
    let realized = store.realized_total().unwrap_or(0);
    let opportunities = store.count_opportunities().unwrap_or(0);
    let fills = store.count_fills().unwrap_or(0);
    let halts = store.count_halts().unwrap_or(0);
    let write_errors = store.write_errors;

    // The summary must reach stdout in BOTH modes: in TUI mode tracing went to
    // the ring buffer (now gone with the torn-down screen), so println! is the
    // only path to the human. One info! mirror is kept for headless log capture.
    match &coord_summary {
        Some(summary) => {
            println!(
                "session summary: cash_micro={} equity_micro={} open_positions={}",
                summary.cash.0, summary.equity.0, summary.open_positions
            );
            info!(
                cash_micro = summary.cash.0,
                equity_micro = summary.equity.0,
                open_positions = summary.open_positions,
                "session summary"
            );
        }
        None => {
            println!("session summary: coordinator panicked — no position data available");
            info!("session summary: coordinator panicked — no position data available");
        }
    }
    println!(
        "session counts: realized_micro={realized} opportunities={opportunities} fills={fills} halts={halts} write_errors={write_errors}"
    );
    println!("FINAL stats: {}", stats.line());
    println!("session ended: duration_s={}", start.elapsed().as_secs());

    let coord_panicked = coord_summary.is_none();
    let healthy = write_errors == 0 && !restart_storm && supervisors_started > 0 && !coord_panicked;
    println!("arb session result: healthy={healthy}");
    if healthy {
        std::process::exit(0);
    } else {
        std::process::exit(2);
    }
}

/// Receive the next TUI command, or pend forever when no TUI is wired
/// (headless) or the channel is closed (TUI gone). Closed-channel `recv()`
/// returns None immediately, so this costs one wasted poll per loop wake before
/// pending — acceptable; the arm is only re-armed when another arm fires.
async fn recv_tui(
    rx: &mut Option<mpsc::Receiver<pm_tui::state::TuiCommand>>,
) -> pm_tui::state::TuiCommand {
    match rx.as_mut() {
        Some(r) => match r.recv().await {
            Some(c) => c,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

/// ExecParams for the LIVE arm: identical to the (config-derived) base except
/// `redeem` is forced to `Hold`. Live never performs on-chain ops, so a filled
/// C1Long HOLDS its complete set (manual redeem until M6); `venue.merge` would
/// return `NotSupportedLive` and fail the basket AFTER real money filled
/// (integration-review catch). The paper arm keeps the config-driven redeem.
fn live_exec_params(base: &ExecParams) -> ExecParams {
    ExecParams {
        redeem: RedeemStrategy::Hold,
        ..*base
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
        books,
        frames,
        reconnects,
        "{}",
        stats.line()
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn live_and_shadow_flags_parse() {
        let a = Args::from_iter(["--live".to_string()].into_iter()).unwrap();
        assert!(a.live && !a.shadow);
        let a = Args::from_iter(["--live".into(), "--shadow".into()].into_iter()).unwrap();
        assert!(a.live && a.shadow);
        assert!(Args::from_iter(["--shadow".into()].into_iter()).is_err());
    }

    /// Integration-review catch: the live arm must NEVER inherit a `merge`
    /// redeem strategy, because `LiveVenue::merge` is an on-chain op that returns
    /// `NotSupportedLive` and would fail a basket AFTER real money filled. The
    /// live ExecParams is derived by forcing redeem = Hold regardless of config,
    /// while preserving the other fields.
    #[test]
    fn live_exec_params_forces_hold_regardless_of_config() {
        use pm_engine::RedeemStrategy;
        let base = ExecParams {
            fill_window: Duration::from_millis(750),
            max_unhedged: pm_core::num::Usdc(123_000_000),
            redeem: RedeemStrategy::Merge, // config default — the dangerous one
        };
        let live = live_exec_params(&base);
        assert_eq!(live.redeem, RedeemStrategy::Hold, "live must force Hold");
        assert_eq!(live.fill_window, base.fill_window, "fill_window preserved");
        assert_eq!(live.max_unhedged, base.max_unhedged, "max_unhedged preserved");
        // Already-Hold config stays Hold (idempotent).
        let base_hold = ExecParams {
            redeem: RedeemStrategy::Hold,
            ..base
        };
        assert_eq!(live_exec_params(&base_hold).redeem, RedeemStrategy::Hold);
    }
}
