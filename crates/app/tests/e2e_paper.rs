//! M3 end-to-end: scripted WS/REST → shard → detect → coordinate → paper-fill
//! → store → P&L. Exact-integer assertions throughout (spec §21).

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use pm_app::coordinator::{Coordinator, LiveParams, run_execution};
use pm_app::detector::Detector;
use pm_app::lp_pool::run_lp_pool;
use pm_app::stats::AppStats;
use pm_app::wiring::{
    BookFetcher, build_component_index, engine_params, fee_map, risk_config, token_maps,
};
use pm_config::Config;
use pm_core::instrument::TokenId;
use pm_core::num::{TickSize, Usdc};
use pm_execution::basket::ExecParams;
use pm_execution::venue::PaperVenue;
use pm_ingestion::IngestError;
use pm_ingestion::livebook::RawLevel;
use pm_ingestion::rest::ParsedBook;
use pm_ingestion::supervisor::{
    FactoryDecision, RestBookSource, Supervisor, SupervisorCommand, SupervisorConfig,
};
use pm_ingestion::ws::WsTransport;
use pm_registry::RegistryBuilder;
use pm_store::Store;
use pm_store::writer::{StoreMsg, run_writer};

// ---------------------------------------------------------------------------
// Scripted transports
// ---------------------------------------------------------------------------

/// WS transport that plays scripted frames then parks on a `Notify` until the
/// test sets `released`. Cancel-safe: `Notify::notified()` re-registers on each
/// poll, so the future may be dropped/recreated by `select!` without losing the
/// signal (mirrors the SeqTransport in supervisor.rs tests).
struct ScriptedWs {
    frames: VecDeque<String>,
    park: Arc<tokio::sync::Notify>,
    released: Arc<AtomicBool>,
}

impl WsTransport for ScriptedWs {
    async fn next_frame(&mut self) -> Option<Result<String, IngestError>> {
        if let Some(f) = self.frames.pop_front() {
            return Some(Ok(f));
        }
        while !self.released.load(Ordering::Acquire) {
            self.park.notified().await;
        }
        None
    }

    async fn send_text(&mut self, _: &str) -> Result<(), IngestError> {
        Ok(())
    }
}

/// REST source seeding the four books. The arb market (ya/na) carries a C1Long:
/// YES ask .44 + NO ask .50 → asks sum 0.94 < 1 (C1Long); bids .40+.45 = 0.85 < 1 (no C1Short).
/// The quiet market (yq/nq) is truly arb-free:
/// asks 0.60+0.55 = 1.15 > 1 (no C1Long); bids 0.40+0.35 = 0.75 < 1 (no C1Short).
#[derive(Clone)]
struct ScriptedRest;

impl RestBookSource for ScriptedRest {
    async fn book(&mut self, venue_token_id: &str) -> Result<ParsedBook, IngestError> {
        let (bid, ask) = match venue_token_id {
            "ya" => (400_000, 440_000),
            "na" => (450_000, 500_000),
            "yq" => (400_000, 600_000), // bid 0.40, ask 0.60
            "nq" => (350_000, 550_000), // bid 0.35, ask 0.55
            other => panic!("unexpected token {other}"),
        };
        Ok(ParsedBook {
            asset_id: venue_token_id.into(),
            hash: "h".into(),
            bids: vec![RawLevel {
                price_micro: bid,
                size_micro: 100_000_000,
            }],
            asks: vec![RawLevel {
                price_micro: ask,
                size_micro: 100_000_000,
            }],
        })
    }
}

// ---------------------------------------------------------------------------
// Test entry — 30s hard ceiling around the deterministic pipeline.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn synthetic_feed_to_paper_pnl() {
    tokio::time::timeout(Duration::from_secs(30), run_e2e())
        .await
        .expect("e2e within 30s");
}

async fn run_e2e() {
    // ---- 1. Registry: arb market (ya/na) + quiet market (yq/nq) ------------
    let mut builder = RegistryBuilder::default();
    builder.add_market(
        "0xarb",
        "ya",
        "na",
        TickSize::Cent,
        0,
        false,
        None,
        true,
        false,
        None,
    );
    builder.add_market(
        "0xquiet",
        "yq",
        "nq",
        TickSize::Cent,
        0,
        false,
        None,
        true,
        false,
        None,
    );
    let reg = Arc::new(builder.finish("").expect("registry finish"));

    // ---- 2. Config: paper, zero latency, validated ------------------------
    let mut cfg = Config::default();
    cfg.execution.paper_latency_ms = 0;
    cfg.validate().expect("config validate");

    // ---- 3. Wiring derivations --------------------------------------------
    let params = engine_params(&cfg).expect("engine_params");
    let risk_cfg = risk_config(&cfg, None).expect("risk_config");
    let (token_market, market_tokens) = token_maps(&reg);
    let token_fee = fee_map(&reg);
    let index = Arc::new(build_component_index(&reg));

    // ---- 4. Store + writer -------------------------------------------------
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("e2e.db");
    let store = Store::open(&db_path).expect("store open");
    let (store_tx, store_rx) = mpsc::channel::<StoreMsg>(4096);
    let writer = tokio::spawn(run_writer(store, store_rx));

    // ---- 5. Channels + shared state ---------------------------------------
    let (opp_tx, opp_rx) = mpsc::channel(1024);
    let (lp_tx, lp_rx) = mpsc::channel(64);
    let (exec_tx, exec_rx) = mpsc::channel(4);
    let (report_tx, report_rx) = mpsc::channel(4);
    let kill = Arc::new(AtomicBool::new(false));
    let stats = AppStats::new();

    // ---- 6. ONE supervisor over all 4 tokens ------------------------------
    let token_triples: Vec<(TokenId, String, TickSize)> = reg
        .all_tokens()
        .into_iter()
        .map(|tok| {
            let venue_id = reg.token_venue_id(tok).expect("venue id").to_owned();
            let tick = reg.tick_of(tok).expect("tick");
            (tok, venue_id, tick)
        })
        .collect();

    let mut sup = Supervisor::new(token_triples, ScriptedRest, SupervisorConfig::default());
    let cmd_tx = sup.command_channel(32);

    // Route every token to this supervisor's command channel.
    let mut routes: HashMap<TokenId, mpsc::Sender<SupervisorCommand>> = HashMap::new();
    for tok in reg.all_tokens() {
        routes.insert(tok, cmd_tx.clone());
    }
    let fetcher = BookFetcher::new(routes);

    // Detector hook installed on the supervisor.
    let lp_min_interval = Duration::from_millis(cfg.lp.min_resolve_interval_ms);
    let mut det = Detector::new(
        Arc::clone(&index),
        params,
        opp_tx.clone(),
        lp_tx.clone(),
        lp_min_interval,
        Arc::clone(&stats),
    );
    sup.set_on_apply(Box::new(move |t, shard| det.on_apply(t, shard)));

    // ---- 7. WS delta frame: harmless bid change on ya ----------------------
    // Exercises the WS apply path + a second detection that cooldown suppresses.
    let frame = r#"{"event_type":"price_change","market":"0xarb","price_changes":[{"asset_id":"ya","price":"0.41","size":"7","side":"BUY"}]}"#;

    // ---- 8. Supervisor task: first Connect, then Stop ----------------------
    let park = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(AtomicBool::new(false));
    let park_factory = Arc::clone(&park);
    let released_factory = Arc::clone(&released);
    let connect_count = Arc::new(AtomicUsize::new(0));
    let frame_owned = frame.to_string();

    let sup_handle = tokio::spawn(async move {
        sup.run(move || {
            let n = connect_count.fetch_add(1, Ordering::SeqCst);
            let park = Arc::clone(&park_factory);
            let released = Arc::clone(&released_factory);
            let frame = frame_owned.clone();
            async move {
                if n == 0 {
                    Ok(FactoryDecision::Connect(ScriptedWs {
                        frames: [frame].into(),
                        park,
                        released,
                    }))
                } else {
                    Ok(FactoryDecision::Stop)
                }
            }
        })
        .await;
    });

    // ---- 9. LP pool (opp_tx clone made before main's drop) ----------------
    let opp_tx_lp = opp_tx.clone();
    let lp_handle = tokio::spawn(run_lp_pool(
        lp_rx,
        opp_tx_lp,
        params,
        cfg.lp.solver_concurrency,
        Arc::clone(&stats),
    ));

    // Drop main's detector-side senders so closure cascades on shutdown.
    drop(opp_tx);
    drop(lp_tx);

    // ---- 10. Execution task ------------------------------------------------
    let venue = PaperVenue::new(fetcher.clone(), Duration::from_millis(0), params.gas);
    let exec_params = ExecParams {
        fill_window: Duration::from_millis(cfg.execution.fill_window_ms),
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

    // ---- 11. Coordinator ---------------------------------------------------
    let coord = Coordinator::new(
        &cfg,
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
        LiveParams {
            live: false,
            released_at_start: true,
            basket_cap: Usdc(0),
            min_leg: pm_core::num::Qty(0),
            min_leg_value: Usdc(0),
        },
    )
    .expect("coordinator new");
    let coord_handle = tokio::spawn(coord.run());

    // ---- 12. Wait for the basket to settle clean --------------------------
    while stats.baskets_clean.load(Ordering::Relaxed) < 1 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ---- 13. Shutdown cascade ----------------------------------------------
    released.store(true, Ordering::Release);
    park.notify_waiters();
    // Transport returns None → TransportLost → factory returns Stop → run ends.
    sup_handle.await.expect("supervisor task");
    // Supervisor dropped → detector dropped → lp_tx / opp_tx clones close.
    lp_handle.await.expect("lp task");
    // LP pool's opp_tx clone dropped → coordinator's opp_rx closes → drains.
    let summary = coord_handle.await.expect("coordinator task");
    // Coordinator dropped exec_tx → execution rx closes → it ends.
    exec_handle.await.expect("exec task");
    // Drop main's writer sender LAST so all producers are gone.
    drop(store_tx);
    let store = writer.await.expect("writer task");

    // ---- 14. Assertions ----------------------------------------------------
    // cost/share-pair = 440_000 + 500_000 = 940_000µ; 100sh → basis 94_000_000.
    // merge proceeds = 100sh × 1_000_000 − gas 10_000 = 99_990_000.
    // net = 99_990_000 − 94_000_000 = 5_990_000 µUSDC.
    assert_eq!(summary.cash, Usdc(5_990_000));
    assert_eq!(summary.equity, Usdc(5_990_000));
    assert_eq!(summary.open_positions, 0);
    assert_eq!(store.realized_total().unwrap(), 5_990_000);
    // Exactly one opportunity row: the C1Long on 0xarb. The WS-frame re-detection
    // of the same fingerprint is cooldown-suppressed (no net improvement), so it
    // does not produce a second row.
    assert_eq!(store.count_opportunities().unwrap(), 1);
    assert_eq!(store.count_fills().unwrap(), 2);
    assert!(store.open_orders().unwrap().is_empty());
    let arb = reg.market_by_condition("0xarb").unwrap();
    assert_eq!(store.position(arb.yes.0 as i64).unwrap(), (0, 0));
    assert_eq!(store.position(arb.no.0 as i64).unwrap(), (0, 0));
    assert_eq!(store.write_errors, 0);
    assert_eq!(stats.opps_dropped.load(Ordering::Relaxed), 0);
}
