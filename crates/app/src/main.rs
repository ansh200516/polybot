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
use tracing::{info, warn};

use pm_app::coordinator::{CoordinatorSummary, CtlCommand, LiveParams, now_ms, run_execution};
use pm_app::kill::spawn_kill_watch;
use pm_app::stats::AppStats;
use pm_app::strategy::arb::{ArbStrategy, ExecTaskBuilder};
use pm_app::strategy::copy::{AppCopyVenue, CopyParams, CopyStrategy, PaperCopyVenue, TradeTokenInfo};
use pm_app::strategy::host::{HostShared, StrategyHost};
use pm_app::strategy::mm::{MmFillsSource, MmLive, MmParams, MmStrategy};
use pm_app::strategy::stub::HeartbeatStrategy;
use pm_app::strategy::StrategyId;
use pm_app::wiring::{
    BookFetcher, PlatformEnvelopes, build_component_index, engine_params, fee_map, inventory_config,
    mm_allowed_segments, mm_market_selection, mm_quote_tokens, mm_use_live, pack_components,
    risk_config, segment_thresholds, strategy_envelopes, token_maps, user_ws_url,
};
use pm_config::Config;
use pm_core::instrument::Relationship;
use pm_engine::RedeemStrategy;
use pm_execution::basket::ExecParams;
use pm_execution::venue::PaperVenue;
use pm_ingestion::confluence::{ConfluenceParams, top_trader_markets};
use pm_ingestion::data_api::{DataApiClient, OrderBy, TimePeriod};
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
    /// Auth-only check: derive the CLOB trading key (plain-EOA L1) and make one
    /// authenticated L2 read, then exit. No market fetch, no TUI, no orders, no
    /// money. Requires `--live`. Run with `--headless` to see the full logs.
    auth_check: bool,
    /// `--relayer-check`: one read-only relayer `GET /nonce` (RELAYER_API_KEY auth),
    /// then exit. Confirms the live relayer accepts our auth headers — NO orders,
    /// NO money, NO on-chain tx. Requires `--live` (for the signer + relayer creds).
    relayer_check: bool,
    /// Probe-order: after arming, place ONE tiny 5-share FAK BUY (on a cheap
    /// liquid token, ≤ $0.10) through the real signing/auth/submit path, report
    /// the venue's verdict, then exit. Settles whether a type-3 deposit-wallet
    /// order is accepted, without waiting for an arb. Requires `--live`.
    probe_order: bool,
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
        let mut auth_check = false;
        let mut relayer_check = false;
        let mut probe_order = false;

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
                "--auth-check" => {
                    auth_check = true;
                }
                "--relayer-check" => {
                    relayer_check = true;
                }
                "--probe-order" => {
                    probe_order = true;
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
        if auth_check && !live {
            return Err("--auth-check requires --live".to_string());
        }
        if relayer_check && !live {
            return Err("--relayer-check requires --live".to_string());
        }
        if probe_order && !live {
            return Err("--probe-order requires --live".to_string());
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
            auth_check,
            relayer_check,
            probe_order,
        })
    }
}

fn fatal(msg: impl std::fmt::Display) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(1);
}

/// Run the `[confluence]` "follow the smart money" selection: pull the Data-API
/// leaderboard's top traders and aggregate their OPEN positions into per-market
/// favored sides (best-confluence-first). The config strings were validated to
/// the accepted spellings upstream, so the matches fall back to the documented
/// default on anything else. Best-effort: any transport/parse failure is returned
/// to the caller, which falls back to the normal liquidity universe rather than
/// aborting startup.
async fn build_confluence(
    cfg: &Config,
) -> Result<Vec<pm_ingestion::confluence::ConfluenceMarket>, pm_ingestion::IngestError> {
    let order_by = match cfg.confluence.order_by.to_ascii_lowercase().as_str() {
        "vol" => OrderBy::Vol,
        _ => OrderBy::Pnl,
    };
    let period = match cfg.confluence.time_period.to_ascii_lowercase().as_str() {
        "day" => TimePeriod::Day,
        "week" => TimePeriod::Week,
        "all" => TimePeriod::All,
        _ => TimePeriod::Month,
    };
    let params = ConfluenceParams {
        order_by,
        period,
        top_traders: cfg.confluence.top_traders,
        scan_limit: cfg.confluence.scan_limit,
        size_threshold: cfg.confluence.size_threshold,
    };
    let client = DataApiClient::new(None)?;
    top_trader_markets(&client, &params).await
}

/// LIVE inventory-seed reconcile. The SQLite seed (`store.open_positions`) can
/// carry STALE lots — paper fills and prior sessions whose markets have since
/// RESOLVED (the bot never recorded the resolution). Seeding those on a live run
/// makes the long-only MM post asks to sell tokens it does NOT actually hold, and
/// the CLOB rejects every one (`balance: 0`) on each cycle. So keep only seed
/// tokens the DEPOSIT WALLET really holds AND that are still tradeable
/// (`!redeemable && size > 0`, via the public Data API), CLAMPED to the real
/// on-chain size; drop the rest. Returns `Err` on any fetch failure so the caller
/// seeds FLAT — bid-only is safe; a phantom ask is not.
async fn reconcile_seed_against_chain(
    seed: &[(pm_core::instrument::TokenId, i128, pm_core::num::Usdc)],
    deposit_wallet: alloy_primitives::Address,
    reg: &pm_registry::Registry,
) -> Result<Vec<(pm_core::instrument::TokenId, i128, pm_core::num::Usdc)>, String> {
    if seed.is_empty() {
        return Ok(Vec::new());
    }
    let client = DataApiClient::new(None).map_err(|e| e.to_string())?;
    // The Data API keys positions by the lowercased on-chain address.
    let positions = client
        .positions(&deposit_wallet.to_string().to_lowercase(), 0.0)
        .await
        .map_err(|e| e.to_string())?;
    Ok(reconcile_seed_with_positions(seed, &positions, reg))
}

/// Pure seed↔positions reconcile (no I/O), split out of
/// [`reconcile_seed_against_chain`] for unit testing. Keeps only seed tokens the
/// wallet still holds AND can trade (`!redeemable && size > 0`), clamped to the
/// real on-chain size, with the cost basis scaled down to the clamped size.
fn reconcile_seed_with_positions(
    seed: &[(pm_core::instrument::TokenId, i128, pm_core::num::Usdc)],
    positions: &[pm_ingestion::data_api::Position],
    reg: &pm_registry::Registry,
) -> Vec<(pm_core::instrument::TokenId, i128, pm_core::num::Usdc)> {
    // Real, still-tradeable holdings: venue token id (`asset`) -> size in shares.
    let real: std::collections::HashMap<&str, f64> = positions
        .iter()
        .filter(|p| !p.redeemable && p.size > 0.0)
        .map(|p| (p.asset.as_str(), p.size))
        .collect();
    seed.iter()
        .filter_map(|(tok, net, cost)| {
            let vid = reg.token_venue_id(*tok)?;
            let &real_size = real.get(vid)?; // not held on-chain → drop the stale lot
            let real_net_micro = (real_size * 1e6) as i128;
            // Clamp to the real holding; a non-positive result (short/zero) drops —
            // we can only ASK against tokens we genuinely hold.
            let clamped = (*net).min(real_net_micro);
            if clamped <= 0 {
                return None;
            }
            // Scale the cost basis to the clamped size (net > 0 here → safe divide).
            let scaled_cost = pm_core::num::Usdc(cost.0.saturating_mul(clamped) / *net);
            Some((*tok, clamped, scaled_cost))
        })
        .collect()
}

/// Advisory soft ceiling (USD) for a LIVE market-maker capital slice (Task 4.5).
/// Live MM is a tiny canary, so a slice above this almost certainly is NOT
/// intended for a first live run and earns a loud WARN. It is NOT a hard cap —
/// the operator may size deliberately — and the HARD guards remain the capital
/// carve, the `Σcapital ≤ bankroll` startup allocator, and the inventory caps.
const MM_LIVE_CANARY_SOFT_CAP_USD: f64 = 100.0;

/// The exact live-venue inputs the market maker reuses to build its OWN
/// `LiveVenue` (Task 4.5). Cloned from arb's RESOLVED creds/signer in the
/// live-arming block BELOW, BEFORE they move into arb's venue, so MM's venue is
/// byte-identical (same account, same key) and NO second API key is derived.
/// Stashed only when MM is cleared for live (`wiring::mm_use_live`), then
/// consumed at the MM wiring. SHARED-ACCOUNT NOTE: arb + MM each end up with an
/// independent REST rate limiter against this one account's budget (documented
/// at the MM wiring) — acceptable for a tiny canary.
struct MmLiveInputs {
    base: String,
    creds: pm_execution::secrets::ApiCreds,
    signer: alloy_signer_local::PrivateKeySigner,
    proxy: alloy_primitives::Address,
    deposit_wallet: alloy_primitives::Address,
    auth_address: alloy_primitives::Address,
}

/// Build the copy executor's `asset (venue token-id) →` [`TradeTokenInfo`]
/// resolver (Task C5) from the registry: for every SYNCED-universe market, map
/// BOTH outcome tokens by their venue token-id string (the value a Data-API
/// trade carries in `asset`), each with the venue tick grid + the on-chain
/// condition id (the relayer redeem key). Keying by asset — instead of an
/// `outcome_index → yes/no` assumption — guarantees a copy buys the EXACT token
/// the trader bought (the prior oi-keyed map silently bought the COMPLEMENT when
/// the Data-API outcome index didn't line up with the registry's label-based
/// yes/no). A market whose condition id does not parse as a `B256` is skipped (it
/// can't be redeemed and is almost never a real market).
///
/// This map SEEDS the executor with the synced universe (the whitelist's active
/// markets). It is NOT a hard coverage ceiling: a fresh smart-money signal in a
/// market NOT in this map is synced ON-DEMAND at signal time on live runs (the
/// strategy's `resolve_ondemand`: one CLOB `/markets/{cid}` fetch +
/// `LiveVenue::ensure_token`, then cached), so entry latency isn't bounded by the
/// universe-snapshot cadence. The seed just front-loads the common case so most
/// signals skip even that one fetch.
fn build_copy_tradeable(reg: &pm_registry::Registry) -> HashMap<String, TradeTokenInfo> {
    let mut map: HashMap<String, TradeTokenInfo> = HashMap::new();
    let mut skipped = 0usize;
    for m in reg.markets() {
        let Some(cond_str) = reg.market_condition(m.id) else {
            skipped += 1;
            continue;
        };
        let Ok(condition) = cond_str.parse::<alloy_primitives::B256>() else {
            skipped += 1;
            continue;
        };
        // Key BY THE VENUE TOKEN-ID STRING (the Data-API trade's `asset`), NOT by
        // (condition, outcome_index). A copy signal carries the exact token the
        // trader bought; resolving by that asset buys the SAME side they did,
        // immune to any Yes/No labeling or outcome-index convention mismatch
        // between the Data API and the registry (an oi→yes/no assumption silently
        // bought the COMPLEMENT — betting AGAINST the smart money). `best_ask`
        // then queries this same venue id, so the priced book is the trader's.
        for tok in [m.yes, m.no] {
            if let Some(venue_id) = reg.token_venue_id(tok) {
                map.insert(
                    venue_id.to_string(),
                    TradeTokenInfo {
                        token: tok,
                        ts: m.tick,
                        condition,
                        neg_risk: m.neg_risk,
                    },
                );
            }
        }
    }
    if skipped > 0 {
        warn!(
            skipped,
            "copy: markets without a parseable condition_id are not tradeable by the copy executor"
        );
    }
    map
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

/// Parse `./.env` (if present) into a key→value map — WITHOUT touching the
/// process environment (the crate forbids `unsafe`, and `set_var` is unsafe).
/// The map is consulted only as a fallback in the secrets lookup, so a real
/// environment variable always wins. Format: one `KEY=value` per line; blank
/// lines and `#` comments skipped; one optional surrounding pair of single or
/// double quotes on the value is stripped. Secret VALUES are never logged.
fn load_dotenv() -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Ok(contents) = std::fs::read_to_string(".env") else {
        return map; // no .env is fine
    };
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let val = val.trim();
        let val = val
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .or_else(|| val.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(val);
        if !key.is_empty() {
            map.insert(key.to_string(), val.to_string());
        }
    }
    if !map.is_empty() {
        eprintln!("arb: loaded {} var(s) from .env", map.len());
    }
    map
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dotenv = load_dotenv();
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => fatal(e),
    };

    // Mode decision: the TUI engages only when not forced headless AND stdout is
    // an actual terminal. A piped/redirected stdout (cron/CI) reports false here,
    // so the dashboard is skipped automatically without --headless.
    use std::io::IsTerminal;
    let tui_active = !args.headless && std::io::stdout().is_terminal();
    // A periodic auto-restart re-exec (universe.auto_restart_secs) carries
    // `PM_RESUME_LIVE=1` when the session it restarted was ALREADY RELEASED — so
    // the fresh process resumes live trading instead of re-holding. The operator
    // already confirmed live in the prior process; the restart is an INTERNAL
    // universe refresh, not a new launch, so it skips the confirm/hold rather than
    // pausing every cycle. Only the bot's own re-exec sets this (never a fresh
    // launch — a held session re-execs WITHOUT it and stays held).
    let resume_live = std::env::var("PM_RESUME_LIVE").as_deref() == Ok("1");

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
        // Real env wins; the .env map is the fallback (so secrets can live in a
        // gitignored .env without exporting them each run).
        let secrets = pm_execution::secrets::LiveSecrets::from_lookup(|k| {
            std::env::var(k).ok().or_else(|| dotenv.get(k).cloned())
        })
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
        // The TUI path confirms via the `l` modal instead (release latch). A
        // RESUME (auto-restart of an already-confirmed, already-released session)
        // skips the prompt — re-confirming on every internal re-sync is both
        // impossible (stdin pipe consumed) and pointless.
        if !args.shadow && !args.auth_check && !args.relayer_check && !tui_active && !resume_live {
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

    // ---- auth-check: derive + one L2 read, then exit (no orders, no money) --
    // The fast, zero-risk verification of the auth path: it exercises ONLY the
    // two layers that gate live trading — plain-EOA L1 key derivation and an L2
    // authenticated read — mirroring py-clob-client-v2. No market fetch, no TUI,
    // no order POST. Run as `--live --auth-check` (add `--headless` for full logs).
    if args.auth_check {
        let Some((_, signer, _, deposit_wallet)) = &live_rt else {
            fatal("--auth-check requires --live");
        };
        let base = config.endpoints.clob_base.clone();
        println!("\n=== auth-check: plain-EOA L1 derive + one L2 read (no orders, no money) ===");
        println!("EOA (signer)   : {}", signer.address());
        println!("deposit wallet : {deposit_wallet}");
        println!("CLOB base      : {base}\n");

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|e| fatal(format!("auth-check http build: {e}")));
        let server_time_s = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // [1/2] derive the trading key via PLAIN-EOA L1 (POLY_ADDRESS = EOA).
        println!("[1/2] deriving API key (plain-EOA L1, POLY_ADDRESS = EOA) ...");
        let creds = match pm_execution::auth::derive_or_create_api_key(
            &http,
            &base,
            signer,
            server_time_s,
            None,
        )
        .await
        {
            Ok(c) => {
                println!("  OK   api key id = {}", c.key);
                println!(
                    "       secret = {} chars (hidden), passphrase = {} chars (hidden)",
                    c.secret.expose().len(),
                    c.passphrase.expose().len()
                );
                c
            }
            Err(e) => {
                println!("  FAIL  {e}");
                println!(
                    "\nVerdict: ❌ plain-EOA L1 derive was REJECTED. Paste this exact error — if it is\n         a 401 / invalid-headers, the endpoint refuses even the plain-EOA path."
                );
                return;
            }
        };

        // [2/2] one L2-authenticated read: GET /data/orders as the EOA.
        println!("\n[2/2] L2 read: GET /data/orders (POLY_ADDRESS = EOA) ...");
        let path = "/data/orders";
        let ts = server_time_s.to_string();
        let eoa = signer.address().to_string();
        let headers =
            match pm_execution::auth::l2_headers(&creds, &eoa, &ts, "GET", path, None) {
                Ok(h) => h,
                Err(e) => {
                    println!("  FAIL building L2 headers: {e}");
                    return;
                }
            };
        let url = format!("{base}{path}?next_cursor=MA==");
        let mut req = http.get(&url);
        for (k, v) in &headers {
            req = req.header(*k, v);
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                let snippet: String = body.chars().take(240).collect();
                println!("  HTTP {status}");
                println!("  body: {snippet}");
                if status.is_success() {
                    println!(
                        "\nVerdict: ✅ BOTH auth layers work — plain-EOA L1 derive AND L2 read succeeded.\n         The bot can now reach order placement. Next: a tiny live canary to get the\n         order verdict for this type-3 (EIP-7702 / POLY_1271) deposit wallet."
                    );
                } else {
                    println!(
                        "\nVerdict: ⚠️  L1 derive worked, but the L2 read returned {status}. Paste the body above."
                    );
                }
            }
            Err(e) => println!("  FAIL  {e}"),
        }
        return;
    }

    // ---- relayer-check: one read-only relayer GET /nonce, then exit --------
    // Confirms the live relayer ACCEPTS our RELAYER_API_KEY auth and that the
    // /nonce endpoint + header scheme are right — the #1 "blind port" unknown —
    // with ZERO risk: a nonce READ moves no money, places no order, sends no
    // on-chain tx. Run `--live --relayer-check` (add `--headless` for full logs).
    // Defaults to the PROD relayer (RELAYER_API_KEY is issued for polymarket.com);
    // set [live].relayer_url to override.
    if args.relayer_check {
        let Some((secrets, signer, _, deposit_wallet)) = &live_rt else {
            fatal("--relayer-check requires --live");
        };
        let Some(creds) = secrets.relayer.as_ref() else {
            fatal(
                "--relayer-check needs RELAYER_API_KEY + RELAYER_API_KEY_ADDRESS (copy both from \
                 Polymarket → Settings → Relayer API keys into your .env)",
            );
        };
        let relayer_url = config
            .live
            .relayer_url
            .clone()
            .unwrap_or_else(|| pm_execution::relayer::RELAYER_URL_PROD.to_string());
        let owner = signer.address();
        println!(
            "\n=== relayer-check: GET /nonce (READ-ONLY — no orders, no money, no on-chain tx) ==="
        );
        println!("owner (signer)          : {owner}");
        println!("deposit wallet          : {deposit_wallet}");
        println!("RELAYER_API_KEY_ADDRESS : {}", creds.api_key_address);
        println!("relayer                 : {relayer_url}\n");

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|e| fatal(format!("relayer-check http build: {e}")));
        println!("[1/1] GET /nonce?address={owner}&type=WALLET ...");
        match pm_execution::relayer::fetch_wallet_nonce(&http, &relayer_url, creds, owner).await {
            Ok(nonce) => {
                println!("  OK   WALLET nonce = {nonce}");
                println!(
                    "\nVerdict: ✅ relayer auth WORKS — the relayer accepted RELAYER_API_KEY and \
                     returned the WALLET nonce. Merge/redeem submission is reachable; the final \
                     step is a tiny FUNDED merge to confirm the on-chain round-trip."
                );
            }
            Err(e) => {
                println!("  FAIL  {e}");
                println!(
                    "\nVerdict: ❌ the relayer REJECTED the read. Check RELAYER_API_KEY + \
                     RELAYER_API_KEY_ADDRESS exactly match Polymarket → Settings → Relayer API \
                     keys, and that the relayer URL is right (paste the error above)."
                );
            }
        }
        return;
    }

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

    // REWARD-FARM mode (Task 7) forces confluence OFF: confluence is a TAKER
    // (smart-money directional) signal, while the reward farmer wants
    // reward-ELIGIBLE markets ranked by reward$/competition (built later in the
    // MM block from `MarketMetrics::reward_eligible`). Leaving confluence on would
    // also restrict the synced universe (`with_confluence_conditions`) to
    // smart-money conditions, starving the reward-eligible selection. So we guard
    // both the `build_confluence` call AND its `with_confluence_conditions`
    // application below on `!reward_farm_mode`. Spread-capture is untouched. Only
    // meaningful when the MM is enabled (the policy field is inert otherwise), so
    // arb-only / spread-capture runs keep confluence exactly as today.
    let reward_farm_mode = config.strategies.mm.enabled
        && pm_app::strategy::quote_policy::Policy::from_cfg(&config.strategies.mm.policy)
            == pm_app::strategy::quote_policy::Policy::RewardFarm;
    if reward_farm_mode && config.confluence.enabled {
        info!("reward_farm policy active: forcing confluence OFF (it is a taker signal); MM quotes reward-eligible markets");
    }

    // COPY mode drives the universe from its OWN EdgePerBet whitelist's active
    // markets (built just below), so confluence's separate top-PnL universe is
    // SKIPPED when copy is enabled — they are both smart-money universe sources
    // and would otherwise double-fetch the leaderboard at startup. The copy
    // whitelist-driven universe takes precedence; confluence remains the path for
    // MM-only directional runs.
    let copy_enabled = config.strategies.copy.enabled;
    if copy_enabled && config.confluence.enabled {
        info!("copy strategy active: confluence universe SKIPPED — the universe is the copy whitelist's active markets");
    }

    // [confluence] — "follow the smart money": build the universe from the top
    // leaderboard traders' OPEN positions (their favored side per market) instead
    // of the liquidity-ranked Gamma keyset. Runs at startup, so auto_restart
    // re-runs it each relaunch for a fresh snapshot. Best-effort: on any Data-API
    // failure (or an empty result) we log and FALL BACK to the normal universe.
    let confluence_markets = if config.confluence.enabled && !reward_farm_mode && !copy_enabled {
        if tui_active {
            println!("confluence: querying top-trader leaderboard + open positions ...");
        }
        info!(
            top_traders = config.confluence.top_traders,
            scan_limit = config.confluence.scan_limit,
            order_by = %config.confluence.order_by,
            period = %config.confluence.time_period,
            "confluence: building smart-money universe"
        );
        match build_confluence(&config).await {
            Ok(m) => m,
            Err(e) => {
                warn!("confluence: disabled this run ({e}); falling back to liquidity universe");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    // Bound the fetched-by-condition universe exactly like the keyset one: keep the
    // strongest-confluence ids up to the candidate pool (or `max_markets` when not
    // prioritizing) so a trader holding hundreds of longshots can't explode the
    // Gamma fetch. The aggregator already sorted them best-confluence-first.
    let confluence_conditions: Option<Vec<String>> = (!confluence_markets.is_empty()).then(|| {
        let cap = if config.universe.prioritize_by_liquidity {
            config
                .universe
                .candidate_pool
                .max(config.universe.max_markets)
        } else {
            config.universe.max_markets
        }
        .max(1);
        confluence_markets
            .iter()
            .take(cap)
            .map(|m| m.condition_id.clone())
            .collect()
    });
    // The favored-outcome venue (CLOB) token ids — in confluence mode the MM quotes
    // ONLY these (a directional lean toward the smart money). Empty when confluence
    // is off/empty, which `directional_quote_tokens` reads as "quote both sides".
    let favored_venue_ids: std::collections::HashSet<String> = confluence_markets
        .iter()
        .map(|m| m.favored_token.clone())
        .collect();
    if config.confluence.enabled && !reward_farm_mode && !copy_enabled {
        info!(
            markets = confluence_markets.len(),
            quoting_conditions = confluence_conditions.as_ref().map_or(0, Vec::len),
            "confluence: smart-money universe ready (empty ⇒ fell back to liquidity universe)"
        );
        // Progress marker (TUI hides the logs above in its ring buffer; print to
        // stdout BEFORE the alternate screen so startup isn't a silent freeze).
        if tui_active {
            println!(
                "arb: confluence ready — {} smart-money markets.",
                confluence_markets.len()
            );
        }
    }

    // COPY whitelist-driven universe: when the copy strategy runs, the synced
    // universe should BE the markets its OWN EdgePerBet whitelist is active in
    // (their OPEN positions) — the exact traders it copies — so a fresh-buy
    // signal lands in a synced, tradeable market (≈1:1 coverage) instead of a
    // generic liquidity scan. We build the whitelist ONCE here and SHARE it with
    // the strategy (`with_initial_whitelist`), so the heavy EdgePerBet rank isn't
    // run twice at startup and universe ↔ whitelist stay on one snapshot. Re-run
    // each auto_restart for a fresh snapshot. Best-effort: any Data-API failure
    // falls back to the liquidity universe (and the strategy builds its own
    // whitelist). Takes precedence over `confluence_conditions`.
    let mut copy_initial_whitelist: Option<pm_app::strategy::copy::Whitelist> = None;
    let copy_universe_conditions: Option<Vec<String>> = if copy_enabled {
        // Cap exactly like the confluence/keyset universe so a trader holding
        // hundreds of longshots can't explode the Gamma fetch.
        let cap = if config.universe.prioritize_by_liquidity {
            config
                .universe
                .candidate_pool
                .max(config.universe.max_markets)
        } else {
            config.universe.max_markets
        }
        .max(1);
        if tui_active {
            println!(
                "copy: building the smart-money universe from the EdgePerBet whitelist's active markets ..."
            );
        }
        info!(
            top_n = config.copy_params.top_n,
            min_bets = config.copy_params.min_bets,
            "copy: building whitelist-driven universe"
        );
        // One snapshot: build the specialist creamy layer exactly as the live loop,
        // then union its open-position condition ids. `1.0` shares ignores dust.
        let built: Option<(pm_app::strategy::copy::Whitelist, Vec<String>)> = async {
            let cp = CopyParams::from_config(&config.strategies.copy, &config.copy_params).ok()?;
            let client = DataApiClient::new(None).ok()?;
            let whitelist = pm_app::strategy::copy::refresh_whitelist(&client, &cp).await?;
            if whitelist.flat.is_empty() {
                return Some((whitelist, Vec::new()));
            }
            let conds = pm_app::strategy::copy::whitelist_universe_conditions(
                &client,
                &whitelist.flat,
                1.0,
                cap,
            )
            .await?;
            Some((whitelist, conds))
        }
        .await;
        match built {
            Some((wl, conds)) if !conds.is_empty() => {
                info!(
                    traders = wl.flat.len(),
                    categories = wl.creamy.len(),
                    markets = conds.len(),
                    "copy: whitelist-driven universe ready"
                );
                if tui_active {
                    println!(
                        "arb: copy universe ready — {} smart-money markets ({} specialists).",
                        conds.len(),
                        wl.flat.len()
                    );
                }
                copy_initial_whitelist = (!wl.flat.is_empty()).then_some(wl);
                Some(conds)
            }
            Some((wl, _)) => {
                warn!(
                    traders = wl.flat.len(),
                    "copy: whitelist-driven universe empty (no open positions / empty whitelist); falling back to liquidity universe"
                );
                copy_initial_whitelist = (!wl.flat.is_empty()).then_some(wl);
                None
            }
            None => {
                warn!(
                    "copy: whitelist-driven universe fetch failed; falling back to liquidity universe (strategy will build its own whitelist)"
                );
                None
            }
        }
    } else {
        None
    };

    // Precedence: copy whitelist-driven > confluence > liquidity (None).
    let universe_conditions = copy_universe_conditions.or(confluence_conditions);

    let filter = UniverseFilter {
        max_markets: config.universe.max_markets,
        require_active: config.universe.require_active,
        // Task 5.3 universe scaling knobs (opt-in; defaults keep keyset order).
        prioritize_by_liquidity: config.universe.prioritize_by_liquidity,
        candidate_pool: config.universe.candidate_pool,
        // Rank by the same `[segments]` thresholds the MM routing uses.
        segment_thresholds: segment_thresholds(&config),
    };
    let mut sync_task = SyncTask::new(
        clob_for_sync,
        &config.endpoints.gamma_base,
        PathBuf::from(&config.ingestion.relationships_path),
        filter,
        tx,
    )
    .unwrap_or_else(|e| fatal(format!("SyncTask init: {e}")))
    .with_confluence_conditions(universe_conditions);

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
    // Progress marker: the rate-limited CLOB sync is the longest silent phase in
    // TUI mode — surface its completion so the screen doesn't look frozen.
    if tui_active {
        println!(
            "arb: universe ready — {} markets, {} tokens.",
            reg.markets().len(),
            reg.all_tokens().len()
        );
    }

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
    let index = Arc::new(build_component_index(
        &reg,
        config.lp.nonexhaustive_negrisk_worlds,
    ));
    let chunk_size = config.ingestion.ws_chunk_size;
    let chunks = pack_components(&reg, chunk_size);

    // ---- shared state ------------------------------------------------------
    // The arb-internal channels (opp/lp/exec/report) now live inside
    // `ArbStrategy`; main only owns the process-wide kill flag and shared stats.
    let kill = Arc::new(AtomicBool::new(false));
    let stats = AppStats::new();

    // ---- supervisors per chunk (created now, spawned after the host) --------
    // The per-supervisor inline hook is the StrategyHost's combined `on_apply`,
    // which needs `ArbStrategy`, which needs the `BookFetcher` — and that is
    // built from THESE supervisors' command channels. So supervisors are CREATED
    // here (to populate `routes` → fetcher) and SPAWNED below, once the host
    // exists, each with `host.make_on_apply()` installed.
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
    let mut routes: HashMap<pm_core::instrument::TokenId, mpsc::Sender<SupervisorCommand>> =
        HashMap::new();
    let mut supervisors: Vec<Supervisor<ClobRest>> = Vec::new();

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

        // The inline detection hook (host.make_on_apply) is installed when these
        // supervisors are spawned below — once the host exists. Here we only
        // populate `routes` and share the stats cell.
        stat_cells.push(sup.share_stats());
        supervisors.push(sup);
    }

    if supervisors.is_empty() {
        eprintln!("error: no supervisors started (empty universe?)");
        // Tear down the writer cleanly before exiting.
        drop(store_tx);
        let _ = writer.await;
        std::process::exit(1);
    }
    let supervisors_started = supervisors.len();
    info!(supervisors = supervisors_started, "supervisors created");

    // ---- book fetcher ------------------------------------------------------
    // Built from the supervisors' command channels; cloned into the paper venue
    // and the publisher, and moved into the strategies via `HostShared` below.
    let fetcher = BookFetcher::new(routes);

    // ---- execution task builder --------------------------------------------
    // The execution task is now spawned INSIDE `ArbStrategy::run`; here we capture
    // the concrete venue (+ run_execution's static inputs) into an
    // `ExecTaskBuilder` closure that arb invokes with the arb-internal exec/report
    // channel halves + the per-run store_tx. Config-driven base ExecParams; the
    // PAPER arm uses it as-is, the LIVE arm forces redeem = Hold via
    // live_exec_params (see that fn).
    let exec_params = ExecParams {
        fill_window: Duration::from_millis(config.execution.fill_window_ms),
        max_unhedged: risk_cfg.max_unhedged,
        redeem: params.redeem,
    };
    // The live arm needs market_tokens for BOTH venue registration and
    // run_execution (which moves it); clone before either arm takes ownership.
    let market_tokens_for_registration = market_tokens.clone();
    // Task 4.5: stashed inside the live arm below (a CLONE of arb's resolved
    // creds/signer) iff the market maker is cleared for live, then consumed when
    // MM is wired — so MM's LiveVenue is identical to arb's with NO second key
    // derivation. Stays `None` on the paper arm (and whenever MM is paper).
    let mut mm_live_inputs: Option<MmLiveInputs> = None;
    // Task C5: the copy executor's live inputs — a CLONE of arb's resolved
    // creds/signer (NO second API key derivation), stashed in the live arm below
    // iff copy is cleared for live. Parallel to `mm_live_inputs`; copy can be
    // live with MM OFF (the copy canary does exactly that), so it has its OWN
    // stash. `None` on paper / arb / whenever copy is paper.
    let mut copy_live_inputs: Option<MmLiveInputs> = None;
    // M6-7: the LIVE on-chain relayer (deposit-wallet redeem / merge), built in
    // the live arm below when EITHER the reward-farm MM OR the copy executor is
    // cleared for live, and SHARED (same wallet/account) by whichever holds it.
    // `None` on paper / arb / non-relayer live — both strategies then keep the
    // hold-to-resolution no-op, so those paths are byte-for-byte unchanged. `Arc`
    // so each spawned, non-blocking sweep task shares the one client.
    let mut mm_merger: Option<std::sync::Arc<pm_execution::relayer::RelayerClient>> = None;
    // Capture the deposit-wallet address BEFORE `live_rt` is consumed below — the
    // MM's live seed reconcile (further down) reads its on-chain holdings. `Some`
    // iff this is a live run.
    let live_deposit_wallet = live_rt.as_ref().map(|(_, _, _, dw)| *dw);
    // Both arms produce the same ExecTaskBuilder so the binding unifies.
    let exec_builder: ExecTaskBuilder = if let Some((secrets, signer, proxy, deposit_wallet)) =
        live_rt
    {
        // M6-7: build the LIVE on-chain relayer BEFORE `secrets.api` is moved out
        // by the match below (so `&secrets` is still a whole borrow). Attempted
        // when EITHER the reward-farm MM OR the copy executor is cleared for live
        // — it is the SAME deposit-wallet account either way, so one client is
        // built and SHARED. `RelayerClient::new` itself returns `None` unless the
        // relayer is enabled AND relayer creds + deposit wallet + a valid EOA key
        // are present (OFF by default, staging-first), so arb-only / non-relayer
        // live stay no-op. The copy executor uses it to redeem resolved winners;
        // without it, copy holds resolved positions for the next reconcile.
        let mm_live_cleared =
            config.strategies.mm.enabled && mm_use_live(args.live, config.strategies.mm.live);
        let copy_live_cleared =
            config.strategies.copy.enabled && mm_use_live(args.live, config.strategies.copy.live);
        if mm_live_cleared || copy_live_cleared {
            let relayer_http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_else(|e| fatal(format!("relayer http client build failed: {e}")));
            mm_merger =
                pm_execution::relayer::RelayerClient::new(&config.live, &secrets, relayer_http)
                    .map(std::sync::Arc::new);
            if mm_merger.is_some() {
                warn!(
                    staging = config.live.relayer_staging,
                    "M6-7: LIVE on-chain MERGE relayer ENABLED — reward-farm complete sets will \
                     be recycled on-chain (periodic, NON-blocking sweep). The submit→confirm \
                     round-trip is validated for real only at the first FUNDED STAGING run."
                );
            } else if config.live.relayer_enabled {
                warn!(
                    "live.relayer_enabled = true but the relayer could NOT be constructed \
                     (missing relayer creds / deposit wallet / unparseable key) — live merge \
                     stays the hold-to-resolution no-op"
                );
            }
        }
        // CLOB trading credentials. py-clob-client-v2 derives these from a
        // PLAIN-EOA L1 signature (create_or_derive_api_key): POLY_ADDRESS = the
        // EOA, plain ECDSA, the key binds to the EOA. The deposit wallet / funder
        // plays NO part in key derivation — it is only the order maker
        // (signatureType 3). We mirror that exactly. An operator can still
        // override with a pre-provisioned PM_API_* key (e.g. one minted by
        // Polymarket's own UI flow).
        // `auth_address` = L2 POLY_ADDRESS + order.signer = the EOA. The CLOB cred
        // authenticates AS the EOA (the frontend map's `baseAddress` field): L2
        // with POLY_ADDRESS = EOA returns 200, with the proxy returns 401 "Invalid
        // api key". The order MAKER is the deposit-wallet proxy (where the funds +
        // the key's order-association live) — that is what the venue's key↔maker
        // check compares (py-clob-client-v2 #64), so maker must be the proxy, not
        // the EOA. (L1 key derive also uses the EOA, plain ecrecover.)
        let (creds, auth_address) = match secrets.api {
            Some(c) => {
                info!("live venue: using operator-supplied PM_API_* credentials");
                (c, signer.address())
            }
            None => {
                let http = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()
                    .unwrap_or_else(|e| fatal(format!("auth http client build failed: {e}")));
                let server_time_s = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                info!("live venue: deriving CLOB API credentials (plain-EOA L1, key binds to EOA)");
                let creds = pm_execution::auth::derive_or_create_api_key(
                    &http,
                    &config.endpoints.clob_base,
                    &signer,
                    server_time_s,
                    None, // plain-EOA path: bind the key to the EOA (py-clob-client-v2)
                )
                .await
                .unwrap_or_else(|e| {
                    fatal(format!(
                        "could not derive CLOB API credentials via plain-EOA L1: {e}. \
                         If your account needs a pre-provisioned key, set \
                         PM_API_KEY / PM_API_SECRET / PM_API_PASSPHRASE instead."
                    ))
                });
                // Auth as the EOA (the key's baseAddress); maker is the proxy.
                (creds, signer.address())
            }
        };
        // No secret fields on this line — keep it that way (creds/signer must
        // never be interpolated into logs).
        info!(shadow = args.shadow, "live venue armed (api key ready)");
        if tui_active {
            println!("arb: live venue armed (api key ready); finishing startup ...");
        }
        // Task 4.5: if the market maker is cleared for live, stash a CLONE of
        // these EXACT live inputs BEFORE they move into arb's venue below, so MM
        // builds a byte-identical LiveVenue (same creds/signer/deposit_wallet/
        // auth_address/base) for the SAME account with NO second key derivation.
        // `args.live` is necessarily true in this arm (live_rt is Some iff
        // --live), so `mm_use_live` reduces to the strategy opt-in; the typed
        // confirmation already ran at startup. SHARED-ACCOUNT RATE BUDGET: arb +
        // MM then hold INDEPENDENT REST limiters against the one account budget
        // (documented at the MM wiring) — fine for a tiny canary.
        if config.strategies.mm.enabled
            && mm_use_live(args.live, config.strategies.mm.live)
        {
            mm_live_inputs = Some(MmLiveInputs {
                base: config.endpoints.clob_base.clone(),
                creds: creds.clone(),
                signer: signer.clone(),
                proxy,
                deposit_wallet,
                auth_address,
            });
        }
        // Task C5: likewise stash a CLONE for the COPY executor when it is
        // cleared for live (independent of MM — copy can be live with MM off, as
        // the copy canary is). Same account/creds/signer/deposit_wallet as arb,
        // so the copy `LiveVenue` is byte-identical with NO second key derivation.
        if config.strategies.copy.enabled
            && mm_use_live(args.live, config.strategies.copy.live)
        {
            copy_live_inputs = Some(MmLiveInputs {
                base: config.endpoints.clob_base.clone(),
                creds: creds.clone(),
                signer: signer.clone(),
                proxy,
                deposit_wallet,
                auth_address,
            });
        }
        // Capture the EOA before `signer` is moved into the venue cfg (probe diag).
        let eoa = signer.address();
        let mut venue = pm_execution::live::LiveVenue::new(pm_execution::live::LiveVenueCfg {
            base: config.endpoints.clob_base.clone(),
            creds,
            signer,
            proxy,
            deposit_wallet,
            auth_address,
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
                        venue.register_token(tok, vid.to_owned(), m.neg_risk, m.tick);
                    }
                }
            }
        }
        // Probe-order: place ONE tiny 5-share FAK BUY through the real signing /
        // auth / submit path on a cheap, liquid YES token, report the venue's
        // verdict, then exit. Settles whether a type-3 deposit-wallet order is
        // accepted, without waiting for an arbitrage to appear.
        if args.probe_order {
            use pm_core::instrument::TokenId;
            use pm_core::num::{Bps, Px, Qty, TickSize};
            use pm_engine::Action;
            use pm_execution::venue::ExecutionVenue; // brings submit_fak into scope
            const COST_CAP_TICKS: u16 = 50; // ignore asks above 0.50/share (keeps the probe cheap)
            const GOOD_ENOUGH_TICKS: u16 = 5; // ≤ 0.05 found → stop scanning, buy now
            const MAX_FETCHES: u32 = 50;
            // The venue rejects marketable BUYs under $1 of value ("min size: 1");
            // size the probe to ~$1.10 (in cents) to clear it with margin.
            const MIN_VALUE_CENTS: u16 = 110;
            println!("\n=== probe-order: one tiny FAK BUY via the real signed-order path ===");
            println!("maker (deposit): {deposit_wallet}");
            println!("signer (EOA)   : {eoa}");
            println!("(the venue's marketable-BUY minimum is $1; sized to ~$1.10 — at ~$0.10/share that's ~11 shares)\n");
            // Scan a sample of tokens (both sides) and keep the CHEAPEST fillable
            // ask, so the probe always finds something to fill, at minimal cost.
            let mut fetches = 0u32;
            let mut best: Option<(TokenId, TickSize, Bps, Px)> = None;
            'scan: for m in reg.markets() {
                if !matches!(m.tick, TickSize::Cent)
                    || !market_tokens_for_registration.contains_key(&m.id)
                {
                    continue;
                }
                for token in [m.yes, m.no] {
                    if fetches >= MAX_FETCHES {
                        break 'scan;
                    }
                    if reg.token_venue_id(token).is_none() {
                        continue;
                    }
                    fetches += 1;
                    let Ok(Some(ask)) = venue.best_ask(token, m.tick).await else {
                        continue;
                    };
                    if ask.get() == 0 || ask.get() > COST_CAP_TICKS {
                        continue;
                    }
                    let cheaper = match &best {
                        None => true,
                        Some((_, _, _, b)) => ask.get() < b.get(),
                    };
                    if cheaper {
                        best = Some((token, m.tick, m.fee_bps, ask));
                        if ask.get() <= GOOD_ENOUGH_TICKS {
                            break 'scan; // cheap enough — no need to scan further
                        }
                    }
                }
            }
            match best {
                None => println!(
                    "\nNo Cent-tick token had an ask in (0, $0.50] within {fetches} book fetches.\nMarkets may be unusually pricey — re-run, or tell me to widen the cap."
                ),
                Some((token, tick, fee, ask)) => {
                    // ask.get() is the per-share price in cents (Cent-tick markets only).
                    // Buy enough shares to clear the venue's $1 marketable-BUY minimum
                    // (~$1.10 of value), never below the legacy 5-share floor.
                    let ticks = ask.get();
                    let shares = MIN_VALUE_CENTS.div_ceil(ticks).max(5);
                    let cost = f64::from(shares) * f64::from(ticks) / 100.0;
                    println!(
                        "cheapest fillable: token {} @ {} ticks (= ${:.2}/share) — buying {} shares (≈ ${:.2}) ...",
                        token.0,
                        ticks,
                        f64::from(ticks) / 100.0,
                        shares,
                        cost
                    );
                    let order = pm_execution::Order::new(
                        "probe-order".into(),
                        token,
                        Action::Buy,
                        tick,
                        ask,
                        Qty(u64::from(shares) * 1_000_000),
                        fee,
                    );
                    match venue.submit_fak(&order).await {
                        Ok(out) => {
                            println!("\n✅ ACCEPTED — the signed order path WORKS.");
                            println!("   venue_order_id = {:?}", out.venue_order_id);
                            println!(
                                "   filled = {} shares ({} µ), {} fill(s)",
                                out.filled.0 / 1_000_000,
                                out.filled.0,
                                out.fills.len()
                            );
                            println!(
                                "\nVerdict: the type-3 deposit-wallet signature / auth / signer are all\n         correct. An arb basket's buy legs submit identically — the M5 live\n         order path is PROVEN."
                            );
                        }
                        Err(e) => {
                            println!("\n❌ venue returned an error:\n   {e}");
                            println!(
                                "\nVerdict: if this is \"signer must = API key\", order.signer must flip\n         (deposit wallet ↔ EOA). Paste this and I'll adjust + you retest."
                            );
                        }
                    }
                }
            }
            return;
        }
        let token_market_exec = token_market.clone();
        Box::new(move |exec_rx, report_tx, store_tx| {
            Box::pin(run_execution(
                venue,
                exec_rx,
                report_tx,
                store_tx,
                token_market_exec,
                market_tokens,
                token_fee,
                // Live never performs on-chain ops: a filled C1Long HOLDS its complete set
                // (manual redeem until M6). venue.merge would return NotSupportedLive and
                // fail the basket AFTER real money filled (integration-review catch).
                live_exec_params(&exec_params),
            ))
        })
    } else {
        let venue = PaperVenue::new(
            fetcher.clone(),
            Duration::from_millis(config.execution.paper_latency_ms),
            params.gas,
        );
        let token_market_exec = token_market.clone();
        Box::new(move |exec_rx, report_tx, store_tx| {
            Box::pin(run_execution(
                venue,
                exec_rx,
                report_tx,
                store_tx,
                token_market_exec,
                market_tokens,
                token_fee,
                exec_params,
            ))
        })
    };

    // Clone the fetcher for the publisher BEFORE it is moved into HostShared
    // (the publisher marks open positions at the live bid). None in headless.
    let fetcher_pub = if tui_active {
        Some(fetcher.clone())
    } else {
        None
    };

    // ---- strategies: arb (#1) + heartbeat (#2) via the StrategyHost ---------
    // Live dispatch params (spec §Mode ladder). released_at_start is true for
    // paper (inert — no live venue), shadow (signs but no money moves), and
    // headless live (the typed phrase was demanded at startup); it is HELD only
    // for TUI live, where the `l` modal releases the latch. Passed verbatim into
    // arb's coordinator, so the live gating is byte-identical.
    // Bound here so the MM reuses the SAME live gate: it trades real money too,
    // so it must NOT quote while live is HELD (TUI live before the `l` release).
    // `resume_live` forces RELEASED — an already-confirmed session continuing
    // across an auto-restart re-sync resumes trading instead of re-holding.
    let released_at_start = !args.live || args.shadow || !tui_active || resume_live;
    // Tracks whether live dispatch is currently RELEASED, for the auto-restart
    // re-exec to carry forward: starts at `released_at_start`, flips true on the
    // `l` modal. A held session that never released re-execs WITHOUT resume.
    let mut live_released = released_at_start;
    if resume_live {
        info!("resume: auto-restart of an already-released live session — resuming live (no re-hold/confirm)");
    }
    let live_params = LiveParams {
        live: args.live,
        released_at_start,
        basket_cap: pm_core::num::Usdc(
            pm_config::usd_to_microusdc(config.live.basket_cap_usd)
                .unwrap_or_else(|e| fatal(format!("live.basket_cap_usd: {e}"))),
        ),
        min_leg: pm_core::num::Qty((config.live.min_leg_shares * 1e6).round() as u64),
        min_leg_value: pm_core::num::Usdc(
            pm_config::usd_to_microusdc(config.live.min_leg_value_usd)
                .unwrap_or_else(|e| fatal(format!("live.min_leg_value_usd: {e}"))),
        ),
    };
    // Platform bankroll = today's configured bankroll. The capital carve-out
    // (Task 4.4b, `strategy_envelopes`) splits it between arb and the OPTIONAL
    // market maker:
    //  - MM OFF (default): BYTE-IDENTICAL to before — arb's envelope AND its
    //    enforced RiskEngine cap claim the WHOLE bankroll, the heartbeat claims
    //    zero, `mm_envelope` is None, so only arb + heartbeat are added. This is
    //    the live arb path the user runs today; nothing about it changes.
    //  - MM ON: `mm.capital_usd` is carved OUT and arb's RiskEngine cap
    //    (`arb_risk.bankroll`) is REDUCED to `bankroll − mm_capital`, so arb +
    //    MM SHARE the bankroll without overlapping real funds.
    // Σcapital == bankroll either way → the startup allocator passes.
    let bankroll = risk_cfg.bankroll;
    let PlatformEnvelopes {
        arb: arb_envelope,
        heartbeat: hb_envelope,
        mm: mm_envelope,
        copy: copy_envelope,
        btc5m: btc5m_envelope,
        arb_risk,
    } = strategy_envelopes(&config, &risk_cfg, bankroll)
        .unwrap_or_else(|e| fatal(format!("strategy capital carve-out: {e}")));

    // Arb keeps every Coordinator/Detector/LP-pool/execution input it has today
    // (byte-identical); ONLY its RiskEngine `bankroll` differs — unchanged when
    // MM is off, reduced to arb's slice (`arb_risk`) when MM is on so it trades
    // within it. The venue arrives as the ExecTaskBuilder captured above, and
    // `starts` is forwarded to the coordinator's restart-storm guard inside arb's
    // run (note_session_starts).
    let arb = ArbStrategy::new(
        config.clone(),
        arb_risk,
        params,
        token_market,
        index,
        Arc::clone(&stats),
        lp_min_interval,
        config.lp.solver_concurrency,
        live_params,
        starts,
        exec_builder,
    );
    // Obtain arb's control/status handles BEFORE moving it into the host:
    // arb_status_rx feeds the publisher's arb-process badges + the final summary;
    // live_release_sender is the TUI `l`-modal's path into the coordinator;
    // arb_coordinator_aborted is the coordinator-death health signal (arb's run
    // swallows the coordinator JoinError, so the host's task-outcome check can't
    // see a mid-session coordinator panic — this flag does).
    let arb_status_rx = arb.arb_status_rx();
    let live_release_sender = arb.live_release_sender();
    let arb_coordinator_aborted = arb.coordinator_aborted();

    let mut host = StrategyHost::new(bankroll);
    host.add(Box::new(arb), arb_envelope);
    host.add(
        Box::new(HeartbeatStrategy::new(StrategyId("heartbeat"))),
        hb_envelope,
    );

    // ---- market maker (#3): DEFAULT-OFF, behind [strategies.mm] enabled -----
    // Present only when the carve-out produced an `mm_envelope` (i.e. MM is
    // enabled). When MM is disabled this whole block is skipped, leaving the arb
    // path the user runs today completely untouched.
    //
    // Task 4.5 LIVE GATING: MM trades REAL maker orders ONLY when cleared by
    // `mm_use_live(args.live, mm.live)` — the PROCESS is --live (which ALSO forced
    // the typed `confirm_phrase` at startup, see the live-arming block above) AND
    // the operator opted the STRATEGY in (`[strategies.mm].live`). EVERY other
    // combination uses the PAPER maker venue (the unchanged 4.4b default); the
    // paper arm never constructs or touches a LiveVenue.
    if let Some(mm_envelope) = mm_envelope {
        let mm_capital = mm_envelope.capital;
        let arb_capital_micro = bankroll.0 - mm_capital.0;
        let mm_live = mm_use_live(args.live, config.strategies.mm.live);

        // Task 4.6 — the LIVE maker-fill source: `"ws"` (default; low-latency
        // user-WS feed) or `"rest"` (the Task-4.5 REST poll). Decided once from
        // the validated config; the variant is built after token registration.
        let mm_fills_source = MmFillsSource::from_config(&config.strategies.mm.live_fills_source);
        // The user-WS URL is the sibling of the market WS URL (…/ws/market →
        // …/ws/user); derived once so no second config field is needed.
        let mm_user_ws_url = user_ws_url(&config.endpoints.ws_market_url);

        // Build MM's OWN LiveVenue up front when cleared, so its token set is
        // registered in the SAME loop that selects MM's universe. It reuses the
        // EXACT arb live inputs (creds/signer/deposit_wallet/auth_address/base)
        // stashed (cloned) in the live-arming block BEFORE they moved into arb's
        // venue — so NO second API key is derived — plus `shadow: args.shadow`
        // (a `--live --shadow` MM run signs but never submits: a dry run, exactly
        // like arb). SHARED-ACCOUNT RATE BUDGET CAVEAT: this is a SECOND
        // LiveVenue, so arb + MM each own an INDEPENDENT REST limiter against the
        // SAME Polymarket account budget. Acceptable for a tiny canary; prefer
        // conservative `ingestion.rest_rate_*` (a shared limiter is future work).
        // `mm_ws_creds` keeps a CLONE of the creds for the user-WS subscribe auth
        // (the WS source owns them by value), captured before the cfg moves them.
        let mut mm_ws_creds: Option<pm_execution::secrets::ApiCreds> = None;
        let mut mm_live_venue: Option<pm_execution::live::LiveVenue> = if mm_live {
            let inputs = mm_live_inputs.take().unwrap_or_else(|| {
                fatal("internal: MM cleared for live but live inputs were not stashed")
            });
            mm_ws_creds = Some(inputs.creds.clone());
            let venue = pm_execution::live::LiveVenue::new(pm_execution::live::LiveVenueCfg {
                base: inputs.base,
                creds: inputs.creds,
                signer: inputs.signer,
                proxy: inputs.proxy,
                deposit_wallet: inputs.deposit_wallet,
                auth_address: inputs.auth_address,
                fill_window: Duration::from_millis(config.execution.fill_window_ms),
                rate_per_sec: config.ingestion.rest_rate_per_sec,
                rate_capacity: config.ingestion.rest_rate_capacity,
                shadow: args.shadow,
            })
            .unwrap_or_else(|e| fatal(format!("MM LiveVenue: {e}")));
            Some(venue)
        } else {
            None
        };

        // MM's universe (Task 5.2 — PER-SEGMENT ROUTING): instead of the first
        // `max_markets` registry markets, the MM quotes only the markets in its
        // allowed liquidity segments (`[segments].mm_segments`, default
        // LiquidStable + Liquid — NEVER Illiquid), skipping fee-free markets when
        // `[segments].mm_exclude_fee_free` (default true; the rebate-driven MM
        // earns no rebate on a fee-free market). The eligible markets are RANKED
        // by PER-MARKET volume (then liquidity) and DE-CONCENTRATED to at most
        // `max_per_event` per event/component before the `max_markets` cap, so
        // the MM spreads across DISTINCT markets instead of piling into one
        // event's many outcomes (which all inherit the event's liquidity). ARB
        // IS UNAFFECTED — it runs on every market unconditionally as the universal
        // safety net; this routing is MM-only and only takes effect because the
        // MM is enabled in this block.
        //
        // For each selected market we build the token set + `token → MarketId`
        // map and (when live) register each token on MM's venue in the SAME pass
        // (the same `register_token(tok, vid, neg_risk, tick)` shape arb uses) —
        // an order for an unregistered token is rejected before any I/O, so MM
        // can only ever place orders within its own carved universe.
        // MM UNIVERSE SELECTION — two policies (spec §5/§7):
        //  * spread_capture (default) → Task 5.2 per-segment routing, UNCHANGED.
        //  * reward_farm (Task 7) → reward-ELIGIBLE markets ranked by an edge
        //    proxy (reward$/competition) and greedily capped to the cash budget.
        let mm_markets: Vec<pm_core::instrument::MarketId> = if reward_farm_mode {
            // REWARD-FARM universe (Task 7, spec §7): every reward-eligible
            // registry market, ranked by `edge = daily_rate / competing_depth`,
            // greedily funded best-edge-first until the cash budget is spent
            // (`select_reward_markets`).
            //
            // DOCUMENTED FALLBACKS — no live book exists at universe-selection
            // time (sync has assembled the registry, but per-market order books
            // are streamed by the engine loop only AFTER this wiring), so two
            // inputs use neutral constants rather than forcing a large refactor:
            //  * competing_depth = 1.0 → ranking collapses to pure `daily_rate`,
            //    the only competition-free signal available here.
            //  * mid = 0.5 → the per-market cost to PARK the minimum incentive
            //    orders is sized off this max-uncertainty binary price (a real
            //    mid replaces it once a selection-time book snapshot is plumbed):
            //      - NON-hedging (Spec-1, ONE token, bid + ask): 2 × min_size × mid.
            //      - HEDGING (Spec-2 Phase B, the complement PAIR — bid YES + bid
            //        NO, BOTH buys): YES-bid notional + NO-bid notional =
            //        min_size·mid + min_size·(1−mid) ≈ min_size·1.0 at the 0.5
            //        fallback (a full YES+NO share-set ≈ $1), so the budget funds
            //        the right number of PAIRS instead of under-counting a leg.
            // CONCERN (later refinement): rank by real in-band resting depth and
            // size cost off a real mid once a selection-time book snapshot is
            // plumbed through to this point.
            let mid = 0.5_f64;
            // Reward-farm hedging is on only when [reward_farm].hedging_enabled
            // (this block is already reward-farm-only); it deploys TWO bid legs
            // per market, so each funded market must reserve BOTH (vs. the
            // single-token bid+ask under Spec-1).
            let hedging = config.reward_farm.hedging_enabled;
            let cands: Vec<(u64, f64, f64, f64)> = reg
                .markets()
                .iter()
                .filter_map(|m| {
                    let metrics = reg.metrics(m.id)?;
                    if !metrics.reward_eligible() {
                        return None;
                    }
                    let daily_rate = metrics.reward_daily_rate_usd;
                    let competing_depth = 1.0; // no book yet → rank by daily_rate alone
                    let per_market_cost = if hedging {
                        // BOTH bid legs (YES + NO), both buys: min_size·mid_yes +
                        // min_size·mid_no = min_size·mid + min_size·(1−mid).
                        metrics.reward_min_size * mid + metrics.reward_min_size * (1.0 - mid)
                    } else {
                        // ONE token, two sides (bid + ask): 2 × min_size × mid.
                        2.0 * metrics.reward_min_size * mid
                    };
                    Some((u64::from(m.id.0), daily_rate, competing_depth, per_market_cost))
                })
                .collect();
            let eligible = cands.len();
            // Per-market cost diagnostics, computed BEFORE the budget fit (which
            // consumes `cands`). `reward_eligible()` already guarantees
            // `daily_rate > 0`, so the ONLY non-budget drop inside
            // `select_reward_markets` is `per_market_cost == 0` — a degenerate
            // `reward_min_size`. Surfacing that count (vs. budget drops) and the
            // cheapest POSITIVE cost lets an operator see WHY funding is low.
            let unfundable_zero_cost =
                cands.iter().filter(|(_, _, _, cost)| *cost <= 0.0).count();
            let cheapest_cost = cands
                .iter()
                .map(|&(_, _, _, cost)| cost)
                .filter(|c| *c > 0.0)
                .reduce(f64::min);
            let picked = pm_app::strategy::quote_policy::select_reward_markets(
                cands,
                config.strategies.mm.capital_usd,
            );
            // Hardened: ids round-trip from `MarketId(u32)` → u64 and back, so the
            // narrowing is safe today, but `try_from` makes a future widening of
            // `MarketId` fail loudly instead of silently truncating.
            let selected: Vec<pm_core::instrument::MarketId> = picked
                .into_iter()
                .map(|id| {
                    pm_core::instrument::MarketId(u32::try_from(id).expect("market id fits u32"))
                })
                .collect();
            let funded = selected.len();
            let budget = config.strategies.mm.capital_usd;
            // Always record the full breakdown: eligible vs funded, and WHY the
            // rest dropped (degenerate zero-cost vs budget).
            info!(
                eligible,
                funded,
                unfundable_zero_cost,
                budget_usd = budget,
                cheapest_cost_usd = ?cheapest_cost,
                "MM reward-farm selection: reward-eligible markets ranked by edge=daily_rate/competing_depth (competing_depth=1.0, mid=0.5 at selection time), budget-capped"
            );
            // Funding nothing — or fewer than `reward_farm.min_markets` — is a
            // budget/min_size ECONOMICS issue, not a bug (`min_markets` is
            // advisory; we never synthesize, spec §7). Spell out the likely fix so
            // the operator isn't left guessing why the MM quotes (almost) nothing.
            if funded == 0 || funded < config.reward_farm.min_markets as usize {
                let min_markets = config.reward_farm.min_markets;
                match cheapest_cost {
                    Some(c) => warn!(
                        eligible,
                        funded,
                        unfundable_zero_cost,
                        min_markets,
                        budget_usd = budget,
                        cheapest_cost_usd = c,
                        "MM reward-farm funded {funded} markets (eligible {eligible}, budget ${budget:.2}, cheapest per-market cost ~${c:.2}) — capital_usd too small for the per-market cost; increase capital_usd or target lower-min_size markets"
                    ),
                    None => warn!(
                        eligible,
                        funded,
                        unfundable_zero_cost,
                        min_markets,
                        budget_usd = budget,
                        "MM reward-farm: no fundable reward-eligible markets (eligible {eligible}, {unfundable_zero_cost} with per_market_cost == 0 / degenerate reward_min_size) — nothing to quote; check rewards data and target positive-min_size reward markets"
                    ),
                }
            }
            selected
        } else {
            // SPREAD-CAPTURE (Task 5.2 per-segment routing) — UNCHANGED.
            let mm_thresholds = segment_thresholds(&config);
            let mm_allowed = mm_allowed_segments(&config);
            let mm_markets = mm_market_selection(
                &reg,
                &mm_thresholds,
                &mm_allowed,
                config.segments.mm_exclude_fee_free,
                config.strategies.mm.max_markets,
                config.strategies.mm.max_per_event,
            );
            // Eligible-before-cap list, for the routing log only (how many markets
            // qualified — across how many distinct events/components — vs. how many
            // we actually quote after the caps). Recomputed UNCAPPED (`usize::MAX`
            // markets, `0` = no per-event cap) so it reflects the full eligible set;
            // the selection is a cheap startup-time filter+rank over the registry.
            let mm_eligible_markets = mm_market_selection(
                &reg,
                &mm_thresholds,
                &mm_allowed,
                config.segments.mm_exclude_fee_free,
                usize::MAX,
                0,
            );
            // Distinct events/components among the eligible markets (a NegRisk event's
            // outcomes share one `component_of`), so the log surfaces how many events
            // the per-event cap is spreading the MM across.
            let mm_eligible_events = mm_eligible_markets
                .iter()
                .map(|&id| reg.component_of(id))
                .collect::<std::collections::HashSet<_>>()
                .len();
            info!(
                "MM segment routing: {} eligible across {} events (segments={:?}, \
                 exclude_fee_free={}) → quoting top {} (≤{} per event)",
                mm_eligible_markets.len(),
                mm_eligible_events,
                mm_allowed,
                config.segments.mm_exclude_fee_free,
                mm_markets.len(),
                config.strategies.mm.max_per_event,
            );
            mm_markets
        };
        // MarketId → Market lookup for the selected ids (registry is dense, but a
        // map keeps the call site independent of that invariant).
        let mm_market_by_id: HashMap<pm_core::instrument::MarketId, pm_core::instrument::Market> =
            reg.markets().iter().map(|m| (m.id, *m)).collect();

        // Spec-2 Phase B (§5.1): complement-pair HEDGING — only meaningful in
        // reward-farm mode. When on, the MM quotes the BID PAIR (YES + NO) per
        // market for two-sided-from-flat reward farming with no naked short.
        let mm_hedging = reward_farm_mode && config.reward_farm.hedging_enabled;
        let mut mm_tokens = Vec::new();
        let mut mm_token_market = HashMap::new();
        // Spec-2 Phase B: yes↔no per quoted market (populated only under hedging),
        // threaded into the MM so the loop/estimator (B3/B4) can pair the two bids.
        let mut mm_complement: HashMap<pm_core::instrument::TokenId, pm_core::instrument::TokenId> =
            HashMap::new();
        // M6-7: token → on-chain conditionId (B256) for the quoted reward-farm
        // universe — the LIVE merge sweep needs it to build each `mergePositions`
        // batch. Populated alongside `mm_complement` (hedging only); empty
        // otherwise, so non-hedging / paper / arb runs thread an empty map.
        let mut mm_cond_by_token: HashMap<pm_core::instrument::TokenId, alloy_primitives::B256> =
            HashMap::new();
        // R2 (auto-redeem): token → CLOB asset id for the quoted reward-farm
        // universe — the resolved-winner redeem sweep matches a Data-API
        // `Position.asset` back to our `TokenId` (for the resolved-price lookup).
        // Populated alongside the conditionId map (hedging only); empty otherwise.
        let mut mm_venue_by_token: HashMap<pm_core::instrument::TokenId, String> = HashMap::new();
        // Task 4.6 user-WS inputs (live only): the markets' condition_ids to
        // subscribe to, and the asset_id→(TokenId, TickSize) map the WS fills
        // source resolves each trade's `asset_id` against (the SAME shape the
        // LiveVenue's REST poll resolves internally).
        let mut mm_condition_ids: Vec<String> = Vec::new();
        let mut mm_ws_resolve: HashMap<
            String,
            (pm_core::instrument::TokenId, pm_core::num::TickSize),
        > = HashMap::new();
        // TOKEN SELECTION per market (`mm_quote_tokens`):
        //  * reward_farm (spec §9) — a SINGLE token (the `yes` outcome): Spec 1 is
        //    single-token two-sided quoting; the complement is a Spec-2 hedging
        //    concern. Mapping both would under-budget each market 2× (per_market_cost
        //    is 2 orders on ONE token) and double-count its reward pool in the
        //    per-token estimator.
        //  * confluence — ONLY the favored outcome (the side the top traders hold).
        //  * neither — BOTH sides (the normal spread-capture MM universe).
        // Either way we still REGISTER + WS-resolve BOTH tokens on the venue so a
        // stray fill on the unquoted side always resolves to a known token.
        for &mid in &mm_markets {
            let m = mm_market_by_id[&mid];
            // Subscribe by condition_id (market), NOT token id (Task 4.6).
            if mm_live && let Some(cid) = reg.market_condition(m.id) {
                mm_condition_ids.push(cid.to_string());
            }
            let quote_toks =
                mm_quote_tokens(&reg, &m, &favored_venue_ids, reward_farm_mode, mm_hedging);
            // Spec-2 Phase B (§5.1): under hedging the MM quotes BOTH outcomes as a
            // bid pair, so record this market's yes↔no complement (both directions)
            // for the markets we actually quote — B3/B4 use it to pair the two bids
            // and score them as the `m`/`m'` books. Empty off the hedging path.
            if mm_hedging {
                mm_complement.insert(m.yes, m.no);
                mm_complement.insert(m.no, m.yes);
                // R2 (auto-redeem): record both legs' CLOB asset ids so the redeem
                // sweep can match a resolved `Position.asset` back to the leg.
                for tok in [m.yes, m.no] {
                    if let Some(vid) = reg.token_venue_id(tok) {
                        mm_venue_by_token.insert(tok, vid.to_owned());
                    }
                }
                // M6-7: map BOTH legs to the market's single on-chain conditionId
                // (hex from the registry) so the live merge sweep can build its
                // `mergePositions` batch. A market with no / an unparseable
                // condition id simply isn't merge-eligible (held to resolution).
                match reg
                    .market_condition(m.id)
                    .and_then(|c| c.parse::<alloy_primitives::B256>().ok())
                {
                    Some(cid) => {
                        mm_cond_by_token.insert(m.yes, cid);
                        mm_cond_by_token.insert(m.no, cid);
                    }
                    None => warn!(
                        market = ?m.id,
                        "MM hedging: market has no parseable condition_id — its complete sets \
                         can't be merged on-chain (held to resolution)"
                    ),
                }
            }
            for tok in [m.yes, m.no] {
                if let Some(vid) = reg.token_venue_id(tok) {
                    if mm_live {
                        mm_ws_resolve.insert(vid.to_owned(), (tok, m.tick));
                    }
                    if let Some(v) = mm_live_venue.as_mut() {
                        v.register_token(tok, vid.to_owned(), m.neg_risk, m.tick);
                    }
                }
                if quote_toks.contains(&tok) {
                    mm_tokens.push(tok);
                    mm_token_market.insert(tok, m.id);
                }
            }
        }
        // Distinct quoted markets (confluence mode quotes ~one token per market, so
        // a `/2` token count would be wrong) — count unique MarketIds we will quote.
        let mm_market_count = mm_token_market
            .values()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len();

        // Startup venue reconciliation (live only), mirroring arb's check: a
        // resting/maker path should leave little open, so any open order is worth
        // a warning. Reads the SAME account as arb's check (shared account).
        if let Some(v) = mm_live_venue.as_mut() {
            match v.open_orders().await {
                Ok(open) if !open.is_empty() => {
                    // A maker path should leave nothing resting between sessions;
                    // any open order is a STRANDED orphan (e.g. a prior run that
                    // exited before cancel_all). Surface it loudly; the in-session
                    // cancel_all is the primary guard against leaving orphans.
                    warn!(count = open.len(), "MM venue reports open orders at startup (possible orphan — check the account)")
                }
                Ok(_) => {}
                Err(e) => warn!("MM venue open-orders check failed at startup: {e}"),
            }
        }

        let mm_params = MmParams::from_config(&config.strategies.mm, &config.reward_farm)
            .unwrap_or_else(|e| fatal(format!("MmParams::from_config: {e}")));
        let mm_inv_cfg =
            inventory_config(&config).unwrap_or_else(|e| fatal(format!("inventory_config: {e}")));
        // INVENTORY RELOAD (Phase-4 seed wiring; auto-restart correctness): resume
        // the MM's signed inventory from its persisted lots so a restart starts
        // from the REAL position (and can offload it via the ask side) instead of
        // flat. The writer `store` was moved into the writer task, so read via a
        // second read-only WAL connection. Scoped to strategy "mm" and to tokens
        // we will quote, so a held position is actually worked off.
        let db_seed: Vec<(pm_core::instrument::TokenId, i128, pm_core::num::Usdc)> =
            pm_store::read::ReadStore::open(Path::new(&config.store.path))
                .and_then(|rs| rs.open_positions())
                .map(|rows| {
                    rows.into_iter()
                        .filter_map(|(t, strat, net, cost)| {
                            let tok = pm_core::instrument::TokenId(t as u64);
                            (strat == "mm" && net != 0 && mm_token_market.contains_key(&tok))
                                .then_some((tok, i128::from(net), pm_core::num::Usdc(i128::from(cost))))
                        })
                        .collect()
                })
                .unwrap_or_else(|e| {
                    warn!("MM inventory reload failed (starting flat): {e}");
                    Vec::new()
                });
        // LIVE seed reconcile: the DB seed can carry STALE lots (paper fills +
        // prior sessions whose markets RESOLVED). Seeding those makes the
        // long-only MM ask for tokens it does not hold → the CLOB rejects every
        // one ("balance: 0"). So on a live run, keep only what the deposit wallet
        // really holds + is still tradeable (clamped); a fetch failure seeds FLAT.
        // Paper runs keep the DB seed as-is.
        let mm_seed = match live_deposit_wallet {
            Some(dw) => match reconcile_seed_against_chain(&db_seed, dw, &reg).await {
                Ok(reconciled) => {
                    info!(
                        db_lots = db_seed.len(),
                        kept = reconciled.len(),
                        "MM live seed reconciled vs deposit-wallet on-chain positions (stale/resolved lots dropped)"
                    );
                    reconciled
                }
                Err(e) => {
                    warn!(
                        "MM live seed reconcile failed ({e}); seeding FLAT (bid-only is safe, a phantom ask is not)"
                    );
                    Vec::new()
                }
            },
            None => db_seed,
        };
        for (tok, net, cost) in &mm_seed {
            info!(
                token = tok.0,
                net_micro = *net as i64,
                cost_micro = cost.0,
                "MM inventory reloaded (resuming held position to manage/offload)"
            );
        }
        // R2 (auto-redeem): the resolved-winner feed — built ONLY on a relayer-backed
        // reward-farm live run (`mm_merger` present). The keyless Data-API positions
        // client polls the deposit wallet for `redeemable` (resolved) markets; the
        // redeem sweep then claims each via the SAME relayer. `None` everywhere else
        // (paper / arb / non-relayer live), so those paths thread None + an empty map.
        let mm_data_api = if mm_merger.is_some() {
            match DataApiClient::new(None) {
                Ok(c) => Some(std::sync::Arc::new(c)),
                Err(e) => {
                    warn!(error = %e, "MM redeem: Data-API client build failed — auto-redeem disabled this run");
                    None
                }
            }
        } else {
            None
        };
        // The Data API keys positions by the LOWERCASED on-chain address (matches
        // `reconcile_seed_against_chain`). `Some` only on a live run.
        let mm_deposit_wallet = live_deposit_wallet.map(|dw| dw.to_string().to_lowercase());
        let mut mm = MmStrategy::new(mm_tokens, mm_token_market, mm_params, mm_inv_cfg, mm_capital)
            .with_seed(mm_seed)
            // Spec-2 Phase B (§5.1): the yes↔no complement map for the quoted
            // markets — empty unless reward-farm hedging is on — so the quote loop
            // and estimator can pair the two complement bids (consumed by B3/B4).
            .with_complement(mm_complement)
            // M6-7: the LIVE on-chain merge relayer + the token→conditionId map so
            // a reward-farm live run RECYCLES a complete YES+NO set on-chain via a
            // periodic, non-blocking sweep. `merger` is `None` (and the map empty)
            // off relayer-enabled reward-farm live, keeping every other path
            // (paper / arb / non-relayer live) byte-for-byte unchanged. CLONE (not
            // move) the shared Arc so the copy executor (below) can share the SAME
            // relayer (same wallet/account) when it too is live.
            .with_merger(mm_merger.clone())
            .with_conditions(mm_cond_by_token)
            // R2 (auto-redeem): the resolved-winner feed — token→asset map + Data-API
            // positions client + lowercased deposit wallet. All empty/None unless a
            // relayer-backed reward-farm live run, so other paths are unchanged. The
            // sweep claims RESOLVED markets via the same relayer (non-blocking).
            .with_venue_ids(mm_venue_by_token)
            .with_data_api(mm_data_api)
            .with_deposit_wallet(mm_deposit_wallet)
            // Task 9 — PERSISTENT UTC-day loss cap: thread the store path so the
            // loop reads today's persisted "mm" P&L at startup and refuses to quote
            // when the day is already at/under the daily-loss cap, making the cap
            // bind across the periodic auto-restart (same file the seed reload
            // above reads). Inert when the DB is fresh / today's P&L is within cap.
            .with_store_path(std::path::PathBuf::from(&config.store.path))
            // Same live gate as arb: when live is HELD (TUI before `l`), the MM
            // starts PAUSED and only quotes once released — never trades real
            // money on its own before the operator confirms.
            .with_start_paused(mm_live && !released_at_start);

        if let Some(venue) = mm_live_venue {
            // Task 4.6 — choose the live FILLS source and build the matching
            // `MmLive` variant:
            //  * `"ws"` (default, NOT shadow) → the LOW-LATENCY user-WS feed:
            //    pair the live `LiveVenue` (the `MakerVenue`) with a
            //    `LiveUserWsFills` (the `UserFillSource`) in a `SplitVenue`. The
            //    WS source spawns a background reader that connects + subscribes.
            //  * `"rest"` (or ANY `--shadow` run) → the Task-4.5 `LiveVenue` REST
            //    poll on the SAME object. SHADOW deliberately skips the WS: a
            //    shadow venue places no real orders, so there are no fills to read
            //    and the REST poll short-circuits to empty with NO network — so a
            //    `--shadow` dry run NEVER opens a live socket.
            let use_ws = mm_fills_source == MmFillsSource::Ws && !args.shadow;
            let mm_live_variant = if use_ws {
                let creds = mm_ws_creds.take().unwrap_or_else(|| {
                    fatal("internal: MM cleared for live WS but creds were not stashed")
                });
                let ws = pm_execution::user_ws::LiveUserWsFills::connect(
                    mm_user_ws_url.clone(),
                    creds,
                    mm_condition_ids,
                    mm_ws_resolve,
                );
                info!(
                    url = %mm_user_ws_url,
                    markets = mm_market_count,
                    "live MM fills source: USER WS (low-latency scalping feed)"
                );
                MmLive::Ws(pm_execution::split_venue::SplitVenue::new(venue, ws))
            } else {
                if mm_fills_source == MmFillsSource::Ws && args.shadow {
                    info!(
                        "live MM fills source: REST poll (--shadow skips the user WS: no live socket)"
                    );
                } else {
                    info!("live MM fills source: REST poll (Task-4.5 offline-verified fallback)");
                }
                MmLive::Rest(venue)
            };
            // Attach the live venue — the ONLY path that puts MM on real money.
            // CANARY SAFETY (no new mechanism; all pre-existing): (a) the tiny
            // `capital_usd` slice carved by `strategy_envelopes`; (b)
            // `max_quote_usd` per order; (c) the `InventoryConfig` caps
            // (per-market / gross / stop-loss / daily) enforced by `InventoryRisk`
            // inside the loop; (d) postOnly maker orders; (e) the startup live
            // confirmation. Surface the canary LOUDLY.
            mm = mm.with_live_venue(mm_live_variant);
            warn!(
                capital_usd = config.strategies.mm.capital_usd,
                markets = mm_market_count,
                max_quote_usd = config.strategies.mm.max_quote_usd,
                max_inventory_usd = config.inventory.max_inventory_usd,
                max_gross_inventory_usd = config.inventory.max_gross_inventory_usd,
                inventory_stop_loss_usd = config.inventory.inventory_stop_loss_usd,
                daily_loss_usd = config.inventory.daily_loss_usd,
                fills_source = config.strategies.mm.live_fills_source.as_str(),
                shadow = args.shadow,
                "LIVE MM ENABLED — real maker orders (canary): safety = capital carve + \
                 inventory caps + postOnly + confirmation; SHARED account rate budget with arb"
            );
            // Advisory canary ceiling (NOT a hard cap; the hard guards are the
            // capital carve + the Σcapital ≤ bankroll allocator + the inventory
            // caps): a live slice above the canary guidance is almost certainly
            // unintended for a first live run.
            if config.strategies.mm.capital_usd > MM_LIVE_CANARY_SOFT_CAP_USD {
                warn!(
                    capital_usd = config.strategies.mm.capital_usd,
                    soft_cap_usd = MM_LIVE_CANARY_SOFT_CAP_USD,
                    "live MM capital exceeds the canary soft-cap guidance — confirm this is intended"
                );
            }
        } else if config.strategies.mm.live {
            // `mm.live` set but the PROCESS is not --live → cannot trade real
            // money (no confirmation, no live secrets); fall back to PAPER.
            warn!(
                "strategies.mm.live set but process not --live; using the PAPER maker venue \
                 (restart with --live to trade real maker orders)"
            );
        }

        host.add(Box::new(mm), mm_envelope);
        info!(
            mm_capital_usd = config.strategies.mm.capital_usd,
            mm_capital_micro = mm_capital.0,
            markets = mm_market_count,
            arb_capital_micro,
            live = mm_live,
            shadow = args.shadow,
            "market maker enabled: bankroll carved between arb and MM"
        );
    }

    // ---- copy executor (#4): DEFAULT-OFF, behind [strategies.copy] enabled --
    // Present only when the carve produced a `copy_envelope` (copy enabled). When
    // disabled this whole block is skipped — the strategy is never constructed,
    // so it is fully DORMANT (no feed, no venue, no orders), and arb/MM/paper are
    // byte-for-byte unaffected.
    //
    // LIVE GATING (same as the MM): copy trades REAL taker orders ONLY when
    // `mm_use_live(args.live, copy.live)` clears — the PROCESS is --live (which
    // forced the typed confirmation at startup) AND `[strategies.copy].live`.
    // EVERY other combination uses the PAPER taker venue.
    //
    // COVERAGE: copy trades the SYNCED universe (the whitelist's active markets)
    // AND, on LIVE runs, syncs any UNSYNCED signal's market ON-DEMAND at signal
    // time (`with_resolver` → `resolve_ondemand`), so a fresh smart-money buy is
    // never skipped just because the snapshot didn't cover it — entry latency
    // isn't bounded by the snapshot cadence. (Paper has no live book feed for
    // unsynced markets, so it stays seed-only.)
    if let Some(copy_envelope) = copy_envelope {
        let copy_capital = copy_envelope.capital;
        let copy_live = mm_use_live(args.live, config.strategies.copy.live);
        let copy_params = CopyParams::from_config(&config.strategies.copy, &config.copy_params)
            .unwrap_or_else(|e| fatal(format!("CopyParams::from_config: {e}")));

        // Signal/whitelist feed: the keyless Data API (the SAME client the MM
        // redeem path + confluence use; no key). A build failure leaves the
        // strategy an inert heartbeat (no whitelist, no orders) rather than
        // aborting startup.
        let copy_feed = match DataApiClient::new(None) {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                warn!(error = %e, "copy: Data-API client build failed — copy strategy inert this run");
                None
            }
        };

        // Tradeable map: venue token-id (the trade's `asset`) → token/tick/condition
        // for every SYNCED market's BOTH outcome tokens. Keyed by asset (not
        // outcome_index) so a copy buys the EXACT token the trader bought. Markets
        // without a parseable condition id are skipped (see `build_copy_tradeable`).
        let copy_tradeable = build_copy_tradeable(&reg);
        let copy_tradeable_markets = copy_tradeable.len();

        // Venue: a LIVE taker venue for THIS account when cleared (reusing the
        // SAME creds/signer/deposit-wallet arb resolved — NO second API key), else
        // the PAPER taker venue over the shared book fetcher.
        let copy_venue: Option<AppCopyVenue> = if copy_live {
            let inputs = copy_live_inputs.take().unwrap_or_else(|| {
                fatal("internal: copy cleared for live but live inputs were not stashed")
            });
            let mut v = pm_execution::live::LiveVenue::new(pm_execution::live::LiveVenueCfg {
                base: inputs.base,
                creds: inputs.creds,
                signer: inputs.signer,
                proxy: inputs.proxy,
                deposit_wallet: inputs.deposit_wallet,
                auth_address: inputs.auth_address,
                fill_window: Duration::from_millis(config.execution.fill_window_ms),
                rate_per_sec: config.ingestion.rest_rate_per_sec,
                rate_capacity: config.ingestion.rest_rate_capacity,
                shadow: args.shadow,
            })
            .unwrap_or_else(|e| fatal(format!("copy LiveVenue: {e}")));
            // Register every synced-universe token so a taker FAK for any tradeable
            // token is accepted (an unregistered token is rejected before any I/O).
            // SHARED-ACCOUNT RATE BUDGET: this is a THIRD LiveVenue (after arb +
            // MM), each with its OWN REST limiter against the one account budget —
            // acceptable for a tiny canary; keep `ingestion.rest_rate_*`
            // conservative (a shared limiter is future work).
            for m in reg.markets() {
                for tok in [m.yes, m.no] {
                    if let Some(vid) = reg.token_venue_id(tok) {
                        v.register_token(tok, vid.to_owned(), m.neg_risk, m.tick);
                    }
                }
            }
            Some(AppCopyVenue::Live(v))
        } else {
            Some(AppCopyVenue::Paper(PaperCopyVenue::new(
                fetcher.clone(),
                Duration::from_millis(config.execution.paper_latency_ms),
                params.gas,
            )))
        };

        // Relayer: SHARE the one `mm_merger` Arc if it was built (same wallet /
        // account); else `None` → copy holds resolved winners for the next
        // session's reconcile (the documented no-relayer behavior).
        let copy_relayer = mm_merger.clone();
        let copy_has_relayer = copy_relayer.is_some();

        // Cumulative-loss circuit breaker (mirrors the MM): the REAL `[inventory]`
        // caps so `inv.halted()` (inventory stop-loss / daily-loss) binds, and the
        // store path so the PERSISTENT `"copy"` day-loss cap arms at startup and
        // binds across the periodic auto-restart.
        let copy_inv_cfg = inventory_config(&config)
            .unwrap_or_else(|e| fatal(format!("inventory_config (copy): {e}")));
        // ON-DEMAND market sync (LIVE only): a CLOB metadata client so a fresh
        // signal in a market the universe snapshot never covered is resolved +
        // registered live and TRADED AT ONCE, instead of waiting for the next
        // snapshot (entry latency is a copy-edge contributor). Paper has no live
        // book feed for unsynced markets, so it stays None (on-demand off). A
        // separate client = its own rate budget (this account already runs the
        // sync + venue clients); init failure just disables on-demand, not the run.
        let copy_resolver: Option<Arc<ClobRest>> = if copy_live {
            match ClobRest::new(
                &config.endpoints.clob_base,
                config.ingestion.rest_rate_capacity,
                config.ingestion.rest_rate_per_sec,
            ) {
                Ok(c) => Some(Arc::new(c)),
                Err(e) => {
                    warn!(
                        "copy: on-demand resolver init failed ({e}); unsynced signals will be skipped until the next snapshot"
                    );
                    None
                }
            }
        } else {
            None
        };
        let copy = CopyStrategy::new(copy_params)
            .with_feed(copy_feed)
            .with_venue(copy_venue)
            .with_relayer(copy_relayer)
            .with_tradeable(copy_tradeable)
            // The REAL `[inventory]` floors the inventory halt keys off + the
            // store path for the persistent `"copy"` day-loss cap (same file the
            // MM threads; the cap binds across the auto-restart).
            .with_inventory_config(copy_inv_cfg)
            .with_store_path(std::path::PathBuf::from(&config.store.path))
            // Seed the whitelist snapshot main already built for the
            // whitelist-driven universe, so the strategy trades the SAME traders
            // the universe was synced for and the heavy EdgePerBet rank isn't run
            // twice at startup (the loop defers its first refresh when seeded).
            // `None` ⇒ the loop builds its own whitelist immediately (fallback).
            .with_initial_whitelist(copy_initial_whitelist)
            // ON-DEMAND market sync (live only): unsynced fresh signals are
            // resolved + traded immediately instead of skipped (None on paper).
            .with_resolver(copy_resolver)
            // Same live gate as arb/MM: when live is HELD (TUI before `l`), copy
            // starts PAUSED and only trades once the operator releases.
            .with_start_paused(copy_live && !released_at_start);

        host.add(Box::new(copy), copy_envelope);

        if copy_live {
            // Surface the DIRECTIONAL canary LOUDLY. Unlike the (delta-neutral-ish)
            // MM, a copied buy is an outright directional bet — a wrong copy can
            // lose the WHOLE position. The CONTROLS are the capital carve + the
            // per-position / gross / concurrency caps + the stop-loss + the drift
            // (freshness) gate + the startup confirmation.
            warn!(
                capital_usd = config.strategies.copy.capital_usd,
                per_position_usd = config.copy_params.per_position_usd,
                max_concurrent_positions = config.copy_params.max_concurrent_positions,
                max_gross_usd = config.copy_params.max_gross_usd,
                stop_loss_pct = config.copy_params.stop_loss_pct,
                max_drift = config.copy_params.max_drift,
                tradeable_markets = copy_tradeable_markets,
                relayer = copy_has_relayer,
                shadow = args.shadow,
                "LIVE COPY ENABLED — DIRECTIONAL real taker orders (canary): a wrong copy can lose \
                 the whole position; controls = capital carve + per-position/gross/concurrency caps \
                 + stop-loss + drift gate + confirmation; SHARED account rate budget with arb"
            );
        } else if config.strategies.copy.live {
            // `copy.live` set but the PROCESS is not --live → cannot trade real
            // money (no confirmation, no live secrets); fall back to PAPER.
            warn!(
                "strategies.copy.live set but process not --live; using the PAPER taker venue \
                 (restart with --live to trade real copy orders)"
            );
        }
        info!(
            copy_capital_usd = config.strategies.copy.capital_usd,
            copy_capital_micro = copy_capital.0,
            tradeable_markets = copy_tradeable_markets,
            live = copy_live,
            relayer = copy_has_relayer,
            shadow = args.shadow,
            "copy executor enabled: bankroll carved for the smart-money copy strategy"
        );
    }

    // ── btc5m: BTC 5-minute up/down strategy (Task 7) ───────────────────────
    // Present ONLY when the carve produced a `btc5m_envelope` (i.e.
    // `[strategies.btc5m] enabled`, DEFAULT OFF). When disabled this whole
    // block is skipped: the strategy is never constructed, no spot feed / gamma
    // client / CLOB book poll is spawned, and arb/MM/copy are byte-for-byte
    // unaffected (the shipped, default behavior).
    if let Some(btc5m_envelope) = btc5m_envelope {
        let btc5m_capital = btc5m_envelope.capital;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent("pm-arb-bot/1.0")
            .build()
            .unwrap_or_else(|e| fatal(format!("btc5m http client: {e}")));
        // Composite BTC spot feed (median of exchanges) + 1-min vol. Stops when
        // the process-wide `kill` flag flips (the SAME Arc the host + kill watch
        // share), so it tears down with the rest of the process.
        let spot_feed = pm_ingestion::spot::spawn(
            http.clone(),
            config.btc5m_params.spot_sources.clone(),
            config.btc5m_params.spot_poll_ms,
            config.btc5m_params.vol_half_life_min,
            config.btc5m_params.vol_warmup_samples,
            Arc::clone(&kill),
        );
        let gamma = pm_ingestion::gamma::GammaClient::new(http.clone(), None);
        // Real CLOB REST base from config (the SAME base arb/copy resolve
        // against); the strategy polls /book for the rotating 5m window token.
        let clob_base = config.endpoints.clob_base.clone();
        // Rotating 5-minute window slug: `btc-updown-5m-<window-open-unix-secs>`
        // (BEST-GUESS format, to be verified at deploy time — see Task 7 notes).
        let slug_fn: Box<dyn Fn(i64) -> String + Send> = Box::new(|now_ms: i64| {
            let boundary = (now_ms / 1000) / 300 * 300;
            format!("btc-updown-5m-{boundary}")
        });
        let btc5m = pm_app::strategy::btc5m::Btc5mStrategy::new(
            gamma,
            slug_fn,
            spot_feed,
            config.btc5m_params.sample_interval_ms,
            http,
            clob_base,
        );
        host.add(Box::new(btc5m), btc5m_envelope);
        info!(
            btc5m_capital_usd = config.strategies.btc5m.capital_usd,
            btc5m_capital_micro = btc5m_capital.0,
            sample_interval_ms = config.btc5m_params.sample_interval_ms,
            spot_poll_ms = config.btc5m_params.spot_poll_ms,
            "btc5m strategy enabled: bankroll carved for the BTC 5-minute up/down strategy"
        );
    }

    // ---- spawn supervisors with the host's combined inline hook ------------
    // host.make_on_apply rebuilds the combined hook per call, constructing FRESH
    // per-supervisor detector state each time — exactly as main.rs built one
    // Detector per supervisor before. Must run before host.run (which consumes
    // the host).
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for mut sup in supervisors {
        if let Some(hook) = host.make_on_apply() {
            sup.set_on_apply(hook);
        }
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
    info!(supervisors = supervisors_started, "supervisors spawned");

    // ---- run the host: spawns arb + heartbeat as fault-isolated tasks ------
    // The capital allocator runs first (over-allocation is a fatal startup error,
    // before any task spawns). Strategies get the shared ingestion/store/kill
    // handles via HostShared; arb's coordinator reads books through `fetcher`.
    let running = host
        .run(HostShared {
            registry: Arc::clone(&reg),
            fetcher,
            store_tx: store_tx.clone(),
            kill: Arc::clone(&kill),
        })
        .unwrap_or_else(|e| fatal(format!("StrategyHost::run: {e}")));

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
            // Loop control: arb's CoordStatus watch closes when the arb
            // coordinator finishes (its status bridge drops the sender) — the
            // same publisher-exit signal the coordinator's status watch gave
            // before. Its value drives nothing on the wired path (badges below).
            status_rx: arb_status_rx.clone(),
            // Task 1.8: the host's aggregated per-strategy view drives header
            // money + the per-strategy breakdown; the header badges are reconciled
            // from that view plus the global kill flag and arb's CoordStatus
            // (live_released/busy — the gates the aggregate drops).
            strategy_status_rx: Some(running.status()),
            kill: Arc::clone(&kill),
            arb_status_rx: Some(arb_status_rx.clone()),
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

        println!("arb: startup complete — launching dashboard ...");
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
    // Periodic auto-restart: re-exec for a fresh universe every N secs (0 = off).
    let auto_restart_secs = config.universe.auto_restart_secs;
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
                if auto_restart_secs > 0 && start.elapsed().as_secs() >= auto_restart_secs {
                    // Periodic re-exec for a fresh universe (handled after the
                    // graceful shutdown cascade below).
                    trigger = "resync";
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
                    // Pause dispatch applies to EVERY trading strategy: arb's
                    // coordinator AND the MM's quote loop (pausing it cancels its
                    // resting quotes). The header `paused` badge ("any strategy
                    // paused") still lights. `pause` on an absent strategy is a
                    // harmless no-op.
                    let _ = running.pause(StrategyId("arb"), p).await;
                    let _ = running.pause(StrategyId("mm"), p).await;
                    // The copy executor is also a real-money trading strategy:
                    // pausing it stops its entry/exit poll cycle (no orders).
                    let _ = running.pause(StrategyId("copy"), p).await;
                }
                pm_tui::state::TuiCommand::Kill => kill.store(true, Ordering::Release),
                pm_tui::state::TuiCommand::GoLive => {
                    if args.live {
                        // Release BOTH live paths: arb's coordinator latch AND the
                        // MM's held quote loop (it starts PAUSED under a held live
                        // latch — see `with_start_paused`). Until this fires, the MM
                        // never places a real order.
                        let _ = live_release_sender.send(CtlCommand::ReleaseLive).await;
                        let _ = running.pause(StrategyId("mm"), false).await;
                        // Release the copy executor too: it starts PAUSED under a
                        // held live latch (see `with_start_paused`), so until this
                        // fires it never places a real copy order.
                        let _ = running.pause(StrategyId("copy"), false).await;
                        // Remember the release so a periodic auto-restart resumes
                        // live (carries PM_RESUME_LIVE) instead of re-holding.
                        live_released = true;
                    } else {
                        warn!("live not armed — restart with --live to trade real money");
                    }
                }
                pm_tui::state::TuiCommand::SetVeto { key, veto } => {
                    // Decode the publisher's opaque "<token_u64>:<b|a>" handle and
                    // route the veto/un-veto to the MM's control channel — only the
                    // MM holds a resting maker book, so the cancel targets it.
                    if let Some((tok_s, side_s)) = key.split_once(':')
                        && let Ok(tok) = tok_s.parse::<u64>()
                    {
                        let side = if side_s == "a" {
                            pm_core::book::Side::Ask
                        } else {
                            pm_core::book::Side::Bid
                        };
                        if let Some(tx) = running.control_sender(StrategyId("mm")) {
                            let _ = tx
                                .send(pm_app::strategy::StrategyCommand::VetoQuote {
                                    token: pm_core::instrument::TokenId(tok),
                                    side,
                                    veto,
                                })
                                .await;
                        }
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
    // Supervisors dropped → detectors dropped → arb's opp/lp senders close → the
    // LP pool + coordinator + execution task inside `ArbStrategy::run` drain — the
    // exact cascade main.rs drove by hand before, now owned by arb. RunningHost::
    // join awaits arb AND the heartbeat; it drops the per-strategy control senders
    // first, so the heartbeat exits on its closed control channel even when the
    // global kill flag was never set (a duration / quit / ctrl-c shutdown).
    // join() returns whether any strategy TASK ended abnormally (panic/cancel —
    // e.g. a future MM strategy). Arb's coordinator death is separate: arb's run
    // swallows the coordinator JoinError and returns Ok, so it is reported via the
    // coordinator_aborted flag instead. Fold BOTH into the health signal.
    let host_any_abnormal = running.join().await;
    let strategy_died = host_any_abnormal || arb_coordinator_aborted.load(Ordering::Acquire);
    // Session summary (display / happy path): arb's coordinator publishes a final
    // CoordStatus on clean shutdown (cash / equity / open_positions — the numbers
    // the discarded CoordinatorSummary carried); reconstruct it from the
    // arb-process status watch (retained after the bridge drops its sender).
    let final_arb = arb_status_rx.borrow().clone();
    let coord_summary = Some(CoordinatorSummary {
        cash: pm_core::num::Usdc(final_arb.cash_micro as i128),
        equity: pm_core::num::Usdc(final_arb.equity_micro as i128),
        open_positions: final_arb.open_positions,
    });
    kill_handle.abort();

    // Drop main's writer sender LAST so all StoreMsg producers are gone.
    drop(store_tx);
    let store = match writer.await {
        Ok(s) => s,
        Err(e) => fatal(format!("writer task join: {e}")),
    };

    // Await the TUI task LAST so its terminal teardown is ordered before the
    // final report hits the (now normal) screen. The host was joined above; arb's
    // coordinator finishing drops the arb-process status sender → the publisher's
    // watch closes → the publisher exits → its state_tx drops → run_tui's state_rx
    // closes → run_tui returns → the task runs disable_raw_mode +
    // LeaveAlternateScreen. So a session ending for NON-TUI reasons
    // (duration/kill/sentinel/ctrl_c) still tears the screen down here.
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

    // Periodic auto-restart (universe.auto_restart_secs): when this session ended
    // because the resync timer fired, re-exec the SAME argv to pick up a fresh
    // (and possibly larger) universe. Everything is already torn down cleanly
    // above — store flushed, host joined, terminal restored — so the new process
    // starts clean and reconciles open positions from the SQLite store. `exec`
    // replaces this process image and only RETURNS on failure. A kill / quit /
    // duration end uses a different `trigger`, so it falls through to a normal
    // exit (no restart).
    if trigger == "resync" {
        use std::os::unix::process::CommandExt;
        println!("resync: re-launching for a fresh universe (positions persist via the store) ...");
        match std::env::current_exe() {
            Ok(exe) => {
                let argv: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
                let mut cmd = std::process::Command::new(exe);
                cmd.args(&argv);
                // Carry the live-release state across the re-exec: a session that
                // was RELEASED resumes live (the fresh process reads PM_RESUME_LIVE
                // and skips the hold/confirm); a session still HELD re-execs WITHOUT
                // it and re-holds. Only ever set for an actually-released live run.
                if args.live && live_released {
                    cmd.env("PM_RESUME_LIVE", "1");
                }
                // exec() returns only if it FAILED; otherwise the image is replaced.
                let err = cmd.exec();
                fatal(format!("resync re-exec failed: {err}"));
            }
            Err(e) => fatal(format!("resync: cannot resolve current exe: {e}")),
        }
    }

    // A dead strategy (host task panic/cancel, or arb's coordinator aborting) is
    // unhealthy — restores the pre-Task-1.8 "exit 2 on coordinator death" signal
    // that the host's fault isolation would otherwise mask.
    let healthy =
        write_errors == 0 && !restart_storm && supervisors_started > 0 && !strategy_died;
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

    /// The live seed reconcile keeps only tokens the wallet still holds AND can
    /// trade, clamped to the real size (cost scaled), and drops everything else.
    /// The empty-positions case is the user's: every stale lot drops → seed FLAT.
    #[test]
    fn reconcile_seed_keeps_only_real_tradeable_holdings() {
        use pm_core::num::{TickSize, Usdc};
        let mut b = pm_registry::RegistryBuilder::default();
        for (c, y) in [
            ("0xc1", "tok_keep"),
            ("0xc2", "tok_redeem"),
            ("0xc3", "tok_absent"),
            ("0xc4", "tok_clamp"),
            ("0xc5", "tok_short"),
        ] {
            b.add_market(
                c,
                y,
                &format!("{y}_no"),
                TickSize::Cent,
                0,
                false,
                None,
                true,
                false,
                None,
            );
        }
        let reg = b.finish("").unwrap();
        let tid = |v: &str| reg.venue_token_id(v).unwrap();
        let pos = |asset: &str, size: f64, redeemable: bool| pm_ingestion::data_api::Position {
            condition_id: String::new(),
            asset: asset.to_string(),
            size,
            outcome: String::new(),
            outcome_index: 0,
            cur_price: 0.0,
            avg_price: 0.0,
            cash_pnl: 0.0,
            redeemable,
            neg_risk: false,
        };

        let seed = vec![
            (tid("tok_keep"), 10_000_000i128, Usdc(5_000_000)), // exact hold → kept as-is
            (tid("tok_redeem"), 10_000_000, Usdc(5_000_000)),   // resolved → drop
            (tid("tok_absent"), 10_000_000, Usdc(5_000_000)),   // not on-chain → drop
            (tid("tok_clamp"), 20_000_000, Usdc(8_000_000)),    // seed 20 > real 12 → clamp
            (tid("tok_short"), -10_000_000, Usdc(0)),           // short → drop
        ];
        let positions = vec![
            pos("tok_keep", 10.0, false),
            pos("tok_redeem", 10.0, true), // redeemable ⇒ not tradeable
            pos("tok_clamp", 12.0, false), // real 12 < seed 20
            pos("tok_short", 10.0, false), // held, but the seed lot is a short
        ];

        let out = reconcile_seed_with_positions(&seed, &positions, &reg);
        let m: std::collections::HashMap<_, _> =
            out.iter().map(|(t, n, c)| (*t, (*n, c.0))).collect();
        assert_eq!(out.len(), 2, "only the two real, tradeable longs survive");
        // Exact hold: net + cost unchanged.
        assert_eq!(m.get(&tid("tok_keep")), Some(&(10_000_000i128, 5_000_000i128)));
        // Clamp: net→12M; cost scales 8M * 12/20 = 4.8M.
        assert_eq!(m.get(&tid("tok_clamp")), Some(&(12_000_000i128, 4_800_000i128)));
        assert!(!m.contains_key(&tid("tok_redeem")));
        assert!(!m.contains_key(&tid("tok_absent")));
        assert!(!m.contains_key(&tid("tok_short")));

        // The user's account: no live holdings match → every lot drops → seed FLAT.
        assert!(
            reconcile_seed_with_positions(&seed, &[], &reg).is_empty(),
            "no on-chain holdings ⇒ bid-only flat seed"
        );
    }
}
