//! `backtest` — BT-2 harness that proves the fetch+cache pipeline works end to
//! end. It builds the keyless Data API client, runs [`pm_backtest::fetch_all`]
//! with defaults, prints a one-line summary, and exits. BT-4 adds the actual
//! simulation; this binary just assembles + caches the inputs.
//!
//! This is an OFFLINE analysis tool. It is read-only and places **NO orders**.
//!
//! Usage: `backtest [--n <traders>] [--refresh] [--cache-dir <path>]`
//! - `--n <traders>`   leaderboard depth per slice (default 30)
//! - `--refresh`       bypass the cache and re-fetch every request
//! - `--cache-dir <p>` cache directory (default `./bt-cache`)

use std::path::PathBuf;
use std::process::ExitCode;

use pm_backtest::{DEFAULT_CACHE_DIR, FetchParams, candidate_markets, fetch_all};
use pm_ingestion::data_api::DataApiClient;

/// Parsed CLI flags (hand-rolled, mirroring `app/src/main.rs`).
struct Args {
    n_traders: Option<usize>,
    refresh: bool,
    cache_dir: PathBuf,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut n_traders: Option<usize> = None;
        let mut refresh = false;
        let mut cache_dir = PathBuf::from(DEFAULT_CACHE_DIR);

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--n" => {
                    let v = args.next().ok_or_else(|| "--n requires a value".to_string())?;
                    n_traders = Some(v.parse::<usize>().map_err(|e| format!("--n: {e}"))?);
                }
                "--refresh" => refresh = true,
                "--cache-dir" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--cache-dir requires a value".to_string())?;
                    cache_dir = PathBuf::from(v);
                }
                "-h" | "--help" => return Err("help".to_string()),
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        Ok(Args {
            n_traders,
            refresh,
            cache_dir,
        })
    }
}

const USAGE: &str = "usage: backtest [--n <traders>] [--refresh] [--cache-dir <path>]";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => {
            if e != "help" {
                eprintln!("backtest: {e}");
            }
            eprintln!("{USAGE}");
            return if e == "help" {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            };
        }
    };

    let mut params = FetchParams::default();
    if let Some(n) = args.n_traders {
        params.n_traders = n;
    }
    params.refresh = args.refresh;

    let client = match DataApiClient::new(None) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("backtest: failed to build Data API client: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "backtest: assembling universe (n={}, cache_dir={}, refresh={}) — OFFLINE, no trading",
        params.n_traders,
        args.cache_dir.display(),
        params.refresh
    );

    let data = match fetch_all(&client, &params, &args.cache_dir).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("backtest: fetch failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Recompute candidates from the assembled bundle as a sanity check: it must
    // equal the number of tapes we fetched.
    let candidates = candidate_markets(&data.trades_by_wallet, &data.resolutions);
    let trader_trades: usize = data.trades_by_wallet.values().map(Vec::len).sum();
    let tape_trades: usize = data.tape_by_market.values().map(Vec::len).sum();

    println!(
        "traders={}, markets_resolved={}, candidate_markets={}, tapes={}, \
         total_trades={} (trader_trades={}, tape_trades={})",
        data.traders.len(),
        data.resolutions.len(),
        candidates.len(),
        data.tape_by_market.len(),
        trader_trades + tape_trades,
        trader_trades,
        tape_trades
    );
    println!("wrote {}/fetched.json", args.cache_dir.display());

    ExitCode::SUCCESS
}
