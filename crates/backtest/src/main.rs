//! `backtest` — BT-4 harness. It builds the keyless Data API client, runs
//! [`pm_backtest::fetch_all`] (cached, offline-replayable), sweeps the full
//! parameter grid with [`pm_backtest::run_grid`], writes the machine-readable
//! `bt-results.json`, and prints a human SUMMARY table + a GO/NO-GO VERDICT.
//!
//! This is an OFFLINE analysis tool. It is read-only and places **NO orders**.
//!
//! Usage: `backtest [--n <traders>] [--refresh] [--cache-dir <path>]`
//! - `--n <traders>`   leaderboard depth per slice (default 30)
//! - `--refresh`       bypass the cache and re-fetch every request
//! - `--cache-dir <p>` cache directory (default `./bt-cache`)
//!
//! The single run produces `./bt-results.json` next to the working directory.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use pm_backtest::core::signals;
use pm_backtest::{
    DEFAULT_CACHE_DIR, FetchParams, GridConfig, GridResult, candidate_markets, fetch_all, run_grid,
};
use pm_ingestion::data_api::DataApiClient;

/// Where the machine-readable grid results are written (CWD-relative).
const RESULTS_FILE: &str = "bt-results.json";

/// Minimum filled sample for a cell to qualify for the GO verdict.
const MIN_VERDICT_SAMPLE: usize = 50;

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

    // Sweep the full grid (pure, deterministic).
    let cfg = GridConfig::default();
    let results = run_grid(&data, &cfg);

    // Universe descriptors for the report header.
    let candidates = candidate_markets(&data.trades_by_wallet, &data.resolutions);
    let universe_wallets: Vec<String> =
        data.traders.iter().map(|t| t.proxy_wallet.clone()).collect();
    // k=1 over the FULL universe = total raw (market,outcome) follow signals
    // before any ranking filter — the top of the simulation funnel.
    let total_signals_k1 = signals(&universe_wallets, &data.trades_by_wallet, 1, cfg.window_secs).len();

    let report = serde_json::json!({
        "generated_at": now_rfc3339(),
        "params": {
            "n_traders": params.n_traders,
            "trade_limit": params.trade_limit,
            "tape_limit": params.tape_limit,
            "refresh": params.refresh,
            "cache_dir": args.cache_dir.display().to_string(),
            "top_n": cfg.top_n,
            "min_bets": cfg.min_bets,
            "window_secs": cfg.window_secs,
            "fee_frac": cfg.fee_frac,
            "rankings": cfg.rankings.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
            "ks": cfg.ks.clone(),
            "lags_min": cfg.lags_min.clone(),
            "exits": cfg.exits.iter().map(|e| e.as_str()).collect::<Vec<_>>(),
        },
        "universe": {
            "traders": data.traders.len(),
            "resolved_markets": data.resolutions.len(),
            "candidate_markets": candidates.len(),
            "total_signals_k1": total_signals_k1,
        },
        "results": &results,
    });

    let body = match serde_json::to_string_pretty(&report) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("backtest: failed to serialize results: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = std::fs::write(RESULTS_FILE, body) {
        eprintln!("backtest: failed to write {RESULTS_FILE}: {e}");
        return ExitCode::FAILURE;
    }

    println!(
        "backtest: universe traders={}, resolved_markets={}, candidate_markets={}, tapes={}, total_signals_k1={}",
        data.traders.len(),
        data.resolutions.len(),
        candidates.len(),
        data.tape_by_market.len(),
        total_signals_k1,
    );
    println!(
        "backtest: grid = {} cells × 3 scopes = {} rows; wrote {RESULTS_FILE} and {}/fetched.json",
        results.len() / 3,
        results.len(),
        args.cache_dir.display(),
    );

    print_summary(&results);

    ExitCode::SUCCESS
}

/// Print the stdout SUMMARY: the `all`-scope cells sorted by Sharpe (desc), a
/// one-line GO/NO-GO VERDICT, and the best sports vs non-sports cells. This
/// stdout is what the operator/parent reads to judge GO/NO-GO.
fn print_summary(results: &[GridResult]) {
    let mut all_rows: Vec<&GridResult> = results.iter().filter(|r| r.scope == "all").collect();
    sort_by_sharpe_desc(&mut all_rows);

    println!();
    println!("SUMMARY — all-scope cells, sorted by Sharpe (desc). Returns are per-trade fractions.");
    println!(
        "{:<14} {:>2} {:>4} {:<11} {:>5} {:>5} {:>9} {:>6} {:>9} {:>8} {:>8}",
        "RANKING", "K", "LAG", "EXIT", "N", "SKIP", "MEANRET%", "HIT%", "TOTRET", "SHARPE", "MAXDD",
    );
    for r in &all_rows {
        let m = &r.metrics;
        println!(
            "{:<14} {:>2} {:>3}m {:<11} {:>5} {:>5} {:>9.2} {:>6.1} {:>9.3} {:>8.3} {:>8.3}",
            r.ranking,
            r.k,
            r.lag_min,
            r.exit,
            m.n,
            m.skipped,
            m.mean_ret * 100.0,
            m.hit_rate * 100.0,
            m.total_ret,
            m.sharpe,
            m.max_drawdown,
        );
    }

    // VERDICT: best all-scope cell by Sharpe with an adequate sample AND a
    // positive mean edge; otherwise an explicit no-edge verdict.
    println!();
    match all_rows.iter().find(|r| r.metrics.n >= MIN_VERDICT_SAMPLE) {
        Some(r) if r.metrics.mean_ret > 0.0 => {
            println!("VERDICT: GO — {}", describe(r));
        }
        _ => {
            println!(
                "VERDICT: NO-GO — no positive edge with adequate sample (n>={MIN_VERDICT_SAMPLE})"
            );
        }
    }

    // Best sports vs non-sports cells (any positive sample), for the split read.
    println!();
    match best_scope_row(results, "sports") {
        Some(r) => println!("BEST sports:    {}", describe(r)),
        None => println!("BEST sports:    none (no filled sports trades)"),
    }
    match best_scope_row(results, "nonsports") {
        Some(r) => println!("BEST nonsports: {}", describe(r)),
        None => println!("BEST nonsports: none (no filled nonsports trades)"),
    }
}

/// The best (highest-Sharpe) row of a given scope with at least one fill.
fn best_scope_row<'a>(results: &'a [GridResult], scope: &str) -> Option<&'a GridResult> {
    let mut rows: Vec<&GridResult> = results
        .iter()
        .filter(|r| r.scope == scope && r.metrics.n > 0)
        .collect();
    sort_by_sharpe_desc(&mut rows);
    rows.into_iter().next()
}

/// Sort rows by Sharpe (desc), with fully deterministic tie-breaks so the table
/// and verdict are stable across runs.
fn sort_by_sharpe_desc(rows: &mut [&GridResult]) {
    rows.sort_by(|a, b| {
        b.metrics
            .sharpe
            .total_cmp(&a.metrics.sharpe)
            .then(b.metrics.total_ret.total_cmp(&a.metrics.total_ret))
            .then(b.metrics.n.cmp(&a.metrics.n))
            .then(a.ranking.cmp(&b.ranking))
            .then(a.k.cmp(&b.k))
            .then(a.lag_min.cmp(&b.lag_min))
            .then(a.exit.cmp(&b.exit))
    });
}

/// One-line description of a grid cell + scope, for the verdict / split lines.
fn describe(r: &GridResult) -> String {
    let m = &r.metrics;
    format!(
        "{} k={} lag={}m {} [{}] | n={} skipped={} mean_ret={:.2}% hit={:.1}% total_ret={:.3} sharpe={:.3} maxDD={:.3}",
        r.ranking,
        r.k,
        r.lag_min,
        r.exit,
        r.scope,
        m.n,
        m.skipped,
        m.mean_ret * 100.0,
        m.hit_rate * 100.0,
        m.total_ret,
        m.sharpe,
        m.max_drawdown,
    )
}

/// Current UTC time as an RFC3339 second-precision string (`…Z`). Dependency-free
/// (no `chrono`): epoch seconds → civil date via Howard Hinnant's algorithm.
fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    format_rfc3339_utc(secs)
}

/// Format Unix-epoch `secs` as `YYYY-MM-DDTHH:MM:SSZ` (UTC).
fn format_rfc3339_utc(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = sod / 3600;
    let minute = (sod % 3600) / 60;
    let second = sod % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Days-since-epoch (1970-01-01) → `(year, month, day)` in the proleptic
/// Gregorian calendar (Howard Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 { shifted } else { shifted - 146_096 } / 146_097;
    let doe = shifted - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if month <= 2 { year + 1 } else { year }, month as u32, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_matches_known_epochs() {
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339_utc(1_000_000_000), "2001-09-09T01:46:40Z");
        assert_eq!(format_rfc3339_utc(1_700_000_000), "2023-11-14T22:13:20Z");
    }
}
