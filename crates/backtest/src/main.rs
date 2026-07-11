//! `backtest` — BT-4 / FIX-B harness. It builds the keyless Data API client, runs
//! [`pm_backtest::fetch_all`] (cached, offline-replayable), sweeps the full
//! parameter grid with [`pm_backtest::run_grid`] under an OUT-OF-SAMPLE split,
//! writes the machine-readable `bt-results.json`, and prints a human SUMMARY
//! table + a GO/NO-GO VERDICT.
//!
//! ## The trust fix (FIX-B)
//! Traders are SELECTED on their PRE-cutoff record (built from `/trades` BUYS
//! scored against the independent Gamma resolutions) and the copy strategy is
//! TESTED only on their POST-cutoff trades — disjoint sets, so "copy the
//! leaderboard" can no longer be graded on the same history that picked it.
//!
//! This is an OFFLINE analysis tool. It is read-only and places **NO orders**.
//!
//! Usage: `backtest [--n <traders>] [--refresh] [--cache-dir <path>] [--gamma-base <url>] [--cutoff-days <N>] [--min-bets <N>]`
//! - `--n <traders>`: leaderboard depth per slice (default 30)
//! - `--refresh`: bypass the cache and re-fetch every request
//! - `--cache-dir <p>`: cache directory (default `./bt-cache`)
//! - `--gamma-base <u>`: Gamma API base for resolutions (default the public host)
//! - `--cutoff-days <N>`: OUT-OF-SAMPLE split — select on trades older than N days, test on the last N days (default 90)
//! - `--min-bets <N>`: minimum PRE-cutoff resolved-bet sample for the skill rankings (default 10)
//!
//! The single run produces `./bt-results.json` next to the working directory.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use pm_backtest::core::{rank_wallets_oos, signals_after, trader_records};
use pm_backtest::{
    DEFAULT_CACHE_DIR, FetchParams, GridConfig, GridResult, bought_condition_ids,
    candidate_markets, fetch_all, run_grid,
};
use pm_ingestion::data_api::DataApiClient;

/// Where the machine-readable grid results are written (CWD-relative).
const RESULTS_FILE: &str = "bt-results.json";

/// Minimum filled sample for a cell to qualify for the GO verdict.
const MIN_VERDICT_SAMPLE: usize = 50;
/// Minimum filled sample for a cell to appear in the stdout summary table.
const MIN_DISPLAY_SAMPLE: usize = 30;
/// Seconds in a day (cutoff arithmetic).
const SECS_PER_DAY: i64 = 86_400;
/// Default OUT-OF-SAMPLE split horizon, in days.
const DEFAULT_CUTOFF_DAYS: i64 = 90;

/// Parsed CLI flags (hand-rolled, mirroring `app/src/main.rs`).
struct Args {
    n_traders: Option<usize>,
    refresh: bool,
    cache_dir: PathBuf,
    gamma_base: Option<String>,
    cutoff_days: i64,
    min_bets: Option<usize>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut n_traders: Option<usize> = None;
        let mut refresh = false;
        let mut cache_dir = PathBuf::from(DEFAULT_CACHE_DIR);
        let mut gamma_base: Option<String> = None;
        let mut cutoff_days: i64 = DEFAULT_CUTOFF_DAYS;
        let mut min_bets: Option<usize> = None;

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
                "--gamma-base" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--gamma-base requires a value".to_string())?;
                    gamma_base = Some(v);
                }
                "--cutoff-days" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--cutoff-days requires a value".to_string())?;
                    let d = v.parse::<i64>().map_err(|e| format!("--cutoff-days: {e}"))?;
                    if d < 0 {
                        return Err("--cutoff-days must be >= 0".to_string());
                    }
                    cutoff_days = d;
                }
                "--min-bets" => {
                    let v = args
                        .next()
                        .ok_or_else(|| "--min-bets requires a value".to_string())?;
                    min_bets = Some(v.parse::<usize>().map_err(|e| format!("--min-bets: {e}"))?);
                }
                "-h" | "--help" => return Err("help".to_string()),
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        Ok(Args {
            n_traders,
            refresh,
            cache_dir,
            gamma_base,
            cutoff_days,
            min_bets,
        })
    }
}

const USAGE: &str = "usage: backtest [--n <traders>] [--refresh] [--cache-dir <path>] \
[--gamma-base <url>] [--cutoff-days <N>] [--min-bets <N>]";

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
    if let Some(ref g) = args.gamma_base {
        params.gamma_base = g.clone();
    }

    // OUT-OF-SAMPLE split point: now − cutoff_days. BUYS strictly before it select
    // traders; BUYS at/after it are the copy-test signals.
    let cutoff_ts = now_epoch_secs() - args.cutoff_days * SECS_PER_DAY;

    let client = match DataApiClient::new(None) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("backtest: failed to build Data API client: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "backtest: assembling universe (n={}, cache_dir={}, refresh={}, gamma_base={}, cutoff_days={} -> cutoff_ts={} [{}]) — OFFLINE, no trading",
        params.n_traders,
        args.cache_dir.display(),
        params.refresh,
        params.gamma_base,
        args.cutoff_days,
        cutoff_ts,
        format_rfc3339_utc(cutoff_ts),
    );

    let data = match fetch_all(&client, &params, &args.cache_dir).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("backtest: fetch failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Build the grid config: spec defaults + the OOS cutoff (and optional override
    // of the skill-ranking sample floor). The conditional override is kept as the
    // first post-default statement so this stays clear of `field_reassign_with_default`.
    let mut cfg = GridConfig::default();
    if let Some(mb) = args.min_bets {
        cfg.min_bets = mb;
    }
    cfg.cutoff_ts = cutoff_ts;

    // Sweep the full grid (pure, deterministic).
    let results = run_grid(&data, &cfg);

    // ---- Universe descriptors for the report header. ----
    let bought = bought_condition_ids(&data.trades_by_wallet);
    let candidates = candidate_markets(&data.trades_by_wallet, &data.resolutions);
    let resolved_fraction = if bought.is_empty() {
        0.0
    } else {
        candidates.len() as f64 / bought.len() as f64
    };

    // PRE-cutoff records (the OUT-OF-SAMPLE selection set) and how many wallets
    // each ranking whitelists from them — the top of the selection funnel.
    let records = trader_records(&data.trades_by_wallet, &data.resolutions, cutoff_ts);
    let ranked: Vec<(&'static str, usize)> = cfg
        .rankings
        .iter()
        .map(|&r| {
            let n = rank_wallets_oos(r, &data.traders, &records, cfg.top_n, cfg.min_bets).len();
            (r.as_str(), n)
        })
        .collect();

    // POST-cutoff k=1 signals over the FULL universe — the top of the copy-test
    // funnel (before any ranking filter).
    let universe_wallets: Vec<String> =
        data.traders.iter().map(|t| t.proxy_wallet.clone()).collect();
    let post_cutoff_signals =
        signals_after(&universe_wallets, &data.trades_by_wallet, 1, cfg.window_secs, cutoff_ts).len();

    let ranked_json: Vec<serde_json::Value> = ranked
        .iter()
        .map(|(name, n)| serde_json::json!({ "ranking": name, "wallets": n }))
        .collect();

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
            "cutoff_days": args.cutoff_days,
            "cutoff_ts": cutoff_ts,
            "rankings": cfg.rankings.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
            "ks": cfg.ks.clone(),
            "lags_min": cfg.lags_min.clone(),
            "exits": cfg.exits.iter().map(|e| e.as_str()).collect::<Vec<_>>(),
            "freshness": cfg.freshness.clone(),
            "stops": cfg.stops.clone(),
        },
        "universe": {
            "traders": data.traders.len(),
            "bought_markets": bought.len(),
            "resolved_markets": data.resolutions.len(),
            "candidate_markets": candidates.len(),
            "resolved_fraction": resolved_fraction,
            "pre_cutoff_ranked_wallets": ranked_json,
            "post_cutoff_signals_k1": post_cutoff_signals,
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
        "backtest: universe traders={}, bought_markets={}, resolved_markets={} (Gamma), candidate_markets={}, coverage={:.1}% ({}/{}), tapes={}",
        data.traders.len(),
        bought.len(),
        data.resolutions.len(),
        candidates.len(),
        resolved_fraction * 100.0,
        candidates.len(),
        bought.len(),
        data.tape_by_market.len(),
    );
    let ranked_str: Vec<String> = ranked.iter().map(|(n, c)| format!("{n}={c}")).collect();
    println!(
        "backtest: OOS split @ cutoff_ts={} — pre-cutoff ranked wallets [{}]; post-cutoff k=1 signals (universe)={}",
        cutoff_ts,
        ranked_str.join(", "),
        post_cutoff_signals,
    );
    println!(
        "backtest: grid = {} cells × {} scopes = {} rows; wrote {RESULTS_FILE} and {}/fetched.json",
        results.len() / (3 + pm_backtest::core::PRICE_BUCKETS.len()),
        3 + pm_backtest::core::PRICE_BUCKETS.len(),
        results.len(),
        args.cache_dir.display(),
    );

    print_summary(&results, &ranked);

    ExitCode::SUCCESS
}

/// Print the stdout SUMMARY: the adequately-sampled `all`-scope cells sorted by
/// Sharpe (desc), a one-line GO/NO-GO VERDICT, the best cell per ranking, the
/// best price-bucket cell, and the best sports vs non-sports cells. This stdout
/// is what the operator/parent reads to judge GO/NO-GO.
fn print_summary(results: &[GridResult], ranked: &[(&'static str, usize)]) {
    let mut all_rows: Vec<&GridResult> = results.iter().filter(|r| r.scope == "all").collect();
    sort_by_sharpe_desc(&mut all_rows);

    println!();
    println!(
        "SUMMARY — all-scope cells with n>={MIN_DISPLAY_SAMPLE}, sorted by Sharpe (desc). Returns are per-trade fractions."
    );
    println!(
        "{:<14} {:>2} {:>4} {:<11} {:>6} {:>5} {:>5} {:>5} {:>9} {:>6} {:>9} {:>8} {:>8}",
        "RANKING", "K", "LAG", "EXIT", "FRESH", "STOP", "N", "SKIP", "MEANRET%", "HIT%", "TOTRET",
        "SHARPE", "MAXDD",
    );
    let mut shown = 0usize;
    for r in &all_rows {
        if r.metrics.n < MIN_DISPLAY_SAMPLE {
            continue;
        }
        let m = &r.metrics;
        println!(
            "{:<14} {:>2} {:>3}m {:<11} {:>6} {:>5} {:>5} {:>5} {:>9.2} {:>6.1} {:>9.3} {:>8.3} {:>8.3}",
            r.ranking,
            r.k,
            r.lag_min,
            r.exit,
            fresh_label(r.max_drift),
            stop_label(r.stop_loss),
            m.n,
            m.skipped,
            m.mean_ret * 100.0,
            m.hit_rate * 100.0,
            m.total_ret,
            m.sharpe,
            m.max_drawdown,
        );
        shown += 1;
    }
    if shown == 0 {
        println!("(no all-scope cell reached n>={MIN_DISPLAY_SAMPLE})");
    }

    // VERDICT: the best-by-Sharpe all-scope cell that BOTH has an adequate sample
    // AND a positive mean edge; otherwise an explicit no-edge verdict.
    println!();
    match all_rows
        .iter()
        .find(|r| r.metrics.n >= MIN_VERDICT_SAMPLE && r.metrics.mean_ret > 0.0)
    {
        Some(r) => println!("VERDICT: GO — {}", describe(r)),
        None => println!(
            "VERDICT: NO-GO — no positive edge with adequate sample (n>={MIN_VERDICT_SAMPLE})"
        ),
    }

    // The headline the operator asked for: stop vs no-stop on the SAME edge.
    print_stop_comparison(results);

    // Best cell per ranking (all scope, any positive sample), so a weak overall
    // verdict still shows which selection rule did best out-of-sample.
    println!();
    for (name, _) in ranked {
        match best_row(results, |r| r.scope == "all" && r.ranking == *name) {
            Some(r) => println!("BEST {name:<14} {}", describe(r)),
            None => println!("BEST {name:<14} none (no filled trades)"),
        }
    }

    // Best price-bucket cell — WHERE the edge lives on the price curve.
    println!();
    match best_row(results, |r| r.scope.starts_with("px:")) {
        Some(r) => println!("BEST price-bucket: {}", describe(r)),
        None => println!("BEST price-bucket: none (no filled trades)"),
    }

    // Best sports vs non-sports cells (any positive sample), for the split read.
    println!();
    match best_row(results, |r| r.scope == "sports") {
        Some(r) => println!("BEST sports:    {}", describe(r)),
        None => println!("BEST sports:    none (no filled sports trades)"),
    }
    match best_row(results, |r| r.scope == "nonsports") {
        Some(r) => println!("BEST nonsports: {}", describe(r)),
        None => println!("BEST nonsports: none (no filled nonsports trades)"),
    }
}

/// STOP-LOSS A/B: hold the winning copy CONFIG fixed and vary ONLY the stop, so
/// all arms replay the SAME signal population — the honest read on "stop vs no
/// stop for our edge over a longer run". The reference config is the best-by-
/// Sharpe NO-STOP `all`-scope cell with an adequate sample; we then pull the
/// matching `none`/`50%`/`25%` rows for `all`, `sports` and `nonsports` and print
/// them together. `total_ret` = cumulative edge (does stopping add or bleed
/// return?), `max_drawdown` = the pain a stop is supposed to buy down.
fn print_stop_comparison(results: &[GridResult]) {
    println!();
    println!("STOP-LOSS A/B — same config, only the stop differs (returns are per-trade fractions):");

    let mut no_stop_all: Vec<&GridResult> = results
        .iter()
        .filter(|r| r.scope == "all" && r.stop_loss.is_none() && r.metrics.n >= MIN_VERDICT_SAMPLE)
        .collect();
    sort_by_sharpe_desc(&mut no_stop_all);
    let Some(reference) = no_stop_all.first() else {
        println!("(no adequately-sampled no-stop cell to anchor the comparison)");
        return;
    };

    println!(
        "reference config: {} k={} lag={}m {} fresh={} (best no-stop cell, n={})",
        reference.ranking,
        reference.k,
        reference.lag_min,
        reference.exit,
        fresh_label(reference.max_drift),
        reference.metrics.n,
    );
    println!(
        "{:<10} {:>5} {:>5} {:>9} {:>6} {:>9} {:>8} {:>8}",
        "STOP·SCOPE", "N", "SKIP", "MEANRET%", "HIT%", "TOTRET", "SHARPE", "MAXDD",
    );
    for scope in ["all", "sports", "nonsports"] {
        for stop in [None, Some(0.50), Some(0.25)] {
            let Some(r) = results.iter().find(|r| {
                r.ranking == reference.ranking
                    && r.k == reference.k
                    && r.lag_min == reference.lag_min
                    && r.exit == reference.exit
                    && fresh_label(r.max_drift) == fresh_label(reference.max_drift)
                    && stop_label(r.stop_loss) == stop_label(stop)
                    && r.scope == scope
            }) else {
                continue;
            };
            let m = &r.metrics;
            println!(
                "{:<10} {:>5} {:>5} {:>9.2} {:>6.1} {:>9.3} {:>8.3} {:>8.3}",
                format!("{}·{}", stop_label(stop), scope),
                m.n,
                m.skipped,
                m.mean_ret * 100.0,
                m.hit_rate * 100.0,
                m.total_ret,
                m.sharpe,
                m.max_drawdown,
            );
        }
    }
}

/// The best (highest-Sharpe) row matching `pred` with at least one fill.
fn best_row(
    results: &[GridResult],
    pred: impl Fn(&GridResult) -> bool,
) -> Option<&GridResult> {
    let mut rows: Vec<&GridResult> = results
        .iter()
        .filter(|r| pred(r) && r.metrics.n > 0)
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
            .then(fresh_label(a.max_drift).cmp(&fresh_label(b.max_drift)))
            .then(stop_label(a.stop_loss).cmp(&stop_label(b.stop_loss)))
            .then(a.scope.cmp(&b.scope))
    });
}

/// A stable display label for a freshness threshold (`none` or e.g. `0.15`).
fn fresh_label(d: Option<f64>) -> String {
    match d {
        None => "none".to_string(),
        Some(x) => format!("{x:.2}"),
    }
}

/// One-line description of a grid cell + scope, for the verdict / split lines.
fn describe(r: &GridResult) -> String {
    let m = &r.metrics;
    format!(
        "{} k={} lag={}m {} fresh={} stop={} [{}] | n={} skipped={} mean_ret={:.2}% hit={:.1}% total_ret={:.3} sharpe={:.3} maxDD={:.3}",
        r.ranking,
        r.k,
        r.lag_min,
        r.exit,
        fresh_label(r.max_drift),
        stop_label(r.stop_loss),
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

/// A stable display label for a stop-loss fraction (`none` or e.g. `50%`).
fn stop_label(s: Option<f64>) -> String {
    match s {
        None => "none".to_string(),
        Some(x) => format!("{:.0}%", x * 100.0),
    }
}

/// Current UTC time as Unix-epoch seconds (0 if the clock predates the epoch).
fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Current UTC time as an RFC3339 second-precision string (`…Z`). Dependency-free
/// (no `chrono`): epoch seconds → civil date via Howard Hinnant's algorithm.
fn now_rfc3339() -> String {
    format_rfc3339_utc(now_epoch_secs())
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

    #[test]
    fn fresh_label_formats_none_and_value() {
        assert_eq!(fresh_label(None), "none");
        assert_eq!(fresh_label(Some(0.15)), "0.15");
        assert_eq!(fresh_label(Some(0.35)), "0.35");
    }
}
