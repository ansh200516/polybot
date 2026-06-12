//! LP solver pool (spec §10/§12): consumes SolveJobs off the hot path,
//! bounded concurrency, exact re-validation already inside solve_component.

use std::sync::Arc;

use pm_engine::EngineParams;
use pm_engine::lp::{ComponentSpec, LpResult, solve_component};
use tokio::sync::{Semaphore, mpsc};

use crate::detector::{DetectedOpp, SolveJob};
use crate::stats::AppStats;

/// Drain SolveJobs, solve each with bounded concurrency, emit Found opps.
///
/// Completed task handles accumulate in `handles` until the pool's receiver
/// closes — bounded by total job count, acceptable for M3 paper sessions.
pub async fn run_lp_pool(
    mut rx: mpsc::Receiver<SolveJob>,
    opp_tx: mpsc::Sender<DetectedOpp>,
    params: EngineParams,
    concurrency: usize,
    stats: Arc<AppStats>,
) {
    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut handles = Vec::new();
    while let Some(job) = rx.recv().await {
        let Ok(permit) = Arc::clone(&sem).acquire_owned().await else {
            break;
        };
        let tx = opp_tx.clone();
        let stats = Arc::clone(&stats);
        handles.push(tokio::task::spawn_blocking(move || {
            let books = job.books;
            let spec = ComponentSpec {
                markets: job.markets,
                partitions: job.partitions,
                relationships: job.relationships,
                books: &books,
            };
            match solve_component(&spec, &params) {
                LpResult::Found(opp) => {
                    let _ = tx.try_send(DetectedOpp { opp, at: job.at });
                }
                LpResult::NoEdge => {}
                LpResult::Skipped(_) => {
                    stats
                        .lp_skips
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            drop(permit);
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use pm_core::book::{Book, Side};
    use pm_core::num::{Px, Qty, TickSize, Usdc};
    use pm_engine::{ArbClass, GasTable};
    use pm_registry::RegistryBuilder;
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::time::timeout;

    fn px(t: u16) -> Px {
        Px::new(t, TickSize::Cent).unwrap()
    }

    /// Build a book with bid and ask at specific tick values (100sh each side).
    fn book_with_quotes(ask_tick: u16, bid_tick: u16) -> Book {
        let mut b = Book::new(TickSize::Cent);
        b.apply(Side::Ask, px(ask_tick), Qty(100_000_000));
        b.apply(Side::Bid, px(bid_tick), Qty(100_000_000));
        b
    }

    /// Zero-gas params with min_profit=0 so nothing is filtered.
    fn solver_params() -> EngineParams {
        EngineParams {
            gas: GasTable {
                split: 0,
                merge: 0,
                redeem: 0,
                negrisk_convert: 0,
            },
            min_profit: Usdc(0),
            ..EngineParams::default()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lp_pool_solves_and_emits() {
        // 2-market C3-violation component: same fixture as detector class3 test.
        // Markets a (0xa) and b (0xb) with Implies(a⇒b).
        // Books: YES_a ask=55, NO_a ask=40, YES_b ask=30, NO_b ask=70
        // C3Implies: NO_a(40) + YES_b(30) = 70¢ → LP must recover as C4Lp.
        let mut builder = RegistryBuilder::default();
        builder.add_market(
            "0xa",
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
            "0xb",
            "yb",
            "nb",
            TickSize::Cent,
            0,
            false,
            None,
            true,
            false,
            None,
        );
        let toml = "[[relationship]]\nkind = \"implies\"\na = \"0xa\"\nb = \"0xb\"\nstatus = \"approved\"\n";
        let reg = builder.finish(toml).unwrap();

        let ma = *reg.market_by_condition("0xa").unwrap();
        let mb = *reg.market_by_condition("0xb").unwrap();

        // Build books with the violating quotes
        let mut books = HashMap::new();
        books.insert(ma.yes, book_with_quotes(55, 45)); // YES_a ask=55, bid=45
        books.insert(ma.no, book_with_quotes(40, 35)); // NO_a ask=40, bid=35
        books.insert(mb.yes, book_with_quotes(30, 25)); // YES_b ask=30, bid=25
        books.insert(mb.no, book_with_quotes(70, 60)); // NO_b ask=70, bid=60

        let cid = reg.component_of(ma.id);
        let job = SolveJob {
            component: cid,
            markets: vec![ma, mb],
            partitions: vec![],
            relationships: reg.approved_relationships().to_vec(),
            books,
            at: Instant::now(),
        };

        let (job_tx, job_rx) = mpsc::channel(8);
        let (opp_tx, mut opp_rx) = mpsc::channel(8);
        let stats = AppStats::new();

        let pool = tokio::spawn(run_lp_pool(
            job_rx,
            opp_tx,
            solver_params(),
            2,
            Arc::clone(&stats),
        ));

        job_tx.send(job).await.unwrap();
        // Drop sender so pool drains and exits.
        drop(job_tx);

        // Expect a C4Lp opportunity within 10 seconds.
        let received = timeout(std::time::Duration::from_secs(10), opp_rx.recv())
            .await
            .expect("timeout waiting for LP result")
            .expect("channel closed without result");

        assert_eq!(
            received.opp.class,
            ArbClass::C4Lp,
            "LP pool must emit C4Lp; got {:?}",
            received.opp.class
        );
        assert!(
            received.opp.net.0 > 0,
            "C4Lp net must be positive; got {:?}",
            received.opp.net
        );

        pool.await.unwrap();
    }
}
