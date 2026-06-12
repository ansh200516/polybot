//! Detection driver: runs in the shard task via Supervisor::on_apply
//! (spec §5/§12). Classes 1–3 inline; LP marked dirty + debounced.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pm_core::book::Book;
use pm_core::instrument::{Market, Partition, Relationship, TokenId};
use pm_engine::{EngineParams, Opportunity};
use pm_ingestion::shard::Shard;
use pm_registry::components::ComponentId;
use tokio::sync::mpsc;

use crate::stats::AppStats;
use crate::wiring::ComponentIndex;

/// An opportunity stamped with its detection instant (age gate + latency stage).
pub struct DetectedOpp {
    pub opp: Opportunity,
    pub at: Instant,
}

/// A dirty component snapshot for the LP pool (books cloned off the shard).
pub struct SolveJob {
    pub component: ComponentId,
    pub markets: Vec<Market>,
    pub partitions: Vec<Partition>,
    pub relationships: Vec<Relationship>,
    pub books: HashMap<TokenId, Book>,
    pub at: Instant,
}

pub struct Detector {
    index: Arc<ComponentIndex>,
    params: EngineParams,
    opp_tx: mpsc::Sender<DetectedOpp>,
    lp_tx: mpsc::Sender<SolveJob>,
    lp_min_interval: Duration,
    lp_last: HashMap<ComponentId, Instant>,
    stats: Arc<AppStats>,
    scratch: HashMap<TokenId, Book>,
}

impl Detector {
    pub fn new(
        index: Arc<ComponentIndex>,
        params: EngineParams,
        opp_tx: mpsc::Sender<DetectedOpp>,
        lp_tx: mpsc::Sender<SolveJob>,
        lp_min_interval: Duration,
        stats: Arc<AppStats>,
    ) -> Self {
        Self {
            index,
            params,
            opp_tx,
            lp_tx,
            lp_min_interval,
            lp_last: HashMap::new(),
            stats,
            scratch: HashMap::new(),
        }
    }

    /// The Supervisor::on_apply payload. Fired post-apply on a live feed;
    /// gates every read on LiveBook::valid() (spec §5 amendment).
    pub fn on_apply(&mut self, token: TokenId, shard: &Shard) {
        let Some(&cid) = self.index.by_token.get(&token) else {
            return;
        };
        let Some(entry) = self.index.entries.get(&cid) else {
            return;
        };
        let t0 = Instant::now();

        // Class 1 on the touched market — zero-clone &Book path.
        if let Some(m) = entry
            .markets
            .iter()
            .find(|m| m.yes == token || m.no == token)
            && let (Some(yes), Some(no)) = (valid_book(shard, m.yes), valid_book(shard, m.no))
        {
            for opp in pm_engine::class1::detect(m, yes, no, &self.params) {
                self.emit(opp, t0);
            }
        }

        // Multi-market components: clone valid books into scratch.
        if entry.markets.len() > 1 {
            self.scratch.clear();
            let mut all_valid = true;
            for &t in &entry.tokens {
                match valid_book(shard, t) {
                    Some(b) => {
                        self.scratch.insert(t, b.clone());
                    }
                    None => all_valid = false,
                }
            }

            for part in &entry.partitions {
                let complete = part
                    .yes_tokens
                    .iter()
                    .chain(part.no_tokens.iter())
                    .all(|t| self.scratch.contains_key(t));
                if complete {
                    for opp in
                        pm_engine::class2::detect(part, &entry.markets, &self.scratch, &self.params)
                    {
                        self.emit(opp, t0);
                    }
                }
            }

            for rel in &entry.relationships {
                for opp in
                    pm_engine::class3::detect(rel, &entry.markets, &self.scratch, &self.params)
                {
                    self.emit(opp, t0);
                }
            }

            // LP dirty mark, debounced per component; only with a full book set
            // (a partial component snapshot would mis-price the LP).
            if all_valid {
                let due = self
                    .lp_last
                    .get(&cid)
                    .is_none_or(|&last| t0.duration_since(last) >= self.lp_min_interval);
                if due {
                    let job = SolveJob {
                        component: cid,
                        markets: entry.markets.clone(),
                        partitions: entry.partitions.clone(),
                        relationships: entry.relationships.clone(),
                        books: self.scratch.clone(),
                        at: t0,
                    };
                    match self.lp_tx.try_send(job) {
                        Ok(()) => {
                            self.lp_last.insert(cid, t0);
                            self.stats
                                .lp_jobs
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(_) => {
                            self.stats
                                .lp_skips
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }

        self.stats.record_detect_us(t0.elapsed().as_micros() as u64);
    }

    fn emit(&self, opp: Opportunity, at: Instant) {
        use std::sync::atomic::Ordering::Relaxed;
        match self.opp_tx.try_send(DetectedOpp { opp, at }) {
            Ok(()) => {
                self.stats.opps_emitted.fetch_add(1, Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.stats.opps_dropped.fetch_add(1, Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// A book usable for detection: present AND integrity-valid.
fn valid_book(shard: &Shard, token: TokenId) -> Option<&Book> {
    let lb = shard.book(token)?;
    if lb.valid() { Some(lb.book()) } else { None }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use pm_core::num::TickSize;
    use pm_engine::ArbClass;
    use pm_ingestion::livebook::RawLevel;
    use pm_registry::RegistryBuilder;

    use crate::wiring::{ComponentEntry, ComponentIndex};

    // ---------------------------------------------------------------------------
    // Test fixture helpers
    // ---------------------------------------------------------------------------

    type Channels = (
        mpsc::Sender<DetectedOpp>,
        mpsc::Receiver<DetectedOpp>,
        mpsc::Sender<SolveJob>,
        mpsc::Receiver<SolveJob>,
        Arc<AppStats>,
    );

    /// Channels + AppStats defaults for test detectors.
    fn make_channels() -> Channels {
        let (opp_tx, opp_rx) = mpsc::channel(64);
        let (lp_tx, lp_rx) = mpsc::channel(64);
        let stats = AppStats::new();
        (opp_tx, opp_rx, lp_tx, lp_rx, stats)
    }

    fn raw(price_micro: u64, size_micro: u64) -> RawLevel {
        RawLevel {
            price_micro,
            size_micro,
        }
    }

    // ---------------------------------------------------------------------------
    // Test: singleton market, class-1 emitted, LP not triggered
    // ---------------------------------------------------------------------------

    #[test]
    fn class1_emitted_on_singleton_zero_clone_path() {
        // Registry: one binary market "0xa" (yes="ya", no="na"), Cent, fee=0.
        let mut b = RegistryBuilder::default();
        b.add_market(
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
        let reg = b.finish("").unwrap();
        let m = *reg.market_by_condition("0xa").unwrap();

        // Build ComponentIndex manually (mirroring wiring::build_component_index).
        let cid = reg.component_of(m.id);
        let mut by_token = HashMap::new();
        by_token.insert(m.yes, cid);
        by_token.insert(m.no, cid);
        let mut entries = HashMap::new();
        entries.insert(
            cid,
            ComponentEntry {
                markets: vec![m],
                partitions: vec![],
                relationships: vec![],
                tokens: vec![m.yes, m.no],
            },
        );
        let index = Arc::new(ComponentIndex { by_token, entries });

        let (opp_tx, mut opp_rx, lp_tx, mut lp_rx, stats) = make_channels();
        let mut det = Detector::new(
            index,
            EngineParams::default(),
            opp_tx,
            lp_tx,
            Duration::from_millis(500),
            stats,
        );

        // Seed shard: YES bid=40/ask=44; NO bid=45/ask=50
        // C1Long: buy YES ask(44) + buy NO ask(50) = 0.94 < 1.00 → arb
        let mut shard = Shard::default();
        let now = Instant::now();
        shard.apply_snapshot(
            now,
            m.yes,
            TickSize::Cent,
            &[raw(40 * 10_000, 100_000_000)], // bid 40¢
            &[raw(44 * 10_000, 100_000_000)], // ask 44¢
            "h1",
        );
        shard.apply_snapshot(
            now,
            m.no,
            TickSize::Cent,
            &[raw(45 * 10_000, 100_000_000)], // bid 45¢
            &[raw(50 * 10_000, 100_000_000)], // ask 50¢
            "h2",
        );

        det.on_apply(m.no, &shard);

        // Expect class-1 opportunity emitted
        let d = opp_rx.try_recv().unwrap();
        assert_eq!(d.opp.class, ArbClass::C1Long, "expected C1Long");
        assert!(d.opp.net.0 > 0, "net must be positive");

        // Singleton: LP job must NOT be enqueued
        assert!(
            lp_rx.try_recv().is_err(),
            "singleton should not enqueue LP job"
        );
    }

    // ---------------------------------------------------------------------------
    // Test: stale books suppress detection
    // ---------------------------------------------------------------------------

    #[test]
    fn invalid_book_suppresses_detection() {
        let mut b = RegistryBuilder::default();
        b.add_market(
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
        let reg = b.finish("").unwrap();
        let m = *reg.market_by_condition("0xa").unwrap();

        let cid = reg.component_of(m.id);
        let mut by_token = HashMap::new();
        by_token.insert(m.yes, cid);
        by_token.insert(m.no, cid);
        let mut entries = HashMap::new();
        entries.insert(
            cid,
            ComponentEntry {
                markets: vec![m],
                partitions: vec![],
                relationships: vec![],
                tokens: vec![m.yes, m.no],
            },
        );
        let index = Arc::new(ComponentIndex { by_token, entries });

        let (opp_tx, mut opp_rx, lp_tx, _lp_rx, stats) = make_channels();
        let mut det = Detector::new(
            index,
            EngineParams::default(),
            opp_tx,
            lp_tx,
            Duration::from_millis(500),
            stats,
        );

        let mut shard = Shard::default();
        let now = Instant::now();
        // Seed valid books
        shard.apply_snapshot(
            now,
            m.yes,
            TickSize::Cent,
            &[raw(40 * 10_000, 100_000_000)],
            &[raw(44 * 10_000, 100_000_000)],
            "h1",
        );
        shard.apply_snapshot(
            now,
            m.no,
            TickSize::Cent,
            &[raw(45 * 10_000, 100_000_000)],
            &[raw(50 * 10_000, 100_000_000)],
            "h2",
        );
        // Invalidate both books
        shard.mark_all_stale();

        det.on_apply(m.no, &shard);

        assert!(
            opp_rx.try_recv().is_err(),
            "stale books must suppress all emission"
        );
    }

    // ---------------------------------------------------------------------------
    // Test: missing one leg suppresses class-1
    // ---------------------------------------------------------------------------

    #[test]
    fn missing_leg_book_suppresses_class1() {
        let mut b = RegistryBuilder::default();
        b.add_market(
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
        let reg = b.finish("").unwrap();
        let m = *reg.market_by_condition("0xa").unwrap();

        let cid = reg.component_of(m.id);
        let mut by_token = HashMap::new();
        by_token.insert(m.yes, cid);
        by_token.insert(m.no, cid);
        let mut entries = HashMap::new();
        entries.insert(
            cid,
            ComponentEntry {
                markets: vec![m],
                partitions: vec![],
                relationships: vec![],
                tokens: vec![m.yes, m.no],
            },
        );
        let index = Arc::new(ComponentIndex { by_token, entries });

        let (opp_tx, mut opp_rx, lp_tx, _lp_rx, stats) = make_channels();
        let mut det = Detector::new(
            index,
            EngineParams::default(),
            opp_tx,
            lp_tx,
            Duration::from_millis(500),
            stats,
        );

        // Only seed YES — NO is absent
        let mut shard = Shard::default();
        let now = Instant::now();
        shard.apply_snapshot(
            now,
            m.yes,
            TickSize::Cent,
            &[raw(40 * 10_000, 100_000_000)],
            &[raw(44 * 10_000, 100_000_000)],
            "h1",
        );

        det.on_apply(m.yes, &shard);

        assert!(
            opp_rx.try_recv().is_err(),
            "missing NO leg must suppress class-1"
        );
    }

    // ---------------------------------------------------------------------------
    // Test: class-3 violation detected + LP job enqueued once (debounce)
    // ---------------------------------------------------------------------------

    #[test]
    fn class3_violation_detected_and_lp_job_enqueued_once() {
        // 2 markets linked by Implies(a⇒b): a.no ask=40, b.yes ask=30 → C3Implies
        // (0.40 + 0.30 = 0.70 < 1.00, 30¢ edge ≥ 100 bps floor)
        let mut b = RegistryBuilder::default();
        b.add_market(
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
        b.add_market(
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
        let reg = b.finish(toml).unwrap();

        let ma = *reg.market_by_condition("0xa").unwrap();
        let mb = *reg.market_by_condition("0xb").unwrap();

        // Both markets must be in the same component (relationship links them)
        let cid = reg.component_of(ma.id);
        assert_eq!(reg.component_of(mb.id), cid);

        let mut by_token = HashMap::new();
        for t in [ma.yes, ma.no, mb.yes, mb.no] {
            by_token.insert(t, cid);
        }
        let mut entries_map = HashMap::new();
        entries_map.insert(
            cid,
            ComponentEntry {
                markets: vec![ma, mb],
                partitions: vec![],
                relationships: reg.approved_relationships().to_vec(),
                tokens: vec![ma.yes, ma.no, mb.yes, mb.no],
            },
        );
        let index = Arc::new(ComponentIndex {
            by_token,
            entries: entries_map,
        });

        // Use zero-gas params + min_profit=0 so the edge is never filtered.
        let params = EngineParams {
            gas: pm_engine::GasTable {
                split: 0,
                merge: 0,
                redeem: 0,
                negrisk_convert: 0,
            },
            min_profit: pm_core::num::Usdc(0),
            ..EngineParams::default()
        };

        let (opp_tx, mut opp_rx, lp_tx, mut lp_rx, stats) = make_channels();
        let mut det = Detector::new(
            Arc::clone(&index),
            params,
            opp_tx,
            lp_tx,
            Duration::from_millis(500),
            Arc::clone(&stats),
        );

        // Seed books: YES_a ask=55, NO_a ask=40, YES_b ask=30, NO_b ask=70
        // Class-3 Implies: buy NO_a (ask=40) + buy YES_b (ask=30) = 70¢ → net 30¢/sh
        let mut shard = Shard::default();
        let now = Instant::now();
        shard.apply_snapshot(
            now,
            ma.yes,
            TickSize::Cent,
            &[raw(45 * 10_000, 100_000_000)],
            &[raw(55 * 10_000, 100_000_000)],
            "h_ya",
        );
        shard.apply_snapshot(
            now,
            ma.no,
            TickSize::Cent,
            &[raw(35 * 10_000, 100_000_000)],
            &[raw(40 * 10_000, 100_000_000)],
            "h_na",
        );
        shard.apply_snapshot(
            now,
            mb.yes,
            TickSize::Cent,
            &[raw(25 * 10_000, 100_000_000)],
            &[raw(30 * 10_000, 100_000_000)],
            "h_yb",
        );
        shard.apply_snapshot(
            now,
            mb.no,
            TickSize::Cent,
            &[raw(60 * 10_000, 100_000_000)],
            &[raw(70 * 10_000, 100_000_000)],
            "h_nb",
        );

        det.on_apply(ma.no, &shard);

        // C3Implies must be emitted (C1 may also fire; drain until we find it)
        let mut found_c3 = false;
        while let Ok(d) = opp_rx.try_recv() {
            if d.opp.class == ArbClass::C3Implies {
                found_c3 = true;
                break;
            }
        }
        assert!(found_c3, "expected C3Implies among emitted opportunities");

        // LP job with 4 books, 2 markets, 1 relationship
        let job = lp_rx.try_recv().unwrap();
        assert_eq!(job.markets.len(), 2, "LP job must have 2 markets");
        assert_eq!(job.books.len(), 4, "LP job must have 4 books");
        assert_eq!(
            job.relationships.len(),
            1,
            "LP job must have 1 relationship"
        );

        // Second on_apply immediately → LP must NOT be re-enqueued (debounce)
        det.on_apply(ma.no, &shard);
        assert!(
            lp_rx.try_recv().is_err(),
            "second immediate on_apply must not bypass debounce"
        );
    }
}
