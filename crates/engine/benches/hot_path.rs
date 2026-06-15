//! Hot-path benchmarks vs spec §20 gates.
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use pm_core::book::{Book, Ladder, Side};
use pm_core::instrument::{EventId, Market, MarketId, Partition, Relationship, TokenId};
use pm_core::num::{Bps, Px, Qty, TickSize, Usdc};
use pm_engine::{EngineParams, GasTable, class1, class2, lp};

const TS: TickSize = TickSize::Cent;

fn px(t: u16) -> Px {
    Px::new(t, TS).unwrap()
}

fn params() -> EngineParams {
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

fn mk(i: u32) -> Market {
    Market {
        id: MarketId(i),
        yes: TokenId(u64::from(i) * 2 + 10),
        no: TokenId(u64::from(i) * 2 + 11),
        tick: TS,
        fee_bps: Bps(0),
        neg_risk: false,
    }
}

fn arb_books() -> (Book, Book) {
    let mut yes = Book::new(TS);
    let mut no = Book::new(TS);
    for (i, q) in [(46u16, 50_000_000u64), (48, 30_000_000), (51, 20_000_000)] {
        yes.apply(Side::Ask, px(i), Qty(q));
    }
    for (i, q) in [(52u16, 50_000_000u64), (53, 30_000_000), (55, 20_000_000)] {
        no.apply(Side::Ask, px(i), Qty(q));
    }
    yes.apply(Side::Bid, px(44), Qty(50_000_000));
    no.apply(Side::Bid, px(50), Qty(50_000_000));
    (yes, no)
}

fn bench_ladder_apply(c: &mut Criterion) {
    // Gate: ≤ 1 µs p99 per set (spec §20). 1024 sets per iteration — divide.
    let deltas: Vec<(Px, Qty)> = (0..1024)
        .map(|i| {
            (
                px(1 + (i * 7) % 98),
                Qty(u64::from((i * 13) % 5) * 1_000_000),
            )
        })
        .collect();
    c.bench_function("ladder_apply_delta_x1024", |b| {
        b.iter_batched_ref(
            || Ladder::new(TS, Side::Ask),
            |l| {
                for &(p, q) in &deltas {
                    l.set(black_box(p), black_box(q));
                }
            },
            BatchSize::SmallInput,
        )
    });
    // Milli worst case: 1000-level ladder, repeatedly zero the best (forces
    // rescans) then refill.
    let mts = TickSize::Milli;
    let mpx = |t: u16| Px::new(t, mts).unwrap();
    c.bench_function("ladder_milli_worstcase_x999", |b| {
        b.iter_batched_ref(
            || {
                let mut l = Ladder::new(mts, Side::Ask);
                for t in 1..1000 {
                    l.set(mpx(t), Qty(1_000_000));
                }
                l
            },
            |l| {
                for t in 1..1000 {
                    l.set(black_box(mpx(t)), black_box(Qty(0))); // zero best → rescan
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_class1_detect(c: &mut Criterion) {
    // Gate: ≤ 20 µs p99 post-apply (spec §20).
    let (yes, no) = arb_books();
    let m = mk(0);
    let p = params();
    c.bench_function("class1_detect", |b| {
        b.iter(|| {
            black_box(class1::detect(
                black_box(&m),
                black_box(&yes),
                black_box(&no),
                &p,
            ))
        })
    });
}

fn bench_class2_scan_n16(c: &mut Criterion) {
    // Gate: ≤ 50 µs p99 for n ≤ 16 (spec §20).
    let n = 16u32;
    let mut markets = Vec::new();
    let mut books = HashMap::new();
    let mut yes_tokens = Vec::new();
    let mut no_tokens = Vec::new();
    let mut market_ids = Vec::new();
    for i in 0..n {
        let m = mk(i);
        let mut yb = Book::new(TS);
        yb.apply(Side::Ask, px(5), Qty(100_000_000)); // 16×0.05 = 0.80 < 1 → arb
        let mut nb = Book::new(TS);
        nb.apply(Side::Ask, px(97), Qty(100_000_000));
        nb.apply(Side::Bid, px(93), Qty(100_000_000));
        yb.apply(Side::Bid, px(3), Qty(100_000_000));
        books.insert(m.yes, yb);
        books.insert(m.no, nb);
        yes_tokens.push(m.yes);
        no_tokens.push(m.no);
        market_ids.push(m.id);
        markets.push(m);
    }
    let part = Partition {
        event: EventId(0),
        markets: market_ids,
        yes_tokens,
        no_tokens,
        verified_exhaustive: true,
        neg_risk: true,
    };
    let p = params();
    c.bench_function("class2_scan_n16", |b| {
        b.iter(|| black_box(class2::detect(black_box(&part), &markets, &books, &p)))
    });
}

fn bench_lp_8_markets(c: &mut Criterion) {
    // Gate: ≤ 10 ms p99 (spec §20): 8 binaries, 2 relationships, planted arb.
    let markets: Vec<Market> = (0..8).map(mk).collect();
    let mut books = HashMap::new();
    for m in &markets {
        let (ya, na) = if m.id == MarketId(0) {
            (46, 52)
        } else {
            (50, 52)
        };
        let mut yb = Book::new(TS);
        yb.apply(Side::Ask, px(ya), Qty(50_000_000));
        yb.apply(Side::Bid, px(ya - 2), Qty(50_000_000));
        let mut nb = Book::new(TS);
        nb.apply(Side::Ask, px(na), Qty(50_000_000));
        nb.apply(Side::Bid, px(na - 2), Qty(50_000_000));
        books.insert(m.yes, yb);
        books.insert(m.no, nb);
    }
    let spec = lp::ComponentSpec {
        markets,
        partitions: vec![],
        relationships: vec![
            Relationship::Implies {
                a: MarketId(1),
                b: MarketId(2),
            },
            Relationship::MutuallyExclusive {
                a: MarketId(3),
                b: MarketId(4),
            },
        ],
        books: &books,
    };
    let p = params();
    c.bench_function("lp_solve_8_markets", |b| {
        b.iter(|| black_box(lp::solve_component(black_box(&spec), &p)))
    });
}

criterion_group!(
    benches,
    bench_ladder_apply,
    bench_class1_detect,
    bench_class2_scan_n16,
    bench_lp_8_markets
);
criterion_main!(benches);
