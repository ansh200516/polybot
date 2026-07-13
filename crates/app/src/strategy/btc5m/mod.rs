//! BTC "Up or Down 5m" strategy (spec 2026-07-13). Phase 0/1 is READ-ONLY:
//! it prices a fair P(up) and logs it against the live book; it emits NO orders.
pub mod model;
