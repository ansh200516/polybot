//! Tracing → ring buffer for the TUI log panel (spec §17 "tracing subscriber
//! tee"). In TUI mode this REPLACES the stdout fmt layer (stdout writes would
//! corrupt the alternate screen).

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// (level code, formatted line); level: 1=ERROR 2=WARN 3=INFO 4=DEBUG 5=TRACE.
pub struct LogBuffer {
    lines: Mutex<VecDeque<(u8, String)>>,
    cap: usize,
}

impl LogBuffer {
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(LogBuffer {
            lines: Mutex::new(VecDeque::with_capacity(cap)),
            cap: cap.max(1),
        })
    }

    fn push(&self, lvl: u8, line: String) {
        if let Ok(mut q) = self.lines.lock() {
            if q.len() == self.cap {
                q.pop_front();
            }
            q.push_back((lvl, line));
        }
    }

    /// Last `n` lines, oldest first.
    pub fn tail(&self, n: usize) -> Vec<(u8, String)> {
        match self.lines.lock() {
            Ok(q) => q
                .iter()
                .rev()
                .take(n)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

pub struct RingLayer {
    buf: Arc<LogBuffer>,
}

impl RingLayer {
    pub fn new(buf: Arc<LogBuffer>) -> Self {
        RingLayer { buf }
    }
}

struct LineVisitor {
    msg: String,
    rest: String,
}

impl Visit for LineVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.msg, "{value:?}");
        } else {
            let _ = write!(self.rest, " {}={:?}", field.name(), value);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        let _ = write!(self.rest, " {}={}", field.name(), value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let _ = write!(self.rest, " {}={}", field.name(), value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            let _ = write!(self.msg, "{value}");
        } else {
            let _ = write!(self.rest, " {}={}", field.name(), value);
        }
    }
}

fn level_code(l: &Level) -> u8 {
    match *l {
        Level::ERROR => 1,
        Level::WARN => 2,
        Level::INFO => 3,
        Level::DEBUG => 4,
        Level::TRACE => 5,
    }
}

impl<S: Subscriber> Layer<S> for RingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut v = LineVisitor {
            msg: String::new(),
            rest: String::new(),
        };
        event.record(&mut v);
        let line = format!("{:>5} {}: {}{}", meta.level(), meta.target(), v.msg, v.rest);
        self.buf.push(level_code(meta.level()), line);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn captures_formatted_events_with_levels() {
        let buf = LogBuffer::new(10);
        let subscriber = tracing_subscriber::registry().with(RingLayer::new(Arc::clone(&buf)));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(answer = 42, "the answer");
            tracing::warn!("watch out");
            tracing::error!("boom");
        });
        let tail = buf.tail(10);
        assert_eq!(tail.len(), 3);
        assert_eq!(tail[0].0, 3); // INFO
        assert!(tail[0].1.contains("the answer"));
        assert!(tail[0].1.contains("answer=42"));
        assert_eq!(tail[1].0, 2); // WARN
        assert_eq!(tail[2].0, 1); // ERROR
        assert!(tail[2].1.contains("boom"));
    }

    #[test]
    fn ring_evicts_oldest_at_capacity() {
        let buf = LogBuffer::new(3);
        let subscriber = tracing_subscriber::registry().with(RingLayer::new(Arc::clone(&buf)));
        tracing::subscriber::with_default(subscriber, || {
            for i in 0..5 {
                tracing::info!("line {i}");
            }
        });
        let tail = buf.tail(10);
        assert_eq!(tail.len(), 3);
        assert!(tail[0].1.contains("line 2"));
        assert!(tail[2].1.contains("line 4"));
    }

    #[test]
    fn tail_n_returns_last_n() {
        let buf = LogBuffer::new(10);
        let subscriber = tracing_subscriber::registry().with(RingLayer::new(Arc::clone(&buf)));
        tracing::subscriber::with_default(subscriber, || {
            for i in 0..6 {
                tracing::info!("l{i}");
            }
        });
        let t = buf.tail(2);
        assert_eq!(t.len(), 2);
        assert!(t[1].1.contains("l5"));
    }
}
