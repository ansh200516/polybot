//! Read-only Polymarket ingestion: REST/WS clients, live books, registry sync.

pub mod confluence;
pub mod data_api;
pub mod decimal;
pub mod livebook;
pub mod rest;
pub mod shard;
pub mod smart_money;
pub mod spot;
pub mod stats;
pub mod supervisor;
pub mod sync;
pub mod ws;

// ---------------------------------------------------------------------------
// Crate-level error type
// ---------------------------------------------------------------------------

/// Top-level error for all ingestion operations.
#[derive(Debug)]
pub enum IngestError {
    /// An HTTP transport or status error.
    Http(String),
    /// A JSON or other parsing error.
    Parse(String),
    /// A decimal parsing error from the money path.
    Decimal(crate::decimal::DecimalError),
    /// A WebSocket transport or protocol error.
    Ws(String),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Http(s) => write!(f, "HTTP error: {s}"),
            IngestError::Parse(s) => write!(f, "parse error: {s}"),
            IngestError::Decimal(e) => write!(f, "decimal error: {e}"),
            IngestError::Ws(s) => write!(f, "WebSocket error: {s}"),
        }
    }
}

impl std::error::Error for IngestError {}

impl From<crate::decimal::DecimalError> for IngestError {
    fn from(e: crate::decimal::DecimalError) -> Self {
        IngestError::Decimal(e)
    }
}
