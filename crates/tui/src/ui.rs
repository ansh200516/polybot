//! Interactive UI state (scroll positions, modal). Input handling in Task 5.

/// Modal overlays. None = normal operation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Modal {
    #[default]
    None,
    /// y/N confirm before tripping the kill switch.
    ConfirmKill,
    /// Typed confirmation for paper→live (spec §17). Holds typed buffer.
    ConfirmLive(String),
}

#[derive(Debug, Clone, Default)]
pub struct UiState {
    pub modal: Modal,
    /// Log panel scroll offset from the tail (0 = follow).
    pub log_scroll: u16,
}
