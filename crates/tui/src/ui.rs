//! Interactive UI state (scroll positions, modal). Input handling in Task 5.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::state::TuiCommand;

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

/// Pure key → command mapping. `paused` is the CURRENT pause state (the
/// toggle emits its inverse). Modal-first: when a modal is open, keys edit
/// the modal and never leak global bindings.
pub fn handle_key(ev: KeyEvent, ui: &mut UiState, paused: bool) -> Option<TuiCommand> {
    // Ctrl-C always quits, modal or not.
    if ev.modifiers.contains(KeyModifiers::CONTROL) && ev.code == KeyCode::Char('c') {
        return Some(TuiCommand::Quit);
    }
    match &mut ui.modal {
        Modal::ConfirmKill => {
            match ev.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    ui.modal = Modal::None;
                    return Some(TuiCommand::Kill);
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => ui.modal = Modal::None,
                _ => {}
            }
            None
        }
        Modal::ConfirmLive(typed) => {
            match ev.code {
                KeyCode::Enter if typed == "live" => {
                    ui.modal = Modal::None;
                    return Some(TuiCommand::GoLive);
                }
                KeyCode::Enter => {} // wrong text: stay for correction
                KeyCode::Backspace => {
                    typed.pop();
                }
                KeyCode::Esc => ui.modal = Modal::None,
                KeyCode::Char(c) => typed.push(c),
                _ => {}
            }
            None
        }
        Modal::None => match ev.code {
            KeyCode::Char('q') => Some(TuiCommand::Quit),
            KeyCode::Char('p') => Some(TuiCommand::SetPaused(!paused)),
            KeyCode::Char('k') => {
                ui.modal = Modal::ConfirmKill;
                None
            }
            KeyCode::Char('l') => {
                ui.modal = Modal::ConfirmLive(String::new());
                None
            }
            KeyCode::Up => {
                ui.log_scroll = ui.log_scroll.saturating_add(1);
                None
            }
            KeyCode::Down => {
                ui.log_scroll = ui.log_scroll.saturating_sub(1);
                None
            }
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::state::TuiCommand;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)
    }
    fn code(k: KeyCode) -> KeyEvent {
        KeyEvent::new(k, KeyModifiers::NONE)
    }

    #[test]
    fn pause_toggles_with_p() {
        let mut ui = UiState::default();
        assert_eq!(
            handle_key(key('p'), &mut ui, false),
            Some(TuiCommand::SetPaused(true))
        );
        assert_eq!(
            handle_key(key('p'), &mut ui, true),
            Some(TuiCommand::SetPaused(false))
        );
    }

    #[test]
    fn quit_with_q_and_ctrl_c() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(key('q'), &mut ui, false), Some(TuiCommand::Quit));
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert_eq!(handle_key(ctrl_c, &mut ui, false), Some(TuiCommand::Quit));
    }

    #[test]
    fn kill_requires_confirm() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(key('k'), &mut ui, false), None);
        assert_eq!(ui.modal, Modal::ConfirmKill);
        // n cancels
        assert_eq!(handle_key(key('n'), &mut ui, false), None);
        assert_eq!(ui.modal, Modal::None);
        // y confirms
        handle_key(key('k'), &mut ui, false);
        assert_eq!(handle_key(key('y'), &mut ui, false), Some(TuiCommand::Kill));
        assert_eq!(ui.modal, Modal::None);
        // Esc cancels too
        handle_key(key('k'), &mut ui, false);
        assert_eq!(handle_key(code(KeyCode::Esc), &mut ui, false), None);
        assert_eq!(ui.modal, Modal::None);
    }

    #[test]
    fn live_toggle_requires_typed_confirmation() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(key('l'), &mut ui, false), None);
        assert_eq!(ui.modal, Modal::ConfirmLive(String::new()));
        for c in ['l', 'i', 'v', 'e'] {
            assert_eq!(handle_key(key(c), &mut ui, false), None);
        }
        assert_eq!(ui.modal, Modal::ConfirmLive("live".into()));
        assert_eq!(
            handle_key(code(KeyCode::Enter), &mut ui, false),
            Some(TuiCommand::GoLive)
        );
        assert_eq!(ui.modal, Modal::None);

        // wrong text + Enter: no command, modal stays for correction
        handle_key(key('l'), &mut ui, false);
        handle_key(key('x'), &mut ui, false);
        assert_eq!(handle_key(code(KeyCode::Enter), &mut ui, false), None);
        assert!(matches!(ui.modal, Modal::ConfirmLive(_)));
        // backspace edits
        assert_eq!(handle_key(code(KeyCode::Backspace), &mut ui, false), None);
        assert_eq!(ui.modal, Modal::ConfirmLive(String::new()));
        // Esc abandons
        assert_eq!(handle_key(code(KeyCode::Esc), &mut ui, false), None);
        assert_eq!(ui.modal, Modal::None);
    }

    #[test]
    fn log_scroll_keys() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(code(KeyCode::Up), &mut ui, false), None);
        assert_eq!(ui.log_scroll, 1);
        handle_key(code(KeyCode::Up), &mut ui, false);
        assert_eq!(ui.log_scroll, 2);
        handle_key(code(KeyCode::Down), &mut ui, false);
        assert_eq!(ui.log_scroll, 1);
        handle_key(code(KeyCode::Down), &mut ui, false);
        handle_key(code(KeyCode::Down), &mut ui, false); // saturates at 0
        assert_eq!(ui.log_scroll, 0);
    }

    #[test]
    fn keys_inside_modal_do_not_leak_global_commands() {
        let mut ui = UiState::default();
        handle_key(key('l'), &mut ui, false); // open live modal
        // 'q' is typed text inside the modal, NOT quit
        assert_eq!(handle_key(key('q'), &mut ui, false), None);
        assert_eq!(ui.modal, Modal::ConfirmLive("q".into()));
    }
}
