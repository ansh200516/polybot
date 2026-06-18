//! Interactive UI state (scroll positions, modal). Input handling in Task 5.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::state::{OpenOrderLine, TuiCommand};

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
    /// Selected row in the open-orders panel (j/k move it; `x` cancels/un-vetoes
    /// it). Clamped to the current list length wherever it is used.
    pub open_orders_selected: usize,
}

/// Pure key → command mapping. `paused` is the CURRENT pause state (the
/// toggle emits its inverse). `open_orders` is the current open-orders panel
/// (for selection + the cancel/un-veto key). Modal-first: when a modal is open,
/// keys edit the modal and never leak global bindings.
pub fn handle_key(
    ev: KeyEvent,
    ui: &mut UiState,
    paused: bool,
    open_orders: &[OpenOrderLine],
) -> Option<TuiCommand> {
    // Ctrl-C always quits, modal or not.
    if ev.modifiers.contains(KeyModifiers::CONTROL) && ev.code == KeyCode::Char('c') {
        return Some(TuiCommand::Quit);
    }
    // Keep the open-orders selection within the current list bounds (the list
    // grows/shrinks as quotes rest, fill, and get vetoed).
    ui.open_orders_selected = ui
        .open_orders_selected
        .min(open_orders.len().saturating_sub(1));
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
            // Open-orders panel selection: Tab moves the cursor down, Shift-Tab up
            // (↑/↓ stay bound to log scroll; `k` is the kill key).
            KeyCode::Tab => {
                let last = open_orders.len().saturating_sub(1);
                ui.open_orders_selected = (ui.open_orders_selected + 1).min(last);
                None
            }
            KeyCode::BackTab => {
                ui.open_orders_selected = ui.open_orders_selected.saturating_sub(1);
                None
            }
            // `x` cancels the selected LIVE order (veto), or UN-vetoes a vetoed
            // slot — `veto = !already_vetoed`. No-op when the panel is empty.
            KeyCode::Char('x') => open_orders.get(ui.open_orders_selected).map(|o| {
                TuiCommand::SetVeto {
                    key: o.key.clone(),
                    veto: !o.vetoed,
                }
            }),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::state::{OpenOrderLine, TuiCommand};
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
            handle_key(key('p'), &mut ui, false, &[]),
            Some(TuiCommand::SetPaused(true))
        );
        assert_eq!(
            handle_key(key('p'), &mut ui, true, &[]),
            Some(TuiCommand::SetPaused(false))
        );
    }

    #[test]
    fn quit_with_q_and_ctrl_c() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(key('q'), &mut ui, false, &[]), Some(TuiCommand::Quit));
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert_eq!(handle_key(ctrl_c, &mut ui, false, &[]), Some(TuiCommand::Quit));
    }

    #[test]
    fn kill_requires_confirm() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(key('k'), &mut ui, false, &[]), None);
        assert_eq!(ui.modal, Modal::ConfirmKill);
        // n cancels
        assert_eq!(handle_key(key('n'), &mut ui, false, &[]), None);
        assert_eq!(ui.modal, Modal::None);
        // y confirms
        handle_key(key('k'), &mut ui, false, &[]);
        assert_eq!(handle_key(key('y'), &mut ui, false, &[]), Some(TuiCommand::Kill));
        assert_eq!(ui.modal, Modal::None);
        // Esc cancels too
        handle_key(key('k'), &mut ui, false, &[]);
        assert_eq!(handle_key(code(KeyCode::Esc), &mut ui, false, &[]), None);
        assert_eq!(ui.modal, Modal::None);
    }

    #[test]
    fn live_toggle_requires_typed_confirmation() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(key('l'), &mut ui, false, &[]), None);
        assert_eq!(ui.modal, Modal::ConfirmLive(String::new()));
        for c in ['l', 'i', 'v', 'e'] {
            assert_eq!(handle_key(key(c), &mut ui, false, &[]), None);
        }
        assert_eq!(ui.modal, Modal::ConfirmLive("live".into()));
        assert_eq!(
            handle_key(code(KeyCode::Enter), &mut ui, false, &[]),
            Some(TuiCommand::GoLive)
        );
        assert_eq!(ui.modal, Modal::None);

        // wrong text + Enter: no command, modal stays for correction
        handle_key(key('l'), &mut ui, false, &[]);
        handle_key(key('x'), &mut ui, false, &[]);
        assert_eq!(handle_key(code(KeyCode::Enter), &mut ui, false, &[]), None);
        assert!(matches!(ui.modal, Modal::ConfirmLive(_)));
        // backspace edits
        assert_eq!(handle_key(code(KeyCode::Backspace), &mut ui, false, &[]), None);
        assert_eq!(ui.modal, Modal::ConfirmLive(String::new()));
        // Esc abandons
        assert_eq!(handle_key(code(KeyCode::Esc), &mut ui, false, &[]), None);
        assert_eq!(ui.modal, Modal::None);
    }

    #[test]
    fn log_scroll_keys() {
        let mut ui = UiState::default();
        assert_eq!(handle_key(code(KeyCode::Up), &mut ui, false, &[]), None);
        assert_eq!(ui.log_scroll, 1);
        handle_key(code(KeyCode::Up), &mut ui, false, &[]);
        assert_eq!(ui.log_scroll, 2);
        handle_key(code(KeyCode::Down), &mut ui, false, &[]);
        assert_eq!(ui.log_scroll, 1);
        handle_key(code(KeyCode::Down), &mut ui, false, &[]);
        handle_key(code(KeyCode::Down), &mut ui, false, &[]); // saturates at 0
        assert_eq!(ui.log_scroll, 0);
    }

    #[test]
    fn keys_inside_modal_do_not_leak_global_commands() {
        let mut ui = UiState::default();
        handle_key(key('l'), &mut ui, false, &[]); // open live modal
        // 'q' is typed text inside the modal, NOT quit
        assert_eq!(handle_key(key('q'), &mut ui, false, &[]), None);
        assert_eq!(ui.modal, Modal::ConfirmLive("q".into()));
    }

    fn oo(key: &str, vetoed: bool) -> OpenOrderLine {
        OpenOrderLine {
            strategy: "mm".into(),
            market: "M".into(),
            side: "Bid".into(),
            px: "0.40".into(),
            qty_shares: 10.0,
            vetoed,
            key: key.into(),
        }
    }

    #[test]
    fn open_orders_select_and_cancel() {
        let mut ui = UiState::default();
        let orders = vec![oo("1:b", false), oo("1:a", false), oo("2:b", true)];

        // Tab moves down (clamped to the last row); Shift-Tab moves up (saturating).
        handle_key(code(KeyCode::Tab), &mut ui, false, &orders);
        assert_eq!(ui.open_orders_selected, 1);
        handle_key(code(KeyCode::Tab), &mut ui, false, &orders);
        handle_key(code(KeyCode::Tab), &mut ui, false, &orders); // clamps at last (2)
        assert_eq!(ui.open_orders_selected, 2);
        handle_key(code(KeyCode::BackTab), &mut ui, false, &orders);
        assert_eq!(ui.open_orders_selected, 1);

        // x on a LIVE order (index 1) → veto = true.
        assert_eq!(
            handle_key(key('x'), &mut ui, false, &orders),
            Some(TuiCommand::SetVeto {
                key: "1:a".into(),
                veto: true
            })
        );
        // x on a VETOED slot (index 2) → veto = false (un-veto).
        handle_key(code(KeyCode::Tab), &mut ui, false, &orders);
        assert_eq!(
            handle_key(key('x'), &mut ui, false, &orders),
            Some(TuiCommand::SetVeto {
                key: "2:b".into(),
                veto: false
            })
        );
    }

    #[test]
    fn open_orders_x_noop_when_empty_and_selection_clamps_on_shrink() {
        let mut ui = UiState::default();
        // Empty panel: x is a no-op.
        assert_eq!(handle_key(key('x'), &mut ui, false, &[]), None);

        // Select the last of three, then the list shrinks to one → the selection
        // clamps so x still targets a valid row (not an out-of-bounds no-op).
        let three = vec![oo("a:b", false), oo("b:b", false), oo("c:b", false)];
        handle_key(code(KeyCode::Tab), &mut ui, false, &three);
        handle_key(code(KeyCode::Tab), &mut ui, false, &three);
        assert_eq!(ui.open_orders_selected, 2);
        let one = vec![oo("z:b", false)];
        assert_eq!(
            handle_key(key('x'), &mut ui, false, &one),
            Some(TuiCommand::SetVeto {
                key: "z:b".into(),
                veto: true
            })
        );
        assert_eq!(ui.open_orders_selected, 0, "clamped to the shrunk list");
    }
}
