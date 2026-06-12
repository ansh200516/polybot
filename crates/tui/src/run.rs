//! The TUI event loop: redraw on AppState changes (~10 Hz) and on key input;
//! emit TuiCommands. Input is read on a dedicated std thread (blocking
//! crossterm::event::read) and forwarded over an mpsc — handle_key stays pure.

use std::sync::Arc;

use crossterm::event::{Event, KeyEvent, KeyEventKind};
use ratatui::Terminal;
use ratatui::backend::Backend;
use tokio::sync::{mpsc, watch};

use crate::render::draw;
use crate::state::{AppState, TuiCommand};
use crate::ui::UiState;

/// Spawn the blocking input reader. Exits when the receiver is dropped.
pub fn spawn_input_thread() -> mpsc::Receiver<KeyEvent> {
    let (tx, rx) = mpsc::channel(32);
    std::thread::spawn(move || {
        loop {
            match crossterm::event::read() {
                Ok(Event::Key(k)) if k.kind == KeyEventKind::Press => {
                    if tx.blocking_send(k).is_err() {
                        return; // TUI gone
                    }
                }
                Ok(_) => {} // resize/mouse: next draw picks it up
                Err(_) => return,
            }
        }
    });
    rx
}

/// Drive the dashboard until Quit is emitted or the state channel closes.
/// The caller owns terminal setup/teardown (raw mode, alternate screen).
pub async fn run_tui<B: Backend>(
    terminal: &mut Terminal<B>,
    mut state_rx: watch::Receiver<Arc<AppState>>,
    mut key_rx: mpsc::Receiver<KeyEvent>,
    cmd_tx: mpsc::Sender<TuiCommand>,
) -> std::io::Result<()> {
    let mut ui = UiState::default();
    loop {
        let state = state_rx.borrow_and_update().clone();
        terminal.draw(|f| draw(f, &state, &ui))?;
        tokio::select! {
            changed = state_rx.changed() => {
                if changed.is_err() {
                    return Ok(()); // publisher gone: session ending
                }
            }
            maybe_key = key_rx.recv() => {
                let Some(key) = maybe_key else { return Ok(()) };
                let paused = state.paused;
                if let Some(cmd) = crate::ui::handle_key(key, &mut ui, paused) {
                    let quit = cmd == TuiCommand::Quit;
                    let _ = cmd_tx.send(cmd).await;
                    if quit {
                        return Ok(());
                    }
                }
            }
        }
    }
}
