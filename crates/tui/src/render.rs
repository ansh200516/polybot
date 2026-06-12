//! Dashboard rendering. Calls `draw(f, state, ui)` from the main event loop.

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
};

use crate::state::AppState;
use crate::ui::{Modal, UiState};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Money color: green ≥ 0, red < 0 (the ONLY places green/red are used are
/// edge and P&L values, per spec §17).
fn money_style(v: f64) -> Style {
    if v < 0.0 {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::Green)
    }
}

fn fmt_usd(v: f64) -> String {
    format!("{v:.2}")
}

fn fmt_uptime(s: u64) -> String {
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
    } else {
        format!("{:02}:{:02}", s / 60, s % 60)
    }
}

/// Returns a centered rect using percentage cuts of the parent area.
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let margin_y = (100 - percent_y) / 2;
    let margin_x = (100 - percent_x) / 2;
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(margin_y),
            Constraint::Percentage(percent_y),
            Constraint::Percentage(margin_y),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(margin_x),
            Constraint::Percentage(percent_x),
            Constraint::Percentage(margin_x),
        ])
        .split(popup_layout[1])[1]
}

// ---------------------------------------------------------------------------
// Panel draw helpers
// ---------------------------------------------------------------------------

fn draw_header(f: &mut Frame, s: &AppState, area: Rect) {
    let mode_badge = if s.mode_paper { "PAPER" } else { "LIVE" };
    let uptime_str = fmt_uptime(s.uptime_s);

    let mut spans: Vec<Span> = vec![
        Span::styled(
            format!(" {mode_badge} "),
            Style::default()
                .fg(Color::Black)
                .bg(if s.mode_paper {
                    Color::Yellow
                } else {
                    Color::Cyan
                })
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  up {uptime_str}  ")),
    ];

    // Status badges (KILLED takes precedence over HALT over PAUSED)
    if s.killed {
        spans.push(Span::styled(
            " KILLED ",
            Style::default()
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw("  "));
    } else if let Some(ref reason) = s.halted {
        spans.push(Span::styled(
            format!(" HALT:{reason} "),
            Style::default()
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw("  "));
    } else if s.paused {
        spans.push(Span::styled(
            " PAUSED ",
            Style::default()
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw("  "));
    }

    // P&L / equity values
    let eq_usd = s.equity_usd;
    let eq_mid = s.equity_mid_usd;
    let realized = s.realized_usd;
    let unrealized = s.unrealized_usd;
    let cash = s.cash_usd;

    spans.push(Span::raw("equity(bid) "));
    spans.push(Span::styled(fmt_usd(eq_usd), money_style(eq_usd)));
    spans.push(Span::raw("  risk-equity(mid) "));
    spans.push(Span::styled(fmt_usd(eq_mid), money_style(eq_mid)));
    spans.push(Span::raw("  realized "));
    spans.push(Span::styled(fmt_usd(realized), money_style(realized)));
    spans.push(Span::raw("  unrealized "));
    spans.push(Span::styled(fmt_usd(unrealized), money_style(unrealized)));
    spans.push(Span::raw("  cash "));
    spans.push(Span::styled(fmt_usd(cash), money_style(cash)));

    let para = Paragraph::new(Line::from(spans))
        .block(Block::default().borders(Borders::ALL).title(" arb "));
    f.render_widget(para, area);
}

fn draw_footer(f: &mut Frame, area: Rect) {
    let para = Paragraph::new(" [p]ause dispatch  [l]ive  [k]ill  [q]uit  [↑/↓] log scroll ")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(para, area);
}

fn draw_opps(f: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Opportunities ");
    f.render_widget(block, area);
}

fn draw_positions(f: &mut Frame, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Positions ");
    f.render_widget(block, area);
}

fn draw_fills_orders(f: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Fills & Orders ");
    f.render_widget(block, area);
}

fn draw_health(f: &mut Frame, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Health ");
    f.render_widget(block, area);
}

fn draw_log(f: &mut Frame, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Log ");
    f.render_widget(block, area);
}

fn draw_modal(f: &mut Frame, modal: &Modal, full: Rect) {
    match modal {
        Modal::None => {}
        Modal::ConfirmKill => {
            let area = centered_rect(50, 30, full);
            f.render_widget(Clear, area);
            let para = Paragraph::new(
                "Trip the kill switch? Dispatch halts until restart.\n\n[y] confirm   [n/Esc] cancel",
            )
            .block(Block::default().borders(Borders::ALL).title(" kill switch "));
            f.render_widget(para, area);
        }
        Modal::ConfirmLive(typed) => {
            let area = centered_rect(50, 30, full);
            f.render_widget(Clear, area);
            let body = format!(
                "Type 'live' and press Enter to switch venues.\n\n> {typed}\n\n[Esc] cancel"
            );
            let para = Paragraph::new(body)
                .block(Block::default().borders(Borders::ALL).title(" go live "));
            f.render_widget(para, area);
        }
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Render the full dashboard for one frame.
pub fn draw(f: &mut Frame, s: &AppState, ui: &UiState) {
    let full = f.area();

    // Guard against degenerate terminal sizes.
    if full.width < 2 || full.height < 2 {
        return;
    }

    // Outer vertical split: header | upper panels | lower panels | log | footer
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),      // header
            Constraint::Percentage(32), // upper panels
            Constraint::Percentage(28), // lower panels
            Constraint::Min(4),         // log
            Constraint::Length(1),      // footer
        ])
        .split(full);

    let header_area = outer[0];
    let upper_area = outer[1];
    let lower_area = outer[2];
    let log_area = outer[3];
    let footer_area = outer[4];

    // Upper: 62% Opportunities | 38% Positions
    let upper = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(upper_area);

    // Lower: 62% Fills & Orders | 38% Health
    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(lower_area);

    draw_header(f, s, header_area);
    draw_opps(f, upper[0]);
    draw_positions(f, upper[1]);
    draw_fills_orders(f, lower[0]);
    draw_health(f, lower[1]);
    draw_log(f, log_area);
    draw_footer(f, footer_area);

    // Modal overlay (rendered last so it appears on top).
    draw_modal(f, &ui.modal, full);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::state::AppState;
    use crate::ui::UiState;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    /// Flatten the test buffer into one searchable string (lossy join).
    pub(crate) fn render_to_text(state: &AppState, ui: &UiState, w: u16, h: u16) -> String {
        let backend = TestBackend::new(w, h);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw(f, state, ui)).unwrap();
        let buf = term.backend().buffer().clone();
        let area = *buf.area();
        let mut out = String::new();
        for y in 0..area.height {
            for x in 0..area.width {
                out.push_str(buf[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn header_shows_mode_pnl_and_uptime() {
        let mut s = AppState {
            mode_paper: true,
            uptime_s: 125,
            ..Default::default()
        };
        s.equity_usd = 12.34;
        s.equity_mid_usd = 13.0;
        s.realized_usd = 5.99;
        let text = render_to_text(&s, &UiState::default(), 120, 30);
        assert!(text.contains("PAPER"), "mode badge missing:\n{text}");
        assert!(text.contains("02:05"), "uptime mm:ss missing");
        assert!(text.contains("12.34"), "bid equity missing");
        assert!(text.contains("13.00"), "mid equity missing");
        assert!(text.contains("5.99"), "realized missing");
    }

    #[test]
    fn header_status_badges() {
        let mut s = AppState {
            mode_paper: true,
            ..Default::default()
        };
        s.paused = true;
        let text = render_to_text(&s, &UiState::default(), 120, 30);
        assert!(text.contains("PAUSED"));

        s.paused = false;
        s.halted = Some("DailyDrawdown".into());
        let text = render_to_text(&s, &UiState::default(), 120, 30);
        assert!(text.contains("HALT:DailyDrawdown"));

        s.killed = true;
        let text = render_to_text(&s, &UiState::default(), 120, 30);
        assert!(text.contains("KILLED"));
    }

    #[test]
    fn footer_lists_keys() {
        let s = AppState {
            mode_paper: true,
            ..Default::default()
        };
        let text = render_to_text(&s, &UiState::default(), 120, 30);
        for key in ["[p]ause dispatch", "[l]ive", "[k]ill", "[q]uit"] {
            assert!(text.contains(key), "footer key {key} missing:\n{text}");
        }
    }

    #[test]
    fn all_five_panels_have_titles() {
        let s = AppState {
            mode_paper: true,
            ..Default::default()
        };
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        for title in [
            "Opportunities",
            "Positions",
            "Fills & Orders",
            "Health",
            "Log",
        ] {
            assert!(text.contains(title), "panel {title} missing:\n{text}");
        }
    }

    #[test]
    fn tiny_terminal_does_not_panic() {
        let s = AppState::default();
        // Degenerate sizes must render (possibly uselessly) without panicking.
        let _ = render_to_text(&s, &UiState::default(), 10, 5);
        let _ = render_to_text(&s, &UiState::default(), 1, 1);
    }
}
