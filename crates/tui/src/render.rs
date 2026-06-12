//! Dashboard rendering. Calls `draw(f, state, ui)` from the main event loop.

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table},
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

fn draw_opps(f: &mut Frame, s: &AppState, area: Rect) {
    let header_style = Style::default().add_modifier(Modifier::BOLD);
    let header =
        Row::new(["age", "class", "market", "edge", "size", "est $", ""]).style(header_style);

    let rows: Vec<Row> = s
        .opportunities
        .iter()
        .map(|o| {
            Row::new([
                Cell::from(format!("{}s", o.age_s)),
                Cell::from(o.class.clone()),
                Cell::from(o.market.clone()),
                Cell::from(o.edge_bps.to_string()).style(money_style(o.edge_bps as f64)),
                Cell::from(format!("{:.0}", o.size_shares)),
                Cell::from(fmt_usd(o.est_profit_usd)).style(money_style(o.est_profit_usd)),
                Cell::from(if o.dispatched { "*" } else { "" }),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(5),
        Constraint::Length(8),
        Constraint::Min(20),
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(2),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Opportunities ");
    let table = Table::new(rows, widths).header(header).block(block);
    f.render_widget(table, area);
}

fn draw_positions(f: &mut Frame, s: &AppState, area: Rect) {
    let header_style = Style::default().add_modifier(Modifier::BOLD);
    let header = Row::new(["market", "qty", "basis $", "mark $"]).style(header_style);

    let rows: Vec<Row> = s
        .positions
        .iter()
        .map(|p| {
            Row::new([
                Cell::from(p.market.clone()),
                Cell::from(format!("{:.0}", p.qty_shares)),
                Cell::from(fmt_usd(p.basis_usd)),
                Cell::from(fmt_usd(p.mark_usd)).style(money_style(p.mark_usd - p.basis_usd)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(16),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(9),
    ];

    let block = Block::default().borders(Borders::ALL).title(" Positions ");
    let table = Table::new(rows, widths).header(header).block(block);
    f.render_widget(table, area);
}

fn draw_fills_orders(f: &mut Frame, s: &AppState, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Fills & Orders ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    // Split inner area 60 / 40 vertically
    let splits = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(inner);

    // Fills table
    {
        let header_style = Style::default().add_modifier(Modifier::BOLD);
        let header = Row::new(["age", "market", "side", "px", "qty", "cash $"]).style(header_style);
        let rows: Vec<Row> = s
            .fills
            .iter()
            .map(|fi| {
                Row::new([
                    Cell::from(format!("{}s", fi.ago_s)),
                    Cell::from(fi.market.clone()),
                    Cell::from(fi.action.clone()),
                    Cell::from(fi.px.clone()),
                    Cell::from(format!("{:.0}", fi.qty_shares)),
                    Cell::from(fmt_usd(fi.cash_usd)).style(money_style(fi.cash_usd)),
                ])
            })
            .collect();
        let widths = [
            Constraint::Length(5),
            Constraint::Min(16),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(7),
            Constraint::Length(9),
        ];
        let table = Table::new(rows, widths).header(header);
        f.render_widget(table, splits[0]);
    }

    // Orders table
    {
        let header_style = Style::default().add_modifier(Modifier::BOLD);
        let header = Row::new(["age", "order", "state", "detail"]).style(header_style);
        let rows: Vec<Row> = s
            .orders
            .iter()
            .map(|o| {
                Row::new([
                    Cell::from(format!("{}s", o.ago_s)),
                    Cell::from(o.order_id_short.clone()),
                    Cell::from(o.state.clone()),
                    Cell::from(o.detail.clone()),
                ])
            })
            .collect();
        let widths = [
            Constraint::Length(5),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Min(10),
        ];
        let table = Table::new(rows, widths).header(header);
        f.render_widget(table, splits[1]);
    }
}

fn draw_health(f: &mut Frame, s: &AppState, area: Rect) {
    let h = &s.health;
    let ws_label = if h.ws_connected { "WS up" } else { "WS DOWN" };
    let text = format!(
        "{ws_label}  books {books}  stale {stale}  reconnects {reconnects}\n\
         frames {frames}  {fps:.1}/s  parse_err {parse_err}\n\
         detect µs p50/p99 {dp50}/{dp99}\n\
         dispatch µs p50/p99 {disp50}/{disp99}\n\
         opps {opps}  admitted {admitted}  dispatched {dispatched}\n\
         baskets clean {clean} repaired {repaired} unwound {unwound}\n\
         solver queue {queue}  solved {solved}",
        books = h.books,
        stale = h.stale,
        reconnects = h.reconnects,
        frames = h.frames,
        fps = h.frames_per_s,
        parse_err = h.parse_errors,
        dp50 = h.detect_p50_us,
        dp99 = h.detect_p99_us,
        disp50 = h.dispatch_p50_us,
        disp99 = h.dispatch_p99_us,
        opps = h.opps_emitted,
        admitted = h.admitted,
        dispatched = h.dispatched,
        clean = h.baskets_clean,
        repaired = h.baskets_repaired,
        unwound = h.baskets_unwound,
        queue = h.solver_queue,
        solved = h.lp_solved,
    );
    let block = Block::default().borders(Borders::ALL).title(" Health ");
    let para = Paragraph::new(text).block(block);
    f.render_widget(para, area);
}

fn draw_log(f: &mut Frame, s: &AppState, ui: &UiState, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Log ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let inner_h = inner.height as usize;
    if inner_h == 0 || s.log.is_empty() {
        return;
    }

    // Tail-follow with scroll: visible window = last inner_h lines offset back by log_scroll.
    let total = s.log.len();
    // scroll offset (from the tail): clamped so we can't scroll past the top
    let scroll = (ui.log_scroll as usize).min(total.saturating_sub(inner_h));
    // end index = total - scroll (exclusive upper bound)
    let end = total.saturating_sub(scroll);
    // start index = end - visible lines (clamped to 0)
    let start = end.saturating_sub(inner_h);

    let lines: Vec<Line> = s.log[start..end]
        .iter()
        .map(|(lvl, msg)| {
            let style = match lvl {
                1 => Style::default().fg(Color::Magenta),
                2 => Style::default().fg(Color::Yellow),
                _ => Style::default(),
            };
            Line::styled(msg.clone(), style)
        })
        .collect();

    let para = Paragraph::new(lines);
    f.render_widget(para, inner);
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
    draw_opps(f, s, upper[0]);
    draw_positions(f, s, upper[1]);
    draw_fills_orders(f, s, lower[0]);
    draw_health(f, s, lower[1]);
    draw_log(f, s, ui, log_area);
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

    fn sample_state() -> AppState {
        AppState {
            mode_paper: true,
            uptime_s: 60,
            cash_usd: 5.99,
            equity_usd: 5.99,
            equity_mid_usd: 6.10,
            realized_usd: 5.99,
            unrealized_usd: 0.0,
            opportunities: vec![crate::state::OppLine {
                age_s: 3,
                class: "C1Long".into(),
                market: "Will X win?".into(),
                edge_bps: 637,
                size_shares: 100.0,
                est_profit_usd: 5.99,
                dispatched: true,
            }],
            positions: vec![crate::state::PositionLine {
                market: "Will Y win? YES".into(),
                qty_shares: 100.0,
                basis_usd: 44.0,
                mark_usd: 42.0,
            }],
            fills: vec![crate::state::FillLine {
                ago_s: 5,
                market: "Will X win?".into(),
                action: "Buy".into(),
                px: "0.44".into(),
                qty_shares: 100.0,
                cash_usd: -44.0,
            }],
            orders: vec![crate::state::OrderLine {
                ago_s: 5,
                order_id_short: "0192abcd".into(),
                state: "Filled".into(),
                detail: String::new(),
            }],
            health: crate::state::Health {
                ws_connected: true,
                books: 400,
                stale: 0,
                frames: 255_800,
                frames_per_s: 110.4,
                reconnects: 0,
                parse_errors: 0,
                detect_p50_us: 34,
                detect_p99_us: 377,
                dispatch_p50_us: 1976,
                dispatch_p99_us: 6583,
                opps_emitted: 7912,
                admitted: 7845,
                dispatched: 8,
                baskets_clean: 6,
                baskets_repaired: 1,
                baskets_unwound: 1,
                solver_queue: 2,
                lp_solved: 311,
            },
            log: vec![
                (2, "WARN risk halt: DailyDrawdown".into()),
                (3, "INFO universe assembled".into()),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn opportunity_feed_renders_rows() {
        let text = render_to_text(&sample_state(), &UiState::default(), 160, 45);
        for needle in ["C1Long", "Will X win?", "637", "100", "5.99", "3s"] {
            assert!(text.contains(needle), "feed missing {needle}:\n{text}");
        }
        assert!(text.contains('*'), "dispatched marker missing");
    }

    #[test]
    fn positions_panel_renders_holdings() {
        let text = render_to_text(&sample_state(), &UiState::default(), 160, 45);
        for needle in ["Will Y win? YES", "44.00", "42.00"] {
            assert!(text.contains(needle), "positions missing {needle}");
        }
    }

    #[test]
    fn fills_and_orders_render() {
        let text = render_to_text(&sample_state(), &UiState::default(), 160, 45);
        for needle in ["0.44", "-44.00", "0192abcd", "Filled"] {
            assert!(text.contains(needle), "fills/orders missing {needle}");
        }
    }

    #[test]
    fn health_panel_renders_gauges_and_latency() {
        let text = render_to_text(&sample_state(), &UiState::default(), 160, 45);
        for needle in [
            "books 400",
            "stale 0",
            "110.4/s",
            "p50/p99 34/377",
            "1976/6583",
            "queue 2",
            "WS up",
        ] {
            assert!(text.contains(needle), "health missing {needle}:\n{text}");
        }
    }

    #[test]
    fn log_panel_renders_tail() {
        let text = render_to_text(&sample_state(), &UiState::default(), 160, 45);
        assert!(text.contains("DailyDrawdown"));
        assert!(text.contains("universe assembled"));
    }

    #[test]
    fn modal_overlays_render() {
        let mut ui = UiState::default();
        ui.modal = crate::ui::Modal::ConfirmKill;
        let text = render_to_text(&sample_state(), &ui, 120, 35);
        assert!(text.contains("Trip the kill switch?"));

        ui.modal = crate::ui::Modal::ConfirmLive("li".into());
        let text = render_to_text(&sample_state(), &ui, 120, 35);
        assert!(text.contains("Type 'live'"));
        assert!(text.contains("> li"));
    }

    #[test]
    fn log_scroll_shows_earlier_lines_with_full_window() {
        let mut s = sample_state();
        s.log = (0..30)
            .map(|i| (3u8, format!("INFO line-{i:02}")))
            .collect();
        // unscrolled: tail visible
        let text = render_to_text(&s, &UiState::default(), 160, 45);
        assert!(text.contains("line-29"));
        // scrolled back 5: tail hidden, earlier lines visible
        // At 160×45 the log panel inner_h = 13. scroll=5 → end=25, start=12 → lines 12..24.
        let mut ui = UiState::default();
        ui.log_scroll = 5;
        let text = render_to_text(&s, &ui, 160, 45);
        assert!(
            !text.contains("line-29"),
            "scrolled view must hide the tail"
        );
        assert!(text.contains("line-24"));
        // scroll far beyond the top: clamps, still renders the earliest lines, no panic
        ui.log_scroll = 500;
        let text = render_to_text(&s, &ui, 160, 45);
        assert!(text.contains("line-00"));
    }

    #[test]
    fn ws_down_renders() {
        let mut s = sample_state();
        s.health.ws_connected = false;
        let text = render_to_text(&s, &UiState::default(), 160, 45);
        assert!(text.contains("WS DOWN"));
    }

    #[test]
    fn standard_80x24_renders_without_panic() {
        let _ = render_to_text(&sample_state(), &UiState::default(), 80, 24);
    }
}
