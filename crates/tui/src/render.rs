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
    let (mode_badge, badge_bg) = if s.mode_paper {
        (" PAPER ", Color::Yellow)
    } else if s.shadow {
        // Shadow signs but never submits — harmless. It takes precedence over
        // LIVE and LIVE·HELD (shadow sessions are always released; never HELD).
        // Cyan = the retired pre-M5 LIVE color; red stays RESERVED for
        // armed-and-released real money so the two are never confused.
        (" SHADOW ", Color::Cyan)
    } else if !s.live_released {
        // Armed but held: latch not yet released — yellow warning chrome.
        (" LIVE\u{00b7}HELD ", Color::Yellow)
    } else {
        // Release confirmed — this is the genuinely dangerous live state.
        (" LIVE ", Color::Red)
    };
    let uptime_str = fmt_uptime(s.uptime_s);

    let mut spans: Vec<Span> = vec![
        Span::styled(
            mode_badge,
            Style::default()
                .fg(Color::Black)
                .bg(badge_bg)
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
    let para = Paragraph::new(
        " [p]ause  [l]ive  [k]ill  [q]uit  [↑/↓] log  [Tab] order  [x] cancel/un-veto ",
    )
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
                // C4Lp baskets have non-uniform legs: units is Qty(0) by the
                // engine contract, so a numeric size would be a lie.
                Cell::from(if o.class == "C4Lp" {
                    "—".to_string()
                } else {
                    format!("{:.0}", o.size_shares)
                }),
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
    let header = Row::new(["strat", "market", "qty", "basis $", "mark $"]).style(header_style);

    let rows: Vec<Row> = s
        .positions
        .iter()
        .map(|p| {
            // qty carries its sign (negative = short). The P&L color compares
            // signed mark vs signed basis, which is correct for both sides: a
            // short profits when its (negative) mark rises toward its (negative)
            // basis, i.e. `mark - basis > 0`.
            Row::new([
                Cell::from(p.strategy.clone()),
                Cell::from(p.market.clone()),
                Cell::from(format!("{:.0}", p.qty_shares)),
                Cell::from(fmt_usd(p.basis_usd)),
                Cell::from(fmt_usd(p.mark_usd)).style(money_style(p.mark_usd - p.basis_usd)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(5),
        Constraint::Min(14),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(9),
    ];

    let block = Block::default().borders(Borders::ALL).title(" Positions ");
    let table = Table::new(rows, widths).header(header).block(block);
    f.render_widget(table, area);
}

fn draw_fills_orders(f: &mut Frame, s: &AppState, area: Rect, selected: usize) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Fills (top) · Open Orders (bottom: Tab select, x cancel) ");
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
        let header = Row::new(["age", "strat", "market", "side", "px", "qty", "cash $"])
            .style(header_style);
        let rows: Vec<Row> = s
            .fills
            .iter()
            .map(|fi| {
                Row::new([
                    Cell::from(format!("{}s", fi.ago_s)),
                    Cell::from(fi.strategy.clone()),
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
            Constraint::Length(5),
            Constraint::Min(14),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(7),
            Constraint::Length(9),
        ];
        let table = Table::new(rows, widths).header(header);
        f.render_widget(table, splits[0]);
    }

    // Open-orders table: LIVE resting quotes + VETOED slots. The highlighted row
    // (`>` marker + reversed style) is the one j/k selects and `x` cancels /
    // un-vetoes. Vetoed rows are dimmed and show no live price/qty.
    {
        let header_style = Style::default().add_modifier(Modifier::BOLD);
        let header =
            Row::new(["", "strat", "market", "side", "px", "qty", "status"]).style(header_style);
        let sel = selected.min(s.open_orders.len().saturating_sub(1));
        let rows: Vec<Row> = s
            .open_orders
            .iter()
            .enumerate()
            .map(|(i, o)| {
                let selected_row = i == sel;
                let row = Row::new([
                    Cell::from(if selected_row { ">" } else { " " }),
                    Cell::from(o.strategy.clone()),
                    Cell::from(o.market.clone()),
                    Cell::from(o.side.clone()),
                    Cell::from(o.px.clone()),
                    Cell::from(format!("{:.0}", o.qty_shares)),
                    Cell::from(if o.vetoed { "VETOED" } else { "Resting" }),
                ]);
                if selected_row {
                    row.style(Style::default().add_modifier(Modifier::REVERSED))
                } else if o.vetoed {
                    row.style(Style::default().add_modifier(Modifier::DIM))
                } else {
                    row
                }
            })
            .collect();
        let widths = [
            Constraint::Length(1),
            Constraint::Length(5),
            Constraint::Min(12),
            Constraint::Length(4),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(8),
        ];
        let table = Table::new(rows, widths).header(header);
        f.render_widget(table, splits[1]);
    }
}

fn draw_health(f: &mut Frame, s: &AppState, area: Rect) {
    let h = &s.health;
    let ws_label = if h.ws_connected { "WS up" } else { "WS DOWN" };
    let mut text = format!(
        "{ws_label}  feeds {feeds_up}/{feeds_total}  oldest {oldest}s  books {books}  stale {stale}  reconnects {reconnects}\n\
         frames {frames}  {fps:.1}/s  parse_err {parse_err}\n\
         detect µs p50/p99 {dp50}/{dp99}\n\
         dispatch µs p50/p99 {disp50}/{disp99}\n\
         opps {opps}  admitted {admitted}  dispatched {dispatched}\n\
         baskets clean {clean} repaired {repaired} unwound {unwound}\n\
         solver queue {queue}  solved {solved}",
        feeds_up = h.feeds_up,
        feeds_total = h.feeds_total,
        oldest = h.oldest_frame_age_s,
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
    if !s.mode_paper {
        text.push_str(&format!("\nlive_rej {}  held {}", h.live_rej, h.live_held));
    }
    // Per-strategy breakdown (multi-strategy platform): one compact line each,
    // shown only when the publisher is fed the host's aggregated view. Empty in
    // single-strategy / CoordStatus sessions, so the panel is otherwise unchanged.
    for line in &s.per_strategy {
        text.push_str(&format!(
            "\n{} eq {} cash {} pos {} rlzd {} unrl {}",
            line.id,
            fmt_usd(line.equity_usd),
            fmt_usd(line.cash_usd),
            line.open_positions,
            fmt_usd(line.realized_usd),
            fmt_usd(line.unrealized_usd),
        ));
        if let Some(reason) = &line.halted {
            text.push_str(&format!(" HALT:{reason}"));
        } else if line.paused {
            text.push_str(" PAUSED");
        }
    }
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
    draw_fills_orders(f, s, lower[0], ui.open_orders_selected);
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

    /// Background color of the first cell of `label` in the rendered header.
    /// Returns None if the label isn't found. Used to assert badge styling
    /// (e.g. SHADOW must not be red).
    pub(crate) fn badge_bg_color(
        state: &AppState,
        label: &str,
        w: u16,
        h: u16,
    ) -> Option<Color> {
        let backend = TestBackend::new(w, h);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw(f, state, &UiState::default())).unwrap();
        let buf = term.backend().buffer().clone();
        let area = *buf.area();
        let first = label.chars().next()?;
        for y in 0..area.height {
            for x in 0..area.width {
                if buf[(x, y)].symbol() != first.to_string() {
                    continue;
                }
                // Candidate start: does the run of cells from here spell `label`?
                let mut run = String::new();
                let mut xx = x;
                while xx < area.width && run.len() < label.len() {
                    run.push_str(buf[(xx, y)].symbol());
                    xx += 1;
                }
                if run.starts_with(label) {
                    return Some(buf[(x, y)].style().bg.unwrap_or(Color::Reset));
                }
            }
        }
        None
    }

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
        for key in ["[p]ause", "[l]ive", "[k]ill", "[q]uit", "[Tab] order", "[x] cancel"] {
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
            "Open Orders",
            "Health",
            "Log",
        ] {
            assert!(text.contains(title), "panel {title} missing:\n{text}");
        }
    }

    #[test]
    fn c4lp_size_renders_as_dash_not_zero() {
        // Engine contract: Opportunity.units is Qty(0) for C4Lp (non-uniform
        // legs; per-leg qtys are authoritative), so a literal "0" in the size
        // column is misleading next to a non-zero est $. Render a dash.
        let s = AppState {
            mode_paper: true,
            opportunities: vec![
                crate::state::OppLine {
                    age_s: 1,
                    class: "C4Lp".into(),
                    market: "Will X win?".into(),
                    edge_bps: 6695,
                    size_shares: 0.0,
                    est_profit_usd: 669.55,
                    dispatched: false,
                },
                crate::state::OppLine {
                    age_s: 2,
                    class: "C1Long".into(),
                    market: "Will Y win?".into(),
                    edge_bps: 637,
                    size_shares: 100.0,
                    est_profit_usd: 5.99,
                    dispatched: false,
                },
            ],
            ..Default::default()
        };
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        let Some(c4_row) = text.lines().find(|l| l.contains("C4Lp")) else {
            panic!("C4Lp row missing:\n{text}");
        };
        assert!(
            c4_row.contains('—'),
            "C4Lp size must render as a dash:\n{c4_row}"
        );
        let Some(c1_row) = text.lines().find(|l| l.contains("C1Long")) else {
            panic!("C1Long row missing:\n{text}");
        };
        assert!(
            c1_row.contains("100"),
            "uniform-class size must stay numeric:\n{c1_row}"
        );
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
                strategy: "arb".into(),
                market: "Will Y win? YES".into(),
                qty_shares: 100.0,
                basis_usd: 44.0,
                mark_usd: 42.0,
            }],
            fills: vec![crate::state::FillLine {
                ago_s: 5,
                strategy: "arb".into(),
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
                feeds_up: 2,
                feeds_total: 2,
                oldest_frame_age_s: 0,
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
                live_rej: 0,
                live_held: 0,
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
        for needle in ["strat", "Will Y win? YES", "44.00", "42.00"] {
            assert!(text.contains(needle), "positions missing {needle}");
        }
        // The leading strat column tags the row with its owning strategy.
        let row = text
            .lines()
            .find(|l| l.contains("Will Y win? YES"))
            .unwrap();
        assert!(row.contains("arb"), "position row must show its strategy:\n{row}");
    }

    #[test]
    fn positions_panel_renders_short_with_signed_qty() {
        // A short shows a NEGATIVE qty, its mm tag, and a negative (liability)
        // mark — the operator can watch the market maker carry inventory.
        let mut s = sample_state();
        s.positions = vec![crate::state::PositionLine {
            strategy: "mm".into(),
            market: "Will Z win? NO".into(),
            qty_shares: -5.0,
            basis_usd: -2.00, // received $2 opening the short
            mark_usd: -2.10,  // costs $2.10 to buy back → underwater
        }];
        let text = render_to_text(&s, &UiState::default(), 160, 45);
        let row = text
            .lines()
            .find(|l| l.contains("Will Z win? NO"))
            .unwrap();
        assert!(row.contains("mm"), "short row must show the mm strategy:\n{row}");
        assert!(row.contains("-5"), "short row must show a signed (negative) qty:\n{row}");
        assert!(row.contains("-2.10"), "short row must show its (negative) mark:\n{row}");
    }

    #[test]
    fn fills_and_orders_render() {
        let mut s = sample_state();
        // A LIVE resting quote + a VETOED slot for the open-orders sub-panel.
        s.open_orders = vec![
            crate::state::OpenOrderLine {
                strategy: "mm".into(),
                market: "Will Q win? YES".into(),
                side: "Bid".into(),
                px: "0.40".into(),
                qty_shares: 25.0,
                vetoed: false,
                key: "111:b".into(),
            },
            crate::state::OpenOrderLine {
                strategy: "mm".into(),
                market: "Will R win? NO".into(),
                side: "Ask".into(),
                px: "—".into(),
                qty_shares: 0.0,
                vetoed: true,
                key: "222:a".into(),
            },
        ];
        let text = render_to_text(&s, &UiState::default(), 160, 45);
        // Fills (top sub-panel).
        for needle in ["strat", "0.44", "-44.00"] {
            assert!(text.contains(needle), "fills missing {needle}:\n{text}");
        }
        // Open orders (bottom sub-panel): a live resting quote + a VETOED slot.
        for needle in ["Bid", "Resting", "VETOED"] {
            assert!(text.contains(needle), "open-orders missing {needle}:\n{text}");
        }
        // The strat column tags each fill with the strategy that traded it.
        let fill_row = text.lines().find(|l| l.contains("-44.00")).unwrap();
        assert!(fill_row.contains("arb"), "fill row must show its strategy:\n{fill_row}");
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
        let mut ui = UiState {
            log_scroll: 5,
            ..Default::default()
        };
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
    fn health_line_shows_feed_counts_and_oldest_age() {
        let mut s = AppState {
            mode_paper: true,
            ..Default::default()
        };
        s.health.feeds_up = 1;
        s.health.feeds_total = 2;
        s.health.oldest_frame_age_s = 30;
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        assert!(text.contains("feeds 1/2"), "feeds count missing:\n{text}");
        assert!(text.contains("oldest 30s"), "oldest frame age missing:\n{text}");
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

    #[test]
    fn live_badge_shows_held_until_released() {
        let mut s = AppState { mode_paper: false, live_released: false, ..Default::default() };
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        assert!(text.contains("LIVE·HELD"), "armed-but-held badge:\n{text}");
        s.live_released = true;
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        assert!(text.contains("LIVE") && !text.contains("LIVE·HELD"));
    }

    #[test]
    fn shadow_badge_is_distinct_and_not_red() {
        // --live --shadow: signs but never submits. Must NOT wear the red LIVE
        // badge of a funded session, and must take precedence over LIVE·HELD
        // (shadow sessions are always released — never render HELD).
        let s = AppState {
            mode_paper: false,
            shadow: true,
            live_released: true,
            ..Default::default()
        };
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        assert!(text.contains("SHADOW"), "shadow badge missing:\n{text}");
        // "SHADOW" does not contain "LIVE", so this also rules out standalone
        // LIVE and LIVE·HELD bleeding through.
        assert!(!text.contains("LIVE"), "shadow must not render any LIVE badge:\n{text}");
        // The SHADOW glyph cells must not be painted red (red is reserved for
        // armed-and-released real money).
        let bg = badge_bg_color(&s, "SHADOW", 140, 40);
        assert_ne!(bg, Some(Color::Red), "SHADOW badge must not use a red background");

        // Even pre-release (held latch) a shadow session shows SHADOW, never HELD.
        let s_held = AppState { live_released: false, ..s };
        let text = render_to_text(&s_held, &UiState::default(), 140, 40);
        assert!(text.contains("SHADOW"), "shadow precedence over HELD:\n{text}");
        assert!(!text.contains("LIVE"), "no LIVE·HELD under shadow:\n{text}");
    }

    #[test]
    fn health_panel_shows_live_counters_in_live_mode() {
        let mut s = AppState { mode_paper: false, ..Default::default() };
        s.health.live_rej = 3;
        s.health.live_held = 2;
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        assert!(text.contains("live_rej 3"), "{text}");
        assert!(text.contains("held 2"), "{text}");
        let p = AppState { mode_paper: true, ..Default::default() };
        let text = render_to_text(&p, &UiState::default(), 140, 40);
        assert!(!text.contains("live_rej"), "no dead chrome in paper mode");
    }

    #[test]
    fn health_panel_shows_per_strategy_breakdown() {
        // Multi-strategy platform: when the aggregated view drives the
        // publisher, the Health panel gains one compact line per strategy
        // (id + display-only money + open-position count + paused/halt flag).
        // Empty otherwise. Rendered wide (200 cols) so the mm line's full
        // HALT suffix is not clipped by the Health panel's width.
        let mut s = sample_state();
        s.per_strategy = vec![
            crate::state::StrategyLine {
                id: "arb".into(),
                equity_usd: 7.00,
                cash_usd: 1.00,
                realized_usd: 2.00,
                unrealized_usd: -0.50,
                open_positions: 3,
                paused: true,
                halted: None,
            },
            crate::state::StrategyLine {
                id: "mm".into(),
                equity_usd: 3.00,
                cash_usd: 0.00,
                realized_usd: 0.00,
                unrealized_usd: 0.00,
                open_positions: 1,
                paused: false,
                halted: Some("DailyDrawdown".into()),
            },
        ];
        let text = render_to_text(&s, &UiState::default(), 200, 60);
        // "eq 7.00" is unique to the per-strategy line ("arb" alone collides
        // with the header title), proving the breakdown rendered.
        assert!(text.contains("eq 7.00"), "arb per-strategy line missing:\n{text}");
        assert!(text.contains("mm eq 3.00"), "mm per-strategy line missing:\n{text}");
        // The enriched fields surface: open-position count, realized, unrealized.
        // "pos 3"/"rlzd 2.00"/"unrl -0.50" are unique to the arb breakdown line.
        assert!(text.contains("pos 3"), "arb open-position count missing:\n{text}");
        assert!(text.contains("rlzd 2.00"), "arb realized missing:\n{text}");
        assert!(text.contains("unrl -0.50"), "arb unrealized missing:\n{text}");
        assert!(text.contains("pos 1"), "mm open-position count missing:\n{text}");
        // Per-strategy control flags surface (header is neither paused nor halted
        // in sample_state, so these come only from the breakdown lines).
        assert!(text.contains("PAUSED"), "arb paused flag missing:\n{text}");
        assert!(
            text.contains("HALT:DailyDrawdown"),
            "mm halt flag missing:\n{text}"
        );

        // No per_strategy ⇒ no breakdown lines (unchanged single-strategy panel).
        let bare = render_to_text(&sample_state(), &UiState::default(), 200, 60);
        assert!(!bare.contains("eq 7.00"), "breakdown must be empty without per_strategy");
    }

    #[test]
    fn session_loss_halt_badge_renders() {
        let s = AppState { mode_paper: false, live_released: true, halted: Some("SessionLoss".into()), ..Default::default() };
        let text = render_to_text(&s, &UiState::default(), 140, 40);
        assert!(text.contains("SessionLoss"), "{text}");
    }
}
