use std::io::{self, Stdout};

use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::cursor::{Hide, Show};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, Padding, Paragraph, Wrap};
use std::time::Duration;

const PANEL_LEFT_PAD: &str = "  ";
const BORDER_COLOR: Color = Color::Cyan;
const ROW_SEPARATOR_COLOR: Color = Color::Rgb(90, 90, 90);
const SUMMARY_TOTALS_WIDTH: u16 = 58;
const SUMMARY_RATES_WIDTH: u16 = 32;

#[derive(Debug, Clone, Default)]
pub(super) struct TuiSnapshot {
    pub connection_state: String,
    pub endpoint: String,
    pub reconnect_ms: u64,
    pub uptime_secs: u64,
    pub total_updates: u64,
    pub dlmm_updates: u64,
    pub dlmm_updates_ok: u64,
    pub dlmm_updates_failed: u64,
    pub parsed_instructions: u64,
    pub failed_instructions: u64,
    pub updates_per_sec: f64,
    pub dlmm_updates_per_sec: f64,
    pub parsed_instr_per_sec: f64,
    pub failed_instr_per_sec: f64,
    pub avg_updates_per_sec: f64,
    pub db_enqueued: u64,
    pub db_dropped: u64,
    pub db_disconnected: u64,
    pub unknown_total: u64,
    pub failed_total: u64,
    pub warning_total: u64,
    pub parsed_bars: Vec<(String, u64)>,
    pub updates_rate_history: Vec<u64>,
    pub dlmm_rate_history: Vec<u64>,
    pub parsed_rate_history: Vec<u64>,
    pub failed_rate_history: Vec<u64>,
    pub unknown_lines: Vec<String>,
    pub failed_lines: Vec<String>,
    pub warning_lines: Vec<String>,
}

pub(super) struct IndexerTui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    parsed_scroll: u16,
}

impl IndexerTui {
    pub(super) fn new() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, Hide)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        terminal.clear()?;
        Ok(Self {
            terminal,
            parsed_scroll: 0,
        })
    }

    pub(super) fn draw(&mut self, snapshot: &TuiSnapshot) -> io::Result<()> {
        self.terminal.draw(|frame| {
            let area = frame.area();
            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(9),
                    Constraint::Min(12),
                    Constraint::Length(10),
                ])
                .split(area);

            let header = Paragraph::new(vec![
                Line::from(vec![
                    Span::raw(PANEL_LEFT_PAD),
                    Span::styled(
                        "DUNE INDEXER TUI",
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(format!(
                    "{PANEL_LEFT_PAD}state={}  uptime={}s  reconnect={}ms  endpoint={}",
                    snapshot.connection_state,
                    snapshot.uptime_secs,
                    snapshot.reconnect_ms,
                    snapshot.endpoint
                )),
            ])
            .block(panel_block("STATUS"));
            frame.render_widget(header, rows[0]);

            let summary_cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(SUMMARY_TOTALS_WIDTH),
                    Constraint::Length(SUMMARY_RATES_WIDTH),
                    Constraint::Min(30),
                ])
                .split(rows[1]);

            frame.render_widget(totals_widget(snapshot), summary_cols[0]);
            frame.render_widget(rates_widget(snapshot), summary_cols[1]);
            render_quality(frame, summary_cols[2], snapshot);

            let middle_cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(72), Constraint::Percentage(28)])
                .split(rows[2]);

            let parsed_lines = parsed_distribution_lines(middle_cols[0], &snapshot.parsed_bars);
            let visible_lines = middle_cols[0].height.saturating_sub(2) as usize;
            let max_scroll = parsed_lines
                .len()
                .saturating_sub(visible_lines)
                .min(u16::MAX as usize) as u16;
            if self.parsed_scroll > max_scroll {
                self.parsed_scroll = max_scroll;
            }
            let parsed_panel = Paragraph::new(parsed_lines)
                .block(panel_block("PARSED DISCRIBUTOR MIX  (↑/↓ PgUp/PgDn Home)"))
                .scroll((self.parsed_scroll, 0));
            frame.render_widget(parsed_panel, middle_cols[0]);

            let anomaly_rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                ])
                .split(middle_cols[1]);

            frame.render_widget(
                anomaly_panel(
                    "UNKNOWN DISCR",
                    snapshot.unknown_total,
                    &snapshot.unknown_lines,
                ),
                anomaly_rows[0],
            );
            frame.render_widget(
                anomaly_panel(
                    "FAILED PARSE",
                    snapshot.failed_total,
                    &snapshot.failed_lines,
                ),
                anomaly_rows[1],
            );
            frame.render_widget(
                anomaly_panel("WARNINGS", snapshot.warning_total, &snapshot.warning_lines),
                anomaly_rows[2],
            );

            let trend_cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                ])
                .split(rows[3]);

            render_sparkline_panel(
                frame,
                trend_cols[0],
                "UPDATES / S",
                snapshot.updates_per_sec,
                &snapshot.updates_rate_history,
            );
            render_sparkline_panel(
                frame,
                trend_cols[1],
                "DLMM / S",
                snapshot.dlmm_updates_per_sec,
                &snapshot.dlmm_rate_history,
            );
            render_sparkline_panel(
                frame,
                trend_cols[2],
                "PARSED / S",
                snapshot.parsed_instr_per_sec,
                &snapshot.parsed_rate_history,
            );
            render_sparkline_panel(
                frame,
                trend_cols[3],
                "FAILED / S",
                snapshot.failed_instr_per_sec,
                &snapshot.failed_rate_history,
            );
        })?;
        Ok(())
    }

    pub(super) fn should_quit(&mut self) -> io::Result<bool> {
        while event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                let is_ctrl_c = key.modifiers.contains(KeyModifiers::CONTROL)
                    && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'));
                let is_quit_key = matches!(key.code, KeyCode::Char('q') | KeyCode::Esc);
                if is_ctrl_c || is_quit_key {
                    return Ok(true);
                }
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        self.parsed_scroll = self.parsed_scroll.saturating_sub(1);
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.parsed_scroll = self.parsed_scroll.saturating_add(1);
                    }
                    KeyCode::PageUp => {
                        self.parsed_scroll = self.parsed_scroll.saturating_sub(10);
                    }
                    KeyCode::PageDown => {
                        self.parsed_scroll = self.parsed_scroll.saturating_add(10);
                    }
                    KeyCode::Home => {
                        self.parsed_scroll = 0;
                    }
                    _ => {}
                }
            }
        }
        Ok(false)
    }
}

impl Drop for IndexerTui {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), Show, LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

fn panel_block<'a, T>(title: T) -> Block<'a>
where
    T: Into<Line<'a>>,
{
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER_COLOR))
        .title_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .title(title)
}

fn totals_widget(snapshot: &TuiSnapshot) -> Paragraph<'static> {
    let label_style = Style::default().fg(Color::White);
    let value_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let make_line =
        |left_name: &'static str, left_val: u64, right_name: &'static str, right_val: u64| {
            Line::from(vec![
                Span::raw(PANEL_LEFT_PAD),
                Span::styled(format!("{left_name:<12}"), label_style),
                Span::styled(format!("{left_val:>10}"), value_style),
                Span::raw("      "),
                Span::styled(format!("{right_name:<12}"), label_style),
                Span::styled(format!("{right_val:>10}"), value_style),
            ])
        };

    Paragraph::new(vec![
        make_line(
            "updates",
            snapshot.total_updates,
            "dlmm",
            snapshot.dlmm_updates,
        ),
        make_line(
            "ok",
            snapshot.dlmm_updates_ok,
            "failed",
            snapshot.dlmm_updates_failed,
        ),
        make_line(
            "no_dlmm",
            snapshot.total_updates.saturating_sub(snapshot.dlmm_updates),
            "parsed",
            snapshot.parsed_instructions,
        ),
        make_line(
            "failed_instr",
            snapshot.failed_instructions,
            "db_enq",
            snapshot.db_enqueued,
        ),
        make_line(
            "db_drop",
            snapshot.db_dropped,
            "db_disc",
            snapshot.db_disconnected,
        ),
    ])
    .block(panel_block("TOTALS").padding(Padding::new(1, 1, 1, 1)))
}

fn rates_widget(snapshot: &TuiSnapshot) -> Paragraph<'static> {
    let label_style = Style::default().fg(Color::White);
    let value_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let make_rate_line = |name: &'static str, value: String| {
        Line::from(vec![
            Span::raw(PANEL_LEFT_PAD),
            Span::styled(format!("{name:<10}"), label_style),
            Span::raw(" "),
            Span::styled(format!("{value:>12}"), value_style),
        ])
    };

    Paragraph::new(vec![
        make_rate_line("updates/s", format!("{:.2}", snapshot.updates_per_sec)),
        make_rate_line("dlmm/s", format!("{:.2}", snapshot.dlmm_updates_per_sec)),
        make_rate_line("parsed/s", format!("{:.2}", snapshot.parsed_instr_per_sec)),
        make_rate_line("failed/s", format!("{:.4}", snapshot.failed_instr_per_sec)),
        make_rate_line("avg/s", format!("{:.2}", snapshot.avg_updates_per_sec)),
    ])
    .block(panel_block("RATES").padding(Padding::new(1, 1, 1, 1)))
}

fn render_quality(frame: &mut ratatui::Frame<'_>, area: Rect, snapshot: &TuiSnapshot) {
    let total_instr = snapshot
        .parsed_instructions
        .saturating_add(snapshot.failed_instructions);
    let parse_ok_ratio = if total_instr == 0 {
        1.0
    } else {
        snapshot.parsed_instructions as f64 / total_instr as f64
    };
    let db_ok_ratio = if snapshot.db_enqueued == 0 {
        1.0
    } else {
        let bad = snapshot
            .db_dropped
            .saturating_add(snapshot.db_disconnected)
            .min(snapshot.db_enqueued);
        (snapshot.db_enqueued.saturating_sub(bad)) as f64 / snapshot.db_enqueued as f64
    };
    let anomaly_total = snapshot
        .unknown_total
        .saturating_add(snapshot.failed_total)
        .saturating_add(snapshot.warning_total);
    let anomaly_ratio = if snapshot.dlmm_updates == 0 {
        0.0
    } else {
        (anomaly_total as f64 / snapshot.dlmm_updates as f64).min(1.0)
    };

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
        ])
        .split(area);

    render_quality_cells(frame, rows[0], "PARSE SUCCESS", parse_ok_ratio, Color::Cyan);
    render_quality_cells(frame, rows[1], "DB DELIVERY", db_ok_ratio, Color::Cyan);
    frame.render_widget(
        Gauge::default()
            .block(panel_block("ANOMALY RATE"))
            .gauge_style(Style::default().fg(Color::Cyan))
            .ratio(anomaly_ratio)
            .label(format!("{:.2}%", anomaly_ratio * 100.0)),
        rows[2],
    );
}

fn render_quality_cells(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    title: &str,
    ratio: f64,
    fill_color: Color,
) {
    let inner_width = area.width.saturating_sub(2) as usize;
    let pct_label = format!("{:>6.2}%", ratio * 100.0);
    let bar_cells = inner_width.saturating_sub(pct_label.len() + 4).max(8);
    let fill_cells = (ratio.clamp(0.0, 1.0) * bar_cells as f64).round() as usize;

    let mut spans = vec![Span::raw(PANEL_LEFT_PAD)];
    spans.extend(segmented_bar_spans(
        fill_cells,
        bar_cells,
        fill_color,
        Color::White,
    ));
    spans.push(Span::raw("  "));
    spans.push(Span::styled(
        pct_label,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    ));

    let panel = Paragraph::new(Line::from(spans)).block(panel_block(title));
    frame.render_widget(panel, area);
}

fn anomaly_panel<'a>(title: &'a str, total: u64, lines: &'a [String]) -> Paragraph<'a> {
    let mut rendered = vec![Line::from(vec![
        Span::styled("total ", Style::default().fg(Color::White)),
        Span::styled(
            total.to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    ])];
    if let Some(first) = lines.first() {
        rendered.push(Line::from(first.as_str()));
    } else {
        rendered.push(Line::from("-"));
    }

    Paragraph::new(rendered)
        .block(panel_block(title).padding(Padding::new(1, 1, 0, 1)))
        .wrap(Wrap { trim: false })
}

fn render_sparkline_panel(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    title: &str,
    latest: f64,
    history: &[u64],
) {
    let block = panel_block(format!("{title}  {latest:.2}"));
    let inner = block.inner(area);
    let lines = mirrored_graph_lines(history, inner.width as usize, inner.height as usize);
    let graph = Paragraph::new(lines).block(block);
    frame.render_widget(graph, area);
}

fn mirrored_graph_lines(history: &[u64], width: usize, height: usize) -> Vec<Line<'static>> {
    if width == 0 || height == 0 {
        return vec![];
    }

    let samples = downsample(history, width);
    let baseline = median_u64(&samples);
    let half = (height / 2).max(1);
    const POINT: char = '⣿';
    let mut matrix = vec![vec![' '; width]; height];

    let axis_row = half.saturating_sub(1);
    let max_dist = half.saturating_sub(1);
    // With the dense dot glyph, one text row renders as multiple micro-lines.
    // Keep center to 2 text rows total (1 above + 1 below) so it appears as ~4 visual lines.
    let center_dist_max = 0.min(max_dist);
    let extra_range = max_dist.saturating_sub(center_dist_max);
    let max_abs_delta = samples
        .iter()
        .map(|v| v.abs_diff(baseline))
        .max()
        .unwrap_or(0);
    // Keep center stable by preventing tiny jitter from using full graph height.
    let scale_den = max_abs_delta.max((baseline / 4).max(1));

    for (x, value) in samples.iter().enumerate() {
        let extra_span = if extra_range == 0 || scale_den == 0 {
            0
        } else {
            let delta = value.abs_diff(baseline);
            (((delta as f64 / scale_den as f64) * extra_range as f64).round() as usize)
                .min(extra_range)
        };
        let plot_dist_max = center_dist_max.saturating_add(extra_span);
        for dist in 0..=plot_dist_max {
            let upper = axis_row.saturating_sub(dist);
            if upper < height {
                matrix[upper][x] = POINT;
            }
            let lower = half.saturating_add(dist);
            if lower < height {
                matrix[lower][x] = POINT;
            }
        }
    }

    let mut lines = Vec::with_capacity(height);
    for (y, row) in matrix.into_iter().enumerate() {
        let mut spans = Vec::with_capacity(width);
        for ch in row {
            let style = if ch == POINT {
                // Mirror color is intentionally identical above and below the center line,
                // following a btop-like vertical gradient from center -> edge.
                let dist = mirrored_distance_from_center(y, axis_row, half);
                let max_dist = half.saturating_sub(1).max(1);
                Style::default().fg(mirrored_gradient_color(dist, max_dist))
            } else {
                Style::default().fg(Color::Reset)
            };
            spans.push(Span::styled(ch.to_string(), style));
        }
        lines.push(Line::from(spans));
    }
    lines
}

fn median_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] / 2).saturating_add(sorted[mid] / 2)
    } else {
        sorted[mid]
    }
}

fn mirrored_distance_from_center(y: usize, axis_row: usize, half: usize) -> usize {
    if y <= axis_row {
        axis_row.saturating_sub(y)
    } else {
        y.saturating_sub(half)
    }
}

fn mirrored_gradient_color(dist: usize, max_dist: usize) -> Color {
    if max_dist == 0 {
        return Color::Rgb(120, 215, 255); // sky blue
    }

    // Band order: sky blue -> dull purple -> pink -> red.
    if dist == 0 {
        return Color::Rgb(120, 215, 255); // sky blue
    }

    if dist == 1 {
        Color::Rgb(92, 52, 138) // dull dark purple (more distinct from sky blue)
    } else if dist == 2 {
        Color::Rgb(255, 85, 180) // pink
    } else {
        Color::Rgb(255, 70, 70) // red right after pink
    }
}

fn downsample(history: &[u64], width: usize) -> Vec<u64> {
    if width == 0 {
        return vec![];
    }
    if history.is_empty() {
        return vec![0; width];
    }
    if history.len() <= width {
        // Do not pad with zeros; it distorts baseline and center thickness.
        let pad_value = *history.first().unwrap_or(&0);
        let mut out = vec![pad_value; width - history.len()];
        out.extend_from_slice(history);
        return out;
    }

    let mut out = Vec::with_capacity(width);
    for i in 0..width {
        let start = i * history.len() / width;
        let end = ((i + 1) * history.len() / width).max(start + 1);
        let slice = &history[start..end.min(history.len())];
        let avg = slice.iter().copied().sum::<u64>() / slice.len() as u64;
        out.push(avg);
    }
    out
}

fn parsed_distribution_lines(area: Rect, bars: &[(String, u64)]) -> Vec<Line<'static>> {
    let top_padding_lines = 1usize;
    let top = bars.iter().collect::<Vec<_>>();
    let max_count = top.iter().map(|(_, count)| *count).max().unwrap_or(1);

    let mut lines = Vec::with_capacity(top.len().saturating_mul(2) + top_padding_lines);
    let label_width = 28usize;
    let count_width = 10usize;
    let usable = area.width.saturating_sub(2) as usize;
    let mut bar_render_width = usable.saturating_sub(label_width + count_width + 5);
    if bar_render_width < 8 {
        bar_render_width = 8;
    }
    let bar_cells = bar_render_width;
    let bar_inner_width = bar_cells;
    if bar_inner_width < 8 {
        return vec![Line::from("-")];
    }

    if top_padding_lines > 0 {
        lines.push(Line::from(Span::raw("")));
    }

    for (idx, (name, count)) in top.iter().enumerate() {
        let label = truncate(name, label_width);
        let fill_cells = ((*count as f64 / max_count as f64) * bar_cells as f64)
            .round()
            .clamp(0.0, bar_cells as f64) as usize;
        let mut spans = vec![
            Span::raw(PANEL_LEFT_PAD),
            Span::styled(
                format!("{label:<label_width$}"),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
        ];
        spans.extend(segmented_bar_spans(
            fill_cells,
            bar_cells,
            Color::Cyan,
            Color::White,
        ));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            format!("{count:>count_width$}"),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ));
        lines.push(Line::from(spans));

        if idx + 1 < top.len() {
            lines.push(Line::from(Span::styled(
                format!("{PANEL_LEFT_PAD}{}", "─".repeat(usable.saturating_sub(3))),
                Style::default().fg(ROW_SEPARATOR_COLOR),
            )));
        }
    }

    if lines.is_empty() {
        lines.push(Line::from("-"));
    }
    lines
}

fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        return value.to_string();
    }
    if max <= 3 {
        return ".".repeat(max);
    }
    format!("{}...", &value[..max - 3])
}

fn segmented_bar_spans(
    fill_cells: usize,
    total_cells: usize,
    fill_color: Color,
    empty_color: Color,
) -> Vec<Span<'static>> {
    let mut spans = Vec::with_capacity(total_cells);
    for i in 0..total_cells {
        let cell_style = if i < fill_cells {
            Style::default().fg(fill_color)
        } else {
            Style::default().fg(empty_color)
        };
        spans.push(Span::styled("▮", cell_style));
    }
    spans
}
