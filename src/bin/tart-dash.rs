use anyhow::Result;
use chrono::{DateTime, Local};
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table},
    Frame, Terminal,
};
use serde::{Deserialize, Serialize};
use std::{
    io,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

#[derive(Parser, Debug)]
#[command(author, version, about = "Terminal dashboard for TART telemetry monitoring", long_about = None)]
struct Args {
    /// API host
    #[arg(long, default_value = "localhost")]
    host: String,

    /// API port
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Refresh interval in milliseconds
    #[arg(long, default_value_t = 1000)]
    refresh_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeInfo {
    node_id: String,
    implementation_name: String,
    implementation_version: String,
    event_count: u64,
    is_connected: bool,
    last_seen_at: String,
    connected_at: String,
    #[serde(default)]
    disconnected_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodesResponse {
    nodes: Vec<NodeInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventInfo {
    event_id: u64,
    node_id: String,
    event_type: u8,
    timestamp: String,
    node_name: String,
    node_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventsResponse {
    events: Vec<EventInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatsInfo {
    total_blocks_authored: u64,
    best_block: u64,
    finalized_block: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ComponentHealth {
    status: String,
    message: String,
    #[serde(default)]
    metrics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HealthInfo {
    status: String,
    version: String,
    uptime_seconds: f64,
    #[serde(default)]
    components: std::collections::HashMap<String, ComponentHealth>,
    #[serde(default)]
    database_size_bytes: Option<u64>,
}

// --- Enhanced data types for metrics tab ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RateWindow {
    #[serde(default)]
    events: i64,
    #[serde(default)]
    blocks: i64,
    #[serde(default)]
    finalized: i64,
    #[serde(default)]
    events_per_second: f64,
    #[serde(default)]
    blocks_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LiveCounters {
    #[serde(default)]
    latest_slot: Option<i64>,
    #[serde(default)]
    finalized_slot: Option<i64>,
    #[serde(default)]
    active_nodes: i64,
    #[serde(default)]
    last_10s: RateWindow,
    #[serde(default)]
    last_1m: RateWindow,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CoreStatusEntry {
    #[serde(default)]
    core_index: i64,
    #[serde(default)]
    active_work_packages: i64,
    #[serde(default)]
    work_packages_last_hour: i64,
    #[serde(default)]
    guarantees_last_hour: i64,
    #[serde(default)]
    status: String,
    #[serde(default)]
    utilization_pct: f64,
    #[serde(default)]
    gas_used: u64,
    #[serde(default)]
    da_load: u64,
    #[serde(default)]
    active_blocks: u32,
    #[serde(default)]
    window_blocks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CoresSummary {
    #[serde(default)]
    total_cores: usize,
    #[serde(default)]
    active_cores: i64,
    #[serde(default)]
    idle_cores: i64,
    #[serde(default)]
    stale_cores: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CoresStatusResponse {
    #[serde(default)]
    cores: Vec<CoreStatusEntry>,
    #[serde(default)]
    summary: CoresSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WorkPackageTotals {
    #[serde(default)]
    submissions: i64,
    #[serde(default)]
    being_shared: i64,
    #[serde(default)]
    failed: i64,
    #[serde(default)]
    duplicates: i64,
    #[serde(default)]
    received: i64,
    #[serde(default)]
    refined: i64,
    #[serde(default)]
    work_reports_built: i64,
    #[serde(default)]
    guarantees_built: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WorkPackageStatsResponse {
    #[serde(default)]
    totals: WorkPackageTotals,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct GuaranteeTotals {
    #[serde(default)]
    built: i64,
    #[serde(default)]
    sending: i64,
    #[serde(default)]
    send_failed: i64,
    #[serde(default)]
    sent: i64,
    #[serde(default)]
    distributed: i64,
    #[serde(default)]
    receiving: i64,
    #[serde(default)]
    receive_failed: i64,
    #[serde(default)]
    received: i64,
    #[serde(default)]
    discarded: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct GuaranteeSuccessRates {
    #[serde(default)]
    send_success_rate: String,
    #[serde(default)]
    receive_success_rate: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct GuaranteeStatsResponse {
    #[serde(default)]
    totals: GuaranteeTotals,
    #[serde(default)]
    success_rates: GuaranteeSuccessRates,
}

// --- Dashboard state ---

#[derive(Clone)]
struct DashboardData {
    nodes: Vec<NodeInfo>,
    events: Vec<EventInfo>,
    stats: StatsInfo,
    health: HealthInfo,
    live: LiveCounters,
    cores: CoresStatusResponse,
    work_packages: WorkPackageStatsResponse,
    guarantees: GuaranteeStatsResponse,
    #[allow(dead_code)]
    last_update: Instant,
    update_count: u64,
    error: Option<String>,
    enhanced_errors: Vec<String>,
}

impl Default for DashboardData {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            events: Vec::new(),
            stats: StatsInfo {
                total_blocks_authored: 0,
                best_block: 0,
                finalized_block: 0,
            },
            health: HealthInfo {
                status: "Unknown".to_string(),
                version: "Unknown".to_string(),
                uptime_seconds: 0.0,
                components: std::collections::HashMap::new(),
                database_size_bytes: None,
            },
            live: LiveCounters::default(),
            cores: CoresStatusResponse::default(),
            work_packages: WorkPackageStatsResponse::default(),
            guarantees: GuaranteeStatsResponse::default(),
            last_update: Instant::now(),
            update_count: 0,
            error: None,
            enhanced_errors: Vec::new(),
        }
    }
}

/// Helper to safely lock a mutex, recovering from poison if necessary.
/// For a dashboard application, recovering the data from a poisoned mutex is acceptable
/// since we're only displaying information, not performing critical operations.
fn safe_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            // Mutex was poisoned (previous holder panicked), but we can still recover the data
            poisoned.into_inner()
        }
    }
}

struct App {
    data: Arc<Mutex<DashboardData>>,
    nodes_scroll: usize,
    events_scroll: usize,
    selected_panel: usize, // 0 = events, 1 = nodes, 2 = metrics
}

impl App {
    fn new(data: Arc<Mutex<DashboardData>>) -> Self {
        Self {
            data,
            nodes_scroll: 0,
            events_scroll: 0,
            selected_panel: 0, // Start with events selected
        }
    }

    fn on_key(&mut self, key: KeyCode) -> bool {
        match key {
            KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => return true,
            KeyCode::Tab => {
                self.selected_panel = (self.selected_panel + 1) % 3;
            }
            KeyCode::Up => match self.selected_panel {
                0 => self.events_scroll = self.events_scroll.saturating_sub(1),
                2 => self.nodes_scroll = self.nodes_scroll.saturating_sub(1),
                _ => {}
            },
            KeyCode::Down => {
                let data = safe_lock(&self.data);
                match self.selected_panel {
                    0 => {
                        if self.events_scroll < data.events.len().saturating_sub(1) {
                            self.events_scroll += 1;
                        }
                    }
                    2 => {
                        if self.nodes_scroll < data.nodes.len().saturating_sub(1) {
                            self.nodes_scroll += 1;
                        }
                    }
                    _ => {}
                }
            }
            KeyCode::PageUp => match self.selected_panel {
                0 => self.events_scroll = self.events_scroll.saturating_sub(10),
                2 => self.nodes_scroll = self.nodes_scroll.saturating_sub(10),
                _ => {}
            },
            KeyCode::PageDown => {
                let data = safe_lock(&self.data);
                match self.selected_panel {
                    0 => {
                        self.events_scroll =
                            (self.events_scroll + 10).min(data.events.len().saturating_sub(1));
                    }
                    2 => {
                        self.nodes_scroll =
                            (self.nodes_scroll + 10).min(data.nodes.len().saturating_sub(1));
                    }
                    _ => {}
                }
            }
            KeyCode::Home => match self.selected_panel {
                0 => self.events_scroll = 0,
                2 => self.nodes_scroll = 0,
                _ => {}
            },
            _ => {}
        }
        false
    }
}

fn fetch_data(api_base: &str) -> Result<DashboardData> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    // Core data (required)
    let health = client
        .get(format!("{}/api/health/detailed", api_base))
        .send()?
        .json::<HealthInfo>()?;
    let stats = client
        .get(format!("{}/api/stats", api_base))
        .send()?
        .json::<StatsInfo>()?;
    let nodes_resp = client
        .get(format!("{}/api/nodes", api_base))
        .send()?
        .json::<NodesResponse>()?;
    let events_resp = client
        .get(format!("{}/api/events?limit=50", api_base))
        .send()?
        .json::<EventsResponse>()?;

    // Enhanced data (optional — gracefully degrade if endpoints unavailable)
    let mut enhanced_errors = Vec::new();

    let live = client
        .get(format!("{}/api/metrics/live", api_base))
        .send()
        .and_then(|r| r.json::<LiveCounters>())
        .unwrap_or_else(|e| {
            enhanced_errors.push(format!("metrics/live: {e}"));
            LiveCounters::default()
        });
    let cores = client
        .get(format!("{}/api/cores/status", api_base))
        .send()
        .and_then(|r| r.json::<CoresStatusResponse>())
        .unwrap_or_else(|e| {
            enhanced_errors.push(format!("cores/status: {e}"));
            CoresStatusResponse::default()
        });
    let work_packages = client
        .get(format!("{}/api/workpackages", api_base))
        .send()
        .and_then(|r| r.json::<WorkPackageStatsResponse>())
        .unwrap_or_else(|e| {
            enhanced_errors.push(format!("workpackages: {e}"));
            WorkPackageStatsResponse::default()
        });
    let guarantees = client
        .get(format!("{}/api/guarantees", api_base))
        .send()
        .and_then(|r| r.json::<GuaranteeStatsResponse>())
        .unwrap_or_else(|e| {
            enhanced_errors.push(format!("guarantees: {e}"));
            GuaranteeStatsResponse::default()
        });

    Ok(DashboardData {
        nodes: nodes_resp.nodes,
        events: events_resp.events,
        stats,
        health,
        live,
        cores,
        work_packages,
        guarantees,
        last_update: Instant::now(),
        update_count: 0,
        error: None,
        enhanced_errors,
    })
}

fn update_loop(api_base: String, data: Arc<Mutex<DashboardData>>, refresh_interval: Duration) {
    loop {
        match fetch_data(&api_base) {
            Ok(mut new_data) => {
                let mut data = safe_lock(&data);
                new_data.update_count = data.update_count + 1;
                *data = new_data;
            }
            Err(e) => {
                let mut data = safe_lock(&data);
                data.error = Some(format!("Failed to fetch data: {}", e));
            }
        }
        thread::sleep(refresh_interval);
    }
}

fn ui(f: &mut Frame, app: &App) {
    let data = safe_lock(&app.data);

    // Fill entire screen with black background
    f.render_widget(
        Block::default().style(Style::default().bg(Color::Black)),
        f.size(),
    );

    // Add padding around the entire UI
    let padded_area = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Top padding
            Constraint::Min(0),    // Main content
            Constraint::Length(1), // Bottom padding
        ])
        .split(f.size());

    let horizontal_padding = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(2), // Left padding
            Constraint::Min(0),    // Main content
            Constraint::Length(2), // Right padding
        ])
        .split(padded_area[1]);

    // Main layout within the padded area
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7), // Header with ASCII art
            Constraint::Min(10),   // Content
            Constraint::Length(3), // Footer
        ])
        .split(horizontal_padding[1]);

    // Y2K TART ASCII art with hot pink color
    let tart_style = Style::default()
        .fg(Color::Rgb(255, 0, 128))
        .add_modifier(Modifier::BOLD); // Hot Pink for all TART letters

    let ascii_art = vec![
        Line::from(vec![
            // T        A        R        T
            Span::styled("▄▄▄█████▓", tart_style),
            Span::styled(" ", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("▄▄▄      ", tart_style),
            Span::styled(" ", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("██▀███  ", tart_style),
            Span::styled(" ", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("▄▄▄█████▓", tart_style),
            Span::styled("  TART dashboard", Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("▓  ██▒ ▓▒", tart_style),
            Span::styled("▒", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("████▄    ", tart_style),
            Span::styled("▓", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("██ ▒ ██▒", tart_style),
            Span::styled("▓", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("  ██▒ ▓▒", tart_style),
            Span::styled(
                "  testing, analytics and research telemetry",
                Style::default().fg(Color::Rgb(150, 150, 150)),
            ),
        ]),
        Line::from(vec![
            Span::styled("▒ ▓██░ ▒░", tart_style),
            Span::styled("▒", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("██  ▀█▄  ", tart_style),
            Span::styled("▓", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("██ ░▄█ ▒", tart_style),
            Span::styled("▒", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled(" ▓██░ ▒░", tart_style),
            Span::styled("  uptime: ", Style::default().fg(Color::White)),
            Span::styled(
                {
                    let uptime_minutes = data.health.uptime_seconds / 60.0;
                    if uptime_minutes < 60.0 {
                        format!("{:5.1} min", uptime_minutes)
                    } else if uptime_minutes < 1440.0 {
                        format!("{:5.1} hrs", uptime_minutes / 60.0)
                    } else {
                        format!("{:5.1} days", uptime_minutes / 1440.0)
                    }
                },
                Style::default()
                    .fg(Color::Rgb(0, 255, 255))
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("░ ▓██▓ ░ ", tart_style),
            Span::styled("░", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("██▄▄▄▄██ ", tart_style),
            Span::styled("▒", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("██▀▀█▄  ", tart_style),
            Span::styled("░", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled(" ▓██▓ ░ ", tart_style),
            Span::styled("  connected: ", Style::default().fg(Color::White)),
            Span::styled(
                format!(
                    "{}/{}",
                    data.nodes.iter().filter(|n| n.is_connected).count(),
                    data.nodes.len()
                ),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" │ ", Style::default().fg(Color::White)),
            Span::styled(
                {
                    // Use 10s live rate when available, fall back to lifetime average
                    if data.live.last_10s.events_per_second > 0.0
                        || data.live.last_1m.events_per_second > 0.0
                    {
                        format!("{:.1} events/s", data.live.last_10s.events_per_second)
                    } else if data.health.uptime_seconds > 0.0 {
                        let total_events: u64 = data.nodes.iter().map(|n| n.event_count).sum();
                        format!(
                            "{:.1} events/s",
                            total_events as f64 / data.health.uptime_seconds
                        )
                    } else {
                        "0.0 events/s".to_string()
                    }
                },
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  ▒██▒ ░ ", tart_style),
            Span::styled(" ", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("▓█   ▓██▒", tart_style),
            Span::styled("░", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled("██▓ ▒██▒", tart_style),
            Span::styled(" ", Style::default().fg(Color::Rgb(50, 50, 50))),
            Span::styled(" ▒██▒ ░ ", tart_style),
            Span::styled(
                "═".repeat((chunks[0].width as usize).saturating_sub(38)),
                Style::default().fg(Color::Rgb(100, 100, 100)),
            ),
        ]),
    ];

    let header = Paragraph::new(ascii_art)
        .style(Style::default().bg(Color::Black))
        .alignment(Alignment::Left);
    f.render_widget(header, chunks[0]);

    // Content area
    let content_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(60), // Top area (stats + events/metrics)
            Constraint::Percentage(40), // Bottom area (nodes)
        ])
        .split(chunks[1]);

    // Top content - split horizontally
    let top_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(32), Constraint::Min(50)])
        .split(content_chunks[0]);

    // Left side - Stats
    render_stats(f, top_chunks[0], &data);

    // Right side - Events or Metrics tab
    if app.selected_panel == 1 {
        render_metrics(f, top_chunks[1], &data, true);
    } else {
        render_events(
            f,
            top_chunks[1],
            &data,
            app.events_scroll,
            app.selected_panel == 0,
        );
    }

    // Bottom - Nodes
    render_nodes(
        f,
        content_chunks[1],
        &data,
        app.nodes_scroll,
        app.selected_panel == 2,
    );

    // Footer
    render_footer(f, chunks[2], &data, app.selected_panel);
}

/// Safe padding: returns spaces to right-align a value within a fixed-width box.
/// Returns empty string if the value is already wider than the box.
fn pad(box_width: usize, prefix_len: usize, value_len: usize) -> String {
    " ".repeat(
        box_width
            .saturating_sub(prefix_len)
            .saturating_sub(value_len)
            .saturating_sub(1),
    )
}

fn render_stats(f: &mut Frame, area: Rect, data: &DashboardData) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // System info
            Constraint::Length(7), // Network stats
            Constraint::Min(5),    // Block info
        ])
        .split(area);

    // System Information

    let system_info = vec![
        Line::from(vec![Span::styled(
            "╔═ system status ════════════╗",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from({
            let status_text = match data.health.status.as_str() {
                "Healthy" => "● ok  ",  // 6 chars total
                "Degraded" => "▲ warn", // 6 chars total
                _ => "▼ fail",          // 6 chars total
            };
            let status_color = match data.health.status.as_str() {
                "Healthy" => Color::Rgb(0, 255, 0),
                "Degraded" => Color::Rgb(255, 200, 0),
                _ => Color::Rgb(255, 0, 0),
            };
            vec![
                Span::styled("║ ", Style::default().fg(Color::Cyan)),
                Span::styled(
                    "◆ status:  ",
                    Style::default().fg(Color::Rgb(255, 255, 150)),
                ),
                Span::styled(pad(30 - 2, 11, 6), Style::default()), // All status texts are 6 chars
                Span::styled(
                    status_text,
                    Style::default()
                        .fg(status_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("║", Style::default().fg(Color::Cyan)),
            ]
        }),
        Line::from({
            let components = data.health.components.len();
            let healthy_components = data
                .health
                .components
                .values()
                .filter(|c| c.status == "Healthy" || c.status == "healthy")
                .count();
            let comp_status = format!("{}/{}", healthy_components, components);
            vec![
                Span::styled("║ ", Style::default().fg(Color::Cyan)),
                Span::styled(
                    "◆ modules: ",
                    Style::default().fg(Color::Rgb(255, 255, 150)),
                ),
                Span::styled(pad(30 - 2, 11, comp_status.len()), Style::default()),
                Span::styled(
                    comp_status,
                    Style::default()
                        .fg(Color::Rgb(0, 255, 255))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("║", Style::default().fg(Color::Cyan)),
            ]
        }),
        Line::from(vec![
            Span::styled("║ ", Style::default().fg(Color::Cyan)),
            Span::styled(
                "◆ version: ",
                Style::default().fg(Color::Rgb(255, 255, 150)),
            ),
            Span::styled(pad(30 - 2, 11, data.health.version.len()), Style::default()),
            Span::styled(
                &data.health.version,
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("║", Style::default().fg(Color::Cyan)),
        ]),
        Line::from({
            // Display database size from health endpoint
            let db_size = data
                .health
                .database_size_bytes
                .map(|bytes| {
                    let size_mb = bytes as f64 / 1_048_576.0;
                    if size_mb < 1.0 {
                        format!("{:.0}kb", bytes as f64 / 1024.0)
                    } else if size_mb < 1024.0 {
                        format!("{:.1}mb", size_mb)
                    } else {
                        format!("{:.2}gb", size_mb / 1024.0)
                    }
                })
                .unwrap_or_else(|| "n/a".to_string());
            vec![
                Span::styled("║ ", Style::default().fg(Color::Cyan)),
                Span::styled(
                    "◆ db size: ",
                    Style::default().fg(Color::Rgb(255, 255, 150)),
                ),
                Span::styled(pad(30 - 2, 11, db_size.len()), Style::default()),
                Span::styled(
                    db_size,
                    Style::default()
                        .fg(Color::Rgb(255, 165, 0))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("║", Style::default().fg(Color::Cyan)),
            ]
        }),
        Line::from(vec![Span::styled(
            "╚════════════════════════════╝",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )]),
    ];

    let system_block = Paragraph::new(system_info).style(Style::default().bg(Color::Black));
    f.render_widget(system_block, chunks[0]);

    // Network Statistics
    let connected_nodes = data.nodes.iter().filter(|n| n.is_connected).count();
    let total_nodes = data.nodes.len();
    let total_events: u64 = data.nodes.iter().map(|n| n.event_count).sum();

    let network_stats = vec![
        Line::from(vec![Span::styled(
            "╔═ network ══════════════════╗",
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("║ ", Style::default().fg(Color::Blue)),
            Span::styled(
                "▸ nodes:   ",
                Style::default().fg(Color::Rgb(150, 255, 255)),
            ),
            Span::styled(
                pad(
                    30 - 2,
                    11,
                    format!("{:3}/{}", connected_nodes, total_nodes).len(),
                ),
                Style::default(),
            ),
            Span::styled(
                format!("{:3}/{}", connected_nodes, total_nodes),
                Style::default()
                    .fg(Color::Rgb(0, 255, 0))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("║", Style::default().fg(Color::Blue)),
        ]),
        Line::from(vec![
            Span::styled("║ ", Style::default().fg(Color::Blue)),
            Span::styled(
                "▸ events:  ",
                Style::default().fg(Color::Rgb(150, 255, 255)),
            ),
            Span::styled(
                pad(30 - 2, 11, total_events.to_string().len()),
                Style::default(),
            ),
            Span::styled(
                format!("{}", total_events),
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("║", Style::default().fg(Color::Blue)),
        ]),
        Line::from(vec![
            Span::styled("║ ", Style::default().fg(Color::Blue)),
            Span::styled(
                "▸ blocks:  ",
                Style::default().fg(Color::Rgb(150, 255, 255)),
            ),
            Span::styled(
                pad(
                    30 - 2,
                    11,
                    data.stats.total_blocks_authored.to_string().len(),
                ),
                Style::default(),
            ),
            Span::styled(
                format!("{}", data.stats.total_blocks_authored),
                Style::default()
                    .fg(Color::Rgb(0, 255, 100))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("║", Style::default().fg(Color::Blue)),
        ]),
        Line::from(vec![Span::styled(
            "╚════════════════════════════╝",
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        )]),
    ];

    let network_block = Paragraph::new(network_stats).style(Style::default().bg(Color::Black));
    f.render_widget(network_block, chunks[1]);

    // Block Information
    let block_info = vec![
        Line::from(vec![Span::styled(
            "╔═ blockchain ═══════════════╗",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("║ ", Style::default().fg(Color::Yellow)),
            Span::styled(
                "# best:     ",
                Style::default().fg(Color::Rgb(255, 200, 100)),
            ),
            Span::styled(
                pad(30 - 2, 12, data.stats.best_block.to_string().len()),
                Style::default(),
            ),
            Span::styled(
                format!("{}", data.stats.best_block),
                Style::default()
                    .fg(Color::Rgb(255, 255, 0))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("║", Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::styled("║ ", Style::default().fg(Color::Yellow)),
            Span::styled(
                "# finalized:",
                Style::default().fg(Color::Rgb(255, 200, 100)),
            ),
            Span::styled(
                pad(30 - 2, 12, data.stats.finalized_block.to_string().len()),
                Style::default(),
            ),
            Span::styled(
                format!("{}", data.stats.finalized_block),
                Style::default()
                    .fg(Color::Rgb(255, 165, 0))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("║", Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![Span::styled(
            "╚════════════════════════════╝",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )]),
    ];

    let block_block = Paragraph::new(block_info).style(Style::default().bg(Color::Black));
    f.render_widget(block_block, chunks[2]);
}

fn render_nodes(f: &mut Frame, area: Rect, data: &DashboardData, scroll: usize, is_selected: bool) {
    let header_cells = vec![
        Cell::from("node").style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("version").style(
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("events").style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("status").style(
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
    ];
    let header = Row::new(header_cells)
        .style(Style::default().bg(Color::Rgb(30, 0, 50)))
        .height(1);

    let mut nodes = data.nodes.clone();
    nodes.sort_by(|a, b| b.event_count.cmp(&a.event_count));

    let rows: Vec<Row> = nodes
        .iter()
        .skip(scroll)
        .take((area.height as usize).saturating_sub(4))
        .map(|node| {
            let status = if node.is_connected {
                Cell::from("● online").style(
                    Style::default()
                        .fg(Color::Rgb(0, 255, 0))
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                Cell::from("○ offline").style(Style::default().fg(Color::Rgb(100, 100, 100)))
            };

            let node_id_display = format!(
                "({}...)",
                node.node_id
                    .get(..16)
                    .unwrap_or(&node.node_id)
                    .to_lowercase()
            );

            Row::new(vec![
                Cell::from(Line::from(vec![
                    Span::styled(
                        &node.implementation_name,
                        Style::default().fg(Color::Rgb(150, 255, 255)),
                    ),
                    Span::styled(" ", Style::default()),
                    Span::styled(
                        node_id_display,
                        Style::default().fg(Color::Rgb(128, 128, 128)),
                    ),
                ])),
                Cell::from(format!("v{}", node.implementation_version))
                    .style(Style::default().fg(Color::Rgb(255, 150, 255))),
                Cell::from(format!("{:8}", node.event_count)).style(
                    Style::default()
                        .fg(Color::Rgb(255, 255, 100))
                        .add_modifier(Modifier::BOLD),
                ),
                status,
            ])
        })
        .collect();

    let border_style = if is_selected {
        Style::default()
            .fg(Color::Rgb(0, 255, 255))
            .add_modifier(Modifier::BOLD) // Electric cyan when selected
    } else {
        Style::default().fg(Color::Rgb(100, 200, 255)) // Light blue when not selected
    };

    let table = Table::new(
        rows,
        &[
            Constraint::Percentage(45),
            Constraint::Percentage(15),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(format!(" node list ({} total) ", data.nodes.len()))
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(Style::default().bg(Color::Black)),
    )
    .style(Style::default().bg(Color::Black))
    .highlight_style(Style::default().bg(Color::Rgb(75, 0, 130)))
    .highlight_symbol("▶ ");

    f.render_widget(table, area);
}

fn render_events(
    f: &mut Frame,
    area: Rect,
    data: &DashboardData,
    scroll: usize,
    is_selected: bool,
) {
    let event_names = [
        // Meta events
        (0, "DROPPED"),
        // Status events
        (10, "STATUS_UPDATE"),
        (11, "BEST_BLOCK_CHANGED"),
        (12, "FINALIZED_BLOCK"),
        (13, "SYNC_STATUS_CHANGED"),
        // Connection events
        (20, "CONNECTION_REFUSED"),
        (21, "CONNECTING_IN"),
        (22, "CONNECTION_IN_FAILED"),
        (23, "CONNECTED_IN"),
        (24, "CONNECTING_OUT"),
        (25, "CONNECTION_OUT_FAILED"),
        (26, "CONNECTED_OUT"),
        (27, "DISCONNECTED"),
        (28, "PEER_MISBEHAVED"),
        // Block authoring/importing
        (40, "AUTHORING"),
        (41, "AUTHORING_FAILED"),
        (42, "AUTHORED"),
        (43, "IMPORTING"),
        (44, "BLOCK_VERIFICATION_FAILED"),
        (45, "BLOCK_VERIFIED"),
        (46, "BLOCK_EXECUTION_FAILED"),
        (47, "BLOCK_EXECUTED"),
        // Block distribution
        (60, "BLOCK_ANNOUNCEMENT_STREAM_OPENED"),
        (61, "BLOCK_ANNOUNCEMENT_STREAM_CLOSED"),
        (62, "BLOCK_ANNOUNCED"),
        (63, "SENDING_BLOCK_REQUEST"),
        (64, "RECEIVING_BLOCK_REQUEST"),
        (65, "BLOCK_REQUEST_FAILED"),
        (66, "BLOCK_REQUEST_SENT"),
        (67, "BLOCK_REQUEST_RECEIVED"),
        (68, "BLOCK_TRANSFERRED"),
        // Safrole ticket events
        (80, "GENERATING_TICKETS"),
        (81, "TICKET_GENERATION_FAILED"),
        (82, "TICKETS_GENERATED"),
        (83, "TICKET_TRANSFER_FAILED"),
        (84, "TICKET_TRANSFERRED"),
        // Work package events
        (90, "WORK_PACKAGE_SUBMISSION"),
        (91, "WORK_PACKAGE_BEING_SHARED"),
        (92, "WORK_PACKAGE_FAILED"),
        (93, "DUPLICATE_WORK_PACKAGE"),
        (94, "WORK_PACKAGE_RECEIVED"),
        (95, "AUTHORIZED"),
        (96, "EXTRINSIC_DATA_RECEIVED"),
        (97, "IMPORTS_RECEIVED"),
        (98, "SHARING_WORK_PACKAGE"),
        (99, "WORK_PACKAGE_SHARING_FAILED"),
        (100, "BUNDLE_SENT"),
        (101, "REFINED"),
        (102, "WORK_REPORT_BUILT"),
        (103, "WORK_REPORT_SIGNATURE_SENT"),
        (104, "WORK_REPORT_SIGNATURE_RECEIVED"),
        // Guarantee events
        (105, "GUARANTEE_BUILT"),
        (106, "SENDING_GUARANTEE"),
        (107, "GUARANTEE_SEND_FAILED"),
        (108, "GUARANTEE_SENT"),
        (109, "GUARANTEES_DISTRIBUTED"),
        (110, "RECEIVING_GUARANTEE"),
        (111, "GUARANTEE_RECEIVE_FAILED"),
        (112, "GUARANTEE_RECEIVED"),
        (113, "GUARANTEE_DISCARDED"),
        // Shard request events
        (120, "SENDING_SHARD_REQUEST"),
        (121, "RECEIVING_SHARD_REQUEST"),
        (122, "SHARD_REQUEST_FAILED"),
        (123, "SHARD_REQUEST_SENT"),
        (124, "SHARD_REQUEST_RECEIVED"),
        (125, "SHARDS_TRANSFERRED"),
        // Assurance events
        (126, "DISTRIBUTING_ASSURANCE"),
        (127, "ASSURANCE_SEND_FAILED"),
        (128, "ASSURANCE_SENT"),
        (129, "ASSURANCE_DISTRIBUTED"),
        (130, "ASSURANCE_RECEIVE_FAILED"),
        (131, "ASSURANCE_RECEIVED"),
        // Bundle shard events
        (140, "SENDING_BUNDLE_SHARD_REQUEST"),
        (141, "RECEIVING_BUNDLE_SHARD_REQUEST"),
        (142, "BUNDLE_SHARD_REQUEST_FAILED"),
        (143, "BUNDLE_SHARD_REQUEST_SENT"),
        (144, "BUNDLE_SHARD_REQUEST_RECEIVED"),
        (145, "BUNDLE_SHARD_TRANSFERRED"),
        (146, "RECONSTRUCTING_BUNDLE"),
        (147, "BUNDLE_RECONSTRUCTED"),
        (148, "SENDING_BUNDLE_REQUEST"),
        (149, "RECEIVING_BUNDLE_REQUEST"),
        (150, "BUNDLE_REQUEST_FAILED"),
        (151, "BUNDLE_REQUEST_SENT"),
        (152, "BUNDLE_REQUEST_RECEIVED"),
        (153, "BUNDLE_TRANSFERRED"),
        // Work package hash mapping
        (160, "WORK_PACKAGE_HASH_MAPPED"),
        (161, "SEGMENTS_ROOT_MAPPED"),
        // Segment shard events
        (162, "SENDING_SEGMENT_SHARD_REQUEST"),
        (163, "RECEIVING_SEGMENT_SHARD_REQUEST"),
        (164, "SEGMENT_SHARD_REQUEST_FAILED"),
        (165, "SEGMENT_SHARD_REQUEST_SENT"),
        (166, "SEGMENT_SHARD_REQUEST_RECEIVED"),
        (167, "SEGMENT_SHARDS_TRANSFERRED"),
        // Segment reconstruction events
        (168, "RECONSTRUCTING_SEGMENTS"),
        (169, "SEGMENT_RECONSTRUCTION_FAILED"),
        (170, "SEGMENTS_RECONSTRUCTED"),
        (171, "SEGMENT_VERIFICATION_FAILED"),
        (172, "SEGMENTS_VERIFIED"),
        (173, "SENDING_SEGMENT_REQUEST"),
        (174, "RECEIVING_SEGMENT_REQUEST"),
        (175, "SEGMENT_REQUEST_FAILED"),
        (176, "SEGMENT_REQUEST_SENT"),
        (177, "SEGMENT_REQUEST_RECEIVED"),
        (178, "SEGMENTS_TRANSFERRED"),
        // Preimage events
        (190, "PREIMAGE_ANNOUNCEMENT_FAILED"),
        (191, "PREIMAGE_ANNOUNCED"),
        (192, "ANNOUNCED_PREIMAGE_FORGOTTEN"),
        (193, "SENDING_PREIMAGE_REQUEST"),
        (194, "RECEIVING_PREIMAGE_REQUEST"),
        (195, "PREIMAGE_REQUEST_FAILED"),
        (196, "PREIMAGE_REQUEST_SENT"),
        (197, "PREIMAGE_REQUEST_RECEIVED"),
        (198, "PREIMAGE_TRANSFERRED"),
        (199, "PREIMAGE_DISCARDED"),
    ];

    let items: Vec<ListItem> = data
        .events
        .iter()
        .skip(scroll)
        .take((area.height as usize).saturating_sub(3))
        .map(|event| {
            let name = event_names
                .iter()
                .find(|(t, _)| *t == event.event_type)
                .map(|(_, n)| *n)
                .unwrap_or("UNKNOWN");

            // Parse UTC timestamp and convert to local time
            let time = DateTime::parse_from_rfc3339(&event.timestamp)
                .ok()
                .map(|dt| dt.with_timezone(&Local).format("%H:%M:%S").to_string())
                .unwrap_or_else(|| {
                    event
                        .timestamp
                        .get(11..19)
                        .unwrap_or(&event.timestamp)
                        .to_string()
                });

            // Categorize events and assign colors/prefixes - Y2K vibrant theme
            let (prefix, color, emoji) = match event.event_type {
                // Meta/dropped events - gray
                0 => ("DROP ", Color::Rgb(128, 128, 128), "💧"),
                // Status events - electric cyan
                10..=13 => ("STAT ", Color::Rgb(0, 255, 255), "📊"),
                // Connection events - electric lime
                20..=28 => ("CONN ", Color::Rgb(0, 255, 0), "🔌"),
                // Authoring/importing events - electric blue
                40..=47 => ("BLCK ", Color::Rgb(30, 144, 255), "⬛"),
                // Block distribution events - hot pink
                60..=68 => ("DIST ", Color::Rgb(255, 20, 147), "📡"),
                // Safrole ticket events - electric orange
                80..=84 => ("TCKT ", Color::Rgb(255, 165, 0), "🎫"),
                // Work package events - electric purple
                90..=113 => ("WORK ", Color::Rgb(191, 0, 255), "📦"),
                // Shard/assurance events - bright green
                120..=131 => ("SHRD ", Color::Rgb(0, 255, 127), "🧩"),
                // Bundle events - bright blue
                140..=153 => ("BNDL ", Color::Rgb(0, 191, 255), "📦"),
                // Segment events - bright magenta
                160..=178 => ("SEGM ", Color::Rgb(255, 0, 255), "🔗"),
                // Preimage events - bright yellow
                190..=199 => ("PREIM", Color::Rgb(255, 255, 0), "🖼\u{fe0f}"),
                // Unknown - dim white
                _ => ("UNKN ", Color::Rgb(200, 200, 200), "❓"),
            };

            let content = Line::from(vec![
                Span::styled(
                    format!("{} ", &time),
                    Style::default()
                        .fg(Color::Rgb(255, 255, 150))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    prefix,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(
                        "({}...) ",
                        event
                            .node_id
                            .get(..12)
                            .unwrap_or(&event.node_id)
                            .to_lowercase()
                    ),
                    Style::default().fg(Color::Rgb(128, 128, 128)),
                ),
                Span::styled(emoji, Style::default()),
                Span::styled(" ", Style::default()),
                Span::styled(
                    name,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
            ]);

            ListItem::new(content)
        })
        .collect();

    let border_style = if is_selected {
        Style::default()
            .fg(Color::Rgb(255, 0, 255))
            .add_modifier(Modifier::BOLD) // Electric magenta when selected
    } else {
        Style::default().fg(Color::Rgb(255, 100, 200)) // Hot pink when not selected
    };

    let events = List::new(items)
        .block(
            Block::default()
                .title(format!(" event log ({} recent) ", data.events.len()))
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .style(Style::default().bg(Color::Black))
        .highlight_style(
            Style::default()
                .bg(Color::Rgb(75, 0, 130))
                .add_modifier(Modifier::BOLD),
        ) // Indigo highlight to match Y2K theme
        .highlight_symbol("▶ ");

    f.render_widget(events, area);
}

fn render_metrics(f: &mut Frame, area: Rect, data: &DashboardData, is_selected: bool) {
    let border_color = if is_selected {
        Color::Rgb(0, 255, 255)
    } else {
        Color::Rgb(100, 200, 255)
    };
    let border_style = Style::default()
        .fg(border_color)
        .add_modifier(Modifier::BOLD);

    let label_style = Style::default().fg(Color::Rgb(150, 150, 150));
    let dim_style = Style::default().fg(Color::Rgb(100, 100, 100));

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5), // Throughput
            Constraint::Length(5), // Pipeline
            Constraint::Length(5), // Guarantees
            Constraint::Min(4),    // Cores (gets remaining space)
        ])
        .split(area);

    // --- Throughput ---
    let eps_10s = data.live.last_10s.events_per_second;
    let bps_10s = data.live.last_10s.blocks_per_second;
    let eps_1m = data.live.last_1m.events_per_second;
    let bps_1m = data.live.last_1m.blocks_per_second;
    let latest_slot = data
        .live
        .latest_slot
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".into());
    let final_slot = data
        .live
        .finalized_slot
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".into());

    let throughput = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("  10s ", label_style),
            Span::styled(
                format!("{:6.1}", eps_10s),
                Style::default()
                    .fg(Color::Rgb(0, 255, 255))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" ev/s ", label_style),
            Span::styled(
                format!("{:5.2}", bps_10s),
                Style::default()
                    .fg(Color::Rgb(0, 255, 100))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" blk/s", label_style),
            Span::styled("  │  ", dim_style),
            Span::styled("1m ", label_style),
            Span::styled(
                format!("{:6.1}", eps_1m),
                Style::default()
                    .fg(Color::Rgb(0, 200, 200))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" ev/s ", label_style),
            Span::styled(
                format!("{:5.2}", bps_1m),
                Style::default()
                    .fg(Color::Rgb(0, 200, 80))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" blk/s", label_style),
        ]),
        Line::from(vec![
            Span::styled("  slot ", label_style),
            Span::styled(
                format!("{:>6}", latest_slot),
                Style::default()
                    .fg(Color::Rgb(255, 255, 0))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" latest", label_style),
            Span::styled("  │  ", dim_style),
            Span::styled("slot ", label_style),
            Span::styled(
                format!("{:>6}", final_slot),
                Style::default()
                    .fg(Color::Rgb(255, 165, 0))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" finalized", label_style),
        ]),
        Line::from(vec![Span::styled("", Style::default())]),
    ])
    .block(
        Block::default()
            .title(" throughput ")
            .borders(Borders::ALL)
            .border_style(border_style),
    )
    .style(Style::default().bg(Color::Black));
    f.render_widget(throughput, chunks[0]);

    // --- Work Package Pipeline ---
    let wp = &data.work_packages.totals;
    let pipeline = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("  submit ", label_style),
            Span::styled(
                format!("{:>6}", wp.submissions),
                Style::default()
                    .fg(Color::Rgb(191, 0, 255))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  recv ", label_style),
            Span::styled(
                format!("{:>6}", wp.received),
                Style::default()
                    .fg(Color::Rgb(150, 100, 255))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  refined ", label_style),
            Span::styled(
                format!("{:>6}", wp.refined),
                Style::default()
                    .fg(Color::Rgb(100, 200, 255))
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  report ", label_style),
            Span::styled(
                format!("{:>6}", wp.work_reports_built),
                Style::default()
                    .fg(Color::Rgb(0, 255, 200))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  guar ", label_style),
            Span::styled(
                format!("{:>6}", wp.guarantees_built),
                Style::default()
                    .fg(Color::Rgb(0, 255, 100))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  failed  ", label_style),
            Span::styled(
                format!("{:>6}", wp.failed),
                Style::default()
                    .fg(if wp.failed > 0 {
                        Color::Rgb(255, 80, 80)
                    } else {
                        Color::Rgb(100, 100, 100)
                    })
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![Span::styled("", Style::default())]),
    ])
    .block(
        Block::default()
            .title(" work package pipeline (24h) ")
            .borders(Borders::ALL)
            .border_style(border_style),
    )
    .style(Style::default().bg(Color::Black));
    f.render_widget(pipeline, chunks[1]);

    // --- Guarantee Health ---
    let gt = &data.guarantees.totals;
    let sr = &data.guarantees.success_rates;
    let send_rate = if sr.send_success_rate.is_empty() {
        "-".to_string()
    } else {
        sr.send_success_rate.clone()
    };
    let recv_rate = if sr.receive_success_rate.is_empty() {
        "-".to_string()
    } else {
        sr.receive_success_rate.clone()
    };

    let guarantees = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("  built ", label_style),
            Span::styled(
                format!("{:>6}", gt.built),
                Style::default()
                    .fg(Color::Rgb(0, 255, 200))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  sent ", label_style),
            Span::styled(
                format!("{:>6}", gt.sent),
                Style::default()
                    .fg(Color::Rgb(0, 200, 255))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  distributed ", label_style),
            Span::styled(
                format!("{:>6}", gt.distributed),
                Style::default()
                    .fg(Color::Rgb(0, 255, 100))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  disc ", label_style),
            Span::styled(
                format!("{:>4}", gt.discarded),
                Style::default()
                    .fg(if gt.discarded > 0 {
                        Color::Rgb(255, 200, 0)
                    } else {
                        Color::Rgb(100, 100, 100)
                    })
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  send success ", label_style),
            Span::styled(
                format!("{:>7}", send_rate),
                Style::default()
                    .fg(Color::Rgb(0, 255, 100))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("    recv success ", label_style),
            Span::styled(
                format!("{:>7}", recv_rate),
                Style::default()
                    .fg(Color::Rgb(0, 255, 100))
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![Span::styled("", Style::default())]),
    ])
    .block(
        Block::default()
            .title(" guarantee health ")
            .borders(Borders::ALL)
            .border_style(border_style),
    )
    .style(Style::default().bg(Color::Black));
    f.render_widget(guarantees, chunks[2]);

    // --- Core Utilization ---
    let summary = &data.cores.summary;
    let has_core_data = !data.cores.cores.is_empty();
    let core_error = data
        .enhanced_errors
        .iter()
        .find(|e| e.starts_with("cores/"))
        .cloned();

    if has_core_data {
        let core_header_cells = vec![
            Cell::from("core").style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("status").style(
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("util%").style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("gas").style(
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("da").style(
                Style::default()
                    .fg(Color::Rgb(0, 200, 255))
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("blocks").style(
                Style::default()
                    .fg(Color::Rgb(191, 0, 255))
                    .add_modifier(Modifier::BOLD),
            ),
        ];
        let core_header = Row::new(core_header_cells)
            .style(Style::default().bg(Color::Rgb(0, 20, 40)))
            .height(1);

        let mut cores = data.cores.cores.clone();
        cores.sort_by_key(|c| c.core_index);

        let core_rows: Vec<Row> = cores
            .iter()
            .take(chunks[3].height.saturating_sub(4) as usize)
            .map(|core| {
                let (status_text, status_color) = match core.status.as_str() {
                    "active" => ("● active", Color::Rgb(0, 255, 0)),
                    "idle" => ("○ idle", Color::Rgb(100, 100, 100)),
                    "stale" => ("◆ stale", Color::Rgb(255, 200, 0)),
                    _ => ("? unknown", Color::Rgb(150, 150, 150)),
                };

                let util_color = if core.utilization_pct >= 50.0 {
                    Color::Rgb(0, 255, 0)
                } else if core.utilization_pct > 0.0 {
                    Color::Rgb(255, 255, 100)
                } else {
                    Color::Rgb(100, 100, 100)
                };

                let gas_str = if core.gas_used >= 1_000_000 {
                    format!("{:.1}M", core.gas_used as f64 / 1_000_000.0)
                } else if core.gas_used >= 1_000 {
                    format!("{:.1}K", core.gas_used as f64 / 1_000.0)
                } else {
                    format!("{}", core.gas_used)
                };

                let gas_color = if core.gas_used > 0 {
                    Color::Rgb(0, 255, 100)
                } else {
                    Color::Rgb(100, 100, 100)
                };

                let da_str = if core.da_load >= 1_000_000 {
                    format!("{:.1}M", core.da_load as f64 / 1_000_000.0)
                } else if core.da_load >= 1_000 {
                    format!("{:.1}K", core.da_load as f64 / 1_000.0)
                } else {
                    format!("{}", core.da_load)
                };

                let da_color = if core.da_load > 0 {
                    Color::Rgb(0, 200, 255)
                } else {
                    Color::Rgb(100, 100, 100)
                };

                let blocks_str = format!("{}/{}", core.active_blocks, core.window_blocks);
                let blocks_color = if core.active_blocks > 0 {
                    Color::Rgb(191, 0, 255)
                } else {
                    Color::Rgb(100, 100, 100)
                };

                Row::new(vec![
                    Cell::from(format!("C{}", core.core_index)).style(
                        Style::default()
                            .fg(Color::Rgb(150, 255, 255))
                            .add_modifier(Modifier::BOLD),
                    ),
                    Cell::from(status_text).style(Style::default().fg(status_color)),
                    Cell::from(format!("{:>5.1}%", core.utilization_pct))
                        .style(Style::default().fg(util_color).add_modifier(Modifier::BOLD)),
                    Cell::from(format!("{:>6}", gas_str))
                        .style(Style::default().fg(gas_color).add_modifier(Modifier::BOLD)),
                    Cell::from(format!("{:>6}", da_str))
                        .style(Style::default().fg(da_color).add_modifier(Modifier::BOLD)),
                    Cell::from(format!("{:>7}", blocks_str)).style(
                        Style::default()
                            .fg(blocks_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                ])
            })
            .collect();

        let core_title = format!(
            " cores ({} total: {} active, {} idle) ",
            summary.total_cores, summary.active_cores, summary.idle_cores
        );

        let core_table = Table::new(
            core_rows,
            &[
                Constraint::Length(5),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(9),
            ],
        )
        .header(core_header)
        .block(
            Block::default()
                .title(core_title)
                .borders(Borders::ALL)
                .border_style(border_style)
                .style(Style::default().bg(Color::Black)),
        )
        .style(Style::default().bg(Color::Black));

        f.render_widget(core_table, chunks[3]);
    } else {
        let msg = if let Some(err) = core_error {
            format!(" cores  (error: {}) ", err)
        } else {
            " cores  (no active cores in last 24h) ".to_string()
        };
        let empty = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "  Waiting for work package events...",
                Style::default().fg(Color::Rgb(100, 100, 100)),
            )),
        ])
        .block(
            Block::default()
                .title(msg)
                .borders(Borders::ALL)
                .border_style(
                    Style::default()
                        .fg(Color::Rgb(255, 200, 0))
                        .add_modifier(Modifier::BOLD),
                ),
        )
        .style(Style::default().bg(Color::Black));
        f.render_widget(empty, chunks[3]);
    }
}

fn render_footer(f: &mut Frame, area: Rect, data: &DashboardData, selected_panel: usize) {
    let width = area.width as usize;
    let top_border = format!("╔{}╗", "═".repeat(width.saturating_sub(2)));
    let bottom_border = format!("╚{}╝", "═".repeat(width.saturating_sub(2)));

    let footer_content = if let Some(error) = &data.error {
        let error_line_width = 2 + 9 + error.len() + 1; // "║ " + "⚠ ERROR: " + error + "║"
        let padding = width.saturating_sub(error_line_width);

        vec![
            Line::from(vec![Span::styled(
                top_border.clone(),
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )]),
            Line::from(vec![
                Span::styled("║ ", Style::default().fg(Color::Red)),
                Span::styled(
                    "⚠ ERROR: ",
                    Style::default()
                        .fg(Color::Red)
                        .bg(Color::Rgb(50, 0, 0))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(error, Style::default().fg(Color::Rgb(255, 100, 100))),
                Span::styled(" ".repeat(padding), Style::default()), // Dynamic padding
                Span::styled("║", Style::default().fg(Color::Red)),
            ]),
            Line::from(vec![Span::styled(
                bottom_border.clone(),
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )]),
        ]
    } else {
        // Tab label styles
        let active_tab = Style::default()
            .fg(Color::Rgb(0, 255, 255))
            .add_modifier(Modifier::BOLD);
        let inactive_tab = Style::default().fg(Color::Rgb(100, 100, 100));

        let events_style = if selected_panel == 0 {
            active_tab
        } else {
            inactive_tab
        };
        let metrics_style = if selected_panel == 1 {
            active_tab
        } else {
            inactive_tab
        };
        let nodes_style = if selected_panel == 2 {
            active_tab
        } else {
            inactive_tab
        };

        // "[HH:MM:SS] " = 11
        // "[q] quit  " = 10
        // "[tab] " = 6
        // "events" = 6, "·" = 1, "nodes" = 5, "·" = 1, "metrics" = 7 = 20
        // "  " = 2
        // "[↑↓] scroll  " = 13
        // "[home] top" = 10
        // Total content = 72 chars
        let nav_content_chars = 72;
        let total_padding = if width > nav_content_chars + 2 {
            width - nav_content_chars - 2
        } else {
            0
        };
        let left_padding = total_padding / 2;
        let right_padding = total_padding - left_padding;

        vec![
            Line::from(vec![Span::styled(
                top_border,
                Style::default()
                    .fg(Color::Rgb(0, 200, 100))
                    .add_modifier(Modifier::BOLD),
            )]),
            Line::from(vec![
                Span::styled("║", Style::default().fg(Color::Rgb(0, 200, 100))),
                Span::styled(" ".repeat(left_padding), Style::default()),
                Span::styled(
                    format!("[{}] ", Local::now().format("%H:%M:%S")),
                    Style::default()
                        .fg(Color::Rgb(100, 255, 100))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    "[q]",
                    Style::default()
                        .fg(Color::Yellow)
                        .bg(Color::Rgb(50, 50, 0))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" quit  ", Style::default().fg(Color::White)),
                Span::styled(
                    "[tab]",
                    Style::default()
                        .fg(Color::Cyan)
                        .bg(Color::Rgb(0, 50, 50))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" ", Style::default().fg(Color::White)),
                Span::styled("events", events_style),
                Span::styled("·", Style::default().fg(Color::Rgb(80, 80, 80))),
                Span::styled("metrics", metrics_style),
                Span::styled("·", Style::default().fg(Color::Rgb(80, 80, 80))),
                Span::styled("nodes", nodes_style),
                Span::styled("  ", Style::default().fg(Color::White)),
                Span::styled(
                    "[↑↓]",
                    Style::default()
                        .fg(Color::Rgb(255, 165, 0))
                        .bg(Color::Rgb(50, 30, 0))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" scroll  ", Style::default().fg(Color::White)),
                Span::styled(
                    "[home]",
                    Style::default()
                        .fg(Color::Blue)
                        .bg(Color::Rgb(0, 0, 50))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" top", Style::default().fg(Color::White)),
                Span::styled(" ".repeat(right_padding), Style::default()),
                Span::styled("║", Style::default().fg(Color::Rgb(0, 200, 100))),
            ]),
            Line::from(vec![Span::styled(
                bottom_border,
                Style::default()
                    .fg(Color::Rgb(0, 200, 100))
                    .add_modifier(Modifier::BOLD),
            )]),
        ]
    };

    let footer = Paragraph::new(footer_content)
        .style(Style::default().bg(Color::Black))
        .alignment(Alignment::Left);
    f.render_widget(footer, area);
}

fn main() -> Result<()> {
    let args = Args::parse();
    let api_base = format!("http://{}:{}", args.host, args.port);

    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Initial data fetch
    let data = Arc::new(Mutex::new(fetch_data(&api_base).unwrap_or_default()));

    // Start update thread
    let data_clone = Arc::clone(&data);
    let api_base_clone = api_base.clone();
    let refresh_interval = Duration::from_millis(args.refresh_ms);
    thread::spawn(move || {
        update_loop(api_base_clone, data_clone, refresh_interval);
    });

    // Create app state
    let mut app = App::new(data);

    // Main loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| ui(f, &app))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press && app.on_key(key.code) {
                    break;
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }

    // Clean up
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
