# TART Dash - Real-time Terminal Dashboard

A high-performance terminal dashboard for TART (Testing, Analytics and Research Telemetry) that provides real-time monitoring of blockchain nodes, events, and system health.

## Overview

TART Dash is a sophisticated terminal user interface (TUI) application built with Rust that displays real-time telemetry data from TART-enabled blockchain nodes. It features an adaptive layout system that automatically adjusts to terminal size, smooth scrolling, and color-coded status indicators for quick visual assessment of system health.

### Key Features

- **Real-time Updates**: Live data refresh with configurable intervals (default: 500ms)
- **Adaptive Terminal Layout**: Automatically adjusts to terminal dimensions
- **Multi-panel Interface**: Simultaneous view of stats, nodes, and events
- **Keyboard Navigation**: Intuitive controls for scrolling and tab switching
- **Color-coded Status**: Visual indicators for health, connection status, and event types
- **Performance Optimized**: Minimal resource usage with efficient rendering

### Screenshot Preview

*The dashboard displays:*
- **Header**: Stylized ASCII art logo with gradient colors
- **Left Panel**: Network statistics, block information, and system health
- **Right Panel**: Tabbed interface showing connected nodes or recent events
- **Footer**: Live status indicator and keyboard shortcuts

## Features

### Real-time Data Visualization

- **Network Statistics**: Live count of connected nodes, total events, and blocks authored
- **Block Information**: Current best block and finalized block numbers
- **System Health**: Overall health status with component-level breakdown
- **Node Monitoring**: Real-time status of all connected nodes with version and event counts
- **Event Stream**: Color-coded event log with timestamps and node identifiers

### Adaptive Terminal Layouts

The dashboard uses Ratatui's constraint-based layout system to ensure optimal display across different terminal sizes:

- Responsive panels that adjust to terminal width
- Scrollable content areas for long lists
- Automatic text truncation with ellipsis for narrow terminals
- Minimum terminal size: 80x24 characters recommended

### Keyboard Shortcuts

Navigation is entirely keyboard-driven for efficiency:

- **Tab**: Switch between Nodes and Events tabs
- **↑/↓**: Scroll up/down by one item
- **PageUp/PageDown**: Scroll by 10 items
- **Home**: Jump to the beginning of the list
- **q/Esc**: Quit the application

### Color Schemes and Theming

The dashboard uses a carefully designed color palette:

- **Cyan/Blue gradient**: TART logo and network statistics
- **Green**: Healthy status, online nodes, successful events
- **Yellow**: Warning states, pending connections
- **Red**: Errors, offline nodes, failed events
- **Gray tones**: UI borders and inactive elements

## Installation

### Building from Source

```bash
# Clone the repository
git clone https://github.com/your-org/tart-backend.git
cd tart-backend

# Build the dashboard
cargo build --release --bin tart-dash

# The binary will be available at:
# target/release/tart-dash
```

### Using Cargo Install

```bash
# Install directly from the repository
cargo install --git https://github.com/your-org/tart-backend.git --bin tart-dash

# Or if you have the repository cloned locally
cargo install --path . --bin tart-dash
```

### Pre-built Binaries

Pre-built binaries are available for:
- Linux (x86_64, ARM64)
- macOS (Intel, Apple Silicon)
- Windows (x86_64)

Download from the [releases page](https://github.com/your-org/tart-backend/releases).

## Usage

### Running the Dashboard

```bash
# Connect to local TART API (default)
tart-dash

# Connect to remote TART API
tart-dash --host api.example.com --port 8080

# Custom refresh interval (milliseconds)
tart-dash --refresh-ms 1000
```

### Command-line Options

```
tart-dash [OPTIONS]

Options:
  --host <HOST>              API host [default: localhost]
  --port <PORT>              API port [default: 8080]
  --refresh-ms <REFRESH_MS>  Refresh interval in milliseconds [default: 500]
  -h, --help                 Print help
  -V, --version              Print version
```

### Configuration Options

The dashboard can be configured through command-line arguments:

- **API Host**: The hostname or IP address of the TART API server
- **API Port**: The port number for the TART API (typically 8080)
- **Refresh Interval**: How often to fetch new data (in milliseconds)

## User Interface

### Layout Description

The interface is divided into four main areas:

1. **Header Section** (9 lines)
   - ASCII art TART logo with gradient coloring
   - Version information and tagline

2. **Left Panel** (30% width)
   - Network Statistics box
   - Blocks information box
   - System Health status box

3. **Right Panel** (70% width)
   - Tab bar for switching views
   - Content area for Nodes or Events

4. **Footer Section** (3 lines)
   - Keyboard shortcuts reminder
   - Live connection status indicator

### Panel Descriptions

#### Network Statistics Panel
- **Connected Nodes**: Number of currently active nodes
- **Total Events**: Cumulative event count from all nodes
- **Blocks Authored**: Total blocks created by the network

#### Blocks Panel
- **Best Block**: The highest block number seen
- **Finalized Block**: The latest finalized block number

#### System Health Panel
- **Overall Status**: Healthy/Degraded/Error
- **Uptime**: Time since API server started
- **Component Status**: Individual health checks for subsystems

#### Nodes Tab
Displays a table with:
- Node name and ID (truncated)
- Implementation version
- Event count
- Connection status (Online/Offline)

#### Events Tab
Shows a chronological list of events with:
- Timestamp (HH:MM:SS format)
- Event type with emoji indicator
- Node ID that generated the event

### Status Indicators

- **Live Indicator**: Green dot for active connection, yellow for connecting
- **Node Status**: Green circle for online, red for offline
- **Health Status**: Color-coded based on severity (green/yellow/red)

## Keyboard Controls

### Navigation

- **Tab**: Toggle between Nodes and Events view
- **↑ (Up Arrow)**: Move selection up by one item
- **↓ (Down Arrow)**: Move selection down by one item
- **Page Up**: Jump up by 10 items
- **Page Down**: Jump down by 10 items
- **Home**: Go to the first item in the list

### Application Control

- **q**: Quit the application
- **Esc**: Alternative quit command

### Scrolling Behavior

- Lists maintain scroll position when switching tabs
- Scroll position resets when new data arrives that changes list length
- Visual indicator (▸) shows current selection in lists

## Performance

### Resource Usage

- **CPU**: < 1% during idle, 2-5% during updates
- **Memory**: ~10-20 MB typical usage
- **Network**: Minimal bandwidth (JSON API calls)

### Update Frequency

- Default refresh rate: 500ms (2 updates per second)
- Configurable from 100ms to 10000ms
- Higher frequencies provide smoother updates but use more resources

### Terminal Requirements

- **Minimum Size**: 80x24 characters
- **Recommended Size**: 120x40 characters or larger
- **Terminal Emulator**: Any modern terminal with 256-color support
- **Font**: Monospace font with Unicode support recommended

## Customization

### Color Themes

While the current version uses a fixed color scheme, the architecture supports theming:

- Colors are defined using RGB values for precise control
- Gradient effects on the logo provide visual appeal
- High contrast between foreground and background for readability

### Layout Options

The constraint-based layout system allows for:
- Percentage-based sizing (adapts to terminal size)
- Fixed-height sections (header and footer)
- Minimum size constraints for content areas

### Event Filtering

The Events tab shows the 50 most recent events. Event types include:
- Meta events (Dropped messages)
- Status events (Block changes, sync status)
- Connection events (Connect/disconnect)
- Block operations (Authoring, importing, verification)
- Safrole ticket events (Generation, transfer)

## Troubleshooting

### Connection Issues

**Problem**: Dashboard shows "CONNECTING" permanently
- **Solution**: Check that the TART API is running and accessible
- **Verify**: `curl http://localhost:8080/api/health`

**Problem**: "Failed to fetch data" error
- **Solution**: Ensure correct host and port settings
- **Check**: Network connectivity and firewall rules

### Display Problems

**Problem**: Garbled or misaligned text
- **Solution**: Ensure terminal supports UTF-8 and 256 colors
- **Try**: Different terminal emulator (iTerm2, Alacritty, Windows Terminal)

**Problem**: Logo appears as question marks
- **Solution**: Use a terminal font with Unicode block character support
- **Recommended fonts**: FiraCode, JetBrains Mono, Cascadia Code

### Performance Tips

**High CPU Usage**:
- Increase refresh interval: `--refresh-ms 1000`
- Close other terminal applications
- Check for terminal GPU acceleration settings

**Slow Updates**:
- Verify network latency to API server
- Reduce terminal size to decrease rendering overhead
- Disable terminal transparency/blur effects

**Memory Growth**:
- The dashboard maintains a fixed-size event buffer
- Memory usage should stabilize after initial startup
- Report persistent growth as a bug

## Contributing

Contributions are welcome! The dashboard is part of the TART backend project. Key files:
- `src/bin/tart-dash.rs`: Main dashboard implementation
- Uses `ratatui` for terminal UI
- Uses `crossterm` for terminal control
- Uses `reqwest` for API communication

## License

This project is part of the TART backend system. See the main project LICENSE file for details.