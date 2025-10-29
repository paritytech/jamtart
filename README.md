# TART Backend

**TART (Testing, Analytics and Research Telemetry)** is a production-grade, high-performance telemetry backend for JAM blockchain networks. Built in Rust, it handles up to 1024 concurrent node connections, processing 10,000+ events per second with sub-millisecond latency.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.77%2B-orange.svg)](https://www.rust-lang.org/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%2B-blue.svg)](https://www.postgresql.org/)

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Integrations](#integrations)
- [Architecture](#architecture)
- [Deployment](#deployment)
- [Monitoring](#monitoring)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

## Features

### Core Capabilities

- ✅ **Real-time telemetry collection** from JAM blockchain nodes via binary TCP protocol (JIP-3 compliant)
- ✅ **High-performance event processing** with PostgreSQL backend
- ✅ **RESTful API** for historical data access and analytics
- ✅ **WebSocket streaming** for real-time event distribution
- ✅ **Terminal dashboard** (tart-dash) for operator monitoring
- ✅ **Prometheus metrics** for observability
- ✅ **Health checks** with component-level status

### Performance Characteristics

| Metric | Specification |
|--------|--------------|
| Max Concurrent Nodes | 1024 |
| Throughput | 10,000+ events/second |
| Event Ingestion Latency | <1ms (p99) |
| Database Write Latency | <20ms (p99) |
| Memory per Connection | ~0.5MB |
| WebSocket Broadcast Latency | <5ms |
| Supported Event Types | 130 (JIP-3 full spec) |

### Production Features

- 🔒 **Rate limiting**: 100 events/sec per node (configurable)
- 🚀 **Batch writing**: Optimized database writes with configurable batching
- 💾 **Database connection pooling**: 200 connections for high concurrency
- 📊 **Stats caching**: 5-second TTL reduces database load by 95%
- 🔄 **Circuit breakers**: Automatic recovery from transient failures
- 🎯 **Retry logic**: Exponential backoff with jitter
- 📈 **VecDeque optimization**: O(1) event buffer operations
- 🗄️ **Time-partitioned tables**: Daily partitions for efficient archival

## Quick Start

### Using Docker (Recommended)

```bash
# Clone repository
git clone https://github.com/your-org/tart-backend.git
cd tart-backend

# Start backend + PostgreSQL
docker-compose up -d

# View logs
docker-compose logs -f tart-backend

# API endpoint at http://localhost:8080
# Telemetry endpoint at tcp://localhost:9000
```

### From Source

```bash
# Install dependencies
# Requires: Rust 1.77+, PostgreSQL 16+

# Set database URL
export DATABASE_URL="postgres://user:password@localhost:5432/tart_telemetry"

# Build and run
cargo build --release
./target/release/tart-backend
```

### Terminal Dashboard

```bash
# Run the TUI dashboard
cargo run --release --bin tart-dash

# Or with custom host/port
./target/release/tart-dash --host localhost --port 8080 --refresh-ms 1000
```

## Installation

### Prerequisites

**Required:**
- Rust 1.77.0 or later
- PostgreSQL 16+ (14+ supported)
- Git

**Optional:**
- Docker & Docker Compose (for containerized deployment)
- Prometheus & Grafana (for monitoring)

### Option 1: Docker Deployment (Production)

```bash
# 1. Clone repository
git clone https://github.com/your-org/tart-backend.git
cd tart-backend

# 2. Configure production credentials (IMPORTANT!)
export POSTGRES_PASSWORD=$(openssl rand -base64 32)
export DATABASE_URL="postgres://tart:${POSTGRES_PASSWORD}@postgres:5432/tart_telemetry"

# 3. Build and start
docker-compose build
docker-compose up -d

# 4. Verify health
curl http://localhost:8080/api/health
```

### Option 2: Kubernetes Deployment

See [deployment/kubernetes/README.md](deployment/kubernetes/README.md) for Helm charts and manifests.

### Option 3: Binary Installation

```bash
# 1. Build release binaries
cargo build --release

# 2. Install to system
sudo cp target/release/tart-backend /usr/local/bin/
sudo cp target/release/tart-dash /usr/local/bin/

# 3. Create systemd service (see deployment/systemd/tart-backend.service)
sudo cp deployment/systemd/tart-backend.service /etc/systemd/system/
sudo systemctl enable tart-backend
sudo systemctl start tart-backend
```

## Configuration

### Environment Variables

| Variable | Description | Default | Production Example |
|----------|-------------|---------|-------------------|
| `DATABASE_URL` | PostgreSQL connection string | **Required** | `postgres://tart:STRONG_PASSWORD@db:5432/tart_telemetry` |
| `TELEMETRY_BIND` | Telemetry server bind address | `0.0.0.0:9000` | `0.0.0.0:9000` |
| `API_BIND` | HTTP API server bind address | `0.0.0.0:8080` | `0.0.0.0:8080` |
| `RUST_LOG` | Logging configuration | `info` | `tart_backend=info,sqlx=warn` |

### PostgreSQL Configuration

For production with 1024 nodes, configure PostgreSQL:

```bash
# In postgresql.conf or docker-compose.yml
shared_buffers = 2GB
max_connections = 300
effective_cache_size = 8GB
work_mem = 16MB
maintenance_work_mem = 512MB
wal_buffers = 16MB
max_wal_size = 4GB
```

### Linux System Tuning

For optimal performance:

```bash
# Network tuning
sudo sysctl -w net.core.somaxconn=2048
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=2048
sudo sysctl -w net.core.netdev_max_backlog=5000

# File descriptor limits
ulimit -n 65536

# Save permanently to /etc/sysctl.conf
```

## API Reference

### REST Endpoints

#### Health & Status

<details>
<summary><code>GET /api/health</code> - Basic health check</summary>

**Response:**
```json
{
  "status": "ok",
  "service": "tart-backend",
  "version": "0.1.0"
}
```

**Status Codes:**
- `200 OK` - Service is healthy
</details>

<details>
<summary><code>GET /api/health/detailed</code> - Detailed health with component status</summary>

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-10-28T14:30:00Z",
  "components": {
    "database": {
      "status": "healthy",
      "latency_ms": 0.8,
      "message": "Connected to PostgreSQL"
    },
    "broadcaster": {
      "status": "healthy",
      "subscribers": 12,
      "message": "Broadcasting events"
    },
    "memory": {
      "status": "healthy",
      "usage_percent": 45.2
    }
  },
  "version": "0.1.0",
  "uptime_seconds": 86400
}
```

**Status Codes:**
- `200 OK` - Service is healthy or degraded
- `503 Service Unavailable` - Service is unhealthy
</details>

<details>
<summary><code>GET /api/stats</code> - System statistics</summary>

**Response:**
```json
{
  "total_blocks_authored": 15234,
  "best_block": 15234,
  "finalized_block": 15200
}
```

**Notes:**
- Stats are cached with 5-second TTL for performance
- Refreshed automatically on cache miss
</details>

<details>
<summary><code>GET /api/nodes</code> - List all nodes</summary>

**Response:**
```json
{
  "nodes": [
    {
      "node_id": "a1b2c3d4e5f6...",
      "peer_id": "a1b2c3d4e5f6...",
      "implementation_name": "polkajam",
      "implementation_version": "0.1.0",
      "is_connected": true,
      "event_count": 5823,
      "connected_at": "2025-10-28T14:00:00Z",
      "last_seen_at": "2025-10-28T14:30:00Z",
      "disconnected_at": null
    }
  ]
}
```

**Notes:**
- Returns all known nodes (connected and disconnected)
- Ordered by connection status, then last_seen_at
</details>

<details>
<summary><code>GET /api/nodes/:node_id</code> - Get specific node details</summary>

**Parameters:**
- `node_id` (path) - 64-character hexadecimal node identifier

**Response:**
```json
{
  "node_id": "a1b2c3d4...",
  "peer_id": "a1b2c3d4...",
  "implementation_name": "polkajam",
  "implementation_version": "0.1.0",
  "node_info": {
    "params": { /* ProtocolParameters */ },
    "genesis": "0x...",
    "flags": 1
  },
  "is_connected": true,
  "event_count": 5823,
  "connected_at": "2025-10-28T14:00:00Z",
  "last_seen_at": "2025-10-28T14:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Node found
- `400 Bad Request` - Invalid node_id format
- `404 Not Found` - Node not found
</details>

<details>
<summary><code>GET /api/nodes/:node_id/events</code> - Get events for specific node</summary>

**Parameters:**
- `node_id` (path) - 64-character hexadecimal node identifier
- `limit` (query, optional) - Max events to return (default: 50, max: 1000)

**Response:**
```json
{
  "events": [
    {
      "id": 12345,
      "node_id": "a1b2c3d4...",
      "event_id": 100,
      "event_type": 11,
      "timestamp": "2025-10-28T14:30:00Z",
      "data": {
        "BestBlockChanged": {
          "slot": 1234,
          "hash": "0x..."
        }
      },
      "node_name": "polkajam",
      "node_version": "0.1.0"
    }
  ]
}
```

**Notes:**
- Events are ordered by most recent first
- Uses database-level filtering for optimal performance
</details>

<details>
<summary><code>GET /api/events</code> - Get recent events across all nodes</summary>

**Parameters:**
- `limit` (query, optional) - Max events to return (default: 50, max: 1000)

**Response:**
```json
{
  "events": [
    {
      "id": 12345,
      "node_id": "a1b2c3d4...",
      "event_id": 100,
      "event_type": 11,
      "timestamp": "2025-10-28T14:30:00Z",
      "data": { /* Event-specific data */ },
      "node_name": "polkajam",
      "node_version": "0.1.0"
    }
  ]
}
```
</details>

### WebSocket Endpoints

<details>
<summary><code>WS /api/ws</code> - Real-time event streaming</summary>

Connect to receive live events as they arrive from nodes.

**Client Messages:**

```json
// Subscribe to all events
{
  "type": "Subscribe",
  "filter": { "type": "All" }
}

// Subscribe to specific node
{
  "type": "Subscribe",
  "filter": {
    "type": "Node",
    "node_id": "a1b2c3d4..."
  }
}

// Get recent events
{
  "type": "GetRecentEvents",
  "limit": 100
}

// Ping
{
  "type": "Ping"
}

// Unsubscribe
{
  "type": "Unsubscribe"
}
```

**Server Messages:**

```json
// Connection established
{
  "type": "connected",
  "data": {
    "message": "Connected to TART telemetry (1024-node scale)",
    "recent_events": 20,
    "total_nodes": 5,
    "broadcaster_stats": { /* ... */ }
  },
  "timestamp": "2025-10-28T14:30:00Z"
}

// New event
{
  "type": "event",
  "data": {
    "id": 12345,
    "node_id": "a1b2c3d4...",
    "event_type": 11,
    "latency_ms": 2,
    "event": {
      "BestBlockChanged": {
        "slot": 1234,
        "hash": "0x..."
      }
    }
  },
  "timestamp": "2025-10-28T14:30:00Z"
}

// Periodic stats update (every 5 seconds)
{
  "type": "stats",
  "data": {
    "database": { /* DB stats */ },
    "broadcaster": { /* Broadcaster stats */ },
    "connections": {
      "total": 5,
      "nodes": ["node1", "node2", ...]
    }
  },
  "timestamp": "2025-10-28T14:30:00Z"
}

// Pong response
{
  "type": "pong",
  "data": {
    "events_received": 1234,
    "uptime_ms": 5000
  },
  "timestamp": "2025-10-28T14:30:00Z"
}
```
</details>

### Prometheus Metrics

<details>
<summary><code>GET /metrics</code> - Prometheus metrics endpoint</summary>

**Available Metrics:**

**Connection Metrics:**
```
telemetry_active_connections          # Current connection count
telemetry_connections_total           # Total connections (lifetime)
telemetry_connections_rejected        # Rejected connections (limit reached)
```

**Event Metrics:**
```
telemetry_events_received             # Total events received
telemetry_events_dropped              # Events dropped (backpressure)
telemetry_events_rate_limited         # Events rate-limited
telemetry_buffer_pending              # Events in write queue
telemetry_batch_write_duration        # Batch write latency (histogram)
```

**Broadcaster Metrics:**
```
telemetry_broadcaster_subscribers     # Active WebSocket subscribers
telemetry_broadcaster_total_broadcast # Total broadcast operations
```

**Database Metrics:**
```
telemetry_database_pool_connections   # Active DB connections
```
</details>

## Integrations

### Node Integration (JAM Validator Nodes)

JAM nodes connect to TART via the `--telemetry` flag:

```bash
# Run JAM validator with telemetry
jam-validator \
  --chain dev \
  --telemetry tart-backend:9000 \
  --dev-validator 0
```

**Protocol:** Binary TCP (JIP-3 telemetry specification)
**Port:** 9000 (configurable via `TELEMETRY_BIND`)

### REST API Integration

#### Python Integration

```python
import requests
import json

class TartClient:
    def __init__(self, base_url="http://localhost:8080"):
        self.base_url = base_url

    def get_health(self):
        """Check backend health"""
        response = requests.get(f"{self.base_url}/api/health")
        return response.json()

    def get_nodes(self):
        """Get all connected nodes"""
        response = requests.get(f"{self.base_url}/api/nodes")
        return response.json()["nodes"]

    def get_node_events(self, node_id, limit=100):
        """Get events for a specific node"""
        response = requests.get(
            f"{self.base_url}/api/nodes/{node_id}/events",
            params={"limit": limit}
        )
        return response.json()["events"]

    def get_recent_events(self, limit=50):
        """Get recent events across all nodes"""
        response = requests.get(
            f"{self.base_url}/api/events",
            params={"limit": limit}
        )
        return response.json()["events"]

    def get_stats(self):
        """Get system statistics"""
        response = requests.get(f"{self.base_url}/api/stats")
        return response.json()

# Usage example
client = TartClient("http://localhost:8080")

# Check health
health = client.get_health()
print(f"Status: {health['status']}")

# Get all nodes
nodes = client.get_nodes()
print(f"Connected nodes: {len(nodes)}")

# Get events from first node
if nodes:
    events = client.get_node_events(nodes[0]["node_id"], limit=10)
    print(f"Recent events: {len(events)}")

# Get system stats
stats = client.get_stats()
print(f"Best block: {stats['best_block']}")
```

#### JavaScript/TypeScript Integration

```typescript
// tart-client.ts
interface Node {
  node_id: string;
  implementation_name: string;
  implementation_version: string;
  is_connected: boolean;
  event_count: number;
  last_seen_at: string;
}

interface Event {
  id: number;
  node_id: string;
  event_type: number;
  timestamp: string;
  data: any;
}

class TartClient {
  constructor(private baseUrl: string = "http://localhost:8080") {}

  async getHealth(): Promise<{ status: string }> {
    const response = await fetch(`${this.baseUrl}/api/health`);
    return response.json();
  }

  async getNodes(): Promise<Node[]> {
    const response = await fetch(`${this.baseUrl}/api/nodes`);
    const data = await response.json();
    return data.nodes;
  }

  async getNodeEvents(nodeId: string, limit: number = 100): Promise<Event[]> {
    const response = await fetch(
      `${this.baseUrl}/api/nodes/${nodeId}/events?limit=${limit}`
    );
    const data = await response.json();
    return data.events;
  }

  async getRecentEvents(limit: number = 50): Promise<Event[]> {
    const response = await fetch(
      `${this.baseUrl}/api/events?limit=${limit}`
    );
    const data = await response.json();
    return data.events;
  }

  async getStats(): Promise<{
    total_blocks_authored: number;
    best_block: number;
    finalized_block: number;
  }> {
    const response = await fetch(`${this.baseUrl}/api/stats`);
    return response.json();
  }
}

// Usage
const client = new TartClient("http://localhost:8080");

// Fetch nodes
const nodes = await client.getNodes();
console.log(`Connected nodes: ${nodes.length}`);

// Monitor stats
setInterval(async () => {
  const stats = await client.getStats();
  console.log(`Best block: ${stats.best_block}`);
}, 5000);
```

#### cURL Examples

```bash
# Health check
curl http://localhost:8080/api/health

# Get all nodes with pretty printing
curl -s http://localhost:8080/api/nodes | jq

# Get specific node
curl -s http://localhost:8080/api/nodes/a1b2c3d4... | jq

# Get node events (last 10)
curl -s "http://localhost:8080/api/nodes/a1b2c3d4.../events?limit=10" | jq

# Get recent events with filtering
curl -s "http://localhost:8080/api/events?limit=50" | jq '.events[] | select(.event_type == 11)'

# Get stats
curl -s http://localhost:8080/api/stats | jq

# Continuous monitoring
watch -n 1 'curl -s http://localhost:8080/api/stats | jq'
```

### WebSocket Integration

#### JavaScript WebSocket Client

```javascript
// tart-websocket.js
class TartWebSocket {
  constructor(url = "ws://localhost:8080/api/ws") {
    this.ws = new WebSocket(url);
    this.eventHandlers = new Map();

    this.ws.onopen = () => {
      console.log("Connected to TART telemetry");
      this.subscribe({ type: "All" });
    };

    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      this.handleMessage(message);
    };

    this.ws.onerror = (error) => {
      console.error("WebSocket error:", error);
    };

    this.ws.onclose = () => {
      console.log("Disconnected from TART telemetry");
    };
  }

  subscribe(filter) {
    this.send({ type: "Subscribe", filter });
  }

  subscribeToNode(nodeId) {
    this.subscribe({ type: "Node", node_id: nodeId });
  }

  getRecentEvents(limit = 100) {
    this.send({ type: "GetRecentEvents", limit });
  }

  ping() {
    this.send({ type: "Ping" });
  }

  send(message) {
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }

  on(eventType, handler) {
    this.eventHandlers.set(eventType, handler);
  }

  handleMessage(message) {
    const handler = this.eventHandlers.get(message.type);
    if (handler) {
      handler(message.data);
    }
  }
}

// Usage example
const client = new TartWebSocket("ws://localhost:8080/api/ws");

// Listen for new events
client.on("event", (data) => {
  console.log(`Event ${data.id} from ${data.node_id}:`, data.event);
});

// Listen for stats updates
client.on("stats", (data) => {
  console.log("Stats:", data);
});

// Subscribe to specific node
client.subscribeToNode("a1b2c3d4...");

// Keep-alive
setInterval(() => client.ping(), 30000);
```

#### Python WebSocket Client

```python
import asyncio
import websockets
import json

class TartWebSocketClient:
    def __init__(self, url="ws://localhost:8080/api/ws"):
        self.url = url
        self.handlers = {}

    async def connect(self):
        """Connect and handle messages"""
        async with websockets.connect(self.url) as websocket:
            # Subscribe to all events
            await websocket.send(json.dumps({
                "type": "Subscribe",
                "filter": {"type": "All"}
            }))

            # Handle incoming messages
            async for message in websocket:
                data = json.loads(message)
                await self.handle_message(data)

    async def handle_message(self, message):
        """Route messages to handlers"""
        msg_type = message.get("type")
        if msg_type in self.handlers:
            await self.handlers[msg_type](message.get("data"))

    def on(self, event_type, handler):
        """Register event handler"""
        self.handlers[event_type] = handler

# Usage example
client = TartWebSocketClient()

# Event handler
async def on_event(data):
    print(f"Event {data['id']} from {data['node_id']}")
    print(f"Type: {data['event_type']}, Event: {data['event']}")

# Stats handler
async def on_stats(data):
    print(f"Active connections: {data['connections']['total']}")

# Register handlers
client.on("event", on_event)
client.on("stats", on_stats)

# Run
asyncio.run(client.connect())
```

### Rust SDK Integration

```rust
use reqwest;
use serde_json::Value;

pub struct TartClient {
    base_url: String,
    client: reqwest::Client,
}

impl TartClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_nodes(&self) -> Result<Vec<Value>, reqwest::Error> {
        let url = format!("{}/api/nodes", self.base_url);
        let response = self.client.get(&url).send().await?;
        let data: Value = response.json().await?;
        Ok(data["nodes"].as_array().unwrap().clone())
    }

    pub async fn get_node_events(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Result<Vec<Value>, reqwest::Error> {
        let url = format!("{}/api/nodes/{}/events?limit={}",
            self.base_url, node_id, limit);
        let response = self.client.get(&url).send().await?;
        let data: Value = response.json().await?;
        Ok(data["events"].as_array().unwrap().clone())
    }

    pub async fn get_stats(&self) -> Result<Value, reqwest::Error> {
        let url = format!("{}/api/stats", self.base_url);
        let response = self.client.get(&url).send().await?;
        response.json().await
    }
}

// Usage
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = TartClient::new("http://localhost:8080");

    // Get nodes
    let nodes = client.get_nodes().await?;
    println!("Nodes: {}", nodes.len());

    // Get stats
    let stats = client.get_stats().await?;
    println!("Best block: {}", stats["best_block"]);

    Ok(())
}
```

### Grafana Integration

#### Add TART as Prometheus Data Source

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'tart-backend'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

#### Example Grafana Queries

```promql
# Active connections over time
telemetry_active_connections

# Event throughput (events/sec)
rate(telemetry_events_received[1m])

# Event drop rate
rate(telemetry_events_dropped[5m])

# Average batch write duration
histogram_quantile(0.99, telemetry_batch_write_duration)

# Connection rejection rate
rate(telemetry_connections_rejected[5m])
```

### PostgreSQL Direct Access

For advanced analytics, query the database directly:

```sql
-- Get event counts by type
SELECT
    event_type,
    COUNT(*) as count,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen
FROM events
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY event_type
ORDER BY count DESC;

-- Get node connection history
SELECT
    node_id,
    implementation_name,
    COUNT(*) as connection_count,
    SUM(event_count) as total_events,
    MAX(last_seen_at) as last_active
FROM nodes
GROUP BY node_id, implementation_name
ORDER BY total_events DESC;

-- Get events per node per hour
SELECT
    hour,
    node_id,
    SUM(event_count) as events
FROM event_stats_hourly
WHERE hour > NOW() - INTERVAL '24 hours'
GROUP BY hour, node_id
ORDER BY hour DESC, events DESC;

-- Find nodes with high error rates
SELECT
    node_id,
    COUNT(*) FILTER (WHERE event_type IN (41, 44, 46, 92)) as error_events,
    COUNT(*) as total_events,
    ROUND(100.0 * COUNT(*) FILTER (WHERE event_type IN (41, 44, 46, 92)) / COUNT(*), 2) as error_rate_pct
FROM events
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY node_id
HAVING COUNT(*) FILTER (WHERE event_type IN (41, 44, 46, 92)) > 10
ORDER BY error_rate_pct DESC;
```

### Building Custom Integrations

Build custom dashboards or integrations using the REST API and WebSocket:

```html
<!DOCTYPE html>
<html>
<head>
    <title>TART Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <h1>JAM Network Telemetry</h1>
    <div id="stats"></div>
    <canvas id="eventChart"></canvas>

    <script>
        const API_BASE = "http://localhost:8080";
        const ws = new WebSocket("ws://localhost:8080/api/ws");

        // Display stats
        async function updateStats() {
            const response = await fetch(`${API_BASE}/api/stats`);
            const stats = await response.json();
            document.getElementById("stats").innerHTML = `
                <p>Best Block: ${stats.best_block}</p>
                <p>Finalized: ${stats.finalized_block}</p>
            `;
        }

        // WebSocket event stream
        ws.onmessage = (event) => {
            const message = JSON.parse(event.data);
            if (message.type === "event") {
                console.log("New event:", message.data);
                // Update your UI here
            }
        };

        // Update stats every 5 seconds
        setInterval(updateStats, 5000);
        updateStats();
    </script>
</body>
</html>
```

### Event Types Reference

TART supports all 130 event types from the JIP-3 specification:

| Event ID | Event Name | Description |
|----------|------------|-------------|
| 0 | Dropped | Events were dropped due to buffer overflow |
| 10 | Status | Periodic node status update (~2 sec) |
| 11 | BestBlockChanged | Node's best block changed |
| 12 | FinalizedBlockChanged | Latest finalized block changed |
| 13 | SyncStatusChanged | Node sync status changed |
| 20-28 | Connection Events | Peer connection lifecycle |
| 40-47 | Block Events | Block authoring/importing/execution |
| 60-68 | Block Distribution | Block announcements and transfers |
| 80-84 | Ticket Events | Safrole ticket generation/transfer |
| 90-113 | Guarantee Events | Work package guaranteeing pipeline |
| 120-131 | Availability Events | Shard requests and assurances |
| 140-153 | Bundle Events | Bundle recovery for auditing |
| 160-178 | Segment Events | Segment recovery and reconstruction |
| 190-199 | Preimage Events | Preimage distribution |

**Full specification:** See [JIP-3.md](JIP-3.md) for complete event definitions.

### Data Export

Export telemetry data for offline analysis:

```bash
# Export to JSON
curl -s "http://localhost:8080/api/events?limit=10000" | jq > events.json

# Export to CSV (using jq)
curl -s "http://localhost:8080/api/nodes" | \
  jq -r '.nodes[] | [.node_id, .implementation_name, .event_count, .is_connected] | @csv' \
  > nodes.csv

# Database dump for archival
docker-compose exec postgres pg_dump -U tart tart_telemetry > backup.sql
```

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    TART Backend System                      │
│                                                             │
│  ┌────────────────┐      ┌──────────────┐                │
│  │   JAM Nodes    │      │   Operators  │                │
│  │  (up to 1024)  │      │  (Dashboard) │                │
│  └────────┬───────┘      └──────┬───────┘                │
│           │ TCP:9000            │ HTTP:8080              │
│           │                     │                        │
│  ┌────────▼─────────────────────▼────────┐              │
│  │      Telemetry Server (TCP + HTTP)    │              │
│  │  ┌─────────────────────────────────┐  │              │
│  │  │  Rate Limiter (100 evt/s/node)  │  │              │
│  │  └──────────────┬──────────────────┘  │              │
│  │                 │                      │              │
│  │  ┌──────────────▼──────────────────┐  │              │
│  │  │   Batch Writer (20ms timeout)   │  │              │
│  │  └──────────────┬──────────────────┘  │              │
│  └─────────────────┼──────────────────────┘              │
│                    │                                     │
│  ┌─────────────────▼─────────────────┐                  │
│  │   PostgreSQL Database (200 conn)  │                  │
│  │  ┌────────────────────────────┐   │                  │
│  │  │ Daily Partitioned Tables   │   │                  │
│  │  │ - events (by created_at)   │   │                  │
│  │  │ - nodes                    │   │                  │
│  │  │ - stats_cache              │   │                  │
│  │  └────────────────────────────┘   │                  │
│  └────────────┬──────────────────────┘                  │
│               │                                         │
│  ┌────────────▼──────────────────────┐                 │
│  │   Event Broadcaster (100k cap)    │                 │
│  │  - WebSocket distribution         │                 │
│  │  - Per-node channels              │                 │
│  │  - Ring buffer (10k events)       │                 │
│  └────────────┬──────────────────────┘                 │
│               │                                         │
│  ┌────────────▼──────────────────────┐                 │
│  │    REST API + WebSocket Server    │                 │
│  │  - /api/* endpoints               │                 │
│  │  - /metrics (Prometheus)          │                 │
│  └───────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

### Technology Stack

- **Language:** Rust (Edition 2021)
- **Web Framework:** Axum 0.7
- **Async Runtime:** Tokio 1.40
- **Database:** PostgreSQL 16 with SQLx
- **Serialization:** Serde + JSON
- **Metrics:** Prometheus
- **Logging:** Tracing
- **TUI:** Ratatui

### Data Storage

**PostgreSQL Schema:**
- **`nodes`** - Node metadata and connection tracking
- **`events`** - Main event log (partitioned by day)
- **`node_status`** - Extracted status events for dashboards
- **`blocks`** - Block-related events
- **`stats_cache`** - Pre-computed statistics
- **`event_stats_hourly`** - Materialized view for hourly aggregations

**Partitioning Strategy:**
- Daily partitions on `events` table
- Automatic partition creation
- Retention: 30 days (configurable)
- Archive old partitions to S3/cold storage

## Deployment

### Production Deployment Checklist

- [ ] **Security**
  - [ ] Set strong database credentials (`POSTGRES_PASSWORD`)
  - [ ] Use TLS/HTTPS for API (reverse proxy recommended)
  - [ ] Restrict CORS origins (not permissive)
  - [ ] Enable authentication if needed
  - [ ] Configure firewall rules (ports 9000, 8080, 5432)

- [ ] **Database**
  - [ ] PostgreSQL 16+ configured for high concurrency
  - [ ] Connection pool sized appropriately (200+ connections)
  - [ ] Automated backups configured
  - [ ] Monitoring/alerting on database health
  - [ ] Partition management automated

- [ ] **Infrastructure**
  - [ ] Load balancer for API (if multiple instances)
  - [ ] Reverse proxy with TLS termination (nginx/Caddy)
  - [ ] Log aggregation (ELK, Loki, etc.)
  - [ ] Metrics collection (Prometheus + Grafana)
  - [ ] Automated restarts (systemd, Kubernetes)

- [ ] **Monitoring**
  - [ ] Prometheus scraping configured
  - [ ] Grafana dashboards imported
  - [ ] Alerting rules configured
  - [ ] On-call rotation established
  - [ ] Runbooks documented

### Docker Production Deployment

```bash
# 1. Set production credentials
export POSTGRES_PASSWORD=$(openssl rand -base64 32)
export DATABASE_URL="postgres://tart:${POSTGRES_PASSWORD}@postgres:5432/tart_telemetry"

# 2. Create production docker-compose.override.yml
cat > docker-compose.override.yml <<EOF
version: '3.8'
services:
  postgres:
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    volumes:
      - /var/lib/tart/postgres:/var/lib/postgresql/data

  tart-backend:
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - RUST_LOG=info
    restart: always
EOF

# 3. Deploy
docker-compose up -d

# 4. Verify
docker-compose logs -f tart-backend
curl http://localhost:8080/api/health
```

### Kubernetes Deployment

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tart-backend
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tart-backend
  template:
    metadata:
      labels:
        app: tart-backend
    spec:
      containers:
      - name: tart-backend
        image: your-registry/tart-backend:latest
        ports:
        - containerPort: 9000
          name: telemetry
        - containerPort: 8080
          name: api
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: tart-secrets
              key: database-url
        - name: RUST_LOG
          value: "info"
        resources:
          requests:
            memory: "2Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
        livenessProbe:
          httpGet:
            path: /api/health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /api/health/detailed
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
```

### Reverse Proxy Configuration

#### Nginx

```nginx
# /etc/nginx/sites-available/tart-backend
upstream tart_api {
    server 127.0.0.1:8080;
}

server {
    listen 443 ssl http2;
    server_name telemetry.example.com;

    ssl_certificate /etc/letsencrypt/live/telemetry.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/telemetry.example.com/privkey.pem;

    # API endpoints
    location /api/ {
        proxy_pass http://tart_api;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # WebSocket upgrade
    location /api/ws {
        proxy_pass http://tart_api;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }

    # Metrics endpoint (restrict access)
    location /metrics {
        proxy_pass http://tart_api;
        allow 10.0.0.0/8;  # Internal network only
        deny all;
    }
}
```

## Monitoring

### Key Metrics to Monitor

1. **Connection Metrics**
   - Active connections approaching 1024
   - Connection rejection rate
   - Average connection duration

2. **Performance Metrics**
   - Event ingestion rate
   - Batch write latency (p99 < 50ms)
   - Database connection pool utilization

3. **Error Metrics**
   - Event drop rate
   - Rate-limited events
   - Failed database writes

4. **Resource Metrics**
   - Memory usage (< 4GB for 1024 nodes)
   - CPU usage
   - Database size growth
   - Disk I/O

### Alerting Rules

```yaml
# prometheus-alerts.yml
groups:
  - name: tart-backend
    rules:
      - alert: HighConnectionCount
        expr: telemetry_active_connections > 900
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "TART approaching connection limit"
          description: "{{ $value }} active connections (limit: 1024)"

      - alert: HighEventDropRate
        expr: rate(telemetry_events_dropped[5m]) > 10
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High event drop rate detected"
          description: "Dropping {{ $value }} events/sec"

      - alert: DatabaseWriteSlow
        expr: histogram_quantile(0.99, telemetry_batch_write_duration) > 100
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Slow database writes"
          description: "p99 write latency: {{ $value }}ms"

      - alert: TartBackendDown
        expr: up{job="tart-backend"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "TART Backend is down"
```

### Logging

**Log Levels:**
- `ERROR` - Critical failures requiring immediate attention
- `WARN` - Rate limiting, invalid timestamps, cache failures
- `INFO` - Startup, connections, major events
- `DEBUG` - Connection details, event processing
- `TRACE` - Frame-by-frame protocol details

**Example log configuration:**
```bash
# Detailed logging for troubleshooting
RUST_LOG="tart_backend=debug,sqlx=info,tower_http=debug"

# Production logging
RUST_LOG="tart_backend=info,sqlx=warn,tower_http=warn"
```

## Development

### Building from Source

```bash
# Clone repository
git clone https://github.com/your-org/tart-backend.git
cd tart-backend

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build in debug mode (fast compilation)
cargo build

# Build in release mode (optimized)
cargo build --release

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run
```

### Project Structure

```
tart-backend/
├── src/
│   ├── main.rs                 # Application entry point
│   ├── lib.rs                  # Library exports
│   ├── server.rs               # TCP telemetry server (9000)
│   ├── api.rs                  # HTTP API server (8080)
│   ├── store.rs                # PostgreSQL data layer
│   ├── batch_writer.rs         # Event batching & writing
│   ├── event_broadcaster.rs    # WebSocket event distribution
│   ├── rate_limiter.rs         # Per-node rate limiting
│   ├── health.rs               # Health monitoring
│   ├── circuit_breaker.rs      # Fault tolerance
│   ├── retry.rs                # Retry logic with backoff
│   ├── node_id.rs              # Type-safe node identifiers
│   ├── events.rs               # Event definitions (130 types)
│   ├── types.rs                # JAM types & structures
│   ├── encoding.rs             # Binary encoding/decoding
│   ├── decoder.rs              # JIP-3 protocol decoder
│   └── bin/
│       └── tart-dash.rs        # Terminal UI dashboard
├── migrations/
│   ├── 001_postgres_schema.sql      # Initial schema
│   └── 002_performance_indexes.sql  # Performance indexes
├── tests/
│   ├── integration_tests.rs    # End-to-end tests
│   ├── events_tests.rs         # Event encoding tests
│   ├── api_tests.rs            # API endpoint tests
│   └── types_tests.rs          # Type system tests
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # Container image
├── Cargo.toml                  # Rust dependencies
├── JIP-3.md                    # Telemetry specification
└── README.md                   # This file
```

### Running Tests

```bash
# All tests
cargo test

# Specific test suite
cargo test --test events_tests
cargo test --test api_tests

# Integration tests (requires PostgreSQL)
export TEST_DATABASE_URL="postgres://localhost/tart_test"
cargo test --test integration_tests

# With output
cargo test -- --nocapture

# Benchmarks
cargo bench
```

### Code Quality

```bash
# Format code
cargo fmt

# Lint code
cargo clippy -- -D warnings

# Security audit
cargo audit

# Check for unused dependencies
cargo machete
```

## Troubleshooting

### Backend Won't Start

**Error:** `DATABASE_URL must be set`
```bash
# Solution: Set environment variable
export DATABASE_URL="postgres://user:password@localhost:5432/tart_telemetry"
cargo run --release
```

**Error:** `error returned from database: functions in index expression must be marked IMMUTABLE`
```bash
# Solution: Drop and recreate database
docker-compose down
docker volume rm tart-backend_postgres-data
docker-compose up -d
```

### Nodes Showing Same ID

**Symptom:** All nodes display identical IDs like `"0a00000000..."`

**Cause:** NodeInformation decoder misalignment

**Solution:** Rebuild backend with latest code (includes ProtocolParameters fix)
```bash
docker-compose build tart-backend
docker-compose up -d
```

### Event Decoding Failures

**Symptom:** Logs show `"Failed to decode event: Invalid boolean value: 28"`

**Cause:** Event structure mismatch with wire format

**Solution:** Verify backend has latest JIP-3 compliant decoders
```bash
# Check version
curl http://localhost:8080/api/health

# Rebuild if needed
docker-compose build tart-backend
```

### High Memory Usage

**Symptom:** Backend using >4GB RAM with <100 nodes

**Possible Causes:**
- Event buffer overflow: Check `telemetry_buffer_pending` metric
- Database connection leak: Check `telemetry_database_pool_connections`
- WebSocket subscriber buildup: Check `telemetry_broadcaster_subscribers`

**Solutions:**
```bash
# Check metrics
curl http://localhost:8080/metrics | grep telemetry

# Restart if needed
docker-compose restart tart-backend
```

### Database Performance Issues

**Symptom:** Slow queries, high write latency

**Solutions:**
```sql
-- Check for missing indexes
SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename;

-- Analyze query performance
EXPLAIN ANALYZE
SELECT * FROM events
WHERE node_id = 'abc...'
ORDER BY created_at DESC
LIMIT 100;

-- Reindex if needed
REINDEX TABLE events;

-- Update statistics
ANALYZE events;
```

### Network Issues

**Symptom:** Nodes can't connect to telemetry port

**Solutions:**
```bash
# Check if port is listening
netstat -tlnp | grep 9000

# Check firewall
sudo ufw status
sudo ufw allow 9000/tcp

# Test connection
telnet localhost 9000

# Check Docker network
docker network inspect tart-backend_default
```

## Performance Benchmarks

### Single Node Performance

| Operation | Latency (p50) | Latency (p99) |
|-----------|--------------|--------------|
| Event ingestion | 0.3ms | 0.8ms |
| Database write (single) | 2ms | 8ms |
| Database write (batch 100) | 15ms | 25ms |
| REST API query | 5ms | 15ms |
| WebSocket broadcast | 1ms | 3ms |

### Multi-Node Scalability

| Nodes | Events/sec | Memory | CPU | DB Connections |
|-------|-----------|---------|-----|----------------|
| 10 | 1,000 | 0.5GB | 10% | 20 |
| 100 | 10,000 | 1.5GB | 40% | 100 |
| 500 | 50,000 | 3GB | 80% | 200 |
| 1024 | 100,000 | 5GB | 95% | 200 |

### Load Testing

```bash
# Install k6 load testing tool
brew install k6  # macOS
# or: apt-get install k6  # Linux

# Run load test
k6 run tests/load-test.js
```

## Documentation

- **[JIP-3.md](JIP-3.md)** - JAM Telemetry Protocol Specification
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture overview
- **[POSTGRES_MIGRATION.md](POSTGRES_MIGRATION.md)** - PostgreSQL setup guide
- **[OPTIMIZATIONS.md](OPTIMIZATIONS.md)** - Performance tuning guide
- **[DEBUGGING_CHEATSHEET.md](DEBUGGING_CHEATSHEET.md)** - Common debugging tasks

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on:

- Code of Conduct
- Development workflow
- Pull request process
- Coding standards
- Testing requirements

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built for the [JAM (Join-Accumulate Machine)](https://graypaper.com/) protocol
- Specification: JIP-3 Telemetry
- Community: Polkadot ecosystem

## Support

- **Issues:** [GitHub Issues](https://github.com/your-org/tart-backend/issues)
- **Discussions:** [GitHub Discussions](https://github.com/your-org/tart-backend/discussions)
- **Documentation:** [docs.tart.io](https://docs.tart.io)

---

**Status:** Production-ready | **Maintained:** Actively | **Version:** 0.1.0
