#!/bin/bash

# TART Telemetry System Status Script
# Shows comprehensive status of the telemetry system

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
API_PORT=8080
TELEMETRY_PORT=9000
SHOW_EVENTS=10
WATCH_MODE=false
JSON_OUTPUT=false

# Parse command line arguments
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -a, --api-port         API port (default: 8080)"
    echo "  -t, --telemetry-port   Telemetry port (default: 9000)"
    echo "  -e, --events COUNT     Number of recent events to show (default: 10)"
    echo "  -w, --watch            Watch mode - refresh every 5 seconds"
    echo "  -j, --json             Output raw JSON"
    echo "  -h, --help             Show this help message"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -a|--api-port)
            API_PORT="$2"
            shift 2
            ;;
        -t|--telemetry-port)
            TELEMETRY_PORT="$2"
            shift 2
            ;;
        -e|--events)
            SHOW_EVENTS="$2"
            shift 2
            ;;
        -w|--watch)
            WATCH_MODE=true
            shift
            ;;
        -j|--json)
            JSON_OUTPUT=true
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Event type mapping function (for compatibility)
get_event_type_name() {
    case $1 in
        0) echo "Dropped";;
        10) echo "Status";;
        11) echo "BestBlockChanged";;
        12) echo "FinalizedBlockChanged";;
        13) echo "SyncStatusChanged";;
        20) echo "ConnectionRefused";;
        21) echo "ConnectingIn";;
        22) echo "ConnectionInFailed";;
        23) echo "ConnectedIn";;
        24) echo "ConnectingOut";;
        25) echo "ConnectionOutFailed";;
        26) echo "ConnectedOut";;
        27) echo "Disconnected";;
        40) echo "Authoring";;
        41) echo "AuthoringFailed";;
        42) echo "Authored";;
        43) echo "Importing";;
        44) echo "BlockVerificationFailed";;
        45) echo "BlockVerified";;
        46) echo "BlockExecutionFailed";;
        47) echo "BlockExecuted";;
        *) echo "Unknown($1)";;
    esac
}

# Check if API is accessible
check_api() {
    if ! curl -sf "http://localhost:$API_PORT/health" > /dev/null; then
        echo -e "${RED}Error: TART API is not accessible on port $API_PORT${NC}"
        echo "Is the TART backend running?"
        exit 1
    fi
}

# Check if telemetry port is open
check_telemetry() {
    if command -v nc &> /dev/null; then
        if ! nc -z localhost "$TELEMETRY_PORT" 2>/dev/null; then
            echo -e "${YELLOW}Warning: Telemetry port $TELEMETRY_PORT appears to be closed${NC}"
        fi
    fi
}

# Get process information
show_processes() {
    echo -e "${CYAN}=== Running Processes ===${NC}"
    echo ""
    
    # Check TART backend
    TART_PID=$(pgrep -f "tart-backend" | head -1)
    if [ -n "$TART_PID" ]; then
        echo -e "${GREEN}✓${NC} TART Backend (PID: $TART_PID)"
        ps -p "$TART_PID" -o %cpu,%mem,etime | tail -1 | while read cpu mem time; do
            echo "  CPU: ${cpu}%, Memory: ${mem}%, Uptime: ${time}"
        done
    else
        echo -e "${RED}✗${NC} TART Backend not running"
    fi
    
    # Check PolkaJam nodes
    echo ""
    POLKAJAM_PIDS=$(pgrep -f "polkajam")
    if [ -n "$POLKAJAM_PIDS" ]; then
        NODE_COUNT=$(echo "$POLKAJAM_PIDS" | wc -l | tr -d ' ')
        echo -e "${GREEN}✓${NC} PolkaJam Nodes: $NODE_COUNT running"
        echo "$POLKAJAM_PIDS" | while read pid; do
            if ps -p "$pid" > /dev/null 2>&1; then
                ps -p "$pid" -o pid,command | tail -1 | grep -o "dev-validator [0-9]*" || echo "  Node PID: $pid"
            fi
        done
    else
        echo -e "${YELLOW}⚠${NC} No PolkaJam nodes running"
    fi
    echo ""
}

# Show node status
show_nodes() {
    echo -e "${CYAN}=== Connected Nodes ===${NC}"
    echo ""
    
    NODES_JSON=$(curl -s "http://localhost:$API_PORT/nodes")
    NODE_COUNT=$(echo "$NODES_JSON" | jq '.nodes | length')
    
    if [ "$NODE_COUNT" -eq 0 ]; then
        echo -e "${YELLOW}No nodes connected${NC}"
    else
        echo "$NODES_JSON" | jq -r '.nodes[] | 
            "\(.implementation_name) v\(.implementation_version)\n" +
            "  ID: \(.node_id[0:8])...\(.node_id[-8:])\n" +
            "  Connected: \(.connected_at)\n" +
            "  Events: \(.event_count)\n" +
            "  Status: \(if .is_connected then "🟢 Connected" else "🔴 Disconnected" end)\n"'
    fi
    echo ""
}

# Show event statistics
show_event_stats() {
    echo -e "${CYAN}=== Event Statistics ===${NC}"
    echo ""
    
    # Get event type counts
    EVENTS=$(curl -s "http://localhost:$API_PORT/events?limit=1000")
    
    echo "$EVENTS" | jq -r '.events | 
        group_by(.event_type) | 
        map({
            type: .[0].event_type,
            count: length
        }) | 
        sort_by(.count) | 
        reverse | 
        .[] | 
        "\(.type)\t\(.count)"' | while read type count; do
        TYPE_NAME=$(get_event_type_name $type)
        printf "%-25s %5d\n" "$TYPE_NAME:" "$count"
    done
    
    echo ""
    TOTAL_EVENTS=$(echo "$EVENTS" | jq '.events | length')
    echo -e "${GREEN}Total events in last 1000: $TOTAL_EVENTS${NC}"
    echo ""
}

# Show recent events
show_recent_events() {
    echo -e "${CYAN}=== Recent Events (Last $SHOW_EVENTS) ===${NC}"
    echo ""
    
    curl -s "http://localhost:$API_PORT/events?limit=$SHOW_EVENTS" | \
    jq -r '.events[] | 
        "\(.timestamp | split("T")[1] | split(".")[0]) \(.event_type) \(.node_id[0:8])..."' | \
    while read time type node; do
        TYPE_NAME=$(get_event_type_name $type)
        printf "%-12s %-25s %s\n" "$time" "$TYPE_NAME" "$node"
    done
    echo ""
}

# Show block information
show_block_info() {
    echo -e "${CYAN}=== Blockchain Status ===${NC}"
    echo ""
    
    # Get latest block info from events
    LATEST_BEST=$(curl -s "http://localhost:$API_PORT/events?limit=100" | \
        jq -r '.events[] | select(.event_type == 11) | .data.BestBlockChanged.slot' | \
        head -1)
    
    LATEST_FINALIZED=$(curl -s "http://localhost:$API_PORT/events?limit=100" | \
        jq -r '.events[] | select(.event_type == 12) | .data.FinalizedBlockChanged.slot' | \
        head -1)
    
    # Get total blocks authored from stats endpoint
    STATS=$(curl -s "http://localhost:$API_PORT/stats")
    TOTAL_AUTHORED=$(echo "$STATS" | jq -r '.total_blocks_authored // 0')
    
    if [ -n "$LATEST_BEST" ]; then
        echo -e "Best Block:      ${GREEN}#$LATEST_BEST${NC}"
    else
        echo -e "Best Block:      ${YELLOW}Unknown${NC}"
    fi
    
    if [ -n "$LATEST_FINALIZED" ]; then
        echo -e "Finalized Block: ${GREEN}#$LATEST_FINALIZED${NC}"
    else
        echo -e "Finalized Block: ${YELLOW}Unknown${NC}"
    fi
    
    echo -e "Blocks Authored: ${GREEN}$TOTAL_AUTHORED${NC} (total)"
    echo ""
}

# Show system summary
show_summary() {
    echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║       TART Telemetry System Status             ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════╝${NC}"
    echo ""
    
    # Get basic stats
    HEALTH=$(curl -s "http://localhost:$API_PORT/health")
    VERSION=$(echo "$HEALTH" | jq -r '.version // "unknown"')
    
    echo -e "Version:    ${GREEN}$VERSION${NC}"
    echo -e "API:        ${GREEN}http://localhost:$API_PORT${NC}"
    echo -e "Telemetry:  ${GREEN}localhost:$TELEMETRY_PORT${NC}"
    echo ""
}

# JSON output mode
json_output() {
    NODES=$(curl -s "http://localhost:$API_PORT/nodes")
    EVENTS=$(curl -s "http://localhost:$API_PORT/events?limit=$SHOW_EVENTS")
    HEALTH=$(curl -s "http://localhost:$API_PORT/health")
    
    jq -n \
        --argjson health "$HEALTH" \
        --argjson nodes "$NODES" \
        --argjson events "$EVENTS" \
        '{
            health: $health,
            nodes: $nodes,
            recent_events: $events,
            timestamp: now | strftime("%Y-%m-%d %H:%M:%S")
        }'
}

# Main display function
display_status() {
    if [ "$JSON_OUTPUT" = true ]; then
        json_output
        return
    fi
    
    clear
    show_summary
    show_processes
    show_nodes
    show_block_info
    show_event_stats
    show_recent_events
    
    if [ "$WATCH_MODE" = true ]; then
        echo -e "${YELLOW}Refreshing every 5 seconds... (Press Ctrl+C to stop)${NC}"
    fi
}

# Main execution
check_api
check_telemetry

if [ "$WATCH_MODE" = true ]; then
    while true; do
        display_status
        sleep 5
    done
else
    display_status
fi