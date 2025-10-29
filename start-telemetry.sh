#!/bin/bash

# TART Telemetry System Startup Script
# This script starts the TART backend and optionally JAM nodes with telemetry

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
TELEMETRY_PORT=9000
API_PORT=8080
DATABASE_URL="postgres://tart:tart_password@localhost:5432/tart_telemetry"
LOG_LEVEL="info"
NODE_COUNT=0
NODE_TYPE="dev"
CLEANUP_ON_EXIT=true

# Parse command line arguments
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -n, --nodes COUNT      Number of validator nodes to start (default: 0)"
    echo "  -t, --node-type TYPE   Node type: dev, testnet (default: dev)"
    echo "  -p, --telemetry-port   Telemetry server port (default: 9000)"
    echo "  -a, --api-port         API port (default: 8080)"
    echo "  -d, --database         Database URL (default: postgres://tart:tart_password@localhost:5432/tart_telemetry)"
    echo "  -l, --log-level        Log level: error, warn, info, debug (default: info)"
    echo "  -k, --keep-data        Don't cleanup data on exit"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Start only TART backend"
    echo "  $0"
    echo ""
    echo "  # Start TART with 1 dev validator"
    echo "  $0 -n 1"
    echo ""
    echo "  # Start TART with 3 validators, debug logging"
    echo "  $0 -n 3 -l debug"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--nodes)
            NODE_COUNT="$2"
            shift 2
            ;;
        -t|--node-type)
            NODE_TYPE="$2"
            shift 2
            ;;
        -p|--telemetry-port)
            TELEMETRY_PORT="$2"
            shift 2
            ;;
        -a|--api-port)
            API_PORT="$2"
            shift 2
            ;;
        -d|--database)
            DATABASE_URL="$2"
            shift 2
            ;;
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -k|--keep-data)
            CLEANUP_ON_EXIT=false
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

# Check for required binaries
check_binary() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}Error: $1 not found${NC}"
        echo "Please ensure $1 is in your PATH"
        exit 1
    fi
}

echo -e "${BLUE}=== TART Telemetry System Startup ===${NC}"
echo ""

# Check for TART backend
if [ -f "./target/release/tart-backend" ]; then
    TART_BIN="./target/release/tart-backend"
elif [ -f "./tart-backend" ]; then
    TART_BIN="./tart-backend"
else
    echo -e "${YELLOW}Warning: tart-backend not found, attempting to build...${NC}"
    if command -v cargo &> /dev/null; then
        echo "Building TART backend..."
        cargo build --release --bin tart-backend
        TART_BIN="./target/release/tart-backend"
    else
        echo -e "${RED}Error: Cannot build tart-backend, cargo not found${NC}"
        exit 1
    fi
fi

# Check for PolkaJam binary if nodes requested
if [ $NODE_COUNT -gt 0 ]; then
    if [ -f "./polkajam" ]; then
        JAM_BIN="./polkajam"
    elif [ -f "./polkajam-testnet" ]; then
        JAM_BIN="./polkajam-testnet"
    else
        echo -e "${RED}Error: No PolkaJam binary found${NC}"
        echo "Please ensure polkajam or polkajam-testnet is in the current directory"
        exit 1
    fi
fi

# Create PID file to track processes
PID_FILE="/tmp/tart-telemetry.pids"
rm -f "$PID_FILE"

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down TART telemetry system...${NC}"
    
    if [ -f "$PID_FILE" ]; then
        while read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "Stopping process $pid"
                kill "$pid" 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    
    if [ "$CLEANUP_ON_EXIT" = true ] && [ $NODE_COUNT -gt 0 ]; then
        echo "Cleaning up temporary node data..."
        rm -rf /tmp/jam-node-* 2>/dev/null || true
    fi
    
    echo -e "${GREEN}Shutdown complete${NC}"
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Start TART backend
echo -e "${GREEN}Starting TART backend...${NC}"
echo "  Telemetry: 0.0.0.0:$TELEMETRY_PORT"
echo "  API: 0.0.0.0:$API_PORT"
echo "  Database: $DATABASE_URL"
echo "  Log level: $LOG_LEVEL"
echo ""

DATABASE_URL="$DATABASE_URL" \
TELEMETRY_BIND="0.0.0.0:$TELEMETRY_PORT" \
API_BIND="0.0.0.0:$API_PORT" \
RUST_LOG="tart_backend=$LOG_LEVEL,tower_http=$LOG_LEVEL" \
"$TART_BIN" &

TART_PID=$!
echo $TART_PID >> "$PID_FILE"

# Wait for TART to start
echo "Waiting for TART backend to start..."
for i in {1..30}; do
    if curl -s "http://localhost:$API_PORT/health" > /dev/null 2>&1; then
        echo -e "${GREEN}TART backend is ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Error: TART backend failed to start${NC}"
        exit 1
    fi
    sleep 1
done

# Start JAM nodes if requested
if [ $NODE_COUNT -gt 0 ]; then
    echo ""
    echo -e "${GREEN}Starting $NODE_COUNT JAM validator node(s)...${NC}"
    
    BASE_PORT=40000
    BASE_RPC_PORT=19800
    
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        NODE_PORT=$((BASE_PORT + i))
        NODE_RPC_PORT=$((BASE_RPC_PORT + i))
        
        echo -e "${BLUE}Starting validator $i...${NC}"
        echo "  JAM port: $NODE_PORT"
        echo "  RPC port: $NODE_RPC_PORT"
        
        if [ "$JAM_BIN" = "./polkajam" ]; then
            # Use polkajam binary
            "$JAM_BIN" \
                --chain "$NODE_TYPE" \
                run \
                --telemetry "127.0.0.1:$TELEMETRY_PORT" \
                --dev-validator "$i" \
                --temp \
                --rpc-port "$NODE_RPC_PORT" \
                2>&1 | sed "s/^/[Node$i] /" &
        else
            # Use polkajam-testnet binary
            if [ $i -eq 0 ]; then
                # First run starts all validators
                "$JAM_BIN" \
                    --num-nonval-nodes 0 \
                    --base-port "$BASE_PORT" \
                    --base-rpc-port "$BASE_RPC_PORT" \
                    2>&1 | sed "s/^/[Testnet] /" &
            fi
        fi
        
        NODE_PID=$!
        echo $NODE_PID >> "$PID_FILE"
        
        # Give node time to start
        sleep 2
    done
    
    echo -e "${GREEN}All nodes started!${NC}"
fi

# Print status
echo ""
echo -e "${GREEN}=== TART Telemetry System Running ===${NC}"
echo ""
echo "Telemetry endpoint: localhost:$TELEMETRY_PORT"
echo ""
echo "API Endpoints:"
echo "  Health: http://localhost:$API_PORT/api/health"
echo "  Nodes: http://localhost:$API_PORT/api/nodes"
echo "  Events: http://localhost:$API_PORT/api/events"
echo "  WebSocket: ws://localhost:$API_PORT/api/ws"
echo "  Metrics: http://localhost:$API_PORT/metrics"
echo ""

if [ $NODE_COUNT -gt 0 ]; then
    echo "JAM Nodes:"
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        echo "  Validator $i RPC: http://localhost:$((BASE_RPC_PORT + i))"
    done
    echo ""
fi

echo -e "${YELLOW}Press Ctrl+C to stop all services${NC}"
echo ""

# Monitor logs
if [ $NODE_COUNT -gt 0 ]; then
    echo "=== Combined Logs ==="
    echo ""
fi

# Wait for all background processes
wait