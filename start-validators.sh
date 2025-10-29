#!/bin/bash

# Start JAM Validator Nodes Script
# This script starts validator nodes that connect to an existing TART backend

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
TELEMETRY_HOST="127.0.0.1"
TELEMETRY_PORT=9000
NODE_COUNT=3
NODE_TYPE="dev"
CLEANUP_ON_EXIT=true
BASE_PORT=40000
BASE_RPC_PORT=19800

# Parse command line arguments
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -n, --nodes COUNT      Number of validator nodes to start (default: 3)"
    echo "  -t, --node-type TYPE   Node type: dev, testnet (default: dev)"
    echo "  -h, --host HOST        TART telemetry host (default: localhost)"
    echo "  -p, --port PORT        TART telemetry port (default: 9000)"
    echo "  -b, --base-port PORT   Base port for validators (default: 40000)"
    echo "  -r, --base-rpc PORT    Base RPC port for validators (default: 19800)"
    echo "  -k, --keep-data        Don't cleanup data on exit"
    echo "  --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Start 3 dev validators (default)"
    echo "  $0"
    echo ""
    echo "  # Start 5 validators"
    echo "  $0 -n 5"
    echo ""
    echo "  # Start 10 validators connecting to remote TART"
    echo "  $0 -n 10 -h tart.example.com -p 9000"
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
        -h|--host)
            TELEMETRY_HOST="$2"
            shift 2
            ;;
        -p|--port)
            TELEMETRY_PORT="$2"
            shift 2
            ;;
        -b|--base-port)
            BASE_PORT="$2"
            shift 2
            ;;
        -r|--base-rpc)
            BASE_RPC_PORT="$2"
            shift 2
            ;;
        -k|--keep-data)
            CLEANUP_ON_EXIT=false
            shift
            ;;
        --help)
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

echo -e "${BLUE}=== JAM Validator Startup ===${NC}"
echo ""

# Check for PolkaJam binary
if [ -f "./polkajam" ]; then
    JAM_BIN="./polkajam"
    echo -e "${GREEN}Found polkajam binary${NC}"
elif [ -f "./polkajam-testnet" ]; then
    JAM_BIN="./polkajam-testnet"
    echo -e "${GREEN}Found polkajam-testnet binary${NC}"
else
    echo -e "${RED}Error: No PolkaJam binary found${NC}"
    echo "Please ensure polkajam or polkajam-testnet is in the current directory"
    echo ""
    echo "You can download or build the binary from:"
    echo "  - https://github.com/polkadot/polkajam"
    exit 1
fi

# Create PID file to track processes
PID_FILE="/tmp/tart-validators.pids"
rm -f "$PID_FILE"

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down validators...${NC}"

    if [ -f "$PID_FILE" ]; then
        while read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "Stopping validator process $pid"
                kill "$pid" 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi

    if [ "$CLEANUP_ON_EXIT" = true ]; then
        echo "Cleaning up temporary node data..."
        rm -rf /tmp/jam-node-* 2>/dev/null || true
    fi

    echo -e "${GREEN}Shutdown complete${NC}"
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Check if TART backend is accessible
echo "Checking TART backend connectivity..."
TELEMETRY_URL="${TELEMETRY_HOST}:${TELEMETRY_PORT}"

# Try to connect to the API endpoint (HTTP) to verify TART is running
API_PORT=8080
if curl -s "http://${TELEMETRY_HOST}:${API_PORT}/health" > /dev/null 2>&1; then
    echo -e "${GREEN}TART backend is accessible at ${TELEMETRY_HOST}:${API_PORT}${NC}"
else
    echo -e "${YELLOW}Warning: Cannot reach TART API at http://${TELEMETRY_HOST}:${API_PORT}/health${NC}"
    echo "Continuing anyway - telemetry will try to connect to ${TELEMETRY_URL}"
fi

# Start JAM nodes
echo ""
echo -e "${GREEN}Starting $NODE_COUNT JAM validator node(s)...${NC}"
echo "Telemetry endpoint: ${TELEMETRY_URL}"
echo ""

for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_PORT=$((BASE_PORT + i))
    NODE_RPC_PORT=$((BASE_RPC_PORT + i))

    echo -e "${BLUE}Starting validator $i...${NC}"
    echo "  JAM port: $NODE_PORT"
    echo "  RPC port: $NODE_RPC_PORT"
    echo "  Telemetry: $TELEMETRY_URL"

    if [ "$JAM_BIN" = "./polkajam" ]; then
        # Use polkajam binary
        "$JAM_BIN" \
            --chain "$NODE_TYPE" \
            run \
            --telemetry "${TELEMETRY_HOST}:${TELEMETRY_PORT}" \
            --dev-validator "$i" \
            --temp \
            --rpc-port "$NODE_RPC_PORT" \
            2>&1 | sed "s/^/[Node$i] /" &
    else
        # Use polkajam-testnet binary
        if [ $i -eq 0 ]; then
            # First run starts all validators
            echo "Starting testnet with $NODE_COUNT validators..."
            "$JAM_BIN" \
                --num-nonval-nodes 0 \
                --base-port "$BASE_PORT" \
                --base-rpc-port "$BASE_RPC_PORT" \
                2>&1 | sed "s/^/[Testnet] /" &
        fi
    fi

    NODE_PID=$!
    echo $NODE_PID >> "$PID_FILE"

    echo -e "${GREEN}Started validator $i (PID: $NODE_PID)${NC}"
    echo ""

    # Give node time to start
    sleep 2
done

echo -e "${GREEN}All $NODE_COUNT validators started!${NC}"
echo ""

# Print status
echo -e "${GREEN}=== JAM Validators Running ===${NC}"
echo ""
echo "TART Backend: http://${TELEMETRY_HOST}:${API_PORT}"
echo "Telemetry endpoint: ${TELEMETRY_URL}"
echo ""
echo "Validators:"
for i in $(seq 0 $((NODE_COUNT - 1))); do
    echo "  Validator $i RPC: http://localhost:$((BASE_RPC_PORT + i))"
done
echo ""
echo "Monitor via TART API:"
echo "  Nodes API: http://${TELEMETRY_HOST}:${API_PORT}/api/nodes"
echo "  Events API: http://${TELEMETRY_HOST}:${API_PORT}/api/events"
echo "  Health: http://${TELEMETRY_HOST}:${API_PORT}/api/health"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop all validators${NC}"
echo ""

# Monitor logs
echo "=== Combined Validator Logs ==="
echo ""

# Wait for all background processes
wait