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
NODE_COUNT=6
NODE_TYPE="dev"
CLEANUP_ON_EXIT=true
BASE_PORT=40000
BASE_RPC_PORT=19800
FORCE_DOWNLOAD=false

# Parse command line arguments
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -n, --nodes COUNT      Number of validator nodes to start (default: 6)"
    echo "  -t, --node-type TYPE   Node type: dev, testnet (default: dev)"
    echo "  -h, --host HOST        TART telemetry host (default: 127.0.0.1)"
    echo "  -p, --port PORT        TART telemetry port (default: 9000)"
    echo "  -b, --base-port PORT   Base port for validators (default: 40000)"
    echo "  -r, --base-rpc PORT    Base RPC port for validators (default: 19800)"
    echo "  -k, --keep-data        Don't cleanup data on exit"
    echo "  --update               Force download of the latest nightly binary"
    echo "  --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                     # Start 6 dev validators (auto-downloads binary if needed)"
    echo "  $0 -n 10               # Start 10 validators"
    echo "  $0 --update            # Re-download latest nightly before starting"
    echo "  $0 -n 3 --update       # Download latest + start 3 validators"
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
        --update)
            FORCE_DOWNLOAD=true
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

# Download latest nightly PolkaJam binary
download_polkajam() {
    echo -e "${BLUE}Fetching latest nightly release...${NC}"

    # Detect OS
    case "$(uname -s)" in
        Linux)  OS="linux" ;;
        Darwin) OS="macos" ;;
        *)
            echo -e "${RED}Error: Unsupported OS: $(uname -s)${NC}"
            exit 1
            ;;
    esac

    # Detect architecture
    case "$(uname -m)" in
        x86_64|amd64)  ARCH="x86_64" ;;
        aarch64|arm64) ARCH="aarch64" ;;
        *)
            echo -e "${RED}Error: Unsupported architecture: $(uname -m)${NC}"
            exit 1
            ;;
    esac

    # Find the latest nightly tag via the GitHub API
    RELEASES_URL="https://api.github.com/repos/paritytech/polkajam-releases/releases"
    LATEST_TAG=$(curl -sfL "$RELEASES_URL?per_page=50" \
        | grep -o '"tag_name":"nightly-[^"]*"' \
        | head -1 \
        | cut -d'"' -f4)

    if [ -z "$LATEST_TAG" ]; then
        echo -e "${RED}Error: Could not determine latest nightly release${NC}"
        return 1
    fi

    echo -e "  Latest nightly: ${GREEN}${LATEST_TAG}${NC}"
    echo -e "  Platform:       ${OS}-${ARCH}"

    ASSET_NAME="polkajam-${LATEST_TAG}-${OS}-${ARCH}.tgz"
    DOWNLOAD_URL="https://github.com/paritytech/polkajam-releases/releases/download/${LATEST_TAG}/${ASSET_NAME}"

    echo -e "  Downloading ${ASSET_NAME}..."

    TMPDIR=$(mktemp -d)
    trap "rm -rf '$TMPDIR'" RETURN

    if ! curl -sfL -o "${TMPDIR}/${ASSET_NAME}" "$DOWNLOAD_URL"; then
        echo -e "${RED}Error: Failed to download ${DOWNLOAD_URL}${NC}"
        return 1
    fi

    # Extract — the tarball contains a directory with the binaries inside
    tar -xzf "${TMPDIR}/${ASSET_NAME}" -C "$TMPDIR"

    # The inner directory name matches the asset name minus .tgz
    INNER_DIR="${TMPDIR}/${ASSET_NAME%.tgz}"

    # Copy the binaries we care about into the current directory
    for bin in polkajam polkajam-testnet; do
        if [ -f "${INNER_DIR}/${bin}" ]; then
            cp "${INNER_DIR}/${bin}" "./${bin}"
            chmod +x "./${bin}"
            echo -e "  ${GREEN}Installed ./${bin}${NC}"
        fi
    done

    rm -rf "$TMPDIR"
    # Clear the RETURN trap we set for TMPDIR cleanup
    trap - RETURN
    echo ""
}

# Download binary if missing or if --update was passed
if [ "$FORCE_DOWNLOAD" = true ]; then
    download_polkajam
elif [ ! -f "./polkajam" ] && [ ! -f "./polkajam-testnet" ]; then
    echo -e "${YELLOW}No polkajam binary found — downloading latest nightly...${NC}"
    download_polkajam
else
    echo -e "${GREEN}Using existing polkajam binary (pass --update to refresh)${NC}"
fi

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
    echo "You can download the latest nightly from:"
    echo "  https://github.com/paritytech/polkajam-releases/releases"
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