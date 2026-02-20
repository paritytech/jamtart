#!/usr/bin/env bash
# profile.sh — Profiling orchestrator for TART backend
#
# Usage: ./scripts/profile.sh <mode> [options]
#
# Modes:
#   cpu       CPU profile using macOS 'sample' (text output, AI-readable)
#   samply    CPU profile using samply (interactive Firefox Profiler)
#   heap      Heap allocation profile using dhat (JSON output, AI-readable)
#
# Options:
#   --nodes N      Number of simulated nodes (default: 100)
#   --rate R       Events per second per node (default: 100)
#   --duration S   Profiling duration in seconds (default: 15)
#   --output DIR   Output directory (default: profile-results/)
#   --no-db        Start server without database (dry-run mode — not yet supported)

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

MODE=""
NODES=100
RATE=100
DURATION=15
OUTPUT_DIR="profile-results"
NO_DB=false

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <cpu|samply|heap> [options]"
    echo "Run '$0 --help' for details."
    exit 1
fi

MODE="$1"
shift

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes)    NODES="$2"; shift 2 ;;
        --rate)     RATE="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --output)   OUTPUT_DIR="$2"; shift 2 ;;
        --no-db)    NO_DB=true; shift ;;
        --help|-h)
            cat <<'HELP'
profile.sh — Profiling orchestrator for TART backend

USAGE:
    ./scripts/profile.sh <mode> [options]

MODES:
    cpu       CPU profile using macOS 'sample' command (text output, AI-readable)
    samply    CPU profile using samply (interactive Firefox Profiler)
    heap      Heap allocation profile using dhat (JSON output, AI-readable)

OPTIONS:
    --nodes N      Simulated nodes          [default: 100]
    --rate R       Events/sec per node      [default: 100]
    --duration S   Profiling duration (sec)  [default: 15]
    --output DIR   Output directory          [default: profile-results/]
    --no-db        (Reserved for future use)

OUTPUT:
    profile-results/
      cpu-profile.txt       macOS sample output (cpu mode)
      throughput.txt         loadgen final report
      dhat-heap.json         heap allocation data (heap mode)
      run-info.txt           metadata: date, git rev, params
HELP
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cleanup() {
    # Kill any processes we started
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  Stopping server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "${LOADGEN_PID:-}" ]] && kill -0 "$LOADGEN_PID" 2>/dev/null; then
        echo "  Stopping loadgen (PID $LOADGEN_PID)..."
        kill "$LOADGEN_PID" 2>/dev/null || true
        wait "$LOADGEN_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

wait_for_port() {
    local port="$1"
    local timeout="${2:-10}"
    local elapsed=0
    while ! nc -z 127.0.0.1 "$port" 2>/dev/null; do
        sleep 0.2
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $((timeout * 5)) ]]; then
            echo "ERROR: Port $port not ready after ${timeout}s"
            exit 1
        fi
    done
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build_profiling() {
    echo "=== Building with profiling profile ==="
    local features_flag=""
    if [[ "$MODE" == "heap" ]]; then
        features_flag="--features profiling"
    fi
    # shellcheck disable=SC2086
    cargo build --profile profiling --bin tart-backend --bin tart-loadgen $features_flag
    echo "  Build complete."
}

# ---------------------------------------------------------------------------
# CPU mode — macOS 'sample' command
# ---------------------------------------------------------------------------

run_cpu() {
    build_profiling
    mkdir -p "$OUTPUT_DIR"

    echo ""
    echo "=== CPU Profile (macOS sample) ==="
    echo "  nodes=$NODES  rate=$RATE  duration=${DURATION}s"
    echo ""

    # Start server
    echo "  Starting tart-backend..."
    ./target/profiling/tart-backend &
    SERVER_PID=$!
    wait_for_port 9000

    # Start loadgen (duration = warmup + sample duration + drain buffer)
    local loadgen_duration=$((DURATION + 3))
    echo "  Starting loadgen ($NODES nodes, ${RATE} ev/s, ${loadgen_duration}s)..."
    ./target/profiling/tart-loadgen \
        --nodes "$NODES" --rate "$RATE" --duration "$loadgen_duration" \
        > "$OUTPUT_DIR/throughput.txt" 2>/dev/null &
    LOADGEN_PID=$!

    # Warmup
    echo "  Warming up (2s)..."
    sleep 2

    # Sample
    echo "  Sampling PID $SERVER_PID for ${DURATION}s..."
    sample "$SERVER_PID" "$DURATION" -f "$OUTPUT_DIR/cpu-profile.txt" 2>/dev/null || {
        echo "WARNING: sample command failed (PID may have exited)"
    }

    echo "  Sampling complete."

    # Wait for loadgen to finish naturally so it writes its final report to stdout
    echo "  Waiting for loadgen to finish..."
    wait "$LOADGEN_PID" 2>/dev/null || true
    LOADGEN_PID=""

    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""

    # Write run metadata
    write_run_info

    echo ""
    echo "=== Results ==="
    echo "  CPU profile:  $OUTPUT_DIR/cpu-profile.txt"
    echo "  Throughput:    $OUTPUT_DIR/throughput.txt"
    echo "  Run info:      $OUTPUT_DIR/run-info.txt"
    echo ""
    echo "Read with: cat $OUTPUT_DIR/cpu-profile.txt"
}

# ---------------------------------------------------------------------------
# Samply mode — interactive Firefox Profiler
# ---------------------------------------------------------------------------

run_samply() {
    if ! command -v samply &>/dev/null; then
        echo "ERROR: samply not found. Install with: cargo install samply"
        exit 1
    fi

    build_profiling
    mkdir -p "$OUTPUT_DIR"

    echo ""
    echo "=== CPU Profile (samply — interactive) ==="
    echo "  nodes=$NODES  rate=$RATE  duration=${DURATION}s"
    echo ""

    # Start server under samply
    echo "  Starting tart-backend under samply..."
    echo "  (samply will open Firefox Profiler when done)"
    echo ""

    # Start loadgen in background first — it will retry connecting
    local loadgen_duration=$((DURATION + 10))
    ./target/profiling/tart-loadgen \
        --nodes "$NODES" --rate "$RATE" --duration "$loadgen_duration" \
        > "$OUTPUT_DIR/throughput.txt" 2>/dev/null &
    LOADGEN_PID=$!

    # Run server under samply (this blocks until duration elapses)
    samply record --duration "$DURATION" -- ./target/profiling/tart-backend || true

    # Cleanup
    kill "$LOADGEN_PID" 2>/dev/null || true
    wait "$LOADGEN_PID" 2>/dev/null || true
    LOADGEN_PID=""

    write_run_info
}

# ---------------------------------------------------------------------------
# Heap mode — dhat
# ---------------------------------------------------------------------------

run_heap() {
    build_profiling
    mkdir -p "$OUTPUT_DIR"

    echo ""
    echo "=== Heap Profile (dhat) ==="
    echo "  nodes=$NODES  rate=$RATE  duration=${DURATION}s"
    echo ""

    # Start server (dhat writes JSON on clean exit)
    echo "  Starting tart-backend (with dhat)..."
    ./target/profiling/tart-backend &
    SERVER_PID=$!
    wait_for_port 9000

    # Start loadgen
    echo "  Starting loadgen ($NODES nodes, ${RATE} ev/s, ${DURATION}s)..."
    ./target/profiling/tart-loadgen \
        --nodes "$NODES" --rate "$RATE" --duration "$DURATION" \
        > "$OUTPUT_DIR/throughput.txt" 2>/dev/null &
    LOADGEN_PID=$!

    # Wait for loadgen to finish
    echo "  Waiting for loadgen to complete..."
    wait "$LOADGEN_PID" 2>/dev/null || true
    LOADGEN_PID=""

    # SIGTERM the server for clean dhat shutdown
    echo "  Sending SIGTERM to server for dhat output..."
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""

    # dhat writes dhat-heap.json in CWD
    if [[ -f "dhat-heap.json" ]]; then
        mv "dhat-heap.json" "$OUTPUT_DIR/dhat-heap.json"
        echo "  dhat output captured."
    else
        echo "WARNING: dhat-heap.json not found (was dhat feature enabled?)"
    fi

    write_run_info

    echo ""
    echo "=== Results ==="
    echo "  Heap profile:  $OUTPUT_DIR/dhat-heap.json"
    echo "  Throughput:     $OUTPUT_DIR/throughput.txt"
    echo "  Run info:       $OUTPUT_DIR/run-info.txt"
    echo ""
    echo "Read with: cat $OUTPUT_DIR/dhat-heap.json"
}

# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------

write_run_info() {
    cat > "$OUTPUT_DIR/run-info.txt" <<EOF
--- Profile Run Info ---
Date:       $(date -u +"%Y-%m-%dT%H:%M:%SZ")
Git rev:    $(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
Git branch: $(git branch --show-current 2>/dev/null || echo "unknown")
Mode:       $MODE
Nodes:      $NODES
Rate:       $RATE events/sec/node
Duration:   ${DURATION}s
EOF
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

case "$MODE" in
    cpu)    run_cpu ;;
    samply) run_samply ;;
    heap)   run_heap ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Valid modes: cpu, samply, heap"
        exit 1
        ;;
esac
