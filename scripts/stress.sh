#!/usr/bin/env bash
# stress.sh — Combined ingestion + API stress test for TART backend
#
# Runs tart-loadgen (TCP ingestion) and tart-bench (API stress) simultaneously
# against the Docker backend to reproduce UI timeout issues under load.
#
# Usage:
#   ./scripts/stress.sh [options]
#
# Options:
#   --nodes N       Simulated telemetry nodes   [default: 1023]
#   --rate R        Events/sec per node          [default: 600]
#   --tabs T        Simulated browser tabs       [default: 10]
#   --duration S    Bench measurement duration   [default: 30]
#   --scenario S    Bench scenario               [default: dashboard]
#   --profile       Also run macOS 'sample' on the backend container PID
#   --help          Print this help

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

NODES=1023
RATE=600
TABS=10
DURATION=30
SCENARIO="dashboard"
PROFILE=false

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes)    NODES="$2"; shift 2 ;;
        --rate)     RATE="$2"; shift 2 ;;
        --tabs)     TABS="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --scenario) SCENARIO="$2"; shift 2 ;;
        --profile)  PROFILE=true; shift ;;
        --help|-h)
            cat <<'HELP'
stress.sh — Combined ingestion + API stress test for TART backend

USAGE:
    ./scripts/stress.sh [options]

OPTIONS:
    --nodes N       Simulated telemetry nodes   [default: 1023]
    --rate R        Events/sec per node          [default: 600]
    --tabs T        Simulated browser tabs       [default: 10]
    --duration S    Bench measurement duration   [default: 30]
    --scenario S    Bench scenario               [default: dashboard]
    --profile       Also run macOS 'sample' on the backend container PID
    --help          Print this help

WHAT IT DOES:
    1. Resets the database (DROP + CREATE SCHEMA)
    2. Rebuilds and restarts the Docker backend
    3. Waits for backend health check
    4. Builds loadgen + bench binaries (release)
    5. Starts tart-loadgen in background (TCP ingestion)
    6. Waits for connections to ramp up
    7. Runs tart-bench (API stress) while ingestion is ongoing
    8. Optionally profiles the backend with macOS 'sample'
    9. Kills loadgen, prints results
HELP
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

LOADGEN_PID=""
SAMPLE_PID=""

cleanup() {
    if [[ -n "$LOADGEN_PID" ]] && kill -0 "$LOADGEN_PID" 2>/dev/null; then
        echo "  Stopping loadgen (PID $LOADGEN_PID)..."
        kill "$LOADGEN_PID" 2>/dev/null || true
        wait "$LOADGEN_PID" 2>/dev/null || true
    fi
    if [[ -n "$SAMPLE_PID" ]] && kill -0 "$SAMPLE_PID" 2>/dev/null; then
        echo "  Stopping profiler (PID $SAMPLE_PID)..."
        kill "$SAMPLE_PID" 2>/dev/null || true
        wait "$SAMPLE_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

wait_for_health() {
    local url="$1"
    local timeout="${2:-60}"
    local elapsed=0
    echo -n "  Waiting for $url "
    while ! curl -sf "$url" >/dev/null 2>&1; do
        sleep 1
        elapsed=$((elapsed + 1))
        echo -n "."
        if [[ $elapsed -ge $timeout ]]; then
            echo " TIMEOUT (${timeout}s)"
            echo "ERROR: Backend did not become healthy"
            exit 1
        fi
    done
    echo " ready (${elapsed}s)"
}

# ---------------------------------------------------------------------------
# Step 1: Reset database
# ---------------------------------------------------------------------------

echo ""
echo "================================================================================"
echo "  TART Combined Stress Test"
echo "================================================================================"
echo "  nodes=$NODES  rate=$RATE ev/s  tabs=$TABS  duration=${DURATION}s  scenario=$SCENARIO"
echo "================================================================================"
echo ""

echo "=== Step 1: Reset database ==="
PGPASSWORD="${POSTGRES_PASSWORD:-tart_password}" psql \
    -h 127.0.0.1 \
    -U "${POSTGRES_USER:-tart}" \
    -d "${POSTGRES_DB:-tart_telemetry}" \
    -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;" \
    2>/dev/null || {
        echo "  WARNING: Could not reset database (is PostgreSQL running?)"
        echo "  Continuing anyway — backend will run migrations on startup."
    }
echo "  Database reset."

# ---------------------------------------------------------------------------
# Step 2: Rebuild and restart Docker backend
# ---------------------------------------------------------------------------

echo ""
echo "=== Step 2: Rebuild and restart backend ==="
docker compose up -d --build tart-backend 2>&1 | while IFS= read -r line; do
    echo "  $line"
done

# ---------------------------------------------------------------------------
# Step 3: Wait for health check
# ---------------------------------------------------------------------------

echo ""
echo "=== Step 3: Wait for backend health ==="
wait_for_health "http://localhost:8080/api/health" 60

# ---------------------------------------------------------------------------
# Step 4: Build loadgen + bench
# ---------------------------------------------------------------------------

echo ""
echo "=== Step 4: Build loadgen + bench (release) ==="
cargo build --release --bin tart-loadgen --bin tart-bench --features bench 2>&1 | while IFS= read -r line; do
    echo "  $line"
done
echo "  Build complete."

# ---------------------------------------------------------------------------
# Step 5: Start loadgen in background
# ---------------------------------------------------------------------------

echo ""
echo "=== Step 5: Start loadgen ($NODES nodes, $RATE ev/s) ==="

# Loadgen runs for: ramp-up + bench warmup + bench duration + buffer
LOADGEN_DURATION=$(( DURATION + 20 ))

./target/release/tart-loadgen \
    --nodes "$NODES" \
    --rate "$RATE" \
    --duration "$LOADGEN_DURATION" &
LOADGEN_PID=$!

echo "  Loadgen PID: $LOADGEN_PID"

# ---------------------------------------------------------------------------
# Step 6: Wait for connections to ramp up
# ---------------------------------------------------------------------------

echo ""
echo "=== Step 6: Ramp-up (5s) ==="
sleep 5

# ---------------------------------------------------------------------------
# Step 7: (Optional) Start profiler
# ---------------------------------------------------------------------------

if [[ "$PROFILE" == "true" ]]; then
    echo ""
    echo "=== Profiling: capturing macOS sample ==="
    mkdir -p profile-results

    # Get the PID of the tart-backend process inside Docker
    CONTAINER_PID=$(docker inspect --format '{{.State.Pid}}' tart-backend 2>/dev/null || echo "")

    if [[ -n "$CONTAINER_PID" && "$CONTAINER_PID" != "0" ]]; then
        echo "  Container PID: $CONTAINER_PID"
        echo "  Sampling for ${DURATION}s..."
        sample "$CONTAINER_PID" "$DURATION" -f profile-results/stress-cpu-profile.txt 2>/dev/null &
        SAMPLE_PID=$!
    else
        echo "  WARNING: Could not get container PID (macOS Docker may not expose it)"
        echo "  Skipping profiling."
    fi
fi

# ---------------------------------------------------------------------------
# Step 7: Run bench
# ---------------------------------------------------------------------------

echo ""
echo "=== Step 7: Run tart-bench ($TABS tabs, ${DURATION}s, scenario=$SCENARIO) ==="
echo ""

./target/release/tart-bench \
    --tabs "$TABS" \
    --duration "$DURATION" \
    --scenario "$SCENARIO"

BENCH_EXIT=$?

# ---------------------------------------------------------------------------
# Step 8: Cleanup and results
# ---------------------------------------------------------------------------

echo ""
echo "=== Step 8: Cleanup ==="

# Kill loadgen
if [[ -n "$LOADGEN_PID" ]] && kill -0 "$LOADGEN_PID" 2>/dev/null; then
    echo "  Stopping loadgen..."
    kill "$LOADGEN_PID" 2>/dev/null || true
    wait "$LOADGEN_PID" 2>/dev/null || true
fi
LOADGEN_PID=""

# Wait for profiler if running
if [[ -n "$SAMPLE_PID" ]] && kill -0 "$SAMPLE_PID" 2>/dev/null; then
    echo "  Waiting for profiler to finish..."
    wait "$SAMPLE_PID" 2>/dev/null || true
    SAMPLE_PID=""
    echo "  CPU profile: profile-results/stress-cpu-profile.txt"
fi

echo ""
echo "================================================================================"
echo "  Stress test complete (bench exit code: $BENCH_EXIT)"
echo "================================================================================"
echo ""

exit $BENCH_EXIT
