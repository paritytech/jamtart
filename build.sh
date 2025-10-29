#!/bin/bash
# Build script for TART backend

echo "Building TART backend..."
echo "========================"

# Build the binary
cargo build --release --bin tart-backend

if [ $? -eq 0 ]; then
    echo ""
    echo "Build successful! The binary is at:"
    echo "  target/release/tart-backend"
    echo ""
    echo "Key features:"
    echo "  - PostgreSQL with time-based partitioning"
    echo "  - Batched writes (100 events/batch)"
    echo "  - Rate limiting (100 events/sec/node)"
    echo "  - Connection limit (1024 nodes max)"
    echo "  - 50 connection pool for PostgreSQL"
    echo "  - Bounded buffers with backpressure"
    echo "  - Prometheus metrics at /metrics"
    echo ""
    echo "To run:"
    echo "  docker-compose up -d  # Start PostgreSQL + TART"
    echo "  # OR"
    echo "  ./target/release/tart-backend  # Requires PostgreSQL running"
    echo ""
    echo "Environment variables:"
    echo "  TELEMETRY_BIND=0.0.0.0:9000"
    echo "  API_BIND=0.0.0.0:8080"
    echo "  DATABASE_URL=postgres://tart:tart_password@localhost:5432/tart_telemetry"
else
    echo "Build failed!"
    exit 1
fi