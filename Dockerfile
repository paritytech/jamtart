# Build stage - use Debian-based nightly for glibc compatibility
FROM rust:bookworm AS builder

# Install nightly toolchain
RUN rustup default nightly

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy migrations for sqlx compile-time verification
COPY migrations ./migrations

# Copy source code
COPY src ./src

# Build for release
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/tart-backend /app/tart-backend

# Create data directory
RUN mkdir -p /data

# Expose ports
EXPOSE 9000 8080

# Set default bind addresses (non-sensitive defaults)
ENV TELEMETRY_BIND=0.0.0.0:9000
ENV API_BIND=0.0.0.0:8080

# DATABASE_URL must be provided at runtime via environment variable or docker-compose
# Example: DATABASE_URL=postgres://user:password@host:5432/dbname

# Run the binary
CMD ["/app/tart-backend"]