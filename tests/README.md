# TART Backend Test Suite

This document explains the testing architecture and best practices for the TART telemetry backend.

## Test Organization

### Unit Tests (No Database Required)
These tests run in parallel and don't require external services:
- `types_tests.rs` - Type encoding/decoding
- `events_tests.rs` - Event serialization
- `error_tests.rs` - Error handling and edge cases
- `encoding_tests.rs` - Binary protocol encoding
- Library tests in `src/` - Core logic

### Integration Tests (Require PostgreSQL)
These tests use a real PostgreSQL database and MUST run serially:
- `api_tests.rs` - REST API endpoints
- `integration_tests.rs` - End-to-end telemetry flow
- `optimized_server_tests.rs` - Performance and concurrency

## Running Tests Locally

```bash
# Unit tests only (fast, no setup needed)
cargo test --lib --test types_tests --test events_tests --test error_tests --test encoding_tests

# Integration tests (requires PostgreSQL)
export TEST_DATABASE_URL="postgres://tart:tart_password@localhost:5432/tart_test"

# Start PostgreSQL (using docker-compose)
docker-compose up -d postgres

# Create test database and run migrations
cargo sqlx database create
cargo sqlx migrate run

# Run integration tests SERIALLY
cargo test --test api_tests --test integration_tests --test optimized_server_tests -- --test-threads=1

# To reset the database completely (removes all data)
docker-compose down -v && docker-compose up -d postgres
```

## Why Tests Must Run Serially (`--test-threads=1`)

**Problem**: Integration tests share the same PostgreSQL database `tart_test`.

**Without serial execution:**
- Test A connects 2 nodes → expects 2 in database
- Test B connects 2 nodes → expects 2 in database
- Tests run in parallel → both see 4 nodes → BOTH FAIL

**Solution**: Run with `--test-threads=1` to execute one test at a time.

Each test:
1. Cleans the database (TRUNCATE all tables)
2. Runs its scenario
3. Next test cleans and runs

## The Flush Pattern - Why It's Necessary

### The Problem: Asynchronous Background Writer

TART uses a `BatchWriter` that runs in a background task for performance:

```
Test sends data → Queues in channel → Background task → Batches → PostgreSQL
      ↓
Test continues immediately!
      ↓
Test queries database... but data might not be written yet!
```

Even though:
- Node connections flush immediately
- Events batch periodically or when the batch is full

The `node_connected()` method returns as soon as it QUEUES the message, not when it's written.

### The Solution: Explicit Flush with Synchronization

```rust
// Test helper that WAITS for flush to complete
async fn flush_and_wait(telemetry_server: &Arc<TelemetryServer>) {
    telemetry_server.flush_writes().await.expect("Flush failed");
    sleep(Duration::from_millis(50)).await; // PostgreSQL commit margin
}

// In tests:
connect_test_node(port, 1).await;
flush_and_wait(&server).await;  // ← BLOCKS until database write completes
let response = get("/api/nodes").await;  // Now data is guaranteed to be there
```

### Why Not Just Sleep Longer?

❌ **Bad approach:**
```rust
connect_test_node(port, 1).await;
sleep(Duration::from_millis(5000)).await;  // Hope this is enough?
let response = get("/api/nodes").await;
```

Problems:
- Non-deterministic: Might work locally, fail in CI
- Slow: Wastes time waiting
- Brittle: Breaks if server is under load

✅ **Good approach (current):**
```rust
connect_test_node(port, 1).await;
flush_and_wait(&server).await;  // Deterministic, fast, reliable
let response = get("/api/nodes").await;
```

## Test Isolation Pattern

### Database Cleanup (`#[cfg(test)]` protected)

```rust
#[cfg(test)]
impl EventStore {
    pub async fn cleanup_test_data(&self) -> Result<(), sqlx::Error> {
        sqlx::query("TRUNCATE TABLE events, nodes, ...").execute(&self.pool).await?;
    }
}
```

**Safety features:**
- Only compiled in test builds (not available in production)
- Used in `setup_test_api()` before each test
- Ensures clean state for every test

### Common Test Fixtures

Located in `tests/common/mod.rs`:
- `test_protocol_params()` - Creates valid ProtocolParameters
- `test_node_info(peer_id)` - Creates valid NodeInformation
- Reduces duplication across test files

## Best Practices We Follow

✅ **Test Isolation**: Each test starts with clean database
✅ **Deterministic**: flush() instead of arbitrary sleeps
✅ **Safety**: Dangerous methods protected with #[cfg(test)]
✅ **Clear Intent**: Well-documented test helpers
✅ **Fast Unit Tests**: No database for unit tests
✅ **Realistic Integration Tests**: Real PostgreSQL for integration tests
✅ **CI Optimized**: Parallel unit tests, serial integration tests

## Alternative Approaches Considered

### 1. Separate Database Per Test
```rust
let db_name = format!("tart_test_{}", uuid::new_v4());
// Create database, run test, drop database
```
- ✅ Perfect isolation
- ❌ Very slow (create/drop overhead)
- ❌ CI complexity

### 2. In-Memory Mock Database
```rust
let store = Arc::new(MockEventStore::new());
```
- ✅ Fast tests
- ❌ Doesn't test real PostgreSQL behavior
- ❌ Can miss query bugs, index issues, etc.

### 3. Transaction Rollback Pattern
```rust
BEGIN TRANSACTION;
// Run test
ROLLBACK;
```
- ✅ Good isolation
- ❌ Can't use with async background writers
- ❌ Doesn't work with multiple connections

### 4. Separate Writer for Tests
```rust
#[cfg(test)]
struct SyncWriter { ... }  // No batching
#[cfg(not(test))]
struct BatchWriter { ... }  // Batching
```
- ✅ Tests are simple
- ❌ Tests don't match production behavior
- ❌ Large code duplication

**Our chosen approach (#flush + serial execution) balances all concerns.**

## Common Pitfalls

### ❌ Running Integration Tests in Parallel
```bash
cargo test  # BAD: Tests conflict in shared database
```

### ✅ Correct Way
```bash
cargo test --test api_tests -- --test-threads=1
```

### ❌ Forgetting to Flush
```rust
connect_test_node(port, 1).await;
// Immediately query - DATA MIGHT NOT BE THERE YET
let response = get("/api/nodes").await;
```

### ✅ Correct Pattern
```rust
connect_test_node(port, 1).await;
flush_and_wait(&server).await;  // Ensure data is written
let response = get("/api/nodes").await;
```

## CI Configuration

GitHub Actions workflow (`.github/workflows/ci.yml`):

```yaml
# Unit tests in parallel (fast)
cargo test --lib --test types_tests --test events_tests --test error_tests --test encoding_tests

# Integration tests serially (safe)
cargo test --test api_tests --test integration_tests --test optimized_server_tests -- --test-threads=1
```

This ensures:
- Fast feedback for unit tests
- Reliable integration tests
- No database conflicts

## Future Improvements

Potential enhancements for the test suite:

1. **Test fixtures with realistic data** - Pre-populate database with sample nodes/events
2. **Property-based testing** - Use proptest for fuzz testing encoders
3. **Load testing** - Verify 1024 concurrent connections
4. **Chaos testing** - Simulate network failures, database outages
5. **Benchmark suite** - Track performance regressions

## Summary

Our testing approach follows industry best practices:
- Separate unit and integration tests
- Explicit synchronization instead of sleeps
- Test isolation through database cleanup
- Safety through compile-time checks (#[cfg(test)])
- Clear documentation of patterns

The flush pattern is used by many production systems (Kafka, async loggers, batch processors) and is the correct solution for testing asynchronous background workers.
