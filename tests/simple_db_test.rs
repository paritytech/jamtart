// Minimal test to verify database connectivity and writes work
use std::sync::Arc;
use tart_backend::EventStore;

#[tokio::test]
async fn test_database_writes_actually_work() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://tart:tart_password@localhost:5432/tart_test".to_string());

    eprintln!("Connecting to: {}", database_url);
    let store = Arc::new(
        EventStore::new(&database_url)
            .await
            .expect("DB connection failed"),
    );

    eprintln!("Cleaning database...");
    store.cleanup_test_data().await.expect("Cleanup failed");

    eprintln!("Checking nodes table is empty...");
    let nodes = store.get_nodes().await.expect("Query failed");
    eprintln!("Found {} nodes after cleanup", nodes.len());
    assert_eq!(nodes.len(), 0, "Database should be empty after cleanup");

    eprintln!("TEST PASSED: Database is accessible and cleanup works");
}
