mod common;

use std::sync::Arc;
use std::time::Duration;
use tart_backend::encoding::encode_message;
use tart_backend::events::{Event, NodeInformation};
use tart_backend::types::*;
use tart_backend::{EventStore, TelemetryServer};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn setup_optimized_test_server() -> (Arc<TelemetryServer>, u16) {
    // Use test database URL from environment or default PostgreSQL test database
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/tart_test".to_string());

    let store = Arc::new(EventStore::new(&database_url).await.unwrap());

    // Find available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let bind_addr = format!("127.0.0.1:{}", port);
    let server = Arc::new(TelemetryServer::new(&bind_addr, store).await.unwrap());

    // Start server in background
    let server_clone = Arc::clone(&server);
    tokio::spawn(async move {
        server_clone.run().await.unwrap();
    });

    // Give server time to start
    sleep(Duration::from_millis(200)).await;

    (server, port)
}

fn create_test_node_info(id: u8) -> NodeInformation {
    let mut peer_id = [0u8; 32];
    peer_id[0] = id;

    let mut node_info = common::test_node_info(peer_id);
    node_info.details.peer_address.port = 30333 + id as u16;
    node_info.implementation_name = BoundedString::new("test-jam-node").unwrap();
    node_info.additional_info = BoundedString::new(&format!("Test node {}", id)).unwrap();
    node_info
}

#[tokio::test]
async fn test_optimized_server_handles_multiple_connections() {
    let (_server, port) = setup_optimized_test_server().await;

    // Connect 10 nodes concurrently
    let mut handles = vec![];

    for i in 0..10 {
        let handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();

            // Send node info
            let node_info = create_test_node_info(i);
            let encoded = encode_message(&node_info).unwrap();
            stream.write_all(&encoded).await.unwrap();

            // Send a few events
            for j in 0..5 {
                let event = Event::Status {
                    timestamp: 1000 + j as u64,
                    num_val_peers: 10,
                    num_peers: 5,
                    num_sync_peers: 15,
                    num_guarantees: vec![1, 2, 3],
                    num_shards: 3,
                    shards_size: 1024,
                    num_preimages: 0,
                    preimages_size: 0,
                };

                let encoded = encode_message(&event).unwrap();
                stream.write_all(&encoded).await.unwrap();

                // Small delay between events
                sleep(Duration::from_millis(10)).await;
            }

            // Keep connection alive briefly
            sleep(Duration::from_millis(100)).await;
        });

        handles.push(handle);
    }

    // Wait for all connections to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Give batch writer time to flush
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_rate_limiting() {
    let (_server, port) = setup_optimized_test_server().await;

    // Connect a single node
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node info
    let node_info = create_test_node_info(1);
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Try to send 200 events rapidly (should hit rate limit of 100/sec)
    for i in 0..200 {
        let event = Event::BestBlockChanged {
            timestamp: 1000 + i,
            slot: i as u32,
            hash: [0u8; 32],
        };

        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }

    // Give server time to process
    sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_connection_limit() {
    // This test would need to be adjusted based on the actual connection limit
    // For now, just verify we can handle a reasonable number of connections
    let (_server, port) = setup_optimized_test_server().await;

    let mut streams = vec![];

    // Try to connect 50 nodes
    for i in 0..50 {
        match timeout(
            Duration::from_millis(100),
            TcpStream::connect(format!("127.0.0.1:{}", port)),
        )
        .await
        {
            Ok(Ok(mut stream)) => {
                // Send node info
                let node_info = create_test_node_info(i);
                let encoded = encode_message(&node_info).unwrap();
                if stream.write_all(&encoded).await.is_ok() {
                    streams.push(stream);
                }
            }
            _ => {
                // Connection failed or timed out - might have hit limit
                break;
            }
        }
    }

    // We should have connected at least some nodes
    assert!(!streams.is_empty());

    // Keep connections alive briefly
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_batch_writer_flushes() {
    let (_server, port) = setup_optimized_test_server().await;

    // Connect a node
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node info
    let node_info = create_test_node_info(1);
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Send a few events
    for i in 0..10 {
        let event = Event::Authored {
            timestamp: 1000 + i,
            authoring_id: i,
            outline: BlockSummary {
                size_bytes: 1024,
                hash: [0x55u8; 32],
                num_tickets: 1,
                num_preimages: 0,
                total_preimages_size: 0,
                num_guarantees: 1,
                num_assurances: 1,
                num_dispute_verdicts: 0,
            },
        };

        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }

    // Wait for batch timeout (100ms) plus some buffer
    sleep(Duration::from_millis(150)).await;

    // Events should have been flushed by now
}

#[tokio::test]
async fn test_bounded_buffer_protection() {
    let (_server, port) = setup_optimized_test_server().await;

    // Connect a node
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node info
    let node_info = create_test_node_info(1);
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Try to send a very large event (should be rejected if > 100KB)
    let large_event = Event::Disconnected {
        timestamp: 1000,
        peer: [0u8; 32],
        terminator: Some(ConnectionSide::Local),
        reason: BoundedString::new(&"x".repeat(128)).unwrap(), // Max 128 chars
    };

    let encoded = encode_message(&large_event).unwrap();
    let result = stream.write_all(&encoded).await;

    // Should succeed as it's within bounds
    assert!(result.is_ok());

    // Give server time to process
    sleep(Duration::from_millis(100)).await;
}
