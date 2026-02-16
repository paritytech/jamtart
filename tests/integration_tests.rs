mod common;

use std::sync::Arc;
use std::time::Duration;
use tart_backend::encoding::encode_message;
use tart_backend::events::{Event, NodeInformation};
use tart_backend::types::*;
use tart_backend::{EventStore, TelemetryServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

async fn setup_test_server() -> (Arc<TelemetryServer>, u16) {
    // Use test PostgreSQL database
    let database_url = common::test_database_url();
    let store = Arc::new(EventStore::new(&database_url).await.unwrap());

    // Clean database before each test
    let _ = store.cleanup_test_data().await;

    let server = Arc::new(TelemetryServer::new("127.0.0.1:0", Some(store)).await.unwrap());
    let port = server.local_addr().unwrap().port();

    // Start server in background
    let server_clone = Arc::clone(&server);
    tokio::spawn(async move {
        server_clone.run().await.unwrap();
    });

    (server, port)
}

fn create_test_node_info() -> NodeInformation {
    let mut node_info = common::test_node_info([42u8; 32]);
    node_info.implementation_name = BoundedString::new("test-jam-node").unwrap();
    node_info.additional_info = BoundedString::new("Integration test node").unwrap();
    node_info
}

#[tokio::test]
async fn test_tcp_connection_and_node_info() {
    let (server, port) = setup_test_server().await;

    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node information
    let node_info = create_test_node_info();
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Wait for server to register the connection
    server.wait_for_connections(1).await;

    // Connection should remain open
    assert!(stream.peer_addr().is_ok());
}

#[tokio::test]
async fn test_send_multiple_events() {
    let (server, port) = setup_test_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node information first
    let node_info = create_test_node_info();
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Send various events
    let events = vec![
        Event::SyncStatusChanged {
            timestamp: 1_000_000,
            synced: false,
        },
        Event::Status {
            timestamp: 2_000_000,
            num_val_peers: 5,
            num_peers: 10,
            num_sync_peers: 8,
            num_guarantees: vec![0, 1, 2, 3],
            num_shards: 50,
            shards_size: 1024 * 1024,
            num_preimages: 3,
            preimages_size: 7,
        },
        Event::BestBlockChanged {
            timestamp: 3_000_000,
            slot: 100,
            hash: [0xAAu8; 32],
        },
        Event::FinalizedBlockChanged {
            timestamp: 4_000_000,
            slot: 95,
            hash: [0xBBu8; 32],
        },
        Event::SyncStatusChanged {
            timestamp: 5_000_000,
            synced: true,
        },
    ];

    for event in events {
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    // Wait for server to process all events
    common::flush_and_wait(&server).await;

    // Connection should still be open
    assert!(stream.peer_addr().is_ok());
}

#[tokio::test]
async fn test_multiple_concurrent_connections() {
    let (server, port) = setup_test_server().await;

    let mut handles = vec![];

    // Create 5 concurrent connections
    for i in 0..5 {
        let handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();

            // Create unique node info for each connection
            let mut node_info = create_test_node_info();
            node_info.details.peer_id[0] = i;

            let encoded = encode_message(&node_info).unwrap();
            stream.write_all(&encoded).await.unwrap();

            // Send some events
            for j in 0..10 {
                let event = Event::Status {
                    timestamp: (i as u64 * 1000) + (j as u64),
                    num_val_peers: i as u32,
                    num_peers: j as u32,
                    num_sync_peers: 0,
                    num_guarantees: vec![],
                    num_shards: 0,
                    shards_size: 0,
                    num_preimages: 0,
                    preimages_size: 0,
                };

                let encoded = encode_message(&event).unwrap();
                stream.write_all(&encoded).await.unwrap();
                sleep(Duration::from_millis(10)).await;
            }

            // Return stream to keep connection alive
            stream
        });

        handles.push(handle);
    }

    // Wait for all tasks to finish sending, keeping streams alive
    let mut streams = vec![];
    for handle in handles {
        streams.push(handle.await.unwrap());
    }

    // Wait for server to register all connections
    server.wait_for_connections(5).await;

    // Check that server tracked all connections (streams still alive)
    let connections = server.get_connections();
    assert_eq!(connections.len(), 5);

    // Now drop streams to allow cleanup
    drop(streams);
}

#[tokio::test]
async fn test_connection_without_node_info() {
    let (_server, port) = setup_test_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Try to send an event without sending node info first
    let event = Event::SyncStatusChanged {
        timestamp: 1_000_000,
        synced: true,
    };

    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Server should close the connection
    let mut buf = vec![0u8; 1024];
    let result = timeout(Duration::from_secs(1), stream.read(&mut buf)).await;

    match result {
        Ok(Ok(0)) => {
            // Connection closed as expected
        }
        Ok(Ok(_)) => panic!("Server should have closed connection"),
        Ok(Err(_)) => panic!("Read error"),
        Err(_) => panic!("Timeout - server didn't close connection"),
    }
}

#[tokio::test]
async fn test_invalid_message_handling() {
    let (_server, port) = setup_test_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node info first
    let node_info = create_test_node_info();
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Send invalid message (size too large)
    let invalid_size = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Max u32
    stream.write_all(&invalid_size).await.unwrap();

    // Server should close connection
    let mut buf = vec![0u8; 1024];
    let result = timeout(Duration::from_secs(1), stream.read(&mut buf)).await;

    match result {
        Ok(Ok(0)) => {
            // Connection closed as expected
        }
        _ => panic!("Server should have closed connection on invalid message"),
    }
}

#[tokio::test]
async fn test_connection_tracking() {
    let (server, port) = setup_test_server().await;

    // Initially no connections
    assert_eq!(server.get_connections().len(), 0);

    // Connect first node
    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let mut node_info1 = create_test_node_info();
    node_info1.details.peer_id[0] = 1;
    let encoded = encode_message(&node_info1).unwrap();
    stream1.write_all(&encoded).await.unwrap();

    server.wait_for_connections(1).await;
    assert_eq!(server.get_connections().len(), 1);

    // Connect second node
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let mut node_info2 = create_test_node_info();
    node_info2.details.peer_id[0] = 2;
    let encoded = encode_message(&node_info2).unwrap();
    stream2.write_all(&encoded).await.unwrap();

    server.wait_for_connections(2).await;
    assert_eq!(server.get_connections().len(), 2);

    // Disconnect first node
    drop(stream1);
    server.wait_for_connections(1).await;
    assert_eq!(server.get_connections().len(), 1);

    // Disconnect second node
    drop(stream2);
    server.wait_for_connections(0).await;
    assert_eq!(server.get_connections().len(), 0);
}

#[tokio::test]
async fn test_large_event_handling() {
    let (server, port) = setup_test_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node info
    let node_info = create_test_node_info();
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Create event with large data
    let large_event = Event::BlockExecuted {
        timestamp: 1_000_000,
        authoring_or_importing_id: 42,
        accumulate_costs: (0..100)
            .map(|i| {
                (
                    i,
                    AccumulateCost {
                        num_calls: i * 10,
                        num_transfers: i * 20,
                        num_items: i * 30,
                        total: ExecCost {
                            gas_used: (i as u64) * 1_000_000,
                            elapsed_ns: (i as u64) * 500_000,
                        },
                        load_ns: (i as u64) * 50_000,
                        host_call: AccumulateHostCallCost {
                            state: ExecCost {
                                gas_used: (i as u64) * 100_000,
                                elapsed_ns: (i as u64) * 50_000,
                            },
                            lookup: ExecCost {
                                gas_used: (i as u64) * 200_000,
                                elapsed_ns: (i as u64) * 100_000,
                            },
                            preimage: ExecCost {
                                gas_used: (i as u64) * 50_000,
                                elapsed_ns: (i as u64) * 25_000,
                            },
                            service: ExecCost {
                                gas_used: (i as u64) * 150_000,
                                elapsed_ns: (i as u64) * 75_000,
                            },
                            transfer: ExecCost {
                                gas_used: (i as u64) * 150_000,
                                elapsed_ns: (i as u64) * 75_000,
                            },
                            transfer_dest_gas: (i as u64) * 50_000,
                            other: ExecCost {
                                gas_used: (i as u64) * 50_000,
                                elapsed_ns: (i as u64) * 25_000,
                            },
                        },
                    },
                )
            })
            .collect(),
    };

    let encoded = encode_message(&large_event).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Wait for server to process the event
    common::flush_and_wait(&server).await;

    // Connection should still be open
    assert!(stream.peer_addr().is_ok());
}

#[tokio::test]
async fn test_rapid_event_sending() {
    let (server, port) = setup_test_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send node info
    let node_info = create_test_node_info();
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Send 1000 events rapidly
    for i in 0..1000 {
        let event = Event::BestBlockChanged {
            timestamp: i as u64,
            slot: i,
            hash: [(i % 256) as u8; 32],
        };

        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }

    // Flush the stream
    stream.flush().await.unwrap();

    // Wait for server to process all events
    common::flush_and_wait(&server).await;

    // Connection should still be open
    assert!(stream.peer_addr().is_ok());
}
