mod common;

use bytes::BytesMut;
use std::io::Cursor;
use tart_backend::decoder::{decode_message_frame, Decode};
use tart_backend::encoding::*;
use tart_backend::events::*;
use tart_backend::types::*;

fn get_test_timestamp() -> Timestamp {
    1_700_000_000_000_000 // Some timestamp in microseconds
}

#[test]
fn test_node_information_encoding_decoding() {
    let mut node_info = common::test_node_info([42u8; 32]);
    node_info.details.peer_address.ipv6 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    node_info.details.peer_address.port = 9000;
    node_info.flags = 1; // Uses PVM recompiler
    node_info.implementation_name = BoundedString::new("PolkaJam").unwrap();
    node_info.additional_info = BoundedString::new("Test node for telemetry").unwrap();

    let mut buf = BytesMut::new();
    node_info.encode(&mut buf).unwrap();

    // Check that protocol version is included
    assert_eq!(buf[0], TELEMETRY_PROTOCOL_VERSION);

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = NodeInformation::decode(&mut cursor).unwrap();
    assert_eq!(decoded.details.peer_id, node_info.details.peer_id);
    assert_eq!(decoded.details.peer_address.port, node_info.details.peer_address.port);
    assert_eq!(decoded.flags, node_info.flags);
    assert_eq!(decoded.implementation_name.as_str().unwrap(), "PolkaJam");
    assert_eq!(decoded.implementation_version.as_str().unwrap(), "1.0.0");
}

#[test]
fn test_dropped_event_encoding_decoding() {
    let event = Event::Dropped {
        timestamp: get_test_timestamp(),
        last_timestamp: get_test_timestamp() + 1000,
        num: 42,
    };

    let mut buf = BytesMut::new();
    event.encode(&mut buf).unwrap();

    // Check discriminator
    assert_eq!(buf[8], 0); // After timestamp

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::Dropped {
            timestamp,
            last_timestamp,
            num,
        } => {
            assert_eq!(timestamp, get_test_timestamp());
            assert_eq!(last_timestamp, get_test_timestamp() + 1000);
            assert_eq!(num, 42);
        }
        _ => panic!("Wrong event type decoded"),
    }
}

#[test]
fn test_status_event_encoding_decoding() {
    let guarantees = vec![0u8, 1, 2, 3, 4, 5]; // 6 cores example
    let event = Event::Status {
        timestamp: get_test_timestamp(),
        num_val_peers: 10,
        num_peers: 20,
        num_sync_peers: 15,
        num_guarantees: guarantees.clone(),
        num_shards: 100,
        shards_size: 1024 * 1024 * 10, // 10MB
        num_preimages: 5,
        preimages_size: 8,
    };

    let mut buf = BytesMut::new();
    event.encode(&mut buf).unwrap();

    // Check discriminator
    assert_eq!(buf[8], 10);

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::Status {
            timestamp,
            num_val_peers,
            num_peers,
            num_sync_peers,
            num_guarantees,
            num_shards,
            shards_size,
            num_preimages,
            preimages_size,
        } => {
            assert_eq!(timestamp, get_test_timestamp());
            assert_eq!(num_val_peers, 10);
            assert_eq!(num_peers, 20);
            assert_eq!(num_sync_peers, 15);
            assert_eq!(num_guarantees, guarantees);
            assert_eq!(num_shards, 100);
            assert_eq!(shards_size, 1024 * 1024 * 10);
            assert_eq!(num_preimages, 5);
            assert_eq!(preimages_size, 8);
        }
        _ => panic!("Wrong event type decoded"),
    }
}

#[test]
fn test_best_block_changed_event() {
    let event = Event::BestBlockChanged {
        timestamp: get_test_timestamp(),
        slot: 12345,
        hash: [0xAAu8; 32],
    };

    let mut buf = BytesMut::new();
    event.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 11); // Discriminator

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::BestBlockChanged {
            timestamp,
            slot,
            hash,
        } => {
            assert_eq!(timestamp, get_test_timestamp());
            assert_eq!(slot, 12345);
            assert_eq!(hash, [0xAAu8; 32]);
        }
        _ => panic!("Wrong event type decoded"),
    }
}

#[test]
fn test_finalized_block_changed_event() {
    let event = Event::FinalizedBlockChanged {
        timestamp: get_test_timestamp(),
        slot: 12340,
        hash: [0xBBu8; 32],
    };

    let mut buf = BytesMut::new();
    event.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 12); // Discriminator

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::FinalizedBlockChanged {
            timestamp,
            slot,
            hash,
        } => {
            assert_eq!(timestamp, get_test_timestamp());
            assert_eq!(slot, 12340);
            assert_eq!(hash, [0xBBu8; 32]);
        }
        _ => panic!("Wrong event type decoded"),
    }
}

#[test]
fn test_sync_status_changed_event() {
    for is_synced in [true, false] {
        let event = Event::SyncStatusChanged {
            timestamp: get_test_timestamp(),
            synced: is_synced,
        };

        let mut buf = BytesMut::new();
        event.encode(&mut buf).unwrap();
        assert_eq!(buf[8], 13); // Discriminator

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = Event::decode(&mut cursor).unwrap();

        match decoded {
            Event::SyncStatusChanged {
                timestamp,
                synced: decoded_synced,
            } => {
                assert_eq!(timestamp, get_test_timestamp());
                assert_eq!(decoded_synced, is_synced);
            }
            _ => panic!("Wrong event type decoded"),
        }
    }
}

#[test]
fn test_connection_refused_event() {
    let event = Event::ConnectionRefused {
        timestamp: get_test_timestamp(),
        from: PeerAddress {
            ipv6: [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            port: 30333,
        },
    };

    let mut buf = BytesMut::new();
    event.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 20); // Discriminator

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::ConnectionRefused {
            timestamp,
            from: peer_address,
        } => {
            assert_eq!(timestamp, get_test_timestamp());
            assert_eq!(peer_address.port, 30333);
        }
        _ => panic!("Wrong event type decoded"),
    }
}

#[test]
fn test_connecting_in_out_events() {
    // Test ConnectingIn
    let event_in = Event::ConnectingIn {
        timestamp: get_test_timestamp(),
        from: PeerAddress {
            ipv6: [0; 16],
            port: 9000,
        },
    };

    let mut buf = BytesMut::new();
    event_in.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 21); // Discriminator

    // Test ConnectingOut
    let event_out = Event::ConnectingOut {
        timestamp: get_test_timestamp(),
        to: PeerDetails {
            peer_id: [99u8; 32],
            peer_address: PeerAddress {
                ipv6: [0; 16],
                port: 9001,
            },
        },
    };

    buf.clear();
    event_out.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 24); // Discriminator
}

#[test]
fn test_connection_failed_events() {
    // Test ConnectionInFailed
    let event_in_failed = Event::ConnectInFailed {
        timestamp: get_test_timestamp(),
        connecting_id: 42,
        reason: BoundedString::new("Connection timeout").unwrap(),
    };

    let mut buf = BytesMut::new();
    event_in_failed.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 22); // Discriminator

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::ConnectInFailed {
            connecting_id,
            reason,
            ..
        } => {
            assert_eq!(connecting_id, 42);
            assert_eq!(reason.as_str().unwrap(), "Connection timeout");
        }
        _ => panic!("Wrong event type decoded"),
    }

    // Test ConnectionOutFailed
    let event_out_failed = Event::ConnectOutFailed {
        timestamp: get_test_timestamp(),
        connecting_id: 43,
        reason: BoundedString::new("Host unreachable").unwrap(),
    };

    buf.clear();
    event_out_failed.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 25); // Discriminator
}

#[test]
fn test_connected_events() {
    // Test ConnectedIn
    let event_in = Event::ConnectedIn {
        timestamp: get_test_timestamp(),
        connecting_id: 44,
        peer_id: [0x11u8; 32],
    };

    let mut buf = BytesMut::new();
    event_in.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 23); // Discriminator

    // Test ConnectedOut
    let event_out = Event::ConnectedOut {
        timestamp: get_test_timestamp(),
        connecting_id: 45,
    };

    buf.clear();
    event_out.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 26); // Discriminator
}

#[test]
fn test_disconnected_event() {
    // Test with terminator
    let event_with_terminator = Event::Disconnected {
        timestamp: get_test_timestamp(),
        peer: [0x22u8; 32],
        terminator: Some(ConnectionSide::Remote),
        reason: BoundedString::new("Peer disconnected").unwrap(),
    };

    let mut buf = BytesMut::new();
    event_with_terminator.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 27); // Discriminator

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::Disconnected {
            terminator, reason, ..
        } => {
            assert!(matches!(terminator, Some(ConnectionSide::Remote)));
            assert_eq!(reason.as_str().unwrap(), "Peer disconnected");
        }
        _ => panic!("Wrong event type decoded"),
    }

    // Test without terminator
    let event_no_terminator = Event::Disconnected {
        timestamp: get_test_timestamp(),
        peer: [0x33u8; 32],
        terminator: None,
        reason: BoundedString::new("Timeout").unwrap(),
    };

    buf.clear();
    event_no_terminator.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::Disconnected { terminator, .. } => {
            assert!(terminator.is_none());
        }
        _ => panic!("Wrong event type decoded"),
    }
}

#[test]
fn test_authoring_events() {
    // Test Authoring
    let authoring = Event::Authoring {
        timestamp: get_test_timestamp(),
        slot: 54321,
        parent: [0x44u8; 32],
    };

    let mut buf = BytesMut::new();
    authoring.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 40); // Discriminator

    // Test AuthoringFailed
    let authoring_failed = Event::AuthoringFailed {
        timestamp: get_test_timestamp(),
        authoring_id: 100,
        reason: BoundedString::new("Insufficient stake").unwrap(),
    };

    buf.clear();
    authoring_failed.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 41); // Discriminator

    // Test Authored
    let authored = Event::Authored {
        timestamp: get_test_timestamp(),
        authoring_id: 101,
        outline: BlockSummary {
            size_bytes: 100_000,
            hash: [0x55u8; 32],
            num_tickets: 10,
            num_preimages: 5,
            total_preimages_size: 1000,
            num_guarantees: 20,
            num_assurances: 50,
            num_dispute_verdicts: 0,
        },
    };

    buf.clear();
    authored.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 42); // Discriminator
}

#[test]
fn test_importing_events() {
    // Test Importing
    let importing = Event::Importing {
        timestamp: get_test_timestamp(),
        slot: 54322,
        outline: BlockSummary {
            size_bytes: 200_000,
            hash: [0x55u8; 32],
            num_tickets: 15,
            num_preimages: 8,
            total_preimages_size: 2000,
            num_guarantees: 30,
            num_assurances: 60,
            num_dispute_verdicts: 1,
        },
    };

    let mut buf = BytesMut::new();
    importing.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 43); // Discriminator

    // Test BlockVerificationFailed
    let verification_failed = Event::BlockVerificationFailed {
        timestamp: get_test_timestamp(),
        importing_id: 200,
        reason: BoundedString::new("Invalid signature").unwrap(),
    };

    buf.clear();
    verification_failed.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 44); // Discriminator

    // Test BlockVerified
    let verified = Event::BlockVerified {
        timestamp: get_test_timestamp(),
        importing_id: 201,
    };

    buf.clear();
    verified.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 45); // Discriminator
}

#[test]
fn test_block_execution_events() {
    // Test BlockExecutionFailed
    let exec_failed = Event::BlockExecutionFailed {
        timestamp: get_test_timestamp(),
        authoring_or_importing_id: 300,
        reason: BoundedString::new("Service ID collision").unwrap(),
    };

    let mut buf = BytesMut::new();
    exec_failed.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 46); // Discriminator

    // Test BlockExecuted
    let exec_success = Event::BlockExecuted {
        timestamp: get_test_timestamp(),
        authoring_or_importing_id: 301,
        accumulate_costs: vec![
            (
                100,
                AccumulateCost {
                    num_calls: 5,
                    num_transfers: 10,
                    num_items: 15,
                    total: ExecCost {
                        gas_used: 1_000_000,
                        elapsed_ns: 500_000,
                    },
                    load_ns: 50_000,
                    host_call: AccumulateHostCallCost {
                        state: ExecCost { gas_used: 200_000, elapsed_ns: 100_000 },
                        lookup: ExecCost { gas_used: 300_000, elapsed_ns: 150_000 },
                        preimage: ExecCost { gas_used: 100_000, elapsed_ns: 50_000 },
                        service: ExecCost { gas_used: 150_000, elapsed_ns: 75_000 },
                        transfer: ExecCost { gas_used: 150_000, elapsed_ns: 75_000 },
                        transfer_dest_gas: 50_000,
                        other: ExecCost { gas_used: 100_000, elapsed_ns: 50_000 },
                    },
                },
            ),
            (
                200,
                AccumulateCost {
                    num_calls: 3,
                    num_transfers: 5,
                    num_items: 8,
                    total: ExecCost {
                        gas_used: 500_000,
                        elapsed_ns: 250_000,
                    },
                    load_ns: 25_000,
                    host_call: AccumulateHostCallCost {
                        state: ExecCost { gas_used: 100_000, elapsed_ns: 50_000 },
                        lookup: ExecCost { gas_used: 150_000, elapsed_ns: 75_000 },
                        preimage: ExecCost { gas_used: 50_000, elapsed_ns: 25_000 },
                        service: ExecCost { gas_used: 75_000, elapsed_ns: 37_500 },
                        transfer: ExecCost { gas_used: 75_000, elapsed_ns: 37_500 },
                        transfer_dest_gas: 25_000,
                        other: ExecCost { gas_used: 50_000, elapsed_ns: 25_000 },
                    },
                },
            ),
        ],
    };

    buf.clear();
    exec_success.encode(&mut buf).unwrap();
    assert_eq!(buf[8], 47); // Discriminator

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::BlockExecuted {
            authoring_or_importing_id,
            accumulate_costs,
            ..
        } => {
            assert_eq!(authoring_or_importing_id, 301);
            assert_eq!(accumulate_costs.len(), 2);
            assert_eq!(accumulate_costs[0].0, 100);
            assert_eq!(accumulate_costs[1].0, 200);
        }
        _ => panic!("Wrong event type decoded"),
    }
}

#[test]
fn test_event_type_discriminators() {
    // Verify all event types have correct discriminators
    assert_eq!(EventType::Dropped as u8, 0);
    assert_eq!(EventType::Status as u8, 10);
    assert_eq!(EventType::BestBlockChanged as u8, 11);
    assert_eq!(EventType::FinalizedBlockChanged as u8, 12);
    assert_eq!(EventType::SyncStatusChanged as u8, 13);
    assert_eq!(EventType::ConnectionRefused as u8, 20);
    assert_eq!(EventType::ConnectingIn as u8, 21);
    assert_eq!(EventType::ConnectInFailed as u8, 22);
    assert_eq!(EventType::ConnectedIn as u8, 23);
    assert_eq!(EventType::ConnectingOut as u8, 24);
    assert_eq!(EventType::ConnectOutFailed as u8, 25);
    assert_eq!(EventType::ConnectedOut as u8, 26);
    assert_eq!(EventType::Disconnected as u8, 27);
    assert_eq!(EventType::Authoring as u8, 40);
    assert_eq!(EventType::AuthoringFailed as u8, 41);
    assert_eq!(EventType::Authored as u8, 42);
    assert_eq!(EventType::Importing as u8, 43);
    assert_eq!(EventType::BlockVerificationFailed as u8, 44);
    assert_eq!(EventType::BlockVerified as u8, 45);
    assert_eq!(EventType::BlockExecutionFailed as u8, 46);
    assert_eq!(EventType::BlockExecuted as u8, 47);
}

#[test]
fn test_event_timestamp_and_type_extraction() {
    let events = vec![
        Event::Dropped {
            timestamp: 1000,
            last_timestamp: 2000,
            num: 5,
        },
        Event::Status {
            timestamp: 3000,
            num_val_peers: 10,
            num_peers: 20,
            num_sync_peers: 15,
            num_guarantees: vec![],
            num_shards: 100,
            shards_size: 1000,
            num_preimages: 5,
            preimages_size: 8,
        },
        Event::BestBlockChanged {
            timestamp: 4000,
            slot: 100,
            hash: [0; 32],
        },
    ];

    for event in events {
        let timestamp = event.timestamp();
        let event_type = event.event_type();

        match &event {
            Event::Dropped { timestamp: ts, .. } => {
                assert_eq!(timestamp, *ts);
                assert_eq!(event_type as u8, 0);
            }
            Event::Status { timestamp: ts, .. } => {
                assert_eq!(timestamp, *ts);
                assert_eq!(event_type as u8, 10);
            }
            Event::BestBlockChanged { timestamp: ts, .. } => {
                assert_eq!(timestamp, *ts);
                assert_eq!(event_type as u8, 11);
            }
            _ => {}
        }
    }
}

#[test]
fn test_event_encoded_size() {
    // Test that encoded_size matches actual encoded size
    let events = vec![
        Event::SyncStatusChanged {
            timestamp: get_test_timestamp(),
            synced: true,
        },
        Event::ConnectionRefused {
            timestamp: get_test_timestamp(),
            from: PeerAddress {
                ipv6: [0; 16],
                port: 9000,
            },
        },
        Event::BestBlockChanged {
            timestamp: get_test_timestamp(),
            slot: 12345,
            hash: [0xAAu8; 32],
        },
    ];

    for event in events {
        let mut buf = BytesMut::new();
        event.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), event.encoded_size());
    }
}

#[test]
fn test_status_event_with_message_frame() {
    // Test the exact Status event that's failing in api_tests
    let event = Event::Status {
        timestamp: 3_000_000,
        num_val_peers: 5,
        num_peers: 10,
        num_sync_peers: 8,
        num_guarantees: vec![1, 2, 3, 4],
        num_shards: 50,
        shards_size: 1024 * 1024,
        num_preimages: 3,
        preimages_size: 7,
    };

    // Encode with message frame (as done in api_tests)
    let encoded = encode_message(&event).unwrap();
    println!("Encoded message size: {}", encoded.len());

    // Decode the message frame
    let (size, msg_data) = decode_message_frame(&encoded).unwrap();
    println!("Frame size: {}, data len: {}", size, msg_data.len());

    // Decode the event
    let mut cursor = Cursor::new(msg_data);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::Status {
            timestamp,
            num_val_peers,
            num_guarantees,
            ..
        } => {
            assert_eq!(timestamp, 3_000_000);
            assert_eq!(num_val_peers, 5);
            assert_eq!(num_guarantees, vec![1, 2, 3, 4]);
        }
        _ => panic!("Wrong event type decoded"),
    }
}
