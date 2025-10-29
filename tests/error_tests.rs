use bytes::{BufMut, BytesMut};
use std::io::Cursor;
use tart_backend::decoder::{decode_message_frame, decode_variable_length, Decode, DecodingError};
use tart_backend::encoding::*;
use tart_backend::events::*;
use tart_backend::types::*;

#[test]
fn test_decode_insufficient_data_errors() {
    // Test u8 with empty buffer
    let buf = vec![];
    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        u8::decode(&mut cursor),
        Err(DecodingError::InsufficientData {
            needed: 1,
            available: 0
        })
    ));

    // Test u16 with 1 byte
    let buf = vec![0xFF];
    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        u16::decode(&mut cursor),
        Err(DecodingError::InsufficientData {
            needed: 2,
            available: 1
        })
    ));

    // Test u32 with 3 bytes
    let buf = vec![0xFF, 0xFF, 0xFF];
    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        u32::decode(&mut cursor),
        Err(DecodingError::InsufficientData {
            needed: 4,
            available: 3
        })
    ));

    // Test u64 with 7 bytes
    let buf = vec![0xFF; 7];
    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        u64::decode(&mut cursor),
        Err(DecodingError::InsufficientData {
            needed: 8,
            available: 7
        })
    ));

    // Test array with insufficient data
    let buf = vec![0xFF; 31];
    let mut cursor = Cursor::new(&buf[..]);
    assert!(<[u8; 32]>::decode(&mut cursor).is_err());
}

#[test]
fn test_invalid_discriminator_errors() {
    // Test invalid bool value
    let buf = vec![2]; // Only 0 or 1 are valid
    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        bool::decode(&mut cursor),
        Err(DecodingError::InvalidBool(2))
    ));

    // Test invalid Option discriminator
    let buf = vec![2, 0, 0, 0, 0]; // Only 0 or 1 are valid
    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        Option::<u32>::decode(&mut cursor),
        Err(DecodingError::InvalidDiscriminator(2))
    ));

    // Test invalid ConnectionSide
    let buf = vec![2]; // Only 0 or 1 are valid
    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        ConnectionSide::decode(&mut cursor),
        Err(DecodingError::InvalidDiscriminator(2))
    ));
}

#[test]
fn test_string_length_limit_errors() {
    // Test string longer than limit
    let mut buf = BytesMut::new();
    encode_variable_length(33, &mut buf).unwrap(); // Length 33 for String<32>
    buf.extend_from_slice(&[b'a'; 33]);

    let mut cursor = Cursor::new(&buf[..]);
    // After resilience improvements, moderately oversized strings are truncated
    let result = BoundedString::<32>::decode(&mut cursor);
    assert!(
        result.is_ok(),
        "Moderately oversized strings should be truncated and recovered"
    );

    // Test various string limits
    let limits = vec![
        (32, 33),   // NodeName, NodeVersion
        (128, 129), // Reason
        (512, 513), // NodeNote
    ];

    for (max, length) in limits {
        let mut buf = BytesMut::new();
        encode_variable_length(length as u64, &mut buf).unwrap();
        buf.extend_from_slice(&vec![b'x'; length]);

        let mut cursor = Cursor::new(&buf[..]);
        // After resilience improvements, moderately oversized strings are truncated
        match max {
            32 => {
                let result = BoundedString::<32>::decode(&mut cursor);
                assert!(
                    result.is_ok(),
                    "Moderately oversized strings should be truncated"
                );
            }
            128 => {
                let result = BoundedString::<128>::decode(&mut cursor);
                assert!(
                    result.is_ok(),
                    "Moderately oversized strings should be truncated"
                );
            }
            512 => {
                let result = BoundedString::<512>::decode(&mut cursor);
                assert!(
                    result.is_ok(),
                    "Moderately oversized strings should be truncated"
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_invalid_utf8_errors() {
    // Test invalid UTF-8 sequences
    let invalid_sequences = vec![
        vec![0xFF, 0xFE],             // Invalid start bytes
        vec![0xC0, 0x80],             // Overlong encoding
        vec![0xED, 0xA0, 0x80],       // Surrogate half
        vec![0xF4, 0x90, 0x80, 0x80], // Out of range
        vec![0xC2],                   // Incomplete sequence
        vec![0xE0, 0x80],             // Incomplete 3-byte
        vec![0xF0, 0x80, 0x80],       // Incomplete 4-byte
    ];

    for invalid in invalid_sequences {
        let mut buf = BytesMut::new();
        encode_variable_length(invalid.len() as u64, &mut buf).unwrap();
        buf.extend_from_slice(&invalid);

        let mut cursor = Cursor::new(&buf[..]);
        // After resilience improvements, invalid UTF-8 is recovered using lossy conversion
        let result = BoundedString::<128>::decode(&mut cursor);
        assert!(
            result.is_ok(),
            "Invalid UTF-8 should be recovered using lossy conversion"
        );
    }
}

#[test]
fn test_message_frame_errors() {
    // Test frame with insufficient size bytes
    let buf = vec![0xFF, 0xFF, 0xFF]; // Only 3 bytes, need 4
    assert!(decode_message_frame(&buf).is_err());

    // Test frame with size too large
    let mut buf = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Max u32 (> 10MB limit)
    buf.extend_from_slice(&[0; 100]); // Some data
    assert!(matches!(
        decode_message_frame(&buf),
        Err(DecodingError::MessageTooLarge(4294967295))
    ));

    // Test frame with insufficient message data
    let buf = vec![10, 0, 0, 0, 1, 2, 3]; // Says 10 bytes but only has 3
    assert!(matches!(
        decode_message_frame(&buf),
        Err(DecodingError::InsufficientData {
            needed: 14,
            available: 7
        })
    ));
}

#[test]
fn test_variable_length_overflow() {
    // Test variable length encoding that would overflow
    let mut buf = vec![];
    for _ in 0..10 {
        buf.push(0xFF); // All continuation bits set
    }
    buf.push(0x01); // Final byte

    let mut cursor = Cursor::new(&buf[..]);
    // Should fail as it would exceed 64 bits
    assert!(decode_variable_length(&mut cursor).is_err());
}

#[test]
fn test_event_invalid_discriminators() {
    // Test unknown event discriminator
    let mut buf = BytesMut::new();
    let timestamp: u64 = 1_000_000;
    timestamp.encode(&mut buf).unwrap();
    buf.put_u8(255); // Invalid discriminator

    let mut cursor = Cursor::new(&buf[..]);
    assert!(matches!(
        Event::decode(&mut cursor),
        Err(DecodingError::InvalidDiscriminator(255))
    ));

    // Test discriminators in gaps (e.g., between event groups)
    let invalid_discriminators = vec![
        1, 2, 3, 4, 5, 6, 7, 8, 9, // Gap before status events
        14, 15, 16, 17, 18, 19, // Gap before networking events
        28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, // Gap before block events
        48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, // Gap before block distribution
        68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, // Gap before tickets
        86, 87, 88, 89, // Gap before guaranteeing
        114, 115, 116, 117, 118, 119, // Gap before availability
        148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, // Gap before preimage
        168, 169, 170, // After preimage events
    ];

    for discriminator in invalid_discriminators {
        let mut buf = BytesMut::new();
        timestamp.encode(&mut buf).unwrap();
        buf.put_u8(discriminator);

        let mut cursor = Cursor::new(&buf[..]);
        assert!(Event::decode(&mut cursor).is_err());
    }
}

#[test]
fn test_node_information_protocol_version() {
    // Test invalid protocol version
    let mut buf = BytesMut::new();
    buf.put_u8(1); // Wrong version (should be 0)

    // Add rest of node info
    let peer_id = [0u8; 32];
    peer_id.encode(&mut buf).unwrap();

    let peer_address = PeerAddress {
        ipv6: [0; 16],
        port: 9000,
    };
    peer_address.encode(&mut buf).unwrap();

    let flags = 0u32;
    flags.encode(&mut buf).unwrap();

    BoundedString::<32>::new("test")
        .unwrap()
        .encode(&mut buf)
        .unwrap();
    BoundedString::<32>::new("1.0")
        .unwrap()
        .encode(&mut buf)
        .unwrap();
    BoundedString::<512>::new("info")
        .unwrap()
        .encode(&mut buf)
        .unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    assert!(NodeInformation::decode(&mut cursor).is_err());
}

#[test]
fn test_maximum_value_edge_cases() {
    // Test encoding/decoding maximum values for each type
    let test_cases: Vec<(Box<dyn Encode>, usize)> = vec![
        (Box::new(u8::MAX), 1),
        (Box::new(u16::MAX), 2),
        (Box::new(u32::MAX), 4),
        (Box::new(u64::MAX), 8),
    ];

    for (value, expected_size) in test_cases {
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), expected_size);
        assert_eq!(value.encoded_size(), expected_size);
    }
}

#[test]
fn test_zero_length_sequences() {
    // Test empty Vec<u8>
    let empty_vec: Vec<u8> = vec![];
    let mut buf = BytesMut::new();
    empty_vec.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 1); // Just length byte (0)

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Vec::<u8>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.len(), 0);

    // Test empty string
    let empty_string = BoundedString::<32>::new("").unwrap();
    buf.clear();
    empty_string.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 1); // Just length byte (0)

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = BoundedString::<32>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.as_str().unwrap(), "");
}

#[test]
fn test_complex_nested_structures() {
    // Test deeply nested Option types
    let nested: Option<Option<Option<u32>>> = Some(Some(Some(42)));
    let mut buf = BytesMut::new();
    nested.encode(&mut buf).unwrap();

    // Should be: 1 (Some) + 1 (Some) + 1 (Some) + 4 (u32)
    assert_eq!(buf.len(), 7);

    // Test WorkPackageSummary with maximum nesting
    let summary = WorkPackageSummary {
        work_package_size: u32::MAX,
        anchor: [0xFF; 32],
        lookup_anchor_slot: u32::MAX,
        prerequisites: vec![[0xAA; 32]; 100], // Many prerequisites
        work_items: vec![
            WorkItemSummary {
                service_id: u32::MAX,
                payload_size: u32::MAX,
                refine_gas_limit: u64::MAX,
                accumulate_gas_limit: u64::MAX,
                sum_of_extrinsic_lengths: u32::MAX,
                imports: vec![
                    ImportSpec {
                        root_identifier: RootIdentifier::WorkPackageHash([0xFF; 32]),
                        export_index: 32767, // Max without flag
                    };
                    50
                ], // Many imports
                num_exported_segments: u16::MAX,
            };
            10
        ], // Multiple work items
    };

    buf.clear();
    summary.encode(&mut buf).unwrap();
    assert!(buf.len() > 1000); // Should be quite large
}

#[test]
fn test_event_edge_cases() {
    // Test Status event with maximum core count
    let large_guarantees = vec![0u8; 1000]; // 1000 cores
    let event = Event::Status {
        timestamp: u64::MAX,
        num_val_peers: u32::MAX,
        num_peers: u32::MAX,
        num_sync_peers: u32::MAX,
        num_guarantees: large_guarantees.clone(),
        num_shards: u32::MAX,
        shards_size: u64::MAX,
        num_preimages: u32::MAX,
        preimages_size: u32::MAX,
    };

    let mut buf = BytesMut::new();
    event.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::Status { num_guarantees, .. } => {
            assert_eq!(num_guarantees.len(), 1000);
        }
        _ => panic!("Wrong event type"),
    }

    // Test BlockExecuted with many services
    let services: Vec<(ServiceId, AccumulateCost)> = (0..1000)
        .map(|i| {
            (
                i,
                AccumulateCost {
                    num_calls: 0,
                    num_transfers: 0,
                    num_items: 0,
                    total: ExecCost {
                        gas_used: 0,
                        elapsed_ns: 0,
                    },
                    read_write_calls: ExecCost {
                        gas_used: 0,
                        elapsed_ns: 0,
                    },
                    lookup_query_calls: ExecCost {
                        gas_used: 0,
                        elapsed_ns: 0,
                    },
                    info_new_calls: ExecCost {
                        gas_used: 0,
                        elapsed_ns: 0,
                    },
                    total_gas_charged: 0,
                    other_host_calls: ExecCost {
                        gas_used: 0,
                        elapsed_ns: 0,
                    },
                },
            )
        })
        .collect();

    let event = Event::BlockExecuted {
        timestamp: 1_000_000,
        authoring_or_importing_id: 42,
        accumulate_costs: services,
    };

    buf.clear();
    event.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Event::decode(&mut cursor).unwrap();

    match decoded {
        Event::BlockExecuted {
            accumulate_costs, ..
        } => {
            assert_eq!(accumulate_costs.len(), 1000);
        }
        _ => panic!("Wrong event type"),
    }
}

#[test]
fn test_import_spec_bit_flags() {
    // Test ImportSpec with bit 15 set and unset
    let spec1 = ImportSpec {
        root_identifier: RootIdentifier::SegmentsRoot([0xFF; 32]),
        export_index: 32767, // Maximum without flag
    };

    let mut buf = BytesMut::new();
    spec1.encode(&mut buf).unwrap();

    // Check encoded value
    let index_bytes = &buf[32..34];
    let encoded_index = u16::from_le_bytes([index_bytes[0], index_bytes[1]]);
    assert_eq!(encoded_index, 32767);
    assert_eq!(encoded_index & 0x8000, 0);

    let spec2 = ImportSpec {
        root_identifier: RootIdentifier::WorkPackageHash([0xAA; 32]),
        export_index: 0, // Minimum with flag
    };

    buf.clear();
    spec2.encode(&mut buf).unwrap();

    let index_bytes = &buf[32..34];
    let encoded_index = u16::from_le_bytes([index_bytes[0], index_bytes[1]]);
    assert_eq!(encoded_index, 0x8000);
    assert_ne!(encoded_index & 0x8000, 0);

    // Test maximum export index with flag
    let spec3 = ImportSpec {
        root_identifier: RootIdentifier::WorkPackageHash([0xBB; 32]),
        export_index: 32767,
    };

    buf.clear();
    spec3.encode(&mut buf).unwrap();

    let index_bytes = &buf[32..34];
    let encoded_index = u16::from_le_bytes([index_bytes[0], index_bytes[1]]);
    assert_eq!(encoded_index, 32767 | 0x8000);
}

#[test]
fn test_reason_string_edge_cases() {
    // Test maximum length reason (128 bytes)
    let max_reason = "a".repeat(128);
    let reason = BoundedString::<128>::new(&max_reason).unwrap();

    let mut buf = BytesMut::new();
    reason.encode(&mut buf).unwrap();

    // Length encoding for 128 should be 2 bytes: [128, 1]
    assert_eq!(buf[0], 128);
    assert_eq!(buf[1], 1);
    assert_eq!(&buf[2..], max_reason.as_bytes());

    // Test reason with UTF-8 characters
    let utf8_reason = "Error: 🚨 Critical failure 💥";
    let reason = BoundedString::<128>::new(utf8_reason).unwrap();

    buf.clear();
    reason.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = BoundedString::<128>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.as_str().unwrap(), utf8_reason);
}

#[test]
fn test_node_flags_validation() {
    // Test valid node flags
    let valid_flags = vec![
        0b00000000, // All flags off
        0b00000001, // PVM recompiler
    ];

    for flags in valid_flags {
        let node_info = NodeInformation {
            peer_id: [0; 32],
            peer_address: PeerAddress {
                ipv6: [0; 16],
                port: 9000,
            },
            node_flags: flags,
            implementation_name: BoundedString::new("test").unwrap(),
            implementation_version: BoundedString::new("1.0").unwrap(),
            additional_info: BoundedString::new("").unwrap(),
        };

        let mut buf = BytesMut::new();
        node_info.encode(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = NodeInformation::decode(&mut cursor).unwrap();
        assert_eq!(decoded.node_flags, flags);
    }

    // Note: According to spec, other bits should be 0, but there's no validation
    // This is a potential area for strict validation in the future
}
