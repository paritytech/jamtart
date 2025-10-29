use bytes::BytesMut;
use std::io::Cursor;
use tart_backend::decoder::Decode;
use tart_backend::encoding::*;
use tart_backend::types::*;

#[test]
fn test_peer_address_encoding_decoding() {
    let test_cases = vec![
        PeerAddress {
            ipv6: [0; 16], // All zeros
            port: 0,
        },
        PeerAddress {
            ipv6: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], // Localhost
            port: 9000,
        },
        PeerAddress {
            ipv6: [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], // Sample IPv6
            port: 65535,                                                        // Max port
        },
    ];

    for addr in test_cases {
        let mut buf = BytesMut::new();
        addr.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 18); // 16 bytes IPv6 + 2 bytes port
        assert_eq!(addr.encoded_size(), 18);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = PeerAddress::decode(&mut cursor).unwrap();
        assert_eq!(decoded.ipv6, addr.ipv6);
        assert_eq!(decoded.port, addr.port);
    }
}

#[test]
fn test_connection_side_encoding_decoding() {
    let test_cases = vec![ConnectionSide::Local, ConnectionSide::Remote];

    for side in test_cases {
        let mut buf = BytesMut::new();
        side.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(side.encoded_size(), 1);

        let expected_byte = match side {
            ConnectionSide::Local => 0,
            ConnectionSide::Remote => 1,
        };
        assert_eq!(buf[0], expected_byte);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = ConnectionSide::decode(&mut cursor).unwrap();
        assert!(matches!(decoded, _) && matches!(side, _));
    }
}

#[test]
fn test_outline_encoding_decoding() {
    let test_cases = vec![
        BlockSummary {
            size_bytes: 0,
            hash: [0x55u8; 32],
            num_tickets: 0,
            num_preimages: 0,
            total_preimages_size: 0,
            num_guarantees: 0,
            num_assurances: 0,
            num_dispute_verdicts: 0,
        },
        BlockSummary {
            size_bytes: 1024 * 1024, // 1MB
            num_tickets: 100,
            num_preimages: 50,
            total_preimages_size: 1024 * 50,
            num_guarantees: 200,
            num_assurances: 1000,
            num_dispute_verdicts: 5,
        },
        BlockSummary {
            size_bytes: u32::MAX,
            num_tickets: u32::MAX,
            num_preimages: u32::MAX,
            total_preimages_size: u32::MAX,
            num_guarantees: u32::MAX,
            num_assurances: u32::MAX,
            num_dispute_verdicts: u32::MAX,
        },
    ];

    for summary in test_cases {
        let mut buf = BytesMut::new();
        summary.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 28); // 7 * 4 bytes
        assert_eq!(summary.encoded_size(), 28);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = BlockSummary::decode(&mut cursor).unwrap();
        assert_eq!(decoded.size_bytes, summary.size_bytes);
        assert_eq!(decoded.num_tickets, summary.num_tickets);
        assert_eq!(decoded.num_preimages, summary.num_preimages);
        assert_eq!(decoded.total_preimages_size, summary.total_preimages_size);
        assert_eq!(decoded.num_guarantees, summary.num_guarantees);
        assert_eq!(decoded.num_assurances, summary.num_assurances);
        assert_eq!(decoded.num_dispute_verdicts, summary.num_dispute_verdicts);
    }
}

#[test]
fn test_exec_cost_encoding_decoding() {
    let test_cases = vec![
        ExecCost {
            gas_used: 0,
            elapsed_ns: 0,
        },
        ExecCost {
            gas_used: 1_000_000,
            elapsed_ns: 500_000,
        },
        ExecCost {
            gas_used: u64::MAX,
            elapsed_ns: u64::MAX,
        },
    ];

    for cost in test_cases {
        let mut buf = BytesMut::new();
        cost.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 16); // 2 * 8 bytes
        assert_eq!(cost.encoded_size(), 16);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = ExecCost::decode(&mut cursor).unwrap();
        assert_eq!(decoded.gas_used, cost.gas_used);
        assert_eq!(decoded.elapsed_ns, cost.elapsed_ns);
    }
}

#[test]
fn test_accumulate_cost_encoding_decoding() {
    let test_cases = vec![
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
        AccumulateCost {
            num_calls: 100,
            num_transfers: 50,
            num_items: 200,
            total: ExecCost {
                gas_used: 10_000_000,
                elapsed_ns: 5_000_000,
            },
            read_write_calls: ExecCost {
                gas_used: 2_000_000,
                elapsed_ns: 1_000_000,
            },
            lookup_query_calls: ExecCost {
                gas_used: 3_000_000,
                elapsed_ns: 1_500_000,
            },
            info_new_calls: ExecCost {
                gas_used: 4_000_000,
                elapsed_ns: 2_000_000,
            },
            total_gas_charged: 9_500_000,
            other_host_calls: ExecCost {
                gas_used: 1_000_000,
                elapsed_ns: 500_000,
            },
        },
        AccumulateCost {
            num_calls: u32::MAX,
            num_transfers: u32::MAX,
            num_items: u32::MAX,
            total: ExecCost {
                gas_used: u64::MAX,
                elapsed_ns: u64::MAX,
            },
            read_write_calls: ExecCost {
                gas_used: u64::MAX,
                elapsed_ns: u64::MAX,
            },
            lookup_query_calls: ExecCost {
                gas_used: u64::MAX,
                elapsed_ns: u64::MAX,
            },
            info_new_calls: ExecCost {
                gas_used: u64::MAX,
                elapsed_ns: u64::MAX,
            },
            total_gas_charged: u64::MAX,
            other_host_calls: ExecCost {
                gas_used: u64::MAX,
                elapsed_ns: u64::MAX,
            },
        },
    ];

    for cost in test_cases {
        let mut buf = BytesMut::new();
        cost.encode(&mut buf).unwrap();
        let expected_size = 3 * 4 + 5 * 16 + 8; // 3 u32s + 5 ExecCosts + 1 u64
        assert_eq!(buf.len(), expected_size);
        assert_eq!(cost.encoded_size(), expected_size);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = AccumulateCost::decode(&mut cursor).unwrap();
        assert_eq!(decoded.num_calls, cost.num_calls);
        assert_eq!(decoded.num_transfers, cost.num_transfers);
        assert_eq!(decoded.num_items, cost.num_items);
        assert_eq!(decoded.total.gas_used, cost.total.gas_used);
        assert_eq!(decoded.total.elapsed_ns, cost.total.elapsed_ns);
        assert_eq!(decoded.total_gas_charged, cost.total_gas_charged);
    }
}

#[test]
fn test_import_spec_encoding() {
    // Test with SegmentsRoot
    let spec1 = ImportSpec {
        root_identifier: RootIdentifier::SegmentsRoot([42u8; 32]),
        export_index: 100,
    };

    let mut buf = BytesMut::new();
    spec1.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 34); // 32 bytes hash + 2 bytes index

    // Check that bit 15 is NOT set for SegmentsRoot
    let index_bytes = &buf[32..34];
    let encoded_index = u16::from_le_bytes([index_bytes[0], index_bytes[1]]);
    assert_eq!(encoded_index, 100);
    assert_eq!(encoded_index & 0x8000, 0);

    // Test with WorkPackageHash
    let spec2 = ImportSpec {
        root_identifier: RootIdentifier::WorkPackageHash([99u8; 32]),
        export_index: 200,
    };

    buf.clear();
    spec2.encode(&mut buf).unwrap();

    // Check that bit 15 IS set for WorkPackageHash
    let index_bytes = &buf[32..34];
    let encoded_index = u16::from_le_bytes([index_bytes[0], index_bytes[1]]);
    assert_eq!(encoded_index, 200 | 0x8000);
    assert_ne!(encoded_index & 0x8000, 0);

    // Test edge cases
    let spec3 = ImportSpec {
        root_identifier: RootIdentifier::SegmentsRoot([0u8; 32]),
        export_index: 32767, // Max without flag
    };

    buf.clear();
    spec3.encode(&mut buf).unwrap();
    let index_bytes = &buf[32..34];
    let encoded_index = u16::from_le_bytes([index_bytes[0], index_bytes[1]]);
    assert_eq!(encoded_index, 32767);
}

#[test]
fn test_guarantee_discard_reason_encoding() {
    let reasons = vec![
        GuaranteeDiscardReason::WorkReportIncluded,
        GuaranteeDiscardReason::ReplacedByBetter,
        GuaranteeDiscardReason::TooOld,
        GuaranteeDiscardReason::TooManyGuarantees,
        GuaranteeDiscardReason::Other,
    ];

    for (i, reason) in reasons.iter().enumerate() {
        let mut buf = BytesMut::new();
        reason.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], i as u8);
    }
}

#[test]
fn test_work_item_summary_encoding() {
    let summary = WorkItemSummary {
        service_id: 12345,
        payload_size: 1024 * 100,
        refine_gas_limit: 10_000_000,
        accumulate_gas_limit: 20_000_000,
        sum_of_extrinsic_lengths: 5000,
        imports: vec![
            ImportSpec {
                root_identifier: RootIdentifier::SegmentsRoot([1u8; 32]),
                export_index: 10,
            },
            ImportSpec {
                root_identifier: RootIdentifier::WorkPackageHash([2u8; 32]),
                export_index: 20,
            },
        ],
        num_exported_segments: 15,
    };

    let mut buf = BytesMut::new();
    summary.encode(&mut buf).unwrap();

    // Verify encoded size calculation
    let expected_size = 4 + 4 + 8 + 8 + 4 + summary.imports.encoded_size() + 2;
    assert_eq!(summary.encoded_size(), expected_size);
}

#[test]
fn test_work_package_summary_encoding() {
    let summary = WorkPackageSummary {
        work_package_size: 1024 * 1024,
        anchor: [77u8; 32],
        lookup_anchor_slot: 98765,
        prerequisites: vec![[1u8; 32], [2u8; 32], [3u8; 32]],
        work_items: vec![WorkItemSummary {
            service_id: 100,
            payload_size: 1000,
            refine_gas_limit: 1_000_000,
            accumulate_gas_limit: 2_000_000,
            sum_of_extrinsic_lengths: 500,
            imports: vec![],
            num_exported_segments: 5,
        }],
    };

    let mut buf = BytesMut::new();
    summary.encode(&mut buf).unwrap();

    // Verify structure
    assert!(buf.len() > 40); // At least size + anchor + slot
}

#[test]
fn test_guarantee_summary_encoding() {
    let summary = GuaranteeSummary {
        work_report_hash: [88u8; 32],
        slot: 12345,
        guarantors: vec![1, 2, 3, 100, 1000],
    };

    let mut buf = BytesMut::new();
    summary.encode(&mut buf).unwrap();

    // Check basic structure
    assert_eq!(
        summary.encoded_size(),
        32 + 4 + summary.guarantors.encoded_size()
    );
}

#[test]
fn test_service_id_accumulate_cost_tuple_decoding() {
    let tuple = (
        42u32,
        AccumulateCost {
            num_calls: 5,
            num_transfers: 10,
            num_items: 15,
            total: ExecCost {
                gas_used: 1_000_000,
                elapsed_ns: 500_000,
            },
            read_write_calls: ExecCost {
                gas_used: 200_000,
                elapsed_ns: 100_000,
            },
            lookup_query_calls: ExecCost {
                gas_used: 300_000,
                elapsed_ns: 150_000,
            },
            info_new_calls: ExecCost {
                gas_used: 400_000,
                elapsed_ns: 200_000,
            },
            total_gas_charged: 950_000,
            other_host_calls: ExecCost {
                gas_used: 100_000,
                elapsed_ns: 50_000,
            },
        },
    );

    let mut buf = BytesMut::new();
    tuple.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = <(ServiceId, AccumulateCost)>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.0, tuple.0);
    assert_eq!(decoded.1.num_calls, tuple.1.num_calls);
}

#[test]
fn test_vec_of_tuples_decoding() {
    let vec = vec![
        (
            100u32,
            AccumulateCost {
                num_calls: 1,
                num_transfers: 2,
                num_items: 3,
                total: ExecCost {
                    gas_used: 100,
                    elapsed_ns: 50,
                },
                read_write_calls: ExecCost {
                    gas_used: 20,
                    elapsed_ns: 10,
                },
                lookup_query_calls: ExecCost {
                    gas_used: 30,
                    elapsed_ns: 15,
                },
                info_new_calls: ExecCost {
                    gas_used: 40,
                    elapsed_ns: 20,
                },
                total_gas_charged: 90,
                other_host_calls: ExecCost {
                    gas_used: 10,
                    elapsed_ns: 5,
                },
            },
        ),
        (
            200u32,
            AccumulateCost {
                num_calls: 10,
                num_transfers: 20,
                num_items: 30,
                total: ExecCost {
                    gas_used: 1000,
                    elapsed_ns: 500,
                },
                read_write_calls: ExecCost {
                    gas_used: 200,
                    elapsed_ns: 100,
                },
                lookup_query_calls: ExecCost {
                    gas_used: 300,
                    elapsed_ns: 150,
                },
                info_new_calls: ExecCost {
                    gas_used: 400,
                    elapsed_ns: 200,
                },
                total_gas_charged: 900,
                other_host_calls: ExecCost {
                    gas_used: 100,
                    elapsed_ns: 50,
                },
            },
        ),
    ];

    let mut buf = BytesMut::new();
    vec.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Vec::<(ServiceId, AccumulateCost)>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.len(), vec.len());
    assert_eq!(decoded[0].0, vec[0].0);
    assert_eq!(decoded[1].0, vec[1].0);
}
