use std::sync::Arc;
use std::time::Duration;
use tart_backend::events::{Event, NodeInformation};
use tart_backend::types::*;
// GuaranteeDiscardReason, GuaranteeSummary, ConnectionSide, BoundedString — all from types::*
use tart_backend::TelemetryServer;
use tokio::time::sleep;

/// Core count used in test_protocol_params(). Status events must have
/// num_guarantees with exactly this many elements to match the decoder.
#[allow(dead_code)]
pub const TEST_CORE_COUNT: usize = 16;

/// Returns a "now" timestamp in JCE-relative microseconds.
/// Use this in test events so they fall within time-bounded query windows.
#[allow(dead_code)]
pub fn now_jce_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    const JCE_EPOCH_UNIX_MICROS: u64 = 1_735_732_800_000_000;
    let unix_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    unix_micros.saturating_sub(JCE_EPOCH_UNIX_MICROS)
}

/// Returns TEST_DATABASE_URL after verifying it points to a test database.
/// Panics if TEST_DATABASE_URL is unset or doesn't contain "test" in the name.
#[allow(dead_code)]
pub fn test_database_url() -> String {
    let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    assert!(
        url.contains("test"),
        "Refusing to run tests against non-test database: {url}"
    );
    url
}

/// Flush all pending batch writes and wait for database visibility.
#[allow(dead_code)]
pub async fn flush_and_wait(server: &Arc<TelemetryServer>) {
    sleep(Duration::from_millis(100)).await;
    server.flush_writes().await.expect("Flush failed");
}

/// Creates a test BlockSummary with reasonable default values
#[allow(dead_code)]
pub fn test_block_summary() -> BlockSummary {
    BlockSummary {
        size_bytes: 1024,
        hash: [0x11; 32],
        num_tickets: 1,
        num_preimages: 0,
        total_preimages_size: 0,
        num_guarantees: 1,
        num_assurances: 1,
        num_dispute_verdicts: 0,
    }
}

/// Creates a test AccumulateCost with reasonable default values
#[allow(dead_code)]
pub fn test_accumulate_cost() -> AccumulateCost {
    AccumulateCost {
        num_calls: 10,
        num_transfers: 5,
        num_items: 3,
        total: test_exec_cost(),
        load_ns: 1000,
        host_call: test_accumulate_host_call_cost(),
    }
}

/// Creates a test AccumulateHostCallCost with reasonable default values
#[allow(dead_code)]
pub fn test_accumulate_host_call_cost() -> AccumulateHostCallCost {
    AccumulateHostCallCost {
        state: test_exec_cost(),
        lookup: test_exec_cost(),
        preimage: test_exec_cost(),
        service: test_exec_cost(),
        transfer: test_exec_cost(),
        transfer_dest_gas: 1000,
        other: test_exec_cost(),
    }
}

/// Creates a test ExecCost with reasonable default values
#[allow(dead_code)]
pub fn test_exec_cost() -> ExecCost {
    ExecCost {
        gas_used: 500,
        elapsed_ns: 1000,
    }
}

/// Creates a test ProtocolParameters with reasonable default values for testing
#[allow(dead_code)]
pub fn test_protocol_params() -> ProtocolParameters {
    ProtocolParameters {
        deposit_per_item: 1000,
        deposit_per_byte: 10,
        deposit_per_account: 10000,
        core_count: 16,
        min_turnaround_period: 10,
        epoch_period: 600,
        max_accumulate_gas: 1000000,
        max_is_authorized_gas: 100000,
        max_refine_gas: 100000,
        block_gas_limit: 10000000,
        recent_block_count: 128,
        max_work_items: 100,
        max_dependencies: 100,
        max_tickets_per_block: 10,
        max_lookup_anchor_age: 100,
        tickets_attempts_number: 10,
        auth_window: 10,
        slot_period_sec: 6,
        auth_queue_len: 100,
        rotation_period: 24,
        max_extrinsics: 1000,
        availability_timeout: 10,
        val_count: 1023,
        max_authorizer_code_size: 1024 * 1024,
        max_input: 1024 * 1024,
        max_service_code_size: 10 * 1024 * 1024,
        basic_piece_len: 4096,
        max_imports: 1024,
        segment_piece_count: 16,
        max_report_elective_data: 1024,
        transfer_memo_size: 128,
        max_exports: 256,
        epoch_tail_start: 500,
    }
}

/// Creates a test NodeInformation with the given peer ID
#[allow(dead_code)]
pub fn test_node_info(peer_id: [u8; 32]) -> NodeInformation {
    NodeInformation {
        params: test_protocol_params(),
        genesis: [0u8; 32],
        details: PeerDetails {
            peer_id,
            peer_address: PeerAddress {
                ipv6: [0; 16],
                port: 30333,
            },
        },
        flags: 1,
        implementation_name: BoundedString::new("test-node").unwrap(),
        implementation_version: BoundedString::new("1.0.0").unwrap(),
        gp_version: BoundedString::new("0.1.0").unwrap(),
        additional_info: BoundedString::new("Test node").unwrap(),
    }
}

/// Force-refresh all continuous aggregates used by grafana endpoints.
/// TimescaleDB continuous aggregates don't auto-refresh fast enough in tests.
#[allow(dead_code)]
pub async fn refresh_aggregates(pool: &sqlx::PgPool) {
    // The refresh window covers a generous range so test data is always included
    let aggregates = [
        "event_stats_30s",
        "event_stats_1m",
        "event_stats_1h",
        "core_stats_1m",
        "service_stats_1m",
        "node_stats_1m",
        // Count table aggregates (pre-aggregated events)
        "block_distribution_counts_1m",
        "ticket_counts_1m",
        "guarantee_sending_counts_1m",
        "guarantee_receiving_counts_1m",
        "shard_counts_1m",
        "assurance_counts_1m",
        "bundle_counts_1m",
        "segment_counts_1m",
        "preimage_counts_1m",
    ];
    for agg in aggregates {
        let sql = format!(
            "CALL refresh_continuous_aggregate('{agg}', NOW() - INTERVAL '1 hour', NOW() + INTERVAL '1 hour')"
        );
        // Some aggregates may not exist in all test schemas — ignore errors
        let _ = sqlx::query(&sql).execute(pool).await;
    }
}

/// Flush batch_writer + trackers. Combined helper for grafana tests.
#[allow(dead_code)]
pub async fn flush_all(server: &Arc<TelemetryServer>) {
    sleep(Duration::from_millis(100)).await;
    server.flush_writes().await.expect("Flush writes failed");
    server.flush_trackers_for_test().await;
}

/// Construct a Status event (event_type=10) with the given timestamp.
/// num_guarantees has TEST_CORE_COUNT elements (required by the decoder).
#[allow(dead_code)]
pub fn status_event(ts: u64) -> Event {
    Event::Status {
        timestamp: ts,
        num_peers: 25,
        num_val_peers: 20,
        num_sync_peers: 5,
        num_guarantees: vec![3; TEST_CORE_COUNT],
        num_shards: 100,
        shards_size: 50000,
        num_preimages: 10,
        preimages_size: 4096,
    }
}

/// Construct a BestBlockChanged event (event_type=11).
#[allow(dead_code)]
pub fn best_block_event(ts: u64, slot: u32) -> Event {
    Event::BestBlockChanged {
        timestamp: ts,
        slot,
        hash: [0xBB; 32],
    }
}

/// Construct a FinalizedBlockChanged event (event_type=12).
#[allow(dead_code)]
pub fn finalized_block_event(ts: u64, slot: u32) -> Event {
    Event::FinalizedBlockChanged {
        timestamp: ts,
        slot,
        hash: [0xFF; 32],
    }
}

/// Construct an Authoring event (event_type=40) with the given slot.
#[allow(dead_code)]
pub fn authoring_event(ts: u64, slot: u32) -> Event {
    Event::Authoring {
        timestamp: ts,
        slot,
        parent: [0u8; 32],
    }
}

/// Construct an Authored event (event_type=42) with BlockOutline.
#[allow(dead_code)]
pub fn authored_event(ts: u64, authoring_id: u64) -> Event {
    use tart_backend::types::BlockSummary;
    Event::Authored {
        timestamp: ts,
        authoring_id,
        outline: BlockSummary {
            size_bytes: 2048,
            hash: [0xAA; 32],
            num_tickets: 2,
            num_preimages: 1,
            total_preimages_size: 512,
            num_guarantees: 3,
            num_assurances: 2,
            num_dispute_verdicts: 0,
        },
    }
}

/// Construct an Importing event (event_type=43) with the given slot and block hash.
#[allow(dead_code)]
pub fn importing_event(ts: u64, slot: u32, block_hash: [u8; 32]) -> Event {
    use tart_backend::types::BlockSummary;
    Event::Importing {
        timestamp: ts,
        slot,
        outline: BlockSummary {
            size_bytes: 2048,
            hash: block_hash,
            num_tickets: 2,
            num_preimages: 1,
            total_preimages_size: 512,
            num_guarantees: 3,
            num_assurances: 2,
            num_dispute_verdicts: 0,
        },
    }
}

/// Construct a WorkPackageReceived event (event_type=94).
#[allow(dead_code)]
pub fn wp_received_event(ts: u64, submission_id: u64, core: u16) -> Event {
    use tart_backend::types::*;
    Event::WorkPackageReceived {
        timestamp: ts,
        submission_or_share_id: submission_id,
        core,
        outline: WorkPackageSummary {
            work_package_size: 2048,
            work_package_hash: {
                let mut h = [0xCC; 32];
                h[..8].copy_from_slice(&submission_id.to_le_bytes());
                h
            },
            anchor: [0xAA; 32],
            lookup_anchor_slot: 100,
            prerequisites: vec![],
            work_items: vec![
                WorkItemSummary {
                    service_id: 10,
                    payload_size: 512,
                    refine_gas_limit: 1_000_000,
                    accumulate_gas_limit: 500_000,
                    sum_of_extrinsic_lengths: 128,
                    imports: vec![],
                    num_exported_segments: 2,
                },
                WorkItemSummary {
                    service_id: 20,
                    payload_size: 256,
                    refine_gas_limit: 2_000_000,
                    accumulate_gas_limit: 300_000,
                    sum_of_extrinsic_lengths: 64,
                    imports: vec![],
                    num_exported_segments: 1,
                },
            ],
        },
    }
}

/// Construct an Authorized event (event_type=95).
#[allow(dead_code)]
pub fn authorized_event(ts: u64, submission_id: u64) -> Event {
    use tart_backend::types::*;
    Event::Authorized {
        timestamp: ts,
        submission_or_share_id: submission_id,
        cost: IsAuthorizedCost {
            total: ExecCost {
                gas_used: 100_000,
                elapsed_ns: 200_000,
            },
            load_ns: 50_000,
            host_call: ExecCost {
                gas_used: 30_000,
                elapsed_ns: 60_000,
            },
        },
    }
}

/// Construct a Refined event (event_type=101).
#[allow(dead_code)]
pub fn refined_event(ts: u64, submission_id: u64) -> Event {
    use tart_backend::types::*;
    Event::Refined {
        timestamp: ts,
        submission_or_share_id: submission_id,
        costs: vec![RefineCost {
            total: ExecCost {
                gas_used: 500_000,
                elapsed_ns: 1_000_000,
            },
            load_ns: 100_000,
            host_call: RefineHostCallCost {
                lookup: ExecCost {
                    gas_used: 50_000,
                    elapsed_ns: 100_000,
                },
                vm: ExecCost {
                    gas_used: 200_000,
                    elapsed_ns: 400_000,
                },
                mem: ExecCost {
                    gas_used: 30_000,
                    elapsed_ns: 60_000,
                },
                invoke: ExecCost {
                    gas_used: 100_000,
                    elapsed_ns: 200_000,
                },
                other: ExecCost {
                    gas_used: 20_000,
                    elapsed_ns: 40_000,
                },
            },
        }],
    }
}

/// Construct a WorkReportBuilt event (event_type=102).
#[allow(dead_code)]
pub fn work_report_built_event(ts: u64, submission_id: u64) -> Event {
    use tart_backend::types::*;
    Event::WorkReportBuilt {
        timestamp: ts,
        submission_or_share_id: submission_id,
        outline: WorkReportSummary {
            work_report_hash: [0xDD; 32],
            bundle_size: 4096,
            erasure_root: [0xEE; 32],
            segments_root: [0x11; 32],
        },
    }
}

/// Construct a GuaranteeBuilt event (event_type=105).
#[allow(dead_code)]
pub fn guarantee_built_event(ts: u64, submission_id: u64) -> Event {
    use tart_backend::types::*;
    Event::GuaranteeBuilt {
        timestamp: ts,
        submission_id,
        outline: GuaranteeSummary {
            work_report_hash: [0xBB; 32],
            slot: 200,
            guarantors: vec![0, 1, 2],
        },
    }
}

/// Construct a GuaranteesDistributed event (event_type=109).
#[allow(dead_code)]
pub fn guarantees_distributed_event(ts: u64, submission_id: u64) -> Event {
    Event::GuaranteesDistributed {
        timestamp: ts,
        submission_id,
    }
}

/// Construct a WorkPackageFailed event (event_type=92).
#[allow(dead_code)]
pub fn wp_failed_event(ts: u64, submission_id: u64) -> Event {
    Event::WorkPackageFailed {
        timestamp: ts,
        submission_or_share_id: submission_id,
        reason: BoundedString::new("test failure").unwrap(),
    }
}

/// Construct a WorkPackageFailed event (event_type=92) with a custom reason.
#[allow(dead_code)]
pub fn wp_failed_event_with_reason(ts: u64, submission_id: u64, reason: &str) -> Event {
    Event::WorkPackageFailed {
        timestamp: ts,
        submission_or_share_id: submission_id,
        reason: BoundedString::new(reason).unwrap(),
    }
}

/// Returns the hex-encoded node_id for a test node created with `connect_test_node(port, id, server)`.
#[allow(dead_code)]
pub fn node_id_hex(node_id: u8) -> String {
    hex::encode([node_id; 32])
}

// ─────────────────────────────────────────────────────────────────────────────
// Pre-aggregated event constructors (for storage optimization tests)
// ─────────────────────────────────────────────────────────────────────────────

/// Construct a BlockAnnounced event (event_type=62).
#[allow(dead_code)]
pub fn block_announced_event(ts: u64, slot: u32) -> Event {
    Event::BlockAnnounced {
        timestamp: ts,
        peer: [0x01; 32],
        announcer: ConnectionSide::Remote,
        slot,
        hash: [0xAA; 32],
    }
}

/// Construct an AssuranceSent event (event_type=128).
#[allow(dead_code)]
pub fn assurance_sent_event(ts: u64) -> Event {
    Event::AssuranceSent {
        timestamp: ts,
        distributing_id: 1,
        recipient: [0x02; 32],
    }
}

/// Construct an AssuranceReceived event (event_type=131).
#[allow(dead_code)]
pub fn assurance_received_event(ts: u64) -> Event {
    Event::AssuranceReceived {
        timestamp: ts,
        sender: [0x03; 32],
        anchor: [0xBB; 32],
    }
}

/// Construct an AssuranceReceived event with custom anchor and sender (event_type=131).
#[allow(dead_code)]
pub fn assurance_received_event_with(ts: u64, anchor: [u8; 32], sender: [u8; 32]) -> Event {
    Event::AssuranceReceived {
        timestamp: ts,
        sender,
        anchor,
    }
}

/// Construct a DistributingAssurance event (event_type=126).
#[allow(dead_code)]
pub fn distributing_assurance_event(ts: u64, anchor: [u8; 32]) -> Event {
    use tart_backend::types::AvailabilityStatement;
    Event::DistributingAssurance {
        timestamp: ts,
        statement: AvailabilityStatement {
            anchor,
            bitfield: vec![0xFF; 43], // ~343 cores / 8 = 43 bytes
        },
    }
}

/// Construct an AssuranceSendFailed event (event_type=127).
#[allow(dead_code)]
pub fn assurance_send_failed_event(ts: u64, reason: &str) -> Event {
    Event::AssuranceSendFailed {
        timestamp: ts,
        distributing_id: 1,
        recipient: [0x04; 32],
        reason: BoundedString::new(reason).unwrap(),
    }
}

/// Construct a GuaranteeReceived event (event_type=112).
#[allow(dead_code)]
pub fn guarantee_received_event(ts: u64, slot: u32, report_hash: [u8; 32]) -> Event {
    Event::GuaranteeReceived {
        timestamp: ts,
        receiving_id: 1,
        outline: GuaranteeSummary {
            work_report_hash: report_hash,
            slot,
            guarantors: vec![0, 1, 2],
        },
    }
}

/// Construct a GuaranteeDiscarded event (event_type=113).
#[allow(dead_code)]
pub fn guarantee_discarded_event(
    ts: u64,
    slot: u32,
    report_hash: [u8; 32],
    reason: GuaranteeDiscardReason,
) -> Event {
    Event::GuaranteeDiscarded {
        timestamp: ts,
        outline: GuaranteeSummary {
            work_report_hash: report_hash,
            slot,
            guarantors: vec![0, 1, 2],
        },
        reason,
    }
}

/// Construct a SendingGuarantee event (event_type=106).
#[allow(dead_code)]
pub fn sending_guarantee_event(ts: u64, built_id: u64) -> Event {
    Event::SendingGuarantee {
        timestamp: ts,
        built_id,
        recipient: [0x05; 32],
    }
}

/// Construct a GuaranteeSent event (event_type=108).
#[allow(dead_code)]
pub fn guarantee_sent_event(ts: u64, sending_id: u64) -> Event {
    Event::GuaranteeSent {
        timestamp: ts,
        sending_id,
    }
}

/// Construct a TicketTransferred event (event_type=84).
#[allow(dead_code)]
pub fn ticket_transferred_event(ts: u64, from_proxy: bool, epoch: u32) -> Event {
    Event::TicketTransferred {
        timestamp: ts,
        peer: [0x06; 32],
        sender: ConnectionSide::Local,
        from_proxy,
        epoch,
        attempt: 0,
        id: [0x77; 32],
    }
}

/// Construct a ShardRequestFailed event (event_type=122).
#[allow(dead_code)]
pub fn shard_request_failed_event(ts: u64, reason: &str) -> Event {
    Event::ShardRequestFailed {
        timestamp: ts,
        request_id: 1,
        reason: BoundedString::new(reason).unwrap(),
    }
}

/// Construct a ShardRequestFailed event (event_type=122) with custom request_id.
#[allow(dead_code)]
pub fn shard_request_failed_event_with_id(ts: u64, request_id: u64, reason: &str) -> Event {
    Event::ShardRequestFailed {
        timestamp: ts,
        request_id,
        reason: BoundedString::new(reason).unwrap(),
    }
}

/// Construct a GuaranteeBuilt event (event_type=105) with custom report_hash and slot.
#[allow(dead_code)]
pub fn guarantee_built_event_with_hash(
    ts: u64,
    submission_id: u64,
    report_hash: [u8; 32],
    slot: u32,
) -> Event {
    use tart_backend::types::*;
    Event::GuaranteeBuilt {
        timestamp: ts,
        submission_id,
        outline: GuaranteeSummary {
            work_report_hash: report_hash,
            slot,
            guarantors: vec![0, 1, 2],
        },
    }
}

/// Construct a PreimageAnnounced event (event_type=191).
#[allow(dead_code)]
pub fn preimage_announced_event(ts: u64, service: u32) -> Event {
    Event::PreimageAnnounced {
        timestamp: ts,
        peer: [0x07; 32],
        announcer: ConnectionSide::Remote,
        service,
        hash: [0xCC; 32],
        length: 1024,
    }
}

/// Construct a BlockExecuted event (event_type=47) with service gas data.
#[allow(dead_code)]
pub fn block_executed_event(ts: u64, authoring_id: u64, services: &[(u32, u64)]) -> Event {
    use tart_backend::types::*;
    let accumulate_costs: Vec<(ServiceId, AccumulateCost)> = services
        .iter()
        .map(|(sid, gas)| {
            (
                *sid,
                AccumulateCost {
                    num_calls: 1,
                    num_transfers: 0,
                    num_items: 1,
                    total: ExecCost {
                        gas_used: *gas,
                        elapsed_ns: gas * 2,
                    },
                    load_ns: 1000,
                    host_call: AccumulateHostCallCost {
                        state: ExecCost {
                            gas_used: 0,
                            elapsed_ns: 0,
                        },
                        lookup: ExecCost {
                            gas_used: 0,
                            elapsed_ns: 0,
                        },
                        preimage: ExecCost {
                            gas_used: 0,
                            elapsed_ns: 0,
                        },
                        service: ExecCost {
                            gas_used: 0,
                            elapsed_ns: 0,
                        },
                        transfer: ExecCost {
                            gas_used: 0,
                            elapsed_ns: 0,
                        },
                        transfer_dest_gas: 0,
                        other: ExecCost {
                            gas_used: 0,
                            elapsed_ns: 0,
                        },
                    },
                },
            )
        })
        .collect();
    Event::BlockExecuted {
        timestamp: ts,
        authoring_or_importing_id: authoring_id,
        accumulate_costs,
    }
}

/// Construct a SendingShardRequest event (event_type=120).
#[allow(dead_code)]
pub fn sending_shard_request_event(ts: u64, guarantor: [u8; 32], erasure_root: [u8; 32], shard: u16) -> Event {
    Event::SendingShardRequest {
        timestamp: ts,
        guarantor,
        erasure_root,
        shard,
    }
}

/// Construct a ReceivingShardRequest event (event_type=121).
#[allow(dead_code)]
pub fn receiving_shard_request_event(ts: u64, assurer: [u8; 32]) -> Event {
    Event::ReceivingShardRequest {
        timestamp: ts,
        assurer,
    }
}

/// Construct a ShardRequestReceived event (event_type=124).
#[allow(dead_code)]
pub fn shard_request_received_event(ts: u64, request_id: u64, erasure_root: [u8; 32], shard: u16) -> Event {
    Event::ShardRequestReceived {
        timestamp: ts,
        request_id,
        erasure_root,
        shard,
    }
}

/// Construct a ShardsTransferred event (event_type=125).
#[allow(dead_code)]
pub fn shards_transferred_event(ts: u64, request_id: u64) -> Event {
    Event::ShardsTransferred {
        timestamp: ts,
        request_id,
    }
}
