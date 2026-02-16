use std::sync::Arc;
use std::time::Duration;
use tart_backend::events::NodeInformation;
use tart_backend::types::*;
use tart_backend::TelemetryServer;
use tokio::time::sleep;

/// Returns DATABASE_URL after verifying it points to a test database.
/// Panics if DATABASE_URL is unset or doesn't contain "test" in the name.
pub fn test_database_url() -> String {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    assert!(
        url.contains("test"),
        "Refusing to run tests against non-test database: {url}"
    );
    url
}

/// Flush all pending batch writes and wait for database visibility.
///
/// The 100ms pre-flush sleep covers the TCP propagation gap: the test writes
/// bytes to a TCP socket, but the server's async read loop must complete
/// `read_buf()` → `decode` → `batch_writer.write_event()` before events
/// enter the batch writer channel. The flush command has correct channel
/// ordering (events first, then flush sentinel), but only if events are
/// already in the channel.
///
/// Possible future improvements to eliminate this sleep:
/// 1. Track bytes_consumed per connection; wait until consumed >= bytes_sent
/// 2. Expose per-connection event_count via watch channel; wait until >= expected
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
