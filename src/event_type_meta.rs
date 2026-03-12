use serde::Serialize;
use std::collections::HashMap;
use std::sync::LazyLock;

#[derive(Debug, Clone, Serialize)]
pub struct EventTypeMeta {
    pub id: i16,
    pub name: &'static str,
    pub group: &'static str,
}

/// O(1) lookup from event type ID to human-readable name.
static NAME_TO_ID: LazyLock<HashMap<&'static str, i16>> = LazyLock::new(|| {
    event_type_metadata().iter().map(|m| (m.name, m.id)).collect()
});

static ID_TO_NAME: LazyLock<HashMap<i16, &'static str>> = LazyLock::new(|| {
    event_type_metadata().iter().map(|m| (m.id, m.name)).collect()
});

/// Returns the human-readable name for an event type ID, or "Unknown" if not found.
pub fn event_type_name(id: i16) -> &'static str {
    ID_TO_NAME.get(&id).copied().unwrap_or("Unknown")
}

/// All 99 event types with canonical names from JIP-3 / polkajam telemetry.rs.
pub fn event_type_metadata() -> &'static [EventTypeMeta] {
    static META: &[EventTypeMeta] = &[
        // system
        EventTypeMeta { id: 0, name: "Dropped", group: "system" },
        // status
        EventTypeMeta { id: 10, name: "Status", group: "status" },
        EventTypeMeta { id: 11, name: "BestBlockChanged", group: "status" },
        EventTypeMeta { id: 12, name: "FinalizedBlockChanged", group: "status" },
        EventTypeMeta { id: 13, name: "SyncStatusChanged", group: "status" },
        // connections
        EventTypeMeta { id: 20, name: "ConnectionRefused", group: "connections" },
        EventTypeMeta { id: 21, name: "ConnectingIn", group: "connections" },
        EventTypeMeta { id: 22, name: "ConnectInFailed", group: "connections" },
        EventTypeMeta { id: 23, name: "ConnectedIn", group: "connections" },
        EventTypeMeta { id: 24, name: "ConnectingOut", group: "connections" },
        EventTypeMeta { id: 25, name: "ConnectOutFailed", group: "connections" },
        EventTypeMeta { id: 26, name: "ConnectedOut", group: "connections" },
        EventTypeMeta { id: 27, name: "Disconnected", group: "connections" },
        EventTypeMeta { id: 28, name: "PeerMisbehaved", group: "connections" },
        // blocks
        EventTypeMeta { id: 40, name: "Authoring", group: "blocks" },
        EventTypeMeta { id: 41, name: "AuthoringFailed", group: "blocks" },
        EventTypeMeta { id: 42, name: "Authored", group: "blocks" },
        EventTypeMeta { id: 43, name: "Importing", group: "blocks" },
        EventTypeMeta { id: 44, name: "BlockVerificationFailed", group: "blocks" },
        EventTypeMeta { id: 45, name: "BlockVerified", group: "blocks" },
        EventTypeMeta { id: 46, name: "BlockExecutionFailed", group: "blocks" },
        EventTypeMeta { id: 47, name: "BlockExecuted", group: "blocks" },
        // block_distribution
        EventTypeMeta { id: 60, name: "BlockAnnouncementStreamOpened", group: "block_distribution" },
        EventTypeMeta { id: 61, name: "BlockAnnouncementStreamClosed", group: "block_distribution" },
        EventTypeMeta { id: 62, name: "BlockAnnounced", group: "block_distribution" },
        EventTypeMeta { id: 63, name: "SendingBlockRequest", group: "block_distribution" },
        EventTypeMeta { id: 64, name: "ReceivingBlockRequest", group: "block_distribution" },
        EventTypeMeta { id: 65, name: "BlockRequestFailed", group: "block_distribution" },
        EventTypeMeta { id: 66, name: "BlockRequestSent", group: "block_distribution" },
        EventTypeMeta { id: 67, name: "BlockRequestReceived", group: "block_distribution" },
        EventTypeMeta { id: 68, name: "BlockTransferred", group: "block_distribution" },
        // tickets
        EventTypeMeta { id: 80, name: "GeneratingTickets", group: "tickets" },
        EventTypeMeta { id: 81, name: "TicketGenerationFailed", group: "tickets" },
        EventTypeMeta { id: 82, name: "TicketsGenerated", group: "tickets" },
        EventTypeMeta { id: 83, name: "TicketTransferFailed", group: "tickets" },
        EventTypeMeta { id: 84, name: "TicketTransferred", group: "tickets" },
        // wp_pipeline
        EventTypeMeta { id: 90, name: "WorkPackageSubmission", group: "wp_pipeline" },
        EventTypeMeta { id: 91, name: "WorkPackageBeingShared", group: "wp_pipeline" },
        EventTypeMeta { id: 92, name: "WorkPackageFailed", group: "wp_pipeline" },
        EventTypeMeta { id: 93, name: "DuplicateWorkPackage", group: "wp_pipeline" },
        EventTypeMeta { id: 94, name: "WorkPackageReceived", group: "wp_pipeline" },
        EventTypeMeta { id: 95, name: "Authorized", group: "wp_pipeline" },
        EventTypeMeta { id: 96, name: "ExtrinsicDataReceived", group: "wp_pipeline" },
        EventTypeMeta { id: 97, name: "ImportsReceived", group: "wp_pipeline" },
        EventTypeMeta { id: 98, name: "SharingWorkPackage", group: "wp_pipeline" },
        EventTypeMeta { id: 99, name: "WorkPackageSharingFailed", group: "wp_pipeline" },
        EventTypeMeta { id: 100, name: "BundleSent", group: "wp_pipeline" },
        EventTypeMeta { id: 101, name: "Refined", group: "wp_pipeline" },
        EventTypeMeta { id: 102, name: "WorkReportBuilt", group: "wp_pipeline" },
        EventTypeMeta { id: 103, name: "WorkReportSignatureSent", group: "wp_pipeline" },
        EventTypeMeta { id: 104, name: "WorkReportSignatureReceived", group: "wp_pipeline" },
        EventTypeMeta { id: 105, name: "GuaranteeBuilt", group: "wp_pipeline" },
        EventTypeMeta { id: 106, name: "SendingGuarantee", group: "wp_pipeline" },
        EventTypeMeta { id: 107, name: "GuaranteeSendFailed", group: "wp_pipeline" },
        EventTypeMeta { id: 108, name: "GuaranteeSent", group: "wp_pipeline" },
        EventTypeMeta { id: 109, name: "GuaranteesDistributed", group: "wp_pipeline" },
        // guarantee_receiving
        EventTypeMeta { id: 110, name: "ReceivingGuarantee", group: "guarantee_receiving" },
        EventTypeMeta { id: 111, name: "GuaranteeReceiveFailed", group: "guarantee_receiving" },
        EventTypeMeta { id: 112, name: "GuaranteeReceived", group: "guarantee_receiving" },
        EventTypeMeta { id: 113, name: "GuaranteeDiscarded", group: "guarantee_receiving" },
        // shards
        EventTypeMeta { id: 120, name: "SendingShardRequest", group: "shards" },
        EventTypeMeta { id: 121, name: "ReceivingShardRequest", group: "shards" },
        EventTypeMeta { id: 122, name: "ShardRequestFailed", group: "shards" },
        EventTypeMeta { id: 123, name: "ShardRequestSent", group: "shards" },
        EventTypeMeta { id: 124, name: "ShardRequestReceived", group: "shards" },
        EventTypeMeta { id: 125, name: "ShardsTransferred", group: "shards" },
        // assurances
        EventTypeMeta { id: 126, name: "DistributingAssurance", group: "assurances" },
        EventTypeMeta { id: 127, name: "AssuranceSendFailed", group: "assurances" },
        EventTypeMeta { id: 128, name: "AssuranceSent", group: "assurances" },
        EventTypeMeta { id: 129, name: "AssuranceDistributed", group: "assurances" },
        EventTypeMeta { id: 130, name: "AssuranceReceiveFailed", group: "assurances" },
        EventTypeMeta { id: 131, name: "AssuranceReceived", group: "assurances" },
        // bundles
        EventTypeMeta { id: 140, name: "SendingBundleShardRequest", group: "bundles" },
        EventTypeMeta { id: 141, name: "ReceivingBundleShardRequest", group: "bundles" },
        EventTypeMeta { id: 142, name: "BundleShardRequestFailed", group: "bundles" },
        EventTypeMeta { id: 143, name: "BundleShardRequestSent", group: "bundles" },
        EventTypeMeta { id: 144, name: "BundleShardRequestReceived", group: "bundles" },
        EventTypeMeta { id: 145, name: "BundleShardTransferred", group: "bundles" },
        EventTypeMeta { id: 146, name: "ReconstructingBundle", group: "bundles" },
        EventTypeMeta { id: 147, name: "BundleReconstructed", group: "bundles" },
        EventTypeMeta { id: 148, name: "SendingBundleRequest", group: "bundles" },
        EventTypeMeta { id: 149, name: "ReceivingBundleRequest", group: "bundles" },
        EventTypeMeta { id: 150, name: "BundleRequestFailed", group: "bundles" },
        EventTypeMeta { id: 151, name: "BundleRequestSent", group: "bundles" },
        EventTypeMeta { id: 152, name: "BundleRequestReceived", group: "bundles" },
        EventTypeMeta { id: 153, name: "BundleTransferred", group: "bundles" },
        // segments
        EventTypeMeta { id: 160, name: "WorkPackageHashMapped", group: "segments" },
        EventTypeMeta { id: 161, name: "SegmentsRootMapped", group: "segments" },
        EventTypeMeta { id: 162, name: "SendingSegmentShardRequest", group: "segments" },
        EventTypeMeta { id: 163, name: "ReceivingSegmentShardRequest", group: "segments" },
        EventTypeMeta { id: 164, name: "SegmentShardRequestFailed", group: "segments" },
        EventTypeMeta { id: 165, name: "SegmentShardRequestSent", group: "segments" },
        EventTypeMeta { id: 166, name: "SegmentShardRequestReceived", group: "segments" },
        EventTypeMeta { id: 167, name: "SegmentShardsTransferred", group: "segments" },
        EventTypeMeta { id: 168, name: "ReconstructingSegments", group: "segments" },
        EventTypeMeta { id: 169, name: "SegmentReconstructionFailed", group: "segments" },
        EventTypeMeta { id: 170, name: "SegmentsReconstructed", group: "segments" },
        EventTypeMeta { id: 171, name: "SegmentVerificationFailed", group: "segments" },
        EventTypeMeta { id: 172, name: "SegmentsVerified", group: "segments" },
        EventTypeMeta { id: 173, name: "SendingSegmentRequest", group: "segments" },
        EventTypeMeta { id: 174, name: "ReceivingSegmentRequest", group: "segments" },
        EventTypeMeta { id: 175, name: "SegmentRequestFailed", group: "segments" },
        EventTypeMeta { id: 176, name: "SegmentRequestSent", group: "segments" },
        EventTypeMeta { id: 177, name: "SegmentRequestReceived", group: "segments" },
        EventTypeMeta { id: 178, name: "SegmentsTransferred", group: "segments" },
        // preimages
        EventTypeMeta { id: 190, name: "PreimageAnnouncementFailed", group: "preimages" },
        EventTypeMeta { id: 191, name: "PreimageAnnounced", group: "preimages" },
        EventTypeMeta { id: 192, name: "AnnouncedPreimageForgotten", group: "preimages" },
        EventTypeMeta { id: 193, name: "SendingPreimageRequest", group: "preimages" },
        EventTypeMeta { id: 194, name: "ReceivingPreimageRequest", group: "preimages" },
        EventTypeMeta { id: 195, name: "PreimageRequestFailed", group: "preimages" },
        EventTypeMeta { id: 196, name: "PreimageRequestSent", group: "preimages" },
        EventTypeMeta { id: 197, name: "PreimageRequestReceived", group: "preimages" },
        EventTypeMeta { id: 198, name: "PreimageTransferred", group: "preimages" },
        EventTypeMeta { id: 199, name: "PreimageDiscarded", group: "preimages" },
    ];
    META
}

/// Predefined event type groups. Returns None for unknown group names.
pub fn event_type_group(name: &str) -> Option<&'static [i16]> {
    match name {
        "system" => Some(&[0]),
        "status" => Some(&[10, 11, 12, 13]),
        "connections" => Some(&[20, 21, 22, 23, 24, 25, 26, 27, 28]),
        "blocks" => Some(&[40, 41, 42, 43, 44, 45, 46, 47]),
        "block_distribution" => Some(&[60, 61, 62, 63, 64, 65, 66, 67, 68]),
        "tickets" => Some(&[80, 81, 82, 83, 84]),
        "wp_pipeline" => Some(&[90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]),
        "guarantee_receiving" => Some(&[110, 111, 112, 113]),
        "shards" => Some(&[120, 121, 122, 123, 124, 125]),
        "assurances" => Some(&[126, 127, 128, 129, 130, 131]),
        "bundles" => Some(&[140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153]),
        "segments" => Some(&[160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178]),
        "preimages" => Some(&[190, 191, 192, 193, 194, 195, 196, 197, 198, 199]),
        // Virtual group: all failure/error/discard events across all categories
        "failures" => Some(&[
            20, 22, 25, 41, 44, 46, 65, 81, 83, 92, 93, 99,
            107, 111, 113, 122, 127, 130, 142, 150, 164, 169, 171, 175,
            190, 195, 199,
        ]),
        _ => None,
    }
}

/// Expand an event_types query parameter string into a deduplicated list of IDs.
/// Accepts comma-separated mix of numeric IDs, group names, and event names.
/// Grafana multi-select may wrap values in curly braces: "{a,b}" — these are stripped.
/// Example: "failures,42" → [20, 22, 25, ..., 42, ..., 199]
pub fn expand_event_types(input: &str) -> Vec<i16> {
    let input = input.strip_prefix('{').and_then(|s| s.strip_suffix('}')).unwrap_or(input);
    let mut result = Vec::new();
    for token in input.split(',') {
        let token = token.trim();
        if let Ok(id) = token.parse::<i16>() {
            result.push(id);
        } else if let Some(ids) = event_type_group(token) {
            result.extend_from_slice(ids);
        } else if let Some(&id) = NAME_TO_ID.get(token) {
            result.push(id);
        }
    }
    result.sort();
    result.dedup();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_count() {
        assert_eq!(event_type_metadata().len(), 115);
    }

    #[test]
    fn metadata_ids_unique() {
        let ids: Vec<i16> = event_type_metadata().iter().map(|m| m.id).collect();
        let mut deduped = ids.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(ids.len(), deduped.len(), "duplicate event type IDs found");
    }

    #[test]
    fn group_failures_contains_expected() {
        let failures = event_type_group("failures").unwrap();
        assert!(failures.contains(&92), "WorkPackageFailed");
        assert!(failures.contains(&41), "AuthoringFailed");
        assert!(failures.contains(&199), "PreimageDiscarded");
        assert!(!failures.contains(&42), "Authored should not be a failure");
    }

    #[test]
    fn expand_mixed_ids_and_groups() {
        let result = expand_event_types("42,failures,10");
        assert!(result.contains(&42));
        assert!(result.contains(&92)); // from failures group
        assert!(result.contains(&10));
        // verify deduplication
        let mut sorted = result.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(result.len(), sorted.len());
    }

    #[test]
    fn expand_event_names() {
        let result = expand_event_types("Authored,BlockExecuted");
        assert_eq!(result, vec![42, 47]);
    }

    #[test]
    fn expand_mixed_ids_groups_and_names() {
        let result = expand_event_types("10,blocks,WorkPackageFailed");
        assert!(result.contains(&10));  // numeric ID
        assert!(result.contains(&42));  // from blocks group
        assert!(result.contains(&92));  // event name
    }

    #[test]
    fn expand_unknown_group_ignored() {
        let result = expand_event_types("42,nonexistent,10");
        assert_eq!(result, vec![10, 42]);
    }

    #[test]
    fn expand_grafana_curly_braces() {
        let result = expand_event_types("{tickets,connections}");
        assert!(result.contains(&80)); // GeneratingTickets (tickets group)
        assert!(result.contains(&21)); // ConnectingIn (connections group)
        assert!(!result.contains(&42)); // Authored should not be present
    }

    #[test]
    fn expand_curly_braces_unknown_ignored() {
        let result = expand_event_types("{nonexistent}");
        assert!(result.is_empty());
    }

    #[test]
    fn group_unknown_returns_none() {
        assert!(event_type_group("foobar").is_none());
    }

    #[test]
    fn event_type_name_known() {
        assert_eq!(event_type_name(42), "Authored");
        assert_eq!(event_type_name(92), "WorkPackageFailed");
        assert_eq!(event_type_name(0), "Dropped");
    }

    #[test]
    fn event_type_name_unknown() {
        assert_eq!(event_type_name(9999), "Unknown");
    }
}
