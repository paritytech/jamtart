use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::batch_writer::NodeId;
use crate::events::Event;

/// Maximum entries per map per node (hard cap to prevent unbounded growth).
const MAX_MAP_ENTRIES: usize = 10_000;

/// TTL for stale entries.
const STALE_TTL: Duration = Duration::from_secs(60);

/// How often (in calls to `process`) to run eviction of stale map entries.
const EVICTION_INTERVAL: u64 = 1000;

/// Enriched fields derived from cross-event correlation.
#[derive(Debug, Clone, Default)]
pub struct EnrichedFields {
    pub slot: Option<u32>,
    pub core: Option<u16>,
    pub submission_id: Option<u64>,
    pub service_ids: Option<Vec<u32>>,
    pub wp_hash: Option<[u8; 32]>,
}

pub type EnricherMap = Arc<DashMap<NodeId, NodeEventEnricher>>;

pub fn new_enricher_map() -> EnricherMap {
    Arc::new(DashMap::new())
}

struct SubmissionContext {
    core: u16,
    work_package_hash: Option<[u8; 32]>,
    service_ids: Vec<u32>,
    inserted_at: Instant,
}

struct ChainEntry {
    core: u16,
    service_ids: Option<Vec<u32>>,
    inserted_at: Instant,
}

pub struct NodeEventEnricher {
    /// submission_or_share_id / submission_id -> context
    submissions: HashMap<u64, SubmissionContext>,
    /// built_id -> context (GuaranteeBuilt event_id)
    built_ids: HashMap<u64, ChainEntry>,
    /// sending_id -> context (SendingGuarantee event_id)
    sending_ids: HashMap<u64, ChainEntry>,
    /// request_id -> context (SendingSegmentShardRequest / SendingSegmentRequest event_id)
    request_ids: HashMap<u64, ChainEntry>,
    /// reconstructing_id -> context (ReconstructingSegments event_id)
    reconstructing_ids: HashMap<u64, ChainEntry>,
    last_activity: Instant,
    call_count: u64,
}

impl Default for NodeEventEnricher {
    fn default() -> Self {
        Self {
            submissions: HashMap::new(),
            built_ids: HashMap::new(),
            sending_ids: HashMap::new(),
            request_ids: HashMap::new(),
            reconstructing_ids: HashMap::new(),
            last_activity: Instant::now(),
            call_count: 0,
        }
    }
}

/// Clear a map if it has reached the hard cap.
fn cap_map<V>(map: &mut HashMap<u64, V>, limit: usize) {
    if map.len() >= limit {
        map.clear();
    }
}

impl NodeEventEnricher {
    /// Returns `true` if this enricher has not been used for longer than [`STALE_TTL`].
    pub fn is_stale(&self) -> bool {
        self.last_activity.elapsed() > STALE_TTL
    }

    /// Process an event and return any enriched fields that could be derived.
    pub fn process(&mut self, event: &Event, event_id: u64) -> EnrichedFields {
        self.last_activity = Instant::now();
        self.call_count += 1;

        if self.call_count % EVICTION_INTERVAL == 0 {
            self.evict_stale();
        }

        let mut fields = EnrichedFields::default();

        // --- 1. Extract slot directly from events that carry it ---
        match event {
            Event::BestBlockChanged { slot, .. }
            | Event::FinalizedBlockChanged { slot, .. }
            | Event::Authoring { slot, .. }
            | Event::Importing { slot, .. }
            | Event::BlockAnnounced { slot, .. }
            | Event::BlockTransferred { slot, .. } => {
                fields.slot = Some(*slot);
            }
            _ => {}
        }

        // --- 2. Extract core directly from events that carry it ---
        match event {
            Event::WorkPackageReceived { core, .. }
            | Event::DuplicateWorkPackage { core, .. } => {
                fields.core = Some(*core);
            }
            _ => {}
        }

        // --- 3. Extract submission-family id ---
        let submission_key: Option<u64> = match event {
            // Events with submission_or_share_id
            Event::WorkPackageFailed {
                submission_or_share_id,
                ..
            }
            | Event::DuplicateWorkPackage {
                submission_or_share_id,
                ..
            }
            | Event::WorkPackageReceived {
                submission_or_share_id,
                ..
            }
            | Event::Authorized {
                submission_or_share_id,
                ..
            }
            | Event::ExtrinsicDataReceived {
                submission_or_share_id,
                ..
            }
            | Event::ImportsReceived {
                submission_or_share_id,
                ..
            }
            | Event::Refined {
                submission_or_share_id,
                ..
            }
            | Event::WorkReportBuilt {
                submission_or_share_id,
                ..
            } => Some(*submission_or_share_id),

            // Events with submission_id
            Event::SharingWorkPackage { submission_id, .. }
            | Event::WorkPackageSharingFailed { submission_id, .. }
            | Event::BundleSent { submission_id, .. }
            | Event::WorkReportSignatureReceived { submission_id, .. }
            | Event::GuaranteeBuilt { submission_id, .. }
            | Event::GuaranteesDistributed { submission_id, .. }
            | Event::WorkPackageHashMapped { submission_id, .. }
            | Event::SegmentsRootMapped { submission_id, .. }
            | Event::SendingSegmentShardRequest { submission_id, .. }
            | Event::ReconstructingSegments { submission_id, .. }
            | Event::SegmentVerificationFailed { submission_id, .. }
            | Event::SegmentsVerified { submission_id, .. }
            | Event::SendingSegmentRequest { submission_id, .. } => Some(*submission_id),

            // Events with share_id
            Event::WorkReportSignatureSent { share_id, .. } => Some(*share_id),

            _ => None,
        };

        if let Some(sid) = submission_key {
            fields.submission_id = Some(sid);
        }

        // --- 4. WorkPackageReceived: store context in submissions map ---
        if let Event::WorkPackageReceived {
            core,
            submission_or_share_id,
            outline,
            ..
        } = event
        {
            let service_ids: Vec<u32> = outline
                .work_items
                .iter()
                .map(|wi| wi.service_id)
                .collect();
            fields.service_ids = Some(service_ids.clone());
            fields.wp_hash = Some(outline.work_package_hash);

            cap_map(&mut self.submissions, MAX_MAP_ENTRIES);
            self.submissions.insert(
                *submission_or_share_id,
                SubmissionContext {
                    core: *core,
                    work_package_hash: Some(outline.work_package_hash),
                    service_ids,
                    inserted_at: Instant::now(),
                },
            );
        }

        // --- 5. Look up submissions map for events that have a submission key but lack fields ---
        if fields.core.is_none() || fields.service_ids.is_none() || fields.wp_hash.is_none() {
            if let Some(sid) = submission_key {
                if let Some(ctx) = self.submissions.get(&sid) {
                    if fields.core.is_none() {
                        fields.core = Some(ctx.core);
                    }
                    if fields.service_ids.is_none() {
                        fields.service_ids = Some(ctx.service_ids.clone());
                    }
                    if fields.wp_hash.is_none() {
                        fields.wp_hash = ctx.work_package_hash;
                    }
                }
            }
        }

        // --- 6. Chain correlations ---

        // GuaranteeBuilt -> store in built_ids
        if let Event::GuaranteeBuilt { .. } = event {
            if let Some(core) = fields.core {
                cap_map(&mut self.built_ids, MAX_MAP_ENTRIES);
                self.built_ids.insert(
                    event_id,
                    ChainEntry {
                        core,
                        service_ids: fields.service_ids.clone(),
                        inserted_at: Instant::now(),
                    },
                );
            }
        }

        // SendingGuarantee(built_id) -> look up built_ids, then store in sending_ids
        if let Event::SendingGuarantee { built_id, .. } = event {
            if let Some(entry) = self.built_ids.get(built_id) {
                fields.core = fields.core.or(Some(entry.core));
                if fields.service_ids.is_none() {
                    fields.service_ids = entry.service_ids.clone();
                }
            }
            if let Some(core) = fields.core {
                cap_map(&mut self.sending_ids, MAX_MAP_ENTRIES);
                self.sending_ids.insert(
                    event_id,
                    ChainEntry {
                        core,
                        service_ids: fields.service_ids.clone(),
                        inserted_at: Instant::now(),
                    },
                );
            }
        }

        // GuaranteeSent / GuaranteeSendFailed(sending_id) -> look up sending_ids
        match event {
            Event::GuaranteeSent { sending_id, .. }
            | Event::GuaranteeSendFailed { sending_id, .. } => {
                if let Some(entry) = self.sending_ids.get(sending_id) {
                    fields.core = fields.core.or(Some(entry.core));
                    if fields.service_ids.is_none() {
                        fields.service_ids = entry.service_ids.clone();
                    }
                }
            }
            _ => {}
        }

        // SendingSegmentShardRequest / SendingSegmentRequest -> store in request_ids
        match event {
            Event::SendingSegmentShardRequest { .. }
            | Event::SendingSegmentRequest { .. } => {
                if let Some(core) = fields.core {
                    cap_map(&mut self.request_ids, MAX_MAP_ENTRIES);
                    self.request_ids.insert(
                        event_id,
                        ChainEntry {
                            core,
                            service_ids: fields.service_ids.clone(),
                            inserted_at: Instant::now(),
                        },
                    );
                }
            }
            _ => {}
        }

        // Segment*Request* events with request_id -> look up request_ids
        match event {
            Event::SegmentShardRequestFailed { request_id, .. }
            | Event::SegmentShardRequestSent { request_id, .. }
            | Event::SegmentShardRequestReceived { request_id, .. }
            | Event::SegmentShardsTransferred { request_id, .. }
            | Event::SegmentRequestFailed { request_id, .. }
            | Event::SegmentRequestSent { request_id, .. }
            | Event::SegmentRequestReceived { request_id, .. }
            | Event::SegmentsTransferred { request_id, .. } => {
                if let Some(entry) = self.request_ids.get(request_id) {
                    fields.core = fields.core.or(Some(entry.core));
                    if fields.service_ids.is_none() {
                        fields.service_ids = entry.service_ids.clone();
                    }
                }
            }
            _ => {}
        }

        // ReconstructingSegments -> store in reconstructing_ids
        if let Event::ReconstructingSegments { .. } = event {
            if let Some(core) = fields.core {
                cap_map(&mut self.reconstructing_ids, MAX_MAP_ENTRIES);
                self.reconstructing_ids.insert(
                    event_id,
                    ChainEntry {
                        core,
                        service_ids: fields.service_ids.clone(),
                        inserted_at: Instant::now(),
                    },
                );
            }
        }

        // SegmentReconstructionFailed / SegmentsReconstructed(reconstructing_id) -> look up
        match event {
            Event::SegmentReconstructionFailed {
                reconstructing_id, ..
            }
            | Event::SegmentsReconstructed {
                reconstructing_id, ..
            } => {
                if let Some(entry) = self.reconstructing_ids.get(reconstructing_id) {
                    fields.core = fields.core.or(Some(entry.core));
                    if fields.service_ids.is_none() {
                        fields.service_ids = entry.service_ids.clone();
                    }
                }
            }
            _ => {}
        }

        fields
    }

    /// Evict entries older than [`STALE_TTL`] from all internal maps.
    fn evict_stale(&mut self) {
        let cutoff = STALE_TTL;
        self.submissions
            .retain(|_, ctx| ctx.inserted_at.elapsed() <= cutoff);
        self.built_ids
            .retain(|_, e| e.inserted_at.elapsed() <= cutoff);
        self.sending_ids
            .retain(|_, e| e.inserted_at.elapsed() <= cutoff);
        self.request_ids
            .retain(|_, e| e.inserted_at.elapsed() <= cutoff);
        self.reconstructing_ids
            .retain(|_, e| e.inserted_at.elapsed() <= cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Event;
    use crate::types::*;

    fn make_wp_outline(service_ids: &[u32], wp_hash: [u8; 32]) -> WorkPackageOutline {
        WorkPackageSummary {
            work_package_size: 100,
            work_package_hash: wp_hash,
            anchor: [0u8; 32],
            lookup_anchor_slot: 0,
            prerequisites: vec![],
            work_items: service_ids
                .iter()
                .map(|&sid| WorkItemSummary {
                    service_id: sid,
                    payload_size: 0,
                    refine_gas_limit: 0,
                    accumulate_gas_limit: 0,
                    sum_of_extrinsic_lengths: 0,
                    imports: vec![],
                    num_exported_segments: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn test_wp_received_populates_submission() {
        let mut enricher = NodeEventEnricher::default();
        let wp_hash = [42u8; 32];
        let event = Event::WorkPackageReceived {
            timestamp: 1000,
            submission_or_share_id: 100,
            core: 5,
            outline: make_wp_outline(&[10, 20, 30], wp_hash),
        };

        let fields = enricher.process(&event, 1);

        assert_eq!(fields.core, Some(5));
        assert_eq!(fields.submission_id, Some(100));
        assert_eq!(fields.service_ids, Some(vec![10, 20, 30]));
        assert_eq!(fields.wp_hash, Some(wp_hash));
    }

    #[test]
    fn test_submission_chain_lookup() {
        let mut enricher = NodeEventEnricher::default();
        let wp_hash = [42u8; 32];

        // First: WPReceived stores context
        let wp_event = Event::WorkPackageReceived {
            timestamp: 1000,
            submission_or_share_id: 100,
            core: 5,
            outline: make_wp_outline(&[10, 20], wp_hash),
        };
        enricher.process(&wp_event, 1);

        // Then: Authorized with same submission_or_share_id inherits
        let auth_event = Event::Authorized {
            timestamp: 1001,
            submission_or_share_id: 100,
            cost: IsAuthorizedCost {
                total: ExecCost { gas_used: 500, elapsed_ns: 100 },
                load_ns: 50,
                host_call: ExecCost { gas_used: 200, elapsed_ns: 40 },
            },
        };
        let fields = enricher.process(&auth_event, 2);

        assert_eq!(fields.core, Some(5));
        assert_eq!(fields.submission_id, Some(100));
        assert_eq!(fields.service_ids, Some(vec![10, 20]));
        assert_eq!(fields.wp_hash, Some(wp_hash));
    }

    #[test]
    fn test_guarantee_chain() {
        let mut enricher = NodeEventEnricher::default();
        let wp_hash = [42u8; 32];

        // WPReceived → stores submission context
        let wp_event = Event::WorkPackageReceived {
            timestamp: 1000,
            submission_or_share_id: 100,
            core: 3,
            outline: make_wp_outline(&[10], wp_hash),
        };
        enricher.process(&wp_event, 1);

        // GuaranteeBuilt (submission_id=100) → inherits core from submission, stores in built_ids
        let built_event = Event::GuaranteeBuilt {
            timestamp: 1001,
            submission_id: 100,
            outline: GuaranteeSummary {
                work_report_hash: [0u8; 32],
                slot: 10,
                guarantors: vec![],
            },
        };
        let fields = enricher.process(&built_event, 2);
        assert_eq!(fields.core, Some(3));

        // SendingGuarantee (built_id=2) → looks up built_ids, stores in sending_ids
        let sending_event = Event::SendingGuarantee {
            timestamp: 1002,
            built_id: 2,
            recipient: [0u8; 32],
        };
        let fields = enricher.process(&sending_event, 3);
        assert_eq!(fields.core, Some(3));

        // GuaranteeSent (sending_id=3) → looks up sending_ids
        let sent_event = Event::GuaranteeSent {
            timestamp: 1003,
            sending_id: 3,
        };
        let fields = enricher.process(&sent_event, 4);
        assert_eq!(fields.core, Some(3));
    }

    #[test]
    fn test_segment_chain() {
        let mut enricher = NodeEventEnricher::default();
        let wp_hash = [42u8; 32];

        // WPReceived
        let wp_event = Event::WorkPackageReceived {
            timestamp: 1000,
            submission_or_share_id: 100,
            core: 7,
            outline: make_wp_outline(&[10], wp_hash),
        };
        enricher.process(&wp_event, 1);

        // SendingSegmentShardRequest (submission_id=100) → inherits core, stores in request_ids
        let request_event = Event::SendingSegmentShardRequest {
            timestamp: 1001,
            submission_id: 100,
            assurer: [0u8; 32],
            proofs: false,
            shards: vec![],
        };
        let fields = enricher.process(&request_event, 2);
        assert_eq!(fields.core, Some(7));

        // SegmentShardRequestSent (request_id=2) → looks up request_ids
        let sent_event = Event::SegmentShardRequestSent {
            timestamp: 1002,
            request_id: 2,
        };
        let fields = enricher.process(&sent_event, 3);
        assert_eq!(fields.core, Some(7));
    }

    #[test]
    fn test_reconstructing_chain() {
        let mut enricher = NodeEventEnricher::default();
        let wp_hash = [42u8; 32];

        // WPReceived
        let wp_event = Event::WorkPackageReceived {
            timestamp: 1000,
            submission_or_share_id: 100,
            core: 2,
            outline: make_wp_outline(&[10], wp_hash),
        };
        enricher.process(&wp_event, 1);

        // ReconstructingSegments (submission_id=100) → inherits core, stores in reconstructing_ids
        let recon_event = Event::ReconstructingSegments {
            timestamp: 1001,
            submission_id: 100,
            segments: vec![],
            kind: ReconstructionKind::Trivial,
        };
        let fields = enricher.process(&recon_event, 2);
        assert_eq!(fields.core, Some(2));

        // SegmentsReconstructed (reconstructing_id=2) → looks up reconstructing_ids
        let done_event = Event::SegmentsReconstructed {
            timestamp: 1002,
            reconstructing_id: 2,
        };
        let fields = enricher.process(&done_event, 3);
        assert_eq!(fields.core, Some(2));
    }

    #[test]
    fn test_cap_map_clears() {
        let mut enricher = NodeEventEnricher::default();

        // Insert MAX_MAP_ENTRIES + 1 submissions to trigger cap
        // cap_map clears when len >= limit, so the (MAX_MAP_ENTRIES+1)th insert triggers clear
        for i in 0..=MAX_MAP_ENTRIES {
            let event = Event::WorkPackageReceived {
                timestamp: 1000,
                submission_or_share_id: i as u64,
                core: 1,
                outline: make_wp_outline(&[10], [0u8; 32]),
            };
            enricher.process(&event, i as u64);
        }

        // After cap triggers, map was cleared, then the last insert added 1 entry
        assert_eq!(enricher.submissions.len(), 1);
    }

    #[test]
    fn test_dropped_event_no_enrich() {
        let mut enricher = NodeEventEnricher::default();

        let event = Event::Dropped {
            timestamp: 1000,
            last_timestamp: 999,
            num: 5,
        };
        let fields = enricher.process(&event, 1);

        assert!(fields.slot.is_none());
        assert!(fields.core.is_none());
        assert!(fields.submission_id.is_none());
        assert!(fields.service_ids.is_none());
        assert!(fields.wp_hash.is_none());
    }

    #[test]
    fn test_slot_extraction() {
        let mut enricher = NodeEventEnricher::default();

        let event = Event::BestBlockChanged {
            timestamp: 1000,
            slot: 42,
            hash: [0u8; 32],
        };
        let fields = enricher.process(&event, 1);
        assert_eq!(fields.slot, Some(42));
    }
}
