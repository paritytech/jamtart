use crate::encoding::{Encode, EncodingError};
use crate::types::*;
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

pub const TELEMETRY_PROTOCOL_VERSION: u8 = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInformation {
    pub params: ProtocolParameters,
    pub genesis: HeaderHash,
    pub details: PeerDetails, // Contains peer_id + peer_address
    pub flags: u32,
    pub implementation_name: NodeName,
    pub implementation_version: NodeVersion,
    pub gp_version: BoundedString<16>, // Gray Paper version
    pub additional_info: NodeNote,
}

// Helper methods for backward compatibility
impl NodeInformation {
    pub fn peer_id(&self) -> &PeerId {
        &self.details.peer_id
    }

    pub fn peer_address(&self) -> &PeerAddress {
        &self.details.peer_address
    }
}

impl Encode for NodeInformation {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        TELEMETRY_PROTOCOL_VERSION.encode(buf)?;
        self.params.encode(buf)?;
        self.genesis.encode(buf)?;
        self.details.encode(buf)?;
        self.flags.encode(buf)?;
        self.implementation_name.encode(buf)?;
        self.implementation_version.encode(buf)?;
        self.gp_version.encode(buf)?;
        self.additional_info.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + self.params.encoded_size()
            + 32
            + self.details.encoded_size()
            + 4
            + self.implementation_name.encoded_size()
            + self.implementation_version.encoded_size()
            + self.gp_version.encoded_size()
            + self.additional_info.encoded_size()
    }
}

/// Event types matching the JAM telemetry specification
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum EventType {
    // Core events
    Dropped = 0,

    // Status and sync
    Status = 10,
    BestBlockChanged = 11,
    FinalizedBlockChanged = 12,
    SyncStatusChanged = 13,

    // Connection events
    ConnectionRefused = 20,
    ConnectingIn = 21,
    ConnectInFailed = 22,
    ConnectedIn = 23,
    ConnectingOut = 24,
    ConnectOutFailed = 25,
    ConnectedOut = 26,
    Disconnected = 27,
    PeerMisbehaved = 28,

    // Authoring and importing
    Authoring = 40,
    AuthoringFailed = 41,
    Authored = 42,
    Importing = 43,
    BlockVerificationFailed = 44,
    BlockVerified = 45,
    BlockExecutionFailed = 46,
    BlockExecuted = 47,

    // Block distribution
    BlockAnnouncementStreamOpened = 60,
    BlockAnnouncementStreamClosed = 61,
    BlockAnnounced = 62,
    SendingBlockRequest = 63,
    ReceivingBlockRequest = 64,
    BlockRequestFailed = 65,
    BlockRequestSent = 66,
    BlockRequestReceived = 67,
    BlockTransferred = 68,

    // Ticket generation and transfer
    GeneratingTickets = 80,
    TicketGenerationFailed = 81,
    TicketsGenerated = 82,
    TicketTransferFailed = 83,
    TicketTransferred = 84,

    // Work package submission
    WorkPackageSubmission = 90,
    WorkPackageBeingShared = 91,
    WorkPackageFailed = 92,
    DuplicateWorkPackage = 93,
    WorkPackageReceived = 94,
    Authorized = 95,
    ExtrinsicDataReceived = 96,
    ImportsReceived = 97,
    SharingWorkPackage = 98,
    WorkPackageSharingFailed = 99,
    BundleSent = 100,
    Refined = 101,
    WorkReportBuilt = 102,
    WorkReportSignatureSent = 103,
    WorkReportSignatureReceived = 104,
    GuaranteeBuilt = 105,
    SendingGuarantee = 106,
    GuaranteeSendFailed = 107,
    GuaranteeSent = 108,
    GuaranteesDistributed = 109,
    ReceivingGuarantee = 110,
    GuaranteeReceiveFailed = 111,
    GuaranteeReceived = 112,
    GuaranteeDiscarded = 113,

    // Shard requests
    SendingShardRequest = 120,
    ReceivingShardRequest = 121,
    ShardRequestFailed = 122,
    ShardRequestSent = 123,
    ShardRequestReceived = 124,
    ShardsTransferred = 125,

    // Assurance distribution
    DistributingAssurance = 126,
    AssuranceSendFailed = 127,
    AssuranceSent = 128,
    AssuranceDistributed = 129,
    AssuranceReceiveFailed = 130,
    AssuranceReceived = 131,

    // Bundle shard requests
    SendingBundleShardRequest = 140,
    ReceivingBundleShardRequest = 141,
    BundleShardRequestFailed = 142,
    BundleShardRequestSent = 143,
    BundleShardRequestReceived = 144,
    BundleShardTransferred = 145,
    ReconstructingBundle = 146,
    BundleReconstructed = 147,
    SendingBundleRequest = 148,
    ReceivingBundleRequest = 149,
    BundleRequestFailed = 150,
    BundleRequestSent = 151,
    BundleRequestReceived = 152,
    BundleTransferred = 153,

    // Work package hash mapping
    WorkPackageHashMapped = 160,
    SegmentsRootMapped = 161,

    // Segment shard requests
    SendingSegmentShardRequest = 162,
    ReceivingSegmentShardRequest = 163,
    SegmentShardRequestFailed = 164,
    SegmentShardRequestSent = 165,
    SegmentShardRequestReceived = 166,
    SegmentShardsTransferred = 167,

    // Segment reconstruction
    ReconstructingSegments = 168,
    SegmentReconstructionFailed = 169,
    SegmentsReconstructed = 170,
    SegmentVerificationFailed = 171,
    SegmentsVerified = 172,
    SendingSegmentRequest = 173,
    ReceivingSegmentRequest = 174,
    SegmentRequestFailed = 175,
    SegmentRequestSent = 176,
    SegmentRequestReceived = 177,
    SegmentsTransferred = 178,

    // Preimage events
    PreimageAnnouncementFailed = 190,
    PreimageAnnounced = 191,
    AnnouncedPreimageForgotten = 192,
    SendingPreimageRequest = 193,
    ReceivingPreimageRequest = 194,
    PreimageRequestFailed = 195,
    PreimageRequestSent = 196,
    PreimageRequestReceived = 197,
    PreimageTransferred = 198,
    PreimageDiscarded = 199,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    // Core events
    Dropped {
        timestamp: Timestamp,
        last_timestamp: Timestamp,
        num: u64,
    },

    // Status
    Status {
        timestamp: Timestamp,
        num_peers: u32,
        num_val_peers: u32,
        num_sync_peers: u32,
        num_guarantees: FixedVec<u8, 341>, // CoreCount max
        num_shards: u32,
        shards_size: u64,
        num_preimages: u32,
        preimages_size: u32,
    },

    BestBlockChanged {
        timestamp: Timestamp,
        slot: Slot,
        hash: HeaderHash,
    },

    FinalizedBlockChanged {
        timestamp: Timestamp,
        slot: Slot,
        hash: HeaderHash,
    },

    SyncStatusChanged {
        timestamp: Timestamp,
        synced: bool,
    },

    // Connection events
    ConnectionRefused {
        timestamp: Timestamp,
        from: PeerAddr,
    },

    ConnectingIn {
        timestamp: Timestamp,
        from: PeerAddr,
    },

    ConnectInFailed {
        timestamp: Timestamp,
        connecting_id: EventId,
        reason: Reason,
    },

    ConnectedIn {
        timestamp: Timestamp,
        connecting_id: EventId,
        peer_id: PeerId,
    },

    ConnectingOut {
        timestamp: Timestamp,
        to: PeerDetails,
    },

    ConnectOutFailed {
        timestamp: Timestamp,
        connecting_id: EventId,
        reason: Reason,
    },

    ConnectedOut {
        timestamp: Timestamp,
        connecting_id: EventId,
    },

    Disconnected {
        timestamp: Timestamp,
        peer: PeerId,
        terminator: Option<ConnectionSide>,
        reason: Reason,
    },

    PeerMisbehaved {
        timestamp: Timestamp,
        peer: PeerId,
        reason: Reason,
    },

    // Authoring events
    Authoring {
        timestamp: Timestamp,
        slot: Slot,
        parent: HeaderHash,
    },

    AuthoringFailed {
        timestamp: Timestamp,
        authoring_id: EventId,
        reason: Reason,
    },

    Authored {
        timestamp: Timestamp,
        authoring_id: EventId,
        outline: BlockOutline,
    },

    Importing {
        timestamp: Timestamp,
        slot: Slot,
        outline: BlockOutline,
    },

    BlockVerificationFailed {
        timestamp: Timestamp,
        importing_id: EventId,
        reason: Reason,
    },

    BlockVerified {
        timestamp: Timestamp,
        importing_id: EventId,
    },

    BlockExecutionFailed {
        timestamp: Timestamp,
        authoring_or_importing_id: EventId,
        reason: Reason,
    },

    BlockExecuted {
        timestamp: Timestamp,
        authoring_or_importing_id: EventId,
        accumulate_costs: AccumulateCosts,
    },

    // Block distribution (60-68)
    BlockAnnouncementStreamOpened {
        timestamp: Timestamp,
        peer: PeerId,
        opener: ConnectionSide,
    },

    BlockAnnouncementStreamClosed {
        timestamp: Timestamp,
        peer: PeerId,
        closer: ConnectionSide,
        reason: Reason,
    },

    BlockAnnounced {
        timestamp: Timestamp,
        peer: PeerId,
        announcer: ConnectionSide,
        slot: Slot,
        hash: HeaderHash,
    },

    SendingBlockRequest {
        timestamp: Timestamp,
        recipient: PeerId,
        hash: HeaderHash,
        direction: BlockRequestDirection,
        max_blocks: u32,
    },

    ReceivingBlockRequest {
        timestamp: Timestamp,
        sender: PeerId,
    },

    BlockRequestFailed {
        timestamp: Timestamp,
        request_id: EventId,
        reason: Reason,
    },

    BlockRequestSent {
        timestamp: Timestamp,
        request_id: EventId,
    },

    BlockRequestReceived {
        timestamp: Timestamp,
        request_id: EventId,
        hash: HeaderHash,
        direction: BlockRequestDirection,
        max_blocks: u32,
    },

    BlockTransferred {
        timestamp: Timestamp,
        request_id: EventId,
        slot: Slot,
        outline: BlockOutline,
        last: bool,
    },

    // Ticket events (80-84)
    GeneratingTickets {
        timestamp: Timestamp,
        epoch: EpochIndex,
    },

    TicketGenerationFailed {
        timestamp: Timestamp,
        generating_id: EventId,
        reason: Reason,
    },

    TicketsGenerated {
        timestamp: Timestamp,
        generating_id: EventId,
        ids: BoundedVec<TicketId, TICKETS_ATTEMPTS_NUMBER>,
    },

    TicketTransferFailed {
        timestamp: Timestamp,
        peer: PeerId,
        sender: ConnectionSide,
        from_proxy: bool,
        reason: Reason,
    },

    TicketTransferred {
        timestamp: Timestamp,
        peer: PeerId,
        sender: ConnectionSide,
        from_proxy: bool,
        epoch: EpochIndex,
        attempt: TicketAttempt,
        id: TicketId,
    },

    // Work package events (90-104)
    WorkPackageSubmission {
        timestamp: Timestamp,
        builder: PeerId,
        bundle: bool,
    },

    WorkPackageBeingShared {
        timestamp: Timestamp,
        primary: PeerId,
    },

    WorkPackageFailed {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
        reason: Reason,
    },

    DuplicateWorkPackage {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
        core: CoreIndex,
        hash: WorkPackageHash,
    },

    WorkPackageReceived {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
        core: CoreIndex,
        outline: WorkPackageOutline,
    },

    Authorized {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
        cost: IsAuthorizedCost,
    },

    ExtrinsicDataReceived {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
    },

    ImportsReceived {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
    },

    SharingWorkPackage {
        timestamp: Timestamp,
        submission_id: EventId,
        secondary: PeerId,
    },

    WorkPackageSharingFailed {
        timestamp: Timestamp,
        submission_id: EventId,
        secondary: PeerId,
        reason: Reason,
    },

    BundleSent {
        timestamp: Timestamp,
        submission_id: EventId,
        secondary: PeerId,
    },

    Refined {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
        costs: RefineCosts,
    },

    WorkReportBuilt {
        timestamp: Timestamp,
        submission_or_share_id: EventId,
        outline: WorkReportOutline,
    },

    WorkReportSignatureSent {
        timestamp: Timestamp,
        share_id: EventId,
    },

    WorkReportSignatureReceived {
        timestamp: Timestamp,
        submission_id: EventId,
        secondary: PeerId,
    },

    // Guarantee events (105-113)
    GuaranteeBuilt {
        timestamp: Timestamp,
        submission_id: EventId,
        outline: GuaranteeOutline,
    },

    SendingGuarantee {
        timestamp: Timestamp,
        built_id: EventId,
        recipient: PeerId,
    },

    GuaranteeSendFailed {
        timestamp: Timestamp,
        sending_id: EventId,
        reason: Reason,
    },

    GuaranteeSent {
        timestamp: Timestamp,
        sending_id: EventId,
    },

    GuaranteesDistributed {
        timestamp: Timestamp,
        submission_id: EventId,
    },

    ReceivingGuarantee {
        timestamp: Timestamp,
        sender: PeerId,
    },

    GuaranteeReceiveFailed {
        timestamp: Timestamp,
        receiving_id: EventId,
        reason: Reason,
    },

    GuaranteeReceived {
        timestamp: Timestamp,
        receiving_id: EventId,
        outline: GuaranteeOutline,
    },

    GuaranteeDiscarded {
        timestamp: Timestamp,
        outline: GuaranteeOutline,
        reason: GuaranteeDiscardReason,
    },

    // Shard request events (120-125)
    SendingShardRequest {
        timestamp: Timestamp,
        guarantor: PeerId,
        erasure_root: ErasureRoot,
        shard: ShardIndex,
    },

    ReceivingShardRequest {
        timestamp: Timestamp,
        assurer: PeerId,
    },

    ShardRequestFailed {
        timestamp: Timestamp,
        request_id: EventId,
        reason: Reason,
    },

    ShardRequestSent {
        timestamp: Timestamp,
        request_id: EventId,
    },

    ShardRequestReceived {
        timestamp: Timestamp,
        request_id: EventId,
        erasure_root: ErasureRoot,
        shard: ShardIndex,
    },

    ShardsTransferred {
        timestamp: Timestamp,
        request_id: EventId,
    },

    // Assurance distribution (126-131)
    DistributingAssurance {
        timestamp: Timestamp,
        statement: AvailabilityStatement,
    },

    AssuranceSendFailed {
        timestamp: Timestamp,
        distributing_id: EventId,
        recipient: PeerId,
        reason: Reason,
    },

    AssuranceSent {
        timestamp: Timestamp,
        distributing_id: EventId,
        recipient: PeerId,
    },

    AssuranceDistributed {
        timestamp: Timestamp,
        distributing_id: EventId,
    },

    AssuranceReceiveFailed {
        timestamp: Timestamp,
        sender: PeerId,
        reason: Reason,
    },

    AssuranceReceived {
        timestamp: Timestamp,
        sender: PeerId,
        anchor: HeaderHash,
    },

    // Bundle shard requests (140-153)
    SendingBundleShardRequest {
        timestamp: Timestamp,
        audit_id: EventId,
        assurer: PeerId,
        shard: ShardIndex,
    },

    ReceivingBundleShardRequest {
        timestamp: Timestamp,
        auditor: PeerId,
    },

    BundleShardRequestFailed {
        timestamp: Timestamp,
        request_id: EventId,
        reason: Reason,
    },

    BundleShardRequestSent {
        timestamp: Timestamp,
        request_id: EventId,
    },

    BundleShardRequestReceived {
        timestamp: Timestamp,
        request_id: EventId,
        erasure_root: ErasureRoot,
        shard: ShardIndex,
    },

    BundleShardTransferred {
        timestamp: Timestamp,
        request_id: EventId,
    },

    ReconstructingBundle {
        timestamp: Timestamp,
        audit_id: EventId,
        kind: ReconstructionKind,
    },

    BundleReconstructed {
        timestamp: Timestamp,
        audit_id: EventId,
    },

    SendingBundleRequest {
        timestamp: Timestamp,
        audit_id: EventId,
        guarantor: PeerId,
    },

    ReceivingBundleRequest {
        timestamp: Timestamp,
        auditor: PeerId,
    },

    BundleRequestFailed {
        timestamp: Timestamp,
        request_id: EventId,
        reason: Reason,
    },

    BundleRequestSent {
        timestamp: Timestamp,
        request_id: EventId,
    },

    BundleRequestReceived {
        timestamp: Timestamp,
        request_id: EventId,
        erasure_root: ErasureRoot,
    },

    BundleTransferred {
        timestamp: Timestamp,
        request_id: EventId,
    },

    // Work package hash mapping (160-161)
    WorkPackageHashMapped {
        timestamp: Timestamp,
        submission_id: EventId,
        work_package_hash: WorkPackageHash,
        segments_root: SegmentTreeRoot,
    },

    SegmentsRootMapped {
        timestamp: Timestamp,
        submission_id: EventId,
        segments_root: SegmentTreeRoot,
        erasure_root: ErasureRoot,
    },

    // Segment shard requests (162-167)
    SendingSegmentShardRequest {
        timestamp: Timestamp,
        submission_id: EventId,
        assurer: PeerId,
        proofs: bool,
        shards: BoundedVec<(ImportSegmentId, ShardIndex), MAX_IMPORT_SEGMENTS>,
    },

    ReceivingSegmentShardRequest {
        timestamp: Timestamp,
        sender: PeerId,
        proofs: bool,
    },

    SegmentShardRequestFailed {
        timestamp: Timestamp,
        request_id: EventId,
        reason: Reason,
    },

    SegmentShardRequestSent {
        timestamp: Timestamp,
        request_id: EventId,
    },

    SegmentShardRequestReceived {
        timestamp: Timestamp,
        request_id: EventId,
        num: u16,
    },

    SegmentShardsTransferred {
        timestamp: Timestamp,
        request_id: EventId,
    },

    // Segment reconstruction (168-172)
    ReconstructingSegments {
        timestamp: Timestamp,
        submission_id: EventId,
        segments: BoundedVec<ImportSegmentId, MAX_IMPORT_SEGMENTS>,
        kind: ReconstructionKind,
    },

    SegmentReconstructionFailed {
        timestamp: Timestamp,
        reconstructing_id: EventId,
        reason: Reason,
    },

    SegmentsReconstructed {
        timestamp: Timestamp,
        reconstructing_id: EventId,
    },

    SegmentVerificationFailed {
        timestamp: Timestamp,
        submission_id: EventId,
        segments: BoundedVec<u16, MAX_IMPORTS>,
        reason: Reason,
    },

    SegmentsVerified {
        timestamp: Timestamp,
        submission_id: EventId,
        segments: BoundedVec<u16, MAX_IMPORTS>,
    },

    // Segment requests (173-178)
    SendingSegmentRequest {
        timestamp: Timestamp,
        submission_id: EventId,
        prev_guarantor: PeerId,
        segments: BoundedVec<u16, MAX_IMPORT_SEGMENTS>,
    },

    ReceivingSegmentRequest {
        timestamp: Timestamp,
        guarantor: PeerId,
    },

    SegmentRequestFailed {
        timestamp: Timestamp,
        request_id: EventId,
        reason: Reason,
    },

    SegmentRequestSent {
        timestamp: Timestamp,
        request_id: EventId,
    },

    SegmentRequestReceived {
        timestamp: Timestamp,
        request_id: EventId,
        num: u16,
    },

    SegmentsTransferred {
        timestamp: Timestamp,
        request_id: EventId,
    },

    // Preimage events (190-199)
    PreimageAnnouncementFailed {
        timestamp: Timestamp,
        peer: PeerId,
        announcer: ConnectionSide,
        reason: Reason,
    },

    PreimageAnnounced {
        timestamp: Timestamp,
        peer: PeerId,
        announcer: ConnectionSide,
        service: ServiceId,
        hash: Hash,
        length: u32,
    },

    AnnouncedPreimageForgotten {
        timestamp: Timestamp,
        service: ServiceId,
        hash: Hash,
        length: u32,
        reason: AnnouncedPreimageForgetReason,
    },

    SendingPreimageRequest {
        timestamp: Timestamp,
        recipient: PeerId,
        hash: Hash,
    },

    ReceivingPreimageRequest {
        timestamp: Timestamp,
        sender: PeerId,
    },

    PreimageRequestFailed {
        timestamp: Timestamp,
        request_id: EventId,
        reason: Reason,
    },

    PreimageRequestSent {
        timestamp: Timestamp,
        request_id: EventId,
    },

    PreimageRequestReceived {
        timestamp: Timestamp,
        request_id: EventId,
        hash: Hash,
    },

    PreimageTransferred {
        timestamp: Timestamp,
        request_id: EventId,
        length: u32,
    },

    PreimageDiscarded {
        timestamp: Timestamp,
        hash: Hash,
        length: u32,
        reason: PreimageDiscardReason,
    },
}

impl Event {
    pub fn event_type(&self) -> EventType {
        match self {
            Event::Dropped { .. } => EventType::Dropped,
            Event::Status { .. } => EventType::Status,
            Event::BestBlockChanged { .. } => EventType::BestBlockChanged,
            Event::FinalizedBlockChanged { .. } => EventType::FinalizedBlockChanged,
            Event::SyncStatusChanged { .. } => EventType::SyncStatusChanged,
            Event::ConnectionRefused { .. } => EventType::ConnectionRefused,
            Event::ConnectingIn { .. } => EventType::ConnectingIn,
            Event::ConnectInFailed { .. } => EventType::ConnectInFailed,
            Event::ConnectedIn { .. } => EventType::ConnectedIn,
            Event::ConnectingOut { .. } => EventType::ConnectingOut,
            Event::ConnectOutFailed { .. } => EventType::ConnectOutFailed,
            Event::ConnectedOut { .. } => EventType::ConnectedOut,
            Event::Disconnected { .. } => EventType::Disconnected,
            Event::PeerMisbehaved { .. } => EventType::PeerMisbehaved,
            Event::Authoring { .. } => EventType::Authoring,
            Event::AuthoringFailed { .. } => EventType::AuthoringFailed,
            Event::Authored { .. } => EventType::Authored,
            Event::Importing { .. } => EventType::Importing,
            Event::BlockVerificationFailed { .. } => EventType::BlockVerificationFailed,
            Event::BlockVerified { .. } => EventType::BlockVerified,
            Event::BlockExecutionFailed { .. } => EventType::BlockExecutionFailed,
            Event::BlockExecuted { .. } => EventType::BlockExecuted,
            Event::BlockAnnouncementStreamOpened { .. } => EventType::BlockAnnouncementStreamOpened,
            Event::BlockAnnouncementStreamClosed { .. } => EventType::BlockAnnouncementStreamClosed,
            Event::BlockAnnounced { .. } => EventType::BlockAnnounced,
            Event::SendingBlockRequest { .. } => EventType::SendingBlockRequest,
            Event::ReceivingBlockRequest { .. } => EventType::ReceivingBlockRequest,
            Event::BlockRequestFailed { .. } => EventType::BlockRequestFailed,
            Event::BlockRequestSent { .. } => EventType::BlockRequestSent,
            Event::BlockRequestReceived { .. } => EventType::BlockRequestReceived,
            Event::BlockTransferred { .. } => EventType::BlockTransferred,
            Event::GeneratingTickets { .. } => EventType::GeneratingTickets,
            Event::TicketGenerationFailed { .. } => EventType::TicketGenerationFailed,
            Event::TicketsGenerated { .. } => EventType::TicketsGenerated,
            Event::TicketTransferFailed { .. } => EventType::TicketTransferFailed,
            Event::TicketTransferred { .. } => EventType::TicketTransferred,
            Event::WorkPackageSubmission { .. } => EventType::WorkPackageSubmission,
            Event::WorkPackageBeingShared { .. } => EventType::WorkPackageBeingShared,
            Event::WorkPackageFailed { .. } => EventType::WorkPackageFailed,
            Event::DuplicateWorkPackage { .. } => EventType::DuplicateWorkPackage,
            Event::WorkPackageReceived { .. } => EventType::WorkPackageReceived,
            Event::Authorized { .. } => EventType::Authorized,
            Event::ExtrinsicDataReceived { .. } => EventType::ExtrinsicDataReceived,
            Event::ImportsReceived { .. } => EventType::ImportsReceived,
            Event::SharingWorkPackage { .. } => EventType::SharingWorkPackage,
            Event::WorkPackageSharingFailed { .. } => EventType::WorkPackageSharingFailed,
            Event::BundleSent { .. } => EventType::BundleSent,
            Event::Refined { .. } => EventType::Refined,
            Event::WorkReportBuilt { .. } => EventType::WorkReportBuilt,
            Event::WorkReportSignatureSent { .. } => EventType::WorkReportSignatureSent,
            Event::WorkReportSignatureReceived { .. } => EventType::WorkReportSignatureReceived,
            Event::GuaranteeBuilt { .. } => EventType::GuaranteeBuilt,
            Event::SendingGuarantee { .. } => EventType::SendingGuarantee,
            Event::GuaranteeSendFailed { .. } => EventType::GuaranteeSendFailed,
            Event::GuaranteeSent { .. } => EventType::GuaranteeSent,
            Event::GuaranteesDistributed { .. } => EventType::GuaranteesDistributed,
            Event::ReceivingGuarantee { .. } => EventType::ReceivingGuarantee,
            Event::GuaranteeReceiveFailed { .. } => EventType::GuaranteeReceiveFailed,
            Event::GuaranteeReceived { .. } => EventType::GuaranteeReceived,
            Event::GuaranteeDiscarded { .. } => EventType::GuaranteeDiscarded,
            Event::SendingShardRequest { .. } => EventType::SendingShardRequest,
            Event::ReceivingShardRequest { .. } => EventType::ReceivingShardRequest,
            Event::ShardRequestFailed { .. } => EventType::ShardRequestFailed,
            Event::ShardRequestSent { .. } => EventType::ShardRequestSent,
            Event::ShardRequestReceived { .. } => EventType::ShardRequestReceived,
            Event::ShardsTransferred { .. } => EventType::ShardsTransferred,
            Event::DistributingAssurance { .. } => EventType::DistributingAssurance,
            Event::AssuranceSendFailed { .. } => EventType::AssuranceSendFailed,
            Event::AssuranceSent { .. } => EventType::AssuranceSent,
            Event::AssuranceDistributed { .. } => EventType::AssuranceDistributed,
            Event::AssuranceReceiveFailed { .. } => EventType::AssuranceReceiveFailed,
            Event::AssuranceReceived { .. } => EventType::AssuranceReceived,
            Event::SendingBundleShardRequest { .. } => EventType::SendingBundleShardRequest,
            Event::ReceivingBundleShardRequest { .. } => EventType::ReceivingBundleShardRequest,
            Event::BundleShardRequestFailed { .. } => EventType::BundleShardRequestFailed,
            Event::BundleShardRequestSent { .. } => EventType::BundleShardRequestSent,
            Event::BundleShardRequestReceived { .. } => EventType::BundleShardRequestReceived,
            Event::BundleShardTransferred { .. } => EventType::BundleShardTransferred,
            Event::ReconstructingBundle { .. } => EventType::ReconstructingBundle,
            Event::BundleReconstructed { .. } => EventType::BundleReconstructed,
            Event::SendingBundleRequest { .. } => EventType::SendingBundleRequest,
            Event::ReceivingBundleRequest { .. } => EventType::ReceivingBundleRequest,
            Event::BundleRequestFailed { .. } => EventType::BundleRequestFailed,
            Event::BundleRequestSent { .. } => EventType::BundleRequestSent,
            Event::BundleRequestReceived { .. } => EventType::BundleRequestReceived,
            Event::BundleTransferred { .. } => EventType::BundleTransferred,
            Event::WorkPackageHashMapped { .. } => EventType::WorkPackageHashMapped,
            Event::SegmentsRootMapped { .. } => EventType::SegmentsRootMapped,
            Event::SendingSegmentShardRequest { .. } => EventType::SendingSegmentShardRequest,
            Event::ReceivingSegmentShardRequest { .. } => EventType::ReceivingSegmentShardRequest,
            Event::SegmentShardRequestFailed { .. } => EventType::SegmentShardRequestFailed,
            Event::SegmentShardRequestSent { .. } => EventType::SegmentShardRequestSent,
            Event::SegmentShardRequestReceived { .. } => EventType::SegmentShardRequestReceived,
            Event::SegmentShardsTransferred { .. } => EventType::SegmentShardsTransferred,
            Event::ReconstructingSegments { .. } => EventType::ReconstructingSegments,
            Event::SegmentReconstructionFailed { .. } => EventType::SegmentReconstructionFailed,
            Event::SegmentsReconstructed { .. } => EventType::SegmentsReconstructed,
            Event::SegmentVerificationFailed { .. } => EventType::SegmentVerificationFailed,
            Event::SegmentsVerified { .. } => EventType::SegmentsVerified,
            Event::SendingSegmentRequest { .. } => EventType::SendingSegmentRequest,
            Event::ReceivingSegmentRequest { .. } => EventType::ReceivingSegmentRequest,
            Event::SegmentRequestFailed { .. } => EventType::SegmentRequestFailed,
            Event::SegmentRequestSent { .. } => EventType::SegmentRequestSent,
            Event::SegmentRequestReceived { .. } => EventType::SegmentRequestReceived,
            Event::SegmentsTransferred { .. } => EventType::SegmentsTransferred,
            Event::PreimageAnnouncementFailed { .. } => EventType::PreimageAnnouncementFailed,
            Event::PreimageAnnounced { .. } => EventType::PreimageAnnounced,
            Event::AnnouncedPreimageForgotten { .. } => EventType::AnnouncedPreimageForgotten,
            Event::SendingPreimageRequest { .. } => EventType::SendingPreimageRequest,
            Event::ReceivingPreimageRequest { .. } => EventType::ReceivingPreimageRequest,
            Event::PreimageRequestFailed { .. } => EventType::PreimageRequestFailed,
            Event::PreimageRequestSent { .. } => EventType::PreimageRequestSent,
            Event::PreimageRequestReceived { .. } => EventType::PreimageRequestReceived,
            Event::PreimageTransferred { .. } => EventType::PreimageTransferred,
            Event::PreimageDiscarded { .. } => EventType::PreimageDiscarded,
        }
    }

    pub fn timestamp(&self) -> Timestamp {
        match self {
            Event::Dropped { timestamp, .. }
            | Event::Status { timestamp, .. }
            | Event::BestBlockChanged { timestamp, .. }
            | Event::FinalizedBlockChanged { timestamp, .. }
            | Event::SyncStatusChanged { timestamp, .. }
            | Event::ConnectionRefused { timestamp, .. }
            | Event::ConnectingIn { timestamp, .. }
            | Event::ConnectInFailed { timestamp, .. }
            | Event::ConnectedIn { timestamp, .. }
            | Event::ConnectingOut { timestamp, .. }
            | Event::ConnectOutFailed { timestamp, .. }
            | Event::ConnectedOut { timestamp, .. }
            | Event::Disconnected { timestamp, .. }
            | Event::PeerMisbehaved { timestamp, .. }
            | Event::Authoring { timestamp, .. }
            | Event::AuthoringFailed { timestamp, .. }
            | Event::Authored { timestamp, .. }
            | Event::Importing { timestamp, .. }
            | Event::BlockVerificationFailed { timestamp, .. }
            | Event::BlockVerified { timestamp, .. }
            | Event::BlockExecutionFailed { timestamp, .. }
            | Event::BlockExecuted { timestamp, .. }
            | Event::BlockAnnouncementStreamOpened { timestamp, .. }
            | Event::BlockAnnouncementStreamClosed { timestamp, .. }
            | Event::BlockAnnounced { timestamp, .. }
            | Event::SendingBlockRequest { timestamp, .. }
            | Event::ReceivingBlockRequest { timestamp, .. }
            | Event::BlockRequestFailed { timestamp, .. }
            | Event::BlockRequestSent { timestamp, .. }
            | Event::BlockRequestReceived { timestamp, .. }
            | Event::BlockTransferred { timestamp, .. }
            | Event::GeneratingTickets { timestamp, .. }
            | Event::TicketGenerationFailed { timestamp, .. }
            | Event::TicketsGenerated { timestamp, .. }
            | Event::TicketTransferFailed { timestamp, .. }
            | Event::TicketTransferred { timestamp, .. }
            | Event::WorkPackageSubmission { timestamp, .. }
            | Event::WorkPackageBeingShared { timestamp, .. }
            | Event::WorkPackageFailed { timestamp, .. }
            | Event::DuplicateWorkPackage { timestamp, .. }
            | Event::WorkPackageReceived { timestamp, .. }
            | Event::Authorized { timestamp, .. }
            | Event::ExtrinsicDataReceived { timestamp, .. }
            | Event::ImportsReceived { timestamp, .. }
            | Event::SharingWorkPackage { timestamp, .. }
            | Event::WorkPackageSharingFailed { timestamp, .. }
            | Event::BundleSent { timestamp, .. }
            | Event::Refined { timestamp, .. }
            | Event::WorkReportBuilt { timestamp, .. }
            | Event::WorkReportSignatureSent { timestamp, .. }
            | Event::WorkReportSignatureReceived { timestamp, .. }
            | Event::GuaranteeBuilt { timestamp, .. }
            | Event::SendingGuarantee { timestamp, .. }
            | Event::GuaranteeSendFailed { timestamp, .. }
            | Event::GuaranteeSent { timestamp, .. }
            | Event::GuaranteesDistributed { timestamp, .. }
            | Event::ReceivingGuarantee { timestamp, .. }
            | Event::GuaranteeReceiveFailed { timestamp, .. }
            | Event::GuaranteeReceived { timestamp, .. }
            | Event::GuaranteeDiscarded { timestamp, .. }
            | Event::SendingShardRequest { timestamp, .. }
            | Event::ReceivingShardRequest { timestamp, .. }
            | Event::ShardRequestFailed { timestamp, .. }
            | Event::ShardRequestSent { timestamp, .. }
            | Event::ShardRequestReceived { timestamp, .. }
            | Event::ShardsTransferred { timestamp, .. }
            | Event::DistributingAssurance { timestamp, .. }
            | Event::AssuranceSendFailed { timestamp, .. }
            | Event::AssuranceSent { timestamp, .. }
            | Event::AssuranceDistributed { timestamp, .. }
            | Event::AssuranceReceiveFailed { timestamp, .. }
            | Event::AssuranceReceived { timestamp, .. }
            | Event::SendingBundleShardRequest { timestamp, .. }
            | Event::ReceivingBundleShardRequest { timestamp, .. }
            | Event::BundleShardRequestFailed { timestamp, .. }
            | Event::BundleShardRequestSent { timestamp, .. }
            | Event::BundleShardRequestReceived { timestamp, .. }
            | Event::BundleShardTransferred { timestamp, .. }
            | Event::ReconstructingBundle { timestamp, .. }
            | Event::BundleReconstructed { timestamp, .. }
            | Event::SendingBundleRequest { timestamp, .. }
            | Event::ReceivingBundleRequest { timestamp, .. }
            | Event::BundleRequestFailed { timestamp, .. }
            | Event::BundleRequestSent { timestamp, .. }
            | Event::BundleRequestReceived { timestamp, .. }
            | Event::BundleTransferred { timestamp, .. }
            | Event::WorkPackageHashMapped { timestamp, .. }
            | Event::SegmentsRootMapped { timestamp, .. }
            | Event::SendingSegmentShardRequest { timestamp, .. }
            | Event::ReceivingSegmentShardRequest { timestamp, .. }
            | Event::SegmentShardRequestFailed { timestamp, .. }
            | Event::SegmentShardRequestSent { timestamp, .. }
            | Event::SegmentShardRequestReceived { timestamp, .. }
            | Event::SegmentShardsTransferred { timestamp, .. }
            | Event::ReconstructingSegments { timestamp, .. }
            | Event::SegmentReconstructionFailed { timestamp, .. }
            | Event::SegmentsReconstructed { timestamp, .. }
            | Event::SegmentVerificationFailed { timestamp, .. }
            | Event::SegmentsVerified { timestamp, .. }
            | Event::SendingSegmentRequest { timestamp, .. }
            | Event::ReceivingSegmentRequest { timestamp, .. }
            | Event::SegmentRequestFailed { timestamp, .. }
            | Event::SegmentRequestSent { timestamp, .. }
            | Event::SegmentRequestReceived { timestamp, .. }
            | Event::SegmentsTransferred { timestamp, .. }
            | Event::PreimageAnnouncementFailed { timestamp, .. }
            | Event::PreimageAnnounced { timestamp, .. }
            | Event::AnnouncedPreimageForgotten { timestamp, .. }
            | Event::SendingPreimageRequest { timestamp, .. }
            | Event::ReceivingPreimageRequest { timestamp, .. }
            | Event::PreimageRequestFailed { timestamp, .. }
            | Event::PreimageRequestSent { timestamp, .. }
            | Event::PreimageRequestReceived { timestamp, .. }
            | Event::PreimageTransferred { timestamp, .. }
            | Event::PreimageDiscarded { timestamp, .. } => *timestamp,
        }
    }
}

impl Encode for (ServiceId, AccumulateCost) {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.0.encode(buf)?;
        self.1.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        4 + self.1.encoded_size()
    }
}

impl Encode for Event {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        // All events start with timestamp and discriminator
        self.timestamp().encode(buf)?;
        (self.event_type() as u8).encode(buf)?;

        // Encode event-specific fields based on type
        match self {
            Event::Dropped {
                last_timestamp,
                num,
                ..
            } => {
                last_timestamp.encode(buf)?;
                num.encode(buf)?;
            }
            Event::Status {
                num_peers,
                num_val_peers,
                num_sync_peers,
                num_guarantees,
                num_shards,
                shards_size,
                num_preimages,
                preimages_size,
                ..
            } => {
                num_peers.encode(buf)?;
                num_val_peers.encode(buf)?;
                num_sync_peers.encode(buf)?;
                // JIP-3: num_guarantees is [u8; C] — raw bytes, no length prefix
                buf.extend_from_slice(num_guarantees);
                num_shards.encode(buf)?;
                shards_size.encode(buf)?;
                num_preimages.encode(buf)?;
                preimages_size.encode(buf)?;
            }
            Event::BestBlockChanged { slot, hash, .. } => {
                slot.encode(buf)?;
                hash.encode(buf)?;
            }
            Event::FinalizedBlockChanged { slot, hash, .. } => {
                slot.encode(buf)?;
                hash.encode(buf)?;
            }
            Event::SyncStatusChanged { synced, .. } => {
                synced.encode(buf)?;
            }
            Event::ConnectionRefused { from, .. } => {
                from.encode(buf)?;
            }
            Event::ConnectingIn { from, .. } => {
                from.encode(buf)?;
            }
            Event::ConnectInFailed {
                connecting_id,
                reason,
                ..
            } => {
                connecting_id.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::ConnectedIn {
                connecting_id,
                peer_id,
                ..
            } => {
                connecting_id.encode(buf)?;
                peer_id.encode(buf)?;
            }
            Event::ConnectingOut { to, .. } => {
                to.encode(buf)?;
            }
            Event::ConnectOutFailed {
                connecting_id,
                reason,
                ..
            } => {
                connecting_id.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::ConnectedOut { connecting_id, .. } => {
                connecting_id.encode(buf)?;
            }
            Event::Disconnected {
                peer,
                terminator,
                reason,
                ..
            } => {
                peer.encode(buf)?;
                terminator.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::PeerMisbehaved { peer, reason, .. } => {
                peer.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::Authoring { slot, parent, .. } => {
                slot.encode(buf)?;
                parent.encode(buf)?;
            }
            Event::AuthoringFailed {
                authoring_id,
                reason,
                ..
            } => {
                authoring_id.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::Authored {
                authoring_id,
                outline,
                ..
            } => {
                authoring_id.encode(buf)?;
                outline.encode(buf)?;
            }
            Event::Importing { slot, outline, .. } => {
                slot.encode(buf)?;
                outline.encode(buf)?;
            }
            Event::BlockVerificationFailed {
                importing_id,
                reason,
                ..
            } => {
                importing_id.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::BlockVerified { importing_id, .. } => {
                importing_id.encode(buf)?;
            }
            Event::BlockExecutionFailed {
                authoring_or_importing_id,
                reason,
                ..
            } => {
                authoring_or_importing_id.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::BlockExecuted {
                authoring_or_importing_id,
                accumulate_costs,
                ..
            } => {
                authoring_or_importing_id.encode(buf)?;
                accumulate_costs.encode(buf)?;
            }
            Event::BlockAnnouncementStreamOpened { peer, opener, .. } => {
                peer.encode(buf)?;
                opener.encode(buf)?;
            }
            Event::BlockAnnouncementStreamClosed {
                peer,
                closer,
                reason,
                ..
            } => {
                peer.encode(buf)?;
                closer.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::BlockAnnounced {
                peer,
                announcer,
                slot,
                hash,
                ..
            } => {
                peer.encode(buf)?;
                announcer.encode(buf)?;
                slot.encode(buf)?;
                hash.encode(buf)?;
            }
            Event::SendingBlockRequest {
                recipient,
                hash,
                direction,
                max_blocks,
                ..
            } => {
                recipient.encode(buf)?;
                hash.encode(buf)?;
                direction.encode(buf)?;
                max_blocks.encode(buf)?;
            }
            Event::ReceivingBlockRequest { sender, .. } => {
                sender.encode(buf)?;
            }
            Event::BlockRequestFailed {
                request_id, reason, ..
            } => {
                request_id.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::BlockRequestSent { request_id, .. } => {
                request_id.encode(buf)?;
            }
            Event::BlockRequestReceived {
                request_id,
                hash,
                direction,
                max_blocks,
                ..
            } => {
                request_id.encode(buf)?;
                hash.encode(buf)?;
                direction.encode(buf)?;
                max_blocks.encode(buf)?;
            }
            Event::BlockTransferred {
                request_id,
                slot,
                outline,
                last,
                ..
            } => {
                request_id.encode(buf)?;
                slot.encode(buf)?;
                outline.encode(buf)?;
                last.encode(buf)?;
            }
            Event::GeneratingTickets { epoch, .. } => {
                epoch.encode(buf)?;
            }
            Event::TicketGenerationFailed {
                generating_id,
                reason,
                ..
            } => {
                generating_id.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::TicketsGenerated {
                generating_id, ids, ..
            } => {
                generating_id.encode(buf)?;
                ids.encode(buf)?;
            }
            Event::TicketTransferFailed {
                peer,
                sender,
                from_proxy,
                reason,
                ..
            } => {
                peer.encode(buf)?;
                sender.encode(buf)?;
                from_proxy.encode(buf)?;
                reason.encode(buf)?;
            }
            Event::TicketTransferred {
                peer,
                sender,
                from_proxy,
                epoch,
                attempt,
                id,
                ..
            } => {
                peer.encode(buf)?;
                sender.encode(buf)?;
                from_proxy.encode(buf)?;
                epoch.encode(buf)?;
                attempt.encode(buf)?;
                buf.extend_from_slice(id);
            }
            Event::WorkPackageReceived {
                submission_or_share_id,
                core,
                outline,
                ..
            } => {
                submission_or_share_id.encode(buf)?;
                core.encode(buf)?;
                outline.encode(buf)?;
            }
            Event::Authorized {
                submission_or_share_id,
                cost,
                ..
            } => {
                submission_or_share_id.encode(buf)?;
                cost.encode(buf)?;
            }
            Event::Refined {
                submission_or_share_id,
                costs,
                ..
            } => {
                submission_or_share_id.encode(buf)?;
                costs.encode(buf)?;
            }
            Event::GuaranteeBuilt {
                submission_id,
                outline,
                ..
            } => {
                submission_id.encode(buf)?;
                outline.encode(buf)?;
            }
            _ => {
                todo!("Event::Encode not implemented for {:?}", self.event_type())
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        let base_size = 8 + 1; // timestamp + discriminator

        let specific_size = match self {
            Event::Dropped { .. } => 8 + 8, // last_timestamp + num (u64)
            Event::Status { num_guarantees, .. } => {
                // JIP-3: num_guarantees is [u8; C] — raw bytes, no length prefix
                4 + 4 + 4 + num_guarantees.len() + 4 + 8 + 4 + 4
            }
            Event::BestBlockChanged { .. } => 4 + 32, // slot + hash
            Event::FinalizedBlockChanged { .. } => 4 + 32,
            Event::SyncStatusChanged { .. } => 1, // bool
            Event::ConnectionRefused { from, .. } => from.encoded_size(),
            Event::ConnectingIn { from, .. } => from.encoded_size(),
            Event::ConnectInFailed { reason, .. } => 8 + reason.encoded_size(),
            Event::ConnectedIn { .. } => 8 + 32, // event_id + peer_id
            Event::ConnectingOut { to, .. } => to.encoded_size(),
            Event::ConnectOutFailed { reason, .. } => 8 + reason.encoded_size(),
            Event::ConnectedOut { .. } => 8,
            Event::Disconnected {
                terminator, reason, ..
            } => 32 + terminator.encoded_size() + reason.encoded_size(),
            Event::PeerMisbehaved { reason, .. } => 32 + reason.encoded_size(),
            Event::Authoring { .. } => 4 + 32, // slot + parent
            Event::AuthoringFailed { reason, .. } => 8 + reason.encoded_size(),
            Event::Authored { outline, .. } => 8 + outline.encoded_size(),
            Event::Importing { outline, .. } => 4 + outline.encoded_size(),
            Event::BlockVerificationFailed { reason, .. } => 8 + reason.encoded_size(),
            Event::BlockVerified { .. } => 8,
            Event::BlockExecutionFailed { reason, .. } => 8 + reason.encoded_size(),
            Event::BlockExecuted {
                accumulate_costs, ..
            } => 8 + accumulate_costs.encoded_size(),
            Event::BlockAnnouncementStreamOpened { .. } => 32 + 1,
            Event::BlockAnnouncementStreamClosed { reason, .. } => 32 + 1 + reason.encoded_size(),
            Event::BlockAnnounced { .. } => 32 + 1 + 4 + 32,
            Event::SendingBlockRequest { .. } => 32 + 32 + 1 + 4,
            Event::ReceivingBlockRequest { .. } => 32,
            Event::BlockRequestFailed { reason, .. } => 8 + reason.encoded_size(),
            Event::BlockRequestSent { .. } => 8,
            Event::BlockRequestReceived { .. } => 8 + 32 + 1 + 4,
            Event::BlockTransferred { outline, .. } => 8 + 4 + outline.encoded_size() + 1,
            Event::GeneratingTickets { .. } => 4, // epoch
            Event::TicketGenerationFailed { reason, .. } => 8 + reason.encoded_size(),
            Event::TicketsGenerated { ids, .. } => 8 + ids.encoded_size(),
            Event::TicketTransferFailed { reason, .. } => 32 + 1 + 1 + reason.encoded_size(),
            Event::TicketTransferred { .. } => 32 + 1 + 1 + 4 + 1 + 32,
            Event::WorkPackageReceived { outline, .. } => 8 + 2 + outline.encoded_size(), // submission_id + core + outline
            Event::Authorized { cost, .. } => 8 + cost.encoded_size(),
            Event::Refined { costs, .. } => 8 + costs.encoded_size(),
            Event::GuaranteeBuilt { outline, .. } => 8 + outline.encoded_size(),
            _ => todo!(
                "Event::encoded_size not implemented for {:?}",
                self.event_type()
            ),
        };

        base_size + specific_size
    }
}
