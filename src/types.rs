use crate::encoding::{variable_length_size, Encode, EncodingError};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

/// JAM Common Era (JCE) epoch starts on January 1, 2025 12:00:00 UTC (noon)
/// This is 1735732800000000 microseconds after Unix epoch
pub const JCE_EPOCH_UNIX_MICROS: i64 = 1735732800000000;

pub type Timestamp = u64;
pub type EventId = u64;
pub type PeerId = [u8; 32];
pub type Hash = [u8; 32];
pub type HeaderHash = Hash;
pub type WorkPackageHash = Hash;
pub type WorkReportHash = Hash;
pub type ErasureRoot = Hash;
pub type SegmentsRoot = Hash;
pub type Ed25519Signature = [u8; 64];
pub type Slot = u32;
pub type EpochIndex = u32;
pub type ValidatorIndex = u16;
pub type ValIndex = u16; // Alias for ValidatorIndex
pub type CoreIndex = u16;
pub type ServiceId = u32;
pub type ShardIndex = u16;
pub type Gas = u64;
pub type UnsignedGas = u64; // Gas without sign
pub type Balance = u64; // Account balance type

#[derive(Debug, Clone)]
pub struct BoundedString<const N: usize> {
    pub data: Vec<u8>,
}

// Custom Serialize implementation to output as string instead of byte array
impl<const N: usize> Serialize for BoundedString<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match std::str::from_utf8(&self.data) {
            Ok(s) => serializer.serialize_str(s),
            Err(_) => {
                // If not valid UTF-8, serialize as byte array for debugging
                serializer.serialize_bytes(&self.data)
            }
        }
    }
}

// Custom Deserialize implementation to accept both string and byte array
impl<'de, const N: usize> Deserialize<'de> for BoundedString<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoundedStringVisitor<const N: usize>;

        impl<'de, const N: usize> serde::de::Visitor<'de> for BoundedStringVisitor<N> {
            type Value = BoundedString<N>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or byte array")
            }

            fn visit_str<E>(self, value: &str) -> Result<BoundedString<N>, E>
            where
                E: serde::de::Error,
            {
                if value.len() > N {
                    return Err(E::custom(format!(
                        "string too long: {} > {}",
                        value.len(),
                        N
                    )));
                }
                Ok(BoundedString {
                    data: value.as_bytes().to_vec(),
                })
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<BoundedString<N>, E>
            where
                E: serde::de::Error,
            {
                if value.len() > N {
                    return Err(E::custom(format!(
                        "bytes too long: {} > {}",
                        value.len(),
                        N
                    )));
                }
                Ok(BoundedString {
                    data: value.to_vec(),
                })
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<BoundedString<N>, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut data = Vec::new();
                while let Some(byte) = seq.next_element()? {
                    data.push(byte);
                }
                if data.len() > N {
                    return Err(serde::de::Error::custom(format!(
                        "sequence too long: {} > {}",
                        data.len(),
                        N
                    )));
                }
                Ok(BoundedString { data })
            }
        }

        deserializer.deserialize_any(BoundedStringVisitor::<N>)
    }
}

impl<const N: usize> BoundedString<N> {
    pub fn new(s: &str) -> Result<Self, EncodingError> {
        let bytes = s.as_bytes();
        if bytes.len() > N {
            return Err(EncodingError::StringTooLong {
                length: bytes.len(),
                max: N,
            });
        }
        Ok(Self {
            data: bytes.to_vec(),
        })
    }

    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.data)
    }
}

impl<const N: usize> Encode for BoundedString<N> {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.data.encode(buf)
    }

    fn encoded_size(&self) -> usize {
        variable_length_size(self.data.len() as u64) + self.data.len()
    }
}

pub type Reason = BoundedString<128>;
pub type NodeName = BoundedString<32>;
pub type NodeVersion = BoundedString<32>;
pub type NodeNote = BoundedString<512>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PeerAddress {
    pub ipv6: [u8; 16],
    pub port: u16,
}

impl Encode for PeerAddress {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.ipv6.encode(buf)?;
        self.port.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        16 + 2
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConnectionSide {
    Local = 0,
    Remote = 1,
}

impl Encode for ConnectionSide {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        (*self as u8).encode(buf)
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSummary {
    pub size_bytes: u32,
    pub hash: HeaderHash, // Added: hash is part of BlockOutline
    pub num_tickets: u32,
    pub num_preimages: u32,
    pub total_preimages_size: u32,
    pub num_guarantees: u32,
    pub num_assurances: u32,
    pub num_dispute_verdicts: u32,
}

impl Encode for BlockSummary {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.size_bytes.encode(buf)?;
        self.hash.encode(buf)?; // Added
        self.num_tickets.encode(buf)?;
        self.num_preimages.encode(buf)?;
        self.total_preimages_size.encode(buf)?;
        self.num_guarantees.encode(buf)?;
        self.num_assurances.encode(buf)?;
        self.num_dispute_verdicts.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        4 + 32 + (6 * 4) // size + hash + 6 u32 fields
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecCost {
    pub gas_used: Gas,
    pub elapsed_ns: u64,
}

impl Encode for ExecCost {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.gas_used.encode(buf)?;
        self.elapsed_ns.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        16
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsAuthorizedCost {
    pub total: ExecCost,
    pub load_ns: u64,        // Added: time to load and compile code
    pub host_call: ExecCost, // Changed from host_calls (plural) to host_call (singular)
}

impl Encode for IsAuthorizedCost {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.total.encode(buf)?;
        self.load_ns.encode(buf)?; // Added
        self.host_call.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.total.encoded_size() + 8 + self.host_call.encoded_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefineHostCallCost {
    pub lookup: ExecCost,
    pub vm: ExecCost,
    pub mem: ExecCost,
    pub invoke: ExecCost,
    pub other: ExecCost,
}

impl Encode for RefineHostCallCost {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.lookup.encode(buf)?;
        self.vm.encode(buf)?;
        self.mem.encode(buf)?;
        self.invoke.encode(buf)?;
        self.other.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        5 * self.lookup.encoded_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefineCost {
    pub total: ExecCost,
    pub load_ns: u64,                  // Added: time to load and compile code
    pub host_call: RefineHostCallCost, // Changed: nested struct
}

impl Encode for RefineCost {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.total.encode(buf)?;
        self.load_ns.encode(buf)?; // Added
        self.host_call.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.total.encoded_size() + 8 + self.host_call.encoded_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccumulateHostCallCost {
    pub state: ExecCost,
    pub lookup: ExecCost,
    pub preimage: ExecCost,
    pub service: ExecCost,
    pub transfer: ExecCost,
    pub transfer_dest_gas: Gas,
    pub other: ExecCost,
}

impl Encode for AccumulateHostCallCost {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.state.encode(buf)?;
        self.lookup.encode(buf)?;
        self.preimage.encode(buf)?;
        self.service.encode(buf)?;
        self.transfer.encode(buf)?;
        self.transfer_dest_gas.encode(buf)?;
        self.other.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        6 * self.state.encoded_size() + 8
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccumulateCost {
    pub num_calls: u32,     // Changed from num_accumulate_calls
    pub num_transfers: u32, // Changed from num_transfers_processed
    pub num_items: u32,     // Changed from num_items_accumulated
    pub total: ExecCost,
    pub load_ns: u64,                      // Added: time to load and compile code
    pub host_call: AccumulateHostCallCost, // Changed: nested struct
}

impl Encode for AccumulateCost {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.num_calls.encode(buf)?;
        self.num_transfers.encode(buf)?;
        self.num_items.encode(buf)?;
        self.total.encode(buf)?;
        self.load_ns.encode(buf)?; // Added
        self.host_call.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        12 + self.total.encoded_size() + 8 + self.host_call.encoded_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RootIdentifier {
    SegmentsRoot(SegmentsRoot),
    WorkPackageHash(WorkPackageHash),
}

impl Encode for RootIdentifier {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        match self {
            RootIdentifier::SegmentsRoot(root) => root.encode(buf),
            RootIdentifier::WorkPackageHash(hash) => hash.encode(buf),
        }
    }

    fn encoded_size(&self) -> usize {
        32
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportSpec {
    pub root_identifier: RootIdentifier,
    pub export_index: u16,
}

// Work-package and work-report related types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItemSummary {
    pub service_id: ServiceId,
    pub payload_size: u32,
    pub refine_gas_limit: Gas,
    pub accumulate_gas_limit: Gas,
    pub sum_of_extrinsic_lengths: u32,
    pub imports: Vec<ImportSpec>,
    pub num_exported_segments: u16,
}

impl Encode for WorkItemSummary {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.service_id.encode(buf)?;
        self.payload_size.encode(buf)?;
        self.refine_gas_limit.encode(buf)?;
        self.accumulate_gas_limit.encode(buf)?;
        self.sum_of_extrinsic_lengths.encode(buf)?;
        self.imports.encode(buf)?;
        self.num_exported_segments.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        4 + 4 + 8 + 8 + 4 + self.imports.encoded_size() + 2
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkPackageSummary {
    pub work_package_size: u32,
    pub work_package_hash: WorkPackageHash,
    pub anchor: HeaderHash,
    pub lookup_anchor_slot: Slot,
    pub prerequisites: Vec<WorkPackageHash>,
    pub work_items: Vec<WorkItemSummary>,
}

impl Encode for WorkPackageSummary {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.work_package_size.encode(buf)?;
        self.work_package_hash.encode(buf)?;
        self.anchor.encode(buf)?;
        self.lookup_anchor_slot.encode(buf)?;
        self.prerequisites.encode(buf)?;
        self.work_items.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        4 + 32 + 32 + 4 + self.prerequisites.encoded_size() + self.work_items.encoded_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkReportSummary {
    pub work_report_hash: WorkReportHash,
    pub bundle_size: u32,
    pub erasure_root: ErasureRoot,
    pub segments_root: SegmentsRoot,
}

impl Encode for WorkReportSummary {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.work_report_hash.encode(buf)?;
        self.bundle_size.encode(buf)?;
        self.erasure_root.encode(buf)?;
        self.segments_root.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        32 + 4 + 32 + 32
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuaranteeSummary {
    pub work_report_hash: WorkReportHash,
    pub slot: Slot,
    pub guarantors: Vec<ValidatorIndex>,
}

impl Encode for GuaranteeSummary {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.work_report_hash.encode(buf)?;
        self.slot.encode(buf)?;
        self.guarantors.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        32 + 4 + self.guarantors.encoded_size()
    }
}

impl Encode for ImportSpec {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.root_identifier.encode(buf)?;
        let index = match &self.root_identifier {
            RootIdentifier::SegmentsRoot(_) => self.export_index,
            RootIdentifier::WorkPackageHash(_) => self.export_index | 0x8000,
        };
        index.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.root_identifier.encoded_size() + 2
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum GuaranteeDiscardReason {
    PackageReportedOnChain = 0, // Work-package reported on-chain
    ReplacedByBetter = 1,
    CannotReportOnChain = 2, // Fixed: was "TooOld", should be "Cannot be reported on-chain"
    TooManyGuarantees = 3,
    Other = 4,
}

impl Encode for GuaranteeDiscardReason {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        (*self as u8).encode(buf)
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum AnnouncedPreimageForgetReason {
    ProvidedOnChain = 0,     // Added: JIP-3 value 0
    NotRequestedOnChain = 1, // Fixed: was 0, now 1
    FailedToAcquire = 2,     // Fixed: was 1, now 2
    TooManyAnnounced = 3,    // Fixed: was 2, now 3
    BadLength = 4,           // Added: JIP-3 value 4
    Other = 5,               // Fixed: was 3, now 5
}

impl Encode for AnnouncedPreimageForgetReason {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        (*self as u8).encode(buf)
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum PreimageDiscardReason {
    ProvidedOnChain = 0,     // Added: JIP-3 value 0
    NotRequestedOnChain = 1, // Fixed: was "NoLongerRequested", now correct
    TooManyPreimages = 2,    // Fixed: was value 1, now 2
    Other = 3,               // Fixed: was value 2, now 3
}

impl Encode for PreimageDiscardReason {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        (*self as u8).encode(buf)
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

// Additional types for full event specification compliance

pub type TicketId = Hash;
pub type TicketAttempt = u8;
pub type SegmentTreeRoot = Hash;
pub type ImportSegmentId = u16;
pub type CoreCount = u16;

// Outline types (aliases for existing Summary types)
pub type BlockOutline = BlockSummary;
pub type WorkPackageOutline = WorkPackageSummary;
pub type WorkReportOutline = WorkReportSummary;
pub type GuaranteeOutline = GuaranteeSummary;

// Cost types
pub type AccumulateCosts = Vec<(ServiceId, AccumulateCost)>;
pub type RefineCosts = Vec<RefineCost>;

// Generic bounded vec type for specification compliance
pub type BoundedVec<T, const N: usize> = Vec<T>;
pub type FixedVec<T, const N: usize> = Vec<T>;

// Constants for bounded vecs
pub const MAX_IMPORT_SEGMENTS: usize = 128;
pub const MAX_IMPORTS: usize = 128;
pub const TICKETS_ATTEMPTS_NUMBER: usize = 128;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum BlockRequestDirection {
    Ascending = 0,
    Descending = 1,
}

impl Encode for BlockRequestDirection {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        (*self as u8).encode(buf)
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

impl crate::decoder::Decode for BlockRequestDirection {
    fn decode(buf: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::decoder::DecodingError> {
        let value = crate::decoder::Decode::decode(buf)?;
        match value {
            0 => Ok(BlockRequestDirection::Ascending),
            1 => Ok(BlockRequestDirection::Descending),
            _ => Err(crate::decoder::DecodingError::InvalidEnumValue(value)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDetails {
    pub peer_id: PeerId,
    pub peer_address: PeerAddress,
}

impl Encode for PeerDetails {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.peer_id.encode(buf)?;
        self.peer_address.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        32 + self.peer_address.encoded_size()
    }
}

impl crate::decoder::Decode for PeerDetails {
    fn decode(buf: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::decoder::DecodingError> {
        Ok(Self {
            peer_id: crate::decoder::Decode::decode(buf)?,
            peer_address: crate::decoder::Decode::decode(buf)?,
        })
    }
}

// Alias for compatibility with spec
pub type PeerAddr = PeerAddress;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailabilityStatement {
    pub anchor: HeaderHash, // Fixed: was work_report_hash
    pub bitfield: Vec<u8>,  // Fixed: Availability bitfield, ceil(core_count / 8) bytes
}

impl Encode for AvailabilityStatement {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.anchor.encode(buf)?;
        self.bitfield.encode(buf)?; // Encoded with variable-length prefix
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        32 + variable_length_size(self.bitfield.len() as u64) + self.bitfield.len()
    }
}

impl crate::decoder::Decode for AvailabilityStatement {
    fn decode(buf: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::decoder::DecodingError> {
        Ok(Self {
            anchor: crate::decoder::Decode::decode(buf)?,
            bitfield: crate::decoder::Decode::decode(buf)?,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum ReconstructionKind {
    /// Non-trivial reconstruction, involving at least some recovery shards
    NonTrivial = 0,
    /// Trivial reconstruction, using only original-data shards
    Trivial = 1,
}

impl Encode for ReconstructionKind {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        (*self as u8).encode(buf)
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

impl crate::decoder::Decode for ReconstructionKind {
    fn decode(buf: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::decoder::DecodingError> {
        let value = crate::decoder::Decode::decode(buf)?;
        match value {
            0 => Ok(ReconstructionKind::NonTrivial), // Fixed
            1 => Ok(ReconstructionKind::Trivial),    // Fixed
            _ => Err(crate::decoder::DecodingError::InvalidEnumValue(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ProtocolParameters {
    pub deposit_per_item: Balance,
    pub deposit_per_byte: Balance,
    pub deposit_per_account: Balance,
    pub core_count: CoreIndex,
    pub min_turnaround_period: Slot,
    pub epoch_period: Slot,
    pub max_accumulate_gas: UnsignedGas,
    pub max_is_authorized_gas: UnsignedGas,
    pub max_refine_gas: UnsignedGas,
    pub block_gas_limit: UnsignedGas,
    pub recent_block_count: u16,
    pub max_work_items: u16,
    pub max_dependencies: u16,
    pub max_tickets_per_block: u16,
    pub max_lookup_anchor_age: Slot,
    pub tickets_attempts_number: u16,
    pub auth_window: u16,
    pub slot_period_sec: u16,
    pub auth_queue_len: u16,
    pub rotation_period: u16,
    pub max_extrinsics: u16,
    pub availability_timeout: u16,
    pub val_count: ValIndex,
    pub max_authorizer_code_size: u32,
    pub max_input: u32,
    pub max_service_code_size: u32,
    pub basic_piece_len: u32,
    pub max_imports: u32,
    pub segment_piece_count: u32,
    pub max_report_elective_data: u32,
    pub transfer_memo_size: u32,
    pub max_exports: u32,
    pub epoch_tail_start: Slot,
}

impl Encode for ProtocolParameters {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        self.deposit_per_item.encode(buf)?;
        self.deposit_per_byte.encode(buf)?;
        self.deposit_per_account.encode(buf)?;
        self.core_count.encode(buf)?;
        self.min_turnaround_period.encode(buf)?;
        self.epoch_period.encode(buf)?;
        self.max_accumulate_gas.encode(buf)?;
        self.max_is_authorized_gas.encode(buf)?;
        self.max_refine_gas.encode(buf)?;
        self.block_gas_limit.encode(buf)?;
        self.recent_block_count.encode(buf)?;
        self.max_work_items.encode(buf)?;
        self.max_dependencies.encode(buf)?;
        self.max_tickets_per_block.encode(buf)?;
        self.max_lookup_anchor_age.encode(buf)?;
        self.tickets_attempts_number.encode(buf)?;
        self.auth_window.encode(buf)?;
        self.slot_period_sec.encode(buf)?;
        self.auth_queue_len.encode(buf)?;
        self.rotation_period.encode(buf)?;
        self.max_extrinsics.encode(buf)?;
        self.availability_timeout.encode(buf)?;
        self.val_count.encode(buf)?;
        self.max_authorizer_code_size.encode(buf)?;
        self.max_input.encode(buf)?;
        self.max_service_code_size.encode(buf)?;
        self.basic_piece_len.encode(buf)?;
        self.max_imports.encode(buf)?;
        self.segment_piece_count.encode(buf)?;
        self.max_report_elective_data.encode(buf)?;
        self.transfer_memo_size.encode(buf)?;
        self.max_exports.encode(buf)?;
        self.epoch_tail_start.encode(buf)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        // 3 Balance (8 each) + 4 UnsignedGas (8 each) + 4 Slot (4 each) + 13 u16 (2 each) + 9 u32 (4 each)
        3 * 8 + 4 * 8 + 4 * 4 + 13 * 2 + 9 * 4
    }
}
