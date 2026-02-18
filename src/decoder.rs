use crate::events::*;
use crate::types::*;
use bytes::Buf;
use std::io::Cursor;
use thiserror::Error;

/// Helper function to create a hex dump for debugging
fn hex_dump(data: &[u8], max_bytes: usize) -> String {
    let end = std::cmp::min(data.len(), max_bytes);
    let hex_part: String = data[..end]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");
    if data.len() > max_bytes {
        format!("{} ... ({} total bytes)", hex_part, data.len())
    } else {
        hex_part
    }
}

#[derive(Error, Debug)]
pub enum DecodingError {
    #[error("Insufficient data: needed {needed}, available {available}")]
    InsufficientData { needed: usize, available: usize },
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("String too long: {length} > {max}")]
    StringTooLong { length: usize, max: usize },
    #[error("Invalid discriminator: {0}")]
    InvalidDiscriminator(u8),
    #[error("Invalid boolean value: {0}")]
    InvalidBool(u8),
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(u32),
    #[error("Invalid enum value: {0}")]
    InvalidEnumValue(u8),
}

pub trait Decode: Sized {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError>;
}

impl Decode for u8 {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        if buf.remaining() < 1 {
            return Err(DecodingError::InsufficientData {
                needed: 1,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u8())
    }
}

impl Decode for u16 {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        if buf.remaining() < 2 {
            return Err(DecodingError::InsufficientData {
                needed: 2,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u16_le())
    }
}

impl Decode for u32 {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        if buf.remaining() < 4 {
            return Err(DecodingError::InsufficientData {
                needed: 4,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u32_le())
    }
}

impl Decode for u64 {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        if buf.remaining() < 8 {
            return Err(DecodingError::InsufficientData {
                needed: 8,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u64_le())
    }
}

impl Decode for bool {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let val = u8::decode(buf)?;
        match val {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodingError::InvalidBool(val)),
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let discriminator = u8::decode(buf)?;
        match discriminator {
            0 => Ok(None),
            1 => Ok(Some(T::decode(buf)?)),
            _ => Err(DecodingError::InvalidDiscriminator(discriminator)),
        }
    }
}

impl<T: Decode, const N: usize> Decode for [T; N] {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let mut arr = Vec::with_capacity(N);
        for _ in 0..N {
            arr.push(T::decode(buf)?);
        }
        arr.try_into().ok().ok_or(DecodingError::InsufficientData {
            needed: N,
            available: 0,
        })
    }
}

impl Decode for Vec<u8> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        if buf.remaining() < len {
            return Err(DecodingError::InsufficientData {
                needed: len,
                available: buf.remaining(),
            });
        }
        let mut vec = vec![0u8; len];
        buf.copy_to_slice(&mut vec);
        Ok(vec)
    }
}

pub fn decode_variable_length(buf: &mut Cursor<&[u8]>) -> Result<u64, DecodingError> {
    let mut value = 0u64;
    let mut shift = 0;
    let start_pos = buf.position();
    let available = buf.remaining();

    loop {
        if buf.remaining() < 1 {
            return Err(DecodingError::InsufficientData {
                needed: 1,
                available: buf.remaining(),
            });
        }

        let byte = buf.get_u8();
        value |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(DecodingError::InvalidDiscriminator(byte));
        }

        // Sanity check: if we've read more than 9 bytes for a u64, something is wrong
        if buf.position() - start_pos > 9 {
            let data_slice = &buf.get_ref()[start_pos as usize..];
            tracing::warn!("Variable length decode reading too many bytes. Available: {}, Value so far: {}, Data: {}", 
                available, value, hex_dump(data_slice, 20));
            return Err(DecodingError::InvalidDiscriminator(byte));
        }
    }

    // Additional sanity check for unreasonable values
    if value > 1_000_000 {
        // 1MB seems like a reasonable upper bound for string lengths
        let data_slice = &buf.get_ref()[start_pos as usize..];
        tracing::warn!(
            "Variable length decoded unreasonably large value: {} (available bytes: {}), Data: {}",
            value,
            available,
            hex_dump(data_slice, 20)
        );
        return Err(DecodingError::StringTooLong {
            length: value as usize,
            max: 1_000_000,
        });
    }

    Ok(value)
}

impl<const N: usize> Decode for BoundedString<N> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        // First decode the length
        let len = decode_variable_length(buf)? as usize;

        // Enhanced sanity checks with better error recovery
        if len > 100_000 {
            // Extremely large length indicates corruption
            return Err(DecodingError::StringTooLong {
                length: len,
                max: N,
            });
        }

        if buf.remaining() < len {
            return Err(DecodingError::InsufficientData {
                needed: len,
                available: buf.remaining(),
            });
        }

        // For problematic events, try to recover gracefully
        if len > N {
            if len < 1000 {
                // Moderately oversized - truncate to fit
                tracing::warn!("Truncating oversized string from {} to {} bytes", len, N);
                let mut data = vec![0u8; N];
                buf.copy_to_slice(&mut data);
                // Skip remaining bytes
                buf.advance(len - N);

                // Validate and recover UTF-8
                return match std::str::from_utf8(&data) {
                    Ok(_) => Ok(BoundedString { data }),
                    Err(_) => {
                        let recovered = String::from_utf8_lossy(&data);
                        Ok(BoundedString {
                            data: recovered.as_bytes().to_vec(),
                        })
                    }
                };
            }
            return Err(DecodingError::StringTooLong {
                length: len,
                max: N,
            });
        }

        let mut data = vec![0u8; len];
        buf.copy_to_slice(&mut data);

        // Validate UTF-8 and recover if possible
        match std::str::from_utf8(&data) {
            Ok(_) => Ok(BoundedString { data }),
            Err(e) => {
                tracing::warn!("Invalid UTF-8 in BoundedString: {}", e);

                // Attempt to recover by replacing invalid sequences
                let recovered = String::from_utf8_lossy(&data);
                if recovered.len() <= N {
                    Ok(BoundedString {
                        data: recovered.as_bytes().to_vec(),
                    })
                } else {
                    // If recovery results in oversized string, truncate
                    let truncated = recovered.chars().take(N).collect::<String>();
                    Ok(BoundedString {
                        data: truncated.as_bytes().to_vec(),
                    })
                }
            }
        }
    }
}

impl Decode for PeerAddress {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(PeerAddress {
            ipv6: <[u8; 16]>::decode(buf)?,
            port: u16::decode(buf)?,
        })
    }
}

impl Decode for ConnectionSide {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        match u8::decode(buf)? {
            0 => Ok(ConnectionSide::Local),
            1 => Ok(ConnectionSide::Remote),
            v => Err(DecodingError::InvalidDiscriminator(v)),
        }
    }
}

impl Decode for BlockSummary {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(BlockSummary {
            size_bytes: u32::decode(buf)?,
            hash: HeaderHash::decode(buf)?, // Added: hash is part of BlockOutline
            num_tickets: u32::decode(buf)?,
            num_preimages: u32::decode(buf)?,
            total_preimages_size: u32::decode(buf)?,
            num_guarantees: u32::decode(buf)?,
            num_assurances: u32::decode(buf)?,
            num_dispute_verdicts: u32::decode(buf)?,
        })
    }
}

impl Decode for ExecCost {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(ExecCost {
            gas_used: Gas::decode(buf)?,
            elapsed_ns: u64::decode(buf)?,
        })
    }
}

impl Decode for AccumulateHostCallCost {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(AccumulateHostCallCost {
            state: ExecCost::decode(buf)?,
            lookup: ExecCost::decode(buf)?,
            preimage: ExecCost::decode(buf)?,
            service: ExecCost::decode(buf)?,
            transfer: ExecCost::decode(buf)?,
            transfer_dest_gas: Gas::decode(buf)?,
            other: ExecCost::decode(buf)?,
        })
    }
}

impl Decode for AccumulateCost {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(AccumulateCost {
            num_calls: u32::decode(buf)?,     // Changed from num_accumulate_calls
            num_transfers: u32::decode(buf)?, // Changed from num_transfers_processed
            num_items: u32::decode(buf)?,     // Changed from num_items_accumulated
            total: ExecCost::decode(buf)?,
            load_ns: u64::decode(buf)?,                      // Added
            host_call: AccumulateHostCallCost::decode(buf)?, // Changed: nested struct
        })
    }
}

impl Decode for IsAuthorizedCost {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(IsAuthorizedCost {
            total: ExecCost::decode(buf)?,
            load_ns: u64::decode(buf)?,        // Added
            host_call: ExecCost::decode(buf)?, // Changed from host_calls to host_call
        })
    }
}

impl Decode for RefineHostCallCost {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(RefineHostCallCost {
            lookup: ExecCost::decode(buf)?,
            vm: ExecCost::decode(buf)?,
            mem: ExecCost::decode(buf)?,
            invoke: ExecCost::decode(buf)?,
            other: ExecCost::decode(buf)?,
        })
    }
}

impl Decode for RefineCost {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(RefineCost {
            total: ExecCost::decode(buf)?,
            load_ns: u64::decode(buf)?,                  // Added
            host_call: RefineHostCallCost::decode(buf)?, // Changed: nested struct
        })
    }
}

impl Decode for Vec<RefineCost> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(RefineCost::decode(buf)?);
        }
        Ok(vec)
    }
}

impl Decode for GuaranteeDiscardReason {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let value = u8::decode(buf)?;
        match value {
            0 => Ok(GuaranteeDiscardReason::PackageReportedOnChain),
            1 => Ok(GuaranteeDiscardReason::ReplacedByBetter),
            2 => Ok(GuaranteeDiscardReason::CannotReportOnChain), // Fixed
            3 => Ok(GuaranteeDiscardReason::TooManyGuarantees),
            4 => Ok(GuaranteeDiscardReason::Other),
            _ => Err(DecodingError::InvalidEnumValue(value)),
        }
    }
}

impl Decode for AnnouncedPreimageForgetReason {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let value = u8::decode(buf)?;
        match value {
            0 => Ok(AnnouncedPreimageForgetReason::ProvidedOnChain), // Fixed
            1 => Ok(AnnouncedPreimageForgetReason::NotRequestedOnChain), // Fixed
            2 => Ok(AnnouncedPreimageForgetReason::FailedToAcquire), // Fixed
            3 => Ok(AnnouncedPreimageForgetReason::TooManyAnnounced), // Fixed
            4 => Ok(AnnouncedPreimageForgetReason::BadLength),       // Added
            5 => Ok(AnnouncedPreimageForgetReason::Other),           // Fixed
            _ => Err(DecodingError::InvalidEnumValue(value)),
        }
    }
}

impl Decode for PreimageDiscardReason {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let value = u8::decode(buf)?;
        match value {
            0 => Ok(PreimageDiscardReason::ProvidedOnChain), // Fixed
            1 => Ok(PreimageDiscardReason::NotRequestedOnChain), // Fixed
            2 => Ok(PreimageDiscardReason::TooManyPreimages), // Fixed
            3 => Ok(PreimageDiscardReason::Other),           // Fixed
            _ => Err(DecodingError::InvalidEnumValue(value)),
        }
    }
}

// Note: Vec<TicketId> is the same as Vec<Hash>, which is Vec<[u8; 32]>
// It's already implemented as Vec<WorkPackageHash> since they're all Hash aliases

// Note: Vec<u16> is already implemented as Vec<ValidatorIndex> since ValidatorIndex = u16
// Note: Vec<ImportSegmentId> is the same as Vec<u16>, so uses the ValidatorIndex implementation

impl Decode for Vec<(ImportSegmentId, ShardIndex)> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            let segment_id = u16::decode(buf)?;
            let shard_index = u16::decode(buf)?;
            vec.push((segment_id, shard_index));
        }
        Ok(vec)
    }
}

impl Decode for (ServiceId, AccumulateCost) {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok((ServiceId::decode(buf)?, AccumulateCost::decode(buf)?))
    }
}

impl Decode for Vec<(ServiceId, AccumulateCost)> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(<(ServiceId, AccumulateCost)>::decode(buf)?);
        }
        Ok(vec)
    }
}

impl Decode for NodeInformation {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let version = u8::decode(buf)?;
        if version != TELEMETRY_PROTOCOL_VERSION {
            return Err(DecodingError::InvalidDiscriminator(version));
        }

        Ok(NodeInformation {
            params: ProtocolParameters::decode(buf)?,
            genesis: HeaderHash::decode(buf)?,
            details: PeerDetails::decode(buf)?,
            flags: u32::decode(buf)?,
            implementation_name: NodeName::decode(buf)?,
            implementation_version: NodeVersion::decode(buf)?,
            gp_version: BoundedString::decode(buf)?,
            additional_info: NodeNote::decode(buf)?,
        })
    }
}

impl Decode for WorkItemSummary {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(WorkItemSummary {
            service_id: ServiceId::decode(buf)?,
            payload_size: u32::decode(buf)?,
            refine_gas_limit: Gas::decode(buf)?,
            accumulate_gas_limit: Gas::decode(buf)?,
            sum_of_extrinsic_lengths: u32::decode(buf)?,
            imports: Vec::<ImportSpec>::decode(buf)?,
            num_exported_segments: u16::decode(buf)?,
        })
    }
}

impl Decode for WorkPackageSummary {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(WorkPackageSummary {
            work_package_size: u32::decode(buf)?,
            work_package_hash: WorkPackageHash::decode(buf)?,
            anchor: HeaderHash::decode(buf)?,
            lookup_anchor_slot: Slot::decode(buf)?,
            prerequisites: Vec::<WorkPackageHash>::decode(buf)?,
            work_items: Vec::<WorkItemSummary>::decode(buf)?,
        })
    }
}

impl Decode for WorkReportSummary {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(WorkReportSummary {
            work_report_hash: WorkReportHash::decode(buf)?,
            bundle_size: u32::decode(buf)?,
            erasure_root: ErasureRoot::decode(buf)?,
            segments_root: SegmentsRoot::decode(buf)?,
        })
    }
}

impl Decode for GuaranteeSummary {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(GuaranteeSummary {
            work_report_hash: WorkReportHash::decode(buf)?,
            slot: Slot::decode(buf)?,
            guarantors: Vec::<ValidatorIndex>::decode(buf)?,
        })
    }
}

impl Decode for ImportSpec {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let root_bytes = <[u8; 32]>::decode(buf)?;
        let index = u16::decode(buf)?;

        let (root_identifier, export_index) = if index & 0x8000 != 0 {
            (RootIdentifier::WorkPackageHash(root_bytes), index & 0x7FFF)
        } else {
            (RootIdentifier::SegmentsRoot(root_bytes), index)
        };

        Ok(ImportSpec {
            root_identifier,
            export_index,
        })
    }
}

impl Decode for Vec<ImportSpec> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(ImportSpec::decode(buf)?);
        }
        Ok(vec)
    }
}

impl Decode for Vec<WorkPackageHash> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(WorkPackageHash::decode(buf)?);
        }
        Ok(vec)
    }
}

impl Decode for Vec<WorkItemSummary> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(WorkItemSummary::decode(buf)?);
        }
        Ok(vec)
    }
}

impl Decode for Vec<ValidatorIndex> {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        let len = decode_variable_length(buf)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(ValidatorIndex::decode(buf)?);
        }
        Ok(vec)
    }
}

impl Event {
    /// Decode an event from the wire format, using `core_count` (C) from
    /// the node's ProtocolParameters for fixed-size [u8; C] arrays (JIP-3).
    pub fn decode_event(buf: &mut Cursor<&[u8]>, core_count: u16) -> Result<Self, DecodingError> {
        let timestamp = Timestamp::decode(buf)?;
        let discriminator = u8::decode(buf)?;

        if ![10, 11, 12, 13].contains(&discriminator) {
            tracing::trace!(
                "Decoding event with discriminator: {}, remaining bytes: {}",
                discriminator,
                buf.remaining()
            );
        }

        match discriminator {
            0 => Ok(Event::Dropped {
                timestamp,
                last_timestamp: Timestamp::decode(buf)?,
                num: u64::decode(buf)?,
            }),
            10 => {
                let num_peers = u32::decode(buf)?;
                let num_val_peers = u32::decode(buf)?;
                let num_sync_peers = u32::decode(buf)?;

                // JIP-3: num_guarantees is [u8; C] — fixed-size array, no length prefix
                let c = core_count as usize;
                let mut num_guarantees = vec![0u8; c];
                if buf.remaining() < c {
                    return Err(DecodingError::InsufficientData {
                        needed: c,
                        available: buf.remaining(),
                    });
                }
                buf.copy_to_slice(&mut num_guarantees);

                Ok(Event::Status {
                    timestamp,
                    num_peers,
                    num_val_peers,
                    num_sync_peers,
                    num_guarantees,
                    num_shards: u32::decode(buf)?,
                    shards_size: u64::decode(buf)?,
                    num_preimages: u32::decode(buf)?,
                    preimages_size: u32::decode(buf)?,
                })
            }
            11 => Ok(Event::BestBlockChanged {
                timestamp,
                slot: Slot::decode(buf)?,
                hash: HeaderHash::decode(buf)?,
            }),
            12 => Ok(Event::FinalizedBlockChanged {
                timestamp,
                slot: Slot::decode(buf)?,
                hash: HeaderHash::decode(buf)?,
            }),
            13 => Ok(Event::SyncStatusChanged {
                timestamp,
                synced: bool::decode(buf)?,
            }),
            20 => Ok(Event::ConnectionRefused {
                timestamp,
                from: PeerAddress::decode(buf)?,
            }),
            21 => Ok(Event::ConnectingIn {
                timestamp,
                from: PeerAddress::decode(buf)?,
            }),
            22 => Ok(Event::ConnectInFailed {
                timestamp,
                connecting_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            23 => Ok(Event::ConnectedIn {
                timestamp,
                connecting_id: EventId::decode(buf)?,
                peer_id: PeerId::decode(buf)?,
            }),
            24 => Ok(Event::ConnectingOut {
                timestamp,
                to: PeerDetails {
                    peer_id: PeerId::decode(buf)?,
                    peer_address: PeerAddress::decode(buf)?,
                },
            }),
            25 => Ok(Event::ConnectOutFailed {
                timestamp,
                connecting_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            26 => Ok(Event::ConnectedOut {
                timestamp,
                connecting_id: EventId::decode(buf)?,
            }),
            27 => Ok(Event::Disconnected {
                timestamp,
                peer: PeerId::decode(buf)?,
                terminator: Option::<ConnectionSide>::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            28 => Ok(Event::PeerMisbehaved {
                timestamp,
                peer: PeerId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            40 => Ok(Event::Authoring {
                timestamp,
                slot: Slot::decode(buf)?,
                parent: HeaderHash::decode(buf)?,
            }),
            41 => Ok(Event::AuthoringFailed {
                timestamp,
                authoring_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            42 => Ok(Event::Authored {
                timestamp,
                authoring_id: EventId::decode(buf)?,
                outline: BlockSummary::decode(buf)?, // BlockSummary includes hash internally
            }),
            43 => Ok(Event::Importing {
                timestamp,
                slot: Slot::decode(buf)?,
                outline: BlockSummary::decode(buf)?, // BlockSummary includes hash internally
            }),
            44 => Ok(Event::BlockVerificationFailed {
                timestamp,
                importing_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            45 => Ok(Event::BlockVerified {
                timestamp,
                importing_id: EventId::decode(buf)?,
            }),
            46 => Ok(Event::BlockExecutionFailed {
                timestamp,
                authoring_or_importing_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            47 => Ok(Event::BlockExecuted {
                timestamp,
                authoring_or_importing_id: EventId::decode(buf)?,
                accumulate_costs: Vec::<(ServiceId, AccumulateCost)>::decode(buf)?,
            }),
            60 => Ok(Event::BlockAnnouncementStreamOpened {
                timestamp,
                peer: PeerId::decode(buf)?,
                opener: ConnectionSide::decode(buf)?,
            }),
            61 => Ok(Event::BlockAnnouncementStreamClosed {
                timestamp,
                peer: PeerId::decode(buf)?,
                closer: ConnectionSide::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            62 => Ok(Event::BlockAnnounced {
                timestamp,
                peer: PeerId::decode(buf)?,
                announcer: ConnectionSide::decode(buf)?,
                slot: Slot::decode(buf)?,
                hash: HeaderHash::decode(buf)?,
            }),
            63 => Ok(Event::SendingBlockRequest {
                timestamp,
                recipient: PeerId::decode(buf)?,
                hash: HeaderHash::decode(buf)?,
                direction: BlockRequestDirection::decode(buf)?,
                max_blocks: u32::decode(buf)?,
            }),
            64 => Ok(Event::ReceivingBlockRequest {
                timestamp,
                sender: PeerId::decode(buf)?,
            }),
            65 => Ok(Event::BlockRequestFailed {
                timestamp,
                request_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            66 => Ok(Event::BlockRequestSent {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            67 => Ok(Event::BlockRequestReceived {
                timestamp,
                request_id: EventId::decode(buf)?,
                hash: HeaderHash::decode(buf)?,
                direction: BlockRequestDirection::decode(buf)?,
                max_blocks: u32::decode(buf)?,
            }),
            68 => Ok(Event::BlockTransferred {
                timestamp,
                request_id: EventId::decode(buf)?,
                slot: Slot::decode(buf)?,
                outline: BlockSummary::decode(buf)?,
                last: bool::decode(buf)?,
            }),
            80 => Ok(Event::GeneratingTickets {
                timestamp,
                epoch: EpochIndex::decode(buf)?,
            }),
            81 => Ok(Event::TicketGenerationFailed {
                timestamp,
                generating_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            82 => Ok(Event::TicketsGenerated {
                timestamp,
                generating_id: EventId::decode(buf)?,
                ids: Vec::<TicketId>::decode(buf)?,
            }),
            83 => Ok(Event::TicketTransferFailed {
                timestamp,
                peer: PeerId::decode(buf)?,
                sender: ConnectionSide::decode(buf)?,
                from_proxy: bool::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            84 => Ok(Event::TicketTransferred {
                timestamp,
                peer: PeerId::decode(buf)?,
                sender: ConnectionSide::decode(buf)?,
                from_proxy: bool::decode(buf)?,
                epoch: EpochIndex::decode(buf)?,
                attempt: u8::decode(buf)?,
                id: {
                    let mut output = [0u8; 32];
                    if buf.remaining() < 32 {
                        return Err(DecodingError::InsufficientData {
                            needed: 32,
                            available: buf.remaining(),
                        });
                    }
                    buf.copy_to_slice(&mut output);
                    output
                },
            }),

            // Work package events (90-104)
            90 => Ok(Event::WorkPackageSubmission {
                timestamp,
                builder: PeerId::decode(buf)?,
                bundle: bool::decode(buf)?,
            }),
            91 => Ok(Event::WorkPackageBeingShared {
                timestamp,
                primary: PeerId::decode(buf)?,
            }),
            92 => Ok(Event::WorkPackageFailed {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            93 => Ok(Event::DuplicateWorkPackage {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
                core: CoreIndex::decode(buf)?,
                hash: WorkPackageHash::decode(buf)?,
            }),
            94 => Ok(Event::WorkPackageReceived {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
                core: CoreIndex::decode(buf)?,
                outline: WorkPackageSummary::decode(buf)?,
            }),
            95 => Ok(Event::Authorized {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
                cost: IsAuthorizedCost::decode(buf)?,
            }),
            96 => Ok(Event::ExtrinsicDataReceived {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
            }),
            97 => Ok(Event::ImportsReceived {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
            }),
            98 => Ok(Event::SharingWorkPackage {
                timestamp,
                submission_id: EventId::decode(buf)?,
                secondary: PeerId::decode(buf)?,
            }),
            99 => Ok(Event::WorkPackageSharingFailed {
                timestamp,
                submission_id: EventId::decode(buf)?,
                secondary: PeerId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            100 => Ok(Event::BundleSent {
                timestamp,
                submission_id: EventId::decode(buf)?,
                secondary: PeerId::decode(buf)?,
            }),
            101 => Ok(Event::Refined {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
                costs: Vec::<RefineCost>::decode(buf)?,
            }),
            102 => Ok(Event::WorkReportBuilt {
                timestamp,
                submission_or_share_id: EventId::decode(buf)?,
                outline: WorkReportSummary::decode(buf)?,
            }),
            103 => Ok(Event::WorkReportSignatureSent {
                timestamp,
                share_id: EventId::decode(buf)?,
            }),
            104 => Ok(Event::WorkReportSignatureReceived {
                timestamp,
                submission_id: EventId::decode(buf)?,
                secondary: PeerId::decode(buf)?,
            }),

            // Guarantee events (105-113)
            105 => Ok(Event::GuaranteeBuilt {
                timestamp,
                submission_id: EventId::decode(buf)?,
                outline: GuaranteeSummary::decode(buf)?,
            }),
            106 => Ok(Event::SendingGuarantee {
                timestamp,
                built_id: EventId::decode(buf)?,
                recipient: PeerId::decode(buf)?,
            }),
            107 => Ok(Event::GuaranteeSendFailed {
                timestamp,
                sending_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            108 => Ok(Event::GuaranteeSent {
                timestamp,
                sending_id: EventId::decode(buf)?,
            }),
            109 => Ok(Event::GuaranteesDistributed {
                timestamp,
                submission_id: EventId::decode(buf)?,
            }),
            110 => Ok(Event::ReceivingGuarantee {
                timestamp,
                sender: PeerId::decode(buf)?,
            }),
            111 => Ok(Event::GuaranteeReceiveFailed {
                timestamp,
                receiving_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            112 => Ok(Event::GuaranteeReceived {
                timestamp,
                receiving_id: EventId::decode(buf)?,
                outline: GuaranteeSummary::decode(buf)?,
            }),
            113 => Ok(Event::GuaranteeDiscarded {
                timestamp,
                outline: GuaranteeSummary::decode(buf)?,
                reason: GuaranteeDiscardReason::decode(buf)?,
            }),

            // Shard request events (120-125)
            120 => Ok(Event::SendingShardRequest {
                timestamp,
                guarantor: PeerId::decode(buf)?,
                erasure_root: ErasureRoot::decode(buf)?,
                shard: ShardIndex::decode(buf)?,
            }),
            121 => Ok(Event::ReceivingShardRequest {
                timestamp,
                assurer: PeerId::decode(buf)?,
            }),
            122 => Ok(Event::ShardRequestFailed {
                timestamp,
                request_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            123 => Ok(Event::ShardRequestSent {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            124 => Ok(Event::ShardRequestReceived {
                timestamp,
                request_id: EventId::decode(buf)?,
                erasure_root: ErasureRoot::decode(buf)?,
                shard: ShardIndex::decode(buf)?,
            }),
            125 => Ok(Event::ShardsTransferred {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),

            // Assurance distribution (126-131)
            126 => {
                // JIP-3: AvailabilityStatement is anchor (32 bytes) +
                // bitfield [u8; ceil(C/8)] — fixed-size, no length prefix
                let anchor = HeaderHash::decode(buf)?;
                let bitfield_len = (core_count as usize).div_ceil(8);
                if buf.remaining() < bitfield_len {
                    return Err(DecodingError::InsufficientData {
                        needed: bitfield_len,
                        available: buf.remaining(),
                    });
                }
                let mut bitfield = vec![0u8; bitfield_len];
                buf.copy_to_slice(&mut bitfield);
                Ok(Event::DistributingAssurance {
                    timestamp,
                    statement: AvailabilityStatement { anchor, bitfield },
                })
            }
            127 => Ok(Event::AssuranceSendFailed {
                timestamp,
                distributing_id: EventId::decode(buf)?,
                recipient: PeerId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            128 => Ok(Event::AssuranceSent {
                timestamp,
                distributing_id: EventId::decode(buf)?,
                recipient: PeerId::decode(buf)?,
            }),
            129 => Ok(Event::AssuranceDistributed {
                timestamp,
                distributing_id: EventId::decode(buf)?,
            }),
            130 => Ok(Event::AssuranceReceiveFailed {
                timestamp,
                sender: PeerId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            131 => Ok(Event::AssuranceReceived {
                timestamp,
                sender: PeerId::decode(buf)?,
                anchor: HeaderHash::decode(buf)?,
            }),

            // Bundle shard requests (140-153)
            140 => Ok(Event::SendingBundleShardRequest {
                timestamp,
                audit_id: EventId::decode(buf)?,
                assurer: PeerId::decode(buf)?,
                shard: ShardIndex::decode(buf)?,
            }),
            141 => Ok(Event::ReceivingBundleShardRequest {
                timestamp,
                auditor: PeerId::decode(buf)?,
            }),
            142 => Ok(Event::BundleShardRequestFailed {
                timestamp,
                request_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            143 => Ok(Event::BundleShardRequestSent {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            144 => Ok(Event::BundleShardRequestReceived {
                timestamp,
                request_id: EventId::decode(buf)?,
                erasure_root: ErasureRoot::decode(buf)?,
                shard: ShardIndex::decode(buf)?,
            }),
            145 => Ok(Event::BundleShardTransferred {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            146 => Ok(Event::ReconstructingBundle {
                timestamp,
                audit_id: EventId::decode(buf)?,
                kind: ReconstructionKind::decode(buf)?,
            }),
            147 => Ok(Event::BundleReconstructed {
                timestamp,
                audit_id: EventId::decode(buf)?,
            }),
            148 => Ok(Event::SendingBundleRequest {
                timestamp,
                audit_id: EventId::decode(buf)?,
                guarantor: PeerId::decode(buf)?,
            }),
            149 => Ok(Event::ReceivingBundleRequest {
                timestamp,
                auditor: PeerId::decode(buf)?,
            }),
            150 => Ok(Event::BundleRequestFailed {
                timestamp,
                request_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            151 => Ok(Event::BundleRequestSent {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            152 => Ok(Event::BundleRequestReceived {
                timestamp,
                request_id: EventId::decode(buf)?,
                erasure_root: ErasureRoot::decode(buf)?,
            }),
            153 => Ok(Event::BundleTransferred {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),

            // Work package hash mapping (160-161)
            160 => Ok(Event::WorkPackageHashMapped {
                timestamp,
                submission_id: EventId::decode(buf)?,
                work_package_hash: WorkPackageHash::decode(buf)?,
                segments_root: SegmentTreeRoot::decode(buf)?,
            }),
            161 => Ok(Event::SegmentsRootMapped {
                timestamp,
                submission_id: EventId::decode(buf)?,
                segments_root: SegmentTreeRoot::decode(buf)?,
                erasure_root: ErasureRoot::decode(buf)?,
            }),

            // Segment shard requests (162-167)
            162 => Ok(Event::SendingSegmentShardRequest {
                timestamp,
                submission_id: EventId::decode(buf)?,
                assurer: PeerId::decode(buf)?,
                proofs: bool::decode(buf)?,
                shards: Vec::<(ImportSegmentId, ShardIndex)>::decode(buf)?,
            }),
            163 => Ok(Event::ReceivingSegmentShardRequest {
                timestamp,
                sender: PeerId::decode(buf)?,
                proofs: bool::decode(buf)?,
            }),
            164 => Ok(Event::SegmentShardRequestFailed {
                timestamp,
                request_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            165 => Ok(Event::SegmentShardRequestSent {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            166 => Ok(Event::SegmentShardRequestReceived {
                timestamp,
                request_id: EventId::decode(buf)?,
                num: u16::decode(buf)?,
            }),
            167 => Ok(Event::SegmentShardsTransferred {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),

            // Segment reconstruction (168-172)
            168 => Ok(Event::ReconstructingSegments {
                timestamp,
                submission_id: EventId::decode(buf)?,
                segments: Vec::<ImportSegmentId>::decode(buf)?,
                kind: ReconstructionKind::decode(buf)?,
            }),
            169 => Ok(Event::SegmentReconstructionFailed {
                timestamp,
                reconstructing_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            170 => Ok(Event::SegmentsReconstructed {
                timestamp,
                reconstructing_id: EventId::decode(buf)?,
            }),
            171 => Ok(Event::SegmentVerificationFailed {
                timestamp,
                submission_id: EventId::decode(buf)?,
                segments: Vec::<u16>::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            172 => Ok(Event::SegmentsVerified {
                timestamp,
                submission_id: EventId::decode(buf)?,
                segments: Vec::<u16>::decode(buf)?,
            }),

            // Segment requests (173-178)
            173 => Ok(Event::SendingSegmentRequest {
                timestamp,
                submission_id: EventId::decode(buf)?,
                prev_guarantor: PeerId::decode(buf)?,
                segments: Vec::<u16>::decode(buf)?,
            }),
            174 => Ok(Event::ReceivingSegmentRequest {
                timestamp,
                guarantor: PeerId::decode(buf)?,
            }),
            175 => Ok(Event::SegmentRequestFailed {
                timestamp,
                request_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            176 => Ok(Event::SegmentRequestSent {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            177 => Ok(Event::SegmentRequestReceived {
                timestamp,
                request_id: EventId::decode(buf)?,
                num: u16::decode(buf)?,
            }),
            178 => Ok(Event::SegmentsTransferred {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),

            // Preimage events (190-199)
            190 => Ok(Event::PreimageAnnouncementFailed {
                timestamp,
                peer: PeerId::decode(buf)?,
                announcer: ConnectionSide::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            191 => Ok(Event::PreimageAnnounced {
                timestamp,
                peer: PeerId::decode(buf)?,
                announcer: ConnectionSide::decode(buf)?,
                service: ServiceId::decode(buf)?,
                hash: Hash::decode(buf)?,
                length: u32::decode(buf)?,
            }),
            192 => Ok(Event::AnnouncedPreimageForgotten {
                timestamp,
                service: ServiceId::decode(buf)?,
                hash: Hash::decode(buf)?,
                length: u32::decode(buf)?,
                reason: AnnouncedPreimageForgetReason::decode(buf)?,
            }),
            193 => Ok(Event::SendingPreimageRequest {
                timestamp,
                recipient: PeerId::decode(buf)?,
                hash: Hash::decode(buf)?,
            }),
            194 => Ok(Event::ReceivingPreimageRequest {
                timestamp,
                sender: PeerId::decode(buf)?,
            }),
            195 => Ok(Event::PreimageRequestFailed {
                timestamp,
                request_id: EventId::decode(buf)?,
                reason: Reason::decode(buf)?,
            }),
            196 => Ok(Event::PreimageRequestSent {
                timestamp,
                request_id: EventId::decode(buf)?,
            }),
            197 => Ok(Event::PreimageRequestReceived {
                timestamp,
                request_id: EventId::decode(buf)?,
                hash: Hash::decode(buf)?,
            }),
            198 => Ok(Event::PreimageTransferred {
                timestamp,
                request_id: EventId::decode(buf)?,
                length: u32::decode(buf)?,
            }),
            199 => Ok(Event::PreimageDiscarded {
                timestamp,
                hash: Hash::decode(buf)?,
                length: u32::decode(buf)?,
                reason: PreimageDiscardReason::decode(buf)?,
            }),

            _ => Err(DecodingError::InvalidDiscriminator(discriminator)),
        }
    }
}

impl Decode for Event {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        // Fallback: decode with core_count=0 (Status/DistributingAssurance will
        // produce empty arrays). Use decode_event() directly with the real
        // core_count for production decoding.
        Event::decode_event(buf, 0)
    }
}

pub fn decode_message_frame(data: &[u8]) -> Result<(u32, &[u8]), DecodingError> {
    if data.len() < 4 {
        return Err(DecodingError::InsufficientData {
            needed: 4,
            available: data.len(),
        });
    }

    let size_bytes = &data[..4];
    let msg_size = u32::from_le_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]]);

    if msg_size > 256_000 {
        return Err(DecodingError::MessageTooLarge(msg_size));
    }

    if data.len() < 4 + msg_size as usize {
        return Err(DecodingError::InsufficientData {
            needed: 4 + msg_size as usize,
            available: data.len(),
        });
    }

    Ok((msg_size, &data[4..4 + msg_size as usize]))
}

impl Decode for ProtocolParameters {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self, DecodingError> {
        Ok(ProtocolParameters {
            deposit_per_item: Balance::decode(buf)?,
            deposit_per_byte: Balance::decode(buf)?,
            deposit_per_account: Balance::decode(buf)?,
            core_count: CoreIndex::decode(buf)?,
            min_turnaround_period: Slot::decode(buf)?,
            epoch_period: Slot::decode(buf)?,
            max_accumulate_gas: UnsignedGas::decode(buf)?,
            max_is_authorized_gas: UnsignedGas::decode(buf)?,
            max_refine_gas: UnsignedGas::decode(buf)?,
            block_gas_limit: UnsignedGas::decode(buf)?,
            recent_block_count: u16::decode(buf)?,
            max_work_items: u16::decode(buf)?,
            max_dependencies: u16::decode(buf)?,
            max_tickets_per_block: u16::decode(buf)?,
            max_lookup_anchor_age: Slot::decode(buf)?,
            tickets_attempts_number: u16::decode(buf)?,
            auth_window: u16::decode(buf)?,
            slot_period_sec: u16::decode(buf)?,
            auth_queue_len: u16::decode(buf)?,
            rotation_period: u16::decode(buf)?,
            max_extrinsics: u16::decode(buf)?,
            availability_timeout: u16::decode(buf)?,
            val_count: ValIndex::decode(buf)?,
            max_authorizer_code_size: u32::decode(buf)?,
            max_input: u32::decode(buf)?,
            max_service_code_size: u32::decode(buf)?,
            basic_piece_len: u32::decode(buf)?,
            max_imports: u32::decode(buf)?,
            segment_piece_count: u32::decode(buf)?,
            max_report_elective_data: u32::decode(buf)?,
            transfer_memo_size: u32::decode(buf)?,
            max_exports: u32::decode(buf)?,
            epoch_tail_start: Slot::decode(buf)?,
        })
    }
}
