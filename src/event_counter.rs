//! In-memory pre-aggregation of high-volume events into per-group count tables.
//!
//! Events are counted in a concurrent DashMap keyed by (30s bucket, node_id, event_type, dims).
//! Every 5s the map is drained and flushed to PostgreSQL via COPY BINARY.
//! All query paths do SUM(event_count) GROUP BY, so append-only writes are correct.

use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use sqlx::PgPool;
use tracing::warn;

use crate::enricher::EnrichedFields;
use crate::events::{Event, EventType};

/// 30-second bucket alignment in microseconds.
const BUCKET_MICROS: i64 = 30_000_000;

/// Key for pre-aggregated event counts.
#[derive(Hash, Eq, PartialEq, Clone)]
pub struct CountKey {
    pub bucket: i64, // 30s-aligned unix_micros
    pub node_id: Arc<str>,
    pub event_type: i16,
    pub core: Option<i16>,
    pub slot: Option<i32>,
    pub reason: Option<String>,
    pub service_id: Option<i32>,
    pub kind: Option<i16>,
    pub from_proxy: Option<bool>,
    pub epoch: Option<i32>,
}

pub type EventCounter = Arc<DashMap<CountKey, i64>>;

pub fn new_event_counter() -> EventCounter {
    Arc::new(DashMap::new())
}

/// Event types that are pre-aggregated (not written to ingested_raw_events).
pub const PRE_AGGREGATED_TYPES: &[u16] = &[
    // block_distribution (60-68)
    60, 61, 62, 63, 64, 65, 66, 67, 68,
    // tickets (83-84)
    83, 84,
    // guarantee_sending (106-108)
    106, 107, 108,
    // guarantee_receiving (110-113)
    110, 111, 112, 113,
    // shards (120-125)
    120, 121, 122, 123, 124, 125,
    // assurances (126-131)
    126, 127, 128, 129, 130, 131,
    // bundles (140-153)
    140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
    // segments (160-178)
    160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178,
    // preimages (190-199)
    190, 191, 192, 193, 194, 195, 196, 197, 198, 199,
];

pub fn is_pre_aggregated(et: u16) -> bool {
    PRE_AGGREGATED_TYPES.contains(&et)
}

/// Extract reason string from events that carry a Reason (BoundedString<128>) field.
fn extract_reason(event: &Event) -> Option<String> {
    match event {
        // block_distribution
        Event::BlockAnnouncementStreamClosed { reason, .. }
        | Event::BlockRequestFailed { reason, .. }
        // tickets
        | Event::TicketTransferFailed { reason, .. }
        // guarantee_sending
        | Event::GuaranteeSendFailed { reason, .. }
        // guarantee_receiving
        | Event::GuaranteeReceiveFailed { reason, .. }
        // shards
        | Event::ShardRequestFailed { reason, .. }
        // assurances
        | Event::AssuranceSendFailed { reason, .. }
        | Event::AssuranceReceiveFailed { reason, .. }
        // bundles
        | Event::BundleShardRequestFailed { reason, .. }
        | Event::BundleRequestFailed { reason, .. }
        // segments
        | Event::SegmentShardRequestFailed { reason, .. }
        | Event::SegmentReconstructionFailed { reason, .. }
        | Event::SegmentVerificationFailed { reason, .. }
        | Event::SegmentRequestFailed { reason, .. }
        // preimages
        | Event::PreimageAnnouncementFailed { reason, .. }
        | Event::PreimageRequestFailed { reason, .. } => {
            reason.as_str().ok().map(|s| s.to_string())
        }
        _ => None,
    }
}

/// Record an event in the in-memory counter. Called after enrichment.
pub fn record_event(
    counter: &EventCounter,
    event: &Event,
    enriched: &EnrichedFields,
    unix_micros: i64,
    node_id: &Arc<str>,
    et: EventType,
) {
    let bucket = unix_micros - (unix_micros % BUCKET_MICROS);
    let mut key = CountKey {
        bucket,
        node_id: Arc::clone(node_id),
        event_type: et as i16,
        core: None,
        slot: None,
        reason: None,
        service_id: None,
        kind: None,
        from_proxy: None,
        epoch: None,
    };

    match et {
        // block_distribution: slot + reason
        EventType::BlockAnnounced | EventType::BlockTransferred => {
            key.slot = enriched.slot.map(|s| s as i32);
        }
        EventType::BlockAnnouncementStreamClosed | EventType::BlockRequestFailed => {
            key.reason = extract_reason(event);
        }

        // guarantee_sending: core from enricher
        EventType::SendingGuarantee | EventType::GuaranteeSent => {
            key.core = enriched.core.map(|c| c as i16);
        }
        EventType::GuaranteeSendFailed => {
            key.core = enriched.core.map(|c| c as i16);
            key.reason = extract_reason(event);
        }

        // guarantee_receiving: slot from outline (Part C enricher fix)
        EventType::GuaranteeReceived => {
            key.slot = enriched.slot.map(|s| s as i32);
        }
        EventType::GuaranteeDiscarded => {
            key.slot = enriched.slot.map(|s| s as i32);
            if let Event::GuaranteeDiscarded { reason, .. } = event {
                key.reason = Some(format!("{reason:?}({n})", n = *reason as u8));
            }
        }

        // tickets
        EventType::TicketTransferFailed => {
            if let Event::TicketTransferFailed { from_proxy, .. } = event {
                key.from_proxy = Some(*from_proxy);
                key.reason = extract_reason(event);
            }
        }
        EventType::TicketTransferred => {
            if let Event::TicketTransferred {
                from_proxy, epoch, ..
            } = event
            {
                key.from_proxy = Some(*from_proxy);
                key.epoch = Some(*epoch as i32);
            }
        }

        // segments: core from enricher
        EventType::WorkPackageHashMapped
        | EventType::SegmentsRootMapped
        | EventType::SegmentsReconstructed
        | EventType::SegmentsVerified
        | EventType::SendingSegmentShardRequest
        | EventType::ReceivingSegmentShardRequest
        | EventType::SegmentShardRequestSent
        | EventType::SegmentShardRequestReceived
        | EventType::SegmentShardsTransferred
        | EventType::SendingSegmentRequest
        | EventType::ReceivingSegmentRequest
        | EventType::SegmentRequestSent
        | EventType::SegmentRequestReceived
        | EventType::SegmentsTransferred => {
            key.core = enriched.core.map(|c| c as i16);
        }
        EventType::ReconstructingSegments => {
            key.core = enriched.core.map(|c| c as i16);
            if let Event::ReconstructingSegments { kind, .. } = event {
                key.kind = Some(*kind as i16);
            }
        }
        // segment failures: core + reason
        EventType::SegmentShardRequestFailed
        | EventType::SegmentReconstructionFailed
        | EventType::SegmentVerificationFailed
        | EventType::SegmentRequestFailed => {
            key.core = enriched.core.map(|c| c as i16);
            key.reason = extract_reason(event);
        }

        // bundles: kind on ReconstructingBundle
        EventType::ReconstructingBundle => {
            if let Event::ReconstructingBundle { kind, .. } = event {
                key.kind = Some(*kind as i16);
            }
        }
        // bundle failures: reason
        EventType::BundleShardRequestFailed | EventType::BundleRequestFailed => {
            key.reason = extract_reason(event);
        }

        // preimages with service_id
        EventType::PreimageAnnounced => {
            if let Event::PreimageAnnounced { service, .. } = event {
                key.service_id = Some(*service as i32);
            }
        }
        EventType::AnnouncedPreimageForgotten => {
            if let Event::AnnouncedPreimageForgotten {
                service, reason, ..
            } = event
            {
                key.service_id = Some(*service as i32);
                key.reason = Some(format!("{reason:?}({n})", n = *reason as u8));
            }
        }
        EventType::PreimageDiscarded => {
            if let Event::PreimageDiscarded { reason, .. } = event {
                key.reason = Some(format!("{reason:?}({n})", n = *reason as u8));
            }
        }

        // failure events with reason but no special dimensions
        EventType::GuaranteeReceiveFailed
        | EventType::ShardRequestFailed
        | EventType::AssuranceSendFailed
        | EventType::AssuranceReceiveFailed
        | EventType::PreimageAnnouncementFailed
        | EventType::PreimageRequestFailed => {
            key.reason = extract_reason(event);
        }

        _ => {} // pure count, no extra dimensions
    }

    *counter.entry(key).or_insert(0) += 1;
}

/// Map event_type to its count table name.
fn event_type_to_table(et: i16) -> &'static str {
    match et {
        60..=68 => "block_distribution_counts",
        83..=84 => "ticket_counts",
        106..=108 => "guarantee_sending_counts",
        110..=113 => "guarantee_receiving_counts",
        120..=125 => "shard_counts",
        126..=131 => "assurance_counts",
        140..=153 => "bundle_counts",
        160..=178 => "segment_counts",
        190..=199 => "preimage_counts",
        _ => unreachable!("not a pre-aggregated event type: {}", et),
    }
}

/// Drain the counter and flush to PostgreSQL via INSERT batches.
pub async fn flush_event_counter(counter: &EventCounter, pool: &PgPool) {
    if counter.is_empty() {
        return;
    }

    // Drain atomically: collect keys, then remove each.
    let keys: Vec<CountKey> = counter.iter().map(|e| e.key().clone()).collect();
    let mut entries: Vec<(CountKey, i64)> = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some((k, v)) = counter.remove(&key) {
            entries.push((k, v));
        }
    }

    if entries.is_empty() {
        return;
    }

    // Partition by table
    let mut groups: std::collections::HashMap<&str, Vec<(&CountKey, i64)>> =
        std::collections::HashMap::new();
    for (key, count) in &entries {
        let table = event_type_to_table(key.event_type);
        groups.entry(table).or_default().push((key, *count));
    }

    for (table_name, rows) in &groups {
        if let Err(e) = copy_to_count_table(pool, table_name, rows).await {
            warn!("Failed to flush {table_name}: {e}");
        }
    }
}

/// COPY BINARY rows into a count table.
async fn copy_to_count_table(
    pool: &PgPool,
    table: &str,
    rows: &[(&CountKey, i64)],
) -> Result<(), sqlx::Error> {
    // Determine columns based on table
    let columns = match table {
        "block_distribution_counts" => "bucket, node_id, event_type, event_count, slot, reason",
        "ticket_counts" => "bucket, node_id, event_type, event_count, reason, from_proxy, epoch",
        "guarantee_sending_counts" => "bucket, node_id, event_type, event_count, core, reason",
        "guarantee_receiving_counts" => "bucket, node_id, event_type, event_count, slot, reason",
        "shard_counts" => "bucket, node_id, event_type, event_count, reason",
        "assurance_counts" => "bucket, node_id, event_type, event_count, reason",
        "bundle_counts" => "bucket, node_id, event_type, event_count, reason, kind",
        "segment_counts" => "bucket, node_id, event_type, event_count, core, reason, kind",
        "preimage_counts" => "bucket, node_id, event_type, event_count, reason, service_id",
        _ => unreachable!(),
    };

    let copy_sql = format!(
        "COPY {table} ({columns}) FROM STDIN WITH (FORMAT binary)"
    );

    let mut buf = BytesMut::with_capacity(rows.len() * 128);

    // Binary COPY header: 11-byte signature + 4-byte flags + 4-byte extension
    buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    buf.put_i32(0); // flags
    buf.put_i32(0); // header extension length

    for (key, count) in rows {
        // key.bucket is already unix_micros (JCE_EPOCH + jce_timestamp), aligned to 30s
        let timestamp = chrono::DateTime::from_timestamp_micros(key.bucket)
            .unwrap_or_else(chrono::Utc::now);
        // PostgreSQL epoch is 2000-01-01, convert from unix epoch
        let pg_micros = (timestamp - pg_epoch()).num_microseconds().unwrap_or(0);

        match table {
            "block_distribution_counts" => {
                buf.put_i16(6); // 6 columns
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_i32(&mut buf, key.slot);
                write_nullable_text(&mut buf, key.reason.as_deref());
            }
            "ticket_counts" => {
                buf.put_i16(7); // 7 columns
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_text(&mut buf, key.reason.as_deref());
                write_nullable_bool(&mut buf, key.from_proxy);
                write_nullable_i32(&mut buf, key.epoch);
            }
            "guarantee_sending_counts" => {
                buf.put_i16(6);
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_i16(&mut buf, key.core);
                write_nullable_text(&mut buf, key.reason.as_deref());
            }
            "guarantee_receiving_counts" => {
                buf.put_i16(6);
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_i32(&mut buf, key.slot);
                write_nullable_text(&mut buf, key.reason.as_deref());
            }
            "shard_counts" | "assurance_counts" => {
                buf.put_i16(5);
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_text(&mut buf, key.reason.as_deref());
            }
            "bundle_counts" => {
                buf.put_i16(6);
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_text(&mut buf, key.reason.as_deref());
                write_nullable_i16(&mut buf, key.kind);
            }
            "segment_counts" => {
                buf.put_i16(7);
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_i16(&mut buf, key.core);
                write_nullable_text(&mut buf, key.reason.as_deref());
                write_nullable_i16(&mut buf, key.kind);
            }
            "preimage_counts" => {
                buf.put_i16(6);
                write_timestamptz(&mut buf, pg_micros);
                write_text(&mut buf, &key.node_id);
                write_i16(&mut buf, key.event_type);
                write_i64(&mut buf, *count);
                write_nullable_text(&mut buf, key.reason.as_deref());
                write_nullable_i32(&mut buf, key.service_id);
            }
            _ => unreachable!(),
        }
    }

    // Binary COPY trailer
    buf.put_i16(-1);

    let mut conn = pool.acquire().await?;
    let mut copy_in = conn.copy_in_raw(&copy_sql).await?;
    copy_in.send(buf.as_ref()).await?;
    copy_in.finish().await?;
    Ok(())
}

fn pg_epoch() -> chrono::DateTime<chrono::Utc> {
    chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

// ── Binary COPY helpers ──────────────────────────────────────────────────

fn write_timestamptz(buf: &mut BytesMut, pg_micros: i64) {
    buf.put_i32(8);
    buf.put_i64(pg_micros);
}

fn write_text(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_i32(bytes.len() as i32);
    buf.extend_from_slice(bytes);
}

fn write_i16(buf: &mut BytesMut, v: i16) {
    buf.put_i32(2);
    buf.put_i16(v);
}

fn write_i64(buf: &mut BytesMut, v: i64) {
    buf.put_i32(8);
    buf.put_i64(v);
}

fn write_nullable_i16(buf: &mut BytesMut, v: Option<i16>) {
    match v {
        Some(v) => {
            buf.put_i32(2);
            buf.put_i16(v);
        }
        None => buf.put_i32(-1),
    }
}

fn write_nullable_i32(buf: &mut BytesMut, v: Option<i32>) {
    match v {
        Some(v) => {
            buf.put_i32(4);
            buf.put_i32(v);
        }
        None => buf.put_i32(-1),
    }
}

fn write_nullable_text(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => write_text(buf, s),
        None => buf.put_i32(-1),
    }
}

fn write_nullable_bool(buf: &mut BytesMut, v: Option<bool>) {
    match v {
        Some(v) => {
            buf.put_i32(1);
            buf.put_u8(if v { 1 } else { 0 });
        }
        None => buf.put_i32(-1),
    }
}
