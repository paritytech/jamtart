//! On-chain statistics subscription engine.
//!
//! Subscribes to JAM node RPC for `Statistics` updates and finalized block
//! notifications, stores per-block core/service/validator stats in TimescaleDB,
//! and handles fork cleanup on finalization.
//!
//! Supports multiple RPC URLs (comma-separated `JAM_RPC_URL`) for redundancy.
//! Each URL gets its own independent connection with dedup via in-memory LRU.

use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use jam_std_common::{BlockDesc, ChainSubUpdate, NodeExt, RpcClient as _, Statistics};
use jam_types::{HeaderHash, JAM_COMMON_ERA};
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

const MAX_REQUEST_SIZE: u32 = 100 * 1024 * 1024;
const MAX_RESPONSE_SIZE: u32 = 100 * 1024 * 1024;

/// Convert a JAM slot number to a UTC timestamp.
///
/// Uses `JAM_COMMON_ERA` (the JAM epoch start, Unix seconds) from `jam_types`.
/// Formula: `UNIX_EPOCH + JAM_COMMON_ERA + slot * slot_period_secs`
pub fn slot_to_timestamp(slot: u32, slot_period_secs: u16) -> DateTime<Utc> {
    let epoch_secs = JAM_COMMON_ERA + slot as u64 * slot_period_secs as u64;
    Utc.timestamp_opt(epoch_secs as i64, 0)
        .single()
        .unwrap_or_else(Utc::now)
}

/// Ingestion counters for periodic logging.
struct IngestCounters {
    blocks_ingested: u64,
    forks_detected: u64,
    last_log: std::time::Instant,
    last_finalized_slot: u32,
}

impl IngestCounters {
    fn new() -> Self {
        Self {
            blocks_ingested: 0,
            forks_detected: 0,
            last_log: std::time::Instant::now(),
            last_finalized_slot: 0,
        }
    }

    fn maybe_log(&mut self) {
        if self.last_log.elapsed() >= Duration::from_secs(60) {
            info!(
                "Onchain stats: ingested {} blocks, finalized up to slot {}, {} forks seen",
                self.blocks_ingested, self.last_finalized_slot, self.forks_detected
            );
            self.last_log = std::time::Instant::now();
        }
    }
}

/// Spawn on-chain statistics ingestion tasks for all configured RPC URLs.
///
/// Each URL gets an independent connection loop that subscribes to both
/// statistics updates and finalized block notifications.
pub fn spawn_onchain_ingestion(
    urls: Vec<String>,
    pool: PgPool,
    slot_period_secs: u16,
) -> Vec<tokio::task::JoinHandle<()>> {
    let pool = Arc::new(pool);
    let counters = Arc::new(RwLock::new(IngestCounters::new()));

    urls.into_iter()
        .enumerate()
        .map(|(idx, url)| {
            let pool = Arc::clone(&pool);
            let counters = Arc::clone(&counters);
            tokio::spawn(async move {
                loop {
                    if let Err(e) =
                        run_connection(&url, &pool, slot_period_secs, &counters, idx).await
                    {
                        error!(
                            "Onchain stats connection {} ({}) error: {}, reconnecting in 5s...",
                            idx, url, e
                        );
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            })
        })
        .collect()
}

/// Run a single RPC connection: subscribe to stats + finalization, process both.
async fn run_connection(
    url: &str,
    pool: &PgPool,
    slot_period_secs: u16,
    counters: &RwLock<IngestCounters>,
    conn_idx: usize,
) -> anyhow::Result<()> {
    let uri = url::Url::parse(url)?;
    let client = WsClientBuilder::default()
        .max_request_size(MAX_REQUEST_SIZE)
        .max_response_size(MAX_RESPONSE_SIZE)
        .build(&uri)
        .await?;

    info!(
        "Onchain stats connection {} established to {}",
        conn_idx, url
    );

    // LRU dedup: track recently seen (slot, header_hash) pairs
    let mut seen: HashSet<(u32, [u8; 32])> = HashSet::new();
    const MAX_SEEN: usize = 200;

    let mut stats_sub = NodeExt::subscribe_statistics(&client, false).await?;
    let mut finalized_sub = client.subscribe_finalized_block().await?;

    info!(
        "Onchain stats connection {} subscribed to statistics + finalization",
        conn_idx
    );

    loop {
        tokio::select! {
            Some(result) = stats_sub.next() => {
                match result {
                    Ok(ChainSubUpdate { header_hash, slot, value: statistics }) => {
                        let hash_bytes = header_hash.0;
                        if seen.contains(&(slot, hash_bytes)) {
                            continue;
                        }

                        let ts = slot_to_timestamp(slot, slot_period_secs);

                        // Fork detection: check if we have rows for this slot with a different hash
                        if let Err(e) = handle_fork_detection(pool, slot, &header_hash, ts).await {
                            warn!("Fork detection error at slot {}: {}", slot, e);
                        }

                        // Insert stats
                        if let Err(e) = insert_block_stats(pool, ts, slot, &header_hash, &statistics).await {
                            error!("Failed to insert onchain stats for slot {}: {}", slot, e);
                            continue;
                        }

                        // Update dedup set
                        seen.insert((slot, hash_bytes));
                        if seen.len() > MAX_SEEN {
                            // Remove oldest entries (approximate: just clear half)
                            let to_remove: Vec<_> = seen.iter().take(MAX_SEEN / 2).cloned().collect();
                            for key in to_remove {
                                seen.remove(&key);
                            }
                        }

                        {
                            let mut c = counters.write().await;
                            c.blocks_ingested += 1;
                            c.maybe_log();
                        }

                        debug!(
                            "Ingested slot {} hash {:.16}: {} cores, {} services, {} validators",
                            slot,
                            hex::encode(hash_bytes),
                            statistics.cores.len(),
                            statistics.services.len(),
                            statistics.vals_curr.len(),
                        );
                    }
                    Err(e) => {
                        warn!("Statistics subscription error on connection {}: {}", conn_idx, e);
                    }
                }
            }
            Some(result) = finalized_sub.next() => {
                match result {
                    Ok(finalized) => {
                        if let Err(e) = handle_finalization(pool, &client, &finalized, slot_period_secs, counters).await {
                            error!("Finalization handling error at slot {}: {}", finalized.slot, e);
                        }
                    }
                    Err(e) => {
                        warn!("Finalization subscription error on connection {}: {}", conn_idx, e);
                    }
                }
            }
            else => {
                warn!("Both subscriptions ended on connection {}", conn_idx);
                break;
            }
        }
    }

    Ok(())
}

/// Check if we already have data for this slot with a different hash (fork).
/// If so, mark existing rows as not on best chain.
async fn handle_fork_detection(
    pool: &PgPool,
    slot: u32,
    new_hash: &HeaderHash,
    _ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    // Check for existing rows at this slot with different header_hash
    let existing: Option<(Vec<u8>,)> = sqlx::query_as(
        "SELECT DISTINCT header_hash FROM onchain_core_stats \
         WHERE slot = $1 AND on_best_chain = true AND header_hash != $2 \
         LIMIT 1",
    )
    .bind(slot as i32)
    .bind(&new_hash.0[..])
    .fetch_optional(pool)
    .await?;

    if let Some((old_hash,)) = existing {
        info!(
            "Fork detected at slot {}: new best {:.16}, previous {:.16}",
            slot,
            hex::encode(new_hash.0),
            hex::encode(&old_hash),
        );

        // Mark old rows as not on best chain (all three tables)
        for table in &[
            "onchain_core_stats",
            "onchain_service_stats",
            "onchain_validator_stats",
        ] {
            let sql = format!(
                "UPDATE {} SET on_best_chain = false \
                 WHERE slot = $1 AND on_best_chain = true AND header_hash != $2",
                table
            );
            sqlx::query(&sql)
                .bind(slot as i32)
                .bind(&new_hash.0[..])
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

/// Insert one block's worth of on-chain statistics into all three tables.
async fn insert_block_stats(
    pool: &PgPool,
    ts: DateTime<Utc>,
    slot: u32,
    header_hash: &HeaderHash,
    statistics: &Statistics,
) -> anyhow::Result<()> {
    let hash = &header_hash.0[..];
    let slot_i32 = slot as i32;

    // Insert core stats (341 rows typically)
    if !statistics.cores.is_empty() {
        let mut sql = String::from(
            "INSERT INTO onchain_core_stats \
             (timestamp, slot, header_hash, core, gas_used, da_load, popularity, \
              imports, extrinsic_count, extrinsic_size, exports, bundle_size, on_best_chain) VALUES ",
        );
        let mut first = true;
        let mut param_idx = 1u32;

        for _ in 0..statistics.cores.len() {
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, true)",
                param_idx,
                param_idx + 1,
                param_idx + 2,
                param_idx + 3,
                param_idx + 4,
                param_idx + 5,
                param_idx + 6,
                param_idx + 7,
                param_idx + 8,
                param_idx + 9,
                param_idx + 10,
                param_idx + 11,
            ));
            param_idx += 12;
        }

        let mut query = sqlx::query(&sql);
        for (i, core) in statistics.cores.iter().enumerate() {
            query = query
                .bind(ts)
                .bind(slot_i32)
                .bind(hash)
                .bind(i as i16)
                .bind(core.gas_used as i64)
                .bind(core.da_load as i32)
                .bind(core.popularity as i16)
                .bind(core.imports as i16)
                .bind(core.extrinsic_count as i16)
                .bind(core.extrinsic_size as i32)
                .bind(core.exports as i16)
                .bind(core.bundle_size as i32);
        }
        query.execute(pool).await?;
    }

    // Insert service stats (only services with non-zero activity)
    if !statistics.services.is_empty() {
        let mut sql = String::from(
            "INSERT INTO onchain_service_stats \
             (timestamp, slot, header_hash, service_id, provided_count, provided_size, \
              refinement_count, refinement_gas, imports, extrinsic_count, extrinsic_size, \
              exports, accumulate_count, accumulate_gas, on_best_chain) VALUES ",
        );
        let mut first = true;
        let mut param_idx = 1u32;

        for _ in 0..statistics.services.len() {
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, true)",
                param_idx,
                param_idx + 1,
                param_idx + 2,
                param_idx + 3,
                param_idx + 4,
                param_idx + 5,
                param_idx + 6,
                param_idx + 7,
                param_idx + 8,
                param_idx + 9,
                param_idx + 10,
                param_idx + 11,
                param_idx + 12,
                param_idx + 13,
            ));
            param_idx += 14;
        }

        let mut query = sqlx::query(&sql);
        for (service_id, svc) in statistics.services.iter() {
            query = query
                .bind(ts)
                .bind(slot_i32)
                .bind(hash)
                .bind(*service_id as i32)
                .bind(svc.provided_count as i16)
                .bind(svc.provided_size as i32)
                .bind(svc.refinement_count as i32)
                .bind(svc.refinement_gas_used as i64)
                .bind(svc.imports as i32)
                .bind(svc.extrinsic_count as i32)
                .bind(svc.extrinsic_size as i32)
                .bind(svc.exports as i32)
                .bind(svc.accumulate_count as i32)
                .bind(svc.accumulate_gas_used as i64);
        }
        query.execute(pool).await?;
    }

    // Insert validator stats (1024 rows typically, vals_curr only)
    if !statistics.vals_curr.is_empty() {
        // Batch in chunks of 256 to keep query size reasonable
        for chunk_start in (0..statistics.vals_curr.len()).step_by(256) {
            let chunk_end = (chunk_start + 256).min(statistics.vals_curr.len());
            let chunk = &statistics.vals_curr[chunk_start..chunk_end];

            let mut sql = String::from(
                "INSERT INTO onchain_validator_stats \
                 (timestamp, slot, header_hash, validator_index, blocks_produced, tickets, \
                  preimages, preimages_size, guarantees, assurances, on_best_chain) VALUES ",
            );
            let mut first = true;
            let mut param_idx = 1u32;

            for _ in 0..chunk.len() {
                if !first {
                    sql.push(',');
                }
                first = false;
                sql.push_str(&format!(
                    "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, true)",
                    param_idx,
                    param_idx + 1,
                    param_idx + 2,
                    param_idx + 3,
                    param_idx + 4,
                    param_idx + 5,
                    param_idx + 6,
                    param_idx + 7,
                    param_idx + 8,
                    param_idx + 9,
                ));
                param_idx += 10;
            }

            let mut query = sqlx::query(&sql);
            for (offset, val) in chunk.iter().enumerate() {
                let vi = (chunk_start + offset) as i16;
                query = query
                    .bind(ts)
                    .bind(slot_i32)
                    .bind(hash)
                    .bind(vi)
                    .bind(val.blocks as i32)
                    .bind(val.tickets as i32)
                    .bind(val.preimages as i32)
                    .bind(val.preimages_size as i32)
                    .bind(val.guarantees as i32)
                    .bind(val.assurances as i32);
            }
            query.execute(pool).await?;
        }
    }

    Ok(())
}

/// Handle finalization: walk canonical chain, delete orphans, correct on_best_chain.
async fn handle_finalization(
    pool: &PgPool,
    client: &WsClient,
    finalized: &BlockDesc,
    slot_period_secs: u16,
    counters: &RwLock<IngestCounters>,
) -> anyhow::Result<()> {
    // Read current finalization state
    let row: Option<(i32, Vec<u8>)> = sqlx::query_as(
        "SELECT finalized_slot, finalized_hash FROM onchain_finalization WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let prev_finalized_slot = row.as_ref().map(|(s, _)| *s).unwrap_or(0);
    let new_slot = finalized.slot as i32;

    if new_slot <= prev_finalized_slot {
        debug!(
            "Stale finalization notification slot {}, already at {}",
            new_slot, prev_finalized_slot
        );
        return Ok(());
    }

    // CAS update: only one connection wins
    let rows_affected = sqlx::query(
        "UPDATE onchain_finalization \
         SET finalized_slot = $1, finalized_hash = $2, updated_at = NOW() \
         WHERE finalized_slot < $1",
    )
    .bind(new_slot)
    .bind(&finalized.header_hash.0[..])
    .execute(pool)
    .await?
    .rows_affected();

    if rows_affected == 0 {
        debug!(
            "Finalization race lost for slot {}, another connection handled it",
            new_slot
        );
        return Ok(());
    }

    info!(
        "Finalization advanced: slot {} -> {} (hash {:.16})",
        prev_finalized_slot,
        new_slot,
        hex::encode(finalized.header_hash.0),
    );

    // Walk canonical chain backwards from finalized to prev_finalized
    let mut canonical: Vec<(i32, Vec<u8>)> = Vec::new();
    let mut current = *finalized;
    let mut max_walk = 1000u32;

    while (current.slot as i32) > prev_finalized_slot && max_walk > 0 {
        canonical.push((current.slot as i32, current.header_hash.0.to_vec()));
        match client.parent(current.header_hash).await {
            Ok(parent) => current = parent,
            Err(e) => {
                error!(
                    "parent() RPC failed at slot {}: {}, deferring cleanup",
                    current.slot, e
                );
                // Update counters anyway
                let mut c = counters.write().await;
                c.last_finalized_slot = finalized.slot;
                return Ok(());
            }
        }
        max_walk -= 1;
    }

    if max_walk == 0 {
        warn!(
            "Fork cleanup walk exceeded 1000 blocks (slot range {}–{}), cleaning range wholesale",
            prev_finalized_slot, new_slot
        );
    }

    debug!(
        "Canonical chain walk: {} blocks from slot {} back to {}",
        canonical.len(),
        new_slot,
        prev_finalized_slot + 1
    );

    let ts_from = slot_to_timestamp((prev_finalized_slot + 1) as u32, slot_period_secs);
    let ts_to = slot_to_timestamp(finalized.slot, slot_period_secs);

    // Build canonical set for SQL IN clause
    if !canonical.is_empty() {
        // Delete orphaned fork rows and correct on_best_chain for all three tables
        for table in &[
            "onchain_core_stats",
            "onchain_service_stats",
            "onchain_validator_stats",
        ] {
            // Delete non-canonical rows
            let delete_sql = format!(
                "DELETE FROM {} \
                 WHERE timestamp >= $1 AND timestamp <= $2 \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM unnest($3::int[], $4::bytea[]) AS c(s, h) \
                       WHERE c.s = {}.slot AND c.h = {}.header_hash \
                   )",
                table, table, table
            );

            let slots: Vec<i32> = canonical.iter().map(|(s, _)| *s).collect();
            let hashes: Vec<Vec<u8>> = canonical.iter().map(|(_, h)| h.clone()).collect();

            let deleted = sqlx::query(&delete_sql)
                .bind(ts_from)
                .bind(ts_to)
                .bind(&slots)
                .bind(&hashes)
                .execute(pool)
                .await?
                .rows_affected();

            if deleted > 0 {
                info!(
                    "Fork cleanup: deleted {} orphan rows from {} (slots {}..{})",
                    deleted,
                    table,
                    prev_finalized_slot + 1,
                    new_slot
                );
            }

            // Fix on_best_chain for canonical rows that were wrongly marked false
            let fix_sql = format!(
                "UPDATE {} SET on_best_chain = true \
                 WHERE timestamp >= $1 AND timestamp <= $2 \
                   AND on_best_chain = false \
                   AND EXISTS ( \
                       SELECT 1 FROM unnest($3::int[], $4::bytea[]) AS c(s, h) \
                       WHERE c.s = {}.slot AND c.h = {}.header_hash \
                   )",
                table, table, table
            );

            let fixed = sqlx::query(&fix_sql)
                .bind(ts_from)
                .bind(ts_to)
                .bind(&slots)
                .bind(&hashes)
                .execute(pool)
                .await?
                .rows_affected();

            if fixed > 0 {
                warn!(
                    "Fork correction: restored {} rows in {} — finalized fork differs from last best chain",
                    fixed, table
                );
            }
        }
    }

    // Update counters
    {
        let mut c = counters.write().await;
        c.last_finalized_slot = finalized.slot;
    }

    Ok(())
}
