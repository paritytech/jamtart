//! JAM RPC client for connecting to JAM nodes and fetching service/core statistics.
//!
//! This module provides functionality similar to `jamtop` - connecting to a JAM node's
//! RPC endpoint to fetch live service information, core statistics, and network state.

use anyhow::Result;
use futures::StreamExt;
use jam_codec::{Compact, Decode};
use jam_program_blob_common::{ConventionalMetadata as Metadata, CrateInfo};
use jam_std_common::{
    BlockDesc, ChainSubUpdate, CoreActivityRecord, Node, NodeExt as _, ServiceActivityRecord,
    Statistics, VersionedParameters,
};
use jam_types::{HeaderHash, ServiceId};
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

const MAX_REQUEST_SIZE: u32 = 100 * 1024 * 1024;
const MAX_RESPONSE_SIZE: u32 = 100 * 1024 * 1024;

/// Information about a service's code
#[derive(Debug, Clone, Serialize)]
pub enum CodeInfo {
    /// Code metadata is known
    Known(ServiceCodeInfo),
    /// Code exists but no metadata
    Undefined { code_hash: String },
    /// Code not provided yet
    NotProvided { code_hash: String },
}

/// Service code metadata
#[derive(Debug, Clone, Serialize)]
pub struct ServiceCodeInfo {
    pub name: String,
    pub version: String,
}

impl From<CrateInfo> for ServiceCodeInfo {
    fn from(info: CrateInfo) -> Self {
        Self {
            name: info.name.to_string(),
            version: info.version.to_string(),
        }
    }
}

/// Combined service information
#[derive(Debug, Clone, Serialize)]
pub struct ServiceInfo {
    pub id: u32,
    pub code_info: CodeInfo,
    pub balance: u64,
    pub min_item_gas: u64,
    pub min_memo_gas: u64,
    /// Number of storage items
    pub items: u32,
    /// Total bytes used for storage
    pub bytes: u64,
    /// Creation slot
    pub creation_slot: u32,
    /// Activity statistics (if available)
    pub activity: Option<ServiceActivity>,
}

/// Service activity record
#[derive(Debug, Clone, Serialize, Default)]
pub struct ServiceActivity {
    pub refinement_count: u32,
    pub refinement_gas_used: u64,
    pub accumulate_count: u32,
    pub accumulate_gas_used: u64,
    pub imports: u32,
    pub exports: u32,
    pub extrinsic_count: u32,
    pub extrinsic_size: u32,
    pub provided_count: u16,
    pub provided_size: u32,
}

impl From<&ServiceActivityRecord> for ServiceActivity {
    fn from(r: &ServiceActivityRecord) -> Self {
        Self {
            refinement_count: r.refinement_count,
            refinement_gas_used: r.refinement_gas_used,
            accumulate_count: r.accumulate_count,
            accumulate_gas_used: r.accumulate_gas_used,
            imports: r.imports,
            exports: r.exports,
            extrinsic_count: r.extrinsic_count,
            extrinsic_size: r.extrinsic_size,
            provided_count: r.provided_count,
            provided_size: r.provided_size,
        }
    }
}

/// Core activity information
#[derive(Debug, Clone, Serialize, Default)]
pub struct CoreActivity {
    pub core_index: u16,
    pub gas_used: u64,
    pub da_load: u32,
    pub popularity: u16,
    pub imports: u16,
    pub extrinsic_count: u16,
    pub extrinsic_size: u32,
}

impl CoreActivity {
    fn from_record(index: u16, r: &CoreActivityRecord) -> Self {
        Self {
            core_index: index,
            gas_used: r.gas_used,
            da_load: r.da_load,
            popularity: r.popularity,
            imports: r.imports,
            extrinsic_count: r.extrinsic_count,
            extrinsic_size: r.extrinsic_size,
        }
    }
}

/// Network statistics snapshot
#[derive(Debug, Clone, Serialize, Default)]
pub struct NetworkStats {
    pub slot: u32,
    pub header_hash: String,
    pub core_count: u16,
    pub validator_count: u16,
    pub services: Vec<ServiceInfo>,
    pub cores: Vec<CoreActivity>,
    pub totals: NetworkTotals,
}

/// Aggregate network totals
#[derive(Debug, Clone, Serialize, Default)]
pub struct NetworkTotals {
    pub total_services: usize,
    pub active_services: usize,
    pub refining_services: usize,
    pub accumulating_services: usize,
    pub total_refinement_gas: u64,
    pub total_accumulate_gas: u64,
    pub total_imports: u32,
    pub total_exports: u32,
    pub total_extrinsics: u32,
}

/// Rolling-window per-core statistics accumulated from JAM RPC subscription
/// updates.  Each subscription event delivers one block's `pi_C` snapshot;
/// we keep the last `WINDOW_SLOTS` samples so callers get stable cumulative
/// metrics instead of flickering per-block values.
#[derive(Debug, Clone, Serialize)]
pub struct CoreWindowStats {
    pub core_index: u16,
    /// Sum of refinement gas across all blocks in the window.
    pub gas_used: u64,
    /// Sum of DA load (bundle + segment footprint) across the window.
    pub da_load: u64,
    /// Average assurance count (popularity) across blocks *with* activity.
    pub popularity_avg: f64,
    /// Total import segments across the window.
    pub imports: u64,
    /// Total extrinsic count across the window.
    pub extrinsic_count: u64,
    /// Total extrinsic bytes across the window.
    pub extrinsic_size: u64,
    /// Number of blocks in the window where this core had a guarantee.
    pub active_blocks: u32,
    /// Total blocks in the window.
    pub window_blocks: u32,
    /// Most recent slot with non-zero activity (0 if never active).
    pub last_active_slot: u32,
}

/// One block's per-core snapshot from the JAM RPC subscription.
#[derive(Debug, Clone)]
struct CoreSample {
    slot: u32,
    records: Vec<CoreActivity>,
}

/// Number of recent blocks to accumulate (roughly one epoch at 6s/slot).
const WINDOW_SLOTS: usize = 600;

/// JAM RPC client that maintains a connection to a JAM node
pub struct JamRpcClient {
    rpc_url: String,
    client: Option<WsClient>,
    /// Cached network stats, updated by background subscription
    stats: Arc<RwLock<Option<NetworkStats>>>,
    /// Protocol parameters
    params: Arc<RwLock<Option<ProtocolParams>>>,
    /// Rolling window of per-block core samples for accumulation
    core_samples: Arc<RwLock<std::collections::VecDeque<CoreSample>>>,
}

/// Protocol parameters from the chain
#[derive(Debug, Clone, Serialize)]
pub struct ProtocolParams {
    pub core_count: u16,
    pub validator_count: u16,
    pub epoch_period: u32,
    pub slot_period_sec: u16,
    pub max_refine_gas: u64,
    pub max_accumulate_gas: u64,
}

impl JamRpcClient {
    /// Create a new JAM RPC client
    pub fn new(rpc_url: &str) -> Self {
        Self {
            rpc_url: rpc_url.to_string(),
            client: None,
            stats: Arc::new(RwLock::new(None)),
            params: Arc::new(RwLock::new(None)),
            core_samples: Arc::new(RwLock::new(std::collections::VecDeque::with_capacity(
                WINDOW_SLOTS + 1,
            ))),
        }
    }

    /// Connect to the JAM node RPC endpoint
    pub async fn connect(&mut self) -> Result<()> {
        info!("Connecting to JAM RPC at {}", self.rpc_url);

        let uri = url::Url::parse(&self.rpc_url)?;
        let client = WsClientBuilder::default()
            .max_request_size(MAX_REQUEST_SIZE)
            .max_response_size(MAX_RESPONSE_SIZE)
            .build(&uri)
            .await?;

        // Fetch and apply protocol parameters
        let VersionedParameters::V1(params) = client.parameters().await?;

        // Store params
        {
            let mut p = self.params.write().await;
            *p = Some(ProtocolParams {
                core_count: params.core_count,
                validator_count: params.val_count,
                epoch_period: params.epoch_period,
                slot_period_sec: params.slot_period_sec,
                max_refine_gas: params.max_refine_gas,
                max_accumulate_gas: params.max_accumulate_gas,
            });
        }

        // Apply params globally for jam-types
        params.apply().map_err(|s| anyhow::anyhow!("{}", s))?;

        info!(
            "Connected to JAM RPC: {} cores, {} validators",
            params.core_count, params.val_count
        );

        self.client = Some(client);
        Ok(())
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    /// Record one block's per-core stats into the rolling window.
    async fn record_core_sample(&self, slot: u32, cores: Vec<CoreActivity>) {
        let mut samples = self.core_samples.write().await;
        // Deduplicate by slot (subscription may fire more than once)
        if samples.back().map(|s| s.slot) == Some(slot) {
            return;
        }
        samples.push_back(CoreSample {
            slot,
            records: cores,
        });
        while samples.len() > WINDOW_SLOTS {
            samples.pop_front();
        }
    }

    /// Compute accumulated per-core stats over the rolling window.
    pub async fn get_core_window_stats(&self) -> Vec<CoreWindowStats> {
        let samples = self.core_samples.read().await;
        let params = self.params.read().await;
        let core_count = params.as_ref().map(|p| p.core_count).unwrap_or(0) as usize;
        if core_count == 0 || samples.is_empty() {
            return Vec::new();
        }

        let window_blocks = samples.len() as u32;

        // Accumulators per core
        let mut gas: Vec<u64> = vec![0; core_count];
        let mut da: Vec<u64> = vec![0; core_count];
        let mut pop_sum: Vec<u64> = vec![0; core_count];
        let mut pop_count: Vec<u32> = vec![0; core_count];
        let mut imports: Vec<u64> = vec![0; core_count];
        let mut xt_count: Vec<u64> = vec![0; core_count];
        let mut xt_size: Vec<u64> = vec![0; core_count];
        let mut active_blocks: Vec<u32> = vec![0; core_count];
        let mut last_active_slot: Vec<u32> = vec![0; core_count];

        for sample in samples.iter() {
            for ca in &sample.records {
                let i = ca.core_index as usize;
                if i >= core_count {
                    continue;
                }
                gas[i] += ca.gas_used;
                da[i] += ca.da_load as u64;
                imports[i] += ca.imports as u64;
                xt_count[i] += ca.extrinsic_count as u64;
                xt_size[i] += ca.extrinsic_size as u64;
                if ca.gas_used > 0 || ca.popularity > 0 || ca.da_load > 0 {
                    active_blocks[i] += 1;
                    pop_sum[i] += ca.popularity as u64;
                    pop_count[i] += 1;
                    last_active_slot[i] = sample.slot;
                }
            }
        }

        (0..core_count)
            .map(|i| CoreWindowStats {
                core_index: i as u16,
                gas_used: gas[i],
                da_load: da[i],
                popularity_avg: if pop_count[i] > 0 {
                    pop_sum[i] as f64 / pop_count[i] as f64
                } else {
                    0.0
                },
                imports: imports[i],
                extrinsic_count: xt_count[i],
                extrinsic_size: xt_size[i],
                active_blocks: active_blocks[i],
                window_blocks,
                last_active_slot: last_active_slot[i],
            })
            .collect()
    }

    /// Get the current protocol parameters
    pub async fn get_params(&self) -> Option<ProtocolParams> {
        self.params.read().await.clone()
    }

    /// Get cached network statistics
    pub async fn get_stats(&self) -> Option<NetworkStats> {
        self.stats.read().await.clone()
    }

    /// Fetch current network statistics (one-shot, not subscription)
    pub async fn fetch_stats(&self) -> Result<NetworkStats> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Not connected to RPC"))?;

        let params = self
            .params
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Protocol parameters not loaded"))?;

        // Get best block
        let BlockDesc { header_hash, slot } = client.best_block().await?;

        // Get statistics
        let statistics = client.statistics(header_hash).await?;

        // Get list of services
        let service_ids = client.list_services(header_hash).await?;

        // Fetch service info for each service
        let mut services = Vec::new();
        for &id in &service_ids {
            match self
                .fetch_service_info(client, header_hash, id, &statistics)
                .await
            {
                Ok(info) => services.push(info),
                Err(e) => {
                    warn!("Failed to fetch service {}: {}", id, e);
                }
            }
        }

        // Build core activity info
        let cores: Vec<CoreActivity> = statistics
            .cores
            .iter()
            .enumerate()
            .map(|(i, r)| CoreActivity::from_record(i as u16, r))
            .collect();

        // Calculate totals
        let totals = self.calculate_totals(&services, &statistics);

        Ok(NetworkStats {
            slot,
            header_hash: hex::encode(header_hash.0),
            core_count: params.core_count,
            validator_count: params.validator_count,
            services,
            cores,
            totals,
        })
    }

    /// Fetch information for a single service
    async fn fetch_service_info(
        &self,
        client: &WsClient,
        head: HeaderHash,
        id: ServiceId,
        statistics: &Statistics,
    ) -> Result<ServiceInfo> {
        // Get service data
        let service = client
            .service_data(head, id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Service {} not found", id))?;

        // Try to get code metadata
        let code_info = match client
            .service_preimage(head, id, service.code_hash.0)
            .await?
        {
            None => CodeInfo::NotProvided {
                code_hash: hex::encode(service.code_hash.0),
            },
            Some(code) => {
                match <(Compact<u32>, Metadata)>::decode(&mut &code[..])
                    .ok()
                    .map(|x| x.1)
                {
                    None => CodeInfo::Undefined {
                        code_hash: hex::encode(service.code_hash.0),
                    },
                    Some(Metadata::Info(crate_info)) => CodeInfo::Known(crate_info.into()),
                }
            }
        };

        // Get activity if available
        let activity = statistics
            .services
            .iter()
            .find(|(sid, _)| *sid == id)
            .map(|(_, r)| ServiceActivity::from(r));

        Ok(ServiceInfo {
            id,
            code_info,
            balance: service.balance,
            min_item_gas: service.min_item_gas,
            min_memo_gas: service.min_memo_gas,
            items: service.items,
            bytes: service.bytes,
            creation_slot: service.creation_slot,
            activity,
        })
    }

    /// Calculate network totals from services and statistics
    fn calculate_totals(&self, services: &[ServiceInfo], statistics: &Statistics) -> NetworkTotals {
        let active_services = statistics.services.len();
        let refining = statistics
            .services
            .iter()
            .filter(|(_, r)| r.refinement_count > 0)
            .count();
        let accumulating = statistics
            .services
            .iter()
            .filter(|(_, r)| r.accumulate_count > 0)
            .count();

        let total_ref_gas: u64 = statistics
            .services
            .iter()
            .map(|(_, r)| r.refinement_gas_used)
            .sum();
        let total_acc_gas: u64 = statistics
            .services
            .iter()
            .map(|(_, r)| r.accumulate_gas_used)
            .sum();
        let total_imports: u32 = statistics.services.iter().map(|(_, r)| r.imports).sum();
        let total_exports: u32 = statistics.services.iter().map(|(_, r)| r.exports).sum();
        let total_xts: u32 = statistics
            .services
            .iter()
            .map(|(_, r)| r.extrinsic_count)
            .sum();

        NetworkTotals {
            total_services: services.len(),
            active_services,
            refining_services: refining,
            accumulating_services: accumulating,
            total_refinement_gas: total_ref_gas,
            total_accumulate_gas: total_acc_gas,
            total_imports,
            total_exports,
            total_extrinsics: total_xts,
        }
    }

    /// Start a background task that subscribes to statistics updates
    pub fn start_stats_subscription(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let stats = self.stats.clone();
        let params = self.params.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::run_subscription(&self, stats.clone(), params.clone()).await {
                    error!("Stats subscription error: {}, reconnecting in 5s...", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        })
    }

    async fn run_subscription(
        client: &Arc<Self>,
        stats: Arc<RwLock<Option<NetworkStats>>>,
        params: Arc<RwLock<Option<ProtocolParams>>>,
    ) -> Result<()> {
        let rpc = client
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Not connected"))?;

        let mut sub = rpc.subscribe_statistics(false).await?;
        info!("Subscribed to JAM statistics updates");

        while let Some(result) = sub.next().await {
            match result {
                Ok(ChainSubUpdate {
                    header_hash,
                    slot,
                    value: statistics,
                }) => {
                    let p = params.read().await.clone();
                    if let Some(p) = p {
                        // Build core stats
                        let cores: Vec<CoreActivity> = statistics
                            .cores
                            .iter()
                            .enumerate()
                            .map(|(i, r)| CoreActivity::from_record(i as u16, r))
                            .collect();

                        // Build service activity (without full service info for subscription)
                        let services: Vec<ServiceInfo> = statistics
                            .services
                            .iter()
                            .map(|(id, activity)| ServiceInfo {
                                id: *id,
                                code_info: CodeInfo::Undefined {
                                    code_hash: "subscription".to_string(),
                                },
                                balance: 0,
                                min_item_gas: 0,
                                min_memo_gas: 0,
                                items: 0,
                                bytes: 0,
                                creation_slot: 0,
                                activity: Some(ServiceActivity::from(activity)),
                            })
                            .collect();

                        let totals = NetworkTotals {
                            total_services: services.len(),
                            active_services: statistics.services.len(),
                            refining_services: statistics
                                .services
                                .iter()
                                .filter(|(_, r)| r.refinement_count > 0)
                                .count(),
                            accumulating_services: statistics
                                .services
                                .iter()
                                .filter(|(_, r)| r.accumulate_count > 0)
                                .count(),
                            total_refinement_gas: statistics
                                .services
                                .iter()
                                .map(|(_, r)| r.refinement_gas_used)
                                .sum(),
                            total_accumulate_gas: statistics
                                .services
                                .iter()
                                .map(|(_, r)| r.accumulate_gas_used)
                                .sum(),
                            total_imports: statistics.services.iter().map(|(_, r)| r.imports).sum(),
                            total_exports: statistics.services.iter().map(|(_, r)| r.exports).sum(),
                            total_extrinsics: statistics
                                .services
                                .iter()
                                .map(|(_, r)| r.extrinsic_count)
                                .sum(),
                        };

                        let network_stats = NetworkStats {
                            slot,
                            header_hash: hex::encode(header_hash.0),
                            core_count: p.core_count,
                            validator_count: p.validator_count,
                            services,
                            cores,
                            totals,
                        };

                        // Record per-core sample into rolling window
                        client
                            .record_core_sample(slot, network_stats.cores.clone())
                            .await;

                        debug!(
                            "Stats update: slot={}, {} active services, {} cores",
                            slot,
                            network_stats.totals.active_services,
                            network_stats.cores.len()
                        );

                        let mut s = stats.write().await;
                        *s = Some(network_stats);
                    }
                }
                Err(e) => {
                    warn!("Statistics subscription error: {}", e);
                }
            }
        }

        Ok(())
    }
}
