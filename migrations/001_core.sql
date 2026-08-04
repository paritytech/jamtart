-- TART baseline schema (squashed from historical migrations 001-024).
--
-- The old migration history created continuous aggregates with live refresh
-- policies and later DROPped them (006, 015, 020). On a fresh database the
-- policy jobs start running mid-sequence and DROP MATERIALIZED VIEW ... CASCADE
-- deadlocks against the TimescaleDB job scheduler. This baseline creates only
-- the final schema, so no aggregate is ever dropped during migration.
--
-- RULE FOR FUTURE MIGRATIONS: never DROP a continuous aggregate (or a table
-- with policy jobs) that an EARLIER migration created — on a fresh database
-- its background job may already be running and the DROP can deadlock with
-- the scheduler. If a drop is unavoidable, remove the policies first and
-- accept that the race still exists; prefer additive changes.
--
-- Column order note: several tables list columns in "historical" order
-- (original columns first, later ALTER TABLE ADD COLUMNs last) so that a
-- fresh database is catalog-identical to one that replayed the old history.

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ============================================================
-- Nodes table (regular PostgreSQL table - low cardinality, ~1024 rows)
-- ============================================================
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    peer_id TEXT NOT NULL,
    implementation_name TEXT NOT NULL,
    implementation_version TEXT NOT NULL,
    node_info JSONB NOT NULL,
    connected_at TIMESTAMPTZ NOT NULL,
    disconnected_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ NOT NULL,
    is_connected BOOLEAN DEFAULT true,
    event_count BIGINT DEFAULT 0,
    total_events BIGINT DEFAULT 0,
    address TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_nodes_connected ON nodes(is_connected, last_seen_at DESC) WHERE is_connected = true;
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen_at DESC);

-- ============================================================
-- Raw events hypertable: 1h browsing store with hot columns.
-- All 115 event types are written here; aggregation lives in count tables.
-- Historically created as 'events' and renamed; index names keep the
-- original idx_events_* prefix on purpose.
-- ============================================================
CREATE TABLE IF NOT EXISTS ingested_raw_events (
    timestamp    TIMESTAMPTZ NOT NULL,
    node_id      TEXT        NOT NULL,
    event_id     BIGINT      NOT NULL,
    event_type   SMALLINT    NOT NULL,
    data         JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Hot columns: frequently-queried JSONB fields promoted to real columns
    slot         INT,
    core         SMALLINT,
    submission_id BIGINT
);

-- Convert to hypertable with 1-hour chunks
SELECT create_hypertable('ingested_raw_events', 'timestamp',
    chunk_time_interval => INTERVAL '1 hour',
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);

-- Space partitioning on node_id (32 hash buckets for write distribution)
SELECT add_dimension('ingested_raw_events', by_hash('node_id', 32), if_not_exists => TRUE);

-- Minimal indexes (each index costs write throughput at 3M events/s)
CREATE INDEX IF NOT EXISTS idx_events_node_time ON ingested_raw_events (node_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON ingested_raw_events (event_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_slot ON ingested_raw_events (slot, timestamp DESC) WHERE slot IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_core ON ingested_raw_events (core, timestamp DESC) WHERE core IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_submission_id ON ingested_raw_events (node_id, submission_id, timestamp DESC) WHERE submission_id IS NOT NULL;

-- Compression (compress chunks older than 2 hours)
ALTER TABLE ingested_raw_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('ingested_raw_events', INTERVAL '2 hours', if_not_exists => TRUE);

-- Pure browsing store — aggressive 1h retention, checked every 5 minutes
SELECT add_retention_policy('ingested_raw_events', INTERVAL '1 hour', schedule_interval => INTERVAL '5 minutes');

-- VIEW alias: legacy endpoints reference 'events'.
-- Created BEFORE the wp_hash column is added so the view's column list
-- (SELECT * expands at creation time) matches the historical schema.
CREATE VIEW events AS SELECT * FROM ingested_raw_events;

-- wp_hash hot column (added after the view on purpose — see above)
ALTER TABLE ingested_raw_events ADD COLUMN IF NOT EXISTS wp_hash BYTEA;
CREATE INDEX IF NOT EXISTS idx_ire_wp_hash
    ON ingested_raw_events (wp_hash, timestamp DESC) WHERE wp_hash IS NOT NULL;

-- ============================================================
-- Stats cache table for pre-computed aggregations
-- ============================================================
CREATE TABLE IF NOT EXISTS stats_cache (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Event type lookup table for human-readable names and grouping.
-- TODO: Generate this table automatically from src/events.rs definitions.
-- For now, this is a hardcoded list matching the JIP-3 telemetry protocol event types.
-- ============================================================
CREATE TABLE IF NOT EXISTS event_types (
    id SMALLINT PRIMARY KEY,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL
);

INSERT INTO event_types (id, name, group_name) VALUES
-- status
(10, 'Status', 'status'),
(11, 'BestBlockChanged', 'status'),
(12, 'FinalizedBlockChanged', 'status'),
(13, 'SyncStatusChanged', 'status'),
-- networking
(20, 'ConnectionRefused', 'networking'),
(21, 'ConnectingIn', 'networking'),
(22, 'ConnectInFailed', 'networking'),
(23, 'ConnectedIn', 'networking'),
(24, 'ConnectingOut', 'networking'),
(25, 'ConnectOutFailed', 'networking'),
(26, 'ConnectedOut', 'networking'),
(27, 'Disconnected', 'networking'),
(28, 'PeerMisbehaved', 'networking'),
-- blocks
(40, 'Authoring', 'blocks'),
(41, 'AuthoringFailed', 'blocks'),
(42, 'Authored', 'blocks'),
(43, 'Importing', 'blocks'),
(44, 'BlockVerificationFailed', 'blocks'),
(45, 'BlockVerified', 'blocks'),
(46, 'BlockExecutionFailed', 'blocks'),
(47, 'BlockExecuted', 'blocks'),
(60, 'BlockAnnouncementStreamOpened', 'blocks'),
(61, 'BlockAnnouncementStreamClosed', 'blocks'),
(62, 'BlockAnnounced', 'blocks'),
(63, 'SendingBlockRequest', 'blocks'),
(64, 'ReceivingBlockRequest', 'blocks'),
(65, 'BlockRequestFailed', 'blocks'),
(66, 'BlockRequestSent', 'blocks'),
(67, 'BlockRequestReceived', 'blocks'),
(68, 'BlockTransferred', 'blocks'),
-- tickets
(80, 'GeneratingTickets', 'tickets'),
(81, 'TicketGenerationFailed', 'tickets'),
(82, 'TicketsGenerated', 'tickets'),
(83, 'TicketTransferFailed', 'tickets'),
(84, 'TicketTransferred', 'tickets'),
-- work-package
(90, 'WorkPackageSubmission', 'work-package'),
(91, 'WorkPackageBeingShared', 'work-package'),
(92, 'WorkPackageFailed', 'work-package'),
(93, 'DuplicateWorkPackage', 'work-package'),
(94, 'WorkPackageReceived', 'work-package'),
(95, 'Authorized', 'work-package'),
(96, 'ExtrinsicDataReceived', 'work-package'),
(97, 'ImportsReceived', 'work-package'),
(98, 'SharingWorkPackage', 'work-package'),
(99, 'WorkPackageSharingFailed', 'work-package'),
(100, 'BundleSent', 'work-package'),
(101, 'Refined', 'work-package'),
(102, 'WorkReportBuilt', 'work-package'),
(103, 'WorkReportSignatureSent', 'work-package'),
(104, 'WorkReportSignatureReceived', 'work-package'),
-- guaranteeing
(105, 'GuaranteeBuilt', 'guaranteeing'),
(106, 'SendingGuarantee', 'guaranteeing'),
(107, 'GuaranteeSendFailed', 'guaranteeing'),
(108, 'GuaranteeSent', 'guaranteeing'),
(109, 'GuaranteesDistributed', 'guaranteeing'),
(110, 'ReceivingGuarantee', 'guaranteeing'),
(111, 'GuaranteeReceiveFailed', 'guaranteeing'),
(112, 'GuaranteeReceived', 'guaranteeing'),
(113, 'GuaranteeDiscarded', 'guaranteeing'),
-- availability
(120, 'SendingShardRequest', 'availability'),
(121, 'ReceivingShardRequest', 'availability'),
(122, 'ShardRequestFailed', 'availability'),
(123, 'ShardRequestSent', 'availability'),
(124, 'ShardRequestReceived', 'availability'),
(125, 'ShardsTransferred', 'availability'),
(126, 'DistributingAssurance', 'availability'),
(127, 'AssuranceSendFailed', 'availability'),
(128, 'AssuranceSent', 'availability'),
(129, 'AssuranceDistributed', 'availability'),
(130, 'AssuranceReceiveFailed', 'availability'),
(131, 'AssuranceReceived', 'availability'),
-- auditing
(140, 'SendingBundleShardRequest', 'auditing'),
(141, 'ReceivingBundleShardRequest', 'auditing'),
(142, 'BundleShardRequestFailed', 'auditing'),
(143, 'BundleShardRequestSent', 'auditing'),
(144, 'BundleShardRequestReceived', 'auditing'),
(145, 'BundleShardTransferred', 'auditing'),
(146, 'ReconstructingBundle', 'auditing'),
(147, 'BundleReconstructed', 'auditing'),
(148, 'SendingBundleRequest', 'auditing'),
(149, 'ReceivingBundleRequest', 'auditing'),
(150, 'BundleRequestFailed', 'auditing'),
(151, 'BundleRequestSent', 'auditing'),
(152, 'BundleRequestReceived', 'auditing'),
(153, 'BundleTransferred', 'auditing'),
-- segments
(160, 'WorkPackageHashMapped', 'segments'),
(161, 'SegmentsRootMapped', 'segments'),
(162, 'SendingSegmentShardRequest', 'segments'),
(163, 'ReceivingSegmentShardRequest', 'segments'),
(164, 'SegmentShardRequestFailed', 'segments'),
(165, 'SegmentShardRequestSent', 'segments'),
(166, 'SegmentShardRequestReceived', 'segments'),
(167, 'SegmentShardsTransferred', 'segments'),
(168, 'ReconstructingSegments', 'segments'),
(169, 'SegmentReconstructionFailed', 'segments'),
(170, 'SegmentsReconstructed', 'segments'),
(171, 'SegmentVerificationFailed', 'segments'),
(172, 'SegmentsVerified', 'segments'),
(173, 'SendingSegmentRequest', 'segments'),
(174, 'ReceivingSegmentRequest', 'segments'),
(175, 'SegmentRequestFailed', 'segments'),
(176, 'SegmentRequestSent', 'segments'),
(177, 'SegmentRequestReceived', 'segments'),
(178, 'SegmentsTransferred', 'segments'),
-- preimages
(190, 'PreimageAnnouncementFailed', 'preimages'),
(191, 'PreimageAnnounced', 'preimages'),
(192, 'AnnouncedPreimageForgotten', 'preimages'),
(193, 'SendingPreimageRequest', 'preimages'),
(194, 'ReceivingPreimageRequest', 'preimages'),
(195, 'PreimageRequestFailed', 'preimages'),
(196, 'PreimageRequestSent', 'preimages'),
(197, 'PreimageRequestReceived', 'preimages'),
(198, 'PreimageTransferred', 'preimages'),
(199, 'PreimageDiscarded', 'preimages')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, group_name = EXCLUDED.group_name;
