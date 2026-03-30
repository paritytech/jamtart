-- Add execution timing columns to event_services.
-- elapsed_ns: total wall-clock execution time (from ExecCost.ns)
-- load_ns: PVM code loading/compilation time
-- Populated at ingestion for types 47 (BlockExecuted), 95 (Authorized), 101 (Refined).

ALTER TABLE event_services ADD COLUMN IF NOT EXISTS elapsed_ns BIGINT;
ALTER TABLE event_services ADD COLUMN IF NOT EXISTS load_ns BIGINT;
