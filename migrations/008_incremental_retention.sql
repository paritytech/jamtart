-- Incremental, self-healing retention for the raw events hypertable.
--
-- The built-in retention policy is a single transaction with 5-minute time
-- budget and "all-or-nothing" approach. During an event storm, starting from
-- the moment when it cannot drop all the stale events, it never succeeds and
-- never self-heals, constantly exhausting the budget and rolling back the
-- transaction.
--
-- This job drops chunks one at a time, oldest first, committing after each
-- drop. Lock-contended / deadlocked chunks are skipped and retried next run.
--
-- Limitation: this does not work together with chunk compression, so the
-- compression job must be disabled.

CREATE OR REPLACE PROCEDURE tart_incremental_retention(job_id INT, config JSONB)
LANGUAGE plpgsql
AS $proc$
DECLARE
    ht         TEXT;
    ht_schema  TEXT;
    drop_after INTERVAL;
    lock_wait  TEXT;
    budget     INTERVAL;
    started_at TIMESTAMPTZ := clock_timestamp();
    cutoff     TIMESTAMPTZ;
    total      BIGINT;
    chunk      REGCLASS;
    dropped    BIGINT := 0;
    skipped    BIGINT := 0;
BEGIN
    ht         := config ->> 'hypertable';
    ht_schema  := COALESCE(config ->> 'hypertable_schema', 'public');
    drop_after := (config ->> 'drop_after')::INTERVAL;
    lock_wait  := COALESCE(config ->> 'lock_timeout', '2s');
    budget     := COALESCE((config ->> 'max_runtime')::INTERVAL, INTERVAL '4 minutes');

    IF ht IS NULL OR drop_after IS NULL THEN
        RAISE EXCEPTION 'tart_incremental_retention: config must provide "hypertable" and "drop_after", got %', config;
    END IF;
    IF NOT EXISTS (
        SELECT FROM timescaledb_information.hypertables
        WHERE hypertable_schema = ht_schema AND hypertable_name = ht
    ) THEN
        RAISE EXCEPTION 'tart_incremental_retention: %.% is not a hypertable', ht_schema, ht;
    END IF;

    -- Stable cutoff for the whole run: chunks becoming eligible mid-run wait
    -- for the next run instead of turning the loop into a moving target.
    cutoff := started_at - drop_after;

    SELECT count(*) INTO total
    FROM timescaledb_information.chunks
    WHERE hypertable_schema = ht_schema
      AND hypertable_name = ht
      AND range_end <= cutoff;

    -- Oldest first, so under contention the backlog still shrinks from the
    -- tail. The chunk list is snapshotted when the loop opens (the cursor
    -- becomes holdable at the first COMMIT); chunks created later are not
    -- seen, which is fine — they cannot be eligible under this run's cutoff.
    FOR chunk IN
        SELECT format('%I.%I', chunk_schema, chunk_name)::REGCLASS
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = ht_schema
          AND hypertable_name = ht
          AND range_end <= cutoff
        ORDER BY range_end, chunk_name
    LOOP
        EXIT WHEN clock_timestamp() - started_at >= budget;

        -- Transaction-local, so it must be re-armed after every COMMIT.
        PERFORM set_config('lock_timeout', lock_wait, true);
        BEGIN
            EXECUTE format('DROP TABLE %s', chunk);
            dropped := dropped + 1;
        EXCEPTION
            WHEN lock_not_available OR deadlock_detected THEN
                skipped := skipped + 1;
                RAISE LOG 'tart_incremental_retention(%.%): skipped % (%: %)',
                    ht_schema, ht, chunk, SQLSTATE, SQLERRM;
            WHEN undefined_table THEN
                -- Dropped concurrently (e.g. manual intervention) — still progress.
                dropped := dropped + 1;
        END;
        COMMIT;
    END LOOP;

    RAISE LOG 'tart_incremental_retention(%.%): dropped %, skipped %, left % of % eligible chunk(s) in %',
        ht_schema, ht, dropped, skipped, total - dropped - skipped, total,
        clock_timestamp() - started_at;
END
$proc$;

SELECT remove_compression_policy('ingested_raw_events', if_exists => TRUE);

DO $block$
BEGIN
    IF EXISTS (
        SELECT FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'public'
          AND hypertable_name = 'ingested_raw_events'
          AND compression_enabled
    ) THEN
        ALTER TABLE public.ingested_raw_events SET (timescaledb.compress = false);
    END IF;
END
$block$;

-- Swap the built-in policy for the incremental job.
SELECT remove_retention_policy('ingested_raw_events', if_exists => TRUE);

SELECT add_job(
    'tart_incremental_retention',
    schedule_interval => INTERVAL '5 minutes',
    config => '{"hypertable": "ingested_raw_events", "drop_after": "1 hour", "lock_timeout": "2s", "max_runtime": "4 minutes"}',
    fixed_schedule => false
);
