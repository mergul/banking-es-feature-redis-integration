CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- High-performance database schema optimizations

-- 1. Events table with optimized partitioning and indexes
CREATE TABLE IF NOT EXISTS events (
    id UUID NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    version BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions with optimized fillfactor
CREATE TABLE IF NOT EXISTS events_2025_01 PARTITION OF events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_02 PARTITION OF events
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_03 PARTITION OF events
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_04 PARTITION OF events
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_05 PARTITION OF events
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_06 PARTITION OF events
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_07 PARTITION OF events
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_08 PARTITION OF events
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_09 PARTITION OF events
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_10 PARTITION OF events
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_11 PARTITION OF events
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01')
    WITH (fillfactor = 90);

CREATE TABLE IF NOT EXISTS events_2025_12 PARTITION OF events
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01')
    WITH (fillfactor = 90);

-- Optimized indexes for events with fillfactor
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, version)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events (timestamp)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events (event_type)
    WITH (fillfactor = 90);

-- CREATE INDEX IF NOT EXISTS idx_events_recent
--     ON events (timestamp)
--     WHERE timestamp > NOW() - INTERVAL '30 days'
--     WITH (fillfactor = 90);

-- GIN index for JSONB queries (GIN indexes don't support fillfactor)
CREATE INDEX IF NOT EXISTS idx_events_data_gin
    ON events USING GIN (event_data jsonb_path_ops);

-- Add unique constraint for event ordering
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_aggregate_version_timestamp'
    ) THEN
        ALTER TABLE events ADD CONSTRAINT unique_aggregate_version_timestamp
            UNIQUE (aggregate_id, version, timestamp);
    END IF;
END$$;

-- 2. Snapshots table with optimized indexes
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id UUID PRIMARY KEY,
    snapshot_data JSONB NOT NULL,
    version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (fillfactor = 90);

-- Optimized indexes for snapshots
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
    ON snapshots (created_at)
    WITH (fillfactor = 90);

-- Add index for version lookups
CREATE INDEX IF NOT EXISTS idx_snapshots_version
    ON snapshots (version)
    WITH (fillfactor = 90);

-- Add GIN index for JSONB queries (GIN indexes don't support fillfactor)
CREATE INDEX IF NOT EXISTS idx_snapshots_data_gin
    ON snapshots USING GIN (snapshot_data jsonb_path_ops);

-- Optional: Create a function to automatically create future partitions
CREATE OR REPLACE FUNCTION create_monthly_partition(table_name TEXT, start_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    end_date DATE;
BEGIN
    partition_name := table_name || '_' || TO_CHAR(start_date, 'YYYY_MM');
    end_date := start_date + INTERVAL '1 month';
    
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L) WITH (fillfactor = 90)',
                   partition_name, table_name, start_date, end_date);
END;
$$ LANGUAGE plpgsql;

-- Usage example (commented out):
-- SELECT create_monthly_partition('events', '2026-01-01'::DATE);