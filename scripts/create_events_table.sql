-- Events Table Creation Script
-- This script creates the events table with all partitions and indexes
-- Includes all alter operations from migrations 007, 008, and 0008

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create events table with partitioning (HASH partitioning as per migration 007)
CREATE TABLE IF NOT EXISTS events (
    id UUID NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data BYTEA NOT NULL,
    version BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata BYTEA NOT NULL DEFAULT ''::bytea
) PARTITION BY HASH (aggregate_id);

-- Create hash partitions (as per migration 007)
CREATE TABLE IF NOT EXISTS events_p0 PARTITION OF events FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE IF NOT EXISTS events_p1 PARTITION OF events FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE IF NOT EXISTS events_p2 PARTITION OF events FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE IF NOT EXISTS events_p3 PARTITION OF events FOR VALUES WITH (modulus 4, remainder 3);

-- Create optimized indexes
CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events (timestamp)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events (event_type)
    WITH (fillfactor = 90);

-- Add unique constraint (as per migration 007)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_aggregate_id_version'
    ) THEN
        ALTER TABLE events ADD CONSTRAINT unique_aggregate_id_version
            UNIQUE (aggregate_id, version);
    END IF;
END$$;

-- Add comment explaining the constraint (as per migration 007)
COMMENT ON CONSTRAINT unique_aggregate_id_version ON events IS 'Ensures optimistic concurrency control by preventing duplicate versions for the same aggregate';

-- Add comment explaining the metadata column (as per migration 008)
COMMENT ON COLUMN events.metadata IS 'BYTEA metadata for events, serialized using bincode';

-- Create snapshots table with optimized indexes
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

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Events table and partitions created successfully!';
    RAISE NOTICE 'Partitions: 4 hash partitions (p0, p1, p2, p3)';
    RAISE NOTICE 'Indexes: timestamp, event_type';
    RAISE NOTICE 'Constraints: unique_aggregate_id_version';
    RAISE NOTICE 'Metadata: BYTEA (bincode serialized)';
    RAISE NOTICE 'Snapshots table created with optimized indexes';
END $$; 