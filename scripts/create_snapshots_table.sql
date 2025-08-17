-- Snapshots Table Creation Script
-- This script creates the snapshots table for event sourcing

-- Create snapshots table
CREATE TABLE IF NOT EXISTS snapshots (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    snapshot_data BYTEA NOT NULL,
    version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Create indexes for snapshots table
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_id
    ON snapshots (aggregate_id)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type
    ON snapshots (aggregate_type)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_snapshots_version
    ON snapshots (version)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
    ON snapshots (created_at)
    WITH (fillfactor = 90);

-- Add unique constraint for aggregate_id and version
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_snapshot_aggregate_version'
    ) THEN
        ALTER TABLE snapshots ADD CONSTRAINT unique_snapshot_aggregate_version
            UNIQUE (aggregate_id, version);
    END IF;
END$$;

-- Add check constraint for version
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'check_snapshot_version'
    ) THEN
        ALTER TABLE snapshots ADD CONSTRAINT check_snapshot_version
            CHECK (version > 0);
    END IF;
END$$;

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Snapshots table created successfully!';
    RAISE NOTICE 'Indexes: aggregate_id, aggregate_type, version, created_at';
    RAISE NOTICE 'Constraints: unique_snapshot_aggregate_version, check_snapshot_version';
END $$; 