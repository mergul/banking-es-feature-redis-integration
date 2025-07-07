-- Migration to change event_data column from JSONB to BYTEA for binary serialization
-- This migration supports the switch from serde_json to bincode for better performance

-- First, drop any indexes that use jsonb_path_ops on the metadata column
DO $$
DECLARE
    index_record RECORD;
BEGIN
    FOR index_record IN 
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = 'events' 
        AND indexdef LIKE '%metadata%'
        AND indexdef LIKE '%jsonb_path_ops%'
    LOOP
        EXECUTE 'DROP INDEX IF EXISTS ' || index_record.indexname;
    END LOOP;
END $$;

-- Change the column type from JSONB to BYTEA
ALTER TABLE events ALTER COLUMN event_data TYPE BYTEA USING event_data::text::bytea;

-- Also change snapshot_data to BYTEA for consistency
ALTER TABLE snapshots ALTER COLUMN snapshot_data TYPE BYTEA USING snapshot_data::text::bytea;

-- Add a comment to document the change
COMMENT ON COLUMN events.event_data IS 'Binary serialized event data using bincode';
COMMENT ON COLUMN snapshots.snapshot_data IS 'Binary serialized snapshot data using bincode'; 