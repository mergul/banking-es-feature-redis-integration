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

-- Drop the GIN index on metadata since we're changing it to BYTEA
-- Use a more specific approach to find and drop the index
DO $$
DECLARE
    index_record RECORD;
BEGIN
    FOR index_record IN 
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = 'events' 
        AND indexdef LIKE '%metadata%'
        AND indexdef LIKE '%gin%'
    LOOP
        EXECUTE 'DROP INDEX IF EXISTS ' || index_record.indexname;
    END LOOP;
END $$;

-- Remove the default value first, then change the column type
ALTER TABLE events ALTER COLUMN metadata DROP DEFAULT;
ALTER TABLE events ALTER COLUMN metadata TYPE BYTEA USING '{}'::bytea;
ALTER TABLE events ALTER COLUMN metadata SET DEFAULT ''::bytea;

-- Change the event_data column type from JSONB to BYTEA
ALTER TABLE events ALTER COLUMN event_data TYPE BYTEA USING '{}'::bytea;

-- Also change snapshot_data to BYTEA for consistency
ALTER TABLE snapshots ALTER COLUMN snapshot_data TYPE BYTEA USING '{}'::bytea;

-- Add a comment to document the change
COMMENT ON COLUMN events.event_data IS 'Binary serialized event data using bincode';
COMMENT ON COLUMN events.metadata IS 'Binary serialized metadata using bincode';
COMMENT ON COLUMN snapshots.snapshot_data IS 'Binary serialized snapshot data using bincode'; 