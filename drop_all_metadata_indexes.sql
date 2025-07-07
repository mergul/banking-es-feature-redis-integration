-- Drop all gin indexes on metadata column for events table and all its partitions

-- First, drop any gin index on the main events table
DROP INDEX IF EXISTS idx_events_metadata_gin;

-- Then drop gin indexes on all partitions
DO $$
DECLARE
    part_name TEXT;
BEGIN
    -- Loop through all partitions
    FOR part_name IN 
        SELECT inhrelid::regclass::text
        FROM pg_inherits
        WHERE inhparent = 'events'::regclass
    LOOP
        -- Drop any gin index on metadata for this partition
        EXECUTE format('DROP INDEX IF EXISTS %I_metadata_gin', part_name);
        EXECUTE format('DROP INDEX IF EXISTS %I_metadata_idx', part_name);
        EXECUTE format('DROP INDEX IF EXISTS idx_%s_metadata_gin', part_name);
        EXECUTE format('DROP INDEX IF EXISTS idx_%s_metadata_idx', part_name);
        
        RAISE NOTICE 'Checked partition: %', part_name;
    END LOOP;
END $$;

-- Also try to drop any other gin indexes that might exist
DO $$
DECLARE
    idx_record RECORD;
BEGIN
    FOR idx_record IN
        SELECT indexname, tablename
        FROM pg_indexes
        WHERE (tablename = 'events' OR tablename LIKE 'events_2025_%')
          AND indexdef LIKE '%USING gin%'
          AND indexdef LIKE '%metadata%'
    LOOP
        RAISE NOTICE 'Dropping index % on table %', idx_record.indexname, idx_record.tablename;
        EXECUTE 'DROP INDEX IF EXISTS ' || idx_record.indexname;
    END LOOP;
END $$; 