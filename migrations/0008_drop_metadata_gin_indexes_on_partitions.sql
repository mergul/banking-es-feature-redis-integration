-- Migration: Drop all gin indexes on metadata for all partitions of the events table

DO $$
DECLARE
    part RECORD;
    idx RECORD;
BEGIN
    -- Loop through all partitions of the events table
    FOR part IN
        SELECT inhrelid::regclass::text AS partition_name
        FROM pg_inherits
        WHERE inhparent = 'events'::regclass
    LOOP
        -- For each partition, find gin indexes on metadata
        FOR idx IN
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = part.partition_name
              AND indexdef LIKE '%USING gin%'
              AND indexdef LIKE '%metadata%'
        LOOP
            RAISE NOTICE 'Dropping index % on partition %', idx.indexname, part.partition_name;
            EXECUTE 'DROP INDEX IF EXISTS ' || idx.indexname;
        END LOOP;
    END LOOP;
END $$; 