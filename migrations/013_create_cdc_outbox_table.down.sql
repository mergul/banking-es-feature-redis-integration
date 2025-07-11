-- Rollback migration for kafka_outbox_cdc table
-- This script removes the CDC outbox table and related objects

-- Drop the trigger first
DROP TRIGGER IF EXISTS update_outbox_cdc_updated_at ON kafka_outbox_cdc;

-- Drop the function
DROP FUNCTION IF EXISTS update_updated_at_column();

-- Remove the table from the publication
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'banking_cdc_publication'
    ) THEN
        ALTER PUBLICATION banking_cdc_publication DROP TABLE kafka_outbox_cdc;
    END IF;
END $$;

-- Drop the publication if it's empty (optional)
-- Uncomment the following lines if you want to drop the publication entirely
-- DO $$
-- BEGIN
--     IF EXISTS (
--         SELECT 1 FROM pg_publication WHERE pubname = 'banking_cdc_publication'
--     ) AND NOT EXISTS (
--         SELECT 1 FROM pg_publication_tables WHERE pubname = 'banking_cdc_publication'
--     ) THEN
--         DROP PUBLICATION banking_cdc_publication;
--     END IF;
-- END $$;

-- Drop indexes
DROP INDEX IF EXISTS idx_outbox_cdc_created_at;
DROP INDEX IF EXISTS idx_outbox_cdc_aggregate_id;
DROP INDEX IF EXISTS idx_outbox_cdc_event_id;
DROP INDEX IF EXISTS idx_outbox_cdc_topic;

-- Drop the table
DROP TABLE IF EXISTS kafka_outbox_cdc; 