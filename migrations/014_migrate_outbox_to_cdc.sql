-- Data migration script to migrate existing outbox data to CDC format
-- This script helps transition from the polling-based outbox to CDC-based outbox

-- Function to migrate pending outbox messages to CDC format
CREATE OR REPLACE FUNCTION migrate_outbox_to_cdc()
RETURNS INTEGER AS $$
DECLARE
    migrated_count INTEGER := 0;
    outbox_record RECORD;
BEGIN
    -- Migrate only PENDING messages from the old outbox to the new CDC outbox
    FOR outbox_record IN 
        SELECT 
            id,
            aggregate_id,
            event_id,
            event_type,
            payload,
            topic,
            metadata,
            created_at,
            updated_at
        FROM kafka_outbox 
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
    LOOP
        -- Insert into CDC outbox table
        INSERT INTO kafka_outbox_cdc (
            aggregate_id,
            event_id,
            event_type,
            payload,
            topic,
            metadata,
            created_at,
            updated_at
        ) VALUES (
            outbox_record.aggregate_id,
            outbox_record.event_id,
            outbox_record.event_type,
            outbox_record.payload,
            outbox_record.topic,
            outbox_record.metadata,
            outbox_record.created_at,
            outbox_record.updated_at
        )
        ON CONFLICT (event_id) DO NOTHING; -- Avoid duplicates
        
        migrated_count := migrated_count + 1;
    END LOOP;
    
    RETURN migrated_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get migration statistics
CREATE OR REPLACE FUNCTION get_outbox_migration_stats()
RETURNS TABLE (
    old_outbox_total INTEGER,
    old_outbox_pending INTEGER,
    old_outbox_processing INTEGER,
    old_outbox_processed INTEGER,
    old_outbox_failed INTEGER,
    cdc_outbox_total INTEGER,
    migration_ready BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*) FROM kafka_outbox) as old_outbox_total,
        (SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PENDING') as old_outbox_pending,
        (SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PROCESSING') as old_outbox_processing,
        (SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PROCESSED') as old_outbox_processed,
        (SELECT COUNT(*) FROM kafka_outbox WHERE status = 'FAILED') as old_outbox_failed,
        (SELECT COUNT(*) FROM kafka_outbox_cdc) as cdc_outbox_total,
        (SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PROCESSING') = 0 as migration_ready;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old outbox after successful migration
CREATE OR REPLACE FUNCTION cleanup_old_outbox_after_migration()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    -- Only delete if there are no processing messages
    IF (SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PROCESSING') = 0 THEN
        -- Delete all messages from old outbox (they should be in CDC outbox now)
        DELETE FROM kafka_outbox;
        GET DIAGNOSTICS deleted_count = ROW_COUNT;
    ELSE
        RAISE EXCEPTION 'Cannot cleanup old outbox: there are still processing messages';
    END IF;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Create a view to monitor migration progress
CREATE OR REPLACE VIEW outbox_migration_status AS
SELECT 
    'Old Outbox' as table_name,
    status,
    COUNT(*) as message_count,
    MIN(created_at) as oldest_message,
    MAX(created_at) as newest_message
FROM kafka_outbox 
GROUP BY status
UNION ALL
SELECT 
    'CDC Outbox' as table_name,
    'ALL' as status,
    COUNT(*) as message_count,
    MIN(created_at) as oldest_message,
    MAX(created_at) as newest_message
FROM kafka_outbox_cdc;

-- Add comments for documentation
COMMENT ON FUNCTION migrate_outbox_to_cdc() IS 'Migrates pending messages from old outbox to CDC outbox format';
COMMENT ON FUNCTION get_outbox_migration_stats() IS 'Returns statistics about outbox migration status';
COMMENT ON FUNCTION cleanup_old_outbox_after_migration() IS 'Cleans up old outbox after successful migration';
COMMENT ON VIEW outbox_migration_status IS 'View to monitor outbox migration progress'; 