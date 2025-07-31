-- Migration: Add deleted_at column to kafka_outbox_cdc table for advanced cleanup
-- This enables the two-phase cleanup strategy: mark for deletion, then physically delete

-- Add deleted_at column to kafka_outbox_cdc table
ALTER TABLE kafka_outbox_cdc 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;

-- Create index on deleted_at for efficient cleanup queries
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_cdc_deleted_at 
ON kafka_outbox_cdc (deleted_at);

-- Create index on created_at for efficient retention queries
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_cdc_created_at 
ON kafka_outbox_cdc (created_at);

-- Create composite index for cleanup operations
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_cdc_cleanup 
ON kafka_outbox_cdc (created_at, deleted_at) 
WHERE deleted_at IS NULL;

-- Add comment for documentation
COMMENT ON COLUMN kafka_outbox_cdc.deleted_at IS 'Timestamp when record was marked for deletion (used by advanced cleanup service)';

-- Function to get cleanup statistics
CREATE OR REPLACE FUNCTION get_cdc_outbox_cleanup_stats()
RETURNS TABLE (
    total_records BIGINT,
    marked_for_deletion BIGINT,
    active_records BIGINT,
    oldest_record TIMESTAMP,
    newest_record TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) as marked_for_deletion,
        COUNT(*) FILTER (WHERE deleted_at IS NULL) as active_records,
        MIN(created_at) as oldest_record,
        MAX(created_at) as newest_record
    FROM kafka_outbox_cdc;
END;
$$ LANGUAGE plpgsql;

-- Function to manually trigger cleanup (for admin operations)
CREATE OR REPLACE FUNCTION trigger_cdc_outbox_cleanup(
    retention_hours INTEGER DEFAULT 24,
    safety_margin_minutes INTEGER DEFAULT 30
)
RETURNS TABLE (
    marked_count BIGINT,
    deleted_count BIGINT
) AS $$
DECLARE
    mark_cutoff TIMESTAMP;
    delete_cutoff TIMESTAMP;
    marked BIGINT := 0;
    deleted BIGINT := 0;
BEGIN
    -- Phase 1: Mark old records for deletion
    mark_cutoff := NOW() - INTERVAL '1 hour' * retention_hours;
    
    UPDATE kafka_outbox_cdc 
    SET deleted_at = NOW() 
    WHERE created_at < mark_cutoff 
    AND deleted_at IS NULL;
    
    GET DIAGNOSTICS marked = ROW_COUNT;
    
    -- Phase 2: Physically delete marked records after safety margin
    delete_cutoff := NOW() - INTERVAL '1 minute' * safety_margin_minutes;
    
    DELETE FROM kafka_outbox_cdc 
    WHERE deleted_at IS NOT NULL 
    AND deleted_at < delete_cutoff;
    
    GET DIAGNOSTICS deleted = ROW_COUNT;
    
    RETURN QUERY SELECT marked, deleted;
END;
$$ LANGUAGE plpgsql;

-- Add comments for documentation
COMMENT ON FUNCTION get_cdc_outbox_cleanup_stats() IS 'Returns statistics about CDC outbox cleanup status';
COMMENT ON FUNCTION trigger_cdc_outbox_cleanup(INTEGER, INTEGER) IS 'Manually triggers CDC outbox cleanup with specified retention and safety margin'; 