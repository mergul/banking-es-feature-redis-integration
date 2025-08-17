-- Outbox Tables Creation Script
-- This script creates Kafka outbox CDC table for event streaming
-- Includes alter operations from migrations 011 and 015

-- Create kafka_outbox_cdc table for CDC (Change Data Capture) implementation
-- This table is optimized for Debezium CDC processing without status tracking

CREATE TABLE IF NOT EXISTS kafka_outbox_cdc (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    topic VARCHAR(255) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create optimized indexes for CDC performance
CREATE INDEX IF NOT EXISTS idx_outbox_cdc_created_at ON kafka_outbox_cdc(created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_cdc_aggregate_id ON kafka_outbox_cdc(aggregate_id);
CREATE INDEX IF NOT EXISTS idx_outbox_cdc_event_id ON kafka_outbox_cdc(event_id);
CREATE INDEX IF NOT EXISTS idx_outbox_cdc_topic ON kafka_outbox_cdc(topic);

-- Enable logical replication for Debezium CDC
-- This is required for Debezium to capture changes
ALTER TABLE kafka_outbox_cdc REPLICA IDENTITY FULL;

-- Create a publication for the CDC table (if not exists)
-- This allows Debezium to subscribe to changes
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'banking_cdc_publication'
    ) THEN
        CREATE PUBLICATION banking_cdc_publication FOR TABLE kafka_outbox_cdc;
    END IF;
END $$;

-- Add comments for documentation
COMMENT ON TABLE kafka_outbox_cdc IS 'CDC-optimized outbox table for real-time event capture via Debezium';
COMMENT ON COLUMN kafka_outbox_cdc.id IS 'Primary key for the CDC outbox message';
COMMENT ON COLUMN kafka_outbox_cdc.aggregate_id IS 'ID of the aggregate root (e.g., account ID)';
COMMENT ON COLUMN kafka_outbox_cdc.event_id IS 'Unique identifier for the domain event';
COMMENT ON COLUMN kafka_outbox_cdc.event_type IS 'Type of the domain event (e.g., AccountCreated, MoneyDeposited)';
COMMENT ON COLUMN kafka_outbox_cdc.payload IS 'Serialized domain event data (binary format)';
COMMENT ON COLUMN kafka_outbox_cdc.topic IS 'Kafka topic where the event should be published';
COMMENT ON COLUMN kafka_outbox_cdc.metadata IS 'Additional metadata for the event (JSON format)';
COMMENT ON COLUMN kafka_outbox_cdc.created_at IS 'Timestamp when the message was created';
COMMENT ON COLUMN kafka_outbox_cdc.updated_at IS 'Timestamp when the message was last updated';

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_outbox_cdc_updated_at 
    BEFORE UPDATE ON kafka_outbox_cdc 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Grant necessary permissions for Debezium
-- Note: In production, use more restrictive permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka_outbox_cdc TO postgres;
GRANT USAGE ON SCHEMA public TO postgres;

-- Add deleted_at column for advanced cleanup (as per migration 015)
ALTER TABLE kafka_outbox_cdc 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;

-- Create index on deleted_at for efficient cleanup queries (as per migration 015)
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_cdc_deleted_at 
ON kafka_outbox_cdc (deleted_at);

-- Create composite index for cleanup operations (as per migration 015)
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_cdc_cleanup 
ON kafka_outbox_cdc (created_at, deleted_at) 
WHERE deleted_at IS NULL;

-- Add comment for documentation (as per migration 015)
COMMENT ON COLUMN kafka_outbox_cdc.deleted_at IS 'Timestamp when record was marked for deletion (used by advanced cleanup service)';

-- Function to get cleanup statistics (as per migration 015)
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

-- Function to manually trigger cleanup (for admin operations) (as per migration 015)
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

-- Add comments for documentation (as per migration 015)
COMMENT ON FUNCTION get_cdc_outbox_cleanup_stats() IS 'Returns statistics about CDC outbox cleanup status';
COMMENT ON FUNCTION trigger_cdc_outbox_cleanup(INTEGER, INTEGER) IS 'Manually triggers CDC outbox cleanup with specified retention and safety margin';

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Outbox table created successfully!';
    RAISE NOTICE 'Table: kafka_outbox_cdc';
    RAISE NOTICE 'Indexes: created_at, aggregate_id, event_id, topic, deleted_at, cleanup';
    RAISE NOTICE 'CDC: Replica identity FULL, publication banking_cdc_publication';
    RAISE NOTICE 'Triggers: update_outbox_cdc_updated_at';
    RAISE NOTICE 'Cleanup: deleted_at column, cleanup functions available';
END $$; 