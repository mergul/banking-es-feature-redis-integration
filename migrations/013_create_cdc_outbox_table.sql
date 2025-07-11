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