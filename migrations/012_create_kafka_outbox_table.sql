-- Create kafka_outbox table for outbox pattern implementation
CREATE TABLE IF NOT EXISTS kafka_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,
    event_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    topic VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    metadata JSONB,
    error_details TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_status ON kafka_outbox(status);
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_created_at ON kafka_outbox(created_at);
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_aggregate_id ON kafka_outbox(aggregate_id);
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_event_id ON kafka_outbox(event_id); 