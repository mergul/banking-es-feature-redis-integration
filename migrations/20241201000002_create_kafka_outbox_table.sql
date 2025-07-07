-- Migration: Create kafka_outbox table for outbox pattern
-- This table stores messages that need to be published to Kafka
-- It ensures reliable message delivery even if Kafka is temporarily unavailable

CREATE TABLE IF NOT EXISTS kafka_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,
    event_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    topic VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    retry_count INTEGER NOT NULL DEFAULT 0,
    metadata JSONB
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_status ON kafka_outbox(status);
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_created_at ON kafka_outbox(created_at);
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_aggregate_id ON kafka_outbox(aggregate_id);
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_event_id ON kafka_outbox(event_id);
CREATE INDEX IF NOT EXISTS idx_kafka_outbox_processing ON kafka_outbox(status, created_at) WHERE status IN ('PENDING', 'PROCESSING');

-- Add comments for documentation
COMMENT ON TABLE kafka_outbox IS 'Outbox table for reliable Kafka message delivery';
COMMENT ON COLUMN kafka_outbox.status IS 'Message status: PENDING, PROCESSING, PROCESSED, FAILED';
COMMENT ON COLUMN kafka_outbox.retry_count IS 'Number of retry attempts for failed messages';
COMMENT ON COLUMN kafka_outbox.metadata IS 'Additional metadata for the message (Kafka headers, etc.)'; 