-- Add error_details column to kafka_outbox table
-- This column will store error messages when message processing fails

ALTER TABLE kafka_outbox 
ADD COLUMN IF NOT EXISTS error_details TEXT; 