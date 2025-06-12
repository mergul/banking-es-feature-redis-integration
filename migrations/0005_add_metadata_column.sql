-- Add migration script here

-- Add metadata column to events table
ALTER TABLE events ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Add GIN index for metadata queries
CREATE INDEX IF NOT EXISTS idx_events_metadata_gin
    ON events USING GIN (metadata jsonb_path_ops);
