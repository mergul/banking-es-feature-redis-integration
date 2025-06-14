-- Add unique constraint for Optimistic Concurrency Control
-- This ensures that for a given aggregate_id, each version number is unique.
ALTER TABLE events
ADD CONSTRAINT unique_aggregate_id_version UNIQUE (aggregate_id, version);
