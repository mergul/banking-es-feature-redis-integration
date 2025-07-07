-- Migration: Add version column to account_projections table
-- This migration adds a version column to track optimistic concurrency control
-- and ensure proper event ordering in the projection

-- Add version column with default value 0
ALTER TABLE account_projections ADD COLUMN version BIGINT DEFAULT 0;

-- Create index on version for better query performance
CREATE INDEX idx_account_projections_version ON account_projections(version);

-- Add comment to document the column purpose
COMMENT ON COLUMN account_projections.version IS 'Version number for optimistic concurrency control and event ordering'; 