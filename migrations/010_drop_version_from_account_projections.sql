-- Drop the version column from account_projections if it exists
ALTER TABLE account_projections DROP COLUMN IF EXISTS version; 