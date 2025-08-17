-- Accounts Table Creation Script
-- This script creates the account_projections table with optimized indexes

-- Create account_projections table with optimized fillfactor
CREATE TABLE IF NOT EXISTS account_projections (
    id UUID PRIMARY KEY,
    owner_name VARCHAR(255) NOT NULL,
    balance DECIMAL(20,2) NOT NULL DEFAULT 0.00,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (fillfactor = 90);

-- Create optimized indexes for account_projections
CREATE INDEX IF NOT EXISTS idx_account_projections_owner_name
    ON account_projections (owner_name)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_account_projections_is_active
    ON account_projections (is_active)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_account_projections_created_at
    ON account_projections (created_at)
    WITH (fillfactor = 90);

-- Add composite index for common queries
CREATE INDEX IF NOT EXISTS idx_account_projections_active_created
    ON account_projections (is_active, created_at DESC)
    WITH (fillfactor = 90);

-- Add index for balance range queries
CREATE INDEX IF NOT EXISTS idx_account_projections_balance
    ON account_projections (balance)
    WITH (fillfactor = 90);

-- Add index for updated_at queries
CREATE INDEX IF NOT EXISTS idx_account_projections_updated_at
    ON account_projections (updated_at)
    WITH (fillfactor = 90);

-- Drop version column if it exists (as per migration 010)
ALTER TABLE account_projections DROP COLUMN IF EXISTS version;

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Account projections table created successfully!';
    RAISE NOTICE 'Table: account_projections';
    RAISE NOTICE 'Indexes: owner_name, is_active, created_at, active_created, balance, updated_at';
END $$; 