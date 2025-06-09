-- 3. Optimized projection tables with fillfactor
CREATE TABLE IF NOT EXISTS account_projections (
    id UUID PRIMARY KEY,
    owner_name VARCHAR(255) NOT NULL,
    balance DECIMAL(20,2) NOT NULL DEFAULT 0.00,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (fillfactor = 90);

-- Optimized indexes for account projections
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
