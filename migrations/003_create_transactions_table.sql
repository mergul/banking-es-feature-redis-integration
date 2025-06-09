-- 4. Transaction projections with optimized partitioning
CREATE TABLE IF NOT EXISTS transaction_projections (
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    amount DECIMAL(20,2) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions with optimized fillfactor
CREATE TABLE transaction_projections_2024_01 PARTITION OF transaction_projections
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
    WITH (fillfactor = 90);
CREATE TABLE transaction_projections_2024_02 PARTITION OF transaction_projections
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')
    WITH (fillfactor = 90);

-- Optimized indexes for transaction projections
CREATE INDEX IF NOT EXISTS idx_transaction_projections_account_id
    ON transaction_projections (account_id, timestamp DESC)
    WITH (fillfactor = 90);
CREATE INDEX IF NOT EXISTS idx_transaction_projections_type
    ON transaction_projections (transaction_type)
    WITH (fillfactor = 90);

-- Add partial index for recent transactions
-- CREATE INDEX IF NOT EXISTS idx_transaction_projections_recent
--     ON transaction_projections (timestamp)
--     WHERE timestamp > NOW() - INTERVAL '30 days'
--     WITH (fillfactor = 90);

-- Add index for amount range queries
CREATE INDEX IF NOT EXISTS idx_transaction_projections_amount
    ON transaction_projections (amount)
    WITH (fillfactor = 90);

-- Add composite index for account type queries
CREATE INDEX IF NOT EXISTS idx_transaction_projections_account_type
    ON transaction_projections (account_id, transaction_type, timestamp DESC)
    WITH (fillfactor = 90);

ALTER TABLE transaction_projections ADD CONSTRAINT unique_projections_id_timestamp 
    UNIQUE (id, timestamp);