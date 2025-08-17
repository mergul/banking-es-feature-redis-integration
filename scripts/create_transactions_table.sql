-- Transactions Table Creation Script
-- This script creates the transaction_projections table with optimized partitioning

-- Create transaction_projections table with optimized partitioning
CREATE TABLE IF NOT EXISTS transaction_projections (
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    amount DECIMAL(20,2) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions with optimized fillfactor
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'transaction_projections_2024_01') THEN
        CREATE TABLE transaction_projections_2024_01 PARTITION OF transaction_projections
            FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
            WITH (fillfactor = 90);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'transaction_projections_2024_02') THEN
        CREATE TABLE transaction_projections_2024_02 PARTITION OF transaction_projections
            FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')
            WITH (fillfactor = 90);
    END IF;
END$$;

-- Create optimized indexes for transaction_projections
CREATE INDEX IF NOT EXISTS idx_transaction_projections_account_id
    ON transaction_projections (account_id, timestamp DESC)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_transaction_projections_type
    ON transaction_projections (transaction_type)
    WITH (fillfactor = 90);

-- Add index for amount range queries
CREATE INDEX IF NOT EXISTS idx_transaction_projections_amount
    ON transaction_projections (amount)
    WITH (fillfactor = 90);

-- Add composite index for account type queries
CREATE INDEX IF NOT EXISTS idx_transaction_projections_account_type
    ON transaction_projections (account_id, transaction_type, timestamp DESC)
    WITH (fillfactor = 90);

-- Add unique constraint to each partition instead of parent table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_projections_id_timestamp_2024_01'
    ) THEN
        ALTER TABLE transaction_projections_2024_01 
            ADD CONSTRAINT unique_projections_id_timestamp_2024_01 
            UNIQUE (id, timestamp);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_projections_id_timestamp_2024_02'
    ) THEN
        ALTER TABLE transaction_projections_2024_02 
            ADD CONSTRAINT unique_projections_id_timestamp_2024_02 
            UNIQUE (id, timestamp);
    END IF;
END$$;

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Transaction projections table created successfully!';
    RAISE NOTICE 'Table: transaction_projections';
    RAISE NOTICE 'Partitions: 2024_01, 2024_02';
    RAISE NOTICE 'Indexes: account_id, type, amount, account_type';
    RAISE NOTICE 'Constraints: unique_projections_id_timestamp_2024_01, unique_projections_id_timestamp_2024_02';
END $$; 