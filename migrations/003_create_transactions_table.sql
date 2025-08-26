-- Create the main partitioned table with the unique constraint
CREATE TABLE IF NOT EXISTS transaction_projections (
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    amount DECIMAL(20,2) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Define the unique constraint directly on the parent table
    UNIQUE (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions with optimized fillfactor
-- This DO block remains the same to ensure idempotency
DO $$
BEGIN
    -- Check if the 2024_01 partition exists before creating it
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'transaction_projections_2024_01') THEN
        CREATE TABLE transaction_projections_2024_01 PARTITION OF transaction_projections
            FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
            WITH (fillfactor = 90);
    END IF;

    -- Check if the 2024_02 partition exists before creating it
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'transaction_projections_2024_02') THEN
        CREATE TABLE transaction_projections_2024_02 PARTITION OF transaction_projections
            FOR VALUES FROM ('2024-02-01') TO ('2025-03-01')
            WITH (fillfactor = 90);
    END IF;

    -- Check if the 2025_08 partition exists before creating it
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'transaction_projections_2025_08') THEN
        CREATE TABLE transaction_projections_2025_08 PARTITION OF transaction_projections
            FOR VALUES FROM ('2025-08-01') TO ('2025-09-01')
            WITH (fillfactor = 90);
    END IF;

    -- Check if the 2025_09 partition exists before creating it
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'transaction_projections_2025_09') THEN
        CREATE TABLE transaction_projections_2025_09 PARTITION OF transaction_projections
            FOR VALUES FROM ('2025-09-01') TO ('2025-10-01')
            WITH (fillfactor = 90);
    END IF;

END$$;

-- Create optimized indexes on the parent table
-- These indexes are automatically created on the child partitions
CREATE INDEX IF NOT EXISTS idx_transaction_projections_account_id
    ON transaction_projections (account_id, timestamp DESC)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_transaction_projections_type
    ON transaction_projections (transaction_type)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_transaction_projections_amount
    ON transaction_projections (amount)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_transaction_projections_account_type
    ON transaction_projections (account_id, transaction_type, timestamp DESC)
    WITH (fillfactor = 90);

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Transaction projections table created successfully!';
    RAISE NOTICE 'Table: transaction_projections';
    RAISE NOTICE 'Partitions: 2024_01, 2024_02, 2025_08, 2025_09';
    RAISE NOTICE 'Indexes: account_id, type, amount, account_type';
    RAISE NOTICE 'Constraints: unique (id, timestamp)';
END $$;
