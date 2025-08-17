-- -- Create materialized views for better query performance
-- -- This migration adds optimized views for common queries

-- -- Materialized view for active accounts with balance index
-- CREATE MATERIALIZED VIEW IF NOT EXISTS account_projections_mv AS
-- SELECT id, owner_name, balance, is_active, created_at, updated_at
-- FROM account_projections
-- WHERE is_active = true;

-- -- Create indexes on the materialized view for better performance
-- CREATE INDEX IF NOT EXISTS idx_account_projections_mv_balance 
-- ON account_projections_mv(balance DESC);

-- CREATE INDEX IF NOT EXISTS idx_account_projections_mv_updated_at 
-- ON account_projections_mv(updated_at DESC);

-- CREATE INDEX IF NOT EXISTS idx_account_projections_mv_owner_name 
-- ON account_projections_mv(owner_name);

-- -- Materialized view for transaction summaries
-- CREATE MATERIALIZED VIEW IF NOT EXISTS transaction_summary_mv AS
-- SELECT 
--     account_id,
--     COUNT(*) as transaction_count,
--     SUM(CASE WHEN transaction_type = 'MoneyDeposited' THEN amount ELSE 0 END) as total_deposits,
--     SUM(CASE WHEN transaction_type = 'MoneyWithdrawn' THEN amount ELSE 0 END) as total_withdrawals,
--     MAX(timestamp) as last_transaction_time
-- FROM transaction_projections
-- GROUP BY account_id;

-- -- Create indexes on transaction summary view
-- CREATE INDEX IF NOT EXISTS idx_transaction_summary_mv_account_id 
-- ON transaction_summary_mv(account_id);

-- CREATE INDEX IF NOT EXISTS idx_transaction_summary_mv_last_transaction 
-- ON transaction_summary_mv(last_transaction_time DESC);

-- -- Function to refresh materialized views
-- CREATE OR REPLACE FUNCTION refresh_materialized_views()
-- RETURNS void AS $$
-- BEGIN
--     REFRESH MATERIALIZED VIEW CONCURRENTLY account_projections_mv;
--     REFRESH MATERIALIZED VIEW CONCURRENTLY transaction_summary_mv;
-- END;
-- $$ LANGUAGE plpgsql;

-- -- Create a trigger to refresh materialized views when base tables change
-- CREATE OR REPLACE FUNCTION trigger_refresh_materialized_views()
-- RETURNS trigger AS $$
-- BEGIN
--     -- Schedule a refresh (in production, you might want to use a job queue)
--     PERFORM pg_notify('refresh_materialized_views', '');
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;

-- -- Create triggers on base tables
-- DROP TRIGGER IF EXISTS trigger_refresh_account_projections_mv ON account_projections;
-- CREATE TRIGGER trigger_refresh_account_projections_mv
--     AFTER INSERT OR UPDATE OR DELETE ON account_projections
--     FOR EACH ROW EXECUTE FUNCTION trigger_refresh_materialized_views();

-- DROP TRIGGER IF EXISTS trigger_refresh_transaction_projections_mv ON transaction_projections;
-- CREATE TRIGGER trigger_refresh_transaction_projections_mv
--     AFTER INSERT OR UPDATE OR DELETE ON transaction_projections
--     FOR EACH ROW EXECUTE FUNCTION trigger_refresh_materialized_views();

-- -- Add additional indexes on base tables for better performance
-- CREATE INDEX IF NOT EXISTS idx_account_projections_balance_active 
-- ON account_projections(balance DESC) WHERE is_active = true;

-- CREATE INDEX IF NOT EXISTS idx_transaction_projections_account_type 
-- ON transaction_projections(account_id, transaction_type);

-- CREATE INDEX IF NOT EXISTS idx_transaction_projections_timestamp 
-- ON transaction_projections(timestamp DESC);

-- -- Create a function to get top accounts by balance
-- CREATE OR REPLACE FUNCTION get_top_accounts_by_balance(limit_count integer DEFAULT 10)
-- RETURNS TABLE(
--     id uuid,
--     owner_name text,
--     balance decimal,
--     is_active boolean,
--     created_at timestamptz,
--     updated_at timestamptz
-- ) AS $$
-- BEGIN
--     RETURN QUERY
--     SELECT ap.id, ap.owner_name, ap.balance, ap.is_active, ap.created_at, ap.updated_at
--     FROM account_projections_mv ap
--     WHERE ap.is_active = true
--     ORDER BY ap.balance DESC
--     LIMIT limit_count;
-- END;
-- $$ LANGUAGE plpgsql;

-- -- Create a function to get accounts by balance range
-- CREATE OR REPLACE FUNCTION get_accounts_by_balance_range(
--     min_balance decimal,
--     max_balance decimal,
--     limit_count integer DEFAULT 1000
-- )
-- RETURNS TABLE(
--     id uuid,
--     owner_name text,
--     balance decimal,
--     is_active boolean,
--     created_at timestamptz,
--     updated_at timestamptz
-- ) AS $$
-- BEGIN
--     RETURN QUERY
--     SELECT ap.id, ap.owner_name, ap.balance, ap.is_active, ap.created_at, ap.updated_at
--     FROM account_projections_mv ap
--     WHERE ap.is_active = true 
--     AND ap.balance BETWEEN min_balance AND max_balance
--     ORDER BY ap.balance DESC
--     LIMIT limit_count;
-- END;
-- $$ LANGUAGE plpgsql; 