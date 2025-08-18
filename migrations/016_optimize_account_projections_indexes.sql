-- Migration: 016_optimize_account_projections_indexes.sql
-- Description: Add covering indexes and optimizations for account_projections table
-- Date: 2025-08-18
-- Author: System

-- 1. Add covering index for bulk read operations (ANY() queries)
-- This will dramatically improve the slow SELECT queries with id = ANY($1)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_covering_read 
ON account_projections (id) 
INCLUDE (owner_name, balance, is_active, created_at, updated_at)
WITH (fillfactor = 90);

-- 2. Add composite index optimized for bulk operations
-- This provides better performance for large batch reads
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_bulk_optimized 
ON account_projections (id, owner_name, balance, is_active, created_at, updated_at)
WITH (fillfactor = 90);

-- 3. Add partial index for active accounts only (most common query pattern)
-- This reduces index size and improves performance for active account queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_active_covering 
ON account_projections (id) 
INCLUDE (owner_name, balance, created_at, updated_at)
WHERE is_active = true;

-- 4. Add index for balance range queries with covering columns
-- Optimizes queries that filter by balance ranges
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_balance_covering 
ON account_projections (balance DESC, id) 
INCLUDE (owner_name, is_active, created_at, updated_at)
WITH (fillfactor = 90);

-- 5. Add index for updated_at queries (useful for CDC and sync operations)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_updated_at_covering 
ON account_projections (updated_at DESC, id) 
INCLUDE (owner_name, balance, is_active, created_at)
WITH (fillfactor = 90);

-- 6. Add index for owner_name searches with covering columns
-- Optimizes account lookup by owner name
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_owner_covering 
ON account_projections (owner_name, id) 
INCLUDE (balance, is_active, created_at, updated_at)
WITH (fillfactor = 90);

-- 7. Add composite index for common filtering patterns
-- Optimizes queries that filter by multiple conditions
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_filter_covering 
ON account_projections (is_active, balance DESC, created_at DESC, id) 
INCLUDE (owner_name, updated_at)
WITH (fillfactor = 90);

-- 8. Add index for created_at range queries
-- Useful for time-based account queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_created_at_covering 
ON account_projections (created_at DESC, id) 
INCLUDE (owner_name, balance, is_active, updated_at)
WITH (fillfactor = 90);

-- 9. Add unique constraint index for id lookups (if not already exists)
-- Ensures fast primary key lookups
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_id_unique 
ON account_projections (id)
WITH (fillfactor = 90);

-- 10. Add index for concurrent bulk operations
-- Optimizes parallel read operations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_concurrent_read 
ON account_projections (id, is_active) 
INCLUDE (owner_name, balance, created_at, updated_at)
WITH (fillfactor = 90);

-- 11. Add index for transaction-related queries
-- Optimizes queries that join with transaction data
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_transaction_ready 
ON account_projections (id, is_active, balance) 
INCLUDE (owner_name, created_at, updated_at)
WITH (fillfactor = 90);

-- 12. Add index for reporting queries
-- Optimizes analytical queries and reports
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_reporting 
ON account_projections (is_active, balance DESC, created_at DESC) 
INCLUDE (id, owner_name, updated_at);

-- 13. Add index for CDC operations
-- Optimizes change data capture operations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_cdc_optimized 
ON account_projections (updated_at DESC, id) 
INCLUDE (owner_name, balance, is_active, created_at)
WITH (fillfactor = 90);

-- 14. Add index for high-frequency read patterns
-- Optimizes the most common read patterns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_high_freq_read 
ON account_projections (id, is_active, balance) 
INCLUDE (owner_name, created_at, updated_at)
WITH (fillfactor = 90);

-- 15. Add index for batch operations
-- Specifically optimized for batch read operations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_batch_read 
ON account_projections (id) 
INCLUDE (owner_name, balance, is_active, created_at, updated_at)
WITH (fillfactor = 90);

-- 16. Add index for performance monitoring
-- Helps with query performance analysis
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_perf_monitor 
ON account_projections (created_at, updated_at, id) 
INCLUDE (owner_name, balance, is_active)
WITH (fillfactor = 90);

-- 17. Add index for data consistency checks
-- Optimizes data integrity verification queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_consistency 
ON account_projections (id, updated_at DESC) 
INCLUDE (owner_name, balance, is_active, created_at)
WITH (fillfactor = 90);

-- 18. Add index for load balancing
-- Optimizes queries across multiple read replicas
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_load_balanced 
ON account_projections (id, is_active) 
INCLUDE (owner_name, balance, created_at, updated_at)
WITH (fillfactor = 90);

-- 19. Add index for cache warming
-- Optimizes cache warming operations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_cache_warm 
ON account_projections (id, is_active, balance DESC) 
INCLUDE (owner_name, created_at, updated_at)
WITH (fillfactor = 90);

-- 20. Add index for emergency queries
-- Optimizes critical system queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_emergency 
ON account_projections (id, is_active, balance, created_at) 
INCLUDE (owner_name, updated_at)
WITH (fillfactor = 90);

-- Update table statistics to help query planner
ANALYZE account_projections;

-- Add comment to document the optimization
COMMENT ON INDEX idx_account_projections_covering_read IS 'Covering index for bulk read operations - optimizes id = ANY($1) queries';
COMMENT ON INDEX idx_account_projections_bulk_optimized IS 'Composite index for large batch operations';
COMMENT ON INDEX idx_account_projections_active_covering IS 'Partial covering index for active accounts only';
COMMENT ON INDEX idx_account_projections_balance_covering IS 'Covering index for balance range queries';
COMMENT ON INDEX idx_account_projections_updated_at_covering IS 'Covering index for CDC and sync operations';
COMMENT ON INDEX idx_account_projections_owner_covering IS 'Covering index for owner name lookups';
COMMENT ON INDEX idx_account_projections_filter_covering IS 'Composite covering index for multi-condition filters';
COMMENT ON INDEX idx_account_projections_created_at_covering IS 'Covering index for time-based queries';
COMMENT ON INDEX idx_account_projections_id_unique IS 'Unique index for primary key lookups';
COMMENT ON INDEX idx_account_projections_concurrent_read IS 'Index optimized for concurrent read operations';
COMMENT ON INDEX idx_account_projections_transaction_ready IS 'Index optimized for transaction-related queries';
COMMENT ON INDEX idx_account_projections_reporting IS 'Index optimized for reporting and analytics';
COMMENT ON INDEX idx_account_projections_cdc_optimized IS 'Index optimized for change data capture';
COMMENT ON INDEX idx_account_projections_high_freq_read IS 'Index for high-frequency read patterns';
COMMENT ON INDEX idx_account_projections_batch_read IS 'Index specifically for batch read operations';
COMMENT ON INDEX idx_account_projections_perf_monitor IS 'Index for performance monitoring queries';
COMMENT ON INDEX idx_account_projections_consistency IS 'Index for data consistency verification';
COMMENT ON INDEX idx_account_projections_load_balanced IS 'Index for load-balanced read operations';
COMMENT ON INDEX idx_account_projections_cache_warm IS 'Index for cache warming operations';
COMMENT ON INDEX idx_account_projections_emergency IS 'Index for emergency system queries'; 