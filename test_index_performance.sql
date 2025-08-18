-- Test Index Performance for account_projections table
-- This script tests the performance improvements from the new indexes

-- 1. Test bulk read performance (ANY() queries)
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, is_active, created_at, updated_at 
FROM account_projections 
WHERE id = ANY(ARRAY[
    '550e8400-e29b-41d4-a716-446655440000'::uuid,
    '550e8400-e29b-41d4-a716-446655440001'::uuid,
    '550e8400-e29b-41d4-a716-446655440002'::uuid,
    '550e8400-e29b-41d4-a716-446655440003'::uuid,
    '550e8400-e29b-41d4-a716-446655440004'::uuid
]);

-- 2. Test covering index usage
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, is_active, created_at, updated_at 
FROM account_projections 
WHERE id = '550e8400-e29b-41d4-a716-446655440000'::uuid;

-- 3. Test active accounts query
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, created_at, updated_at 
FROM account_projections 
WHERE is_active = true 
ORDER BY created_at DESC 
LIMIT 100;

-- 4. Test balance range query
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, is_active, created_at, updated_at 
FROM account_projections 
WHERE balance BETWEEN 1000 AND 10000 
ORDER BY balance DESC;

-- 5. Test owner name search
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, is_active, created_at, updated_at 
FROM account_projections 
WHERE owner_name LIKE 'Test%';

-- 6. Test CDC optimized query
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, is_active, created_at 
FROM account_projections 
WHERE updated_at > NOW() - INTERVAL '1 hour' 
ORDER BY updated_at DESC;

-- 7. Test reporting query
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, updated_at 
FROM account_projections 
WHERE is_active = true 
ORDER BY balance DESC, created_at DESC 
LIMIT 50;

-- 8. Test concurrent read pattern
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, created_at, updated_at 
FROM account_projections 
WHERE id = ANY(ARRAY['550e8400-e29b-41d4-a716-446655440000'::uuid]) 
AND is_active = true;

-- 9. Test high frequency read pattern
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, created_at, updated_at 
FROM account_projections 
WHERE id = ANY(ARRAY['550e8400-e29b-41d4-a716-446655440000'::uuid]) 
AND is_active = true 
AND balance > 0;

-- 10. Test batch read optimization
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, owner_name, balance, is_active, created_at, updated_at 
FROM account_projections 
WHERE id = ANY(ARRAY[
    '550e8400-e29b-41d4-a716-446655440000'::uuid,
    '550e8400-e29b-41d4-a716-446655440001'::uuid,
    '550e8400-e29b-41d4-a716-446655440002'::uuid,
    '550e8400-e29b-41d4-a716-446655440003'::uuid,
    '550e8400-e29b-41d4-a716-446655440004'::uuid,
    '550e8400-e29b-41d4-a716-446655440005'::uuid,
    '550e8400-e29b-41d4-a716-446655440006'::uuid,
    '550e8400-e29b-41d4-a716-446655440007'::uuid,
    '550e8400-e29b-41d4-a716-446655440008'::uuid,
    '550e8400-e29b-41d4-a716-446655440009'::uuid
]);

-- 11. Show index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE tablename = 'account_projections' 
ORDER BY idx_scan DESC;

-- 12. Show table statistics
SELECT 
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del
FROM pg_stat_user_tables 
WHERE tablename = 'account_projections'; 