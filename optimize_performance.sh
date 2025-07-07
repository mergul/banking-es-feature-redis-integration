#!/bin/bash

echo "🚀 Optimizing Redis and PostgreSQL for maximum concurrency..."

# Redis optimizations
echo "📊 Optimizing Redis settings..."
redis-cli CONFIG SET maxmemory 2gb
redis-cli CONFIG SET maxmemory-policy allkeys-lru
redis-cli CONFIG SET save ""
redis-cli CONFIG SET appendonly no
redis-cli CONFIG SET tcp-keepalive 300
redis-cli CONFIG SET timeout 0
redis-cli CONFIG SET tcp-backlog 511
redis-cli CONFIG SET databases 16
redis-cli CONFIG SET hash-max-ziplist-entries 512
redis-cli CONFIG SET hash-max-ziplist-value 64
redis-cli CONFIG SET list-max-ziplist-size -2
redis-cli CONFIG SET set-max-intset-entries 512
redis-cli CONFIG SET zset-max-ziplist-entries 128
redis-cli CONFIG SET zset-max-ziplist-value 64
redis-cli CONFIG SET hll-sparse-max-bytes 3000
redis-cli CONFIG SET activerehashing yes
redis-cli CONFIG SET client-output-buffer-limit normal 0 0 0
redis-cli CONFIG SET client-output-buffer-limit slave 256mb 64mb 60
redis-cli CONFIG SET client-output-buffer-limit pubsub 32mb 8mb 60
redis-cli CONFIG SET hz 10
redis-cli CONFIG SET aof-rewrite-incremental-fsync yes
redis-cli CONFIG SET rdb-save-incremental-fsync yes

echo "✅ Redis optimizations applied"

# PostgreSQL optimizations
echo "📊 Optimizing PostgreSQL settings..."
psql -U postgres -d banking_es -c "ALTER SYSTEM SET max_connections = 1000;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET shared_buffers = '2GB';"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET effective_cache_size = '8GB';"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET maintenance_work_mem = '512MB';"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET checkpoint_completion_target = 0.9;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET wal_buffers = '16MB';"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET default_statistics_target = 100;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET random_page_cost = 1.1;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET effective_io_concurrency = 200;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET work_mem = '64MB';"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET min_wal_size = '1GB';"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET max_wal_size = '4GB';"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET max_worker_processes = 8;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 4;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET max_parallel_workers = 8;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET max_parallel_maintenance_workers = 4;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET synchronous_commit = off;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET fsync = off;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET full_page_writes = off;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET commit_delay = 1000;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET commit_siblings = 5;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET autovacuum = on;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET autovacuum_max_workers = 3;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET autovacuum_naptime = 10;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET autovacuum_vacuum_threshold = 50;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET autovacuum_analyze_threshold = 50;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.1;"
psql -U postgres -d banking_es -c "ALTER SYSTEM SET autovacuum_analyze_scale_factor = 0.05;"

echo "✅ PostgreSQL optimizations applied"

# Reload PostgreSQL configuration
echo "🔄 Reloading PostgreSQL configuration..."
psql -U postgres -d banking_es -c "SELECT pg_reload_conf();"

echo "🎉 Performance optimizations completed!"
echo ""
echo "📋 Summary of optimizations:"
echo "  - Redis: Increased memory, optimized eviction, disabled persistence"
echo "  - PostgreSQL: Increased connections, optimized buffers, disabled fsync"
echo "  - Application: Increased account count to 5000, optimized cache and batch sizes"
echo ""
echo "🚀 Ready for high-throughput testing!" 