# PostgreSQL Optimization Guide for High Concurrency Banking Application

## Overview

This guide provides comprehensive instructions for optimizing PostgreSQL settings to handle high concurrency workloads in your banking application. The optimizations focus on:

- **Connection Management**: Handling 200+ concurrent write connections and 500+ read connections
- **Memory Management**: Optimizing shared buffers, work memory, and cache usage
- **Write Performance**: Improving transaction throughput and reducing conflicts
- **Monitoring**: Tracking performance metrics and connection usage

## 🚀 Quick Start

### 1. Run the Optimization Script

```bash
# Check current settings
./scripts/optimize_postgresql.sh check

# Create optimized configuration
./scripts/optimize_postgresql.sh create-config

# Apply settings via SQL (requires restart)
./scripts/optimize_postgresql.sh apply-sql

# Run all optimizations
./scripts/optimize_postgresql.sh all
```

### 2. Restart PostgreSQL

```bash
# Ubuntu/Debian
sudo systemctl restart postgresql

# CentOS/RHEL
sudo systemctl restart postgresql-13

# macOS (Homebrew)
brew services restart postgresql
```

## 📊 Key PostgreSQL Settings

### Connection Settings

```sql
-- Maximum concurrent connections (increased from default 100)
max_connections = 1000

-- Reserved connections for superuser
superuser_reserved_connections = 3
```

**Calculation**: `max_connections = (CPU cores × 4) + 100`

### Memory Settings

```sql
-- Shared buffers (25% of total RAM)
shared_buffers = 2GB

-- Effective cache size (75% of total RAM)
effective_cache_size = 6GB

-- Work memory per operation
work_mem = 16MB

-- Maintenance work memory
maintenance_work_mem = 256MB
```

**Calculations**:

- `shared_buffers = RAM / 4`
- `effective_cache_size = RAM × 3/4`
- `work_mem = Available RAM / (CPU cores × 2)`
- `maintenance_work_mem = Available RAM / 8`

### Write-Ahead Logging (WAL)

```sql
-- WAL buffers
wal_buffers = 16MB

-- Checkpoint completion target
checkpoint_completion_target = 0.9

-- WAL writer delay
wal_writer_delay = 200ms

-- Commit delay for batching
commit_delay = 1000
commit_siblings = 5
```

### Query Planner

```sql
-- Random page cost (for SSD)
random_page_cost = 1.1

-- Effective I/O concurrency
effective_io_concurrency = 200
```

### Background Writer

```sql
-- Background writer delay
bgwriter_delay = 200ms

-- Background writer LRU settings
bgwriter_lru_maxpages = 100
bgwriter_lru_multiplier = 2.0
```

### Autovacuum

```sql
-- Enable autovacuum
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = 1min

-- Autovacuum thresholds
autovacuum_vacuum_threshold = 50
autovacuum_analyze_threshold = 50
autovacuum_vacuum_scale_factor = 0.2
autovacuum_analyze_scale_factor = 0.1
```

## 🔧 Manual Configuration

### 1. Locate PostgreSQL Configuration

```bash
# Find postgresql.conf location
sudo find /etc -name "postgresql.conf" 2>/dev/null
sudo find /var/lib -name "postgresql.conf" 2>/dev/null

# Common locations:
# Ubuntu/Debian: /etc/postgresql/*/main/postgresql.conf
# CentOS/RHEL: /var/lib/pgsql/data/postgresql.conf
# macOS: /usr/local/var/postgresql@13/postgresql.conf
```

### 2. Backup Current Configuration

```bash
sudo cp /etc/postgresql/*/main/postgresql.conf /etc/postgresql/*/main/postgresql.conf.backup.$(date +%Y%m%d)
```

### 3. Apply Optimized Settings

```bash
# Copy the generated configuration
sudo cp postgresql_optimized.conf /etc/postgresql/*/main/postgresql.conf

# Or apply via SQL (requires restart)
psql -h localhost -U postgres -d banking_es -f apply_settings.sql
```

## 📈 Performance Monitoring

### Connection Usage

```sql
-- Check current connection usage
SELECT
    setting as max_connections,
    (SELECT count(*) FROM pg_stat_activity) as current_connections,
    (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
    (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle') as idle_connections,
    ROUND(
        (SELECT count(*) FROM pg_stat_activity)::numeric /
        (SELECT setting FROM pg_settings WHERE name = 'max_connections')::numeric * 100, 2
    ) as connection_usage_percent
FROM pg_settings
WHERE name = 'max_connections';
```

### Lock Monitoring

```sql
-- Check for blocking queries
SELECT
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS current_statement_in_blocking_process
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks
    ON (blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid)
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
```

### Performance Statistics

```sql
-- Check table statistics
SELECT
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats
WHERE schemaname = 'public'
ORDER BY n_distinct DESC
LIMIT 10;

-- Check index usage
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. "FATAL: remaining connection slots are reserved for non-replication superuser connections"

**Solution**: Increase `max_connections`

```sql
ALTER SYSTEM SET max_connections = '1000';
SELECT pg_reload_conf();
```

#### 2. "could not serialize access due to read/write dependencies among transactions"

**Solutions**:

- Increase `shared_buffers`
- Optimize transaction isolation levels
- Use write batching (implemented in your application)

```sql
-- Increase shared buffers
ALTER SYSTEM SET shared_buffers = '2GB';

-- Adjust transaction isolation
SET default_transaction_isolation = 'read committed';
```

#### 3. High Memory Usage

**Solutions**:

- Monitor `work_mem` usage
- Adjust `maintenance_work_mem`
- Check for memory leaks

```sql
-- Check memory usage
SELECT
    name,
    setting,
    unit,
    context
FROM pg_settings
WHERE name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size');
```

### Performance Tuning

#### 1. Query Optimization

```sql
-- Enable query logging
ALTER SYSTEM SET log_min_duration_statement = '1000';
ALTER SYSTEM SET log_checkpoints = 'on';
ALTER SYSTEM SET log_connections = 'on';
ALTER SYSTEM SET log_disconnections = 'on';
ALTER SYSTEM SET log_lock_waits = 'on';

-- Reload configuration
SELECT pg_reload_conf();
```

#### 2. Index Optimization

```sql
-- Analyze tables for better query planning
ANALYZE accounts;
ANALYZE account_events;
ANALYZE account_projections;

-- Check for missing indexes
SELECT
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats
WHERE schemaname = 'public'
AND n_distinct > 1000
AND correlation < 0.1;
```

## 🔄 Maintenance

### Regular Maintenance Tasks

```sql
-- Update table statistics
ANALYZE;

-- Vacuum tables
VACUUM ANALYZE;

-- Reindex if needed
REINDEX DATABASE banking_es;
```

### Automated Maintenance

```sql
-- Enable autovacuum for all tables
ALTER TABLE accounts SET (autovacuum_enabled = on);
ALTER TABLE account_events SET (autovacuum_enabled = on);
ALTER TABLE account_projections SET (autovacuum_enabled = on);
```

## 📊 Monitoring Dashboard

Create a monitoring dashboard with these key metrics:

1. **Connection Usage**: Current vs max connections
2. **Lock Wait Time**: Average time spent waiting for locks
3. **Transaction Rate**: Transactions per second
4. **Cache Hit Ratio**: Shared buffer hit ratio
5. **WAL Generation**: Write-ahead log generation rate
6. **Checkpoint Frequency**: Checkpoint completion time

## 🚀 Advanced Optimizations

### 1. Connection Pooling

Consider using PgBouncer for connection pooling:

```bash
# Install PgBouncer
sudo apt-get install pgbouncer

# Configure PgBouncer
sudo nano /etc/pgbouncer/pgbouncer.ini
```

### 2. Partitioning

For large tables, consider partitioning:

```sql
-- Example: Partition account_events by date
CREATE TABLE account_events (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP NOT NULL
) PARTITION BY RANGE (created_at);

-- Create partitions
CREATE TABLE account_events_2024_01 PARTITION OF account_events
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

### 3. Read Replicas

For read-heavy workloads, consider read replicas:

```sql
-- On primary server
ALTER SYSTEM SET wal_level = 'replica';
ALTER SYSTEM SET max_wal_senders = 3;
ALTER SYSTEM SET wal_keep_segments = 32;
```

## 📝 Configuration Checklist

- [ ] Increase `max_connections` to 1000+
- [ ] Set `shared_buffers` to 25% of RAM
- [ ] Configure `effective_cache_size` to 75% of RAM
- [ ] Optimize `work_mem` based on CPU cores
- [ ] Enable autovacuum with appropriate thresholds
- [ ] Configure WAL settings for write performance
- [ ] Set up monitoring and logging
- [ ] Test with your application's load patterns
- [ ] Monitor performance metrics
- [ ] Adjust settings based on real-world usage

## 🔗 Additional Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [PostgreSQL Performance Tuning](https://www.postgresql.org/docs/current/runtime-config-resource.html)
- [Connection Pooling with PgBouncer](https://www.pgbouncer.org/)
- [PostgreSQL Monitoring](https://www.postgresql.org/docs/current/monitoring.html)

---

**Note**: Always test configuration changes in a staging environment before applying to production. Monitor system resources (CPU, memory, disk I/O) when making changes to ensure optimal performance.
