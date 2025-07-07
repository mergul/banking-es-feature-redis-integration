# Banking Event Sourcing System - Troubleshooting Guide

## Overview

This guide provides comprehensive troubleshooting steps for resolving stuck operations and performance issues in the banking event sourcing system.

## Common Issues and Solutions

### 1. Database Connection Pool Exhaustion

**Symptoms:**
- High connection pool utilization (>80%)
- Connection timeout errors
- Slow query response times
- "Too many connections" errors

**Causes:**
- Long-running transactions
- Connection leaks
- Insufficient pool size
- Database performance issues

**Solutions:**

```bash
# Check current pool status
curl -X GET http://localhost:3000/health/pool

# Monitor pool metrics
curl -X GET http://localhost:3000/metrics/pool
```

**Configuration Adjustments:**
```env
# Increase pool size
POOL_MAX_CONNECTIONS=200
POOL_MIN_CONNECTIONS=20

# Adjust timeouts
CONNECTION_ACQUIRE_TIMEOUT=15
TRANSACTION_TIMEOUT=60

# Enable auto-scaling
POOL_AUTO_SCALING=true
```

**Code Fixes:**
```rust
// Use connection with timeout
let conn = tokio::time::timeout(
    Duration::from_secs(10),
    pool.acquire()
).await??;

// Ensure connections are properly released
// Use RAII patterns or explicit drop()
```

### 2. Deadlock Detection and Resolution

**Symptoms:**
- Operations hanging indefinitely
- High deadlock count in metrics
- Optimistic concurrency conflicts
- Transaction rollbacks

**Causes:**
- Concurrent modifications to same aggregate
- Long-running transactions
- Improper transaction isolation levels
- Resource contention

**Solutions:**

```bash
# Check deadlock statistics
curl -X GET http://localhost:3000/health/deadlocks

# View active operations
curl -X GET http://localhost:3000/debug/active-operations
```

**Configuration:**
```env
# Deadlock detection settings
DEADLOCK_CHECK_INTERVAL=5
DEADLOCK_OPERATION_TIMEOUT=30
MAX_CONCURRENT_OPERATIONS=1000
ENABLE_AUTO_RESOLUTION=true
```

**Code Patterns:**
```rust
// Use proper version checking
let current_version = event_store.get_current_version(account_id).await?;
if current_version != expected_version {
    return Err(EventStoreError::OptimisticConcurrencyConflict {
        aggregate_id: account_id,
        expected: expected_version,
        actual: Some(current_version),
    });
}

// Use shorter transactions
let mut tx = pool.begin().await?;
// ... perform operations ...
tx.commit().await?;
```

### 3. Timeout Management

**Symptoms:**
- Operations timing out frequently
- High timeout rate in metrics
- User requests failing with timeout errors
- System unresponsiveness

**Causes:**
- Slow database queries
- Network latency
- Resource contention
- Insufficient timeout values

**Solutions:**

```bash
# Check timeout statistics
curl -X GET http://localhost:3000/health/timeouts

# View timeout configuration
curl -X GET http://localhost:3000/config/timeouts
```

**Configuration:**
```env
# Timeout settings
DB_OPERATION_TIMEOUT=30
CACHE_OPERATION_TIMEOUT=10
KAFKA_OPERATION_TIMEOUT=15
REDIS_OPERATION_TIMEOUT=5
BATCH_PROCESSING_TIMEOUT=60
HEALTH_CHECK_TIMEOUT=10
```

**Code Patterns:**
```rust
// Use timeout manager
let result = timeout_manager.with_database_timeout(
    &operation_id,
    async {
        // Database operation
        pool.execute(query).await
    }
).await?;

// Implement retry logic with exponential backoff
let mut retries = 0;
let max_retries = 3;
while retries < max_retries {
    match operation().await {
        Ok(result) => return Ok(result),
        Err(e) => {
            retries += 1;
            if retries >= max_retries {
                return Err(e);
            }
            tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(retries))).await;
        }
    }
}
```

### 4. Cache Performance Issues

**Symptoms:**
- Low cache hit rates
- High cache miss rates
- Slow cache operations
- Memory pressure

**Causes:**
- Inappropriate cache keys
- Memory constraints
- Eviction policies
- Cache warming issues

**Solutions:**

```bash
# Check cache metrics
curl -X GET http://localhost:3000/health/cache

# Monitor cache performance
curl -X GET http://localhost:3000/metrics/cache
```

**Configuration:**
```env
# Cache settings
CACHE_DEFAULT_TTL=3600
CACHE_MAX_SIZE=5000
CACHE_SHARD_COUNT=8
CACHE_WARMUP_BATCH_SIZE=50
CACHE_WARMUP_INTERVAL=300
```

**Code Patterns:**
```rust
// Implement proper cache warming
pub async fn warmup_cache(&self, account_ids: Vec<Uuid>) -> Result<()> {
    for chunk in account_ids.chunks(100) {
        let mut pipeline = redis::pipe();
        for &account_id in chunk {
            pipeline.get(format!("account:{}", account_id));
        }
        let results: Vec<redis::Value> = pipeline.query_async(&mut conn).await?;
        // Process results...
    }
    Ok(())
}

// Use appropriate cache keys
let cache_key = format!("account:{}:v{}", account_id, version);
```

### 5. Kafka Event Processing Issues

**Symptoms:**
- High consumer lag
- Event processing delays
- Failed event deliveries
- DLQ (Dead Letter Queue) growth

**Causes:**
- Slow event processing
- Consumer group rebalancing
- Network issues
- Insufficient consumer parallelism

**Solutions:**

```bash
# Check Kafka health
curl -X GET http://localhost:3000/health/kafka

# Monitor consumer lag
curl -X GET http://localhost:3000/metrics/kafka
```

**Configuration:**
```env
# Kafka settings
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=banking-es-group
KAFKA_CONSUMER_MAX_POLL_RECORDS=250
KAFKA_CONSUMER_SESSION_TIMEOUT_MS=30000
KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS=600000
```

**Code Patterns:**
```rust
// Implement proper error handling
match process_event(event).await {
    Ok(_) => {
        consumer.commit_message(&msg, CommitMode::Async).await?;
    }
    Err(e) => {
        // Send to DLQ
        dlq.send_to_dlq(account_id, events, version, e.to_string()).await?;
        // Don't commit - let it be retried
    }
}

// Use batch processing
let mut batch = Vec::new();
for msg in consumer.iter() {
    batch.push(msg?);
    if batch.len() >= batch_size {
        process_batch(batch).await?;
        batch.clear();
    }
}
```

### 6. Redis Performance Issues

**Symptoms:**
- Slow Redis operations
- Connection timeouts
- Memory pressure
- High latency

**Causes:**
- Large datasets
- Inefficient commands
- Network latency
- Memory constraints

**Solutions:**

```bash
# Check Redis health
curl -X GET http://localhost:3000/health/redis

# Monitor Redis metrics
curl -X GET http://localhost:3000/metrics/redis
```

**Configuration:**
```env
# Redis settings
REDIS_OPERATION_TIMEOUT=5
REDIS_MAX_CONNECTIONS=50
REDIS_MIN_CONNECTIONS=5
```

**Code Patterns:**
```rust
// Use pipelining for multiple operations
let mut pipeline = redis::pipe();
for key in keys {
    pipeline.get(key);
}
let results: Vec<redis::Value> = pipeline.query_async(&mut conn).await?;

// Use appropriate data structures
// For counters: INCR/DECR
// For sets: SADD/SREM
// For sorted sets: ZADD/ZRANGE
```

## Monitoring and Alerting

### Health Check Endpoints

```bash
# Overall system health
curl -X GET http://localhost:3000/health

# Component-specific health
curl -X GET http://localhost:3000/health/database
curl -X GET http://localhost:3000/health/cache
curl -X GET http://localhost:3000/health/kafka
curl -X GET http://localhost:3000/health/redis

# Detailed metrics
curl -X GET http://localhost:3000/metrics
```

### Key Metrics to Monitor

1. **Database Metrics:**
   - Connection pool utilization
   - Query execution time
   - Transaction count
   - Deadlock count

2. **Cache Metrics:**
   - Hit rate
   - Miss rate
   - Eviction rate
   - Memory usage

3. **Kafka Metrics:**
   - Consumer lag
   - Producer throughput
   - Error rate
   - Partition count

4. **System Metrics:**
   - CPU usage
   - Memory usage
   - Network I/O
   - Disk I/O

### Alerting Rules

```yaml
# Example Prometheus alerting rules
groups:
  - name: banking-es-alerts
    rules:
      - alert: HighConnectionPoolUtilization
        expr: connection_pool_utilization > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High connection pool utilization"
          
      - alert: HighDeadlockCount
        expr: deadlock_count > 5
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "High deadlock count detected"
          
      - alert: HighTimeoutRate
        expr: timeout_rate > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High timeout rate detected"
```

## Performance Optimization

### Database Optimization

1. **Index Optimization:**
   ```sql
   -- Add indexes for frequently queried columns
   CREATE INDEX idx_events_aggregate_id_version ON events(aggregate_id, version);
   CREATE INDEX idx_events_timestamp ON events(timestamp);
   ```

2. **Query Optimization:**
   ```sql
   -- Use prepared statements
   -- Implement query result caching
   -- Use appropriate WHERE clauses
   ```

3. **Connection Pool Tuning:**
   ```env
   # Optimize pool settings based on load
   POOL_MAX_CONNECTIONS=200
   POOL_MIN_CONNECTIONS=20
   POOL_ACQUIRE_TIMEOUT=10
   ```

### Cache Optimization

1. **Cache Key Design:**
   ```rust
   // Use consistent, predictable keys
   let cache_key = format!("account:{}:v{}", account_id, version);
   
   // Implement cache warming
   pub async fn warmup_frequently_accessed_accounts(&self) -> Result<()> {
       // Warm up accounts with high access patterns
   }
   ```

2. **Eviction Policy:**
   ```rust
   // Use LRU for most cases
   // Use TTL for time-sensitive data
   // Implement custom eviction for business logic
   ```

### Event Processing Optimization

1. **Batch Processing:**
   ```rust
   // Process events in batches
   let batch_size = 1000;
   let mut batch = Vec::new();
   
   for event in events {
       batch.push(event);
       if batch.len() >= batch_size {
           process_batch(batch).await?;
           batch.clear();
       }
   }
   ```

2. **Parallel Processing:**
   ```rust
   // Use multiple workers for event processing
   let workers = 8;
   let (tx, rx) = tokio::sync::mpsc::channel(1000);
   
   for _ in 0..workers {
       let rx = rx.clone();
       tokio::spawn(async move {
           while let Some(event) = rx.recv().await {
               process_event(event).await?;
           }
       });
   }
   ```

## Emergency Procedures

### System Recovery

1. **Graceful Shutdown:**
   ```bash
   # Send SIGTERM to allow graceful shutdown
   kill -TERM <pid>
   
   # Wait for shutdown to complete
   # Force kill if necessary after timeout
   kill -KILL <pid>
   ```

2. **Service Restart:**
   ```bash
   # Restart individual services
   systemctl restart banking-es-event-store
   systemctl restart banking-es-kafka-processor
   systemctl restart banking-es-cache-service
   ```

3. **Database Recovery:**
   ```sql
   -- Kill long-running transactions
   SELECT pg_terminate_backend(pid) 
   FROM pg_stat_activity 
   WHERE state = 'active' 
   AND query_start < NOW() - INTERVAL '5 minutes';
   
   -- Reset connection pool
   SELECT pg_terminate_backend(pid) 
   FROM pg_stat_activity 
   WHERE datname = 'banking_es';
   ```

### Data Recovery

1. **Event Replay:**
   ```rust
   // Replay events from a specific point
   pub async fn replay_events_from_version(
       &self,
       aggregate_id: Uuid,
       from_version: i64,
   ) -> Result<()> {
       let events = self.get_events(aggregate_id, Some(from_version)).await?;
       for event in events {
           self.process_event(event).await?;
       }
       Ok(())
   }
   ```

2. **Snapshot Recovery:**
   ```rust
   // Recover from snapshot
   pub async fn recover_from_snapshot(
       &self,
       aggregate_id: Uuid,
   ) -> Result<Account> {
       if let Some(snapshot) = self.get_snapshot(aggregate_id).await? {
           let mut account: Account = bincode::deserialize(&snapshot.data)?;
           // Apply events after snapshot
           let events = self.get_events(aggregate_id, Some(snapshot.version)).await?;
           for event in events {
               account.apply_event(&event)?;
           }
           Ok(account)
       } else {
           Err(anyhow::anyhow!("No snapshot found"))
       }
   }
   ```

## Debugging Tools

### Logging Configuration

```env
# Enable debug logging
RUST_LOG=debug
RUST_LOG_BANKING_ES=debug

# Enable structured logging
RUST_LOG_FORMAT=json
```

### Debug Endpoints

```bash
# Get system state
curl -X GET http://localhost:3000/debug/state

# Get active operations
curl -X GET http://localhost:3000/debug/active-operations

# Get stuck operations
curl -X GET http://localhost:3000/debug/stuck-operations

# Force cleanup
curl -X POST http://localhost:3000/debug/cleanup
```

### Performance Profiling

```bash
# Use cargo flamegraph for CPU profiling
cargo install flamegraph
cargo flamegraph

# Use perf for system-level profiling
perf record -g ./target/release/banking-es
perf report
```

## Best Practices

1. **Always use timeouts** for external operations
2. **Implement proper error handling** with retry logic
3. **Monitor key metrics** continuously
4. **Use connection pooling** effectively
5. **Implement circuit breakers** for external services
6. **Use appropriate cache strategies**
7. **Implement proper logging** and tracing
8. **Test failure scenarios** regularly
9. **Have rollback procedures** ready
10. **Document operational procedures**

## Support

For additional support:

1. Check the logs: `tail -f /var/log/banking-es/application.log`
2. Monitor metrics: `curl http://localhost:3000/metrics`
3. Check health status: `curl http://localhost:3000/health`
4. Review configuration: `curl http://localhost:3000/config`

## Emergency Contacts

- **Database Issues**: DBA Team
- **Infrastructure Issues**: DevOps Team
- **Application Issues**: Development Team
- **Security Issues**: Security Team 