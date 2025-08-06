#!/bin/bash

echo "🚀 Setting up optimized performance configuration with enhanced connection pooling..."

# Database Pool Optimization - Enhanced for CDC Processing
export DB_MAX_CONNECTIONS=100
export DB_MIN_CONNECTIONS=20
export DB_ACQUIRE_TIMEOUT=15
export DB_IDLE_TIMEOUT=300
export DB_MAX_LIFETIME=900
export DB_STATEMENT_TIMEOUT=10000
export DB_LOCK_TIMEOUT=100
export DB_IDLE_IN_TRANSACTION_TIMEOUT=30000

# Database Operation Timeouts - Optimized for CDC
export DB_OPERATION_TIMEOUT=10
export TRANSACTION_TIMEOUT=15
export CONNECTION_ACQUIRE_TIMEOUT=5
export LOCK_TIMEOUT=100
export RETRY_TIMEOUT=2

# Batch Processing Optimization - Enhanced for Parallel Processing
export DB_BATCH_SIZE=2000
export DB_BATCH_TIMEOUT_MS=10
export DB_MAX_BATCH_QUEUE_SIZE=10000
export DB_BATCH_PROCESSOR_COUNT=16

# CDC Specific Optimizations
export CDC_BATCH_SIZE=2000
export CDC_BATCH_TIMEOUT_MS=10
export CDC_MAX_CONCURRENT_AGGREGATES=8
export CDC_CACHE_LOCK_TIMEOUT=5
export CDC_CONNECTION_RETRY_COUNT=3
export CDC_CONNECTION_RETRY_DELAY=100

# Redis Optimization - Enhanced Connection Pooling
export REDIS_MAX_CONNECTIONS=200
export REDIS_MIN_CONNECTIONS=50
export REDIS_CONNECTION_TIMEOUT=5
export REDIS_IDLE_TIMEOUT=300
export REDIS_OPERATION_TIMEOUT=5

# Kafka Optimization - Enhanced for CDC
export KAFKA_OPERATION_TIMEOUT=10
export KAFKA_MAX_POLL_INTERVAL_MS=300000
export KAFKA_SESSION_TIMEOUT_MS=10000
export KAFKA_MAX_POLL_RECORDS=1000

# Cache Optimization - Enhanced for Lock Contention Reduction
export CACHE_OPERATION_TIMEOUT=5
export CACHE_MAX_SIZE=2000
export CACHE_DEFAULT_TTL=300
export CACHE_LOCK_TIMEOUT=5
export CACHE_MAX_CONCURRENT_READS=16

# Projection Optimization - Enhanced Connection Pooling
export PROJECTION_MAX_CONNECTIONS=200
export PROJECTION_MIN_CONNECTIONS=50
export PROJECTION_ACQUIRE_TIMEOUT=5
export PROJECTION_BATCH_SIZE=2000
export PROJECTION_BATCH_TIMEOUT_MS=10

# Pool Monitoring - Enhanced for CDC
export POOL_HEALTH_CHECK_INTERVAL=5
export POOL_CONNECTION_TIMEOUT=10
export POOL_MAX_WAIT_TIME=2
export POOL_EXHAUSTION_THRESHOLD=0.8
export POOL_AUTO_SCALING=true

# Deadlock Detection - Enhanced for CDC
export DEADLOCK_CHECK_INTERVAL=3
export DEADLOCK_OPERATION_TIMEOUT=30
export MAX_CONCURRENT_OPERATIONS=1000
export ENABLE_AUTO_RESOLUTION=true

# General Application Settings - Optimized for CDC
export MAX_CONCURRENT_OPERATIONS=500
export MAX_REQUESTS_PER_SECOND=2000
export BATCH_FLUSH_INTERVAL_MS=10

# Lock Contention Reduction
export LOCK_TIMEOUT_MS=5000
export CACHE_LOCK_TIMEOUT_MS=5000
export PROJECTION_CACHE_LOCK_TIMEOUT_MS=5000
export MAX_CONCURRENT_CACHE_OPERATIONS=16
export CACHE_BATCH_SIZE=100

# Logging
export RUST_LOG=info
export RUST_BACKTRACE=1

echo "✅ Enhanced performance configuration applied:"
echo "   📊 DB Max Connections: $DB_MAX_CONNECTIONS"
echo "   ⏱️  DB Operation Timeout: $DB_OPERATION_TIMEOUT"
echo "   🔄 CDC Batch Size: $CDC_BATCH_SIZE"
echo "   🔒 Lock Timeout: $LOCK_TIMEOUT_MS ms"
echo "   🚀 CDC Max Concurrent Aggregates: $CDC_MAX_CONCURRENT_AGGREGATES"
echo "   📈 Cache Max Concurrent Reads: $CACHE_MAX_CONCURRENT_READS"

# Kafka Client Optimization
export KAFKA_BATCH_SIZE=65536
export KAFKA_LINGER_MS=10
export KAFKA_ACKS=1
export KAFKA_COMPRESSION_TYPE=snappy