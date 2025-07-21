#!/bin/bash

echo "🚀 Setting up optimized performance configuration..."

# Database Connection Pool Optimization
export DB_MAX_CONNECTIONS=200
export DB_MIN_CONNECTIONS=50
export DB_ACQUIRE_TIMEOUT=5
export DB_IDLE_TIMEOUT=600
export DB_MAX_LIFETIME=1800

# Database Operation Timeouts
export DB_OPERATION_TIMEOUT=15
export TRANSACTION_TIMEOUT=15
export CONNECTION_ACQUIRE_TIMEOUT=5
export LOCK_TIMEOUT=10
export RETRY_TIMEOUT=3

# Batch Processing Optimization
export DB_BATCH_SIZE=500
export DB_BATCH_TIMEOUT_MS=50
export DB_MAX_BATCH_QUEUE_SIZE=5000
export DB_BATCH_PROCESSOR_COUNT=8

# Redis Optimization
export REDIS_MAX_CONNECTIONS=100
export REDIS_MIN_CONNECTIONS=20
export REDIS_CONNECTION_TIMEOUT=5
export REDIS_IDLE_TIMEOUT=300
export REDIS_OPERATION_TIMEOUT=5

# Kafka Optimization
export KAFKA_OPERATION_TIMEOUT=10
export KAFKA_MAX_POLL_INTERVAL_MS=300000
export KAFKA_SESSION_TIMEOUT_MS=10000
export KAFKA_MAX_POLL_RECORDS=100

# Cache Optimization
export CACHE_OPERATION_TIMEOUT=5
export CACHE_MAX_SIZE=2000
export CACHE_DEFAULT_TTL=300

# Projection Optimization
export PROJECTION_MAX_CONNECTIONS=100
export PROJECTION_MIN_CONNECTIONS=20
export PROJECTION_ACQUIRE_TIMEOUT=5
export PROJECTION_BATCH_SIZE=1000
export PROJECTION_BATCH_TIMEOUT_MS=25

# Pool Monitoring
export POOL_HEALTH_CHECK_INTERVAL=10
export POOL_CONNECTION_TIMEOUT=15
export POOL_MAX_WAIT_TIME=5
export POOL_EXHAUSTION_THRESHOLD=0.8
export POOL_AUTO_SCALING=true

# Deadlock Detection
export DEADLOCK_CHECK_INTERVAL=5
export DEADLOCK_OPERATION_TIMEOUT=30
export MAX_CONCURRENT_OPERATIONS=1000
export ENABLE_AUTO_RESOLUTION=true

# General Application Settings
export MAX_CONCURRENT_OPERATIONS=500
export MAX_REQUESTS_PER_SECOND=2000
export BATCH_FLUSH_INTERVAL_MS=50

# Logging
export RUST_LOG=info
export RUST_BACKTRACE=1

echo "✅ Performance configuration applied:"
echo "   📊 DB Max Connections: $DB_MAX_CONNECTIONS"
echo "   ⏱️  DB Operation Timeout: $DB_OPERATION_TIMEOUT"
echo "   🔄 Batch Size: $DB_BATCH_SIZE"
echo "   🚀 Max Concurrent Operations: $MAX_CONCURRENT_OPERATIONS"
echo "   📈 Max Requests/Second: $MAX_REQUESTS_PER_SECOND"

echo ""
echo "🎯 Ready to run optimized stress test!"
echo "   Run: cargo test test_batch_processing_stress --test working_stress_test -- --nocapture" 