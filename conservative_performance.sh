#!/bin/bash

echo "🛡️ Setting up conservative performance configuration..."

# Database Connection Pool - Conservative Settings
export DB_MAX_CONNECTIONS=50
export DB_MIN_CONNECTIONS=10
export DB_ACQUIRE_TIMEOUT=10
export DB_IDLE_TIMEOUT=600
export DB_MAX_LIFETIME=1800

# Database Operation Timeouts - Conservative
export DB_OPERATION_TIMEOUT=30
export TRANSACTION_TIMEOUT=30
export CONNECTION_ACQUIRE_TIMEOUT=10
export LOCK_TIMEOUT=15
export RETRY_TIMEOUT=5

# Batch Processing - Conservative
export DB_BATCH_SIZE=100
export DB_BATCH_TIMEOUT_MS=100
export DB_MAX_BATCH_QUEUE_SIZE=1000
export DB_BATCH_PROCESSOR_COUNT=4

# Redis - Conservative
export REDIS_MAX_CONNECTIONS=20
export REDIS_MIN_CONNECTIONS=5
export REDIS_CONNECTION_TIMEOUT=10
export REDIS_IDLE_TIMEOUT=300
export REDIS_OPERATION_TIMEOUT=10

# Kafka - Conservative
export KAFKA_OPERATION_TIMEOUT=20
export KAFKA_MAX_POLL_INTERVAL_MS=300000
export KAFKA_SESSION_TIMEOUT_MS=30000
export KAFKA_MAX_POLL_RECORDS=50

# Cache - Conservative
export CACHE_OPERATION_TIMEOUT=10
export CACHE_MAX_SIZE=500
export CACHE_DEFAULT_TTL=600

# Projection - Conservative
export PROJECTION_MAX_CONNECTIONS=20
export PROJECTION_MIN_CONNECTIONS=5
export PROJECTION_ACQUIRE_TIMEOUT=10
export PROJECTION_BATCH_SIZE=100
export PROJECTION_BATCH_TIMEOUT_MS=100

# Pool Monitoring
export POOL_HEALTH_CHECK_INTERVAL=30
export POOL_CONNECTION_TIMEOUT=30
export POOL_MAX_WAIT_TIME=10
export POOL_EXHAUSTION_THRESHOLD=0.6
export POOL_AUTO_SCALING=false

# Deadlock Detection
export DEADLOCK_CHECK_INTERVAL=10
export DEADLOCK_OPERATION_TIMEOUT=60
export MAX_CONCURRENT_OPERATIONS=100
export ENABLE_AUTO_RESOLUTION=true

# General Application Settings - Conservative
export MAX_CONCURRENT_OPERATIONS=50
export MAX_REQUESTS_PER_SECOND=100
export BATCH_FLUSH_INTERVAL_MS=200

# Logging
export RUST_LOG=info
export RUST_BACKTRACE=1

echo "✅ Conservative configuration applied:"
echo "   📊 DB Max Connections: $DB_MAX_CONNECTIONS"
echo "   ⏱️  DB Operation Timeout: $DB_OPERATION_TIMEOUT"
echo "   🔄 Batch Size: $DB_BATCH_SIZE"
echo "   🚀 Max Concurrent Operations: $MAX_CONCURRENT_OPERATIONS"
echo "   📈 Max Requests/Second: $MAX_REQUESTS_PER_SECOND"

echo ""
echo "🎯 Ready to run conservative stress test!"
echo "   Run: cargo test test_batch_processing_stress --test working_stress_test -- --nocapture"