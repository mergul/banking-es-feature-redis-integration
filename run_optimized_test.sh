#!/bin/bash

echo "🚀 Running optimized CDC high-throughput test with resource management..."

# Set environment variables for conservative resource usage
export RUST_LOG=info
export RUST_BACKTRACE=1

# Set conservative database pool settings
export DB_MAX_CONNECTIONS=100
export DB_MIN_CONNECTIONS=20
export DB_ACQUIRE_TIMEOUT=10
export DB_IDLE_TIMEOUT=600
export DB_MAX_LIFETIME=1800

# Set conservative Redis settings
export REDIS_MAX_CONNECTIONS=50
export REDIS_MIN_CONNECTIONS=10
export REDIS_CONNECTION_TIMEOUT=5
export REDIS_IDLE_TIMEOUT=300

# Set conservative Kafka settings
export KAFKA_MAX_POLL_INTERVAL_MS=300000
export KAFKA_SESSION_TIMEOUT_MS=10000
export KAFKA_MAX_POLL_RECORDS=100

# Set conservative batch settings
export DB_BATCH_SIZE=1000
export DB_BATCH_TIMEOUT_MS=10
export DB_MAX_BATCH_QUEUE_SIZE=10000
export DB_BATCH_PROCESSOR_COUNT=8

echo "📊 Environment configured for conservative resource usage"
echo "   DB Max Connections: $DB_MAX_CONNECTIONS"
echo "   DB Min Connections: $DB_MIN_CONNECTIONS"
echo "   Redis Max Connections: $REDIS_MAX_CONNECTIONS"
echo "   Batch Size: $DB_BATCH_SIZE"

# Run the optimized test
echo "🧪 Starting optimized test..."
cargo test test_real_cdc_high_throughput_performance -- --nocapture --test-threads=1

echo "✅ Test completed"