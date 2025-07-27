#!/bin/bash

echo "🚀 Fixing Performance Issues - Comprehensive Solution"

echo ""
echo "🔍 Issues Identified:"
echo "   1. ConsistencyManager timeout: 60s (too long)"
echo "   2. Debezium polling: ~5s (too slow)"
echo "   3. CDC processing: 20+ seconds"
echo "   4. Total Phase 1 time: 61+ seconds"
echo ""

echo "🛠️ Applying Fixes..."

# 1. Restart Debezium with optimized configuration
echo "📊 Step 1: Restarting Debezium with optimized configuration..."
./restart_debezium_optimized.sh

# 2. Set environment variables for performance optimization
echo "⚙️ Step 2: Setting performance environment variables..."

# Consistency Manager Optimization
export CONSISTENCY_TIMEOUT_SECS=10
export CONSISTENCY_CLEANUP_INTERVAL_SECS=30

# CDC Processing Optimization
export CDC_BATCH_SIZE=1000
export CDC_BATCH_TIMEOUT_MS=50
export CDC_MAX_CONCURRENT=32
export CDC_POLL_INTERVAL_MS=50

# Database Optimization
export DB_BATCH_SIZE=1000
export DB_BATCH_TIMEOUT_MS=50
export DB_MAX_BATCH_QUEUE_SIZE=5000
export DB_BATCH_PROCESSOR_COUNT=16

# Projection Optimization
export PROJECTION_BATCH_SIZE=1000
export PROJECTION_BATCH_TIMEOUT_MS=50
export PROJECTION_MAX_CONNECTIONS=200

# Kafka Optimization
export KAFKA_MAX_POLL_RECORDS=1000
export KAFKA_MAX_POLL_INTERVAL_MS=300000
export KAFKA_SESSION_TIMEOUT_MS=10000

# Cache Optimization
export CACHE_MAX_SIZE=2000
export CACHE_DEFAULT_TTL=300

echo "✅ Environment variables set"

# 3. Create optimized configuration file
echo "📝 Step 3: Creating optimized configuration..."
cat > optimized_config.env << EOF
# Performance Optimization Configuration
CONSISTENCY_TIMEOUT_SECS=10
CONSISTENCY_CLEANUP_INTERVAL_SECS=30
CDC_BATCH_SIZE=1000
CDC_BATCH_TIMEOUT_MS=50
CDC_MAX_CONCURRENT=32
CDC_POLL_INTERVAL_MS=50
DB_BATCH_SIZE=1000
DB_BATCH_TIMEOUT_MS=50
DB_MAX_BATCH_QUEUE_SIZE=5000
DB_BATCH_PROCESSOR_COUNT=16
PROJECTION_BATCH_SIZE=1000
PROJECTION_BATCH_TIMEOUT_MS=50
PROJECTION_MAX_CONNECTIONS=200
KAFKA_MAX_POLL_RECORDS=1000
KAFKA_MAX_POLL_INTERVAL_MS=300000
KAFKA_SESSION_TIMEOUT_MS=10000
CACHE_MAX_SIZE=2000
CACHE_DEFAULT_TTL=300
EOF

echo "✅ Optimized configuration created"

# 4. Check Debezium connector status
echo "🔍 Step 4: Checking Debezium connector status..."
sleep 5
curl -X GET http://localhost:8083/connectors/banking-es-connector/status

echo ""
echo "🎯 Expected Performance Improvements:"
echo "   • ConsistencyManager timeout: 60s → 10s (6x faster failure detection)"
echo "   • Debezium polling: 5s → 100ms (50x faster)"
echo "   • CDC processing: 20s → 2-3s (7x faster)"
echo "   • Total Phase 1 time: 61s → 5-6s (10x faster)"
echo ""
echo "📊 Run your test again to verify the improvements!"
echo ""
echo "🔧 To monitor performance:"
echo "   tail -f test_read_operations_after_writes.log"
echo "   curl -X GET http://localhost:8083/connectors/banking-es-connector/status" 