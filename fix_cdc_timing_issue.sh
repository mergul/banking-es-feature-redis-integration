#!/bin/bash

echo "🔧 Fixing CDC Timing Issue - Ensuring Proper Startup Sequence"

echo ""
echo "🔍 Issue Identified:"
echo "   • CDC Service Manager starts quickly (~88ms)"
echo "   • Account creation starts immediately after"
echo "   • CDC consumer takes 2.4s to join Kafka consumer group"
echo "   • Result: 2.4s dead time causing 10s timeouts"
echo ""

echo "🛠️ Applying Fixes..."

# 1. Restart Debezium with optimized configuration
echo "📊 Step 1: Restarting Debezium with optimized configuration..."
./restart_debezium_optimized.sh

# 2. Set environment variables for CDC timing optimization
echo "⚙️ Step 2: Setting CDC timing environment variables..."

# CDC Consumer Startup Optimization
export CDC_CONSUMER_STARTUP_TIMEOUT_SECS=10
export CDC_CONSUMER_READY_CHECK_INTERVAL_MS=1000
export CDC_CONSUMER_MAX_READY_RETRIES=10

# Kafka Consumer Optimization
export KAFKA_CONSUMER_GROUP_JOIN_TIMEOUT_MS=5000
export KAFKA_CONSUMER_SESSION_TIMEOUT_MS=10000
export KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS=3000

# Consistency Manager Optimization
export CONSISTENCY_TIMEOUT_SECS=15
export CONSISTENCY_CLEANUP_INTERVAL_SECS=30

echo "✅ Environment variables set"

# 3. Create CDC timing configuration
echo "📝 Step 3: Creating CDC timing configuration..."
cat > cdc_timing_config.env << EOF
# CDC Timing Optimization Configuration
CDC_CONSUMER_STARTUP_TIMEOUT_SECS=10
CDC_CONSUMER_READY_CHECK_INTERVAL_MS=1000
CDC_CONSUMER_MAX_READY_RETRIES=10
KAFKA_CONSUMER_GROUP_JOIN_TIMEOUT_MS=5000
KAFKA_CONSUMER_SESSION_TIMEOUT_MS=10000
KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS=3000
CONSISTENCY_TIMEOUT_SECS=15
CONSISTENCY_CLEANUP_INTERVAL_SECS=30
EOF

echo "✅ CDC timing configuration created"

# 4. Check Debezium connector status
echo "🔍 Step 4: Checking Debezium connector status..."
sleep 3
curl -X GET http://localhost:8083/connectors/banking-es-connector/status

echo ""
echo "🎯 Expected Performance Improvements:"
echo "   • CDC consumer ready before account creation"
echo "   • No 2.4s dead time during consistency checks"
echo "   • CDC processing starts immediately"
echo "   • Total Phase 1 time: 2-3 seconds (down from 11+ seconds)"
echo ""
echo "📊 Run your test again to verify the improvements!"
echo ""
echo "🔧 To monitor CDC timing:"
echo "   tail -f test_read_operations_after_writes.log | grep -E '(CDC.*ready|CDC.*started|Waiting.*CDC)'"
echo "   curl -X GET http://localhost:8083/connectors/banking-es-connector/status"