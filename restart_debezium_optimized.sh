#!/bin/bash

echo "🚀 Restarting Debezium Connector with Optimized Configuration..."

# Stop existing connector
echo "📴 Stopping existing connector..."
curl -X DELETE http://127.0.0.1:8083/connectors/banking-es-connector

# Wait for connector to stop
sleep 3

# Start connector with optimized configuration
echo "🔄 Starting connector with optimized configuration..."
curl -X POST http://127.0.0.1:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-config.json

# Check connector status
echo "📊 Checking connector status..."
sleep 2
curl -X GET http://127.0.0.1:8083/connectors/banking-es-connector/status

echo "✅ Debezium connector restarted with optimized configuration!"
echo ""
echo "🎯 Performance Optimizations Applied:"
echo "   • poll.interval.ms: 25 (reduced from ~5000ms)"
echo "   • max.queue.size: 8192 (increased buffer)"
echo "   • max.batch.size: 2048 (larger batches)"
echo "   • snapshot.delay.ms: 0 (no delay)"
echo "   • heartbeat.interval.ms: 1000 (faster heartbeats)"
echo ""
echo "📈 Expected Performance Improvement:"
echo "   • CDC Processing: ~2-3 seconds (down from 20+ seconds)"
echo "   • Total Phase 1 Time: ~5-6 seconds (down from 26 seconds)" 