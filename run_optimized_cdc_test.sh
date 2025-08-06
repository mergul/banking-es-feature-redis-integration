#!/bin/bash

echo "🚀 Running Optimized CDC Performance Test"
echo "=========================================="

# Set environment variables for optimal performance
export RUST_LOG=info
export RUST_BACKTRACE=1

# Clear Redis cache to start fresh
echo "🧹 Clearing Redis cache..."
redis-cli FLUSHALL

# Run the optimized test
echo "🏃 Running CQRS performance test with CDC optimizations..."
cargo test test_cqrs_high_throughput_performance -- --nocapture

echo "✅ Test completed!"
echo ""
echo "📊 Expected improvements:"
echo "- Higher cache hit rate (target: >30%)"
echo "- Better success rate (target: >85%)"
echo "- Improved OPS (target: >1000)"
echo "- Reduced cache invalidation issues"