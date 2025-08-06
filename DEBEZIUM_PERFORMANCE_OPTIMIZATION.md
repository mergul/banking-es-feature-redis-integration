# Debezium Performance Optimization

## 🚨 Problem Identified

The CDC (Change Data Capture) processing was taking **20+ seconds** while account creation and outbox batching were only taking **~1 second each**. This created a significant bottleneck in the event sourcing pipeline.

## 🔍 Root Cause Analysis

### Timeline Breakdown (Before Optimization):

1. **Account Creation**: ~1 second ✅
2. **Outbox Batching**: ~1 second ✅
3. **Debezium Polling**: ~5 seconds ❌ (Default polling interval)
4. **CDC Processing**: ~3.6 seconds ❌ (First projection update)
5. **Total Time**: 26+ seconds ❌

### The Bottleneck:

- **Debezium Default Polling**: ~5000ms (5 seconds)
- **Kafka Consumer Polling**: 100ms (mismatch!)
- **Processing Pipeline Delays**: Multiple 5-second intervals stacking up

## 🛠️ Performance Optimizations Applied

### 1. Polling Interval Optimization

```json
"poll.interval.ms": "100"
```

- **Before**: ~5000ms (5 seconds)
- **After**: 100ms
- **Impact**: 50x faster polling

### 2. Queue and Batch Size Optimization

```json
"max.queue.size": "8192",
"max.batch.size": "2048"
```

- **Before**: Default values (smaller)
- **After**: Increased for better throughput
- **Impact**: Higher throughput, reduced latency

### 3. Snapshot Optimization

```json
"snapshot.delay.ms": "0",
"snapshot.fetch.size": "1024"
```

- **Before**: Default delays and smaller fetch sizes
- **After**: No delay, larger fetch sizes
- **Impact**: Faster initial snapshot processing

### 4. Heartbeat Optimization

```json
"heartbeat.interval.ms": "1000"
```

- **Before**: Default (longer intervals)
- **After**: 1 second intervals
- **Impact**: Faster failure detection and recovery

### 5. Schema History Optimization

```json
"include.schema.changes": "false",
"provide.transaction.metadata": "false",
"tombstones.on.delete": "false"
```

- **Before**: Unnecessary metadata processing
- **After**: Disabled for performance
- **Impact**: Reduced processing overhead

## 📈 Expected Performance Improvement

### Before Optimization:

- **Account Creation**: ~1 second
- **Outbox Batching**: ~1 second
- **CDC Processing**: ~20+ seconds
- **Total Phase 1**: ~26 seconds

### After Optimization:

- **Account Creation**: ~1 second
- **Outbox Batching**: ~1 second
- **CDC Processing**: ~2-3 seconds
- **Total Phase 1**: ~5-6 seconds

### Performance Gain:

- **75-80% reduction** in CDC processing time
- **80% reduction** in total Phase 1 time
- **5x faster** overall pipeline

## 🚀 Implementation

### 1. Updated Configuration

The `debezium-config.json` file has been updated with all performance optimizations.

### 2. Restart Script

Use the provided script to restart the connector:

```bash
./restart_debezium_optimized.sh
```

### 3. Verification

Monitor the logs to verify the improvements:

```bash
# Check connector status
curl -X GET http://localhost:8083/connectors/banking-es-connector/status

# Monitor logs for faster processing
tail -f test_read_operations_after_writes.log
```

## 🔧 Additional Recommendations

### 1. Kafka Consumer Optimization

Consider reducing the Kafka consumer timeout to match Debezium:

```rust
// In kafka_abstraction.rs
match timeout(Duration::from_millis(50), stream.next()).await {
```

### 2. Batch Processing Optimization

- Increase CDC batch processor worker count
- Optimize projection update intervals
- Enable parallel processing for projections

### 3. Monitoring

- Monitor Debezium connector metrics
- Track CDC processing latency
- Set up alerts for performance degradation

## 📊 Monitoring Metrics

### Key Metrics to Watch:

- **Debezium Polling Frequency**: Should be ~100ms
- **CDC Processing Latency**: Should be <3 seconds
- **Projection Update Time**: Should be <1 second
- **Total Pipeline Time**: Should be <6 seconds

### Health Checks:

```bash
# Check connector health
curl -X GET http://localhost:8083/connectors/banking-es-connector/status

# Check Kafka consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 --group banking-es-group --describe
```

## 🎯 Success Criteria

The optimization is successful when:

- ✅ CDC processing time < 3 seconds
- ✅ Total Phase 1 time < 6 seconds
- ✅ No increase in error rates
- ✅ Stable connector status
- ✅ Consistent performance across multiple test runs
