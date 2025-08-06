# Complete DLQ Recovery/Consumer Implementation

This document describes the **complete Dead Letter Queue (DLQ) recovery system** implemented for the `UltraOptimizedCDCEventProcessor` in the banking CQRS/Event Sourcing system.

## Table of Contents

1. [Overview](#overview)
2. [DLQ Event Structure](#dlq-event-structure)
3. [DLQ Recovery Configuration](#dlq-recovery-configuration)
4. [DLQ Consumer](#dlq-consumer)
5. [Key Methods](#key-methods)
6. [Integration with Existing Pipeline](#integration-with-existing-pipeline)
7. [Usage Examples](#usage-examples)
8. [Monitoring & Observability](#monitoring--observability)
9. [Error Handling](#error-handling)
10. [Performance Considerations](#performance-considerations)

---

## Overview

The DLQ recovery system provides comprehensive error handling and recovery capabilities for the CDC event processing pipeline. It includes:

- **Automatic Recovery**: Background processing of failed events
- **Manual Recovery**: On-demand reprocessing of DLQ messages
- **Retry Logic**: Configurable retry mechanisms with exponential backoff
- **Metrics Tracking**: Comprehensive monitoring and observability
- **Circuit Breaker Integration**: Works with existing circuit breaker patterns

---

## DLQ Event Structure

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DLQEvent {
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub error: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub retry_count: u32,
}
```

**Fields:**

- `event_id`: Unique identifier for the failed event
- `aggregate_id`: ID of the aggregate (account) the event belongs to
- `event_type`: Type of the event (e.g., "MoneyDeposited", "AccountCreated")
- `payload`: Serialized event data
- `error`: Error message describing why the event failed
- `timestamp`: When the event was sent to DLQ
- `retry_count`: Number of retry attempts before being sent to DLQ

---

## DLQ Recovery Configuration

```rust
#[derive(Debug, Clone)]
pub struct DLQRecoveryConfig {
    pub dlq_topic: String,                    // "banking-es-dlq"
    pub consumer_group_id: String,            // "banking-es-dlq-recovery"
    pub max_reprocessing_retries: u32,        // 5
    pub reprocessing_backoff_ms: u64,         // 1000ms
    pub batch_size: usize,                    // 10
    pub poll_timeout_ms: u64,                 // 100ms
    pub enable_auto_recovery: bool,           // false by default
    pub auto_recovery_interval_secs: u64,     // 300 seconds
}
```

**Default Configuration:**

- **DLQ Topic**: `banking-es-dlq`
- **Consumer Group**: `banking-es-dlq-recovery`
- **Max Retries**: 5 attempts
- **Backoff**: 1000ms base, exponential increase
- **Batch Size**: 10 messages per batch
- **Auto Recovery**: Disabled by default
- **Recovery Interval**: 5 minutes (when enabled)

---

## DLQ Consumer

The `DLQConsumer` provides both automatic and manual recovery capabilities:

### Features

1. **Automatic Recovery**

   - Runs as a background task
   - Configurable polling intervals
   - Graceful shutdown handling

2. **Manual Recovery**

   - Process all available DLQ messages on demand
   - Return comprehensive metrics
   - Error handling and logging

3. **Batch Processing**

   - Process messages in configurable batches
   - Optimized for throughput
   - Memory-efficient processing

4. **Retry Logic**
   - Exponential backoff strategy
   - Configurable retry limits
   - Detailed error tracking

### DLQ Recovery Metrics

```rust
#[derive(Debug, Default)]
pub struct DLQRecoveryMetrics {
    pub dlq_messages_consumed: std::sync::atomic::AtomicU64,
    pub dlq_messages_reprocessed: std::sync::atomic::AtomicU64,
    pub dlq_messages_failed: std::sync::atomic::AtomicU64,
    pub dlq_reprocessing_retries: std::sync::atomic::AtomicU64,
    pub dlq_consumer_restarts: std::sync::atomic::AtomicU64,
    pub dlq_processing_latency_ms: std::sync::atomic::AtomicU64,
}
```

---

## Key Methods

### 1. Manual DLQ Reprocessing

```rust
// Process all available DLQ messages and return metrics
let metrics = processor.reprocess_dlq().await?;

println!("Processed: {}, Failed: {}",
    metrics.dlq_messages_reprocessed.load(Ordering::Relaxed),
    metrics.dlq_messages_failed.load(Ordering::Relaxed));
```

### 2. Create and Start Auto-Recovery Consumer

```rust
// Configure auto-recovery
let mut config = DLQRecoveryConfig::default();
config.enable_auto_recovery = true;
config.auto_recovery_interval_secs = 60; // Every minute
config.max_reprocessing_retries = 3;     // Reduce retries for auto-recovery

// Create and start the consumer
let mut dlq_consumer = processor.create_dlq_consumer(config)?;
dlq_consumer.start().await?;

// Later, stop the consumer gracefully
dlq_consumer.stop().await?;
```

### 3. Get DLQ Statistics

```rust
// Get current DLQ statistics
let stats = processor.get_dlq_stats().await?;
println!("DLQ Stats: {:?}", stats);

// Example output:
// {
//   "dlq_topic": "banking-es-dlq",
//   "total_messages": 15,
//   "oldest_message_age_seconds": 3600,
//   "recovery_enabled": true,
//   "last_recovery_attempt": "2024-01-15T10:30:00Z"
// }
```

### 4. Get DLQ Recovery Metrics

```rust
// Get metrics from an active DLQ consumer
let metrics = dlq_consumer.get_metrics();
println!("DLQ Recovery Metrics:");
println!("  Consumed: {}", metrics.dlq_messages_consumed.load(Ordering::Relaxed));
println!("  Reprocessed: {}", metrics.dlq_messages_reprocessed.load(Ordering::Relaxed));
println!("  Failed: {}", metrics.dlq_messages_failed.load(Ordering::Relaxed));
println!("  Latency: {}ms", metrics.dlq_processing_latency_ms.load(Ordering::Relaxed));
```

---

## Integration with Existing Pipeline

The DLQ system integrates seamlessly with the existing event processing pipeline:

### 1. Event Processing Flow

```
1. CDC Event Received
   ↓
2. Business Logic Validation
   ↓
3. Duplicate Detection
   ↓
4. Event Processing (with retries)
   ↓
5. Success → Update Projections
   ↓
6. Failure → Send to DLQ
```

### 2. Circuit Breaker Integration

- Works with existing circuit breaker logic
- Failed events respect circuit breaker state
- DLQ processing doesn't trigger circuit breaker trips

### 3. Metrics Integration

- Extends existing metrics with DLQ-specific counters
- Provides comprehensive observability
- Integrates with existing monitoring dashboards

### 4. Business Logic Respect

- Respects existing business validation rules
- Maintains data consistency
- Preserves audit trails

---

## Usage Examples

### 1. Enable Auto-Recovery in Main Application

```rust
// In main.rs or service initialization
async fn start_dlq_recovery(processor: Arc<UltraOptimizedCDCEventProcessor>) -> Result<()> {
    let mut dlq_config = DLQRecoveryConfig::default();
    dlq_config.enable_auto_recovery = true;
    dlq_config.auto_recovery_interval_secs = 300; // Every 5 minutes
    dlq_config.max_reprocessing_retries = 3;
    dlq_config.batch_size = 20;

    let mut dlq_consumer = processor.create_dlq_consumer(dlq_config)?;
    dlq_consumer.start().await?;

    Ok(())
}
```

### 2. Manual Recovery API Endpoint

```rust
// Add to your API endpoints
#[post("/api/cqrs/dlq/reprocess")]
async fn reprocess_dlq(
    processor: State<Arc<UltraOptimizedCDCEventProcessor>>
) -> Json<serde_json::Value> {
    match processor.reprocess_dlq().await {
        Ok(metrics) => Json(serde_json::json!({
            "success": true,
            "processed": metrics.dlq_messages_reprocessed.load(Ordering::Relaxed),
            "failed": metrics.dlq_messages_failed.load(Ordering::Relaxed),
            "total_consumed": metrics.dlq_messages_consumed.load(Ordering::Relaxed)
        })),
        Err(e) => Json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        }))
    }
}

#[get("/api/cqrs/dlq/stats")]
async fn get_dlq_stats(
    processor: State<Arc<UltraOptimizedCDCEventProcessor>>
) -> Json<serde_json::Value> {
    match processor.get_dlq_stats().await {
        Ok(stats) => Json(stats),
        Err(e) => Json(serde_json::json!({
            "error": e.to_string()
        }))
    }
}
```

### 3. Scheduled Recovery Task

```rust
// Run manual recovery on a schedule
async fn scheduled_dlq_recovery(processor: Arc<UltraOptimizedCDCEventProcessor>) {
    let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Every hour

    loop {
        interval.tick().await;

        info!("Starting scheduled DLQ recovery");
        match processor.reprocess_dlq().await {
            Ok(metrics) => {
                let processed = metrics.dlq_messages_reprocessed.load(Ordering::Relaxed);
                let failed = metrics.dlq_messages_failed.load(Ordering::Relaxed);
                info!("Scheduled DLQ recovery completed: {} processed, {} failed", processed, failed);
            }
            Err(e) => {
                error!("Scheduled DLQ recovery failed: {}", e);
            }
        }
    }
}
```

### 4. Health Check Integration

```rust
// Add DLQ health check to your health endpoint
async fn health_check(processor: State<Arc<UltraOptimizedCDCEventProcessor>>) -> Json<serde_json::Value> {
    let mut health = serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "services": {
            "cdc_processor": "healthy",
            "dlq_recovery": "unknown"
        }
    });

    // Check DLQ stats
    if let Ok(dlq_stats) = processor.get_dlq_stats().await {
        let total_messages = dlq_stats["total_messages"].as_u64().unwrap_or(0);
        let oldest_age = dlq_stats["oldest_message_age_seconds"].as_u64().unwrap_or(0);

        health["services"]["dlq_recovery"] = if total_messages == 0 {
            serde_json::json!("healthy")
        } else if oldest_age > 86400 { // Older than 24 hours
            serde_json::json!("warning")
        } else {
            serde_json::json!("healthy")
        };

        health["dlq_stats"] = dlq_stats;
    }

    Json(health)
}
```

---

## Monitoring & Observability

### 1. Key Metrics

| Metric                      | Description                       | Alert Threshold   |
| --------------------------- | --------------------------------- | ----------------- |
| `dlq_messages_consumed`     | Total messages read from DLQ      | -                 |
| `dlq_messages_reprocessed`  | Successfully reprocessed messages | -                 |
| `dlq_messages_failed`       | Messages that failed reprocessing | > 10% of consumed |
| `dlq_processing_latency_ms` | Processing time for batches       | > 5000ms          |
| `dlq_consumer_restarts`     | Number of consumer restarts       | > 5 per hour      |

### 2. Prometheus Metrics (Example)

```rust
// Example Prometheus metric names
dlq_messages_total{status="consumed"} 150
dlq_messages_total{status="reprocessed"} 145
dlq_messages_total{status="failed"} 5
dlq_processing_duration_seconds 2.5
dlq_consumer_restarts_total 2
```

### 3. Logging

The system provides comprehensive logging:

```rust
// Example log messages
INFO  - Starting manual DLQ reprocessing
INFO  - Successfully reprocessed DLQ message: 123e4567-e89b-12d3-a456-426614174000
WARN  - Retrying DLQ message 123e4567-e89b-12d3-a456-426614174000 (attempt 2): Business validation failed
ERROR - Failed to reprocess DLQ message 123e4567-e89b-12d3-a456-426614174000: Insufficient funds
INFO  - Manual DLQ reprocessing completed: 145 processed, 5 failed
```

---

## Error Handling

### 1. Retry Strategy

```rust
// Exponential backoff retry logic
let mut retries = 0;
loop {
    match processor.try_process_event(&processable_event).await {
        Ok(_) => return Ok(()),
        Err(e) => {
            retries += 1;
            if retries > config.max_reprocessing_retries {
                return Err(e);
            } else {
                let backoff = config.reprocessing_backoff_ms * retries as u64;
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
        }
    }
}
```

### 2. Error Types

- **Business Validation Errors**: Invalid transaction amounts, insufficient funds
- **Serialization Errors**: Corrupted event data
- **Database Errors**: Connection issues, constraint violations
- **Network Errors**: Kafka connectivity issues

### 3. Error Recovery

- **Transient Errors**: Automatically retried
- **Permanent Errors**: Logged and tracked in metrics
- **System Errors**: Trigger circuit breaker if needed

---

## Performance Considerations

### 1. Batch Processing

- **Batch Size**: Configurable (default: 10 messages)
- **Memory Usage**: Efficient memory management with shared caches
- **Throughput**: Optimized for high-volume processing

### 2. Resource Management

- **Connection Pooling**: Reuses Kafka connections
- **Memory Limits**: Configurable cache sizes
- **CPU Usage**: Non-blocking async processing

### 3. Scalability

- **Horizontal Scaling**: Multiple DLQ consumers can run simultaneously
- **Partitioning**: DLQ messages are partitioned by aggregate ID
- **Load Balancing**: Automatic load distribution across consumers

### 4. Monitoring Performance

```rust
// Monitor DLQ processing performance
let metrics = dlq_consumer.get_metrics();
let latency = metrics.dlq_processing_latency_ms.load(Ordering::Relaxed);
let throughput = metrics.dlq_messages_reprocessed.load(Ordering::Relaxed);

if latency > 5000 {
    warn!("DLQ processing latency is high: {}ms", latency);
}

if throughput < 10 {
    warn!("DLQ processing throughput is low: {} messages", throughput);
}
```

---

## Configuration Best Practices

### 1. Production Settings

```rust
let production_config = DLQRecoveryConfig {
    dlq_topic: "banking-es-dlq".to_string(),
    consumer_group_id: "banking-es-dlq-recovery-prod".to_string(),
    max_reprocessing_retries: 3,              // Lower for production
    reprocessing_backoff_ms: 2000,            // Higher backoff
    batch_size: 50,                           // Larger batches
    poll_timeout_ms: 100,
    enable_auto_recovery: true,
    auto_recovery_interval_secs: 300,         // Every 5 minutes
};
```

### 2. Development Settings

```rust
let development_config = DLQRecoveryConfig {
    dlq_topic: "banking-es-dlq-dev".to_string(),
    consumer_group_id: "banking-es-dlq-recovery-dev".to_string(),
    max_reprocessing_retries: 5,              // More retries for debugging
    reprocessing_backoff_ms: 500,             // Faster retries
    batch_size: 10,                           // Smaller batches
    poll_timeout_ms: 100,
    enable_auto_recovery: false,              // Manual only in dev
    auto_recovery_interval_secs: 60,
};
```

---

## Troubleshooting

### 1. Common Issues

**High DLQ Message Count**

- Check business logic validation rules
- Review error logs for patterns
- Consider adjusting retry limits

**Slow DLQ Processing**

- Increase batch size
- Reduce processing latency
- Check system resources

**Consumer Restarts**

- Review Kafka connectivity
- Check consumer group configuration
- Monitor system resources

### 2. Debug Commands

```bash
# Check DLQ topic messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic banking-es-dlq --from-beginning

# Monitor consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group banking-es-dlq-recovery

# Check topic partitions
kafka-topics --bootstrap-server localhost:9092 --describe --topic banking-es-dlq
```

---

## Conclusion

The DLQ Recovery/Consumer Implementation provides a robust, scalable, and observable solution for handling failed events in the CDC processing pipeline. It integrates seamlessly with existing systems while providing comprehensive monitoring and recovery capabilities.

Key benefits:

- **Reliability**: Ensures no events are lost
- **Observability**: Comprehensive metrics and logging
- **Flexibility**: Configurable for different environments
- **Performance**: Optimized for high-throughput processing
- **Maintainability**: Clean separation of concerns

This implementation is production-ready and can handle the demands of a high-volume banking system while maintaining data consistency and providing excellent observability.
