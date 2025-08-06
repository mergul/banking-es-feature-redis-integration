# CDC Integration Guide

## Overview

This guide demonstrates how to integrate the optimized CDC producer and event processor with the existing CDC Debezium implementation. The integration provides enhanced performance, business logic validation, and comprehensive monitoring.

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Application   │───▶│  CDC Producer    │───▶│  CDC Outbox     │
│   (Commands)    │    │  (Optimized)     │    │  (Database)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Projections   │◀───│ CDC Event Proc.  │◀───│   Debezium      │
│   (Read Model)  │    │  (Optimized)     │    │   (CDC)         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Integration Components

### 1. Optimized CDC Producer

- **Purpose**: Handles message production with business validation
- **Features**: Batch processing, deduplication, circuit breaker
- **Location**: `src/infrastructure/cdc_producer.rs`

### 2. Optimized CDC Event Processor

- **Purpose**: Processes CDC events with business logic validation
- **Features**: Batch processing, duplicate detection, memory optimization
- **Location**: `src/infrastructure/cdc_event_processor.rs`

### 3. Enhanced CDC Service Manager

- **Purpose**: Orchestrates the entire CDC pipeline
- **Features**: Integrates both producer and processor
- **Location**: `src/infrastructure/cdc_debezium.rs`

## Usage Examples

### 1. Basic Integration

```rust
use crate::infrastructure::cdc_debezium::{
    CDCServiceManager, DebeziumConfig, CDCOutboxRepository
};
use crate::infrastructure::cdc_producer::CDCProducerConfig;
use crate::infrastructure::cdc_event_processor::BusinessLogicConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create configurations
    let debezium_config = DebeziumConfig::default();

    let business_config = BusinessLogicConfig {
        enable_validation: true,
        max_balance: Decimal::from_str("999999999.99").unwrap(),
        min_balance: Decimal::ZERO,
        max_transaction_amount: Decimal::from_str("1000000.00").unwrap(),
        enable_duplicate_detection: true,
        cache_invalidation_delay_ms: 10,
        batch_processing_enabled: true,
    };

    let producer_config = CDCProducerConfig {
        batch_size: 100,
        batch_timeout_ms: 1000,
        validation_enabled: true,
        max_message_size_bytes: 1024 * 1024,
        ..Default::default()
    };

    // 2. Create dependencies
    let pool = sqlx::PgPool::connect("postgresql://...").await?;
    let outbox_repo = Arc::new(CDCOutboxRepository::new(pool.clone()));

    let kafka_producer = KafkaProducer::new(kafka_config).await?;
    let kafka_consumer = KafkaConsumer::new(kafka_config).await?;

    let cache_service = Arc::new(CacheService::new(cache_config));
    let projection_store = Arc::new(ProjectionStore::new(pool.clone()));

    // 3. Create CDC Service Manager
    let mut cdc_manager = CDCServiceManager::new(
        debezium_config,
        outbox_repo,
        kafka_producer,
        kafka_consumer,
        cache_service,
        projection_store,
        Some(business_config),
        Some(producer_config),
    )?;

    // 4. Start the CDC service
    cdc_manager.start().await?;

    // 5. Use the service
    let message = CDCOutboxMessage {
        id: Uuid::new_v4(),
        aggregate_id: Uuid::new_v4(),
        event_id: Uuid::new_v4(),
        event_type: "AccountCreated".to_string(),
        topic: "banking-es.public.kafka_outbox_cdc".to_string(),
        metadata: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    cdc_manager.produce_message(message).await?;

    // 6. Monitor the service
    let metrics = cdc_manager.get_metrics();
    let optimized_metrics = cdc_manager.get_optimized_metrics().await;
    let producer_health = cdc_manager.get_producer_health().await;

    println!("Events processed: {}", metrics.events_processed.load(std::sync::atomic::Ordering::Relaxed));
    println!("Business validation failures: {}", optimized_metrics.business_validation_failures.load(std::sync::atomic::Ordering::Relaxed));

    // 7. Graceful shutdown
    cdc_manager.stop().await?;

    Ok(())
}
```

### 2. Advanced Configuration

```rust
// Custom business logic configuration
let business_config = BusinessLogicConfig {
    enable_validation: true,
    max_balance: Decimal::from_str("500000.00").unwrap(), // Lower limit for testing
    min_balance: Decimal::ZERO,
    max_transaction_amount: Decimal::from_str("50000.00").unwrap(), // Lower transaction limit
    enable_duplicate_detection: true,
    cache_invalidation_delay_ms: 5, // Very aggressive cache invalidation
    batch_processing_enabled: true,
};

// High-performance producer configuration
let producer_config = CDCProducerConfig {
    batch_size: 500, // Larger batches for high throughput
    batch_timeout_ms: 50, // Very aggressive batching
    max_retries: 5,
    retry_delay_ms: 50,
    health_check_interval_ms: 1000, // More frequent health checks
    circuit_breaker_threshold: 10,
    circuit_breaker_timeout_ms: 60000, // 1 minute timeout
    enable_compression: true,
    enable_idempotence: true,
    max_in_flight_requests: 10,
    validation_enabled: true,
    max_message_size_bytes: 2 * 1024 * 1024, // 2MB limit
};
```

### 3. Health Monitoring

```rust
// Create comprehensive health check
let health_check = CDCHealthCheck::new(Arc::clone(&metrics))
    .with_optimized_metrics(optimized_metrics)
    .with_producer_health(producer_health.unwrap_or_default());

let health_status = health_check.get_health_status();
println!("Health status: {}", serde_json::to_string_pretty(&health_status)?);

// Check specific health indicators
if health_check.is_healthy() {
    println!("CDC service is healthy");
} else {
    println!("CDC service has issues");

    // Check specific components
    if let Some(ref prod_health) = producer_health {
        if !prod_health.is_healthy {
            println!("Producer is unhealthy: {:?}", prod_health.issues);
        }
    }
}
```

### 4. Metrics and Monitoring

```rust
// Get comprehensive metrics
let legacy_metrics = cdc_manager.get_metrics();
let optimized_metrics = cdc_manager.get_optimized_metrics().await;
let producer_metrics = cdc_manager.get_producer_metrics().await;

// Legacy metrics
println!("Events processed: {}", legacy_metrics.events_processed.load(std::sync::atomic::Ordering::Relaxed));
println!("Events failed: {}", legacy_metrics.events_failed.load(std::sync::atomic::Ordering::Relaxed));
println!("Cache invalidations: {}", legacy_metrics.cache_invalidations.load(std::sync::atomic::Ordering::Relaxed));

// Optimized metrics
println!("Business validation failures: {}", optimized_metrics.business_validation_failures.load(std::sync::atomic::Ordering::Relaxed));
println!("Duplicate events skipped: {}", optimized_metrics.duplicate_events_skipped.load(std::sync::atomic::Ordering::Relaxed));
println!("Projection update failures: {}", optimized_metrics.projection_update_failures.load(std::sync::atomic::Ordering::Relaxed));
println!("Memory usage: {} bytes", optimized_metrics.memory_usage_bytes.load(std::sync::atomic::Ordering::Relaxed));

// Producer metrics
if let Some(prod_metrics) = producer_metrics {
    println!("Messages produced: {}", prod_metrics.messages_produced.load(std::sync::atomic::Ordering::Relaxed));
    println!("Success rate: {:.2}%", prod_metrics.get_success_rate());
    println!("Average latency: {}ms", prod_metrics.get_avg_latency());
    println!("Validation failures: {}", prod_metrics.validation_failures.load(std::sync::atomic::Ordering::Relaxed));
}
```

### 5. Error Handling and Recovery

```rust
// Handle producer errors
match cdc_manager.produce_message(message).await {
    Ok(_) => println!("Message produced successfully"),
    Err(e) => {
        if e.to_string().contains("Circuit breaker is open") {
            println!("Producer circuit breaker is open, retrying later");
            // Implement retry logic
        } else if e.to_string().contains("Business validation failed") {
            println!("Business validation failed: {}", e);
            // Handle validation errors
        } else {
            println!("Producer error: {}", e);
            // Handle other errors
        }
    }
}

// Monitor circuit breaker status
if let Some(producer) = &cdc_manager.cdc_producer {
    let health = producer.get_health_status().await;
    if health.circuit_breaker_open {
        println!("Circuit breaker is open, service is degraded");
    }
}
```

### 6. Dynamic Configuration Updates

```rust
// Update business logic configuration at runtime
let mut new_business_config = cdc_manager.processor.get_business_config().clone();
new_business_config.max_transaction_amount = Decimal::from_str("75000.00").unwrap();
new_business_config.enable_validation = false; // Disable validation temporarily

cdc_manager.processor.update_business_config(new_business_config);

println!("Business configuration updated");
```

### 7. Integration with Existing CQRS System

```rust
// Use CDC outbox repository as drop-in replacement
let cdc_outbox_repo = Arc::new(CDCOutboxRepository::new(pool.clone()));

// The CDC outbox repository implements the existing OutboxRepositoryTrait
let command_bus = CommandBus::new(
    event_store,
    cdc_outbox_repo, // Use CDC outbox instead of regular outbox
    db_pool,
    kafka_config,
);

// Commands will now use the optimized CDC pipeline
let result = command_bus.execute(CreateAccountCommand {
    account_id: Uuid::new_v4(),
    owner_name: "John Doe".to_string(),
    initial_balance: Decimal::from_str("1000.00").unwrap(),
}).await?;
```

## Performance Tuning

### 1. Batch Size Optimization

```rust
// For high-throughput scenarios
let producer_config = CDCProducerConfig {
    batch_size: 1000, // Large batches
    batch_timeout_ms: 25, // Very short timeout
    ..Default::default()
};

// For low-latency scenarios
let producer_config = CDCProducerConfig {
    batch_size: 10, // Small batches
    batch_timeout_ms: 5000, // Longer timeout
    ..Default::default()
};
```

### 2. Memory Management

```rust
// Monitor memory usage
let memory_usage = cdc_manager.processor.optimized_processor.get_memory_usage().await;
println!("Current memory usage: {} bytes", memory_usage);

// Clean up expired cache entries
cdc_manager.processor.optimized_processor.clear_expired_cache_entries().await;
```

### 3. Circuit Breaker Tuning

```rust
let producer_config = CDCProducerConfig {
    circuit_breaker_threshold: 3, // More sensitive
    circuit_breaker_timeout_ms: 15000, // Faster recovery
    ..Default::default()
};
```

## Best Practices

### 1. Gradual Rollout

```rust
// Start with validation disabled
let business_config = BusinessLogicConfig {
    enable_validation: false, // Disable initially
    ..Default::default()
};

// Enable after monitoring
let business_config = BusinessLogicConfig {
    enable_validation: true, // Enable after confidence
    ..Default::default()
};
```

### 2. Monitoring Setup

```rust
// Set up alerts for critical metrics
let health_check = CDCHealthCheck::new(metrics);
if !health_check.is_healthy() {
    // Send alert
    send_alert("CDC service is unhealthy").await;
}

// Monitor business validation failures
let optimized_metrics = cdc_manager.get_optimized_metrics().await;
let validation_failures = optimized_metrics.business_validation_failures.load(std::sync::atomic::Ordering::Relaxed);
if validation_failures > 100 {
    // Send alert
    send_alert("High business validation failure rate").await;
}
```

### 3. Error Handling

```rust
// Implement graceful degradation
match cdc_manager.produce_message(message).await {
    Ok(_) => println!("Message produced"),
    Err(e) => {
        if e.to_string().contains("Circuit breaker") {
            // Store message for later processing
            store_message_for_retry(message).await;
        } else {
            // Log and continue
            error!("Failed to produce message: {}", e);
        }
    }
}
```

## Migration Strategy

### Phase 1: Parallel Deployment

1. Deploy optimized components alongside existing system
2. Monitor performance and stability
3. Gradually shift traffic to optimized components

### Phase 2: Full Migration

1. Enable business validation
2. Monitor for validation failures
3. Adjust business rules as needed

### Phase 3: Optimization

1. Tune batch sizes and timeouts
2. Monitor memory usage
3. Optimize circuit breaker settings

## Troubleshooting

### Common Issues

1. **High Validation Failure Rate**

   - Check business rule configuration
   - Review event data for compliance
   - Adjust validation thresholds

2. **Memory Usage Issues**

   - Reduce cache TTL
   - Implement cache cleanup
   - Monitor for memory leaks

3. **Circuit Breaker Trips**

   - Check database connectivity
   - Monitor Kafka health
   - Adjust circuit breaker thresholds

4. **Performance Issues**
   - Optimize batch sizes
   - Check database performance
   - Monitor network latency

This integration guide provides a comprehensive approach to using the optimized CDC components while maintaining backward compatibility and providing enhanced monitoring and control capabilities.
