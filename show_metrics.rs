use banking_es::infrastructure::cache_service::RealRedisClient;
use banking_es::infrastructure::cdc_debezium::{CDCOutboxRepository, DebeziumConfig};
use banking_es::infrastructure::cdc_service_manager::CDCServiceManager;
use banking_es::infrastructure::kafka_abstraction::{KafkaConfig, KafkaConsumer, KafkaProducer};
use banking_es::infrastructure::projections::ProjectionStore;
use sqlx::PgPool;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🔍 CDC Metrics Display Tool");
    println!("==========================");

    // Create database pool
    let database_url = "postgresql://postgres:Francisco1@localhost:5432/banking_es";
    let pool = PgPool::connect(database_url).await?;
    let pool = Arc::new(pool);

    // Create Redis client
    let redis_url = "redis://localhost:6379";
    let redis_client = RealRedisClient::new(redis_url)?;
    let cache_service = Arc::new(redis_client);

    // Create projection store
    let projection_store = Arc::new(ProjectionStore::new(pool.clone()));

    // Create Kafka producer and consumer
    let kafka_config = KafkaConfig::default();
    let kafka_producer = KafkaProducer::new(kafka_config.clone())?;
    let kafka_consumer = KafkaConsumer::new(kafka_config)?;

    // Create CDC outbox repository
    let outbox_repo = Arc::new(CDCOutboxRepository::new(pool.as_ref().clone()));

    // Create Debezium config
    let debezium_config = DebeziumConfig::default();

    // Create ConsistencyManager for metrics collection
    let consistency_manager = Arc::new(
        banking_es::infrastructure::consistency_manager::ConsistencyManager::new(
            std::time::Duration::from_secs(30), // max_wait_time
            std::time::Duration::from_secs(60), // cleanup_interval
        ),
    );

    // Create CDC service manager
    let mut cdc_service_manager = CDCServiceManager::new(
        debezium_config,
        outbox_repo,
        kafka_producer,
        kafka_consumer,
        cache_service,
        projection_store,
        None,
        Some(consistency_manager.clone()),
    )?;

    // Start the service briefly to get metrics
    println!("🚀 Starting CDC service to collect metrics...");
    cdc_service_manager.start().await?;

    // Wait a moment for metrics to accumulate
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Get service status and metrics
    let status = cdc_service_manager.get_service_status().await;
    println!("\n📊 CDC Service Status:");
    println!("{}", serde_json::to_string_pretty(&status)?);

    // Get optimized metrics
    let optimized_metrics = cdc_service_manager.processor_arc().get_metrics().await;
    println!("\n🚀 Optimized CDC Metrics:");
    println!(
        "Events Processed: {}",
        optimized_metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Events Failed: {}",
        optimized_metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Processing Latency (ms): {}",
        optimized_metrics
            .processing_latency_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Total Latency (ms): {}",
        optimized_metrics
            .total_latency_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Cache Invalidations: {}",
        optimized_metrics
            .cache_invalidations
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Projection Updates: {}",
        optimized_metrics
            .projection_updates
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Batches Processed: {}",
        optimized_metrics
            .batches_processed
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Circuit Breaker Trips: {}",
        optimized_metrics
            .circuit_breaker_trips
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Consumer Restarts: {}",
        optimized_metrics
            .consumer_restarts
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Memory Usage (bytes): {}",
        optimized_metrics
            .memory_usage_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Throughput (events/sec): {}",
        optimized_metrics
            .throughput_per_second
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "Error Rate (%): {}",
        optimized_metrics
            .error_rate
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    // Stop the service
    cdc_service_manager.stop().await?;
    println!("\n✅ CDC service stopped");

    Ok(())
}
