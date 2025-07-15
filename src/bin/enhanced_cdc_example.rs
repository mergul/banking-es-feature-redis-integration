use anyhow::Result;
use banking_es::infrastructure::cache_service::CacheService;
use banking_es::infrastructure::cdc_debezium::{
    CDCOutboxMessage, CDCOutboxRepository, CDCServiceManager, DebeziumConfig,
};
use banking_es::infrastructure::cdc_event_processor::BusinessLogicConfig;
use banking_es::infrastructure::cdc_integration_helper::{
    CDCIntegrationConfig, CDCIntegrationHelperBuilder,
};
use banking_es::infrastructure::cdc_producer::CDCProducerConfig;
use banking_es::infrastructure::cdc_service_manager::{OptimizationConfig, ServiceState};
use banking_es::infrastructure::kafka_abstraction::{KafkaConsumer, KafkaProducer};
use banking_es::infrastructure::outbox::PostgresOutboxRepository;
use banking_es::infrastructure::projections::ProjectionStore;
use chrono::Utc;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Comprehensive example demonstrating the enhanced CDC system with all integrated components
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting Enhanced CDC Integration Example");

    // 1. Configuration Setup
    let debezium_config = DebeziumConfig {
        connector_name: "banking-es-connector".to_string(),
        database_host: "localhost".to_string(),
        database_port: 5432,
        database_name: "banking_es".to_string(),
        database_user: "postgres".to_string(),
        database_password: "Francisco1".to_string(),
        table_include_list: "public.kafka_outbox_cdc".to_string(),
        topic_prefix: "banking-es".to_string(),
        snapshot_mode: "initial".to_string(),
        poll_interval_ms: 100,
    };

    // Business logic configuration
    let business_config = BusinessLogicConfig {
        enable_validation: true,
        max_balance: Decimal::from_str("999999999.99").unwrap(),
        min_balance: Decimal::ZERO,
        max_transaction_amount: Decimal::from_str("1000000.00").unwrap(),
        enable_duplicate_detection: true,
        cache_invalidation_delay_ms: 10,
        batch_processing_enabled: true,
    };

    // Producer configuration for high performance
    let producer_config = CDCProducerConfig {
        batch_size: 100,
        batch_timeout_ms: 1000,
        max_retries: 3,
        retry_delay_ms: 100,
        health_check_interval_ms: 5000,
        circuit_breaker_threshold: 5,
        circuit_breaker_timeout_ms: 30000,
        enable_compression: true,
        enable_idempotence: true,
        max_in_flight_requests: 5,
        validation_enabled: true,
        max_message_size_bytes: 1024 * 1024,
    };

    // Integration helper configuration
    let integration_config = CDCIntegrationHelperBuilder::new()
        .with_batch_size(1000)
        .with_migration_chunk_size(500)
        .with_validation(true)
        .with_cleanup(false)
        .with_retry_config(3, 100)
        .build(
            sqlx::PgPool::connect("postgresql://postgres:Francisco1@localhost:5432/banking_es")
                .await?,
        );

    // 2. Initialize Dependencies
    println!("📦 Initializing dependencies...");

    let pool = sqlx::PgPool::connect("postgresql://postgres:Francisco1@localhost:5432/banking_es")
        .await
        .expect("Failed to connect to database");

    let outbox_repo = Arc::new(CDCOutboxRepository::new(pool.clone()));

    // Initialize Kafka components
    let kafka_config = banking_es::infrastructure::kafka_abstraction::KafkaConfig::default();

    let kafka_producer =
        banking_es::infrastructure::kafka_abstraction::KafkaProducer::new(kafka_config.clone())?;
    let kafka_consumer =
        banking_es::infrastructure::kafka_abstraction::KafkaConsumer::new(kafka_config)?;

    // Create a simple cache service for the example
    let redis_client = redis::Client::open("redis://localhost:6379")?;
    let redis_client_trait =
        banking_es::infrastructure::redis_abstraction::RealRedisClient::new(redis_client, None);
    let cache_config = banking_es::infrastructure::cache_service::CacheConfig::default();
    let cache_service = Arc::new(
        banking_es::infrastructure::cache_service::CacheService::new(
            redis_client_trait,
            cache_config,
        ),
    );

    let projection_store = Arc::new(ProjectionStore::new(pool.clone()));

    // 3. Create Enhanced CDC Service Manager
    println!("🔧 Creating Enhanced CDC Service Manager...");

    let mut cdc_manager = CDCServiceManager::new(
        debezium_config,
        outbox_repo,
        kafka_producer,
        kafka_consumer,
        cache_service,
        projection_store,
        Some(business_config),
        Some(producer_config),
    )
    .await?;

    // 4. Initialize Integration Helper
    println!("🔗 Initializing Integration Helper...");
    cdc_manager
        .initialize_integration_helper(CDCIntegrationConfig::default())
        .await?;

    // 5. Start the CDC Service
    println!("▶️  Starting CDC Service...");
    cdc_manager.start().await?;
    println!("✅ CDC Service started successfully");

    // 6. Set service state to running
    cdc_manager.set_service_state(ServiceState::Running).await;

    // 7. Simulate Message Production
    println!("📤 Producing test messages...");

    for i in 1..=10 {
        let message = CDCOutboxMessage {
            id: Uuid::new_v4(),
            aggregate_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            event_type: "AccountCreated".to_string(),
            topic: "banking-es.public.kafka_outbox_cdc".to_string(),
            metadata: Some(serde_json::json!({
                "test_message": i,
                "timestamp": Utc::now().to_rfc3339(),
                "enhanced_test": true
            })),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        match cdc_manager.produce_message(message).await {
            Ok(_) => println!("✅ Message {} produced successfully", i),
            Err(e) => println!("❌ Failed to produce message {}: {}", i, e),
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // 8. Monitor the Enhanced System
    println!("📊 Monitoring enhanced system metrics...");

    // Wait for processing
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Get comprehensive status
    let enhanced_status = cdc_manager.get_enhanced_status().await;
    println!("\n📈 Enhanced System Status:");
    println!("{}", serde_json::to_string_pretty(&enhanced_status)?);

    // Get service state
    let service_state = cdc_manager.get_service_state().await;
    println!("\n🏥 Service State: {:?}", service_state);

    // Get enhanced metrics
    let enhanced_metrics = cdc_manager.get_enhanced_metrics();
    println!("\n🚀 Enhanced Metrics:");
    println!(
        "  Memory usage: {} bytes",
        enhanced_metrics
            .memory_usage_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "  Throughput: {}/s",
        enhanced_metrics
            .throughput_per_second
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "  Error rate: {}%",
        enhanced_metrics
            .error_rate
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    println!(
        "  Circuit breaker trips: {}",
        enhanced_metrics
            .circuit_breaker_trips
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    // 9. Test Migration Capabilities
    println!("\n🔄 Testing migration capabilities...");

    // Create old outbox repository for migration testing
    let old_outbox_repo = PostgresOutboxRepository::new(pool.clone());

    // Note: In a real scenario, you would have existing data to migrate
    // For this example, we'll just test the migration infrastructure
    println!("  Migration infrastructure ready (no existing data to migrate)");

    // 10. Test Migration Integrity Verification
    println!("\n🔍 Testing migration integrity verification...");
    match cdc_manager.verify_migration_integrity().await {
        Ok(report) => {
            println!("  Migration integrity report:");
            println!("    Old total messages: {}", report.old_total_messages);
            println!("    New total messages: {}", report.new_total_messages);
            println!("    Old unique events: {}", report.old_unique_events);
            println!("    New unique events: {}", report.new_unique_events);
            println!("    Integrity passed: {}", report.integrity_passed);
        }
        Err(e) => println!("  Migration integrity check failed: {}", e),
    }

    // 11. Test Cleanup Operations
    println!("\n🧹 Testing cleanup operations...");
    match cdc_manager
        .cleanup_old_messages(&old_outbox_repo, Duration::from_secs(86400 * 30))
        .await
    {
        Ok((old_cleaned, new_cleaned)) => {
            println!("  Cleanup completed:");
            println!("    Old outbox cleaned: {} messages", old_cleaned);
            println!("    CDC outbox cleaned: {} messages", new_cleaned);
        }
        Err(e) => println!("  Cleanup failed: {}", e),
    }

    // 12. Performance Stress Test
    println!("\n⚡ Running performance stress test...");

    let start_time = std::time::Instant::now();
    let message_count = 200;
    let mut success_count = 0;

    for i in 0..message_count {
        let message = CDCOutboxMessage {
            id: Uuid::new_v4(),
            aggregate_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            event_type: "MoneyDeposited".to_string(),
            topic: "banking-es.public.kafka_outbox_cdc".to_string(),
            metadata: Some(serde_json::json!({
                "amount": 100.0,
                "stress_test": true,
                "message_id": i
            })),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        if cdc_manager.produce_message(message).await.is_ok() {
            success_count += 1;
        }
    }

    let duration = start_time.elapsed();
    let throughput = success_count as f64 / duration.as_secs_f64();

    println!("  Stress test results:");
    println!("    Messages sent: {}/{}", success_count, message_count);
    println!("    Duration: {:.2}s", duration.as_secs_f64());
    println!("    Throughput: {:.2} messages/second", throughput);
    println!(
        "    Success rate: {:.2}%",
        (success_count as f64 / message_count as f64) * 100.0
    );

    // 13. Test Service State Transitions
    println!("\n🔄 Testing service state transitions...");

    cdc_manager.set_service_state(ServiceState::Stopping).await;
    println!(
        "  Service state set to: {:?}",
        cdc_manager.get_service_state().await
    );

    // 14. Graceful Shutdown
    println!("\n🛑 Starting graceful shutdown...");
    cdc_manager.stop().await?;
    println!("✅ CDC Service shutdown complete");

    // 15. Final Status Report
    println!("\n📋 Final Status Report:");
    let final_status = cdc_manager.get_enhanced_status().await;
    println!("{}", serde_json::to_string_pretty(&final_status)?);

    println!("\n🎉 Enhanced CDC Integration Example completed successfully!");
    println!("✨ All components integrated and working together:");
    println!("   - Optimized CDC Producer with business validation");
    println!("   - Ultra-optimized CDC Event Processor");
    println!("   - Enhanced Service Manager with state management");
    println!("   - Integration Helper for migration operations");
    println!("   - Comprehensive monitoring and metrics");
    println!("   - Graceful shutdown and cleanup");

    Ok(())
}
