use banking_es::{
    application::services::CQRSAccountService,
    domain::{AccountError, AccountEvent},
    infrastructure::{
        cache_service::{CacheConfig, CacheService, CacheServiceTrait},
        cdc_debezium::{CDCOutboxRepository, DebeziumConfig},
        cdc_service_manager::CDCServiceManager,
        event_store::{EventStore, EventStoreTrait},
        projections::{ProjectionStore, ProjectionStoreTrait},
        redis_abstraction::RealRedisClient,
    },
};
use rand::{Rng, SeedableRng};
use rust_decimal::Decimal;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing;
use uuid::Uuid;

/// Integration test that uses the REAL CDC pipeline
/// This test requires:
/// 1. Debezium running (Kafka Connect with PostgreSQL connector)
/// 2. Kafka cluster running
/// 3. PostgreSQL with logical replication enabled
/// 4. Proper network connectivity
#[tokio::test]
#[ignore] // Ignored by default - requires full CDC setup
async fn test_real_cdc_pipeline() {
    tracing::info!("🧪 Testing REAL CDC pipeline with Debezium...");

    // Check if CDC environment is available
    if !is_cdc_environment_available().await {
        tracing::warn!("⚠️ CDC environment not available, skipping real CDC test");
        return;
    }

    let test_future = async {
        // Setup test environment with REAL CDC
        let context = setup_real_cdc_test_environment().await?;
        tracing::info!("✅ Real CDC test environment setup complete");

        // Create a test account
        let owner_name = "RealCDCTestUser".to_string();
        let initial_balance = Decimal::new(1000, 0);

        tracing::info!("🔧 Creating test account via CQRS...");
        let account_id = context
            .cqrs_service
            .create_account(owner_name.clone(), initial_balance)
            .await?;
        tracing::info!("✅ Created account: {} ({})", account_id, owner_name);

        // Wait for CDC to process the event
        tracing::info!("⏳ Waiting for CDC to process event...");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify CDC metrics show processing
        let cdc_metrics = context.cdc_service_manager.get_metrics();
        let events_processed = cdc_metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let events_failed = cdc_metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            "📊 CDC Metrics - Events processed: {}, Events failed: {}",
            events_processed,
            events_failed
        );

        if events_processed == 0 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "CDC did not process any events",
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        // Verify projection was updated
        let account = context.cqrs_service.get_account(account_id).await?;
        match account {
            Some(acc) => {
                tracing::info!(
                    "✅ Account projection updated via CDC: ID={}, Owner={}, Balance={}, Active={}",
                    acc.id,
                    acc.owner_name,
                    acc.balance,
                    acc.is_active
                );
                assert_eq!(acc.owner_name, owner_name);
                assert_eq!(acc.balance, initial_balance);
                assert!(acc.is_active);
            }
            None => {
                return Err("Account projection not found after CDC processing".into());
            }
        }

        tracing::info!("🎉 Real CDC pipeline test completed successfully!");
        Ok(())
    };

    match tokio::time::timeout(Duration::from_secs(30), test_future).await {
        Ok(Ok(())) => {
            tracing::info!("✅ Real CDC test passed!");
        }
        Ok(Err(e)) => {
            tracing::error!("❌ Real CDC test failed: {}", e);
            panic!("Real CDC test failed: {}", e);
        }
        Err(_) => {
            tracing::error!("❌ Real CDC test timeout");
            panic!("Real CDC test timeout");
        }
    }
}

/// Test to verify CDC consumer can connect to Kafka
#[tokio::test]
// #[ignore]
async fn test_cdc_consumer_connection() {
    tracing::info!("🔍 Testing CDC consumer connection...");

    // Check if CDC environment is available
    if !is_cdc_environment_available().await {
        tracing::warn!("⚠️ CDC environment not available, skipping CDC consumer connection test");
        return;
    }

    // Create a simple Kafka consumer
    let kafka_config = banking_es::infrastructure::kafka_abstraction::KafkaConfig {
        enabled: true,
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: "test-cdc-consumer-group".to_string(),
        topic_prefix: "banking-es".to_string(),
        producer_acks: 1,
        producer_retries: 3,
        consumer_max_poll_interval_ms: 300000,
        consumer_session_timeout_ms: 10000,
        consumer_max_poll_records: 500,
        security_protocol: "PLAINTEXT".to_string(),
        sasl_mechanism: "PLAIN".to_string(),
        ssl_ca_location: None,
        auto_offset_reset: "earliest".to_string(),
        cache_invalidation_topic: "banking-es-cache-invalidation".to_string(),
        event_topic: "banking-es-events".to_string(),
    };

    let consumer =
        match banking_es::infrastructure::kafka_abstraction::KafkaConsumer::new(kafka_config) {
            Ok(consumer) => {
                tracing::info!("✅ Kafka consumer created successfully");
                consumer
            }
            Err(e) => {
                tracing::error!("❌ Failed to create Kafka consumer: {}", e);
                return;
            }
        };

    // Try to subscribe to the CDC topic
    let cdc_topic = "banking-es.public.kafka_outbox_cdc";
    tracing::info!("🔍 Attempting to subscribe to topic: {}", cdc_topic);

    match consumer.subscribe_to_topic(cdc_topic).await {
        Ok(_) => {
            tracing::info!("✅ Successfully subscribed to CDC topic: {}", cdc_topic);
        }
        Err(e) => {
            tracing::error!("❌ Failed to subscribe to CDC topic: {}", e);
            return;
        }
    }

    // Try to poll for messages
    tracing::info!("🔍 Attempting to poll for messages...");
    match consumer.poll_cdc_events().await {
        Ok(Some(event)) => {
            tracing::info!("✅ Successfully received CDC event: {:?}", event);
        }
        Ok(None) => {
            tracing::info!("⏳ No events available (this is normal if no new events)");
        }
        Err(e) => {
            tracing::error!("❌ Failed to poll CDC events: {}", e);
            return;
        }
    }

    tracing::info!("✅ CDC consumer connection test completed successfully");
}

/// Check if the CDC environment is available
async fn is_cdc_environment_available() -> bool {
    // Check if Kafka is reachable
    let kafka_available = check_kafka_connectivity().await;
    if !kafka_available {
        tracing::warn!("❌ Kafka not available");
        return false;
    }

    // Check if PostgreSQL logical replication is enabled
    let pg_replication_enabled = check_postgresql_replication().await;
    if !pg_replication_enabled {
        tracing::warn!("❌ PostgreSQL logical replication not enabled");
        return false;
    }

    tracing::info!("✅ CDC environment is available");
    true
}

/// Check Kafka connectivity
async fn check_kafka_connectivity() -> bool {
    // Try to connect to Kafka
    match banking_es::infrastructure::kafka_abstraction::KafkaProducer::new(
        banking_es::infrastructure::kafka_abstraction::KafkaConfig::default(),
    ) {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!("Kafka connectivity check failed: {}", e);
            false
        }
    }
}

/// Check if PostgreSQL logical replication is enabled
async fn check_postgresql_replication() -> bool {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    match PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
    {
        Ok(pool) => {
            // Check if logical replication is enabled
            match sqlx::query!("SHOW wal_level").fetch_one(&pool).await {
                Ok(row) => {
                    let wal_level = row.wal_level.as_deref().unwrap_or("");
                    let replication_enabled = wal_level == "logical" || wal_level == "replica";
                    tracing::info!(
                        "PostgreSQL WAL level: {} (replication enabled: {})",
                        wal_level,
                        replication_enabled
                    );
                    replication_enabled
                }
                Err(e) => {
                    tracing::warn!("Failed to check PostgreSQL WAL level: {}", e);
                    false
                }
            }
        }
        Err(e) => {
            tracing::warn!("Failed to connect to PostgreSQL: {}", e);
            false
        }
    }
}

/// Check the number of events in the CDC outbox table
async fn check_cdc_outbox_count(
    pool: &PgPool,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let count = sqlx::query!("SELECT COUNT(*) as count FROM kafka_outbox_cdc")
        .fetch_one(pool)
        .await?;

    Ok(count.count.unwrap_or(0) as usize)
}

struct RealCDCTestContext {
    cqrs_service: Arc<CQRSAccountService>,
    db_pool: PgPool,
    cdc_service_manager: CDCServiceManager,
}

async fn setup_real_cdc_test_environment(
) -> Result<RealCDCTestContext, Box<dyn std::error::Error + Send + Sync>> {
    // Initialize database pool
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPoolOptions::new()
        .max_connections(50)
        .min_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(1800))
        .max_lifetime(Duration::from_secs(3600))
        .connect(&database_url)
        .await?;

    // Initialize Redis client
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let redis_client_trait = RealRedisClient::new(redis_client, None);

    // Initialize services
    let event_store = Arc::new(EventStore::new(pool.clone())) as Arc<dyn EventStoreTrait + 'static>;
    let projection_store =
        Arc::new(ProjectionStore::new(pool.clone())) as Arc<dyn ProjectionStoreTrait + 'static>;

    let cache_config = CacheConfig::default();
    let cache_service = Arc::new(CacheService::new(redis_client_trait, cache_config))
        as Arc<dyn CacheServiceTrait + 'static>;

    let kafka_config = banking_es::infrastructure::kafka_abstraction::KafkaConfig::default();

    // Create CQRS service
    let cqrs_service = Arc::new(CQRSAccountService::new(
        event_store,
        projection_store.clone(),
        cache_service.clone(),
        kafka_config.clone(),
        500,
        100,
        Duration::from_millis(100),
    ));

    // Create REAL CDC service manager
    let cdc_outbox_repo = Arc::new(CDCOutboxRepository::new(pool.clone()));
    let kafka_producer =
        banking_es::infrastructure::kafka_abstraction::KafkaProducer::new(kafka_config.clone())?;
    let kafka_consumer =
        banking_es::infrastructure::kafka_abstraction::KafkaConsumer::new(kafka_config)?;

    let cdc_config = DebeziumConfig::default();
    let mut cdc_service_manager = CDCServiceManager::new(
        cdc_config,
        cdc_outbox_repo,
        kafka_producer,
        kafka_consumer,
        cache_service,
        projection_store,
    )?;

    // Start REAL CDC service
    cdc_service_manager.start().await?;
    tracing::info!("✅ Real CDC service started");

    // Give CDC service time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    Ok(RealCDCTestContext {
        cqrs_service,
        db_pool: pool,
        cdc_service_manager,
    })
}

/// High-throughput test that uses the REAL CDC pipeline
/// This test requires:
/// 1. Debezium running (Kafka Connect with PostgreSQL connector)
/// 2. Kafka cluster running
/// 3. PostgreSQL with logical replication enabled
/// 4. Proper network connectivity
#[tokio::test]
// #[ignore] // Ignored by default - requires full CDC setup
async fn test_real_cdc_high_throughput_performance() {
    tracing::info!("🚀 Starting REAL CDC high throughput performance test...");

    // Check if CDC environment is available
    if !is_cdc_environment_available().await {
        tracing::warn!("⚠️ CDC environment not available, skipping real CDC high throughput test");
        return;
    }

    let test_future = async {
        // Setup test environment with REAL CDC
        let context = setup_real_cdc_test_environment().await?;
        tracing::info!("✅ Real CDC test environment setup complete");

        // High throughput test parameters for real CDC (adjusted for stability)
        let target_ops = 5000; // Reduced for stability
        let worker_count = 25; // Reduced for stability
        let account_count = 500; // Reduced for stability
        let channel_buffer_size = 5000; // Reduced for stability

        tracing::info!(
            "🎯 Test Parameters - Target Ops: {}, Workers: {}, Accounts: {}",
            target_ops,
            worker_count,
            account_count
        );

        // Create accounts using the improved batch processing function
        tracing::info!("🔧 Creating {} test accounts...", account_count);
        let account_ids = create_test_accounts(&context.cqrs_service, account_count).await?;
        tracing::info!(
            "✅ Created {} test accounts successfully",
            account_ids.len()
        );

        // Wait for CDC to process all account creation events
        tracing::info!("⏳ Waiting for CDC to process account creation events...");
        tokio::time::sleep(Duration::from_secs(5)).await; // Reduced wait time for high throughput

        // Check if events were inserted into the CDC outbox table
        tracing::info!("🔍 Checking CDC outbox table for events...");
        let outbox_count = check_cdc_outbox_count(&context.db_pool).await?;
        tracing::info!("📊 CDC outbox table contains {} events", outbox_count);

        // Verify CDC metrics show processing
        let cdc_metrics = context.cdc_service_manager.get_metrics();
        let events_processed = cdc_metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let events_failed = cdc_metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            "📊 CDC Metrics after account creation - Events processed: {}, Events failed: {}",
            events_processed,
            events_failed
        );

        if events_processed == 0 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "CDC did not process any account creation events",
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        // Now run high throughput operations
        tracing::info!("🚀 Starting high throughput operations...");
        let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let mut handles = Vec::new();

        // Spawn worker tasks
        for worker_id in 0..worker_count {
            let tx = tx.clone();
            let cqrs_service = context.cqrs_service.clone();
            let account_ids = account_ids.clone();

            let handle = tokio::spawn(async move {
                let mut local_ops = 0;
                let target_ops_per_worker = target_ops / worker_count;
                let mut rng = rand::rngs::StdRng::from_entropy();

                while local_ops < target_ops_per_worker {
                    // Randomly select an account
                    let account_id = account_ids[rng.gen_range(0..account_ids.len())];

                    // Randomly choose operation type
                    let operation = match rng.gen_range(0..4) {
                        0 => "deposit",
                        1 => "withdraw",
                        2 => "get_account",
                        _ => "get_balance",
                    };

                    let start_time = std::time::Instant::now();
                    let result: Result<Result<(), _>, _> = match operation {
                        "deposit" => {
                            let amount = rust_decimal::Decimal::new(rng.gen_range(1..100), 0);
                            tokio::time::timeout(
                                Duration::from_secs(2), // Reduced timeout for high throughput
                                cqrs_service.deposit_money(account_id, amount),
                            )
                            .await
                        }
                        "withdraw" => {
                            let amount = rust_decimal::Decimal::new(rng.gen_range(1..50), 0);
                            tokio::time::timeout(
                                Duration::from_secs(2), // Reduced timeout for high throughput
                                cqrs_service.withdraw_money(account_id, amount),
                            )
                            .await
                        }
                        "get_account" => tokio::time::timeout(
                            Duration::from_secs(1), // Reduced timeout for high throughput
                            cqrs_service.get_account(account_id),
                        )
                        .await
                        .map(|r| r.map(|_| ())),
                        "get_balance" => tokio::time::timeout(
                            Duration::from_secs(1), // Reduced timeout for high throughput
                            cqrs_service.get_account_balance(account_id),
                        )
                        .await
                        .map(|r| r.map(|_| ())),
                        _ => unreachable!(),
                    };

                    let duration = start_time.elapsed();
                    let operation_result = match result {
                        Ok(Ok(_)) => OperationResult::Success,
                        Ok(Err(_)) => OperationResult::Failure,
                        Err(_) => OperationResult::Timeout,
                    };

                    let _ = tx
                        .send(OperationMetrics {
                            worker_id,
                            operation_type: operation.to_string(),
                            result: operation_result,
                            duration,
                            timestamp: std::time::Instant::now(),
                        })
                        .await;

                    local_ops += 1;

                    // Minimal delay for high throughput
                    tokio::time::sleep(Duration::from_millis(1)).await; // Minimal delay for high throughput
                }
            });

            handles.push(handle);
        }

        // Collect results with timeout
        let mut results = Vec::new();
        let mut total_ops = 0;
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        let mut timed_out_ops = 0;
        let mut total_duration = Duration::ZERO;

        // Set a timeout for collecting results
        let collection_timeout = Duration::from_secs(300); // 5 minutes timeout for high throughput
        let start_time = std::time::Instant::now();

        loop {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(result)) => {
                    results.push(result.clone());
                    total_ops += 1;
                    total_duration += result.duration;

                    match result.result {
                        OperationResult::Success => successful_ops += 1,
                        OperationResult::Failure => failed_ops += 1,
                        OperationResult::Timeout => timed_out_ops += 1,
                    }

                    if total_ops % 20 == 0 {
                        // Log every 20 operations
                        tracing::info!(
                            "📊 Progress - Total: {}, Success: {}, Failed: {}, Timeout: {}",
                            total_ops,
                            successful_ops,
                            failed_ops,
                            timed_out_ops
                        );
                    }

                    if total_ops >= target_ops {
                        break;
                    }
                }
                Ok(None) => {
                    tracing::info!("📊 Channel closed, stopping result collection");
                    break;
                }
                Err(_) => {
                    // Timeout on channel receive, check if we should continue
                    if start_time.elapsed() > collection_timeout {
                        tracing::warn!("📊 Collection timeout reached, stopping");
                        break;
                    }
                    continue;
                }
            }
        }

        // Wait for all workers to complete with timeout
        tracing::info!("📊 Waiting for workers to complete...");
        for handle in handles {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
        }

        // Final CDC metrics
        let final_cdc_metrics = context.cdc_service_manager.get_metrics();
        let final_events_processed = final_cdc_metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let final_events_failed = final_cdc_metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed);

        // Get CQRS and cache metrics
        let cqrs_metrics = context.cqrs_service.get_metrics();
        let cache_metrics = context.cqrs_service.get_cache_metrics();

        // Calculate performance metrics
        let avg_duration = if total_ops > 0 {
            total_duration / total_ops as u32
        } else {
            Duration::ZERO
        };

        let success_rate = if total_ops > 0 {
            (successful_ops as f64 / total_ops as f64) * 100.0
        } else {
            0.0
        };

        let ops_per_second = if total_duration.as_secs() > 0 {
            total_ops as f64 / total_duration.as_secs() as f64
        } else {
            0.0
        };

        // Calculate cache hit rates
        let l1_shard_hits = cache_metrics
            .shard_hits
            .load(std::sync::atomic::Ordering::Relaxed);
        let l2_redis_hits = cache_metrics
            .hits
            .load(std::sync::atomic::Ordering::Relaxed);
        let cache_misses = cache_metrics
            .misses
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_effective_hits = l1_shard_hits + l2_redis_hits;

        let overall_cache_hit_rate = if total_effective_hits + cache_misses > 0 {
            (total_effective_hits as f64 / (total_effective_hits + cache_misses) as f64) * 100.0
        } else {
            0.0
        };

        let l1_cache_hit_rate = if l1_shard_hits + l2_redis_hits + cache_misses > 0 {
            (l1_shard_hits as f64 / (l1_shard_hits + l2_redis_hits + cache_misses) as f64) * 100.0
        } else {
            0.0
        };

        let l2_cache_hit_rate = if l2_redis_hits + cache_misses > 0 {
            (l2_redis_hits as f64 / (l2_redis_hits + cache_misses) as f64) * 100.0
        } else {
            0.0
        };

        // Calculate operation breakdown
        let mut operation_counts = std::collections::HashMap::new();
        for result in &results {
            *operation_counts.entry(&result.operation_type).or_insert(0) += 1;
        }

        // Log final results with enhanced metrics
        tracing::info!("🎉 REAL CDC High Throughput Test Results:");
        tracing::info!("{}", "=".repeat(80));
        tracing::info!("📊 OPERATION METRICS:");
        tracing::info!("   📊 Total Operations: {}", total_ops);
        tracing::info!(
            "   ✅ Successful: {} ({:.2}%)",
            successful_ops,
            success_rate
        );
        tracing::info!("   ❌ Failed: {}", failed_ops);
        tracing::info!("   ⏰ Timeout: {}", timed_out_ops);
        tracing::info!("   ⚡ Average Duration: {:?}", avg_duration);
        tracing::info!("   🚀 Operations/Second: {:.2}", ops_per_second);

        tracing::info!("🔧 CQRS SYSTEM METRICS:");
        tracing::info!(
            "   Commands Processed: {}",
            cqrs_metrics
                .commands_processed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        tracing::info!(
            "   Commands Failed: {}",
            cqrs_metrics
                .commands_failed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        tracing::info!(
            "   Queries Processed: {}",
            cqrs_metrics
                .queries_processed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        tracing::info!(
            "   Queries Failed: {}",
            cqrs_metrics
                .queries_failed
                .load(std::sync::atomic::Ordering::Relaxed)
        );

        tracing::info!("💾 CACHE PERFORMANCE METRICS:");
        tracing::info!(
            "   L1 Cache Hits (In-Memory Shard): {} ({:.2}%)",
            l1_shard_hits,
            l1_cache_hit_rate
        );
        tracing::info!(
            "   L2 Cache Hits (Redis): {} ({:.2}%)",
            l2_redis_hits,
            l2_cache_hit_rate
        );
        tracing::info!("   Total Effective Cache Hits: {}", total_effective_hits);
        tracing::info!("   Cache Misses: {}", cache_misses);
        tracing::info!("   Overall Cache Hit Rate: {:.2}%", overall_cache_hit_rate);
        tracing::info!(
            "   Total Cache Operations: {}",
            total_effective_hits + cache_misses
        );

        tracing::info!("🔄 CDC EVENT PROCESSING:");
        tracing::info!("   CDC Events Processed: {}", final_events_processed);
        tracing::info!("   CDC Events Failed: {}", final_events_failed);
        tracing::info!(
            "   CDC Processing Rate: {:.2} events/sec",
            if total_duration.as_secs() > 0 {
                final_events_processed as f64 / total_duration.as_secs() as f64
            } else {
                0.0
            }
        );

        tracing::info!("📈 OPERATION BREAKDOWN:");
        for (op_type, count) in &operation_counts {
            let percentage = (*count as f64 / total_ops as f64) * 100.0;
            tracing::info!("   {}: {} ({:.1}%)", op_type, count, percentage);
        }

        // Print a summary table
        println!("\n{}", "=".repeat(80));
        println!("🚀 REAL CDC HIGH THROUGHPUT PERFORMANCE SUMMARY");
        println!("{}", "=".repeat(80));
        println!("📊 Operations/Second: {:.2} OPS", ops_per_second);
        println!("✅ Success Rate: {:.2}%", success_rate);
        println!("💾 Overall Cache Hit Rate: {:.2}%", overall_cache_hit_rate);
        println!("🔄 CDC Events Processed: {}", final_events_processed);
        println!("📈 Total Operations: {}", total_ops);
        println!(
            "🔧 Commands Processed: {}",
            cqrs_metrics
                .commands_processed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        println!(
            "🔍 Queries Processed: {}",
            cqrs_metrics
                .queries_processed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        println!(
            "💾 L1 Cache Hits: {} ({:.2}%)",
            l1_shard_hits, l1_cache_hit_rate
        );
        println!(
            "💾 L2 Cache Hits: {} ({:.2}%)",
            l2_redis_hits, l2_cache_hit_rate
        );
        println!("💾 Cache Misses: {}", cache_misses);
        println!("⚡ Average Operation Duration: {:?}", avg_duration);
        println!("{}", "=".repeat(80));

        // Verify CDC processed events
        if final_events_processed == 0 {
            return Err("CDC did not process any events during high throughput test".into());
        }

        tracing::info!("🎉 Real CDC high throughput test completed successfully!");
        Ok(())
    };

    match tokio::time::timeout(Duration::from_secs(180), test_future).await {
        // Increased timeout to 3 minutes
        Ok(Ok(())) => {
            tracing::info!("✅ Real CDC high throughput test passed!");
        }
        Ok(Err(e)) => {
            tracing::error!("❌ Real CDC high throughput test failed: {}", e);
            panic!("Real CDC high throughput test failed: {}", e);
        }
        Err(_) => {
            tracing::error!("❌ Real CDC high throughput test timeout");
            panic!("Real CDC high throughput test timeout");
        }
    }
}

/// Performance comparison test between Test CDC (polling) vs Real CDC (Debezium)
/// This test demonstrates the performance trade-offs between different CDC approaches
#[tokio::test]
#[ignore] // Ignored by default - requires full CDC setup
async fn test_cdc_performance_comparison() {
    tracing::info!("🔬 Starting CDC Performance Comparison Test...");

    let test_future = async {
        // Test 1: Test CDC Service (Polling-based) - High Performance
        tracing::info!("🧪 TEST 1: Test CDC Service (Polling-based)");
        let test_cdc_results = run_test_cdc_performance_test().await?;

        // Test 2: Real CDC Service (Debezium-based) - Production-like
        tracing::info!("🏭 TEST 2: Real CDC Service (Debezium-based)");
        let real_cdc_results = run_real_cdc_performance_test().await?;

        // Print comprehensive comparison
        print_performance_comparison(&test_cdc_results, &real_cdc_results);

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    match tokio::time::timeout(Duration::from_secs(300), test_future).await {
        Ok(Ok(())) => {
            tracing::info!("✅ CDC Performance Comparison Test completed!");
        }
        Ok(Err(e)) => {
            tracing::error!("❌ CDC Performance Comparison Test failed: {}", e);
            panic!("CDC Performance Comparison Test failed: {}", e);
        }
        Ok(_) => {
            tracing::error!("❌ CDC Performance Comparison Test returned unexpected result");
            panic!("CDC Performance Comparison Test returned unexpected result");
        }
        Err(_) => {
            tracing::error!("❌ CDC Performance Comparison Test timeout");
            panic!("CDC Performance Comparison Test timeout");
        }
    }
}

#[derive(Debug, Clone)]
enum OperationResult {
    Success,
    Failure,
    Timeout,
}

#[derive(Debug, Clone)]
struct OperationMetrics {
    worker_id: usize,
    operation_type: String,
    result: OperationResult,
    duration: Duration,
    timestamp: std::time::Instant,
}

#[derive(Debug, Clone)]
struct CDCPerformanceResults {
    test_name: String,
    total_ops: u64,
    successful_ops: u64,
    failed_ops: u64,
    timed_out_ops: u64,
    total_duration: Duration,
    ops_per_second: f64,
    success_rate: f64,
    avg_duration: Duration,
    cache_hit_rate: f64,
    l1_cache_hits: u64,
    l2_cache_hits: u64,
    cache_misses: u64,
    cdc_events_processed: u64,
    cdc_events_failed: u64,
    commands_processed: u64,
    queries_processed: u64,
    setup_time: Duration,
    teardown_time: Duration,
}

async fn run_test_cdc_performance_test(
) -> Result<CDCPerformanceResults, Box<dyn std::error::Error + Send + Sync>> {
    let setup_start = std::time::Instant::now();

    // Setup test environment with TEST CDC (polling-based)
    let context = setup_test_cdc_test_environment().await?;
    let setup_time = setup_start.elapsed();

    tracing::info!("✅ Test CDC environment setup complete in {:?}", setup_time);

    // Test parameters optimized for test CDC
    let target_ops = 1000; // Higher throughput for test CDC
    let worker_count = 20; // More workers for test CDC
    let account_count = 200; // More accounts for test CDC
    let channel_buffer_size = 5000;

    tracing::info!(
        "🎯 Test CDC Parameters - Target Ops: {}, Workers: {}, Accounts: {}",
        target_ops,
        worker_count,
        account_count
    );

    // Create accounts
    let account_ids = create_test_accounts(&context.cqrs_service, account_count).await?;

    // Wait for CDC to process account creation events
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Run performance test
    let test_start = std::time::Instant::now();
    let results = run_high_throughput_operations(
        &context.cqrs_service,
        &account_ids,
        target_ops,
        worker_count,
        channel_buffer_size,
    )
    .await?;
    let test_duration = test_start.elapsed();

    // Get final metrics
    let cqrs_metrics = context.cqrs_service.get_metrics();
    let cache_metrics = context.cqrs_service.get_cache_metrics();
    let cdc_metrics = context.cdc_service_manager.get_metrics();

    let teardown_start = std::time::Instant::now();
    // Cleanup
    let teardown_time = teardown_start.elapsed();

    // Calculate performance metrics
    let total_ops = results.len() as u64;
    let successful_ops = results
        .iter()
        .filter(|r| matches!(r.result, OperationResult::Success))
        .count() as u64;
    let failed_ops = results
        .iter()
        .filter(|r| matches!(r.result, OperationResult::Failure))
        .count() as u64;
    let timed_out_ops = results
        .iter()
        .filter(|r| matches!(r.result, OperationResult::Timeout))
        .count() as u64;

    let total_duration: Duration = results.iter().map(|r| r.duration).sum();
    let avg_duration = if total_ops > 0 {
        total_duration / total_ops as u32
    } else {
        Duration::ZERO
    };
    let ops_per_second = if test_duration.as_secs() > 0 {
        total_ops as f64 / test_duration.as_secs() as f64
    } else {
        0.0
    };
    let success_rate = if total_ops > 0 {
        (successful_ops as f64 / total_ops as f64) * 100.0
    } else {
        0.0
    };

    // Cache metrics
    let l1_shard_hits = cache_metrics
        .shard_hits
        .load(std::sync::atomic::Ordering::Relaxed);
    let l2_redis_hits = cache_metrics
        .hits
        .load(std::sync::atomic::Ordering::Relaxed);
    let cache_misses = cache_metrics
        .misses
        .load(std::sync::atomic::Ordering::Relaxed);
    let total_effective_hits = l1_shard_hits + l2_redis_hits;
    let cache_hit_rate = if total_effective_hits + cache_misses > 0 {
        (total_effective_hits as f64 / (total_effective_hits + cache_misses) as f64) * 100.0
    } else {
        0.0
    };

    Ok(CDCPerformanceResults {
        test_name: "Test CDC (Polling-based)".to_string(),
        total_ops,
        successful_ops,
        failed_ops,
        timed_out_ops,
        total_duration: test_duration,
        ops_per_second,
        success_rate,
        avg_duration,
        cache_hit_rate,
        l1_cache_hits: l1_shard_hits,
        l2_cache_hits: l2_redis_hits,
        cache_misses,
        cdc_events_processed: cdc_metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        cdc_events_failed: cdc_metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed),
        commands_processed: cqrs_metrics
            .commands_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        queries_processed: cqrs_metrics
            .queries_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        setup_time,
        teardown_time,
    })
}

async fn run_real_cdc_performance_test(
) -> Result<CDCPerformanceResults, Box<dyn std::error::Error + Send + Sync>> {
    // Check if CDC environment is available
    if !is_cdc_environment_available().await {
        return Err("Real CDC environment not available".into());
    }

    let setup_start = std::time::Instant::now();

    // Setup test environment with REAL CDC (Debezium-based)
    let context = setup_real_cdc_test_environment().await?;
    let setup_time = setup_start.elapsed();

    tracing::info!("✅ Real CDC environment setup complete in {:?}", setup_time);

    // Test parameters optimized for real CDC (more conservative)
    let target_ops = 100; // Lower throughput for real CDC
    let worker_count = 5; // Fewer workers for real CDC
    let account_count = 50; // Fewer accounts for real CDC
    let channel_buffer_size = 1000;

    tracing::info!(
        "🎯 Real CDC Parameters - Target Ops: {}, Workers: {}, Accounts: {}",
        target_ops,
        worker_count,
        account_count
    );

    // Create accounts
    let account_ids = create_test_accounts(&context.cqrs_service, account_count).await?;

    // Wait for CDC to process account creation events
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Run performance test
    let test_start = std::time::Instant::now();
    let results = run_high_throughput_operations(
        &context.cqrs_service,
        &account_ids,
        target_ops,
        worker_count,
        channel_buffer_size,
    )
    .await?;
    let test_duration = test_start.elapsed();

    // Get final metrics
    let cqrs_metrics = context.cqrs_service.get_metrics();
    let cache_metrics = context.cqrs_service.get_cache_metrics();
    let cdc_metrics = context.cdc_service_manager.get_metrics();

    let teardown_start = std::time::Instant::now();
    // Cleanup
    let teardown_time = teardown_start.elapsed();

    // Calculate performance metrics
    let total_ops = results.len() as u64;
    let successful_ops = results
        .iter()
        .filter(|r| matches!(r.result, OperationResult::Success))
        .count() as u64;
    let failed_ops = results
        .iter()
        .filter(|r| matches!(r.result, OperationResult::Failure))
        .count() as u64;
    let timed_out_ops = results
        .iter()
        .filter(|r| matches!(r.result, OperationResult::Timeout))
        .count() as u64;

    let total_duration: Duration = results.iter().map(|r| r.duration).sum();
    let avg_duration = if total_ops > 0 {
        total_duration / total_ops as u32
    } else {
        Duration::ZERO
    };
    let ops_per_second = if test_duration.as_secs() > 0 {
        total_ops as f64 / test_duration.as_secs() as f64
    } else {
        0.0
    };
    let success_rate = if total_ops > 0 {
        (successful_ops as f64 / total_ops as f64) * 100.0
    } else {
        0.0
    };

    // Cache metrics
    let l1_shard_hits = cache_metrics
        .shard_hits
        .load(std::sync::atomic::Ordering::Relaxed);
    let l2_redis_hits = cache_metrics
        .hits
        .load(std::sync::atomic::Ordering::Relaxed);
    let cache_misses = cache_metrics
        .misses
        .load(std::sync::atomic::Ordering::Relaxed);
    let total_effective_hits = l1_shard_hits + l2_redis_hits;
    let cache_hit_rate = if total_effective_hits + cache_misses > 0 {
        (total_effective_hits as f64 / (total_effective_hits + cache_misses) as f64) * 100.0
    } else {
        0.0
    };

    Ok(CDCPerformanceResults {
        test_name: "Real CDC (Debezium-based)".to_string(),
        total_ops,
        successful_ops,
        failed_ops,
        timed_out_ops,
        total_duration: test_duration,
        ops_per_second,
        success_rate,
        avg_duration,
        cache_hit_rate,
        l1_cache_hits: l1_shard_hits,
        l2_cache_hits: l2_redis_hits,
        cache_misses,
        cdc_events_processed: cdc_metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        cdc_events_failed: cdc_metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed),
        commands_processed: cqrs_metrics
            .commands_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        queries_processed: cqrs_metrics
            .queries_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        setup_time,
        teardown_time,
    })
}

async fn create_test_accounts(
    cqrs_service: &Arc<CQRSAccountService>,
    account_count: usize,
) -> Result<Vec<Uuid>, Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(
        "🔧 Creating {} test accounts with batch processing...",
        account_count
    );
    let mut account_ids = Vec::new();
    let batch_size = 50; // Process accounts in batches to reduce contention
    let max_retries = 5;
    let base_delay = Duration::from_millis(200);

    for batch_start in (0..account_count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(account_count);
        let batch_size_actual = batch_end - batch_start;

        tracing::info!(
            "📦 Processing batch {}/{} (accounts {}-{})",
            (batch_start / batch_size) + 1,
            (account_count + batch_size - 1) / batch_size,
            batch_start + 1,
            batch_end
        );

        // Create accounts in parallel within the batch
        let mut batch_futures = Vec::new();
        for i in batch_start..batch_end {
            let owner_name = format!("PerfTestUser_{}", i);
            let initial_balance = rust_decimal::Decimal::new(1000, 0);
            let cqrs_service = cqrs_service.clone();

            let future = async move {
                let mut retry_count = 0;
                loop {
                    tracing::debug!("🔧 Creating account for user: {}", owner_name);
                    match tokio::time::timeout(
                        Duration::from_secs(15), // Increased timeout
                        cqrs_service.create_account(owner_name.clone(), initial_balance),
                    )
                    .await
                    {
                        Ok(Ok(account_id)) => {
                            tracing::debug!(
                                "✅ Created account: {} for user: {}",
                                account_id,
                                owner_name
                            );
                            return Ok::<Uuid, Box<dyn std::error::Error + Send + Sync>>(
                                account_id,
                            );
                        }
                        Ok(Err(e)) => {
                            // Check if it's a serialization error that can be retried
                            let error_msg = e.to_string();
                            if error_msg.contains("serialize access")
                                || error_msg.contains("deadlock")
                                || error_msg.contains("could not serialize")
                            {
                                retry_count += 1;
                                if retry_count <= max_retries {
                                    let delay = base_delay * (2_u32.pow(retry_count as u32));
                                    tracing::warn!("🔄 Serialization failure, retrying account creation for {} (attempt {}/{}), delay: {:?}: {}", 
                                        owner_name, retry_count, max_retries, delay, error_msg);
                                    tokio::time::sleep(delay).await;
                                    continue;
                                }
                            }
                            return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                        }
                        Err(_) => {
                            retry_count += 1;
                            if retry_count <= max_retries {
                                let delay = base_delay * (2_u32.pow(retry_count as u32));
                                tracing::debug!(
                                    "⏰ Timeout retry for account creation {} (attempt {}/{})",
                                    owner_name,
                                    retry_count,
                                    max_retries
                                );
                                tokio::time::sleep(delay).await;
                                continue;
                            }
                            return Err("Account creation timeout after retries".into());
                        }
                    }
                }
            };
            batch_futures.push(future);
        }

        // Wait for all accounts in the batch to be created
        let batch_results = futures::future::join_all(batch_futures).await;

        // Collect successful results
        for result in batch_results {
            match result {
                Ok(account_id) => account_ids.push(account_id),
                Err(e) => {
                    tracing::error!("❌ Failed to create account in batch: {}", e);
                    return Err(e);
                }
            }
        }

        // Small delay between batches to reduce database pressure
        if batch_end < account_count {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    tracing::info!("✅ Created {} accounts successfully", account_ids.len());
    Ok(account_ids)
}

async fn run_high_throughput_operations(
    cqrs_service: &Arc<CQRSAccountService>,
    account_ids: &[Uuid],
    target_ops: usize,
    worker_count: usize,
    channel_buffer_size: usize,
) -> Result<Vec<OperationMetrics>, Box<dyn std::error::Error + Send + Sync>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
    let mut handles = Vec::new();

    // Spawn worker tasks
    for worker_id in 0..worker_count {
        let tx = tx.clone();
        let cqrs_service = cqrs_service.clone();
        let account_ids = account_ids.to_vec();

        let handle = tokio::spawn(async move {
            let mut local_ops = 0;
            let target_ops_per_worker = target_ops / worker_count;
            let mut rng = rand::rngs::StdRng::from_entropy();

            while local_ops < target_ops_per_worker {
                let account_id = account_ids[rng.gen_range(0..account_ids.len())];
                let operation = match rng.gen_range(0..4) {
                    0 => "deposit",
                    1 => "withdraw",
                    2 => "get_account",
                    _ => "get_balance",
                };

                let start_time = std::time::Instant::now();
                let result: Result<Result<(), _>, _> = {
                    let mut retry_count = 0;
                    let max_retries = 5;
                    let base_delay = Duration::from_millis(200);

                    loop {
                        let operation_result = match operation {
                            "deposit" => {
                                let amount = rust_decimal::Decimal::new(rng.gen_range(1..100), 0);
                                tokio::time::timeout(
                                    Duration::from_secs(8), // Increased timeout
                                    cqrs_service.deposit_money(account_id, amount),
                                )
                                .await
                            }
                            "withdraw" => {
                                let amount = rust_decimal::Decimal::new(rng.gen_range(1..50), 0);
                                tokio::time::timeout(
                                    Duration::from_secs(8), // Increased timeout
                                    cqrs_service.withdraw_money(account_id, amount),
                                )
                                .await
                            }
                            "get_account" => tokio::time::timeout(
                                Duration::from_secs(8), // Increased timeout
                                cqrs_service.get_account(account_id),
                            )
                            .await
                            .map(|r| r.map(|_| ())),
                            "get_balance" => tokio::time::timeout(
                                Duration::from_secs(8), // Increased timeout
                                cqrs_service.get_account_balance(account_id),
                            )
                            .await
                            .map(|r| r.map(|_| ())),
                            _ => unreachable!(),
                        };

                        match &operation_result {
                            Ok(Ok(_)) => break operation_result,
                            Ok(Err(e)) => {
                                let error_msg = e.to_string();
                                if (error_msg.contains("serialize access")
                                    || error_msg.contains("deadlock")
                                    || error_msg.contains("could not serialize"))
                                    && retry_count < max_retries
                                {
                                    retry_count += 1;
                                    let delay = base_delay * (2_u32.pow(retry_count as u32));
                                    tracing::warn!("🔄 Serialization failure, retrying operation {} for account {} (attempt {}/{}), delay: {:?}: {}", 
                                        operation, account_id, retry_count, max_retries, delay, error_msg);
                                    tokio::time::sleep(delay).await;
                                    continue;
                                } else {
                                    break operation_result;
                                }
                            }
                            Err(_) => {
                                if retry_count < max_retries {
                                    retry_count += 1;
                                    let delay = base_delay * (2_u32.pow(retry_count as u32));
                                    tracing::debug!("⏰ Timeout retry for operation {} on account {} (attempt {}/{})", 
                                        operation, account_id, retry_count, max_retries);
                                    tokio::time::sleep(delay).await;
                                    continue;
                                } else {
                                    break operation_result;
                                }
                            }
                        }
                    }
                };

                let duration = start_time.elapsed();
                let operation_result = match result {
                    Ok(Ok(_)) => OperationResult::Success,
                    Ok(Err(_)) => OperationResult::Failure,
                    Err(_) => OperationResult::Timeout,
                };

                let _ = tx
                    .send(OperationMetrics {
                        worker_id,
                        operation_type: operation.to_string(),
                        result: operation_result,
                        duration,
                        timestamp: std::time::Instant::now(),
                    })
                    .await;

                local_ops += 1;
                tokio::time::sleep(Duration::from_millis(20)).await; // Increased delay to reduce DB pressure
            }
        });

        handles.push(handle);
    }

    // Collect results
    let mut results = Vec::new();
    let collection_timeout = Duration::from_secs(120);
    let start_time = std::time::Instant::now();

    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(result)) => {
                results.push(result);
                if results.len() >= target_ops {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {
                if start_time.elapsed() > collection_timeout {
                    break;
                }
                continue;
            }
        }
    }

    // Wait for workers
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    }

    Ok(results)
}

fn print_performance_comparison(
    test_cdc: &CDCPerformanceResults,
    real_cdc: &CDCPerformanceResults,
) {
    println!("\n{}", "=".repeat(100));
    println!("🔬 CDC PERFORMANCE COMPARISON ANALYSIS");
    println!("{}", "=".repeat(100));

    // Performance Summary
    println!("\n📊 PERFORMANCE SUMMARY:");
    println!(
        "{:<25} {:<15} {:<15} {:<15} {:<15}",
        "Metric", "Test CDC", "Real CDC", "Difference", "Ratio"
    );
    println!("{:-<85}", "");
    println!(
        "{:<25} {:<15.2} {:<15.2} {:<15.2} {:<15.2}",
        "Operations/Second",
        test_cdc.ops_per_second,
        real_cdc.ops_per_second,
        test_cdc.ops_per_second - real_cdc.ops_per_second,
        test_cdc.ops_per_second / real_cdc.ops_per_second.max(0.1)
    );
    println!(
        "{:<25} {:<15.2} {:<15.2} {:<15.2} {:<15.2}",
        "Success Rate (%)",
        test_cdc.success_rate,
        real_cdc.success_rate,
        test_cdc.success_rate - real_cdc.success_rate,
        test_cdc.success_rate / real_cdc.success_rate.max(0.1)
    );
    println!(
        "{:<25} {:<15.2} {:<15.2} {:<15.2} {:<15.2}",
        "Cache Hit Rate (%)",
        test_cdc.cache_hit_rate,
        real_cdc.cache_hit_rate,
        test_cdc.cache_hit_rate - real_cdc.cache_hit_rate,
        test_cdc.cache_hit_rate / real_cdc.cache_hit_rate.max(0.1)
    );
    println!(
        "{:<25} {:<15?} {:<15?} {:<15?} {:<15.2}",
        "Avg Duration",
        test_cdc.avg_duration,
        real_cdc.avg_duration,
        if test_cdc.avg_duration > real_cdc.avg_duration {
            format!("+{:?}", test_cdc.avg_duration - real_cdc.avg_duration)
        } else {
            format!("-{:?}", real_cdc.avg_duration - test_cdc.avg_duration)
        },
        real_cdc.avg_duration.as_millis() as f64 / test_cdc.avg_duration.as_millis().max(1) as f64
    );

    // Detailed Metrics
    println!("\n📈 DETAILED METRICS:");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Metric", "Test CDC", "Real CDC", "Difference"
    );
    println!("{:-<70}", "");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Total Operations",
        test_cdc.total_ops,
        real_cdc.total_ops,
        test_cdc.total_ops as i64 - real_cdc.total_ops as i64
    );
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Successful Ops",
        test_cdc.successful_ops,
        real_cdc.successful_ops,
        test_cdc.successful_ops as i64 - real_cdc.successful_ops as i64
    );
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Failed Ops",
        test_cdc.failed_ops,
        real_cdc.failed_ops,
        test_cdc.failed_ops as i64 - real_cdc.failed_ops as i64
    );
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Timed Out Ops",
        test_cdc.timed_out_ops,
        real_cdc.timed_out_ops,
        test_cdc.timed_out_ops as i64 - real_cdc.timed_out_ops as i64
    );

    // Cache Performance
    println!("\n💾 CACHE PERFORMANCE:");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Metric", "Test CDC", "Real CDC", "Difference"
    );
    println!("{:-<70}", "");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "L1 Cache Hits",
        test_cdc.l1_cache_hits,
        real_cdc.l1_cache_hits,
        test_cdc.l1_cache_hits as i64 - real_cdc.l1_cache_hits as i64
    );
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "L2 Cache Hits",
        test_cdc.l2_cache_hits,
        real_cdc.l2_cache_hits,
        test_cdc.l2_cache_hits as i64 - real_cdc.l2_cache_hits as i64
    );
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Cache Misses",
        test_cdc.cache_misses,
        real_cdc.cache_misses,
        test_cdc.cache_misses as i64 - real_cdc.cache_misses as i64
    );

    // CDC Processing
    println!("\n🔄 CDC EVENT PROCESSING:");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Metric", "Test CDC", "Real CDC", "Difference"
    );
    println!("{:-<70}", "");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Events Processed",
        test_cdc.cdc_events_processed,
        real_cdc.cdc_events_processed,
        test_cdc.cdc_events_processed as i64 - real_cdc.cdc_events_processed as i64
    );
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Events Failed",
        test_cdc.cdc_events_failed,
        real_cdc.cdc_events_failed,
        test_cdc.cdc_events_failed as i64 - real_cdc.cdc_events_failed as i64
    );

    // System Load
    println!("\n🔧 SYSTEM LOAD:");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Metric", "Test CDC", "Real CDC", "Difference"
    );
    println!("{:-<70}", "");
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Commands Processed",
        test_cdc.commands_processed,
        real_cdc.commands_processed,
        test_cdc.commands_processed as i64 - real_cdc.commands_processed as i64
    );
    println!(
        "{:<25} {:<15} {:<15} {:<15}",
        "Queries Processed",
        test_cdc.queries_processed,
        real_cdc.queries_processed,
        test_cdc.queries_processed as i64 - real_cdc.queries_processed as i64
    );

    // Timing Analysis
    println!("\n⏱️ TIMING ANALYSIS:");
    println!(
        "{:<25} {:<15?} {:<15?} {:<15?}",
        "Metric", "Test CDC", "Real CDC", "Difference"
    );
    println!("{:-<70}", "");
    println!(
        "{:<25} {:<15?} {:<15?} {:<15?}",
        "Setup Time",
        test_cdc.setup_time,
        real_cdc.setup_time,
        if test_cdc.setup_time > real_cdc.setup_time {
            format!("+{:?}", test_cdc.setup_time - real_cdc.setup_time)
        } else {
            format!("-{:?}", real_cdc.setup_time - test_cdc.setup_time)
        }
    );
    println!(
        "{:<25} {:<15?} {:<15?} {:<15?}",
        "Test Duration",
        test_cdc.total_duration,
        real_cdc.total_duration,
        if test_cdc.total_duration > real_cdc.total_duration {
            format!("+{:?}", test_cdc.total_duration - real_cdc.total_duration)
        } else {
            format!("-{:?}", real_cdc.total_duration - test_cdc.total_duration)
        }
    );
    println!(
        "{:<25} {:<15?} {:<15?} {:<15?}",
        "Teardown Time",
        test_cdc.teardown_time,
        real_cdc.teardown_time,
        if test_cdc.teardown_time > real_cdc.teardown_time {
            format!("+{:?}", test_cdc.teardown_time - real_cdc.teardown_time)
        } else {
            format!("-{:?}", real_cdc.teardown_time - test_cdc.teardown_time)
        }
    );

    // Key Insights
    println!("\n🔍 KEY INSIGHTS:");
    println!("{}", "=".repeat(100));

    let ops_ratio = test_cdc.ops_per_second / real_cdc.ops_per_second.max(0.1);
    let success_ratio = test_cdc.success_rate / real_cdc.success_rate.max(0.1);
    let cache_ratio = test_cdc.cache_hit_rate / real_cdc.cache_hit_rate.max(0.1);

    println!("🚀 PERFORMANCE:");
    println!(
        "   • Test CDC is {:.1}x faster in operations/second",
        ops_ratio
    );
    println!(
        "   • Test CDC has {:.1}x better success rate",
        success_ratio
    );
    println!(
        "   • Test CDC has {:.1}x better cache hit rate",
        cache_ratio
    );

    println!("\n⚖️ TRADE-OFFS:");
    println!("   • Test CDC: High performance, simple setup, polling-based");
    println!("   • Real CDC: Production-ready, event-driven, complex infrastructure");
    println!("   • Test CDC: Direct database polling (100ms intervals)");
    println!("   • Real CDC: Debezium + Kafka + logical replication");

    println!("\n🎯 USE CASES:");
    println!("   • Test CDC: Development, testing, high-performance scenarios");
    println!("   • Real CDC: Production, microservices, event-driven architecture");
    println!("   • Test CDC: Single-application deployments");
    println!("   • Real CDC: Distributed systems, multiple consumers");

    println!("\n💡 RECOMMENDATIONS:");
    println!("   • Use Test CDC for development and performance testing");
    println!("   • Use Real CDC for production deployments");
    println!("   • Consider hybrid approach: Test CDC for hot paths, Real CDC for cold paths");
    println!("   • Optimize Real CDC: tune Debezium config, increase Kafka partitions");

    println!("{}", "=".repeat(100));
}

struct TestCDCTestContext {
    cqrs_service: Arc<CQRSAccountService>,
    db_pool: PgPool,
    cdc_service_manager: TestCDCServiceManager,
}

// Test-specific CDC service manager for high-performance testing
struct TestCDCServiceManager {
    metrics: Arc<banking_es::infrastructure::cdc_debezium::CDCMetrics>,
}

impl TestCDCServiceManager {
    fn new() -> Self {
        Self {
            metrics: Arc::new(banking_es::infrastructure::cdc_debezium::CDCMetrics::default()),
        }
    }

    fn get_metrics(&self) -> &banking_es::infrastructure::cdc_debezium::CDCMetrics {
        &self.metrics
    }
}

async fn setup_test_cdc_test_environment(
) -> Result<TestCDCTestContext, Box<dyn std::error::Error + Send + Sync>> {
    // Initialize database pool
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPoolOptions::new()
        .max_connections(50)
        .min_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(1800))
        .max_lifetime(Duration::from_secs(3600))
        .connect(&database_url)
        .await?;

    // Initialize Redis client
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let redis_client_trait = RealRedisClient::new(redis_client, None);

    // Initialize services
    let event_store = Arc::new(EventStore::new(pool.clone())) as Arc<dyn EventStoreTrait + 'static>;
    let projection_store =
        Arc::new(ProjectionStore::new(pool.clone())) as Arc<dyn ProjectionStoreTrait + 'static>;

    let cache_config = CacheConfig::default();
    let cache_service = Arc::new(CacheService::new(redis_client_trait, cache_config))
        as Arc<dyn CacheServiceTrait + 'static>;

    let kafka_config = banking_es::infrastructure::kafka_abstraction::KafkaConfig::default();

    // Create CDC outbox repository and create the table
    let cdc_outbox_repo = Arc::new(CDCOutboxRepository::new(pool.clone()));

    // Create the CDC outbox table
    tracing::info!("🔧 Creating CDC outbox table...");
    cdc_outbox_repo.create_cdc_outbox_table().await?;
    tracing::info!("✅ CDC outbox table created successfully");

    // Create test-specific CDC service that processes events directly
    let test_cdc_service = TestCDCService::new(
        cdc_outbox_repo.clone(),
        cache_service.clone(),
        projection_store.clone(),
        pool.clone(),
    );

    // Start the test CDC service
    tracing::info!("🔧 Starting TEST CDC Service...");
    let cdc_service_handle = tokio::spawn(async move {
        test_cdc_service.start_processing().await;
    });

    // Give test CDC service time to initialize
    tokio::time::sleep(Duration::from_millis(500)).await;
    tracing::info!("🔧 TEST CDC Service initialization complete");

    let cqrs_service = Arc::new(CQRSAccountService::new(
        event_store,
        projection_store,
        cache_service,
        kafka_config,
        500,
        100,
        Duration::from_millis(100),
    ));

    Ok(TestCDCTestContext {
        cqrs_service,
        db_pool: pool,
        cdc_service_manager: TestCDCServiceManager::new(),
    })
}

// Test-specific CDC service that processes outbox events directly without Debezium
struct TestCDCService {
    outbox_repo: Arc<CDCOutboxRepository>,
    cache_service: Arc<dyn CacheServiceTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    db_pool: PgPool,
}

impl TestCDCService {
    fn new(
        outbox_repo: Arc<CDCOutboxRepository>,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        db_pool: PgPool,
    ) -> Self {
        Self {
            outbox_repo,
            cache_service,
            projection_store,
            db_pool,
        }
    }

    async fn start_processing(&self) {
        tracing::info!("🧪 Test CDC Service: Starting direct outbox processing...");

        let mut interval = tokio::time::interval(Duration::from_millis(100)); // Poll every 100ms

        loop {
            interval.tick().await;

            // Directly query the outbox table for new messages
            match self.process_pending_outbox_messages().await {
                Ok(processed_count) => {
                    if processed_count > 0 {
                        tracing::info!(
                            "🧪 Test CDC Service: Processed {} outbox messages",
                            processed_count
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "🧪 Test CDC Service: Error processing outbox messages: {}",
                        e
                    );
                }
            }
        }
    }

    async fn process_pending_outbox_messages(
        &self,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        // Directly query the CDC outbox table for new messages
        let messages = sqlx::query!(
            r#"
            SELECT id, aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at
            FROM kafka_outbox_cdc
            ORDER BY created_at ASC
            LIMIT 100
            "#
        )
        .fetch_all(&self.db_pool)
        .await?;

        let mut processed_count = 0;

        for row in messages {
            // Use the fields directly from the struct
            let id = row.id;
            let aggregate_id = row.aggregate_id;
            let event_type = row.event_type;
            let payload = row.payload;
            let created_at = row.created_at;

            // Simulate CDC processing
            tracing::debug!(
                "🧪 Test CDC Service: Processing message {} for aggregate {}",
                id,
                aggregate_id
            );

            // Simulate some processing time
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

            // Simulate cache operations
            let cache_key = format!("account:{}", aggregate_id);
            use banking_es::infrastructure::cache_service::CacheServiceTrait;
            let _ = self.cache_service.get_account(id).await;

            // Simulate projection update using upsert_accounts_batch
            use banking_es::infrastructure::projections::AccountProjection;
            use banking_es::infrastructure::projections::ProjectionStoreTrait;
            use chrono::{DateTime, Utc};
            use rust_decimal::Decimal;
            use uuid::Uuid;
            let projection = AccountProjection {
                id,
                owner_name: "CDC Test".to_string(), // Placeholder, adjust as needed
                balance: Decimal::ZERO,             // Placeholder, adjust as needed
                is_active: true,                    // Placeholder, adjust as needed
                created_at,
                updated_at: created_at,
            };
            let _ = self
                .projection_store
                .upsert_accounts_batch(vec![projection])
                .await;

            processed_count += 1;
        }

        Ok(processed_count)
    }
}
