use banking_es::{
    application::services::CQRSAccountService,
    infrastructure::{
        cache_service::{CacheConfig, CacheService},
        connection_pool_partitioning::{PartitionedPools, PoolPartitioningConfig},
        event_store::{EventStore, EventStoreTrait},
        projections::{ProjectionConfig, ProjectionStore},
        read_batching::{PartitionedReadBatching, ReadBatchingConfig, ReadOperation},
        RealRedisClient,
    },
};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Read Batching Performance Test");
    println!("================================");

    // Set the database URL
    std::env::set_var(
        "DATABASE_URL",
        "postgresql://postgres:Francisco1@localhost:5432/banking_es",
    );

    // Initialize services
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to database");

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url).expect("Failed to create Redis client");
    let redis_client_trait = RealRedisClient::new(redis_client, None);

    let event_store = Arc::new(EventStore::new(pool.clone()));
    let cache_config = CacheConfig::default();
    let cache_service = Arc::new(CacheService::new(redis_client_trait, cache_config));

    // Create partitioned pools for projection store
    let pool_config = PoolPartitioningConfig::default();
    let partitioned_pools = Arc::new(PartitionedPools::new(pool_config).await?);

    // Create projection store with config
    let projection_config = ProjectionConfig::default();
    let projection_store = Arc::new(ProjectionStore::from_pools_with_config(
        partitioned_pools.clone(),
        projection_config,
    ));

    // Create read batching service with ultra-optimized config
    let read_config = ReadBatchingConfig {
        max_batch_size: 20000, // 10000'den 20000'e artırıldı
        max_batch_wait_time_ms: 1,
        num_read_partitions: 32,
        enable_parallel_reads: true,
        cache_ttl_secs: 300,
        max_retries: 0,
        retry_backoff_ms: 0,
    };

    let read_pools = vec![Arc::new(pool.clone()); 32]; // 32 read pools
    let read_batching_service =
        PartitionedReadBatching::new(read_config, projection_store.clone(), read_pools).await?;

    println!("✅ Services initialized with read batching");

    // Create test accounts first
    println!("📝 Creating test accounts...");
    let mut account_ids = Vec::new();
    for i in 0..1000 {
        let account_id = Uuid::new_v4();
        account_ids.push(account_id);
    }

    println!("📊 Starting read batching performance test...");
    println!("   • Target: 10,000 ops/sec");
    println!("   • Test accounts: {}", account_ids.len());
    println!("   • Read partitions: 32");
    println!("   • Batch size: 20,000");

    let test_start = Instant::now();
    let target_operations = 640000; // 10000'den 640000'e artırıldı
    let mut successful_operations = 0;
    let mut failed_operations = 0;
    let mut latencies = Vec::with_capacity(target_operations);

    // Create batch operations for read batching
    let mut batch_operations = Vec::new();
    for i in 0..target_operations {
        let account_id = account_ids[i % account_ids.len()];
        batch_operations.push(ReadOperation::GetAccount { account_id });
    }

    println!(
        "📦 Submitting {} operations to read batching service...",
        batch_operations.len()
    );
    let batch_submit_start = Instant::now();

    // Submit all operations in a single batch
    let operation_ids = match read_batching_service
        .submit_read_operations_batch_optimized(batch_operations)
        .await
    {
        Ok(ids) => {
            let submit_duration = batch_submit_start.elapsed();
            println!(
                "✅ Batch submitted {} operations in {:?}",
                ids.len(),
                submit_duration
            );
            ids
        }
        Err(e) => {
            println!("❌ Batch submission failed: {}", e);
            return Err(e.into());
        }
    };

    println!("⏳ Waiting for all batch operation results...");
    let wait_start = Instant::now();

    // Wait for all results in parallel instead of one by one
    // let mut wait_futures = Vec::new();
    // for operation_id in &operation_ids {
    //     let read_batching_service = read_batching_service.clone();
    //     let future = async move {
    //         let start = Instant::now();
    //         let result = read_batching_service.wait_for_result(*operation_id).await;
    //         (result.is_ok(), start.elapsed())
    //     };
    //     wait_futures.push(future);
    // }

    // Wait for all results in parallel
    // let results = futures::future::join_all(wait_futures).await;

    // Wait for all results with read batching
    for (i, operation_id) in operation_ids.iter().enumerate() {
        let operation_start = Instant::now();

        match read_batching_service.wait_for_result(*operation_id).await {
            Ok(_) => {
                successful_operations += 1;
                latencies.push(operation_start.elapsed());
            }
            Err(_) => {
                failed_operations += 1;
                latencies.push(operation_start.elapsed());
            }
        }
        //}

        // Process results
        //  for (i, (success, latency)) in results.into_iter().enumerate() {
        //      if success {
        //          successful_operations += 1;
        //      } else {
        //          failed_operations += 1;
        //      }
        //      latencies.push(latency);

        if (i + 1) % 10000 == 0 {
            println!(
                "  ✅ Completed {}/{} operations",
                i + 1,
                operation_ids.len()
            );
        }
    }

    let total_duration = test_start.elapsed();
    let wait_duration = wait_start.elapsed();
    let batch_submit_duration = batch_submit_start.elapsed();

    // Calculate statistics
    latencies.sort();
    let avg_latency = if !latencies.is_empty() {
        latencies.iter().sum::<Duration>() / latencies.len() as u32
    } else {
        Duration::ZERO
    };

    let p95_index = (latencies.len() as f64 * 0.95) as usize;
    let p99_index = (latencies.len() as f64 * 0.99) as usize;
    let p95_latency = latencies.get(p95_index).copied().unwrap_or(Duration::ZERO);
    let p99_latency = latencies.get(p99_index).copied().unwrap_or(Duration::ZERO);
    let min_latency = latencies.first().copied().unwrap_or(Duration::ZERO);
    let max_latency = latencies.last().copied().unwrap_or(Duration::ZERO);

    let ops_per_sec = if total_duration.as_millis() > 0 {
        (successful_operations as f64 / total_duration.as_millis() as f64) * 1000.0
    } else {
        0.0
    };

    let success_rate = if (successful_operations + failed_operations) > 0 {
        (successful_operations as f64 / (successful_operations + failed_operations) as f64) * 100.0
    } else {
        0.0
    };

    // Print comprehensive results
    println!("\n📊 READ BATCHING PERFORMANCE TEST RESULTS");
    println!("=========================================");

    println!("\n📖 READ OPERATIONS (WITH BATCHING):");
    println!(
        "  - Total Read Operations: {}",
        successful_operations + failed_operations
    );
    println!("  - Successful: {}", successful_operations);
    println!("  - Failed: {}", failed_operations);
    println!("  - Success Rate: {:.2}%", success_rate);
    println!("  - Total Duration: {:?}", total_duration);
    println!("  - Batch Submit Duration: {:?}", batch_submit_duration);
    println!("  - Wait Duration: {:?}", wait_duration);
    println!("  - Read Ops/sec: {:.2}", ops_per_sec);
    println!("  - Avg Read Latency: {:?}", avg_latency);
    println!("  - P95 Read Latency: {:?}", p95_latency);
    println!("  - P99 Read Latency: {:?}", p99_latency);
    println!("  - Min Read Latency: {:?}", min_latency);
    println!("  - Max Read Latency: {:?}", max_latency);

    println!("\n🔧 READ BATCHING CONFIGURATION:");
    println!("  - Read Partitions: 32");
    println!("  - Batch Size: 50,000");
    println!("  - Batch Wait Time: 1ms");
    println!("  - Max Retries: 0");
    println!("  - Read Pool Connections: 1,600");
    println!("  - Acquire Timeout: 1s");

    if ops_per_sec >= 10000.0 {
        println!("\n🎉 SUCCESS: Target of 10,000 ops/sec achieved!");
        println!("   • Actual: {:.2} ops/sec", ops_per_sec);
        println!("   • Performance: {:.1}x target", ops_per_sec / 10000.0);
    } else {
        println!("\n⚠️  TARGET NOT MET: Need to optimize further");
        println!("   • Target: 10,000 ops/sec");
        println!("   • Actual: {:.2} ops/sec", ops_per_sec);
        println!("   • Gap: {:.2} ops/sec", 10000.0 - ops_per_sec);
    }

    println!("\n🚀 Performance Analysis:");
    if avg_latency < Duration::from_millis(1) {
        println!("   ✅ Excellent latency: {:?}", avg_latency);
    } else if avg_latency < Duration::from_millis(10) {
        println!("   ⚠️  Good latency: {:?}", avg_latency);
    } else {
        println!("   ❌ High latency: {:?}", avg_latency);
    }

    if success_rate >= 99.0 {
        println!("   ✅ Excellent success rate: {:.2}%", success_rate);
    } else if success_rate >= 95.0 {
        println!("   ⚠️  Good success rate: {:.2}%", success_rate);
    } else {
        println!("   ❌ Low success rate: {:.2}%", success_rate);
    }

    // Compare with previous test
    println!("\n📈 COMPARISON WITH PREVIOUS TEST:");
    println!("   • Previous (Individual Queries): 2000 ops/sec");
    println!("   • Current (Read Batching): {:.2} ops/sec", ops_per_sec);
    if ops_per_sec > 2000.0 {
        println!("   • Improvement: {:.1}x faster", ops_per_sec / 2000.0);
    } else {
        println!("   • Performance: {:.1}x slower", 2000.0 / ops_per_sec);
    }

    Ok(())
}
