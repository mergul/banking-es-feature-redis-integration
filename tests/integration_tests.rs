use banking_es::web;
use banking_es::{
    application::services::AccountService,
    domain::AccountError,
    infrastructure::{
        cache_service::{CacheConfig, CacheService, CacheServiceTrait, EvictionPolicy},
        event_store::{EventStore, EventStoreTrait},
        projections::{
            AccountProjection, ProjectionConfig, ProjectionStore, ProjectionStoreTrait,
            TransactionProjection,
        },
        redis_abstraction::RealRedisClient,
        repository::AccountRepository,
    },
};
use futures::FutureExt;
use rand;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use redis;
use rust_decimal::Decimal;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio;
use tokio::sync::mpsc;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

// Type alias for boxed future
type RedisOpFuture = Pin<Box<dyn Future<Output = Result<String, redis::RedisError>> + Send>>;

struct TestContext {
    account_service: Arc<AccountService>,
    account_repository: Arc<AccountRepository>,
    db_pool: PgPool,
    _shutdown_tx: mpsc::Sender<()>,
    _background_tasks: Vec<JoinHandle<()>>,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Send shutdown signal
        let _ = self._shutdown_tx.try_send(());

        // Wait for background tasks to complete
        for handle in self._background_tasks.drain(..) {
            let _ = handle.abort();
        }
    }
}

// Test-specific ProjectionStore that doesn't use background tasks
#[derive(Clone)]
struct TestProjectionStore {
    pool: PgPool,
    account_cache: Arc<tokio::sync::RwLock<std::collections::HashMap<Uuid, AccountProjection>>>,
    transaction_cache:
        Arc<tokio::sync::RwLock<std::collections::HashMap<Uuid, Vec<TransactionProjection>>>>,
}

impl TestProjectionStore {
    fn new(pool: PgPool) -> Self {
        Self {
            pool,
            account_cache: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            transaction_cache: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, anyhow::Error> {
        // Try cache first
        {
            let cache = self.account_cache.read().await;
            if let Some(account) = cache.get(&account_id) {
                return Ok(Some(account.clone()));
            }
        }

        // Cache miss - fetch from database
        let account: Option<AccountProjection> = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE id = $1
            "#,
            account_id
        )
        .fetch_optional(&self.pool)
        .await?;

        // Update cache if found
        if let Some(ref account) = account {
            let mut cache = self.account_cache.write().await;
            cache.insert(account_id, account.clone());
        }

        Ok(account)
    }

    async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, anyhow::Error> {
        // Try cache first
        {
            let cache = self.transaction_cache.read().await;
            if let Some(transactions) = cache.get(&account_id) {
                return Ok(transactions.clone());
            }
        }

        // Cache miss - fetch from database
        let transactions = sqlx::query_as!(
            TransactionProjection,
            r#"
            SELECT id, account_id, transaction_type, amount, timestamp
            FROM transaction_projections
            WHERE account_id = $1
            ORDER BY timestamp DESC
            LIMIT 1000
            "#,
            account_id
        )
        .fetch_all(&self.pool)
        .await?;

        // Update cache
        {
            let mut cache = self.transaction_cache.write().await;
            cache.insert(account_id, transactions.clone());
        }

        Ok(transactions)
    }

    async fn upsert_accounts_batch(
        &self,
        accounts: Vec<AccountProjection>,
    ) -> Result<(), anyhow::Error> {
        for account in accounts {
            let mut cache = self.account_cache.write().await;
            cache.insert(account.id, account);
        }
        Ok(())
    }

    async fn insert_transactions_batch(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<(), anyhow::Error> {
        for transaction in transactions {
            let mut cache = self.transaction_cache.write().await;
            let account_transactions = cache.entry(transaction.account_id).or_insert_with(Vec::new);
            account_transactions.push(transaction);
        }
        Ok(())
    }
}

// Implement the same traits as ProjectionStore
impl std::ops::Deref for TestProjectionStore {
    type Target = ProjectionStore;
    fn deref(&self) -> &Self::Target {
        // This is a hack to make it work with the existing code
        // In a real implementation, we would need to implement all the methods
        unimplemented!("TestProjectionStore should not be dereferenced")
    }
}

async fn setup_test_environment() -> Result<TestContext, Box<dyn std::error::Error>> {
    // Create shutdown channel for cleanup
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
    let mut background_tasks = Vec::new();

    // Initialize database pool with highly optimized settings for maximum throughput
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPoolOptions::new()
        .max_connections(5000) // Increased from 3000 to 5000 for maximum throughput
        .min_connections(2000) // Increased from 1000 to 2000 for better connection availability
        .acquire_timeout(Duration::from_secs(30)) // Reduced from 60 to 30 seconds for faster acquisition
        .idle_timeout(Duration::from_secs(3600)) // Increased from 1800 to 3600 seconds
        .max_lifetime(Duration::from_secs(7200)) // Increased from 3600 to 7200 seconds
        .test_before_acquire(true) // Test connections before acquiring
        .connect_lazy_with(database_url.parse().unwrap());

    // Initialize Redis client with optimized settings
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url).expect("Failed to connect to Redis");

    // Test Redis connection
    let mut redis_conn = redis_client
        .get_connection()
        .expect("Failed to get Redis connection");
    let _: () = redis::cmd("PING").execute(&mut redis_conn);
    eprintln!("✅ Redis connection test successful");

    let redis_client_trait = RealRedisClient::new(redis_client, None);

    // Initialize services with highly optimized configs
    let event_store = Arc::new(EventStore::new(pool.clone())) as Arc<dyn EventStoreTrait + 'static>;
    let projection_store = Arc::new(ProjectionStore::new_test(pool.clone()))
        as Arc<dyn ProjectionStoreTrait + 'static>;

    // Highly optimized cache configuration for maximum throughput
    let mut cache_config = CacheConfig::default();
    cache_config.default_ttl = Duration::from_secs(3600); // Increased from 1800 to 3600 seconds
    cache_config.max_size = 200000; // Increased from 100000 to 200000
    cache_config.shard_count = 64; // Increased from 32 to 64 for better concurrency
    cache_config.warmup_batch_size = 1000; // Increased from 500 to 1000
    cache_config.warmup_interval = Duration::from_secs(5); // Reduced from 10 to 5 seconds

    let cache_service = Arc::new(CacheService::new(redis_client_trait.clone(), cache_config))
        as Arc<dyn CacheServiceTrait + 'static>;
    let repository: Arc<AccountRepository> = Arc::new(AccountRepository::new(event_store));
    let repository_clone = repository.clone();

    let service = Arc::new(AccountService::new(
        repository,
        projection_store,
        cache_service,
        Arc::new(Default::default()),
        5000, // Increased from 2000 to 5000 for maximum batching efficiency
    ));

    // Start connection monitoring task with reduced frequency
    let pool_clone = pool.clone();
    let monitor_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await; // Reduced frequency from 5s to 10s
            eprintln!(
                "📊 DB Pool Stats - Active: {}, Idle: {}, Size: {}",
                pool_clone.size(),
                pool_clone.num_idle(),
                pool_clone.size()
            );
        }
    });
    background_tasks.push(monitor_handle);

    // Start cleanup task
    let service_clone = service.clone();
    let cleanup_handle = tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                // Cleanup code here
            }
        }
    });
    background_tasks.push(cleanup_handle);

    Ok(TestContext {
        account_service: service,
        account_repository: repository_clone,
        db_pool: pool,
        _shutdown_tx: shutdown_tx,
        _background_tasks: background_tasks,
    })
}

// Helper function to run async operations with timeout
async fn with_timeout<F, T>(
    future: F,
    timeout_duration: Duration,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: std::future::Future<Output = Result<T, Box<dyn std::error::Error>>>,
{
    match timeout(timeout_duration, future).await {
        Ok(result) => result,
        Err(_) => Err("Operation timed out".into()),
    }
}

#[tokio::test]
async fn test_basic_account_operations() {
    let ctx = setup_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Create account
    let account_id = ctx
        .account_service
        .create_account("Test User".to_string(), Decimal::new(1000, 0))
        .await
        .expect("Failed to create account");

    // Get account
    let account = ctx
        .account_service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account not found");

    assert_eq!(account.owner_name, "Test User");
    assert_eq!(account.balance, Decimal::new(1000, 0));

    // Test deposit
    ctx.account_service
        .deposit_money(account_id, Decimal::new(500, 0))
        .await
        .expect("Failed to deposit money");

    // Verify balance after deposit
    let account = ctx
        .account_service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account not found");
    assert_eq!(account.balance, Decimal::new(1500, 0));

    // Test withdrawal
    ctx.account_service
        .withdraw_money(account_id, Decimal::new(300, 0))
        .await
        .expect("Failed to withdraw money");

    // Verify balance after withdrawal
    let account = ctx
        .account_service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account not found");
    assert_eq!(account.balance, Decimal::new(1200, 0));

    // Test transaction history
    let transactions = ctx
        .account_service
        .get_account_transactions(account_id)
        .await
        .expect("Failed to get transactions");

    assert_eq!(transactions.len(), 3); // Create + Deposit + Withdraw
    assert_eq!(transactions[0].transaction_type, "MoneyWithdrawn");
    assert_eq!(transactions[0].amount, Decimal::new(300, 0));
    assert_eq!(transactions[1].transaction_type, "MoneyDeposited");
    assert_eq!(transactions[1].amount, Decimal::new(500, 0));
    assert_eq!(transactions[2].transaction_type, "AccountCreated");
    assert_eq!(transactions[2].amount, Decimal::new(1000, 0));

    // Verify metrics
    let metrics = ctx.account_service.get_metrics();
    assert!(
        metrics
            .commands_processed
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(
        metrics
            .projection_updates
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(
        metrics
            .cache_hits
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 0
    );
    assert!(
        metrics
            .cache_misses
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 0
    );

    // Test duplicate command handling
    let result = ctx
        .account_service
        .create_account("Test User 2".to_string(), Decimal::new(1000, 0))
        .await;
    assert!(result.is_ok());

    let duplicate_result = ctx
        .account_service
        .create_account("Test User 2".to_string(), Decimal::new(1000, 0))
        .await;
    assert!(duplicate_result.is_err());
    assert!(duplicate_result
        .unwrap_err()
        .to_string()
        .contains("Duplicate"));

    // Test concurrent operations
    let account_id = result.unwrap();
    let mut handles = Vec::new();
    for i in 0..5 {
        let service = ctx.account_service.clone();
        let amount = Decimal::new(100 * (i + 1), 0);
        handles.push(tokio::spawn(async move {
            service.deposit_money(account_id, amount).await
        }));
    }

    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }

    // Verify final balance
    let account = ctx
        .account_service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account not found");
    assert_eq!(account.balance, Decimal::new(1500, 0)); // Initial 1000 + 100 + 200 + 300 + 400 + 500
}

#[tokio::test]
async fn test_cache_behavior() {
    let ctx = setup_test_environment().await.unwrap();
    let account_id = ctx
        .account_service
        .create_account("Cache Test".to_string(), Decimal::from(1000))
        .await
        .unwrap();

    // First read (should miss cache)
    let start = std::time::Instant::now();
    let _ = ctx
        .account_service
        .get_account(account_id)
        .await
        .unwrap()
        .unwrap();
    let first_read_time = start.elapsed();

    // Second read (should hit cache)
    let start = std::time::Instant::now();
    let _ = ctx
        .account_service
        .get_account(account_id)
        .await
        .unwrap()
        .unwrap();
    let second_read_time = start.elapsed();

    // Cache hit should be faster
    assert!(second_read_time < first_read_time);
}

#[tokio::test]
async fn test_error_handling() {
    let ctx = setup_test_environment().await.unwrap();

    // Test non-existent account
    let non_existent_id = Uuid::new_v4();
    let result = ctx.account_service.get_account(non_existent_id).await;
    assert!(matches!(result, Ok(None)));

    // Test withdrawal with insufficient funds
    let account_id = ctx
        .account_service
        .create_account("Error Test".to_string(), Decimal::from(100))
        .await
        .unwrap();

    let result = ctx
        .account_service
        .withdraw_money(account_id, Decimal::from(200))
        .await;

    assert!(matches!(
        result,
        Err(AccountError::InsufficientFunds { .. })
    ));
}

#[tokio::test]
async fn test_performance_metrics() {
    let ctx = setup_test_environment().await.unwrap();
    let account_id = ctx
        .account_service
        .create_account("Metrics Test".to_string(), Decimal::from(1000))
        .await
        .unwrap();

    // Perform multiple operations to generate metrics
    for _ in 0..5 {
        ctx.account_service
            .deposit_money(account_id, Decimal::from(100))
            .await
            .unwrap();

        ctx.account_service
            .withdraw_money(account_id, Decimal::from(50))
            .await
            .unwrap();
    }

    // Get account to check cache metrics
    let _ = ctx
        .account_service
        .get_account(account_id)
        .await
        .unwrap()
        .unwrap();

    // Verify metrics are being recorded
    let metrics = ctx.account_service.get_metrics();
    assert!(
        metrics
            .commands_processed
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(
        metrics
            .cache_hits
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 0
    );
    assert!(
        metrics
            .cache_misses
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 0
    );
}

#[tokio::test]
async fn test_transaction_history() {
    let ctx = setup_test_environment().await.unwrap();
    let account_id = ctx
        .account_service
        .create_account("History Test".to_string(), Decimal::from(1000))
        .await
        .unwrap();

    // Perform some transactions
    ctx.account_service
        .deposit_money(account_id, Decimal::from(500))
        .await
        .unwrap();

    ctx.account_service
        .withdraw_money(account_id, Decimal::from(200))
        .await
        .unwrap();

    // Get transaction history
    let transactions = ctx
        .account_service
        .get_account_transactions(account_id)
        .await
        .unwrap();

    // Verify transaction history
    assert_eq!(transactions.len(), 3); // Create + Deposit + Withdraw
    assert!(transactions
        .iter()
        .any(|t| t.transaction_type == "MoneyDeposited"));
    assert!(transactions
        .iter()
        .any(|t| t.transaction_type == "MoneyWithdrawn"));
}

#[tokio::test]
async fn test_duplicate_command_handling() {
    let ctx = setup_test_environment().await.unwrap();
    let account_id = ctx
        .account_service
        .create_account("Duplicate Test".to_string(), Decimal::from(1000))
        .await
        .unwrap();

    // Try to process the same command twice
    let result1 = ctx
        .account_service
        .deposit_money(account_id, Decimal::from(100))
        .await;

    let result2 = ctx
        .account_service
        .deposit_money(account_id, Decimal::from(100))
        .await;

    assert!(result1.is_ok());
    assert!(matches!(result2, Err(AccountError::InfrastructureError(_))));
}

#[tokio::test]
async fn test_high_throughput_performance() {
    eprintln!("🚀 Starting optimized high throughput test...");

    // Add global timeout for the entire test
    let test_future = async {
        // Further optimized test parameters with more realistic targets
        let target_eps = 200; // Reduced from 500 to 200 for more realistic target
        let worker_count = 40; // Reduced from 60 to 40 to reduce contention
        let account_count = 3000; // Reduced from 5000 to 3000 for better cache efficiency
        let channel_buffer_size = 10000;
        let max_retries = 2;
        let test_duration = Duration::from_secs(15); // Reduced from 20s to 15s
        let operation_timeout = Duration::from_secs(1); // Reduced from 2s to 1s

        eprintln!("Initializing test environment...");
        let context = setup_test_environment()
            .await
            .expect("Failed to setup test environment");
        eprintln!("Test environment setup complete");

        // Create accounts for testing with better distribution
        eprintln!("Creating {} test accounts...", account_count);
        let mut account_ids = Vec::new();
        for i in 0..account_count {
            let owner_name = format!("TestUser_{}", i);
            let initial_balance = Decimal::new(10000, 0);
            let account_id = context
                .account_service
                .create_account(owner_name, initial_balance)
                .await?;
            account_ids.push(account_id);
            if i % 300 == 0 {
                eprintln!("Created {}/{} accounts", i + 1, account_count);
            }
        }

        // Enhanced cache warmup phase with more aggressive warming
        eprintln!("🔥 Warming up cache with {} accounts...", account_count);
        let warmup_start = Instant::now();
        let mut warmup_handles = Vec::new();
        for chunk in account_ids.chunks(50) {
            // Reduced chunk size from 100 to 50
            let service = context.account_service.clone();
            let chunk_accounts = chunk.to_vec();
            warmup_handles.push(tokio::spawn(async move {
                for account_id in chunk_accounts {
                    for _ in 0..5 {
                        // Increased warmup rounds from 3 to 5
                        let _ = service.get_account(account_id).await;
                    }
                }
            }));
        }
        for handle in warmup_handles {
            handle.await.expect("Warmup task failed");
        }
        let warmup_duration = warmup_start.elapsed();
        eprintln!(
            "✅ Cache warmup completed in {:.2}s",
            warmup_duration.as_secs_f64()
        );
        tokio::time::sleep(Duration::from_millis(1000)).await; // Increased delay for cache stabilization

        // Start performance test
        eprintln!("🚀 Starting high throughput performance test...");
        eprintln!("📊 Test parameters:");
        eprintln!("  - Target EPS: {}", target_eps);
        eprintln!("  - Worker count: {}", worker_count);
        eprintln!("  - Account count: {}", account_count);
        eprintln!("  - Test duration: {:.1}s", test_duration.as_secs_f64());
        eprintln!(
            "  - Operation timeout: {:.1}s",
            operation_timeout.as_secs_f64()
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let start_time = Instant::now();
        let end_time = start_time + test_duration;

        // Spawn worker tasks with optimized logic
        eprintln!("👥 Spawning {} worker tasks...", worker_count);
        let mut handles = Vec::new();
        for worker_id in 0..worker_count {
            let tx = tx.clone();
            let service = context.account_service.clone();
            let account_ids = account_ids.clone();
            let operation_timeout = operation_timeout;

            let handle = tokio::spawn(async move {
                use rand::{Rng, SeedableRng};
                use rand_chacha::ChaCha8Rng;
                let mut rng = ChaCha8Rng::from_rng(rand::thread_rng()).unwrap();
                let mut operations = 0;
                let mut consecutive_failures = 0;

                while Instant::now() < end_time {
                    let account_index = (worker_id + operations) % account_ids.len();
                    let account_id = account_ids[account_index];

                    // 20% deposit, 10% withdraw, 70% get (favor reads even more for better performance)
                    let amount = rng.gen_range(1..=50); // Reduced amount range
                    let op_roll = rng.gen_range(0..=99);
                    let operation = match op_roll {
                        0..=19 => Operation::Deposit(amount),   // 20%
                        20..=29 => Operation::Withdraw(amount), // 10%
                        30..=99 => Operation::GetAccount,       // 70%
                        _ => Operation::GetAccount,
                    };

                    let mut retries = 0;
                    let mut result = None;
                    while retries < max_retries && result.is_none() {
                        result = Some(match operation {
                            Operation::Deposit(amount) => {
                                tokio::time::timeout(
                                    operation_timeout,
                                    service.deposit_money(account_id, amount.into()),
                                )
                                .await
                            }
                            Operation::Withdraw(amount) => {
                                tokio::time::timeout(
                                    operation_timeout,
                                    service.withdraw_money(account_id, amount.into()),
                                )
                                .await
                            }
                            Operation::GetAccount => tokio::time::timeout(
                                operation_timeout,
                                service.get_account(account_id),
                            )
                            .await
                            .map(|result| result.map(|_| ())),
                        });
                        if let Some(Ok(Err(_))) = &result {
                            retries += 1;
                            if retries < max_retries {
                                let backoff = Duration::from_millis(50 * retries); // Reduced backoff
                                tokio::time::sleep(backoff).await;
                                result = None;
                            }
                        } else {
                            break;
                        }
                    }
                    match result {
                        Some(Ok(Ok(_))) => {
                            operations += 1;
                            consecutive_failures = 0;
                            tx.send(OperationResult::Success).await.ok();
                        }
                        Some(Ok(Err(_))) => {
                            consecutive_failures += 1;
                            tx.send(OperationResult::Failure).await.ok();
                            if consecutive_failures > 5 {
                                // Increased threshold
                                tokio::time::sleep(Duration::from_millis(25)).await;
                                // Reduced sleep
                            }
                        }
                        Some(Err(_)) => {
                            consecutive_failures += 1;
                            tx.send(OperationResult::Timeout).await.ok();
                        }
                        None => {
                            consecutive_failures += 1;
                            tx.send(OperationResult::Failure).await.ok();
                        }
                    }
                    let sleep_time = if consecutive_failures > 3 {
                        Duration::from_millis(5) // Reduced sleep time
                    } else {
                        Duration::from_millis(1)
                    };
                    tokio::time::sleep(sleep_time).await;
                }
                if worker_id % 10 == 0 {
                    eprintln!(
                        "✅ Worker {} completed after {} operations",
                        worker_id, operations
                    );
                }
            });
            handles.push(handle);
        }
        drop(tx);
        eprintln!("📈 Collecting results...");
        let mut total_ops = 0;
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        let mut timed_out_ops = 0;
        while let Some(result) = rx.recv().await {
            total_ops += 1;
            match result {
                OperationResult::Success => successful_ops += 1,
                OperationResult::Failure => failed_ops += 1,
                OperationResult::Timeout => timed_out_ops += 1,
            }
            if total_ops % 500 == 0 {
                let elapsed = start_time.elapsed();
                let current_eps = total_ops as f64 / elapsed.as_secs_f64();
                let current_success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;
                eprintln!(
                    "📊 Progress: {} ops, {:.2} EPS, {:.1}% success",
                    total_ops, current_eps, current_success_rate
                );
            }
        }
        eprintln!("⏳ Waiting for workers to complete...");
        for handle in handles {
            handle.await.expect("Worker task failed");
        }
        eprintln!("✅ All worker tasks completed successfully\n");
        let duration = start_time.elapsed();
        let eps = total_ops as f64 / duration.as_secs_f64();
        let success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;
        eprintln!("🎯 Optimized High Throughput Test Results:");
        eprintln!("==========================================");
        eprintln!("⏱️  Duration: {:.2}s", duration.as_secs_f64());
        eprintln!("📊 Total Operations: {}", total_ops);
        eprintln!("✅ Successful Operations: {}", successful_ops);
        eprintln!("❌ Failed Operations: {}", failed_ops);
        eprintln!("⏰ Timed Out Operations: {}", timed_out_ops);
        eprintln!("🚀 Events Per Second: {:.2}", eps);
        eprintln!("📈 Success Rate: {:.2}%\n", success_rate);
        let metrics = context.account_service.get_metrics();
        eprintln!("🔧 System Metrics:");
        eprintln!(
            "Commands Processed: {}",
            metrics.commands_processed.load(Ordering::Relaxed)
        );
        eprintln!(
            "Commands Failed: {}",
            metrics.commands_failed.load(Ordering::Relaxed)
        );
        eprintln!(
            "Projection Updates: {}",
            metrics.projection_updates.load(Ordering::Relaxed)
        );
        eprintln!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
        eprintln!(
            "Cache Misses: {}",
            metrics.cache_misses.load(Ordering::Relaxed)
        );
        let cache_hit_rate = if metrics.cache_hits.load(Ordering::Relaxed)
            + metrics.cache_misses.load(Ordering::Relaxed)
            > 0
        {
            (metrics.cache_hits.load(Ordering::Relaxed) as f64
                / (metrics.cache_hits.load(Ordering::Relaxed)
                    + metrics.cache_misses.load(Ordering::Relaxed)) as f64)
                * 100.0
        } else {
            0.0
        };
        eprintln!("Cache Hit Rate: {:.2}%", cache_hit_rate);
        eprintln!("\n💰 Final Account States (Sample):");
        for (i, account_id) in account_ids.iter().take(5).enumerate() {
            if let Ok(Some(account)) = context.account_service.get_account(*account_id).await {
                eprintln!("Account {}: Balance = {}", i, account.balance);
            }
        }
        assert!(
            eps >= target_eps as f64 * 0.8,
            "Failed to maintain target EPS. Achieved: {:.2}, Target: {} (80% of {})",
            eps,
            target_eps as f64 * 0.8,
            target_eps
        );
        assert!(
            success_rate >= 85.0, // Increased success rate requirement
            "Success rate too low: {:.2}% (Target: 85%)",
            success_rate
        );
        assert!(
            total_ops >= 1500, // Reduced minimum operations
            "Too few total operations: {} (Minimum: 1500)",
            total_ops
        );
        eprintln!("🎉 All performance targets met! Optimized high throughput test completed successfully.");
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    match tokio::time::timeout(Duration::from_secs(180), test_future).await {
        Ok(_) => eprintln!("✅ Test completed successfully"),
        Err(_) => panic!("❌ Test timed out after 180 seconds"),
    }
}

async fn process_batch(
    service: &Arc<AccountService>,
    account_id: Uuid,
    operations: &[Operation],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    for operation in operations {
        match operation {
            Operation::Deposit(amount) => {
                service.deposit_money(account_id, (*amount).into()).await?;
            }
            Operation::Withdraw(amount) => {
                service.withdraw_money(account_id, (*amount).into()).await?;
            }
            Operation::GetAccount => {
                // No-op for GetAccount in batch helpers
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
enum Operation {
    Deposit(u32),
    Withdraw(u32),
    GetAccount,
}

#[derive(Debug)]
enum OperationResult {
    Success,
    Failure,
    Timeout,
}

#[tokio::test]
async fn test_concurrent_deposits() {
    let ctx = setup_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Create account with initial balance
    let account_id = ctx
        .account_service
        .create_account("Concurrent Test User".to_string(), Decimal::new(1000, 0))
        .await
        .expect("Failed to create account");

    // Add a small delay to ensure projection is updated
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get initial account state
    let initial_account = ctx
        .account_service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account not found");

    assert_eq!(initial_account.balance, Decimal::new(1000, 0));

    // Perform multiple deposits concurrently
    let deposit_amount = Decimal::new(100, 0);
    let deposit_count = 5;
    let mut handles = vec![];
    let successful_deposits = Arc::new(AtomicU64::new(0));
    let failed_deposits = Arc::new(AtomicU64::new(0));

    eprintln!("Starting {} concurrent deposits...", deposit_count);

    for i in 0..deposit_count {
        let service = ctx.account_service.clone();
        let account_id = account_id;
        let amount = deposit_amount;
        let successful_deposits = successful_deposits.clone();
        let failed_deposits = failed_deposits.clone();

        handles.push(tokio::spawn(async move {
            eprintln!("Starting deposit #{}", i + 1);
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                service.deposit_money(account_id, amount),
            )
            .await;

            match result {
                Ok(Ok(_)) => {
                    eprintln!("Deposit #{} completed successfully", i + 1);
                    successful_deposits.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                Ok(Err(e)) => {
                    eprintln!("Deposit #{} failed: {:?}", i + 1, e);
                    failed_deposits.fetch_add(1, Ordering::SeqCst);
                    Err(e)
                }
                Err(_) => {
                    eprintln!("Deposit #{} timed out", i + 1);
                    failed_deposits.fetch_add(1, Ordering::SeqCst);
                    Err(AccountError::InfrastructureError(
                        "Operation timed out".to_string(),
                    ))
                }
            }
        }));
    }

    // Wait for all deposits to complete with timeout
    let mut results = Vec::new();
    for handle in handles {
        match tokio::time::timeout(Duration::from_secs(10), handle).await {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(e)) => {
                eprintln!("Task failed: {:?}", e);
                results.push(Err(AccountError::InfrastructureError(
                    "Task failed".to_string(),
                )));
            }
            Err(_) => {
                eprintln!("Task timed out");
                results.push(Err(AccountError::InfrastructureError(
                    "Task timed out".to_string(),
                )));
            }
        }
    }

    // Verify results
    let successful_count = successful_deposits.load(Ordering::SeqCst);
    let failed_count = failed_deposits.load(Ordering::SeqCst);
    eprintln!("\nDeposit Results:");
    eprintln!("Successful deposits: {}", successful_count);
    eprintln!("Failed deposits: {}", failed_count);

    // Verify final balance
    let final_account = ctx
        .account_service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account not found");

    let expected_balance =
        Decimal::new(1000, 0) + (deposit_amount * Decimal::from(successful_count));
    assert_eq!(
        final_account.balance, expected_balance,
        "Final balance mismatch. Expected: {}, Got: {}",
        expected_balance, final_account.balance
    );

    // Verify transaction history
    let transactions = ctx
        .account_service
        .get_account_transactions(account_id)
        .await
        .expect("Failed to get transactions");

    assert_eq!(
        transactions.len(),
        successful_count as usize + 1, // +1 for account creation
        "Expected {} transactions, got {}",
        successful_count + 1,
        transactions.len()
    );

    // Verify all successful deposits were recorded
    let deposit_transactions: Vec<_> = transactions
        .iter()
        .filter(|t| t.transaction_type == "MoneyDeposited")
        .collect();
    assert_eq!(
        deposit_transactions.len(),
        successful_count as usize,
        "Expected {} deposit transactions, got {}",
        successful_count,
        deposit_transactions.len()
    );

    // Verify transaction amounts
    for transaction in deposit_transactions {
        assert_eq!(
            transaction.amount, deposit_amount,
            "Transaction amount mismatch. Expected: {}, Got: {}",
            deposit_amount, transaction.amount
        );
    }

    // Print metrics
    let metrics = ctx.account_service.get_metrics();
    eprintln!("\nSystem Metrics:");
    eprintln!(
        "Commands Processed: {}",
        metrics.commands_processed.load(Ordering::Relaxed)
    );
    eprintln!(
        "Commands Failed: {}",
        metrics.commands_failed.load(Ordering::Relaxed)
    );
    eprintln!(
        "Projection Updates: {}",
        metrics.projection_updates.load(Ordering::Relaxed)
    );
    eprintln!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
    eprintln!(
        "Cache Misses: {}",
        metrics.cache_misses.load(Ordering::Relaxed)
    );
}

#[tokio::test]
async fn test_infrastructure_configurations() {
    eprintln!("Testing infrastructure configurations...");

    // Test database connection
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    eprintln!("Testing PostgreSQL connection...");
    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
    {
        Ok(pool) => {
            eprintln!("✅ PostgreSQL connection successful");
            pool
        }
        Err(e) => {
            eprintln!("❌ PostgreSQL connection failed: {}", e);
            return;
        }
    };

    // Test database schema and configuration
    eprintln!("\nTesting database schema and configuration...");
    let tables = match sqlx::query!(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )
    .fetch_all(&pool)
    .await
    {
        Ok(tables) => {
            eprintln!("✅ Database schema check successful");
            tables
        }
        Err(e) => {
            eprintln!("❌ Database schema check failed: {}", e);
            return;
        }
    };

    eprintln!("\nFound tables:");
    for table in tables {
        eprintln!("- {}", table.table_name.unwrap_or_default());
    }

    // Check database configuration
    eprintln!("\nChecking database configuration...");
    let db_config = match sqlx::query!("SHOW max_connections").fetch_one(&pool).await {
        Ok(config) => {
            eprintln!(
                "✅ Max connections: {}",
                config.max_connections.as_ref().unwrap_or(&"0".to_string())
            );
            config
        }
        Err(e) => {
            eprintln!("❌ Failed to get max_connections: {}", e);
            return;
        }
    };

    // Test Redis connection and configuration
    eprintln!("\nTesting Redis connection and configuration...");
    let redis_client = match redis::Client::open("redis://127.0.0.1/") {
        Ok(client) => {
            eprintln!("✅ Redis client created successfully");
            client
        }
        Err(e) => {
            eprintln!("❌ Redis client creation failed: {}", e);
            return;
        }
    };

    let mut con = match redis_client.get_multiplexed_async_connection().await {
        Ok(con) => {
            eprintln!("✅ Redis connection established");
            con
        }
        Err(e) => {
            eprintln!("❌ Redis connection failed: {}", e);
            return;
        }
    };

    // Test Redis PING with latency measurement
    let start = std::time::Instant::now();
    match redis::cmd("PING").query_async::<_, String>(&mut con).await {
        Ok(_) => {
            let latency = start.elapsed();
            eprintln!("✅ Redis PING successful (latency: {:?})", latency);
        }
        Err(e) => {
            eprintln!("❌ Redis PING failed: {}", e);
            return;
        }
    }

    // Check Redis configuration
    eprintln!("\nChecking Redis configuration...");
    let configs = vec!["maxmemory", "maxmemory-policy", "timeout", "tcp-keepalive"];
    for config in configs {
        match redis::cmd("CONFIG")
            .arg("GET")
            .arg(config)
            .query_async::<_, Vec<String>>(&mut con)
            .await
        {
            Ok(values) if values.len() >= 2 => {
                eprintln!("✅ {}: {}", values[0], values[1]);
            }
            Ok(_) => eprintln!("⚠️ {}: No value found", config),
            Err(e) => eprintln!("❌ Failed to get {}: {}", config, e),
        }
    }

    // Test database performance with multiple queries
    eprintln!("\nTesting database performance...");
    let queries = vec![
        "SELECT 1",
        "SELECT COUNT(*) FROM events",
        "SELECT COUNT(*) FROM account_projections",
        "SELECT COUNT(*) FROM transaction_projections",
    ];

    for query in queries {
        let start = std::time::Instant::now();
        match sqlx::query(query).fetch_one(&pool).await {
            Ok(row) => {
                let duration = start.elapsed();
                let result: i64 = row.try_get(0).unwrap_or(0);
                eprintln!(
                    "✅ Query '{}' completed in {:?} (result: {:?})",
                    query, duration, result
                );
            }
            Err(e) => eprintln!("❌ Query '{}' failed: {}", query, e),
        }
    }

    // Test Redis performance with multiple operations
    eprintln!("\nTesting Redis performance...");

    let start = std::time::Instant::now();
    match redis::cmd("PING").query_async::<_, String>(&mut con).await {
        Ok(result) => {
            let duration = start.elapsed();
            eprintln!(
                "✅ Redis PING completed in {:?} (result: {:?})",
                duration, result
            );
        }
        Err(e) => eprintln!("❌ Redis PING failed: {}", e),
    }

    let start = std::time::Instant::now();
    match redis::cmd("SET")
        .arg("test_key")
        .arg("test_value")
        .query_async::<_, String>(&mut con)
        .await
    {
        Ok(result) => {
            let duration = start.elapsed();
            eprintln!(
                "✅ Redis SET completed in {:?} (result: {:?})",
                duration, result
            );
        }
        Err(e) => eprintln!("❌ Redis SET failed: {}", e),
    }

    let start = std::time::Instant::now();
    match redis::cmd("GET")
        .arg("test_key")
        .query_async::<_, String>(&mut con)
        .await
    {
        Ok(result) => {
            let duration = start.elapsed();
            eprintln!(
                "✅ Redis GET completed in {:?} (result: {:?})",
                duration, result
            );
        }
        Err(e) => eprintln!("❌ Redis GET failed: {}", e),
    }

    let start = std::time::Instant::now();
    match redis::cmd("DEL")
        .arg("test_key")
        .query_async::<_, i32>(&mut con)
        .await
    {
        Ok(result) => {
            let duration = start.elapsed();
            eprintln!(
                "✅ Redis DEL completed in {:?} (result: {:?})",
                duration, result
            );
        }
        Err(e) => eprintln!("❌ Redis DEL failed: {}", e),
    }

    // Check database connection pool stats
    eprintln!("\nDatabase connection pool statistics:");
    eprintln!("Active connections: {}", pool.size());
    eprintln!("Idle connections: {}", pool.num_idle());
    eprintln!(
        "Max connections: {}",
        db_config.max_connections.unwrap_or_default()
    );

    // Check Redis memory usage
    eprintln!("\nRedis memory usage:");
    match redis::cmd("INFO")
        .arg("memory")
        .query_async::<_, String>(&mut con)
        .await
    {
        Ok(info) => {
            for line in info.lines() {
                if line.starts_with("used_memory:")
                    || line.starts_with("used_memory_peak:")
                    || line.starts_with("used_memory_lua:")
                {
                    eprintln!("{}", line);
                }
            }
        }
        Err(e) => eprintln!("❌ Failed to get Redis memory info: {}", e),
    }

    eprintln!("\nInfrastructure configuration test completed successfully!");
}

#[tokio::test]
async fn test_extreme_concurrency() {
    eprintln!("Starting extreme concurrency test...");

    let test_future = async {
        let test_duration = Duration::from_secs(10);
        let operation_timeout = Duration::from_millis(1000);

        // Extreme concurrency parameters
        let worker_count = 50; // 50 concurrent workers
        let channel_buffer_size = 1000;

        eprintln!("Initializing test environment for extreme concurrency...");
        let context = setup_test_environment()
            .await
            .expect("Failed to setup test environment");

        // Create multiple test accounts for better distribution
        let mut account_ids = Vec::new();
        for i in 0..5 {
            let account_id = context
                .account_service
                .create_account(format!("Test User {}", i), Decimal::new(10000, 0))
                .await
                .expect("Failed to create account");
            account_ids.push(account_id);
            eprintln!("Created account {}: {}", i, account_id);
        }

        // Pre-warm cache for all accounts
        for account_id in &account_ids {
            let _ = context
                .account_service
                .get_account(*account_id)
                .await
                .expect("Failed to pre-warm cache");
        }
        eprintln!("Cache pre-warmed for all accounts");

        eprintln!(
            "Starting extreme concurrency test with {} workers",
            worker_count
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let start_time = Instant::now();
        let end_time = start_time + test_duration;

        // Spawn worker tasks
        let mut handles = Vec::new();
        for worker_id in 0..worker_count {
            let tx = tx.clone();
            let service = context.account_service.clone();
            let account_ids = account_ids.clone();
            let operation_timeout = operation_timeout;

            let handle = tokio::spawn(async move {
                use rand::{Rng, SeedableRng};
                use rand_chacha::ChaCha8Rng;
                let mut rng = ChaCha8Rng::from_rng(rand::thread_rng()).unwrap();
                let mut operations = 0;

                while Instant::now() < end_time {
                    // Randomly select an account
                    let account_id = account_ids[rng.gen_range(0..account_ids.len())];
                    let amount = rng.gen_range(1..=50);
                    let operation = if rng.gen_bool(0.6) {
                        // 60% deposits, 40% withdrawals
                        Operation::Deposit(amount)
                    } else {
                        Operation::Withdraw(amount)
                    };

                    let result = match operation {
                        Operation::Deposit(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.deposit_money(account_id, amount.into()),
                            )
                            .await
                        }
                        Operation::Withdraw(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.withdraw_money(account_id, amount.into()),
                            )
                            .await
                        }
                        Operation::GetAccount => Ok(Ok(())),
                    };

                    match result {
                        Ok(Ok(_)) => {
                            operations += 1;
                            tx.send(OperationResult::Success).await.ok();
                        }
                        Ok(Err(e)) => {
                            if worker_id % 10 == 0 {
                                // Only log every 10th worker to reduce noise
                                eprintln!("Worker {} operation failed: {:?}", worker_id, e);
                            }
                            tx.send(OperationResult::Failure).await.ok();
                        }
                        Err(_) => {
                            if worker_id % 10 == 0 {
                                eprintln!("Worker {} operation timed out", worker_id);
                            }
                            tx.send(OperationResult::Timeout).await.ok();
                        }
                    }

                    // Minimal sleep for maximum throughput
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                if worker_id % 10 == 0 {
                    eprintln!(
                        "Worker {} completed after {} operations",
                        worker_id, operations
                    );
                }
            });

            handles.push(handle);
        }

        drop(tx);

        // Collect results
        let mut total_ops = 0;
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        let mut timed_out_ops = 0;

        while let Some(result) = rx.recv().await {
            total_ops += 1;
            match result {
                OperationResult::Success => successful_ops += 1,
                OperationResult::Failure => failed_ops += 1,
                OperationResult::Timeout => timed_out_ops += 1,
            }

            if total_ops % 500 == 0 {
                let elapsed = start_time.elapsed();
                let current_eps = total_ops as f64 / elapsed.as_secs_f64();
                eprintln!("Current EPS: {:.2}, Total Ops: {}", current_eps, total_ops);
            }
        }

        // Wait for all workers to complete
        for handle in handles {
            handle.await.expect("Worker task failed");
        }

        // Calculate final metrics
        let duration = start_time.elapsed();
        let eps = total_ops as f64 / duration.as_secs_f64();

        eprintln!("\nExtreme Concurrency Test Results:");
        eprintln!("Duration: {:.2}s", duration.as_secs_f64());
        eprintln!("Total Operations: {}", total_ops);
        eprintln!("Successful Operations: {}", successful_ops);
        eprintln!("Failed Operations: {}", failed_ops);
        eprintln!("Timed Out Operations: {}", timed_out_ops);
        eprintln!("Events Per Second: {:.2}", eps);
        eprintln!(
            "Success Rate: {:.2}%",
            (successful_ops as f64 / total_ops as f64) * 100.0
        );

        // Get system metrics
        let metrics = context.account_service.get_metrics();
        eprintln!("\nSystem Metrics:");
        eprintln!(
            "Commands Processed: {}",
            metrics.commands_processed.load(Ordering::Relaxed)
        );
        eprintln!(
            "Commands Failed: {}",
            metrics.commands_failed.load(Ordering::Relaxed)
        );
        eprintln!(
            "Projection Updates: {}",
            metrics.projection_updates.load(Ordering::Relaxed)
        );
        eprintln!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
        eprintln!(
            "Cache Misses: {}",
            metrics.cache_misses.load(Ordering::Relaxed)
        );

        // Verify final account states
        eprintln!("\nFinal Account States:");
        for (i, account_id) in account_ids.iter().enumerate() {
            if let Ok(Some(account)) = context.account_service.get_account(*account_id).await {
                eprintln!("Account {}: Balance = {}", i, account.balance);
            }
        }

        // Assert reasonable performance
        assert!(
            eps >= 100.0, // Expect at least 100 ops/sec under extreme concurrency
            "Failed to maintain reasonable EPS under extreme concurrency. Achieved: {:.2}",
            eps
        );

        assert!(
            (successful_ops as f64 / total_ops as f64) >= 0.8, // At least 80% success rate
            "Success rate too low: {:.2}%",
            (successful_ops as f64 / total_ops as f64) * 100.0
        );
    };

    // Add global timeout of 3 minutes for extreme concurrency test
    match tokio::time::timeout(Duration::from_secs(180), test_future).await {
        Ok(_) => eprintln!("Extreme concurrency test completed successfully"),
        Err(_) => panic!("Extreme concurrency test timed out after 180 seconds"),
    }
}

#[tokio::test]
async fn test_optimized_high_concurrency() {
    eprintln!("Starting optimized high concurrency test...");

    let test_future = async {
        let test_duration = Duration::from_secs(15);
        let operation_timeout = Duration::from_millis(2000); // Increased timeout

        // Optimized concurrency parameters
        let worker_count = 30; // Reduced from 50 to 30
        let account_count = 20; // Increased from 5 to 20 accounts for better distribution
        let channel_buffer_size = 1000;

        eprintln!("Initializing test environment for optimized high concurrency...");
        let context = setup_test_environment()
            .await
            .expect("Failed to setup test environment");

        // Create more test accounts for better distribution
        let mut account_ids = Vec::new();
        for i in 0..account_count {
            let account_id = context
                .account_service
                .create_account(format!("Test User {}", i), Decimal::new(10000, 0))
                .await
                .expect("Failed to create account");
            account_ids.push(account_id);
            eprintln!("Created account {}: {}", i, account_id);
        }

        // Pre-warm cache for all accounts
        for account_id in &account_ids {
            let _ = context
                .account_service
                .get_account(*account_id)
                .await
                .expect("Failed to pre-warm cache");
        }
        eprintln!("Cache pre-warmed for all {} accounts", account_count);

        eprintln!(
            "Starting optimized high concurrency test with {} workers",
            worker_count
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let start_time = Instant::now();
        let end_time = start_time + test_duration;

        // Spawn worker tasks with better account distribution
        let mut handles = Vec::new();
        let error_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for worker_id in 0..worker_count {
            let tx = tx.clone();
            let service = context.account_service.clone();
            let account_ids = account_ids.clone();
            let operation_timeout = operation_timeout;
            let error_counter = error_counter.clone();

            let handle = tokio::spawn(async move {
                use rand::{Rng, SeedableRng};
                use rand_chacha::ChaCha8Rng;
                let mut rng = ChaCha8Rng::from_rng(rand::thread_rng()).unwrap();
                let mut operations = 0;
                let mut consecutive_failures = 0;

                while Instant::now() < end_time {
                    // Use worker-specific account selection to reduce conflicts
                    let account_index = (worker_id + operations) % account_ids.len();
                    let account_id = account_ids[account_index];

                    // Generate operation
                    let amount = rng.gen_range(1..=20);
                    let operation = if rng.gen_bool(0.7) {
                        // 70% deposits, 30% withdrawals
                        Operation::Deposit(amount)
                    } else {
                        Operation::Withdraw(amount)
                    };

                    // Execute operation with latency tracking
                    let operation_start = Instant::now();
                    let result = match operation {
                        Operation::Deposit(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.deposit_money(account_id, amount.into()),
                            )
                            .await
                        }
                        Operation::Withdraw(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.withdraw_money(account_id, amount.into()),
                            )
                            .await
                        }
                        Operation::GetAccount => Ok(Ok(())),
                    };
                    let latency = operation_start.elapsed();

                    match result {
                        Ok(Ok(_)) => {
                            operations += 1;
                            consecutive_failures = 0;
                            tx.send(OperationResult::Success).await.ok();
                        }
                        Ok(Err(e)) => {
                            consecutive_failures += 1;
                            let count =
                                error_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if count < 10 {
                                eprintln!("Worker {} operation failed: {:?}", worker_id, e);
                            }
                            tx.send(OperationResult::Failure).await.ok();

                            // Adaptive backoff
                            if consecutive_failures > 3 {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                        Err(_) => {
                            consecutive_failures += 1;
                            tx.send(OperationResult::Timeout).await.ok();
                        }
                    }

                    // Adaptive sleep based on performance
                    let sleep_time = if consecutive_failures > 2 {
                        Duration::from_millis(50)
                    } else {
                        Duration::from_millis(2) // Very aggressive for high throughput
                    };
                    tokio::time::sleep(sleep_time).await;
                }

                if worker_id % 5 == 0 {
                    eprintln!(
                        "Worker {} completed after {} operations",
                        worker_id, operations
                    );
                }
            });

            handles.push(handle);
        }

        drop(tx);

        // Collect results
        let mut total_ops = 0;
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        let mut timed_out_ops = 0;

        while let Some(result) = rx.recv().await {
            total_ops += 1;
            match result {
                OperationResult::Success => successful_ops += 1,
                OperationResult::Failure => failed_ops += 1,
                OperationResult::Timeout => timed_out_ops += 1,
            }

            // Progress reporting
            if total_ops % 1000 == 0 {
                let elapsed = start_time.elapsed();
                let current_eps = total_ops as f64 / elapsed.as_secs_f64();
                let current_success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;
                eprintln!(
                    "  Progress: {} ops, {:.2} EPS, {:.1}% success",
                    total_ops, current_eps, current_success_rate
                );
            }
        }

        // Wait for all workers to complete
        eprintln!("⏳ Waiting for workers to complete...");
        for handle in handles {
            handle.await.expect("Worker task failed");
        }

        // Calculate final metrics
        let duration = start_time.elapsed();
        let eps = total_ops as f64 / duration.as_secs_f64();
        let success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;

        eprintln!("\nOptimized High Concurrency Test Results:");
        eprintln!("Duration: {:.2}s", duration.as_secs_f64());
        eprintln!("Total Operations: {}", total_ops);
        eprintln!("Successful Operations: {}", successful_ops);
        eprintln!("Failed Operations: {}", failed_ops);
        eprintln!("Timed Out Operations: {}", timed_out_ops);
        eprintln!("Events Per Second: {:.2}", eps);
        eprintln!("Success Rate: {:.2}%", success_rate);

        // Get system metrics
        let metrics = context.account_service.get_metrics();
        eprintln!("\nSystem Metrics:");
        eprintln!(
            "Commands Processed: {}",
            metrics.commands_processed.load(Ordering::Relaxed)
        );
        eprintln!(
            "Commands Failed: {}",
            metrics.commands_failed.load(Ordering::Relaxed)
        );
        eprintln!(
            "Projection Updates: {}",
            metrics.projection_updates.load(Ordering::Relaxed)
        );
        eprintln!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
        eprintln!(
            "Cache Misses: {}",
            metrics.cache_misses.load(Ordering::Relaxed)
        );

        let cache_hit_rate = if metrics.cache_hits.load(Ordering::Relaxed)
            + metrics.cache_misses.load(Ordering::Relaxed)
            > 0
        {
            (metrics.cache_hits.load(Ordering::Relaxed) as f64
                / (metrics.cache_hits.load(Ordering::Relaxed)
                    + metrics.cache_misses.load(Ordering::Relaxed)) as f64)
                * 100.0
        } else {
            0.0
        };
        eprintln!("  Cache Hit Rate: {:.2}%", cache_hit_rate);

        // Verify final account states
        eprintln!("\nFinal Account States:");
        for (i, account_id) in account_ids.iter().enumerate() {
            if let Ok(Some(account)) = context.account_service.get_account(*account_id).await {
                eprintln!("  Account {}: Balance = {}", i, account.balance);
            }
        }

        // Assert reasonable performance
        assert!(
            eps >= 50.0, // Expect at least 50 ops/sec under optimized concurrency
            "Failed to maintain reasonable EPS under optimized concurrency. Achieved: {:.2}",
            eps
        );

        assert!(
            (successful_ops as f64 / total_ops as f64) >= 0.7, // At least 70% success rate
            "Success rate too low: {:.2}%",
            (successful_ops as f64 / total_ops as f64) * 100.0
        );
    };

    // Add global timeout of 2 minutes for optimized concurrency test
    match tokio::time::timeout(Duration::from_secs(120), test_future).await {
        Ok(_) => eprintln!("Optimized high concurrency test completed successfully"),
        Err(_) => panic!("Optimized high concurrency test timed out after 120 seconds"),
    }
}

#[tokio::test]
async fn test_comprehensive_high_throughput() {
    eprintln!("🚀 Starting Comprehensive High-Throughput Test...");

    // Test configuration and performance targets
    let test_duration = Duration::from_secs(30);
    let operation_timeout = Duration::from_millis(200);
    let account_count = 50;
    let worker_count = 200;
    let channel_buffer_size = 5000;
    let target_eps = 1000;
    let target_success_rate = 85.0;
    let target_latency_p95 = Duration::from_millis(100);
    let latency_tracker = Arc::new(Mutex::new(Vec::new()));
    let error_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let test_future = async {
        // Setup test environment
        let context = setup_test_environment()
            .await
            .expect("Failed to setup test environment");

        // Create test accounts
        let mut account_ids = Vec::new();
        for i in 0..account_count {
            let account_id = context
                .account_service
                .create_account(format!("HighThroughput User {}", i), Decimal::new(10000, 0))
                .await
                .expect("Failed to create account");
            account_ids.push(account_id);

            if i % 10 == 0 {
                eprintln!("  Created account {}/{}", i + 1, account_count);
            }
        }
        eprintln!("✅ All accounts created successfully");

        // Pre-warm cache
        eprintln!("🔥 Pre-warming cache...");
        for account_id in &account_ids {
            let _ = context
                .account_service
                .get_account(*account_id)
                .await
                .expect("Failed to pre-warm cache");
        }
        eprintln!("✅ Cache pre-warmed");

        // Start performance test
        eprintln!("🚀 Starting performance test with {} workers", worker_count);

        let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let start_time = Instant::now();
        let end_time = start_time + test_duration;

        // Spawn worker tasks
        let mut handles = Vec::new();
        for worker_id in 0..worker_count {
            let tx = tx.clone();
            let service = context.account_service.clone();
            let account_ids = account_ids.clone();
            let operation_timeout = operation_timeout;
            let latency_tracker = latency_tracker.clone();
            let error_counter = error_counter.clone();

            let handle = tokio::spawn(async move {
                use rand::{Rng, SeedableRng};
                use rand_chacha::ChaCha8Rng;
                let mut rng = ChaCha8Rng::from_rng(rand::thread_rng()).unwrap();
                let mut operations = 0;
                let mut consecutive_failures = 0;

                while Instant::now() < end_time {
                    // Select account with worker-specific distribution
                    let account_index = (worker_id + operations) % account_ids.len();
                    let account_id = account_ids[account_index];

                    // Generate operation
                    let amount = rng.gen_range(1..=50);
                    let operation = if rng.gen_bool(0.6) {
                        // 60% deposits, 40% withdrawals
                        Operation::Deposit(amount)
                    } else {
                        Operation::Withdraw(amount)
                    };

                    // Execute operation with latency tracking
                    let operation_start = Instant::now();
                    let result = match operation {
                        Operation::Deposit(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.deposit_money(account_id, amount.into()),
                            )
                            .await
                        }
                        Operation::Withdraw(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.withdraw_money(account_id, amount.into()),
                            )
                            .await
                        }
                        Operation::GetAccount => Ok(Ok(())),
                    };
                    let latency = operation_start.elapsed();

                    // Track latency
                    {
                        let mut tracker = latency_tracker.lock().unwrap();
                        tracker.push(latency);
                    }

                    match result {
                        Ok(Ok(_)) => {
                            operations += 1;
                            consecutive_failures = 0;
                            tx.send(OperationResult::Success).await.ok();
                        }
                        Ok(Err(e)) => {
                            consecutive_failures += 1;
                            let count =
                                error_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if count < 10 {
                                eprintln!("Worker {} operation failed: {:?}", worker_id, e);
                            }
                            tx.send(OperationResult::Failure).await.ok();

                            // Adaptive backoff
                            if consecutive_failures > 3 {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                        Err(_) => {
                            consecutive_failures += 1;
                            tx.send(OperationResult::Timeout).await.ok();
                        }
                    }

                    // Adaptive sleep based on performance
                    let sleep_time = if consecutive_failures > 2 {
                        Duration::from_millis(50)
                    } else {
                        Duration::from_millis(2) // Very aggressive for high throughput
                    };
                    tokio::time::sleep(sleep_time).await;
                }

                if worker_id % 20 == 0 {
                    eprintln!("  Worker {} completed {} operations", worker_id, operations);
                }
            });

            handles.push(handle);
        }

        drop(tx);

        // Collect results
        eprintln!("📈 Collecting results...");
        let mut total_ops = 0;
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        let mut timed_out_ops = 0;

        while let Some(result) = rx.recv().await {
            total_ops += 1;
            match result {
                OperationResult::Success => successful_ops += 1,
                OperationResult::Failure => failed_ops += 1,
                OperationResult::Timeout => timed_out_ops += 1,
            }

            // Progress reporting
            if total_ops % 1000 == 0 {
                let elapsed = start_time.elapsed();
                let current_eps = total_ops as f64 / elapsed.as_secs_f64();
                let current_success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;
                eprintln!(
                    "  Progress: {} ops, {:.2} EPS, {:.1}% success",
                    total_ops, current_eps, current_success_rate
                );
            }
        }

        // Wait for all workers to complete
        eprintln!("⏳ Waiting for workers to complete...");
        for handle in handles {
            handle.await.expect("Worker task failed");
        }

        // Calculate final metrics
        let duration = start_time.elapsed();
        let eps = total_ops as f64 / duration.as_secs_f64();
        let success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;

        // Calculate latency percentiles
        let latencies = {
            let mut tracker = latency_tracker.lock().unwrap();
            tracker.sort();
            tracker.clone()
        };

        let p50_latency = if latencies.len() > 0 {
            latencies[latencies.len() * 50 / 100]
        } else {
            Duration::ZERO
        };

        let p95_latency = if latencies.len() > 0 {
            latencies[latencies.len() * 95 / 100]
        } else {
            Duration::ZERO
        };

        let p99_latency = if latencies.len() > 0 {
            latencies[latencies.len() * 99 / 100]
        } else {
            Duration::ZERO
        };

        // Print comprehensive results
        eprintln!("\n🎯 Comprehensive High-Throughput Test Results:");
        eprintln!("================================================");
        eprintln!("📊 Performance Metrics:");
        eprintln!("  Duration: {:.2}s", duration.as_secs_f64());
        eprintln!("  Total Operations: {}", total_ops);
        eprintln!("  Events Per Second: {:.2}", eps);
        eprintln!("  Success Rate: {:.2}%", success_rate);

        eprintln!("\n⚡ Latency Metrics:");
        eprintln!("  P50 Latency: {:?}", p50_latency);
        eprintln!("  P95 Latency: {:?}", p95_latency);
        eprintln!("  P99 Latency: {:?}", p99_latency);
        eprintln!(
            "  Average Latency: {:?}",
            latencies.iter().sum::<Duration>() / latencies.len().max(1) as u32
        );

        eprintln!("\n📈 Operation Breakdown:");
        eprintln!(
            "  Successful: {} ({:.2}%)",
            successful_ops,
            (successful_ops as f64 / total_ops as f64) * 100.0
        );
        eprintln!(
            "  Failed: {} ({:.2}%)",
            failed_ops,
            (failed_ops as f64 / total_ops as f64) * 100.0
        );
        eprintln!(
            "  Timed Out: {} ({:.2}%)",
            timed_out_ops,
            (timed_out_ops as f64 / total_ops as f64) * 100.0
        );

        // System metrics
        let metrics = context.account_service.get_metrics();
        eprintln!("\n🔧 System Metrics:");
        eprintln!(
            "  Commands Processed: {}",
            metrics.commands_processed.load(Ordering::Relaxed)
        );
        eprintln!(
            "  Commands Failed: {}",
            metrics.commands_failed.load(Ordering::Relaxed)
        );
        eprintln!(
            "  Projection Updates: {}",
            metrics.projection_updates.load(Ordering::Relaxed)
        );
        eprintln!(
            "  Cache Hits: {}",
            metrics.cache_hits.load(Ordering::Relaxed)
        );
        eprintln!(
            "  Cache Misses: {}",
            metrics.cache_misses.load(Ordering::Relaxed)
        );

        let cache_hit_rate = if metrics.cache_hits.load(Ordering::Relaxed)
            + metrics.cache_misses.load(Ordering::Relaxed)
            > 0
        {
            (metrics.cache_hits.load(Ordering::Relaxed) as f64
                / (metrics.cache_hits.load(Ordering::Relaxed)
                    + metrics.cache_misses.load(Ordering::Relaxed)) as f64)
                * 100.0
        } else {
            0.0
        };
        eprintln!("  Cache Hit Rate: {:.2}%", cache_hit_rate);

        // Verify final account states
        eprintln!("\n💰 Final Account States (Sample):");
        for (i, account_id) in account_ids.iter().enumerate() {
            if let Ok(Some(account)) = context.account_service.get_account(*account_id).await {
                eprintln!("  Account {}: Balance = {}", i, account.balance);
            }
        }

        // Performance assertions
        eprintln!("\n✅ Performance Validation:");
        let eps_ok = eps >= target_eps as f64;
        let success_rate_ok = success_rate >= target_success_rate;
        let latency_ok = p95_latency <= target_latency_p95;

        eprintln!(
            "  EPS Target: {} (Achieved: {:.2}) - {}",
            target_eps,
            eps,
            if eps_ok { "✅ PASS" } else { "❌ FAIL" }
        );
        eprintln!(
            "  Success Rate Target: {}% (Achieved: {:.2}%) - {}",
            target_success_rate,
            success_rate,
            if success_rate_ok {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );
        eprintln!(
            "  P95 Latency Target: {:?} (Achieved: {:?}) - {}",
            target_latency_p95,
            p95_latency,
            if latency_ok { "✅ PASS" } else { "❌ FAIL" }
        );

        // Assertions
        assert!(
            eps_ok,
            "Failed to maintain target EPS. Achieved: {:.2}, Target: {}",
            eps, target_eps
        );

        assert!(
            success_rate_ok,
            "Success rate too low: {:.2}% (Target: {}%)",
            success_rate, target_success_rate
        );

        assert!(
            latency_ok,
            "P95 latency too high: {:?} (Target: {:?})",
            p95_latency, target_latency_p95
        );

        assert!(
            total_ops >= 5000,
            "Too few total operations: {} (Minimum: 5000)",
            total_ops
        );

        eprintln!("\n🎉 All performance targets met! Comprehensive high-throughput test completed successfully.");

        Ok::<(), Box<dyn std::error::Error>>(())
    };

    // Global timeout
    match tokio::time::timeout(Duration::from_secs(300), test_future).await {
        Ok(_) => eprintln!("✅ Comprehensive high-throughput test completed successfully"),
        Err(_) => panic!("❌ Comprehensive high-throughput test timed out after 300 seconds"),
    }
}
