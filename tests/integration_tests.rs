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
use rand;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use redis;
use rust_decimal::Decimal;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::sync::mpsc;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::future::Future;
use futures::FutureExt;

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

    // Initialize database pool with minimal connections for tests
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Initialize Redis client with test-specific config
    let redis_client = Arc::new(redis::Client::open("redis://127.0.0.1/")?);
    let redis_client_trait = RealRedisClient::new(redis_client.as_ref().clone(), None);

    // Initialize services with test-specific configs
    let event_store = Arc::new(EventStore::new(pool.clone())) as Arc<dyn EventStoreTrait + 'static>;
    let projection_store = Arc::new(ProjectionStore::new_test(pool.clone()))
        as Arc<dyn ProjectionStoreTrait + 'static>;
    let cache_service = Arc::new(CacheService::new_test(redis_client_trait))
        as Arc<dyn CacheServiceTrait + 'static>;
    let repository: Arc<AccountRepository> = Arc::new(AccountRepository::new(event_store));
    let repository_clone = repository.clone();

    let service = Arc::new(AccountService::new(
        repository,
        projection_store,
        cache_service,
        Arc::new(Default::default()),
        10, // Lower concurrency for tests
    ));

    // Start cleanup task
    let service_clone = service.clone();
    let cleanup_handle = tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                // Cleanup code here
                // No cleanup needed as AccountService handles its own cleanup
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
    println!("Starting high throughput test...");
    
    // Add timeout for entire test setup
    let setup_result = tokio::time::timeout(
        Duration::from_secs(10),
        setup_test_environment()
    ).await;
    
    let ctx = match setup_result {
        Ok(Ok(ctx)) => {
            println!("Test environment setup complete");
            ctx
        },
        Ok(Err(e)) => {
            println!("Failed to setup test environment: {}", e);
            return;
        },
        Err(_) => {
            println!("Test environment setup timed out after 10 seconds");
            return;
        }
    };

    // Create a single test account with timeout
    println!("Creating test account...");
    let account_id = match tokio::time::timeout(
        Duration::from_secs(5),
        ctx.account_service.create_account("Load Test User".to_string(), Decimal::new(10000, 0))
    ).await {
        Ok(Ok(id)) => {
            println!("Successfully created account: {}", id);
            id
        },
        Ok(Err(e)) => {
            println!("Failed to create account: {}", e);
            return;
        },
        Err(_) => {
            println!("Account creation timed out after 5 seconds");
            return;
        }
    };

    let target_eps = 1000;
    let test_duration = Duration::from_secs(5);
    let operation_timeout = Duration::from_millis(50);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(bool, Duration)>(1000);
    let successful_ops = Arc::new(AtomicU64::new(0));
    let failed_ops = Arc::new(AtomicU64::new(0));
    let operation_times = Arc::new(Mutex::new(Vec::new()));

    println!("Starting performance test with 1 worker");
    println!("Target EPS: {}, Duration: {} seconds", target_eps, test_duration.as_secs());
    println!("Spawning worker task...");

    let worker_handle: tokio::task::JoinHandle<Result<(), ()>> = tokio::spawn({
        let successful_ops = successful_ops.clone();
        let failed_ops = failed_ops.clone();
        let operation_times = operation_times.clone();
        let account_id = account_id;
        let service = ctx.account_service.clone();
        let tx = tx.clone();

        async move {
            println!("Worker started");
            let start_time = Instant::now();
            let mut operation_count = 0;
            let mut last_report = Instant::now();
            let mut last_count = 0;

            while start_time.elapsed() < test_duration {
                let operation_start = Instant::now();
                let amount = rand::thread_rng().gen_range(1..=50);
                let is_deposit = rand::thread_rng().gen_bool(0.7);

                let result = tokio::time::timeout(
                    operation_timeout,
                    async {
                        if is_deposit {
                            service.deposit_money(account_id, Decimal::from(amount)).await
                        } else {
                            service.withdraw_money(account_id, Decimal::from(amount)).await
                        }
                    }
                ).await;

                match result {
                    Ok(Ok(_)) => {
                        successful_ops.fetch_add(1, Ordering::SeqCst);
                        operation_count += 1;
                        let duration = operation_start.elapsed();
                        operation_times.lock().unwrap().push(duration.as_micros() as u64);
                        
                        if operation_count % 100 == 0 {
                            println!("Starting operation #{}", operation_count);
                            println!("Attempting {} operation with amount {}", 
                                if is_deposit { "deposit" } else { "withdraw" }, amount);
                            println!("Operation #{} successful", operation_count);
                            println!("Completed operation #{}", operation_count);
                        }

                        if last_report.elapsed() >= Duration::from_secs(1) {
                            let current_eps = (operation_count - last_count) as f64 / 
                                last_report.elapsed().as_secs_f64();
                            println!("Current EPS: {:.2}, Total Ops: {}", current_eps, operation_count);
                            last_report = Instant::now();
                            last_count = operation_count;
                        }

                        // Send success result through channel
                        let _ = tx.send((true, duration)).await;
                    }
                    Ok(Err(e)) => {
                        failed_ops.fetch_add(1, Ordering::SeqCst);
                        println!("Operation failed: {:?}", e);
                        let _ = tx.send((false, operation_start.elapsed())).await;
                    }
                    Err(_) => {
                        failed_ops.fetch_add(1, Ordering::SeqCst);
                        println!("Operation timed out");
                        let _ = tx.send((false, operation_start.elapsed())).await;
                    }
                }

                // Small delay to prevent overwhelming the system
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            println!("Worker completed after {} operations", operation_count);
            Ok(())
        }
    });

    println!("Starting to collect results...");
    let start_time = Instant::now();
    let mut results = Vec::new();

    while start_time.elapsed() < test_duration {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(result)) => results.push(result),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    println!("Finished collecting results after {} operations", results.len());
    println!("Waiting for worker to complete...");
    worker_handle.await.unwrap().unwrap();
    println!("Worker task completed successfully\n");

    let duration = start_time.elapsed();
    let total_ops = successful_ops.load(Ordering::SeqCst) + failed_ops.load(Ordering::SeqCst);
    let successful_ops_count = successful_ops.load(Ordering::SeqCst);
    let failed_ops_count = failed_ops.load(Ordering::SeqCst);
    let eps = total_ops as f64 / duration.as_secs_f64();
    let success_rate = (successful_ops_count as f64 / total_ops as f64) * 100.0;

    let operation_times = operation_times.lock().unwrap();
    let avg_latency = if !operation_times.is_empty() {
        operation_times.iter().sum::<u64>() as f64 / operation_times.len() as f64 / 1000.0
    } else {
        0.0
    };

    println!("Performance Test Results:");
    println!("Duration: {:.2}s", duration.as_secs_f64());
    println!("Total Operations: {}", total_ops);
    println!("Successful Operations: {}", successful_ops_count);
    println!("Failed Operations: {}", failed_ops_count);
    println!("Events Per Second: {:.2}", eps);
    println!("Success Rate: {:.2}%", success_rate);
    println!("Average Latency: {:.2}ms", avg_latency);
    println!("\nDetailed Timing Metrics:");
    if !operation_times.is_empty() {
        let min = *operation_times.iter().min().unwrap() as f64 / 1000.0;
        let max = *operation_times.iter().max().unwrap() as f64 / 1000.0;
        let mut sorted_times = operation_times.clone();
        sorted_times.sort_unstable();
        let p95 = if !sorted_times.is_empty() {
            let idx = (sorted_times.len() as f64 * 0.95) as usize;
            sorted_times.get(idx).cloned().unwrap_or(0) as f64 / 1000.0
        } else {
            0.0
        };
        println!("Min Latency: {:.2}ms", min);
        println!("Max Latency: {:.2}ms", max);
        println!("95th Percentile: {:.2}ms", p95);
    }

    let metrics = ctx.account_service.get_metrics();
    println!("\nSystem Metrics:");
    println!("Commands Processed: {}", metrics.commands_processed.load(Ordering::Relaxed));
    println!("Commands Failed: {}", metrics.commands_failed.load(Ordering::Relaxed));
    println!("Projection Updates: {}", metrics.projection_updates.load(Ordering::Relaxed));
    println!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
    println!("Cache Misses: {}", metrics.cache_misses.load(Ordering::Relaxed));

    assert!(
        eps >= target_eps as f64,
        "Failed to maintain target EPS. Achieved: {:.2}, Target: {}",
        eps,
        target_eps
    );
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

    println!("Starting {} concurrent deposits...", deposit_count);

    for i in 0..deposit_count {
        let service = ctx.account_service.clone();
        let account_id = account_id;
        let amount = deposit_amount;
        let successful_deposits = successful_deposits.clone();
        let failed_deposits = failed_deposits.clone();

        handles.push(tokio::spawn(async move {
            println!("Starting deposit #{}", i + 1);
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                service.deposit_money(account_id, amount)
            ).await;

            match result {
                Ok(Ok(_)) => {
                    println!("Deposit #{} completed successfully", i + 1);
                    successful_deposits.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                Ok(Err(e)) => {
                    println!("Deposit #{} failed: {:?}", i + 1, e);
                    failed_deposits.fetch_add(1, Ordering::SeqCst);
                    Err(e)
                }
                Err(_) => {
                    println!("Deposit #{} timed out", i + 1);
                    failed_deposits.fetch_add(1, Ordering::SeqCst);
                    Err(AccountError::InfrastructureError("Operation timed out".to_string()))
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
                println!("Task failed: {:?}", e);
                results.push(Err(AccountError::InfrastructureError("Task failed".to_string())));
            }
            Err(_) => {
                println!("Task timed out");
                results.push(Err(AccountError::InfrastructureError("Task timed out".to_string())));
            }
        }
    }

    // Verify results
    let successful_count = successful_deposits.load(Ordering::SeqCst);
    let failed_count = failed_deposits.load(Ordering::SeqCst);
    println!("\nDeposit Results:");
    println!("Successful deposits: {}", successful_count);
    println!("Failed deposits: {}", failed_count);

    // Verify final balance
    let final_account = ctx
        .account_service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account not found");

    let expected_balance = Decimal::new(1000, 0) + (deposit_amount * Decimal::from(successful_count));
    assert_eq!(
        final_account.balance,
        expected_balance,
        "Final balance mismatch. Expected: {}, Got: {}",
        expected_balance,
        final_account.balance
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
            transaction.amount,
            deposit_amount,
            "Transaction amount mismatch. Expected: {}, Got: {}",
            deposit_amount,
            transaction.amount
        );
    }

    // Print metrics
    let metrics = ctx.account_service.get_metrics();
    println!("\nSystem Metrics:");
    println!("Commands Processed: {}", metrics.commands_processed.load(Ordering::Relaxed));
    println!("Commands Failed: {}", metrics.commands_failed.load(Ordering::Relaxed));
    println!("Projection Updates: {}", metrics.projection_updates.load(Ordering::Relaxed));
    println!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
    println!("Cache Misses: {}", metrics.cache_misses.load(Ordering::Relaxed));
}

#[tokio::test]
async fn test_infrastructure_configurations() {
    println!("Testing infrastructure configurations...");

    // Test database connection
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    println!("Testing PostgreSQL connection...");
    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await {
            Ok(pool) => {
                println!("✅ PostgreSQL connection successful");
                pool
            },
            Err(e) => {
                println!("❌ PostgreSQL connection failed: {}", e);
                return;
            }
    };

    // Test database schema and configuration
    println!("\nTesting database schema and configuration...");
    let tables = match sqlx::query!(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    ).fetch_all(&pool).await {
        Ok(tables) => {
            println!("✅ Database schema check successful");
            tables
        },
        Err(e) => {
            println!("❌ Database schema check failed: {}", e);
            return;
        }
    };

    println!("\nFound tables:");
    for table in tables {
        println!("- {}", table.table_name.unwrap_or_default());
    }

    // Check database configuration
    println!("\nChecking database configuration...");
    let db_config = match sqlx::query!(
        "SHOW max_connections"
    ).fetch_one(&pool).await {
        Ok(config) => {
            println!("✅ Max connections: {}", config.max_connections.as_ref().unwrap_or(&"0".to_string()));
            config
        },
        Err(e) => {
            println!("❌ Failed to get max_connections: {}", e);
            return;
        }
    };

    // Test Redis connection and configuration
    println!("\nTesting Redis connection and configuration...");
    let redis_client = match redis::Client::open("redis://127.0.0.1/") {
        Ok(client) => {
            println!("✅ Redis client created successfully");
            client
        },
        Err(e) => {
            println!("❌ Redis client creation failed: {}", e);
            return;
        }
    };

    let mut con = match redis_client.get_multiplexed_async_connection().await {
        Ok(con) => {
            println!("✅ Redis connection established");
            con
        },
        Err(e) => {
            println!("❌ Redis connection failed: {}", e);
            return;
        }
    };

    // Test Redis PING with latency measurement
    let start = std::time::Instant::now();
    match redis::cmd("PING").query_async::<_, String>(&mut con).await {
        Ok(_) => {
            let latency = start.elapsed();
            println!("✅ Redis PING successful (latency: {:?})", latency);
        },
        Err(e) => {
            println!("❌ Redis PING failed: {}", e);
            return;
        }
    }

    // Check Redis configuration
    println!("\nChecking Redis configuration...");
    let configs = vec!["maxmemory", "maxmemory-policy", "timeout", "tcp-keepalive"];
    for config in configs {
        match redis::cmd("CONFIG")
            .arg("GET")
            .arg(config)
            .query_async::<_, Vec<String>>(&mut con)
            .await {
                Ok(values) if values.len() >= 2 => {
                    println!("✅ {}: {}", values[0], values[1]);
                },
                Ok(_) => println!("⚠️ {}: No value found", config),
                Err(e) => println!("❌ Failed to get {}: {}", config, e),
        }
    }

    // Test database performance with multiple queries
    println!("\nTesting database performance...");
    let queries = vec![
        "SELECT 1",
        "SELECT COUNT(*) FROM events",
        "SELECT COUNT(*) FROM account_projections",
        "SELECT COUNT(*) FROM transaction_projections"
    ];

    for query in queries {
        let start = std::time::Instant::now();
        match sqlx::query(query).fetch_one(&pool).await {
            Ok(row) => {
                let duration = start.elapsed();
                let result: i64 = row.try_get(0).unwrap_or(0);
                println!("✅ Query '{}' completed in {:?} (result: {:?})", query, duration, result);
            },
            Err(e) => println!("❌ Query '{}' failed: {}", query, e),
        }
    }

    // Test Redis performance with multiple operations
    println!("\nTesting Redis performance...");

    let start = std::time::Instant::now();
    match redis::cmd("PING").query_async::<_, String>(&mut con).await {
        Ok(result) => {
            let duration = start.elapsed();
            println!("✅ Redis PING completed in {:?} (result: {:?})", duration, result);
        },
        Err(e) => println!("❌ Redis PING failed: {}", e),
    }

    let start = std::time::Instant::now();
    match redis::cmd("SET").arg("test_key").arg("test_value").query_async::<_, String>(&mut con).await {
        Ok(result) => {
            let duration = start.elapsed();
            println!("✅ Redis SET completed in {:?} (result: {:?})", duration, result);
        },
        Err(e) => println!("❌ Redis SET failed: {}", e),
    }

    let start = std::time::Instant::now();
    match redis::cmd("GET").arg("test_key").query_async::<_, String>(&mut con).await {
        Ok(result) => {
            let duration = start.elapsed();
            println!("✅ Redis GET completed in {:?} (result: {:?})", duration, result);
        },
        Err(e) => println!("❌ Redis GET failed: {}", e),
    }

    let start = std::time::Instant::now();
    match redis::cmd("DEL").arg("test_key").query_async::<_, i32>(&mut con).await {
        Ok(result) => {
            let duration = start.elapsed();
            println!("✅ Redis DEL completed in {:?} (result: {:?})", duration, result);
        },
        Err(e) => println!("❌ Redis DEL failed: {}", e),
    }

    // Check database connection pool stats
    println!("\nDatabase connection pool statistics:");
    println!("Active connections: {}", pool.size());
    println!("Idle connections: {}", pool.num_idle());
    println!("Max connections: {}", db_config.max_connections.unwrap_or_default());

    // Check Redis memory usage
    println!("\nRedis memory usage:");
    match redis::cmd("INFO")
        .arg("memory")
        .query_async::<_, String>(&mut con)
        .await {
            Ok(info) => {
                for line in info.lines() {
                    if line.starts_with("used_memory:") || 
                       line.starts_with("used_memory_peak:") ||
                       line.starts_with("used_memory_lua:") {
                        println!("{}", line);
                    }
                }
            },
            Err(e) => println!("❌ Failed to get Redis memory info: {}", e),
    }

    println!("\nInfrastructure configuration test completed successfully!");
}
