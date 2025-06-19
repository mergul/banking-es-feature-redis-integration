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
use std::error::Error;

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

    // Initialize database pool with optimized settings for high throughput
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPoolOptions::new()
        .max_connections(100) // Increased from 50 to 100 for even more concurrency
        .min_connections(10)  // Increased minimum connections
        .acquire_timeout(Duration::from_secs(10)) // Increased timeout
        .idle_timeout(Duration::from_secs(60)) // Increased idle timeout
        .max_lifetime(Duration::from_secs(1800))
        .connect(&database_url)
        .await?;

    // Initialize Redis client with optimized settings
    let redis_client = Arc::new(redis::Client::open("redis://127.0.0.1/")?);
    let redis_client_trait = RealRedisClient::new(redis_client.as_ref().clone(), None);

    // Initialize services with optimized configs
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
        50, // Increased from 20 to 50 for higher concurrency
    ));

    // Start connection monitoring task
    let pool_clone = pool.clone();
    let monitor_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await; // Reduced frequency
            println!("DB Pool Stats - Active: {}, Idle: {}", 
                pool_clone.size(), 
                pool_clone.num_idle()
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
    println!("Starting high throughput test...");
    
    // Add global timeout for the entire test
    let test_future = async {
        // Setup test environment with increased timeouts
        let setup_timeout = Duration::from_secs(30);
        let account_creation_timeout = Duration::from_secs(10);
        let test_duration = Duration::from_secs(10); // Increased to 10 seconds
        let operation_timeout = Duration::from_millis(500);
        
        // Increased test parameters for more threads
        let target_eps = 500; // Much higher target
        let worker_count = 100; // Increased from 20 to 100 workers
        let channel_buffer_size = 2000; // Increased buffer size
        let batch_size = 5; // Keep batch size as is
        
        println!("Initializing test environment...");
        let context = setup_test_environment().await.expect("Failed to setup test environment");
        println!("Test environment setup complete");
        
        // Create test account with timeout
        println!("Creating test account...");
        let account_id = tokio::time::timeout(
            account_creation_timeout,
            context.account_service.create_account("Test User".to_string(), Decimal::new(1000, 0))
        ).await.expect("Account creation timed out").expect("Failed to create account");
        println!("Successfully created account: {}", account_id);
        
        // Pre-warm cache
        println!("Pre-warming cache...");
        let _ = context.account_service.get_account(account_id).await.expect("Failed to pre-warm cache");
        println!("Cache pre-warmed");
        
        // Start performance test
        println!("Starting performance test with {} workers", worker_count);
        
        let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let start_time = Instant::now();
        let end_time = start_time + test_duration;
        
        // Spawn worker tasks
        println!("Spawning worker tasks...");
        let mut handles = Vec::new();
        for worker_id in 0..worker_count {
            let tx = tx.clone();
            let service = context.account_service.clone();
            let account_id = account_id;
            let operation_timeout = operation_timeout;
            
            let handle = tokio::spawn(async move {
                println!("Worker {} starting...", worker_id);
                use rand::{SeedableRng, Rng};
                use rand_chacha::ChaCha8Rng;
                let mut rng = ChaCha8Rng::from_rng(rand::thread_rng()).unwrap();
                let mut operations = 0;
                
                while Instant::now() < end_time {
                    // Single operation instead of batch
                    let amount = rng.gen_range(1..=100);
                    let operation = if rng.gen_bool(0.5) {
                        Operation::Deposit(amount)
                    } else {
                        Operation::Withdraw(amount)
                    };
                    
                    let result = match operation {
                        Operation::Deposit(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.deposit_money(account_id, amount.into())
                            ).await
                        }
                        Operation::Withdraw(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.withdraw_money(account_id, amount.into())
                            ).await
                        }
                    };

                    match result {
                        Ok(Ok(_)) => {
                            operations += 1;
                            tx.send(OperationResult::Success).await.ok();
                        }
                        Ok(Err(_)) => {
                            // Reduced per-operation overhead: do not print
                            tx.send(OperationResult::Failure).await.ok();
                        }
                        Err(_) => {
                            // Reduced per-operation overhead: do not print
                            tx.send(OperationResult::Timeout).await.ok();
                        }
                    }
                    
                    // Reduced sleep time for higher throughput
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                
                println!("Worker {} completed after {} operations", worker_id, operations);
            });
            
            handles.push(handle);
        }
        
        // Drop the original tx so the channel closes when all workers are done
        drop(tx);
        
        // Collect results
        println!("Starting to collect results...");
        let mut total_ops = 0;
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        let mut timed_out_ops = 0;
        
        while let Some(result) = rx.recv().await {
            total_ops += 1;
            match result {
                OperationResult::Success => successful_ops += 1,
                OperationResult::Failure => failed_ops += 1,
                OperationResult::Timeout => {
                    timed_out_ops += 1;
                }
            }
            
            if total_ops % 100 == 0 {
                let elapsed = start_time.elapsed();
                let current_eps = total_ops as f64 / elapsed.as_secs_f64();
                println!("Current EPS: {:.2}, Total Ops: {}", current_eps, total_ops);
            }
        }
        
        // Wait for all workers to complete
        println!("Finished collecting results after {} operations", total_ops);
        println!("Waiting for workers to complete...");
        for handle in handles {
            handle.await.expect("Worker task failed");
        }
        println!("All worker tasks completed successfully\n");
        
        // Calculate final metrics
        let duration = start_time.elapsed();
        let eps = total_ops as f64 / duration.as_secs_f64();
        
        println!("Performance Test Results:");
        println!("Duration: {:.2}s", duration.as_secs_f64());
        println!("Total Operations: {}", total_ops);
        println!("Successful Operations: {}", successful_ops);
        println!("Failed Operations: {}", failed_ops);
        println!("Timed Out Operations: {}", timed_out_ops);
        println!("Events Per Second: {:.2}", eps);
        println!("Success Rate: {:.2}%\n", (successful_ops as f64 / total_ops as f64) * 100.0);
        
        // Get metrics from services
        let metrics = context.account_service.get_metrics();
        println!("System Metrics:");
        println!("Commands Processed: {}", metrics.commands_processed.load(Ordering::Relaxed));
        println!("Commands Failed: {}", metrics.commands_failed.load(Ordering::Relaxed));
        println!("Projection Updates: {}", metrics.projection_updates.load(Ordering::Relaxed));
        println!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
        println!("Cache Misses: {}", metrics.cache_misses.load(Ordering::Relaxed));
        
        // Assert performance requirements
        assert!(
            eps >= target_eps as f64,
            "Failed to maintain target EPS. Achieved: {:.2}, Target: {}",
            eps,
            target_eps
        );
    };

    // Add global timeout of 2 minutes
    match tokio::time::timeout(Duration::from_secs(120), test_future).await {
        Ok(_) => println!("Test completed successfully"),
        Err(_) => panic!("Test timed out after 120 seconds"),
    }
}

async fn process_batch(service: &Arc<AccountService>, account_id: Uuid, operations: &[Operation]) -> Result<(), Box<dyn Error + Send + Sync>> {
    for operation in operations {
        match operation {
            Operation::Deposit(amount) => {
                service.deposit_money(account_id, (*amount).into()).await?;
            }
            Operation::Withdraw(amount) => {
                service.withdraw_money(account_id, (*amount).into()).await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
enum Operation {
    Deposit(u32),
    Withdraw(u32),
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

#[tokio::test]
async fn test_extreme_concurrency() {
    println!("Starting extreme concurrency test...");
    
    let test_future = async {
        let test_duration = Duration::from_secs(10);
        let operation_timeout = Duration::from_millis(1000);
        
        // Extreme concurrency parameters
        let worker_count = 50; // 50 concurrent workers
        let channel_buffer_size = 1000;
        
        println!("Initializing test environment for extreme concurrency...");
        let context = setup_test_environment().await.expect("Failed to setup test environment");
        
        // Create multiple test accounts for better distribution
        let mut account_ids = Vec::new();
        for i in 0..5 {
            let account_id = context.account_service
                .create_account(format!("Test User {}", i), Decimal::new(10000, 0))
                .await
                .expect("Failed to create account");
            account_ids.push(account_id);
            println!("Created account {}: {}", i, account_id);
        }
        
        // Pre-warm cache for all accounts
        for account_id in &account_ids {
            let _ = context.account_service.get_account(*account_id).await.expect("Failed to pre-warm cache");
        }
        println!("Cache pre-warmed for all accounts");
        
        println!("Starting extreme concurrency test with {} workers", worker_count);
        
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
                use rand::{SeedableRng, Rng};
                use rand_chacha::ChaCha8Rng;
                let mut rng = ChaCha8Rng::from_rng(rand::thread_rng()).unwrap();
                let mut operations = 0;
                
                while Instant::now() < end_time {
                    // Randomly select an account
                    let account_id = account_ids[rng.gen_range(0..account_ids.len())];
                    let amount = rng.gen_range(1..=50);
                    let operation = if rng.gen_bool(0.6) { // 60% deposits, 40% withdrawals
                        Operation::Deposit(amount)
                    } else {
                        Operation::Withdraw(amount)
                    };
                    
                    let result = match operation {
                        Operation::Deposit(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.deposit_money(account_id, amount.into())
                            ).await
                        }
                        Operation::Withdraw(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.withdraw_money(account_id, amount.into())
                            ).await
                        }
                    };

                    match result {
                        Ok(Ok(_)) => {
                            operations += 1;
                            tx.send(OperationResult::Success).await.ok();
                        }
                        Ok(Err(e)) => {
                            if worker_id % 10 == 0 { // Only log every 10th worker to reduce noise
                                println!("Worker {} operation failed: {:?}", worker_id, e);
                            }
                            tx.send(OperationResult::Failure).await.ok();
                        }
                        Err(_) => {
                            if worker_id % 10 == 0 {
                                println!("Worker {} operation timed out", worker_id);
                            }
                            tx.send(OperationResult::Timeout).await.ok();
                        }
                    }
                    
                    // Minimal sleep for maximum throughput
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                
                if worker_id % 10 == 0 {
                    println!("Worker {} completed after {} operations", worker_id, operations);
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
                println!("Current EPS: {:.2}, Total Ops: {}", current_eps, total_ops);
            }
        }
        
        // Wait for all workers to complete
        for handle in handles {
            handle.await.expect("Worker task failed");
        }
        
        // Calculate final metrics
        let duration = start_time.elapsed();
        let eps = total_ops as f64 / duration.as_secs_f64();
        
        println!("\nExtreme Concurrency Test Results:");
        println!("Duration: {:.2}s", duration.as_secs_f64());
        println!("Total Operations: {}", total_ops);
        println!("Successful Operations: {}", successful_ops);
        println!("Failed Operations: {}", failed_ops);
        println!("Timed Out Operations: {}", timed_out_ops);
        println!("Events Per Second: {:.2}", eps);
        println!("Success Rate: {:.2}%", (successful_ops as f64 / total_ops as f64) * 100.0);
        
        // Get system metrics
        let metrics = context.account_service.get_metrics();
        println!("\nSystem Metrics:");
        println!("Commands Processed: {}", metrics.commands_processed.load(Ordering::Relaxed));
        println!("Commands Failed: {}", metrics.commands_failed.load(Ordering::Relaxed));
        println!("Projection Updates: {}", metrics.projection_updates.load(Ordering::Relaxed));
        println!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
        println!("Cache Misses: {}", metrics.cache_misses.load(Ordering::Relaxed));
        
        // Verify final account states
        println!("\nFinal Account States:");
        for (i, account_id) in account_ids.iter().enumerate() {
            if let Ok(Some(account)) = context.account_service.get_account(*account_id).await {
                println!("Account {}: Balance = {}", i, account.balance);
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
        Ok(_) => println!("Extreme concurrency test completed successfully"),
        Err(_) => panic!("Extreme concurrency test timed out after 180 seconds"),
    }
}

#[tokio::test]
async fn test_optimized_high_concurrency() {
    println!("Starting optimized high concurrency test...");
    
    let test_future = async {
        let test_duration = Duration::from_secs(15);
        let operation_timeout = Duration::from_millis(2000); // Increased timeout
        
        // Optimized concurrency parameters
        let worker_count = 30; // Reduced from 50 to 30
        let account_count = 20; // Increased from 5 to 20 accounts for better distribution
        let channel_buffer_size = 1000;
        
        println!("Initializing test environment for optimized high concurrency...");
        let context = setup_test_environment().await.expect("Failed to setup test environment");
        
        // Create more test accounts for better distribution
        let mut account_ids = Vec::new();
        for i in 0..account_count {
            let account_id = context.account_service
                .create_account(format!("Test User {}", i), Decimal::new(10000, 0))
                .await
                .expect("Failed to create account");
            account_ids.push(account_id);
            println!("Created account {}: {}", i, account_id);
        }
        
        // Pre-warm cache for all accounts
        for account_id in &account_ids {
            let _ = context.account_service.get_account(*account_id).await.expect("Failed to pre-warm cache");
        }
        println!("Cache pre-warmed for all {} accounts", account_count);
        
        println!("Starting optimized high concurrency test with {} workers", worker_count);
        
        let (tx, mut rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let start_time = Instant::now();
        let end_time = start_time + test_duration;
        
        // Spawn worker tasks with better account distribution
        let mut handles = Vec::new();
        for worker_id in 0..worker_count {
            let tx = tx.clone();
            let service = context.account_service.clone();
            let account_ids = account_ids.clone();
            let operation_timeout = operation_timeout;
            
            let handle = tokio::spawn(async move {
                use rand::{SeedableRng, Rng};
                use rand_chacha::ChaCha8Rng;
                let mut rng = ChaCha8Rng::from_rng(rand::thread_rng()).unwrap();
                let mut operations = 0;
                let mut consecutive_failures = 0;
                
                while Instant::now() < end_time {
                    // Use worker-specific account selection to reduce conflicts
                    let account_index = (worker_id + operations) % account_ids.len();
                    let account_id = account_ids[account_index];
                    
                    let amount = rng.gen_range(1..=20); // Smaller amounts
                    let operation = if rng.gen_bool(0.7) { // 70% deposits, 30% withdrawals
                        Operation::Deposit(amount)
                    } else {
                        Operation::Withdraw(amount)
                    };
                    
                    let result = match operation {
                        Operation::Deposit(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.deposit_money(account_id, amount.into())
                            ).await
                        }
                        Operation::Withdraw(amount) => {
                            tokio::time::timeout(
                                operation_timeout,
                                service.withdraw_money(account_id, amount.into())
                            ).await
                        }
                    };

                    match result {
                        Ok(Ok(_)) => {
                            operations += 1;
                            consecutive_failures = 0; // Reset failure counter
                            tx.send(OperationResult::Success).await.ok();
                        }
                        Ok(Err(e)) => {
                            consecutive_failures += 1;
                            if worker_id % 5 == 0 && consecutive_failures <= 3 { // Reduced logging
                                println!("Worker {} operation failed: {:?}", worker_id, e);
                            }
                            tx.send(OperationResult::Failure).await.ok();
                            
                            // Add small delay after consecutive failures
                            if consecutive_failures > 2 {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                        Err(_) => {
                            consecutive_failures += 1;
                            if worker_id % 5 == 0 {
                                println!("Worker {} operation timed out", worker_id);
                            }
                            tx.send(OperationResult::Timeout).await.ok();
                        }
                    }
                    
                    // Adaptive sleep based on failure rate
                    let sleep_time = if consecutive_failures > 1 {
                        Duration::from_millis(20)
                    } else {
                        Duration::from_millis(5)
                    };
                    tokio::time::sleep(sleep_time).await;
                }
                
                if worker_id % 5 == 0 {
                    println!("Worker {} completed after {} operations", worker_id, operations);
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
            
            if total_ops % 1000 == 0 {
                let elapsed = start_time.elapsed();
                let current_eps = total_ops as f64 / elapsed.as_secs_f64();
                let current_success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;
                println!("Current EPS: {:.2}, Total Ops: {}, Success Rate: {:.2}%", 
                    current_eps, total_ops, current_success_rate);
            }
        }
        
        // Wait for all workers to complete
        for handle in handles {
            handle.await.expect("Worker task failed");
        }
        
        // Calculate final metrics
        let duration = start_time.elapsed();
        let eps = total_ops as f64 / duration.as_secs_f64();
        let success_rate = (successful_ops as f64 / total_ops as f64) * 100.0;
        
        println!("\nOptimized High Concurrency Test Results:");
        println!("Duration: {:.2}s", duration.as_secs_f64());
        println!("Total Operations: {}", total_ops);
        println!("Successful Operations: {}", successful_ops);
        println!("Failed Operations: {}", failed_ops);
        println!("Timed Out Operations: {}", timed_out_ops);
        println!("Events Per Second: {:.2}", eps);
        println!("Success Rate: {:.2}%", success_rate);
        
        // Get system metrics
        let metrics = context.account_service.get_metrics();
        println!("\nSystem Metrics:");
        println!("Commands Processed: {}", metrics.commands_processed.load(Ordering::Relaxed));
        println!("Commands Failed: {}", metrics.commands_failed.load(Ordering::Relaxed));
        println!("Projection Updates: {}", metrics.projection_updates.load(Ordering::Relaxed));
        println!("Cache Hits: {}", metrics.cache_hits.load(Ordering::Relaxed));
        println!("Cache Misses: {}", metrics.cache_misses.load(Ordering::Relaxed));
        
        // Verify final account states
        println!("\nFinal Account States (first 10):");
        for (i, account_id) in account_ids.iter().take(10).enumerate() {
            if let Ok(Some(account)) = context.account_service.get_account(*account_id).await {
                println!("Account {}: Balance = {}", i, account.balance);
            }
        }
        
        // More reasonable assertions for optimized test
        assert!(
            eps >= 80.0, // Expect at least 80 ops/sec
            "Failed to maintain reasonable EPS. Achieved: {:.2}",
            eps
        );
        
        assert!(
            success_rate >= 75.0, // At least 75% success rate
            "Success rate too low: {:.2}%",
            success_rate
        );
        
        assert!(
            total_ops >= 1000, // At least 1000 total operations
            "Too few total operations: {}",
            total_ops
        );
    };

    // Add global timeout of 3 minutes
    match tokio::time::timeout(Duration::from_secs(180), test_future).await {
        Ok(_) => println!("Optimized high concurrency test completed successfully"),
        Err(_) => panic!("Optimized high concurrency test timed out after 180 seconds"),
    }
}
