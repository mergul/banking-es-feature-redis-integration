use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::domain::{Account, AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use crate::infrastructure::redis_abstraction::{RedisClientTrait, RedisConnectionCommands};
use crate::infrastructure::repository::AccountRepositoryTrait;
use crate::infrastructure::{AccountRepository, EventStore, EventStoreConfig, ProjectionStore};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use redis::RedisError;
use redis::Value as RedisValue;
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Constants for command de-duplication
const COMMAND_DEDUP_KEY_PREFIX: &str = "command_dedup:";
const COMMAND_DEDUP_TTL_SECONDS: usize = 60; // Changed to usize for redis lib

// Service metrics
#[derive(Debug, Default)]
struct ServiceMetrics {
    command_processing_time: std::sync::atomic::AtomicU64,
    commands_processed: std::sync::atomic::AtomicU64,
    commands_failed: std::sync::atomic::AtomicU64,
    projection_updates: std::sync::atomic::AtomicU64,
    projection_errors: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
pub struct AccountService {
    repository: Arc<dyn AccountRepositoryTrait + 'static>,
    projections: ProjectionStore,
    pub redis_client: Arc<dyn RedisClientTrait>, // Made public
    metrics: Arc<ServiceMetrics>,
}

impl AccountService {
    /// Creates a new `AccountService`.
    ///
    /// # Arguments
    ///
    /// * `repository`: The account repository for data access.
    /// * `projections`: The projection store for querying denormalized views.
    /// * `redis_client`: An `Arc` wrapped Redis client trait object, used for command de-duplication.
    pub fn new(
        repository: Arc<dyn AccountRepositoryTrait + 'static>,
        projections: ProjectionStore,
        redis_client: Arc<dyn RedisClientTrait>, // Changed signature
    ) -> Self {
        let service = Self {
            repository,
            projections,
            redis_client, // Now an Arc<dyn RedisClientTrait>
            metrics: Arc::new(ServiceMetrics::default()),
        };

        // Start metrics reporter
        let metrics = service.metrics.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let processed = metrics
                    .commands_processed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let failed = metrics
                    .commands_failed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let avg_processing_time = metrics
                    .command_processing_time
                    .load(std::sync::atomic::Ordering::Relaxed)
                    as f64
                    / 1000.0; // Convert to milliseconds
                let projection_updates = metrics
                    .projection_updates
                    .load(std::sync::atomic::Ordering::Relaxed);
                let projection_errors = metrics
                    .projection_errors
                    .load(std::sync::atomic::Ordering::Relaxed);

                let success_rate = if processed > 0 {
                    ((processed - failed) as f64 / processed as f64) * 100.0
                } else {
                    0.0
                };

                info!(
                    "Account Service Metrics - Commands: {} ({}% success), Avg Processing Time: {:.2}ms, Projection Updates: {}, Errors: {}",
                    processed, success_rate, avg_processing_time, projection_updates, projection_errors
                );
            }
        });

        service
    }

    pub async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let start_time = Instant::now();
        let account_id = Uuid::new_v4();
        let command_id_for_dedup = account_id; // Using account_id as command_id for deduplication for now

        // Check for duplicate command
        match self.is_duplicate_command(command_id_for_dedup).await {
            Ok(true) => {
                warn!(
                    "Duplicate create_account command detected for ID: {}",
                    command_id_for_dedup
                );
                return Err(AccountError::InfrastructureError(
                    "Duplicate create account command".to_string(),
                ));
            }
            Ok(false) => { /* Not a duplicate, proceed */ }
            Err(e) => {
                // Log the error and decide whether to fail open or closed.
                // Failing open (treating as not duplicate) might be risky for financial ops.
                // Failing closed (treating as duplicate or infra error) is safer.
                error!("Redis error during command deduplication for create_account ID {}: {}. Failing operation.", command_id_for_dedup, e);
                return Err(e); // Propagate the infrastructure error
            }
        }

        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name: owner_name.clone(),
            initial_balance,
        };

        let account = Account::default();
        let events = account.handle_command(&command)?;

        // Save events via batching mechanism
        self.repository
            .save_batched(account.id, account.version, events.clone());
        // Save events via batching mechanism
        self.repository
            .save_batched(account.id, account.version, events.clone());
        // Save events via batching mechanism
        self.repository
            .save_batched(account.id, account.version, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        let projection = AccountProjection {
            id: account_id,
            owner_name,
            balance: initial_balance,
            is_active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        if let Err(e) = self
            .projections
            .upsert_accounts_batch(vec![projection])
            .await
        {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(
                "Failed to update projection for account {}: {}",
                account_id, e
            );
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(account_id)
    }

    pub async fn deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = Instant::now();

        let command_id_for_dedup = account_id; // Using account_id as command_id for deduplication for now

        // Check for duplicate command
        match self.is_duplicate_command(command_id_for_dedup).await {
            Ok(true) => {
                warn!(
                    "Duplicate deposit_money command detected for ID: {}",
                    command_id_for_dedup
                );
                return Err(AccountError::InfrastructureError(
                    "Duplicate deposit command".to_string(),
                ));
            }
            Ok(false) => { /* Not a duplicate, proceed */ }
            Err(e) => {
                error!("Redis error during command deduplication for deposit_money ID {}: {}. Failing operation.", command_id_for_dedup, e);
                return Err(e);
            }
        }

        let mut account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::DepositMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        // Save events with retry logic
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        if let Err(e) = self.update_projections_from_events(&events).await {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!("Failed to update projections for deposit: {}", e);
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub async fn withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = Instant::now();

        let command_id_for_dedup = account_id; // Using account_id as command_id for deduplication for now

        // Check for duplicate command
        match self.is_duplicate_command(command_id_for_dedup).await {
            Ok(true) => {
                warn!(
                    "Duplicate withdraw_money command detected for ID: {}",
                    command_id_for_dedup
                );
                return Err(AccountError::InfrastructureError(
                    "Duplicate withdraw command".to_string(),
                ));
            }
            Ok(false) => { /* Not a duplicate, proceed */ }
            Err(e) => {
                error!("Redis error during command deduplication for withdraw_money ID {}: {}. Failing operation.", command_id_for_dedup, e);
                return Err(e);
            }
        }

        let mut account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::WithdrawMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        // Save events with retry logic
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        if let Err(e) = self.update_projections_from_events(&events).await {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!("Failed to update projections for withdrawal: {}", e);
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        self.projections
            .get_account(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>, AccountError> {
        self.projections
            .get_all_accounts()
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, AccountError> {
        self.projections
            .get_account_transactions(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    async fn update_projections_from_events(
        &self,
        events: &[crate::domain::AccountEvent],
    ) -> Result<(), AccountError> {
        use crate::domain::AccountEvent;

        let mut account_updates = Vec::new();
        let mut transaction_updates = Vec::new();

        for event in events {
            match event {
                AccountEvent::MoneyDeposited {
                    account_id,
                    amount,
                    transaction_id,
                } => {
                    // Prepare account balance update
                    if let Some(mut account_proj) = self
                        .projections
                        .get_account(*account_id)
                        .await
                        .map_err(|e| AccountError::InfrastructureError(e.to_string()))?
                    {
                        account_proj.balance += amount;
                        account_proj.updated_at = Utc::now();
                        account_updates.push(account_proj);
                    }

                    // Prepare transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id: *account_id,
                        transaction_type: "deposit".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(),
                    };
                    transaction_updates.push(transaction);
                }
                AccountEvent::MoneyWithdrawn {
                    account_id,
                    amount,
                    transaction_id,
                } => {
                    // Prepare account balance update
                    if let Some(mut account_proj) = self
                        .projections
                        .get_account(*account_id)
                        .await
                        .map_err(|e| AccountError::InfrastructureError(e.to_string()))?
                    {
                        account_proj.balance -= amount;
                        account_proj.updated_at = Utc::now();
                        account_updates.push(account_proj);
                    }

                    // Prepare transaction record
                    let transaction = TransactionProjection {
                        id: *transaction_id,
                        account_id: *account_id,
                        transaction_type: "withdrawal".to_string(),
                        amount: *amount,
                        timestamp: Utc::now(),
                    };
                    transaction_updates.push(transaction);
                }
                _ => {} // Handle other events as needed
            }
        }

        // Batch update projections
        if !account_updates.is_empty() {
            self.projections
                .upsert_accounts_batch(account_updates)
                .await
                .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;
        }

        if !transaction_updates.is_empty() {
            self.projections
                .insert_transactions_batch(transaction_updates)
                .await
                .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;
        }

        Ok(())
    }

    /// Checks if a command with the given ID has already been processed within a defined time window.
    ///
    /// This uses Redis with a SET NX EX command to achieve atomic check-and-set.
    ///
    /// # Arguments
    /// * `command_id`: A unique identifier for the command.
    ///
    /// # Returns
    /// * `Ok(true)` if the command is a duplicate (already processed).
    /// * `Ok(false)` if the command is new.
    /// * `Err(AccountError::InfrastructureError)` if there's an issue with Redis.
    async fn is_duplicate_command(&self, command_id: Uuid) -> Result<bool, AccountError> {
        let key = format!("command:{}", command_id);
        let key_bytes = key.as_bytes();
        let mut con = self
            .redis_client
            .get_async_connection()
            .await
            .map_err(|e| {
                error!(
                    "Failed to get Redis connection for command deduplication: {}",
                    e
                );
                AccountError::InfrastructureError(format!("Redis connection error: {}", e))
            })?;

        // Use the specific trait method for SET NX EX
        match con
            .set_nx_ex_bytes(key_bytes, b"1", COMMAND_DEDUP_TTL_SECONDS as u64)
            .await
        {
            Ok(true) => Ok(false), // Key was set, so command is new (not a duplicate)
            Ok(false) => Ok(true), // Key was not set (already existed), so command is a duplicate
            Err(e) => {
                error!("Redis SET NX EX command failed for key {}: {}", key, e);
                Err(AccountError::InfrastructureError(format!(
                    "Redis SET NX EX error: {}",
                    e
                )))
            }
        }
    }

    pub fn start_background_tasks(&self) {
        // Start cache warming task
        let repository = self.repository.clone();
        let redis_client = self.redis_client.clone();
        tokio::spawn(async move {
            loop {
                // Get hot accounts (e.g., accounts with recent activity)
                let hot_accounts = repository.get_hot_accounts().await.unwrap_or_default();
                if !hot_accounts.is_empty() {
                    if let Err(e) = repository.warm_cache_for_hot_accounts(hot_accounts).await {
                        error!("Failed to warm cache for hot accounts: {}", e);
                    }
                }
                tokio::time::sleep(Duration::from_secs(300)).await; // Run every 5 minutes
            }
        });

        // Start cache consistency checker
        let repository = self.repository.clone();
        tokio::spawn(async move {
            loop {
                // Get all active accounts
                let active_accounts = repository.get_active_accounts().await.unwrap_or_default();
                if !active_accounts.is_empty() {
                    if let Err(e) = repository
                        .check_cache_consistency_bulk(active_accounts)
                        .await
                    {
                        error!("Failed to check cache consistency: {}", e);
                    }
                }
                tokio::time::sleep(Duration::from_secs(600)).await; // Run every 10 minutes
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::AccountError;
    use crate::infrastructure::redis_abstraction::{
        RedisClientTrait, RedisConnectionCommands, RedisPipeline, RedisPoolConfig,
    };
    use crate::infrastructure::repository::AccountRepositoryTrait;
    use crate::infrastructure::{AccountRepository, EventStore, EventStoreConfig, ProjectionStore};
    use async_trait::async_trait;
    use redis::{RedisError, Value as RedisValue};
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use uuid::Uuid;

    // Manual mock for RedisConnectionCommands
    #[derive(Clone)]
    struct ManualMockRedisConnection {
        pub set_nx_ex_result: Arc<Result<bool, RedisError>>,
    }

    #[async_trait]
    impl RedisConnectionCommands for ManualMockRedisConnection {
        async fn get_bytes(&mut self, _key: &[u8]) -> Result<RedisValue, RedisError> {
            Ok(RedisValue::Nil)
        }

        async fn set_ex_bytes(
            &mut self,
            _key: &[u8],
            _value: &[u8],
            _seconds: u64,
        ) -> Result<(), RedisError> {
            Ok(())
        }

        async fn del_bytes(&mut self, _key: &[u8]) -> Result<(), RedisError> {
            Ok(())
        }

        async fn rpush_bytes(&mut self, _key: &[u8], _values: &[&[u8]]) -> Result<(), RedisError> {
            Ok(())
        }

        async fn lrange_bytes(
            &mut self,
            _key: &[u8],
            _start: isize,
            _stop: isize,
        ) -> Result<Vec<RedisValue>, RedisError> {
            Ok(vec![])
        }

        async fn scan_match_bytes(&mut self, _pattern: &[u8]) -> Result<Vec<String>, RedisError> {
            Ok(vec![])
        }

        async fn set_options_bytes(
            &mut self,
            _key: &[u8],
            _value: &[u8],
            _options: redis::SetOptions,
        ) -> Result<Option<String>, RedisError> {
            Ok(None)
        }

        async fn set_nx_ex_bytes(
            &mut self,
            _key: &[u8],
            _value: &[u8],
            _seconds: u64,
        ) -> Result<bool, RedisError> {
            match &*self.set_nx_ex_result {
                Ok(value) => Ok(*value),
                Err(_) => Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Mock Redis error",
                ))),
            }
        }

        async fn execute_pipeline(
            &mut self,
            _pipeline: RedisPipeline,
        ) -> Result<Vec<RedisValue>, RedisError> {
            Ok(vec![])
        }
    }

    // Manual mock for RedisClientTrait
    #[derive(Clone)]
    struct ManualMockRedisClient {
        pub conn: ManualMockRedisConnection,
    }

    #[async_trait]
    impl RedisClientTrait for ManualMockRedisClient {
        async fn get_async_connection(
            &self,
        ) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError> {
            Ok(Box::new(self.conn.clone()))
        }

        fn clone_client(&self) -> Arc<dyn RedisClientTrait> {
            Arc::new(Self {
                conn: self.conn.clone(),
            })
        }

        async fn get_pooled_connection(
            &self,
        ) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError> {
            Ok(Box::new(self.conn.clone()))
        }

        fn get_pool_config(&self) -> RedisPoolConfig {
            RedisPoolConfig::default()
        }
    }

    // Manual mock for AccountRepositoryTrait
    #[derive(Clone)]
    struct ManualMockAccountRepository {
        pub save_batched_result: Arc<Result<(), anyhow::Error>>,
        pub get_by_id_result: Arc<Result<Option<Account>, AccountError>>,
    }

    #[async_trait]
    impl AccountRepositoryTrait for ManualMockAccountRepository {
        async fn save(&self, _account: &Account, _events: Vec<AccountEvent>) -> Result<()> {
            Ok(())
        }
        async fn get_by_id(&self, _id: Uuid) -> Result<Option<Account>, AccountError> {
            match &*self.get_by_id_result {
                Ok(value) => Ok(value.clone()),
                Err(e) => Err((*e).clone()),
            }
        }
        async fn save_batched(
            &self,
            _account_id: Uuid,
            _expected_version: i64,
            _events: Vec<AccountEvent>,
        ) -> Result<()> {
            match &*self.save_batched_result {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!("{}", e)),
            }
        }
        async fn save_immediate(
            &self,
            _account: &Account,
            _events: Vec<AccountEvent>,
        ) -> Result<()> {
            Ok(())
        }
        async fn flush_all(&self) -> Result<()> {
            Ok(())
        }
        fn start_batch_flush_task(&self) {}

        // Add mock implementations for the new methods
        async fn get_hot_accounts(&self) -> Result<Vec<Uuid>> {
            Ok(vec![]) // Mock implementation returns empty list
        }

        async fn get_active_accounts(&self) -> Result<Vec<Uuid>> {
            Ok(vec![]) // Mock implementation returns empty list
        }

        async fn warm_cache_for_hot_accounts(&self, _account_ids: Vec<Uuid>) -> Result<()> {
            Ok(()) // Mock implementation does nothing
        }

        async fn check_cache_consistency_bulk(&self, _account_ids: Vec<Uuid>) -> Result<()> {
            Ok(()) // Mock implementation does nothing
        }
    }

    fn account_service_with_mock_redis(
        mock_redis_client: Arc<dyn RedisClientTrait>,
    ) -> AccountService {
        let mock_repo = Arc::new(ManualMockAccountRepository {
            save_batched_result: Arc::new(Ok(())),
            get_by_id_result: Arc::new(Ok(None)),
        });

        let ps_pool = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(sqlx::PgPool::connect(
                "postgres://postgres:postgres@localhost:5432/banking_test",
            ))
            .expect("Failed to create postgres pool for ProjectionStore test double");
        let projection_store = ProjectionStore::new(ps_pool);

        AccountService::new(mock_repo, projection_store, mock_redis_client)
    }

    #[tokio::test]
    async fn test_is_duplicate_command_new_command() {
        let command_id = Uuid::new_v4();
        let mock_redis_conn = ManualMockRedisConnection {
            set_nx_ex_result: Arc::new(Ok(true)), // true means key was set (command is new)
        };

        let mock_redis_client = ManualMockRedisClient {
            conn: mock_redis_conn,
        };

        let mock_repo = ManualMockAccountRepository {
            save_batched_result: Arc::new(Ok(())),
            get_by_id_result: Arc::new(Ok(None)),
        };

        let ps_pool =
            sqlx::PgPool::connect("postgres://postgres:postgres@localhost:5432/banking_test")
                .await
                .unwrap();
        let projection_store = ProjectionStore::new(ps_pool);

        let service = AccountService::new(
            Arc::new(mock_repo),
            projection_store,
            Arc::new(mock_redis_client),
        );

        let result = service
            .create_account("Test Owner".to_string(), Decimal::new(100, 0))
            .await;
        assert!(
            result.is_ok(),
            "Create account failed when command should be new: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_is_duplicate_command_duplicate_detected() {
        let account_id_for_deposit = Uuid::new_v4();

        let mock_redis_conn = ManualMockRedisConnection {
            set_nx_ex_result: Arc::new(Ok(false)), // false means key was NOT set (it's a duplicate)
        };

        let mock_redis_client = ManualMockRedisClient {
            conn: mock_redis_conn,
        };

        let mock_repo = ManualMockAccountRepository {
            save_batched_result: Arc::new(Ok(())),
            get_by_id_result: Arc::new(Ok(None)),
        };

        let ps_pool =
            sqlx::PgPool::connect("postgres://postgres:postgres@localhost:5432/banking_test")
                .await
                .unwrap();
        let projection_store = ProjectionStore::new(ps_pool);

        let service = AccountService::new(
            Arc::new(mock_repo),
            projection_store,
            Arc::new(mock_redis_client),
        );

        let result = service
            .deposit_money(account_id_for_deposit, Decimal::new(50, 0))
            .await;

        assert!(
            result.is_err(),
            "Deposit money should have failed due to duplicate command"
        );
        if let Err(AccountError::InfrastructureError(msg)) = result {
            assert!(msg.contains("Duplicate deposit command"));
        } else {
            panic!(
                "Expected InfrastructureError with duplicate message, got {:?}",
                result
            );
        }
    }
}
