use crate::application::cqrs::{commands::*, queries::*};
use crate::domain::{AccountCommand, AccountError};
use crate::infrastructure::{
    cache_service::CacheServiceTrait, event_store::EventStoreTrait, kafka_abstraction::KafkaConfig,
    projections::ProjectionStoreTrait, OutboxRepositoryTrait,
};
use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Main CQRS handler that coordinates commands and queries
pub struct CQRSHandler {
    command_bus: CommandBus,
    query_bus: QueryBus,
    semaphore: Arc<Semaphore>,
    metrics: Arc<CQRSMetrics>,
}

impl CQRSHandler {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_service: Arc<dyn CacheServiceTrait>,
        outbox_repository: Arc<dyn OutboxRepositoryTrait>,
        db_pool: Arc<PgPool>,
        kafka_config: Arc<KafkaConfig>,
        max_concurrent_operations: usize,
    ) -> Self {
        let command_bus = CommandBus::new(
            event_store.clone(),
            outbox_repository.clone(),
            db_pool.clone(),
            kafka_config.clone(),
        );
        let query_bus = QueryBus::new(projection_store, cache_service);

        Self {
            command_bus,
            query_bus,
            semaphore: Arc::new(Semaphore::new(max_concurrent_operations)),
            metrics: Arc::new(CQRSMetrics::default()),
        }
    }

    /// Execute a command with rate limiting
    pub async fn execute_command<C, R>(&self, command: C) -> Result<R, AccountError>
    where
        C: Into<AccountCommand>,
        R: From<CommandResult>,
    {
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            error!("Failed to acquire semaphore permit: {}", e);
            AccountError::InfrastructureError("Service overloaded".to_string())
        })?;

        let start_time = Instant::now();
        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.command_bus.execute(command).await;

        match &result {
            Ok(_) => {
                self.metrics
                    .commands_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Err(e) => {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        result
    }

    /// Execute a query with rate limiting
    pub async fn execute_query<Q, R>(&self, query: Q) -> Result<R, AccountError>
    where
        Q: Into<AccountQuery>,
        R: From<QueryResult>,
    {
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            error!("Failed to acquire semaphore permit: {}", e);
            AccountError::InfrastructureError("Service overloaded".to_string())
        })?;

        let start_time = Instant::now();
        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.query_bus.execute(query).await;

        match &result {
            Ok(_) => {
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Err(e) => {
                self.metrics
                    .queries_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        result
    }

    /// Create account command
    pub async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let command = CreateAccountCommand {
            owner_name,
            initial_balance,
        };
        self.execute_command(command).await
    }

    /// Deposit money command
    pub async fn deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let command = DepositMoneyCommand { account_id, amount };
        self.execute_command(command).await
    }

    /// Withdraw money command
    pub async fn withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let command = WithdrawMoneyCommand { account_id, amount };
        self.execute_command(command).await
    }

    /// Close account command
    pub async fn close_account(
        &self,
        account_id: Uuid,
        reason: String,
    ) -> Result<(), AccountError> {
        let command = CloseAccountCommand { account_id, reason };
        self.execute_command(command).await
    }

    /// Get account query
    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<crate::infrastructure::projections::AccountProjection>, AccountError> {
        let query = GetAccountQuery { account_id };
        self.execute_query(query).await
    }

    /// Get all accounts query
    pub async fn get_all_accounts(
        &self,
    ) -> Result<Vec<crate::infrastructure::projections::AccountProjection>, AccountError> {
        let query = AccountQuery::GetAllAccounts;
        self.execute_query(query).await
    }

    /// Get account transactions query
    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<crate::infrastructure::projections::TransactionProjection>, AccountError> {
        let query = GetAccountTransactionsQuery { account_id };
        self.execute_query(query).await
    }

    /// Get account balance query
    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        let query = GetAccountBalanceQuery { account_id };
        self.execute_query(query).await
    }

    /// Check if account is active query
    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        let query = IsAccountActiveQuery { account_id };
        self.execute_query(query).await
    }

    /// Get metrics
    pub fn get_metrics(&self) -> &CQRSMetrics {
        &self.metrics
    }

    /// Get cache metrics
    pub fn get_cache_metrics(&self) -> &crate::infrastructure::cache_service::CacheMetrics {
        self.query_bus.get_cache_metrics()
    }

    /// Health check
    pub async fn health_check(&self) -> Result<CQRSHealth, AccountError> {
        let start_time = Instant::now();

        let health = CQRSHealth {
            status: "healthy".to_string(),
            commands_processed: self
                .metrics
                .commands_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            commands_successful: self
                .metrics
                .commands_successful
                .load(std::sync::atomic::Ordering::Relaxed),
            commands_failed: self
                .metrics
                .commands_failed
                .load(std::sync::atomic::Ordering::Relaxed),
            queries_processed: self
                .metrics
                .queries_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            queries_successful: self
                .metrics
                .queries_successful
                .load(std::sync::atomic::Ordering::Relaxed),
            queries_failed: self
                .metrics
                .queries_failed
                .load(std::sync::atomic::Ordering::Relaxed),
            available_permits: self.semaphore.available_permits(),
            total_permits: 1000, // Fixed value since we can't get total permits from tokio::sync::Semaphore
            uptime: start_time.elapsed(),
        };

        Ok(health)
    }
}

/// CQRS metrics
#[derive(Debug, Default)]
pub struct CQRSMetrics {
    pub commands_processed: std::sync::atomic::AtomicU64,
    pub commands_successful: std::sync::atomic::AtomicU64,
    pub commands_failed: std::sync::atomic::AtomicU64,
    pub queries_processed: std::sync::atomic::AtomicU64,
    pub queries_successful: std::sync::atomic::AtomicU64,
    pub queries_failed: std::sync::atomic::AtomicU64,
}

/// CQRS health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CQRSHealth {
    pub status: String,
    pub commands_processed: u64,
    pub commands_successful: u64,
    pub commands_failed: u64,
    pub queries_processed: u64,
    pub queries_successful: u64,
    pub queries_failed: u64,
    pub available_permits: usize,
    pub total_permits: usize,
    pub uptime: Duration,
}

/// Batch transaction handler for high-throughput operations
pub struct BatchTransactionHandler {
    cqrs_handler: Arc<CQRSHandler>,
    batch_size: usize,
    batch_timeout: Duration,
}

impl BatchTransactionHandler {
    pub fn new(cqrs_handler: Arc<CQRSHandler>, batch_size: usize, batch_timeout: Duration) -> Self {
        Self {
            cqrs_handler,
            batch_size,
            batch_timeout,
        }
    }

    /// Process batch transactions
    pub async fn process_batch(
        &self,
        transactions: Vec<BatchTransaction>,
    ) -> Result<BatchTransactionResult, AccountError> {
        let start_time = Instant::now();
        let mut successful = 0;
        let mut failed = 0;
        let mut errors = Vec::new();

        // Process transactions in batches
        for chunk in transactions.chunks(self.batch_size) {
            let mut handles = Vec::new();

            for transaction in chunk {
                let handler = self.cqrs_handler.clone();
                let transaction = transaction.clone();

                let handle = tokio::spawn(async move {
                    match transaction.transaction_type.as_str() {
                        "deposit" => {
                            handler
                                .deposit_money(transaction.account_id, transaction.amount)
                                .await
                        }
                        "withdraw" => {
                            handler
                                .withdraw_money(transaction.account_id, transaction.amount)
                                .await
                        }
                        _ => Err(AccountError::InfrastructureError(
                            "Invalid transaction type".to_string(),
                        )),
                    }
                });

                handles.push(handle);
            }

            // Wait for batch to complete with timeout
            for handle in handles {
                match tokio::time::timeout(self.batch_timeout, handle).await {
                    Ok(Ok(Ok(_))) => successful += 1,
                    Ok(Ok(Err(e))) => {
                        failed += 1;
                        errors.push(e.to_string());
                    }
                    Ok(Err(e)) => {
                        failed += 1;
                        errors.push("Task failed: {}".to_string() + &(e).to_string());
                    }
                    Err(_) => {
                        failed += 1;
                        errors.push("Transaction timed out".to_string());
                    }
                }
            }
        }

        info!(
            "Batch processing completed: {} successful, {} failed in {:.2}s",
            successful,
            failed,
            start_time.elapsed().as_secs_f64()
        );

        Ok(BatchTransactionResult {
            successful,
            failed,
            errors,
            processing_time: start_time.elapsed(),
        })
    }
}

/// Batch transaction request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchTransaction {
    pub account_id: Uuid,
    pub transaction_type: String,
    pub amount: Decimal,
}

/// Batch transaction result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchTransactionResult {
    pub successful: usize,
    pub failed: usize,
    pub errors: Vec<String>,
    pub processing_time: Duration,
}
