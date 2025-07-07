use crate::application::cqrs::handlers::{
    BatchTransaction, BatchTransactionHandler, BatchTransactionResult, CQRSHandler, CQRSHealth,
    CQRSMetrics,
};
use crate::domain::{AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::event_store::EventStoreTrait;
use crate::infrastructure::projections::{
    AccountProjection, ProjectionStoreTrait, TransactionProjection,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

/// CQRS-based account service that separates commands and queries
pub struct CQRSAccountService {
    cqrs_handler: Arc<CQRSHandler>,
    batch_handler: Arc<BatchTransactionHandler>,
    metrics: Arc<CQRSMetrics>,
}

impl CQRSAccountService {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_service: Arc<dyn CacheServiceTrait>,
        outbox_repository: Arc<dyn crate::infrastructure::OutboxRepositoryTrait>,
        db_pool: Arc<sqlx::PgPool>,
        kafka_config: Arc<crate::infrastructure::kafka_abstraction::KafkaConfig>,
        max_concurrent_operations: usize,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        let cqrs_handler = Arc::new(CQRSHandler::new(
            event_store,
            projection_store,
            cache_service,
            outbox_repository,
            db_pool,
            kafka_config,
            max_concurrent_operations,
        ));

        let batch_handler = Arc::new(BatchTransactionHandler::new(
            cqrs_handler.clone(),
            batch_size,
            batch_timeout,
        ));

        let metrics = Arc::new(CQRSMetrics::default());

        Self {
            cqrs_handler,
            batch_handler,
            metrics,
        }
    }

    /// Create a new account
    pub async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let start_time = std::time::Instant::now();
        info!(
            "Creating account for owner: {} with initial balance: {}",
            owner_name, initial_balance
        );

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self
            .cqrs_handler
            .create_account(owner_name.clone(), initial_balance)
            .await;

        let duration = start_time.elapsed();
        match &result {
            Ok(account_id) => {
                self.metrics
                    .commands_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully created account {} for owner {} in {:?}",
                    account_id, owner_name, duration
                );
            }
            Err(e) => {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to create account for owner {}: {} (took {:?})",
                    owner_name, e, duration
                );
            }
        }

        result
    }

    /// Deposit money into an account
    pub async fn deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = std::time::Instant::now();
        info!("Depositing {} into account {}", amount, account_id);

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.cqrs_handler.deposit_money(account_id, amount).await;

        let duration = start_time.elapsed();
        match &result {
            Ok(_) => {
                self.metrics
                    .commands_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully deposited {} into account {} in {:?}",
                    amount, account_id, duration
                );
            }
            Err(e) => {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to deposit {} into account {}: {} (took {:?})",
                    amount, account_id, e, duration
                );
            }
        }

        result
    }

    /// Withdraw money from an account
    pub async fn withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = std::time::Instant::now();
        info!("Withdrawing {} from account {}", amount, account_id);

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.cqrs_handler.withdraw_money(account_id, amount).await;

        let duration = start_time.elapsed();
        match &result {
            Ok(_) => {
                self.metrics
                    .commands_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully withdrew {} from account {} in {:?}",
                    amount, account_id, duration
                );
            }
            Err(e) => {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to withdraw {} from account {}: {} (took {:?})",
                    amount, account_id, e, duration
                );
            }
        }

        result
    }

    /// Close an account
    pub async fn close_account(
        &self,
        account_id: Uuid,
        reason: String,
    ) -> Result<(), AccountError> {
        let start_time = std::time::Instant::now();
        info!("Closing account {} with reason: {}", account_id, reason);

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self
            .cqrs_handler
            .close_account(account_id, reason.clone())
            .await;

        let duration = start_time.elapsed();
        match &result {
            Ok(_) => {
                self.metrics
                    .commands_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully closed account {} with reason '{}' in {:?}",
                    account_id, reason, duration
                );
            }
            Err(e) => {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to close account {} with reason '{}': {} (took {:?})",
                    account_id, reason, e, duration
                );
            }
        }

        result
    }

    /// Get account by ID
    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Querying account {}", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.cqrs_handler.get_account(account_id).await;

        let duration = start_time.elapsed();
        match &result {
            Ok(Some(_)) => {
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully retrieved account {} in {:?}",
                    account_id, duration
                );
            }
            Ok(None) => {
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!("Account {} not found in {:?}", account_id, duration);
            }
            Err(e) => {
                self.metrics
                    .queries_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to query account {}: {} (took {:?})",
                    account_id, e, duration
                );
            }
        }

        result
    }

    /// Get all accounts
    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Querying all accounts");

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.cqrs_handler.get_all_accounts().await;

        let duration = start_time.elapsed();
        match &result {
            Ok(accounts) => {
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully retrieved {} accounts in {:?}",
                    accounts.len(),
                    duration
                );
            }
            Err(e) => {
                self.metrics
                    .queries_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!("Failed to query all accounts: {} (took {:?})", e, duration);
            }
        }

        result
    }

    /// Get account transactions
    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Querying transactions for account {}", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.cqrs_handler.get_account_transactions(account_id).await;

        let duration = start_time.elapsed();
        match &result {
            Ok(transactions) => {
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully retrieved {} transactions for account {} in {:?}",
                    transactions.len(),
                    account_id,
                    duration
                );
            }
            Err(e) => {
                self.metrics
                    .queries_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to query transactions for account {}: {} (took {:?})",
                    account_id, e, duration
                );
            }
        }

        result
    }

    /// Get account balance
    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Querying balance for account {}", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.cqrs_handler.get_account_balance(account_id).await;

        let duration = start_time.elapsed();
        match &result {
            Ok(balance) => {
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully retrieved balance {} for account {} in {:?}",
                    balance, account_id, duration
                );
            }
            Err(e) => {
                self.metrics
                    .queries_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to query balance for account {}: {} (took {:?})",
                    account_id, e, duration
                );
            }
        }

        result
    }

    /// Check if account is active
    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Checking if account {} is active", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.cqrs_handler.is_account_active(account_id).await;

        let duration = start_time.elapsed();
        match &result {
            Ok(is_active) => {
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Account {} is {} in {:?}",
                    account_id,
                    if *is_active { "active" } else { "inactive" },
                    duration
                );
            }
            Err(e) => {
                self.metrics
                    .queries_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to check if account {} is active: {} (took {:?})",
                    account_id, e, duration
                );
            }
        }

        result
    }

    /// Process batch transactions
    pub async fn batch_transactions(
        &self,
        transactions: Vec<BatchTransaction>,
    ) -> Result<BatchTransactionResult, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Processing batch of {} transactions", transactions.len());

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = self.batch_handler.process_batch(transactions.clone()).await;

        let duration = start_time.elapsed();
        match &result {
            Ok(batch_result) => {
                self.metrics
                    .commands_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully processed batch: {} successful, {} failed in {:?}",
                    batch_result.successful, batch_result.failed, duration
                );
            }
            Err(e) => {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Failed to process batch of {} transactions: {} (took {:?})",
                    transactions.len(),
                    e,
                    duration
                );
            }
        }

        result
    }

    /// Get service metrics
    pub fn get_metrics(&self) -> &CQRSMetrics {
        self.cqrs_handler.get_metrics()
    }

    /// Get cache metrics
    pub fn get_cache_metrics(&self) -> &crate::infrastructure::cache_service::CacheMetrics {
        self.cqrs_handler.get_cache_metrics()
    }

    /// Health check
    pub async fn health_check(&self) -> Result<CQRSHealth, AccountError> {
        self.cqrs_handler.health_check().await
    }
}

// API Request/Response DTOs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAccountRequest {
    pub owner_name: String,
    pub initial_balance: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAccountResponse {
    pub account_id: Uuid,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRequest {
    pub amount: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountsResponse {
    pub accounts: Vec<AccountProjection>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountResponse {
    pub account: Option<AccountProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchTransactionRequest {
    pub transactions: Vec<BatchTransaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchTransactionResponse {
    pub result: BatchTransactionResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}
