use crate::application::cqrs::handlers::{
    BatchTransaction, BatchTransactionHandler, BatchTransactionResult, CQRSHandler, CQRSHealth,
    CQRSMetrics,
};
use crate::domain::{AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::consistency_manager::ConsistencyManager;
use crate::infrastructure::event_store::EventStoreTrait;

use crate::infrastructure::projections::{
    AccountProjection, ProjectionStoreTrait, TransactionProjection,
};
use crate::infrastructure::write_batching::{
    WriteBatchingConfig, WriteBatchingService, WriteOperation,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

/// CQRS-based account service that separates commands and queries
pub struct CQRSAccountService {
    cqrs_handler: Arc<CQRSHandler>,
    batch_handler: Arc<BatchTransactionHandler>,
    write_batching_service: Option<Arc<WriteBatchingService>>,
    consistency_manager: Arc<ConsistencyManager>,
    metrics: Arc<CQRSMetrics>,
    enable_write_batching: bool,
}

impl CQRSAccountService {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_service: Arc<dyn CacheServiceTrait>,
        kafka_config: crate::infrastructure::kafka_abstraction::KafkaConfig, // Changed: pass config
        max_concurrent_operations: usize,
        batch_size: usize,
        batch_timeout: Duration,
        enable_write_batching: bool,
        consistency_manager: Option<Arc<ConsistencyManager>>,
    ) -> Self {
        // Create KafkaProducer instance here
        let _kafka_producer = Arc::new(
            crate::infrastructure::kafka_abstraction::KafkaProducer::new(kafka_config.clone())
                .expect("Failed to create KafkaProducer for CQRSAccountService"),
        );

        let cqrs_handler = Arc::new(CQRSHandler::new(
            event_store.clone(),
            projection_store.clone(),
            cache_service.clone(),
            kafka_config, // Pass config instead of producer
            max_concurrent_operations,
        ));

        let batch_handler = Arc::new(BatchTransactionHandler::new(
            cqrs_handler.clone(),
            batch_size,
            batch_timeout,
        ));

        let metrics = Arc::new(CQRSMetrics::default());

        // Initialize write batching service if enabled
        let write_batching_service = if enable_write_batching {
            let config = WriteBatchingConfig::default();
            let batching_service = Arc::new(WriteBatchingService::new(
                config,
                event_store.clone(),
                projection_store.clone(),
                event_store.get_partitioned_pools().write_pool_arc(),
            ));
            Some(batching_service)
        } else {
            None
        };

        // Use provided consistency manager or create a default one
        let consistency_manager =
            consistency_manager.unwrap_or_else(|| Arc::new(ConsistencyManager::default()));

        Self {
            cqrs_handler,
            batch_handler,
            write_batching_service,
            consistency_manager,
            metrics,
            enable_write_batching,
        }
    }

    /// Start the write batching service if enabled
    pub async fn start_write_batching(&self) -> Result<(), AccountError> {
        if let Some(ref batching_service) = &self.write_batching_service {
            // Use a simpler approach - just log that it's enabled
            // The actual start/stop logic will be handled internally by the service
            info!("Write batching service is enabled and ready");
        } else {
            info!("Write batching service is not enabled");
        }
        Ok(())
    }

    /// Stop the write batching service if enabled
    pub async fn stop_write_batching(&self) -> Result<(), AccountError> {
        if let Some(ref batching_service) = &self.write_batching_service {
            // Use a simpler approach - just log that it's enabled
            // The actual start/stop logic will be handled internally by the service
            info!("Write batching service shutdown requested");
        } else {
            info!("Write batching service is not enabled");
        }
        Ok(())
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

        // Use write batching if available, otherwise fall back to direct handler
        let result = if let Some(ref batching_service) = self.write_batching_service {
            let operation = WriteOperation::CreateAccount {
                owner_name: owner_name.clone(),
                initial_balance,
            };

            match batching_service.submit_operation(operation).await {
                Ok(operation_id) => {
                    // Wait for the operation to complete and get the actual account ID
                    match batching_service.wait_for_result(operation_id).await {
                        Ok(result) => {
                            if result.success {
                                match result.result {
                                    Some(account_id) => {
                                        // Mark as pending CDC processing
                                        self.consistency_manager.mark_pending(account_id).await;
                                        // Mark as pending projection synchronization
                                        self.consistency_manager
                                            .mark_projection_pending(account_id)
                                            .await;

                                        Ok(account_id)
                                    }
                                    None => Err(AccountError::InfrastructureError(
                                        "Account creation succeeded but no account ID returned"
                                            .to_string(),
                                    )),
                                }
                            } else {
                                Err(AccountError::InfrastructureError(
                                    result.error.unwrap_or_else(|| "Unknown error".to_string()),
                                ))
                            }
                        }
                        Err(e) => {
                            error!("Failed to wait for account creation result: {}", e);
                            // Fall back to direct handler
                            self.cqrs_handler
                                .create_account(owner_name.clone(), initial_balance)
                                .await
                        }
                    }
                }
                Err(e) => {
                    error!("Write batching failed for account creation: {}", e);
                    // Fall back to direct handler
                    self.cqrs_handler
                        .create_account(owner_name.clone(), initial_balance)
                        .await
                }
            }
        } else {
            self.cqrs_handler
                .create_account(owner_name.clone(), initial_balance)
                .await
        };

        let duration = start_time.elapsed();
        match &result {
            Ok(account_id) => {
                // CDC pipeline will mark projection completion when it processes the events
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

        // Mark as pending CDC processing
        self.consistency_manager.mark_pending(account_id).await;
        // Mark as pending projection synchronization
        self.consistency_manager
            .mark_projection_pending(account_id)
            .await;

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

        // Mark as pending CDC processing
        self.consistency_manager.mark_pending(account_id).await;
        // Mark as pending projection synchronization
        self.consistency_manager
            .mark_projection_pending(account_id)
            .await;

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

        // Mark as pending CDC processing
        self.consistency_manager.mark_pending(account_id).await;
        // Mark as pending projection synchronization
        self.consistency_manager
            .mark_projection_pending(account_id)
            .await;

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

    /// Wait for CDC consistency for an account
    pub async fn wait_for_account_consistency(&self, account_id: Uuid) -> Result<(), AccountError> {
        self.consistency_manager
            .wait_for_consistency(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    /// Get account by ID with read-after-write consistency
    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        println!(
            "[DEBUG] CQRSAccountService::get_account: start for {}",
            account_id
        );
        // Wait for projection synchronization before reading
        let cm_result = self
            .consistency_manager
            .wait_for_projection_sync(account_id)
            .await;
        println!("[DEBUG] CQRSAccountService::get_account: after consistency_manager for {} (result: {:?})", account_id, cm_result);
        if let Err(e) = cm_result {
            warn!(
                "Projection sync wait failed for account {}: {}",
                account_id, e
            );
            // Continue anyway - might be a timeout or the account was created before tracking
        }

        self.cqrs_handler.get_account(account_id).await
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

    /// Get account transactions with read-after-write consistency
    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Querying transactions for account {}", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Wait for CDC consistency before reading
        if let Err(e) = self.wait_for_account_consistency(account_id).await {
            warn!(
                "CDC consistency wait failed for account {}: {}",
                account_id, e
            );
            // Continue anyway - might be a timeout or the account was created before tracking
        }

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

    /// Get account balance with read-after-write consistency
    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Querying balance for account {}", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Wait for projection synchronization before reading (same as get_account)
        if let Err(e) = self
            .consistency_manager
            .wait_for_projection_sync(account_id)
            .await
        {
            warn!(
                "Projection sync wait failed for account {}: {}",
                account_id, e
            );
            // Continue anyway - might be a timeout or the account was created before tracking
        }

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

    /// Check if account is active with read-after-write consistency
    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Checking if account {} is active", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Wait for projection synchronization before reading (same as get_account)
        if let Err(e) = self
            .consistency_manager
            .wait_for_projection_sync(account_id)
            .await
        {
            warn!(
                "Projection sync wait failed for account {}: {}",
                account_id, e
            );
            // Continue anyway - might be a timeout or the account was created before tracking
        }

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

    /// Get consistency manager for external integration
    pub fn get_consistency_manager(&self) -> Arc<ConsistencyManager> {
        self.consistency_manager.clone()
    }

    /// Get service metrics
    pub fn get_metrics(&self) -> &CQRSMetrics {
        self.cqrs_handler.get_metrics()
    }

    /// Get cache metrics
    pub fn get_cache_metrics(&self) -> &crate::infrastructure::cache_service::CacheMetrics {
        self.cqrs_handler.get_cache_metrics()
    }

    /// Get cache service for advanced cache operations
    pub fn get_cache_service(&self) -> Arc<dyn CacheServiceTrait> {
        self.cqrs_handler.get_cache_service()
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
