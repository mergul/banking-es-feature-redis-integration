use crate::application::cqrs::handlers::{
    BatchTransaction, BatchTransactionHandler, BatchTransactionResult, CQRSHandler, CQRSHealth,
    CQRSMetrics,
};
use crate::domain::{AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::consistency_manager::ConsistencyManager;
use crate::infrastructure::event_store::EventStoreTrait;
use crate::infrastructure::lock_free_operations::{
    LockFreeConfig, LockFreeOperations, LockFreeOperationsTrait,
};

use crate::infrastructure::projections::{
    AccountProjection, ProjectionStoreTrait, TransactionProjection,
};
use crate::infrastructure::write_batching::save_events_with_retry;
use crate::infrastructure::write_batching::{
    PartitionedBatching, WriteBatchingConfig, WriteBatchingService, WriteOperation,
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
    write_batching_service: Option<Arc<PartitionedBatching>>,
    consistency_manager: Arc<ConsistencyManager>,
    metrics: Arc<CQRSMetrics>,
    enable_write_batching: bool,
    // NEW: Lock-free operations for fast reads
    lock_free_ops: Arc<LockFreeOperations>,
}

impl CQRSAccountService {
    pub async fn new(
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
            // Create CDC outbox repository for outbox batcher
            let outbox_repo = Arc::new(
                crate::infrastructure::cdc_debezium::CDCOutboxRepository::new(
                    event_store.get_partitioned_pools().clone(),
                ),
            )
                as Arc<dyn crate::infrastructure::outbox::OutboxRepositoryTrait>;

            // Create outbox batcher
            let outbox_batcher = crate::infrastructure::cdc_debezium::OutboxBatcher::new_default(
                outbox_repo,
                event_store.get_partitioned_pools().clone(),
            );

            // Create partitioned batching service
            let partitioned_batching = Arc::new(
                PartitionedBatching::new(
                    event_store.clone(),
                    projection_store.clone(),
                    event_store.get_partitioned_pools().write_pool_arc(),
                    outbox_batcher,
                )
                .await,
            );
            Some(partitioned_batching)
        } else {
            None
        };

        // Use provided consistency manager or create a default one
        let consistency_manager =
            consistency_manager.unwrap_or_else(|| Arc::new(ConsistencyManager::default()));

        // Initialize lock-free operations
        let lock_free_ops = Arc::new(LockFreeOperations::new(
            event_store.clone(),
            projection_store.clone(),
            LockFreeConfig::default(),
        ));

        Self {
            cqrs_handler,
            batch_handler,
            write_batching_service,
            consistency_manager,
            metrics,
            enable_write_batching,
            lock_free_ops,
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

    /// Get the write batching service for direct access
    pub fn get_write_batching_service(&self) -> Option<&Arc<PartitionedBatching>> {
        self.write_batching_service.as_ref()
    }

    /// Create a new account
    pub async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        self.create_account_with_auth_user(Uuid::nil(), owner_name, initial_balance)
            .await
    }

    pub async fn create_account_with_auth_user(
        &self,
        auth_user_id: Uuid,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let start_time = std::time::Instant::now();
        info!(
            "Creating account for auth_user_id: {} with owner_name: {} and initial balance: {}",
            auth_user_id, owner_name, initial_balance
        );

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Use write batching if available, otherwise fall back to direct handler
        let result = if let Some(ref batching_service) = self.write_batching_service {
            // For now, use owner_name as the identifier since auth_user_id is not yet integrated
            // TODO: Integrate auth_user_id properly in the future
            let operation = WriteOperation::CreateAccount {
                account_id: Uuid::new_v4(),
                owner_name: owner_name.clone(),
                initial_balance,
            };

            let aggregate_id = operation.get_aggregate_id();
            match batching_service
                .submit_operation(aggregate_id, operation)
                .await
            {
                Ok(operation_id) => {
                    // Wait for the operation to complete and get the actual account ID
                    // Use a reasonable timeout that matches the test expectations
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        batching_service.wait_for_result(operation_id),
                    )
                    .await
                    {
                        Ok(result) => {
                            match result {
                                Ok(result) => {
                                    if result.success {
                                        match result.result {
                                            Some(account_id) => {
                                                // OPTIMIZED: Create accounts don't need consistency manager confirmation
                                                // The account is guaranteed to exist in the event store
                                                Ok(account_id)
                                            }
                                            None => Err(AccountError::InfrastructureError(
                                                "Account creation succeeded but no account ID returned"
                                                    .to_string(),
                                            )),
                                        }
                                    } else {
                                        Err(AccountError::InfrastructureError(
                                            result
                                                .error
                                                .unwrap_or_else(|| "Unknown error".to_string()),
                                        ))
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to wait for account creation result: {}", e);
                                    // Fall back to direct handler (no consistency manager needed for create)
                                    self.cqrs_handler
                                        .create_account(owner_name.clone(), initial_balance)
                                        .await
                                }
                            }
                        }
                        Err(_) => {
                            error!("Timeout waiting for account creation result after 30 seconds");
                            // Fall back to direct handler (no consistency manager needed for create)
                            self.cqrs_handler
                                .create_account(owner_name.clone(), initial_balance)
                                .await
                        }
                    }
                }
                Err(e) => {
                    error!("Write batching failed for account creation: {}", e);
                    // Fall back to direct handler (no consistency manager needed for create)
                    self.cqrs_handler
                        .create_account(owner_name.clone(), initial_balance)
                        .await
                }
            }
        } else {
            // Direct handler (no consistency manager needed for create)
            self.cqrs_handler
                .create_account(owner_name.clone(), initial_balance)
                .await
        };

        let duration = start_time.elapsed();
        match &result {
            Ok(_) => {
                self.metrics
                    .commands_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!("✅ Account created successfully in {:?}", duration);
            }
            Err(_) => {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!("❌ Account creation failed in {:?}", duration);
            }
        }

        result
    }

    /// Phase 1: Bulk create accounts using create-specific batch method
    /// This method handles aggregate_id collisions by using a dedicated create partition
    /// and optimized batch processing for bulk operations
    pub async fn create_accounts_batch(
        &self,
        accounts: Vec<(String, Decimal)>,
    ) -> Result<Vec<Uuid>, AccountError> {
        let start_time = std::time::Instant::now();
        let accounts_count = accounts.len();
        info!(
            "🚀 Starting bulk account creation for {} accounts",
            accounts_count
        );

        self.metrics
            .commands_processed
            .fetch_add(accounts_count as u64, std::sync::atomic::Ordering::Relaxed);

        // Use create-specific batch method if available, otherwise fall back to individual creates
        let result = if let Some(ref batching_service) = self.write_batching_service {
            match batching_service
                .submit_create_operations_batch(accounts.clone())
                .await
            {
                Ok(account_ids) => {
                    // OPTIMIZED: Create accounts don't need consistency manager confirmation
                    // The accounts are guaranteed to exist in the event store
                    info!(
                        "✅ Batch account creation completed for {} accounts (no consistency wait needed)",
                        account_ids.len()
                    );

                    // Return immediately - accounts are created and will be processed by CDC asynchronously
                    Ok(account_ids)
                }
                Err(e) => {
                    error!("Bulk account creation failed: {}", e);
                    Err(AccountError::InfrastructureError(format!(
                        "Bulk account creation failed: {}",
                        e
                    )))
                }
            }
        } else {
            // Fall back to individual processing if write batching is not enabled
            error!("Write batching service not available for bulk operations");
            self.create_accounts_individual(accounts).await
        };

        let duration = start_time.elapsed();
        match &result {
            Ok(account_ids) => {
                self.metrics.commands_successful.fetch_add(
                    account_ids.len() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                info!(
                    "✅ Bulk account creation completed successfully: {} accounts in {:?}",
                    account_ids.len(),
                    duration
                );
            }
            Err(_) => {
                self.metrics
                    .commands_failed
                    .fetch_add(accounts_count as u64, std::sync::atomic::Ordering::Relaxed);
                error!("❌ Bulk account creation failed in {:?}", duration);
            }
        }

        result
    }

    /// Fallback method for individual account creation when bulk method is not available
    async fn create_accounts_individual(
        &self,
        accounts: Vec<(String, Decimal)>,
    ) -> Result<Vec<Uuid>, AccountError> {
        let mut account_ids = Vec::with_capacity(accounts.len());
        let mut failed_accounts = Vec::new();
        let total_accounts = accounts.len();

        for (i, (owner_name, balance)) in accounts.into_iter().enumerate() {
            match self.create_account(owner_name, balance).await {
                Ok(account_id) => {
                    account_ids.push(account_id);
                    if (i + 1) % 10 == 0 {
                        info!(
                            "📊 Individual create progress: {}/{} accounts created",
                            i + 1,
                            total_accounts
                        );
                    }
                }
                Err(e) => {
                    failed_accounts.push((i, e.clone()));
                    error!("❌ Failed to create account {}: {:?}", i, e);
                }
            }
        }

        if !failed_accounts.is_empty() {
            warn!(
                "⚠️ Some individual account creations failed: {:?}",
                failed_accounts
            );
        }

        Ok(account_ids)
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

        // Use write batching if available, otherwise fall back to direct handler
        let result = if let Some(ref batching_service) = self.write_batching_service {
            let operation = WriteOperation::DepositMoney { account_id, amount };
            let aggregate_id = operation.get_aggregate_id();
            match batching_service
                .submit_operation(aggregate_id, operation)
                .await
            {
                Ok(operation_id) => {
                    // Wait for the operation to complete with timeout
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        batching_service.wait_for_result(operation_id),
                    )
                    .await
                    {
                        Ok(result) => {
                            match result {
                                Ok(result) => {
                                    if result.success {
                                        Ok(())
                                    } else {
                                        Err(AccountError::InfrastructureError(
                                            result
                                                .error
                                                .unwrap_or_else(|| "Unknown error".to_string()),
                                        ))
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to wait for deposit result: {}", e);
                                    // Fall back to direct handler
                                    self.cqrs_handler.deposit_money(account_id, amount).await
                                }
                            }
                        }
                        Err(_) => {
                            error!("Timeout waiting for deposit result after 30 seconds");
                            // Fall back to direct handler
                            self.cqrs_handler.deposit_money(account_id, amount).await
                        }
                    }
                }
                Err(e) => {
                    error!("Write batching failed for deposit: {}", e);
                    // Fall back to direct handler
                    self.cqrs_handler.deposit_money(account_id, amount).await
                }
            }
        } else {
            self.cqrs_handler.deposit_money(account_id, amount).await
        };

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

        // Use write batching if available, otherwise fall back to direct handler
        let result = if let Some(ref batching_service) = self.write_batching_service {
            let operation = WriteOperation::WithdrawMoney { account_id, amount };
            let aggregate_id = operation.get_aggregate_id();

            match batching_service
                .submit_operation(aggregate_id, operation)
                .await
            {
                Ok(operation_id) => {
                    // Wait for the operation to complete with timeout
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        batching_service.wait_for_result(operation_id),
                    )
                    .await
                    {
                        Ok(result) => {
                            match result {
                                Ok(result) => {
                                    if result.success {
                                        Ok(())
                                    } else {
                                        Err(AccountError::InfrastructureError(
                                            result
                                                .error
                                                .unwrap_or_else(|| "Unknown error".to_string()),
                                        ))
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to wait for withdraw result: {}", e);
                                    // Fall back to direct handler
                                    self.cqrs_handler.withdraw_money(account_id, amount).await
                                }
                            }
                        }
                        Err(_) => {
                            error!("Timeout waiting for withdraw result after 30 seconds");
                            // Fall back to direct handler
                            self.cqrs_handler.withdraw_money(account_id, amount).await
                        }
                    }
                }
                Err(e) => {
                    error!("Write batching failed for withdraw: {}", e);
                    // Fall back to direct handler
                    self.cqrs_handler.withdraw_money(account_id, amount).await
                }
            }
        } else {
            self.cqrs_handler.withdraw_money(account_id, amount).await
        };

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

    /// Wait for batch consistency for multiple accounts without individual operations
    /// This is useful for external batch operations that need to wait for consistency
    pub async fn wait_for_batch_consistency(
        &self,
        account_ids: Vec<Uuid>,
    ) -> Result<(), AccountError> {
        info!(
            "📦 Waiting for batch consistency for {} accounts",
            account_ids.len()
        );

        // Wait for batch CDC consistency
        match self
            .consistency_manager
            .wait_for_consistency_batch(account_ids.clone())
            .await
        {
            Ok(_) => {
                info!(
                    "✅ Batch CDC consistency completed for {} accounts",
                    account_ids.len()
                );

                // Wait for batch projection sync
                match self
                    .consistency_manager
                    .wait_for_projection_sync_batch(account_ids.clone())
                    .await
                {
                    Ok(_) => {
                        info!(
                            "✅ Batch projection sync completed for {} accounts",
                            account_ids.len()
                        );
                        Ok(())
                    }
                    Err(e) => {
                        error!("❌ Batch projection sync failed: {}", e);
                        Err(AccountError::InfrastructureError(format!(
                            "Batch projection sync failed: {}",
                            e
                        )))
                    }
                }
            }
            Err(e) => {
                error!("❌ Batch CDC consistency failed: {}", e);
                Err(AccountError::InfrastructureError(format!(
                    "Batch CDC consistency failed: {}",
                    e
                )))
            }
        }
    }

    /// Get account by ID with optimized consistency
    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        println!(
            "[DEBUG] CQRSAccountService::get_account: start for {}",
            account_id
        );

        // OPTIMIZED: Try lock-free read first (fast path)
        let fast_result = self
            .lock_free_ops
            .get_account_projection(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        // If we got data, check if we need consistency
        if let Some(projection) = &fast_result {
            // Check if this account has pending writes (needs consistency)
            let has_pending_writes = self
                .consistency_manager
                .has_pending_writes(account_id)
                .await;

            if !has_pending_writes {
                // No pending writes, fast path is safe
                println!(
                    "[DEBUG] CQRSAccountService::get_account: fast path for {}",
                    account_id
                );
                return Ok(fast_result);
            } else {
                // Has pending writes, need to wait for consistency
                println!(
                    "[DEBUG] CQRSAccountService::get_account: consistency path for {}",
                    account_id
                );
                let cm_result = self
                    .consistency_manager
                    .wait_for_projection_sync(account_id)
                    .await;

                if let Err(e) = cm_result {
                    warn!(
                        "Projection sync wait failed for account {}: {}",
                        account_id, e
                    );
                    // Return fast result anyway (eventual consistency)
                }

                // Re-read after consistency wait
                return self
                    .lock_free_ops
                    .get_account_projection(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()));
            }
        }

        // No data found, return None
        Ok(fast_result)
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

    /// Get account balance with optimized consistency
    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Querying balance for account {}", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // OPTIMIZED: Try lock-free read first (fast path)
        let fast_result = self
            .lock_free_ops
            .get_account_projection(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        let duration = start_time.elapsed();

        // If we got data, check if we need consistency
        if let Some(projection) = &fast_result {
            // Check if this account has pending writes (needs consistency)
            let has_pending_writes = self
                .consistency_manager
                .has_pending_writes(account_id)
                .await;

            if !has_pending_writes {
                // No pending writes, fast path is safe
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Successfully retrieved balance {} for account {} in {:?} (fast path)",
                    projection.balance, account_id, duration
                );
                return Ok(projection.balance);
            } else {
                // Has pending writes, need to wait for consistency
                info!(
                    "Account {} has pending writes, waiting for consistency",
                    account_id
                );
                let cm_result = self
                    .consistency_manager
                    .wait_for_projection_sync(account_id)
                    .await;

                if let Err(e) = cm_result {
                    warn!(
                        "Projection sync wait failed for account {}: {}",
                        account_id, e
                    );
                    // Return fast result anyway (eventual consistency)
                }

                // Re-read after consistency wait
                let consistent_result = self
                    .lock_free_ops
                    .get_account_projection(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                match consistent_result {
                    Some(projection) => {
                        self.metrics
                            .queries_successful
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        info!(
                            "Successfully retrieved balance {} for account {} in {:?} (consistency path)",
                            projection.balance, account_id, start_time.elapsed()
                        );
                        Ok(projection.balance)
                    }
                    None => {
                        self.metrics
                            .queries_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        error!(
                            "Account not found: {} (took {:?})",
                            account_id,
                            start_time.elapsed()
                        );
                        Err(AccountError::NotFound)
                    }
                }
            }
        } else {
            // No data found
            self.metrics
                .queries_failed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            error!("Account not found: {} (took {:?})", account_id, duration);
            Err(AccountError::NotFound)
        }
    }

    /// Check if account is active with optimized consistency
    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Checking if account {} is active", account_id);

        self.metrics
            .queries_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // OPTIMIZED: Try lock-free read first (fast path)
        let fast_result = self
            .lock_free_ops
            .get_account_projection(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        let duration = start_time.elapsed();

        // If we got data, check if we need consistency
        if let Some(projection) = &fast_result {
            // Check if this account has pending writes (needs consistency)
            let has_pending_writes = self
                .consistency_manager
                .has_pending_writes(account_id)
                .await;

            if !has_pending_writes {
                // No pending writes, fast path is safe
                self.metrics
                    .queries_successful
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "Account {} is {} in {:?} (fast path)",
                    account_id,
                    if projection.is_active {
                        "active"
                    } else {
                        "inactive"
                    },
                    duration
                );
                return Ok(projection.is_active);
            } else {
                // Has pending writes, need to wait for consistency
                info!(
                    "Account {} has pending writes, waiting for consistency",
                    account_id
                );
                let cm_result = self
                    .consistency_manager
                    .wait_for_projection_sync(account_id)
                    .await;

                if let Err(e) = cm_result {
                    warn!(
                        "Projection sync wait failed for account {}: {}",
                        account_id, e
                    );
                    // Return fast result anyway (eventual consistency)
                }

                // Re-read after consistency wait
                let consistent_result = self
                    .lock_free_ops
                    .get_account_projection(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                match consistent_result {
                    Some(projection) => {
                        self.metrics
                            .queries_successful
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        info!(
                            "Account {} is {} in {:?} (consistency path)",
                            account_id,
                            if projection.is_active {
                                "active"
                            } else {
                                "inactive"
                            },
                            start_time.elapsed()
                        );
                        Ok(projection.is_active)
                    }
                    None => {
                        self.metrics
                            .queries_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        error!(
                            "Account not found: {} (took {:?})",
                            account_id,
                            start_time.elapsed()
                        );
                        Err(AccountError::NotFound)
                    }
                }
            }
        } else {
            // No data found
            self.metrics
                .queries_failed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            error!("Account not found: {} (took {:?})", account_id, duration);
            Err(AccountError::NotFound)
        }
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

    /// Submit a batch of events for a single account in one transaction
    pub async fn submit_events_batch(
        &self,
        account_id: Uuid,
        events: Vec<AccountEvent>,
        _expected_version: i64, // No longer used
    ) -> Result<(), AccountError> {
        println!(
            "[DEBUG] submit_events_batch: account_id={:?}, events={:?}",
            account_id, events
        );
        let max_retries = 5;
        let result = save_events_with_retry(
            &self.cqrs_handler.event_store(),
            account_id,
            events,
            max_retries,
        )
        .await;
        println!(
            "[DEBUG] submit_events_batch: completed for account_id={:?} with result={:?}",
            account_id, result
        );
        result.map_err(|e| AccountError::InfrastructureError(format!("Event store error: {:?}", e)))
    }

    /// Phase 2: True batch processing for write operations
    /// This method handles aggregate_id and version collisions by using the new batch processing
    /// capabilities of the write batching service
    pub async fn submit_events_batch_bulk(
        &self,
        operations: Vec<(Uuid, Vec<AccountEvent>)>, // (aggregate_id, events)
    ) -> Result<Vec<Uuid>, AccountError> {
        let start_time = std::time::Instant::now();
        let operations_count = operations.len();

        info!(
            "🚀 Starting bulk event submission for {} operations",
            operations_count
        );

        self.metrics.commands_processed.fetch_add(
            operations_count as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Use the new batch processing method if available
        let result = if let Some(ref batching_service) = self.write_batching_service {
            match batching_service
                .submit_write_operations_batch(operations)
                .await
            {
                Ok(account_ids) => {
                    // OPTIMIZED: Use batch waiting with partial success handling
                    info!(
                        "📦 Waiting for batch CDC consistency for {} operations",
                        account_ids.len()
                    );

                    // Mark all accounts as pending CDC processing in batch
                    for account_id in &account_ids {
                        self.consistency_manager.mark_pending(*account_id).await;
                        self.consistency_manager
                            .mark_projection_pending(*account_id)
                            .await;
                    }

                    // OPTIMIZED: Wait for batch consistency with partial success
                    let cdc_result = self
                        .consistency_manager
                        .wait_for_consistency_batch(account_ids.clone())
                        .await;

                    match cdc_result {
                        Ok(_) => {
                            info!(
                                "✅ Batch CDC consistency completed for {} operations",
                                account_ids.len()
                            );

                            // OPTIMIZED: Wait for batch projection sync with partial success
                            let projection_result = self
                                .consistency_manager
                                .wait_for_projection_sync_batch(account_ids.clone())
                                .await;

                            match projection_result {
                                Ok(_) => {
                                    info!(
                                        "✅ Batch projection sync completed for {} operations",
                                        account_ids.len()
                                    );
                                    Ok(account_ids)
                                }
                                Err(e) => {
                                    warn!("⚠️ Batch projection sync failed: {}, but operations were processed successfully", e);
                                    // Return partial success - operations were processed even if projections failed
                                    Ok(account_ids)
                                }
                            }
                        }
                        Err(e) => {
                            warn!("⚠️ Batch CDC consistency failed: {}, but operations were processed successfully", e);
                            // Return partial success - operations were processed even if CDC failed
                            Ok(account_ids)
                        }
                    }
                }
                Err(e) => {
                    error!("Bulk write operation failed: {}", e);
                    Err(AccountError::InfrastructureError(format!(
                        "Bulk write operation failed: {}",
                        e
                    )))
                }
            }
        } else {
            // Fall back to individual processing if write batching is not enabled
            error!("Write batching service not available for bulk operations");
            Err(AccountError::InfrastructureError(
                "Write batching service not available".to_string(),
            ))
        };

        let duration = start_time.elapsed();
        match &result {
            Ok(account_ids) => {
                self.metrics.commands_successful.fetch_add(
                    account_ids.len() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                info!(
                    "✅ Bulk event submission completed successfully: {} operations in {:?}",
                    account_ids.len(),
                    duration
                );
            }
            Err(_) => {
                self.metrics.commands_failed.fetch_add(
                    operations_count as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                error!("❌ Bulk event submission failed in {:?}", duration);
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
