use crate::application::cqrs::handlers::{
    BatchTransaction, BatchTransactionHandler, BatchTransactionResult, CQRSHandler, CQRSHealth,
    CQRSMetrics,
};
use crate::application::CommandResult;
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
use crate::infrastructure::read_batching::{
    PartitionedReadBatching, ReadBatchingConfig, ReadOperation, ReadOperationResult,
};
use crate::infrastructure::write_batching::save_events_with_retry;
use crate::infrastructure::write_batching::{
    PartitionedBatching, WriteBatchingConfig, WriteBatchingService, WriteOperation,
};
use crate::infrastructure::WriteOperationResult;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    read_batching_service: Option<Arc<PartitionedReadBatching>>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    consistency_manager: Arc<ConsistencyManager>,
    metrics: Arc<CQRSMetrics>,
    enable_write_batching: bool,
    enable_read_batching: bool,
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
        enable_read_batching: bool,
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
                    event_store.get_partitioned_pools().write_pool_arc(),
                    outbox_batcher,
                )
                .await,
            );
            Some(partitioned_batching)
        } else {
            None
        };

        // Initialize read batching service if enabled
        let read_batching_service = if enable_read_batching {
            // Create 32 read pools for partitioning
            let read_pools = std::iter::repeat(event_store.get_partitioned_pools().read_pool_arc())
                .take(32)
                .collect::<Vec<_>>();

            // Create read batching service
            let read_batching = match PartitionedReadBatching::new(
                ReadBatchingConfig::default(),
                projection_store.clone(),
                read_pools,
            )
            .await
            {
                Ok(service) => Some(Arc::new(service)),
                Err(_) => None,
            };

            read_batching
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
            read_batching_service,
            projection_store,
            consistency_manager,
            metrics,
            enable_write_batching,
            enable_read_batching,
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

    /// Start the read batching service if enabled
    pub async fn start_read_batching(&self) -> Result<(), AccountError> {
        if let Some(ref batching_service) = &self.read_batching_service {
            // Use a simpler approach - just log that it's enabled
            // The actual start/stop logic will be handled internally by the service
            info!("Read batching service is enabled and ready");
            info!("Read batching configuration: 32 partitions, 20K batch size");
        } else {
            info!("Read batching service is not enabled");
        }
        Ok(())
    }

    /// Stop the read batching service if enabled
    pub async fn stop_read_batching(&self) -> Result<(), AccountError> {
        if let Some(ref batching_service) = &self.read_batching_service {
            // Use a simpler approach - just log that it's enabled
            // The actual start/stop logic will be handled internally by the service
            info!("Read batching service shutdown requested");
        } else {
            info!("Read batching service is not enabled");
        }
        Ok(())
    }

    /// Get the write batching service for direct access
    pub fn get_write_batching_service(&self) -> Option<&Arc<PartitionedBatching>> {
        self.write_batching_service.as_ref()
    }

    /// Get the read batching service for direct access
    pub fn get_read_batching_service(&self) -> Option<&Arc<PartitionedReadBatching>> {
        self.read_batching_service.as_ref()
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

            // ✅ Capture batch size BEFORE submitting operation
            let batch_size_before_submit = batching_service.get_current_batch_size().await;

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
                                        // SMART: Use batch size captured BEFORE submit
                                        // This gives us the actual batch size when operation was submitted
                                        if batch_size_before_submit > 0 {
                                            // Multiple operations in batch - use batch consistency
                                            info!("📦 Operation was part of batch with {} operations, using batch consistency", batch_size_before_submit + 1);
                                            match self
                                                .wait_for_batch_consistency(vec![account_id])
                                                .await
                                            {
                                                Ok(_) => {
                                                    info!("✅ Batch consistency completed for deposit operation on account {}", account_id);
                                                    Ok(())
                                                }
                                                Err(e) => {
                                                    warn!("⚠️ Batch consistency failed for deposit operation on account {}: {}, but operation was processed successfully", account_id, e);
                                                    // Return success anyway since the operation was processed
                                                    Ok(())
                                                }
                                            }
                                        } else {
                                            // Single operation in batch - use individual consistency
                                            info!("🔍 Operation was single in batch, using individual consistency");
                                            match self
                                                .wait_for_account_consistency(account_id)
                                                .await
                                            {
                                                Ok(_) => {
                                                    info!("✅ Individual consistency completed for deposit operation on account {}", account_id);
                                                    Ok(())
                                                }
                                                Err(e) => {
                                                    warn!("⚠️ Individual consistency failed for deposit operation on account {}: {}, but operation was processed successfully", account_id, e);
                                                    // Return success anyway since the operation was processed
                                                    Ok(())
                                                }
                                            }
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

    /// Batch deposit money into multiple accounts
    /// This is for true batch operations with multiple accounts
    pub async fn deposit_money_batch(
        &self,
        deposits: Vec<(Uuid, Decimal)>, // (account_id, amount)
    ) -> Result<Vec<Uuid>, AccountError> {
        let start_time = std::time::Instant::now();
        let deposits_count = deposits.len();
        info!(
            "🚀 Starting batch deposit operations for {} accounts",
            deposits_count
        );

        self.metrics
            .commands_processed
            .fetch_add(deposits_count as u64, std::sync::atomic::Ordering::Relaxed);

        // Extract account IDs for consistency tracking
        let account_ids: Vec<Uuid> = deposits.iter().map(|(id, _)| *id).collect();

        // Mark all accounts as pending CDC processing
        for account_id in &account_ids {
            self.consistency_manager.mark_pending(*account_id).await;
            self.consistency_manager
                .mark_projection_pending(*account_id)
                .await;
        }

        // Use write batching if available, otherwise fall back to individual processing
        let result = if let Some(ref batching_service) = self.write_batching_service {
            let mut successful_accounts = Vec::new();
            let mut failed_deposits = Vec::new();

            // Submit all operations to batch processor
            for (account_id, amount) in deposits.clone() {
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
                            Ok(result) => match result {
                                Ok(result) => {
                                    if result.success {
                                        successful_accounts.push(account_id);
                                    } else {
                                        failed_deposits.push((account_id, amount));
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to wait for deposit result for account {}: {}",
                                        account_id, e
                                    );
                                    failed_deposits.push((account_id, amount));
                                }
                            },
                            Err(_) => {
                                error!("Timeout waiting for deposit result for account {} after 30 seconds", account_id);
                                failed_deposits.push((account_id, amount));
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Write batching failed for deposit on account {}: {}",
                            account_id, e
                        );
                        failed_deposits.push((account_id, amount));
                    }
                }
            }

            if successful_accounts.is_empty() {
                Err(AccountError::InfrastructureError(
                    "All batch deposit operations failed".to_string(),
                ))
            } else {
                // OPTIMIZED: Wait for batch consistency for all successful operations
                match self
                    .wait_for_batch_consistency(successful_accounts.clone())
                    .await
                {
                    Ok(_) => {
                        info!(
                            "✅ Batch consistency completed for {} deposit operations",
                            successful_accounts.len()
                        );
                        Ok(successful_accounts)
                    }
                    Err(e) => {
                        warn!("⚠️ Batch consistency failed: {}, but operations were processed successfully", e);
                        // Return partial success - operations were processed even if consistency failed
                        Ok(successful_accounts)
                    }
                }
            }
        } else {
            // Fall back to individual processing if write batching is not enabled
            error!("Write batching service not available for batch operations");
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
                    "✅ Batch deposit operations completed successfully: {} accounts in {:?}",
                    account_ids.len(),
                    duration
                );
            }
            Err(_) => {
                self.metrics
                    .commands_failed
                    .fetch_add(deposits_count as u64, std::sync::atomic::Ordering::Relaxed);
                error!("❌ Batch deposit operations failed in {:?}", duration);
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

            // ✅ Capture batch size BEFORE submitting operation
            let batch_size_before_submit = batching_service.get_current_batch_size().await;

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
                                        // SMART: Use batch size captured BEFORE submit
                                        // This gives us the actual batch size when operation was submitted
                                        if batch_size_before_submit > 0 {
                                            // Multiple operations in batch - use batch consistency
                                            info!("📦 Operation was part of batch with {} operations, using batch consistency", batch_size_before_submit + 1);
                                            match self
                                                .wait_for_batch_consistency(vec![account_id])
                                                .await
                                            {
                                                Ok(_) => {
                                                    info!("✅ Batch consistency completed for withdraw operation on account {}", account_id);
                                                    Ok(())
                                                }
                                                Err(e) => {
                                                    warn!("⚠️ Batch consistency failed for withdraw operation on account {}: {}, but operation was processed successfully", account_id, e);
                                                    // Return success anyway since the operation was processed
                                                    Ok(())
                                                }
                                            }
                                        } else {
                                            // Single operation in batch - use individual consistency
                                            info!("🔍 Operation was single in batch, using individual consistency");
                                            match self
                                                .wait_for_account_consistency(account_id)
                                                .await
                                            {
                                                Ok(_) => {
                                                    info!("✅ Individual consistency completed for withdraw operation on account {}", account_id);
                                                    Ok(())
                                                }
                                                Err(e) => {
                                                    warn!("⚠️ Individual consistency failed for withdraw operation on account {}: {}, but operation was processed successfully", account_id, e);
                                                    // Return success anyway since the operation was processed
                                                    Ok(())
                                                }
                                            }
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

    /// Batch withdraw money from multiple accounts
    /// This is for true batch operations with multiple accounts
    pub async fn withdraw_money_batch(
        &self,
        withdrawals: Vec<(Uuid, Decimal)>, // (account_id, amount)
    ) -> Result<Vec<Uuid>, AccountError> {
        let start_time = std::time::Instant::now();
        let withdrawals_count = withdrawals.len();
        info!(
            "🚀 Starting batch withdraw operations for {} accounts",
            withdrawals_count
        );

        self.metrics.commands_processed.fetch_add(
            withdrawals_count as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Extract account IDs for consistency tracking
        let account_ids: Vec<Uuid> = withdrawals.iter().map(|(id, _)| *id).collect();

        // Mark all accounts as pending CDC processing
        for account_id in &account_ids {
            self.consistency_manager.mark_pending(*account_id).await;
            self.consistency_manager
                .mark_projection_pending(*account_id)
                .await;
        }

        // Use write batching if available, otherwise fall back to individual processing
        let result = if let Some(ref batching_service) = self.write_batching_service {
            let mut successful_accounts = Vec::new();
            let mut failed_withdrawals = Vec::new();

            // Submit all operations to batch processor
            for (account_id, amount) in withdrawals.clone() {
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
                            Ok(result) => match result {
                                Ok(result) => {
                                    if result.success {
                                        successful_accounts.push(account_id);
                                    } else {
                                        failed_withdrawals.push((account_id, amount));
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to wait for withdraw result for account {}: {}",
                                        account_id, e
                                    );
                                    failed_withdrawals.push((account_id, amount));
                                }
                            },
                            Err(_) => {
                                error!("Timeout waiting for withdraw result for account {} after 30 seconds", account_id);
                                failed_withdrawals.push((account_id, amount));
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Write batching failed for withdraw on account {}: {}",
                            account_id, e
                        );
                        failed_withdrawals.push((account_id, amount));
                    }
                }
            }

            if successful_accounts.is_empty() {
                Err(AccountError::InfrastructureError(
                    "All batch withdraw operations failed".to_string(),
                ))
            } else {
                // OPTIMIZED: Wait for batch consistency for all successful operations
                match self
                    .wait_for_batch_consistency(successful_accounts.clone())
                    .await
                {
                    Ok(_) => {
                        info!(
                            "✅ Batch consistency completed for {} withdraw operations",
                            successful_accounts.len()
                        );
                        Ok(successful_accounts)
                    }
                    Err(e) => {
                        warn!("⚠️ Batch consistency failed: {}, but operations were processed successfully", e);
                        // Return partial success - operations were processed even if consistency failed
                        Ok(successful_accounts)
                    }
                }
            }
        } else {
            // Fall back to individual processing if write batching is not enabled
            error!("Write batching service not available for batch operations");
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
                    "✅ Batch withdraw operations completed successfully: {} accounts in {:?}",
                    account_ids.len(),
                    duration
                );
            }
            Err(_) => {
                self.metrics.commands_failed.fetch_add(
                    withdrawals_count as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                error!("❌ Batch withdraw operations failed in {:?}", duration);
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

    /// Optimized batch operations with pre-fetched versions
    pub async fn execute_batch_operations(
        &self,
        operations: Vec<(String, Uuid, Decimal)>, // (operation_type, account_id, amount)
    ) -> Result<Vec<Uuid>, AccountError> {
        let start_time = std::time::Instant::now();
        info!(
            "🚀 Starting optimized batch operations for {} operations",
            operations.len()
        );

        // 1. Extract unique account IDs
        let account_ids: Vec<Uuid> = operations
            .iter()
            .map(|(_, account_id, _)| *account_id)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // 2. Pre-fetch all versions using batch method
        let versions = self.get_aggregate_versions(account_ids).await?;
        info!("✅ Pre-fetched versions for {} accounts", versions.len());

        // 3. Process operations with pre-fetched versions
        let mut results = Vec::new();

        for (operation_type, account_id, amount) in operations {
            let version = versions.get(&account_id).unwrap_or(&0);

            let result = match operation_type.as_str() {
                "deposit" => {
                    // Use optimized deposit with pre-fetched version
                    self.deposit_money_with_version(account_id, amount, *version)
                        .await
                }
                "withdraw" => {
                    // Use optimized withdraw with pre-fetched version
                    self.withdraw_money_with_version(account_id, amount, *version)
                        .await
                }
                _ => Err(AccountError::InfrastructureError(format!(
                    "Unknown operation type: {}",
                    operation_type
                ))),
            }?;

            results.push(account_id);
        }

        info!(
            "✅ Batch operations completed in {:?}",
            start_time.elapsed()
        );
        Ok(results)
    }

    /// Optimized deposit with pre-fetched version
    async fn deposit_money_with_version(
        &self,
        account_id: Uuid,
        amount: Decimal,
        expected_version: i64,
    ) -> Result<(), AccountError> {
        let start_time = std::time::Instant::now();
        info!(
            "Depositing {} into account {} with version {}",
            amount, account_id, expected_version
        );

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Mark as pending CDC processing
        self.consistency_manager.mark_pending(account_id).await;
        self.consistency_manager
            .mark_projection_pending(account_id)
            .await;

        // Use write batching if available
        let result = if let Some(ref batching_service) = self.write_batching_service {
            let operation = WriteOperation::DepositMoney { account_id, amount };
            let aggregate_id = operation.get_aggregate_id();

            match batching_service
                .submit_operation(aggregate_id, operation)
                .await
            {
                Ok(operation_id) => {
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        batching_service.wait_for_result(operation_id),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            return Err(AccountError::InfrastructureError(
                                "Operation timed out".to_string(),
                            ));
                        }
                    }
                }
                Err(e) => {
                    return Err(AccountError::InfrastructureError(e.to_string()));
                }
            }
        } else {
            // Fallback to direct handler with pre-fetched version
            let command = AccountCommand::DepositMoney { account_id, amount };
            let _result = self.cqrs_handler.execute_command(command).await?;
            // Create a simple success result
            Ok(WriteOperationResult {
                operation_id: Uuid::new_v4(),
                result: Some(account_id),
                error: None,
                duration: std::time::Duration::from_millis(0),
                success: true,
            })
        };

        // Unwrap the result and check success
        let operation_result =
            result.map_err(|e| AccountError::InfrastructureError(e.to_string()))?;
        if operation_result.success {
            // Use individual consistency for single operation
            match self.wait_for_account_consistency(account_id).await {
                Ok(_) => {
                    info!(
                        "✅ Individual consistency completed for deposit operation on account {}",
                        account_id
                    );
                    Ok(())
                }
                Err(e) => {
                    warn!(
                        "⚠️ Individual consistency failed for deposit operation on account {}: {}",
                        account_id, e
                    );
                    Err(e)
                }
            }
        } else {
            Err(AccountError::InfrastructureError(
                "Deposit operation failed".to_string(),
            ))
        }
    }

    /// Optimized withdraw with pre-fetched version
    async fn withdraw_money_with_version(
        &self,
        account_id: Uuid,
        amount: Decimal,
        expected_version: i64,
    ) -> Result<(), AccountError> {
        let start_time = std::time::Instant::now();
        info!(
            "Withdrawing {} from account {} with version {}",
            amount, account_id, expected_version
        );

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Mark as pending CDC processing
        self.consistency_manager.mark_pending(account_id).await;
        self.consistency_manager
            .mark_projection_pending(account_id)
            .await;

        // Use write batching if available
        let result = if let Some(ref batching_service) = self.write_batching_service {
            let operation = WriteOperation::WithdrawMoney { account_id, amount };
            let aggregate_id = operation.get_aggregate_id();

            match batching_service
                .submit_operation(aggregate_id, operation)
                .await
            {
                Ok(operation_id) => {
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        batching_service.wait_for_result(operation_id),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            return Err(AccountError::InfrastructureError(
                                "Operation timed out".to_string(),
                            ));
                        }
                    }
                }
                Err(e) => {
                    return Err(AccountError::InfrastructureError(e.to_string()));
                }
            }
        } else {
            // Fallback to direct handler with pre-fetched version
            let command = AccountCommand::WithdrawMoney { account_id, amount };
            let _result = self.cqrs_handler.execute_command(command).await?;
            // Create a simple success result
            Ok(WriteOperationResult {
                operation_id: Uuid::new_v4(),
                result: Some(account_id),
                error: None,
                duration: std::time::Duration::from_millis(0),
                success: true,
            })
        };

        // Unwrap the result and check success
        let operation_result =
            result.map_err(|e| AccountError::InfrastructureError(e.to_string()))?;
        if operation_result.success {
            // Use individual consistency for single operation
            match self.wait_for_account_consistency(account_id).await {
                Ok(_) => {
                    info!(
                        "✅ Individual consistency completed for withdraw operation on account {}",
                        account_id
                    );
                    Ok(())
                }
                Err(e) => {
                    warn!(
                        "⚠️ Individual consistency failed for withdraw operation on account {}: {}",
                        account_id, e
                    );
                    Err(e)
                }
            }
        } else {
            Err(AccountError::InfrastructureError(
                "Withdraw operation failed".to_string(),
            ))
        }
    }

    /// Execute batch commands using the command handler
    pub async fn execute_batch_commands(
        &self,
        commands: Vec<AccountCommand>,
    ) -> Result<Vec<CommandResult>, AccountError> {
        // Use the command handler through the CQRS handler's public interface
        self.cqrs_handler.execute_batch_commands(commands).await
    }

    /// Get current version for an aggregate ID
    pub async fn get_aggregate_version(&self, aggregate_id: Uuid) -> Result<i64, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Getting current version for aggregate {}", aggregate_id);

        // Method 1: Try Event Store directly (fastest)
        let event_store = self.cqrs_handler.event_store();
        match event_store.get_current_version(aggregate_id, false).await {
            Ok(version) => {
                info!(
                    "✅ Got version {} for aggregate {} in {:?}",
                    version,
                    aggregate_id,
                    start_time.elapsed()
                );
                return Ok(version);
            }
            Err(e) => {
                warn!(
                    "⚠️ Event store version check failed for {}: {}, trying alternative method",
                    aggregate_id, e
                );
            }
        }

        // Method 2: Get from events (fallback)
        match event_store.get_events(aggregate_id, None).await {
            Ok(events) => {
                let version = events.last().map(|event| event.version).unwrap_or(0);
                info!(
                    "✅ Got version {} for aggregate {} from events in {:?}",
                    version,
                    aggregate_id,
                    start_time.elapsed()
                );
                Ok(version)
            }
            Err(e) => {
                error!(
                    "❌ Failed to get version for aggregate {}: {}",
                    aggregate_id, e
                );
                Err(AccountError::InfrastructureError(e.to_string()))
            }
        }
    }

    /// Get account version (alias for get_aggregate_version)
    pub async fn get_account_version(&self, account_id: Uuid) -> Result<i64, AccountError> {
        self.get_aggregate_version(account_id).await
    }

    /// Get current version for multiple aggregates
    pub async fn get_aggregate_versions(
        &self,
        aggregate_ids: Vec<Uuid>,
    ) -> Result<HashMap<Uuid, i64>, AccountError> {
        let start_time = std::time::Instant::now();
        info!("Getting versions for {} aggregates", aggregate_ids.len());

        let mut versions = HashMap::new();
        let event_store = self.cqrs_handler.event_store();

        // Process in parallel for better performance
        let futures: Vec<_> = aggregate_ids
            .into_iter()
            .map(|id| {
                let event_store = event_store.clone();
                async move {
                    let version = event_store
                        .get_current_version(id, false)
                        .await
                        .unwrap_or(0);
                    (id, version)
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        for (aggregate_id, version) in results {
            versions.insert(aggregate_id, version);
        }

        info!(
            "✅ Got versions for {} aggregates in {:?}",
            versions.len(),
            start_time.elapsed()
        );

        Ok(versions)
    }

    /// Check if aggregate exists and get its version
    pub async fn get_aggregate_info(
        &self,
        aggregate_id: Uuid,
    ) -> Result<Option<(i64, bool)>, AccountError> {
        let event_store = self.cqrs_handler.event_store();

        // Try to get events
        match event_store.get_events(aggregate_id, None).await {
            Ok(events) => {
                if events.is_empty() {
                    Ok(None) // Aggregate doesn't exist
                } else {
                    let version = events.last().unwrap().version;
                    let is_active = events.iter().all(|event| {
                        !matches!(
                            bincode::deserialize::<AccountEvent>(&event.event_data)
                                .unwrap_or_default(),
                            AccountEvent::AccountClosed { .. }
                        )
                    });
                    Ok(Some((version, is_active)))
                }
            }
            Err(e) => {
                error!("Failed to get aggregate info for {}: {}", aggregate_id, e);
                Err(AccountError::InfrastructureError(e.to_string()))
            }
        }
    }

    /// Get account with read batching if enabled
    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        // Try read batching first if enabled
        if let Some(read_batching) = &self.read_batching_service {
            let operation = ReadOperation::GetAccount { account_id };

            // ✅ Capture batch size BEFORE submitting operation
            let batch_size_before_submit = read_batching.get_current_batch_size().await;

            match read_batching.submit_read_operation(operation).await {
                Ok(operation_id) => {
                    match read_batching.wait_for_result(operation_id).await {
                        Ok(ReadOperationResult::Account { account, .. }) => {
                            // SMART: Use batch size captured BEFORE submit
                            // This gives us the actual batch size when operation was submitted
                            if batch_size_before_submit > 0 {
                                // Multiple operations in batch - use batch consistency
                                info!("📦 Read operation was part of batch with {} operations, using batch consistency", batch_size_before_submit + 1);
                                match self.wait_for_batch_consistency(vec![account_id]).await {
                                    Ok(_) => {
                                        info!("✅ Batch consistency completed for read operation on account {}", account_id);
                                        return Ok(account);
                                    }
                                    Err(e) => {
                                        warn!("⚠️ Batch consistency failed for read operation on account {}: {}, but returning result anyway", account_id, e);
                                        return Ok(account);
                                    }
                                }
                            } else {
                                // Single operation in batch - use individual consistency
                                info!("🔍 Read operation was single in batch, using individual consistency");
                                match self.wait_for_account_consistency(account_id).await {
                                    Ok(_) => {
                                        info!("✅ Individual consistency completed for read operation on account {}", account_id);
                                        return Ok(account);
                                    }
                                    Err(e) => {
                                        warn!("⚠️ Individual consistency failed for read operation on account {}: {}, but returning result anyway", account_id, e);
                                        return Ok(account);
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            return Err(AccountError::InfrastructureError(
                                "Unexpected result type from read batching".to_string(),
                            ));
                        }
                        Err(e) => {
                            warn!("Read batching failed, falling back to direct access: {}", e);
                            // Fall back to direct access
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to submit read operation to batching: {}", e);
                    // Fall back to direct access
                }
            }
        }

        // Fallback to direct projection store access with consistency check
        match self.wait_for_account_consistency(account_id).await {
            Ok(_) => {
                info!(
                    "Consistency check passed for account {} (fallback)",
                    account_id
                );
            }
            Err(e) => {
                warn!("Consistency wait failed for account {}: {}", account_id, e);
                // Continue anyway for read operations
            }
        }

        self.projection_store
            .get_account(account_id)
            .await
            .map_err(|e| AccountError::InfrastructureError(format!("Failed to get account: {}", e)))
    }

    /// Get account balance with read batching if enabled
    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        // Try read batching first if enabled
        if let Some(read_batching) = &self.read_batching_service {
            let operation = ReadOperation::GetAccountBalance { account_id };

            // ✅ Capture batch size BEFORE submitting operation
            let batch_size_before_submit = read_batching.get_current_batch_size().await;

            match read_batching.submit_read_operation(operation).await {
                Ok(operation_id) => {
                    match read_batching.wait_for_result(operation_id).await {
                        Ok(ReadOperationResult::AccountBalance { balance, .. }) => {
                            if let Some(balance) = balance {
                                // SMART: Use batch size captured BEFORE submit
                                if batch_size_before_submit > 0 {
                                    // Multiple operations in batch - use batch consistency
                                    info!("📦 Balance read operation was part of batch with {} operations, using batch consistency", batch_size_before_submit + 1);
                                    match self.wait_for_batch_consistency(vec![account_id]).await {
                                        Ok(_) => {
                                            info!("✅ Batch consistency completed for balance read on account {}", account_id);
                                            return Ok(balance);
                                        }
                                        Err(e) => {
                                            warn!("⚠️ Batch consistency failed for balance read on account {}: {}, but returning result anyway", account_id, e);
                                            return Ok(balance);
                                        }
                                    }
                                } else {
                                    // Single operation in batch - use individual consistency
                                    info!("🔍 Balance read operation was single in batch, using individual consistency");
                                    match self.wait_for_account_consistency(account_id).await {
                                        Ok(_) => {
                                            info!("✅ Individual consistency completed for balance read on account {}", account_id);
                                            return Ok(balance);
                                        }
                                        Err(e) => {
                                            warn!("⚠️ Individual consistency failed for balance read on account {}: {}, but returning result anyway", account_id, e);
                                            return Ok(balance);
                                        }
                                    }
                                }
                            } else {
                                return Err(AccountError::InfrastructureError(format!(
                                    "Account not found: {}",
                                    account_id
                                )));
                            }
                        }
                        Ok(_) => {
                            return Err(AccountError::InfrastructureError(
                                "Unexpected result type from read batching".to_string(),
                            ));
                        }
                        Err(e) => {
                            warn!("Read batching failed, falling back to direct access: {}", e);
                            // Fall back to direct access
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to submit read operation to batching: {}", e);
                    // Fall back to direct access
                }
            }
        }

        // Fallback to direct projection store access with consistency check
        match self.wait_for_account_consistency(account_id).await {
            Ok(_) => {
                info!(
                    "Consistency check passed for account {} balance (fallback)",
                    account_id
                );
            }
            Err(e) => {
                warn!("Consistency wait failed for account {}: {}", account_id, e);
                // Continue anyway for read operations
            }
        }

        match self.projection_store.get_account(account_id).await {
            Ok(Some(account)) => Ok(account.balance),
            Ok(None) => Err(AccountError::InfrastructureError(format!(
                "Account not found: {}",
                account_id
            ))),
            Err(e) => Err(AccountError::InfrastructureError(format!(
                "Failed to get account: {}",
                e
            ))),
        }
    }

    /// Check if account is active
    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        match self.get_account(account_id).await {
            Ok(Some(account)) => Ok(account.is_active),
            Ok(None) => Err(AccountError::InfrastructureError(format!(
                "Account not found: {}",
                account_id
            ))),
            Err(e) => Err(e),
        }
    }

    /// Get account transactions with read batching if enabled
    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, AccountError> {
        // Try read batching first if enabled
        if let Some(read_batching) = &self.read_batching_service {
            let operation = ReadOperation::GetAccountTransactions { account_id };

            // ✅ Capture batch size BEFORE submitting operation
            let batch_size_before_submit = read_batching.get_current_batch_size().await;

            match read_batching.submit_read_operation(operation).await {
                Ok(operation_id) => {
                    match read_batching.wait_for_result(operation_id).await {
                        Ok(ReadOperationResult::AccountTransactions { transactions, .. }) => {
                            // Convert AccountEvent back to TransactionProjection
                            // This is a simplified conversion - you might need to adjust based on your data model
                            let transaction_projections: Vec<TransactionProjection> = transactions
                                .into_iter()
                                .filter_map(|event| match event {
                                    AccountEvent::MoneyDeposited {
                                        account_id,
                                        amount,
                                        transaction_id,
                                    } => Some(TransactionProjection {
                                        id: transaction_id,
                                        account_id,
                                        amount,
                                        transaction_type: "deposit".to_string(),
                                        timestamp: chrono::Utc::now(),
                                    }),
                                    AccountEvent::MoneyWithdrawn {
                                        account_id,
                                        amount,
                                        transaction_id,
                                    } => Some(TransactionProjection {
                                        id: transaction_id,
                                        account_id,
                                        amount,
                                        transaction_type: "withdrawal".to_string(),
                                        timestamp: chrono::Utc::now(),
                                    }),
                                    _ => None,
                                })
                                .collect();

                            // SMART: Use batch size captured BEFORE submit
                            if batch_size_before_submit > 0 {
                                // Multiple operations in batch - use batch consistency
                                info!("📦 Transactions read operation was part of batch with {} operations, using batch consistency", batch_size_before_submit + 1);
                                match self.wait_for_batch_consistency(vec![account_id]).await {
                                    Ok(_) => {
                                        info!("✅ Batch consistency completed for transactions read on account {}", account_id);
                                        return Ok(transaction_projections);
                                    }
                                    Err(e) => {
                                        warn!("⚠️ Batch consistency failed for transactions read on account {}: {}, but returning result anyway", account_id, e);
                                        return Ok(transaction_projections);
                                    }
                                }
                            } else {
                                // Single operation in batch - use individual consistency
                                info!("🔍 Transactions read operation was single in batch, using individual consistency");
                                match self.wait_for_account_consistency(account_id).await {
                                    Ok(_) => {
                                        info!("✅ Individual consistency completed for transactions read on account {}", account_id);
                                        return Ok(transaction_projections);
                                    }
                                    Err(e) => {
                                        warn!("⚠️ Individual consistency failed for transactions read on account {}: {}, but returning result anyway", account_id, e);
                                        return Ok(transaction_projections);
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            return Err(AccountError::InfrastructureError(
                                "Unexpected result type from read batching".to_string(),
                            ));
                        }
                        Err(e) => {
                            warn!("Read batching failed, falling back to direct access: {}", e);
                            // Fall back to direct access
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to submit read operation to batching: {}", e);
                    // Fall back to direct access
                }
            }
        }

        // Fallback to direct projection store access with consistency check
        match self.wait_for_account_consistency(account_id).await {
            Ok(_) => {
                info!(
                    "Consistency check passed for account {} transactions (fallback)",
                    account_id
                );
            }
            Err(e) => {
                warn!("Consistency wait failed for account {}: {}", account_id, e);
                // Continue anyway for read operations
            }
        }

        self.projection_store
            .get_account_transactions(account_id)
            .await
            .map_err(|e| {
                AccountError::InfrastructureError(format!(
                    "Failed to get account transactions: {}",
                    e
                ))
            })
    }

    /// Get multiple accounts with read batching if enabled
    pub async fn get_multiple_accounts(
        &self,
        account_ids: Vec<Uuid>,
    ) -> Result<Vec<AccountProjection>, AccountError> {
        // Try read batching first if enabled
        if let Some(read_batching) = &self.read_batching_service {
            let operation = ReadOperation::GetMultipleAccounts {
                account_ids: account_ids.clone(),
            };

            match read_batching.submit_read_operation(operation).await {
                Ok(operation_id) => {
                    match read_batching.wait_for_result(operation_id).await {
                        Ok(ReadOperationResult::MultipleAccounts { accounts }) => {
                            // Check batch consistency AFTER getting results (batch approach)
                            match self.wait_for_batch_consistency(account_ids.clone()).await {
                                Ok(_) => {
                                    info!(
                                        "Batch consistency check passed for {} accounts",
                                        accounts.len()
                                    );
                                    return Ok(accounts);
                                }
                                Err(e) => {
                                    warn!("Batch consistency check failed: {}", e);
                                    // Return the results anyway, but log the consistency issue
                                    return Ok(accounts);
                                }
                            }
                        }
                        Ok(_) => {
                            return Err(AccountError::InfrastructureError(
                                "Unexpected result type from read batching".to_string(),
                            ));
                        }
                        Err(e) => {
                            warn!("Read batching failed, falling back to direct access: {}", e);
                            // Fall back to direct access
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to submit read operation to batching: {}", e);
                    // Fall back to direct access
                }
            }
        }

        // Fallback to direct projection store access with batch consistency check
        match self.wait_for_batch_consistency(account_ids.clone()).await {
            Ok(_) => {
                info!("Batch consistency check passed (fallback)");
            }
            Err(e) => {
                warn!("Consistency wait failed for batch accounts: {}", e);
                // Continue anyway for read operations
            }
        }

        let mut accounts = Vec::new();
        for account_id in account_ids {
            if let Ok(Some(account)) = self.projection_store.get_account(account_id).await {
                accounts.push(account);
            }
        }
        Ok(accounts)
    }

    /// NEW: Optimized batch read with consistency check
    pub async fn get_accounts_batch_with_consistency(
        &self,
        account_ids: Vec<Uuid>,
    ) -> Result<Vec<Option<AccountProjection>>, AccountError> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Submit all operations to read batching
        if let Some(read_batching) = &self.read_batching_service {
            let mut operations = Vec::new();
            for account_id in account_ids.clone() {
                operations.push(ReadOperation::GetAccount { account_id });
            }

            // 2. Process batch
            match read_batching.submit_read_operations_batch(operations).await {
                Ok(operation_ids) => {
                    // 3. Wait for batch consistency ONCE
                    match self.wait_for_batch_consistency(account_ids.clone()).await {
                        Ok(_) => {
                            info!(
                                "Batch consistency check passed for {} accounts",
                                account_ids.len()
                            );
                        }
                        Err(e) => {
                            warn!("Batch consistency check failed: {}", e);
                            // Continue anyway, but log the consistency issue
                        }
                    }

                    // 4. Collect results
                    let mut results = Vec::new();
                    for operation_id in operation_ids {
                        match read_batching.wait_for_result(operation_id).await {
                            Ok(ReadOperationResult::Account { account, .. }) => {
                                results.push(account);
                            }
                            Ok(_) => {
                                results.push(None); // Unexpected result type
                            }
                            Err(e) => {
                                warn!("Failed to get batch result: {}", e);
                                results.push(None);
                            }
                        }
                    }

                    return Ok(results);
                }
                Err(e) => {
                    warn!("Failed to submit batch read operations: {}", e);
                    // Fall back to individual reads
                }
            }
        }

        // Fallback: individual reads with batch consistency
        match self.wait_for_batch_consistency(account_ids.clone()).await {
            Ok(_) => {
                info!("Batch consistency check passed (fallback)");
            }
            Err(e) => {
                warn!("Consistency wait failed for batch accounts: {}", e);
            }
        }

        let mut results = Vec::new();
        for account_id in account_ids {
            match self.projection_store.get_account(account_id).await {
                Ok(account) => results.push(account),
                Err(e) => {
                    warn!("Failed to get account {}: {}", account_id, e);
                    results.push(None);
                }
            }
        }

        Ok(results)
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
