use crate::domain::{Account, AccountEvent};
use crate::infrastructure::event_store::EventStoreTrait;
use crate::infrastructure::outbox::OutboxMessage;
use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use anyhow::{Context, Result};
use bincode;
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::{PgPool, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Configuration for write batching
#[derive(Debug, Clone)]
pub struct WriteBatchingConfig {
    pub max_batch_size: usize,
    pub max_batch_wait_time_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

impl Default for WriteBatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 250,        // Updated for better performance
            max_batch_wait_time_ms: 25, // Updated for better performance
            max_retries: 3,
            retry_backoff_ms: 50,
        }
    }
}

/// Types of write operations that can be batched
#[derive(Debug, Clone)]
pub enum WriteOperation {
    CreateAccount {
        owner_name: String,
        initial_balance: Decimal,
    },
    DepositMoney {
        account_id: Uuid,
        amount: Decimal,
    },
    WithdrawMoney {
        account_id: Uuid,
        amount: Decimal,
    },
}

/// Result of a write operation
#[derive(Debug, Clone)]
pub struct WriteOperationResult {
    pub operation_id: Uuid,
    pub success: bool,
    pub result: Option<Uuid>, // Account ID for create operations
    pub error: Option<String>,
    pub duration: Duration,
}

/// A batch of write operations
#[derive(Debug)]
pub struct WriteBatch {
    pub batch_id: Uuid,
    pub operations: Vec<(Uuid, WriteOperation)>, // (operation_id, operation)
    pub created_at: Instant,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            operations: Vec::new(),
            created_at: Instant::now(),
        }
    }

    pub fn add_operation(&mut self, operation_id: Uuid, operation: WriteOperation) {
        self.operations.push((operation_id, operation));
    }

    pub fn is_full(&self, max_size: usize) -> bool {
        self.operations.len() >= max_size
    }

    pub fn is_old(&self, max_wait_time: Duration) -> bool {
        self.created_at.elapsed() >= max_wait_time
    }

    pub fn should_process(&self, max_size: usize, max_wait_time: Duration) -> bool {
        self.is_full(max_size) || self.is_old(max_wait_time)
    }
}

/// Write batching service that groups operations into batches
pub struct WriteBatchingService {
    config: WriteBatchingConfig,
    event_store: Arc<dyn EventStoreTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    write_pool: Arc<PgPool>,
    outbox_batcher: crate::infrastructure::cdc_debezium::OutboxBatcher,

    // Batching state
    current_batch: Arc<Mutex<WriteBatch>>,
    pending_results: Arc<Mutex<HashMap<Uuid, mpsc::Sender<WriteOperationResult>>>>,

    // FIXED: Store completed results
    completed_results: Arc<Mutex<HashMap<Uuid, WriteOperationResult>>>,

    // Processing
    batch_processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_tx: Arc<Mutex<Option<mpsc::Sender<()>>>>,

    // Metrics
    batches_processed: Arc<Mutex<u64>>,
    operations_processed: Arc<Mutex<u64>>,
}

impl WriteBatchingService {
    pub fn new(
        config: WriteBatchingConfig,
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        // Create CDC outbox repository
        let outbox_repo = Arc::new(
            crate::infrastructure::cdc_debezium::CDCOutboxRepository::new(
                event_store.get_partitioned_pools().clone(),
            ),
        )
            as Arc<dyn crate::infrastructure::outbox::OutboxRepositoryTrait>;

        Self {
            config,
            event_store: event_store.clone(),
            projection_store,
            write_pool: write_pool.clone(),
            outbox_batcher: crate::infrastructure::cdc_debezium::OutboxBatcher::new_default(
                outbox_repo,
                event_store.get_partitioned_pools().clone(),
            ),
            current_batch: Arc::new(Mutex::new(WriteBatch::new())),
            pending_results: Arc::new(Mutex::new(HashMap::new())),
            completed_results: Arc::new(Mutex::new(HashMap::new())), // FIXED: Add result storage
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            batches_processed: Arc::new(Mutex::new(0)),
            operations_processed: Arc::new(Mutex::new(0)),
        }
    }

    /// Start the batch processor from an Arc (for use with shared references)
    pub async fn start(&self) -> Result<()> {
        // Check if already running
        if self.batch_processor_handle.lock().await.is_some() {
            return Ok(());
        }

        info!("🚀 Starting write batching service...");

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        let current_batch = self.current_batch.clone();
        let pending_results = self.pending_results.clone();
        let completed_results = self.completed_results.clone(); // FIXED: Pass completed results
        let config = self.config.clone();
        let event_store = self.event_store.clone();
        let projection_store = self.projection_store.clone();
        let write_pool = self.write_pool.clone();
        let outbox_batcher = self.outbox_batcher.clone();
        let batches_processed = self.batches_processed.clone();
        let operations_processed = self.operations_processed.clone();

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(config.max_batch_wait_time_ms));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check if current batch should be processed
                        let should_process = {
                            let batch = current_batch.lock().await;
                            batch.should_process(config.max_batch_size, Duration::from_millis(config.max_batch_wait_time_ms))
                        };

                        if should_process {
                            Self::process_current_batch(
                                &current_batch,
                                &pending_results,
                                &completed_results, // FIXED: Pass completed results
                                &event_store,
                                &projection_store,
                                &write_pool,
                                &outbox_batcher,
                                &config,
                                &batches_processed,
                                &operations_processed,
                            ).await;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("🛑 Write batching service shutdown signal received");
                        break;
                    }
                }
            }

            // Process any remaining operations
            Self::process_current_batch(
                &current_batch,
                &pending_results,
                &completed_results, // FIXED: Pass completed results
                &event_store,
                &projection_store,
                &write_pool,
                &outbox_batcher,
                &config,
                &batches_processed,
                &operations_processed,
            )
            .await;
        });

        *self.batch_processor_handle.lock().await = Some(handle);

        info!("✅ Write batching service started");
        Ok(())
    }

    /// Stop the batch processor
    pub async fn stop(&mut self) -> Result<()> {
        info!("🛑 Stopping write batching service...");

        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(()).await;
        }

        if let Some(handle) = self.batch_processor_handle.lock().await.take() {
            let _ = handle.await;
        }

        info!("✅ Write batching service stopped");
        Ok(())
    }

    /// Submit a write operation for batching
    pub async fn submit_operation(&self, operation: WriteOperation) -> Result<Uuid> {
        let operation_id = Uuid::new_v4();
        let (result_tx, result_rx) = mpsc::channel::<WriteOperationResult>(1);

        // Add to pending results
        self.pending_results
            .lock()
            .await
            .insert(operation_id, result_tx);

        // Add to current batch
        {
            let mut batch = self.current_batch.lock().await;
            batch.add_operation(operation_id, operation);

            // FIXED: Process immediately for single operations to avoid delays
            let should_process_immediately = batch.operations.len() == 1;

            if should_process_immediately {
                drop(batch); // Release lock before processing

                // Process immediately for single operations
                Self::process_current_batch(
                    &self.current_batch,
                    &self.pending_results,
                    &self.completed_results,
                    &self.event_store,
                    &self.projection_store,
                    &self.write_pool,
                    &self.outbox_batcher,
                    &self.config,
                    &self.batches_processed,
                    &self.operations_processed,
                )
                .await;
            } else if batch.is_full(self.config.max_batch_size) {
                drop(batch); // Release lock before processing

                Self::process_current_batch(
                    &self.current_batch,
                    &self.pending_results,
                    &self.completed_results,
                    &self.event_store,
                    &self.projection_store,
                    &self.write_pool,
                    &self.outbox_batcher,
                    &self.config,
                    &self.batches_processed,
                    &self.operations_processed,
                )
                .await;
            }
        }

        Ok(operation_id)
    }

    /// FIXED: Wait for the result of a submitted operation
    pub async fn wait_for_result(&self, operation_id: Uuid) -> Result<WriteOperationResult> {
        // First check if result is already completed
        {
            let completed = self.completed_results.lock().await;
            if let Some(result) = completed.get(&operation_id) {
                return Ok(result.clone());
            }
        }

        // Check if operation is still pending
        {
            let pending = self.pending_results.lock().await;
            if !pending.contains_key(&operation_id) {
                return Err(anyhow::anyhow!(
                    "Operation {} not found or already completed",
                    operation_id
                ));
            }
        }

        // Wait for the result with a reasonable timeout
        let timeout = Duration::from_secs(5); // FIXED: Reduced from 30s to 5s
        match tokio::time::timeout(timeout, async {
            // Poll for the result more efficiently
            loop {
                // Check completed results first
                {
                    let completed = self.completed_results.lock().await;
                    if let Some(result) = completed.get(&operation_id) {
                        return result.clone();
                    }
                }

                // Check if still pending
                {
                    let pending = self.pending_results.lock().await;
                    if !pending.contains_key(&operation_id) {
                        // Operation completed, check completed results again
                        let completed = self.completed_results.lock().await;
                        if let Some(result) = completed.get(&operation_id) {
                            return result.clone();
                        }
                        // If not found in completed, something went wrong
                        return WriteOperationResult {
                            operation_id,
                            success: false,
                            result: None,
                            error: Some("Operation completed but result not found".to_string()),
                            duration: Duration::ZERO,
                        };
                    }
                }

                tokio::time::sleep(Duration::from_millis(50)).await; // FIXED: Increased from 10ms to 50ms
            }
        })
        .await
        {
            Ok(result) => Ok(result),
            Err(_) => Err(anyhow::anyhow!(
                "Timeout waiting for operation {} result after 5 seconds",
                operation_id
            )),
        }
    }

    /// FIXED: Process the current batch of operations
    async fn process_current_batch(
        current_batch: &Arc<Mutex<WriteBatch>>,
        pending_results: &Arc<Mutex<HashMap<Uuid, mpsc::Sender<WriteOperationResult>>>>,
        completed_results: &Arc<Mutex<HashMap<Uuid, WriteOperationResult>>>, // FIXED: Add completed results
        event_store: &Arc<dyn EventStoreTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        write_pool: &Arc<PgPool>,
        outbox_batcher: &crate::infrastructure::cdc_debezium::OutboxBatcher,
        config: &WriteBatchingConfig,
        batches_processed: &Arc<Mutex<u64>>,
        operations_processed: &Arc<Mutex<u64>>,
    ) {
        let start_time = Instant::now();

        // Take the current batch
        let batch = {
            let mut batch_guard = current_batch.lock().await;
            if batch_guard.operations.is_empty() {
                return;
            }

            let batch = std::mem::replace(&mut *batch_guard, WriteBatch::new());
            batch
        };

        info!(
            "📦 Processing write batch {} with {} operations",
            batch.batch_id,
            batch.operations.len()
        );

        // Process the batch
        let results = Self::execute_batch(
            &batch,
            event_store,
            projection_store,
            write_pool,
            outbox_batcher,
            config,
        )
        .await;

        // FIXED: Store results and send to waiting operations
        {
            let mut completed = completed_results.lock().await;
            let mut pending = pending_results.lock().await;

            for (operation_id, result) in results {
                // Store the result
                completed.insert(operation_id, result.clone());

                // Send to waiting operation if any
                if let Some(tx) = pending.remove(&operation_id) {
                    let _ = tx.send(result).await;
                }
            }
        }

        // Update metrics
        {
            *batches_processed.lock().await += 1;
            *operations_processed.lock().await += batch.operations.len() as u64;
        }

        info!(
            "✅ Batch {} processed in {:?}",
            batch.batch_id,
            start_time.elapsed()
        );
    }

    /// Execute a batch of operations in a single transaction
    async fn execute_batch(
        batch: &WriteBatch,
        event_store: &Arc<dyn EventStoreTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        write_pool: &Arc<PgPool>,
        outbox_batcher: &crate::infrastructure::cdc_debezium::OutboxBatcher,
        config: &WriteBatchingConfig,
    ) -> Vec<(Uuid, WriteOperationResult)> {
        let mut results = Vec::new();
        let mut retry_count = 0;

        while retry_count < config.max_retries {
            match Self::execute_batch_transaction(
                batch,
                event_store,
                projection_store,
                write_pool,
                outbox_batcher,
            )
            .await
            {
                Ok(batch_results) => {
                    results = batch_results;
                    break;
                }
                Err(e) => {
                    retry_count += 1;
                    warn!(
                        "❌ Batch execution failed (attempt {}/{}): {}",
                        retry_count, config.max_retries, e
                    );

                    if retry_count < config.max_retries {
                        let backoff =
                            Duration::from_millis(config.retry_backoff_ms * retry_count as u64);
                        tokio::time::sleep(backoff).await;
                    } else {
                        // Mark all operations as failed
                        for (operation_id, _) in &batch.operations {
                            results.push((
                                *operation_id,
                                WriteOperationResult {
                                    operation_id: *operation_id,
                                    success: false,
                                    result: None,
                                    error: Some(format!(
                                        "Batch failed after {} retries: {}",
                                        config.max_retries, e
                                    )),
                                    duration: Duration::ZERO,
                                },
                            ));
                        }
                    }
                }
            }
        }

        results
    }

    /// Execute a batch in a single database transaction
    async fn execute_batch_transaction(
        batch: &WriteBatch,
        event_store: &Arc<dyn EventStoreTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        write_pool: &Arc<PgPool>,
        outbox_batcher: &crate::infrastructure::cdc_debezium::OutboxBatcher,
    ) -> Result<Vec<(Uuid, WriteOperationResult)>> {
        let mut results = Vec::new();
        let mut transaction = write_pool.begin().await?;

        for (operation_id, operation) in &batch.operations {
            let start_time = Instant::now();

            let result = match operation {
                WriteOperation::CreateAccount {
                    owner_name,
                    initial_balance,
                } => {
                    Self::execute_create_account(
                        &mut transaction,
                        event_store,
                        projection_store,
                        outbox_batcher,
                        owner_name,
                        *initial_balance,
                    )
                    .await
                }
                WriteOperation::DepositMoney { account_id, amount } => {
                    Self::execute_deposit_money(
                        &mut transaction,
                        event_store,
                        projection_store,
                        outbox_batcher,
                        *account_id,
                        *amount,
                    )
                    .await
                }
                WriteOperation::WithdrawMoney { account_id, amount } => {
                    Self::execute_withdraw_money(
                        &mut transaction,
                        event_store,
                        projection_store,
                        outbox_batcher,
                        *account_id,
                        *amount,
                    )
                    .await
                }
            };

            let duration = start_time.elapsed();

            match result {
                Ok(account_id) => {
                    results.push((
                        *operation_id,
                        WriteOperationResult {
                            operation_id: *operation_id,
                            success: true,
                            result: Some(account_id),
                            error: None,
                            duration,
                        },
                    ));
                }
                Err(e) => {
                    results.push((
                        *operation_id,
                        WriteOperationResult {
                            operation_id: *operation_id,
                            success: false,
                            result: None,
                            error: Some(e.to_string()),
                            duration,
                        },
                    ));
                }
            }
        }

        // Commit the transaction
        transaction.commit().await?;

        Ok(results)
    }

    /// Execute create account operation
    async fn execute_create_account(
        transaction: &mut Transaction<'_, sqlx::Postgres>,
        event_store: &Arc<dyn EventStoreTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        outbox_batcher: &crate::infrastructure::cdc_debezium::OutboxBatcher,
        owner_name: &str,
        initial_balance: Decimal,
    ) -> Result<Uuid> {
        let account_id = Uuid::new_v4();
        let account = Account::new(account_id, owner_name.to_string(), initial_balance);

        let event = AccountEvent::AccountCreated {
            account_id,
            owner_name: owner_name.to_string(),
            initial_balance,
        };

        // Save event to event store
        event_store
            .save_events_in_transaction(transaction, account_id, vec![event.clone()], 0)
            .await
            .map_err(|e| anyhow::anyhow!("Event store error: {:?}", e))?;

        // Create outbox message for CDC pipeline
        let outbox_message = OutboxMessage {
            aggregate_id: account_id,
            event_id: Uuid::new_v4(),
            event_type: "AccountCreated".to_string(),
            payload: bincode::serialize(&event)
                .map_err(|e| anyhow::anyhow!("Event serialization error: {:?}", e))?,
            topic: "account-events".to_string(),
            metadata: None,
        };

        // FIXED: Submit to outbox batcher NON-BLOCKING (don't wait for it)
        // This prevents the outbox batcher from blocking the database transaction
        let outbox_batcher_clone = outbox_batcher.clone();
        tokio::spawn(async move {
            if let Err(e) = outbox_batcher_clone.submit(outbox_message).await {
                error!("Failed to submit outbox message (non-blocking): {}", e);
            }
        });

        Ok(account_id)
    }

    /// Execute deposit money operation
    async fn execute_deposit_money(
        transaction: &mut Transaction<'_, sqlx::Postgres>,
        event_store: &Arc<dyn EventStoreTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        outbox_batcher: &crate::infrastructure::cdc_debezium::OutboxBatcher,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<Uuid> {
        let event = AccountEvent::MoneyDeposited {
            account_id,
            amount,
            transaction_id: Uuid::new_v4(),
        };

        // Save event to event store
        event_store
            .save_events_in_transaction(transaction, account_id, vec![event.clone()], 0)
            .await
            .map_err(|e| anyhow::anyhow!("Event store error: {:?}", e))?;

        // Create outbox message for CDC pipeline
        let outbox_message = OutboxMessage {
            aggregate_id: account_id,
            event_id: Uuid::new_v4(),
            event_type: "MoneyDeposited".to_string(),
            payload: bincode::serialize(&event)
                .map_err(|e| anyhow::anyhow!("Event serialization error: {:?}", e))?,
            topic: "account-events".to_string(),
            metadata: None,
        };

        // FIXED: Submit to outbox batcher NON-BLOCKING (don't wait for it)
        let outbox_batcher_clone = outbox_batcher.clone();
        tokio::spawn(async move {
            if let Err(e) = outbox_batcher_clone.submit(outbox_message).await {
                error!("Failed to submit outbox message (non-blocking): {}", e);
            }
        });

        Ok(account_id)
    }

    /// Execute withdraw money operation
    async fn execute_withdraw_money(
        transaction: &mut Transaction<'_, sqlx::Postgres>,
        event_store: &Arc<dyn EventStoreTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        outbox_batcher: &crate::infrastructure::cdc_debezium::OutboxBatcher,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<Uuid> {
        let event = AccountEvent::MoneyWithdrawn {
            account_id,
            amount,
            transaction_id: Uuid::new_v4(),
        };

        // Save event to event store
        event_store
            .save_events_in_transaction(transaction, account_id, vec![event.clone()], 0)
            .await
            .map_err(|e| anyhow::anyhow!("Event store error: {:?}", e))?;

        // Create outbox message for CDC pipeline
        let outbox_message = OutboxMessage {
            aggregate_id: account_id,
            event_id: Uuid::new_v4(),
            event_type: "MoneyWithdrawn".to_string(),
            payload: bincode::serialize(&event)
                .map_err(|e| anyhow::anyhow!("Event serialization error: {:?}", e))?,
            topic: "account-events".to_string(),
            metadata: None,
        };

        // FIXED: Submit to outbox batcher NON-BLOCKING (don't wait for it)
        let outbox_batcher_clone = outbox_batcher.clone();
        tokio::spawn(async move {
            if let Err(e) = outbox_batcher_clone.submit(outbox_message).await {
                error!("Failed to submit outbox message (non-blocking): {}", e);
            }
        });

        Ok(account_id)
    }

    /// Get service statistics
    pub async fn get_stats(&self) -> serde_json::Value {
        let batches_processed = *self.batches_processed.lock().await;
        let operations_processed = *self.operations_processed.lock().await;

        serde_json::json!({
            "batches_processed": batches_processed,
            "operations_processed": operations_processed,
            "config": {
                "max_batch_size": self.config.max_batch_size,
                "max_batch_wait_time_ms": self.config.max_batch_wait_time_ms,
                "max_retries": self.config.max_retries,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_write_batching_config() {
        let config = WriteBatchingConfig::default();
        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.max_batch_wait_time_ms, 100);
        assert_eq!(config.max_retries, 3);
    }

    #[tokio::test]
    async fn test_write_batch_creation() {
        let mut batch = WriteBatch::new();
        assert_eq!(batch.operations.len(), 0);

        batch.add_operation(
            Uuid::new_v4(),
            WriteOperation::CreateAccount {
                owner_name: "Test User".to_string(),
                initial_balance: Decimal::new(1000, 0),
            },
        );

        assert_eq!(batch.operations.len(), 1);
    }

    #[tokio::test]
    async fn test_write_batch_should_process() {
        let mut batch = WriteBatch::new();

        // Test size-based processing
        for _ in 0..50 {
            batch.add_operation(
                Uuid::new_v4(),
                WriteOperation::CreateAccount {
                    owner_name: "Test User".to_string(),
                    initial_balance: Decimal::new(1000, 0),
                },
            );
        }

        assert!(batch.should_process(50, Duration::from_millis(100)));
        assert!(batch.is_full(50));
    }
}
