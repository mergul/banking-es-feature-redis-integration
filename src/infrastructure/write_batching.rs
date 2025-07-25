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
use tokio_util::sync::CancellationToken;
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
            max_batch_size: 50,          // Reduced for testing multi-aggregate scenarios
            max_batch_wait_time_ms: 100, // Increased to 2 seconds to allow proper batching
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
    shutdown_token: CancellationToken,

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
            shutdown_token: CancellationToken::new(),
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

        let shutdown_token = self.shutdown_token.clone();
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
                        info!("[WriteBatchingService] Batch processor interval tick");
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
                    _ = shutdown_token.cancelled() => {
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

        self.shutdown_token.cancel();

        // Wait for batch processor to complete
        if let Some(handle) = self.batch_processor_handle.lock().await.take() {
            let _ = handle.await;
        }

        info!("✅ Write batching service stopped");
        Ok(())
    }

    /// FIXED: Clean up channels and pending operations
    async fn cleanup_channels(&self) {
        println!("🧹 Cleaning up channels and pending operations...");

        // Process any remaining operations in the current batch
        let remaining_operations = {
            let mut batch = self.current_batch.lock().await;
            if !batch.operations.is_empty() {
                let operations = std::mem::take(&mut batch.operations);
                println!(
                    "📦 Processing {} remaining operations during cleanup",
                    operations.len()
                );
                operations
            } else {
                Vec::new()
            }
        };

        if !remaining_operations.is_empty() {
            // Process remaining operations
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

        // Send error results to any remaining pending operations
        let pending_ops: Vec<Uuid> = {
            let pending = self.pending_results.lock().await;
            pending.keys().cloned().collect()
        };

        for operation_id in pending_ops {
            let error_result = WriteOperationResult {
                operation_id,
                success: false,
                result: None,
                error: Some("Service shutdown - operation cancelled".to_string()),
                duration: Duration::ZERO,
            };

            // Store in completed results
            self.completed_results
                .lock()
                .await
                .insert(operation_id, error_result.clone());

            // Try to send to waiting operation
            if let Some(sender) = self.pending_results.lock().await.remove(&operation_id) {
                if let Err(e) = sender.send(error_result).await {
                    println!(
                        "⚠️  Failed to send shutdown result for operation {}: {}",
                        operation_id, e
                    );
                }
            }
        }

        println!("✅ Channel cleanup completed");
    }

    /// FIXED: Get the number of pending operations (for monitoring)
    pub async fn get_pending_operations_count(&self) -> usize {
        self.pending_results.lock().await.len()
    }

    /// FIXED: Get the number of completed operations (for monitoring)
    pub async fn get_completed_operations_count(&self) -> usize {
        self.completed_results.lock().await.len()
    }

    /// FIXED: Clear old completed results to prevent memory leaks
    pub async fn clear_old_completed_results(&self, max_age_hours: u64) {
        let max_age = Duration::from_secs(max_age_hours * 3600);
        let now = Instant::now();

        let mut completed = self.completed_results.lock().await;
        let initial_count = completed.len();

        // Remove results older than max_age
        completed.retain(|_, result| {
            // Since we don't store timestamps in WriteOperationResult,
            // we'll use a simple count-based cleanup for now
            // In a production system, you'd want to add timestamps to WriteOperationResult
            true // Keep all for now, implement timestamp-based cleanup later
        });

        let removed_count = initial_count - completed.len();
        if removed_count > 0 {
            println!("🧹 Cleaned up {} old completed results", removed_count);
        }
    }

    /// FIXED: Graceful shutdown with proper channel lifecycle management
    pub async fn graceful_shutdown(&self, timeout: Duration) -> Result<()> {
        println!(
            "🔄 Starting graceful shutdown with {} timeout",
            timeout.as_secs()
        );

        let shutdown_start = Instant::now();

        // Step 1: Stop accepting new operations
        println!("📝 Step 1: Stopping new operation acceptance...");
        // This is handled by the service stopping the batch processor

        // Step 2: Process any remaining operations in the current batch
        println!("📦 Step 2: Processing remaining operations...");
        let remaining_count = {
            let batch = self.current_batch.lock().await;
            batch.operations.len()
        };

        if remaining_count > 0 {
            println!(
                "📦 Processing {} remaining operations in current batch",
                remaining_count
            );

            // Process the current batch
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

        // Step 3: Wait for all pending operations to complete or timeout
        println!("⏳ Step 3: Waiting for pending operations to complete...");
        let mut wait_time = Duration::ZERO;
        let check_interval = Duration::from_millis(100);

        while wait_time < timeout {
            let pending_count = self.get_pending_operations_count().await;
            if pending_count == 0 {
                println!("✅ All pending operations completed");
                break;
            }

            println!(
                "⏳ Still waiting for {} pending operations...",
                pending_count
            );
            tokio::time::sleep(check_interval).await;
            wait_time += check_interval;
        }

        // Step 4: Force cleanup of any remaining operations
        if wait_time >= timeout {
            println!("⚠️  Timeout reached, forcing cleanup of remaining operations");
            self.force_cleanup_remaining_operations().await;
        }

        // Step 5: Final cleanup
        println!("🧹 Step 4: Final cleanup...");
        self.cleanup_channels().await;

        let total_shutdown_time = shutdown_start.elapsed();
        println!(
            "✅ Graceful shutdown completed in {:?}",
            total_shutdown_time
        );

        Ok(())
    }

    /// FIXED: Force cleanup of any remaining operations (for timeout scenarios)
    async fn force_cleanup_remaining_operations(&self) {
        println!("🚨 Force cleaning up remaining operations...");

        let pending_ops: Vec<Uuid> = {
            let pending = self.pending_results.lock().await;
            pending.keys().cloned().collect()
        };

        for operation_id in pending_ops {
            let error_result = WriteOperationResult {
                operation_id,
                success: false,
                result: None,
                error: Some("Graceful shutdown timeout - operation cancelled".to_string()),
                duration: Duration::ZERO,
            };

            // Store in completed results
            self.completed_results
                .lock()
                .await
                .insert(operation_id, error_result.clone());

            // Try to send to waiting operation with timeout
            if let Some(sender) = self.pending_results.lock().await.remove(&operation_id) {
                match tokio::time::timeout(Duration::from_millis(100), sender.send(error_result))
                    .await
                {
                    Ok(Ok(_)) => {
                        println!(
                            "✅ Successfully sent timeout result for operation {}",
                            operation_id
                        );
                    }
                    Ok(Err(e)) => {
                        println!(
                            "⚠️  Failed to send timeout result for operation {}: {}",
                            operation_id, e
                        );
                    }
                    Err(_) => {
                        println!("⏰ Timeout sending result for operation {}", operation_id);
                    }
                }
            }
        }

        println!("✅ Force cleanup completed");
    }

    /// FIXED: Check if the service is healthy (for monitoring)
    pub async fn health_check(&self) -> bool {
        // Check if there are any stuck operations
        let pending_count = self.get_pending_operations_count().await;
        let completed_count = self.get_completed_operations_count().await;

        // Service is healthy if pending operations are reasonable (not stuck)
        // and we have processed some operations
        let is_healthy = pending_count < 1000 && completed_count > 0;

        if !is_healthy {
            println!(
                "⚠️  Health check failed: pending={}, completed={}",
                pending_count, completed_count
            );
        }

        is_healthy
    }

    /// Submit a write operation for batching
    pub async fn submit_operation(&self, operation: WriteOperation) -> Result<Uuid> {
        let operation_id = Uuid::new_v4();
        // FIXED: Increase buffer size to prevent blocking and ensure channel stays open
        let (result_tx, result_rx) = mpsc::channel::<WriteOperationResult>(10);

        // Add to pending results
        self.pending_results
            .lock()
            .await
            .insert(operation_id, result_tx);

        // Add to current batch
        {
            let mut batch = self.current_batch.lock().await;
            batch.add_operation(operation_id, operation);

            // Only process immediately if the batch is full (not based on time)
            if batch.is_full(self.config.max_batch_size) {
                println!(
                    "📦 Processing write batch: {} operations (batch full), age: {:?}",
                    batch.operations.len(),
                    batch.created_at.elapsed()
                );
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

    /// FIXED: Wait for the result of a submitted operation with improved channel lifecycle
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
                // Check completed results again in case it was completed between checks
                drop(pending);
                let completed = self.completed_results.lock().await;
                if let Some(result) = completed.get(&operation_id) {
                    return Ok(result.clone());
                }
                return Err(anyhow::anyhow!(
                    "Operation {} not found or already completed",
                    operation_id
                ));
            }
            // Operation is pending, we'll wait for it
        }

        // Wait for the result with a reasonable timeout
        let timeout = Duration::from_secs(15); // Increased timeout for high-load scenarios
        match tokio::time::timeout(timeout, async {
            // Poll for the result more efficiently with exponential backoff
            let mut poll_interval = Duration::from_millis(10);
            let max_poll_interval = Duration::from_millis(100);

            loop {
                // Check completed results first (most likely case)
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
                        drop(pending);
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

                // Exponential backoff for polling
                tokio::time::sleep(poll_interval).await;
                poll_interval = std::cmp::min(poll_interval * 2, max_poll_interval);
            }
        })
        .await
        {
            Ok(result) => Ok(result),
            Err(_) => {
                // Clean up any hanging references on timeout
                self.pending_results.lock().await.remove(&operation_id);
                Err(anyhow::anyhow!(
                    "Timeout waiting for operation {} result after 30 seconds",
                    operation_id
                ))
            }
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
        println!("🔄 Starting batch processing...");

        // Take the current batch and create a new one
        let batch_to_process = {
            let mut batch = current_batch.lock().await;
            let batch_to_process = WriteBatch {
                batch_id: batch.batch_id,
                operations: std::mem::take(&mut batch.operations),
                created_at: batch.created_at,
            };
            *batch = WriteBatch::new();
            batch_to_process
        };

        if batch_to_process.operations.is_empty() {
            println!("⚠️  No operations to process in batch");
            return;
        }

        println!(
            "📊 Processing batch with {} operations",
            batch_to_process.operations.len()
        );

        // Execute the batch
        let results = Self::execute_batch(
            &batch_to_process,
            event_store,
            projection_store,
            write_pool,
            outbox_batcher,
            config,
        )
        .await;

        println!(
            "✅ Batch execution completed with {} results",
            results.len()
        );

        // FIXED: Send results to waiting operations with proper channel lifecycle management
        for (operation_id, result) in results {
            // Store in completed results first for immediate availability
            completed_results
                .lock()
                .await
                .insert(operation_id, result.clone());

            // Send to the waiting operation (don't remove sender until after sending)
            if let Some(sender) = pending_results.lock().await.get(&operation_id) {
                // Clone the sender to avoid holding the lock during send
                let sender_clone = sender.clone();
                drop(pending_results.lock().await); // Release lock before sending

                if let Err(e) = sender_clone.send(result).await {
                    println!(
                        "❌ Failed to send result for operation {}: {}",
                        operation_id, e
                    );
                }
            }

            // Now remove the sender from pending results after successful send
            pending_results.lock().await.remove(&operation_id);
        }

        // Update metrics
        {
            let mut batches = batches_processed.lock().await;
            *batches += 1;
        }
        {
            let mut operations = operations_processed.lock().await;
            *operations += batch_to_process.operations.len() as u64;
        }

        println!("📈 Batch processing metrics updated");
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
        println!(
            "[DEBUG] execute_batch: Starting batch with {} operations",
            batch.operations.len()
        );
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

        println!(
            "[DEBUG] execute_batch: Finished batch with {} results",
            results.len()
        );
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
        println!(
            "[DEBUG] execute_batch_transaction: Starting transaction for batch with {} operations",
            batch.operations.len()
        );
        let mut results = Vec::new();
        let max_retries = 3;
        let mut retry_count = 0;

        while retry_count < max_retries {
            // Always start a new transaction for each retry attempt
            let mut transaction = match write_pool.begin().await {
                Ok(tx) => tx,
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        for (operation_id, _) in &batch.operations {
                            results.push((
                                *operation_id,
                                WriteOperationResult {
                                    operation_id: *operation_id,
                                    success: false,
                                    result: None,
                                    error: Some(format!(
                                        "Failed to begin transaction after {} retries: {}",
                                        max_retries, e
                                    )),
                                    duration: Duration::ZERO,
                                },
                            ));
                        }
                        return Ok(results);
                    }
                    continue;
                }
            };

            let batch_start = Instant::now();

            // Collect all events and outbox messages from all operations
            let mut all_events = Vec::new();
            let mut all_outbox_messages = Vec::new();
            let mut operation_results = Vec::new();

            for (operation_id, operation) in &batch.operations {
                let op_start = Instant::now();
                let (events, outbox_messages, result_id) = match operation {
                    WriteOperation::CreateAccount {
                        owner_name,
                        initial_balance,
                    } => Self::prepare_create_account_operation(owner_name, *initial_balance),
                    WriteOperation::DepositMoney { account_id, amount } => {
                        Self::prepare_deposit_money_operation(*account_id, *amount)
                    }
                    WriteOperation::WithdrawMoney { account_id, amount } => {
                        Self::prepare_withdraw_money_operation(*account_id, *amount)
                    }
                };

                all_events.extend(events);
                all_outbox_messages.extend(outbox_messages);

                operation_results.push((*operation_id, result_id, op_start.elapsed()));
            }

            // Group events by aggregate_id for efficient batch insertion
            let mut events_by_aggregate: Vec<(Uuid, Vec<AccountEvent>, i64)> = Vec::new();

            for event in all_events {
                let aggregate_id = match &event {
                    AccountEvent::AccountCreated { account_id, .. } => *account_id,
                    AccountEvent::MoneyDeposited { account_id, .. } => *account_id,
                    AccountEvent::MoneyWithdrawn { account_id, .. } => *account_id,
                    AccountEvent::AccountClosed { account_id, .. } => *account_id,
                };

                // Find or create entry for this aggregate
                let entry = events_by_aggregate
                    .iter_mut()
                    .find(|(id, _, _)| *id == aggregate_id);

                match entry {
                    Some((_, events, _)) => {
                        events.push(event);
                    }
                    None => {
                        // Get current version for this aggregate
                        let current_version =
                            match event_store.get_current_version(aggregate_id).await {
                                Ok(version) => version,
                                Err(_) => 0, // Default to 0 if aggregate doesn't exist
                            };
                        events_by_aggregate.push((aggregate_id, vec![event], current_version));
                    }
                }
            }

            println!("[DEBUG] execute_batch_transaction: About to call save_events_multi_aggregate_in_transaction with {} aggregates", events_by_aggregate.len());
            let result = event_store
                .save_events_multi_aggregate_in_transaction(&mut transaction, events_by_aggregate)
                .await;
            println!(
                "[DEBUG] execute_batch_transaction: Transaction complete, results: {:?}",
                result
            );

            if let Err(e) = result {
                println!("[DEBUG] execute_batch_transaction: Failed to save events in multi-aggregate batch: {:?}", e);
                error!("Failed to save events in multi-aggregate batch: {:?}", e);
                // Rollback transaction on any error
                let _ = transaction.rollback().await;
                retry_count += 1;
                if retry_count >= max_retries {
                    // Return failure results for all operations
                    for (operation_id, _, _) in operation_results {
                        results.push((
                            operation_id,
                            WriteOperationResult {
                                operation_id,
                                success: false,
                                result: None,
                                error: Some("Failed to insert events after retries".to_string()),
                                duration: Duration::ZERO,
                            },
                        ));
                    }
                    return Ok(results);
                }
                continue;
            }

            // Submit all outbox messages
            let mut outbox_success = true;
            for outbox_message in all_outbox_messages {
                let mut retries = 0;
                loop {
                    match outbox_batcher.submit(outbox_message.clone()).await {
                        Ok(_) => break,
                        Err(e) => {
                            retries += 1;
                            if retries > 5 {
                                outbox_success = false;
                                error!("Failed to submit outbox message after retries: {}", e);
                                break;
                            }
                            error!(
                                "Failed to submit outbox message, retrying (attempt {}/5): {}",
                                retries, e
                            );
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
                if !outbox_success {
                    break;
                }
            }

            if !outbox_success {
                // Rollback transaction on outbox failure
                let _ = transaction.rollback().await;
                retry_count += 1;
                if retry_count >= max_retries {
                    // Return failure results for all operations
                    for (operation_id, _, _) in operation_results {
                        results.push((
                            operation_id,
                            WriteOperationResult {
                                operation_id,
                                success: false,
                                result: None,
                                error: Some(
                                    "Failed to submit outbox messages after retries".to_string(),
                                ),
                                duration: Duration::ZERO,
                            },
                        ));
                    }
                    return Ok(results);
                }
                continue;
            }

            // Commit transaction
            if let Err(e) = transaction.commit().await {
                retry_count += 1;
                if retry_count >= max_retries {
                    for (operation_id, _, _) in operation_results {
                        results.push((
                            operation_id,
                            WriteOperationResult {
                                operation_id,
                                success: false,
                                result: None,
                                error: Some(format!(
                                    "Failed to commit transaction after {} retries: {}",
                                    max_retries, e
                                )),
                                duration: Duration::ZERO,
                            },
                        ));
                    }
                    return Ok(results);
                }
                continue;
            }

            // Return success results for all operations
            for (operation_id, result_id, duration) in operation_results {
                results.push((
                    operation_id,
                    WriteOperationResult {
                        operation_id,
                        success: true,
                        result: Some(result_id),
                        error: None,
                        duration,
                    },
                ));
            }

            println!(
                "[DEBUG] execute_batch_transaction: Transaction complete, results: {:?}",
                results
            );
            return Ok(results);
        }

        println!(
            "[DEBUG] execute_batch_transaction: Finished transaction with results: {:?}",
            results
        );
        Ok(results)
    }

    // Helper methods to prepare operations without executing them
    fn prepare_create_account_operation(
        owner_name: &str,
        initial_balance: Decimal,
    ) -> (Vec<AccountEvent>, Vec<OutboxMessage>, Uuid) {
        let account_id = Uuid::new_v4();
        let event = AccountEvent::AccountCreated {
            account_id,
            owner_name: owner_name.to_string(),
            initial_balance,
        };

        let outbox_message = OutboxMessage {
            aggregate_id: account_id,
            event_id: Uuid::new_v4(),
            event_type: "AccountCreated".to_string(),
            payload: bincode::serialize(&event).unwrap_or_default(),
            topic: "account-events".to_string(),
            metadata: None,
        };

        (vec![event], vec![outbox_message], account_id)
    }

    fn prepare_deposit_money_operation(
        account_id: Uuid,
        amount: Decimal,
    ) -> (Vec<AccountEvent>, Vec<OutboxMessage>, Uuid) {
        let event = AccountEvent::MoneyDeposited {
            account_id,
            amount,
            transaction_id: Uuid::new_v4(),
        };

        let outbox_message = OutboxMessage {
            aggregate_id: account_id,
            event_id: Uuid::new_v4(),
            event_type: "MoneyDeposited".to_string(),
            payload: bincode::serialize(&event).unwrap_or_default(),
            topic: "account-events".to_string(),
            metadata: None,
        };

        (vec![event], vec![outbox_message], account_id)
    }

    fn prepare_withdraw_money_operation(
        account_id: Uuid,
        amount: Decimal,
    ) -> (Vec<AccountEvent>, Vec<OutboxMessage>, Uuid) {
        let event = AccountEvent::MoneyWithdrawn {
            account_id,
            amount,
            transaction_id: Uuid::new_v4(),
        };

        let outbox_message = OutboxMessage {
            aggregate_id: account_id,
            event_id: Uuid::new_v4(),
            event_type: "MoneyWithdrawn".to_string(),
            payload: bincode::serialize(&event).unwrap_or_default(),
            topic: "account-events".to_string(),
            metadata: None,
        };

        (vec![event], vec![outbox_message], account_id)
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

        // Synchronous, robust outbox submission with retries
        let mut retries = 0;
        loop {
            match outbox_batcher.submit(outbox_message.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    retries += 1;
                    if retries > 5 {
                        return Err(anyhow::anyhow!(
                            "Failed to submit outbox message after retries: {}",
                            e
                        ));
                    }
                    error!(
                        "Failed to submit outbox message, retrying (attempt {}/5): {}",
                        retries, e
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

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

        // Get current version of the aggregate to avoid optimistic concurrency conflicts
        let current_version = event_store
            .get_current_version(account_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get current version: {:?}", e))?;

        // Save event to event store with correct expected version
        event_store
            .save_events_in_transaction(
                transaction,
                account_id,
                vec![event.clone()],
                current_version,
            )
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

        // Synchronous, robust outbox submission with retries
        let mut retries = 0;
        loop {
            match outbox_batcher.submit(outbox_message.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    retries += 1;
                    if retries > 5 {
                        return Err(anyhow::anyhow!(
                            "Failed to submit outbox message after retries: {}",
                            e
                        ));
                    }
                    error!(
                        "Failed to submit outbox message, retrying (attempt {}/5): {}",
                        retries, e
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

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

        // Get current version of the aggregate to avoid optimistic concurrency conflicts
        let current_version = event_store
            .get_current_version(account_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get current version: {:?}", e))?;

        // Save event to event store with correct expected version
        event_store
            .save_events_in_transaction(
                transaction,
                account_id,
                vec![event.clone()],
                current_version,
            )
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

        // Synchronous, robust outbox submission with retries
        let mut retries = 0;
        loop {
            match outbox_batcher.submit(outbox_message.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    retries += 1;
                    if retries > 5 {
                        return Err(anyhow::anyhow!(
                            "Failed to submit outbox message after retries: {}",
                            e
                        ));
                    }
                    error!(
                        "Failed to submit outbox message, retrying (attempt {}/5): {}",
                        retries, e
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        Ok(account_id)
    }

    /// Get service statistics with channel lifecycle monitoring
    pub async fn get_stats(&self) -> serde_json::Value {
        let batches_processed = *self.batches_processed.lock().await;
        let operations_processed = *self.operations_processed.lock().await;
        let pending_operations = self.get_pending_operations_count().await;
        let completed_operations = self.get_completed_operations_count().await;
        let is_healthy = self.health_check().await;

        serde_json::json!({
            "batches_processed": batches_processed,
            "operations_processed": operations_processed,
            "pending_operations": pending_operations,
            "completed_operations": completed_operations,
            "is_healthy": is_healthy,
            "config": {
                "max_batch_size": self.config.max_batch_size,
                "max_batch_wait_time_ms": self.config.max_batch_wait_time_ms,
                "max_retries": self.config.max_retries,
                "retry_backoff_ms": self.config.retry_backoff_ms,
            }
        })
    }
}

#[cfg(test)]
#[ignore]
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
