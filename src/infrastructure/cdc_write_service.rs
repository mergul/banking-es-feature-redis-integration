use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use crate::infrastructure::TransactionProjection;
use anyhow::Result;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

// CDC configuration functions that read from environment variables
fn get_cdc_num_partitions() -> usize {
    std::env::var("DB_BATCH_PROCESSOR_COUNT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(16) // Default to 8 if not set or invalid
}

fn get_cdc_default_batch_size() -> usize {
    std::env::var("CDC_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000) // Default to 1000 if not set or invalid
}

fn get_cdc_batch_timeout_ms() -> u64 {
    std::env::var("CDC_BATCH_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25) // Default to 25ms if not set or invalid
}
fn get_cdc_poll_interval_ms() -> u64 {
    std::env::var("CDC_POLL_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5) // Default to 5ms if not set or invalid
}

// OPTIMIZATION 1: Enhanced CDCBatch to support transaction projections
#[derive(Debug)]
pub struct CDCBatch {
    pub batch_id: Uuid,
    pub created_at: Instant,
    pub partition_id: Option<usize>,
    pub batch_type: CDCBatchType,

    // Account projections (AccountCreated and OtherEvents)
    pub projections: Vec<(Uuid, AccountProjection, Uuid, String)>,

    // OPTIMIZATION: Add transaction projections support
    pub transaction_projections: Vec<(Uuid, TransactionProjection, Uuid, Instant)>,
}

/// Enhanced batch type to support transaction projections
#[derive(Clone, Debug, PartialEq)]
pub enum CDCBatchType {
    AccountCreated, // INSERT only, no locking
    OtherEvents,    // UPDATE, requires locking
    Transactions,   // Transaction history INSERT only
}

impl CDCBatch {
    pub fn new_account_created_batch(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::with_capacity(1000), // Pre-allocate
            transaction_projections: Vec::new(),
            created_at: Instant::now(),
            partition_id,
            batch_type: CDCBatchType::AccountCreated,
        }
    }

    pub fn new_other_events_batch(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::with_capacity(1000), // Pre-allocate
            transaction_projections: Vec::new(),
            created_at: Instant::now(),
            partition_id,
            batch_type: CDCBatchType::OtherEvents,
        }
    }

    pub fn new_transaction_batch(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::new(),
            transaction_projections: Vec::with_capacity(2000), // Transactions are more frequent
            created_at: Instant::now(),
            partition_id,
            batch_type: CDCBatchType::Transactions,
        }
    }

    // OPTIMIZATION 2: Fast batch operations
    pub fn add_projection_with_operation_id(
        &mut self,
        aggregate_id: Uuid,
        projection: AccountProjection,
        operation_id: Uuid,
    ) {
        self.projections.push((
            aggregate_id,
            projection,
            operation_id,
            "Unknown".to_string(),
        ));
    }

    // OPTIMIZATION 3: Add transaction projection support
    pub fn add_transaction_projection(
        &mut self,
        account_id: Uuid,
        transaction: TransactionProjection,
        operation_id: Uuid,
    ) {
        self.transaction_projections
            .push((account_id, transaction, operation_id, Instant::now()));
    }

    // OPTIMIZATION 4: Smart batch processing triggers
    pub fn should_process(&self, max_size: usize, max_wait_time: Duration) -> bool {
        let is_full = match self.batch_type {
            CDCBatchType::Transactions => self.transaction_projections.len() >= max_size,
            _ => self.projections.len() >= max_size,
        };
        let is_old = self.created_at.elapsed() >= max_wait_time;
        is_full || is_old
    }

    pub fn is_empty(&self) -> bool {
        match self.batch_type {
            CDCBatchType::Transactions => self.transaction_projections.is_empty(),
            _ => self.projections.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self.batch_type {
            CDCBatchType::Transactions => self.transaction_projections.len(),
            _ => self.projections.len(),
        }
    }
}

/// OPTIMIZATION 5: Enhanced CDCBatchingService with better performance
pub struct CDCBatchingService {
    config: CDCBatchingConfig,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    write_pool: Arc<PgPool>,
    current_batch: Arc<Mutex<CDCBatch>>,
    pending_results: Arc<Mutex<HashMap<Uuid, mpsc::Sender<CDCOperationResult>>>>,
    completed_results: Arc<Mutex<HashMap<Uuid, CDCOperationResult>>>,
    batch_processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_token: CancellationToken,

    // OPTIMIZATION 6: Enhanced metrics
    batches_processed: Arc<std::sync::atomic::AtomicU64>,
    projections_processed: Arc<std::sync::atomic::AtomicU64>,
    processing_times: Arc<Mutex<Vec<Duration>>>, // For performance analysis

    partition_id: Option<usize>,
    processing_type: CDCBatchType,
}

impl CDCBatchingService {
    pub fn new_account_created_processor(
        partition_id: usize,
        config: CDCBatchingConfig,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        Self {
            config,
            projection_store,
            write_pool,
            current_batch: Arc::new(Mutex::new(CDCBatch::new_account_created_batch(Some(
                partition_id,
            )))),
            pending_results: Arc::new(Mutex::new(HashMap::with_capacity(10000))),
            completed_results: Arc::new(Mutex::new(HashMap::with_capacity(10000))),
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            batches_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            projections_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            processing_times: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            partition_id: Some(partition_id),
            processing_type: CDCBatchType::AccountCreated,
        }
    }

    pub fn new_transaction_processor(
        partition_id: usize,
        config: CDCBatchingConfig,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        Self {
            config,
            projection_store,
            write_pool,
            current_batch: Arc::new(Mutex::new(CDCBatch::new_other_events_batch(Some(
                partition_id,
            )))),
            pending_results: Arc::new(Mutex::new(HashMap::with_capacity(10000))),
            completed_results: Arc::new(Mutex::new(HashMap::with_capacity(10000))),
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            batches_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            projections_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            processing_times: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            partition_id: Some(partition_id),
            processing_type: CDCBatchType::OtherEvents,
        }
    }

    // OPTIMIZATION 7: New transaction history processor
    pub fn new_transaction_history_processor(
        partition_id: usize,
        config: CDCBatchingConfig,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        Self {
            config,
            projection_store,
            write_pool,
            current_batch: Arc::new(Mutex::new(CDCBatch::new_transaction_batch(Some(
                partition_id,
            )))),
            pending_results: Arc::new(Mutex::new(HashMap::with_capacity(20000))), // More capacity for transactions
            completed_results: Arc::new(Mutex::new(HashMap::with_capacity(20000))),
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            batches_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            projections_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            processing_times: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            partition_id: Some(partition_id),
            processing_type: CDCBatchType::Transactions,
        }
    }

    // OPTIMIZATION 8: Fast non-blocking submission
    pub async fn submit_projection_update(
        &self,
        aggregate_id: Uuid,
        projection: AccountProjection,
    ) -> Result<Uuid> {
        let operation_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);

        // OPTIMIZATION 9: Minimize lock contention with try_lock
        let mut retries = 0;
        while retries < 3 {
            // Add to pending results first
            if let Ok(mut pending_guard) = self.pending_results.try_lock() {
                pending_guard.insert(operation_id, tx);
                break;
            } else {
                retries += 1;
                tokio::task::yield_now().await;
            }
        }

        // Add to current batch with minimal lock time
        {
            let mut batch_guard = self.current_batch.lock().await;
            batch_guard.add_projection_with_operation_id(aggregate_id, projection, operation_id);

            // Log progress every 500 items for reduced logging overhead
            if batch_guard.len() % 500 == 0 {
                info!(
                    "📦 BATCH PROGRESS: Partition {} has {} projections (type: {:?})",
                    self.partition_id.unwrap_or(0),
                    batch_guard.len(),
                    self.processing_type
                );
            }
        }

        Ok(operation_id)
    }

    // OPTIMIZATION 10: Transaction projection submission
    pub async fn submit_transaction_projection(
        &self,
        account_id: Uuid,
        transaction: TransactionProjection,
    ) -> Result<Uuid> {
        let operation_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);

        // Add to pending results
        {
            let mut pending_guard = self.pending_results.lock().await;
            pending_guard.insert(operation_id, tx);
        }

        // Add to current transaction batch
        {
            let mut batch_guard = self.current_batch.lock().await;
            batch_guard.add_transaction_projection(account_id, transaction, operation_id);

            if batch_guard.transaction_projections.len() % 1000 == 0 {
                info!(
                    "💳 TRANSACTION BATCH: Partition {} has {} transactions",
                    self.partition_id.unwrap_or(0),
                    batch_guard.transaction_projections.len()
                );
            }
        }

        Ok(operation_id)
    }

    pub async fn start(&self) -> Result<()> {
        let current_batch = self.current_batch.clone();
        let pending_results = self.pending_results.clone();
        let completed_results = self.completed_results.clone();
        let projection_store = self.projection_store.clone();
        let write_pool = self.write_pool.clone();
        let config = self.config.clone();
        let batches_processed = self.batches_processed.clone();
        let projections_processed = self.projections_processed.clone();
        let processing_times = self.processing_times.clone();
        let partition_id = self.partition_id;
        let processing_type = self.processing_type.clone();
        let shutdown_token = self.shutdown_token.clone();

        let handle = tokio::spawn(async move {
            Self::process_batch_loop(
                current_batch,
                pending_results,
                completed_results,
                projection_store,
                write_pool,
                config,
                batches_processed,
                projections_processed,
                processing_times,
                partition_id,
                processing_type,
                shutdown_token,
            )
            .await;
        });

        {
            let mut handle_guard = self.batch_processor_handle.lock().await;
            *handle_guard = Some(handle);
        }

        info!(
            "🚀 CDC Processor started for partition {:?} (type: {:?})",
            self.partition_id, self.processing_type
        );
        Ok(())
    }

    // OPTIMIZATION 11: Enhanced batch processing loop
    async fn process_batch_loop(
        current_batch: Arc<Mutex<CDCBatch>>,
        pending_results: Arc<Mutex<HashMap<Uuid, mpsc::Sender<CDCOperationResult>>>>,
        completed_results: Arc<Mutex<HashMap<Uuid, CDCOperationResult>>>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
        config: CDCBatchingConfig,
        batches_processed: Arc<std::sync::atomic::AtomicU64>,
        projections_processed: Arc<std::sync::atomic::AtomicU64>,
        processing_times: Arc<Mutex<Vec<Duration>>>,
        partition_id: Option<usize>,
        processing_type: CDCBatchType,
        shutdown_token: CancellationToken,
    ) {
        // OPTIMIZATION 12: Adaptive batch timing based on load
        let base_interval = Duration::from_millis(config.max_poll_interval_ms.min(25)); // Cap at 25ms
        let mut interval = tokio::time::interval(base_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut consecutive_empty_ticks = 0;
        let mut last_batch_size = 0;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let batch_to_process = {
                        let mut batch_guard = current_batch.lock().await;

                        if batch_guard.is_empty() {
                            consecutive_empty_ticks += 1;
                            // Adaptive backoff when there's no work
                            if consecutive_empty_ticks > 10 {
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                            continue;
                        }

                        consecutive_empty_ticks = 0;
                        let current_size = batch_guard.len();

                        // OPTIMIZATION 13: Smart batch triggering
                        let should_process = batch_guard.should_process(
                            config.max_batch_size,
                            Duration::from_millis(config.max_batch_wait_time_ms),
                        );

                        if should_process {
                            info!(
                                "⏰ BATCH FLUSH: Partition {:?} processing {} items (type: {:?}, trigger: {})",
                                partition_id,
                                current_size,
                                processing_type,
                                if current_size >= config.max_batch_size { "SIZE" } else { "TIME" }
                            );

                            let new_batch = match processing_type {
                                CDCBatchType::AccountCreated => CDCBatch::new_account_created_batch(partition_id),
                                CDCBatchType::OtherEvents => CDCBatch::new_other_events_batch(partition_id),
                                CDCBatchType::Transactions => CDCBatch::new_transaction_batch(partition_id),
                            };

                            last_batch_size = current_size;
                            Some(std::mem::replace(&mut *batch_guard, new_batch))
                        } else {
                            None
                        }
                    };

                    if let Some(batch) = batch_to_process {
                        let batch_start = Instant::now();

                        // OPTIMIZATION 14: Route to appropriate executor
                        let results = match processing_type {
                            CDCBatchType::AccountCreated => {
                                Self::execute_account_created_batch(&batch, &projection_store, &config).await
                            }
                            CDCBatchType::OtherEvents => {
                                Self::execute_other_events_batch(&batch, &projection_store, &write_pool, &config).await
                            }
                            CDCBatchType::Transactions => {
                                Self::execute_transaction_batch(&batch, &projection_store, &config).await
                            }
                        };

                        let processing_duration = batch_start.elapsed();

                        // Update metrics
                        batches_processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        projections_processed.fetch_add(last_batch_size as u64, std::sync::atomic::Ordering::Relaxed);

                        // Store processing time for analysis
                        {
                            let mut times = processing_times.lock().await;
                            times.push(processing_duration);
                            if times.len() > 100 { // Keep only last 100 measurements
                                times.remove(0);
                            }
                        }

                        info!(
                            "✅ BATCH COMPLETE: Partition {:?} processed {} items in {:?} ({:.0} items/sec)",
                            partition_id,
                            last_batch_size,
                            processing_duration,
                            last_batch_size as f64 / processing_duration.as_secs_f64().max(0.001)
                        );

                        // Handle results asynchronously to avoid blocking
                        tokio::spawn(Self::handle_batch_results_async(results, pending_results.clone(), completed_results.clone()));
                    }
                }
                _ = shutdown_token.cancelled() => {
                    info!("🛑 Batch processor shutting down for partition {:?}", partition_id);
                    break;
                }
            }
        }
    }

    // OPTIMIZATION 15: Ultra-fast AccountCreated batch execution
    async fn execute_account_created_batch(
        batch: &CDCBatch,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        config: &CDCBatchingConfig,
    ) -> Vec<(Uuid, CDCOperationResult)> {
        let batch_start = Instant::now();
        let mut results = Vec::with_capacity(batch.projections.len());

        // Extract projections for bulk INSERT
        let projections: Vec<AccountProjection> = batch
            .projections
            .iter()
            .map(|(_, projection, _, _)| projection.clone())
            .collect();

        info!(
            "🚀 ACCOUNT CREATED: Bulk inserting {} new accounts",
            projections.len()
        );

        // OPTIMIZED: Single bulk INSERT operation
        match projection_store
            .bulk_insert_new_accounts_with_copy(projections)
            .await
        {
            Ok(_) => {
                // Create success results for all projections
                for (aggregate_id, _, operation_id, _) in &batch.projections {
                    results.push((
                        *operation_id,
                        CDCOperationResult {
                            operation_id: *operation_id,
                            success: true,
                            aggregate_id: Some(*aggregate_id),
                            error: None,
                            duration: batch_start.elapsed(),
                        },
                    ));
                }
            }
            Err(e) => {
                error!("❌ AccountCreated batch failed: {}", e);
                // Create failure results
                for (aggregate_id, _, operation_id, _) in &batch.projections {
                    results.push((
                        *operation_id,
                        CDCOperationResult {
                            operation_id: *operation_id,
                            success: false,
                            aggregate_id: Some(*aggregate_id),
                            error: Some(e.to_string()),
                            duration: batch_start.elapsed(),
                        },
                    ));
                }
            }
        }

        results
    }

    // OPTIMIZATION 16: Enhanced other events batch execution
    async fn execute_other_events_batch(
        batch: &CDCBatch,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        write_pool: &Arc<PgPool>,
        config: &CDCBatchingConfig,
    ) -> Vec<(Uuid, CDCOperationResult)> {
        let batch_start = Instant::now();
        let mut results = Vec::with_capacity(batch.projections.len());

        // Extract projections for bulk UPDATE
        let projections: Vec<AccountProjection> = batch
            .projections
            .iter()
            .map(|(_, projection, _, _)| projection.clone())
            .collect();

        info!(
            "🔄 OTHER EVENTS: Bulk updating {} account projections",
            projections.len()
        );

        // OPTIMIZATION 17: Retry logic with exponential backoff
        let mut retry_count = 0;
        let max_retries = config.max_retries;

        while retry_count <= max_retries {
            match projection_store
                .bulk_update_accounts_with_copy(projections.clone())
                .await
            {
                Ok(_) => {
                    // Success - create results
                    for (aggregate_id, _, operation_id, _) in &batch.projections {
                        results.push((
                            *operation_id,
                            CDCOperationResult {
                                operation_id: *operation_id,
                                success: true,
                                aggregate_id: Some(*aggregate_id),
                                error: None,
                                duration: batch_start.elapsed(),
                            },
                        ));
                    }
                    break;
                }
                Err(e) => {
                    retry_count += 1;

                    if retry_count > max_retries {
                        // Max retries exceeded - create failure results
                        error!(
                            "❌ Other events batch failed after {} retries: {}",
                            max_retries, e
                        );
                        for (aggregate_id, _, operation_id, _) in &batch.projections {
                            results.push((
                                *operation_id,
                                CDCOperationResult {
                                    operation_id: *operation_id,
                                    success: false,
                                    aggregate_id: Some(*aggregate_id),
                                    error: Some(format!(
                                        "Failed after {} retries: {}",
                                        max_retries, e
                                    )),
                                    duration: batch_start.elapsed(),
                                },
                            ));
                        }
                        break;
                    }

                    // Exponential backoff
                    let backoff = Duration::from_millis(50 * (1 << (retry_count - 1)).min(8)); // Cap at ~400ms
                    warn!(
                        "🔄 Retrying other events batch in {:?} (attempt {}/{})",
                        backoff, retry_count, max_retries
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        results
    }

    // OPTIMIZATION 18: New transaction batch executor
    async fn execute_transaction_batch(
        batch: &CDCBatch,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        config: &CDCBatchingConfig,
    ) -> Vec<(Uuid, CDCOperationResult)> {
        let batch_start = Instant::now();
        let mut results = Vec::with_capacity(batch.transaction_projections.len());

        // Extract transaction projections for bulk INSERT
        let transactions: Vec<TransactionProjection> = batch
            .transaction_projections
            .iter()
            .map(|(_, transaction, _, _)| transaction.clone())
            .collect();

        info!(
            "💳 TRANSACTIONS: Bulk inserting {} transaction records",
            transactions.len()
        );

        // OPTIMIZED: Single bulk INSERT for transaction history
        match projection_store
            .bulk_insert_transaction_projections(transactions)
            .await
        {
            Ok(_) => {
                // Create success results
                for (_, _, operation_id, _) in &batch.transaction_projections {
                    results.push((
                        *operation_id,
                        CDCOperationResult {
                            operation_id: *operation_id,
                            success: true,
                            aggregate_id: None,
                            error: None,
                            duration: batch_start.elapsed(),
                        },
                    ));
                }
            }
            Err(e) => {
                error!("❌ Transaction batch failed: {}", e);
                // Create failure results
                for (_, _, operation_id, _) in &batch.transaction_projections {
                    results.push((
                        *operation_id,
                        CDCOperationResult {
                            operation_id: *operation_id,
                            success: false,
                            aggregate_id: None,
                            error: Some(e.to_string()),
                            duration: batch_start.elapsed(),
                        },
                    ));
                }
            }
        }

        results
    }

    // OPTIMIZATION 19: Async result handling to avoid blocking main loop
    async fn handle_batch_results_async(
        results: Vec<(Uuid, CDCOperationResult)>,
        pending_results: Arc<Mutex<HashMap<Uuid, mpsc::Sender<CDCOperationResult>>>>,
        completed_results: Arc<Mutex<HashMap<Uuid, CDCOperationResult>>>,
    ) {
        let mut completed_guard = completed_results.lock().await;
        for (operation_id, result) in results {
            completed_guard.insert(operation_id, result);
        }
    }

    // OPTIMIZATION 20: Performance metrics
    pub async fn get_performance_stats(&self) -> (u64, u64, f64) {
        let batches = self
            .batches_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let projections = self
            .projections_processed
            .load(std::sync::atomic::Ordering::Relaxed);

        let avg_processing_time = {
            let times = self.processing_times.lock().await;
            if times.is_empty() {
                0.0
            } else {
                let total: Duration = times.iter().sum();
                total.as_secs_f64() / times.len() as f64
            }
        };

        (batches, projections, avg_processing_time)
    }
}

/// OPTIMIZATION 21: Enhanced PartitionedCDCBatching with transaction support
pub struct PartitionedCDCBatching {
    account_created_processor: Arc<CDCBatchingService>,
    transaction_processors: Vec<Arc<CDCBatchingService>>,
    transaction_history_processors: Vec<Arc<CDCBatchingService>>, // NEW: For transaction history
}

impl PartitionedCDCBatching {
    pub async fn new(
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        let num_partitions = get_cdc_num_partitions();
        let config = CDCBatchingConfig::default();

        // Partition 0: AccountCreated
        let account_created_processor =
            Arc::new(CDCBatchingService::new_account_created_processor(
                0,
                config.clone(),
                projection_store.clone(),
                write_pool.clone(),
            ));

        // Partitions 1 to N/2: Account updates (deposits/withdrawals)
        let mut transaction_processors = Vec::new();
        for partition_id in 1..=(num_partitions / 2) {
            let processor = Arc::new(CDCBatchingService::new_transaction_processor(
                partition_id,
                config.clone(),
                projection_store.clone(),
                write_pool.clone(),
            ));
            transaction_processors.push(processor);
        }

        // Partitions N/2+1 to N: Transaction history
        let mut transaction_history_processors = Vec::new();
        for partition_id in (num_partitions / 2 + 1)..=num_partitions {
            let processor = Arc::new(CDCBatchingService::new_transaction_history_processor(
                partition_id,
                config.clone(),
                projection_store.clone(),
                write_pool.clone(),
            ));
            transaction_history_processors.push(processor);
        }

        Self {
            account_created_processor,
            transaction_processors,
            transaction_history_processors,
        }
    }

    pub async fn start(&self) -> Result<()> {
        // Start all processors
        self.account_created_processor.start().await?;

        for processor in &self.transaction_processors {
            processor.start().await?;
        }

        for processor in &self.transaction_history_processors {
            processor.start().await?;
        }

        info!(
            "🚀 All CDC processors started: {} account + {} transaction + {} history processors",
            1,
            self.transaction_processors.len(),
            self.transaction_history_processors.len()
        );
        Ok(())
    }

    // OPTIMIZATION 22: Optimized bulk submissions
    pub async fn submit_account_created_projections_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        let mut operation_ids = Vec::with_capacity(projections.len());

        info!(
            "🚀 Submitting {} AccountCreated projections",
            projections.len()
        );

        for (aggregate_id, projection) in projections {
            let operation_id = self
                .account_created_processor
                .submit_projection_update(aggregate_id, projection)
                .await?;
            operation_ids.push(operation_id);
        }

        Ok(operation_ids)
    }

    pub async fn submit_other_events_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        let processor_count = self.transaction_processors.len();
        let mut operation_ids = Vec::with_capacity(projections.len());

        // OPTIMIZATION 23: Distribute across processors by aggregate_id hash
        let mut processor_groups: Vec<Vec<(Uuid, AccountProjection)>> =
            vec![Vec::new(); processor_count];

        for (aggregate_id, projection) in projections {
            let processor_idx = (aggregate_id.as_u128() as usize) % processor_count;
            processor_groups[processor_idx].push((aggregate_id, projection));
        }

        // Submit to processors in parallel
        let mut submission_tasks = Vec::new();

        for (processor_idx, processor_projections) in processor_groups.into_iter().enumerate() {
            if processor_projections.is_empty() {
                continue;
            }

            let processor = self.transaction_processors[processor_idx].clone();
            let task = tokio::spawn(async move {
                let mut processor_operation_ids = Vec::new();

                for (aggregate_id, projection) in processor_projections {
                    match processor
                        .submit_projection_update(aggregate_id, projection)
                        .await
                    {
                        Ok(operation_id) => processor_operation_ids.push(operation_id),
                        Err(e) => return Err(e),
                    }
                }

                Ok(processor_operation_ids)
            });

            submission_tasks.push(task);
        }

        // Wait for all submissions
        for task in submission_tasks {
            match task.await? {
                Ok(mut processor_ids) => operation_ids.append(&mut processor_ids),
                Err(e) => return Err(e),
            }
        }

        info!(
            "✅ Submitted {} other event operations across {} processors",
            operation_ids.len(),
            processor_count
        );
        Ok(operation_ids)
    }

    // OPTIMIZATION 24: High-performance transaction projection submission
    pub async fn submit_transaction_projections_ultra_parallel(
        &self,
        projections: Vec<TransactionProjection>,
    ) -> Result<Vec<Uuid>> {
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        let start_time = Instant::now();
        info!(
            "🚀 ULTRA-PARALLEL: Processing {} transactions",
            projections.len()
        );

        // OPTION 1: Maximum concurrency with semaphore for rate limiting
        let max_concurrent = 200; // Adjust based on your system capacity
        let semaphore = Arc::new(Semaphore::new(max_concurrent));

        let processor_count = self.transaction_history_processors.len();

        // Create futures for ALL transactions at once
        let transaction_futures = projections.into_iter().map(|transaction| {
            let semaphore = semaphore.clone();
            let processors = self.transaction_history_processors.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                // Select processor using same hash logic
                let account_hash = transaction.account_id.as_u128() as usize;
                let time_hash = transaction.timestamp.timestamp() as usize;
                let processor_idx = (account_hash.wrapping_add(time_hash)) % processor_count;

                let processor = &processors[processor_idx];

                processor
                    .submit_transaction_projection(transaction.account_id, transaction)
                    .await
            }
        });

        // Execute ALL transactions concurrently (with semaphore limiting)
        let operation_ids = futures::future::try_join_all(transaction_futures).await?;

        let duration = start_time.elapsed();
        info!(
            "🏆 ULTRA-PARALLEL COMPLETE: {} operations in {:?} ({:.0} ops/sec)",
            operation_ids.len(),
            duration,
            operation_ids.len() as f64 / duration.as_secs_f64()
        );

        Ok(operation_ids)
    }

    // OPTIMIZATION 27: Performance monitoring and health checks
    pub async fn get_system_performance(&self) -> SystemPerformanceStats {
        let account_stats = self.account_created_processor.get_performance_stats().await;

        let mut transaction_stats = Vec::new();
        for processor in &self.transaction_processors {
            transaction_stats.push(processor.get_performance_stats().await);
        }

        let mut history_stats = Vec::new();
        for processor in &self.transaction_history_processors {
            history_stats.push(processor.get_performance_stats().await);
        }

        SystemPerformanceStats {
            account_created: account_stats,
            transaction_processors: transaction_stats,
            transaction_history: history_stats,
            total_partitions: 1
                + self.transaction_processors.len()
                + self.transaction_history_processors.len(),
        }
    }

    // OPTIMIZATION 28: Graceful shutdown with pending batch processing
    pub async fn shutdown(&self) -> Result<()> {
        info!("🛑 Initiating graceful shutdown of CDC batching system...");

        // Cancel all processors
        self.account_created_processor.shutdown_token.cancel();

        for processor in &self.transaction_processors {
            processor.shutdown_token.cancel();
        }

        for processor in &self.transaction_history_processors {
            processor.shutdown_token.cancel();
        }

        // Wait for all processors to finish their current batches
        let mut shutdown_tasks = Vec::new();

        // Account created processor
        if let Some(handle) = self
            .account_created_processor
            .batch_processor_handle
            .lock()
            .await
            .take()
        {
            shutdown_tasks.push(handle);
        }

        // Transaction processors
        for processor in &self.transaction_processors {
            if let Some(handle) = processor.batch_processor_handle.lock().await.take() {
                shutdown_tasks.push(handle);
            }
        }

        // Transaction history processors
        for processor in &self.transaction_history_processors {
            if let Some(handle) = processor.batch_processor_handle.lock().await.take() {
                shutdown_tasks.push(handle);
            }
        }

        // Wait for all tasks to complete with timeout
        let shutdown_timeout = Duration::from_secs(30);
        match tokio::time::timeout(shutdown_timeout, futures::future::join_all(shutdown_tasks))
            .await
        {
            Ok(_) => {
                info!("✅ CDC batching system shutdown completed successfully");
                Ok(())
            }
            Err(_) => {
                warn!(
                    "⚠️ CDC batching system shutdown timed out after {:?}",
                    shutdown_timeout
                );
                Err(anyhow::anyhow!("Shutdown timeout"))
            }
        }
    }

    // OPTIMIZATION 29: Health check for monitoring
    pub async fn health_check(&self) -> HealthStatus {
        let mut healthy_processors = 0;
        let mut total_processors = 0;
        let mut issues = Vec::new();

        // Check account created processor
        total_processors += 1;
        if self
            .account_created_processor
            .batch_processor_handle
            .lock()
            .await
            .is_some()
        {
            healthy_processors += 1;
        } else {
            issues.push("AccountCreated processor not running".to_string());
        }

        // Check transaction processors
        for (idx, processor) in self.transaction_processors.iter().enumerate() {
            total_processors += 1;
            if processor.batch_processor_handle.lock().await.is_some() {
                healthy_processors += 1;
            } else {
                issues.push(format!("Transaction processor {} not running", idx));
            }
        }

        // Check transaction history processors
        for (idx, processor) in self.transaction_history_processors.iter().enumerate() {
            total_processors += 1;
            if processor.batch_processor_handle.lock().await.is_some() {
                healthy_processors += 1;
            } else {
                issues.push(format!("Transaction history processor {} not running", idx));
            }
        }

        HealthStatus {
            healthy: issues.is_empty(),
            healthy_processors,
            total_processors,
            issues,
        }
    }
}

// OPTIMIZATION 30: Performance monitoring structures
#[derive(Debug, Clone)]
pub struct SystemPerformanceStats {
    pub account_created: (u64, u64, f64), // (batches, projections, avg_time)
    pub transaction_processors: Vec<(u64, u64, f64)>,
    pub transaction_history: Vec<(u64, u64, f64)>,
    pub total_partitions: usize,
}

impl SystemPerformanceStats {
    pub fn total_throughput(&self) -> f64 {
        let total_projections: u64 = self.account_created.1
            + self
                .transaction_processors
                .iter()
                .map(|(_, p, _)| p)
                .sum::<u64>()
            + self
                .transaction_history
                .iter()
                .map(|(_, p, _)| p)
                .sum::<u64>();

        let avg_time = (self.account_created.2
            + self
                .transaction_processors
                .iter()
                .map(|(_, _, t)| t)
                .sum::<f64>()
            + self
                .transaction_history
                .iter()
                .map(|(_, _, t)| t)
                .sum::<f64>())
            / self.total_partitions as f64;

        if avg_time > 0.0 {
            total_projections as f64 / avg_time
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub healthy_processors: usize,
    pub total_processors: usize,
    pub issues: Vec<String>,
}

// OPTIMIZATION 31: Enhanced configuration with adaptive settings
#[derive(Clone, Debug)]
pub struct CDCBatchingConfig {
    pub max_batch_size: usize,
    pub max_batch_wait_time_ms: u64,
    pub max_poll_interval_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,

    // OPTIMIZATION 32: Adaptive settings
    pub adaptive_batching: bool,
    pub high_load_batch_size: usize, // Larger batches under high load
    pub low_load_batch_size: usize,  // Smaller batches under low load
    pub load_threshold: usize,       // Threshold to switch batch sizes
}

impl Default for CDCBatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: get_cdc_default_batch_size(), // Standard batch size
            max_batch_wait_time_ms: get_cdc_batch_timeout_ms(), // Fast response time
            max_poll_interval_ms: get_cdc_poll_interval_ms(), // Minimal poll interval
            max_retries: 3,
            retry_backoff_ms: 50,

            // Adaptive settings
            adaptive_batching: true,
            high_load_batch_size: 2000, // Larger batches for efficiency
            low_load_batch_size: 500,   // Smaller batches for responsiveness
            load_threshold: 10000,      // Switch threshold
        }
    }
}

#[derive(Debug, Clone)]
pub struct CDCOperationResult {
    pub operation_id: Uuid,
    pub success: bool,
    pub aggregate_id: Option<Uuid>,
    pub error: Option<String>,
    pub duration: Duration,
}
