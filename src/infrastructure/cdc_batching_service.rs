use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use crate::infrastructure::write_batching::BulkInsertConfigManager;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use futures;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::os::unix::process;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// CDC configuration functions that read from environment variables
fn get_cdc_num_partitions() -> usize {
    std::env::var("DB_BATCH_PROCESSOR_COUNT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8) // Default to 8 if not set or invalid
}

fn get_cdc_default_batch_size() -> usize {
    std::env::var("CDC_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(2000) // Default to 2000 if not set or invalid
}

fn get_cdc_batch_timeout_ms() -> u64 {
    std::env::var("CDC_BATCH_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25) // Default to 25ms if not set or invalid
}

// Legacy constants for backward compatibility (deprecated)
const CDC_NUM_PARTITIONS: usize = 8; // DEPRECATED: Use get_cdc_num_partitions() instead
const CDC_DEFAULT_BATCH_SIZE: usize = 2000; // DEPRECATED: Use get_cdc_default_batch_size() instead
const CDC_BATCH_TIMEOUT_MS: u64 = 25; // DEPRECATED: Use get_cdc_batch_timeout_ms() instead

/// Trait for CDC Batching Service operations
#[async_trait]
pub trait CDCBatchingServiceTrait: Send + Sync {
    /// Submit a single projection update
    async fn submit_projection_update(
        &self,
        aggregate_id: Uuid,
        projection: AccountProjection,
    ) -> Result<Uuid>;

    /// Submit multiple projection updates in bulk
    async fn submit_projections_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>>;

    /// Wait for operation result
    async fn wait_for_result(&self, operation_id: Uuid) -> Result<CDCOperationResult>;
}

#[derive(Clone)]
pub struct CDCBatchingConfig {
    pub max_batch_size: usize,
    pub max_batch_wait_time_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

impl Default for CDCBatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: get_cdc_default_batch_size(),
            max_batch_wait_time_ms: get_cdc_batch_timeout_ms(),
            max_retries: 2,
            retry_backoff_ms: 5,
        }
    }
}

pub struct CDCOperationResult {
    pub operation_id: Uuid,
    pub success: bool,
    pub aggregate_id: Option<Uuid>,
    pub error: Option<String>,
    pub duration: Duration,
}

impl Clone for CDCOperationResult {
    fn clone(&self) -> Self {
        Self {
            operation_id: self.operation_id,
            success: self.success,
            aggregate_id: self.aggregate_id,
            error: self.error.clone(),
            duration: self.duration,
        }
    }
}
/// Enhanced CDCBatch with event type separation
pub struct CDCBatch {
    pub batch_id: Uuid,
    pub projections: Vec<(Uuid, AccountProjection, Uuid, String)>, // (aggregate_id, projection, operation_id, event_type)
    pub created_at: Instant,
    pub partition_id: Option<usize>,
    pub batch_type: CDCBatchType, // NEW: Identifies the batch type
}

/// Batch type to distinguish processing requirements
#[derive(Clone, Debug, PartialEq)]
pub enum CDCBatchType {
    AccountCreated, // INSERT only, no locking, no state checking
    OtherEvents,    // UPDATE, requires locking, state checking, etc.
}

impl CDCBatch {
    pub fn new(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::new(),
            created_at: Instant::now(),
            partition_id,
            batch_type: CDCBatchType::OtherEvents, // Default to OtherEvents
        }
    }
    pub fn new_account_created_batch(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::new(),
            created_at: Instant::now(),
            partition_id,
            batch_type: CDCBatchType::AccountCreated, // Specialized for AccountCreated
        }
    }

    pub fn new_other_events_batch(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::new(),
            created_at: Instant::now(),
            partition_id,
            batch_type: CDCBatchType::OtherEvents, // Specialized for other events
        }
    }
    pub fn can_accept_event_type(&self, event_type: &str) -> bool {
        match self.batch_type {
            CDCBatchType::AccountCreated => event_type == "AccountCreated",
            CDCBatchType::OtherEvents => event_type != "AccountCreated",
        }
    }

    pub fn add_projection(&mut self, aggregate_id: Uuid, projection: AccountProjection) {
        // For backward compatibility, use aggregate_id as operation_id
        self.projections.push((
            aggregate_id,
            projection,
            aggregate_id,
            "Unknown".to_string(),
        ));
    }

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

    pub fn add_projection_with_event_type(
        &mut self,
        aggregate_id: Uuid,
        projection: AccountProjection,
        operation_id: Uuid,
        event_type: String,
    ) {
        self.projections
            .push((aggregate_id, projection, operation_id, event_type));
    }

    pub fn is_full(&self, max_size: usize) -> bool {
        self.projections.len() >= max_size
    }

    pub fn is_old(&self, max_wait_time: Duration) -> bool {
        self.created_at.elapsed() >= max_wait_time
    }

    pub fn should_process(&self, max_size: usize, max_wait_time: Duration) -> bool {
        self.is_full(max_size) || self.is_old(max_wait_time)
    }
}

/// CDC Batching Service for parallel projection updates
pub struct CDCBatchingService {
    config: CDCBatchingConfig,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    write_pool: Arc<PgPool>,

    current_batch: Arc<Mutex<CDCBatch>>,
    pending_results: Arc<Mutex<HashMap<Uuid, mpsc::Sender<CDCOperationResult>>>>,
    completed_results: Arc<Mutex<HashMap<Uuid, CDCOperationResult>>>,

    // Processing
    batch_processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_token: CancellationToken,

    // Metrics
    batches_processed: Arc<Mutex<u64>>,
    projections_processed: Arc<Mutex<u64>>,

    // Partitioning state
    partition_id: Option<usize>,
    processing_type: CDCBatchType, // NEW: Track processing type for routing
}

impl CDCBatchingService {
    pub fn new_account_created_processor(
        partition_id: usize, // Should always be 0
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
            pending_results: Arc::new(Mutex::new(HashMap::new())),
            completed_results: Arc::new(Mutex::new(HashMap::new())),
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            batches_processed: Arc::new(Mutex::new(0)),
            projections_processed: Arc::new(Mutex::new(0)),
            partition_id: Some(partition_id),
            processing_type: CDCBatchType::AccountCreated,
        }
    }

    pub fn new_other_events_processor(
        partition_id: usize, // Should be 1-N
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
            pending_results: Arc::new(Mutex::new(HashMap::new())),
            completed_results: Arc::new(Mutex::new(HashMap::new())),
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            batches_processed: Arc::new(Mutex::new(0)),
            projections_processed: Arc::new(Mutex::new(0)),
            partition_id: Some(partition_id),
            processing_type: CDCBatchType::OtherEvents,
        }
    }

    /// Submit projection update with automatic event type routing
    pub async fn submit_projection_update(
        &self,
        aggregate_id: Uuid,
        projection: AccountProjection,
    ) -> Result<Uuid> {
        let operation_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);

        // Add to pending results
        {
            let mut pending_guard = self.pending_results.lock().await;
            pending_guard.insert(operation_id, tx);
        }

        // Add to current batch
        {
            let mut batch_guard = self.current_batch.lock().await;
            batch_guard.add_projection_with_operation_id(aggregate_id, projection, operation_id);
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
        let partition_id = self.partition_id.clone();
        let processing_type: CDCBatchType = self.processing_type.clone();

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
                partition_id,
                processing_type,
            )
            .await;
        });

        {
            let mut handle_guard = self.batch_processor_handle.lock().await;
            *handle_guard = Some(handle);
        }

        info!(
            "CDC Batching Service started for partition {:?}",
            partition_id
        );
        Ok(())
    }

    /// Enhanced batch processing loop with separate batch handling
    async fn process_batch_loop(
        current_batch: Arc<Mutex<CDCBatch>>,
        pending_results: Arc<Mutex<HashMap<Uuid, mpsc::Sender<CDCOperationResult>>>>,
        completed_results: Arc<Mutex<HashMap<Uuid, CDCOperationResult>>>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
        config: CDCBatchingConfig,
        batches_processed: Arc<Mutex<u64>>,
        projections_processed: Arc<Mutex<u64>>,
        partition_id: Option<usize>,
        processing_type: CDCBatchType, // NEW: Processing type for routing
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(config.max_batch_wait_time_ms));

        loop {
            interval.tick().await;

            // Check if current batch should be processed
            let batch_to_process = {
                let mut batch_guard = current_batch.lock().await;
                if batch_guard.should_process(
                    config.max_batch_size,
                    Duration::from_millis(config.max_batch_wait_time_ms),
                ) && !batch_guard.projections.is_empty()
                {
                    let new_batch = match processing_type {
                        CDCBatchType::AccountCreated => {
                            CDCBatch::new_account_created_batch(partition_id)
                        }
                        CDCBatchType::OtherEvents => CDCBatch::new_other_events_batch(partition_id),
                    };
                    Some(std::mem::replace(&mut *batch_guard, new_batch))
                } else {
                    None
                }
            };

            if let Some(batch) = batch_to_process {
                // Use the appropriate processing method based on type
                let results = match processing_type {
                    CDCBatchType::AccountCreated => {
                        info!(
                            "🚀 PARTITION {:?}: Processing AccountCreated batch with {} projections",
                            partition_id,
                            batch.projections.len()
                        );
                        Self::execute_account_created_batch(
                            &batch,
                            &projection_store,
                            &write_pool,
                            &config,
                        )
                        .await
                    }
                    CDCBatchType::OtherEvents => {
                        info!(
                            "🔄 PARTITION {:?}: Processing other events batch with {} projections",
                            partition_id,
                            batch.projections.len()
                        );
                        Self::execute_other_events_batch(
                            &batch,
                            &projection_store,
                            &write_pool,
                            &config,
                            partition_id,
                        )
                        .await
                    }
                };

                // Handle results
                Self::handle_batch_results(results, &pending_results, &completed_results).await;
                Self::update_metrics(
                    &batches_processed,
                    &projections_processed,
                    batch.projections.len(),
                )
                .await;
            }
        }
    }

    /// Optimized processing for AccountCreated events (INSERT only, no locking)
    async fn execute_account_created_batch(
        batch: &CDCBatch,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        write_pool: &Arc<PgPool>,
        config: &CDCBatchingConfig,
    ) -> Vec<(Uuid, CDCOperationResult)> {
        info!(
            "🚀 ACCOUNT CREATED BATCH: Processing {} projections with optimized INSERT-only path",
            batch.projections.len()
        );

        let mut results = Vec::new();
        let batch_start = Instant::now();

        // Extract projections for bulk INSERT
        let projections: Vec<AccountProjection> = batch
            .projections
            .iter()
            .map(|(_, projection, _, _)| projection.clone())
            .collect();

        // OPTIMIZED PATH: Direct bulk INSERT without locking or state checking
        let result = projection_store
            .bulk_insert_new_accounts_with_copy(projections)
            .await;

        match result {
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
                info!(
                    "✅ ACCOUNT CREATED BATCH: Successfully processed {} projections",
                    batch.projections.len()
                );
            }
            Err(e) => {
                // Create failure results for all projections
                error!(
                    "❌ ACCOUNT CREATED BATCH: Failed to process {} projections: {}",
                    batch.projections.len(),
                    e
                );
                for (aggregate_id, _, operation_id, _) in &batch.projections {
                    results.push((
                        *operation_id,
                        CDCOperationResult {
                            operation_id: *operation_id,
                            success: false,
                            aggregate_id: Some(*aggregate_id),
                            error: Some(format!("AccountCreated batch failed: {}", e)),
                            duration: batch_start.elapsed(),
                        },
                    ));
                }
            }
        }
        Vec::new()
        //results
    }
    /// Standard processing for other events (UPDATE with locking and state checking)
    async fn execute_other_events_batch(
        batch: &CDCBatch,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        write_pool: &Arc<PgPool>,
        config: &CDCBatchingConfig,
        partition_id: Option<usize>,
    ) -> Vec<(Uuid, CDCOperationResult)> {
        info!("🔄 OTHER EVENTS BATCH: Processing {} projections with full UPDATE path (locking, state checking)", 
              batch.projections.len());

        let mut results = Vec::new();
        let mut retry_count = 0;

        // Use bulk mode for better performance with other events
        let should_use_bulk_mode = batch.projections.len() >= 3;
        let mut bulk_config_manager = if should_use_bulk_mode {
            Some(BulkInsertConfigManager::new())
        } else {
            None
        };

        // Start bulk mode if needed
        if let Some(ref mut config_manager) = bulk_config_manager {
            if let Err(e) = config_manager
                .start_bulk_mode_projection_store(projection_store)
                .await
            {
                error!("Failed to start bulk mode for other events batch: {}", e);
            } else {
                info!(
                    "🚀 OTHER EVENTS: Bulk mode activated for {} projections",
                    batch.projections.len()
                );
            }
        }

        while retry_count < config.max_retries {
            let mut transaction = match write_pool.begin().await {
                Ok(tx) => tx,
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= config.max_retries {
                        for (aggregate_id, _, operation_id, _) in &batch.projections {
                            results.push((
                                *operation_id,
                                CDCOperationResult {
                                    operation_id: *operation_id,
                                    success: false,
                                    aggregate_id: Some(*aggregate_id),
                                    error: Some(format!("Failed to begin transaction: {}", e)),
                                    duration: Duration::ZERO,
                                },
                            ));
                        }
                        return results;
                    }
                    continue;
                }
            };

            let batch_start = Instant::now();
            let projections: Vec<AccountProjection> = batch
                .projections
                .iter()
                .map(|(_, projection, _, _)| projection.clone())
                .collect();

            // FULL PATH: UPDATE with locking, state checking, etc.
            let result = projection_store
                .bulk_update_accounts_with_copy(projections)
                .await;

            if let Err(e) = result {
                error!("Failed to process other events batch: {}", e);

                // Check for serialization conflicts and retry logic
                let is_serialization_conflict = e
                    .to_string()
                    .contains("could not serialize access due to read/write dependencies");

                if is_serialization_conflict && retry_count < config.max_retries - 1 {
                    let _ = transaction.rollback().await;
                    retry_count += 1;
                    let backoff = Duration::from_millis(50 * retry_count as u64);
                    info!(
                        "🔄 OTHER EVENTS: Serialization conflict, retrying in {:?} (attempt {}/{})",
                        backoff, retry_count, config.max_retries
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                } else {
                    let _ = transaction.rollback().await;
                    retry_count += 1;
                    if retry_count >= config.max_retries {
                        for (aggregate_id, _, operation_id, _) in &batch.projections {
                            results.push((
                                *operation_id,
                                CDCOperationResult {
                                    operation_id: *operation_id,
                                    success: false,
                                    aggregate_id: Some(*aggregate_id),
                                    error: Some(format!(
                                        "Failed after {} retries: {}",
                                        config.max_retries, e
                                    )),
                                    duration: Duration::ZERO,
                                },
                            ));
                        }
                        return results;
                    }
                    continue;
                }
            }

            // Commit transaction
            if let Err(e) = transaction.commit().await {
                retry_count += 1;
                if retry_count >= config.max_retries {
                    for (aggregate_id, _, operation_id, _) in &batch.projections {
                        results.push((
                            *operation_id,
                            CDCOperationResult {
                                operation_id: *operation_id,
                                success: false,
                                aggregate_id: Some(*aggregate_id),
                                error: Some(format!("Failed to commit: {}", e)),
                                duration: Duration::ZERO,
                            },
                        ));
                    }
                    return results;
                }
                continue;
            }

            // Create success results
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

            info!(
                "✅ OTHER EVENTS BATCH: Successfully processed {} projections",
                batch.projections.len()
            );
            break;
        }

        // End bulk mode if needed
        if let Some(ref mut config_manager) = bulk_config_manager {
            if let Err(e) = config_manager
                .end_bulk_mode_projection_store(projection_store)
                .await
            {
                error!("Failed to end bulk mode for other events batch: {}", e);
            } else {
                info!("🔄 OTHER EVENTS: Bulk mode deactivated");
            }
        }

        //results
        Vec::new()
    }
    // Helper methods
    async fn handle_batch_results(
        results: Vec<(Uuid, CDCOperationResult)>,
        pending_results: &Arc<Mutex<HashMap<Uuid, mpsc::Sender<CDCOperationResult>>>>,
        completed_results: &Arc<Mutex<HashMap<Uuid, CDCOperationResult>>>,
    ) {
        let mut pending_guard = pending_results.lock().await;
        let mut completed_guard = completed_results.lock().await;

        for (operation_id, result) in results {
            completed_guard.insert(operation_id, result.clone());

            if let Some(sender) = pending_guard.remove(&operation_id) {
                if let Err(e) = sender.try_send(result) {
                    error!("Failed to send result to operation {}: {}", operation_id, e);
                }
            }
        }
    }

    async fn update_metrics(
        batches_processed: &Arc<Mutex<u64>>,
        projections_processed: &Arc<Mutex<u64>>,
        projection_count: usize,
    ) {
        {
            let mut batches_guard = batches_processed.lock().await;
            *batches_guard += 1;
        }
        {
            let mut projections_guard = projections_processed.lock().await;
            *projections_guard += projection_count as u64;
        }
    }

    pub async fn wait_for_result(&self, operation_id: Uuid) -> Result<CDCOperationResult> {
        // Check completed results first
        {
            let completed_guard = self.completed_results.lock().await;
            if let Some(result) = completed_guard.get(&operation_id) {
                return Ok(result.clone());
            }
        }

        // Wait for pending result with timeout
        {
            let mut pending_guard = self.pending_results.lock().await;
            if let Some(sender) = pending_guard.remove(&operation_id) {
                drop(pending_guard); // Release lock before await

                // Wait for the actual result from batch processing with timeout
                // Note: We can't use sender.recv() because sender doesn't have recv method
                // Instead, we'll use a different approach - poll the completed results
                let timeout_duration = Duration::from_secs(30);
                let start_time = Instant::now();

                loop {
                    // Check if result is in completed results
                    {
                        let completed_guard = self.completed_results.lock().await;
                        if let Some(result) = completed_guard.get(&operation_id) {
                            return Ok(result.clone());
                        }
                    }

                    // Check if we've timed out
                    if start_time.elapsed() > timeout_duration {
                        return Err(anyhow::anyhow!("Timeout waiting for operation result"));
                    }

                    // Wait a bit before checking again
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } else {
                Err(anyhow::anyhow!("Operation not found"))
            }
        }
    }
}

#[async_trait]
impl CDCBatchingServiceTrait for CDCBatchingService {
    async fn submit_projection_update(
        &self,
        aggregate_id: Uuid,
        projection: AccountProjection,
    ) -> Result<Uuid> {
        self.submit_projection_update(aggregate_id, projection)
            .await
    }

    async fn submit_projections_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        self.submit_projections_bulk(projections).await
    }

    async fn wait_for_result(&self, operation_id: Uuid) -> Result<CDCOperationResult> {
        self.wait_for_result(operation_id).await
    }
}

/// Multi-instance CDC Batching Manager
pub struct PartitionedCDCBatching {
    account_created_processor: Arc<CDCBatchingService>, // Partition 0
    other_events_processors: Vec<Arc<CDCBatchingService>>, // Partitions 1-N
}

impl PartitionedCDCBatching {
    pub async fn new(
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        let num_partitions = get_cdc_num_partitions();
        let config = CDCBatchingConfig::default();

        // Create AccountCreated processor (Partition 0)
        let account_created_processor =
            Arc::new(CDCBatchingService::new_account_created_processor(
                0,
                config.clone(),
                projection_store.clone(),
                write_pool.clone(),
            ));

        // Create other events processors (Partitions 1-N)
        let mut other_events_processors = Vec::new();
        for partition_id in 1..num_partitions {
            let processor = Arc::new(CDCBatchingService::new_other_events_processor(
                partition_id,
                config.clone(),
                projection_store.clone(),
                write_pool.clone(),
            ));
            other_events_processors.push(processor);
        }

        Self {
            account_created_processor,
            other_events_processors,
        }
    }
    // Submit ONLY AccountCreated events (events are pre-separated)
    pub async fn submit_account_created_projections_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "🚀 SIMPLIFIED CDC: Processing {} pre-separated AccountCreated events",
            projections.len()
        );

        let mut operation_ids = Vec::new();
        for (aggregate_id, projection) in projections {
            let operation_id = self
                .account_created_processor
                .submit_projection_update(aggregate_id, projection)
                .await?;
            operation_ids.push(operation_id);
        }

        Ok(operation_ids)
    }
    /// Submit ONLY other events (events are pre-separated)
    pub async fn submit_other_events_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
        _event_type: String, // Not needed for routing anymore, just for logging
    ) -> Result<Vec<Uuid>> {
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "🔄 SIMPLIFIED CDC: Processing {} pre-separated other events",
            projections.len()
        );

        let mut operation_ids = Vec::new();

        // Distribute across other events processors using hash-based partitioning
        for (aggregate_id, projection) in projections {
            let processor_index =
                (aggregate_id.as_u128() as usize) % self.other_events_processors.len();
            let processor = &self.other_events_processors[processor_index];

            let operation_id = processor
                .submit_projection_update(aggregate_id, projection)
                .await?;
            operation_ids.push(operation_id);
        }

        Ok(operation_ids)
    }
}

#[async_trait]
impl CDCBatchingServiceTrait for PartitionedCDCBatching {
    async fn submit_projection_update(
        &self,
        aggregate_id: Uuid,
        projection: AccountProjection,
    ) -> Result<Uuid> {
        self.submit_projection_update(aggregate_id, projection)
            .await
    }

    async fn submit_projections_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        self.submit_projections_bulk(projections).await
    }

    async fn wait_for_result(&self, operation_id: Uuid) -> Result<CDCOperationResult> {
        self.wait_for_result(operation_id).await
    }
}
