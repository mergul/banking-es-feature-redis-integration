use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use crate::infrastructure::write_batching::BulkInsertConfigManager;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use futures;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
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

    /// Start bulk mode for optimized processing
    async fn start_bulk_mode(&self) -> Result<()>;

    /// End bulk mode
    async fn end_bulk_mode(&self) -> Result<()>;

    /// Check if bulk mode is active
    async fn is_bulk_mode(&self) -> bool;

    /// Get processor for specific aggregate
    fn get_processor_for_aggregate(&self, aggregate_id: &Uuid)
        -> &Arc<dyn CDCBatchingServiceTrait>;

    /// Get processor for bulk operations
    fn get_processor_for_bulk_operation(
        &self,
        operation_index: usize,
    ) -> &Arc<dyn CDCBatchingServiceTrait>;
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

pub struct CDCBatch {
    pub batch_id: Uuid,
    pub projections: Vec<(Uuid, AccountProjection, Uuid)>, // (aggregate_id, projection, operation_id)
    pub created_at: Instant,
    pub partition_id: Option<usize>,
}

impl CDCBatch {
    pub fn new(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::new(),
            created_at: Instant::now(),
            partition_id,
        }
    }

    pub fn add_projection(&mut self, aggregate_id: Uuid, projection: AccountProjection) {
        // For backward compatibility, use aggregate_id as operation_id
        self.projections
            .push((aggregate_id, projection, aggregate_id));
    }

    pub fn add_projection_with_operation_id(
        &mut self,
        aggregate_id: Uuid,
        projection: AccountProjection,
        operation_id: Uuid,
    ) {
        self.projections
            .push((aggregate_id, projection, operation_id));
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

    // Batching state
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
    num_partitions: Option<usize>,
}

impl CDCBatchingService {
    pub fn new(
        config: CDCBatchingConfig,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        Self {
            config,
            projection_store,
            write_pool,
            current_batch: Arc::new(Mutex::new(CDCBatch::new(None))),
            pending_results: Arc::new(Mutex::new(HashMap::new())),
            completed_results: Arc::new(Mutex::new(HashMap::new())),
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            batches_processed: Arc::new(Mutex::new(0)),
            projections_processed: Arc::new(Mutex::new(0)),
            partition_id: None,
            num_partitions: None,
        }
    }

    pub fn new_for_partition(
        partition_id: usize,
        num_partitions: usize,
        config: CDCBatchingConfig,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        let mut service = Self::new(config, projection_store, write_pool);
        service.partition_id = Some(partition_id);
        service.num_partitions = Some(num_partitions);
        service.current_batch = Arc::new(Mutex::new(CDCBatch::new(Some(partition_id))));
        service
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
        let partition_id = self.partition_id;

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
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(config.max_batch_wait_time_ms));

        loop {
            interval.tick().await;

            // Check if current batch should be processed
            let should_process = {
                let batch_guard = current_batch.lock().await;
                batch_guard.should_process(
                    config.max_batch_size,
                    Duration::from_millis(config.max_batch_wait_time_ms),
                )
            };

            if should_process {
                let batch_to_process = {
                    let mut batch_guard = current_batch.lock().await;
                    if batch_guard.projections.is_empty() {
                        continue;
                    }
                    std::mem::replace(&mut *batch_guard, CDCBatch::new(partition_id))
                };

                // Process the batch
                let results = Self::execute_cdc_batch(
                    &batch_to_process,
                    &projection_store,
                    &write_pool,
                    &config,
                    partition_id,
                )
                .await;

                // Update metrics
                {
                    let mut batches_guard = batches_processed.lock().await;
                    *batches_guard += 1;
                }
                {
                    let mut projections_guard = projections_processed.lock().await;
                    *projections_guard += batch_to_process.projections.len() as u64;
                }

                // Send results to pending operations and store in completed results
                {
                    let mut pending_guard = pending_results.lock().await;
                    let mut completed_guard = completed_results.lock().await;
                    for (operation_id, result) in results {
                        // Store in completed results
                        completed_guard.insert(operation_id, result.clone());

                        // Send to pending operation if still waiting
                        if let Some(sender) = pending_guard.remove(&operation_id) {
                            // Non-blocking send to prevent deadlocks
                            if let Err(e) = sender.try_send(result) {
                                error!(
                                    "Failed to send result to operation {}: {}",
                                    operation_id, e
                                );
                            }
                        }
                    }
                }

                info!(
                    "CDC Batch processed: {} projections in partition {:?}",
                    batch_to_process.projections.len(),
                    partition_id
                );
            }
        }
    }

    pub async fn execute_cdc_batch(
        batch: &CDCBatch,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        write_pool: &Arc<PgPool>,
        config: &CDCBatchingConfig,
        partition_id: Option<usize>,
    ) -> Vec<(Uuid, CDCOperationResult)> {
        println!(
            "[DEBUG] execute_cdc_batch: Starting CDC batch with {} projections",
            batch.projections.len()
        );
        let mut results = Vec::new();
        let mut retry_count = 0;

        // BULK MODE: Batch boyutu büyükse bulk mode başlat
        let should_use_bulk_mode = batch.projections.len() >= 3; // 3+ projection için bulk mode
        let mut bulk_config_manager = if should_use_bulk_mode {
            Some(BulkInsertConfigManager::new())
        } else {
            None
        };

        // BULK MODE: Eğer bulk mode kullanılacaksa başlat
        if let Some(ref mut config_manager) = bulk_config_manager {
            if let Err(e) = config_manager
                .start_bulk_mode_projection_store(projection_store)
                .await
            {
                error!("Failed to start bulk mode in CDC batch: {}", e);
                // Bulk mode başarısız olsa bile normal modda devam et
            } else {
                info!(
                    "🚀 CDC Bulk mode activated for batch with {} projections",
                    batch.projections.len()
                );
            }
        }

        while retry_count < config.max_retries {
            // Always start a new transaction for each retry attempt
            let mut transaction = match write_pool.begin().await {
                Ok(tx) => tx,
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= config.max_retries {
                        for (aggregate_id, _, operation_id) in &batch.projections {
                            results.push((
                                *operation_id,
                                CDCOperationResult {
                                    operation_id: *operation_id,
                                    success: false,
                                    aggregate_id: Some(*aggregate_id),
                                    error: Some(format!(
                                        "Failed to begin transaction after {} retries: {}",
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
            };

            let batch_start = Instant::now();

            // Extract projections for bulk upsert
            let projections: Vec<AccountProjection> = batch
                .projections
                .iter()
                .map(|(_, projection, _)| projection.clone())
                .collect();

            // Bulk upsert projections
            let result = projection_store.upsert_accounts_batch(projections).await;

            if let Err(e) = result {
                let partition_info = partition_id
                    .map(|p| format!("partition_{}", p))
                    .unwrap_or_else(|| "unknown_partition".to_string());

                error!(
                    "Failed to upsert projections in CDC batch ({}): {:?}",
                    partition_info, e
                );

                // Check if this is a serialization conflict that we should retry
                let is_serialization_conflict = e
                    .to_string()
                    .contains("could not serialize access due to read/write dependencies");

                if is_serialization_conflict && retry_count < config.max_retries - 1 {
                    let _ = transaction.rollback().await;
                    retry_count += 1;
                    let backoff = Duration::from_millis(50 * retry_count as u64);
                    info!(
                        "Serialization conflict detected, retrying in {:?} (attempt {}/{})",
                        backoff, retry_count, config.max_retries
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                } else {
                    let _ = transaction.rollback().await;
                    retry_count += 1;
                    if retry_count >= config.max_retries {
                        for (aggregate_id, _, operation_id) in &batch.projections {
                            results.push((
                                *operation_id,
                                CDCOperationResult {
                                    operation_id: *operation_id,
                                    success: false,
                                    aggregate_id: Some(*aggregate_id),
                                    error: Some(format!(
                                        "Failed to upsert projections after {} retries: {}",
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
                    for (aggregate_id, _, operation_id) in &batch.projections {
                        results.push((
                            *operation_id,
                            CDCOperationResult {
                                operation_id: *operation_id,
                                success: false,
                                aggregate_id: Some(*aggregate_id),
                                error: Some(format!(
                                    "Failed to commit transaction after {} retries: {}",
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

            // Create success results
            for (aggregate_id, _, operation_id) in &batch.projections {
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

            // BULK MODE: İşlem başarılı olduysa bulk mode'u sonlandır
            if let Some(ref mut config_manager) = bulk_config_manager {
                if let Err(e) = config_manager
                    .end_bulk_mode_projection_store(projection_store)
                    .await
                {
                    error!("Failed to end bulk mode in CDC batch: {}", e);
                } else {
                    info!("🔄 CDC Bulk mode deactivated after successful batch processing");
                }
            }

            return results;
        }

        // BULK MODE: Tüm retry'lar başarısız olduysa bulk mode'u sonlandır
        if let Some(ref mut config_manager) = bulk_config_manager {
            if let Err(e) = config_manager
                .end_bulk_mode_projection_store(projection_store)
                .await
            {
                error!(
                    "Failed to end bulk mode after retry failures in CDC batch: {}",
                    e
                );
            }
        }

        // Return failure results for all operations
        for (aggregate_id, _, operation_id) in &batch.projections {
            results.push((
                *operation_id,
                CDCOperationResult {
                    operation_id: *operation_id,
                    success: false,
                    aggregate_id: Some(*aggregate_id),
                    error: Some(format!(
                        "Failed to process CDC batch after {} retries",
                        config.max_retries
                    )),
                    duration: Duration::ZERO,
                },
            ));
        }

        results
    }

    pub async fn submit_projection_update(
        &self,
        aggregate_id: Uuid,
        projection: AccountProjection,
    ) -> Result<Uuid> {
        let operation_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);

        // Add to pending results with operation_id -> sender mapping
        {
            let mut pending_guard = self.pending_results.lock().await;
            pending_guard.insert(operation_id, tx);
        }

        // Add to current batch with operation_id tracking
        {
            let mut batch_guard = self.current_batch.lock().await;
            batch_guard.add_projection_with_operation_id(aggregate_id, projection, operation_id);
        }

        Ok(operation_id)
    }

    /// Submit multiple projections as a dedicated batch (like write_batching)
    pub async fn submit_projections_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>, // (aggregate_id, projection)
    ) -> Result<Vec<Uuid>> {
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "🚀 CDC Batching Service: Starting bulk processing for {} projections",
            projections.len()
        );

        // Create a dedicated batch for immediate processing
        let mut dedicated_batch = CDCBatch::new(self.partition_id);
        let mut operation_ids = Vec::new();

        let projections_len = projections.len();

        // Add all projections to the dedicated batch
        for (aggregate_id, projection) in projections {
            let operation_id = Uuid::new_v4();
            dedicated_batch.add_projection_with_operation_id(
                aggregate_id,
                projection,
                operation_id,
            );
            operation_ids.push(operation_id);
        }

        info!(
            "📦 CDC Batching Service: Created dedicated batch with {} projections",
            projections_len
        );

        // Execute the dedicated batch immediately
        let batch_results = Self::execute_cdc_batch(
            &dedicated_batch,
            &self.projection_store,
            &self.write_pool,
            &self.config,
            self.partition_id,
        )
        .await;

        // Store completed results and send to pending operations
        {
            let mut pending_guard = self.pending_results.lock().await;
            let mut completed_guard = self.completed_results.lock().await;

            for (operation_id, result) in batch_results {
                // Store in completed results
                completed_guard.insert(operation_id, result.clone());

                // Send to pending operation if still waiting
                if let Some(sender) = pending_guard.remove(&operation_id) {
                    if let Err(e) = sender.try_send(result) {
                        error!("Failed to send result to operation {}: {}", operation_id, e);
                    }
                }
            }
        }

        info!(
            "✅ CDC Batching Service: Bulk processing completed for {} projections",
            projections_len
        );

        Ok(operation_ids)
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

    /// Start bulk mode for CDC batching
    pub async fn start_bulk_mode(&self) -> Result<()> {
        let mut config_manager = BulkInsertConfigManager::new();
        config_manager
            .start_bulk_mode_projection_store(&self.projection_store)
            .await
    }

    /// End bulk mode for CDC batching
    pub async fn end_bulk_mode(&self) -> Result<()> {
        let mut config_manager = BulkInsertConfigManager::new();
        config_manager
            .end_bulk_mode_projection_store(&self.projection_store)
            .await
    }

    /// Check if bulk mode is active
    pub async fn is_bulk_mode(&self) -> bool {
        let config_manager = BulkInsertConfigManager::new();
        config_manager.is_bulk_mode()
    }

    /// Execute CDC batch with bulk config
    pub async fn execute_cdc_batch_with_bulk_config(
        &self,
        batch: &CDCBatch,
    ) -> Result<Vec<(Uuid, CDCOperationResult)>> {
        let results = CDCBatchingService::execute_cdc_batch(
            batch,
            &self.projection_store,
            &self.write_pool,
            &self.config,
            self.partition_id,
        )
        .await;

        Ok(results)
    }

    /// Submit projections with bulk config
    pub async fn submit_projections_bulk_with_bulk_config(
        &self,
        projections: Vec<(Uuid, AccountProjection)>, // (aggregate_id, projection)
    ) -> Result<Vec<Uuid>> {
        // Use bulk config for this operation
        let mut bulk_config_manager = BulkInsertConfigManager::new();
        if let Err(e) = bulk_config_manager
            .start_bulk_mode_projection_store(&self.projection_store)
            .await
        {
            error!("Failed to start bulk mode for CDC batch: {}", e);
            return Err(anyhow::anyhow!("Failed to start bulk mode: {}", e));
        }

        let result = self.submit_projections_bulk(projections).await;

        // Always end bulk mode
        if let Err(e) = bulk_config_manager
            .end_bulk_mode_projection_store(&self.projection_store)
            .await
        {
            error!("Failed to end bulk mode for CDC batch: {}", e);
        }

        result
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

    async fn start_bulk_mode(&self) -> Result<()> {
        self.start_bulk_mode().await
    }

    async fn end_bulk_mode(&self) -> Result<()> {
        self.end_bulk_mode().await
    }

    async fn is_bulk_mode(&self) -> bool {
        self.is_bulk_mode().await
    }

    fn get_processor_for_aggregate(
        &self,
        _aggregate_id: &Uuid,
    ) -> &Arc<dyn CDCBatchingServiceTrait> {
        // For single service, return self
        // This is a bit tricky with Arc<dyn Trait>, so we'll need to handle this differently
        // For now, we'll panic as this shouldn't be called on a single service
        unimplemented!("get_processor_for_aggregate not implemented for single CDCBatchingService")
    }

    fn get_processor_for_bulk_operation(
        &self,
        _operation_index: usize,
    ) -> &Arc<dyn CDCBatchingServiceTrait> {
        // For single service, return self
        // This is a bit tricky with Arc<dyn Trait>, so we'll need to handle this differently
        // For now, we'll panic as this shouldn't be called on a single service
        unimplemented!(
            "get_processor_for_bulk_operation not implemented for single CDCBatchingService"
        )
    }
}

/// Multi-instance CDC Batching Manager
pub struct PartitionedCDCBatching {
    processors: Vec<Arc<CDCBatchingService>>,
}

impl PartitionedCDCBatching {
    pub async fn new(
        projection_store: Arc<dyn ProjectionStoreTrait>,
        write_pool: Arc<PgPool>,
    ) -> Self {
        let num_partitions = get_cdc_num_partitions();
        let mut processors = Vec::with_capacity(num_partitions);

        for partition_id in 0..num_partitions {
            let config = CDCBatchingConfig::default();
            let processor = Arc::new(CDCBatchingService::new_for_partition(
                partition_id,
                num_partitions,
                config,
                projection_store.clone(),
                write_pool.clone(),
            ));

            if let Err(e) = processor.start().await {
                error!(
                    "Failed to start CDC batching service for partition {}: {}",
                    partition_id, e
                );
            }

            processors.push(processor);
        }

        Self { processors }
    }

    pub fn get_processor_for_aggregate(&self, aggregate_id: &Uuid) -> &Arc<CDCBatchingService> {
        // HYBRID: Hash-based partitioning for consistency
        let num_partitions = get_cdc_num_partitions();
        let partition_id = (aggregate_id.as_u128() as usize) % num_partitions;
        &self.processors[partition_id]
    }

    /// HYBRID: Get processor for bulk operations using round-robin distribution
    pub fn get_processor_for_bulk_operation(
        &self,
        operation_index: usize,
    ) -> &Arc<CDCBatchingService> {
        let num_partitions = get_cdc_num_partitions();
        let partition_id = operation_index % num_partitions;
        &self.processors[partition_id]
    }

    pub async fn submit_projection_update(
        &self,
        aggregate_id: Uuid,
        projection: AccountProjection,
    ) -> Result<Uuid> {
        let processor = self.get_processor_for_aggregate(&aggregate_id);
        processor
            .submit_projection_update(aggregate_id, projection)
            .await
    }

    /// SEQUENTIAL: Submit multiple projections with sequential partitioning
    pub async fn submit_projections_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>, // (aggregate_id, projection)
    ) -> Result<Vec<Uuid>> {
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "🚀 PartitionedCDC: Starting SEQUENTIAL bulk processing for {} projections",
            projections.len()
        );

        let projections_len = projections.len();

        // SEQUENTIAL APPROACH:
        // 1. First group by aggregate_id for consistency
        // 2. Then distribute using sequential partitioning (1000 per partition)
        let mut projections_by_aggregate: HashMap<Uuid, Vec<AccountProjection>> = HashMap::new();

        // Group projections by aggregate_id
        for (aggregate_id, projection) in projections {
            projections_by_aggregate
                .entry(aggregate_id)
                .or_insert_with(Vec::new)
                .push(projection);
        }

        let aggregate_count = projections_by_aggregate.len();
        info!(
            "📊 PartitionedCDC: Grouped {} projections into {} aggregate groups",
            projections_len, aggregate_count
        );

        // SEQUENTIAL: Distribute aggregates using sequential partitioning (1000 aggregates per partition)
        let mut projections_by_partition: HashMap<usize, Vec<(Uuid, AccountProjection)>> =
            HashMap::new();
        let mut aggregate_index = 0;
        let aggregates_per_partition = 1000; // First 1000 aggregates to partition 0, next 1000 to partition 1, etc.

        // CRITICAL: Process aggregates sequentially, keeping all projections of an aggregate together
        for (aggregate_id, aggregate_projections) in projections_by_aggregate {
            // Sequential distribution: aggregate_index / aggregates_per_partition
            let num_partitions = get_cdc_num_partitions();
            let partition_id = (aggregate_index / aggregates_per_partition) % num_partitions;

            // Add ALL projections for this aggregate to the selected partition
            for projection in aggregate_projections {
                projections_by_partition
                    .entry(partition_id)
                    .or_insert_with(Vec::new)
                    .push((aggregate_id, projection));
            }

            aggregate_index += 1; // Move to next aggregate
        }

        let partitions_count = projections_by_partition.len();
        info!(
            "🔄 PartitionedCDC: Distributed {} aggregates across {} partitions using sequential partitioning ({} aggregates per partition)",
            aggregate_count, partitions_count, aggregates_per_partition
        );

        // Process each partition's projections as dedicated batches IN PARALLEL
        let mut partition_futures = Vec::new();

        for (partition_id, partition_projections) in projections_by_partition {
            let processor = self.processors[partition_id].clone();

            info!(
                "📦 PartitionedCDC: Preparing parallel processing for partition {} with {} projections",
                partition_id,
                partition_projections.len()
            );

            // Create future for parallel execution
            let future = async move {
                let operation_ids = processor
                    .submit_projections_bulk(partition_projections)
                    .await?;

                info!(
                    "✅ PartitionedCDC: Completed partition {} with {} projections",
                    partition_id,
                    operation_ids.len()
                );

                Ok::<Vec<Uuid>, anyhow::Error>(operation_ids)
            };

            partition_futures.push(future);
        }

        // Execute all partitions in parallel
        info!(
            "🚀 PartitionedCDC: Starting parallel processing of {} partitions",
            partition_futures.len()
        );

        let partition_results = futures::future::join_all(partition_futures).await;

        // Collect all operation IDs
        let mut all_operation_ids = Vec::new();
        for result in partition_results {
            let operation_ids = result?;
            all_operation_ids.extend(operation_ids);
        }

        info!(
            "✅ PartitionedCDC: SEQUENTIAL bulk processing completed for {} projections ({} aggregates) across {} partitions",
            projections_len, aggregate_count, partitions_count
        );

        Ok(all_operation_ids)
    }

    pub async fn wait_for_result(&self, operation_id: Uuid) -> Result<CDCOperationResult> {
        // Try all processors (operation_id contains aggregate_id info)
        for processor in &self.processors {
            if let Ok(result) = processor.wait_for_result(operation_id).await {
                return Ok(result);
            }
        }
        Err(anyhow::anyhow!("Operation not found in any processor"))
    }

    /// Start bulk mode for all partitions
    pub async fn start_bulk_mode_all_partitions(&self) -> Result<()> {
        for processor in &self.processors {
            processor.start_bulk_mode().await?;
        }
        info!("🚀 CDC Bulk mode activated for all partitions");
        Ok(())
    }

    /// End bulk mode for all partitions
    pub async fn end_bulk_mode_all_partitions(&self) -> Result<()> {
        for processor in &self.processors {
            processor.end_bulk_mode().await?;
        }
        info!("🔄 CDC Bulk mode deactivated for all partitions");
        Ok(())
    }

    /// Submit projections bulk with bulk config
    pub async fn submit_projections_bulk_with_bulk_config(
        &self,
        projections: Vec<(Uuid, AccountProjection)>, // (aggregate_id, projection)
    ) -> Result<Vec<Uuid>> {
        // Start bulk mode for all partitions
        self.start_bulk_mode_all_partitions().await?;

        // Submit projections
        let results = self.submit_projections_bulk(projections).await;

        // End bulk mode for all partitions
        self.end_bulk_mode_all_partitions().await?;

        results
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

    async fn start_bulk_mode(&self) -> Result<()> {
        self.start_bulk_mode_all_partitions().await
    }

    async fn end_bulk_mode(&self) -> Result<()> {
        self.end_bulk_mode_all_partitions().await
    }

    async fn is_bulk_mode(&self) -> bool {
        // Check if any partition is in bulk mode
        for processor in &self.processors {
            if processor.is_bulk_mode().await {
                return true;
            }
        }
        false
    }

    fn get_processor_for_aggregate(
        &self,
        aggregate_id: &Uuid,
    ) -> &Arc<dyn CDCBatchingServiceTrait> {
        // Convert Arc<CDCBatchingService> to Arc<dyn CDCBatchingServiceTrait>
        // This is a bit tricky, but we can use the fact that CDCBatchingService implements the trait
        let processor = self.get_processor_for_aggregate(aggregate_id);
        // We need to return a reference to Arc<dyn Trait>, but we have Arc<ConcreteType>
        // For now, we'll use a workaround by storing the processors as Arc<dyn Trait>
        // This requires restructuring the PartitionedCDCBatching to store Arc<dyn CDCBatchingServiceTrait>
        // For now, we'll panic as this is a complex conversion
        unimplemented!("get_processor_for_aggregate needs restructuring for trait objects")
    }

    fn get_processor_for_bulk_operation(
        &self,
        operation_index: usize,
    ) -> &Arc<dyn CDCBatchingServiceTrait> {
        // Similar issue as above
        let processor = self.get_processor_for_bulk_operation(operation_index);
        unimplemented!("get_processor_for_bulk_operation needs restructuring for trait objects")
    }
}
