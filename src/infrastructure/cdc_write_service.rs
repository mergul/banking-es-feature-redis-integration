use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use crate::infrastructure::TransactionProjection;
use anyhow::Result;
use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
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

fn get_cdc_write_batch_timeout_ms() -> u64 {
    std::env::var("CDC_WRITE_BATCH_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(50) // Default to 50ms if not set or invalid
}
fn get_cdc_poll_interval_ms() -> u64 {
    std::env::var("CDC_POLL_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5) // Default to 5ms if not set or invalid
}
// OPTIMIZATION 1: Type-specific batch containers
pub enum TypedBatch {
    // Lock-free for unique operations
    AccountCreation(LockFreeBatch<AccountProjection>),
    TransactionCreation(LockFreeBatch<TransactionProjection>),

    // Locked for potential conflicts
    AccountUpdate(LockedBatch<AccountProjection>),
}

// OPTIMIZATION 2: Lock-free batch for unique operations (Account Creation & Transaction Creation)
pub struct LockFreeBatch<T> {
    // Lock-free concurrent queue
    items: SegQueue<(Uuid, T, Uuid)>, // (entity_id, data, operation_id)

    // Atomic counters
    size: AtomicUsize,
    created_at: Instant,
    batch_type: String,
}

impl<T> LockFreeBatch<T> {
    pub fn new(batch_type: String) -> Self {
        Self {
            items: SegQueue::new(),
            size: AtomicUsize::new(0),
            created_at: Instant::now(),
            batch_type,
        }
    }

    // OPTIMIZATION 3: Lock-free append (perfect for unique operations)
    pub fn add_item(&self, entity_id: Uuid, item: T, operation_id: Uuid) {
        self.items.push((entity_id, item, operation_id));
        let new_size = self.size.fetch_add(1, Ordering::Relaxed) + 1;

        // Log progress without locks
        if new_size % 1000 == 0 {
            tracing::info!("🚀 LOCKFREE {}: has {} items", self.batch_type, new_size);
        }
    }

    // NEW: Bulk add items for better performance
    pub fn add_items_bulk(&self, items: Vec<(Uuid, T, Uuid)>) {
        let count = items.len();
        for (entity_id, item, operation_id) in items {
            self.items.push((entity_id, item, operation_id));
        }
        let new_size = self.size.fetch_add(count, Ordering::Relaxed) + count;

        tracing::info!(
            "🚀 LOCKFREE BULK {}: added {} items, total: {}",
            self.batch_type,
            count,
            new_size
        );
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn should_flush(&self, max_size: usize, max_age: Duration) -> bool {
        self.len() >= max_size || self.created_at.elapsed() > max_age
    }

    // OPTIMIZATION 4: Drain all items efficiently
    pub fn drain_all(&self) -> Vec<(Uuid, T, Uuid)> {
        let mut items = Vec::new();
        while let Some(item) = self.items.pop() {
            items.push(item);
        }
        self.size.store(0, Ordering::Relaxed);
        items
    }
}

// OPTIMIZATION 5: Locked batch for operations that might conflict (Account Updates)
pub struct LockedBatch<T> {
    // Use RwLock for better read performance
    items: RwLock<Vec<(Uuid, T, Uuid)>>,
    created_at: Instant,
    batch_type: String,
}

impl<T> LockedBatch<T> {
    pub fn new(batch_type: String) -> Self {
        Self {
            items: RwLock::new(Vec::with_capacity(1000)),
            created_at: Instant::now(),
            batch_type,
        }
    }

    // OPTIMIZATION 6: Fast read operations
    pub async fn len(&self) -> usize {
        self.items.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.items.read().await.is_empty()
    }

    pub async fn should_flush(&self, max_size: usize, max_age: Duration) -> bool {
        let len = self.len().await;
        len >= max_size || self.created_at.elapsed() > max_age
    }

    // OPTIMIZATION 7: Write operation only when necessary
    pub async fn add_item(&self, entity_id: Uuid, item: T, operation_id: Uuid) {
        let mut items = self.items.write().await;
        items.push((entity_id, item, operation_id));

        if items.len() % 500 == 0 {
            tracing::info!("📝 LOCKED {}: has {} items", self.batch_type, items.len());
        }
    }

    // NEW: Bulk add items for better performance
    pub async fn add_items_bulk(&self, new_items: Vec<(Uuid, T, Uuid)>) {
        let count = new_items.len();
        let mut items = self.items.write().await;
        items.extend(new_items);

        tracing::info!(
            "📝 LOCKED BULK {}: added {} items, total: {}",
            self.batch_type,
            count,
            items.len()
        );
    }

    pub async fn drain_all(&self) -> Vec<(Uuid, T, Uuid)> {
        let mut items = self.items.write().await;
        std::mem::take(&mut *items)
    }
}

// OPTIMIZATION 8: Type-specific processors
#[derive(Clone)]
pub struct OptimizedCDCProcessor {
    // Lock-free processors for unique operations
    account_creation_batch: Arc<LockFreeBatch<AccountProjection>>,
    transaction_creation_batch: Arc<LockFreeBatch<TransactionProjection>>,

    // Locked processor for updates
    account_update_batch: Arc<LockedBatch<AccountProjection>>,

    // Shared resources
    pending_results: Arc<DashMap<Uuid, mpsc::Sender<CDCOperationResult>>>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    // Metrics
    metrics: ProcessorMetrics,
}

#[derive(Clone)]
pub struct ProcessorMetrics {
    pub accounts_created: Arc<AtomicUsize>,
    pub accounts_updated: Arc<AtomicUsize>,
    pub transactions_created: Arc<AtomicUsize>,
    pub batches_processed: Arc<AtomicUsize>,
}

impl ProcessorMetrics {
    pub fn new() -> Self {
        Self {
            accounts_created: Arc::new(AtomicUsize::new(0)),
            accounts_updated: Arc::new(AtomicUsize::new(0)),
            transactions_created: Arc::new(AtomicUsize::new(0)),
            batches_processed: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_totals(&self) -> (usize, usize, usize, usize) {
        (
            self.accounts_created.load(Ordering::Relaxed),
            self.accounts_updated.load(Ordering::Relaxed),
            self.transactions_created.load(Ordering::Relaxed),
            self.batches_processed.load(Ordering::Relaxed),
        )
    }
}

impl OptimizedCDCProcessor {
    pub async fn new(projection_store: Arc<dyn ProjectionStoreTrait>) -> Self {
        tracing::info!("Creating OptimizedCDCProcessor");
        Self {
            account_creation_batch: Arc::new(LockFreeBatch::new("ACCOUNT_CREATION".to_string())),
            transaction_creation_batch: Arc::new(LockFreeBatch::new(
                "TRANSACTION_CREATION".to_string(),
            )),
            account_update_batch: Arc::new(LockedBatch::new("ACCOUNT_UPDATE".to_string())),
            pending_results: Arc::new(DashMap::new()),
            projection_store,
            metrics: ProcessorMetrics::new(),
        }
    }

    // OPTIMIZATION 9: Lock-free account creation submission
    pub async fn submit_account_creation(&self, account: AccountProjection) -> Result<Uuid> {
        tracing::debug!("Submitting account creation");
        let operation_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);

        // Lock-free operations
        self.pending_results.insert(operation_id, tx);
        self.account_creation_batch
            .add_item(account.id, account, operation_id);
        self.metrics
            .accounts_created
            .fetch_add(1, Ordering::Relaxed);

        Ok(operation_id)
    }

    // NEW: Bulk account creation submission - OPTIMIZED
    pub async fn submit_account_creations_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        tracing::debug!("Submitting account creations in bulk");
        let batch_size = projections.len();
        let mut operation_ids = Vec::with_capacity(batch_size);
        let mut batch_items = Vec::with_capacity(batch_size);

        // Single pass: generate IDs, create channels, and prepare batch items
        for (entity_id, account) in projections {
            let operation_id = Uuid::new_v4();
            let (tx, _rx) = mpsc::channel(1);

            // The for loop is ONLY needed for these two operations:
            // 1. Store result channel for later notification
            self.pending_results.insert(operation_id, tx);
            // 2. Collect operation IDs to return to caller
            operation_ids.push(operation_id);

            // Prepare batch item (could be done in the loop above, but separated for clarity)
            batch_items.push((entity_id, account, operation_id));
        }

        // Single atomic operation: add all items to lock-free batch
        self.account_creation_batch.add_items_bulk(batch_items);
        // Single atomic operation: update metrics
        self.metrics
            .accounts_created
            .fetch_add(batch_size, Ordering::Relaxed);

        tracing::info!(
            "🚀 BULK ACCOUNT CREATION: Submitted {} accounts",
            batch_size
        );

        Ok(operation_ids)
    }

    // OPTIMIZATION 10: Lock-free transaction creation submission
    pub async fn submit_transaction_creation(
        &self,
        transaction: TransactionProjection,
    ) -> Result<Uuid> {
        tracing::debug!("Submitting transaction creation");
        let operation_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);

        // Lock-free operations
        self.pending_results.insert(operation_id, tx);
        self.transaction_creation_batch
            .add_item(transaction.account_id, transaction, operation_id);
        self.metrics
            .transactions_created
            .fetch_add(1, Ordering::Relaxed);

        Ok(operation_id)
    }

    // NEW: Bulk transaction creation submission
    pub async fn submit_transaction_creations_bulk(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<Vec<Uuid>> {
        tracing::debug!("Submitting transaction creations in bulk");
        let mut operation_ids = Vec::with_capacity(transactions.len());
        let mut batch_items = Vec::with_capacity(transactions.len());

        // Prepare all operations and channels
        for transaction in transactions {
            let operation_id = Uuid::new_v4();
            let (tx, _rx) = mpsc::channel(1);

            // Store the result channel
            self.pending_results.insert(operation_id, tx);

            // Prepare batch item
            batch_items.push((transaction.account_id, transaction, operation_id));
            operation_ids.push(operation_id);
        }

        // Add all items to batch at once (lock-free)
        self.transaction_creation_batch.add_items_bulk(batch_items);
        self.metrics
            .transactions_created
            .fetch_add(operation_ids.len(), Ordering::Relaxed);

        tracing::info!(
            "💳 BULK TRANSACTION CREATION: Submitted {} transactions",
            operation_ids.len()
        );

        Ok(operation_ids)
    }

    // OPTIMIZATION 11: Locked account update submission (only when necessary)
    pub async fn submit_account_update(&self, account: AccountProjection) -> Result<Uuid> {
        tracing::debug!("Submitting account update");
        let operation_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);

        // Lock-free result tracking
        self.pending_results.insert(operation_id, tx);

        // Locked batch addition (necessary for potential conflicts)
        self.account_update_batch
            .add_item(account.id, account, operation_id)
            .await;
        self.metrics
            .accounts_updated
            .fetch_add(1, Ordering::Relaxed);

        Ok(operation_id)
    }

    // NEW: Bulk account update submission
    pub async fn submit_account_updates_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        tracing::debug!("Submitting account updates in bulk");
        let mut operation_ids = Vec::with_capacity(projections.len());
        let mut batch_items = Vec::with_capacity(projections.len());

        // Prepare all operations and channels
        for (entity_id, account) in projections {
            let operation_id = Uuid::new_v4();
            let (tx, _rx) = mpsc::channel(1);

            // Store the result channel
            self.pending_results.insert(operation_id, tx);

            // Prepare batch item
            batch_items.push((entity_id, account, operation_id));
            operation_ids.push(operation_id);
        }

        // Add all items to batch at once (requires lock for updates)
        self.account_update_batch.add_items_bulk(batch_items).await;
        self.metrics
            .accounts_updated
            .fetch_add(operation_ids.len(), Ordering::Relaxed);

        tracing::info!(
            "🔄 BULK ACCOUNT UPDATE: Submitted {} account updates",
            operation_ids.len()
        );

        Ok(operation_ids)
    }

    // OPTIMIZATION 12: Parallel processing of all batch types
    pub async fn start_processing(&self) -> Result<()> {
        tracing::info!("Starting OptimizedCDCProcessor processing");
        let account_creation_batch = self.account_creation_batch.clone();
        let transaction_creation_batch = self.transaction_creation_batch.clone();
        let account_update_batch = self.account_update_batch.clone();
        let pending_results = self.pending_results.clone();
        let projection_store = self.projection_store.clone();
        let metrics = self.metrics.clone();

        // OPTIMIZATION 13: Spawn separate processors for each batch type
        let account_creation_handle = tokio::spawn({
            let batch = account_creation_batch.clone();
            let store = projection_store.clone();
            let results = pending_results.clone();
            let metrics = metrics.clone();

            async move { Self::process_account_creation_batches(batch, store, results, metrics).await }
        });

        let transaction_creation_handle = tokio::spawn({
            let batch = transaction_creation_batch.clone();
            let store = projection_store.clone();
            let results = pending_results.clone();
            let metrics = metrics.clone();

            async move {
                Self::process_transaction_creation_batches(batch, store, results, metrics).await
            }
        });

        let account_update_handle = tokio::spawn({
            let batch = account_update_batch.clone();
            let store = projection_store.clone();
            let results = pending_results.clone();
            let metrics = metrics.clone();

            async move { Self::process_account_update_batches(batch, store, results, metrics).await }
        });

        tracing::info!("🚀 Started optimized CDC processor with 3 specialized batch processors",);

        // In a real service, you would store these handles to manage their lifecycle (e.g., for graceful shutdown).
        // For this application's structure, we spawn them as background tasks that run indefinitely.
        // We do NOT `await` or `join` them here, as that would block the `start_processing` call forever.
        // The handles will be dropped, but the tasks will continue to run in the background.
        Ok(())
    }

    // OPTIMIZATION 14: Ultra-fast account creation processing (lock-free)
    async fn process_account_creation_batches(
        batch: Arc<LockFreeBatch<AccountProjection>>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        pending_results: Arc<DashMap<Uuid, mpsc::Sender<CDCOperationResult>>>,
        metrics: ProcessorMetrics,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(5)); // Aggressive for lock-free
        tracing::info!("Starting account creation batch processor");
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if batch.should_flush(2000, Duration::from_millis(10)) && !batch.is_empty() {
                let batch_start = Instant::now();
                let items = batch.drain_all();
                let batch_size = items.len();

                if batch_size == 0 {
                    continue;
                }

                tracing::info!("⚡ ACCOUNT CREATION: Processing {} items", batch_size);

                // Extract projections for bulk insert
                let projections: Vec<AccountProjection> = items
                    .iter()
                    .map(|(_, projection, _)| projection.clone())
                    .collect();

                // OPTIMIZATION 15: Single bulk INSERT (perfect for unique data)
                match projection_store
                    .bulk_insert_new_accounts_with_copy(projections)
                    .await
                {
                    Ok(_) => {
                        // Notify all successful operations
                        for (aggregate_id, _, operation_id) in items {
                            if let Some((_, sender)) = pending_results.remove(&operation_id) {
                                let result = CDCOperationResult {
                                    operation_id,
                                    success: true,
                                    aggregate_id: Some(aggregate_id),
                                    error: None,
                                    duration: batch_start.elapsed(),
                                };
                                let _ = sender.try_send(result);
                            }
                        }

                        let duration = batch_start.elapsed();
                        metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

                        tracing::info!(
                            "✅ ACCOUNT CREATION: {} accounts created in {:?} ({:.0} accounts/sec)",
                            batch_size,
                            duration,
                            batch_size as f64 / duration.as_secs_f64()
                        );
                    }
                    Err(e) => {
                        tracing::error!("❌ Account creation batch failed: {:?}", e);

                        // Notify all failed operations
                        for (aggregate_id, _, operation_id) in items {
                            if let Some((_, sender)) = pending_results.remove(&operation_id) {
                                let result = CDCOperationResult {
                                    operation_id,
                                    success: false,
                                    error: Some(e.to_string()),
                                    aggregate_id: Some(aggregate_id),
                                    duration: batch_start.elapsed(),
                                };
                                let _ = sender.try_send(result);
                            }
                        }
                    }
                }
            }

            // Minimal sleep for empty batches
            if batch.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    // OPTIMIZATION 16: Ultra-fast transaction creation processing (lock-free)
    async fn process_transaction_creation_batches(
        batch: Arc<LockFreeBatch<TransactionProjection>>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        pending_results: Arc<DashMap<Uuid, mpsc::Sender<CDCOperationResult>>>,
        metrics: ProcessorMetrics,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(5)); // Aggressive for lock-free
        tracing::info!("Starting transaction creation batch processor");
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if batch.should_flush(3000, Duration::from_millis(10)) && !batch.is_empty() {
                let batch_start = Instant::now();
                let items = batch.drain_all();
                let batch_size = items.len();

                if batch_size == 0 {
                    continue;
                }

                tracing::info!("💳 TRANSACTION CREATION: Processing {} items", batch_size);

                // Extract transactions for bulk insert
                let transactions: Vec<TransactionProjection> = items
                    .iter()
                    .map(|(_, transaction, _)| transaction.clone())
                    .collect();

                // OPTIMIZATION 17: Single bulk INSERT (perfect for unique transactions)
                match projection_store
                    .bulk_insert_transaction_projections(transactions)
                    .await
                {
                    Ok(_) => {
                        // Notify all successful operations
                        for (aggregate_id, _, operation_id) in items {
                            if let Some((_, sender)) = pending_results.remove(&operation_id) {
                                let result = CDCOperationResult {
                                    operation_id,
                                    success: true,
                                    error: None,
                                    aggregate_id: Some(aggregate_id),
                                    duration: batch_start.elapsed(),
                                };
                                let _ = sender.try_send(result);
                            }
                        }

                        let duration = batch_start.elapsed();
                        metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

                        tracing::info!(
                            "✅ TRANSACTION CREATION: {} transactions created in {:?} ({:.0} tx/sec)",
                            batch_size, duration,
                            batch_size as f64 / duration.as_secs_f64()
                        );
                    }
                    Err(e) => {
                        let duration = batch_start.elapsed();
                        metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

                        tracing::info!(
                            "✅ TRANSACTION CREATION: {} transactions created in {:?} ({:.0} tx/sec)",
                            batch_size, duration,
                            batch_size as f64 / duration.as_secs_f64()
                        );
                    }
                    Err(e) => {
                        tracing::error!("❌ Transaction creation batch failed: {:?}", e);

                        // Notify all failed operations
                        for (aggregate_id, _, operation_id) in items {
                            if let Some((_, sender)) = pending_results.remove(&operation_id) {
                                let result = CDCOperationResult {
                                    operation_id,
                                    success: false,
                                    error: Some(e.to_string()),
                                    aggregate_id: Some(aggregate_id),
                                    duration: batch_start.elapsed(),
                                };
                                let _ = sender.try_send(result);
                            }
                        }
                    }
                }
            }

            // Minimal sleep for empty batches
            if batch.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    // OPTIMIZATION 18: Careful account update processing (with conflict handling)
    async fn process_account_update_batches(
        batch: Arc<LockedBatch<AccountProjection>>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        pending_results: Arc<DashMap<Uuid, mpsc::Sender<CDCOperationResult>>>,
        metrics: ProcessorMetrics,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(10)); // Slower for locked operations
        tracing::info!("Starting account update batch processor");
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if batch.should_flush(1000, Duration::from_millis(25)).await && !batch.is_empty().await
            {
                let batch_start = Instant::now();
                let items = batch.drain_all().await;
                let batch_size = items.len();

                if batch_size == 0 {
                    continue;
                }
                tracing::info!("🔄 ACCOUNT UPDATE: Processing {} items", batch_size);

                // OPTIMIZATION 19: Detect and handle potential conflicts
                let mut unique_accounts: std::collections::HashMap<
                    Uuid,
                    (AccountProjection, Vec<Uuid>),
                > = std::collections::HashMap::new();

                // Group by account ID to handle conflicts
                for (account_id, projection, operation_id) in items {
                    unique_accounts
                        .entry(account_id)
                        .and_modify(|(latest_proj, op_ids)| {
                            // Keep the latest update
                            if projection.updated_at > latest_proj.updated_at {
                                *latest_proj = projection.clone();
                            }
                            op_ids.push(operation_id);
                        })
                        .or_insert((projection, vec![operation_id]));
                }

                let final_projections: Vec<AccountProjection> = unique_accounts
                    .values()
                    .map(|(proj, _)| proj.clone())
                    .collect();

                tracing::info!(
                    "🔄 ACCOUNT UPDATE: Resolved {} updates to {} unique accounts",
                    batch_size,
                    final_projections.len()
                );

                // OPTIMIZATION 20: Bulk UPDATE with conflict resolution
                match projection_store
                    .bulk_update_accounts_with_copy(final_projections)
                    .await
                {
                    Ok(_) => {
                        // Notify all successful operations
                        for (aggregate, operation_ids) in unique_accounts.values() {
                            for operation_id in operation_ids {
                                if let Some((_, sender)) = pending_results.remove(operation_id) {
                                    let result = CDCOperationResult {
                                        operation_id: *operation_id,
                                        success: true,
                                        error: None,
                                        aggregate_id: Some(aggregate.id),
                                        duration: batch_start.elapsed(),
                                    };
                                    let _ = sender.try_send(result);
                                }
                            }
                        }

                        let duration = batch_start.elapsed();
                        metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

                        tracing::info!(
                            "✅ ACCOUNT UPDATE: {} accounts updated in {:?} ({:.0} updates/sec)",
                            batch_size,
                            duration,
                            batch_size as f64 / duration.as_secs_f64()
                        );
                    }
                    Err(e) => {
                        tracing::error!("❌ Account update batch failed: {:?}", e);

                        // Notify all failed operations
                        for (aggregate, operation_ids) in unique_accounts.values() {
                            for operation_id in operation_ids {
                                if let Some((_, sender)) = pending_results.remove(operation_id) {
                                    let result = CDCOperationResult {
                                        operation_id: *operation_id,
                                        success: false,
                                        error: Some(e.to_string()),
                                        aggregate_id: Some(aggregate.id),
                                        duration: batch_start.elapsed(),
                                    };
                                    let _ = sender.try_send(result);
                                }
                            }
                        }
                    }
                }
            }

            // Sleep for empty batches
            if batch.is_empty().await {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    // OPTIMIZATION 21: Get comprehensive metrics
    pub fn get_performance_metrics(&self) -> String {
        let (created, updated, transactions, batches) = self.metrics.get_totals();

        format!(
            "CDC Metrics: {} accounts created, {} accounts updated, {} transactions created, {} batches processed",
            created, updated, transactions, batches
        )
    }
}

/*
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
    UpdateEvents,   // UPDATE, requires locking
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

    pub fn new_update_events_batch(partition_id: Option<usize>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            projections: Vec::with_capacity(1000), // Pre-allocate
            transaction_projections: Vec::new(),
            created_at: Instant::now(),
            partition_id,
            batch_type: CDCBatchType::UpdateEvents,
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
            current_batch: Arc::new(Mutex::new(CDCBatch::new_update_events_batch(Some(
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
            processing_type: CDCBatchType::UpdateEvents,
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
        let base_interval = Duration::from_millis(config.max_poll_interval_ms.min(5)); // Cap at 5ms
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
                                CDCBatchType::UpdateEvents => CDCBatch::new_update_events_batch(partition_id),
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
                            CDCBatchType::UpdateEvents => {
                                Self::execute_update_events_batch(&batch, &projection_store, &write_pool, &config).await
                            }
                            CDCBatchType::Transactions => {
                                Self::execute_transaction_batch(&batch, &projection_store, &config).await
                            }
                        };

                        let processing_duration = batch_start.elapsed();
                        batches_processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        projections_processed.fetch_add(last_batch_size as u64, std::sync::atomic::Ordering::Relaxed);

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
    async fn execute_update_events_batch(
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
        let mut pending_guard = pending_results.lock().await;
        let mut completed_guard = completed_results.lock().await;
        for (operation_id, result) in results {
            if let Some(sender) = pending_guard.remove(&result.operation_id) {
                // Try to send result, but don't block if receiver is gone
                let _ = sender.try_send(result.clone());
            }
            completed_guard.insert(operation_id, result);
        }
        // Clean up old completed results to prevent memory leaks
        if completed_guard.len() > 10000 {
            let cutoff = completed_guard.len() - 5000;
            let keys_to_remove: Vec<Uuid> = completed_guard.keys().take(cutoff).cloned().collect();
            for key in keys_to_remove {
                completed_guard.remove(&key);
            }
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

    pub async fn submit_update_events_bulk(
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
} */

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
        tracing::info!("Loading CDCBatchingConfig from environment");
        Self {
            max_batch_size: get_cdc_default_batch_size(), // Standard batch size
            max_batch_wait_time_ms: get_cdc_write_batch_timeout_ms(), // Fast response time
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
