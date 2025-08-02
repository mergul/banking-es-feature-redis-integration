use crate::domain::{Account, AccountEvent};
use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use anyhow::{Context, Result};
use fxhash::FxHashMap;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const NUM_READ_PARTITIONS: usize = 8;
const DEFAULT_READ_BATCH_SIZE: usize = 100;
const READ_CACHE_TTL_SECS: u64 = 300;

fn partition_for_read_operation(account_id: &Uuid, num_partitions: usize) -> usize {
    let mut hasher = fxhash::FxHasher::default();
    account_id.hash(&mut hasher);
    (hasher.finish() as usize) % num_partitions
}

/// Enhanced configuration for read batching
#[derive(Debug, Clone)]
pub struct ReadBatchingConfig {
    pub max_batch_size: usize,
    pub max_batch_wait_time_ms: u64,
    pub num_read_partitions: usize,
    pub enable_parallel_reads: bool,
    pub cache_ttl_secs: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

impl Default for ReadBatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_READ_BATCH_SIZE,
            max_batch_wait_time_ms: 50,
            num_read_partitions: NUM_READ_PARTITIONS,
            enable_parallel_reads: true,
            cache_ttl_secs: READ_CACHE_TTL_SECS,
            max_retries: 2,
            retry_backoff_ms: 5,
        }
    }
}

/// Enhanced read operation types
#[derive(Debug, Clone)]
pub enum ReadOperation {
    GetAccount { account_id: Uuid },
    GetAccountTransactions { account_id: Uuid },
    GetMultipleAccounts { account_ids: Vec<Uuid> },
    GetAccountBalance { account_id: Uuid },
    GetAccountHistory { account_id: Uuid, limit: usize },
}

impl ReadOperation {
    pub fn get_account_ids(&self) -> Vec<Uuid> {
        match self {
            ReadOperation::GetAccount { account_id } => vec![*account_id],
            ReadOperation::GetAccountTransactions { account_id } => vec![*account_id],
            ReadOperation::GetMultipleAccounts { account_ids } => account_ids.clone(),
            ReadOperation::GetAccountBalance { account_id } => vec![*account_id],
            ReadOperation::GetAccountHistory { account_id, .. } => vec![*account_id],
        }
    }

    pub fn get_primary_account_id(&self) -> Uuid {
        match self {
            ReadOperation::GetAccount { account_id } => *account_id,
            ReadOperation::GetAccountTransactions { account_id } => *account_id,
            ReadOperation::GetMultipleAccounts { account_ids } => account_ids[0],
            ReadOperation::GetAccountBalance { account_id } => *account_id,
            ReadOperation::GetAccountHistory { account_id, .. } => *account_id,
        }
    }
}

/// Enhanced result of a read operation
#[derive(Debug, Clone)]
pub enum ReadOperationResult {
    Account {
        account_id: Uuid,
        account: Option<AccountProjection>,
    },
    AccountTransactions {
        account_id: Uuid,
        transactions: Vec<AccountEvent>,
    },
    MultipleAccounts {
        accounts: Vec<AccountProjection>,
    },
    AccountBalance {
        account_id: Uuid,
        balance: Option<Decimal>,
    },
    AccountHistory {
        account_id: Uuid,
        history: Vec<AccountEvent>,
    },
}

/// Enhanced read batch for processing multiple read operations
#[derive(Debug)]
pub struct ReadBatch {
    pub batch_id: Uuid,
    pub operations: Vec<(Uuid, ReadOperation)>, // (operation_id, operation)
    pub created_at: Instant,
}

impl ReadBatch {
    pub fn new() -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            operations: Vec::new(),
            created_at: Instant::now(),
        }
    }

    pub fn add_operation(&mut self, operation_id: Uuid, operation: ReadOperation) {
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

/// Enhanced partitioned read batching service
pub struct PartitionedReadBatching {
    config: ReadBatchingConfig,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    read_pools: Vec<Arc<PgPool>>,

    // Batching state per partition
    partitions: Vec<Arc<ReadBatchingPartition>>,

    // Metrics
    batches_processed: Arc<Mutex<u64>>,
    operations_processed: Arc<Mutex<u64>>,
    cache_hits: Arc<Mutex<u64>>,
    cache_misses: Arc<Mutex<u64>>,
}

impl PartitionedReadBatching {
    pub async fn new(
        config: ReadBatchingConfig,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        read_pools: Vec<Arc<PgPool>>,
    ) -> Result<Self> {
        let mut partitions = Vec::new();

        for partition_id in 0..config.num_read_partitions {
            let partition = Arc::new(ReadBatchingPartition::new(
                partition_id,
                config.clone(),
                projection_store.clone(),
                read_pools[partition_id % read_pools.len()].clone(),
            ));
            partitions.push(partition);
        }

        Ok(Self {
            config,
            projection_store,
            read_pools,
            partitions,
            batches_processed: Arc::new(Mutex::new(0)),
            operations_processed: Arc::new(Mutex::new(0)),
            cache_hits: Arc::new(Mutex::new(0)),
            cache_misses: Arc::new(Mutex::new(0)),
        })
    }

    /// Submit a single read operation with enhanced batching
    pub async fn submit_read_operation(&self, operation: ReadOperation) -> Result<Uuid> {
        let operation_id = Uuid::new_v4();
        let account_ids = operation.get_account_ids();

        if account_ids.is_empty() {
            return Err(anyhow::anyhow!("No account IDs in read operation"));
        }

        // Determine partition based on primary account ID
        let partition_id =
            partition_for_read_operation(&account_ids[0], self.config.num_read_partitions);
        let partition = &self.partitions[partition_id];

        partition.submit_operation(operation_id, operation).await?;

        Ok(operation_id)
    }

    /// Enhanced batch processing for multiple read operations
    pub async fn submit_read_operations_batch(
        &self,
        operations: Vec<ReadOperation>,
    ) -> Result<Vec<Uuid>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        info!(
            "📖 Starting enhanced batch read processing for {} operations across {} partitions",
            operations.len(),
            self.config.num_read_partitions
        );

        // Group operations by partition for efficient processing
        let mut operations_by_partition: HashMap<usize, Vec<(Uuid, ReadOperation)>> =
            HashMap::new();

        for operation in operations {
            let account_ids = operation.get_account_ids();
            if account_ids.is_empty() {
                continue;
            }

            let partition_id =
                partition_for_read_operation(&account_ids[0], self.config.num_read_partitions);
            let operation_id = Uuid::new_v4();

            operations_by_partition
                .entry(partition_id)
                .or_default()
                .push((operation_id, operation));
        }

        // Process each partition in parallel with enhanced error handling
        let mut handles = Vec::new();
        for (partition_id, partition_operations) in operations_by_partition {
            let partition = self.partitions[partition_id].clone();
            handles.push(tokio::spawn(async move {
                let mut operation_ids = Vec::new();
                for (operation_id, operation) in partition_operations {
                    if let Ok(_) = partition.submit_operation(operation_id, operation).await {
                        operation_ids.push(operation_id);
                    }
                }
                (partition_id, operation_ids)
            }));
        }

        // Collect all operation IDs with partition information
        let mut all_operation_ids = Vec::new();
        for handle in handles {
            if let Ok((partition_id, operation_ids)) = handle.await {
                let operation_count = operation_ids.len();
                all_operation_ids.extend(operation_ids);
                info!(
                    "✅ Partition {} processed {} operations",
                    partition_id, operation_count
                );
            }
        }

        info!(
            "✅ Enhanced batch read processing completed with {} operation IDs",
            all_operation_ids.len()
        );

        Ok(all_operation_ids)
    }

    /// Enhanced aggregate-based parallel batching for read operations
    pub async fn submit_read_operations_aggregate_based_parallel(
        &self,
        operations: Vec<ReadOperation>,
    ) -> Result<Vec<Uuid>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let operations_count = operations.len();
        info!(
            "🚀 Starting aggregate-based parallel read batching for {} operations",
            operations_count
        );

        // Group operations by account_id for efficient processing
        let mut account_groups: HashMap<Uuid, Vec<ReadOperation>> = HashMap::new();
        for operation in operations {
            let account_id = operation.get_primary_account_id();
            account_groups
                .entry(account_id)
                .or_default()
                .push(operation);
        }

        let account_groups_count = account_groups.len();
        info!(
            "📦 Grouped {} operations into {} account groups",
            operations_count, account_groups_count
        );

        // Process each account group in parallel
        let mut futures = Vec::new();
        for (account_id, account_operations) in account_groups {
            let partition_id =
                partition_for_read_operation(&account_id, self.config.num_read_partitions);
            let partition = &self.partitions[partition_id];

            let future = async move {
                let mut results = Vec::new();
                for operation in account_operations {
                    let operation_id = Uuid::new_v4();
                    if let Ok(_) = partition.submit_operation(operation_id, operation).await {
                        results.push(operation_id);
                    }
                }
                (account_id, results)
            };

            futures.push(future);
        }

        // Wait for all parallel processing to complete
        let results = futures::future::join_all(futures).await;

        let mut all_operation_ids = Vec::new();
        let mut successful_count = 0;

        for (account_id, operation_ids) in results {
            all_operation_ids.extend(operation_ids.clone());
            successful_count += operation_ids.len();
            info!(
                "✅ Processed {} read operations for account {}",
                operation_ids.len(),
                account_id
            );
        }

        info!(
            "✅ Aggregate-based parallel read batching completed: {} successful operations across {} accounts",
            successful_count,
            account_groups_count
        );

        Ok(all_operation_ids)
    }

    /// Wait for a read operation result with enhanced error handling
    pub async fn wait_for_result(&self, operation_id: Uuid) -> Result<ReadOperationResult> {
        // Try all partitions to find the result with timeout
        let timeout = Duration::from_secs(30);

        match tokio::time::timeout(timeout, async {
            for partition in &self.partitions {
                if let Ok(result) = partition.wait_for_result(operation_id).await {
                    return Ok(result);
                }
            }
            Err(anyhow::anyhow!("Operation not found in any partition"))
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "Timeout waiting for operation {} result after 30 seconds",
                operation_id
            )),
        }
    }

    /// Get current batch size for smart consistency optimization
    pub async fn get_current_batch_size(&self) -> usize {
        let mut total_batch_size = 0;

        // Sum up current batch sizes across all partitions
        for partition in &self.partitions {
            total_batch_size += partition.get_current_batch_size().await;
        }

        total_batch_size
    }

    /// Get enhanced statistics
    pub async fn get_stats(&self) -> serde_json::Value {
        let batches_processed = *self.batches_processed.lock().await;
        let operations_processed = *self.operations_processed.lock().await;
        let cache_hits = *self.cache_hits.lock().await;
        let cache_misses = *self.cache_misses.lock().await;

        // Get partition-specific stats
        let mut partition_stats = Vec::new();
        for (i, partition) in self.partitions.iter().enumerate() {
            let partition_stat = partition.get_stats().await;
            partition_stats.push((i, partition_stat));
        }

        serde_json::json!({
            "batches_processed": batches_processed,
            "operations_processed": operations_processed,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "cache_hit_rate": if cache_hits + cache_misses > 0 {
                (cache_hits as f64 / (cache_hits + cache_misses) as f64) * 100.0
            } else {
                0.0
            },
            "partition_stats": partition_stats,
            "num_partitions": self.config.num_read_partitions,
        })
    }
}

/// Enhanced individual read batching partition
pub struct ReadBatchingPartition {
    partition_id: usize,
    config: ReadBatchingConfig,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    read_pool: Arc<PgPool>,

    // Current batch
    current_batch: Arc<Mutex<ReadBatch>>,

    // Pending results with enhanced channel management
    pending_results: Arc<Mutex<HashMap<Uuid, mpsc::Sender<ReadOperationResult>>>>,

    // Completed results for better performance
    completed_results: Arc<Mutex<HashMap<Uuid, ReadOperationResult>>>,

    // Processing
    batch_processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_token: CancellationToken,

    // Metrics
    batches_processed: Arc<Mutex<u64>>,
    operations_processed: Arc<Mutex<u64>>,
}

impl ReadBatchingPartition {
    pub fn new(
        partition_id: usize,
        config: ReadBatchingConfig,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        read_pool: Arc<PgPool>,
    ) -> Self {
        Self {
            partition_id,
            config,
            projection_store,
            read_pool,
            current_batch: Arc::new(Mutex::new(ReadBatch::new())),
            pending_results: Arc::new(Mutex::new(HashMap::new())),
            completed_results: Arc::new(Mutex::new(HashMap::new())),
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            batches_processed: Arc::new(Mutex::new(0)),
            operations_processed: Arc::new(Mutex::new(0)),
        }
    }

    /// Submit an operation to this partition with enhanced batching
    pub async fn submit_operation(
        &self,
        operation_id: Uuid,
        operation: ReadOperation,
    ) -> Result<()> {
        let (tx, _rx) = mpsc::channel::<ReadOperationResult>(10);

        // Store the sender for later result delivery
        {
            let mut pending_results = self.pending_results.lock().await;
            pending_results.insert(operation_id, tx);
        }

        // Add to current batch
        {
            let mut current_batch = self.current_batch.lock().await;
            current_batch.add_operation(operation_id, operation);

            // Start processor if not already running
            if current_batch.should_process(
                self.config.max_batch_size,
                Duration::from_millis(self.config.max_batch_wait_time_ms),
            ) {
                self.start_batch_processor().await?;
            }
        }

        Ok(())
    }

    /// Start the batch processor with enhanced error handling
    async fn start_batch_processor(&self) -> Result<()> {
        let mut processor_handle = self.batch_processor_handle.lock().await;

        if processor_handle.is_some() {
            return Ok(()); // Already running
        }

        let current_batch = self.current_batch.clone();
        let pending_results = self.pending_results.clone();
        let completed_results = self.completed_results.clone();
        let projection_store = self.projection_store.clone();
        let read_pool = self.read_pool.clone();
        let config = self.config.clone();
        let partition_id = self.partition_id;
        let batches_processed = self.batches_processed.clone();
        let operations_processed = self.operations_processed.clone();
        let shutdown_token = self.shutdown_token.clone();

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
                            let has_operations = {
                                let batch = current_batch.lock().await;
                                !batch.operations.is_empty()
                            };

                            if has_operations {
                                Self::process_read_batch(
                                    &current_batch,
                                    &pending_results,
                                    &completed_results,
                                    &projection_store,
                                    &read_pool,
                                    &config,
                                    &batches_processed,
                                    &operations_processed,
                                    partition_id,
                                ).await;
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("🛑 Read batching partition {} shutdown signal received", partition_id);
                        break;
                    }
                }
            }

            // Process any remaining operations
            Self::process_read_batch(
                &current_batch,
                &pending_results,
                &completed_results,
                &projection_store,
                &read_pool,
                &config,
                &batches_processed,
                &operations_processed,
                partition_id,
            )
            .await;
        });

        *processor_handle = Some(handle);

        Ok(())
    }

    /// Enhanced batch processing without Redis locking for read operations
    async fn process_read_batch(
        current_batch: &Arc<Mutex<ReadBatch>>,
        pending_results: &Arc<Mutex<HashMap<Uuid, mpsc::Sender<ReadOperationResult>>>>,
        completed_results: &Arc<Mutex<HashMap<Uuid, ReadOperationResult>>>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        read_pool: &Arc<PgPool>,
        config: &ReadBatchingConfig,
        batches_processed: &Arc<Mutex<u64>>,
        operations_processed: &Arc<Mutex<u64>>,
        partition_id: usize,
    ) {
        let batch = {
            let mut batch_guard = current_batch.lock().await;
            let batch = ReadBatch {
                batch_id: batch_guard.batch_id,
                operations: batch_guard.operations.clone(),
                created_at: batch_guard.created_at,
            };
            *batch_guard = ReadBatch::new(); // Reset for next batch
            batch
        };

        if batch.operations.is_empty() {
            return;
        }

        info!(
            "📖 Processing enhanced read batch {} with {} operations in partition {}",
            batch.batch_id,
            batch.operations.len(),
            partition_id
        );

        // Group operations by type for efficient processing
        let mut account_operations = Vec::new();
        let mut transaction_operations = Vec::new();
        let mut multiple_account_operations = Vec::new();
        let mut balance_operations = Vec::new();
        let mut history_operations = Vec::new();

        for (operation_id, operation) in &batch.operations {
            match operation {
                ReadOperation::GetAccount { account_id } => {
                    account_operations.push((*operation_id, *account_id));
                }
                ReadOperation::GetAccountTransactions { account_id } => {
                    transaction_operations.push((*operation_id, *account_id));
                }
                ReadOperation::GetMultipleAccounts { account_ids } => {
                    multiple_account_operations.push((*operation_id, account_ids.clone()));
                }
                ReadOperation::GetAccountBalance { account_id } => {
                    balance_operations.push((*operation_id, *account_id));
                }
                ReadOperation::GetAccountHistory { account_id, limit } => {
                    history_operations.push((*operation_id, *account_id, *limit));
                }
            }
        }

        // Process account operations in batch without Redis locking
        if !account_operations.is_empty() {
            let account_ids: Vec<Uuid> = account_operations.iter().map(|(_, id)| *id).collect();

            // Batch query for accounts - no locking needed for reads
            let accounts_result = Self::batch_get_accounts(read_pool, &account_ids).await;

            // Send results
            for (operation_id, account_id) in account_operations {
                let result = match &accounts_result {
                    Ok(accounts) => {
                        let account = accounts.iter().find(|a| a.id == account_id).cloned();
                        ReadOperationResult::Account {
                            account_id,
                            account,
                        }
                    }
                    Err(_) => ReadOperationResult::Account {
                        account_id,
                        account: None,
                    },
                };

                // Store completed result
                completed_results
                    .lock()
                    .await
                    .insert(operation_id, result.clone());

                if let Some(sender) = pending_results.lock().await.remove(&operation_id) {
                    let _ = sender.send(result).await;
                }
            }
        }

        // Process transaction operations with enhanced error handling
        if !transaction_operations.is_empty() {
            for (operation_id, account_id) in transaction_operations {
                let transactions = projection_store
                    .get_account_transactions(account_id)
                    .await
                    .unwrap_or_default();

                let account_events: Vec<AccountEvent> = transactions
                    .into_iter()
                    .filter_map(|tx| {
                        Some(AccountEvent::MoneyDeposited {
                            account_id: tx.account_id,
                            amount: tx.amount,
                            transaction_id: tx.id,
                        })
                    })
                    .collect();

                let result = ReadOperationResult::AccountTransactions {
                    account_id,
                    transactions: account_events,
                };

                completed_results
                    .lock()
                    .await
                    .insert(operation_id, result.clone());

                if let Some(sender) = pending_results.lock().await.remove(&operation_id) {
                    let _ = sender.send(result).await;
                }
            }
        }

        // Process multiple account operations
        for (operation_id, account_ids) in multiple_account_operations {
            let accounts_result = Self::batch_get_accounts(read_pool, &account_ids).await;

            let result = match accounts_result {
                Ok(accounts) => ReadOperationResult::MultipleAccounts { accounts },
                Err(_) => ReadOperationResult::MultipleAccounts {
                    accounts: Vec::new(),
                },
            };

            completed_results
                .lock()
                .await
                .insert(operation_id, result.clone());

            if let Some(sender) = pending_results.lock().await.remove(&operation_id) {
                let _ = sender.send(result).await;
            }
        }

        // Process balance operations
        for (operation_id, account_id) in balance_operations {
            let balance = Self::get_account_balance(read_pool, account_id).await;
            let result = ReadOperationResult::AccountBalance {
                account_id,
                balance,
            };

            completed_results
                .lock()
                .await
                .insert(operation_id, result.clone());

            if let Some(sender) = pending_results.lock().await.remove(&operation_id) {
                let _ = sender.send(result).await;
            }
        }

        // Process history operations
        for (operation_id, account_id, limit) in history_operations {
            let history = Self::get_account_history(read_pool, account_id, limit).await;
            let result = ReadOperationResult::AccountHistory {
                account_id,
                history,
            };

            completed_results
                .lock()
                .await
                .insert(operation_id, result.clone());

            if let Some(sender) = pending_results.lock().await.remove(&operation_id) {
                let _ = sender.send(result).await;
            }
        }

        // Update metrics
        {
            let mut batches = batches_processed.lock().await;
            *batches += 1;
        }
        {
            let mut operations = operations_processed.lock().await;
            *operations += batch.operations.len() as u64;
        }

        info!(
            "✅ Enhanced read batch {} completed in partition {}",
            batch.batch_id, partition_id
        );
    }

    /// Enhanced batch get multiple accounts with retry mechanism
    async fn batch_get_accounts(
        read_pool: &PgPool,
        account_ids: &[Uuid],
    ) -> Result<Vec<AccountProjection>> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut retry_count = 0;
        let max_retries = 3;

        while retry_count < max_retries {
            match sqlx::query_as!(
                AccountProjection,
                "SELECT * FROM account_projections WHERE id = ANY($1)",
                account_ids
            )
            .fetch_all(read_pool)
            .await
            {
                Ok(accounts) => return Ok(accounts),
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        return Err(anyhow::anyhow!(
                            "Failed to get accounts after {} retries: {}",
                            max_retries,
                            e
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(10 * retry_count as u64)).await;
                }
            }
        }

        Ok(Vec::new())
    }

    /// Get account balance with enhanced error handling
    async fn get_account_balance(read_pool: &PgPool, account_id: Uuid) -> Option<Decimal> {
        match sqlx::query!(
            "SELECT balance FROM account_projections WHERE id = $1",
            account_id
        )
        .fetch_optional(read_pool)
        .await
        {
            Ok(Some(row)) => Some(row.balance),
            Ok(None) => None,
            Err(_) => None,
        }
    }

    /// Get account history with limit
    async fn get_account_history(
        read_pool: &PgPool,
        account_id: Uuid,
        limit: usize,
    ) -> Vec<AccountEvent> {
        // This would typically query the event store for account history
        // For now, return empty vector as placeholder
        Vec::new()
    }

    /// Enhanced wait for result with timeout and retry
    pub async fn wait_for_result(&self, operation_id: Uuid) -> Result<ReadOperationResult> {
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
        }

        // Wait for the result with timeout
        let timeout = Duration::from_secs(30);
        match tokio::time::timeout(timeout, async {
            let mut poll_interval = Duration::from_millis(10);
            let max_poll_interval = Duration::from_millis(100);

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
                        drop(pending);
                        let completed = self.completed_results.lock().await;
                        if let Some(result) = completed.get(&operation_id) {
                            return result.clone();
                        }
                        return ReadOperationResult::Account {
                            account_id: Uuid::nil(),
                            account: None,
                        };
                    }
                }

                tokio::time::sleep(poll_interval).await;
                poll_interval = std::cmp::min(poll_interval * 2, max_poll_interval);
            }
        })
        .await
        {
            Ok(result) => Ok(result),
            Err(_) => {
                self.pending_results.lock().await.remove(&operation_id);
                Err(anyhow::anyhow!(
                    "Timeout waiting for operation {} result after 30 seconds",
                    operation_id
                ))
            }
        }
    }

    /// Get current batch size for smart consistency optimization
    pub async fn get_current_batch_size(&self) -> usize {
        let current_batch = self.current_batch.lock().await;
        current_batch.operations.len()
    }

    /// Get partition statistics
    pub async fn get_stats(&self) -> serde_json::Value {
        let batches_processed = *self.batches_processed.lock().await;
        let operations_processed = *self.operations_processed.lock().await;
        let pending_count = self.pending_results.lock().await.len();
        let completed_count = self.completed_results.lock().await.len();

        serde_json::json!({
            "partition_id": self.partition_id,
            "batches_processed": batches_processed,
            "operations_processed": operations_processed,
            "pending_operations": pending_count,
            "completed_operations": completed_count,
        })
    }
}
