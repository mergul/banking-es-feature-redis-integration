use crate::domain::events::AccountEvent;
use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use crate::infrastructure::event_store::DB_POOL;
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, OnceCell, RwLock};
use tracing::{debug, error, info, warn, Level};
use uuid::Uuid;

// Enhanced error types for projections
#[derive(Debug)]
pub enum ProjectionError {
    DatabaseError(sqlx::Error),
    CacheError(String),
    BatchError(String),
}

impl std::fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionError::DatabaseError(e) => {
                f.write_str(&("Database error: ".to_string() + &e.to_string()))
            }
            ProjectionError::CacheError(e) => {
                f.write_str(&("Cache error: ".to_string() + &e.to_string()))
            }
            ProjectionError::BatchError(e) => {
                f.write_str(&("Batch processing error: ".to_string() + &e.to_string()))
            }
        }
    }
}

impl std::error::Error for ProjectionError {}

impl From<sqlx::Error> for ProjectionError {
    fn from(err: sqlx::Error) -> Self {
        ProjectionError::DatabaseError(err)
    }
}

// Projection metrics
#[derive(Debug, Default)]
struct ProjectionMetrics {
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
    batch_updates: std::sync::atomic::AtomicU64,
    events_processed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    query_duration: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountProjection {
    pub id: Uuid,
    pub owner_name: String,
    pub balance: Decimal,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionProjection {
    pub id: Uuid,
    pub account_id: Uuid,
    pub transaction_type: String,
    pub amount: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone)]
struct CacheEntry<T> {
    data: T,
    last_accessed: Instant,
    version: u64,
    ttl: Duration,
}

#[derive(Clone)]
pub struct ProjectionStore {
    pools: Arc<PartitionedPools>,
    account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
    transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
    update_sender: mpsc::UnboundedSender<ProjectionUpdate>,
    cache_version: Arc<std::sync::atomic::AtomicU64>,
    metrics: Arc<ProjectionMetrics>,
    config: ProjectionConfig,
}

#[derive(Debug, Clone)]
pub struct ProjectionConfig {
    pub cache_ttl_secs: u64,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

impl Default for ProjectionConfig {
    fn default() -> Self {
        Self {
            cache_ttl_secs: std::env::var("CACHE_DEFAULT_TTL")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
            batch_size: std::env::var("DB_BATCH_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            batch_timeout_ms: 50, // Increased from 25ms to 50ms
            max_connections: std::env::var("PROJECTION_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "80".to_string())
                .parse()
                .unwrap_or(80),
            min_connections: std::env::var("PROJECTION_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "20".to_string())
                .parse()
                .unwrap_or(20),
            acquire_timeout_secs: std::env::var("DB_ACQUIRE_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            idle_timeout_secs: std::env::var("DB_IDLE_TIMEOUT")
                .unwrap_or_else(|_| "600".to_string())
                .parse()
                .unwrap_or(600),
            max_lifetime_secs: std::env::var("DB_MAX_LIFETIME")
                .unwrap_or_else(|_| "1800".to_string())
                .parse()
                .unwrap_or(1800),
        }
    }
}

#[derive(Debug)]
enum ProjectionUpdate {
    AccountBatch(Vec<AccountProjection>),
    TransactionBatch(Vec<TransactionProjection>),
}

// Global connection pool (deprecated - now using partitioned pools)
// static PROJECTION_POOL: OnceCell<Arc<PgPool>> = OnceCell::const_new();

#[async_trait]
pub trait ProjectionStoreTrait: Send + Sync {
    async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>>;
    async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>>;
    async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>>;
    async fn upsert_accounts_batch(&self, accounts: Vec<AccountProjection>) -> Result<()>;
    async fn insert_transactions_batch(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()>;
}

#[async_trait]
impl ProjectionStoreTrait for ProjectionStore {
    async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>> {
        self.get_account(account_id).await
    }
    async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>> {
        self.get_all_accounts().await
    }
    async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>> {
        self.get_account_transactions(account_id).await
    }
    async fn upsert_accounts_batch(&self, accounts: Vec<AccountProjection>) -> Result<()> {
        tracing::info!(
            "ProjectionStore: upsert_accounts_batch called with {} accounts. IDs: {:?}",
            accounts.len(),
            accounts.iter().map(|a| a.id).collect::<Vec<_>>()
        );
        for acc in &accounts {
            tracing::info!(
                "[ProjectionStore] upsert_accounts_batch: account_id={}, owner_name={}",
                acc.id,
                acc.owner_name
            );
        }
        let send_result = self
            .update_sender
            .send(ProjectionUpdate::AccountBatch(accounts));
        match send_result {
            Ok(_) => tracing::info!(
                "[ProjectionStore] upsert_accounts_batch: sent to update_sender successfully"
            ),
            Err(ref e) => tracing::error!(
                "[ProjectionStore] upsert_accounts_batch: failed to send to update_sender: {}",
                e
            ),
        }
        send_result.map_err(|e| {
            anyhow::Error::msg("Failed to send account batch update: ".to_string() + &e.to_string())
        })?;
        Ok(())
    }
    async fn insert_transactions_batch(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()> {
        self.update_sender
            .send(ProjectionUpdate::TransactionBatch(transactions))
            .map_err(|e| {
                anyhow::Error::msg(
                    "Failed to send transaction batch update: ".to_string() + &e.to_string(),
                )
            })?;
        Ok(())
    }
}

impl ProjectionStore {
    pub fn new(pool: PgPool) -> Self {
        // Create partitioned pools from the single pool
        let pools = Arc::new(PartitionedPools {
            write_pool: pool.clone(),
            read_pool: pool,
            config:
                crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig::default(
                ),
        });
        Self::from_pools_with_config(pools, ProjectionConfig::default())
    }

    // pub fn new_test(pool: PgPool) -> Self {
    //     let mut config = ProjectionConfig::default();
    //     config.batch_size = 1; // Process immediately in test mode
    //     config.batch_timeout_ms = 0; // No batching in test mode
    //     let pools = Arc::new(PartitionedPools {
    //         write_pool: pool.clone(),
    //         read_pool: pool,
    //         config:
    //             crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig::default(
    //             ),
    //     });
    //     Self::from_pools_with_config(pools, config)
    // }

    pub fn from_pool_with_config(pool: PgPool, config: ProjectionConfig) -> Self {
        let pools = Arc::new(PartitionedPools {
            write_pool: pool.clone(),
            read_pool: pool,
            config:
                crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig::default(
                ),
        });
        Self::from_pools_with_config(pools, config)
    }

    pub fn from_pools_with_config(pools: Arc<PartitionedPools>, config: ProjectionConfig) -> Self {
        let account_cache = Arc::new(RwLock::new(HashMap::new()));
        let transaction_cache = Arc::new(RwLock::new(HashMap::new()));
        let (update_sender, update_receiver) = mpsc::unbounded_channel();
        let cache_version = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let metrics = Arc::new(ProjectionMetrics::default());

        // Clone before move
        let processor_cache_version = cache_version.clone();
        let processor_metrics = metrics.clone();

        let store = Self {
            pools: pools.clone(),
            account_cache: account_cache.clone(),
            transaction_cache: transaction_cache.clone(),
            update_sender,
            cache_version,
            metrics,
            config: config.clone(),
        };

        // Start background processor
        let processor_pools = pools.clone();
        let processor_account_cache = account_cache.clone();
        let processor_transaction_cache = transaction_cache.clone();
        let processor_config = config.clone();
        tokio::spawn(async move {
            Self::update_processor(
                processor_pools,
                update_receiver,
                processor_account_cache,
                processor_transaction_cache,
                processor_cache_version,
                processor_metrics,
                processor_config,
            )
            .await;
        });

        // Start metrics reporter
        let metrics = store.metrics.clone();
        tokio::spawn(Self::metrics_reporter(metrics));

        store
    }

    pub async fn new_with_config(config: ProjectionConfig) -> Result<Self> {
        panic!("ProjectionStore::new_with_config is not supported. Please use ProjectionStore::from_pool_with_config(pool, config) and pass a shared PgPool instance.");
    }

    pub async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>> {
        let start_time = Instant::now();
        println!("[DEBUG] get_account: start for {}", account_id);

        // Try cache first
        {
            println!(
                "[DEBUG] get_account: acquiring cache read lock for {}",
                account_id
            );
            let cache = self.account_cache.read().await;
            println!(
                "[DEBUG] get_account: acquired cache read lock for {}",
                account_id
            );
            if let Some(entry) = cache.get(&account_id) {
                if entry.last_accessed.elapsed() < entry.ttl {
                    println!("[DEBUG] get_account: cache hit for {}", account_id);
                    self.metrics
                        .cache_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(Some(entry.data.clone()));
                }
            }
        }
        println!("[DEBUG] get_account: cache miss for {}", account_id);

        self.metrics
            .cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Add debug print before DB query
        println!(
            "[DEBUG] ProjectionStore: About to query DB for account_id={}",
            account_id
        );
        // Print pool status if possible
        #[cfg(feature = "sqlx")] // Only if sqlx PgPool is used
        {
            println!(
                "[DEBUG] Pool status: size={}, num_idle={}",
                self.pools.write_pool.size(),
                self.pools.write_pool.num_idle()
            );
        }

        // Cache miss - fetch from database with prepared statement
        let account: Option<AccountProjection> = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE id = $1
            "#,
            account_id
        )
        .fetch_optional(&self.pools.write_pool)
        .await?;

        // Add debug print after DB query
        println!(
            "[DEBUG] ProjectionStore: Finished DB query for account_id={}",
            account_id
        );

        // Update cache if found
        if let Some(ref account) = account {
            let mut cache = self.account_cache.write().await;
            cache.insert(
                account_id,
                CacheEntry {
                    data: account.clone(),
                    last_accessed: Instant::now(),
                    version: self
                        .cache_version
                        .load(std::sync::atomic::Ordering::Relaxed),
                    ttl: Duration::from_secs(self.config.cache_ttl_secs),
                },
            );
        }

        self.metrics.query_duration.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(account)
    }

    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>> {
        let start_time = Instant::now();

        // Try cache first
        {
            let cache = self.transaction_cache.read().await;
            if let Some(entry) = cache.get(&account_id) {
                if entry.last_accessed.elapsed() < entry.ttl {
                    self.metrics
                        .cache_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(entry.data.clone());
                }
            }
        }

        self.metrics
            .cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Cache miss - fetch from database with prepared statement
        let transactions = sqlx::query_as!(
            TransactionProjection,
            r#"
            SELECT id, account_id, transaction_type, amount, timestamp
            FROM transaction_projections
            WHERE account_id = $1
            ORDER BY timestamp DESC
            LIMIT 1000
            "#,
            account_id
        )
        .fetch_all(self.pools.select_pool(OperationType::Read))
        .await?;

        // Update cache
        {
            let mut cache = self.transaction_cache.write().await;
            cache.insert(
                account_id,
                CacheEntry {
                    data: transactions.clone(),
                    last_accessed: Instant::now(),
                    version: self
                        .cache_version
                        .load(std::sync::atomic::Ordering::Relaxed),
                    ttl: Duration::from_secs(self.config.cache_ttl_secs),
                },
            );
        }

        self.metrics.query_duration.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(transactions)
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>> {
        let start_time = Instant::now();

        // TODO: Use materialized view account_projections_mv for better performance
        // Run migration: migrations/20241201000000_create_materialized_views.sql
        let accounts = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE is_active = true
            ORDER BY updated_at DESC
            LIMIT 10000
            "#
        )
        .fetch_all(self.pools.select_pool(OperationType::Read))
        .await?;

        self.metrics.query_duration.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(accounts)
    }

    pub async fn get_accounts_by_balance_range(
        &self,
        min_balance: Decimal,
        max_balance: Decimal,
    ) -> Result<Vec<AccountProjection>> {
        let start_time = Instant::now();

        // TODO: Use indexed materialized view for balance range queries
        let accounts = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE is_active = true 
            AND balance BETWEEN $1 AND $2
            ORDER BY balance DESC
            LIMIT 1000
            "#,
            min_balance,
            max_balance
        )
        .fetch_all(self.pools.select_pool(OperationType::Read))
        .await?;

        self.metrics.query_duration.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(accounts)
    }

    pub async fn get_top_accounts_by_balance(&self, limit: i64) -> Result<Vec<AccountProjection>> {
        let start_time = Instant::now();

        // TODO: Use materialized view with balance index for top accounts
        let accounts = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE is_active = true
            ORDER BY balance DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pools.select_pool(OperationType::Read))
        .await?;

        self.metrics.query_duration.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(accounts)
    }

    // async fn update_processor(
    //     pool: PgPool,
    //     mut receiver: mpsc::UnboundedReceiver<ProjectionUpdate>,
    //     account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
    //     transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
    //     config: ProjectionConfig,
    // ) {
    //     let mut account_batch = Vec::with_capacity(config.batch_size);
    //     let mut transaction_batch = Vec::with_capacity(config.batch_size);
    //     let mut last_flush = Instant::now();
    //     let batch_timeout = Duration::from_millis(config.batch_timeout_ms);
    //     let cache_version = Arc::new(std::sync::atomic::AtomicU64::new(0));
    //     let metrics = Arc::new(ProjectionMetrics::default());

    //     tracing::info!("[ProjectionStore] Background update_processor started with batch_size={} batch_timeout_ms={}", config.batch_size, config.batch_timeout_ms);

    //     while let Some(update) = receiver.recv().await {
    //         match update {
    //             ProjectionUpdate::AccountBatch(accounts) => {
    //                 let ids: Vec<_> = accounts.iter().map(|a| a.id).collect();
    //                 tracing::info!(
    //                     "[ProjectionStore] Received AccountBatch of size {}: ids={:?}",
    //                     accounts.len(),
    //                     ids
    //                 );
    //                 account_batch.extend(accounts);
    //             }
    //             ProjectionUpdate::TransactionBatch(transactions) => {
    //                 tracing::info!(
    //                     "[ProjectionStore] Received TransactionBatch of size {}",
    //                     transactions.len()
    //                 );
    //                 transaction_batch.extend(transactions);
    //             }
    //         }

    //         // Flush if batch size reached or timeout exceeded
    //         if account_batch.len() >= config.batch_size
    //             || transaction_batch.len() >= config.batch_size
    //             || last_flush.elapsed() >= batch_timeout
    //         {
    //             let ids: Vec<_> = account_batch.iter().map(|a| a.id).collect();
    //             tracing::info!("[ProjectionStore] Flushing batches: account_batch_size={}, transaction_batch_size={}, account_ids={:?}", account_batch.len(), transaction_batch.len(), ids);
    //             if let Err(e) = Self::flush_batches(
    //                 &pool,
    //                 &mut account_batch,
    //                 &mut transaction_batch,
    //                 &account_cache,
    //                 &transaction_cache,
    //                 &cache_version,
    //                 &metrics,
    //             )
    //             .await
    //             {
    //                 error!(
    //                     "[ProjectionStore] Failed to flush batches: {} (account_ids={:?})",
    //                     e, ids
    //                 );
    //             } else {
    //                 tracing::info!(
    //                     "[ProjectionStore] Successfully flushed batches (account_ids={:?})",
    //                     ids
    //                 );
    //             }
    //             last_flush = Instant::now();
    //         }
    //     }
    //     tracing::warn!("[ProjectionStore] update_processor exiting: channel closed");
    // }
    async fn update_processor(
        pools: Arc<PartitionedPools>,
        mut receiver: mpsc::UnboundedReceiver<ProjectionUpdate>,
        _account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
        _transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
        _cache_version: Arc<std::sync::atomic::AtomicU64>,
        metrics: Arc<ProjectionMetrics>,
        config: ProjectionConfig,
    ) {
        let mut account_batch = Vec::with_capacity(config.batch_size);
        let mut transaction_batch = Vec::with_capacity(config.batch_size);
        let mut interval = tokio::time::interval(Duration::from_millis(config.batch_timeout_ms));

        loop {
            tokio::select! {
                Some(update) = receiver.recv() => {
                    match update {
                        ProjectionUpdate::AccountBatch(accounts) => {
                            account_batch.extend(accounts);
                        }
                        ProjectionUpdate::TransactionBatch(transactions) => {
                            transaction_batch.extend(transactions);
                        }
                    }
                }
                _ = interval.tick() => {
                    if !account_batch.is_empty() {
                        tracing::info!("ProjectionStore: update_processor flushing account batch of size {}. IDs: {:?}", account_batch.len(), account_batch.iter().map(|a| a.id).collect::<Vec<_>>());
                        let batch_to_process = std::mem::take(&mut account_batch);
                        let pools = pools.clone();
                        let metrics = metrics.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::upsert_accounts_batch_parallel(&pools, &metrics, batch_to_process).await {
                                error!("[ProjectionStore] Error upserting account batch: {}", e);
                            }
                        });
                    }
                    if !transaction_batch.is_empty() {
                        let batch_to_process = std::mem::take(&mut transaction_batch);
                        let pools = pools.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::insert_transactions_batch_parallel(&pools, batch_to_process).await {
                                error!("[ProjectionStore] Error inserting transaction batch: {}", e);
                            }
                        });
                    }
                }
            }
        }
    }

    async fn upsert_accounts_batch_parallel(
        pools: &Arc<PartitionedPools>,
        metrics: &Arc<ProjectionMetrics>,
        accounts: Vec<AccountProjection>,
    ) -> Result<()> {
        let account_count = accounts.len();
        let account_ids: Vec<_> = accounts.iter().map(|a| a.id).collect();
        tracing::info!(
            "ProjectionStore: upsert_accounts_batch_parallel flushing {} accounts to DB. IDs: {:?}",
            account_count,
            account_ids
        );

        // Use bulk upsert instead of individual transactions for better performance
        let pool = pools.select_pool(OperationType::Write).clone();
        let mut tx = pool.begin().await?;

        // Process accounts in chunks for better memory management
        let chunk_size = 100; // Process 100 accounts at a time
        for chunk in accounts.chunks(chunk_size) {
            Self::bulk_upsert_accounts(&mut tx, chunk).await?;
        }

        tx.commit().await?;

        // Increment batch_updates metric
        metrics
            .batch_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            "ProjectionStore: upsert_accounts_batch_parallel DB upsert completed successfully for {} accounts. IDs: {:?}",
            account_count,
            account_ids
        );
        Ok(())
    }

    async fn insert_transactions_batch_parallel(
        pools: &Arc<PartitionedPools>,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()> {
        // Use bulk insert instead of individual transactions for better performance
        let pool = pools.select_pool(OperationType::Write).clone();
        let mut tx = pool.begin().await?;

        // Process transactions in chunks for better memory management
        let chunk_size = 100; // Process 100 transactions at a time
        for chunk in transactions.chunks(chunk_size) {
            Self::bulk_insert_transactions(&mut tx, chunk).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn bulk_upsert_accounts(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        accounts: &[AccountProjection],
    ) -> Result<()> {
        let account_ids: Vec<_> = accounts.iter().map(|a| a.id).collect();
        tracing::info!(
            "[ProjectionStore] bulk_upsert_accounts: about to upsert accounts: ids={:?}",
            account_ids
        );
        for acc in accounts {
            tracing::info!(
                "[ProjectionStore] bulk_upsert_accounts: account_id={}, owner_name={}",
                acc.id,
                acc.owner_name
            );
        }
        if accounts.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = accounts.iter().map(|a| a.id).collect();
        let owner_names: Vec<String> = accounts.iter().map(|a| a.owner_name.clone()).collect();
        let balances: Vec<Decimal> = accounts.iter().map(|a| a.balance).collect();
        let is_actives: Vec<bool> = accounts.iter().map(|a| a.is_active).collect();
        let created_ats: Vec<DateTime<Utc>> = accounts.iter().map(|a| a.created_at).collect();
        let updated_ats: Vec<DateTime<Utc>> = accounts.iter().map(|a| a.updated_at).collect();
        let result = sqlx::query!(
            r#"
            INSERT INTO account_projections (id, owner_name, balance, is_active, created_at, updated_at)
            SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::decimal[], $4::boolean[], $5::timestamptz[], $6::timestamptz[])
            ON CONFLICT (id) DO UPDATE SET
                owner_name = EXCLUDED.owner_name,
                balance = EXCLUDED.balance,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
            "#,
            &ids,
            &owner_names,
            &balances,
            &is_actives,
            &created_ats,
            &updated_ats
        )
        .execute(&mut **tx)
        .await;
        match result {
            Ok(res) => {
                tracing::info!(
                    "[ProjectionStore] bulk_upsert_accounts: SQL result: rows_affected={}",
                    res.rows_affected()
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    "[ProjectionStore] bulk_upsert_accounts: SQL error for ids={:?}: {}",
                    account_ids,
                    e
                );
                return Err(e.into());
            }
        }
    }

    async fn bulk_insert_transactions(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transactions: &[TransactionProjection],
    ) -> Result<()> {
        if transactions.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = transactions.iter().map(|t| t.id).collect();
        let account_ids: Vec<Uuid> = transactions.iter().map(|t| t.account_id).collect();
        let types: Vec<String> = transactions
            .iter()
            .map(|t| t.transaction_type.clone())
            .collect();
        let amounts: Vec<Decimal> = transactions.iter().map(|t| t.amount).collect();
        let timestamps: Vec<DateTime<Utc>> = transactions.iter().map(|t| t.timestamp).collect();

        sqlx::query!(
            r#"
            INSERT INTO transaction_projections (id, account_id, transaction_type, amount, timestamp)
            SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::decimal[], $5::timestamptz[])
            ON CONFLICT (id, timestamp) DO NOTHING
            "#,
            &ids,
            &account_ids,
            &types,
            &amounts,
            &timestamps
        )
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn cache_cleanup_worker(
        account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
        transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes

        loop {
            interval.tick().await;

            let cutoff = Instant::now() - Duration::from_secs(1800); // 30 minutes

            // Clean account cache
            {
                let mut cache = account_cache.write().await;
                cache.retain(|_, entry| entry.last_accessed > cutoff);
            }

            // Clean transaction cache
            {
                let mut cache = transaction_cache.write().await;
                cache.retain(|_, entry| entry.last_accessed > cutoff);
            }
        }
    }

    async fn metrics_reporter(metrics: Arc<ProjectionMetrics>) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let hits = metrics
                .cache_hits
                .load(std::sync::atomic::Ordering::Relaxed);
            let misses = metrics
                .cache_misses
                .load(std::sync::atomic::Ordering::Relaxed);
            let batches = metrics
                .batch_updates
                .load(std::sync::atomic::Ordering::Relaxed);
            let errors = metrics.errors.load(std::sync::atomic::Ordering::Relaxed);
            let avg_query_time = metrics
                .query_duration
                .load(std::sync::atomic::Ordering::Relaxed) as f64
                / 1000.0; // Convert to milliseconds

            let hit_rate = if hits + misses > 0 {
                (hits as f64 / (hits + misses) as f64) * 100.0
            } else {
                0.0
            };

            info!(
                "Projection Metrics - Cache Hit Rate: {:.2}%, Batch Updates: {}, Errors: {}, Avg Query Time: {:.2}ms",
                hit_rate, batches, errors, avg_query_time
            );
        }
    }
}

// Add Default implementation for ProjectionStore
impl Default for ProjectionStore {
    fn default() -> Self {
        panic!("ProjectionStore::default() is not supported. Please use ProjectionStore::new(pool) and pass a shared PgPool instance.");
    }
}

impl AccountProjection {
    pub fn apply_event(&self, event: &AccountEvent) -> Result<Self> {
        let mut projection = self.clone();
        match event {
            AccountEvent::AccountCreated {
                owner_name,
                initial_balance,
                ..
            } => {
                projection.owner_name = owner_name.clone();
                projection.balance = *initial_balance;
                projection.is_active = true;
            }
            AccountEvent::MoneyDeposited { amount, .. } => {
                projection.balance += *amount;
            }
            AccountEvent::MoneyWithdrawn { amount, .. } => {
                projection.balance -= *amount;
            }
            AccountEvent::AccountClosed { .. } => {
                projection.is_active = false;
            }
        }
        Ok(projection)
    }
}
