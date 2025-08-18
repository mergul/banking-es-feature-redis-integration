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
use sqlx::{Acquire, PgPool};
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
    pub synchronous_commit: bool,
    pub full_page_writes: bool,
}

impl Default for ProjectionConfig {
    fn default() -> Self {
        Self {
            cache_ttl_secs: 600,     // Increased from default
            batch_size: 2000,        // Increased to match CDC batch sizes (336, 542, etc.)
            batch_timeout_ms: 25,    // Fast timeout since messages come as batches
            max_connections: 400,    // Increased from default
            min_connections: 250,    // Increased from default
            acquire_timeout_secs: 5, // Reduced from default
            idle_timeout_secs: 300,  // Reduced from default
            max_lifetime_secs: 1800, // Reduced from default
            synchronous_commit: true,
            full_page_writes: true,
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
    async fn get_accounts_batch(&self, account_ids: &[Uuid]) -> Result<Vec<AccountProjection>>;
    async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>>;
    async fn upsert_accounts_batch(&self, accounts: Vec<AccountProjection>) -> Result<()>;
    async fn insert_transactions_batch(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()>;
    fn as_any(&self) -> &dyn std::any::Any;
}

#[async_trait]
impl ProjectionStoreTrait for ProjectionStore {
    async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>> {
        self.get_account(account_id).await
    }
    async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>> {
        self.get_all_accounts().await
    }

    async fn get_accounts_batch(&self, account_ids: &[Uuid]) -> Result<Vec<AccountProjection>> {
        self.get_accounts_batch(account_ids).await
    }

    async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>> {
        self.get_account_transactions(account_id).await
    }
    async fn upsert_accounts_batch(&self, accounts: Vec<AccountProjection>) -> Result<()> {
        tracing::info!(
            "ProjectionStore: upsert_accounts_batch called with {} accounts.",
            accounts.len(),
            // accounts.iter().map(|a| a.id).collect::<Vec<_>>()
        );
        // for acc in &accounts {
        //     tracing::info!(
        //         "[ProjectionStore] upsert_accounts_batch: account_id={}, owner_name={}",
        //         acc.id,
        //         acc.owner_name
        //     );
        // }
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ProjectionStore {
    pub async fn new(config: ProjectionConfig) -> Result<Self, sqlx::Error> {
        info!(
            "🔧 [ProjectionStore] Creating projection store with optimized connection pooling..."
        );

        // Use default database URL for now
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://localhost/test".to_string());

        // OPTIMIZATION: Enhanced connection pooling with better timeout handling
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            .test_before_acquire(true) // Test connections before use
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = 10000; SET lock_timeout = 100; SET idle_in_transaction_session_timeout = 30000;")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await?;

        // Create partitioned pools configuration
        let pool_config =
            crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig {
                database_url,
                write_pool_max_connections: config.max_connections / 3,
                write_pool_min_connections: config.min_connections / 3,
                read_pool_max_connections: config.max_connections * 2 / 3,
                read_pool_min_connections: config.min_connections * 2 / 3,
                acquire_timeout_secs: config.acquire_timeout_secs,
                write_idle_timeout_secs: config.idle_timeout_secs,
                read_idle_timeout_secs: config.idle_timeout_secs,
                write_max_lifetime_secs: config.max_lifetime_secs,
                read_max_lifetime_secs: config.max_lifetime_secs,
            };

        // OPTIMIZATION: Use partitioned pools for better performance
        let partitioned_pools = Arc::new(PartitionedPools::new(pool_config).await?);

        // Create update sender and receiver
        let (update_sender, update_receiver) = mpsc::unbounded_channel::<ProjectionUpdate>();

        // Create caches
        let account_cache = Arc::new(RwLock::new(HashMap::new()));
        let transaction_cache = Arc::new(RwLock::new(HashMap::new()));
        let cache_version = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let metrics = Arc::new(ProjectionMetrics::default());

        // Start background tasks
        let pools_clone = partitioned_pools.clone();
        let account_cache_clone = account_cache.clone();
        let transaction_cache_clone = transaction_cache.clone();
        let cache_version_clone = cache_version.clone();
        let metrics_clone = metrics.clone();
        let config_clone = config.clone();

        tokio::spawn(async move {
            Self::update_processor(
                pools_clone,
                update_receiver,
                account_cache_clone,
                transaction_cache_clone,
                cache_version_clone,
                metrics_clone,
                config_clone,
            )
            .await;
        });

        // Start cache cleanup worker
        let account_cache_cleanup = account_cache.clone();
        let transaction_cache_cleanup = transaction_cache.clone();
        tokio::spawn(async move {
            Self::cache_cleanup_worker(account_cache_cleanup, transaction_cache_cleanup).await;
        });

        // Start metrics reporter
        let metrics_reporter = metrics.clone();
        tokio::spawn(async move {
            Self::metrics_reporter(metrics_reporter).await;
        });

        Ok(Self {
            pools: partitioned_pools,
            account_cache,
            transaction_cache,
            update_sender,
            cache_version,
            metrics,
            config,
        })
    }

    // OPTIMIZATION: Enhanced connection acquisition with timeout and retry
    async fn get_connection_with_timeout(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, sqlx::Error> {
        let connection_timeout = Duration::from_secs(5);
        let max_retries = 3;
        let retry_delay = Duration::from_millis(100);

        for attempt in 1..=max_retries {
            match tokio::time::timeout(connection_timeout, self.pools.write_pool.acquire()).await {
                Ok(Ok(conn)) => {
                    if attempt > 1 {
                        info!(
                            "🔧 [ProjectionStore] Connection acquired after {} attempts",
                            attempt
                        );
                    }
                    return Ok(conn);
                }
                Ok(Err(e)) => {
                    warn!(
                        "🔧 [ProjectionStore] Connection acquisition failed (attempt {}/{}): {}",
                        attempt, max_retries, e
                    );
                    if attempt < max_retries {
                        tokio::time::sleep(retry_delay).await;
                    } else {
                        return Err(e);
                    }
                }
                Err(_) => {
                    warn!(
                        "🔧 [ProjectionStore] Connection acquisition timeout (attempt {}/{})",
                        attempt, max_retries
                    );
                    if attempt < max_retries {
                        tokio::time::sleep(retry_delay).await;
                    } else {
                        return Err(sqlx::Error::Configuration(
                            "Connection acquisition timeout".into(),
                        ));
                    }
                }
            }
        }

        Err(sqlx::Error::Configuration(
            "Failed to acquire connection after all retries".into(),
        ))
    }

    // OPTIMIZATION: Batch upsert with connection pooling
    async fn upsert_accounts_batch_with_pooling(
        &self,
        accounts: Vec<AccountProjection>,
    ) -> Result<HashMap<Uuid, bool>> {
        if accounts.is_empty() {
            return Ok(HashMap::new());
        }

        let mut results = HashMap::new();
        let batch_size = 100; // Process in smaller batches for better connection utilization

        for chunk in accounts.chunks(batch_size) {
            let mut conn = self.get_connection_with_timeout().await?;

            // OPTIMIZATION: Use transaction with timeout
            let mut tx = conn.begin().await?;

            // Set transaction timeout
            sqlx::query("SET LOCAL statement_timeout = 15000") // 15 second timeout
                .execute(&mut *tx)
                .await?;

            let mut chunk_results = HashMap::new();

            for account in chunk {
                let result = sqlx::query!(
                    r#"
                    INSERT INTO account_projections (id, owner_name, balance, is_active, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (id) DO UPDATE SET
                        owner_name = EXCLUDED.owner_name,
                        balance = EXCLUDED.balance,
                        is_active = EXCLUDED.is_active,
                        updated_at = EXCLUDED.updated_at
                    "#,
                    account.id,
                    account.owner_name,
                    account.balance,
                    account.is_active,
                    account.created_at,
                    account.updated_at
                )
                .execute(&mut *tx)
                .await;

                chunk_results.insert(account.id, result.is_ok());
            }

            // Commit transaction with timeout
            match tokio::time::timeout(Duration::from_secs(10), tx.commit()).await {
                Ok(Ok(_)) => {
                    results.extend(chunk_results);
                }
                Ok(Err(e)) => {
                    error!("🔧 [ProjectionStore] Transaction commit failed: {}", e);
                    // Mark all accounts in this chunk as failed
                    for account in chunk {
                        results.insert(account.id, false);
                    }
                }
                Err(_) => {
                    error!("🔧 [ProjectionStore] Transaction commit timeout");
                    // Mark all accounts in this chunk as failed
                    for account in chunk {
                        results.insert(account.id, false);
                    }
                }
            }
        }

        Ok(results)
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
        .fetch_optional(&self.pools.read_pool) // Use read pool instead of write pool
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

    /// True batch query: Get multiple accounts in a single SQL query
    pub async fn get_accounts_batch(&self, account_ids: &[Uuid]) -> Result<Vec<AccountProjection>> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let start_time = Instant::now();

        // Try cache first for all accounts
        let mut cached_accounts = Vec::new();
        let mut uncached_ids = Vec::new();

        {
            let cache = self.account_cache.read().await;
            for &account_id in account_ids {
                if let Some(entry) = cache.get(&account_id) {
                    if entry.last_accessed.elapsed() < entry.ttl {
                        cached_accounts.push(entry.data.clone());
                    } else {
                        uncached_ids.push(account_id);
                    }
                } else {
                    uncached_ids.push(account_id);
                }
            }
        }

        // If all accounts were cached, return immediately
        if uncached_ids.is_empty() {
            self.metrics.cache_hits.fetch_add(
                account_ids.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            return Ok(cached_accounts);
        }

        self.metrics.cache_misses.fetch_add(
            uncached_ids.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Fetch uncached accounts in a single batch query
        let db_accounts = sqlx::query_as!(
            AccountProjection,
            r#"
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM account_projections
            WHERE id = ANY($1)
            ORDER BY updated_at DESC
            "#,
            &uncached_ids
        )
        .fetch_all(self.pools.select_pool(OperationType::Read))
        .await?;

        // Update cache for fetched accounts
        {
            let mut cache = self.account_cache.write().await;
            for account in &db_accounts {
                cache.insert(
                    account.id,
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
        }

        // Combine cached and database results
        cached_accounts.extend(db_accounts);

        self.metrics.query_duration.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(cached_accounts)
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
                            // CRITICAL OPTIMIZATION: Process immediately if batch is large enough
                            if account_batch.len() >= config.batch_size { // Use config.batch_size instead of hardcoded 500
                                tracing::info!("ProjectionStore: update_processor flushing account batch of size {}.", account_batch.len());
                                let batch_to_process = std::mem::take(&mut account_batch);
                                let pools = pools.clone();
                                let metrics = metrics.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = Self::upsert_accounts_batch_parallel(&pools, &metrics, batch_to_process).await {
                                        error!("[ProjectionStore] Error upserting account batch: {}", e);
                                    }
                                });
                            }
                        }
                        ProjectionUpdate::TransactionBatch(transactions) => {
                            transaction_batch.extend(transactions);
                            // CRITICAL OPTIMIZATION: Process immediately if batch is large enough
                            if transaction_batch.len() >= config.batch_size { // Use config.batch_size instead of hardcoded 500
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
                _ = interval.tick() => {
                    if !account_batch.is_empty() {
                        tracing::info!("ProjectionStore: update_processor flushing account batch of size {}.", account_batch.len());
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
            "ProjectionStore: upsert_accounts_batch_parallel flushing {} accounts to DB.",
            account_count,
            // account_ids
        );

        // Use bulk upsert instead of individual transactions for better performance
        let pool = pools.select_pool(OperationType::Write).clone();
        let mut tx = pool.begin().await?;

        // Process all accounts in a single bulk operation
        Self::bulk_upsert_accounts(&mut tx, &accounts).await?;

        tx.commit().await?;

        // Increment batch_updates metric
        metrics
            .batch_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            "ProjectionStore: upsert_accounts_batch_parallel DB upsert completed successfully for {} accounts.",
            account_count,
           // account_ids
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

        // Process all transactions in a single bulk operation
        Self::bulk_insert_transactions_direct_copy(&mut tx, &transactions)
            .await
            .map_err(|e| anyhow::anyhow!("Error inserting transactions with COPY: {}", e));

        tx.commit().await?;
        Ok(())
    }

    async fn bulk_upsert_accounts(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        accounts: &[AccountProjection],
    ) -> Result<()> {
        let account_ids: Vec<_> = accounts.iter().map(|a| a.id).collect();
        // tracing::info!(
        //     "[ProjectionStore] bulk_upsert_accounts: about to upsert accounts: ids={:?}",
        //     account_ids
        // );

        // Only deduplicate if we have more than 1 account to avoid unnecessary overhead
        let accounts_to_upsert = if accounts.len() > 1 {
            let mut unique_accounts = Vec::new();
            let mut seen_ids = std::collections::HashSet::new();
            let mut duplicate_count = 0;

            for account in accounts {
                if seen_ids.insert(account.id) {
                    unique_accounts.push(account.clone());
                } else {
                    duplicate_count += 1;
                }
            }

            // Only log if we actually found duplicates
            if duplicate_count > 0 {
                tracing::warn!(
                    "[ProjectionStore] bulk_upsert_accounts: Found {} duplicates in batch of {} accounts",
                    duplicate_count,
                    accounts.len()
                );
            }

            unique_accounts
        } else {
            accounts.to_vec()
        };

        if accounts_to_upsert.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = accounts_to_upsert.iter().map(|a| a.id).collect();
        let owner_names: Vec<String> = accounts_to_upsert
            .iter()
            .map(|a| a.owner_name.clone())
            .collect();
        let balances: Vec<Decimal> = accounts_to_upsert.iter().map(|a| a.balance).collect();
        let is_actives: Vec<bool> = accounts_to_upsert.iter().map(|a| a.is_active).collect();
        let created_ats: Vec<DateTime<Utc>> =
            accounts_to_upsert.iter().map(|a| a.created_at).collect();
        let updated_ats: Vec<DateTime<Utc>> =
            accounts_to_upsert.iter().map(|a| a.updated_at).collect();

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

        let result = sqlx::query!(
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
        .await;

        match result {
            Ok(res) => {
                tracing::info!(
                "[ProjectionStore] bulk_insert_transactions: Successfully processed {} transactions, rows_affected={}",
                transactions.len(),
                res.rows_affected()
            );
                if res.rows_affected() < transactions.len() as u64 {
                    tracing::warn!(
                    "[ProjectionStore] bulk_insert_transactions: {} transactions were skipped due to conflicts",
                    transactions.len() as u64 - res.rows_affected()
                );
                }
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                "[ProjectionStore] bulk_insert_transactions: Failed to insert {} transactions: {}",
                transactions.len(),
                e
            );

                // Log additional error details
                if let sqlx::Error::Database(db_err) = &e {
                    tracing::error!(
                    "[ProjectionStore] Database error - code: {:?}, message: {}, constraint: {:?}",
                    db_err.code(),
                    db_err.message(),
                    db_err.constraint()
                );
                }

                Err(e.into())
            }
        }
    }

    /// Direct COPY bulk insert transactions - assumes records are unique (no conflicts)
    /// This is faster than temp table approach but will fail if conflicts occur
    async fn bulk_insert_transactions_direct_copy(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transactions: &[TransactionProjection],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        if transactions.is_empty() {
            return Ok(());
        }

        tracing::info!(
            "[ProjectionStore] bulk_insert_transactions_direct_copy: processing {} transactions (assuming unique)",
            transactions.len()
        );

        // CRITICAL: Use fixed binary writer
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new();
        for transaction in transactions {
            writer.write_row(5)?; // 5 fields
            writer.write_uuid(&transaction.id)?;
            writer.write_uuid(&transaction.account_id)?;
            writer.write_text(&transaction.transaction_type)?;
            writer.write_decimal(&transaction.amount, "amount")?;
            writer.write_timestamp(&transaction.timestamp)?;
        }
        let binary_data = writer.finish()?;

        tracing::debug!(
            "[ProjectionStore] Generated binary data: {} bytes for {} tuples",
            binary_data.len(),
            transactions.len()
        );

        // Execute direct COPY to target table
        let copy_command = "COPY transaction_projections FROM STDIN WITH (FORMAT BINARY)";
        let mut copy_in = tx.copy_in_raw(copy_command).await?;
        copy_in.send(binary_data.as_slice()).await?;
        let copy_result = copy_in.finish().await?;

        tracing::info!(
            "[ProjectionStore] Direct COPY successfully inserted {} transactions",
            copy_result
        );

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

    /// Get the current configuration
    pub fn get_config(&self) -> ProjectionConfig {
        self.config.clone()
    }

    /// Update the configuration (for bulk operations)
    pub fn update_config(&mut self, new_config: ProjectionConfig) {
        self.config = new_config;
    }

    /// Temporarily apply bulk configuration and return original config
    pub fn apply_bulk_config(&mut self) -> ProjectionConfig {
        let original_config = self.config.clone();

        // Apply bulk optimizations
        self.config.cache_ttl_secs = 0; // Disable cache for bulk operations
        self.config.batch_size = 2000; // Normal: 500
        self.config.batch_timeout_ms = 25; // Normal: 100

        // Connection pool optimizations
        self.config.max_connections = 150; // Normal: 50
        self.config.min_connections = 30; // Normal: 5
        self.config.acquire_timeout_secs = 30; // Normal: 10
        self.config.idle_timeout_secs = 300; // Normal: 60
        self.config.max_lifetime_secs = 1800; // Normal: 600

        // Set PostgreSQL synchronous_commit = off for bulk operations
        self.config.synchronous_commit = false;
        // Set PostgreSQL full_page_writes = off for bulk operations
        self.config.full_page_writes = false;

        original_config
    }

    /// Restore original configuration
    pub fn restore_config(&mut self, original_config: ProjectionConfig) {
        self.config = original_config;
    }

    /// Apply PostgreSQL settings for bulk operations
    pub async fn apply_postgres_bulk_settings(
        &self,
        synchronous_commit: bool,
        _full_page_writes: bool, // Ignored - cannot be changed at runtime
    ) -> Result<()> {
        let pool = self.pools.select_pool(OperationType::Write);

        // Set synchronous_commit (only runtime-changeable parameter)
        let sync_setting = if synchronous_commit { "on" } else { "off" };
        sqlx::query(&format!("SET synchronous_commit = {}", sync_setting))
            .execute(pool)
            .await
            .map_err(|e| ProjectionError::DatabaseError(e))?;

        info!(
            "🔧 ProjectionStore PostgreSQL setting: synchronous_commit={} (full_page_writes requires server restart)",
            sync_setting
        );
        Ok(())
    }

    /// Apply bulk config with PostgreSQL settings
    pub async fn apply_bulk_config_with_postgres(&mut self) -> ProjectionConfig {
        let original_config = self.apply_bulk_config();

        // Apply PostgreSQL settings for bulk operations
        if let Err(e) = self.apply_postgres_bulk_settings(false, false).await {
            warn!(
                "Failed to set ProjectionStore PostgreSQL bulk settings: {}",
                e
            );
        }

        original_config
    }

    /// Restore config with PostgreSQL settings
    pub async fn restore_config_with_postgres(
        &mut self,
        original_config: ProjectionConfig,
    ) -> Result<()> {
        self.restore_config(original_config.clone());

        // Restore PostgreSQL settings
        if let Err(e) = self
            .apply_postgres_bulk_settings(
                original_config.synchronous_commit,
                original_config.full_page_writes,
            )
            .await
        {
            warn!(
                "Failed to restore ProjectionStore PostgreSQL settings: {}",
                e
            );
        }

        Ok(())
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
