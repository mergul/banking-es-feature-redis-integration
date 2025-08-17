use crate::domain::events::AccountEvent;
use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use crate::infrastructure::event_store::DB_POOL;
use crate::infrastructure::DiagnosticPgCopyBinaryWriter;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Acquire, PgPool};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, OnceCell, RwLock};
use tokio_util::bytes::BytesMut;
use tokio_util::sync::CancellationToken;
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

// 5. Enhanced ProjectionMetrics with COPY optimization tracking
#[derive(Debug, Default)]
pub struct ProjectionMetrics {
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
    batch_updates: std::sync::atomic::AtomicU64,
    events_processed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    query_duration: std::sync::atomic::AtomicU64,
    // COPY optimization metrics
    pub copy_metrics: CopyOptimizationMetrics,
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
    pub metrics: Arc<ProjectionMetrics>,
    config: ProjectionConfig,
    shutdown_token: CancellationToken,
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

// 2. Enhanced ProjectionConfig for COPY optimization
impl Default for ProjectionConfig {
    fn default() -> Self {
        let cdc_optimized_batch_size = std::env::var("CDC_PROJECTION_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10000);
        Self {
            cache_ttl_secs: 600,
            batch_size: cdc_optimized_batch_size,
            batch_timeout_ms: 25,
            max_connections: 400,
            min_connections: 250,
            acquire_timeout_secs: 5,
            idle_timeout_secs: 300,
            max_lifetime_secs: 1800,
            synchronous_commit: false,
            full_page_writes: false,
        }
    }
}

// 3. Environment variable configuration for COPY thresholds
#[derive(Debug, Clone)]
pub struct CopyOptimizationConfig {
    pub projection_copy_threshold: usize,
    pub transaction_copy_threshold: usize,
    pub cdc_projection_batch_size: usize,
    pub enable_copy_optimization: bool,
}

impl CopyOptimizationConfig {
    pub fn from_env() -> Self {
        Self {
            projection_copy_threshold: std::env::var("PROJECTION_COPY_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000), // Use COPY for batches >= 1000 accounts (optimized for large batches)
            transaction_copy_threshold: std::env::var("TRANSACTION_COPY_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000), // Use COPY for batches >= 1000 transactions (optimized for large batches)
            cdc_projection_batch_size: std::env::var("CDC_PROJECTION_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000), // Optimized CDC batch size for better throughput
            enable_copy_optimization: std::env::var("ENABLE_COPY_OPTIMIZATION")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(true), // Enable by default
        }
    }
}

// 4. Performance metrics for COPY operations
#[derive(Debug, Default)]
pub struct CopyOptimizationMetrics {
    copy_operations_total: std::sync::atomic::AtomicU64,
    copy_operations_successful: std::sync::atomic::AtomicU64,
    copy_operations_failed: std::sync::atomic::AtomicU64,
    pub copy_fallback_to_unnest: std::sync::atomic::AtomicU64,
    copy_duration_total_ms: std::sync::atomic::AtomicU64,
    unnest_operations_total: std::sync::atomic::AtomicU64,
    unnest_duration_total_ms: std::sync::atomic::AtomicU64,
}

impl CopyOptimizationMetrics {
    pub fn record_copy_success(&self, duration: Duration) {
        self.copy_operations_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.copy_operations_successful
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.copy_duration_total_ms.fetch_add(
            duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn record_copy_failure(&self) {
        self.copy_operations_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.copy_operations_failed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.copy_fallback_to_unnest
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_unnest_operation(&self, duration: Duration) {
        self.unnest_operations_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.unnest_duration_total_ms.fetch_add(
            duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn get_copy_success_rate(&self) -> f64 {
        let total = self
            .copy_operations_total
            .load(std::sync::atomic::Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let successful = self
            .copy_operations_successful
            .load(std::sync::atomic::Ordering::Relaxed);
        (successful as f64 / total as f64) * 100.0
    }

    pub fn get_average_copy_duration(&self) -> f64 {
        let total = self
            .copy_operations_successful
            .load(std::sync::atomic::Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let duration = self
            .copy_duration_total_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        duration as f64 / total as f64
    }

    pub fn get_average_unnest_duration(&self) -> f64 {
        let total = self
            .unnest_operations_total
            .load(std::sync::atomic::Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let duration = self
            .unnest_duration_total_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        duration as f64 / total as f64
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
    async fn shutdown(&self) -> Result<()>;
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

    async fn shutdown(&self) -> Result<()> {
        self.shutdown().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ProjectionStore {
    pub async fn new(config: ProjectionConfig) -> Result<Self, sqlx::Error> {
        info!(
            "🔧 [ProjectionStore] Creating projection store with COPY-optimized connection pooling..."
        );

        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://localhost/test".to_string());

        // Enhanced connection pooling for COPY operations
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            .test_before_acquire(true)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // CRITICAL: Optimize for bulk operations
                    sqlx::query(
                        r#"
                        SET statement_timeout = 10000;
                        SET lock_timeout = 3000;
                        SET idle_in_transaction_session_timeout = 10000;
                    "#,
                    )
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await?;

        let pool_config =
            crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig {
                database_url,
                write_pool_max_connections: config.max_connections / 2, // Increased write pool ratio for COPY
                write_pool_min_connections: config.min_connections / 2,
                read_pool_max_connections: config.max_connections / 2,
                read_pool_min_connections: config.min_connections / 2,
                acquire_timeout_secs: config.acquire_timeout_secs,
                write_idle_timeout_secs: config.idle_timeout_secs,
                read_idle_timeout_secs: config.idle_timeout_secs,
                write_max_lifetime_secs: config.max_lifetime_secs,
                read_max_lifetime_secs: config.max_lifetime_secs,
            };

        let partitioned_pools = Arc::new(PartitionedPools::new(pool_config).await?);
        let (update_sender, update_receiver) = mpsc::unbounded_channel::<ProjectionUpdate>();
        let account_cache = Arc::new(RwLock::new(HashMap::new()));
        let transaction_cache = Arc::new(RwLock::new(HashMap::new()));
        let cache_version = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let metrics = Arc::new(ProjectionMetrics::default());
        let shutdown_token = CancellationToken::new();

        // Use optimized processor with COPY support
        let pools_clone = partitioned_pools.clone();
        let account_cache_clone = account_cache.clone();
        let transaction_cache_clone = transaction_cache.clone();
        let cache_version_clone = cache_version.clone();
        let metrics_clone = metrics.clone();
        let config_clone = config.clone();
        let shutdown_token_processor = shutdown_token.clone();

        tokio::spawn(async move {
            Self::update_processor_optimized(
                pools_clone,
                update_receiver,
                account_cache_clone,
                transaction_cache_clone,
                cache_version_clone,
                metrics_clone,
                config_clone,
                shutdown_token_processor,
            )
            .await;
        });

        // Start cache cleanup and metrics workers...
        let account_cache_cleanup = account_cache.clone();
        let transaction_cache_cleanup = transaction_cache.clone();
        let shutdown_token_cleanup = shutdown_token.clone();
        tokio::spawn(async move {
            Self::cache_cleanup_worker(
                account_cache_cleanup,
                transaction_cache_cleanup,
                shutdown_token_cleanup,
            )
            .await;
        });

        let metrics_reporter = metrics.clone();
        let shutdown_token_metrics = shutdown_token.clone();
        tokio::spawn(async move {
            Self::metrics_reporter(metrics_reporter, shutdown_token_metrics).await;
        });

        Ok(Self {
            pools: partitioned_pools,
            account_cache,
            transaction_cache,
            update_sender,
            cache_version,
            metrics,
            config,
            shutdown_token,
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

    // DIAGNOSTIC BULK INSERT WITH EXTENSIVE LOGGING
    async fn bulk_upsert_accounts_with_copy_diagnostic(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        accounts: &[AccountProjection],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if accounts.is_empty() {
            return Ok(());
        }

        tracing::info!(
            "🚀 DIAGNOSTIC: Starting bulk upsert for {} accounts",
            accounts.len()
        );

        // Log first account for inspection
        if let Some(first_account) = accounts.first() {
            tracing::info!("🔍 DIAGNOSTIC: First account sample:");
            tracing::info!("  - ID: {}", first_account.id);
            tracing::info!("  - Owner: '{}'", first_account.owner_name);
            tracing::info!("  - Balance: {}", first_account.balance);
            tracing::info!("  - Active: {}", first_account.is_active);
            tracing::info!("  - Created: {}", first_account.created_at);
            tracing::info!("  - Updated: {}", first_account.updated_at);
        }

        // Create temporary table
        tracing::info!("🔍 DIAGNOSTIC: Creating temporary table");
        sqlx::query!(
            r#"
        CREATE TEMP TABLE temp_account_projections (
            id UUID,
            owner_name TEXT,
            balance DECIMAL,
            is_active BOOLEAN,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ
        ) ON COMMIT DROP
        "#
        )
        .execute(&mut **tx)
        .await?;

        // TEST WITH JUST ONE RECORD FIRST
        let test_accounts = &accounts[..std::cmp::min(1, accounts.len())];
        tracing::warn!(
            "🔍 DIAGNOSTIC: Testing with only {} record(s) first",
            test_accounts.len()
        );

        // Create binary data with extensive logging
        tracing::info!("🔍 DIAGNOSTIC: Creating binary data");
        let mut writer = DiagnosticPgCopyBinaryWriter::new(true)?;

        for (i, account) in test_accounts.iter().enumerate() {
            tracing::info!(
                "🔍 DIAGNOSTIC: Processing account {} of {}",
                i + 1,
                test_accounts.len()
            );

            writer.write_row(6)?; // 6 fields
            writer.write_uuid(&account.id, "id")?;
            writer.write_text(&account.owner_name, "owner_name")?;
            writer.write_decimal(&account.balance, "balance")?;
            writer.write_bool(account.is_active, "is_active")?;
            writer.write_timestamp(&account.created_at, "created_at")?;
            writer.write_timestamp(&account.updated_at, "updated_at")?;
        }

        let binary_data = writer.finish()?;

        tracing::info!(
            "🔍 DIAGNOSTIC: Generated {} bytes of binary data",
            binary_data.len()
        );
        let first_account = &test_accounts[0];
        // Expected size calculation for verification
        let expected_size_per_tuple = 2 +  // tuple header (field count)
        20 + // UUID: 4-byte length + 16-byte data
        4 + first_account.owner_name.len() + // TEXT: 4-byte length + data
        4 + first_account.balance.to_string().len() + // DECIMAL as TEXT: 4-byte length + data  
        5 +  // BOOLEAN: 4-byte length + 1-byte data
        12 + // TIMESTAMP: 4-byte length + 8-byte data
        12; // TIMESTAMP: 4-byte length + 8-byte data

        let expected_total_size = 19 + // header: 11-byte signature + 4-byte flags + 4-byte extension
        expected_size_per_tuple * test_accounts.len() +
        2; // trailer: 2-byte -1

        tracing::info!(
            "🔍 DIAGNOSTIC: Expected size: ~{} bytes, actual: {} bytes",
            expected_total_size,
            binary_data.len()
        );

        // Execute COPY with detailed error handling
        tracing::info!("🔍 DIAGNOSTIC: Executing COPY command");
        let copy_command = "COPY temp_account_projections FROM STDIN WITH (FORMAT BINARY)";

        match tx.copy_in_raw(copy_command).await {
            Ok(mut copy_in) => {
                tracing::info!("🔍 DIAGNOSTIC: COPY command accepted, sending data");

                match copy_in.send(binary_data.as_slice()).await {
                    Ok(_) => {
                        tracing::info!("🔍 DIAGNOSTIC: Data sent successfully, finishing");

                        match copy_in.finish().await {
                            Ok(rows_affected) => {
                                tracing::info!(
                                    "✅ DIAGNOSTIC: COPY succeeded! {} rows inserted",
                                    rows_affected
                                );
                            }
                            Err(e) => {
                                tracing::error!("❌ DIAGNOSTIC: COPY finish failed: {:?}", e);
                                tracing::error!("❌ DIAGNOSTIC: Error details: {}", e);

                                // Log specific error information
                                if let Some(db_error) = e.as_database_error() {
                                    if let Some(code) = db_error.code() {
                                        tracing::error!(
                                            "❌ DIAGNOSTIC: Database error code: {}",
                                            code
                                        );
                                    }
                                    tracing::error!(
                                        "❌ DIAGNOSTIC: Database error message: {}",
                                        db_error.message()
                                    );
                                    // Note: detail() method may not be available depending on sqlx version
                                    tracing::error!("❌ DIAGNOSTIC: Full error: {:?}", db_error);
                                }

                                return Err(e.into());
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("❌ DIAGNOSTIC: Failed to send data: {:?}", e);
                        return Err(e.into());
                    }
                }
            }
            Err(e) => {
                tracing::error!("❌ DIAGNOSTIC: Failed to start COPY: {:?}", e);
                return Err(e.into());
            }
        }

        tracing::info!("🎯 DIAGNOSTIC: Test completed successfully!");
        Ok(())
    }

    // UPDATED BULK INSERT FUNCTIONS WITH FIXED BINARY WRITER

    /// Fixed bulk upsert accounts with proper binary format and unique table names
    async fn bulk_upsert_accounts_with_copy(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        accounts: &[AccountProjection],
        pool: &PgPool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        if accounts.is_empty() {
            return Ok(());
        }

        let account_ids: Vec<_> = accounts.iter().map(|a| a.id).collect();
        tracing::info!(
            "[ProjectionStore] bulk_upsert_accounts_with_copy_fixed: processing {}",
            accounts.len()
        );

        // Deduplicate accounts by ID
        let accounts_to_upsert = if accounts.len() > 1 {
            let mut unique_accounts = std::collections::HashMap::new();
            for account in accounts {
                unique_accounts.insert(account.id, account.clone());
            }
            unique_accounts.into_values().collect::<Vec<_>>()
        } else {
            accounts.to_vec()
        };

        // CRITICAL FIX: Use unique table name to avoid conflicts
        let table_name = format!("temp_account_projections_{}", uuid::Uuid::new_v4().simple());

        // Create TEMPORARY table inside transaction for proper isolation
        let create_table_sql = format!(
            r#"
        CREATE TEMPORARY TABLE IF NOT EXISTS {} (
            id UUID,
            owner_name TEXT,
            balance DECIMAL,
            is_active BOOLEAN,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            PRIMARY KEY (id)
        ) ON COMMIT DROP
        "#,
            table_name
        );

        sqlx::query(&create_table_sql).execute(&mut **tx).await?;

        // CRITICAL: Use fixed binary writer
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new()?;
        for account in &accounts_to_upsert {
            writer.write_row(6)?; // 6 fields
            writer.write_uuid(&account.id)?;
            writer.write_text(&account.owner_name)?;
            writer.write_decimal(&account.balance, "balance")?; // FIXED: now supported
            writer.write_bool(account.is_active)?;
            writer.write_timestamp(&account.created_at)?;
            writer.write_timestamp(&account.updated_at)?;
        }
        let binary_data = writer.finish()?;

        tracing::debug!(
            "[ProjectionStore] Generated binary data: {} bytes for {} tuples",
            binary_data.len(),
            accounts_to_upsert.len()
        );

        // Execute COPY with unique table name
        let copy_command = format!("COPY {} FROM STDIN WITH (FORMAT BINARY)", table_name);
        let mut copy_in = tx.copy_in_raw(&copy_command).await?;
        copy_in.send(binary_data.as_slice()).await?;
        let copy_result = copy_in.finish().await?;

        tracing::info!(
            "[ProjectionStore] COPY BINARY inserted {} rows into temp table {}",
            copy_result,
            table_name
        );

        // Upsert from temp table to main table
        let upsert_sql = format!(
            r#"
        INSERT INTO account_projections (id, owner_name, balance, is_active, created_at, updated_at)
        SELECT id, owner_name, balance, is_active, created_at, updated_at
        FROM {}
        ON CONFLICT (id) DO UPDATE SET
            owner_name = EXCLUDED.owner_name,
            balance = EXCLUDED.balance,
            is_active = EXCLUDED.is_active,
            updated_at = EXCLUDED.updated_at
        "#,
            table_name
        );

        let upsert_result = sqlx::query(&upsert_sql).execute(&mut **tx).await?;

        tracing::info!(
            "[ProjectionStore] Successfully upserted {} accounts via COPY BINARY",
            upsert_result.rows_affected()
        );

        // CRITICAL FIX: Clean up UNLOGGED table with timeout and error handling
        let drop_table_sql = format!("DROP TABLE IF EXISTS {}", table_name);

        // Use timeout to prevent hanging DROP TABLE
        match tokio::time::timeout(
            std::time::Duration::from_secs(5), // 5 second timeout
            sqlx::query(&drop_table_sql).execute(pool),
        )
        .await
        {
            Ok(result) => {
                match result {
                    Ok(_) => {
                        tracing::debug!(
                            "[ProjectionStore] Successfully dropped temp table {}",
                            table_name
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[ProjectionStore] Failed to drop temp table {}: {}",
                            table_name,
                            e
                        );
                        // Don't fail the entire operation for DROP TABLE failure
                    }
                }
            }
            Err(_) => {
                tracing::warn!(
                    "[ProjectionStore] DROP TABLE {} timed out after 5 seconds",
                    table_name
                );
                // Don't fail the entire operation for DROP TABLE timeout
            }
        }

        Ok(())
    }

    /// Fixed bulk insert transactions with proper binary format and unique table names
    async fn bulk_insert_transactions_with_copy(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transactions: &[TransactionProjection],
        pool: &PgPool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        if transactions.is_empty() {
            return Ok(());
        }

        tracing::info!(
        "[ProjectionStore] bulk_insert_transactions_with_copy_fixed: processing {} transactions",
        transactions.len()
    );

        // CRITICAL FIX: Use unique table name to avoid conflicts
        let table_name = format!(
            "temp_transaction_projections_{}",
            uuid::Uuid::new_v4().simple()
        );

        // Create UNLOGGED table outside transaction for better performance
        // Use a separate connection for table creation
        let create_table_sql = format!(
            r#"
        CREATE UNLOGGED TABLE IF NOT EXISTS {} (
            id UUID,
            account_id UUID,
            transaction_type TEXT,
            amount DECIMAL,
            timestamp TIMESTAMPTZ
        )
        "#,
            table_name
        );

        sqlx::query(&create_table_sql).execute(pool).await?;

        // CRITICAL: Use fixed binary writer
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new()?;
        for transaction in transactions {
            writer.write_row(5)?; // 5 fields
            writer.write_uuid(&transaction.id)?;
            writer.write_uuid(&transaction.account_id)?;
            writer.write_text(&transaction.transaction_type)?;
            writer.write_decimal(&transaction.amount, "amount")?; // FIXED: now supported
            writer.write_timestamp(&transaction.timestamp)?;
        }
        let binary_data = writer.finish()?;

        tracing::debug!(
            "[ProjectionStore] Generated binary data: {} bytes for {} tuples",
            binary_data.len(),
            transactions.len()
        );

        // Execute COPY with unique table name
        let copy_command = format!("COPY {} FROM STDIN WITH (FORMAT BINARY)", table_name);
        let mut copy_in = tx.copy_in_raw(&copy_command).await?;
        copy_in.send(binary_data.as_slice()).await?;
        let copy_result = copy_in.finish().await?;

        tracing::info!(
            "[ProjectionStore] COPY BINARY inserted {} rows into temp table {}",
            copy_result,
            table_name
        );

        // Insert from temp table to main table
        let insert_sql = format!(
            r#"
        INSERT INTO transaction_projections (id, account_id, transaction_type, amount, timestamp)
        SELECT id, account_id, transaction_type, amount, timestamp
        FROM {}
        ON CONFLICT (id, timestamp) DO NOTHING
        "#,
            table_name
        );

        let insert_result = sqlx::query(&insert_sql).execute(&mut **tx).await?;

        tracing::info!(
            "[ProjectionStore] Successfully inserted {} transactions via COPY BINARY",
            insert_result.rows_affected()
        );

        // CRITICAL FIX: Clean up UNLOGGED table with timeout and error handling
        let drop_table_sql = format!("DROP TABLE IF EXISTS {}", table_name);

        // Use timeout to prevent hanging DROP TABLE
        match tokio::time::timeout(
            std::time::Duration::from_secs(5), // 5 second timeout
            sqlx::query(&drop_table_sql).execute(pool),
        )
        .await
        {
            Ok(result) => {
                match result {
                    Ok(_) => {
                        tracing::debug!(
                            "[ProjectionStore] Successfully dropped temp table {}",
                            table_name
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[ProjectionStore] Failed to drop temp table {}: {}",
                            table_name,
                            e
                        );
                        // Don't fail the entire operation for DROP TABLE failure
                    }
                }
            }
            Err(_) => {
                tracing::warn!(
                    "[ProjectionStore] DROP TABLE {} timed out after 5 seconds",
                    table_name
                );
                // Don't fail the entire operation for DROP TABLE timeout
            }
        }

        // LOST EVENT LOGGING: rows_affected < transactions.len() implies conflicts or drops
        let inserted_rows: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM transaction_projections WHERE id = ANY($1)"#,
            &transactions.iter().map(|t| t.id).collect::<Vec<_>>()
        )
        .fetch_one(&mut **tx)
        .await
        .ok()
        .flatten()
        .unwrap_or(0_i64);

        let expected = transactions.len() as i64;
        if inserted_rows < expected {
            let lost = expected - inserted_rows;
            tracing::warn!(
                "[ProjectionStore] LOST {} transactions due to conflicts or drops (inserted: {}, expected: {})",
                lost,
                inserted_rows,
                expected
            );
        }

        Ok(())
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
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new()?;
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

    /// CRITICAL OPTIMIZATION: Use PostgreSQL COPY for bulk account upserts
    async fn bulk_upsert_accounts_with_copy_old(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        accounts: &[AccountProjection],
    ) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        let account_ids: Vec<_> = accounts.iter().map(|a| a.id).collect();
        tracing::info!(
            "[ProjectionStore] bulk_upsert_accounts_with_copy: about to COPY BINARY {} accounts: ids={:?}",
            accounts.len(),
            account_ids
        );

        // Deduplicate accounts by ID (keep the latest version)
        let accounts_to_upsert = if accounts.len() > 1 {
            let mut unique_accounts = std::collections::HashMap::new();
            for account in accounts {
                unique_accounts.insert(account.id, account.clone());
            }
            unique_accounts.into_values().collect::<Vec<_>>()
        } else {
            accounts.to_vec()
        };

        // STEP 1: Create temporary table for COPY
        sqlx::query(
            r#"
            CREATE TEMP TABLE temp_account_projections (
                id UUID,
                owner_name TEXT,
                balance DECIMAL,
                is_active BOOLEAN,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            ) ON COMMIT DROP
            "#,
        )
        .execute(&mut **tx)
        .await?;

        // STEP 2: Prepare BINARY data for COPY
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new()?;
        for account in &accounts_to_upsert {
            writer.write_row(6)?;
            writer.write_uuid(&account.id)?;
            writer.write_text(&account.owner_name)?;
            // Write decimal as text to avoid complex binary format
            writer.write_text(&account.balance.to_string())?;
            writer.write_bool(account.is_active)?;
            writer.write_timestamp(&account.created_at)?;
            writer.write_timestamp(&account.updated_at)?;
        }
        let binary_data = writer.finish()?;

        // STEP 3: Use COPY to bulk insert into temp table
        let mut copy_in = tx
            .copy_in_raw("COPY temp_account_projections FROM STDIN WITH (FORMAT BINARY)")
            .await?;
        copy_in.send(binary_data.as_slice()).await?;
        let copy_result = copy_in.finish().await;

        match copy_result {
            Ok(result) => {
                tracing::info!(
                    "[ProjectionStore] bulk_upsert_accounts_with_copy: COPY BINARY inserted {} rows into temp table",
                    result
                );
            }
            Err(e) => {
                tracing::error!(
                    "[ProjectionStore] bulk_upsert_accounts_with_copy: COPY BINARY failed: {}",
                    e
                );

                // Fallback to UNNEST method
                tracing::warn!("[ProjectionStore] Falling back to UNNEST method");
                return Self::bulk_upsert_accounts_unnest(tx, &accounts_to_upsert).await;
            }
        }

        // STEP 4: Merge from temp table to main table
        let upsert_result = sqlx::query(
            r#"
            INSERT INTO account_projections (id, owner_name, balance, is_active, created_at, updated_at)
            SELECT id, owner_name, balance, is_active, created_at, updated_at
            FROM temp_account_projections
            ON CONFLICT (id) DO UPDATE SET
                owner_name = EXCLUDED.owner_name,
                balance = EXCLUDED.balance,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
            "#
        )
        .execute(&mut **tx)
        .await;

        match upsert_result {
            Ok(res) => {
                tracing::info!(
                    "[ProjectionStore] bulk_upsert_accounts_with_copy: Successfully upserted {} accounts via COPY BINARY method",
                    res.rows_affected()
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    "[ProjectionStore] bulk_upsert_accounts_with_copy: Upsert failed for ids={:?}: {}",
                    account_ids,
                    e
                );
                Err(e.into())
            }
        }
    }

    /// CRITICAL OPTIMIZATION: Use PostgreSQL COPY for bulk transaction inserts
    async fn bulk_insert_transactions_with_copy_old(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transactions: &[TransactionProjection],
    ) -> Result<()> {
        if transactions.is_empty() {
            return Ok(());
        }

        tracing::info!(
            "[ProjectionStore] bulk_insert_transactions_with_copy: about to COPY BINARY {} transactions",
            transactions.len()
        );

        // STEP 1: Create temporary table for COPY
        sqlx::query(
            r#"
            CREATE TEMP TABLE temp_transaction_projections (
                id UUID,
                account_id UUID,
                transaction_type TEXT,
                amount DECIMAL,
                timestamp TIMESTAMPTZ
            ) ON COMMIT DROP
            "#,
        )
        .execute(&mut **tx)
        .await?;

        // STEP 2: Prepare BINARY data for COPY
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new()?;
        for transaction in transactions {
            writer.write_row(5)?;
            writer.write_uuid(&transaction.id)?;
            writer.write_uuid(&transaction.account_id)?;
            writer.write_text(&transaction.transaction_type)?;
            // Write decimal as text
            writer.write_text(&transaction.amount.to_string())?;
            writer.write_timestamp(&transaction.timestamp)?;
        }
        let binary_data = writer.finish()?;

        // STEP 3: Use COPY to bulk insert into temp table
        let mut copy_in = tx
            .copy_in_raw("COPY temp_transaction_projections FROM STDIN WITH (FORMAT BINARY)")
            .await?;
        copy_in.send(binary_data.as_slice()).await?;
        let copy_result = copy_in.finish().await;

        match copy_result {
            Ok(result) => {
                tracing::info!(
                    "[ProjectionStore] bulk_insert_transactions_with_copy: COPY BINARY inserted {} rows into temp table",
                    result
                );
            }
            Err(e) => {
                tracing::error!(
                    "[ProjectionStore] bulk_insert_transactions_with_copy: COPY BINARY failed: {}",
                    e
                );

                // Fallback to UNNEST method
                tracing::warn!("[ProjectionStore] Falling back to UNNEST method for transactions");
                return Self::bulk_insert_transactions_unnest(tx, transactions).await;
            }
        }

        // STEP 4: Insert from temp table to main table (with conflict handling)
        let insert_result = sqlx::query(
            r#"
            INSERT INTO transaction_projections (id, account_id, transaction_type, amount, timestamp)
            SELECT id, account_id, transaction_type, amount, timestamp
            FROM temp_transaction_projections
            ON CONFLICT (id, timestamp) DO NOTHING
            "#
        )
        .execute(&mut **tx)
        .await;

        match insert_result {
            Ok(res) => {
                tracing::info!(
                    "[ProjectionStore] bulk_insert_transactions_with_copy: Successfully inserted {} transactions via COPY BINARY method",
                    res.rows_affected()
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    "[ProjectionStore] bulk_insert_transactions_with_copy: Insert failed: {}",
                    e
                );
                Err(e.into())
            }
        }
    }

    /// Fallback method using UNNEST (original implementation)
    async fn bulk_upsert_accounts_unnest(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        accounts: &[AccountProjection],
    ) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = accounts.iter().map(|a| a.id).collect();
        let owner_names: Vec<String> = accounts.iter().map(|a| a.owner_name.clone()).collect();
        let balances: Vec<Decimal> = accounts.iter().map(|a| a.balance).collect();
        let is_actives: Vec<bool> = accounts.iter().map(|a| a.is_active).collect();
        let created_ats: Vec<DateTime<Utc>> = accounts.iter().map(|a| a.created_at).collect();
        let updated_ats: Vec<DateTime<Utc>> = accounts.iter().map(|a| a.updated_at).collect();

        sqlx::query!(
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
        .await?;

        Ok(())
    }

    /// Fallback method using UNNEST (original implementation)
    async fn bulk_insert_transactions_unnest(
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

        // LOST EVENT LOGGING: rows_affected < transactions.len() implies conflicts or drops
        let inserted_rows: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM transaction_projections WHERE id = ANY($1)"#,
            &transactions.iter().map(|t| t.id).collect::<Vec<_>>()
        )
        .fetch_one(&mut **tx)
        .await
        .ok()
        .flatten()
        .unwrap_or(0_i64);

        let expected = transactions.len() as i64;
        if inserted_rows < expected {
            let lost = expected - inserted_rows;
            tracing::warn!(
                "[ProjectionStore] LOST transactions detected: expected={}, present={}, lost={}",
                expected,
                inserted_rows,
                lost
            );

            // Log sample of missing ids to help debugging
            let present_ids: std::collections::HashSet<Uuid> = sqlx::query_scalar!(
                r#"SELECT id FROM transaction_projections WHERE id = ANY($1)"#,
                &transactions.iter().map(|t| t.id).collect::<Vec<_>>()
            )
            .fetch_all(&mut **tx)
            .await
            .unwrap_or_default()
            .into_iter()
            .collect();

            let missing: Vec<Uuid> = transactions
                .iter()
                .map(|t| t.id)
                .filter(|id| !present_ids.contains(id))
                .take(50)
                .collect();
            tracing::warn!(
                "[ProjectionStore] Missing transaction ids (sample up to 50): {:?}",
                missing
            );
        }

        Ok(())
    }

    /// Enhanced upsert method with COPY optimization and fallback
    async fn upsert_accounts_batch_parallel_optimized(
        pools: &Arc<PartitionedPools>,
        metrics: &Arc<ProjectionMetrics>,
        accounts: Vec<AccountProjection>,
    ) -> Result<()> {
        let account_count = accounts.len();
        let account_ids: Vec<_> = accounts.iter().map(|a| a.id).collect();

        tracing::info!(
            "ProjectionStore: upsert_accounts_batch_parallel_optimized processing {} accounts. IDs: {:?}",
            account_count,
            account_ids
        );

        let pool = pools.select_pool(OperationType::Write).clone();
        let mut tx = pool.begin().await?;

        // OPTIMIZATION: Use COPY for large batches, UNNEST for small batches
        let use_copy_threshold = std::env::var("PROJECTION_COPY_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1000); // Use COPY for batches >= 1000 accounts (optimized for large batches)

        if account_count >= use_copy_threshold {
            tracing::info!(
                "[ProjectionStore] Using COPY method for large batch of {} accounts (threshold: {})",
                account_count, use_copy_threshold
            );
            Self::bulk_upsert_accounts_with_copy(
                &mut tx,
                &accounts,
                &pools.select_pool(OperationType::Write),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Error upserting accounts with COPY: {}", e))?;
        } else {
            tracing::info!(
                "[ProjectionStore] Using UNNEST method for small batch of {} accounts (threshold: {})",
                account_count, use_copy_threshold
            );
            Self::bulk_upsert_accounts_unnest(&mut tx, &accounts)
                .await
                .map_err(|e| anyhow::anyhow!("Error upserting accounts with UNNEST: {}", e))?;
        }

        tx.commit().await?;

        metrics
            .batch_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            "ProjectionStore: upsert_accounts_batch_parallel_optimized completed successfully for {} accounts. IDs: {:?}",
            account_count,
            account_ids
        );

        Ok(())
    }

    /// Enhanced transaction insert method with COPY optimization and fallback
    async fn insert_transactions_batch_parallel_optimized(
        pools: &Arc<PartitionedPools>,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()> {
        let transaction_count = transactions.len();

        tracing::info!(
            "ProjectionStore: insert_transactions_batch_parallel_optimized processing {} transactions",
            transaction_count
        );

        let pool = pools.select_pool(OperationType::Write).clone();
        let mut tx = pool.begin().await?;

        // OPTIMIZATION: Use COPY for large batches, UNNEST for small batches
        let use_copy_threshold = std::env::var("TRANSACTION_COPY_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(50); // Use COPY for batches >= 50 transactions

        if transaction_count >= use_copy_threshold {
            tracing::info!(
                "[ProjectionStore] Using COPY method for large transaction batch of {} (threshold: {})",
                transaction_count, use_copy_threshold
            );
            Self::bulk_insert_transactions_direct_copy(&mut tx, &transactions)
                .await
                .map_err(|e| anyhow::anyhow!("Error inserting transactions with COPY: {}", e))?;
        } else {
            tracing::info!(
                "[ProjectionStore] Using UNNEST method for small transaction batch of {} (threshold: {})",
                transaction_count, use_copy_threshold
            );
            Self::bulk_insert_transactions_unnest(&mut tx, &transactions)
                .await
                .map_err(|e| anyhow::anyhow!("Error inserting transactions with UNNEST: {}", e))?;
        }

        tx.commit().await?;

        tracing::info!(
            "ProjectionStore: insert_transactions_batch_parallel_optimized completed successfully for {} transactions",
            transaction_count
        );

        Ok(())
    }

    async fn update_processor_optimized(
        pools: Arc<PartitionedPools>,
        mut receiver: mpsc::UnboundedReceiver<ProjectionUpdate>,
        _account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
        _transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
        _cache_version: Arc<std::sync::atomic::AtomicU64>,
        metrics: Arc<ProjectionMetrics>,
        config: ProjectionConfig,
        shutdown_token: CancellationToken,
    ) {
        let mut account_batch = Vec::with_capacity(config.batch_size);

        // CRITICAL OPTIMIZATION: Batch accumulation for transactions
        let mut transaction_accumulator = Vec::with_capacity(config.batch_size * 2); // Larger capacity for accumulation
        let mut last_transaction_flush = std::time::Instant::now();
        let transaction_accumulation_timeout = Duration::from_millis(100); // 100ms accumulation window

        // OPTIMIZATION: Use adaptive timeout based on batch size
        let adaptive_timeout = Duration::from_millis(if config.batch_size >= 1000 {
            50
        } else {
            config.batch_timeout_ms
        });

        // CDC-specific optimizations
        let cdc_batch_size = std::env::var("CDC_PROJECTION_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(config.batch_size); // Default to config batch size

        // CRITICAL: Get COPY threshold for transaction accumulation
        let copy_config = CopyOptimizationConfig::from_env();
        let transaction_copy_threshold = copy_config.transaction_copy_threshold;

        tracing::info!(
            "[ProjectionStore] update_processor_optimized started with CDC batch_size={}, adaptive_timeout={:?}, transaction_copy_threshold={}, accumulation_timeout={:?}",
            cdc_batch_size, adaptive_timeout, transaction_copy_threshold, transaction_accumulation_timeout
        );

        // OPTIMIZATION: Pre-allocate processing tasks
        let mut processing_tasks = Vec::new();

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("🛑 ProjectionStore: update_processor_optimized received shutdown signal");

                    // OPTIMIZATION: Wait for any ongoing processing tasks
                    for task in processing_tasks {
                        let _ = task.await;
                    }

                    // Process remaining batches before shutdown
                    if !account_batch.is_empty() {
                        info!("🛑 ProjectionStore: Processing final account batch of {} items before shutdown", account_batch.len());
                        let batch_to_process = std::mem::take(&mut account_batch);
                        let pools = pools.clone();
                        let metrics = metrics.clone();
                        if let Err(e) = Self::upsert_accounts_batch_parallel_optimized(&pools, &metrics, batch_to_process).await {
                            error!("[ProjectionStore] Error upserting final account batch with COPY optimization: {}", e);
                        }
                    }

                    if !transaction_accumulator.is_empty() {
                        info!("🛑 ProjectionStore: Processing final accumulated transaction batch of {} items before shutdown", transaction_accumulator.len());
                        let batch_to_process = std::mem::take(&mut transaction_accumulator);
                        let pools = pools.clone();
                        if let Err(e) = Self::insert_transactions_batch_parallel_optimized(&pools, batch_to_process).await {
                            error!("[ProjectionStore] Error inserting final accumulated transaction batch with COPY optimization: {}", e);
                        }
                    }

                    info!("✅ ProjectionStore: update_processor_optimized shutdown complete");
                    break;
                }
                Some(update) = receiver.recv() => {
                    match update {
                        ProjectionUpdate::AccountBatch(accounts) => {
                            account_batch.extend(accounts);

                            // OPTIMIZATION: Immediate flush for large batches
                            if account_batch.len() >= cdc_batch_size {
                                let batch_to_process = std::mem::take(&mut account_batch);
                                let pools = pools.clone();
                                let metrics = metrics.clone();

                                // OPTIMIZATION: Track processing tasks
                                let task = tokio::spawn(async move {
                                    if let Err(e) = Self::upsert_accounts_batch_parallel_optimized(&pools, &metrics, batch_to_process).await {
                                        error!("[ProjectionStore] Error upserting account batch with COPY optimization: {}", e);
                                    }
                                });
                                processing_tasks.push(task);
                            }
                        }
                        ProjectionUpdate::TransactionBatch(transactions) => {
                            transaction_accumulator.extend(transactions);

                            // CRITICAL OPTIMIZATION: Accumulate transactions until we reach COPY threshold
                            if transaction_accumulator.len() >= transaction_copy_threshold {
                                tracing::info!(
                                    "[ProjectionStore] Transaction accumulator reached threshold: {} >= {}, flushing to direct COPY",
                                    transaction_accumulator.len(),
                                    transaction_copy_threshold
                                );

                                let batch_to_process = std::mem::take(&mut transaction_accumulator);
                                let pools = pools.clone();

                                // OPTIMIZATION: Track processing tasks
                                let task = tokio::spawn(async move {
                                    if let Err(e) = Self::insert_transactions_batch_parallel_optimized(&pools, batch_to_process).await {
                                        error!("[ProjectionStore] Error inserting accumulated transaction batch with COPY optimization: {}", e);
                                    }
                                });
                                processing_tasks.push(task);
                                last_transaction_flush = std::time::Instant::now();
                            }
                        }
                    }
                }
                                _ = tokio::time::sleep(adaptive_timeout) => {
                    // OPTIMIZATION: Process account batches independently
                    if !account_batch.is_empty() {
                        let batch_to_process = std::mem::take(&mut account_batch);
                        let pools = pools.clone();
                        let metrics = metrics.clone();

                        let task = tokio::spawn(async move {
                            if let Err(e) = Self::upsert_accounts_batch_parallel_optimized(&pools, &metrics, batch_to_process).await {
                                error!("[ProjectionStore] Error upserting account batch (timeout) with COPY optimization: {}", e);
                            }
                        });
                        processing_tasks.push(task);
                    }

                    // CRITICAL FIX: Process transaction batches independently (don't wait for account tasks)
                    if !transaction_accumulator.is_empty() {
                        // CRITICAL: Check if we should flush based on accumulation timeout
                        let should_flush_by_timeout = last_transaction_flush.elapsed() >= transaction_accumulation_timeout;

                        if should_flush_by_timeout {
                            tracing::info!(
                                "[ProjectionStore] Transaction accumulator timeout reached: {}ms, flushing {} transactions (threshold: {})",
                                last_transaction_flush.elapsed().as_millis(),
                                transaction_accumulator.len(),
                                transaction_copy_threshold
                            );
                        }

                        let batch_to_process = std::mem::take(&mut transaction_accumulator);
                        let pools = pools.clone();

                        let task = tokio::spawn(async move {
                            if let Err(e) = Self::insert_transactions_batch_parallel_optimized(&pools, batch_to_process).await {
                                error!("[ProjectionStore] Error inserting transaction batch (timeout) with COPY optimization: {}", e);
                            }
                        });
                        processing_tasks.push(task);
                        last_transaction_flush = std::time::Instant::now();
                    }
                }
            }

            // OPTIMIZATION: Clean up completed tasks
            processing_tasks.retain(|task| !task.is_finished());
        }
    }

    /// CDC-optimized batch processing with true parallel processing
    pub async fn process_cdc_batch_optimized(
        &self,
        accounts: Vec<AccountProjection>,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        let account_count = accounts.len();
        let transaction_count = transactions.len();

        tracing::info!(
            "🚀 OPTIMIZED: process_cdc_batch_optimized: processing {} accounts, {} transactions",
            account_count,
            transaction_count
        );

        // Get COPY optimization config
        let copy_config = CopyOptimizationConfig::from_env();

        if !copy_config.enable_copy_optimization {
            tracing::info!("[ProjectionStore] COPY optimization disabled, using standard batching");
            if !accounts.is_empty() {
                self.update_sender
                    .send(ProjectionUpdate::AccountBatch(accounts))
                    .map_err(|e| anyhow::anyhow!("Failed to send account batch: {}", e))?;
            }
            if !transactions.is_empty() {
                self.update_sender
                    .send(ProjectionUpdate::TransactionBatch(transactions))
                    .map_err(|e| anyhow::anyhow!("Failed to send transaction batch: {}", e))?;
            }
            return Ok(());
        }

        // CRITICAL OPTIMIZATION: Use direct COPY for large batches
        let use_direct_copy_accounts = account_count >= copy_config.projection_copy_threshold;
        let use_direct_copy_transactions =
            transaction_count >= copy_config.transaction_copy_threshold;

        if use_direct_copy_accounts || use_direct_copy_transactions {
            tracing::info!(
                "🚀 OPTIMIZED: Using direct COPY processing for accounts: {}, transactions: {}",
                use_direct_copy_accounts,
                use_direct_copy_transactions
            );

            // CRITICAL: Process accounts and transactions in parallel
            let (account_result, transaction_result) = tokio::join!(
                async {
                    if use_direct_copy_accounts && !accounts.is_empty() {
                        self.upsert_accounts_direct_copy(accounts).await
                    } else {
                        Ok(())
                    }
                },
                async {
                    if use_direct_copy_transactions && !transactions.is_empty() {
                        self.insert_transactions_direct_copy(transactions).await
                    } else {
                        Ok(())
                    }
                }
            );

            // Handle results
            if let Err(e) = account_result {
                tracing::error!(
                    "🚀 OPTIMIZED: Failed to upsert accounts with direct COPY: {}",
                    e
                );
                return Err(e);
            }

            if let Err(e) = transaction_result {
                tracing::error!(
                    "🚀 OPTIMIZED: Failed to insert transactions with direct COPY: {}",
                    e
                );
                return Err(e);
            }

            let duration = start_time.elapsed();
            tracing::info!(
                "🚀 OPTIMIZED: process_cdc_batch_optimized completed in {:?} (accounts: {}, transactions: {})",
                duration, account_count, transaction_count
            );

            return Ok(());
        }

        // Fallback to standard batching for small batches
        tracing::info!("[ProjectionStore] Using standard batching for CDC batch (accounts: {}, transactions: {})", account_count, transaction_count);

        if !accounts.is_empty() {
            self.update_sender
                .send(ProjectionUpdate::AccountBatch(accounts))
                .map_err(|e| anyhow::anyhow!("Failed to send account batch: {}", e))?;
        }
        if !transactions.is_empty() {
            self.update_sender
                .send(ProjectionUpdate::TransactionBatch(transactions))
                .map_err(|e| anyhow::anyhow!("Failed to send transaction batch: {}", e))?;
        }

        let duration = start_time.elapsed();
        tracing::info!(
            "[ProjectionStore] process_cdc_batch_optimized completed in {:?} (accounts: {}, transactions: {})",
            duration, account_count, transaction_count
        );

        Ok(())
    }

    /// Direct COPY upsert for accounts with optimized processing
    async fn upsert_accounts_direct_copy(&self, accounts: Vec<AccountProjection>) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        let pool = self.pools.select_pool(OperationType::Write);
        let mut tx = pool.begin().await?;

        // Use the optimized COPY function
        if let Err(e) = Self::bulk_upsert_accounts_with_copy(&mut tx, &accounts, pool).await {
            tx.rollback().await?;
            return Err(anyhow::anyhow!("Bulk upsert accounts failed: {:?}", e));
        }

        tx.commit().await?;
        Ok(())
    }

    /// Direct COPY insert for transactions with optimized processing
    async fn insert_transactions_direct_copy(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<()> {
        if transactions.is_empty() {
            return Ok(());
        }

        let pool = self.pools.select_pool(OperationType::Write);
        let mut tx = pool.begin().await?;

        // Use the optimized COPY function
        if let Err(e) = Self::bulk_insert_transactions_with_copy(&mut tx, &transactions, pool).await
        {
            tx.rollback().await?;
            return Err(anyhow::anyhow!("Bulk insert transactions failed: {:?}", e));
        }

        tx.commit().await?;
        Ok(())
    }

    async fn metrics_reporter(metrics: Arc<ProjectionMetrics>, shutdown_token: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("🛑 ProjectionStore: metrics_reporter received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
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
                        / 1000.0;

                    let hit_rate = if hits + misses > 0 {
                        (hits as f64 / (hits + misses) as f64) * 100.0
                    } else {
                        0.0
                    };

                    // COPY optimization metrics
                    let copy_success_rate = metrics.copy_metrics.get_copy_success_rate();
                    let avg_copy_duration = metrics.copy_metrics.get_average_copy_duration();
                    let avg_unnest_duration = metrics.copy_metrics.get_average_unnest_duration();
                    let copy_fallbacks = metrics
                        .copy_metrics
                        .copy_fallback_to_unnest
                        .load(std::sync::atomic::Ordering::Relaxed);

                    info!(
                        "Projection Metrics - Cache Hit Rate: {:.2}%, Batch Updates: {}, Errors: {}, Avg Query Time: {:.2}ms",
                        hit_rate, batches, errors, avg_query_time
                    );

                    info!(
                        "COPY Optimization Metrics - Success Rate: {:.2}%, Avg COPY Duration: {:.2}ms, Avg UNNEST Duration: {:.2}ms, Fallbacks: {}",
                        copy_success_rate, avg_copy_duration, avg_unnest_duration, copy_fallbacks
                    );
                }
            }
        }
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
        let shutdown_token = CancellationToken::new();

        let processor_cache_version = cache_version.clone();
        let processor_metrics = metrics.clone();
        let shutdown_token_processor = shutdown_token.clone();

        let store = Self {
            pools: pools.clone(),
            account_cache: account_cache.clone(),
            transaction_cache: transaction_cache.clone(),
            update_sender,
            cache_version,
            metrics,
            config: config.clone(),
            shutdown_token,
        };

        // Start optimized background processor
        let processor_pools = pools.clone();
        let processor_account_cache = account_cache.clone();
        let processor_transaction_cache = transaction_cache.clone();
        let processor_config = config.clone();
        tokio::spawn(async move {
            Self::update_processor_optimized(
                processor_pools,
                update_receiver,
                processor_account_cache,
                processor_transaction_cache,
                processor_cache_version,
                processor_metrics,
                processor_config,
                shutdown_token_processor,
            )
            .await;
        });

        let metrics = store.metrics.clone();
        let shutdown_token_metrics = store.shutdown_token.clone();
        tokio::spawn(Self::metrics_reporter(metrics, shutdown_token_metrics));

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

        // Process all accounts in a single bulk operation
        Self::bulk_upsert_accounts(&mut tx, &accounts).await?;

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

        // Process all transactions in a single bulk operation
        Self::bulk_insert_transactions(&mut tx, &transactions).await?;

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

        // LOST EVENT LOGGING: rows_affected < transactions.len() implies conflicts or drops
        let inserted_rows: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM transaction_projections WHERE id = ANY($1)"#,
            &transactions.iter().map(|t| t.id).collect::<Vec<_>>()
        )
        .fetch_one(&mut **tx)
        .await
        .ok()
        .flatten()
        .unwrap_or(0_i64);

        let expected = transactions.len() as i64;
        if inserted_rows < expected {
            let lost = expected - inserted_rows;
            tracing::warn!(
                "[ProjectionStore] LOST transactions detected: expected={}, present={}, lost={}",
                expected,
                inserted_rows,
                lost
            );

            // Log sample of missing ids to help debugging
            let present_ids: std::collections::HashSet<Uuid> = sqlx::query_scalar!(
                r#"SELECT id FROM transaction_projections WHERE id = ANY($1)"#,
                &transactions.iter().map(|t| t.id).collect::<Vec<_>>()
            )
            .fetch_all(&mut **tx)
            .await
            .unwrap_or_default()
            .into_iter()
            .collect();

            let missing: Vec<Uuid> = transactions
                .iter()
                .map(|t| t.id)
                .filter(|id| !present_ids.contains(id))
                .take(50)
                .collect();
            tracing::warn!(
                "[ProjectionStore] Missing transaction ids (sample up to 50): {:?}",
                missing
            );
        }

        Ok(())
    }

    async fn cache_cleanup_worker(
        account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<AccountProjection>>>>,
        transaction_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Vec<TransactionProjection>>>>>,
        shutdown_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("🛑 ProjectionStore: cache_cleanup_worker received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
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
        }
    }

    // async fn metrics_reporter(metrics: Arc<ProjectionMetrics>) {
    //     let mut interval = tokio::time::interval(Duration::from_secs(60));

    //     loop {
    //         interval.tick().await;

    //         let hits = metrics
    //             .cache_hits
    //             .load(std::sync::atomic::Ordering::Relaxed);
    //         let misses = metrics
    //             .cache_misses
    //             .load(std::sync::atomic::Ordering::Relaxed);
    //         let batches = metrics
    //             .batch_updates
    //             .load(std::sync::atomic::Ordering::Relaxed);
    //         let errors = metrics.errors.load(std::sync::atomic::Ordering::Relaxed);
    //         let avg_query_time = metrics
    //             .query_duration
    //             .load(std::sync::atomic::Ordering::Relaxed) as f64
    //             / 1000.0; // Convert to milliseconds

    //         let hit_rate = if hits + misses > 0 {
    //             (hits as f64 / (hits + misses) as f64) * 100.0
    //         } else {
    //             0.0
    //         };

    //         info!(
    //             "Projection Metrics - Cache Hit Rate: {:.2}%, Batch Updates: {}, Errors: {}, Avg Query Time: {:.2}ms",
    //             hit_rate, batches, errors, avg_query_time
    //         );
    //     }
    // }

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

    /// NEW: Direct COPY for new accounts (INSERT only, no UPSERT)
    /// This is optimized for AccountCreated events where we know the account doesn't exist
    pub async fn bulk_insert_new_accounts_with_copy(
        &self,
        accounts: Vec<AccountProjection>,
    ) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        let account_count = accounts.len();
        tracing::info!(
            "[ProjectionStore] bulk_insert_new_accounts_with_copy: processing {} new accounts",
            account_count
        );

        let pool = self.pools.select_pool(OperationType::Write);
        let mut tx = pool.begin().await?;

        // CRITICAL: Direct COPY to target table (no temp table needed for new accounts)
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new()?;
        for account in &accounts {
            writer.write_row(6)?; // 6 fields
            writer.write_uuid(&account.id)?;
            writer.write_text(&account.owner_name)?;
            writer.write_decimal(&account.balance, "balance")?;
            writer.write_bool(account.is_active)?;
            writer.write_timestamp(&account.created_at)?;
            writer.write_timestamp(&account.updated_at)?;
        }
        let binary_data = writer.finish()?;

        tracing::debug!(
            "[ProjectionStore] Generated binary data: {} bytes for {} new accounts",
            binary_data.len(),
            accounts.len()
        );

        // CRITICAL: Direct COPY to target table (no temp table, no UPSERT)
        let copy_command = "COPY account_projections FROM STDIN WITH (FORMAT BINARY)";
        let mut copy_in = tx.copy_in_raw(copy_command).await?;
        copy_in.send(binary_data.as_slice()).await?;
        let copy_result = copy_in.finish().await?;

        tracing::info!(
            "[ProjectionStore] Direct COPY successfully inserted {} new accounts (no temp table, no UPSERT)",
            copy_result
        );

        tx.commit().await?;

        // Update metrics
        self.metrics
            .batch_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Graceful shutdown of the projection store
    pub async fn shutdown(&self) -> Result<()> {
        info!("🛑 ProjectionStore: Starting graceful shutdown");

        // Cancel shutdown token to signal all background tasks
        self.shutdown_token.cancel();

        // Wait a bit for background tasks to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("✅ ProjectionStore: Graceful shutdown complete");
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
