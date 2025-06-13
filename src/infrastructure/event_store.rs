use crate::domain::{Account, AccountEvent};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    postgres::{PgPoolOptions, PgValue},
    PgPool, Postgres, Row, Transaction,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, OnceCell, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Global connection pool
pub static DB_POOL: OnceCell<Arc<PgPool>> = OnceCell::const_new();

#[derive(Debug, Clone)]
pub struct Event {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub event_data: Value,
    pub version: i64,
    pub timestamp: DateTime<Utc>,
    pub metadata: EventMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    pub user_id: Option<String>,
    pub source: String,
    pub schema_version: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct EventValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
    batch_sender: mpsc::UnboundedSender<BatchedEvent>,
    snapshot_cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
    config: EventStoreConfig,
    batch_semaphore: Arc<Semaphore>,
    metrics: Arc<EventStoreMetrics>,
    event_validators: Arc<DashMap<String, Box<dyn EventValidator + Send + Sync>>>,
    event_handlers: Arc<DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>>,
    version_cache: Arc<DashMap<Uuid, i64>>,
}

#[derive(Debug)]
struct BatchedEvent {
    aggregate_id: Uuid,
    events: Vec<AccountEvent>,
    expected_version: i64,
    response_tx: tokio::sync::oneshot::Sender<Result<()>>,
    created_at: Instant,
    priority: EventPriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EventPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

#[derive(Debug, Clone)]
struct CachedSnapshot {
    aggregate_id: Uuid,
    data: serde_json::Value,
    version: i64,
    created_at: Instant,
    ttl: Duration,
}

#[derive(Debug, Default, Serialize)]
pub struct EventStoreMetrics {
    pub events_processed: AtomicU64,
    pub events_failed: AtomicU64,
    pub batch_count: AtomicU64,
    pub avg_batch_size: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub success_rate: f64,
    pub cache_hit_rate: f64,
    pub connection_acquire_time: AtomicU64,
    pub connection_acquire_errors: AtomicU64,
    pub connection_reuse_count: AtomicU64,
    pub connection_timeout_count: AtomicU64,
    pub connection_lifetime: AtomicU64,
}

impl EventStoreMetrics {
    pub fn new() -> Self {
        Self {
            events_processed: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
            avg_batch_size: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            success_rate: 0.0,
            cache_hit_rate: 0.0,
            connection_acquire_time: AtomicU64::new(0),
            connection_acquire_errors: AtomicU64::new(0),
            connection_reuse_count: AtomicU64::new(0),
            connection_timeout_count: AtomicU64::new(0),
            connection_lifetime: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
pub trait EventValidator: Send + Sync {
    async fn validate(&self, event: &Event) -> EventValidationResult;
}

#[async_trait::async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle(&self, event: &Event) -> Result<()>;
}

impl EventStore {
    /// Create EventStore with an existing PgPool
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_config_and_pool(pool, EventStoreConfig::default())
    }

    /// Create EventStore with custom config and existing pool
    pub fn new_with_config_and_pool(pool: PgPool, config: EventStoreConfig) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        let snapshot_cache = Arc::new(RwLock::new(HashMap::new()));
        let batch_semaphore = Arc::new(Semaphore::new(config.max_batch_queue_size));
        let metrics = Arc::new(EventStoreMetrics::default());
        let event_validators = Arc::new(DashMap::new());
        let event_handlers = Arc::new(DashMap::new());
        let version_cache = Arc::new(DashMap::new());

        let store = Self {
            pool: pool.clone(),
            batch_sender,
            snapshot_cache: snapshot_cache.clone(),
            config: config.clone(),
            batch_semaphore: batch_semaphore.clone(),
            metrics: metrics.clone(),
            event_validators,
            event_handlers: event_handlers.clone(),
            version_cache,
        };

        // Start background batch processor
        tokio::spawn(Self::batch_processor(
            pool.clone(),
            batch_receiver,
            config.clone(),
            batch_semaphore,
            metrics.clone(),
            event_handlers.clone(),
            0,
        ));

        // Start periodic snapshot creation
        tokio::spawn(Self::snapshot_worker(
            pool.clone(),
            snapshot_cache.clone(),
            config.clone(),
        ));

        // Start cache cleanup worker
        tokio::spawn(Self::cache_cleanup_worker(snapshot_cache));

        // Start metrics reporter
        let reporter_metrics = metrics.clone();
        tokio::spawn(async move {
            Self::metrics_reporter(reporter_metrics).await;
        });

        store
    }

    /// Create EventStore with a specific pool size
    pub async fn new_with_pool_size(pool_size: u32) -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .context("DATABASE_URL environment variable is required")?;

        Self::new_with_pool_size_and_url(pool_size, &database_url).await
    }

    /// Create EventStore with a specific pool size and database URL
    pub async fn new_with_pool_size_and_url(pool_size: u32, database_url: &str) -> Result<Self> {
        let mut config = EventStoreConfig::default();
        config.database_url = database_url.to_string();
        config.max_connections = pool_size;

        Self::new_with_config(config).await
    }

    /// Create EventStore with full configuration options
    pub async fn new_with_config(config: EventStoreConfig) -> Result<Self> {
        // Get or initialize the global connection pool
        let pool = DB_POOL
            .get_or_try_init(|| async {
                let pool = PgPoolOptions::new()
                    .max_connections(config.max_connections)
                    .min_connections(config.min_connections)
                    .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
                    .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
                    .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
                    // Enable prepared statement caching and optimize connection settings
                    .after_connect(|conn, _meta| {
                        Box::pin(async move {
                            // Set only essential and well-supported PostgreSQL parameters
                            sqlx::query("SET SESSION synchronous_commit = 'off'")
                                .execute(&mut *conn)
                                .await?;
                            sqlx::query("SET SESSION work_mem = '32MB'")
                                .execute(&mut *conn)
                                .await?;
                            sqlx::query("SET SESSION maintenance_work_mem = '128MB'")
                                .execute(&mut *conn)
                                .await?;
                            sqlx::query("SET SESSION effective_cache_size = '2GB'")
                                .execute(&mut *conn)
                                .await?;
                            sqlx::query("SET SESSION random_page_cost = 1.1")
                                .execute(&mut *conn)
                                .await?;
                            // sqlx::query("SET SESSION effective_io_concurrency = 1")
                            //     .execute(&mut *conn)
                            //     .await?;
                            sqlx::query("SET SESSION statement_timeout = '5s'")
                                .execute(&mut *conn)
                                .await?;
                            Ok(())
                        })
                    })
                    .connect(&config.database_url)
                    .await
                    .context("Failed to connect to database")?;

                Ok::<_, anyhow::Error>(Arc::new(pool))
            })
            .await?;

        Ok(Self::new_with_config_and_pool(
            pool.as_ref().clone(),
            config,
        ))
    }

    // Enhanced event saving with priority support
    pub async fn save_events_with_priority(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
        priority: EventPriority,
    ) -> Result<()> {
        let start_time = Instant::now();
        let permit = self.batch_semaphore.clone().acquire_owned().await?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let batched_event = BatchedEvent {
            aggregate_id,
            events,
            expected_version,
            response_tx,
            created_at: Instant::now(),
            priority,
        };

        self.batch_sender.send(batched_event)?;
        self.metrics
            .batch_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match response_rx.await {
            Ok(result) => {
                let duration = start_time.elapsed();
                self.metrics.connection_acquire_time.fetch_add(
                    duration.as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                result
            }
            Err(_) => Err(anyhow::anyhow!(
                "Failed to receive batch processing response"
            )),
        }
    }

    // Modify the with_retry method to handle lifetimes correctly
    async fn with_retry<F, Fut, T, E>(&self, operation: F, config: &RetryConfig) -> Result<T>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, E>> + Send,
        T: Send,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        let mut retries = 0;
        let mut delay = config.initial_delay;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if retries >= config.max_retries {
                        return Err(anyhow::anyhow!(
                            "Operation failed after {} retries: {}",
                            retries,
                            e
                        ));
                    }

                    warn!(
                        "Operation failed (attempt {}/{}): {}. Retrying in {:?}...",
                        retries + 1,
                        config.max_retries,
                        e,
                        delay
                    );

                    tokio::time::sleep(delay).await;
                    retries += 1;
                    delay = std::cmp::min(
                        Duration::from_secs_f64(delay.as_secs_f64() * config.backoff_factor),
                        config.max_delay,
                    );
                }
            }
        }
    }

    // Modify save_events to handle the move issue correctly
    pub async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<()> {
        self.save_events_with_priority(
            aggregate_id,
            events,
            expected_version,
            EventPriority::Normal,
        )
        .await
    }

    // Multi-threaded batch processor with priority queuing
    async fn batch_processor(
        pool: PgPool,
        mut receiver: mpsc::UnboundedReceiver<BatchedEvent>,
        config: EventStoreConfig,
        semaphore: Arc<Semaphore>,
        metrics: Arc<EventStoreMetrics>,
        event_handlers: Arc<DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>>,
        worker_id: usize,
    ) {
        let mut batch = Vec::new();
        let mut last_flush = Instant::now();

        loop {
            let timeout = tokio::time::sleep(Duration::from_millis(config.batch_timeout_ms));

            tokio::select! {
                Some(event) = receiver.recv() => {
                    batch.push(event);

                    if batch.len() >= config.batch_size {
                        let batch_to_process = std::mem::replace(&mut batch, Vec::new());
                        let pool = pool.clone();
                        let metrics = metrics.clone();
                        let event_handlers = event_handlers.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::flush_batch(&pool, batch_to_process, &metrics, &event_handlers).await {
                                error!("Worker {} failed to flush batch: {}", worker_id, e);
                            }
                        });
                        last_flush = Instant::now();
                    }
                }
                _ = timeout => {
                    if !batch.is_empty() {
                        let batch_to_process = std::mem::replace(&mut batch, Vec::new());
                        let pool = pool.clone();
                        let metrics = metrics.clone();
                        let event_handlers = event_handlers.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::flush_batch(&pool, batch_to_process, &metrics, &event_handlers).await {
                                error!("Worker {} failed to flush batch: {}", worker_id, e);
                            }
                        });
                        last_flush = Instant::now();
                    }
                }
            }
        }
    }

    // Optimized batch flushing with prepared statements and COPY
    async fn flush_batch(
        pool: &PgPool,
        batch: Vec<BatchedEvent>,
        metrics: &EventStoreMetrics,
        event_handlers: &DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>,
    ) -> Result<()> {
        let mut tx = pool.begin().await?;

        for batched_event in batch {
            let event_data = Self::prepare_events_for_insert_optimized(&batched_event)?;

            // Validate events
            for event in &event_data {
                let validation = Self::validate_event(event).await;
                if !validation.is_valid {
                    return Err(anyhow::anyhow!(
                        "Event validation failed: {:?}",
                        validation.errors
                    ));
                }
            }

            // Handle events
            for event in &event_data {
                Self::handle_event(event, event_handlers).await?;
            }

            Self::bulk_insert_events_optimized(&mut tx, event_data).await?;

            if let Err(e) = batched_event.response_tx.send(Ok(())) {
                warn!("Failed to send batch response: {:?}", e);
            }
        }

        tx.commit().await?;
        Ok(())
    }

    // Pre-calculate event data to reduce serialization overhead
    fn prepare_events_for_insert_optimized(batched_event: &BatchedEvent) -> Result<Vec<Event>> {
        let mut events = Vec::with_capacity(batched_event.events.len());
        let mut version = batched_event.expected_version;

        for event in &batched_event.events {
            version += 1;
            let event = Event {
                id: Uuid::new_v4(),
                aggregate_id: batched_event.aggregate_id,
                event_type: event.event_type().to_string(),
                event_data: serde_json::to_value(event)?,
                version,
                timestamp: Utc::now(),
                metadata: EventMetadata {
                    correlation_id: None,
                    causation_id: None,
                    user_id: None,
                    source: "event_store".to_string(),
                    schema_version: "1.0".to_string(),
                    tags: Vec::new(),
                },
            };
            events.push(event);
        }

        Ok(events)
    }

    // Use PostgreSQL COPY for maximum insert performance
    async fn bulk_insert_events_optimized(
        tx: &mut Transaction<'_, Postgres>,
        events: Vec<Event>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut query = String::from(
            r#"
            INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp, metadata)
            VALUES
            "#,
        );

        let mut values = Vec::new();
        let mut params: Vec<(Uuid, Uuid, String, Value, i64, DateTime<Utc>, Value)> = Vec::new();
        let mut param_index = 1;

        for event in events {
            values.push(format!(
                "(${},${},${},${},${},${},${})",
                param_index,
                param_index + 1,
                param_index + 2,
                param_index + 3,
                param_index + 4,
                param_index + 5,
                param_index + 6
            ));

            params.push((
                event.id,
                event.aggregate_id,
                event.event_type,
                event.event_data,
                event.version,
                event.timestamp,
                serde_json::to_value(event.metadata)?,
            ));

            param_index += 7;
        }

        query.push_str(&values.join(","));

        let mut query = sqlx::query(&query);
        for (id, aggregate_id, event_type, event_data, version, timestamp, metadata) in params {
            query = query
                .bind(id)
                .bind(aggregate_id)
                .bind(event_type)
                .bind(event_data)
                .bind(version)
                .bind(timestamp)
                .bind(metadata);
        }

        query.execute(&mut **tx).await?;

        Ok(())
    }

    // Modify get_events to add proper type annotations
    pub async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>> {
        let start_time = Instant::now();
        let mut conn = self.pool.acquire().await?;

        let events = sqlx::query!(
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            WHERE aggregate_id = $1
            AND version > $2
            ORDER BY version ASC
            "#,
            aggregate_id,
            from_version
        )
        .fetch_all(&mut *conn)
        .await?;

        let duration = start_time.elapsed();
        self.metrics.connection_acquire_time.fetch_add(
            duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(events
            .into_iter()
            .map(|row| Event {
                id: row.id,
                aggregate_id: row.aggregate_id,
                event_type: row.event_type,
                event_data: row.event_data,
                version: row.version,
                timestamp: row.timestamp,
                metadata: EventMetadata::default(),
            })
            .collect())
    }

    fn create_snapshot_event(&self, snapshot: &CachedSnapshot) -> Event {
        Event {
            id: Uuid::new_v4(),
            aggregate_id: snapshot.aggregate_id,
            event_type: "Snapshot".to_string(),
            event_data: snapshot.data.clone(),
            version: snapshot.version,
            timestamp: Utc::now(),
            metadata: EventMetadata::default(),
        }
    }

    // Async snapshot worker with better batching
    async fn snapshot_worker(
        pool: PgPool,
        cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        config: EventStoreConfig,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.snapshot_interval_secs));
        interval.tick().await; // Initial tick to run immediately if needed

        loop {
            interval.tick().await;

            // Query to find aggregates that might need a new snapshot
            // This query now directly compares the max event version with the last snapshot version.
            let candidates_result = sqlx::query!(
                r#"
                SELECT
                    e.aggregate_id,
                    COUNT(e.id) as event_count,
                    MAX(e.version) as max_version,
                    COALESCE(s.version, 0) as last_snapshot_version_db -- Alias for clarity and correct field access
                FROM events e
                LEFT JOIN snapshots s ON e.aggregate_id = s.aggregate_id
                GROUP BY e.aggregate_id, s.version -- Group by s.version to distinguish if there's no snapshot
                HAVING COUNT(e.id) > $1 AND MAX(e.version) > COALESCE(s.version, 0)
                ORDER BY COUNT(e.id) DESC
                LIMIT $2
                "#,
                config.snapshot_threshold as i64,
                config.max_snapshots_per_run as i64
            )
            .fetch_all(&pool)
            .await;

            match candidates_result {
                Ok(candidates) => {
                    // Collect tasks to run in parallel
                    let mut snapshot_tasks = Vec::new();

                    for candidate_row in candidates {
                        // Access fields directly from the generated struct
                        let aggregate_id = candidate_row.aggregate_id;
                        let max_version = candidate_row.max_version.unwrap_or(0); // max_version from query is i64, so it needs unwrap_or
                        let last_snapshot_version =
                            candidate_row.last_snapshot_version_db.unwrap_or(0); // Coalesce makes it i64, unwrap_or for safety

                        // Only create a snapshot if there are new events since the last snapshot
                        if max_version > last_snapshot_version {
                            let pool_clone = pool.clone();
                            let cache_clone = cache.clone();
                            let config_clone = config.clone();

                            snapshot_tasks.push(tokio::spawn(async move {
                                if let Err(e) =
                                    Self::create_snapshot(&pool_clone, &cache_clone, aggregate_id, &config_clone)
                                        .await
                                {
                                    warn!("Failed to create snapshot for {}: {}", aggregate_id, e);
                                } else {
                                    info!("Successfully created snapshot for aggregate {} at version {}", aggregate_id, max_version);
                                }
                            }));
                        } else {
                            debug!("Skipping snapshot for aggregate {}: no new events (max_version: {}, last_snapshot_version: {})", aggregate_id, max_version, last_snapshot_version);
                        }
                    }

                    // Wait for all snapshot tasks to complete
                    // This ensures backpressure on snapshot creation and provides visibility into their completion.
                    for task in snapshot_tasks {
                        if let Err(join_error) = task.await {
                            error!("Snapshot task failed to join: {}", join_error);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to query snapshot candidates: {}", e);
                }
            }
        }
    }
    async fn create_snapshot(
        pool: &PgPool,
        cache: &Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        aggregate_id: Uuid,
        config: &EventStoreConfig,
    ) -> Result<()> {
        let events = sqlx::query_as!(
            EventRow,
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            WHERE aggregate_id = $1
            ORDER BY version
            "#,
            aggregate_id
        )
        .fetch_all(pool)
        .await
        .context("Failed to fetch events for snapshot")?;

        if events.is_empty() {
            return Ok(());
        }

        let mut account = crate::domain::Account::default();
        account.id = aggregate_id;

        for event_row in &events {
            let account_event: AccountEvent = serde_json::from_value(event_row.event_data.clone())
                .context("Failed to deserialize event")?;
            account.apply_event(&account_event);
        }

        let snapshot_data =
            serde_json::to_value(&account).context("Failed to serialize account snapshot")?;
        let max_version = events.last().unwrap().version;

        sqlx::query!(
            r#"
            INSERT INTO snapshots (aggregate_id, snapshot_data, version, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (aggregate_id) DO UPDATE SET
                snapshot_data = EXCLUDED.snapshot_data,
                version = EXCLUDED.version,
                created_at = EXCLUDED.created_at
            "#,
            aggregate_id,
            snapshot_data,
            max_version
        )
        .execute(pool)
        .await
        .context("Failed to save snapshot to database")?;

        let snapshot = CachedSnapshot {
            aggregate_id,
            data: snapshot_data,
            version: max_version,
            created_at: Instant::now(),
            ttl: Duration::from_secs(config.snapshot_cache_ttl_secs),
        };

        {
            let mut cache_guard = cache.write().await;
            cache_guard.insert(aggregate_id, snapshot);
        }

        debug!(
            "Created snapshot for aggregate {} at version {}",
            aggregate_id, max_version
        );
        Ok(())
    }

    async fn cache_cleanup_worker(cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>) {
        let mut interval = tokio::time::interval(Duration::from_secs(300));

        loop {
            interval.tick().await;

            let mut expired_keys = Vec::new();
            {
                let cache_guard = cache.read().await;
                for (key, snapshot) in cache_guard.iter() {
                    if snapshot.created_at.elapsed() >= snapshot.ttl {
                        expired_keys.push(*key);
                    }
                }
            }

            if !expired_keys.is_empty() {
                let mut cache_guard = cache.write().await;
                for key in expired_keys {
                    cache_guard.remove(&key);
                }
            }
        }
    }

    // Metrics reporting task
    async fn metrics_reporter(metrics: Arc<EventStoreMetrics>) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            let processed = metrics.events_processed.load(Ordering::Relaxed);
            let failed = metrics.events_failed.load(Ordering::Relaxed);
            let batches = metrics.batch_count.load(Ordering::Relaxed);
            let cache_hits = metrics.cache_hits.load(Ordering::Relaxed);
            let cache_misses = metrics.cache_misses.load(Ordering::Relaxed);

            let success_rate = if processed > 0 {
                ((processed - failed) as f64 / processed as f64) * 100.0
            } else {
                0.0
            };

            let cache_hit_rate = if cache_hits + cache_misses > 0 {
                (cache_hits as f64 / (cache_hits + cache_misses) as f64) * 100.0
            } else {
                0.0
            };

            info!(
                "EventStore Metrics - Processed: {}, Failed: {}, Success: {:.2}%, Batches: {}, Cache Hit Rate: {:.2}%",
                processed, failed, success_rate, batches, cache_hit_rate
            );
        }
    }

    pub fn get_metrics(&self) -> EventStoreMetrics {
        let processed = self.metrics.events_processed.load(Ordering::Relaxed);
        let failed = self.metrics.events_failed.load(Ordering::Relaxed);
        let batches = self.metrics.batch_count.load(Ordering::Relaxed);
        let cache_hits = self.metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.metrics.cache_misses.load(Ordering::Relaxed);

        let success_rate = if processed > 0 {
            ((processed - failed) as f64 / processed as f64) * 100.0
        } else {
            0.0
        };

        let cache_hit_rate = if cache_hits + cache_misses > 0 {
            (cache_hits as f64 / (cache_hits + cache_misses) as f64) * 100.0
        } else {
            0.0
        };

        EventStoreMetrics {
            events_processed: AtomicU64::new(processed),
            events_failed: AtomicU64::new(failed),
            batch_count: AtomicU64::new(batches),
            avg_batch_size: AtomicU64::new(self.metrics.avg_batch_size.load(Ordering::Relaxed)),
            cache_hits: AtomicU64::new(cache_hits),
            cache_misses: AtomicU64::new(cache_misses),
            success_rate,
            cache_hit_rate,
            connection_acquire_time: AtomicU64::new(
                self.metrics.connection_acquire_time.load(Ordering::Relaxed),
            ),
            connection_acquire_errors: AtomicU64::new(
                self.metrics
                    .connection_acquire_errors
                    .load(Ordering::Relaxed),
            ),
            connection_reuse_count: AtomicU64::new(
                self.metrics.connection_reuse_count.load(Ordering::Relaxed),
            ),
            connection_timeout_count: AtomicU64::new(
                self.metrics
                    .connection_timeout_count
                    .load(Ordering::Relaxed),
            ),
            connection_lifetime: AtomicU64::new(
                self.metrics.connection_lifetime.load(Ordering::Relaxed),
            ),
        }
    }

    pub fn pool_stats(&self) -> (u32, u32) {
        (self.pool.size(), self.pool.num_idle() as u32)
    }

    /// Health check with detailed diagnostics
    pub async fn health_check(&self) -> Result<EventStoreHealth> {
        let (total_connections, idle_connections) = self.pool_stats();
        let start_time = Instant::now();

        // Check database connection and get detailed metrics
        let db_status = match sqlx::query(
            r#"
            SELECT 
                count(*) as active_connections,
                sum(case when state = 'idle' then 1 else 0 end) as idle_connections,
                sum(case when state = 'active' then 1 else 0 end) as active_queries,
                sum(case when state = 'idle in transaction' then 1 else 0 end) as idle_transactions
            FROM pg_stat_activity 
            WHERE datname = current_database()
            "#,
        )
        .fetch_one(&self.pool)
        .await
        {
            Ok(row) => {
                let active_connections: i64 = row.get("active_connections");
                let idle_connections: i64 = row.get("idle_connections");
                let active_queries: i64 = row.get("active_queries");
                let idle_transactions: i64 = row.get("idle_transactions");

                format!(
                    "healthy (active: {}, idle: {}, queries: {}, idle_tx: {})",
                    active_connections, idle_connections, active_queries, idle_transactions
                )
            }
            Err(e) => format!("unhealthy: {}", e),
        };

        let cache_size = {
            let cache = self.snapshot_cache.read().await;
            cache.len()
        };

        // Calculate batch queue metrics
        let batch_queue_metrics = {
            let available_permits = self.batch_semaphore.available_permits();
            let total_permits = self.config.max_batch_queue_size;
            let used_permits = total_permits - available_permits;
            let queue_utilization = (used_permits as f64 / total_permits as f64) * 100.0;

            format!(
                "Queue: {}/{} permits used ({:.1}% utilization)",
                used_permits, total_permits, queue_utilization
            )
        };

        Ok(EventStoreHealth {
            database_status: db_status,
            total_connections,
            idle_connections,
            cache_size,
            batch_queue_permits: self.batch_semaphore.available_permits(),
            metrics: self.get_metrics(),
            batch_queue_metrics,
            health_check_duration: start_time.elapsed(),
        })
    }

    // Add this method to monitor connection pool
    async fn monitor_connection_pool(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let pool_stats = self.pool_stats();
            let metrics = self.get_metrics();

            info!(
                "Connection Pool Stats - Total: {}, Idle: {}, Active: {}, Acquire Time: {}ms, Errors: {}, Reuse: {}, Timeouts: {}, Avg Lifetime: {}s",
                pool_stats.0,
                pool_stats.1,
                pool_stats.0 - pool_stats.1,
                metrics.connection_acquire_time.load(Ordering::Relaxed),
                metrics.connection_acquire_errors.load(Ordering::Relaxed),
                metrics.connection_reuse_count.load(Ordering::Relaxed),
                metrics.connection_timeout_count.load(Ordering::Relaxed),
                metrics.connection_lifetime.load(Ordering::Relaxed) / 1000
            );
        }
    }

    pub async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>> {
        let mut account = Account::default();
        let events = self.get_events(account_id, None).await?;

        for event in events {
            let account_event: AccountEvent = serde_json::from_value(event.event_data)?;
            account.apply_event(&account_event);
        }

        Ok(Some(account))
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<Account>> {
        let account_ids: Vec<Uuid> = sqlx::query!(
            r#"
            SELECT DISTINCT aggregate_id as account_id
            FROM events
            ORDER BY aggregate_id
            "#
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.account_id)
        .collect();

        let mut accounts = Vec::new();
        for account_id in account_ids {
            if let Some(account) = self.get_account(account_id).await? {
                accounts.push(account);
            }
        }

        Ok(accounts)
    }

    pub async fn register_validator(
        &self,
        event_type: String,
        validator: Box<dyn EventValidator + Send + Sync>,
    ) {
        self.event_validators.insert(event_type, validator);
    }

    pub async fn register_handler(
        &self,
        event_type: String,
        handler: Box<dyn EventHandler + Send + Sync>,
    ) {
        self.event_handlers
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(handler);
    }

    async fn validate_event(event: &Event) -> EventValidationResult {
        let mut result = EventValidationResult {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        };

        // Basic validation
        if event.event_type.is_empty() {
            result.is_valid = false;
            result.errors.push("Event type cannot be empty".to_string());
        }

        if event.version <= 0 {
            result.is_valid = false;
            result
                .errors
                .push("Event version must be positive".to_string());
        }

        result
    }

    async fn handle_event(
        event: &Event,
        event_handlers: &DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>,
    ) -> Result<()> {
        if let Some(handlers) = event_handlers.get(&event.event_type) {
            for handler in handlers.iter() {
                handler.handle(event).await?;
            }
        }
        Ok(())
    }

    pub async fn get_current_version(&self, aggregate_id: Uuid) -> Result<i64> {
        let version = sqlx::query!(
            r#"
            SELECT COALESCE(MAX(version), 0) as version
            FROM events
            WHERE aggregate_id = $1
            "#,
            aggregate_id
        )
        .fetch_one(&self.pool)
        .await?
        .version
        .unwrap_or(0);

        self.version_cache.insert(aggregate_id, version);
        Ok(version)
    }

    pub async fn update_version(&self, aggregate_id: Uuid, version: i64) {
        self.version_cache.insert(aggregate_id, version);
    }
}

/// Health check response
#[derive(Debug, serde::Serialize)]
pub struct EventStoreHealth {
    pub database_status: String,
    pub total_connections: u32,
    pub idle_connections: u32,
    pub cache_size: usize,
    pub batch_queue_permits: usize,
    pub metrics: EventStoreMetrics,
    pub batch_queue_metrics: String,
    pub health_check_duration: Duration,
}

// Helper structs
#[derive(Debug)]
struct EventData {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    event_data: Value,
    version: i64,
    timestamp: DateTime<Utc>,
}

struct EventRow {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    event_data: Value,
    version: i64,
    timestamp: DateTime<Utc>,
}

/// Enhanced configuration struct for EventStore
#[derive(Debug, Clone)]
pub struct EventStoreConfig {
    pub database_url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,

    // Batching configuration
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_batch_queue_size: usize,
    pub batch_processor_count: usize,
    // Snapshot configuration
    pub snapshot_threshold: usize,
    pub snapshot_interval_secs: u64,
    pub snapshot_cache_ttl_secs: u64,
    pub max_snapshots_per_run: usize,
}

impl Default for EventStoreConfig {
    fn default() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
            }),
            // Optimized connection pool settings
            max_connections: 20, // Reduced from 100 to prevent connection overload
            min_connections: 5,  // Reduced from 20 to maintain a smaller pool
            acquire_timeout_secs: 5, // Reduced from 30 to fail fast
            idle_timeout_secs: 300, // Reduced from 600 to recycle connections faster
            max_lifetime_secs: 900, // Reduced from 1800 to prevent stale connections

            // Optimized batching settings
            batch_size: 1000,     // Reduced from 5000 for better responsiveness
            batch_timeout_ms: 10, // Increased from 5 for better batching
            max_batch_queue_size: 10000, // Reduced from 50000 to prevent memory issues
            batch_processor_count: 4, // Reduced from 8 to prevent CPU overload
            snapshot_threshold: 500, // Reduced from 1000 for more frequent snapshots
            snapshot_interval_secs: 300,
            snapshot_cache_ttl_secs: 3600,
            max_snapshots_per_run: 100, // Reduced from 500 to prevent overload
        }
    }
}

impl EventStoreConfig {
    /// Returns a configuration suitable for in-memory testing.
    /// This uses an in-memory SQLite database URL and minimal settings.
    pub fn default_in_memory_for_tests() -> Result<Self, anyhow::Error> {
        Ok(Self {
            database_url: "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string(),
            max_connections: 5,
            min_connections: 1,
            acquire_timeout_secs: 5,
            idle_timeout_secs: 60,
            max_lifetime_secs: 300,
            batch_size: 100,
            batch_timeout_ms: 10,
            max_batch_queue_size: 1000,
            batch_processor_count: 1,
            snapshot_threshold: 10,
            snapshot_interval_secs: 60,
            snapshot_cache_ttl_secs: 300,
            max_snapshots_per_run: 10,
        })
    }
}

impl Default for EventMetadata {
    fn default() -> Self {
        Self {
            correlation_id: None,
            causation_id: None,
            user_id: None,
            source: "system".to_string(),
            schema_version: "1.0".to_string(),
            tags: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay: std::time::Duration,
    pub max_delay: std::time::Duration,
    pub backoff_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: std::time::Duration::from_millis(100),
            max_delay: std::time::Duration::from_secs(5),
            backoff_factor: 2.0,
        }
    }
}

impl Default for EventStore {
    fn default() -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy("postgresql://postgres:Francisco1@localhost:5432/banking_es")
            .expect("Failed to connect to database");
        EventStore::new(pool)
    }
}
