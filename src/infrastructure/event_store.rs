use crate::domain::{AccountEvent, Event};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
    batch_sender: mpsc::UnboundedSender<BatchedEvent>,
    snapshot_cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
    config: EventStoreConfig,
    // Add semaphore for backpressure control
    batch_semaphore: Arc<Semaphore>,
    metrics: Arc<EventStoreMetrics>,
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
    // Add TTL for cache invalidation
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

// Add a new struct for retry events
#[derive(Debug, Clone)]
struct RetryEvent {
    aggregate_id: Uuid,
    events: Vec<AccountEvent>,
    expected_version: i64,
    created_at: Instant,
    priority: EventPriority,
}

// Add this struct for retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_factor: 2.0,
        }
    }
}

impl EventStore {
    /// Create EventStore with an existing PgPool
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_config_and_pool(pool, EventStoreConfig::default())
    }

    pub fn new_with_config_and_pool(pool: PgPool, config: EventStoreConfig) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        let shared_receiver = Arc::new(Mutex::new(batch_receiver));
        let snapshot_cache = Arc::new(RwLock::new(HashMap::new()));
        let batch_semaphore = Arc::new(Semaphore::new(config.max_batch_queue_size));
        let metrics = Arc::new(EventStoreMetrics::default());

        let store = Self {
            pool: pool.clone(),
            batch_sender,
            snapshot_cache: snapshot_cache.clone(),
            config: config.clone(),
            batch_semaphore: batch_semaphore.clone(),
            metrics: metrics.clone(),
        };

        // Start multiple batch processors for better throughput
        for i in 0..config.batch_processor_count {
            tokio::spawn(Self::batch_processor(
                pool.clone(),
                shared_receiver.clone(),
                config.clone(),
                batch_semaphore.clone(),
                metrics.clone(),
                i,
            ));
        }

        // Start snapshot worker with lower priority
        tokio::spawn(Self::snapshot_worker(
            pool.clone(),
            snapshot_cache.clone(),
            config.clone(),
        ));

        // Start cache cleanup worker
        tokio::spawn(Self::cache_cleanup_worker(snapshot_cache));

        // Start metrics reporter
        tokio::spawn(Self::metrics_reporter(metrics.clone()));

        store
    }

    /// Create EventStore with a specific pool size
    pub async fn new_with_config(config: EventStoreConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            // Enable prepared statement caching
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET SESSION synchronous_commit = 'off'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET SESSION work_mem = '64MB'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET SESSION maintenance_work_mem = '256MB'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET SESSION effective_cache_size = '4GB'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET SESSION random_page_cost = 1.1")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET SESSION effective_io_concurrency = 200")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SELECT 1").execute(&mut *conn).await?;

                    Ok(())
                })
            })
            .connect(&config.database_url)
            .await
            .context("Failed to connect to database")?;

        let store = Self::new_with_config_and_pool(pool, config);

        // Start connection monitoring
        let store_clone = store.clone();
        tokio::spawn(async move {
            store_clone.monitor_connection_pool().await;
        });

        Ok(store)
    }

    // Enhanced event saving with priority support
    pub async fn save_events_with_priority(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
        priority: EventPriority,
    ) -> Result<()> {
        let _permit = self
            .batch_semaphore
            .acquire()
            .await
            .context("Failed to acquire batch semaphore")?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let batched_event = BatchedEvent {
            aggregate_id,
            events,
            expected_version,
            response_tx,
            created_at: Instant::now(),
            priority,
        };

        self.batch_sender
            .send(batched_event)
            .context("Failed to send event to batch processor")?;

        response_rx
            .await
            .context("Failed to receive response from batch processor")?
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
        let config = RetryConfig::default();
        let events = events.clone(); // Clone events before moving into the closure

        self.with_retry(
            move || {
                let events = events.clone(); // Clone again inside the closure
                async move {
                    self.save_events_with_priority(
                        aggregate_id,
                        events,
                        expected_version,
                        EventPriority::Normal,
                    )
                    .await
                }
            },
            &config,
        )
        .await
    }

    // Multi-threaded batch processor with priority queuing
    async fn batch_processor(
        pool: PgPool,
        receiver: Arc<Mutex<mpsc::UnboundedReceiver<BatchedEvent>>>,
        config: EventStoreConfig,
        semaphore: Arc<Semaphore>,
        metrics: Arc<EventStoreMetrics>,
        worker_id: usize,
    ) {
        let batch_size = config.batch_size;
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);
        let mut retry_queue: Vec<RetryEvent> = Vec::new();
        let mut retry_interval = tokio::time::interval(Duration::from_secs(1));

        let mut priority_batches: HashMap<EventPriority, Vec<BatchedEvent>> = HashMap::new();
        let mut last_flush = Instant::now();

        debug!("Starting batch processor worker {}", worker_id);

        loop {
            tokio::select! {
                // Process retries
                _ = retry_interval.tick() => {
                    if !retry_queue.is_empty() {
                        let retry_batch = std::mem::replace(&mut retry_queue, Vec::new());
                        for retry_event in retry_batch {
                            let (response_tx, _) = tokio::sync::oneshot::channel();
                            let batched_event = BatchedEvent {
                                aggregate_id: retry_event.aggregate_id,
                                events: retry_event.events,
                                expected_version: retry_event.expected_version,
                                response_tx,
                                created_at: retry_event.created_at,
                                priority: retry_event.priority,
                            };
                            priority_batches
                                .entry(batched_event.priority)
                                .or_insert_with(Vec::new)
                                .push(batched_event);
                        }
                    }
                }
                // Process new events
                event = async {
                    let mut rx = receiver.lock().await;
                    match rx.try_recv() {
                        Ok(event) => Some(event),
                        Err(mpsc::error::TryRecvError::Empty) => {
                            if !priority_batches.is_empty() && last_flush.elapsed() >= batch_timeout {
                                None
                            } else {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                                None
                            }
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            debug!("Worker {} shutting down - channel closed", worker_id);
                            None
                        }
                    }
                } => {
                    if let Some(event) = event {
                        priority_batches
                            .entry(event.priority)
                            .or_insert_with(Vec::new)
                            .push(event);
                    }
                }
            }

            let total_events: usize = priority_batches.values().map(|v| v.len()).sum();

            // Enhanced flush conditions
            let should_flush = total_events >= batch_size
                || last_flush.elapsed() >= batch_timeout
                || priority_batches.contains_key(&EventPriority::Critical)
                || priority_batches
                    .values()
                    .flatten()
                    .any(|e| e.created_at.elapsed() >= batch_timeout * 2);

            if should_flush && !priority_batches.is_empty() {
                let mut priorities: Vec<_> = priority_batches.keys().copied().collect();
                priorities.sort_by(|a, b| b.cmp(a));

                for priority in priorities {
                    if let Some(batch) = priority_batches.remove(&priority) {
                        if !batch.is_empty() {
                            let batch_count = batch.len();
                            // Create retry events before processing the batch
                            let retry_events: Vec<_> = batch
                                .iter()
                                .map(|b| RetryEvent {
                                    aggregate_id: b.aggregate_id,
                                    events: b.events.clone(),
                                    expected_version: b.expected_version,
                                    created_at: b.created_at,
                                    priority: b.priority,
                                })
                                .collect();

                            match Self::flush_batch(&pool, batch).await {
                                Ok(()) => {
                                    metrics
                                        .events_processed
                                        .fetch_add(batch_count as u64, Ordering::Relaxed);
                                    metrics.batch_count.fetch_add(1, Ordering::Relaxed);
                                    debug!(
                                        "Worker {} processed batch of {} events with priority {:?}",
                                        worker_id, batch_count, priority
                                    );
                                }
                                Err(e) => {
                                    metrics
                                        .events_failed
                                        .fetch_add(batch_count as u64, Ordering::Relaxed);
                                    error!("Worker {} failed to process batch: {}", worker_id, e);

                                    // Add all retry events to the queue
                                    retry_queue.extend(retry_events);
                                }
                            }
                            semaphore.add_permits(batch_count);
                        }
                    }
                }

                last_flush = Instant::now();
            }
        }
    }
    // Optimized batch flushing with prepared statements and COPY
    async fn flush_batch(pool: &PgPool, mut batch: Vec<BatchedEvent>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let start_time = Instant::now();

        // Sort batch by aggregate_id to improve locality and reduce locks
        batch.sort_by_key(|e| e.aggregate_id);

        let mut tx = pool.begin().await.context("Failed to begin transaction")?;

        let mut all_event_data = Vec::new();
        let mut responses = Vec::new();
        let mut failed_responses = Vec::new();

        // Pre-allocate capacity based on estimated event count
        let estimated_events: usize = batch.iter().map(|b| b.events.len()).sum();
        all_event_data.reserve(estimated_events);

        for batched_event in batch {
            match Self::prepare_events_for_insert_optimized(&batched_event) {
                Ok(event_data) => {
                    all_event_data.extend(event_data);
                    responses.push(batched_event.response_tx);
                }
                Err(e) => {
                    failed_responses.push((batched_event.response_tx, e));
                }
            }
        }

        // Send errors for failed preparations
        for (response_tx, error) in failed_responses {
            let _ = response_tx.send(Err(error));
        }

        if all_event_data.is_empty() {
            return Ok(());
        }

        // Use COPY for maximum throughput on large batches
        let result = if all_event_data.len() > 100 {
            Self::bulk_copy_events(&mut tx, all_event_data).await
        } else {
            Self::bulk_insert_events_optimized(&mut tx, all_event_data).await
        };

        match result {
            Ok(_) => {
                tx.commit().await.context("Failed to commit transaction")?;
                for response_tx in responses {
                    let _ = response_tx.send(Ok(()));
                }
                debug!("Batch flush completed in {:?}", start_time.elapsed());
            }
            Err(e) => {
                let _ = tx.rollback().await;
                let error_msg = e.to_string();
                for response_tx in responses {
                    let _ = response_tx.send(Err(anyhow::anyhow!(error_msg.clone())));
                }
                return Err(e);
            }
        }

        Ok(())
    }
    // Pre-calculate event data to reduce serialization overhead
    fn prepare_events_for_insert_optimized(batched_event: &BatchedEvent) -> Result<Vec<EventData>> {
        let mut event_data = Vec::with_capacity(batched_event.events.len());
        let base_time = Utc::now();

        for (i, event) in batched_event.events.iter().enumerate() {
            let event_id = Uuid::new_v4();
            let version = batched_event.expected_version + i as i64 + 1;

            // Pre-serialize to avoid repeated serialization
            let event_json =
                serde_json::to_value(event).context("Failed to serialize event to JSON")?;

            event_data.push(EventData {
                id: event_id,
                aggregate_id: batched_event.aggregate_id,
                event_type: event.event_type().to_string(),
                event_data: event_json,
                version,
                timestamp: base_time,
            });
        }

        Ok(event_data)
    }

    // Use PostgreSQL COPY for maximum insert performance
    async fn bulk_copy_events(
        tx: &mut Transaction<'_, Postgres>,
        event_data: Vec<EventData>,
    ) -> Result<()> {
        // Group by aggregate for version checking
        let mut aggregates: HashMap<Uuid, (i64, i64)> = HashMap::new();
        for event in &event_data {
            let entry = aggregates
                .entry(event.aggregate_id)
                .or_insert((i64::MAX, i64::MIN));
            entry.0 = entry.0.min(event.version);
            entry.1 = entry.1.max(event.version);
        }

        // Optimized version conflict check with a single query
        if !aggregates.is_empty() {
            let aggregate_ids: Vec<Uuid> = aggregates.keys().copied().collect();
            let current_versions = sqlx::query(
                r#"
                WITH latest_versions AS (
                    SELECT DISTINCT ON (aggregate_id) 
                        aggregate_id, 
                        version
                    FROM events 
                    WHERE aggregate_id = ANY($1)
                    ORDER BY aggregate_id, version DESC
                )
                SELECT aggregate_id, version FROM latest_versions
                "#,
            )
            .bind(&aggregate_ids)
            .fetch_all(&mut **tx)
            .await
            .context("Failed to check current versions")?;

            for row in current_versions {
                let aggregate_id: Uuid = row.get("aggregate_id");
                let current_version: i64 = row.get("version");

                if let Some((min_version, _)) = aggregates.get(&aggregate_id) {
                    if current_version >= *min_version {
                        return Err(anyhow::anyhow!(
                            "Version conflict for aggregate {}: expected {}, found {}",
                            aggregate_id,
                            min_version - 1,
                            current_version
                        ));
                    }
                }
            }
        }

        // Use COPY with optimized buffer size
        let mut copy_writer = tx
            .copy_in_raw(
                "COPY events (id, aggregate_id, event_type, event_data, version, timestamp) FROM STDIN WITH (FORMAT CSV, QUOTE '\"', ESCAPE '\"', BUFFER_SIZE 1048576)"
            )
            .await
            .context("Failed to start COPY operation")?;

        // Pre-allocate buffer for better performance
        let mut buffer = Vec::with_capacity(1024 * 1024); // 1MB buffer

        for event in event_data {
            // Optimize CSV formatting
            let event_data_str = event.event_data.to_string();
            let escaped_data = event_data_str.replace("\"", "\"\"");

            buffer.extend_from_slice(
                format!(
                    "{},{},{},\"{}\",{},{}\n",
                    event.id,
                    event.aggregate_id,
                    event.event_type,
                    escaped_data,
                    event.version,
                    event.timestamp.format("%Y-%m-%d %H:%M:%S%.6f UTC")
                )
                .as_bytes(),
            );

            // Flush buffer when it gets large
            if buffer.len() >= 1024 * 1024 {
                copy_writer
                    .send(buffer.as_slice())
                    .await
                    .context("Failed to send data to COPY")?;
                buffer.clear();
            }
        }

        // Send remaining data
        if !buffer.is_empty() {
            copy_writer
                .send(buffer.as_slice())
                .await
                .context("Failed to send data to COPY")?;
        }

        copy_writer
            .finish()
            .await
            .context("Failed to finish COPY operation")?;

        Ok(())
    }

    // Optimized bulk insert for smaller batches
    async fn bulk_insert_events_optimized(
        tx: &mut Transaction<'_, Postgres>,
        event_data: Vec<EventData>,
    ) -> Result<()> {
        if event_data.is_empty() {
            return Ok(());
        }

        // Prepare arrays for bulk insert
        let ids: Vec<Uuid> = event_data.iter().map(|e| e.id).collect();
        let aggregate_ids: Vec<Uuid> = event_data.iter().map(|e| e.aggregate_id).collect();
        let event_types: Vec<String> = event_data.iter().map(|e| e.event_type.clone()).collect();
        let event_jsons: Vec<Value> = event_data.iter().map(|e| e.event_data.clone()).collect();
        let versions: Vec<i64> = event_data.iter().map(|e| e.version).collect();
        let timestamps: Vec<DateTime<Utc>> = event_data.iter().map(|e| e.timestamp).collect();

        sqlx::query!(
        r#"
        INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp)
        SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::jsonb[], $5::bigint[], $6::timestamptz[])
        "#,
        &ids,
        &aggregate_ids,
        &event_types,
        &event_jsons as &[Value],
        &versions,
        &timestamps
    )
    .execute(&mut **tx)
    .await
    .context("Failed to insert events")?;

        Ok(())
    }

    // Modify get_events to add proper type annotations
    pub async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>> {
        let config = RetryConfig::default();
        let store = self.clone();

        self.with_retry(
            move || {
                let store = store.clone();
                async move {
                    // Check cache first
                    let snapshot = {
                        let cache = store.snapshot_cache.read().await;
                        let cached = cache
                            .get(&aggregate_id)
                            .filter(|s| s.created_at.elapsed() < s.ttl)
                            .cloned();

                        if cached.is_some() {
                            store.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                        } else {
                            store.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                        }

                        cached
                    };

                    let (start_version, mut events) = if let Some(snapshot) = &snapshot {
                        if let Some(from_ver) = from_version {
                            if from_ver <= snapshot.version {
                                (
                                    Some(snapshot.version),
                                    vec![store.create_snapshot_event(snapshot)],
                                )
                            } else {
                                (from_version, vec![])
                            }
                        } else {
                            (
                                Some(snapshot.version),
                                vec![store.create_snapshot_event(snapshot)],
                            )
                        }
                    } else {
                        (from_version, vec![])
                    };

                    // Fetch events with prepared statement
                    let db_events = if let Some(version) = start_version {
                        sqlx::query_as!(
                            EventRow,
                            r#"
                            SELECT id, aggregate_id, event_type, event_data, version, timestamp
                            FROM events
                            WHERE aggregate_id = $1 AND version > $2
                            ORDER BY version
                            "#,
                            aggregate_id,
                            version
                        )
                        .fetch_all(&store.pool)
                        .await
                        .context("Failed to fetch events from database")?
                    } else {
                        sqlx::query_as!(
                            EventRow,
                            r#"
                            SELECT id, aggregate_id, event_type, event_data, version, timestamp
                            FROM events
                            WHERE aggregate_id = $1
                            ORDER BY version
                            "#,
                            aggregate_id
                        )
                        .fetch_all(&store.pool)
                        .await
                        .context("Failed to fetch events from database")?
                    };

                    events.extend(db_events.into_iter().map(|row| Event {
                        id: row.id,
                        aggregate_id: row.aggregate_id,
                        event_type: row.event_type,
                        event_data: row.event_data,
                        version: row.version,
                        timestamp: row.timestamp,
                    }));

                    Ok::<Vec<Event>, anyhow::Error>(events)
                }
            },
            &config,
        )
        .await
    }

    fn create_snapshot_event(&self, snapshot: &CachedSnapshot) -> Event {
        Event {
            id: Uuid::new_v4(),
            aggregate_id: snapshot.aggregate_id,
            event_type: "Snapshot".to_string(),
            event_data: snapshot.data.clone(),
            version: snapshot.version,
            timestamp: Utc::now(),
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
                "postgresql://postgres:password@localhost:5432/banking_es".to_string()
            }),
            // Optimized connection pool settings
            max_connections: 100, // Increased for better concurrency
            min_connections: 20,  // Increased to maintain a healthy pool
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1800,

            // Optimized batching settings
            batch_size: 5000,
            batch_timeout_ms: 5,
            max_batch_queue_size: 50000,
            batch_processor_count: 8,
            snapshot_threshold: 1000,
            snapshot_interval_secs: 300,
            snapshot_cache_ttl_secs: 3600,
            max_snapshots_per_run: 500,
        }
    }
}

impl EventStoreConfig {
    /// Returns a configuration suitable for in-memory testing.
    /// This uses an in-memory SQLite database URL and minimal settings.
    pub fn default_in_memory_for_tests() -> Result<Self, anyhow::Error> {
        Ok(Self {
            database_url: "postgresql://postgres:password@localhost:5432/banking_es_test"
                .to_string(),
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
