use crate::domain::{AccountEvent, Event};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Semaphore};
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
}

#[derive(Debug)]
struct BatchedEvent {
    aggregate_id: Uuid,
    events: Vec<AccountEvent>,
    expected_version: i64,
    response_tx: tokio::sync::oneshot::Sender<Result<()>>,
    // Add timestamp for tracking batch age
    created_at: Instant,
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

        let store = Self {
            pool: pool.clone(),
            batch_sender,
            snapshot_cache: snapshot_cache.clone(),
            config: config.clone(),
            batch_semaphore: batch_semaphore.clone(),
        };

        // Start background batch processor
        tokio::spawn(Self::batch_processor(
            pool.clone(),
            batch_receiver,
            config.clone(),
            batch_semaphore,
        ));

        // Start periodic snapshot creation
        tokio::spawn(Self::snapshot_worker(
            pool.clone(),
            snapshot_cache.clone(),
            config.clone(),
        ));

        // Start cache cleanup worker
        tokio::spawn(Self::cache_cleanup_worker(snapshot_cache));

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
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            .connect(&config.database_url)
            .await
            .context("Failed to connect to database")?;

        Ok(Self::new_with_config_and_pool(pool, config))
    }

    // Enhanced event saving with backpressure control
    pub async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<()> {
        // Apply backpressure if queue is full
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
        };

        self.batch_sender
            .send(batched_event)
            .context("Failed to send event to batch processor")?;

        response_rx
            .await
            .context("Failed to receive response from batch processor")?
    }

    // Enhanced batch processor with better error handling and metrics
    async fn batch_processor(
        pool: PgPool,
        mut receiver: mpsc::UnboundedReceiver<BatchedEvent>,
        config: EventStoreConfig,
        semaphore: Arc<Semaphore>,
    ) {
        let batch_size = config.batch_size;
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);

        let mut batch = Vec::with_capacity(batch_size);
        let mut last_flush = Instant::now();
        let mut total_processed = 0u64;
        let mut total_errors = 0u64;

        while let Some(event) = receiver.recv().await {
            batch.push(event);

            // Flush batch if size limit reached, timeout exceeded, or oldest event is too old
            let should_flush = batch.len() >= batch_size
                || last_flush.elapsed() >= batch_timeout
                || batch
                    .first()
                    .map_or(false, |e| e.created_at.elapsed() >= batch_timeout);

            if should_flush {
                let batch_count = batch.len();
                match Self::flush_batch(&pool, &mut batch).await {
                    Ok(()) => {
                        total_processed += batch_count as u64;
                        debug!("Successfully processed batch of {} events", batch_count);
                    }
                    Err(e) => {
                        total_errors += batch_count as u64;
                        error!("Failed to process batch of {} events: {}", batch_count, e);
                    }
                }

                // Release permits for processed events
                semaphore.add_permits(batch_count);
                last_flush = Instant::now();

                // Log metrics periodically
                if total_processed % 10000 == 0 {
                    info!(
                        "Event store metrics - Processed: {}, Errors: {}, Success rate: {:.2}%",
                        total_processed,
                        total_errors,
                        if total_processed > 0 {
                            (total_processed - total_errors) as f64 / total_processed as f64 * 100.0
                        } else {
                            0.0
                        }
                    );
                }
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            let batch_count = batch.len();
            let _ = Self::flush_batch(&pool, &mut batch).await;
            semaphore.add_permits(batch_count);
        }
    }

    async fn flush_batch(pool: &PgPool, batch: &mut Vec<BatchedEvent>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut tx = pool.begin().await.context("Failed to begin transaction")?;

        // Prepare bulk insert data with better error handling
        let mut all_event_data = Vec::new();
        let mut responses = Vec::new();
        let mut failed_responses = Vec::new();

        for batched_event in batch.drain(..) {
            match Self::prepare_events_for_insert(&batched_event) {
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

        // Perform bulk insert with optimistic concurrency control
        match Self::bulk_insert_events_with_version_check(&mut tx, all_event_data).await {
            Ok(_) => {
                tx.commit().await.context("Failed to commit transaction")?;
                for response_tx in responses {
                    let _ = response_tx.send(Ok(()));
                }
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

    fn prepare_events_for_insert(
        batched_event: &BatchedEvent,
    ) -> Result<Vec<(Uuid, Uuid, String, Value, i64, DateTime<Utc>)>> {
        let mut event_data = Vec::new();

        for (i, event) in batched_event.events.iter().enumerate() {
            let event_id = Uuid::new_v4();
            let version = batched_event.expected_version + i as i64 + 1;
            let event_json =
                serde_json::to_value(event).context("Failed to serialize event to JSON")?;

            event_data.push((
                event_id,
                batched_event.aggregate_id,
                event.event_type().to_string(),
                event_json,
                version,
                Utc::now(),
            ));
        }

        Ok(event_data)
    }

    async fn bulk_insert_events_with_version_check(
        tx: &mut Transaction<'_, Postgres>,
        event_data: Vec<(Uuid, Uuid, String, Value, i64, DateTime<Utc>)>,
    ) -> Result<()> {
        if event_data.is_empty() {
            return Ok(());
        }

        // Group events by aggregate_id for version checking
        let mut aggregates: HashMap<Uuid, (i64, i64)> = HashMap::new();
        for (_, aggregate_id, _, _, version, _) in &event_data {
            let entry = aggregates
                .entry(*aggregate_id)
                .or_insert((i64::MAX, i64::MIN));
            entry.0 = entry.0.min(*version);
            entry.1 = entry.1.max(*version);
        }

        // Check version conflicts
        for (aggregate_id, (min_version, _)) in aggregates {
            let current_version: Option<i64> =
                sqlx::query_scalar("SELECT MAX(version) FROM events WHERE aggregate_id = $1")
                    .bind(aggregate_id)
                    .fetch_one(&mut **tx)
                    .await
                    .context("Failed to check current version")?;

            let current_version = current_version.unwrap_or(0);

            if current_version >= min_version {
                return Err(anyhow::anyhow!(
                    "Version conflict for aggregate {}: expected {}, found {}",
                    aggregate_id,
                    min_version - 1,
                    current_version
                ));
            }
        }

        // Perform bulk insert
        let ids: Vec<Uuid> = event_data.iter().map(|e| e.0).collect();
        let aggregate_ids: Vec<Uuid> = event_data.iter().map(|e| e.1).collect();
        let event_types: Vec<String> = event_data.iter().map(|e| e.2.clone()).collect();
        let event_jsons: Vec<Value> = event_data.iter().map(|e| e.3.clone()).collect();
        let versions: Vec<i64> = event_data.iter().map(|e| e.4).collect();
        let timestamps: Vec<DateTime<Utc>> = event_data.iter().map(|e| e.5).collect();

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

    // Enhanced event retrieval with better caching
    pub async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>> {
        // Check for valid snapshot first
        let snapshot = {
            let cache = self.snapshot_cache.read().await;
            cache
                .get(&aggregate_id)
                .filter(|s| s.created_at.elapsed() < s.ttl)
                .cloned()
        };

        let (start_version, mut events) = if let Some(snapshot) = &snapshot {
            if let Some(from_ver) = from_version {
                if from_ver <= snapshot.version {
                    // Use snapshot as starting point
                    (
                        Some(snapshot.version),
                        vec![self.create_snapshot_event(snapshot)],
                    )
                } else {
                    // Skip snapshot, start from requested version
                    (from_version, vec![])
                }
            } else {
                // Use snapshot as starting point
                (
                    Some(snapshot.version),
                    vec![self.create_snapshot_event(snapshot)],
                )
            }
        } else {
            (from_version, vec![])
        };

        let db_events = if let Some(version) = start_version {
            let rows = sqlx::query(
                r#"
                SELECT id, aggregate_id, event_type, event_data, version, timestamp
                FROM events
                WHERE aggregate_id = $1 AND version > $2
                ORDER BY version
                "#,
            )
            .bind(aggregate_id)
            .bind(version)
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch events from database")?;

            self.rows_to_events(rows)?
        } else {
            let rows = sqlx::query(
                r#"
                SELECT id, aggregate_id, event_type, event_data, version, timestamp
                FROM events
                WHERE aggregate_id = $1
                ORDER BY version
                "#,
            )
            .bind(aggregate_id)
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch events from database")?;

            self.rows_to_events(rows)?
        };

        events.extend(db_events);
        Ok(events)
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

    // Helper method to convert database rows to Event structs
    fn rows_to_events(&self, rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Event>> {
        Self::rows_to_events_static(rows)
    }

    // Static version for use in static contexts
    fn rows_to_events_static(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Event>> {
        use sqlx::Row;

        let mut events = Vec::new();
        for row in rows {
            let event = Event {
                id: row.try_get("id").context("Failed to get event id")?,
                aggregate_id: row
                    .try_get("aggregate_id")
                    .context("Failed to get aggregate_id")?,
                event_type: row
                    .try_get("event_type")
                    .context("Failed to get event_type")?,
                event_data: row
                    .try_get("event_data")
                    .context("Failed to get event_data")?,
                version: row.try_get("version").context("Failed to get version")?,
                timestamp: row
                    .try_get("timestamp")
                    .context("Failed to get timestamp")?,
            };
            events.push(event);
        }
        Ok(events)
    }

    // Enhanced snapshot worker with configurable thresholds
    async fn snapshot_worker(
        pool: PgPool,
        cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        config: EventStoreConfig,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.snapshot_interval_secs));

        loop {
            interval.tick().await;

            let candidates = sqlx::query(
                r#"
                SELECT aggregate_id, COUNT(*) as event_count, MAX(version) as max_version
                FROM events
                WHERE aggregate_id NOT IN (
                    SELECT aggregate_id FROM snapshots
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                )
                GROUP BY aggregate_id
                HAVING COUNT(*) > $1
                ORDER BY COUNT(*) DESC
                LIMIT $2
                "#,
            )
            .bind(config.snapshot_threshold as i64)
            .bind(config.max_snapshots_per_run as i64)
            .fetch_all(&pool)
            .await;

            match candidates {
                Ok(candidates) => {
                    for candidate in candidates {
                        let aggregate_id: Option<Uuid> = candidate.try_get("aggregate_id").ok();
                        if let Some(aggregate_id) = aggregate_id {
                            if let Err(e) =
                                Self::create_snapshot(&pool, &cache, aggregate_id, &config).await
                            {
                                warn!("Failed to create snapshot for {}: {}", aggregate_id, e);
                            }
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
        // Load all events for the aggregate
        let rows = sqlx::query(
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            WHERE aggregate_id = $1
            ORDER BY version
            "#,
        )
        .bind(aggregate_id)
        .fetch_all(pool)
        .await
        .context("Failed to fetch events for snapshot")?;

        let events = Self::rows_to_events_static(rows)?;

        if events.is_empty() {
            return Ok(());
        }

        // Reconstruct aggregate state
        let mut account = crate::domain::Account::default();
        account.id = aggregate_id;

        for event in &events {
            let account_event: AccountEvent = serde_json::from_value(event.event_data.clone())
                .context("Failed to deserialize event")?;
            account.apply_event(&account_event);
        }

        let snapshot_data =
            serde_json::to_value(&account).context("Failed to serialize account snapshot")?;
        let max_version = events.last().unwrap().version;

        // Save snapshot to database
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

        // Update cache
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

    // Cache cleanup worker to remove expired entries
    async fn cache_cleanup_worker(cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>) {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes

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

    /// Get pool statistics for monitoring
    pub fn pool_stats(&self) -> (u32, u32) {
        (self.pool.size(), self.pool.num_idle() as u32)
    }

    /// Get event store health status
    pub async fn health_check(&self) -> Result<EventStoreHealth> {
        let (total_connections, idle_connections) = self.pool_stats();

        // Test database connectivity
        let db_status = match sqlx::query("SELECT 1").fetch_one(&self.pool).await {
            Ok(_) => "healthy".to_string(),
            Err(e) => format!("unhealthy: {}", e),
        };

        // Get cache statistics
        let cache_size = {
            let cache = self.snapshot_cache.read().await;
            cache.len()
        };

        Ok(EventStoreHealth {
            database_status: db_status,
            total_connections,
            idle_connections,
            cache_size,
            batch_queue_permits: self.batch_semaphore.available_permits(),
        })
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
            max_connections: 10,
            min_connections: 1,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1800,

            batch_size: 1000,
            batch_timeout_ms: 10,
            max_batch_queue_size: 10000,

            snapshot_threshold: 100,
            snapshot_interval_secs: 60,
            snapshot_cache_ttl_secs: 3600, // 1 hour
            max_snapshots_per_run: 100,
        }
    }
}
