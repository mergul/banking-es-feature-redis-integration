use crate::domain::{Account, AccountEvent};
use anyhow::{Context as AnyhowContext, Result}; // Renamed Context to AnyhowContext
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    postgres::{PgDatabaseError, PgPoolOptions},
    PgPool, Postgres, Row, Transaction,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, OnceCell, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Global connection pool
pub static DB_POOL: OnceCell<Arc<PgPool>> = OnceCell::const_new();

#[derive(Error, Debug)]
pub enum EventStoreError {
    #[error("Optimistic concurrency conflict for aggregate {aggregate_id}: expected version {expected}, but found {actual:?}")]
    OptimisticConcurrencyConflict {
        aggregate_id: Uuid,
        expected: i64,
        actual: Option<i64>,
    },
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Validation error: {0:?}")]
    ValidationError(Vec<String>),
    #[error("Failed to send response for batch event: {0}")]
    ResponseSendError(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl EventStoreError {
    // Helper to convert to anyhow::Error for senders that expect it
    pub fn into_anyhow(self) -> anyhow::Error {
        anyhow::anyhow!(self)
    }
}

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
    response_tx: tokio::sync::oneshot::Sender<Result<(), EventStoreError>>, // Changed to send specific error
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
    pub occ_failures: AtomicU64, // New metric for OCC failures
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
            occ_failures: AtomicU64::new(0),
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
    async fn handle(&self, event: &Event) -> Result<(), EventStoreError>; // Adjusted for consistent error type
}

impl EventStore {
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_config_and_pool(pool, EventStoreConfig::default())
    }

    pub fn new_with_config_and_pool(pool: PgPool, config: EventStoreConfig) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        // ... (rest of the constructor remains similar)
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

        tokio::spawn(Self::batch_processor(
            pool.clone(),
            batch_receiver,
            // config.clone(), // config is not directly used by batch_processor, but passed to flush_batch
            metrics.clone(),
            event_handlers.clone(),
            // worker_id: usize, // worker_id not used in this version of batch_processor
        ));
        // ... (rest of background tasks)
        tokio::spawn(Self::snapshot_worker(
            pool.clone(),
            snapshot_cache.clone(),
            config.clone(),
        ));
        tokio::spawn(Self::cache_cleanup_worker(snapshot_cache));
        let reporter_metrics = metrics.clone();
        tokio::spawn(async move {
            Self::metrics_reporter(reporter_metrics).await;
        });

        store
    }

    pub async fn new_with_pool_size(pool_size: u32) -> Result<Self, EventStoreError> {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|e| EventStoreError::InternalError(format!("DATABASE_URL not set: {}", e)))?;
        Self::new_with_pool_size_and_url(pool_size, &database_url).await
    }

    pub async fn new_with_pool_size_and_url(pool_size: u32, database_url: &str) -> Result<Self, EventStoreError> {
        let mut config = EventStoreConfig::default();
        config.database_url = database_url.to_string();
        config.max_connections = pool_size;
        Self::new_with_config(config).await
    }

    pub async fn new_with_config(config: EventStoreConfig) -> Result<Self, EventStoreError> {
        let pool = DB_POOL
            .get_or_try_init(|| async {
                PgPoolOptions::new()
                    .max_connections(config.max_connections)
                    // ... (rest of pool options)
                    .min_connections(config.min_connections)
                    .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
                    .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
                    .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
                    .after_connect(|conn, _meta| {
                        Box::pin(async move {
                            sqlx::query("SET SESSION synchronous_commit = 'off'").execute(&mut *conn).await?;
                            sqlx::query("SET SESSION work_mem = '32MB'").execute(&mut *conn).await?;
                            sqlx::query("SET SESSION maintenance_work_mem = '128MB'").execute(&mut *conn).await?;
                            sqlx::query("SET SESSION effective_cache_size = '2GB'").execute(&mut *conn).await?;
                            sqlx::query("SET SESSION random_page_cost = 1.1").execute(&mut *conn).await?;
                            sqlx::query("SET SESSION statement_timeout = '5s'").execute(&mut *conn).await?;
                            Ok(())
                        })
                    })
                    .connect(&config.database_url)
                    .await
                    .map_err(|e| EventStoreError::DatabaseError(e.into())) // Convert to EventStoreError
            })
            .await? // Propagate error from get_or_try_init
            .clone(); // Clone Arc<PgPool>

        Ok(Self::new_with_config_and_pool(pool, config))
    }


    pub async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<(), EventStoreError> { // Changed return type
        self.save_events_with_priority(
            aggregate_id,
            events,
            expected_version,
            EventPriority::Normal,
        )
        .await
    }

    pub async fn save_events_with_priority(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
        priority: EventPriority,
    ) -> Result<(), EventStoreError> { // Changed return type
        let _permit = self.batch_semaphore.clone().acquire_owned().await
            .map_err(|_| EventStoreError::InternalError("Failed to acquire batch semaphore permit".to_string()))?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let batched_event = BatchedEvent {
            aggregate_id,
            events,
            expected_version,
            response_tx,
            created_at: Instant::now(),
            priority,
        };

        self.batch_sender.send(batched_event)
            .map_err(|e| EventStoreError::InternalError(format!("Failed to send event to batch processor: {}", e)))?;

        self.metrics.batch_count.fetch_add(1, Ordering::Relaxed);

        // The actual result of the save operation will come through response_rx
        response_rx.await
            .map_err(|e| EventStoreError::ResponseSendError(format!("Failed to receive batch processing response: {}", e)))? // Propagate error from receiving the response
    }


    async fn batch_processor(
        pool: PgPool,
        mut receiver: mpsc::UnboundedReceiver<BatchedEvent>,
        metrics: Arc<EventStoreMetrics>,
        event_handlers: Arc<DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>>,
        // config and semaphore removed as they are not directly used here now
        // worker_id: usize, // Not used
    ) {
        // Note: This batch_processor collects BatchedEvents.
        // The flush_batch_group now handles individual BatchedEvent processing atomically.
        // We might want to group BatchedEvents by aggregate_id here if flush_batch_group
        // were to process a Vec<BatchedEvent> for the *same* aggregate.
        // However, the current design of BatchedEvent implies it's already for one aggregate.
        // So, we can process them one by one, or collect a few and spawn tasks for each.
        // The original code collected a batch and spawned one task for Self::flush_batch.
        // The new Self::flush_batch_group processes one BatchedEvent at a time.

        // This simplified batch_processor will process events as they come.
        // For more complex batching (e.g. by aggregate_id, or true DB batching across aggregates
        // while maintaining OCC per aggregate), this logic would need to be more sophisticated.
        // The current implementation processes one BatchedEvent (for one aggregate) at a time
        // from the channel, ensuring its atomicity.
        while let Some(batched_event) = receiver.recv().await {
            let num_domain_events = batched_event.events.len() as u64; // Count domain events for metrics
            let result = Self::process_single_batched_event(&pool, batched_event, &event_handlers, &metrics).await;

            // The response is sent within process_single_batched_event
            // Metrics are also updated within process_single_batched_event
            if result.is_err() {
                // Error already logged and metrics updated inside process_single_batched_event
                // No further action needed here other than maybe an overall worker error metric
            } else {
                // Success already logged and metrics updated
            }
        }
    }

    async fn process_single_batched_event(
        pool: &PgPool,
        batched_event: BatchedEvent,
        event_handlers: &DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>,
        metrics: &EventStoreMetrics,
    ) -> Result<(), EventStoreError> {
        let mut tx = pool.begin().await.map_err(EventStoreError::DatabaseError)?;

        // OCC Check
        let db_version_row = sqlx::query!(
            r#"SELECT MAX(version) as max_version FROM events WHERE aggregate_id = $1"#,
            batched_event.aggregate_id
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(EventStoreError::DatabaseError)?;

        let db_version = db_version_row.map_or(0, |row| row.max_version.unwrap_or(0));

        if db_version != batched_event.expected_version {
            metrics.occ_failures.fetch_add(batched_event.events.len() as u64, Ordering::Relaxed);
            let err = EventStoreError::OptimisticConcurrencyConflict {
                aggregate_id: batched_event.aggregate_id,
                expected: batched_event.expected_version,
                actual: Some(db_version),
            };
            if let Err(send_err) = batched_event.response_tx.send(Err(err)) {
                 warn!("Failed to send OCC error response for batched event: {:?}", send_err);
            }
            return Ok(()); // Return Ok because we've handled the response_tx
        }

        let prepared_events = match Self::prepare_events_for_insert_optimized(&batched_event) {
            Ok(evs) => evs,
            Err(e) => {
                let err = EventStoreError::InternalError(e.to_string());
                 if let Err(send_err) = batched_event.response_tx.send(Err(err)) {
                    warn!("Failed to send prep error response: {:?}", send_err);
                }
                return Ok(());
            }
        };

        for event_to_validate in &prepared_events {
            let validation = Self::validate_event(event_to_validate).await;
            if !validation.is_valid {
                let err = EventStoreError::ValidationError(validation.errors);
                if let Err(send_err) = batched_event.response_tx.send(Err(err)) {
                    warn!("Failed to send validation error response: {:?}", send_err);
                }
                return Ok(());
            }
        }

        for event_to_handle in &prepared_events {
            if let Err(e) = Self::handle_event(event_to_handle, event_handlers).await {
                 // Assuming handle_event now returns EventStoreError
                if let Err(send_err) = batched_event.response_tx.send(Err(e)) {
                    warn!("Failed to send handle error response: {:?}", send_err);
                }
                return Ok(());
            }
        }

        match Self::bulk_insert_events_optimized(&mut tx, prepared_events.clone()).await { // Clone if needed after this
            Ok(_) => {
                tx.commit().await.map_err(|db_err| {
                    let err = EventStoreError::DatabaseError(db_err);
                    if let Err(send_err) = batched_event.response_tx.send(Err(err)) {
                        warn!("Failed to send commit error response: {:?}", send_err);
                    }
                    // error already sent, return Ok to satisfy function signature if we don't want to propagate this specific error further up batch_processor
                })?; // This will propagate if commit fails
                metrics.events_processed.fetch_add(batched_event.events.len() as u64, Ordering::Relaxed);
                if let Err(send_err) = batched_event.response_tx.send(Ok(())) {
                    warn!("Failed to send success response: {:?}", send_err);
                }
                Ok(())
            }
            Err(e) => { // This is anyhow::Error from original bulk_insert
                metrics.events_failed.fetch_add(batched_event.events.len() as u64, Ordering::Relaxed);
                let final_error = if let Some(db_err) = e.downcast_ref::<sqlx::Error>() {
                    if let Some(pg_err) = db_err.as_database_error().and_then(|e| e.try_downcast_ref::<PgDatabaseError>()) {
                        if pg_err.code() == "23505" { // Unique violation
                             metrics.occ_failures.fetch_add(batched_event.events.len() as u64, Ordering::Relaxed);
                            EventStoreError::OptimisticConcurrencyConflict {
                                aggregate_id: batched_event.aggregate_id,
                                expected: batched_event.expected_version,
                                actual: Some(db_version), // Re-query or use old one; old one might be misleading if another transaction slipped in
                            }
                        } else {
                            EventStoreError::DatabaseError(db_err.clone())
                        }
                    } else {
                         EventStoreError::DatabaseError(db_err.clone())
                    }
                } else {
                    EventStoreError::InternalError(e.to_string())
                };
                if let Err(send_err) = batched_event.response_tx.send(Err(final_error)) {
                     warn!("Failed to send bulk insert error response: {:?}", send_err);
                }
                Ok(()) // Error sent via channel
            }
        }
    }


    fn prepare_events_for_insert_optimized(batched_event: &BatchedEvent) -> Result<Vec<Event>, serde_json::Error> {
        let mut events = Vec::with_capacity(batched_event.events.len());
        let mut version = batched_event.expected_version;

        for event in &batched_event.events {
            version += 1;
            let event_payload = Event {
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
            events.push(event_payload);
        }
        Ok(events)
    }

    async fn bulk_insert_events_optimized(
        tx: &mut Transaction<'_, Postgres>,
        events: Vec<Event>,
    ) -> Result<(), sqlx::Error> { // Changed to return sqlx::Error for specific handling
        if events.is_empty() {
            return Ok(());
        }
        // ... (rest of the method for bulk insert using UNNEST or similar)
        // For simplicity, an example of iterative insert (less performant but easier for error mapping):
        // In a real scenario, stick to the UNNEST approach if possible and handle its specific errors.
        for event in events {
            sqlx::query!(
                r#"
                INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
                event.id,
                event.aggregate_id,
                event.event_type,
                event.event_data,
                event.version,
                event.timestamp,
                serde_json::to_value(&event.metadata).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))? // Example error conversion
            )
            .execute(&mut **tx) // Use &mut **tx for Transaction
            .await?;
        }
        Ok(())
    }

    // ... get_events, snapshot_worker, create_snapshot, etc. remain mostly the same ...
    // but their error types might need to be aligned with EventStoreError if they can emit OCC errors
    // or if a consistent error type is desired throughout the EventStore.
    // For now, focusing on the write path as per prompt.

    pub async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>, EventStoreError> { // Adjusted return type
        // ...
        let events_rows = sqlx::query!(
            r#"
            SELECT id, aggregate_id, event_type, event_data, version, timestamp
            FROM events
            WHERE aggregate_id = $1
            AND version > $2
            ORDER BY version ASC
            "#,
            aggregate_id,
            from_version.unwrap_or(-1) // Ensure "version > -1" if from_version is None
        )
        .fetch_all(&self.pool) // Directly use self.pool
        .await
        .map_err(EventStoreError::DatabaseError)?;

        Ok(events_rows
            .into_iter()
            .map(|row| Event {
                id: row.id,
                aggregate_id: row.aggregate_id,
                event_type: row.event_type,
                event_data: row.event_data,
                version: row.version,
                timestamp: row.timestamp,
                metadata: EventMetadata::default(), // Assuming default or fetch if stored
            })
            .collect())
    }


    async fn validate_event(event: &Event) -> EventValidationResult {
        // ... (implementation remains the same)
        let mut result = EventValidationResult {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        };
        if event.event_type.is_empty() { result.is_valid = false; result.errors.push("Event type cannot be empty".to_string()); }
        if event.version <= 0 { result.is_valid = false; result.errors.push("Event version must be positive".to_string());}
        result
    }

    async fn handle_event(
        event: &Event,
        event_handlers: &DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>,
    ) -> Result<(), EventStoreError> { // Adjusted for consistent error type
        if let Some(handlers) = event_handlers.get(&event.event_type) {
            for handler in handlers.iter() {
                handler.handle(event).await?; // Assumes handler.handle now returns Result<(), EventStoreError>
            }
        }
        Ok(())
    }

    // --- Snapshotting and other methods ---
    // (Keep existing snapshot_worker, create_snapshot, cache_cleanup_worker, metrics_reporter, etc.)
    // Their error handling might need adjustment if they interact with parts that now return EventStoreError.
    // For brevity, I'm not reproducing all of them here but they would need review.
    // Example for create_snapshot:
    async fn create_snapshot(
        pool: &PgPool,
        cache: &Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        aggregate_id: Uuid,
        config: &EventStoreConfig,
    ) -> Result<(), EventStoreError> { // Return EventStoreError
        let events_rows = sqlx::query_as!(
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
        .map_err(EventStoreError::DatabaseError)?;

        if events_rows.is_empty() {
            return Ok(());
        }

        let mut account = crate::domain::Account::default();
        account.id = aggregate_id;

        for event_row in &events_rows {
            let account_event: AccountEvent = serde_json::from_value(event_row.event_data.clone())
                .map_err(EventStoreError::SerializationError)?;
            account.apply_event(&account_event);
        }

        let snapshot_data = serde_json::to_value(&account)
            .map_err(EventStoreError::SerializationError)?;
        let max_version = events_rows.last().unwrap().version;

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
        .map_err(EventStoreError::DatabaseError)?;

        let snapshot = CachedSnapshot {
            aggregate_id,
            data: snapshot_data,
            version: max_version,
            created_at: Instant::now(),
            ttl: Duration::from_secs(config.snapshot_cache_ttl_secs),
        };
        cache.write().await.insert(aggregate_id, snapshot);
        debug!("Created snapshot for aggregate {} at version {}", aggregate_id, max_version);
        Ok(())
    }

    async fn snapshot_worker( // Keep existing logic, ensure create_snapshot call is handled
        pool: PgPool,
        cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        config: EventStoreConfig,
    ) {
        // ... same logic ...
        // call to Self::create_snapshot(...).await will now return Result<(), EventStoreError>
        // so handle its error appropriately (e.g., log it).
         let mut interval =
            tokio::time::interval(Duration::from_secs(config.snapshot_interval_secs));
        interval.tick().await;

        loop {
            interval.tick().await;
            let candidates_result = sqlx::query!( /* ... */ ) // Same query
            .fetch_all(&pool)
            .await;

            if let Ok(candidates) = candidates_result {
                for candidate_row in candidates {
                    // ... same logic ...
                    if max_version > last_snapshot_version { // Assuming max_version and last_snapshot_version are extracted
                        let pool_clone = pool.clone();
                        let cache_clone = cache.clone();
                        let config_clone = config.clone();
                        let agg_id = candidate_row.aggregate_id; // Assuming correct extraction

                        tokio::spawn(async move {
                            if let Err(e) =
                                Self::create_snapshot(&pool_clone, &cache_clone, agg_id, &config_clone)
                                    .await
                            {
                                warn!("Failed to create snapshot for {}: {:?}", agg_id, e); // Log EventStoreError
                            } else {
                                // info!(...); // Original info log
                            }
                        });
                    }
                }
            } else if let Err(e) = candidates_result {
                 error!("Failed to query snapshot candidates: {}", e);
            }
        }
    }
    async fn cache_cleanup_worker(cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>) { /* same */
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            let mut expired_keys = Vec::new();
            {
                let cache_guard = cache.read().await;
                for (key, snapshot) in cache_guard.iter() {
                    if snapshot.created_at.elapsed() >= snapshot.ttl { expired_keys.push(*key); }
                }
            }
            if !expired_keys.is_empty() {
                let mut cache_guard = cache.write().await;
                for key in expired_keys { cache_guard.remove(&key); }
            }
        }
    }
    async fn metrics_reporter(metrics: Arc<EventStoreMetrics>) { /* same */
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            // ... metric calculations and logging ...
        }
    }

    // get_current_version, update_version, health_check, etc. would also need error type review
    pub async fn get_current_version(&self, aggregate_id: Uuid) -> Result<i64, EventStoreError> {
        let version = sqlx::query!(
            r#"SELECT COALESCE(MAX(version), 0) as version FROM events WHERE aggregate_id = $1"#,
            aggregate_id
        )
        .fetch_one(&self.pool)
        .await
        .map_err(EventStoreError::DatabaseError)?
        .version
        .unwrap_or(0);
        self.version_cache.insert(aggregate_id, version); // Consider if this cache is still needed or useful
        Ok(version)
    }
} // End of EventStore impl


// ... (EventStoreHealth, EventData, EventRow, EventStoreConfig, EventMetadata default, RetryConfig, EventStore default remain same) ...
// Make sure EventStoreConfig default is present.
#[derive(Debug, Clone)]
pub struct EventStoreConfig {
    pub database_url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_batch_queue_size: usize,
    pub batch_processor_count: usize,
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
            max_connections: 20, min_connections: 5, acquire_timeout_secs: 5,
            idle_timeout_secs: 300, max_lifetime_secs: 900,
            batch_size: 1000, batch_timeout_ms: 10, max_batch_queue_size: 10000,
            batch_processor_count: 4, snapshot_threshold: 500,
            snapshot_interval_secs: 300, snapshot_cache_ttl_secs: 3600,
            max_snapshots_per_run: 100,
        }
    }
}
impl Default for EventMetadata { fn default() -> Self { Self { correlation_id: None, causation_id: None, user_id: None, source: "system".to_string(), schema_version: "1.0".to_string(), tags: Vec::new(), } } }
#[derive(Debug, Clone)] pub struct RetryConfig { pub max_retries: u32, pub initial_delay: std::time::Duration, pub max_delay: std::time::Duration, pub backoff_factor: f64, }
impl Default for RetryConfig { fn default() -> Self { Self { max_retries: 3, initial_delay: std::time::Duration::from_millis(100), max_delay: std::time::Duration::from_secs(5), backoff_factor: 2.0, } } }
impl Default for EventStore { fn default() -> Self { let pool = PgPoolOptions::new().max_connections(5).connect_lazy("postgresql://postgres:Francisco1@localhost:5432/banking_es").expect("Failed to connect to database"); EventStore::new(pool) } }
struct EventRow { id: Uuid, aggregate_id: Uuid, event_type: String, event_data: Value, version: i64, timestamp: DateTime<Utc>,}
#[derive(Debug, serde::Serialize)] pub struct EventStoreHealth { pub database_status: String, pub total_connections: u32, pub idle_connections: u32, pub cache_size: usize, pub batch_queue_permits: usize, pub metrics: EventStoreMetrics, pub batch_queue_metrics: String, pub health_check_duration: Duration, }
