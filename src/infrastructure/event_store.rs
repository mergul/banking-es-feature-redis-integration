use crate::domain::{Account, AccountEvent};
use crate::infrastructure::connection_pool_monitor::{
    ConnectionPoolMonitor, PoolHealth, PoolMonitorConfig, PoolMonitorTrait,
};
use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use crate::infrastructure::deadlock_detector::{DeadlockConfig, DeadlockDetector, DeadlockStats};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bincode;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::{PgPoolOptions, PgValue},
    PgPool, Postgres, Row, Transaction,
};
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, OnceCell, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Macro to track operations with deadlock detection
macro_rules! track_operation {
    ($detector:expr, $operation_type:expr, $operation_id:expr, $operation:expr) => {{
        let detector = $detector;
        let operation_type = $operation_type;
        let operation_id = $operation_id;

        // Start operation tracking
        if let Err(e) = detector
            .start_operation(operation_id.clone(), operation_type.to_string())
            .await
        {
            warn!("Failed to start operation tracking: {}", e);
        }

        let result = $operation.await;

        // End operation tracking
        detector.end_operation(&operation_id).await;

        result
    }};
}

// Global connection pool
pub static DB_POOL: OnceCell<Arc<PgPool>> = OnceCell::const_new();

#[derive(Debug)]
pub enum EventStoreError {
    OptimisticConcurrencyConflict {
        aggregate_id: Uuid,
        expected: i64,
        actual: Option<i64>,
    },
    DatabaseError(sqlx::Error),
    SerializationErrorBincode(Box<bincode::ErrorKind>),
    ValidationError(Vec<String>),
    ResponseSendError(String),
    InternalError(String),
    EventHandlingError(String),
}

impl std::fmt::Display for EventStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStoreError::OptimisticConcurrencyConflict {
                aggregate_id,
                expected,
                actual,
            } => {
                let actual_str = match actual {
                    Some(val) => val.to_string(),
                    None => "None".to_string(),
                };
                let msg = "Optimistic concurrency conflict for aggregate ".to_string()
                    + &aggregate_id.to_string()
                    + ": expected version "
                    + &expected.to_string()
                    + ", but found "
                    + &actual_str;
                f.write_str(&msg)
            }
            EventStoreError::DatabaseError(e) => {
                let msg = "Database error: ".to_string() + &e.to_string();
                f.write_str(&msg)
            }
            EventStoreError::SerializationErrorBincode(e) => {
                let msg = "Serialization error (bincode): ".to_string() + &e.to_string();
                f.write_str(&msg)
            }
            EventStoreError::ValidationError(errors) => {
                let errors_str = errors
                    .iter()
                    .map(|e| e.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let msg = "Validation error: [".to_string() + &errors_str + "]";
                f.write_str(&msg)
            }
            EventStoreError::ResponseSendError(msg) => {
                let full_msg = "Failed to send response for batch event: ".to_string() + msg;
                f.write_str(&full_msg)
            }
            EventStoreError::InternalError(msg) => {
                let full_msg = "Internal error: ".to_string() + msg;
                f.write_str(&full_msg)
            }
            EventStoreError::EventHandlingError(msg) => {
                let full_msg = "Event handling error: ".to_string() + msg;
                f.write_str(&full_msg)
            }
        }
    }
}

impl From<sqlx::Error> for EventStoreError {
    fn from(err: sqlx::Error) -> Self {
        EventStoreError::DatabaseError(err)
    }
}

impl From<Box<bincode::ErrorKind>> for EventStoreError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        EventStoreError::SerializationErrorBincode(err)
    }
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
    pub event_data: Vec<u8>,
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
    pools: Arc<PartitionedPools>,
    batch_sender: mpsc::UnboundedSender<BatchedEvent>,
    snapshot_cache: Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
    config: EventStoreConfig,
    batch_semaphore: Arc<Semaphore>,
    metrics: Arc<EventStoreMetrics>,
    event_validators: Arc<DashMap<String, Box<dyn EventValidator + Send + Sync>>>,
    event_handlers: Arc<DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>>,
    version_cache: Arc<DashMap<Uuid, i64>>,
    deadlock_detector: Arc<DeadlockDetector>,
    pool_monitor: Arc<ConnectionPoolMonitor>,
}

#[derive(Debug)]
struct BatchedEvent {
    aggregate_id: Uuid,
    events: Vec<AccountEvent>,
    expected_version: i64,
    response_tx: tokio::sync::oneshot::Sender<Result<(), EventStoreError>>,
    created_at: Instant,
    priority: EventPriority,
}

// Optimized batch processing structures
#[derive(Debug)]
struct OptimizedBatch {
    events: Vec<Event>,
    aggregate_id: Uuid,
    expected_version: i64,
    response_txs: Vec<tokio::sync::oneshot::Sender<Result<(), EventStoreError>>>,
    priority: EventPriority,
    created_at: Instant,
}

#[derive(Debug)]
struct BatchProcessorMetrics {
    batches_processed: AtomicU64,
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    avg_batch_size: AtomicU64,
    processing_time: AtomicU64,
    queue_depth: AtomicU64,
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
    data: Vec<u8>,
    version: i64,
    created_at: Instant,
    ttl: Duration,
}

#[derive(Debug, Default, Serialize)]
pub struct EventStoreMetrics {
    pub events_processed: AtomicU64,
    pub events_failed: AtomicU64,
    pub occ_failures: AtomicU64,
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
    async fn handle(&self, event: &Event) -> Result<(), EventStoreError>;
}

impl EventStore {
    /// Create EventStore with an existing PgPool
    pub fn new(pool: PgPool) -> Self {
        // Create partitioned pools from the single pool
        let pools = Arc::new(PartitionedPools {
            write_pool: pool.clone(),
            read_pool: pool,
            config:
                crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig::default(
                ),
        });

        let deadlock_detector = DeadlockDetector::new(DeadlockConfig::default());
        let pool_monitor = ConnectionPoolMonitor::new(
            pools.select_pool(OperationType::Write).clone(),
            PoolMonitorConfig::default(),
        );

        Self::new_with_config_and_pools(
            pools,
            EventStoreConfig::default(),
            deadlock_detector,
            pool_monitor,
        )
    }

    /// Create EventStore with custom config and existing pool
    pub fn new_with_config_and_pool(
        pool: PgPool,
        config: EventStoreConfig,
        deadlock_detector: DeadlockDetector,
        pool_monitor: ConnectionPoolMonitor,
    ) -> Self {
        // Create partitioned pools from the single pool
        let pools = Arc::new(PartitionedPools {
            write_pool: pool.clone(),
            read_pool: pool,
            config:
                crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig::default(
                ),
        });

        Self::new_with_config_and_pools(pools, config, deadlock_detector, pool_monitor)
    }

    /// Create EventStore with custom config and partitioned pools
    pub fn new_with_config_and_pools(
        pools: Arc<PartitionedPools>,
        config: EventStoreConfig,
        deadlock_detector: DeadlockDetector,
        pool_monitor: ConnectionPoolMonitor,
    ) -> Self {
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        let snapshot_cache = Arc::new(RwLock::new(HashMap::new()));
        let batch_semaphore = Arc::new(Semaphore::new(config.max_batch_queue_size));
        let metrics = Arc::new(EventStoreMetrics::default());
        let event_validators = Arc::new(DashMap::new());
        let event_handlers = Arc::new(DashMap::new());
        let version_cache = Arc::new(DashMap::new());

        let store = Self {
            pools: pools.clone(),
            batch_sender,
            snapshot_cache: snapshot_cache.clone(),
            config: config.clone(),
            batch_semaphore: batch_semaphore.clone(),
            metrics: metrics.clone(),
            event_validators,
            event_handlers: event_handlers.clone(),
            version_cache: version_cache.clone(),
            deadlock_detector: Arc::new(deadlock_detector),
            pool_monitor: Arc::new(pool_monitor),
        };

        // Start multiple batch processors for better throughput
        Self::start_batch_processors(
            pools.clone(),
            batch_receiver,
            config.clone(),
            batch_semaphore.clone(),
            metrics.clone(),
            event_handlers.clone(),
            version_cache.clone(),
        );

        // Start periodic snapshot creation
        tokio::spawn(Self::snapshot_worker(
            pools.clone(),
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
    pub async fn new_with_pool_size(pool_size: u32) -> Result<Self, EventStoreError> {
        let database_url = std::env::var("DATABASE_URL").map_err(|e| {
            EventStoreError::InternalError(
                "DATABASE_URL not set: {}".to_string() + &(e).to_string(),
            )
        })?;
        Self::new_with_pool_size_and_url(pool_size, &database_url).await
    }

    /// Create EventStore with a specific pool size and database URL
    pub async fn new_with_pool_size_and_url(
        pool_size: u32,
        database_url: &str,
    ) -> Result<Self, EventStoreError> {
        let mut config = EventStoreConfig::default();
        config.database_url = database_url.to_string();
        config.max_connections = pool_size;

        Self::new_with_config(config).await
    }

    /// Create EventStore with full configuration options
    pub async fn new_with_config(config: EventStoreConfig) -> Result<Self, EventStoreError> {
        // Create partitioned pools for read/write separation
        let pool_config =
            crate::infrastructure::connection_pool_partitioning::PoolPartitioningConfig {
                database_url: config.database_url.clone(),
                write_pool_max_connections: config.max_connections / 3, // Smaller write pool
                write_pool_min_connections: config.min_connections / 3,
                read_pool_max_connections: config.max_connections * 2 / 3, // Larger read pool
                read_pool_min_connections: config.min_connections * 2 / 3,
                acquire_timeout_secs: config.acquire_timeout_secs,
                write_idle_timeout_secs: config.idle_timeout_secs,
                read_idle_timeout_secs: config.idle_timeout_secs * 2, // Longer idle time for reads
                write_max_lifetime_secs: config.max_lifetime_secs,
                read_max_lifetime_secs: config.max_lifetime_secs * 2, // Longer lifetime for reads
            };

        let pools = Arc::new(PartitionedPools::new(pool_config).await?);
        let write_pool = pools.select_pool(OperationType::Write).clone();

        Ok(Self::new_with_config_and_pools(
            pools,
            config,
            DeadlockDetector::new(DeadlockConfig::default()),
            ConnectionPoolMonitor::new(write_pool, PoolMonitorConfig::default()),
        ))
    }

    // Enhanced event saving with priority support
    pub async fn save_events_with_priority(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
        priority: EventPriority,
    ) -> Result<(), EventStoreError> {
        if events.is_empty() {
            return Ok(());
        }

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let batched_event = BatchedEvent {
            aggregate_id,
            events,
            expected_version,
            response_tx,
            created_at: Instant::now(),
            priority,
        };

        // Acquire permit from semaphore
        let _permit = self
            .batch_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| {
                EventStoreError::InternalError(
                    "Failed to acquire batch permit: {}".to_string() + &(e).to_string(),
                )
            })?;

        // Send event to batch processor
        self.batch_sender.send(batched_event).map_err(|e| {
            EventStoreError::InternalError(
                "Failed to send event to batch processor: ".to_string() + &e.to_string(),
            )
        })?;

        // Wait for response
        match response_rx.await {
            Ok(result) => result,
            Err(e) => Err(EventStoreError::ResponseSendError(
                "Failed to receive response: ".to_string() + &e.to_string(),
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
                            "Operation failed after ".to_string()
                                + &retries.to_string()
                                + " retries: "
                                + &e.to_string()
                        ));
                    }

                    warn!(
                        "Operation failed (attempt {}/{}): {}. Retrying in {}s...",
                        retries + 1,
                        config.max_retries,
                        e,
                        delay.as_secs()
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

    // Enhanced event saving with deadlock detection
    pub async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<(), EventStoreError> {
        if events.is_empty() {
            return Ok(());
        }

        let operation_id = "save_events_{}".to_string() + &(aggregate_id).to_string();

        // Execute the operation directly without the track_operation macro
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let batched_event = BatchedEvent {
            aggregate_id,
            events,
            expected_version,
            response_tx,
            created_at: Instant::now(),
            priority: EventPriority::Normal,
        };

        // Acquire permit from semaphore with timeout
        let _permit = tokio::time::timeout(
            Duration::from_secs(5),
            self.batch_semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            EventStoreError::InternalError("Failed to acquire batch permit: timeout".to_string())
        })?
        .map_err(|e| {
            EventStoreError::InternalError(
                "Failed to acquire batch permit: ".to_string() + &e.to_string(),
            )
        })?;

        // Send event to batch processor
        self.batch_sender.send(batched_event).map_err(|e| {
            EventStoreError::InternalError(
                "Failed to send event to batch processor: ".to_string() + &e.to_string(),
            )
        })?;

        // Wait for response with timeout
        match tokio::time::timeout(Duration::from_secs(30), response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => Err(EventStoreError::ResponseSendError(
                "Failed to receive response: ".to_string() + &e.to_string(),
            )),
            Err(_) => Err(EventStoreError::InternalError(
                "Operation timed out".to_string(),
            )),
        }
    }

    // Start multiple batch processors for parallel processing
    fn start_batch_processors(
        pools: Arc<PartitionedPools>,
        receiver: mpsc::UnboundedReceiver<BatchedEvent>,
        config: EventStoreConfig,
        semaphore: Arc<Semaphore>,
        metrics: Arc<EventStoreMetrics>,
        event_handlers: Arc<DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>>,
        version_cache: Arc<DashMap<Uuid, i64>>,
    ) {
        let processor_count = config.batch_processor_count;

        // Create optimized batch processors with work stealing
        let (work_sender, work_receiver) = mpsc::unbounded_channel();
        let shared_work_receiver = Arc::new(Mutex::new(work_receiver));

        // Start work distributor
        tokio::spawn(Self::optimized_work_distributor(
            receiver,
            work_sender,
            processor_count,
        ));

        // Start multiple optimized batch processors
        for worker_id in 0..processor_count {
            let worker_pools = pools.clone();
            let worker_receiver = shared_work_receiver.clone();
            let worker_config = config.clone();
            let worker_semaphore = semaphore.clone();
            let worker_metrics = metrics.clone();
            let worker_event_handlers = event_handlers.clone();
            let worker_version_cache = version_cache.clone();

            tokio::spawn(async move {
                Self::optimized_batch_processor(
                    worker_pools,
                    worker_receiver,
                    worker_config,
                    worker_semaphore,
                    worker_metrics,
                    worker_event_handlers,
                    worker_version_cache,
                    worker_id,
                )
                .await;
            });
        }
    }

    // Optimized work distributor with priority-aware distribution
    async fn optimized_work_distributor(
        mut receiver: mpsc::UnboundedReceiver<BatchedEvent>,
        work_sender: mpsc::UnboundedSender<OptimizedBatch>,
        processor_count: usize,
    ) {
        let mut processor_index = 0;
        let mut priority_queues: Vec<Vec<BatchedEvent>> = Vec::new();
        for _ in 0..4 {
            priority_queues.push(Vec::new());
        }

        while let Some(event) = receiver.recv().await {
            // Add to appropriate priority queue
            priority_queues[event.priority as usize].push(event);

            // Process high priority events immediately
            if !priority_queues[EventPriority::Critical as usize].is_empty() {
                let critical_events = std::mem::replace(
                    &mut priority_queues[EventPriority::Critical as usize],
                    Vec::new(),
                );
                for event in critical_events {
                    if let Err(e) = work_sender.send(Self::create_optimized_batch(event)) {
                        error!("Failed to send critical event: {}", e);
                    }
                }
            }

            // Process other priority levels in batches
            for priority in [
                EventPriority::High,
                EventPriority::Normal,
                EventPriority::Low,
            ] {
                if priority_queues[priority as usize].len() >= 10 {
                    // Batch size threshold
                    let events =
                        std::mem::replace(&mut priority_queues[priority as usize], Vec::new());
                    let optimized_batch = Self::merge_events_into_batch(events, priority);
                    if let Err(e) = work_sender.send(optimized_batch) {
                        error!("Failed to send batch: {}", e);
                    }
                }
            }
        }
    }

    // Create optimized batch from single event
    fn create_optimized_batch(event: BatchedEvent) -> OptimizedBatch {
        let events =
            Self::prepare_events_for_insert_optimized(&event).unwrap_or_else(|_| Vec::new());

        OptimizedBatch {
            events,
            aggregate_id: event.aggregate_id,
            expected_version: event.expected_version,
            response_txs: vec![event.response_tx],
            priority: event.priority,
            created_at: event.created_at,
        }
    }

    // Merge multiple events into a single optimized batch
    fn merge_events_into_batch(
        events: Vec<BatchedEvent>,
        priority: EventPriority,
    ) -> OptimizedBatch {
        let mut all_events = Vec::new();
        let mut response_txs = Vec::new();
        let mut aggregate_id = Uuid::nil();
        let mut expected_version = 0;
        let mut created_at = Instant::now();

        for event in events {
            if aggregate_id == Uuid::nil() {
                aggregate_id = event.aggregate_id;
                expected_version = event.expected_version;
                created_at = event.created_at;
            }

            if let Ok(prepared_events) = Self::prepare_events_for_insert_optimized(&event) {
                all_events.extend(prepared_events);
                response_txs.push(event.response_tx);
            }
        }

        OptimizedBatch {
            events: all_events,
            aggregate_id,
            expected_version,
            response_txs,
            priority,
            created_at,
        }
    }

    // Optimized batch processor with improved performance
    async fn optimized_batch_processor(
        pools: Arc<PartitionedPools>,
        receiver: Arc<Mutex<mpsc::UnboundedReceiver<OptimizedBatch>>>,
        config: EventStoreConfig,
        semaphore: Arc<Semaphore>,
        metrics: Arc<EventStoreMetrics>,
        event_handlers: Arc<DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>>,
        version_cache: Arc<DashMap<Uuid, i64>>,
        worker_id: usize,
    ) {
        let mut batch_buffer = Vec::new();
        let mut last_flush = Instant::now();
        let mut processed_events = 0u64;

        info!("Optimized batch processor worker {} started", worker_id);

        loop {
            // Collect batches with timeout
            let batch_opt = {
                let mut receiver_guard = receiver.lock().await;
                tokio::time::timeout(
                    Duration::from_millis(config.batch_timeout_ms),
                    receiver_guard.recv(),
                )
                .await
            };

            match batch_opt {
                Ok(Some(batch)) => {
                    batch_buffer.push(batch);

                    // Flush when buffer is full or timeout
                    if batch_buffer.len() >= config.batch_size / 10 {
                        // Smaller threshold for better responsiveness
                        let batches_to_process = std::mem::replace(&mut batch_buffer, Vec::new());
                        let pools = pools.clone();
                        let metrics = metrics.clone();
                        let event_handlers = event_handlers.clone();
                        let version_cache = version_cache.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::flush_optimized_batches(
                                &pools,
                                batches_to_process,
                                &metrics,
                                &event_handlers,
                                &version_cache,
                            )
                            .await
                            {
                                error!(
                                    "Worker {} failed to flush optimized batches: {}",
                                    worker_id, e
                                );
                            }
                        });
                        last_flush = Instant::now();
                    }
                }
                Ok(None) => {
                    // Channel closed, break the loop
                    break;
                }
                Err(_) => {
                    // Timeout occurred, flush if we have batches
                    if !batch_buffer.is_empty() {
                        let batches_to_process = std::mem::replace(&mut batch_buffer, Vec::new());
                        let pools = pools.clone();
                        let metrics = metrics.clone();
                        let event_handlers = event_handlers.clone();
                        let version_cache = version_cache.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::flush_optimized_batches(
                                &pools,
                                batches_to_process,
                                &metrics,
                                &event_handlers,
                                &version_cache,
                            )
                            .await
                            {
                                error!(
                                    "Worker {} failed to flush optimized batches: {}",
                                    worker_id, e
                                );
                            }
                        });
                        last_flush = Instant::now();
                    }
                }
            }

            // Log worker statistics periodically
            if processed_events > 0 && processed_events % 1000 == 0 {
                info!(
                    "Optimized worker {} processed {} events",
                    worker_id, processed_events
                );
            }
        }
    }

    // Optimized batch flushing with improved error handling
    async fn flush_optimized_batches(
        pools: &Arc<PartitionedPools>,
        batches: Vec<OptimizedBatch>,
        metrics: &EventStoreMetrics,
        event_handlers: &DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>,
        version_cache: &DashMap<Uuid, i64>,
    ) -> Result<(), EventStoreError> {
        if batches.is_empty() {
            return Ok(());
        }

        let write_pool = pools.select_pool(OperationType::Write);
        let mut tx = tokio::time::timeout(Duration::from_secs(10), write_pool.begin())
            .await
            .map_err(|_| {
                EventStoreError::DatabaseError(sqlx::Error::Configuration(
                    "Connection timeout".into(),
                ))
            })?
            .map_err(EventStoreError::DatabaseError)?;

        // Collect all events and response channels
        let mut all_events = Vec::new();
        let mut all_response_txs = Vec::new();

        for batch in batches {
            all_events.extend(batch.events);
            all_response_txs.extend(batch.response_txs);
        }

        // Early validation - fail fast if any event is invalid
        let validation_result = Self::bulk_validate_events(&all_events).await;
        if let Err(validation_error) = validation_result {
            metrics.events_failed.fetch_add(
                all_events.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            // Send error responses
            for response_tx in all_response_txs {
                let _ = response_tx.send(Err(EventStoreError::InternalError(
                    validation_error.to_string(),
                )));
            }
            return Err(validation_error);
        }

        // Bulk event handling - fail fast if any handler fails
        let handling_result = Self::bulk_handle_events(&all_events, event_handlers).await;
        if let Err(handling_error) = handling_result {
            metrics.events_failed.fetch_add(
                all_events.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            // Send error responses
            for response_tx in all_response_txs {
                let _ = response_tx.send(Err(EventStoreError::InternalError(
                    handling_error.to_string(),
                )));
            }
            return Err(handling_error);
        }

        // Bulk insert all events
        let events_count = all_events.len();
        let insert_result =
            Self::bulk_insert_events_optimized(&mut tx, all_events, metrics, version_cache).await;

        // Send responses based on insert result
        match &insert_result {
            Ok(_) => {
                metrics
                    .events_processed
                    .fetch_add(events_count as u64, std::sync::atomic::Ordering::Relaxed);
                // Send success responses
                for response_tx in all_response_txs {
                    let _ = response_tx.send(Ok(()));
                }
            }
            Err(e) => {
                metrics
                    .events_failed
                    .fetch_add(events_count as u64, std::sync::atomic::Ordering::Relaxed);
                // Send error responses
                for response_tx in all_response_txs {
                    let _ = response_tx.send(Err(EventStoreError::InternalError(e.to_string())));
                }
            }
        }

        // Commit transaction
        tokio::time::timeout(Duration::from_secs(5), tx.commit())
            .await
            .map_err(|_| {
                EventStoreError::DatabaseError(sqlx::Error::Configuration("Commit timeout".into()))
            })?
            .map_err(EventStoreError::DatabaseError)?;

        Ok(())
    }

    // Bulk validation helper
    async fn bulk_validate_events(events: &[Event]) -> Result<(), EventStoreError> {
        for event in events {
            let validation = Self::validate_event(event).await;
            if !validation.is_valid {
                return Err(EventStoreError::ValidationError(validation.errors));
            }
        }
        Ok(())
    }

    // Bulk event handling helper
    async fn bulk_handle_events(
        events: &[Event],
        event_handlers: &DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>,
    ) -> Result<(), EventStoreError> {
        for event in events {
            if let Err(e) = Self::handle_event(event, event_handlers).await {
                return Err(EventStoreError::EventHandlingError(e.to_string()));
            }
        }
        Ok(())
    }

    // Pre-calculate event data to reduce serialization overhead
    fn prepare_events_for_insert_optimized(
        batched_event: &BatchedEvent,
    ) -> Result<Vec<Event>, bincode::Error> {
        let mut events = Vec::with_capacity(batched_event.events.len());
        let mut version = batched_event.expected_version;

        for event in &batched_event.events {
            version += 1;
            let event = Event {
                id: Uuid::new_v4(),
                aggregate_id: batched_event.aggregate_id,
                event_type: event.event_type().to_string(),
                event_data: bincode::serialize(event)?,
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

    // Helper function to identify transient errors
    fn is_transient_error(err: &sqlx::postgres::PgDatabaseError) -> bool {
        // List of PostgreSQL error codes that indicate transient failures
        const TRANSIENT_ERROR_CODES: &[&str] = &[
            "40001", // serialization_failure
            "40003", // statement_completion_unknown
            "08006", // connection_failure
            "08001", // sqlclient_unable_to_establish_sqlconnection
            "08004", // sqlserver_rejected_establishment_of_sqlconnection
            "08007", // transaction_resolution_unknown
            "08P01", // protocol_violation
            "40000", // transaction_rollback
        ];

        TRANSIENT_ERROR_CODES.contains(&err.code())
    }

    // Use PostgreSQL COPY for maximum insert performance
    async fn bulk_insert_events_optimized(
        tx: &mut Transaction<'_, Postgres>,
        events: Vec<Event>,
        metrics: &EventStoreMetrics,
        version_cache: &DashMap<Uuid, i64>,
    ) -> Result<(), EventStoreError> {
        if events.is_empty() {
            return Ok(());
        }

        // Set transaction isolation level to SERIALIZABLE for strongest consistency
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut **tx)
            .await
            .map_err(EventStoreError::DatabaseError)?;

        let first_event = events.first().unwrap();
        let aggregate_id = first_event.aggregate_id;
        let expected_version = first_event.version - 1; // Subtract 1 since we're checking the version before the new events
        let last_version = events.last().unwrap().version;

        // Retry logic for transient failures
        let mut retries = 0;
        let max_retries = 3;
        let mut delay = Duration::from_millis(100);

        loop {
            // Verify current version and lock the row for update
            let current_version = sqlx::query!(
                r#"
                SELECT COALESCE(e.version, 0) as version
                FROM (SELECT 1) dummy
                LEFT JOIN (
                    SELECT version
                    FROM events
                    WHERE aggregate_id = $1
                    ORDER BY version DESC
                    LIMIT 1
                    FOR UPDATE
                ) e ON true
                "#,
                aggregate_id
            )
            .fetch_one(&mut **tx)
            .await
            .map_err(EventStoreError::DatabaseError)?
            .version
            .unwrap_or(0);

            // Strict version check
            if current_version != expected_version {
                metrics
                    .occ_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Invalidate cache on conflict
                version_cache.remove(&aggregate_id);
                return Err(EventStoreError::OptimisticConcurrencyConflict {
                    aggregate_id,
                    expected: expected_version,
                    actual: Some(current_version),
                });
            }

            // Verify version sequence
            if events.len() > 1 {
                for (i, event) in events.iter().enumerate() {
                    if event.version != expected_version + i as i64 + 1 {
                        return Err(EventStoreError::ValidationError(vec![
                            "Invalid version sequence: expected ".to_string()
                                + &(expected_version + i as i64 + 1).to_string()
                                + ", got "
                                + &event.version.to_string(),
                        ]));
                    }
                }
            }

            // Rest of the existing insert logic...
            let mut query = String::from(
                r#"
                INSERT INTO events (id, aggregate_id, event_type, event_data, version, timestamp, metadata)
                VALUES
                "#,
            );

            let mut values = Vec::new();
            let mut params: Vec<(Uuid, Uuid, String, Vec<u8>, i64, DateTime<Utc>, Vec<u8>)> =
                Vec::new();
            let mut param_index = 1;

            for event in events.clone() {
                values.push(
                    "($".to_string()
                        + &param_index.to_string()
                        + ",$"
                        + &(param_index + 1).to_string()
                        + ",$"
                        + &(param_index + 2).to_string()
                        + ",$"
                        + &(param_index + 3).to_string()
                        + ",$"
                        + &(param_index + 4).to_string()
                        + ",$"
                        + &(param_index + 5).to_string()
                        + ",$"
                        + &(param_index + 6).to_string()
                        + ")",
                );

                params.push((
                    event.id,
                    event.aggregate_id,
                    event.event_type,
                    event.event_data,
                    event.version,
                    event.timestamp,
                    bincode::serialize(&event.metadata).unwrap_or_else(|_| {
                        bincode::serialize(&EventMetadata::default()).unwrap_or_default()
                    }),
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

            match query.execute(&mut **tx).await {
                Ok(_) => {
                    // Update cache with new version after successful insert
                    version_cache.insert(aggregate_id, last_version);
                    return Ok(());
                }
                Err(e) => {
                    if let Some(db_err) = e.as_database_error() {
                        if let Some(pg_err) =
                            db_err.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                        {
                            if pg_err.code() == "23505" {
                                if let Some(constraint) = pg_err.constraint() {
                                    if constraint == "unique_aggregate_id_version" {
                                        metrics
                                            .occ_failures
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        // Invalidate cache on conflict
                                        version_cache.remove(&aggregate_id);
                                        return Err(
                                            EventStoreError::OptimisticConcurrencyConflict {
                                                aggregate_id,
                                                expected: expected_version,
                                                actual: Some(current_version),
                                            },
                                        );
                                    }
                                }
                            }
                            // Check for transient errors that can be retried
                            if Self::is_transient_error(pg_err) {
                                if retries < max_retries {
                                    retries += 1;
                                    tokio::time::sleep(delay).await;
                                    delay *= 2; // Exponential backoff
                                    continue;
                                }
                            }
                        }
                    }
                    return Err(EventStoreError::DatabaseError(e));
                }
            }
        }
    }

    // Modify get_events to add proper type annotations
    pub async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>, EventStoreError> {
        let start_time = Instant::now();
        tracing::info!(
            "🔍 EventStore::get_events: Starting query for aggregate_id: {}, from_version: {:?}",
            aggregate_id,
            from_version
        );

        let read_pool = self.pools.select_pool(OperationType::Read);

        // Log pool state before acquisition
        tracing::info!(
            "🔍 EventStore::get_events: Pool state before acquisition - size: {}, idle: {}, active: {}",
            read_pool.size(),
            read_pool.num_idle(),
            (read_pool.size() as usize).saturating_sub(read_pool.num_idle())
        );

        tracing::info!("🔍 EventStore::get_events: About to acquire connection from pool");
        let connection_result = tokio::time::timeout(
            Duration::from_secs(30), // Increased timeout for debugging
            read_pool.acquire(),
        )
        .await;

        let mut conn = match connection_result {
            Ok(Ok(conn)) => {
                tracing::info!(
                    "🔍 EventStore::get_events: ✅ Successfully acquired connection for aggregate_id: {}",
                    aggregate_id
                );
                conn
            }
            Ok(Err(e)) => {
                tracing::error!(
                    "🔍 EventStore::get_events: ❌ Failed to acquire connection for aggregate_id {}: {}",
                    aggregate_id,
                    e
                );
                return Err(EventStoreError::DatabaseError(e));
            }
            Err(_) => {
                tracing::error!(
                    "🔍 EventStore::get_events: ❌ Connection acquisition timeout for aggregate_id: {}",
                    aggregate_id
                );
                return Err(EventStoreError::DatabaseError(sqlx::Error::Configuration(
                    "Connection acquisition timeout".into(),
                )));
            }
        };

        tracing::info!(
            "🔍 EventStore::get_events: About to execute SQL query for aggregate_id: {}",
            aggregate_id
        );
        let events = sqlx::query_as!(
            EventRow,
            r#"
            SELECT id, aggregate_id, event_type, event_data as "event_data: Vec<u8>", version, timestamp
            FROM events
            WHERE aggregate_id = $1
            AND version > $2
            ORDER BY version ASC
            "#,
            aggregate_id,
            from_version.unwrap_or(-1)
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| {
            tracing::error!("🔍 EventStore::get_events: SQL query failed for aggregate_id {}: {}", aggregate_id, e);
            EventStoreError::DatabaseError(e)
        })?;
        tracing::info!(
            "🔍 EventStore::get_events: SQL query completed for aggregate_id: {}, found {} events",
            aggregate_id,
            events.len()
        );

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
        pools: Arc<PartitionedPools>,
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
            let read_pool = pools.select_pool(OperationType::Read);
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
            .fetch_all(read_pool)
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
                            let pools_clone = pools.clone();
                            let cache_clone = cache.clone();
                            let config_clone = config.clone();

                            snapshot_tasks.push(tokio::spawn(async move {
                                if let Err(e) = Self::create_snapshot(
                                    &pools_clone,
                                    &cache_clone,
                                    aggregate_id,
                                    &config_clone,
                                )
                                .await
                                {
                                    error!("Failed to create snapshot for {}: {}", aggregate_id, e);
                                } else {
                                    info!("Created snapshot for aggregate {}", aggregate_id);
                                }
                            }));
                        } else {
                            info!("Skipping snapshot for aggregate {}: no new events (max_version: {}, last_snapshot_version: {})", aggregate_id, max_version, last_snapshot_version);
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
        pools: &Arc<PartitionedPools>,
        cache: &Arc<RwLock<HashMap<Uuid, CachedSnapshot>>>,
        aggregate_id: Uuid,
        config: &EventStoreConfig,
    ) -> Result<()> {
        let read_pool = pools.select_pool(OperationType::Read);
        let write_pool = pools.select_pool(OperationType::Write);
        let events = sqlx::query_as!(
            EventRow,
            r#"
            SELECT id, aggregate_id, event_type, event_data as "event_data: Vec<u8>", version, timestamp
            FROM events
            WHERE aggregate_id = $1
            ORDER BY version
            "#,
            aggregate_id
        )
        .fetch_all(read_pool)
        .await
        .context("Failed to fetch events for snapshot")?;

        if events.is_empty() {
            return Ok(());
        }

        let mut account = crate::domain::Account::default();
        account.id = aggregate_id;

        for event_row in &events {
            let account_event: AccountEvent = bincode::deserialize(&event_row.event_data)
                .map_err(|e| anyhow::anyhow!(EventStoreError::SerializationErrorBincode(e)))?;
            account.apply_event(&account_event);
        }

        let snapshot_data =
            bincode::serialize(&account).context("Failed to serialize account snapshot")?;
        let max_version = events.last().unwrap().version;

        sqlx::query(
            r#"
            INSERT INTO snapshots (aggregate_id, snapshot_data, version, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (aggregate_id) DO UPDATE SET
                snapshot_data = EXCLUDED.snapshot_data,
                version = EXCLUDED.version,
                created_at = EXCLUDED.created_at
            "#,
        )
        .bind(aggregate_id)
        .bind(&snapshot_data)
        .bind(max_version)
        .execute(write_pool)
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

        info!("Created snapshot for aggregate {}", aggregate_id);
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

    // Background metrics reporter
    async fn metrics_reporter(metrics: Arc<EventStoreMetrics>) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
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

            info!("EventStore Metrics - Processed: {}, Failed: {}, Success: {}%, Batches: {}, Cache Hit Rate: {}%", processed, failed, (success_rate * 100.0).round(), batches, (cache_hit_rate * 100.0).round());
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
            occ_failures: AtomicU64::new(self.metrics.occ_failures.load(Ordering::Relaxed)),
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
        let read_pool = self.pools.select_pool(OperationType::Read);
        let write_pool = self.pools.select_pool(OperationType::Write);
        (
            read_pool.size() + write_pool.size(),
            read_pool.num_idle() as u32 + write_pool.num_idle() as u32,
        )
    }

    /// Health check with detailed diagnostics including deadlock detection
    pub async fn health_check(&self) -> Result<EventStoreHealth, EventStoreError> {
        let (total_connections, idle_connections) = self.pool_stats();
        let start_time = Instant::now();

        // Check database connection and get detailed metrics
        let db_status = match tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query(
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
            .fetch_one(self.pools.select_pool(OperationType::Read))
        ).await {
            Ok(Ok(row)) => {
                let active_connections: i64 = row.get("active_connections");
                let idle_connections: i64 = row.get("idle_connections");
                let active_queries: i64 = row.get("active_queries");
                let idle_transactions: i64 = row.get("idle_transactions");

                "healthy (active: ".to_string()
                    + &active_connections.to_string()
                    + ", idle: "
                    + &idle_connections.to_string()
                    + ", queries: "
                    + &active_queries.to_string()
                    + ", idle_tx: "
                    + &idle_transactions.to_string()
                    + ")"
            }
            Ok(Err(e)) => "unhealthy: {}".to_string() + &(e).to_string(),
            Err(_) => "unhealthy: timeout".to_string(),
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

            "Queue: ".to_string()
                + &used_permits.to_string()
                + "/"
                + &total_permits.to_string()
                + " permits used ("
                + &((queue_utilization * 10.0).round() / 10.0).to_string()
                + "% utilization)"
        };

        // Get deadlock detection stats
        let deadlock_stats = self.deadlock_detector.get_stats().await;
        let pool_health = PoolMonitorTrait::health_check(&*self.pool_monitor)
            .await
            .unwrap_or_else(|_| PoolHealth {
                is_healthy: false,
                utilization: 0.0,
                active_connections: 0,
                total_connections: 0,
                stuck_connections: 0,
                last_check_timestamp: 0,
            });

        Ok(EventStoreHealth {
            database_status: db_status,
            total_connections,
            idle_connections,
            cache_size,
            batch_queue_permits: self.batch_semaphore.available_permits(),
            metrics: self.get_metrics(),
            batch_queue_metrics,
            health_check_duration: start_time.elapsed(),
            deadlock_stats,
            pool_health,
        })
    }

    // Add this method to monitor connection pool
    async fn monitor_connection_pool(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let pool_stats = self.pool_stats();
            let metrics = self.get_metrics();

            info!("Connection Pool Stats - Total: {}, Idle: {}, Active: {}, Acquire Time: {}ms, Errors: {}, Reuse: {}, Timeouts: {}, Avg Lifetime: {}s", pool_stats.0, pool_stats.1, (pool_stats.0 - pool_stats.1), metrics.connection_acquire_time.load(Ordering::Relaxed), metrics.connection_acquire_errors.load(Ordering::Relaxed), metrics.connection_reuse_count.load(Ordering::Relaxed), metrics.connection_timeout_count.load(Ordering::Relaxed), (metrics.connection_lifetime.load(Ordering::Relaxed) / 1000).to_string());
        }
    }

    pub async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>, EventStoreError> {
        let events = self.get_events(account_id, None).await?;
        if events.is_empty() {
            return Ok(None);
        }

        let mut account = Account::default();
        account.id = account_id;

        for event in events {
            let account_event: AccountEvent = bincode::deserialize(&event.event_data)
                .map_err(EventStoreError::SerializationErrorBincode)?;
            account.apply_event(&account_event);
        }

        Ok(Some(account))
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<Account>, EventStoreError> {
        let mut accounts = Vec::new();
        let read_pool = self.pools.select_pool(OperationType::Read);
        let account_ids = sqlx::query!(
            r#"
            SELECT DISTINCT aggregate_id
            FROM events
            ORDER BY aggregate_id
            "#
        )
        .fetch_all(read_pool)
        .await
        .map_err(|e| EventStoreError::DatabaseError(e))?;

        for row in account_ids {
            if let Some(account) = self.get_account(row.aggregate_id).await? {
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
    ) -> Result<(), EventStoreError> {
        if let Some(handlers) = event_handlers.get(&event.event_type) {
            for handler in handlers.iter() {
                handler.handle(event).await.map_err(|e| {
                    EventStoreError::EventHandlingError(
                        "Failed to handle event ".to_string()
                            + &event.event_type
                            + " for aggregate "
                            + &event.aggregate_id.to_string()
                            + ": "
                            + &e.to_string(),
                    )
                })?;
            }
        }
        Ok(())
    }

    // New private method for direct transactional event storage
    async fn store_events_direct_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        aggregate_id: Uuid,
        domain_events: Vec<AccountEvent>, // Renamed from 'events' to avoid conflict with 'Event' struct
        expected_version: i64,
        metrics: &EventStoreMetrics,        // Passed in
        version_cache: &DashMap<Uuid, i64>, // Passed in
    ) -> Result<(), EventStoreError> {
        if domain_events.is_empty() {
            return Ok(());
        }

        // 1. Prepare events (adapted from prepare_events_for_insert_optimized)
        // This part needs to determine the correct starting version for the events in this batch.
        // If expected_version is the version *before* these events, then the first event is expected_version + 1.
        let mut current_event_version = expected_version;
        let prepared_events: Vec<Event> = domain_events
            .into_iter()
            .map(|domain_event| {
                current_event_version += 1;
                let event_data = bincode::serialize(&domain_event)
                    .map_err(EventStoreError::SerializationErrorBincode)?;
                Ok(Event {
                    id: Uuid::new_v4(), // Event ID for the 'events' table row
                    aggregate_id,
                    event_type: domain_event.event_type().to_string(),
                    event_data,
                    version: current_event_version,
                    timestamp: Utc::now(),
                    metadata: EventMetadata::default(), // Default metadata for now
                })
            })
            .collect::<Result<Vec<Event>, EventStoreError>>()?;
        // The above .collect will propagate the first error from bincode::serialize.

        if prepared_events.is_empty() {
            // Should not happen if domain_events was not empty, but good check
            return Ok(());
        }

        // 2. Validate Events (simplified from flush_batch, can be enhanced)
        // In a full implementation, one might want to run validators here if they don't require DB access
        // or if they can also operate on the provided transaction.
        // For this sketch, basic validation or skipping detailed validation.
        for event_to_validate in &prepared_events {
            let validation = Self::validate_event(event_to_validate).await; // validate_event is async
            if !validation.is_valid {
                metrics
                    .events_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(EventStoreError::ValidationError(validation.errors));
            }
        }
        // Event Handlers are typically for after-commit effects, so not called here within the transaction.

        // 3. Bulk Insert Events (adapted from bulk_insert_events_optimized)
        // This existing method already takes a transaction.
        // We pass the externally provided `tx`.
        Self::bulk_insert_events_optimized(
            tx,                      // The crucial part: use the passed-in transaction
            prepared_events.clone(), // Clone if needed later for metrics/return, or pass ownership
            metrics,
            version_cache,
        )
        .await?;

        // Metrics update for processed events (if successful)
        metrics.events_processed.fetch_add(
            prepared_events.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub async fn get_current_version(&self, aggregate_id: Uuid) -> Result<i64, EventStoreError> {
        let read_pool = self.pools.select_pool(OperationType::Read);
        let version = sqlx::query!(
            r#"
            SELECT COALESCE(MAX(version), 0) as version
            FROM events
            WHERE aggregate_id = $1
            "#,
            aggregate_id
        )
        .fetch_one(read_pool)
        .await
        .map_err(|e| EventStoreError::DatabaseError(e))?
        .version
        .unwrap_or(0);

        self.version_cache.insert(aggregate_id, version);
        Ok(version)
    }

    pub async fn update_version(
        &self,
        aggregate_id: Uuid,
        version: i64,
    ) -> Result<(), EventStoreError> {
        self.version_cache.insert(aggregate_id, version);
        Ok(())
    }

    // Optimized batch processing configuration
    pub fn optimized_batch_config() -> EventStoreConfig {
        EventStoreConfig {
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
            }),
            max_connections: 100,    // Increased for better concurrency
            min_connections: 20,     // Increased for better responsiveness
            acquire_timeout_secs: 5, // Reduced for faster failure detection
            idle_timeout_secs: 300,  // Reduced for better connection reuse
            max_lifetime_secs: 1800, // Optimized for performance

            // Optimized batching settings for high throughput
            batch_size: 500,             // Smaller batches for better responsiveness
            batch_timeout_ms: 5,         // Faster timeouts for lower latency
            max_batch_queue_size: 50000, // Increased for high throughput
            batch_processor_count: 16,   // More processors for better parallelism

            // Snapshot configuration optimized for performance
            snapshot_threshold: 1000,      // Increased threshold
            snapshot_interval_secs: 600,   // Longer intervals
            snapshot_cache_ttl_secs: 3600, // Longer TTL
            max_snapshots_per_run: 50,     // More snapshots per run
        }
    }

    // High-performance configuration for production
    pub fn high_performance_config() -> EventStoreConfig {
        EventStoreConfig {
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
            }),
            max_connections: 200,    // High connection count
            min_connections: 50,     // High minimum connections
            acquire_timeout_secs: 3, // Very fast timeout
            idle_timeout_secs: 180,  // Aggressive connection reuse
            max_lifetime_secs: 900,  // Shorter lifetime for fresh connections

            // Ultra-optimized batching for maximum throughput
            batch_size: 1000,             // Large batches for efficiency
            batch_timeout_ms: 2,          // Very fast timeouts
            max_batch_queue_size: 100000, // Very large queue
            batch_processor_count: 32,    // Many processors

            // Aggressive snapshot settings
            snapshot_threshold: 2000,      // High threshold
            snapshot_interval_secs: 300,   // Frequent snapshots
            snapshot_cache_ttl_secs: 7200, // Long TTL
            max_snapshots_per_run: 100,    // Many snapshots per run
        }
    }

    // Backward compatibility method for old batch format
    async fn flush_batch(
        pools: &Arc<PartitionedPools>,
        batch: Vec<BatchedEvent>,
        metrics: &EventStoreMetrics,
        event_handlers: &DashMap<String, Vec<Box<dyn EventHandler + Send + Sync>>>,
        version_cache: &DashMap<Uuid, i64>,
    ) -> Result<(), EventStoreError> {
        // Convert old format to new optimized format
        let optimized_batches: Vec<OptimizedBatch> = batch
            .into_iter()
            .map(|event| Self::create_optimized_batch(event))
            .collect();

        Self::flush_optimized_batches(
            pools,
            optimized_batches,
            metrics,
            event_handlers,
            version_cache,
        )
        .await
    }

    fn get_pool(&self) -> PgPool {
        // Return the write pool as the default pool for backward compatibility
        self.pools.select_pool(OperationType::Write).clone()
    }

    fn get_partitioned_pools(&self) -> Arc<PartitionedPools> {
        self.pools.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
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
    pub deadlock_stats: DeadlockStats,
    pub pool_health: PoolHealth,
}

// Helper structs
struct EventData {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    event_data: Vec<u8>,
    version: i64,
    timestamp: DateTime<Utc>,
}

struct EventRow {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    event_data: Vec<u8>,
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
            max_connections: 50,      // Reduced from 200 to prevent exhaustion
            min_connections: 10,      // Reduced from 50
            acquire_timeout_secs: 10, // Increased from 5 for stability
            idle_timeout_secs: 600,   // Reduced from 1800
            max_lifetime_secs: 1800,  // Reduced from 3600

            // Optimized batching settings for stability
            batch_size: 1000,            // Reduced from 10000
            batch_timeout_ms: 10,        // Increased from 1
            max_batch_queue_size: 10000, // Reduced from 100000
            batch_processor_count: 8,    // Reduced from 32
            snapshot_threshold: 500,     // Reduced from 1000
            snapshot_interval_secs: 300,
            snapshot_cache_ttl_secs: 1800, // Reduced from 3600
            max_snapshots_per_run: 20,     // Reduced from 50
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

#[async_trait]
pub trait EventStoreTrait: Send + Sync + 'static {
    async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<(), EventStoreError>;
    async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>, EventStoreError>;
    async fn get_current_version(&self, aggregate_id: Uuid) -> Result<i64, EventStoreError>;
    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>, EventStoreError>;
    async fn get_all_accounts(&self) -> Result<Vec<Account>, EventStoreError>;
    fn get_pool(&self) -> PgPool;
    fn get_partitioned_pools(&self) -> Arc<PartitionedPools>;
    fn as_any(&self) -> &dyn std::any::Any;

    async fn save_events_in_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<(), EventStoreError>;
}

#[async_trait]
pub trait EventStoreExt: EventStoreTrait {
    async fn save_events_with_priority(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
        priority: EventPriority,
    ) -> Result<(), EventStoreError>;
    async fn update_version(&self, aggregate_id: Uuid, version: i64)
        -> Result<(), EventStoreError>;
    async fn health_check(&self) -> Result<EventStoreHealth, EventStoreError>;
    fn get_metrics(&self) -> EventStoreMetrics;
    fn pool_stats(&self) -> (u32, u32);
}

#[async_trait]
impl EventStoreTrait for EventStore {
    async fn save_events(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<(), EventStoreError> {
        self.save_events(aggregate_id, events, expected_version)
            .await
    }

    async fn get_events(
        &self,
        aggregate_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<Event>, EventStoreError> {
        self.get_events(aggregate_id, from_version).await
    }

    async fn get_current_version(&self, aggregate_id: Uuid) -> Result<i64, EventStoreError> {
        self.get_current_version(aggregate_id).await
    }

    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>, EventStoreError> {
        self.get_account(account_id).await
    }

    async fn get_all_accounts(&self) -> Result<Vec<Account>, EventStoreError> {
        self.get_all_accounts().await
    }

    fn get_pool(&self) -> PgPool {
        // Return the write pool as the default pool for backward compatibility
        self.pools.select_pool(OperationType::Write).clone()
    }

    fn get_partitioned_pools(&self) -> Arc<PartitionedPools> {
        self.pools.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn save_events_in_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
    ) -> Result<(), EventStoreError> {
        // This will call a new private method that contains the core logic
        // of event preparation and bulk insertion, but using the provided transaction.
        // It bypasses the MPSC queue.
        Self::store_events_direct_in_tx(
            tx,
            aggregate_id,
            events,
            expected_version,
            &self.metrics,
            &self.version_cache,
            // event_handlers and event_validators are not directly used in the core saving logic previously,
            // but passed to flush_batch. If direct saving needs them, they should be passed.
            // For now, focusing on the DB persistence part.
        )
        .await
    }
}

#[async_trait]
impl EventStoreExt for EventStore {
    async fn save_events_with_priority(
        &self,
        aggregate_id: Uuid,
        events: Vec<AccountEvent>,
        expected_version: i64,
        priority: EventPriority,
    ) -> Result<(), EventStoreError> {
        self.save_events_with_priority(aggregate_id, events, expected_version, priority)
            .await
    }

    async fn update_version(
        &self,
        aggregate_id: Uuid,
        version: i64,
    ) -> Result<(), EventStoreError> {
        self.update_version(aggregate_id, version).await
    }

    async fn health_check(&self) -> Result<EventStoreHealth, EventStoreError> {
        self.health_check().await
    }

    fn get_metrics(&self) -> EventStoreMetrics {
        self.get_metrics()
    }

    fn pool_stats(&self) -> (u32, u32) {
        self.pool_stats()
    }
}
