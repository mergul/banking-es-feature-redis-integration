use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::time::Instant;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

// Optimized CDC outbox message with memory layout improvements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizedCDCOutboxMessage {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub topic: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<i32>,
}

// Lightweight projection cache entry with business validation
#[derive(Debug, Clone)]
struct ProjectionCacheEntry {
    balance: Decimal,
    owner_name: String,
    is_active: bool,
    version: u64,
    cached_at: Instant,
    last_event_id: Option<Uuid>, // For duplicate detection
}

impl ProjectionCacheEntry {
    fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }

    fn apply_event(&mut self, event: &crate::domain::AccountEvent) -> Result<()> {
        // Business logic validation
        match event {
            crate::domain::AccountEvent::MoneyDeposited { amount, .. } => {
                if *amount <= Decimal::ZERO {
                    return Err(anyhow::anyhow!("Deposit amount must be positive"));
                }
                if self.balance + *amount > Decimal::from_str("999999999.99").unwrap() {
                    return Err(anyhow::anyhow!("Balance would exceed maximum allowed"));
                }
                self.balance += *amount;
                self.version += 1;
            }
            crate::domain::AccountEvent::MoneyWithdrawn { amount, .. } => {
                if *amount <= Decimal::ZERO {
                    return Err(anyhow::anyhow!("Withdrawal amount must be positive"));
                }
                if self.balance < *amount {
                    return Err(anyhow::anyhow!("Insufficient funds for withdrawal"));
                }
                self.balance -= *amount;
                self.version += 1;
            }
            crate::domain::AccountEvent::AccountCreated {
                owner_name,
                initial_balance,
                ..
            } => {
                if initial_balance < &Decimal::ZERO {
                    return Err(anyhow::anyhow!("Initial balance cannot be negative"));
                }
                self.owner_name = owner_name.clone();
                self.balance = *initial_balance;
                self.is_active = true;
                self.version += 1;
            }
            crate::domain::AccountEvent::AccountClosed { .. } => {
                if !self.is_active {
                    return Err(anyhow::anyhow!("Account is already closed"));
                }
                self.is_active = false;
                self.version += 1;
            }
        }
        Ok(())
    }
}

// Optimized batch processing structure
#[derive(Debug)]
struct EventBatch {
    events: Vec<ProcessableEvent>,
    batch_id: Uuid,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct ProcessableEvent {
    event_id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    payload: Vec<u8>,
    partition_key: Option<String>,
    domain_event: Option<crate::domain::AccountEvent>, // Pre-deserialized for performance
}

impl ProcessableEvent {
    fn new(
        event_id: Uuid,
        aggregate_id: Uuid,
        event_type: String,
        payload: Vec<u8>,
        partition_key: Option<String>,
    ) -> Result<Self> {
        // Pre-deserialize domain event for better performance
        let domain_event = bincode::deserialize(&payload)?;

        Ok(Self {
            event_id,
            aggregate_id,
            event_type,
            payload,
            partition_key,
            domain_event: Some(domain_event),
        })
    }

    fn get_domain_event(&self) -> Result<&crate::domain::AccountEvent> {
        self.domain_event
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Domain event not deserialized"))
    }
}

// High-performance metrics with atomic operations and business metrics
#[derive(Debug, Default)]
pub struct OptimizedCDCMetrics {
    pub events_processed: std::sync::atomic::AtomicU64,
    pub events_failed: std::sync::atomic::AtomicU64,
    pub batches_processed: std::sync::atomic::AtomicU64,
    pub avg_processing_latency_ms: std::sync::atomic::AtomicU64,
    pub projection_cache_hits: std::sync::atomic::AtomicU64,
    pub projection_cache_misses: std::sync::atomic::AtomicU64,
    pub circuit_breaker_trips: std::sync::atomic::AtomicU64,
    pub memory_usage_bytes: std::sync::atomic::AtomicU64,
    pub active_goroutines: std::sync::atomic::AtomicU64,
    pub business_validation_failures: std::sync::atomic::AtomicU64,
    pub duplicate_events_skipped: std::sync::atomic::AtomicU64,
    pub projection_update_failures: std::sync::atomic::AtomicU64,
    pub cache_invalidation_failures: std::sync::atomic::AtomicU64,
}

impl OptimizedCDCMetrics {
    pub fn record_processing_time(&self, duration_ms: u64) {
        // Simple exponential moving average
        let current = self
            .avg_processing_latency_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let new_avg = if current == 0 {
            duration_ms
        } else {
            // Weight: 90% historical, 10% new
            (current * 9 + duration_ms) / 10
        };
        self.avg_processing_latency_ms
            .store(new_avg, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_business_validation_failure(&self) {
        self.business_validation_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_duplicate_skip(&self) {
        self.duplicate_events_skipped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// Efficient circuit breaker with exponential backoff
#[derive(Debug, Clone)]
enum CircuitBreakerState {
    Closed,
    Open { until: Instant, failure_count: u32 },
    HalfOpen { test_requests: u32 },
}

impl Default for CircuitBreakerState {
    fn default() -> Self {
        CircuitBreakerState::Closed
    }
}

struct CircuitBreaker {
    state: Arc<RwLock<CircuitBreakerState>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    max_test_requests: u32,
}

impl CircuitBreaker {
    fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitBreakerState::Closed)),
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(30),
            max_test_requests: 3,
        }
    }

    async fn can_execute(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open { until, .. } => Instant::now() >= until,
            CircuitBreakerState::HalfOpen { test_requests } => {
                test_requests < self.max_test_requests
            }
        }
    }

    async fn on_success(&self) {
        let mut state = self.state.write().await;
        *state = CircuitBreakerState::Closed;
    }

    async fn on_failure(&self) {
        let mut state = self.state.write().await;
        match *state {
            CircuitBreakerState::Closed => {
                *state = CircuitBreakerState::Open {
                    until: Instant::now() + self.recovery_timeout,
                    failure_count: 1,
                };
            }
            CircuitBreakerState::Open { failure_count, .. } => {
                let backoff = Duration::from_secs(
                    (self.recovery_timeout.as_secs() * 2_u64.pow(failure_count.min(8))).min(300), // Max 5 minutes
                );
                *state = CircuitBreakerState::Open {
                    until: Instant::now() + backoff,
                    failure_count: failure_count + 1,
                };
            }
            CircuitBreakerState::HalfOpen { .. } => {
                *state = CircuitBreakerState::Open {
                    until: Instant::now() + self.recovery_timeout,
                    failure_count: 1,
                };
            }
        }
    }

    async fn on_attempt(&self) {
        let mut state = self.state.write().await;
        if let CircuitBreakerState::Open { until, .. } = *state {
            if Instant::now() >= until {
                *state = CircuitBreakerState::HalfOpen { test_requests: 1 };
            }
        } else if let CircuitBreakerState::HalfOpen { test_requests } = *state {
            *state = CircuitBreakerState::HalfOpen {
                test_requests: test_requests + 1,
            };
        }
    }
}

// Memory-efficient LRU cache for projections with business validation
struct ProjectionCache {
    cache: HashMap<Uuid, ProjectionCacheEntry>,
    access_order: std::collections::VecDeque<Uuid>,
    max_size: usize,
    ttl: Duration,
    duplicate_detection: DashMap<Uuid, Uuid>, // event_id -> aggregate_id for duplicate detection
}

impl ProjectionCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            access_order: std::collections::VecDeque::with_capacity(max_size),
            max_size,
            ttl,
            duplicate_detection: DashMap::new(),
        }
    }

    fn get(&mut self, key: &Uuid) -> Option<&ProjectionCacheEntry> {
        if let Some(entry) = self.cache.get(key) {
            if !entry.is_expired(self.ttl) {
                // Move to front for LRU
                self.access_order.retain(|&x| x != *key);
                self.access_order.push_front(*key);
                return Some(entry);
            } else {
                // Mark for removal
                self.access_order.retain(|&x| x != *key);
                return None;
            }
        }
        None
    }

    fn remove_expired(&mut self, key: &Uuid) {
        self.cache.remove(key);
    }

    fn put(&mut self, key: Uuid, entry: ProjectionCacheEntry) {
        if self.cache.len() >= self.max_size {
            // Remove LRU item
            if let Some(lru_key) = self.access_order.pop_back() {
                self.cache.remove(&lru_key);
            }
        }

        self.cache.insert(key, entry);
        self.access_order.push_front(key);
    }

    fn invalidate(&mut self, key: &Uuid) {
        self.cache.remove(key);
        self.access_order.retain(|&x| x != *key);
    }

    fn memory_usage(&self) -> usize {
        self.cache.len() * std::mem::size_of::<ProjectionCacheEntry>()
    }

    fn is_duplicate_event(&self, event_id: &Uuid, aggregate_id: &Uuid) -> bool {
        if let Some(existing_aggregate_id) = self.duplicate_detection.get(event_id) {
            existing_aggregate_id.key() == aggregate_id
        } else {
            self.duplicate_detection.insert(*event_id, *aggregate_id);
            false
        }
    }

    fn cleanup_duplicates(&mut self) {
        // Clean up old duplicate detection entries (older than 1 hour)
        let cutoff = Instant::now() - Duration::from_secs(3600);
        // Note: DashMap doesn't have retain, so we'd need to implement this differently
        // For now, we'll let it grow and clean up periodically
    }
}

// Ultra-optimized CDC Event Processor with business logic validation
pub struct UltraOptimizedCDCEventProcessor {
    kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
    cache_service: Arc<dyn CacheServiceTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    metrics: Arc<OptimizedCDCMetrics>,

    // High-performance processing
    processing_semaphore: Arc<Semaphore>,
    batch_queue: Arc<Mutex<Vec<ProcessableEvent>>>,
    batch_size: usize,
    batch_timeout: Duration,

    // Circuit breaker
    circuit_breaker: CircuitBreaker,

    // Memory-efficient projection cache
    projection_cache: Arc<Mutex<ProjectionCache>>,

    // Batch processing coordination
    batch_processor_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,

    // Business logic configuration
    business_config: BusinessLogicConfig,
}

#[derive(Debug, Clone)]
pub struct BusinessLogicConfig {
    pub enable_validation: bool,
    pub max_balance: Decimal,
    pub min_balance: Decimal,
    pub max_transaction_amount: Decimal,
    pub enable_duplicate_detection: bool,
    pub cache_invalidation_delay_ms: u64,
    pub batch_processing_enabled: bool,
}

impl Default for BusinessLogicConfig {
    fn default() -> Self {
        Self {
            enable_validation: true,
            max_balance: Decimal::from_str("999999999.99").unwrap(),
            min_balance: Decimal::ZERO,
            max_transaction_amount: Decimal::from_str("1000000.00").unwrap(),
            enable_duplicate_detection: true,
            cache_invalidation_delay_ms: 10, // Reduced from 50ms
            batch_processing_enabled: true,
        }
    }
}

impl UltraOptimizedCDCEventProcessor {
    pub fn new(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        business_config: Option<BusinessLogicConfig>,
    ) -> Self {
        let projection_cache = Arc::new(Mutex::new(ProjectionCache::new(
            10000,                    // Max 10k cached projections
            Duration::from_secs(300), // 5 minute TTL
        )));

        Self {
            kafka_producer,
            cache_service,
            projection_store,
            metrics: Arc::new(OptimizedCDCMetrics::default()),
            processing_semaphore: Arc::new(Semaphore::new(100)), // Increased concurrency
            batch_queue: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            batch_size: 50,                           // Optimized batch size
            batch_timeout: Duration::from_millis(10), // Aggressive batching
            circuit_breaker: CircuitBreaker::new(),
            projection_cache,
            batch_processor_handle: None,
            shutdown_tx: None,
            business_config: business_config.unwrap_or_default(),
        }
    }

    pub async fn start_batch_processor(&mut self) -> Result<()> {
        if !self.business_config.batch_processing_enabled {
            return Ok(());
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let batch_queue = Arc::clone(&self.batch_queue);
        let projection_store = Arc::clone(&self.projection_store);
        let projection_cache = Arc::clone(&self.projection_cache);
        let metrics = Arc::clone(&self.metrics);
        let batch_size = self.batch_size;
        let batch_timeout = self.batch_timeout;

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(batch_timeout);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Self::process_batch(
                            &batch_queue,
                            &projection_store,
                            &projection_cache,
                            &metrics,
                            batch_size,
                        ).await {
                            error!("Batch processing failed: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Batch processor shutting down");
                        break;
                    }
                }
            }
        });

        self.batch_processor_handle = Some(handle);
        Ok(())
    }

    /// Ultra-fast event processing with business logic validation
    #[instrument(skip(self, cdc_event))]
    pub async fn process_cdc_event_ultra_fast(&self, cdc_event: serde_json::Value) -> Result<()> {
        let start_time = Instant::now();

        // Circuit breaker check
        if !self.circuit_breaker.can_execute().await {
            return Err(anyhow::anyhow!("Circuit breaker is open"));
        }

        self.circuit_breaker.on_attempt().await;

        // Extract event with zero-copy deserialization where possible
        let processable_event = match self.extract_event_zero_copy(&cdc_event)? {
            Some(event) => event,
            None => return Ok(()), // Skip tombstone/delete events
        };

        // Business logic validation
        if self.business_config.enable_validation {
            if let Err(e) = self.validate_business_logic(&processable_event).await {
                self.metrics.record_business_validation_failure();
                error!(
                    "Business validation failed for event {}: {}",
                    processable_event.event_id, e
                );
                return Err(e);
            }
        }

        // Duplicate detection
        if self.business_config.enable_duplicate_detection {
            let mut cache_guard = self.projection_cache.lock().await;
            if cache_guard
                .is_duplicate_event(&processable_event.event_id, &processable_event.aggregate_id)
            {
                self.metrics.record_duplicate_skip();
                debug!("Skipping duplicate event: {}", processable_event.event_id);
                return Ok(());
            }
        }

        // Acquire processing permit
        let _permit = self.processing_semaphore.acquire().await?;

        // Add to batch queue for processing
        {
            let mut queue = self.batch_queue.lock().await;
            queue.push(processable_event);

            // Trigger immediate batch processing if queue is full
            if queue.len() >= self.batch_size {
                drop(queue); // Release lock before processing
                self.process_current_batch().await?;
            }
        }

        // Record metrics
        let processing_time = start_time.elapsed().as_millis() as u64;
        self.metrics.record_processing_time(processing_time);
        self.metrics
            .events_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.circuit_breaker.on_success().await;
        Ok(())
    }

    /// Business logic validation
    async fn validate_business_logic(&self, event: &ProcessableEvent) -> Result<()> {
        let domain_event = event.get_domain_event()?;

        match domain_event {
            crate::domain::AccountEvent::MoneyDeposited { amount, .. } => {
                if *amount <= Decimal::ZERO {
                    return Err(anyhow::anyhow!("Deposit amount must be positive"));
                }
                if *amount > self.business_config.max_transaction_amount {
                    return Err(anyhow::anyhow!("Deposit amount exceeds maximum allowed"));
                }
            }
            crate::domain::AccountEvent::MoneyWithdrawn { amount, .. } => {
                if *amount <= Decimal::ZERO {
                    return Err(anyhow::anyhow!("Withdrawal amount must be positive"));
                }
                if *amount > self.business_config.max_transaction_amount {
                    return Err(anyhow::anyhow!("Withdrawal amount exceeds maximum allowed"));
                }
            }
            crate::domain::AccountEvent::AccountCreated {
                initial_balance, ..
            } => {
                if *initial_balance < self.business_config.min_balance {
                    return Err(anyhow::anyhow!("Initial balance cannot be negative"));
                }
                if *initial_balance > self.business_config.max_balance {
                    return Err(anyhow::anyhow!("Initial balance exceeds maximum allowed"));
                }
            }
            crate::domain::AccountEvent::AccountClosed { .. } => {
                // No additional validation needed for account closure
            }
        }
        Ok(())
    }

    /// Process current batch immediately
    async fn process_current_batch(&self) -> Result<()> {
        Self::process_batch(
            &self.batch_queue,
            &self.projection_store,
            &self.projection_cache,
            &self.metrics,
            self.batch_size,
        )
        .await
    }

    /// Optimized batch processing with bulk operations and business logic
    async fn process_batch(
        batch_queue: &Arc<Mutex<Vec<ProcessableEvent>>>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        projection_cache: &Arc<Mutex<ProjectionCache>>,
        metrics: &Arc<OptimizedCDCMetrics>,
        batch_size: usize,
    ) -> Result<()> {
        let events = {
            let mut queue = batch_queue.lock().await;
            if queue.is_empty() {
                return Ok(());
            }
            let drain_size = queue.len().min(batch_size);
            queue.drain(..drain_size).collect::<Vec<_>>()
        };

        if events.is_empty() {
            return Ok(());
        }

        let batch_start = Instant::now();
        let events_count = events.len();

        // Group events by aggregate_id for efficient processing
        let mut events_by_aggregate: HashMap<Uuid, Vec<ProcessableEvent>> = HashMap::new();
        for event in events {
            events_by_aggregate
                .entry(event.aggregate_id)
                .or_default()
                .push(event);
        }

        // Process each aggregate's events
        let mut updated_projections = Vec::new();
        let mut cache_guard = projection_cache.lock().await;

        for (aggregate_id, aggregate_events) in events_by_aggregate {
            let mut projection = Self::get_or_load_projection(
                &mut cache_guard,
                projection_store,
                aggregate_id,
                metrics,
            )
            .await?;

            // Apply all events for this aggregate with business validation
            for event in aggregate_events {
                if let Err(e) = Self::apply_event_to_projection(&mut projection, &event) {
                    error!(
                        "Failed to apply event {} to projection: {}",
                        event.event_id, e
                    );
                    metrics
                        .projection_update_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue; // Skip this event but continue processing others
                }
            }

            // Update cache
            cache_guard.put(aggregate_id, projection.clone());
            updated_projections.push(Self::projection_cache_to_db_projection(
                &projection,
                aggregate_id,
            ));
        }

        drop(cache_guard);

        // Bulk update projections in database
        if !updated_projections.is_empty() {
            if let Err(e) = projection_store
                .upsert_accounts_batch(updated_projections)
                .await
            {
                error!("Failed to bulk update projections: {}", e);
                metrics
                    .projection_update_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(e);
            }
        }

        // Update metrics
        let batch_time = batch_start.elapsed().as_millis() as u64;
        metrics
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        debug!(
            "Processed batch of {} events in {}ms",
            events_count, batch_time
        );

        Ok(())
    }

    /// Get projection from cache or load from database
    async fn get_or_load_projection(
        cache: &mut ProjectionCache,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        aggregate_id: Uuid,
        metrics: &Arc<OptimizedCDCMetrics>,
    ) -> Result<ProjectionCacheEntry> {
        if let Some(cached) = cache.get(&aggregate_id) {
            metrics
                .projection_cache_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(cached.clone());
        }

        metrics
            .projection_cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Load from database
        let db_projection = projection_store.get_account(aggregate_id).await?;

        let cache_entry = if let Some(proj) = db_projection {
            ProjectionCacheEntry {
                balance: proj.balance,
                owner_name: proj.owner_name,
                is_active: proj.is_active,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            }
        } else {
            // New projection
            ProjectionCacheEntry {
                balance: Decimal::ZERO,
                owner_name: String::new(),
                is_active: false,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            }
        };

        cache.put(aggregate_id, cache_entry.clone());
        Ok(cache_entry)
    }

    /// Convert cache entry to database projection
    fn projection_cache_to_db_projection(
        cache_entry: &ProjectionCacheEntry,
        aggregate_id: Uuid,
    ) -> crate::infrastructure::projections::AccountProjection {
        crate::infrastructure::projections::AccountProjection {
            id: aggregate_id,
            owner_name: cache_entry.owner_name.clone(),
            balance: cache_entry.balance,
            is_active: cache_entry.is_active,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Apply event to projection with business validation
    fn apply_event_to_projection(
        projection: &mut ProjectionCacheEntry,
        event: &ProcessableEvent,
    ) -> Result<()> {
        let domain_event = event.get_domain_event()?;

        // Apply business logic validation and update
        projection.apply_event(domain_event)?;

        // Update last event ID for duplicate detection
        projection.last_event_id = Some(event.event_id);

        Ok(())
    }

    /// Extract event with minimal allocations
    fn extract_event_zero_copy(
        &self,
        cdc_event: &serde_json::Value,
    ) -> Result<Option<ProcessableEvent>> {
        let payload = cdc_event
            .get("payload")
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;

        let after = payload.get("after");
        if after.is_none() || after == Some(&serde_json::Value::Null) {
            return Ok(None); // Skip tombstone
        }

        let after_data = after.unwrap();

        // Extract required fields with minimal string allocations
        let event_id = after_data
            .get("event_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid event_id"))?;

        let aggregate_id = after_data
            .get("aggregate_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid aggregate_id"))?;

        let event_type = after_data
            .get("event_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing event_type"))?
            .to_string();

        let payload_str = after_data
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;

        let payload_bytes = base64::decode(payload_str)?;

        let partition_key = after_data
            .get("partition_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        ProcessableEvent::new(
            event_id,
            aggregate_id,
            event_type,
            payload_bytes,
            partition_key,
        )
        .map(Some)
    }

    /// Get comprehensive metrics
    pub async fn get_metrics(&self) -> OptimizedCDCMetrics {
        let cache_guard = self.projection_cache.lock().await;
        let memory_usage = cache_guard.memory_usage();
        drop(cache_guard);

        self.metrics
            .memory_usage_bytes
            .store(memory_usage as u64, std::sync::atomic::Ordering::Relaxed);

        // Create a copy of metrics for external use
        OptimizedCDCMetrics {
            events_processed: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .events_processed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            events_failed: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .events_failed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            batches_processed: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .batches_processed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            avg_processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .avg_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            projection_cache_hits: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .projection_cache_hits
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            projection_cache_misses: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .projection_cache_misses
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            circuit_breaker_trips: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .circuit_breaker_trips
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            memory_usage_bytes: std::sync::atomic::AtomicU64::new(memory_usage as u64),
            active_goroutines: std::sync::atomic::AtomicU64::new(0),
            business_validation_failures: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .business_validation_failures
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            duplicate_events_skipped: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .duplicate_events_skipped
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            projection_update_failures: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .projection_update_failures
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            cache_invalidation_failures: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .cache_invalidation_failures
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }

        if let Some(handle) = self.batch_processor_handle.take() {
            handle.await?;
        }

        // Process any remaining events
        self.process_current_batch().await?;

        info!("CDC Event Processor shutdown complete");
        Ok(())
    }
}

// Memory monitoring utilities
impl UltraOptimizedCDCEventProcessor {
    pub async fn get_memory_usage(&self) -> usize {
        let cache_guard = self.projection_cache.lock().await;
        let cache_memory = cache_guard.memory_usage();

        let queue_guard = self.batch_queue.lock().await;
        let queue_memory = queue_guard.len() * std::mem::size_of::<ProcessableEvent>();

        cache_memory + queue_memory
    }

    pub async fn clear_expired_cache_entries(&self) {
        let mut cache_guard = self.projection_cache.lock().await;
        let ttl = Duration::from_secs(300);

        let expired_keys: Vec<Uuid> = cache_guard
            .cache
            .iter()
            .filter(|(_, entry)| entry.is_expired(ttl))
            .map(|(k, _)| *k)
            .collect();

        for key in expired_keys {
            cache_guard.invalidate(&key);
        }

        // Clean up duplicate detection
        cache_guard.cleanup_duplicates();
    }

    /// Get business logic configuration
    pub fn get_business_config(&self) -> &BusinessLogicConfig {
        &self.business_config
    }

    /// Update business logic configuration
    pub fn update_business_config(&mut self, config: BusinessLogicConfig) {
        self.business_config = config;
    }
}
