use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rdkafka::Message;
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

impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            failure_threshold: self.failure_threshold,
            recovery_timeout: self.recovery_timeout,
            max_test_requests: self.max_test_requests,
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

// DLQ event structure
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DLQEvent {
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub error: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub retry_count: u32,
}

// Ultra-optimized CDC Event Processor with business logic validation
pub struct UltraOptimizedCDCEventProcessor {
    kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
    dlq_producer: Option<crate::infrastructure::kafka_abstraction::KafkaProducer>,
    max_retries: u32,
    retry_backoff_ms: u64,
    cache_service: Arc<dyn CacheServiceTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    metrics: Arc<EnhancedCDCMetrics>,

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

impl Clone for UltraOptimizedCDCEventProcessor {
    fn clone(&self) -> Self {
        Self {
            kafka_producer: self.kafka_producer.clone(),
            dlq_producer: self.dlq_producer.clone(),
            max_retries: self.max_retries,
            retry_backoff_ms: self.retry_backoff_ms,
            cache_service: self.cache_service.clone(),
            projection_store: self.projection_store.clone(),
            metrics: self.metrics.clone(),
            processing_semaphore: self.processing_semaphore.clone(),
            batch_queue: self.batch_queue.clone(),
            batch_size: self.batch_size,
            batch_timeout: self.batch_timeout,
            circuit_breaker: self.circuit_breaker.clone(),
            projection_cache: self.projection_cache.clone(),
            batch_processor_handle: None, // Don't clone the handle
            shutdown_tx: None,            // Don't clone the sender
            business_config: self.business_config.clone(),
        }
    }
}

impl UltraOptimizedCDCEventProcessor {
    pub fn new(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        metrics: Arc<EnhancedCDCMetrics>, // <-- accept as parameter
        business_config: Option<BusinessLogicConfig>,
    ) -> Self {
        // 1. Increase cache size and TTL
        let projection_cache = Arc::new(Mutex::new(ProjectionCache::new(
            50000,                     // Max 50k cached projections
            Duration::from_secs(1800), // 30 minute TTL
        )));
        Self {
            kafka_producer: kafka_producer.clone(),
            dlq_producer: Some(kafka_producer), // Use the same producer for DLQ by default
            max_retries: 3,
            retry_backoff_ms: 100,
            cache_service,
            projection_store,
            metrics,                                             // <-- use the provided Arc
            processing_semaphore: Arc::new(Semaphore::new(100)), // Increased concurrency
            batch_queue: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            batch_size: 200,                         // Increased batch size
            batch_timeout: Duration::from_millis(5), // Reduced batch timeout
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
        let cache_service = Arc::clone(&self.cache_service);
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
                            &cache_service,
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
        if !self.circuit_breaker.can_execute().await {
            return Err(anyhow::anyhow!("Circuit breaker is open"));
        }
        self.circuit_breaker.on_attempt().await;
        let processable_event = match self.extract_event_zero_copy(&cdc_event)? {
            Some(event) => event,
            None => return Ok(()),
        };
        let mut retries = 0;
        loop {
            match self.try_process_event(&processable_event).await {
                Ok(_) => {
                    self.circuit_breaker.on_success().await;
                    break;
                }
                Err(e) => {
                    retries += 1;
                    if retries > self.max_retries {
                        self.metrics
                            .events_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.send_to_dlq(&processable_event, &e, retries).await?;
                        self.circuit_breaker.on_failure().await;
                        error!("Event sent to DLQ after {} retries: {}", retries, e);
                        break;
                    } else {
                        warn!(
                            "Retrying event {} (attempt {}): {}",
                            processable_event.event_id, retries, e
                        );
                        tokio::time::sleep(Duration::from_millis(
                            self.retry_backoff_ms * retries as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        let processing_time = start_time.elapsed().as_millis() as u64;
        self.metrics
            .processing_latency_ms
            .fetch_add(processing_time, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .events_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn try_process_event(&self, processable_event: &ProcessableEvent) -> Result<()> {
        // Business logic validation
        if self.business_config.enable_validation {
            if let Err(e) = self.validate_business_logic(processable_event).await {
                self.metrics
                    .events_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                self.metrics
                    .consecutive_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!("Skipping duplicate event: {}", processable_event.event_id);
                return Ok(());
            }
        }
        let _permit = self.processing_semaphore.acquire().await?;
        {
            let mut queue = self.batch_queue.lock().await;
            queue.push(processable_event.clone());
            if queue.len() >= self.batch_size {
                drop(queue);
                self.process_current_batch().await?;
            }
        }
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
            &self.cache_service,
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
        cache_service: &Arc<dyn CacheServiceTrait>,
        metrics: &Arc<EnhancedCDCMetrics>,
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
                cache_service,
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
                        .events_failed
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

        // Bulk update projections in database in parallel chunks
        if !updated_projections.is_empty() {
            const MAX_CONCURRENT_UPSERTS: usize = 4; // Tune as needed
            let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_UPSERTS));
            let chunk_count = 8; // Number of parallel chunks
            let chunk_size = (updated_projections.len() + chunk_count - 1) / chunk_count;
            let mut tasks = Vec::new();
            for chunk in updated_projections.chunks(chunk_size) {
                let store = projection_store.clone();
                let chunk_vec = chunk.to_vec();
                let semaphore = semaphore.clone();
                tasks.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire().await;
                    store.upsert_accounts_batch(chunk_vec).await
                }));
            }
            // Await all upsert tasks
            let upsert_start = Instant::now();
            let results = futures::future::join_all(tasks).await;
            let upsert_time = upsert_start.elapsed().as_millis() as u64;
            tracing::info!(
                "Parallel upsert of {} projections in {}ms",
                updated_projections.len(),
                upsert_time
            );
            for res in results {
                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("Failed to bulk update projections in chunk: {}", e);
                        metrics
                            .events_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return Err(anyhow::anyhow!(
                            "Failed to bulk update projections in chunk: {}",
                            e
                        ));
                    }
                    Err(e) => {
                        error!("Join error in parallel upsert: {}", e);
                        metrics
                            .events_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return Err(anyhow::anyhow!("Join error in parallel upsert: {}", e));
                    }
                }
            }
        }

        // Update metrics
        let batch_time = batch_start.elapsed().as_millis() as u64;
        metrics
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tracing::debug!(
            "Processed batch of {} events in {}ms",
            events_count,
            batch_time
        );

        Ok(())
    }

    /// Get projection from cache or load from database
    async fn get_or_load_projection(
        cache: &mut ProjectionCache,
        cache_service: &Arc<dyn CacheServiceTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        aggregate_id: Uuid,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Result<ProjectionCacheEntry> {
        // L1 cache check
        if let Some(cached) = cache.get(&aggregate_id) {
            metrics
                .batches_processed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!("ProjectionCache: L1 cache hit for {}", aggregate_id);
            return Ok(cached.clone());
        }
        metrics
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!("ProjectionCache: L1 cache miss for {}", aggregate_id);
        // L2 (Redis) cache check
        if let Ok(Some(redis_proj)) = cache_service.get_account(aggregate_id).await {
            tracing::debug!("ProjectionCache: L2 (Redis) cache hit for {}", aggregate_id);
            // Populate L1
            let entry = ProjectionCacheEntry {
                balance: redis_proj.balance,
                owner_name: redis_proj.owner_name,
                is_active: redis_proj.is_active,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            };
            cache.put(aggregate_id, entry.clone());
            return Ok(entry);
        } else {
            tracing::debug!(
                "ProjectionCache: L2 (Redis) cache miss for {}",
                aggregate_id
            );
        }
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
            ProjectionCacheEntry {
                balance: Decimal::ZERO,
                owner_name: String::new(),
                is_active: false,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            }
        };
        // Populate both Redis and L1
        // Convert AccountProjection to Account for cache_service
        let account = crate::domain::Account {
            id: aggregate_id,
            owner_name: cache_entry.owner_name.clone(),
            balance: cache_entry.balance,
            is_active: cache_entry.is_active,
            version: 1, // or use a version if available
        };
        let _ = cache_service
            .set_account(&account, Some(Duration::from_secs(1800)))
            .await;
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
    pub async fn get_metrics(&self) -> EnhancedCDCMetrics {
        let cache_guard = self.projection_cache.lock().await;
        let memory_usage = cache_guard.memory_usage();
        drop(cache_guard);

        self.metrics
            .memory_usage_bytes
            .store(memory_usage as u64, std::sync::atomic::Ordering::Relaxed);

        // Create a copy of metrics for external use
        EnhancedCDCMetrics {
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
            processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            total_latency_ms: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .total_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            cache_invalidations: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .cache_invalidations
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            projection_updates: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .projection_updates
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            batches_processed: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .batches_processed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            circuit_breaker_trips: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .circuit_breaker_trips
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            consumer_restarts: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .consumer_restarts
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            cleanup_cycles: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .cleanup_cycles
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            memory_usage_bytes: std::sync::atomic::AtomicU64::new(memory_usage as u64),
            active_connections: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .active_connections
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            queue_depth: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .queue_depth
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            avg_batch_size: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .avg_batch_size
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            p95_processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .p95_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            p99_processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .p99_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            throughput_per_second: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .throughput_per_second
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            consecutive_failures: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .consecutive_failures
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            last_error_time: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .last_error_time
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            error_rate: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .error_rate
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            integration_helper_initialized: std::sync::atomic::AtomicBool::new(
                self.metrics
                    .integration_helper_initialized
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

    pub async fn send_to_dlq_from_cdc(
        &self,
        message: &rdkafka::message::BorrowedMessage<'_>,
        error: &str,
    ) -> Result<()> {
        if let Some(dlq_producer) = &self.dlq_producer {
            let key = message
                .key()
                .map(|k| String::from_utf8_lossy(k).to_string());
            let payload = message.payload().unwrap_or_default();
            let topic = "banking-es-dlq";
            dlq_producer
                .publish_binary_event(topic, payload, &key.unwrap_or_default())
                .await?;
        }
        Ok(())
    }

    async fn send_to_dlq(
        &self,
        event: &ProcessableEvent,
        error: &anyhow::Error,
        retry_count: u32,
    ) -> Result<()> {
        if let Some(dlq_producer) = &self.dlq_producer {
            let dlq_event = DLQEvent {
                event_id: event.event_id,
                aggregate_id: event.aggregate_id,
                event_type: event.event_type.clone(),
                payload: event.payload.clone(),
                error: error.to_string(),
                timestamp: Utc::now(),
                retry_count,
            };
            let payload = serde_json::to_vec(&dlq_event)?;
            let topic = "banking-es-dlq";
            let key = event.aggregate_id.to_string();
            dlq_producer
                .publish_binary_event(topic, &payload, &key)
                .await?;
        }
        Ok(())
    }
}

// DLQ Recovery Configuration
#[derive(Debug, Clone)]
pub struct DLQRecoveryConfig {
    pub dlq_topic: String,
    pub consumer_group_id: String,
    pub max_reprocessing_retries: u32,
    pub reprocessing_backoff_ms: u64,
    pub batch_size: usize,
    pub poll_timeout_ms: u64,
    pub enable_auto_recovery: bool,
    pub auto_recovery_interval_secs: u64,
}

impl Default for DLQRecoveryConfig {
    fn default() -> Self {
        Self {
            dlq_topic: "banking-es-dlq".to_string(),
            consumer_group_id: "banking-es-dlq-recovery".to_string(),
            max_reprocessing_retries: 5,
            reprocessing_backoff_ms: 1000,
            batch_size: 10,
            poll_timeout_ms: 100,
            enable_auto_recovery: false,
            auto_recovery_interval_secs: 300, // 5 minutes
        }
    }
}

// DLQ Consumer for recovery processing
pub struct DLQConsumer {
    kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
    config: DLQRecoveryConfig,
    processor: Arc<UltraOptimizedCDCEventProcessor>,
    metrics: Arc<DLQRecoveryMetrics>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    consumer_handle: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Default)]
pub struct DLQRecoveryMetrics {
    pub dlq_messages_consumed: std::sync::atomic::AtomicU64,
    pub dlq_messages_reprocessed: std::sync::atomic::AtomicU64,
    pub dlq_messages_failed: std::sync::atomic::AtomicU64,
    pub dlq_reprocessing_retries: std::sync::atomic::AtomicU64,
    pub dlq_consumer_restarts: std::sync::atomic::AtomicU64,
    pub dlq_processing_latency_ms: std::sync::atomic::AtomicU64,
}

impl DLQConsumer {
    pub fn new(
        kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
        config: DLQRecoveryConfig,
        processor: Arc<UltraOptimizedCDCEventProcessor>,
    ) -> Self {
        Self {
            kafka_consumer,
            config,
            processor,
            metrics: Arc::new(DLQRecoveryMetrics::default()),
            shutdown_tx: None,
            consumer_handle: None,
        }
    }

    /// Start the DLQ consumer for automatic recovery
    pub async fn start(&mut self) -> Result<()> {
        if !self.config.enable_auto_recovery {
            info!("DLQ auto-recovery is disabled");
            return Ok(());
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let kafka_consumer = self.kafka_consumer.clone();
        let config = self.config.clone();
        let processor = self.processor.clone();
        let metrics = self.metrics.clone();

        let handle = tokio::spawn(async move {
            info!("Starting DLQ consumer for topic: {}", config.dlq_topic);

            // Subscribe to DLQ topic
            if let Err(e) = kafka_consumer.subscribe_to_topic(&config.dlq_topic).await {
                error!("Failed to subscribe to DLQ topic: {}", e);
                return;
            }

            let mut interval =
                tokio::time::interval(Duration::from_secs(config.auto_recovery_interval_secs));

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("DLQ consumer received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = Self::process_dlq_batch(&kafka_consumer, &processor, &metrics, &config).await {
                            error!("DLQ batch processing failed: {}", e);
                            metrics.dlq_consumer_restarts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }

            info!("DLQ consumer stopped");
        });

        self.consumer_handle = Some(handle);
        info!("DLQ consumer started successfully");
        Ok(())
    }

    /// Stop the DLQ consumer
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }

        if let Some(handle) = self.consumer_handle.take() {
            handle.await?;
        }

        info!("DLQ consumer stopped");
        Ok(())
    }

    /// Process a batch of DLQ messages
    async fn process_dlq_batch(
        consumer: &crate::infrastructure::kafka_abstraction::KafkaConsumer,
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        metrics: &Arc<DLQRecoveryMetrics>,
        config: &DLQRecoveryConfig,
    ) -> Result<()> {
        let mut processed_count = 0;
        let start_time = Instant::now();

        for _ in 0..config.batch_size {
            match consumer.poll_dlq_message().await {
                Ok(Some(dlq_message)) => {
                    metrics
                        .dlq_messages_consumed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    if let Err(e) =
                        Self::reprocess_dlq_message(processor, &dlq_message, config).await
                    {
                        error!("Failed to reprocess DLQ message: {}", e);
                        metrics
                            .dlq_messages_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        metrics
                            .dlq_messages_reprocessed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        processed_count += 1;
                    }
                }
                Ok(None) => {
                    // No more messages available
                    break;
                }
                Err(e) => {
                    error!("Error polling DLQ message: {}", e);
                    break;
                }
            }
        }

        if processed_count > 0 {
            let latency = start_time.elapsed().as_millis() as u64;
            metrics
                .dlq_processing_latency_ms
                .store(latency, std::sync::atomic::Ordering::Relaxed);
            info!(
                "Processed {} DLQ messages in {}ms",
                processed_count, latency
            );
        }

        Ok(())
    }

    /// Reprocess a single DLQ message
    async fn reprocess_dlq_message(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        dlq_message: &crate::infrastructure::kafka_dlq::DeadLetterMessage,
        config: &DLQRecoveryConfig,
    ) -> Result<()> {
        // Convert DeadLetterMessage back to ProcessableEvent
        // Note: DeadLetterMessage contains events, not individual event data
        // We'll process the first event from the batch for simplicity
        if let Some(first_event) = dlq_message.events.first() {
            let event_id = Uuid::new_v4(); // Generate new event ID for reprocessing
            let payload = bincode::serialize(first_event)?;

            let processable_event = ProcessableEvent::new(
                event_id,
                dlq_message.account_id,
                format!("{:?}", std::mem::discriminant(first_event)),
                payload,
                None, // partition_key
            )?;

            let mut retries = 0;
            loop {
                match processor.try_process_event(&processable_event).await {
                    Ok(_) => {
                        info!(
                            "Successfully reprocessed DLQ message for account: {}",
                            dlq_message.account_id
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        retries += 1;
                        if retries > config.max_reprocessing_retries {
                            error!(
                                "Failed to reprocess DLQ message for account {} after {} retries: {}",
                                dlq_message.account_id, retries, e
                            );
                            return Err(e);
                        } else {
                            warn!(
                                "Retrying DLQ message for account {} (attempt {}): {}",
                                dlq_message.account_id, retries, e
                            );
                            tokio::time::sleep(Duration::from_millis(
                                config.reprocessing_backoff_ms * retries as u64,
                            ))
                            .await;
                        }
                    }
                }
            }
        } else {
            Err(anyhow::anyhow!("No events found in DLQ message"))
        }
    }

    /// Get DLQ recovery metrics
    pub fn get_metrics(&self) -> DLQRecoveryMetrics {
        DLQRecoveryMetrics {
            dlq_messages_consumed: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .dlq_messages_consumed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            dlq_messages_reprocessed: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .dlq_messages_reprocessed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            dlq_messages_failed: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .dlq_messages_failed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            dlq_reprocessing_retries: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .dlq_reprocessing_retries
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            dlq_consumer_restarts: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .dlq_consumer_restarts
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            dlq_processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.metrics
                    .dlq_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl UltraOptimizedCDCEventProcessor {
    /// Create a DLQ consumer for recovery processing
    pub fn create_dlq_consumer(&self, config: DLQRecoveryConfig) -> Result<DLQConsumer> {
        let kafka_config = crate::infrastructure::kafka_abstraction::KafkaConfig::default();
        let kafka_consumer =
            crate::infrastructure::kafka_abstraction::KafkaConsumer::new(kafka_config)?;

        Ok(DLQConsumer::new(
            kafka_consumer,
            config,
            Arc::new(self.clone()),
        ))
    }

    /// Manual DLQ reprocessing - process all available DLQ messages
    pub async fn reprocess_dlq(&self) -> Result<DLQRecoveryMetrics> {
        let config = DLQRecoveryConfig::default();
        let mut dlq_consumer = self.create_dlq_consumer(config.clone())?;

        info!("Starting manual DLQ reprocessing");

        // Process all available DLQ messages
        let mut total_processed = 0;
        let mut total_failed = 0;

        // Create a Kafka consumer for DLQ polling
        let kafka_config = crate::infrastructure::kafka_abstraction::KafkaConfig::default();
        let kafka_consumer =
            crate::infrastructure::kafka_abstraction::KafkaConsumer::new(kafka_config)?;

        // Subscribe to DLQ topic
        kafka_consumer.subscribe_to_dlq().await?;

        // Clone config to avoid move issues
        let config_clone = config.clone();

        loop {
            match kafka_consumer.poll_dlq_message().await {
                Ok(Some(dlq_message)) => {
                    match self
                        .reprocess_single_dlq_message(&dlq_message, &config_clone)
                        .await
                    {
                        Ok(_) => {
                            total_processed += 1;
                            info!(
                                "Successfully reprocessed DLQ message for account: {}",
                                dlq_message.account_id
                            );
                        }
                        Err(e) => {
                            total_failed += 1;
                            error!(
                                "Failed to reprocess DLQ message for account {}: {}",
                                dlq_message.account_id, e
                            );
                        }
                    }
                }
                Ok(None) => {
                    // No more messages
                    break;
                }
                Err(e) => {
                    error!("Error polling DLQ message: {}", e);
                    break;
                }
            }
        }

        info!(
            "Manual DLQ reprocessing completed: {} processed, {} failed",
            total_processed, total_failed
        );

        // Return metrics
        Ok(DLQRecoveryMetrics {
            dlq_messages_consumed: std::sync::atomic::AtomicU64::new(
                total_processed + total_failed,
            ),
            dlq_messages_reprocessed: std::sync::atomic::AtomicU64::new(total_processed),
            dlq_messages_failed: std::sync::atomic::AtomicU64::new(total_failed),
            dlq_reprocessing_retries: std::sync::atomic::AtomicU64::new(0),
            dlq_consumer_restarts: std::sync::atomic::AtomicU64::new(0),
            dlq_processing_latency_ms: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Reprocess a single DLQ message with retries
    async fn reprocess_single_dlq_message(
        &self,
        dlq_message: &crate::infrastructure::kafka_dlq::DeadLetterMessage,
        config: &DLQRecoveryConfig,
    ) -> Result<()> {
        // Convert DeadLetterMessage to ProcessableEvent
        // Note: DeadLetterMessage contains events, not individual event data
        if let Some(first_event) = dlq_message.events.first() {
            let event_id = Uuid::new_v4(); // Generate new event ID for reprocessing
            let payload = bincode::serialize(first_event)?;

            let processable_event = ProcessableEvent::new(
                event_id,
                dlq_message.account_id,
                format!("{:?}", std::mem::discriminant(first_event)),
                payload,
                None,
            )?;

            let mut retries = 0;
            loop {
                match self.try_process_event(&processable_event).await {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        retries += 1;
                        if retries > config.max_reprocessing_retries {
                            return Err(e);
                        } else {
                            warn!(
                                "Retrying DLQ message for account {} (attempt {}): {}",
                                dlq_message.account_id, retries, e
                            );
                            tokio::time::sleep(Duration::from_millis(
                                config.reprocessing_backoff_ms * retries as u64,
                            ))
                            .await;
                        }
                    }
                }
            }
        } else {
            Err(anyhow::anyhow!("No events found in DLQ message"))
        }
    }

    /// Get DLQ statistics
    pub async fn get_dlq_stats(&self) -> Result<serde_json::Value> {
        // This would typically query the DLQ topic/table for statistics
        // For now, return a placeholder
        Ok(serde_json::json!({
            "dlq_topic": "banking-es-dlq",
            "total_messages": 0, // Would query actual count
            "oldest_message_age_seconds": 0, // Would calculate from timestamps
            "recovery_enabled": false,
            "last_recovery_attempt": null,
        }))
    }
}
