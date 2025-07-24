use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics;
use crate::infrastructure::consistency_manager::ConsistencyManager;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use anyhow::Result;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use hostname;
use rdkafka::Message;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64};
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
        let domain_event = match bincode::deserialize(&payload) {
            Ok(event) => {
                tracing::debug!(
                    "Successfully deserialized domain event for event_id {}",
                    event_id
                );
                event
            }
            Err(e) => {
                tracing::error!(
                    "Failed to deserialize domain event for event_id {}: {}",
                    event_id,
                    e
                );
                return Err(anyhow::anyhow!("Bincode deserialize error: {}", e));
            }
        };

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

// Enhanced circuit breaker with more states and configuration
#[derive(Debug, Clone)]
enum AdvancedCircuitBreakerState {
    Closed,
    Open {
        until: Instant,
        failure_count: u32,
    },
    HalfOpen {
        test_requests: u32,
        success_count: u32,
    },
}

impl Default for AdvancedCircuitBreakerState {
    fn default() -> Self {
        AdvancedCircuitBreakerState::Closed
    }
}

struct AdvancedCircuitBreaker {
    state: Arc<RwLock<AdvancedCircuitBreakerState>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    half_open_success_threshold: u32,
}

impl AdvancedCircuitBreaker {
    fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(AdvancedCircuitBreakerState::Closed)),
            failure_threshold: 10,                     // Increased threshold
            recovery_timeout: Duration::from_secs(60), // Increased timeout
            half_open_success_threshold: 5,
        }
    }

    async fn can_execute(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            AdvancedCircuitBreakerState::Closed => true,
            AdvancedCircuitBreakerState::Open { until, .. } => Instant::now() >= until,
            AdvancedCircuitBreakerState::HalfOpen { .. } => true, // Allow requests in half-open state
        }
    }

    async fn on_success(&self) {
        let mut state = self.state.write().await;
        match *state {
            AdvancedCircuitBreakerState::HalfOpen {
                ref mut success_count,
                ..
            } => {
                *success_count += 1;
                if *success_count >= self.half_open_success_threshold {
                    *state = AdvancedCircuitBreakerState::Closed;
                }
            }
            _ => *state = AdvancedCircuitBreakerState::Closed,
        }
    }

    async fn on_failure(&self) {
        let mut state = self.state.write().await;
        match *state {
            AdvancedCircuitBreakerState::Closed => {
                *state = AdvancedCircuitBreakerState::Open {
                    until: Instant::now() + self.recovery_timeout,
                    failure_count: 1,
                };
            }
            AdvancedCircuitBreakerState::Open {
                ref mut failure_count,
                ..
            } => {
                *failure_count += 1;
                let backoff = Duration::from_secs(
                    (self.recovery_timeout.as_secs() * 2_u64.pow((*failure_count).min(8))).min(600), // Max 10 minutes
                );
                *state = AdvancedCircuitBreakerState::Open {
                    until: Instant::now() + backoff,
                    failure_count: *failure_count,
                };
            }
            AdvancedCircuitBreakerState::HalfOpen { .. } => {
                *state = AdvancedCircuitBreakerState::Open {
                    until: Instant::now() + self.recovery_timeout,
                    failure_count: 1,
                };
            }
        }
    }

    async fn on_attempt(&self) {
        let mut state = self.state.write().await;
        if let AdvancedCircuitBreakerState::Open { until, .. } = *state {
            if Instant::now() >= until {
                *state = AdvancedCircuitBreakerState::HalfOpen {
                    test_requests: 1,
                    success_count: 0,
                };
            }
        } else if let AdvancedCircuitBreakerState::HalfOpen {
            ref mut test_requests,
            ..
        } = *state
        {
            *test_requests += 1;
        }
    }
}

impl Clone for AdvancedCircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            failure_threshold: self.failure_threshold,
            recovery_timeout: self.recovery_timeout,
            half_open_success_threshold: self.half_open_success_threshold,
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
    dlq_router: Arc<crate::infrastructure::dlq_router::dlq_router::DLQRouter>,
    max_retries: u32,
    retry_backoff_ms: u64,
    cache_service: Arc<dyn CacheServiceTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    metrics: Arc<EnhancedCDCMetrics>,
    consistency_manager: Option<Arc<ConsistencyManager>>,

    // High-performance processing
    processing_semaphore: Arc<Semaphore>,
    batch_queue: Arc<Mutex<Vec<ProcessableEvent>>>,
    batch_size: Arc<Mutex<usize>>, // Made mutable for adaptive sizing
    batch_timeout: Duration,

    // Circuit breaker
    circuit_breaker: AdvancedCircuitBreaker,

    // Memory-efficient projection cache
    projection_cache: Arc<Mutex<ProjectionCache>>,

    // Batch processing coordination with interior mutability
    batch_processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_tx: Arc<Mutex<Option<mpsc::Sender<()>>>>,

    // Business logic configuration with interior mutability
    business_config: Arc<Mutex<BusinessLogicConfig>>,

    // Advanced performance features
    performance_config: Arc<Mutex<AdvancedPerformanceConfig>>,
    performance_profiler: Arc<Mutex<PerformanceProfiler>>,

    // Adaptive concurrency control
    adaptive_concurrency: Arc<Mutex<usize>>,
    backpressure_signal: Arc<AtomicBool>,

    // Memory management
    memory_monitor: Arc<Mutex<MemoryMonitor>>,

    // Performance monitoring
    performance_monitor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    // Placeholder for a real cluster manager implementation
    cluster_manager: Arc<Mutex<Option<()>>>,

    // Phase 2.3: Advanced Monitoring
    monitoring_system: Arc<Mutex<Option<AdvancedMonitoringSystem>>>,
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
            batch_processing_enabled: false, // Changed: require explicit start
        }
    }
}

impl Clone for UltraOptimizedCDCEventProcessor {
    fn clone(&self) -> Self {
        Self {
            kafka_producer: self.kafka_producer.clone(),
            dlq_router: self.dlq_router.clone(),
            max_retries: self.max_retries,
            retry_backoff_ms: self.retry_backoff_ms,
            cache_service: self.cache_service.clone(),
            projection_store: self.projection_store.clone(),
            metrics: self.metrics.clone(),
            consistency_manager: self.consistency_manager.clone(),
            processing_semaphore: self.processing_semaphore.clone(),
            batch_queue: self.batch_queue.clone(),
            batch_size: self.batch_size.clone(),
            batch_timeout: self.batch_timeout,
            circuit_breaker: self.circuit_breaker.clone(),
            projection_cache: self.projection_cache.clone(),
            batch_processor_handle: Arc::new(Mutex::new(None)), // Don't clone the handle
            shutdown_tx: Arc::new(Mutex::new(None)),            // Don't clone the sender
            business_config: Arc::new(Mutex::new(BusinessLogicConfig::default())), // Use default instead of trying to clone
            performance_config: self.performance_config.clone(),
            performance_profiler: self.performance_profiler.clone(),
            adaptive_concurrency: self.adaptive_concurrency.clone(),
            backpressure_signal: self.backpressure_signal.clone(),
            memory_monitor: self.memory_monitor.clone(),
            performance_monitor_handle: Arc::new(Mutex::new(None)), // Don't clone the handle
            cluster_manager: Arc::new(Mutex::new(None)), // Don't clone the cluster manager
            monitoring_system: self.monitoring_system.clone(),
        }
    }
}

impl UltraOptimizedCDCEventProcessor {
    pub fn new(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        metrics: Arc<EnhancedCDCMetrics>,
        business_config: Option<BusinessLogicConfig>,
        performance_config: Option<AdvancedPerformanceConfig>,
        consistency_manager: Option<Arc<ConsistencyManager>>,
    ) -> Self {
        let business_config = business_config.unwrap_or_default();
        let performance_config = performance_config.unwrap_or_default();

        // 1. Increase cache size and TTL with advanced configuration
        let projection_cache = Arc::new(Mutex::new(ProjectionCache::new(
            50000,                     // Max 50k cached projections
            Duration::from_secs(1800), // 30 minute TTL
        )));

        let batch_timeout = Duration::from_millis(performance_config.target_batch_time_ms);
        let initial_batch_size = if business_config.batch_processing_enabled {
            performance_config.min_batch_size
        } else {
            // When batch processing is disabled, use a reasonable batch size
            // to avoid processing single events immediately
            performance_config.min_batch_size.max(10)
        };

        Self {
            kafka_producer: kafka_producer.clone(),
            dlq_router: Arc::new(
                crate::infrastructure::dlq_router::dlq_router::DLQRouter::new(Arc::new(
                    kafka_producer,
                )),
            ),
            max_retries: 3,
            retry_backoff_ms: 100,
            cache_service,
            projection_store,
            metrics,
            consistency_manager, // Use the provided consistency manager
            processing_semaphore: Arc::new(Semaphore::new(performance_config.min_concurrency)),
            batch_queue: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            batch_size: Arc::new(Mutex::new(initial_batch_size)),
            batch_timeout,
            circuit_breaker: AdvancedCircuitBreaker::new(),
            projection_cache,
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            business_config: Arc::new(Mutex::new(business_config)),
            performance_config: Arc::new(Mutex::new(performance_config.clone())),
            performance_profiler: Arc::new(Mutex::new(PerformanceProfiler::new(100))),
            adaptive_concurrency: Arc::new(Mutex::new(performance_config.min_concurrency)),
            backpressure_signal: Arc::new(AtomicBool::new(false)),
            memory_monitor: Arc::new(Mutex::new(MemoryMonitor::new(
                performance_config.memory_pressure_threshold,
                performance_config.gc_trigger_threshold,
            ))),
            performance_monitor_handle: Arc::new(Mutex::new(None)),

            // Phase 2.2: Scalability Enhancements
            cluster_manager: Arc::new(Mutex::new(None)),
            monitoring_system: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start_batch_processor(&mut self) -> Result<()> {
        // Check if batch processor is already running
        if self.batch_processor_handle.lock().await.is_some() {
            return Ok(()); // Already running
        }

        // Check if batch processing is enabled in config
        if !self.business_config.lock().await.batch_processing_enabled {
            return Err(anyhow::anyhow!("Batch processing is disabled in configuration. Enable it first with update_business_config()"));
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        let batch_queue = Arc::clone(&self.batch_queue);
        let projection_store = Arc::clone(&self.projection_store);
        let projection_cache = Arc::clone(&self.projection_cache);
        let cache_service = Arc::clone(&self.cache_service);
        let metrics = Arc::clone(&self.metrics);
        let batch_size = *self.batch_size.lock().await;
        let batch_timeout = self.batch_timeout;
        let consistency_manager = self.consistency_manager.clone();

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
                            consistency_manager.as_ref(),
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

        *self.batch_processor_handle.lock().await = Some(handle);

        // Start performance monitor if enabled
        self.start_performance_monitor().await?;

        // Start periodic flush task for when batch processing is disabled
        self.start_periodic_flush_task().await?;

        info!("Batch processor started successfully");
        Ok(())
    }

    /// Check if the batch processor is currently running
    pub async fn is_batch_processor_running(&self) -> bool {
        self.batch_processor_handle.lock().await.is_some()
    }

    /// Enable batch processing and start the processor
    pub async fn enable_and_start_batch_processor(&mut self) -> Result<()> {
        // Update configuration to enable batch processing
        let mut config = self.business_config.lock().await.clone();
        config.batch_processing_enabled = true;
        *self.business_config.lock().await = config;

        // Start the batch processor
        self.start_batch_processor().await
    }

    /// Ultra-fast event processing with business logic validation
    #[instrument(skip(self, cdc_event))]
    pub async fn process_cdc_event_ultra_fast(&self, cdc_event: serde_json::Value) -> Result<()> {
        let start_time = Instant::now();
        if !self.circuit_breaker.can_execute().await {
            return Err(anyhow::anyhow!("Circuit breaker is open"));
        }
        self.circuit_breaker.on_attempt().await;

        // Try serde_json first for debugging
        let processable_event = match self.extract_event_serde_json(&cdc_event)? {
            Some(event) => {
                tracing::debug!(
                    "🔍 Successfully extracted event with serde_json: {}",
                    event.event_id
                );
                event
            }
            None => {
                tracing::debug!("🔍 No event to process (tombstone or empty)");
                return Ok(());
            }
        };

        let mut retries = 0;
        loop {
            match self.try_process_event(&processable_event).await {
                Ok(_) => {
                    self.circuit_breaker.on_success().await;
                    tracing::debug!(
                        "🔍 Event processed successfully: {}",
                        processable_event.event_id
                    );

                    // Mark projection completed immediately after successful processing
                    if let Some(consistency_manager) = &self.consistency_manager {
                        tracing::info!("CDC Event Processor: Marking projection completed for aggregate {} (immediate)", processable_event.aggregate_id);
                        consistency_manager
                            .mark_projection_completed(processable_event.aggregate_id)
                            .await;
                        tracing::info!("CDC Event Processor: Successfully marked projection completed for aggregate {} (immediate)", processable_event.aggregate_id);

                        // Also mark CDC processing as completed
                        tracing::info!("CDC Event Processor: Marking CDC completed for aggregate {} (immediate)", processable_event.aggregate_id);
                        consistency_manager
                            .mark_completed(processable_event.aggregate_id)
                            .await;
                        tracing::info!("CDC Event Processor: Successfully marked CDC completed for aggregate {} (immediate)", processable_event.aggregate_id);
                    }

                    break;
                }
                Err(e) => {
                    retries += 1;
                    if retries > self.max_retries {
                        self.metrics
                            .events_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.send_to_dlq(
                            &processable_event,
                            &e,
                            retries,
                            crate::infrastructure::dlq_router::dlq_router::DLQErrorType::Unknown,
                        )
                        .await?;
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
        let aggregate_id = processable_event.aggregate_id;

        // Business logic validation
        if self.business_config.lock().await.enable_validation {
            if let Err(e) = self.validate_business_logic(processable_event).await {
                self.metrics
                    .events_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(
                    "Business validation failed for event {}: {}",
                    processable_event.event_id, e
                );
                self.send_to_dlq(
                    processable_event,
                    &e,
                    0,
                    crate::infrastructure::dlq_router::dlq_router::DLQErrorType::Validation,
                )
                .await?;
                // Mark as failed in consistency manager
                self.mark_aggregate_failed(aggregate_id, e.to_string())
                    .await;
                return Err(e);
            }
        }
        // Duplicate detection
        if self.business_config.lock().await.enable_duplicate_detection {
            let mut cache_guard = self.projection_cache.lock().await;
            if cache_guard
                .is_duplicate_event(&processable_event.event_id, &processable_event.aggregate_id)
            {
                self.metrics
                    .consecutive_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!("Skipping duplicate event: {}", processable_event.event_id);
                // Mark as completed for duplicate events (they were already processed)
                self.mark_aggregate_completed(aggregate_id).await;
                return Ok(());
            }
        }
        let _permit = self.processing_semaphore.acquire().await?;
        {
            let mut queue = self.batch_queue.lock().await;
            queue.push(processable_event.clone());

            // Process batch when it reaches the configured size
            // This allows batching even when batch processing is disabled
            if queue.len() >= *self.batch_size.lock().await {
                drop(queue);
                if let Err(e) = self.process_current_batch().await {
                    // Mark as failed if batch processing fails
                    self.mark_aggregate_failed(aggregate_id, e.to_string())
                        .await;
                    return Err(e);
                }
                // Mark as completed if batch processing succeeds
                self.mark_aggregate_completed(aggregate_id).await;
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
                if *amount > self.business_config.lock().await.max_transaction_amount {
                    return Err(anyhow::anyhow!("Deposit amount exceeds maximum allowed"));
                }
            }
            crate::domain::AccountEvent::MoneyWithdrawn { amount, .. } => {
                if *amount <= Decimal::ZERO {
                    return Err(anyhow::anyhow!("Withdrawal amount must be positive"));
                }
                if *amount > self.business_config.lock().await.max_transaction_amount {
                    return Err(anyhow::anyhow!("Withdrawal amount exceeds maximum allowed"));
                }
            }
            crate::domain::AccountEvent::AccountCreated {
                initial_balance, ..
            } => {
                if *initial_balance < self.business_config.lock().await.min_balance {
                    return Err(anyhow::anyhow!("Initial balance cannot be negative"));
                }
                if *initial_balance > self.business_config.lock().await.max_balance {
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
        let batch_size = *self.batch_size.lock().await;
        Self::process_batch(
            &self.batch_queue,
            &self.projection_store,
            &self.projection_cache,
            &self.cache_service,
            &self.metrics,
            self.consistency_manager.as_ref(),
            batch_size,
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
        consistency_manager: Option<&Arc<ConsistencyManager>>,
        batch_size: usize,
    ) -> Result<()> {
        let events = Self::drain_batch_queue(batch_queue, batch_size).await;
        if events.is_empty() {
            return Ok(());
        }

        let batch_start = Instant::now();
        let events_count = events.len();
        Self::update_queue_depth_metric(batch_queue, metrics).await;

        let events_by_aggregate = Self::group_events_by_aggregate(events);
        let (updated_projections, processed_aggregates) = Self::process_events_for_aggregates(
            events_by_aggregate,
            projection_cache,
            cache_service,
            projection_store,
            metrics,
        )
        .await?;

        if !updated_projections.is_empty() {
            let upsert_result = Self::upsert_projections_in_parallel(
                updated_projections,
                projection_store,
                metrics,
            )
            .await;
            Self::handle_upsert_results(
                upsert_result,
                processed_aggregates,
                consistency_manager,
                metrics,
            )
            .await;
        }

        Self::update_batch_metrics(batch_start, events_count, metrics);
        Ok(())
    }

    /// Drains the batch queue, returning a vector of processable events.
    async fn drain_batch_queue(
        batch_queue: &Arc<Mutex<Vec<ProcessableEvent>>>,
        batch_size: usize,
    ) -> Vec<ProcessableEvent> {
        let mut queue = batch_queue.lock().await;
        if queue.is_empty() {
            return Vec::new();
        }
        let drain_size = queue.len().min(batch_size);
        queue.drain(..drain_size).collect()
    }

    /// Updates the queue depth metric.
    async fn update_queue_depth_metric(
        batch_queue: &Arc<Mutex<Vec<ProcessableEvent>>>,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) {
        let queue_depth = batch_queue.lock().await.len();
        metrics
            .queue_depth
            .store(queue_depth as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Groups a vector of events by their aggregate ID.
    fn group_events_by_aggregate(
        events: Vec<ProcessableEvent>,
    ) -> HashMap<Uuid, Vec<ProcessableEvent>> {
        let mut events_by_aggregate: HashMap<Uuid, Vec<ProcessableEvent>> = HashMap::new();
        for event in events {
            events_by_aggregate
                .entry(event.aggregate_id)
                .or_default()
                .push(event);
        }
        events_by_aggregate
    }

    /// Processes events for each aggregate, returning updated projections and processed aggregate IDs.
    async fn process_events_for_aggregates(
        events_by_aggregate: HashMap<Uuid, Vec<ProcessableEvent>>,
        projection_cache: &Arc<Mutex<ProjectionCache>>,
        cache_service: &Arc<dyn CacheServiceTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Result<(
        Vec<crate::infrastructure::projections::AccountProjection>,
        Vec<Uuid>,
    )> {
        let mut updated_projections = Vec::new();
        let mut processed_aggregates = Vec::new();
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

            for event in aggregate_events {
                if let Err(e) = Self::apply_event_to_projection(&mut projection, &event) {
                    error!(
                        "Failed to apply event {} to projection: {}",
                        event.event_id, e
                    );
                    metrics
                        .events_failed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metrics
                        .consecutive_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                } else {
                    metrics
                        .consecutive_failures
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                }
            }

            cache_guard.put(aggregate_id, projection.clone());
            updated_projections.push(Self::projection_cache_to_db_projection(
                &projection,
                aggregate_id,
            ));
            processed_aggregates.push(aggregate_id);
        }

        Ok((updated_projections, processed_aggregates))
    }

    /// Upserts projections to the database in parallel.
    async fn upsert_projections_in_parallel(
        projections: Vec<crate::infrastructure::projections::AccountProjection>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> HashMap<Uuid, bool> {
        let upsert_start = Instant::now();
        let max_concurrent = (projections.len() / 50).max(2).min(16);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        let chunk_size = (projections.len() / max_concurrent).max(1);

        let mut tasks = Vec::new();
        for chunk in projections.chunks(chunk_size) {
            let store = projection_store.clone();
            let chunk_vec = chunk.to_vec();
            let semaphore = semaphore.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire().await;
                let mut last_err = None;
                for _ in 0..3 {
                    match store.upsert_accounts_batch(chunk_vec.clone()).await {
                        Ok(_) => return Ok(chunk_vec.iter().map(|p| p.id).collect::<Vec<_>>()),
                        Err(e) => last_err = Some(e),
                    }
                }
                Err(anyhow::anyhow!(
                    "Upsert failed after 3 retries: {}",
                    last_err.unwrap()
                ))
            }));
        }

        let results = futures::future::join_all(tasks).await;
        let upsert_time = upsert_start.elapsed().as_millis() as u64;
        metrics
            .processing_latency_ms
            .store(upsert_time, std::sync::atomic::Ordering::Relaxed);

        let mut success_map = HashMap::new();
        for res in results {
            match res {
                Ok(Ok(ids)) => {
                    for id in ids {
                        success_map.insert(id, true);
                    }
                }
                Ok(Err(e)) => error!("Failed to bulk update projections in chunk: {}", e),
                Err(e) => error!("Join error in parallel upsert: {}", e),
            }
        }
        success_map
    }

    /// Handles the results of the parallel upsert operation.
    async fn handle_upsert_results(
        upsert_results: HashMap<Uuid, bool>,
        processed_aggregates: Vec<Uuid>,
        consistency_manager: Option<&Arc<ConsistencyManager>>,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) {
        for aggregate_id in processed_aggregates {
            let success = upsert_results.get(&aggregate_id).copied().unwrap_or(false);
            if let Some(cm) = consistency_manager {
                if success {
                    cm.mark_projection_completed(aggregate_id).await;
                    cm.mark_completed(aggregate_id).await;
                } else {
                    let err_msg = "DB upsert failed".to_string();
                    cm.mark_projection_failed(aggregate_id, err_msg.clone())
                        .await;
                    cm.mark_failed(aggregate_id, err_msg).await;
                }
            }
            if !success {
                metrics
                    .events_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Updates batch processing metrics.
    fn update_batch_metrics(
        batch_start: Instant,
        events_count: usize,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) {
        let batch_time = batch_start.elapsed().as_millis() as u64;
        metrics
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics
            .events_processed
            .fetch_add(events_count as u64, std::sync::atomic::Ordering::Relaxed);
        metrics
            .avg_batch_size
            .store(events_count as u64, std::sync::atomic::Ordering::Relaxed);
        if batch_time > 0 {
            let throughput = (events_count as f64 / (batch_time as f64 / 1000.0)) as u64;
            metrics
                .throughput_per_second
                .store(throughput, std::sync::atomic::Ordering::Relaxed);
        }
        if metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            metrics.last_error_time.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        tracing::debug!(
            "Optimized batch processing: {} events in {}ms, throughput: {}/s",
            events_count,
            batch_time,
            metrics
                .throughput_per_second
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    /// Get projection from cache or load from database
    async fn get_or_load_projection(
        cache: &mut ProjectionCache,
        cache_service: &Arc<dyn CacheServiceTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        aggregate_id: Uuid,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Result<ProjectionCacheEntry> {
        if let Some(cached) = Self::get_from_l1_cache(cache, &aggregate_id, metrics).await {
            return Ok(cached);
        }
        if let Some(cached) =
            Self::get_from_l2_cache(cache, cache_service, aggregate_id, metrics).await?
        {
            return Ok(cached);
        }
        Self::load_from_db_and_populate_caches(
            cache,
            cache_service,
            projection_store,
            aggregate_id,
            metrics,
        )
        .await
    }

    /// Attempts to retrieve a projection from the L1 cache.
    async fn get_from_l1_cache(
        cache: &mut ProjectionCache,
        aggregate_id: &Uuid,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Option<ProjectionCacheEntry> {
        if let Some(cached) = cache.get(aggregate_id) {
            metrics
                .batches_processed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!("ProjectionCache: L1 cache hit for {}", aggregate_id);
            return Some(cached.clone());
        }
        metrics
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!("ProjectionCache: L1 cache miss for {}", aggregate_id);
        None
    }

    /// Attempts to retrieve a projection from the L2 cache (Redis).
    async fn get_from_l2_cache(
        cache: &mut ProjectionCache,
        cache_service: &Arc<dyn CacheServiceTrait>,
        aggregate_id: Uuid,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Result<Option<ProjectionCacheEntry>> {
        if let Ok(Some(redis_proj)) = cache_service.get_account(aggregate_id).await {
            tracing::debug!("ProjectionCache: L2 (Redis) cache hit for {}", aggregate_id);
            let entry = ProjectionCacheEntry {
                balance: redis_proj.balance,
                owner_name: redis_proj.owner_name.clone(),
                is_active: redis_proj.is_active,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            };
            cache.put(aggregate_id, entry.clone());
            // Pre-fetch related projections
            let proj = crate::infrastructure::projections::AccountProjection {
                id: aggregate_id,
                owner_name: entry.owner_name.clone(),
                balance: entry.balance,
                is_active: entry.is_active,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            Self::prefetch_related_projections(cache_service, &proj).await;
            return Ok(Some(entry));
        }
        tracing::debug!(
            "ProjectionCache: L2 (Redis) cache miss for {}",
            aggregate_id
        );
        Ok(None)
    }

    /// Pre-fetches related projections to warm up the cache.
    async fn prefetch_related_projections(
        cache_service: &Arc<dyn CacheServiceTrait>,
        projection: &crate::infrastructure::projections::AccountProjection,
    ) {
        // In a real application, you would have some logic to determine related projections.
        // For this example, we'll just pre-fetch a few other accounts.
        for i in 0..3 {
            let _ = cache_service.get_account(Uuid::new_v4()).await;
        }
    }

    /// Loads a projection from the database and populates the L1 and L2 caches.
    async fn load_from_db_and_populate_caches(
        cache: &mut ProjectionCache,
        cache_service: &Arc<dyn CacheServiceTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        aggregate_id: Uuid,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Result<ProjectionCacheEntry> {
        let db_projection = projection_store.get_account(aggregate_id).await?;
        let cache_entry = if let Some(proj) = db_projection {
            ProjectionCacheEntry {
                balance: proj.balance,
                owner_name: proj.owner_name.clone(),
                is_active: proj.is_active,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            }
        } else {
            ProjectionCacheEntry {
                balance: Decimal::ZERO,
                owner_name: String::new(),
                is_active: true,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            }
        };
        let account = crate::domain::Account {
            id: aggregate_id,
            owner_name: cache_entry.owner_name.clone(),
            balance: cache_entry.balance,
            is_active: cache_entry.is_active,
            version: 1,
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
        // Convert serde_json::Value to string, then to simd-json OwnedValue for fast field access
        let cdc_event_str = serde_json::to_string(cdc_event)?;
        let mut cdc_event_bytes = cdc_event_str.as_bytes().to_vec();
        let cdc_event_simd: OwnedValue = simd_json::to_owned_value(&mut cdc_event_bytes)?;

        // With SMT, the data is directly in the payload field
        let event_data = match &cdc_event_simd["payload"] {
            simd_json::OwnedValue::Object(map) => map,
            simd_json::OwnedValue::Static(simd_json::StaticNode::Null) => {
                return Ok(None); // Skip tombstone events
            }
            _ => return Err(anyhow::anyhow!("Missing or invalid payload")),
        };

        // Check for __deleted flag (SMT adds this for DELETE operations)
        let is_deleted = event_data
            .get("__deleted")
            .and_then(|v| match v {
                simd_json::OwnedValue::String(s) => Some(s.as_str() == "true"),
                _ => None,
            })
            .unwrap_or(false);

        if is_deleted {
            tracing::info!(
                "CDC SKIP: Deleted event for aggregate_id={:?}",
                event_data.get("aggregate_id")
            );
            return Ok(None); // Skip deleted events
        }

        let event_id = event_data
            .get("event_id")
            .and_then(|v| match v {
                simd_json::OwnedValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid event_id"))?;
        let aggregate_id = event_data
            .get("aggregate_id")
            .and_then(|v| match v {
                simd_json::OwnedValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid aggregate_id"))?;
        let event_type = event_data
            .get("event_type")
            .and_then(|v| match v {
                simd_json::OwnedValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("Missing event_type"))?
            .to_string();
        tracing::info!(
            "CDC EXTRACTED: aggregate_id={}, event_id={}, event_type={}",
            aggregate_id,
            event_id,
            event_type
        );
        let payload_str = event_data
            .get("payload")
            .and_then(|v| match v {
                simd_json::OwnedValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;
        let payload_bytes = BASE64_STANDARD.decode(payload_str)?;
        let partition_key = event_data
            .get("partition_key")
            .and_then(|v| match v {
                simd_json::OwnedValue::String(s) => Some(s.as_str()),
                _ => None,
            })
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

    /// Extract event with serde_json fallback (for debugging)
    fn extract_event_serde_json(
        &self,
        cdc_event: &serde_json::Value,
    ) -> Result<Option<ProcessableEvent>> {
        tracing::debug!(
            "🔍 extract_event_serde_json: Processing CDC event: {:?}",
            cdc_event
        );

        // Try to extract the payload - with SMT, the data is directly in the payload field
        let event_data = cdc_event
            .get("payload")
            .ok_or_else(|| anyhow::anyhow!("Missing payload field"))?;

        // Check if this is a tombstone (null payload)
        if event_data.is_null() {
            tracing::info!("CDC SKIP: Tombstone event");
            return Ok(None);
        }

        // Check for __deleted flag (SMT adds this for DELETE operations)
        let is_deleted = event_data
            .get("__deleted")
            .and_then(|v| v.as_str())
            .map(|s| s == "true")
            .unwrap_or(false);

        if is_deleted {
            tracing::info!(
                "CDC SKIP: Deleted event for aggregate_id={:?}",
                event_data.get("aggregate_id")
            );
            return Ok(None);
        }

        tracing::debug!("🔍 Event data: {:?}", event_data);

        // Extract fields directly from the unwrapped payload
        let event_id = event_data
            .get("event_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid event_id"))?;

        let aggregate_id = event_data
            .get("aggregate_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid aggregate_id"))?;

        let event_type = event_data
            .get("event_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing event_type"))?
            .to_string();
        tracing::info!(
            "CDC EXTRACTED: aggregate_id={}, event_id={}, event_type={}",
            aggregate_id,
            event_id,
            event_type
        );

        let payload_str = event_data
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;

        let payload_bytes = match BASE64_STANDARD.decode(payload_str) {
            Ok(bytes) => {
                tracing::debug!(
                    "Decoded base64 payload for event_id {}: {} bytes (first 16: {:?})",
                    event_id,
                    bytes.len(),
                    &bytes[..bytes.len().min(16)]
                );
                bytes
            }
            Err(e) => {
                tracing::error!(
                    "Failed to base64 decode payload for event_id {}: {}",
                    event_id,
                    e
                );
                return Err(anyhow::anyhow!("Base64 decode error: {}", e));
            }
        };
        let domain_event_result =
            bincode::deserialize::<crate::domain::AccountEvent>(&payload_bytes);
        if domain_event_result.is_err() {
            let this = self.clone();
            let event_type_clone = event_type.clone();
            let payload_bytes_clone = payload_bytes.clone();
            tokio::spawn(async move {
                let _ = this
                    .send_to_dlq(
                        &ProcessableEvent {
                            event_id,
                            aggregate_id,
                            event_type: event_type_clone,
                            payload: payload_bytes_clone,
                            partition_key: None,
                            domain_event: None,
                        },
                        &anyhow::anyhow!("Bincode deserialize error"),
                        0,
                        crate::infrastructure::dlq_router::dlq_router::DLQErrorType::Deserialization,
                    )
                    .await;
            });
        }
        let partition_key = event_data
            .get("partition_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        tracing::debug!(
            "🔍 Extracted event - ID: {}, Aggregate: {}, Type: {}",
            event_id,
            aggregate_id,
            event_type
        );

        ProcessableEvent::new(
            event_id,
            aggregate_id,
            event_type,
            payload_bytes,
            partition_key,
        )
        .map(Some)
    }

    /// Get comprehensive metrics for monitoring and observability
    pub async fn get_metrics(&self) -> EnhancedCDCMetrics {
        // Update metrics with current state
        let queue_depth = self.batch_queue.lock().await.len();
        self.metrics
            .queue_depth
            .store(queue_depth as u64, std::sync::atomic::Ordering::Relaxed);

        let memory_usage = self.get_memory_usage().await;
        self.metrics
            .memory_usage_bytes
            .store(memory_usage as u64, std::sync::atomic::Ordering::Relaxed);

        let active_connections = self.processing_semaphore.available_permits();
        self.metrics.active_connections.store(
            active_connections as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Calculate error rate
        let events_processed = self
            .metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let events_failed = self
            .metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_events = events_processed + events_failed;
        let error_rate = if total_events > 0 {
            ((events_failed as f64 / total_events as f64) * 100.0) as u64
        } else {
            0
        };
        self.metrics
            .error_rate
            .store(error_rate, std::sync::atomic::Ordering::Relaxed);

        // Get business config status
        let business_config = self.business_config.lock().await;
        let batch_processing_enabled = business_config.batch_processing_enabled;
        drop(business_config);

        // Get batch processor status
        let batch_processor_running = self.batch_processor_handle.lock().await.is_some();

        // Log comprehensive metrics
        tracing::info!(
            "CDC Event Processor Metrics - Events: {}/{} ({}% error rate), Batches: {}, Throughput: {}/s, Queue: {}, Memory: {}MB, Active: {}, BatchProcessor: {}",
            events_processed,
            total_events,
            error_rate,
            self.metrics.batches_processed.load(std::sync::atomic::Ordering::Relaxed),
            self.metrics.throughput_per_second.load(std::sync::atomic::Ordering::Relaxed),
            queue_depth,
            memory_usage / 1024 / 1024,
            active_connections,
            if batch_processor_running { "Running" } else { "Stopped" }
        );

        // Clone the metrics struct to return it
        self.metrics.as_ref().clone()
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(()).await;
        }

        if let Some(handle) = self.batch_processor_handle.lock().await.take() {
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
    pub async fn get_business_config(&self) -> BusinessLogicConfig {
        self.business_config.lock().await.clone()
    }

    /// Update business logic configuration
    pub async fn update_business_config(&mut self, config: BusinessLogicConfig) {
        *self.business_config.lock().await = config;
    }

    pub async fn send_to_dlq_from_cdc(
        &self,
        message: &rdkafka::message::BorrowedMessage<'_>,
        error: &str,
    ) -> Result<()> {
        let key = message
            .key()
            .map(|k| String::from_utf8_lossy(k).to_string());
        let payload = message.payload().unwrap_or_default();
        self.dlq_router
            .route(
                crate::infrastructure::dlq_router::dlq_router::DLQErrorType::Unknown,
                payload,
                &key.unwrap_or_default(),
            )
            .await
    }

    async fn send_to_dlq(
        &self,
        event: &ProcessableEvent,
        error: &anyhow::Error,
        retry_count: u32,
        error_type: crate::infrastructure::dlq_router::dlq_router::DLQErrorType,
    ) -> Result<()> {
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
        let key = event.aggregate_id.to_string();
        self.dlq_router.route(error_type, &payload, &key).await
    }

    /// Send to DLQ using primitive fields (for batch DLQ from consumer)
    pub async fn send_to_dlq_from_cdc_parts(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        payload: &[u8],
        key: Option<&[u8]>,
        error: &str,
    ) -> Result<()> {
        // Compose a DLQ event (you can expand this as needed)
        let dlq_event = crate::infrastructure::cdc_event_processor::DLQEvent {
            event_id: Uuid::new_v4(),
            aggregate_id: Uuid::nil(),
            event_type: format!("DLQ_{}_{}_{}", topic, partition, offset),
            payload: payload.to_vec(),
            error: error.to_string(),
            timestamp: chrono::Utc::now(),
            retry_count: 0,
        };
        let payload = serde_json::to_vec(&dlq_event)?;
        let key_str = key
            .map(|k| String::from_utf8_lossy(k).to_string())
            .unwrap_or_default();
        self.dlq_router
            .route(
                crate::infrastructure::dlq_router::dlq_router::DLQErrorType::Unknown,
                &payload,
                &key_str,
            )
            .await
    }

    /// Adaptive batch sizing based on performance metrics
    async fn adjust_batch_size(&self) {
        let mut profiler = self.performance_profiler.lock().await;
        let config = self.performance_config.lock().await;

        let avg_batch_time = profiler.get_avg_batch_time();
        let target_time = Duration::from_millis(config.target_batch_time_ms);
        let current_batch_size = *self.batch_size.lock().await;

        // Calculate adjustment factor based on performance
        let adjustment_factor = if avg_batch_time > target_time {
            // Batch processing is taking too long, reduce batch size
            0.9
        } else if avg_batch_time < target_time / 2 {
            // Batch processing is very fast, increase batch size
            1.1
        } else {
            // Performance is good, maintain current size
            1.0
        };

        // Apply adjustment with bounds checking
        let new_batch_size = ((current_batch_size as f64 * adjustment_factor) as usize)
            .max(config.min_batch_size)
            .min(config.max_batch_size);

        if new_batch_size != current_batch_size {
            *self.batch_size.lock().await = new_batch_size;
            tracing::info!(
                "Adaptive batch sizing: {} -> {} (avg_time: {:?}, target: {:?})",
                current_batch_size,
                new_batch_size,
                avg_batch_time,
                target_time
            );
        }

        drop(config);
        drop(profiler);
    }

    /// Intelligent concurrency control based on system load
    async fn adjust_concurrency(&self) {
        let config = self.performance_config.lock().await;
        let current_concurrency = *self.adaptive_concurrency.lock().await;

        // Get current system metrics
        let memory_pressure = self.memory_monitor.lock().await.get_memory_pressure();
        let queue_depth = self.batch_queue.lock().await.len();
        let backpressure_active = self
            .backpressure_signal
            .load(std::sync::atomic::Ordering::Relaxed);

        let mut new_concurrency = current_concurrency;

        // Reduce concurrency under memory pressure
        if memory_pressure > config.memory_pressure_threshold {
            new_concurrency = new_concurrency.saturating_sub(1);
            tracing::warn!(
                "Reducing concurrency due to memory pressure: {} -> {} (pressure: {:.2})",
                current_concurrency,
                new_concurrency,
                memory_pressure
            );
        }

        // Reduce concurrency under backpressure
        if backpressure_active || queue_depth > config.backpressure_threshold {
            new_concurrency = new_concurrency.saturating_sub(1);
            tracing::warn!(
                "Reducing concurrency due to backpressure: {} -> {} (queue: {}, backpressure: {})",
                current_concurrency,
                new_concurrency,
                queue_depth,
                backpressure_active
            );
        }

        // Increase concurrency if system is underutilized
        if memory_pressure < config.memory_pressure_threshold * 0.5
            && queue_depth < config.backpressure_threshold / 2
            && !backpressure_active
        {
            new_concurrency = (new_concurrency + 1).min(config.max_concurrency);
            tracing::info!(
                "Increasing concurrency due to underutilization: {} -> {}",
                current_concurrency,
                new_concurrency
            );
        }

        // Apply bounds
        new_concurrency = new_concurrency
            .max(config.min_concurrency)
            .min(config.max_concurrency);

        if new_concurrency != current_concurrency {
            *self.adaptive_concurrency.lock().await = new_concurrency;

            // Update semaphore
            let new_semaphore = Arc::new(Semaphore::new(new_concurrency));
            // Note: In a real implementation, you'd need to replace the semaphore atomically
            tracing::info!(
                "Concurrency adjusted: {} -> {} (memory_pressure: {:.2}, queue_depth: {})",
                current_concurrency,
                new_concurrency,
                memory_pressure,
                queue_depth
            );
        }

        drop(config);
    }

    /// Start periodic flush task for when batch processing is disabled
    async fn start_periodic_flush_task(&mut self) -> Result<()> {
        // Start a periodic task to flush any remaining events in the queue
        // This ensures events don't sit indefinitely when batch processing is disabled
        let batch_queue = Arc::clone(&self.batch_queue);
        let projection_store = Arc::clone(&self.projection_store);
        let projection_cache = Arc::clone(&self.projection_cache);
        let cache_service = Arc::clone(&self.cache_service);
        let metrics = Arc::clone(&self.metrics);
        let batch_size = *self.batch_size.lock().await;
        let consistency_manager = self.consistency_manager.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5)); // Flush every 5 seconds
            loop {
                interval.tick().await;

                // Check if there are any events in the queue
                let queue_len = batch_queue.lock().await.len();
                if queue_len > 0 {
                    // Process any remaining events
                    if let Err(e) = Self::process_batch(
                        &batch_queue,
                        &projection_store,
                        &projection_cache,
                        &cache_service,
                        &metrics,
                        consistency_manager.as_ref(), // Always notify consistency manager
                        batch_size,
                    )
                    .await
                    {
                        error!("Periodic flush failed: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Start performance monitoring task
    async fn start_performance_monitor(&mut self) -> Result<()> {
        let config = self.performance_config.lock().await;
        if !config.enable_performance_profiling {
            drop(config);
            return Ok(());
        }

        let interval_ms = config.metrics_collection_interval_ms;
        drop(config);

        let processor = Arc::new(self.clone());

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));

            loop {
                interval.tick().await;

                // Update memory usage
                let memory_usage = processor.get_memory_usage().await;
                processor
                    .memory_monitor
                    .lock()
                    .await
                    .update_memory_usage(memory_usage);

                // Record performance metrics
                let mut profiler = processor.performance_profiler.lock().await;
                profiler.record_memory_usage(memory_usage);

                // Get current metrics
                let events_processed = processor
                    .metrics
                    .events_processed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let events_failed = processor
                    .metrics
                    .events_failed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let total_events = events_processed + events_failed;
                let error_rate = if total_events > 0 {
                    (events_failed as f64 / total_events as f64) * 100.0
                } else {
                    0.0
                };

                profiler.record_error_rate(error_rate);

                // Get throughput
                let throughput = processor
                    .metrics
                    .throughput_per_second
                    .load(std::sync::atomic::Ordering::Relaxed);
                profiler.record_throughput(throughput);

                drop(profiler);

                // Adjust batch size and concurrency
                processor.adjust_batch_size().await;
                processor.adjust_concurrency().await;

                // Check for memory pressure and trigger GC if needed
                if processor.memory_monitor.lock().await.should_trigger_gc() {
                    processor.clear_expired_cache_entries().await;
                    tracing::info!("Memory GC triggered");
                }

                // Log performance summary
                let memory_stats = processor.memory_monitor.lock().await.get_memory_stats();
                let avg_throughput = processor
                    .performance_profiler
                    .lock()
                    .await
                    .get_avg_throughput();
                let avg_error_rate = processor
                    .performance_profiler
                    .lock()
                    .await
                    .get_avg_error_rate();

                tracing::debug!(
                    "Performance Monitor - Memory: {}MB/{}MB ({:.2}), Throughput: {}/s, Error Rate: {:.2}%",
                    memory_stats.0 / 1024 / 1024,
                    memory_stats.1 / 1024 / 1024,
                    memory_stats.2,
                    avg_throughput,
                    avg_error_rate
                );
            }
        });

        *self.performance_monitor_handle.lock().await = Some(handle);
        tracing::info!("Performance monitor started");
        Ok(())
    }

    /// Get advanced performance statistics
    pub async fn get_performance_stats(&self) -> serde_json::Value {
        let profiler = self.performance_profiler.lock().await;
        let memory_stats = self.memory_monitor.lock().await.get_memory_stats();
        let config = self.performance_config.lock().await;
        let batch_size = *self.batch_size.lock().await;
        let concurrency = *self.adaptive_concurrency.lock().await;

        serde_json::json!({
            "batch_size": {
                "current": batch_size,
                "min": config.min_batch_size,
                "max": config.max_batch_size,
                "target_time_ms": config.target_batch_time_ms
            },
            "concurrency": {
                "current": concurrency,
                "min": config.min_concurrency,
                "max": config.max_concurrency
            },
            "memory": {
                "current_mb": memory_stats.0 / 1024 / 1024,
                "peak_mb": memory_stats.1 / 1024 / 1024,
                "pressure": memory_stats.2,
                "threshold": config.memory_pressure_threshold
            },
            "performance": {
                "avg_batch_time_ms": profiler.get_avg_batch_time().as_millis(),
                "avg_throughput_per_sec": profiler.get_avg_throughput(),
                "avg_error_rate_percent": profiler.get_avg_error_rate(),
                "memory_trend": profiler.get_memory_trend()
            },
            "backpressure": {
                "active": self.backpressure_signal.load(std::sync::atomic::Ordering::Relaxed),
                "queue_depth": self.batch_queue.lock().await.len(),
                "threshold": config.backpressure_threshold
            }
        })
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
            poll_timeout_ms: 25,
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

impl UltraOptimizedCDCEventProcessor {
    /// Static method to enable and start batch processor on an Arc (for use in service managers)
    pub async fn enable_and_start_batch_processor_arc(processor: Arc<Self>) -> Result<()> {
        // Update configuration to enable batch processing
        {
            let mut config = processor.business_config.lock().await;
            config.batch_processing_enabled = true;
        }

        // Check if batch processor is already running
        if processor.batch_processor_handle.lock().await.is_some() {
            return Ok(()); // Already running
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        *processor.shutdown_tx.lock().await = Some(shutdown_tx);

        let batch_queue = Arc::clone(&processor.batch_queue);
        let projection_store = Arc::clone(&processor.projection_store);
        let projection_cache = Arc::clone(&processor.projection_cache);
        let cache_service = Arc::clone(&processor.cache_service);
        let metrics = Arc::clone(&processor.metrics);
        let batch_size = *processor.batch_size.lock().await;
        let batch_timeout = processor.batch_timeout;

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
                            None, // No consistency manager in this context
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

        *processor.batch_processor_handle.lock().await = Some(handle);
        info!("Batch processor started successfully");
        Ok(())
    }
}

/// Memory monitor for advanced memory management
#[derive(Debug)]
pub struct MemoryMonitor {
    peak_memory: usize,
    current_memory: usize,
    memory_pressure_threshold: f64,
    gc_trigger_threshold: usize,
    last_gc_time: Instant,
    gc_interval: Duration,
}

impl MemoryMonitor {
    pub fn new(memory_pressure_threshold: f64, gc_trigger_threshold: usize) -> Self {
        Self {
            peak_memory: 0,
            current_memory: 0,
            memory_pressure_threshold,
            gc_trigger_threshold,
            last_gc_time: Instant::now(),
            gc_interval: Duration::from_secs(60), // GC every minute at most
        }
    }

    pub fn update_memory_usage(&mut self, usage: usize) {
        self.current_memory = usage;
        if usage > self.peak_memory {
            self.peak_memory = usage;
        }
    }

    pub fn should_trigger_gc(&mut self) -> bool {
        let now = Instant::now();
        let time_since_last_gc = now.duration_since(self.last_gc_time);

        if time_since_last_gc < self.gc_interval {
            return false;
        }

        if self.current_memory > self.gc_trigger_threshold {
            self.last_gc_time = now;
            return true;
        }

        false
    }

    pub fn get_memory_pressure(&self) -> f64 {
        if self.peak_memory == 0 {
            return 0.0;
        }
        self.current_memory as f64 / self.peak_memory as f64
    }

    pub fn is_under_pressure(&self) -> bool {
        self.get_memory_pressure() > self.memory_pressure_threshold
    }

    pub fn get_memory_stats(&self) -> (usize, usize, f64) {
        (
            self.current_memory,
            self.peak_memory,
            self.get_memory_pressure(),
        )
    }
}

/// Advanced performance configuration for adaptive optimization
#[derive(Debug, Clone)]
pub struct AdvancedPerformanceConfig {
    // Adaptive batch sizing
    pub min_batch_size: usize,
    pub max_batch_size: usize,
    pub target_batch_time_ms: u64,
    pub load_adjustment_factor: f64,

    // Intelligent concurrency control
    pub min_concurrency: usize,
    pub max_concurrency: usize,
    pub cpu_utilization_threshold: f64,
    pub backpressure_threshold: usize,

    // Advanced caching
    pub enable_predictive_caching: bool,
    pub cache_warmup_size: usize,
    pub cache_eviction_policy: CacheEvictionPolicy,

    // Memory management
    pub memory_pressure_threshold: f64,
    pub gc_trigger_threshold: usize,

    // Performance monitoring
    pub enable_performance_profiling: bool,
    pub metrics_collection_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub enum CacheEvictionPolicy {
    LRU,      // Least Recently Used
    LFU,      // Least Frequently Used
    TTL,      // Time To Live
    Adaptive, // Adaptive based on access patterns
}

impl Default for AdvancedPerformanceConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 250,
            max_batch_size: 500,
            target_batch_time_ms: 25,
            load_adjustment_factor: 0.1,

            min_concurrency: 2,
            max_concurrency: 16,
            cpu_utilization_threshold: 0.8,
            backpressure_threshold: 1000,

            enable_predictive_caching: true,
            cache_warmup_size: 100,
            cache_eviction_policy: CacheEvictionPolicy::Adaptive,

            memory_pressure_threshold: 0.85,
            gc_trigger_threshold: 10000,

            enable_performance_profiling: true,
            metrics_collection_interval_ms: 1000,
        }
    }
}

/// Performance profiler for detailed analysis
#[derive(Debug)]
pub struct PerformanceProfiler {
    batch_times: Vec<Duration>,
    memory_usage: Vec<usize>,
    cpu_usage: Vec<f64>,
    throughput_history: Vec<u64>,
    error_rates: Vec<f64>,
    last_reset: Instant,
    max_history_size: usize,
}

impl PerformanceProfiler {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            batch_times: Vec::new(),
            memory_usage: Vec::new(),
            cpu_usage: Vec::new(),
            throughput_history: Vec::new(),
            error_rates: Vec::new(),
            last_reset: Instant::now(),
            max_history_size,
        }
    }

    pub fn record_batch_time(&mut self, duration: Duration) {
        self.batch_times.push(duration);
        if self.batch_times.len() > self.max_history_size {
            self.batch_times.remove(0);
        }
    }

    pub fn record_memory_usage(&mut self, usage: usize) {
        self.memory_usage.push(usage);
        if self.memory_usage.len() > self.max_history_size {
            self.memory_usage.remove(0);
        }
    }

    pub fn record_cpu_usage(&mut self, usage: f64) {
        self.cpu_usage.push(usage);
        if self.cpu_usage.len() > self.max_history_size {
            self.cpu_usage.remove(0);
        }
    }

    pub fn record_throughput(&mut self, throughput: u64) {
        self.throughput_history.push(throughput);
        if self.throughput_history.len() > self.max_history_size {
            self.throughput_history.remove(0);
        }
    }

    pub fn record_error_rate(&mut self, error_rate: f64) {
        self.error_rates.push(error_rate);
        if self.error_rates.len() > self.max_history_size {
            self.error_rates.remove(0);
        }
    }

    pub fn get_avg_batch_time(&self) -> Duration {
        if self.batch_times.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.batch_times.iter().sum();
        total / self.batch_times.len() as u32
    }

    pub fn get_avg_throughput(&self) -> u64 {
        if self.throughput_history.is_empty() {
            return 0;
        }
        self.throughput_history.iter().sum::<u64>() / self.throughput_history.len() as u64
    }

    pub fn get_avg_error_rate(&self) -> f64 {
        if self.error_rates.is_empty() {
            return 0.0;
        }
        self.error_rates.iter().sum::<f64>() / self.error_rates.len() as f64
    }

    pub fn get_memory_trend(&self) -> f64 {
        if self.memory_usage.len() < 2 {
            return 0.0;
        }
        let recent = self.memory_usage[self.memory_usage.len() - 1];
        let older = self.memory_usage[0];
        if older == 0 {
            return 0.0;
        }
        (recent as f64 - older as f64) / older as f64
    }

    pub fn reset(&mut self) {
        self.batch_times.clear();
        self.memory_usage.clear();
        self.cpu_usage.clear();
        self.throughput_history.clear();
        self.error_rates.clear();
        self.last_reset = Instant::now();
    }
}

// Phase 2.3: Advanced Monitoring
// ==============================

/// Health check status for monitoring
#[derive(Debug, Clone, Serialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Health check result with details
#[derive(Debug, Clone, Serialize)]
pub struct HealthCheckResult {
    pub status: HealthStatus,
    pub component: String,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub details: serde_json::Value,
}

/// Alert severity levels
#[derive(Debug, Clone, Serialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "INFO"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Error => write!(f, "ERROR"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    pub enabled: bool,
    pub error_rate_threshold: f64,
    pub latency_threshold_ms: u64,
    pub memory_usage_threshold: f64,
    pub queue_depth_threshold: usize,
    pub consecutive_failures_threshold: u32,
    pub alert_cooldown_seconds: u64,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            error_rate_threshold: 5.0,   // 5% error rate
            latency_threshold_ms: 1000,  // 1 second
            memory_usage_threshold: 0.8, // 80% memory usage
            queue_depth_threshold: 1000,
            consecutive_failures_threshold: 10,
            alert_cooldown_seconds: 300, // 5 minutes
        }
    }
}

/// Alert with metadata
#[derive(Debug, Clone, Serialize)]
pub struct Alert {
    pub id: Uuid,
    pub severity: AlertSeverity,
    pub component: String,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub metadata: serde_json::Value,
    pub acknowledged: bool,
    pub resolved: bool,
}

/// Metrics aggregation for time-series data
#[derive(Clone)]
pub struct MetricsAggregator {
    time_series: Arc<RwLock<HashMap<String, VecDeque<(DateTime<Utc>, f64)>>>>,
    max_data_points: usize,
    aggregation_interval: Duration,
}

impl MetricsAggregator {
    pub fn new(max_data_points: usize, aggregation_interval: Duration) -> Self {
        Self {
            time_series: Arc::new(RwLock::new(HashMap::new())),
            max_data_points,
            aggregation_interval,
        }
    }

    pub async fn record_metric(&self, metric_name: &str, value: f64) {
        let mut time_series = self.time_series.write().await;
        let series = time_series
            .entry(metric_name.to_string())
            .or_insert_with(VecDeque::new);

        let now = Utc::now();
        series.push_back((now, value));

        // Keep only the latest data points
        while series.len() > self.max_data_points {
            series.pop_front();
        }
    }

    pub async fn get_metric_stats(&self, metric_name: &str) -> Option<serde_json::Value> {
        let time_series = self.time_series.read().await;
        let series = time_series.get(metric_name)?;

        if series.is_empty() {
            return None;
        }

        let values: Vec<f64> = series.iter().map(|(_, v)| *v).collect();
        let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let sum: f64 = values.iter().sum();
        let avg = sum / values.len() as f64;

        // Calculate percentiles
        let mut sorted_values = values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_idx = (sorted_values.len() as f64 * 0.5) as usize;
        let p95_idx = (sorted_values.len() as f64 * 0.95) as usize;
        let p99_idx = (sorted_values.len() as f64 * 0.99) as usize;

        let p50 = sorted_values.get(p50_idx).unwrap_or(&0.0);
        let p95 = sorted_values.get(p95_idx).unwrap_or(&0.0);
        let p99 = sorted_values.get(p99_idx).unwrap_or(&0.0);

        Some(serde_json::json!({
            "metric_name": metric_name,
            "count": values.len(),
            "min": min,
            "max": max,
            "avg": avg,
            "sum": sum,
            "p50": p50,
            "p95": p95,
            "p99": p99,
            "latest_value": values.last(),
            "latest_timestamp": series.back().map(|(ts, _)| ts)
        }))
    }

    pub async fn get_all_metrics(&self) -> serde_json::Value {
        let time_series = self.time_series.read().await;
        let mut result = serde_json::Map::new();

        for (metric_name, _) in time_series.iter() {
            if let Some(stats) = self.get_metric_stats(metric_name).await {
                result.insert(metric_name.to_string(), stats);
            }
        }

        serde_json::Value::Object(result)
    }
}

/// Advanced monitoring system
pub struct AdvancedMonitoringSystem {
    alert_config: AlertConfig,
    alerts: Arc<RwLock<Vec<Alert>>>,
    metrics_aggregator: MetricsAggregator,
    health_checks: Arc<RwLock<HashMap<String, HealthCheckResult>>>,
    monitoring_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    last_alert_time: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
}

impl AdvancedMonitoringSystem {
    pub fn new(alert_config: AlertConfig) -> Self {
        Self {
            alert_config,
            alerts: Arc::new(RwLock::new(Vec::new())),
            metrics_aggregator: MetricsAggregator::new(
                1000,                    // 1000 data points per metric
                Duration::from_secs(60), // 1 minute aggregation interval
            ),
            health_checks: Arc::new(RwLock::new(HashMap::new())),
            monitoring_handle: Arc::new(Mutex::new(None)),
            last_alert_time: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start_monitoring(
        &mut self,
        processor: Arc<UltraOptimizedCDCEventProcessor>,
    ) -> Result<()> {
        let alert_config = self.alert_config.clone();
        let alerts = Arc::clone(&self.alerts);
        let metrics_aggregator = self.metrics_aggregator.clone();
        let health_checks = Arc::clone(&self.health_checks);
        let last_alert_time = Arc::clone(&self.last_alert_time);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30)); // Check every 30 seconds

            loop {
                interval.tick().await;

                // Collect metrics
                Self::collect_metrics(&processor, &metrics_aggregator).await;

                // Run health checks
                Self::run_health_checks(&processor, &health_checks).await;

                // Check for alerts
                Self::check_alerts(&processor, &alert_config, &alerts, &last_alert_time).await;
            }
        });

        let mut monitoring_handle = self.monitoring_handle.lock().await;
        *monitoring_handle = Some(handle);

        info!("Advanced monitoring system started");
        Ok(())
    }

    pub async fn stop_monitoring(&mut self) -> Result<()> {
        let mut monitoring_handle = self.monitoring_handle.lock().await;
        if let Some(handle) = monitoring_handle.take() {
            handle.abort();
        }

        info!("Advanced monitoring system stopped");
        Ok(())
    }

    async fn collect_metrics(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        metrics_aggregator: &MetricsAggregator,
    ) {
        let metrics = processor.get_metrics().await;

        // Record key metrics
        metrics_aggregator
            .record_metric(
                "events_processed_per_sec",
                metrics
                    .events_processed
                    .load(std::sync::atomic::Ordering::Relaxed) as f64,
            )
            .await;

        metrics_aggregator
            .record_metric(
                "events_failed_per_sec",
                metrics
                    .events_failed
                    .load(std::sync::atomic::Ordering::Relaxed) as f64,
            )
            .await;

        metrics_aggregator
            .record_metric(
                "processing_latency_ms",
                metrics
                    .processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed) as f64,
            )
            .await;

        metrics_aggregator
            .record_metric(
                "memory_usage_bytes",
                metrics
                    .memory_usage_bytes
                    .load(std::sync::atomic::Ordering::Relaxed) as f64,
            )
            .await;

        metrics_aggregator
            .record_metric(
                "queue_depth",
                metrics
                    .queue_depth
                    .load(std::sync::atomic::Ordering::Relaxed) as f64,
            )
            .await;

        metrics_aggregator
            .record_metric(
                "error_rate",
                metrics
                    .error_rate
                    .load(std::sync::atomic::Ordering::Relaxed) as f64,
            )
            .await;
    }

    async fn run_health_checks(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        health_checks: &Arc<RwLock<HashMap<String, HealthCheckResult>>>,
    ) {
        let mut checks = health_checks.write().await;
        let now = Utc::now();

        // Check batch processor health
        let batch_processor_running = processor.is_batch_processor_running().await;
        checks.insert(
            "batch_processor".to_string(),
            HealthCheckResult {
                status: if batch_processor_running {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Degraded
                },
                component: "batch_processor".to_string(),
                message: if batch_processor_running {
                    "Batch processor is running"
                } else {
                    "Batch processor is not running"
                }
                .to_string(),
                timestamp: now,
                details: serde_json::json!({ "running": batch_processor_running }),
            },
        );

        // Check memory health
        let memory_usage = processor.get_memory_usage().await;
        let memory_status = if memory_usage > 1_000_000_000 {
            // 1GB
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };
        checks.insert(
            "memory".to_string(),
            HealthCheckResult {
                status: memory_status,
                component: "memory".to_string(),
                message: format!("Memory usage: {} bytes", memory_usage),
                timestamp: now,
                details: serde_json::json!({ "usage_bytes": memory_usage }),
            },
        );

        // Check circuit breaker health
        let circuit_breaker_open = !processor.circuit_breaker.can_execute().await;
        checks.insert(
            "circuit_breaker".to_string(),
            HealthCheckResult {
                status: if circuit_breaker_open {
                    HealthStatus::Unhealthy
                } else {
                    HealthStatus::Healthy
                },
                component: "circuit_breaker".to_string(),
                message: if circuit_breaker_open {
                    "Circuit breaker is open"
                } else {
                    "Circuit breaker is closed"
                }
                .to_string(),
                timestamp: now,
                details: serde_json::json!({ "open": circuit_breaker_open }),
            },
        );
    }

    async fn check_alerts(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        alert_config: &AlertConfig,
        alerts: &Arc<RwLock<Vec<Alert>>>,
        last_alert_time: &Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
    ) {
        if !alert_config.enabled {
            return;
        }

        let metrics = processor.get_metrics().await;
        let now = Utc::now();
        let mut new_alerts = Vec::new();
        let mut last_alert_times = last_alert_time.write().await;

        // Check error rate
        let error_rate = metrics
            .error_rate
            .load(std::sync::atomic::Ordering::Relaxed) as f64;
        if error_rate > alert_config.error_rate_threshold {
            let alert_key = "high_error_rate".to_string();
            if Self::should_send_alert(
                &alert_key,
                &last_alert_times,
                now,
                alert_config.alert_cooldown_seconds,
            )
            .await
            {
                new_alerts.push(Alert {
                    id: Uuid::new_v4(),
                    severity: AlertSeverity::Error,
                    component: "cdc_processor".to_string(),
                    message: format!("High error rate detected: {:.2}%", error_rate),
                    timestamp: now,
                    metadata: serde_json::json!({ "error_rate": error_rate, "threshold": alert_config.error_rate_threshold }),
                    acknowledged: false,
                    resolved: false,
                });
                last_alert_times.insert(alert_key, now);
            }
        }

        // Check latency
        let avg_latency = metrics
            .processing_latency_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if avg_latency > alert_config.latency_threshold_ms {
            let alert_key = "high_latency".to_string();
            if Self::should_send_alert(
                &alert_key,
                &last_alert_times,
                now,
                alert_config.alert_cooldown_seconds,
            )
            .await
            {
                new_alerts.push(Alert {
                    id: Uuid::new_v4(),
                    severity: AlertSeverity::Warning,
                    component: "cdc_processor".to_string(),
                    message: format!("High processing latency detected: {}ms", avg_latency),
                    timestamp: now,
                    metadata: serde_json::json!({ "latency_ms": avg_latency, "threshold_ms": alert_config.latency_threshold_ms }),
                    acknowledged: false,
                    resolved: false,
                });
                last_alert_times.insert(alert_key, now);
            }
        }

        // Check queue depth
        let queue_depth = metrics
            .queue_depth
            .load(std::sync::atomic::Ordering::Relaxed);
        if queue_depth > alert_config.queue_depth_threshold as u64 {
            let alert_key = "high_queue_depth".to_string();
            if Self::should_send_alert(
                &alert_key,
                &last_alert_times,
                now,
                alert_config.alert_cooldown_seconds,
            )
            .await
            {
                new_alerts.push(Alert {
                    id: Uuid::new_v4(),
                    severity: AlertSeverity::Warning,
                    component: "cdc_processor".to_string(),
                    message: format!("High queue depth detected: {}", queue_depth),
                    timestamp: now,
                    metadata: serde_json::json!({ "queue_depth": queue_depth, "threshold": alert_config.queue_depth_threshold }),
                    acknowledged: false,
                    resolved: false,
                });
                last_alert_times.insert(alert_key, now);
            }
        }

        // Check consecutive failures
        let consecutive_failures = metrics
            .consecutive_failures
            .load(std::sync::atomic::Ordering::Relaxed);
        if consecutive_failures > alert_config.consecutive_failures_threshold as u64 {
            let alert_key = "consecutive_failures".to_string();
            if Self::should_send_alert(
                &alert_key,
                &last_alert_times,
                now,
                alert_config.alert_cooldown_seconds,
            )
            .await
            {
                new_alerts.push(Alert {
                    id: Uuid::new_v4(),
                    severity: AlertSeverity::Critical,
                    component: "cdc_processor".to_string(),
                    message: format!("High consecutive failures detected: {}", consecutive_failures),
                    timestamp: now,
                    metadata: serde_json::json!({ "consecutive_failures": consecutive_failures, "threshold": alert_config.consecutive_failures_threshold }),
                    acknowledged: false,
                    resolved: false,
                });
                last_alert_times.insert(alert_key, now);
            }
        }

        // Add new alerts
        if !new_alerts.is_empty() {
            let mut alerts_guard = alerts.write().await;
            alerts_guard.extend(new_alerts.clone());

            // Log alerts
            for alert in new_alerts {
                match alert.severity {
                    AlertSeverity::Info => {
                        info!("🚨 ALERT [{}]: {}", alert.severity, alert.message)
                    }
                    AlertSeverity::Warning => {
                        warn!("🚨 ALERT [{}]: {}", alert.severity, alert.message)
                    }
                    AlertSeverity::Error => {
                        error!("🚨 ALERT [{}]: {}", alert.severity, alert.message)
                    }
                    AlertSeverity::Critical => {
                        error!("🚨 CRITICAL ALERT [{}]: {}", alert.severity, alert.message)
                    }
                }
            }
        }
    }

    async fn should_send_alert(
        alert_key: &str,
        last_alert_times: &HashMap<String, DateTime<Utc>>,
        now: DateTime<Utc>,
        cooldown_seconds: u64,
    ) -> bool {
        if let Some(last_time) = last_alert_times.get(alert_key) {
            let time_since_last = now.signed_duration_since(*last_time);
            time_since_last.num_seconds() < cooldown_seconds as i64
        } else {
            true
        }
    }

    pub async fn get_health_status(&self) -> serde_json::Value {
        let health_checks = self.health_checks.read().await;
        let alerts = self.alerts.read().await;

        let mut overall_status = HealthStatus::Healthy;
        let mut component_statuses = serde_json::Map::new();

        for (component, check) in health_checks.iter() {
            component_statuses.insert(
                component.clone(),
                serde_json::json!({
                    "status": check.status,
                    "message": check.message,
                    "timestamp": check.timestamp,
                    "details": check.details
                }),
            );

            // Update overall status
            match check.status {
                HealthStatus::Unhealthy => overall_status = HealthStatus::Unhealthy,
                HealthStatus::Degraded if matches!(overall_status, HealthStatus::Healthy) => {
                    overall_status = HealthStatus::Degraded;
                }
                _ => {}
            }
        }

        let active_alerts: Vec<&Alert> = alerts.iter().filter(|a| !a.resolved).collect();

        serde_json::json!({
            "overall_status": overall_status,
            "components": component_statuses,
            "active_alerts_count": active_alerts.len(),
            "alerts": active_alerts.iter().map(|a| serde_json::json!({
                "id": a.id,
                "severity": a.severity,
                "component": a.component,
                "message": a.message,
                "timestamp": a.timestamp,
                "metadata": a.metadata,
                "acknowledged": a.acknowledged,
                "resolved": a.resolved
            })).collect::<Vec<_>>(),
            "timestamp": Utc::now()
        })
    }

    pub async fn get_metrics_summary(&self) -> serde_json::Value {
        self.metrics_aggregator.get_all_metrics().await
    }

    pub async fn acknowledge_alert(&self, alert_id: Uuid) -> Result<()> {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.acknowledged = true;
            info!("Alert {} acknowledged", alert_id);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Alert not found"))
        }
    }

    pub async fn resolve_alert(&self, alert_id: Uuid) -> Result<()> {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.resolved = true;
            info!("Alert {} resolved", alert_id);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Alert not found"))
        }
    }

    pub async fn clear_resolved_alerts(&self) -> Result<()> {
        let mut alerts = self.alerts.write().await;
        let initial_count = alerts.len();
        alerts.retain(|a| !a.resolved);
        let removed_count = initial_count - alerts.len();
        info!("Cleared {} resolved alerts", removed_count);
        Ok(())
    }
}

// Phase 2.3: Advanced Monitoring Integration Methods
// =================================================

impl UltraOptimizedCDCEventProcessor {
    /// Start the advanced monitoring system
    pub async fn start_advanced_monitoring(
        &mut self,
        alert_config: Option<AlertConfig>,
    ) -> Result<()> {
        let alert_config = alert_config.unwrap_or_default();
        let monitoring_system = AdvancedMonitoringSystem::new(alert_config);

        let mut monitoring_guard = self.monitoring_system.lock().await;
        *monitoring_guard = Some(monitoring_system);

        let processor = Arc::new(self.clone());
        if let Some(monitoring_system) = monitoring_guard.as_mut() {
            monitoring_system.start_monitoring(processor).await?;
        }

        info!("Advanced monitoring system started");
        Ok(())
    }

    /// Stop the advanced monitoring system
    pub async fn stop_advanced_monitoring(&mut self) -> Result<()> {
        let mut monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_mut() {
            monitoring_system.stop_monitoring().await?;
        }
        *monitoring_guard = None;

        info!("Advanced monitoring system stopped");
        Ok(())
    }

    /// Get comprehensive health status
    pub async fn get_health_status(&self) -> Result<serde_json::Value> {
        let monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_ref() {
            Ok(monitoring_system.get_health_status().await)
        } else {
            Err(anyhow::anyhow!("Advanced monitoring system not started"))
        }
    }

    /// Get metrics summary with time-series data
    pub async fn get_metrics_summary(&self) -> Result<serde_json::Value> {
        let monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_ref() {
            Ok(monitoring_system.get_metrics_summary().await)
        } else {
            Err(anyhow::anyhow!("Advanced monitoring system not started"))
        }
    }

    /// Acknowledge an alert
    pub async fn acknowledge_alert(&self, alert_id: Uuid) -> Result<()> {
        let monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_ref() {
            monitoring_system.acknowledge_alert(alert_id).await
        } else {
            Err(anyhow::anyhow!("Advanced monitoring system not started"))
        }
    }

    /// Resolve an alert
    pub async fn resolve_alert(&self, alert_id: Uuid) -> Result<()> {
        let monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_ref() {
            monitoring_system.resolve_alert(alert_id).await
        } else {
            Err(anyhow::anyhow!("Advanced monitoring system not started"))
        }
    }

    /// Clear resolved alerts
    pub async fn clear_resolved_alerts(&self) -> Result<()> {
        let monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_ref() {
            monitoring_system.clear_resolved_alerts().await
        } else {
            Err(anyhow::anyhow!("Advanced monitoring system not started"))
        }
    }

    /// Get comprehensive monitoring dashboard data
    pub async fn get_monitoring_dashboard(&self) -> Result<serde_json::Value> {
        let health_status = self.get_health_status().await?;
        let metrics_summary = self.get_metrics_summary().await?;
        let performance_stats = self.get_performance_stats().await;
        let dlq_stats = self
            .get_dlq_stats()
            .await
            .unwrap_or_else(|_| serde_json::json!({"error": "Failed to get DLQ stats"}));

        Ok(serde_json::json!({
            "health_status": health_status,
            "metrics_summary": metrics_summary,
            "performance_stats": performance_stats,
            "dlq_stats": dlq_stats,
            "timestamp": Utc::now(),
            "version": "2.3.0"
        }))
    }

    /// Get alert history
    pub async fn get_alert_history(&self) -> Result<serde_json::Value> {
        let monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_ref() {
            let alerts = monitoring_system.alerts.read().await;
            Ok(serde_json::json!({
                "alerts": alerts.iter().map(|a| serde_json::json!({
                    "id": a.id,
                    "severity": a.severity,
                    "component": a.component,
                    "message": a.message,
                    "timestamp": a.timestamp,
                    "metadata": a.metadata,
                    "acknowledged": a.acknowledged,
                    "resolved": a.resolved
                })).collect::<Vec<_>>(),
                "total_alerts": alerts.len(),
                "active_alerts": alerts.iter().filter(|a| !a.resolved).count(),
                "resolved_alerts": alerts.iter().filter(|a| a.resolved).count()
            }))
        } else {
            Err(anyhow::anyhow!("Advanced monitoring system not started"))
        }
    }

    /// Get system performance trends
    pub async fn get_performance_trends(&self) -> Result<serde_json::Value> {
        let monitoring_guard = self.monitoring_system.lock().await;
        if let Some(monitoring_system) = monitoring_guard.as_ref() {
            let metrics = monitoring_system.get_metrics_summary().await;

            // Calculate trends from time-series data
            let mut trends = serde_json::Map::new();

            if let Some(events_processed) = metrics.get("events_processed_per_sec") {
                trends.insert(
                    "events_processed_trend".to_string(),
                    events_processed.clone(),
                );
            }

            if let Some(error_rate) = metrics.get("error_rate") {
                trends.insert("error_rate_trend".to_string(), error_rate.clone());
            }

            if let Some(latency) = metrics.get("processing_latency_ms") {
                trends.insert("latency_trend".to_string(), latency.clone());
            }

            if let Some(memory) = metrics.get("memory_usage_bytes") {
                trends.insert("memory_trend".to_string(), memory.clone());
            }

            Ok(serde_json::Value::Object(trends))
        } else {
            Err(anyhow::anyhow!("Advanced monitoring system not started"))
        }
    }

    /// Export monitoring data for external systems
    pub async fn export_monitoring_data(&self) -> Result<serde_json::Value> {
        let dashboard = self.get_monitoring_dashboard().await?;
        let alert_history = self.get_alert_history().await?;
        let performance_trends = self.get_performance_trends().await?;

        Ok(serde_json::json!({
            "export_timestamp": Utc::now(),
            "system_info": {
                "version": "2.3.0",
                "component": "CDC Event Processor",
                "export_format": "json"
            },
            "dashboard": dashboard,
            "alert_history": alert_history,
            "performance_trends": performance_trends
        }))
    }

    /// Mark an aggregate as completed by CDC processing
    pub async fn mark_aggregate_completed(&self, aggregate_id: Uuid) {
        // This method can be called by external consistency managers
        // when CDC events are successfully processed
        info!(
            "🟡 CDC Event Processor: Marked aggregate {} as completed",
            aggregate_id
        );

        // If we have a consistency manager, mark the projection as completed
        if let Some(ref consistency_manager) = self.consistency_manager {
            info!(
                "🟡 CDC Event Processor: Calling consistency manager for aggregate {}",
                aggregate_id
            );
            consistency_manager
                .mark_projection_completed(aggregate_id)
                .await;
            info!(
                "🟡 CDC Event Processor: Successfully called consistency manager for aggregate {}",
                aggregate_id
            );

            // Also mark CDC processing as completed
            tracing::info!(
                "CDC Event Processor: Marking CDC completed for aggregate {} (immediate)",
                aggregate_id
            );
            consistency_manager.mark_completed(aggregate_id).await;
            tracing::info!("CDC Event Processor: Successfully marked CDC completed for aggregate {} (immediate)", aggregate_id);
        } else {
            warn!(
                "🟡 CDC Event Processor: No consistency manager available for aggregate {}",
                aggregate_id
            );
        }
    }

    /// Mark an aggregate as failed by CDC processing
    pub async fn mark_aggregate_failed(&self, aggregate_id: Uuid, error: String) {
        // This method can be called by external consistency managers
        // when CDC events fail to process
        error!(
            "CDC Event Processor: Marked aggregate {} as failed: {}",
            aggregate_id, error
        );

        // If we have a consistency manager, mark the aggregate as failed
        if let Some(ref consistency_manager) = self.consistency_manager {
            consistency_manager
                .mark_failed(aggregate_id, error.clone())
                .await;
        }
    }

    /// Set the consistency manager for this processor
    pub async fn set_consistency_manager(&mut self, consistency_manager: Arc<ConsistencyManager>) {
        self.consistency_manager = Some(consistency_manager);
        info!("CDC Event Processor: Consistency manager set");
    }
}
