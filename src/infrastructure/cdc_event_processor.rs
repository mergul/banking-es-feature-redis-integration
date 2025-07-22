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
    consistency_manager: Option<Arc<ConsistencyManager>>,

    // High-performance processing
    processing_semaphore: Arc<Semaphore>,
    batch_queue: Arc<Mutex<Vec<ProcessableEvent>>>,
    batch_size: Arc<Mutex<usize>>, // Made mutable for adaptive sizing
    batch_timeout: Duration,

    // Circuit breaker
    circuit_breaker: CircuitBreaker,

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

    // Phase 2.2: Scalability Enhancements
    cluster_manager: Arc<Mutex<Option<EnhancedClusterManager>>>,
    load_balancer: Arc<Mutex<LoadBalancer>>,
    shard_manager: Arc<Mutex<ShardManager>>,
    distributed_lock_manager: Arc<Mutex<DistributedLockManager>>,
    distributed_cache_manager: Arc<Mutex<DistributedCacheManager>>,
    state_sync_manager: Arc<Mutex<StateSyncManager>>,

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
            dlq_producer: self.dlq_producer.clone(),
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
            load_balancer: self.load_balancer.clone(),
            shard_manager: self.shard_manager.clone(),
            distributed_lock_manager: self.distributed_lock_manager.clone(),
            distributed_cache_manager: self.distributed_cache_manager.clone(),
            state_sync_manager: self.state_sync_manager.clone(),
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
            dlq_producer: Some(kafka_producer), // Use the same producer for DLQ by default
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
            circuit_breaker: CircuitBreaker::new(),
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
            load_balancer: Arc::new(Mutex::new(LoadBalancer::new(
                LoadBalancingStrategy::Adaptive,
            ))),
            shard_manager: Arc::new(Mutex::new(ShardManager::new(64))), // Default 64 shards
            distributed_lock_manager: Arc::new(Mutex::new(DistributedLockManager::new())),
            distributed_cache_manager: Arc::new(Mutex::new(DistributedCacheManager::new())),
            state_sync_manager: Arc::new(Mutex::new(StateSyncManager::new())),
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
        let start_time = Instant::now();
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

        // Update queue depth metric
        let queue_depth = batch_queue.lock().await.len();
        metrics
            .queue_depth
            .store(queue_depth as u64, std::sync::atomic::Ordering::Relaxed);

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
                    metrics
                        .consecutive_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue; // Skip this event but continue processing others
                } else {
                    // Reset consecutive failures on success
                    metrics
                        .consecutive_failures
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Update cache
            cache_guard.put(aggregate_id, projection.clone());
            updated_projections.push(Self::projection_cache_to_db_projection(
                &projection,
                aggregate_id,
            ));
            processed_aggregates.push(aggregate_id);
        }

        drop(cache_guard);

        // Bulk update projections in database with optimized parallel processing
        if !updated_projections.is_empty() {
            let upsert_start = Instant::now();

            // Use adaptive concurrency based on batch size
            let max_concurrent = (updated_projections.len() / 50).max(2).min(16); // Increased to support stress test
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

            // Optimize chunk size for better performance
            let chunk_size = (updated_projections.len() / max_concurrent).max(1);
            let mut tasks = Vec::new();
            let mut chunk_aggregate_ids: Vec<Vec<Uuid>> = Vec::new();

            for chunk in updated_projections.chunks(chunk_size) {
                let store = projection_store.clone();
                let chunk_vec = chunk.to_vec();
                let semaphore = semaphore.clone();
                let aggregate_ids: Vec<Uuid> = chunk_vec.iter().map(|p| p.id).collect();
                chunk_aggregate_ids.push(aggregate_ids);
                tasks.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire().await;
                    // Retry up to 3 times
                    let mut last_err = None;
                    for _ in 0..3 {
                        match store.upsert_accounts_batch(chunk_vec.clone()).await {
                            Ok(res) => {
                                return Ok::<Result<_, anyhow::Error>, anyhow::Error>(Ok(res))
                            }
                            Err(e) => last_err = Some(e),
                        }
                    }
                    Err(anyhow::anyhow!(format!(
                        "Upsert failed after 3 retries: {}",
                        last_err.unwrap()
                    )))
                }));
            }

            // Await all upsert tasks with timeout
            let results = match tokio::time::timeout(
                Duration::from_secs(30), // 30 second timeout
                futures::future::join_all(tasks),
            )
            .await
            {
                Ok(results) => results,
                Err(_) => {
                    error!("Batch processing timeout after 30 seconds");
                    metrics
                        .events_failed
                        .fetch_add(events_count as u64, std::sync::atomic::Ordering::Relaxed);
                    return Err(anyhow::anyhow!("Batch processing timeout"));
                }
            };

            let upsert_time = upsert_start.elapsed().as_millis() as u64;

            // Update latency metrics
            metrics
                .processing_latency_ms
                .store(upsert_time, std::sync::atomic::Ordering::Relaxed);
            metrics.total_latency_ms.store(
                batch_start.elapsed().as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );

            // Calculate P95 and P99 latencies (simplified - in real implementation you'd track individual latencies)
            if upsert_time
                > metrics
                    .p95_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                metrics
                    .p95_processing_latency_ms
                    .store(upsert_time, std::sync::atomic::Ordering::Relaxed);
            }
            if upsert_time
                > metrics
                    .p99_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                metrics
                    .p99_processing_latency_ms
                    .store(upsert_time, std::sync::atomic::Ordering::Relaxed);
            }

            tracing::info!(
                "Optimized parallel upsert of {} projections in {}ms (concurrent: {})",
                updated_projections.len(),
                upsert_time,
                max_concurrent
            );

            // Track per-aggregate upsert results
            let mut aggregate_upsert_success: std::collections::HashMap<Uuid, bool> =
                std::collections::HashMap::new();
            let mut chunk_idx = 0;
            for res in results {
                let aggregate_ids = &chunk_aggregate_ids[chunk_idx];
                chunk_idx += 1;
                match res {
                    Ok(Ok(_)) => {
                        for id in aggregate_ids {
                            aggregate_upsert_success.insert(*id, true);
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Failed to bulk update projections in chunk: {}", e);
                        metrics.events_failed.fetch_add(
                            aggregate_ids.len() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        for id in aggregate_ids {
                            aggregate_upsert_success.insert(*id, false);
                        }
                    }
                    Err(e) => {
                        error!("Join error in parallel upsert: {}", e);
                        metrics.events_failed.fetch_add(
                            aggregate_ids.len() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        for id in aggregate_ids {
                            aggregate_upsert_success.insert(*id, false);
                        }
                    }
                }
            }

            // Mark projection completion or failure for each aggregate
            for aggregate_id in processed_aggregates {
                if let Some(consistency_manager) = consistency_manager {
                    if aggregate_upsert_success
                        .get(&aggregate_id)
                        .copied()
                        .unwrap_or(false)
                    {
                        tracing::info!(
                            "CDC Event Processor: Marking projection completed for aggregate {}",
                            aggregate_id
                        );
                        consistency_manager
                            .mark_projection_completed(aggregate_id)
                            .await;
                        tracing::info!("CDC Event Processor: Successfully marked projection completed for aggregate {}", aggregate_id);

                        // Also mark CDC processing as completed
                        tracing::info!(
                            "CDC Event Processor: Marking CDC completed for aggregate {}",
                            aggregate_id
                        );
                        consistency_manager.mark_completed(aggregate_id).await;
                        tracing::info!(
                            "CDC Event Processor: Successfully marked CDC completed for aggregate {}",
                            aggregate_id
                        );
                    } else {
                        let err_msg = "DB upsert failed after 3 retries".to_string();
                        tracing::warn!(
                            "CDC Event Processor: Marking projection failed for aggregate {}: {}",
                            aggregate_id,
                            err_msg
                        );
                        consistency_manager
                            .mark_projection_failed(aggregate_id, err_msg.clone())
                            .await;
                        consistency_manager.mark_failed(aggregate_id, err_msg).await;
                    }
                } else {
                    tracing::warn!("CDC Event Processor: No consistency manager available to mark projection completed/failed for aggregate {}", aggregate_id);
                }
            }

            // If any chunks failed, return error but continue processing others
            if aggregate_upsert_success.values().any(|&v| !v) {
                return Err(anyhow::anyhow!(
                    "Some projection update chunks failed (see logs for details)"
                ));
            }
        }

        // Update comprehensive metrics
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

        // Calculate throughput
        if batch_time > 0 {
            let throughput = (events_count as f64 / (batch_time as f64 / 1000.0)) as u64;
            metrics
                .throughput_per_second
                .store(throughput, std::sync::atomic::Ordering::Relaxed);
        }

        // Update last error time if there were failures
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
                owner_name: proj.owner_name.clone(),
                is_active: proj.is_active,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            }
        } else {
            // Create a new, empty projection entry for this aggregate
            ProjectionCacheEntry {
                balance: Decimal::ZERO,
                owner_name: String::new(),
                is_active: true,
                version: 1,
                cached_at: Instant::now(),
                last_event_id: None,
            }
        };
        // Populate both Redis and L1
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
        if let Some(dlq_producer) = &self.dlq_producer {
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
            let dlq_topic = "banking-es-dlq";
            let key_str = key
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_default();
            dlq_producer
                .publish_binary_event(dlq_topic, &payload, &key_str)
                .await?;
        }
        Ok(())
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

/// Instance status for cluster management
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum InstanceStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed(String),
}

/// Instance information for cluster coordination
#[derive(Debug, Clone)]
pub struct InstanceInfo {
    pub instance_id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub status: InstanceStatus,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub load_metrics: InstanceLoadMetrics,
    pub shard_assignments: Vec<u32>,
}

/// Load metrics for an instance
#[derive(Debug, Clone)]
pub struct InstanceLoadMetrics {
    pub cpu_usage_percent: f64,
    pub memory_usage_mb: usize,
    pub queue_depth: usize,
    pub active_connections: usize,
    pub events_processed_per_sec: u64,
    pub error_rate_percent: f64,
    pub last_updated: DateTime<Utc>,
}

impl Default for InstanceLoadMetrics {
    fn default() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_usage_mb: 0,
            queue_depth: 0,
            active_connections: 0,
            events_processed_per_sec: 0,
            error_rate_percent: 0.0,
            last_updated: Utc::now(),
        }
    }
}

/// Cluster configuration for distributed processing
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub instance_id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub shard_count: u32,
    pub enable_leader_election: bool,
    pub leader_election_timeout_ms: u64,
    pub instance_discovery_timeout_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_id: "cdc-cluster".to_string(),
            instance_id: Uuid::new_v4(),
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            port: 8080,
            heartbeat_interval_ms: 5000,
            heartbeat_timeout_ms: 15000,
            shard_count: 256,
            enable_leader_election: true,
            leader_election_timeout_ms: 30000,
            instance_discovery_timeout_ms: 10000,
        }
    }
}

/// Cluster member information
#[derive(Debug, Clone)]
pub struct ClusterMember {
    pub instance_info: InstanceInfo,
    pub is_leader: bool,
    pub last_seen: DateTime<Utc>,
    pub failure_count: u32,
}

/// Load balancing strategy
#[derive(Debug, Clone)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    LeastLoaded,
    ConsistentHash,
    Adaptive,
}

/// Shard assignment information
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    pub shard_id: u32,
    pub instance_id: Uuid,
    pub aggregate_ids: Vec<Uuid>,
    pub last_updated: DateTime<Utc>,
}

/// Cluster manager for distributed CDC processing
#[derive(Debug)]
pub struct ClusterManager {
    config: ClusterConfig,
    instance_info: InstanceInfo,
    members: Arc<RwLock<HashMap<Uuid, ClusterMember>>>,
    shard_assignments: Arc<RwLock<HashMap<u32, ShardAssignment>>>,
    leader_id: Arc<RwLock<Option<Uuid>>>,
    load_balancing_strategy: LoadBalancingStrategy,
    heartbeat_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    discovery_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    leader_election_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl ClusterManager {
    pub fn new(config: ClusterConfig) -> Self {
        let instance_info = InstanceInfo {
            instance_id: config.instance_id,
            hostname: config.hostname.clone(),
            port: config.port,
            status: InstanceStatus::Starting,
            started_at: Utc::now(),
            last_heartbeat: Utc::now(),
            load_metrics: InstanceLoadMetrics::default(),
            shard_assignments: Vec::new(),
        };

        Self {
            config,
            instance_info,
            members: Arc::new(RwLock::new(HashMap::new())),
            shard_assignments: Arc::new(RwLock::new(HashMap::new())),
            leader_id: Arc::new(RwLock::new(None)),
            load_balancing_strategy: LoadBalancingStrategy::Adaptive,
            heartbeat_handle: Arc::new(Mutex::new(None)),
            discovery_handle: Arc::new(Mutex::new(None)),
            leader_election_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the cluster manager
    pub async fn start(&mut self) -> Result<()> {
        tracing::info!(
            "Starting cluster manager for instance {} on {}:{}",
            self.config.instance_id,
            self.config.hostname,
            self.config.port
        );

        // Register this instance
        self.register_instance().await?;

        // Start heartbeat
        self.start_heartbeat().await?;

        // Start instance discovery
        self.start_instance_discovery().await?;

        // Start leader election if enabled
        if self.config.enable_leader_election {
            self.start_leader_election().await?;
        }

        // Update instance status
        self.instance_info.status = InstanceStatus::Running;

        tracing::info!("Cluster manager started successfully");
        Ok(())
    }

    /// Stop the cluster manager
    pub async fn stop(&mut self) -> Result<()> {
        tracing::info!("Stopping cluster manager...");

        // Update instance status
        self.instance_info.status = InstanceStatus::Stopping;

        // Stop background tasks
        if let Some(handle) = self.heartbeat_handle.lock().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.discovery_handle.lock().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.leader_election_handle.lock().await.take() {
            handle.abort();
        }

        // Deregister instance
        self.deregister_instance().await?;

        self.instance_info.status = InstanceStatus::Stopped;
        tracing::info!("Cluster manager stopped");
        Ok(())
    }

    /// Register this instance with the cluster
    async fn register_instance(&self) -> Result<()> {
        // In a real implementation, this would register with a service registry
        // For now, we'll just add ourselves to the local member list
        let mut members = self.members.write().await;
        let member = ClusterMember {
            instance_info: self.instance_info.clone(),
            is_leader: false,
            last_seen: Utc::now(),
            failure_count: 0,
        };
        members.insert(self.config.instance_id, member);

        tracing::info!(
            "Instance {} registered with cluster",
            self.config.instance_id
        );
        Ok(())
    }

    /// Deregister this instance from the cluster
    async fn deregister_instance(&self) -> Result<()> {
        let mut members = self.members.write().await;
        members.remove(&self.config.instance_id);

        tracing::info!(
            "Instance {} deregistered from cluster",
            self.config.instance_id
        );
        Ok(())
    }

    /// Start heartbeat mechanism
    async fn start_heartbeat(&mut self) -> Result<()> {
        let config = self.config.clone();
        let instance_info = Arc::new(RwLock::new(self.instance_info.clone()));
        let members = self.members.clone();

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(config.heartbeat_interval_ms));

            loop {
                interval.tick().await;

                // Update heartbeat timestamp
                {
                    let mut info = instance_info.write().await;
                    info.last_heartbeat = Utc::now();
                }

                // Update load metrics (simplified - in real implementation, get actual metrics)
                {
                    let mut info = instance_info.write().await;
                    info.load_metrics.last_updated = Utc::now();
                    // In real implementation, get actual metrics from the processor
                }

                // Broadcast heartbeat to other instances
                // In a real implementation, this would send UDP multicast or use a service registry
                tracing::debug!("Heartbeat sent from instance {}", config.instance_id);
            }
        });

        *self.heartbeat_handle.lock().await = Some(handle);
        Ok(())
    }

    /// Start instance discovery
    async fn start_instance_discovery(&mut self) -> Result<()> {
        let config = self.config.clone();
        let members = self.members.clone();
        let instance_info = self.instance_info.clone();

        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis(config.instance_discovery_timeout_ms);
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                // Discover new instances (in a real implementation, this would use service discovery)
                let members_guard = members.read().await;
                info!("Current cluster members: {}", members_guard.len());

                // Log member information
                for (instance_id, membership) in members_guard.iter() {
                    debug!(
                        "Member: {} - {}:{} - Status: {:?}",
                        instance_id,
                        membership.instance_info.hostname,
                        membership.instance_info.port,
                        membership.instance_info.status
                    );
                }
            }
        });

        *self.discovery_handle.lock().await = Some(handle);
        Ok(())
    }

    /// Start leader election
    async fn start_leader_election(&mut self) -> Result<()> {
        let config = self.config.clone();
        let members = self.members.clone();
        let leader_id = self.leader_id.clone();

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(config.leader_election_timeout_ms));

            loop {
                interval.tick().await;

                let members_guard = members.read().await;
                let mut leader_guard = leader_id.write().await;

                // Simple leader election: instance with lowest UUID becomes leader
                if let Some((lowest_id, _)) = members_guard.iter().min_by_key(|(id, _)| *id) {
                    if *leader_guard != Some(*lowest_id) {
                        *leader_guard = Some(*lowest_id);
                        tracing::info!("New leader elected: {}", lowest_id);
                    }
                }
            }
        });

        *self.leader_election_handle.lock().await = Some(handle);
        Ok(())
    }

    /// Get cluster status
    pub async fn get_cluster_status(&self) -> serde_json::Value {
        let members = self.members.read().await;
        let leader_id = self.leader_id.read().await;
        let shard_assignments = self.shard_assignments.read().await;

        let leader_id_value = leader_id.as_ref().map(|id| id.to_string());

        serde_json::json!({
            "cluster_id": self.config.cluster_id,
            "instance_id": self.config.instance_id,
            "instance_status": format!("{:?}", self.instance_info.status),
            "total_members": members.len(),
            "leader_id": leader_id_value,
            "is_leader": leader_id.as_ref() == Some(&self.config.instance_id),
            "members": members.iter().map(|(id, member)| {
                serde_json::json!({
                    "instance_id": id,
                    "hostname": member.instance_info.hostname,
                    "status": format!("{:?}", member.instance_info.status),
                    "is_leader": member.is_leader,
                    "last_seen": member.last_seen,
                    "load_metrics": {
                        "cpu_usage_percent": member.instance_info.load_metrics.cpu_usage_percent,
                        "memory_usage_mb": member.instance_info.load_metrics.memory_usage_mb,
                        "queue_depth": member.instance_info.load_metrics.queue_depth,
                        "events_processed_per_sec": member.instance_info.load_metrics.events_processed_per_sec,
                        "error_rate_percent": member.instance_info.load_metrics.error_rate_percent
                    }
                })
            }).collect::<Vec<_>>(),
            "shard_assignments": shard_assignments.len(),
            "load_balancing_strategy": format!("{:?}", self.load_balancing_strategy)
        })
    }

    /// Check if this instance is the leader
    pub async fn is_leader(&self) -> bool {
        let leader_id = self.leader_id.read().await;
        leader_id.as_ref() == Some(&self.config.instance_id)
    }

    /// Get the current leader ID
    pub async fn get_leader_id(&self) -> Option<Uuid> {
        let leader_id = self.leader_id.read().await;
        *leader_id
    }

    /// Get all cluster members
    pub async fn get_members(&self) -> Vec<ClusterMember> {
        let members = self.members.read().await;
        members.values().cloned().collect()
    }

    /// Update load metrics for this instance
    pub async fn update_load_metrics(&mut self, metrics: InstanceLoadMetrics) {
        self.instance_info.load_metrics = metrics;
        self.instance_info.last_heartbeat = Utc::now();
    }
}

/// Consistent hashing ring for shard distribution
#[derive(Debug)]
pub struct ConsistentHashRing {
    ring: Vec<(u32, Uuid)>, // (hash, instance_id)
    virtual_nodes_per_instance: usize,
}

impl ConsistentHashRing {
    pub fn new(virtual_nodes_per_instance: usize) -> Self {
        Self {
            ring: Vec::new(),
            virtual_nodes_per_instance,
        }
    }

    /// Add an instance to the hash ring
    pub fn add_instance(&mut self, instance_id: Uuid) {
        for i in 0..self.virtual_nodes_per_instance {
            let virtual_node_key = format!("{}:{}", instance_id, i);
            let hash = self.hash(&virtual_node_key);
            self.ring.push((hash, instance_id));
        }
        self.ring.sort_by_key(|(hash, _)| *hash);
    }

    /// Remove an instance from the hash ring
    pub fn remove_instance(&mut self, instance_id: Uuid) {
        self.ring.retain(|(_, id)| *id != instance_id);
    }

    /// Get the instance responsible for a given key
    pub fn get_instance(&self, key: &str) -> Option<Uuid> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = self.hash(key);
        let index = self
            .ring
            .binary_search_by_key(&hash, |(h, _)| *h)
            .unwrap_or_else(|i| i % self.ring.len());

        Some(self.ring[index].1)
    }

    /// Get all instances in the ring
    pub fn get_instances(&self) -> Vec<Uuid> {
        let mut instances = self.ring.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        instances.sort();
        instances.dedup();
        instances
    }

    /// Hash a string to a u32
    fn hash(&self, key: &str) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as u32
    }
}

/// Load balancer for distributing events across instances
#[derive(Debug)]
pub struct LoadBalancer {
    strategy: LoadBalancingStrategy,
    hash_ring: ConsistentHashRing,
    round_robin_index: Arc<Mutex<usize>>,
    instance_loads: Arc<RwLock<HashMap<Uuid, InstanceLoadMetrics>>>,
}

impl LoadBalancer {
    pub fn new(strategy: LoadBalancingStrategy) -> Self {
        Self {
            strategy,
            hash_ring: ConsistentHashRing::new(100), // 100 virtual nodes per instance
            round_robin_index: Arc::new(Mutex::new(0)),
            instance_loads: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add an instance to the load balancer
    pub fn add_instance(&mut self, instance_id: Uuid) {
        self.hash_ring.add_instance(instance_id);
        tracing::info!("Added instance {} to load balancer", instance_id);
    }

    /// Remove an instance from the load balancer
    pub fn remove_instance(&mut self, instance_id: Uuid) {
        self.hash_ring.remove_instance(instance_id);
        let mut loads = self.instance_loads.blocking_write();
        loads.remove(&instance_id);
        tracing::info!("Removed instance {} from load balancer", instance_id);
    }

    /// Update load metrics for an instance
    pub async fn update_instance_load(&self, instance_id: Uuid, metrics: InstanceLoadMetrics) {
        let mut loads = self.instance_loads.write().await;
        loads.insert(instance_id, metrics);
    }

    /// Select an instance for processing an event
    pub async fn select_instance(&self, aggregate_id: Uuid) -> Option<Uuid> {
        match self.strategy {
            LoadBalancingStrategy::RoundRobin => self.round_robin_select().await,
            LoadBalancingStrategy::LeastLoaded => self.least_loaded_select().await,
            LoadBalancingStrategy::ConsistentHash => self.consistent_hash_select(aggregate_id),
            LoadBalancingStrategy::Adaptive => self.adaptive_select(aggregate_id).await,
        }
    }

    /// Round-robin selection
    async fn round_robin_select(&self) -> Option<Uuid> {
        let instances = self.hash_ring.get_instances();
        if instances.is_empty() {
            return None;
        }

        let mut index = self.round_robin_index.lock().await;
        let instance_id = instances[*index % instances.len()];
        *index = (*index + 1) % instances.len();
        Some(instance_id)
    }

    /// Least-loaded selection
    async fn least_loaded_select(&self) -> Option<Uuid> {
        let loads = self.instance_loads.read().await;
        if loads.is_empty() {
            return None;
        }

        // Find instance with lowest queue depth
        loads
            .iter()
            .min_by_key(|(_, metrics)| metrics.queue_depth)
            .map(|(id, _)| *id)
    }

    /// Consistent hash selection
    fn consistent_hash_select(&self, aggregate_id: Uuid) -> Option<Uuid> {
        self.hash_ring.get_instance(&aggregate_id.to_string())
    }

    /// Adaptive selection (combination of consistent hash and load balancing)
    async fn adaptive_select(&self, aggregate_id: Uuid) -> Option<Uuid> {
        let loads = self.instance_loads.read().await;
        if loads.is_empty() {
            return None;
        }

        // First try consistent hash
        if let Some(instance_id) = self.consistent_hash_select(aggregate_id) {
            // Check if the instance is overloaded
            if let Some(metrics) = loads.get(&instance_id) {
                if metrics.queue_depth < 1000 && metrics.cpu_usage_percent < 80.0 {
                    return Some(instance_id);
                }
            }
        }

        // Fall back to least loaded
        self.least_loaded_select().await
    }

    /// Get load distribution statistics
    pub async fn get_load_distribution(&self) -> serde_json::Value {
        let loads = self.instance_loads.read().await;
        let instances = self.hash_ring.get_instances();
        let instance_count = instances.len();

        let mut distribution = Vec::new();
        for instance_id in &instances {
            if let Some(metrics) = loads.get(instance_id) {
                distribution.push(serde_json::json!({
                    "instance_id": instance_id,
                    "cpu_usage_percent": metrics.cpu_usage_percent,
                    "memory_usage_mb": metrics.memory_usage_mb,
                    "queue_depth": metrics.queue_depth,
                    "active_connections": metrics.active_connections,
                    "events_processed_per_sec": metrics.events_processed_per_sec,
                    "error_rate_percent": metrics.error_rate_percent,
                    "last_updated": metrics.last_updated
                }));
            }
        }

        serde_json::json!({
            "strategy": format!("{:?}", self.strategy),
            "total_instances": instance_count,
            "distribution": distribution
        })
    }
}

/// Shard manager for distributed event processing
#[derive(Debug)]
pub struct ShardManager {
    shard_count: u32,
    shard_assignments: Arc<RwLock<HashMap<u32, ShardAssignment>>>,
    hash_ring: ConsistentHashRing,
    rebalancing_in_progress: Arc<AtomicBool>,
}

impl ShardManager {
    pub fn new(shard_count: u32) -> Self {
        Self {
            shard_count,
            shard_assignments: Arc::new(RwLock::new(HashMap::new())),
            hash_ring: ConsistentHashRing::new(100),
            rebalancing_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Assign shards to instances
    pub async fn assign_shards(&self, instances: Vec<Uuid>) -> Result<()> {
        if instances.is_empty() {
            return Err(anyhow::anyhow!(
                "No instances available for shard assignment"
            ));
        }

        let mut assignments = self.shard_assignments.write().await;
        assignments.clear();

        // Distribute shards evenly across instances
        for shard_id in 0..self.shard_count {
            let instance_index = (shard_id as usize) % instances.len();
            let instance_id = instances[instance_index];

            let assignment = ShardAssignment {
                shard_id,
                instance_id,
                aggregate_ids: Vec::new(),
                last_updated: Utc::now(),
            };

            assignments.insert(shard_id, assignment);
        }

        tracing::info!(
            "Assigned {} shards across {} instances",
            self.shard_count,
            instances.len()
        );

        Ok(())
    }

    /// Get the instance responsible for a shard
    pub async fn get_shard_instance(&self, shard_id: u32) -> Option<Uuid> {
        let assignments = self.shard_assignments.read().await;
        assignments
            .get(&shard_id)
            .map(|assignment| assignment.instance_id)
    }

    /// Get the shard for an aggregate ID
    pub fn get_shard_for_aggregate(&self, aggregate_id: Uuid) -> u32 {
        // Use consistent hashing to determine shard
        let hash = self.hash_aggregate_id(aggregate_id);
        hash % self.shard_count
    }

    /// Get the instance responsible for an aggregate ID
    pub async fn get_instance_for_aggregate(&self, aggregate_id: Uuid) -> Option<Uuid> {
        let shard_id = self.get_shard_for_aggregate(aggregate_id);
        self.get_shard_instance(shard_id).await
    }

    /// Rebalance shards when instances join or leave
    pub async fn rebalance_shards(&self, instances: Vec<Uuid>) -> Result<()> {
        if self
            .rebalancing_in_progress
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(anyhow::anyhow!("Shard rebalancing already in progress"));
        }

        self.rebalancing_in_progress
            .store(true, std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            "Starting shard rebalancing for {} instances",
            instances.len()
        );

        // Perform shard reassignment
        self.assign_shards(instances).await?;

        self.rebalancing_in_progress
            .store(false, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("Shard rebalancing completed");

        Ok(())
    }

    /// Check if rebalancing is in progress
    pub fn is_rebalancing(&self) -> bool {
        self.rebalancing_in_progress
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get shard assignment statistics
    pub async fn get_shard_stats(&self) -> serde_json::Value {
        let assignments = self.shard_assignments.read().await;

        // Count shards per instance
        let mut instance_shard_counts: HashMap<Uuid, u32> = HashMap::new();
        for assignment in assignments.values() {
            *instance_shard_counts
                .entry(assignment.instance_id)
                .or_insert(0) += 1;
        }

        let mut stats = Vec::new();
        for (instance_id, shard_count) in instance_shard_counts {
            stats.push(serde_json::json!({
                "instance_id": instance_id,
                "shard_count": shard_count,
                "shard_percentage": (shard_count as f64 / self.shard_count as f64) * 100.0
            }));
        }

        serde_json::json!({
            "total_shards": self.shard_count,
            "assigned_shards": assignments.len(),
            "rebalancing_in_progress": self.is_rebalancing(),
            "instance_distribution": stats
        })
    }

    /// Hash an aggregate ID to a u32
    fn hash_aggregate_id(&self, aggregate_id: Uuid) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        aggregate_id.hash(&mut hasher);
        hasher.finish() as u32
    }
}

// Phase 2.2.3: Distributed State Management
// =========================================

/// Distributed lock for coordinating access to shared resources
#[derive(Debug, Clone)]
pub struct DistributedLock {
    pub resource_id: String,
    pub instance_id: Uuid,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub lock_id: Uuid,
}

/// Distributed lock manager for coordinating locks across instances
pub struct DistributedLockManager {
    active_locks: Arc<RwLock<HashMap<String, DistributedLock>>>,
}

impl DistributedLockManager {
    pub fn new() -> Self {
        Self {
            active_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn acquire_lock(
        &mut self,
        resource_id: &str,
        ttl_seconds: u64,
    ) -> Result<DistributedLock> {
        let mut locks = self.active_locks.write().await;
        let now = Utc::now();

        // Check if lock already exists and is still valid
        if let Some(existing_lock) = locks.get(resource_id) {
            if existing_lock.expires_at > now {
                return Err(anyhow::anyhow!("Lock already acquired by another instance"));
            }
        }

        // Create new lock
        let lock = DistributedLock {
            resource_id: resource_id.to_string(),
            instance_id: Uuid::new_v4(), // In real implementation, this would be the current instance ID
            acquired_at: now,
            expires_at: now + chrono::Duration::seconds(ttl_seconds as i64),
            lock_id: Uuid::new_v4(),
        };

        locks.insert(resource_id.to_string(), lock.clone());
        Ok(lock)
    }

    pub async fn release_lock(&mut self, lock: DistributedLock) -> Result<()> {
        let mut locks = self.active_locks.write().await;
        locks.remove(&lock.resource_id);
        Ok(())
    }
}

/// Distributed cache entry for sharing data across instances
#[derive(Debug, Clone)]
pub struct DistributedCacheEntry {
    pub key: String,
    pub value: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub version: u64,
}

/// Distributed cache manager for coordinating cache across instances
pub struct DistributedCacheManager {
    cache: Arc<RwLock<HashMap<String, DistributedCacheEntry>>>,
}

impl DistributedCacheManager {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_entry(&mut self, key: &str) -> Result<Option<DistributedCacheEntry>> {
        let cache = self.cache.read().await;
        let now = Utc::now();

        if let Some(entry) = cache.get(key) {
            if entry.expires_at > now {
                Ok(Some(entry.clone()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn set_entry(
        &mut self,
        key: &str,
        value: serde_json::Value,
        ttl_seconds: u64,
    ) -> Result<()> {
        let mut cache = self.cache.write().await;
        let now = Utc::now();

        let entry = DistributedCacheEntry {
            key: key.to_string(),
            value,
            created_at: now,
            expires_at: now + chrono::Duration::seconds(ttl_seconds as i64),
            version: 1,
        };

        cache.insert(key.to_string(), entry);
        Ok(())
    }
}

/// State synchronization manager for coordinating state across instances
pub struct StateSyncManager {
    sync_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl StateSyncManager {
    pub fn new() -> Self {
        Self {
            sync_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start_sync(&mut self) -> Result<()> {
        let handle = tokio::spawn(async move {
            let interval = Duration::from_secs(60); // Sync every minute
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;
                debug!("State synchronization tick");
            }
        });

        let mut sync_handle = self.sync_handle.lock().await;
        *sync_handle = Some(handle);

        info!("State synchronization started");
        Ok(())
    }

    pub async fn stop_sync(&mut self) -> Result<()> {
        let mut sync_handle = self.sync_handle.lock().await;
        if let Some(handle) = sync_handle.take() {
            handle.abort();
        }

        info!("State synchronization stopped");
        Ok(())
    }
}

// Phase 2.2.4: Cluster Management
// ===============================

/// Leader election state for distributed coordination
#[derive(Debug, Clone, PartialEq)]
pub enum LeaderElectionState {
    Follower,
    Candidate,
    Leader,
}

/// Cluster membership information with health status
#[derive(Debug, Clone)]
pub struct ClusterMembership {
    pub instance_id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub status: InstanceStatus,
    pub last_heartbeat: DateTime<Utc>,
    pub failure_count: u32,
    pub is_leader: bool,
    pub term: u64,
    pub voted_for: Option<Uuid>,
}

/// Failure detection configuration
#[derive(Debug, Clone)]
pub struct FailureDetectionConfig {
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub failure_threshold: u32,
    pub suspicion_timeout_ms: u64,
    pub cleanup_interval_ms: u64,
}

impl Default for FailureDetectionConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 1000,
            heartbeat_timeout_ms: 5000,
            failure_threshold: 3,
            suspicion_timeout_ms: 10000,
            cleanup_interval_ms: 30000,
        }
    }
}

/// Distributed configuration for cluster-wide settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedConfig {
    pub cluster_id: String,
    pub version: u64,
    pub config_data: serde_json::Value,
    pub last_updated: DateTime<Utc>,
    pub updated_by: Uuid,
}

/// Enhanced cluster manager with leader election and failure detection
pub struct EnhancedClusterManager {
    config: ClusterConfig,
    instance_info: InstanceInfo,
    members: Arc<RwLock<HashMap<Uuid, ClusterMembership>>>,
    shard_assignments: Arc<RwLock<HashMap<u32, ShardAssignment>>>,
    leader_id: Arc<RwLock<Option<Uuid>>>,
    current_term: Arc<AtomicU64>,
    voted_for: Arc<RwLock<Option<Uuid>>>,
    leader_election_state: Arc<RwLock<LeaderElectionState>>,
    load_balancing_strategy: LoadBalancingStrategy,
    failure_detection_config: FailureDetectionConfig,

    // Task handles
    heartbeat_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    discovery_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    leader_election_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    failure_detection_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    config_sync_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl EnhancedClusterManager {
    pub fn new(config: ClusterConfig) -> Result<Self> {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let instance_info = InstanceInfo {
            instance_id: config.instance_id,
            hostname: hostname.clone(),
            port: config.port,
            status: InstanceStatus::Starting,
            started_at: Utc::now(),
            last_heartbeat: Utc::now(),
            load_metrics: InstanceLoadMetrics::default(),
            shard_assignments: Vec::new(),
        };

        Ok(Self {
            config,
            instance_info,
            members: Arc::new(RwLock::new(HashMap::new())),
            shard_assignments: Arc::new(RwLock::new(HashMap::new())),
            leader_id: Arc::new(RwLock::new(None)),
            current_term: Arc::new(AtomicU64::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            leader_election_state: Arc::new(RwLock::new(LeaderElectionState::Follower)),
            load_balancing_strategy: LoadBalancingStrategy::Adaptive,
            failure_detection_config: FailureDetectionConfig::default(),
            heartbeat_handle: Arc::new(Mutex::new(None)),
            discovery_handle: Arc::new(Mutex::new(None)),
            leader_election_handle: Arc::new(Mutex::new(None)),
            failure_detection_handle: Arc::new(Mutex::new(None)),
            config_sync_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Start the enhanced cluster manager with all coordination services
    pub async fn start(&mut self) -> Result<()> {
        info!(
            "Starting enhanced cluster manager for instance {}",
            self.config.instance_id
        );

        // Register this instance
        self.register_instance().await?;

        // Start all coordination services
        self.start_heartbeat().await?;
        self.start_instance_discovery().await?;
        self.start_leader_election().await?;
        self.start_failure_detection().await?;
        self.start_config_sync().await?;

        // Update instance status
        self.instance_info.status = InstanceStatus::Running;

        info!("Enhanced cluster manager started successfully");
        Ok(())
    }

    /// Stop the enhanced cluster manager and cleanup
    pub async fn stop(&mut self) -> Result<()> {
        info!(
            "Stopping enhanced cluster manager for instance {}",
            self.config.instance_id
        );

        // Update instance status
        self.instance_info.status = InstanceStatus::Stopping;

        // Stop all coordination services
        self.stop_heartbeat().await?;
        self.stop_instance_discovery().await?;
        self.stop_leader_election().await?;
        self.stop_failure_detection().await?;
        self.stop_config_sync().await?;

        // Deregister instance
        self.deregister_instance().await?;

        // Update instance status
        self.instance_info.status = InstanceStatus::Stopped;

        info!("Enhanced cluster manager stopped successfully");
        Ok(())
    }

    /// Register this instance in the cluster
    async fn register_instance(&self) -> Result<()> {
        let membership = ClusterMembership {
            instance_id: self.instance_info.instance_id,
            hostname: self.instance_info.hostname.clone(),
            port: self.instance_info.port,
            status: self.instance_info.status.clone(),
            last_heartbeat: Utc::now(),
            failure_count: 0,
            is_leader: false,
            term: self.current_term.load(std::sync::atomic::Ordering::Relaxed),
            voted_for: None,
        };

        let mut members = self.members.write().await;
        members.insert(self.instance_info.instance_id, membership);

        info!(
            "Registered instance {} in cluster",
            self.instance_info.instance_id
        );
        Ok(())
    }

    /// Deregister this instance from the cluster
    async fn deregister_instance(&self) -> Result<()> {
        let mut members = self.members.write().await;
        members.remove(&self.instance_info.instance_id);

        info!(
            "Deregistered instance {} from cluster",
            self.instance_info.instance_id
        );
        Ok(())
    }

    /// Start heartbeat service for cluster coordination
    async fn start_heartbeat(&mut self) -> Result<()> {
        let config = self.config.clone();
        let instance_info = self.instance_info.clone();
        let members = Arc::clone(&self.members);
        let current_term = Arc::clone(&self.current_term);
        let leader_id = Arc::clone(&self.leader_id);

        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis(config.heartbeat_interval_ms);
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                // Send heartbeat to all members
                let mut members_guard = members.write().await;
                let now = Utc::now();

                for (instance_id, membership) in members_guard.iter_mut() {
                    if *instance_id != instance_info.instance_id {
                        // Update heartbeat timestamp
                        membership.last_heartbeat = now;

                        // If we're the leader, send heartbeat with current term
                        if let Some(leader) = leader_id.read().await.as_ref() {
                            if *leader == instance_info.instance_id {
                                membership.term =
                                    current_term.load(std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                }

                // Update our own heartbeat
                if let Some(our_membership) = members_guard.get_mut(&instance_info.instance_id) {
                    our_membership.last_heartbeat = now;
                }
            }
        });

        let mut heartbeat_handle = self.heartbeat_handle.lock().await;
        *heartbeat_handle = Some(handle);

        info!("Started heartbeat service");
        Ok(())
    }

    /// Stop heartbeat service
    async fn stop_heartbeat(&mut self) -> Result<()> {
        let mut heartbeat_handle = self.heartbeat_handle.lock().await;
        if let Some(handle) = heartbeat_handle.take() {
            handle.abort();
        }
        info!("Stopped heartbeat service");
        Ok(())
    }

    /// Start instance discovery service
    async fn start_instance_discovery(&mut self) -> Result<()> {
        let config = self.config.clone();
        let members = Arc::clone(&self.members);
        let instance_info = self.instance_info.clone();

        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis(config.instance_discovery_timeout_ms);
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                // Discover new instances (in a real implementation, this would use service discovery)
                let members_guard = members.read().await;
                info!("Current cluster members: {}", members_guard.len());

                // Log member information
                for (instance_id, membership) in members_guard.iter() {
                    debug!(
                        "Member: {} - {}:{} - Status: {:?}",
                        instance_id, membership.hostname, membership.port, membership.status
                    );
                }
            }
        });

        let mut discovery_handle = self.discovery_handle.lock().await;
        *discovery_handle = Some(handle);

        info!("Started instance discovery service");
        Ok(())
    }

    /// Stop instance discovery service
    async fn stop_instance_discovery(&mut self) -> Result<()> {
        let mut discovery_handle = self.discovery_handle.lock().await;
        if let Some(handle) = discovery_handle.take() {
            handle.abort();
        }
        info!("Stopped instance discovery service");
        Ok(())
    }

    /// Start leader election service using Raft-like algorithm
    async fn start_leader_election(&mut self) -> Result<()> {
        let config = self.config.clone();
        let members = Arc::clone(&self.members);
        let leader_id = Arc::clone(&self.leader_id);
        let current_term = Arc::clone(&self.current_term);
        let voted_for = Arc::clone(&self.voted_for);
        let leader_election_state = Arc::clone(&self.leader_election_state);
        let instance_info = self.instance_info.clone();

        let handle = tokio::spawn(async move {
            let election_timeout = Duration::from_millis(config.leader_election_timeout_ms);
            let mut election_timer = tokio::time::interval(election_timeout);

            loop {
                election_timer.tick().await;

                let mut state_guard = leader_election_state.write().await;
                let mut members_guard = members.write().await;
                let mut leader_guard = leader_id.write().await;

                match *state_guard {
                    LeaderElectionState::Follower => {
                        // Check if we should start an election
                        let now = Utc::now();
                        let should_start_election = members_guard.values().any(|m| {
                            m.instance_id != instance_info.instance_id
                                && now
                                    .signed_duration_since(m.last_heartbeat)
                                    .num_milliseconds()
                                    > config.heartbeat_timeout_ms as i64
                        });

                        if should_start_election {
                            *state_guard = LeaderElectionState::Candidate;
                            let new_term =
                                current_term.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                            // Vote for ourselves
                            let mut voted_guard = voted_for.write().await;
                            *voted_guard = Some(instance_info.instance_id);

                            info!("Starting leader election for term {}", new_term);
                        }
                    }
                    LeaderElectionState::Candidate => {
                        // Count votes and check if we won
                        let term = current_term.load(std::sync::atomic::Ordering::Relaxed);
                        let mut votes = 1; // Our own vote

                        for membership in members_guard.values() {
                            if membership.instance_id != instance_info.instance_id
                                && membership.term == term
                            {
                                votes += 1;
                            }
                        }

                        let majority = (members_guard.len() / 2) + 1;
                        if votes >= majority {
                            *state_guard = LeaderElectionState::Leader;
                            *leader_guard = Some(instance_info.instance_id);

                            // Update our membership
                            if let Some(our_membership) =
                                members_guard.get_mut(&instance_info.instance_id)
                            {
                                our_membership.is_leader = true;
                                our_membership.term = term;
                            }

                            info!("Elected as leader for term {}", term);
                        }
                    }
                    LeaderElectionState::Leader => {
                        // Continue as leader, send heartbeats
                        let term = current_term.load(std::sync::atomic::Ordering::Relaxed);

                        // Update all members with our leadership
                        for membership in members_guard.values_mut() {
                            if membership.instance_id != instance_info.instance_id {
                                membership.term = term;
                            }
                        }
                    }
                }
            }
        });

        let mut leader_election_handle = self.leader_election_handle.lock().await;
        *leader_election_handle = Some(handle);

        info!("Started leader election service");
        Ok(())
    }

    /// Stop leader election service
    async fn stop_leader_election(&mut self) -> Result<()> {
        let mut leader_election_handle = self.leader_election_handle.lock().await;
        if let Some(handle) = leader_election_handle.take() {
            handle.abort();
        }
        info!("Stopped leader election service");
        Ok(())
    }

    /// Start failure detection service
    async fn start_failure_detection(&mut self) -> Result<()> {
        let config = self.failure_detection_config.clone();
        let members = Arc::clone(&self.members);
        let instance_info = self.instance_info.clone();

        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis(config.cleanup_interval_ms);
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                let mut members_guard = members.write().await;
                let now = Utc::now();

                // Check for failed instances
                let mut failed_instances = Vec::new();

                for (instance_id, membership) in members_guard.iter_mut() {
                    if *instance_id != instance_info.instance_id {
                        let time_since_heartbeat =
                            now.signed_duration_since(membership.last_heartbeat);

                        if time_since_heartbeat.num_milliseconds()
                            > config.heartbeat_timeout_ms as i64
                        {
                            membership.failure_count += 1;

                            if membership.failure_count >= config.failure_threshold {
                                failed_instances.push(*instance_id);
                                warn!(
                                    "Instance {} marked as failed after {} failures",
                                    instance_id, membership.failure_count
                                );
                            }
                        } else {
                            // Reset failure count if heartbeat received
                            membership.failure_count = 0;
                        }
                    }
                }

                // Remove failed instances
                for failed_instance in failed_instances {
                    members_guard.remove(&failed_instance);
                    warn!("Removed failed instance {} from cluster", failed_instance);
                }
            }
        });

        let mut failure_detection_handle = self.failure_detection_handle.lock().await;
        *failure_detection_handle = Some(handle);

        info!("Started failure detection service");
        Ok(())
    }

    /// Stop failure detection service
    async fn stop_failure_detection(&mut self) -> Result<()> {
        let mut failure_detection_handle = self.failure_detection_handle.lock().await;
        if let Some(handle) = failure_detection_handle.take() {
            handle.abort();
        }
        info!("Stopped failure detection service");
        Ok(())
    }

    /// Start distributed configuration synchronization
    async fn start_config_sync(&mut self) -> Result<()> {
        let config = self.config.clone();
        let members = Arc::clone(&self.members);
        let instance_info = self.instance_info.clone();

        let handle = tokio::spawn(async move {
            let interval = Duration::from_secs(30); // Sync every 30 seconds
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                // In a real implementation, this would sync configuration across all instances
                let members_guard = members.read().await;
                debug!(
                    "Syncing configuration across {} cluster members",
                    members_guard.len()
                );

                // Example: Sync cluster configuration
                let cluster_config = DistributedConfig {
                    cluster_id: config.cluster_id.clone(),
                    version: 1,
                    config_data: serde_json::json!({
                        "shard_count": config.shard_count,
                        "heartbeat_interval_ms": config.heartbeat_interval_ms,
                        "leader_election_timeout_ms": config.leader_election_timeout_ms,
                    }),
                    last_updated: Utc::now(),
                    updated_by: instance_info.instance_id,
                };

                debug!("Distributed config: {:?}", cluster_config);
            }
        });

        let mut config_sync_handle = self.config_sync_handle.lock().await;
        *config_sync_handle = Some(handle);

        info!("Started configuration synchronization service");
        Ok(())
    }

    /// Stop configuration synchronization service
    async fn stop_config_sync(&mut self) -> Result<()> {
        let mut config_sync_handle = self.config_sync_handle.lock().await;
        if let Some(handle) = config_sync_handle.take() {
            handle.abort();
        }
        info!("Stopped configuration synchronization service");
        Ok(())
    }

    /// Get comprehensive cluster status
    pub async fn get_cluster_status(&self) -> serde_json::Value {
        let members = self.members.read().await;
        let leader_id = self.leader_id.read().await;
        let current_term = self.current_term.load(std::sync::atomic::Ordering::Relaxed);
        let leader_election_state = self.leader_election_state.read().await;

        let leader_id_value = leader_id.as_ref().map(|id| id.to_string());

        let member_list: Vec<serde_json::Value> = members
            .values()
            .map(|m| {
                serde_json::json!({
                    "instance_id": m.instance_id,
                    "hostname": m.hostname,
                    "port": m.port,
                    "status": format!("{:?}", m.status),
                    "last_heartbeat": m.last_heartbeat,
                    "failure_count": m.failure_count,
                    "is_leader": m.is_leader,
                    "term": m.term,
                })
            })
            .collect();

        serde_json::json!({
            "cluster_id": self.config.cluster_id,
            "instance_id": self.instance_info.instance_id,
            "current_term": current_term,
            "leader_election_state": format!("{:?}", *leader_election_state),
            "leader_id": leader_id_value,
            "member_count": members.len(),
            "members": member_list,
            "shard_count": self.config.shard_count,
            "load_balancing_strategy": format!("{:?}", self.load_balancing_strategy),
        })
    }

    /// Check if this instance is the current leader
    pub async fn is_leader(&self) -> bool {
        let leader_id = self.leader_id.read().await;
        leader_id.as_ref() == Some(&self.instance_info.instance_id)
    }

    /// Get the current leader ID
    pub async fn get_leader_id(&self) -> Option<Uuid> {
        let leader_id = self.leader_id.read().await;
        leader_id.clone()
    }

    /// Get current term
    pub fn get_current_term(&self) -> u64 {
        self.current_term.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get all cluster members
    pub async fn get_members(&self) -> Vec<ClusterMembership> {
        let members = self.members.read().await;
        members.values().cloned().collect()
    }

    /// Update load metrics for this instance
    pub async fn update_load_metrics(&mut self, metrics: InstanceLoadMetrics) {
        self.instance_info.load_metrics = metrics;

        let mut members = self.members.write().await;
        if let Some(membership) = members.get_mut(&self.instance_info.instance_id) {
            membership.last_heartbeat = Utc::now();
        }
    }

    /// Get cluster health status
    pub async fn get_cluster_health(&self) -> serde_json::Value {
        let members = self.members.read().await;
        let now = Utc::now();

        let mut healthy_count = 0;
        let mut failed_count = 0;
        let mut total_failure_count = 0;

        for membership in members.values() {
            let time_since_heartbeat = now.signed_duration_since(membership.last_heartbeat);

            if time_since_heartbeat.num_milliseconds()
                <= self.failure_detection_config.heartbeat_timeout_ms as i64
            {
                healthy_count += 1;
            } else {
                failed_count += 1;
                total_failure_count += membership.failure_count;
            }
        }

        serde_json::json!({
            "total_members": members.len(),
            "healthy_members": healthy_count,
            "failed_members": failed_count,
            "total_failure_count": total_failure_count,
            "health_percentage": if members.is_empty() { 0.0 } else {
                (healthy_count as f64 / members.len() as f64) * 100.0
            },
        })
    }
}

// Phase 2.2: Scalability Integration Methods
// =========================================

impl UltraOptimizedCDCEventProcessor {
    /// Initialize and start the enhanced cluster manager
    pub async fn start_cluster_manager(&mut self, config: ClusterConfig) -> Result<()> {
        let mut cluster_manager = EnhancedClusterManager::new(config)?;
        cluster_manager.start().await?;

        let mut cluster_guard = self.cluster_manager.lock().await;
        *cluster_guard = Some(cluster_manager);

        info!("Enhanced cluster manager started successfully");
        Ok(())
    }

    /// Stop the enhanced cluster manager
    pub async fn stop_cluster_manager(&mut self) -> Result<()> {
        let mut cluster_guard = self.cluster_manager.lock().await;
        if let Some(mut cluster_manager) = cluster_guard.take() {
            cluster_manager.stop().await?;
        }

        info!("Enhanced cluster manager stopped successfully");
        Ok(())
    }

    /// Get cluster status and health information
    pub async fn get_cluster_status(&self) -> Result<serde_json::Value> {
        let cluster_guard = self.cluster_manager.lock().await;
        if let Some(cluster_manager) = cluster_guard.as_ref() {
            Ok(cluster_manager.get_cluster_status().await)
        } else {
            Err(anyhow::anyhow!("Cluster manager not initialized"))
        }
    }

    /// Check if this instance is the cluster leader
    pub async fn is_cluster_leader(&self) -> Result<bool> {
        let cluster_guard = self.cluster_manager.lock().await;
        if let Some(cluster_manager) = cluster_guard.as_ref() {
            Ok(cluster_manager.is_leader().await)
        } else {
            Err(anyhow::anyhow!("Cluster manager not initialized"))
        }
    }

    /// Get cluster health metrics
    pub async fn get_cluster_health(&self) -> Result<serde_json::Value> {
        let cluster_guard = self.cluster_manager.lock().await;
        if let Some(cluster_manager) = cluster_guard.as_ref() {
            Ok(cluster_manager.get_cluster_health().await)
        } else {
            Err(anyhow::anyhow!("Cluster manager not initialized"))
        }
    }

    /// Update load metrics for this instance
    pub async fn update_load_metrics(&mut self, metrics: InstanceLoadMetrics) -> Result<()> {
        let mut cluster_guard = self.cluster_manager.lock().await;
        if let Some(cluster_manager) = cluster_guard.as_mut() {
            cluster_manager.update_load_metrics(metrics.clone()).await;
        }

        // Also update load balancer
        let mut load_balancer = self.load_balancer.lock().await;
        if let Some(cluster_manager) = self.cluster_manager.lock().await.as_ref() {
            load_balancer
                .update_instance_load(cluster_manager.config.instance_id, metrics)
                .await;
        }

        Ok(())
    }

    /// Acquire a distributed lock for a resource
    pub async fn acquire_distributed_lock(
        &self,
        resource_id: &str,
        ttl_seconds: u64,
    ) -> Result<DistributedLock> {
        let mut lock_manager = self.distributed_lock_manager.lock().await;
        lock_manager.acquire_lock(resource_id, ttl_seconds).await
    }

    /// Release a distributed lock
    pub async fn release_distributed_lock(&self, lock: DistributedLock) -> Result<()> {
        let mut lock_manager = self.distributed_lock_manager.lock().await;
        lock_manager.release_lock(lock).await
    }

    /// Get or set a distributed cache entry
    pub async fn get_distributed_cache_entry(
        &self,
        key: &str,
    ) -> Result<Option<DistributedCacheEntry>> {
        let mut cache_manager = self.distributed_cache_manager.lock().await;
        cache_manager.get_entry(key).await
    }

    /// Set a distributed cache entry
    pub async fn set_distributed_cache_entry(
        &self,
        key: &str,
        value: serde_json::Value,
        ttl_seconds: u64,
    ) -> Result<()> {
        let mut cache_manager = self.distributed_cache_manager.lock().await;
        cache_manager.set_entry(key, value, ttl_seconds).await
    }

    /// Get shard information for an aggregate
    pub async fn get_shard_for_aggregate(&self, aggregate_id: Uuid) -> Result<u32> {
        let shard_manager = self.shard_manager.lock().await;
        Ok(shard_manager.get_shard_for_aggregate(aggregate_id))
    }

    /// Get the instance responsible for a specific aggregate
    pub async fn get_instance_for_aggregate(&self, aggregate_id: Uuid) -> Result<Option<Uuid>> {
        let shard_manager = self.shard_manager.lock().await;
        Ok(shard_manager.get_instance_for_aggregate(aggregate_id).await)
    }

    /// Get load balancing statistics
    pub async fn get_load_balancing_stats(&self) -> Result<serde_json::Value> {
        let load_balancer = self.load_balancer.lock().await;
        Ok(load_balancer.get_load_distribution().await)
    }

    /// Get shard management statistics
    pub async fn get_shard_stats(&self) -> Result<serde_json::Value> {
        let shard_manager = self.shard_manager.lock().await;
        Ok(shard_manager.get_shard_stats().await)
    }

    /// Process an event with distributed coordination
    pub async fn process_event_with_distributed_coordination(
        &self,
        cdc_event: serde_json::Value,
    ) -> Result<()> {
        // Extract event information
        let processable_event = match self.extract_event_serde_json(&cdc_event)? {
            Some(event) => event,
            None => return Ok(()),
        };

        // Get shard for this aggregate
        let shard_id = self
            .get_shard_for_aggregate(processable_event.aggregate_id)
            .await?;

        // Check if this instance should process this event
        let target_instance = self
            .get_instance_for_aggregate(processable_event.aggregate_id)
            .await?;
        let cluster_guard = self.cluster_manager.lock().await;

        if let Some(cluster_manager) = cluster_guard.as_ref() {
            let current_instance_id = cluster_manager.config.instance_id;

            if let Some(target_id) = target_instance {
                if target_id != current_instance_id {
                    // This event should be processed by another instance
                    // In a real implementation, you would forward this event
                    debug!(
                        "Event {} should be processed by instance {}, forwarding",
                        processable_event.event_id, target_id
                    );
                    return Ok(());
                }
            }
        }

        // Acquire distributed lock for this aggregate
        let lock_key = format!("aggregate:{}", processable_event.aggregate_id);
        let lock = self.acquire_distributed_lock(&lock_key, 30).await?;

        // Process the event
        let result = self.process_cdc_event_ultra_fast(cdc_event).await;

        // Release the lock
        self.release_distributed_lock(lock).await?;

        result
    }

    /// Get comprehensive scalability statistics
    pub async fn get_scalability_stats(&self) -> Result<serde_json::Value> {
        let cluster_status = self
            .get_cluster_status()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        let cluster_health = self
            .get_cluster_health()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        let load_balancing_stats = self
            .get_load_balancing_stats()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        let shard_stats = self
            .get_shard_stats()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));

        Ok(serde_json::json!({
            "cluster_status": cluster_status,
            "cluster_health": cluster_health,
            "load_balancing": load_balancing_stats,
            "shard_management": shard_stats,
            "distributed_locks": {
                "active_locks": 0, // Would be implemented with actual lock tracking
            },
            "distributed_cache": {
                "entries": 0, // Would be implemented with actual cache tracking
            },
        }))
    }

    /// Rebalance shards across available instances
    pub async fn rebalance_shards(&self) -> Result<()> {
        let cluster_guard = self.cluster_manager.lock().await;
        let mut shard_manager = self.shard_manager.lock().await;

        if let Some(cluster_manager) = cluster_guard.as_ref() {
            let members = cluster_manager.get_members().await;
            let instance_ids: Vec<Uuid> = members.iter().map(|m| m.instance_id).collect();

            shard_manager.rebalance_shards(instance_ids).await?;
            info!("Shard rebalancing completed");
        }

        Ok(())
    }

    /// Start state synchronization for distributed state management
    pub async fn start_state_sync(&mut self) -> Result<()> {
        let mut state_sync_manager = self.state_sync_manager.lock().await;
        state_sync_manager.start_sync().await?;
        info!("State synchronization started");
        Ok(())
    }

    /// Stop state synchronization
    pub async fn stop_state_sync(&mut self) -> Result<()> {
        let mut state_sync_manager = self.state_sync_manager.lock().await;
        state_sync_manager.stop_sync().await
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
        let scalability_stats = self
            .get_scalability_stats()
            .await
            .unwrap_or_else(|_| serde_json::json!({"error": "Failed to get scalability stats"}));
        let cluster_status = self
            .get_cluster_status()
            .await
            .unwrap_or_else(|_| serde_json::json!({"error": "Failed to get cluster status"}));
        let dlq_stats = self
            .get_dlq_stats()
            .await
            .unwrap_or_else(|_| serde_json::json!({"error": "Failed to get DLQ stats"}));

        Ok(serde_json::json!({
            "health_status": health_status,
            "metrics_summary": metrics_summary,
            "performance_stats": performance_stats,
            "scalability_stats": scalability_stats,
            "cluster_status": cluster_status,
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
