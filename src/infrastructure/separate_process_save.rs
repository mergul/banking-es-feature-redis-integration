use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics;
use crate::infrastructure::consistency_manager::ConsistencyManager;
use crate::infrastructure::dlq_router::dlq_router::DLQRouter;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use crate::infrastructure::CDCBatchingConfig;
use crate::infrastructure::CDCBatchingService;
use crate::infrastructure::CDCBatchingServiceTrait;
use crate::infrastructure::CopyOptimizationConfig;
use crate::infrastructure::ProjectionConfig;
use crate::ProjectionStore;
use anyhow::Result;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures::FutureExt;
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
use tokio_util::sync::CancellationToken;
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
    timestamp: chrono::DateTime<chrono::Utc>, // FIXED: Add timestamp field for transaction projections
}

impl ProcessableEvent {
    fn new(
        event_id: Uuid,
        aggregate_id: Uuid,
        event_type: String,
        payload: Vec<u8>,
        partition_key: Option<String>,
        timestamp: chrono::DateTime<chrono::Utc>, // FIXED: Add timestamp parameter
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
            timestamp, // FIXED: Use provided timestamp
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
            recovery_timeout: Duration::from_secs(30), // Increased timeout
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
    access_order: VecDeque<Uuid>,
    max_size: usize,
    ttl: Duration,
    duplicate_detection: DashMap<Uuid, Uuid>, // event_id -> aggregate_id for duplicate detection
}

impl ProjectionCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: HashMap::new(),
            access_order: VecDeque::new(),
            max_size,
            ttl,
            duplicate_detection: DashMap::new(),
        }
    }

    // OPTIMIZATION: Use read lock when possible, write lock only when needed
    fn get(&mut self, key: &Uuid) -> Option<&ProjectionCacheEntry> {
        if let Some(entry) = self.cache.get(key) {
            // Check if entry is expired
            if entry.is_expired(self.ttl) {
                // Don't call remove_expired here to avoid borrow checker issues
                return None;
            }

            // Update access order (move to front)
            if let Some(pos) = self.access_order.iter().position(|&id| id == *key) {
                self.access_order.remove(pos);
            }
            self.access_order.push_back(*key);

            Some(entry)
        } else {
            None
        }
    }

    // OPTIMIZATION: Efficient removal with minimal lock contention
    fn remove_expired(&mut self, key: &Uuid) {
        self.cache.remove(key);
        if let Some(pos) = self.access_order.iter().position(|&id| id == *key) {
            self.access_order.remove(pos);
        }
    }

    // OPTIMIZATION: Batch put operations to reduce lock contention
    fn put(&mut self, key: Uuid, entry: ProjectionCacheEntry) {
        // Remove from access order if already exists
        if let Some(pos) = self.access_order.iter().position(|&id| id == key) {
            self.access_order.remove(pos);
        }

        // Add to cache
        self.cache.insert(key, entry);

        // Add to access order
        self.access_order.push_back(key);

        // OPTIMIZATION: Evict oldest entries if cache is full
        while self.cache.len() > self.max_size {
            if let Some(oldest_key) = self.access_order.pop_front() {
                self.cache.remove(&oldest_key);
            }
        }
    }

    // OPTIMIZATION: Efficient invalidation
    fn invalidate(&mut self, key: &Uuid) {
        self.cache.remove(key);
        if let Some(pos) = self.access_order.iter().position(|&id| id == *key) {
            self.access_order.remove(pos);
        }
    }

    // OPTIMIZATION: Memory usage calculation with minimal overhead
    fn memory_usage(&self) -> usize {
        let mut total = 0;

        // Calculate memory usage for cache entries
        for (key, entry) in &self.cache {
            total += std::mem::size_of_val(key);
            total += std::mem::size_of_val(entry);
            total += entry.owner_name.capacity();
        }

        // Calculate memory usage for access order
        total += self.access_order.capacity() * std::mem::size_of::<Uuid>();

        // Calculate memory usage for duplicate detection
        total += self.duplicate_detection.len() * std::mem::size_of::<(Uuid, Uuid)>();

        total
    }

    // OPTIMIZATION: Efficient duplicate detection with DashMap
    fn is_duplicate_event(&self, event_id: &Uuid, aggregate_id: &Uuid) -> bool {
        // Check if this event has been processed before
        if let Some(existing_aggregate_id) = self.duplicate_detection.get(event_id) {
            return *existing_aggregate_id == *aggregate_id;
        }

        // Record this event
        self.duplicate_detection.insert(*event_id, *aggregate_id);
        false
    }

    // OPTIMIZATION: Periodic cleanup to prevent memory leaks
    fn cleanup_duplicates(&mut self) {
        // Remove old entries from duplicate detection (keep last 1000)
        if self.duplicate_detection.len() > 1000 {
            let to_remove: Vec<Uuid> = self
                .duplicate_detection
                .iter()
                .take(self.duplicate_detection.len() - 1000)
                .map(|entry| *entry.key())
                .collect();

            for key in to_remove {
                self.duplicate_detection.remove(&key);
            }
        }
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
    projection_cache: Arc<RwLock<ProjectionCache>>,

    // Batch processing coordination with interior mutability
    batch_processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_token: CancellationToken,
    // NEW: Multi-instance CDC Batching Service
    cdc_batching_service:
        Option<Arc<crate::infrastructure::cdc_batching_service::PartitionedCDCBatching>>,

    // CDC Batching Service requires write pool
    write_pool: Option<Arc<sqlx::PgPool>>,
    // REMOVED: Accumulation batching - using ProjectionStore standard batching instead
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
            batch_processing_enabled: true,  // ✅ DÜZELTME: Batch processing'i etkinleştir
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
            shutdown_token: self.shutdown_token.clone(),
            cdc_batching_service: self.cdc_batching_service.clone(),
            write_pool: self.write_pool.clone(),
            // REMOVED: Accumulation batching - using ProjectionStore standard batching instead
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
        consistency_manager: Option<Arc<ConsistencyManager>>,
    ) -> Self {
        Self::new_with_write_pool(
            kafka_producer,
            cache_service,
            projection_store,
            metrics,
            business_config,
            consistency_manager,
            None,
            None, // cdc_batching_service
        )
    }

    pub fn new_with_write_pool(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        metrics: Arc<EnhancedCDCMetrics>,
        business_config: Option<BusinessLogicConfig>,
        consistency_manager: Option<Arc<ConsistencyManager>>,
        write_pool: Option<Arc<sqlx::PgPool>>,
        cdc_batching_service: Option<
            Arc<crate::infrastructure::cdc_batching_service::PartitionedCDCBatching>,
        >,
    ) -> Self {
        let business_config: BusinessLogicConfig = business_config.unwrap_or_default();

        // 1. Increase cache size and TTL with advanced configuration
        let projection_cache = Arc::new(RwLock::new(ProjectionCache::new(
            50000,                     // Max 50k cached projections
            Duration::from_secs(1800), // 30 minute TTL
        )));

        let batch_timeout = Duration::from_millis(25); // Reduced from 50ms to 5ms for ultra-fast processing
        let initial_batch_size = if business_config.batch_processing_enabled {
            2000 // ✅ DÜZELTME: Batch size'ı 2000'e çıkar
        } else {
            // When batch processing is disabled, use a reasonable batch size
            // to avoid processing single events immediately
            100 // ✅ DÜZELTME: Batch processing devre dışı bırakıldığında 100 olarak ayarla
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
            processing_semaphore: Arc::new(Semaphore::new(32)),
            batch_queue: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            batch_size: Arc::new(Mutex::new(initial_batch_size)),
            batch_timeout,
            circuit_breaker: AdvancedCircuitBreaker::new(),
            projection_cache,
            batch_processor_handle: Arc::new(Mutex::new(None)),
            shutdown_token: CancellationToken::new(),
            cdc_batching_service,
            write_pool,
        }
    }

    pub async fn process_cdc_events_batch(&self, cdc_events: Vec<serde_json::Value>) -> Result<()> {
        if cdc_events.is_empty() {
            return Ok(());
        }

        let start_time = Instant::now();
        let batch_size = cdc_events.len();
        tracing::info!(
            "🔍 CDC Event Processor: Starting batch processing of {} events",
            batch_size
        );

        // CRITICAL OPTIMIZATION: Process all events together without chunking
        let mut processable_events = Vec::with_capacity(batch_size);

        // Extract and validate all events first
        for cdc_event in &cdc_events {
            match self.extract_event_serde_json(cdc_event) {
                Ok(Some(event)) => {
                    let aggregate_id = event.aggregate_id;
                    let event_type = &event.event_type;

                    tracing::info!(
                        "🔍 CDC Event Processor: Successfully extracted event - aggregate_id: {}, event_type: {}, event_id: {}",
                        aggregate_id, event_type, event.event_id
                    );

                    processable_events.push(event);
                    tracing::debug!(
                        "CDC Event Processor: Successfully extracted event for aggregate_id: {}",
                        aggregate_id
                    );
                }
                Ok(None) => {
                    // Skip events that don't match our criteria
                    tracing::debug!(
                        "CDC Event Processor: Skipping event that doesn't match criteria"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::error!("CDC Event Processor: Failed to extract event: {:?}", e);
                    continue;
                }
            }
        }

        tracing::info!(
            "CDC Event Processor: Extracted {} processable events from {} CDC events",
            processable_events.len(),
            batch_size
        );

        if processable_events.is_empty() {
            tracing::info!("CDC Event Processor: No processable events found");
            return Ok(());
        }

        // Group events by aggregate for efficient processing
        let events_by_aggregate = Self::group_events_by_aggregate(processable_events);
        let aggregate_count = events_by_aggregate.len();

        let mut success_count = 0;
        let mut error_count = 0;

        // CRITICAL OPTIMIZATION: Process all aggregates in batch instead of individually
        match UltraOptimizedCDCEventProcessor::process_events_for_aggregates_static(
            self,
            events_by_aggregate,
            &self.projection_cache,
            &self.cache_service,
            &self.projection_store,
            &self.metrics,
        )
        .await
        {
            Ok((projections, processed_aggregates)) => {
                success_count += processed_aggregates.len();

                // CRITICAL: Use CDC Batching Service for optimal performance
                if !projections.is_empty() {
                    let projections_len = projections.len();

                    // Use CDC Batching Service if available
                    if let Some(ref batching_service) = self.cdc_batching_service {
                        // Convert projections to (aggregate_id, projection) format for bulk processing
                        let projections_for_bulk: Vec<(
                            Uuid,
                            crate::infrastructure::projections::AccountProjection,
                        )> = projections.iter().map(|p| (p.id, p.clone())).collect();

                        // Submit ALL projections as bulk (like write_batching) - NO CHUNKING
                        // Note: CDC Batching Service will internally separate AccountCreated from other events
                        match batching_service
                            .submit_projections_bulk(projections_for_bulk)
                            .await
                        {
                            Ok(operation_ids) => {
                                tracing::debug!("CDC Event Processor: Successfully submitted {} projections as bulk to CDC Batching Service", projections_len);

                                // Wait for all operations to complete
                                let mut success_count = 0;
                                for operation_id in operation_ids {
                                    match batching_service.wait_for_result(operation_id).await {
                                        Ok(result) => {
                                            if result.success {
                                                success_count += 1;
                                            } else {
                                                error_count += 1;
                                                tracing::error!("CDC Event Processor: CDC Batching Service operation failed: {:?}", result.error);
                                            }
                                        }
                                        Err(e) => {
                                            error_count += 1;
                                            tracing::error!("CDC Event Processor: Failed to wait for CDC Batching Service result: {:?}", e);
                                        }
                                    }
                                }

                                if success_count == projections_len {
                                    tracing::debug!("CDC Event Processor: Successfully batch upserted {} projections via CDC Batching Service", projections_len);
                                } else {
                                    tracing::error!("CDC Event Processor: CDC Batching Service failed for {} projections ({} success, {} errors)", projections_len, success_count, error_count);
                                }
                            }
                            Err(e) => {
                                error_count += 1;
                                tracing::error!("CDC Event Processor: Failed to submit projections as bulk to CDC Batching Service: {:?}", e);
                            }
                        }
                    } else {
                        // Fallback to direct projection store
                        tracing::info!("CDC Event Processor: CDC Batching Service not available, falling back to direct projection store");
                        match self
                            .projection_store
                            .upsert_accounts_batch(projections)
                            .await
                        {
                            Ok(_) => {
                                tracing::debug!(
                                "CDC Event Processor: Successfully batch upserted {} projections",
                                projections_len
                            );
                            }
                            Err(e) => {
                                error_count += 1;
                                tracing::error!(
                                    "CDC Event Processor: Failed to batch upsert projections: {:?}",
                                    e
                                );
                            }
                        }
                    }
                } else {
                    tracing::debug!("CDC Event Processor: No projections to process in batch");
                }
            }
            Err(e) => {
                error_count += 1;
                tracing::error!(
                    "CDC Event Processor: Failed to process aggregates in batch: {:?}",
                    e
                );
            }
        }

        let duration = start_time.elapsed();
        tracing::info!(
            "🔍 CDC Event Processor: Batch processing completed - {} events → {} aggregates in {:?} ({} success, {} errors)",
            batch_size, aggregate_count, duration, success_count, error_count
        );

        // Update batch metrics
        self.metrics
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.metrics
            .events_processed
            .fetch_add(success_count as u64, std::sync::atomic::Ordering::Relaxed);

        self.metrics
            .events_failed
            .fetch_add(error_count, std::sync::atomic::Ordering::Relaxed);

        if error_count > 0 {
            return Err(anyhow::anyhow!(
                "Batch processing had {} errors out of {} events",
                error_count,
                batch_size
            ));
        }

        Ok(())
    }
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

        // FIXED: Extract timestamp from CDC event data
        let timestamp = event_data
            .get("created_at")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|| chrono::Utc::now()); // Fallback to current time if not available

        let payload_str = event_data
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;

        //tracing::info!("Raw payload string from CDC event: {}", payload_str);

        // With the producer now using COPY BINARY, the payload from Debezium is just Base64 encoded.
        let payload_bytes = match BASE64_STANDARD.decode(payload_str) {
            Ok(bytes) => bytes,
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

        let domain_event_result =
            bincode::deserialize::<crate::domain::AccountEvent>(&payload_bytes);

        match domain_event_result {
            Ok(domain_event) => {
                tracing::info!(
                    "✅ Domain event deserialized successfully: {:?}",
                    domain_event
                );
                ProcessableEvent::new(
                    event_id,
                    aggregate_id,
                    event_type,
                    payload_bytes,
                    partition_key,
                    timestamp, // FIXED: Use event timestamp
                )
                .map(Some)
            }
            Err(e) => {
                tracing::error!(
                    "❌ Failed to deserialize domain event for event_id={}, aggregate_id={}, event_type={}: {}",
                    event_id, aggregate_id, event_type, e
                );
                tracing::error!(
                    "❌ Payload bytes (first 100): {:?}",
                    &payload_bytes[..payload_bytes.len().min(100)]
                );
                Err(anyhow::anyhow!(
                    "Domain event deserialization failed: {}",
                    e
                ))
            }
        }
    }
    fn separate_account_created_events(
        events_by_aggregate: HashMap<Uuid, Vec<ProcessableEvent>>,
    ) -> (
        HashMap<Uuid, Vec<ProcessableEvent>>,
        HashMap<Uuid, Vec<ProcessableEvent>>,
    ) {
        let mut account_created_events = HashMap::new();
        let mut other_events = HashMap::new();

        for (aggregate_id, events) in events_by_aggregate {
            let mut has_account_created = false;
            let mut account_created_event = None;
            let mut other_aggregate_events: Vec<ProcessableEvent> = Vec::new();

            // Check if this aggregate has AccountCreated event
            for event in &events {
                if let Ok(domain_event) = event.get_domain_event() {
                    if let crate::domain::AccountEvent::AccountCreated { .. } = domain_event {
                        has_account_created = true;
                        account_created_event = Some(event.clone());
                        break; // Found AccountCreated, stop here
                    }
                }
            }

            if has_account_created {
                // This aggregate has AccountCreated - process separately
                if let Some(account_created) = account_created_event {
                    account_created_events.insert(aggregate_id, vec![account_created]);
                }
            } else {
                // This aggregate has only other events - process with UPSERT
                other_events.insert(aggregate_id, events);
            }
        }

        (account_created_events, other_events)
    }
    async fn process_events_for_aggregates_static(
        &self,
        events_by_aggregate: HashMap<Uuid, Vec<ProcessableEvent>>,
        projection_cache: &Arc<RwLock<ProjectionCache>>,
        cache_service: &Arc<dyn CacheServiceTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Result<(
        Vec<crate::infrastructure::projections::AccountProjection>,
        Vec<Uuid>,
    )> {
        let start_time = std::time::Instant::now();

        // CRITICAL OPTIMIZATION: Separate AccountCreated events from other events
        let (account_created_events, other_events) =
            Self::separate_account_created_events(events_by_aggregate);

        tracing::info!(
            "🚀 OPTIMIZED: Separated events - AccountCreated: {} aggregates, Other events: {} aggregates",
            account_created_events.len(),
            other_events.len()
        );

        let mut all_projections = Vec::new();
        let mut processed_aggregates = Vec::new();

        // CRITICAL OPTIMIZATION: Process AccountCreated events with direct COPY (INSERT)
        if let Some(ref batching_service) = self.cdc_batching_service {
            if !account_created_events.is_empty() {
                let account_created_operation_ids = batching_service
                    .submit_account_created_projections_bulk(account_created_events)
                    .await?;

                tracing::info!("🚀 OPTIMIZED: Processed AccountCreated events with direct COPY");
            }

            // Process other events with standard UPSERT approach
            if !other_events.is_empty() {
                let (updated_projections, updated_aggregates) =
                    Self::process_other_events_with_upsert(
                        other_events,
                        projection_cache,
                        cache_service,
                        projection_store,
                        metrics,
                    )
                    .await?;
                let operation_ids = batching_service
                    .submit_other_events_bulk(projections, event_type.clone())
                    .await?;
                let updated_aggregates_len = updated_aggregates.len();
                all_projections.extend(updated_projections);
                processed_aggregates.extend(updated_aggregates);

                tracing::info!(
                    "🚀 OPTIMIZED: Processed {} other events with UPSERT",
                    updated_aggregates_len
                );
            }
        }

        let duration = start_time.elapsed();
        tracing::info!(
            "🚀 OPTIMIZED: Processed {} aggregates with {} projections in {:?}",
            processed_aggregates.len(),
            all_projections.len(),
            duration
        );

        Ok((all_projections, processed_aggregates))
    }
    async fn process_other_events_with_upsert(
        other_events: HashMap<Uuid, Vec<ProcessableEvent>>,
        projection_cache: &Arc<RwLock<ProjectionCache>>,
        cache_service: &Arc<dyn CacheServiceTrait>,
        projection_store: &Arc<dyn ProjectionStoreTrait>,
        metrics: &Arc<EnhancedCDCMetrics>,
    ) -> Result<(
        Vec<crate::infrastructure::projections::AccountProjection>,
        Vec<Uuid>,
    )> {
        // Use existing logic for processing other events (MoneyDeposited, MoneyWithdrawn, etc.)
        // This is the original process_events_for_aggregates logic for non-AccountCreated events

        let mut updated_projections = Vec::new();
        let mut processed_aggregates = Vec::new();

        // CRITICAL OPTIMIZATION: Batch read all projections in single SQL query
        let aggregate_ids: Vec<Uuid> = other_events.keys().cloned().collect();
        tracing::debug!(
            "🚀 OPTIMIZED: Batch loading projections for {} aggregates (non-AccountCreated)",
            aggregate_ids.len()
        );

        // Single SQL query to load all projections
        let db_projections = match projection_store.get_accounts_batch(&aggregate_ids).await {
            Ok(projections) => {
                tracing::debug!(
                    "🚀 OPTIMIZED: Successfully loaded {} projections from database",
                    projections.len()
                );
                projections
            }
            Err(e) => {
                tracing::error!("🚀 OPTIMIZED: Failed to batch load projections: {:?}", e);
                return Err(anyhow::anyhow!("Batch projection load failed: {:?}", e));
            }
        };

        // Convert to HashMap for fast lookup
        let mut projections_map: HashMap<Uuid, ProjectionCacheEntry> = HashMap::new();
        for db_projection in db_projections {
            let cache_entry = ProjectionCacheEntry {
                balance: db_projection.balance,
                owner_name: db_projection.owner_name.clone(),
                is_active: db_projection.is_active,
                version: 0,
                cached_at: tokio::time::Instant::now(),
                last_event_id: None,
            };
            projections_map.insert(db_projection.id, cache_entry);
        }

        // Process events for each aggregate
        for (aggregate_id, events) in other_events {
            let mut projection =
                if let Some(existing_projection) = projections_map.get(&aggregate_id).cloned() {
                    existing_projection
                } else {
                    // Skip aggregates without existing projections (should not happen for non-AccountCreated events)
                    tracing::warn!(
                "🚀 OPTIMIZED: No existing projection for aggregate {} (non-AccountCreated events)",
                aggregate_id
            );
                    continue;
                };

            // Apply all events to the projection
            for event in events {
                if let Err(e) = Self::apply_event_to_projection(&mut projection, &event) {
                    tracing::error!(
                        "Failed to apply event {} to projection: {:?}",
                        event.event_id,
                        e
                    );
                    continue;
                }
            }

            // Convert to database projection
            let db_projection = Self::projection_cache_to_db_projection(&projection, aggregate_id);
            updated_projections.push(db_projection);
            processed_aggregates.push(aggregate_id);
        }

        Ok((updated_projections, processed_aggregates))
    }
    fn group_events_by_aggregate(
        events: Vec<ProcessableEvent>,
    ) -> HashMap<Uuid, Vec<ProcessableEvent>> {
        let mut events_by_aggregate: HashMap<Uuid, Vec<ProcessableEvent>> = HashMap::new();

        // Group events by aggregate ID
        for event in events {
            events_by_aggregate
                .entry(event.aggregate_id)
                .or_default()
                .push(event);
        }

        // CRITICAL FIX: Sort events within each aggregate by timestamp to ensure proper ordering
        for (_, aggregate_events) in &mut events_by_aggregate {
            aggregate_events.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }

        events_by_aggregate
    }
}
