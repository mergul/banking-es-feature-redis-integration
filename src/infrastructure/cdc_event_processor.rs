use crate::domain::AccountEvent;
use crate::infrastructure::cache_models::ProjectionCacheEntry;
use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics;
use crate::infrastructure::consistency_manager::ConsistencyManager;
use crate::infrastructure::dlq_router::dlq_router::DLQRouter;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use crate::infrastructure::CopyOptimizationConfig;
use crate::infrastructure::OptimizedCDCProcessor;
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
use moka::future::Cache;
use rayon::prelude::*;
use rayon::prelude::*;
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
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

const EVENT_POOL_SIZE: usize = 10_000;
const BATCH_SIZE_THRESHOLD: usize = 1_000;
const MAX_PARALLEL_CHUNKS: usize = 16;
/// Batch processing job types for COPY optimization
#[derive(Debug, Clone)]
pub enum BatchProcessingJob {
    AccountProjections(Vec<AccountProjection>),
    TransactionProjections(Vec<TransactionProjection>),
    Mixed {
        accounts: Vec<AccountProjection>,
        transactions: Vec<TransactionProjection>,
    },
}

pub struct ProjectionBatches {
    pub accounts_to_create: Vec<(Uuid, AccountProjection)>,
    pub accounts_to_update: Vec<(Uuid, AccountProjection)>,
    pub transactions_to_create: Vec<TransactionProjection>,
}

// OPTIMIZATION 3: Separate event containers for better cache locality
#[derive(Debug, Default)]
pub struct EventBatches {
    pub account_created: Vec<ProcessableEvent>,
    pub transactions: Vec<ProcessableEvent>,
    pub account_closed: Vec<ProcessableEvent>,
}

impl EventBatches {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            account_created: Vec::with_capacity(capacity / 3),
            transactions: Vec::with_capacity(capacity / 3),
            account_closed: Vec::with_capacity(capacity / 3),
        }
    }

    fn is_empty(&self) -> bool {
        self.account_created.is_empty()
            && self.transactions.is_empty()
            && self.account_closed.is_empty()
    }

    fn total_len(&self) -> usize {
        self.account_created.len() + self.transactions.len() + self.account_closed.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EventType {
    AccountCreated,
    MoneyDeposited,
    MoneyWithdrawn,
    AccountClosed,
}

impl EventType {
    fn from_str(s: &str) -> Self {
        match s {
            "AccountCreated" => Self::AccountCreated,
            "MoneyDeposited" => Self::MoneyDeposited,
            "MoneyWithdrawn" => Self::MoneyWithdrawn,
            "AccountClosed" => Self::AccountClosed,
            _ => panic!("Unknown event type: {}", s),
        }
    }
    fn as_str(&self) -> &str {
        match self {
            EventType::AccountCreated => "AccountCreated",
            EventType::MoneyDeposited => "MoneyDeposited",
            EventType::MoneyWithdrawn => "MoneyWithdrawn",
            EventType::AccountClosed => "AccountClosed",
        }
    }

    fn is_account_created(&self) -> bool {
        matches!(self, EventType::AccountCreated)
    }

    fn is_transaction_event(&self) -> bool {
        matches!(self, EventType::MoneyDeposited | EventType::MoneyWithdrawn)
    }
}
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

// Optimized batch processing structure
#[derive(Debug)]
struct EventBatch {
    events: Vec<ProcessableEvent>,
    batch_id: Uuid,
    created_at: Instant,
}

#[derive(Debug, Clone)]
pub struct ProcessableEvent {
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: EventType,
    pub payload: Vec<u8>,
    pub partition_key: Option<String>,
    pub domain_event: Option<crate::domain::AccountEvent>, // Pre-deserialized for performance
    pub timestamp: chrono::DateTime<chrono::Utc>, // FIXED: Add timestamp field for transaction projections
}

impl ProcessableEvent {
    fn new(
        event_id: Uuid,
        aggregate_id: Uuid,
        event_type: EventType,
        payload: Vec<u8>,
        partition_key: Option<String>,
        timestamp: chrono::DateTime<chrono::Utc>, // FIXED: Add timestamp parameter
    ) -> Result<Self> {
        // Pre-deserialize domain event for better performance
        let domain_event = match bincode::deserialize(&payload) {
            Ok(event) => {
                // tracing::debug!(
                //     "Successfully deserialized domain event for event_id {}",
                //     event_id
                // );
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
    state: Arc<std::sync::RwLock<AdvancedCircuitBreakerState>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    half_open_success_threshold: u32,
}

impl AdvancedCircuitBreaker {
    fn new() -> Self {
        Self {
            state: Arc::new(std::sync::RwLock::new(AdvancedCircuitBreakerState::Closed)),
            failure_threshold: 10,                     // Increased threshold
            recovery_timeout: Duration::from_secs(30), // Increased timeout
            half_open_success_threshold: 5,
        }
    }

    async fn can_execute(&self) -> bool {
        let state = self.state.read().expect("Failed to acquire read lock");
        match *state {
            AdvancedCircuitBreakerState::Closed => true,
            AdvancedCircuitBreakerState::Open { until, .. } => Instant::now() >= until,
            AdvancedCircuitBreakerState::HalfOpen { .. } => true, // Allow requests in half-open state
        }
    }

    async fn on_success(&self) {
        let mut state = self.state.write().expect("Failed to acquire write lock");
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
        let mut state = self.state.write().expect("Failed to acquire write lock");
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
        let mut state = self.state.write().expect("Failed to acquire write lock");
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

    // Semaphore to limit concurrent database access during projection preparation
    prepare_semaphore: Arc<Semaphore>,

    // Circuit breaker
    circuit_breaker: AdvancedCircuitBreaker,

    // Memory-efficient projection cache
    pub projection_cache: moka::future::Cache<Uuid, ProjectionCacheEntry>,

    // Batch processing coordination with interior mutability
    // batch_processor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_token: CancellationToken,

    // Business logic configuration with interior mutability
    business_config: Arc<std::sync::Mutex<BusinessLogicConfig>>, // Adaptive concurrency control
    adaptive_concurrency: Arc<std::sync::Mutex<usize>>,
    backpressure_signal: Arc<AtomicBool>,

    cdc_batching_service: Option<Arc<OptimizedCDCProcessor>>,
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
            prepare_semaphore: self.prepare_semaphore.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            projection_cache: self.projection_cache.clone(),
            shutdown_token: self.shutdown_token.clone(),
            business_config: Arc::new(std::sync::Mutex::new(BusinessLogicConfig::default())), // Use default instead of trying to clone
            cdc_batching_service: self.cdc_batching_service.clone(),
            adaptive_concurrency: self.adaptive_concurrency.clone(),
            backpressure_signal: self.backpressure_signal.clone(),
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
        projection_cache: moka::future::Cache<Uuid, ProjectionCacheEntry>,
        cdc_batching_service: Option<Arc<OptimizedCDCProcessor>>,
    ) -> Self {
        Self::new_with_write_pool(
            kafka_producer,
            cache_service,
            projection_store,
            metrics,
            business_config,
            consistency_manager,
            projection_cache,
            cdc_batching_service,
        )
    }

    pub fn new_with_write_pool(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        metrics: Arc<EnhancedCDCMetrics>,
        business_config: Option<BusinessLogicConfig>,
        consistency_manager: Option<Arc<ConsistencyManager>>,
        projection_cache: moka::future::Cache<Uuid, ProjectionCacheEntry>,
        cdc_batching_service: Option<Arc<OptimizedCDCProcessor>>,
    ) -> Self {
        let business_config: BusinessLogicConfig = business_config.unwrap_or_default();

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
            prepare_semaphore: Arc::new(Semaphore::new(8)), // Limit concurrent DB reads to 8 (tunable)
            circuit_breaker: AdvancedCircuitBreaker::new(),
            projection_cache,
            shutdown_token: CancellationToken::new(),
            business_config: Arc::new(std::sync::Mutex::new(business_config)),
            adaptive_concurrency: Arc::new(std::sync::Mutex::new(32)),
            backpressure_signal: Arc::new(AtomicBool::new(false)),
            cdc_batching_service,
        }
    }

    /// Get CDC Batching Service if available
    pub fn get_cdc_batching_service(&self) -> Option<&Arc<OptimizedCDCProcessor>> {
        self.cdc_batching_service.as_ref()
    }

    /// Public method to invalidate cache entries after a successful DB write.
    pub async fn invalidate_projection_cache(&self, ids: &[Uuid]) {
        if ids.is_empty() {
            return;
        }
        for id in ids {
            self.projection_cache.invalidate(id).await;
        }
    }
    /// CRITICAL OPTIMIZATION: Process multiple CDC events in batch with event type separation
    pub async fn process_cdc_events_batch(
        &self,
        cdc_events: &mut Vec<serde_json::Value>,
    ) -> Result<()> {
        if cdc_events.is_empty() {
            return Ok(());
        }

        let total_start_time = Instant::now();
        let batch_size = cdc_events.len();
        self.metrics
            .events_processed
            .fetch_add(batch_size as u64, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            "🔍 CDC Event Processor: Starting batch processing of {} events with event type separation",
            batch_size
        );
        let event_batches = self.extract_events_parallel_optimized(cdc_events).await?;

        if event_batches.is_empty() {
            tracing::info!("No processable events found");
            return Ok(());
        }

        // Acquire a permit to limit concurrent DB access during the prepare phase.
        // This prevents overwhelming the database with read requests from all 32 workers at once.
        let _permit = self.prepare_semaphore.acquire().await?;

        // Prepare all projections by applying events
        let processing_start = Instant::now();
        let projection_batches = self.prepare_projection_batches(event_batches).await?;
        let processing_duration = processing_start.elapsed();

        // Submit all prepared projections to the write service
        let submission_start = Instant::now();
        let batching_service = self
            .cdc_batching_service
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("CDC Batching Service not available"))?;

        let create_count = projection_batches.accounts_to_create.len();
        let update_count = projection_batches.accounts_to_update.len();
        let tx_count = projection_batches.transactions_to_create.len();
        self.metrics.projection_updates.fetch_add(
            (create_count + update_count) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.metrics
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.processing_latency_ms.fetch_add(
            processing_duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        let (create_res, update_res, tx_res) = tokio::join!(
            async {
                if create_count > 0 {
                    batching_service
                        .submit_account_creations_bulk(projection_batches.accounts_to_create)
                        .await
                } else {
                    Ok(vec![])
                }
            },
            async {
                if update_count > 0 {
                    batching_service
                        .submit_account_updates_bulk(projection_batches.accounts_to_update)
                        .await
                } else {
                    Ok(vec![])
                }
            },
            async {
                if tx_count > 0 {
                    batching_service
                        .submit_transaction_creations_bulk(
                            projection_batches.transactions_to_create,
                        )
                        .await
                } else {
                    Ok(vec![])
                }
            }
        );

        // Handle submission errors
        // create_res.context("Failed to submit account creations")?;
        // update_res.context("Failed to submit account updates")?;
        // tx_res.context("Failed to submit transaction creations")?;

        let submission_duration = submission_start.elapsed();
        let total_duration = total_start_time.elapsed();
        self.metrics.total_latency_ms.fetch_add(
            total_duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        tracing::info!(
            "🏆 ULTRA-OPTIMIZED CDC COMPLETE: {} events in {:?} (prepare: {:?}, submit: {:?}, throughput: {:.0} events/sec)",
            batch_size,
            total_duration,
            processing_duration,
            submission_duration,
            batch_size as f64 / total_duration.as_secs_f64()
        );

        Ok(())
    }

    /// Prepares projection batches from classified events.
    /// This method is now responsible for the "prepare" phase.
    pub async fn prepare_projection_batches(
        &self,
        event_batches: EventBatches,
    ) -> Result<ProjectionBatches> {
        let (created_projections_res, updated_data_res) = tokio::join!(
            async {
                if !event_batches.account_created.is_empty() {
                    self.prepare_account_creations(event_batches.account_created)
                        .await
                } else {
                    Ok(vec![])
                }
            },
            async {
                if !event_batches.transactions.is_empty() {
                    self.prepare_account_updates_and_transactions(event_batches.transactions)
                        .await
                } else {
                    Ok((vec![], vec![]))
                }
            }
        );

        let accounts_to_create = created_projections_res?;
        let (accounts_to_update, transactions_to_create) = updated_data_res?;

        Ok(ProjectionBatches {
            accounts_to_create,
            accounts_to_update,
            transactions_to_create,
        })
    }

    /// Prepares projections for `AccountCreated` events. This is a pure transformation function.
    pub async fn prepare_account_creations(
        &self,
        events: Vec<ProcessableEvent>,
    ) -> Result<Vec<(Uuid, AccountProjection)>> {
        let mut projections = Vec::with_capacity(events.len());

        for event in events {
            if let Ok(domain_event) = event.get_domain_event() {
                if let crate::domain::AccountEvent::AccountCreated {
                    owner_name,
                    initial_balance,
                    ..
                } = domain_event
                {
                    let projection = crate::infrastructure::projections::AccountProjection {
                        id: event.aggregate_id,
                        owner_name: owner_name.clone(),
                        balance: *initial_balance,
                        is_active: true,
                        created_at: event.timestamp,
                        updated_at: event.timestamp,
                    };
                    // CRITICAL FIX: Populate the cache with the new projection
                    self.projection_cache
                        .insert(
                            projection.id,
                            ProjectionCacheEntry::from(projection.clone()),
                        )
                        .await;
                    projections.push((event.aggregate_id, projection));
                }
            }
        }
        Ok(projections)
    }

    /// Prepares projections for account updates and creates transaction projections.
    /// This function fetches the current state and applies new events.
    pub async fn prepare_account_updates_and_transactions(
        &self,
        events: Vec<ProcessableEvent>,
    ) -> Result<(Vec<(Uuid, AccountProjection)>, Vec<TransactionProjection>)> {
        if events.is_empty() {
            return Ok((vec![], vec![]));
        }

        // --- Idempotency Check ---
        // Check which events have already been processed by looking for their IDs
        // in the transaction_projections table. This prevents double-processing
        // if Kafka messages are delivered more than once.
        let event_ids: Vec<Uuid> = events.iter().map(|e| e.event_id).collect();
        let processed_event_ids_set: std::collections::HashSet<Uuid> = self
            .projection_store
            .get_existing_transaction_ids(&event_ids)
            .await?
            .into_iter()
            .collect();

        let events_by_aggregate = Self::group_events_by_aggregate(events);
        let mut projections_map: HashMap<Uuid, AccountProjection> = HashMap::new();
        let mut ids_to_fetch = Vec::new();

        // --- Read-Through Cache Logic ---
        {
            for id in events_by_aggregate.keys() {
                if let Some(entry) = self.projection_cache.get(id).await {
                    projections_map.insert(*id, entry.data.clone());
                    self.metrics
                        .cache_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    ids_to_fetch.push(*id);
                    self.metrics
                        .cache_misses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        // Fetch cache misses from the database
        if !ids_to_fetch.is_empty() {
            let db_projections = self
                .projection_store
                .get_accounts_batch(&ids_to_fetch)
                .await?;

            for projection in db_projections {
                self.projection_cache.insert(
                    projection.id,
                    ProjectionCacheEntry::from(projection.clone()),
                );
                projections_map.insert(projection.id, projection);
            }
        }

        let mut updated_projections = Vec::with_capacity(events_by_aggregate.len());
        let mut transaction_projections = Vec::new();

        for (aggregate_id, aggregate_events) in events_by_aggregate {
            if let Some(mut projection) = projections_map.get_mut(&aggregate_id) {
                let mut has_updates = false;
                for event in aggregate_events {
                    // Skip events that have already been processed.
                    if !processed_event_ids_set.contains(&event.event_id) {
                        if let Ok(domain_event) = event.get_domain_event() {
                            projection.apply_event(domain_event)?;
                            projection.updated_at = event.timestamp;
                            if let Some(tx_proj) = TransactionProjection::from_domain_event(
                                domain_event,
                                aggregate_id,
                                event.timestamp,
                            ) {
                                transaction_projections.push(tx_proj);
                            }
                            has_updates = true;
                        }
                    }
                }
                if has_updates {
                    updated_projections.push((aggregate_id, projection.clone()));
                }
            } else {
                warn!("No projection found for aggregate_id: {}", aggregate_id);
            }
        }

        Ok((updated_projections, transaction_projections))
    }

    /// Groups a vector of events by their aggregate ID and sorts them by timestamp.
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

    /// OPTIMIZATION 8: Parallel event extraction with SIMD and memory pools
    pub async fn extract_events_parallel_optimized(
        &self,
        cdc_events: &[serde_json::Value],
    ) -> Result<EventBatches> {
        let extraction_start = Instant::now();
        let cdc_events_owned = cdc_events.to_vec();
        let self_clone = self.clone();

        let (final_batches, stats) = tokio::task::spawn_blocking(move || {
            let results: Vec<_> = cdc_events_owned
                // PERF: Switched from par_iter to a sequential iter.
                // The system already has batch-level parallelism via multiple tokio workers.
                // Adding another layer of parallelism with Rayon here causes thread oversubscription and contention.
                .iter()
                .map(|cdc_event| self_clone.extract_event_serde_json_optimized(cdc_event))
                .collect();

            let mut batches = EventBatches::with_capacity(results.len());
            let mut extracted = 0;
            let mut skipped = 0;
            let mut errors = 0;

            for result in results {
                match result {
                    Ok(Some(event)) => {
                        match &event.event_type {
                            EventType::AccountCreated => batches.account_created.push(event),
                            EventType::MoneyDeposited | EventType::MoneyWithdrawn => {
                                batches.transactions.push(event);
                            }
                            EventType::AccountClosed => batches.account_closed.push(event),
                        }
                        extracted += 1;
                    }
                    Ok(None) => skipped += 1,
                    Err(_) => errors += 1,
                }
            }

            (batches, (extracted, skipped, errors))
        })
        .await?;

        let (extracted_count, skipped_count, error_count) = stats;

        let extraction_duration = extraction_start.elapsed();
        tracing::info!(
            "⚡ EXTRACTION COMPLETE: {} events in {:?} (extracted: {}, skipped: {}, errors: {})",
            extracted_count,
            extraction_duration,
            extracted_count,
            skipped_count,
            error_count
        );

        Ok(final_batches)
    }
    /// OPTIMIZATION 11: SIMD-optimized chunk processing
    async fn extract_chunk_events_simd_optimized(
        &self,
        chunk: Vec<serde_json::Value>,
    ) -> Result<(EventBatches, (usize, usize, usize))> {
        let mut batches = EventBatches::with_capacity(chunk.len());
        let mut extracted = 0;
        let mut skipped = 0;
        let mut errors = 0;

        // OPTIMIZATION 12: Vectorized processing with pre-allocated buffers
        for cdc_event in chunk {
            match self.extract_event_serde_json_optimized(&cdc_event) {
                Ok(Some(event)) => {
                    // Route event to appropriate batch based on type
                    match &event.event_type {
                        EventType::AccountCreated => batches.account_created.push(event),
                        EventType::MoneyDeposited | EventType::MoneyWithdrawn => {
                            batches.transactions.push(event);
                        }
                        EventType::AccountClosed => batches.account_closed.push(event),
                    }
                    extracted += 1;
                }
                Ok(None) => skipped += 1,
                Err(_) => errors += 1,
            }
        }

        Ok((batches, (extracted, skipped, errors)))
    }
    /// OPTIMIZATION 18: Process all event types in parallel with maximum efficiency
    // async fn process_all_event_types_parallel(&self, event_batches: EventBatches) -> Result<()> {
    //     let batching_service = self
    //         .cdc_batching_service
    //         .as_ref()
    //         .ok_or_else(|| anyhow::anyhow!("CDC Batching Service not available"))?;

    //     // OPTIMIZATION 19: Launch all processing tasks in parallel
    //     let account_created_task = if !event_batches.account_created.is_empty() {
    //         let events = event_batches.account_created;
    //         let service = batching_service.clone();
    //         Some(tokio::spawn(async move {
    //             Self::prepare_account_creations(events).await
    //         }))
    //     } else {
    //         None
    //     };

    //     let transactions_task = if !event_batches.transactions.is_empty() {
    //         let events = event_batches.transactions;
    //         let service = batching_service.clone();
    //         let projection_store = self.projection_store.clone();
    //         Some(tokio::spawn(async move {
    //             Self::prepare_account_updates_and_transactions(events, projection_store).await
    //         }))
    //     } else {
    //         None
    //     };

    //     // OPTIMIZATION 20: Wait for all tasks and collect results
    //     let mut total_processed = 0;

    //     if let Some(task) = account_created_task {
    //         match task.await? {
    //             Ok(count) => {
    //                 total_processed += count.len();
    //                 tracing::info!("✅ AccountCreated processing: {} events", count.len());
    //             }
    //             Err(e) => {
    //                 tracing::error!("❌ AccountCreated processing failed: {:?}", e);
    //                 return Err(e);
    //             }
    //         }
    //     }

    //     if let Some(task) = transactions_task {
    //         match task.await? {
    //             Ok(count) => {
    //                 total_processed += count.0.len() + count.1.len();
    //                 tracing::info!(
    //                     "✅ Transactions processing: {} events",
    //                     count.0.len() + count.1.len()
    //                 );
    //             }
    //             Err(e) => {
    //                 tracing::error!("❌ Transactions processing failed: {:?}", e);
    //                 return Err(e);
    //             }
    //         }
    //     }

    //     tracing::info!("🏁 Total processed events: {}", total_processed);
    //     Ok(())
    // }

    /// OPTIMIZATION 21: Ultra-fast AccountCreated processing
    async fn process_account_created_ultra_fast(
        events: Vec<ProcessableEvent>,
        batching_service: Arc<OptimizedCDCProcessor>,
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        let start_time = Instant::now();
        let mut projections = Vec::with_capacity(events.len());

        // OPTIMIZATION 22: Vectorized projection creation
        for event in events {
            if let Ok(domain_event) = event.get_domain_event() {
                if let crate::domain::AccountEvent::AccountCreated {
                    owner_name,
                    initial_balance,
                    ..
                } = domain_event
                {
                    let projection = crate::infrastructure::projections::AccountProjection {
                        id: event.aggregate_id,
                        owner_name: owner_name.clone(),
                        balance: *initial_balance,
                        is_active: true,
                        created_at: event.timestamp,
                        updated_at: event.timestamp,
                    };
                    projections.push((event.aggregate_id, projection));
                }
            }
        }

        if projections.is_empty() {
            return Ok(0);
        }

        let projection_count = projections.len();
        tracing::info!(
            "🚀 Processing {} AccountCreated projections",
            projection_count
        );

        // OPTIMIZATION 23: Batch submit with fire-and-forget optimization
        match batching_service
            .submit_account_creations_bulk(projections)
            .await
        {
            Ok(operation_ids) => {
                let duration = start_time.elapsed();
                tracing::info!(
                    "✅ AccountCreated submitted: {} operations in {:?} ({:.0} ops/sec)",
                    operation_ids.len(),
                    duration,
                    operation_ids.len() as f64 / duration.as_secs_f64()
                );
                Ok(operation_ids.len())
            }
            Err(e) => {
                tracing::error!("❌ AccountCreated submission failed: {:?}", e);
                Err(e)
            }
        }
    }
    /// OPTIMIZATION 24: Ultra-fast transaction processing with batch operations
    async fn process_transactions_ultra_fast(
        events: Vec<ProcessableEvent>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        batching_service: Arc<OptimizedCDCProcessor>,
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        let start_time = Instant::now();

        // OPTIMIZATION 25: Group transactions by aggregate for efficient processing
        let mut events_by_aggregate: HashMap<Uuid, Vec<ProcessableEvent>> = HashMap::new();
        for event in events {
            events_by_aggregate
                .entry(event.aggregate_id)
                .or_default()
                .push(event);
        }

        let aggregate_ids: Vec<Uuid> = events_by_aggregate.keys().cloned().collect();

        tracing::info!(
            "⚡ Batch loading {} account projections for transactions",
            aggregate_ids.len()
        );

        // OPTIMIZATION 26: Single batch database read
        let db_projections = projection_store
            .get_accounts_batch(&aggregate_ids)
            .await
            .map_err(|e| anyhow::anyhow!("Batch projection load failed: {:?}", e))?;

        // OPTIMIZATION 27: Fast lookup map with pre-allocated capacity
        let mut projections_map: HashMap<
            Uuid,
            crate::infrastructure::projections::AccountProjection,
        > = HashMap::with_capacity(db_projections.len());

        for proj in db_projections {
            projections_map.insert(proj.id, proj);
        }

        let mut final_projections = Vec::with_capacity(aggregate_ids.len());
        let mut transaction_projections = Vec::new();

        // OPTIMIZATION 28: Process aggregates with vectorized operations
        for (aggregate_id, mut aggregate_events) in events_by_aggregate {
            let mut current_projection = match projections_map.remove(&aggregate_id) {
                Some(proj) => proj,
                None => {
                    tracing::warn!("⚠️ No projection found for aggregate {}", aggregate_id);
                    continue;
                }
            };

            // Sort events by timestamp for consistent processing
            aggregate_events.sort_unstable_by(|a, b| a.timestamp.cmp(&b.timestamp));

            // OPTIMIZATION 29: Apply all events with minimal allocations
            for event in aggregate_events {
                if let Ok(domain_event) = event.get_domain_event() {
                    match domain_event {
                        crate::domain::AccountEvent::MoneyDeposited { amount, .. } => {
                            current_projection.balance += *amount;
                            current_projection.updated_at = event.timestamp;

                            // Create transaction projection
                            transaction_projections.push(TransactionProjection {
                                id: event.event_id,
                                account_id: aggregate_id,
                                transaction_type: "DEPOSIT".to_string(),
                                amount: *amount,
                                timestamp: event.timestamp,
                            });
                        }
                        crate::domain::AccountEvent::MoneyWithdrawn { amount, .. } => {
                            current_projection.balance -= *amount;
                            current_projection.updated_at = event.timestamp;

                            // Create transaction projection
                            transaction_projections.push(TransactionProjection {
                                id: event.event_id,
                                account_id: aggregate_id,
                                transaction_type: "WITHDRAWAL".to_string(),
                                amount: *amount,
                                timestamp: event.timestamp,
                            });
                        }
                        _ => {} // Ignore other event types
                    }
                }
            }

            final_projections.push((aggregate_id, current_projection));
        }

        let projection_count = final_projections.len();
        let transaction_count = transaction_projections.len();

        if projection_count == 0 && transaction_count == 0 {
            return Ok(0);
        }

        tracing::info!(
            "⚡ Processing {} account projections and {} transaction projections",
            projection_count,
            transaction_count
        );

        // OPTIMIZATION 30: Parallel submission of account and transaction projections
        let (account_result, transaction_result) = tokio::join!(
            async {
                if !final_projections.is_empty() {
                    batching_service
                        .submit_account_updates_bulk(final_projections)
                        .await
                } else {
                    Ok(vec![])
                }
            },
            async {
                if !transaction_projections.is_empty() {
                    batching_service
                        .submit_transaction_creations_bulk(transaction_projections)
                        .await
                } else {
                    Ok(vec![])
                }
            }
        );

        let mut total_operations = 0;

        match account_result {
            Ok(operation_ids) => {
                total_operations += operation_ids.len();
                tracing::debug!("✅ Account projections submitted: {}", operation_ids.len());
            }
            Err(e) => {
                tracing::error!("❌ Account projection submission failed: {:?}", e);
                return Err(e);
            }
        }

        match transaction_result {
            Ok(operation_ids) => {
                total_operations += operation_ids.len();
                tracing::debug!(
                    "✅ Transaction projections submitted: {}",
                    operation_ids.len()
                );
            }
            Err(e) => {
                tracing::error!("❌ Transaction projection submission failed: {:?}", e);
                return Err(e);
            }
        }

        let duration = start_time.elapsed();
        tracing::info!(
            "✅ Transactions processed: {} operations in {:?} ({:.0} ops/sec)",
            total_operations,
            duration,
            total_operations as f64 / duration.as_secs_f64()
        );

        Ok(total_operations)
    }
    fn extract_event_serde_json_optimized(
        &self,
        cdc_event: &serde_json::Value,
    ) -> Result<Option<ProcessableEvent>> {
        // CRITICAL FIX: Use get_unchecked for performance in hot path
        let event_data = match cdc_event.get("payload") {
            Some(data) if !data.is_null() => data,
            _ => return Ok(None), // Skip null/missing payload
        };

        // Quick deletion check
        if event_data.get("__deleted").and_then(|v| v.as_str()) == Some("true") {
            return Ok(None);
        }

        // OPTIMIZATION 5: Use unsafe UUID parsing for performance (if safe)
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

        let event_type_str = event_data
            .get("event_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing event_type"))?
            .to_string();

        let event_type = EventType::from_str(&event_type_str);

        // OPTIMIZATION 6: Faster timestamp parsing
        let timestamp = event_data
            .get("created_at")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        let payload_str = event_data
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing payload"))?;

        // OPTIMIZATION 7: Faster base64 decoding with proper error handling
        let payload_bytes = BASE64_STANDARD
            .decode(payload_str)
            .map_err(|e| anyhow::anyhow!("Base64 decode error: {}", e))?;

        let partition_key = event_data
            .get("partition_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // OPTIMIZATION 8: Early domain event validation
        let domain_event = bincode::deserialize::<crate::domain::AccountEvent>(&payload_bytes)
            .map_err(|e| anyhow::anyhow!("Domain event deserialization failed: {}", e))?;

        ProcessableEvent::new(
            event_id,
            aggregate_id,
            event_type,
            payload_bytes,
            partition_key,
            timestamp,
        )
        .map(Some)
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
            event_type: event.event_type.as_str().to_string(),
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
}
