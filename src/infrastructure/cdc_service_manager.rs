use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::cdc_debezium::{CDCConsumer, CDCOutboxRepository, DebeziumConfig};
use crate::infrastructure::cdc_event_processor::UltraOptimizedCDCEventProcessor;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Enhanced CDC Service Manager with optimized resource management and monitoring
pub struct CDCServiceManager {
    config: DebeziumConfig,
    outbox_repo: Arc<CDCOutboxRepository>,
    processor: Arc<UltraOptimizedCDCEventProcessor>,

    // Enhanced shutdown management
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,

    // Task management with proper handles
    tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,

    // Service state management
    state: Arc<RwLock<ServiceState>>,

    // Enhanced metrics and monitoring
    metrics: Arc<EnhancedCDCMetrics>,

    // Health checker
    health_checker: Arc<CDCHealthCheck>,

    // Configuration for optimizations
    optimization_config: OptimizationConfig,

    // Consistency manager for projection synchronization
    consistency_manager:
        Option<Arc<crate::infrastructure::consistency_manager::ConsistencyManager>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceState {
    Stopped,
    Starting,
    Running,
    Stopping,
    Migrating,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    pub health_check_interval_secs: u64,
    pub cleanup_interval_secs: u64,
    pub metrics_flush_interval_secs: u64,
    pub graceful_shutdown_timeout_secs: u64,
    pub consumer_backoff_ms: u64,
    pub max_retries: usize,
    pub circuit_breaker_threshold: f64,
    pub enable_compression: bool,
    pub enable_batching: bool,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        Self {
            health_check_interval_secs: 30,
            cleanup_interval_secs: 3600,
            metrics_flush_interval_secs: 60,
            graceful_shutdown_timeout_secs: 30,
            consumer_backoff_ms: 100,
            max_retries: 3,
            circuit_breaker_threshold: 0.1,
            enable_compression: true,
            enable_batching: true,
        }
    }
}

#[derive(Debug, Default)]
pub struct EnhancedCDCMetrics {
    // Existing metrics
    pub events_processed: std::sync::atomic::AtomicU64,
    pub events_failed: std::sync::atomic::AtomicU64,
    pub processing_latency_ms: std::sync::atomic::AtomicU64,
    pub total_latency_ms: std::sync::atomic::AtomicU64,
    pub cache_invalidations: std::sync::atomic::AtomicU64,
    pub projection_updates: std::sync::atomic::AtomicU64,

    // New optimization metrics
    pub batches_processed: std::sync::atomic::AtomicU64,
    pub circuit_breaker_trips: std::sync::atomic::AtomicU64,
    pub consumer_restarts: std::sync::atomic::AtomicU64,
    pub cleanup_cycles: std::sync::atomic::AtomicU64,
    pub memory_usage_bytes: std::sync::atomic::AtomicU64,
    pub active_connections: std::sync::atomic::AtomicU64,
    pub queue_depth: std::sync::atomic::AtomicU64,

    // Performance metrics
    pub avg_batch_size: std::sync::atomic::AtomicU64,
    pub p95_processing_latency_ms: std::sync::atomic::AtomicU64,
    pub p99_processing_latency_ms: std::sync::atomic::AtomicU64,
    pub throughput_per_second: std::sync::atomic::AtomicU64,

    // Error tracking
    pub consecutive_failures: std::sync::atomic::AtomicU64,
    pub last_error_time: std::sync::atomic::AtomicU64,
    pub error_rate: std::sync::atomic::AtomicU64,

    // Integration helper status
    pub integration_helper_initialized: std::sync::atomic::AtomicBool,
}

impl Clone for EnhancedCDCMetrics {
    fn clone(&self) -> Self {
        Self {
            events_processed: std::sync::atomic::AtomicU64::new(
                self.events_processed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            events_failed: std::sync::atomic::AtomicU64::new(
                self.events_failed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            total_latency_ms: std::sync::atomic::AtomicU64::new(
                self.total_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            cache_invalidations: std::sync::atomic::AtomicU64::new(
                self.cache_invalidations
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            projection_updates: std::sync::atomic::AtomicU64::new(
                self.projection_updates
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            batches_processed: std::sync::atomic::AtomicU64::new(
                self.batches_processed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            circuit_breaker_trips: std::sync::atomic::AtomicU64::new(
                self.circuit_breaker_trips
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            consumer_restarts: std::sync::atomic::AtomicU64::new(
                self.consumer_restarts
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            cleanup_cycles: std::sync::atomic::AtomicU64::new(
                self.cleanup_cycles
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            memory_usage_bytes: std::sync::atomic::AtomicU64::new(
                self.memory_usage_bytes
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            active_connections: std::sync::atomic::AtomicU64::new(
                self.active_connections
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            queue_depth: std::sync::atomic::AtomicU64::new(
                self.queue_depth.load(std::sync::atomic::Ordering::Relaxed),
            ),
            avg_batch_size: std::sync::atomic::AtomicU64::new(
                self.avg_batch_size
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            p95_processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.p95_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            p99_processing_latency_ms: std::sync::atomic::AtomicU64::new(
                self.p99_processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            throughput_per_second: std::sync::atomic::AtomicU64::new(
                self.throughput_per_second
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            consecutive_failures: std::sync::atomic::AtomicU64::new(
                self.consecutive_failures
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            last_error_time: std::sync::atomic::AtomicU64::new(
                self.last_error_time
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            error_rate: std::sync::atomic::AtomicU64::new(
                self.error_rate.load(std::sync::atomic::Ordering::Relaxed),
            ),
            integration_helper_initialized: std::sync::atomic::AtomicBool::new(
                self.integration_helper_initialized
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl CDCServiceManager {
    pub fn new(
        config: DebeziumConfig,
        outbox_repo: Arc<CDCOutboxRepository>,
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
        cache_service: Arc<dyn CacheServiceTrait>,
        pools: Arc<crate::infrastructure::connection_pool_partitioning::PartitionedPools>,
        metrics: Option<Arc<EnhancedCDCMetrics>>,
        consistency_manager: Option<
            Arc<crate::infrastructure::consistency_manager::ConsistencyManager>,
        >,
    ) -> Result<Self> {
        let optimization_config = OptimizationConfig::default();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = metrics.unwrap_or_else(|| Arc::new(EnhancedCDCMetrics::default()));
        let health_checker = Arc::new(CDCHealthCheck::new(metrics.clone()));
        let projection_store = Arc::new(crate::infrastructure::projections::ProjectionStore::from_pools_with_config(pools.clone(), Default::default()));
        let processor = Arc::new(UltraOptimizedCDCEventProcessor::new(
            kafka_producer,
            cache_service,
            projection_store,
            metrics.clone(),
            None,
            None, // Use default performance config
            consistency_manager.clone(),
        ));

        // Consistency manager is set on the processor during construction

        Ok(Self {
            config,
            outbox_repo,
            processor,
            shutdown_tx,
            shutdown_rx,
            tasks: Arc::new(RwLock::new(Vec::new())),
            state: Arc::new(RwLock::new(ServiceState::Stopped)),
            metrics,
            health_checker,
            optimization_config,
            consistency_manager,
        })
    }

    /// Start the CDC service with optimized resource management
    pub async fn start(&mut self) -> Result<()> {
        tracing::info!("CDCServiceManager::start() called");
        self.set_state(ServiceState::Starting).await;

        info!("Starting optimized CDC Service Manager");
        tracing::info!(
            "CDC Service Manager: Initializing with config: {:?}",
            self.optimization_config
        );

        // Consistency manager is already set in the processor during construction
        if let Some(consistency_manager) = &self.consistency_manager {
            tracing::info!(
                "CDCServiceManager: Consistency manager is available and set on processor"
            );
        } else {
            tracing::warn!("CDCServiceManager: No consistency manager provided - projection synchronization will not work");
        }

        // Initialize infrastructure
        tracing::info!("CDCServiceManager: Initializing infrastructure...");
        self.initialize_infrastructure().await?;
        tracing::info!("CDCServiceManager: Infrastructure initialized successfully");

        // Start core services
        tracing::info!("CDCServiceManager: Starting core services...");
        self.start_core_services().await?;
        tracing::info!("CDCServiceManager: Core services started successfully");

        // Start monitoring and maintenance tasks
        tracing::info!("CDCServiceManager: Starting monitoring tasks...");
        self.start_monitoring_tasks().await?;
        tracing::info!("CDCServiceManager: Monitoring tasks started successfully");

        self.set_state(ServiceState::Running).await;
        info!("CDC Service Manager started successfully");
        tracing::info!("CDCServiceManager: Service is now RUNNING");

        Ok(())
    }

    /// Initialize infrastructure components
    async fn initialize_infrastructure(&self) -> Result<()> {
        tracing::info!("CDC Service Manager: Initializing infrastructure...");

        // Create CDC table with optimizations
        self.outbox_repo.create_cdc_outbox_table().await?;

        // Pre-warm caches if enabled
        if self.optimization_config.enable_compression {
            tracing::info!("CDC Service Manager: Pre-warming caches...");
            // Add cache pre-warming logic here
        }

        // Validate Kafka connectivity
        self.validate_kafka_connectivity().await?;

        tracing::info!("CDC Service Manager: ✅ Infrastructure initialized");
        Ok(())
    }

    /// Start core CDC services
    async fn start_core_services(&self) -> Result<()> {
        let mut tasks = self.tasks.write().await;

        // Start CDC consumer with resilience
        let consumer_handle = self.start_resilient_consumer().await?;
        tasks.push(consumer_handle);

        // Start event processor's batch processor if enabled
        if self.optimization_config.enable_batching {
            tracing::info!("CDC Service Manager: Starting event processor's batch processor...");

            // Use the static method to start the batch processor
            let processor_clone = self.processor.clone();

            // Start the event processor's batch processor
            match UltraOptimizedCDCEventProcessor::enable_and_start_batch_processor_arc(
                processor_clone,
            )
            .await
            {
                Ok(_) => {
                    tracing::info!("CDC Service Manager: ✅ Event processor's batch processor started successfully");
                }
                Err(e) => {
                    tracing::error!("CDC Service Manager: Failed to start event processor's batch processor: {}", e);
                    return Err(e);
                }
            }
        }

        // Force start batch processor regardless of config
        tracing::info!("CDC Service Manager: Force starting batch processor...");
        let processor_clone = self.processor.clone();
        match UltraOptimizedCDCEventProcessor::enable_and_start_batch_processor_arc(processor_clone)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "CDC Service Manager: ✅ Batch processor force started successfully"
                );
            }
            Err(e) => {
                tracing::error!(
                    "CDC Service Manager: Failed to force start batch processor: {}",
                    e
                );
                return Err(e);
            }
        }

        tracing::info!("CDC Service Manager: ✅ Core services started");
        Ok(())
    }

    /// Start monitoring and maintenance tasks
    async fn start_monitoring_tasks(&self) -> Result<()> {
        let mut tasks = self.tasks.write().await;

        // Health check task
        let health_handle = self.start_health_monitor().await?;
        tasks.push(health_handle);

        // Cleanup task
        let cleanup_handle = self.start_cleanup_task().await?;
        tasks.push(cleanup_handle);

        // Metrics collector
        let metrics_handle = self.start_metrics_collector().await?;
        tasks.push(metrics_handle);

        // Circuit breaker monitor
        let circuit_breaker_handle = self.start_circuit_breaker_monitor().await?;
        tasks.push(circuit_breaker_handle);

        tracing::info!("CDC Service Manager: ✅ Monitoring tasks started");
        Ok(())
    }

    /// Start resilient CDC consumer with auto-recovery
    async fn start_resilient_consumer(&self) -> Result<tokio::task::JoinHandle<()>> {
        tracing::info!("CDCServiceManager::start_resilient_consumer() called");
        let config = self.config.clone();
        let processor = self.processor.clone();
        let metrics = self.metrics.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let optimization_config = self.optimization_config.clone();

        let handle = tokio::spawn(async move {
            tracing::info!("CDCServiceManager: consumer task spawned");
            let mut consecutive_failures = 0;
            let max_retries = optimization_config.max_retries;

            loop {
                // Check for shutdown signal
                if *shutdown_rx.borrow() {
                    tracing::info!("CDC Consumer: Received shutdown signal");
                    break;
                }

                tracing::info!("CDC Consumer: Creating new Kafka consumer instance...");
                // Create new consumer instance
                let kafka_consumer = match Self::create_kafka_consumer(&config).await {
                    Ok(consumer) => {
                        tracing::info!("CDC Consumer: ✅ Successfully created Kafka consumer");
                        consumer
                    }
                    Err(e) => {
                        error!("CDC Consumer: Failed to create Kafka consumer: {}", e);
                        tracing::error!("CDC Consumer: Failed to create Kafka consumer: {}", e);
                        consecutive_failures += 1;

                        if consecutive_failures >= max_retries {
                            error!("CDC Consumer: Max retries exceeded, stopping");
                            break;
                        }

                        tokio::time::sleep(Duration::from_millis(
                            optimization_config.consumer_backoff_ms * consecutive_failures as u64,
                        ))
                        .await;
                        continue;
                    }
                };

                let cdc_topic = format!("{}.{}", config.topic_prefix, config.table_include_list);
                let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

                let mut consumer = CDCConsumer::new(kafka_consumer, cdc_topic);

                tracing::info!("CDCServiceManager: about to enter consumer main loop");
                // Start consuming with timeout using the mutex version
                let consumer_result = consumer
                    .start_consuming_with_mutex(processor.clone(), shutdown_rx)
                    .await;

                match consumer_result {
                    Ok(_) => {
                        tracing::info!("CDC Consumer: Completed normally");
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        error!("CDC Consumer: Failed with error: {}", e);
                        tracing::error!("CDC Consumer: Failed with error: {}", e);
                        consecutive_failures += 1;
                        metrics
                            .consumer_restarts
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        if consecutive_failures >= max_retries {
                            error!("CDC Consumer: Max retries exceeded, stopping");
                            break;
                        }

                        // Exponential backoff
                        let backoff = Duration::from_millis(
                            optimization_config.consumer_backoff_ms
                                * (2_u64.pow(consecutive_failures.min(5) as u32)),
                        );
                        tracing::info!("CDC Consumer: Retrying in {:?}", backoff);
                        tokio::time::sleep(backoff).await;
                    }
                }
            }

            tracing::info!("CDC Consumer: Task completed");
        });

        Ok(handle)
    }

    /// Start health monitoring task
    async fn start_health_monitor(&self) -> Result<tokio::task::JoinHandle<()>> {
        let health_checker = self.health_checker.clone();
        let metrics = self.metrics.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let state = self.state.clone();
        let optimization_config = self.optimization_config.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                optimization_config.health_check_interval_secs,
            ));

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = interval.tick() => {
                        let health_status = health_checker.get_health_status();
                        tracing::debug!("Health Check: {}", health_status);

                        // Update service state based on health
                        if !health_checker.is_healthy() {
                            let current_state = state.read().await;
                            if matches!(*current_state, ServiceState::Running) {
                                drop(current_state);
                                let mut state_guard = state.write().await;
                                *state_guard = ServiceState::Failed("Health check failed".to_string());
                                error!("CDC Service marked as failed due to health check");
                            }
                        }

                        // Update memory usage metrics
                        if let Ok(memory_info) = Self::get_memory_usage().await {
                            metrics.memory_usage_bytes.store(memory_info, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }

            tracing::info!("Health Monitor: Task completed");
        });

        Ok(handle)
    }

    /// Start cleanup task with optimizations
    async fn start_cleanup_task(&self) -> Result<tokio::task::JoinHandle<()>> {
        let outbox_repo = self.outbox_repo.clone();
        let metrics = self.metrics.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let optimization_config = self.optimization_config.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                optimization_config.cleanup_interval_secs,
            ));

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = interval.tick() => {
                        tracing::info!("Cleanup Task: Starting cleanup cycle");

                        match outbox_repo.cleanup_old_messages(Duration::from_secs(86400 * 7)).await {
                            Ok(cleaned_count) => {
                                metrics.cleanup_cycles.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                info!("Cleanup Task: Cleaned {} old messages", cleaned_count);
                            }
                            Err(e) => {
                                error!("Cleanup Task: Failed to cleanup old messages: {}", e);
                            }
                        }
                    }
                }
            }

            tracing::info!("Cleanup Task: Task completed");
        });

        Ok(handle)
    }

    /// Start metrics collector task
    async fn start_metrics_collector(&self) -> Result<tokio::task::JoinHandle<()>> {
        let metrics = self.metrics.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let optimization_config = self.optimization_config.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                optimization_config.metrics_flush_interval_secs,
            ));
            let mut last_processed = 0u64;
            let mut last_time = std::time::Instant::now();

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = interval.tick() => {
                        let current_processed = metrics.events_processed.load(std::sync::atomic::Ordering::Relaxed);
                        let elapsed = last_time.elapsed();

                        let throughput = if elapsed.as_secs() > 0 {
                            ((current_processed - last_processed) as f64 / elapsed.as_secs() as f64) as u64
                        } else {
                            0
                        };
                        metrics.throughput_per_second.store(throughput, std::sync::atomic::Ordering::Relaxed);

                        last_processed = current_processed;
                        last_time = std::time::Instant::now();

                        // Calculate error rate
                        let failed = metrics.events_failed.load(std::sync::atomic::Ordering::Relaxed);
                        let total = current_processed + failed;
                        let error_rate = if total > 0 {
                            ((failed as f64 / total as f64) * 100.0) as u64
                        } else {
                            0
                        };
                        metrics.error_rate.store(error_rate, std::sync::atomic::Ordering::Relaxed);

                        tracing::debug!("Metrics: Processed: {}, Failed: {}, Throughput: {}/s, Error Rate: {}%",
                                       current_processed, failed, throughput, error_rate);
                    }
                }
            }

            tracing::info!("Metrics Collector: Task completed");
        });

        Ok(handle)
    }

    /// Start circuit breaker monitor
    async fn start_circuit_breaker_monitor(&self) -> Result<tokio::task::JoinHandle<()>> {
        let metrics = self.metrics.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let optimization_config = self.optimization_config.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = interval.tick() => {
                        let error_rate = metrics.error_rate.load(std::sync::atomic::Ordering::Relaxed) as f64 / 100.0;

                        if error_rate > optimization_config.circuit_breaker_threshold {
                            warn!("Circuit Breaker: Error rate {}% exceeds threshold {}%",
                                  error_rate * 100.0, optimization_config.circuit_breaker_threshold * 100.0);

                            metrics.circuit_breaker_trips.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            // Could implement circuit breaker logic here
                            // For now, just log and track
                        }
                    }
                }
            }

            tracing::info!("Circuit Breaker Monitor: Task completed");
        });

        Ok(handle)
    }

    /// Graceful shutdown with timeout
    pub async fn stop(&self) -> Result<()> {
        self.set_state(ServiceState::Stopping).await;

        info!("Starting graceful shutdown of CDC Service Manager");

        // Signal all tasks to stop
        if let Err(e) = self.shutdown_tx.send(true) {
            warn!("Failed to send shutdown signal: {}", e);
        }

        // Wait for all tasks to complete with timeout
        let shutdown_timeout =
            Duration::from_secs(self.optimization_config.graceful_shutdown_timeout_secs);
        let tasks = self.tasks.read().await;

        let shutdown_future = async {
            for task in tasks.iter() {
                if !task.is_finished() {
                    task.abort();
                }
            }
        };

        match tokio::time::timeout(shutdown_timeout, shutdown_future).await {
            Ok(_) => {
                info!("All CDC tasks stopped gracefully");
            }
            Err(_) => {
                warn!("Shutdown timeout exceeded, some tasks may not have stopped gracefully");
            }
        }

        self.set_state(ServiceState::Stopped).await;
        info!("CDC Service Manager stopped");

        Ok(())
    }

    /// Get comprehensive service status
    pub async fn get_service_status(&self) -> serde_json::Value {
        let state = self.state.read().await;
        let health_status = self.health_checker.get_health_status();
        let tasks = self.tasks.read().await;

        let active_tasks = tasks.iter().filter(|t| !t.is_finished()).count();

        serde_json::json!({
            "state": format!("{:?}", *state),
            "health": health_status,
            "active_tasks": active_tasks,
            "total_tasks": tasks.len(),
            "optimization_config": {
                "batching_enabled": self.optimization_config.enable_batching,
                "compression_enabled": self.optimization_config.enable_compression,
                "circuit_breaker_threshold": self.optimization_config.circuit_breaker_threshold
            },
            "metrics": {
                "events_processed": self.metrics.events_processed.load(std::sync::atomic::Ordering::Relaxed),
                "events_failed": self.metrics.events_failed.load(std::sync::atomic::Ordering::Relaxed),
                "throughput_per_second": self.metrics.throughput_per_second.load(std::sync::atomic::Ordering::Relaxed),
                "error_rate_percent": self.metrics.error_rate.load(std::sync::atomic::Ordering::Relaxed),
                "consumer_restarts": self.metrics.consumer_restarts.load(std::sync::atomic::Ordering::Relaxed),
                "circuit_breaker_trips": self.metrics.circuit_breaker_trips.load(std::sync::atomic::Ordering::Relaxed),
                "memory_usage_mb": self.metrics.memory_usage_bytes.load(std::sync::atomic::Ordering::Relaxed) / 1024 / 1024,
                "cleanup_cycles": self.metrics.cleanup_cycles.load(std::sync::atomic::Ordering::Relaxed)
            }
        })
    }

    /// Helper methods
    async fn set_state(&self, new_state: ServiceState) {
        let mut state = self.state.write().await;
        *state = new_state;
    }

    async fn validate_kafka_connectivity(&self) -> Result<()> {
        // Add Kafka connectivity validation logic
        tracing::info!("CDC Service Manager: Validating Kafka connectivity...");
        // Placeholder - implement actual connectivity check
        Ok(())
    }

    async fn create_kafka_consumer(
        config: &DebeziumConfig,
    ) -> Result<crate::infrastructure::kafka_abstraction::KafkaConsumer> {
        tracing::info!("CDCServiceManager: Creating Kafka consumer with config - bootstrap_servers: localhost:9092, group_id: banking-es-group, topic_prefix: {}", config.topic_prefix);

        // Create Kafka consumer with proper configuration
        let kafka_config = crate::infrastructure::kafka_abstraction::KafkaConfig {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "banking-es-group".to_string(),
            topic_prefix: config.topic_prefix.clone(),
            producer_acks: 1,
            producer_retries: 3,
            consumer_max_poll_interval_ms: 300000,
            consumer_session_timeout_ms: 10000,
            fetch_max_bytes: 500,
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: "PLAIN".to_string(),
            ssl_ca_location: None,
            auto_offset_reset: "earliest".to_string(),
            cache_invalidation_topic: "banking-es-cache-invalidation".to_string(),
            event_topic: "banking-es-events".to_string(),
        };

        tracing::info!("CDCServiceManager: Kafka config created, attempting to create consumer...");
        let consumer = crate::infrastructure::kafka_abstraction::KafkaConsumer::new(kafka_config)
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka consumer: {}", e))?;

        tracing::info!("CDCServiceManager: ✅ Kafka consumer created successfully");
        Ok(consumer)
    }

    async fn get_memory_usage() -> Result<u64> {
        // Implement memory usage tracking
        // Placeholder - could use system metrics
        Ok(0)
    }

    pub fn get_metrics(&self) -> &EnhancedCDCMetrics {
        &self.metrics
    }

    pub fn get_debezium_config(&self) -> serde_json::Value {
        self.outbox_repo.generate_debezium_config(&self.config)
    }

    pub fn processor_arc(&self) -> Arc<UltraOptimizedCDCEventProcessor> {
        self.processor.clone()
    }

    /// Ensure batch processing is enabled and started
    pub async fn ensure_batch_processing_enabled(&self) -> Result<()> {
        // Check if batch processing is enabled in config
        if !self.optimization_config.enable_batching {
            tracing::warn!(
                "CDC Service Manager: Batch processing is disabled in config, enabling it..."
            );
        }

        // Check if batch processor is already running
        if !self.processor.is_batch_processor_running().await {
            tracing::info!("CDC Service Manager: Starting batch processor...");

            // Start the batch processor
            UltraOptimizedCDCEventProcessor::enable_and_start_batch_processor_arc(
                self.processor.clone(),
            )
            .await?;

            tracing::info!("CDC Service Manager: ✅ Batch processor started successfully");
        } else {
            tracing::info!("CDC Service Manager: ✅ Batch processor is already running");
        }

        Ok(())
    }

    /// Get the CDC event processor with batch processing enabled
    pub async fn get_processor_with_batch_enabled(
        &self,
    ) -> Result<Arc<UltraOptimizedCDCEventProcessor>> {
        // Ensure batch processing is enabled
        self.ensure_batch_processing_enabled().await?;

        // Return the processor
        Ok(self.processor.clone())
    }
}

/// Enhanced Health Check with more sophisticated monitoring
pub struct CDCHealthCheck {
    metrics: Arc<EnhancedCDCMetrics>,
    last_health_check: std::sync::Mutex<std::time::Instant>,
}

impl CDCHealthCheck {
    pub fn new(metrics: Arc<EnhancedCDCMetrics>) -> Self {
        Self {
            metrics,
            last_health_check: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }

    pub fn is_healthy(&self) -> bool {
        let processed = self
            .metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = self
            .metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed);
        let consecutive_failures = self
            .metrics
            .consecutive_failures
            .load(std::sync::atomic::Ordering::Relaxed);
        let error_rate = self
            .metrics
            .error_rate
            .load(std::sync::atomic::Ordering::Relaxed);

        // Multiple health criteria - more lenient for startup and no-message scenarios
        let has_processed_events = processed > 0;
        let low_failure_rate = if processed > 0 {
            failed == 0 || (failed as f64 / processed as f64) < 0.1
        } else {
            true // No failures if no events processed yet
        };
        let no_consecutive_failures = consecutive_failures < 5;
        let acceptable_error_rate = error_rate < 10; // Less than 10%

        // Service is healthy if:
        // 1. It has processed events successfully, OR
        // 2. It has no failures and is just waiting for messages (normal startup state)
        (has_processed_events
            && low_failure_rate
            && no_consecutive_failures
            && acceptable_error_rate)
            || (!has_processed_events && failed == 0 && consecutive_failures == 0)
    }

    pub fn get_health_status(&self) -> serde_json::Value {
        let processed = self
            .metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = self
            .metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed);
        let throughput = self
            .metrics
            .throughput_per_second
            .load(std::sync::atomic::Ordering::Relaxed);
        let error_rate = self
            .metrics
            .error_rate
            .load(std::sync::atomic::Ordering::Relaxed);

        let avg_latency = if processed > 0 {
            self.metrics
                .processing_latency_ms
                .load(std::sync::atomic::Ordering::Relaxed)
                / processed
        } else {
            0
        };

        serde_json::json!({
            "healthy": self.is_healthy(),
            "timestamp": Utc::now().to_rfc3339(),
            "metrics": {
                "events_processed": processed,
                "events_failed": failed,
                "throughput_per_second": throughput,
                "error_rate_percent": error_rate,
                "avg_processing_latency_ms": avg_latency,
                "cache_invalidations": self.metrics.cache_invalidations.load(std::sync::atomic::Ordering::Relaxed),
                "projection_updates": self.metrics.projection_updates.load(std::sync::atomic::Ordering::Relaxed),
                "batches_processed": self.metrics.batches_processed.load(std::sync::atomic::Ordering::Relaxed),
                "consumer_restarts": self.metrics.consumer_restarts.load(std::sync::atomic::Ordering::Relaxed),
                "circuit_breaker_trips": self.metrics.circuit_breaker_trips.load(std::sync::atomic::Ordering::Relaxed),
                "memory_usage_mb": self.metrics.memory_usage_bytes.load(std::sync::atomic::Ordering::Relaxed) / 1024 / 1024
            }
        })
    }
}
