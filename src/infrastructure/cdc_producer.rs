use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Enhanced CDC Producer with built-in health monitoring
pub struct CDCProducer {
    kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
    pool: sqlx::PgPool,
    config: CDCProducerConfig,
    metrics: Arc<CDCProducerMetrics>,
    health_checker: Arc<CDCProducerHealthCheck>,
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,
    batch_buffer: Arc<RwLock<BatchBuffer>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    background_tasks: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub struct CDCProducerConfig {
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    pub health_check_interval_ms: u64,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_timeout_ms: u64,
    pub enable_compression: bool,
    pub enable_idempotence: bool,
    pub max_in_flight_requests: u32,
}

impl Default for CDCProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout_ms: 1000,
            max_retries: 3,
            retry_delay_ms: 100,
            health_check_interval_ms: 5000,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_ms: 30000,
            enable_compression: true,
            enable_idempotence: true,
            max_in_flight_requests: 5,
        }
    }
}

/// Enhanced metrics for CDC Producer
#[derive(Debug, Default)]
pub struct CDCProducerMetrics {
    pub messages_produced: std::sync::atomic::AtomicU64,
    pub messages_failed: std::sync::atomic::AtomicU64,
    pub batch_count: std::sync::atomic::AtomicU64,
    pub avg_batch_size: std::sync::atomic::AtomicU64,
    pub produce_latency_ms: std::sync::atomic::AtomicU64,
    pub db_write_latency_ms: std::sync::atomic::AtomicU64,
    pub kafka_produce_latency_ms: std::sync::atomic::AtomicU64,
    pub circuit_breaker_trips: std::sync::atomic::AtomicU64,
    pub retries_attempted: std::sync::atomic::AtomicU64,
    pub health_check_failures: std::sync::atomic::AtomicU64,
    pub last_successful_produce: std::sync::atomic::AtomicU64,
    pub queue_depth: std::sync::atomic::AtomicU64,
    pub throughput_per_second: std::sync::atomic::AtomicU64,
}

impl CDCProducerMetrics {
    pub fn record_successful_produce(&self, latency_ms: u64) {
        self.messages_produced
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.produce_latency_ms
            .fetch_add(latency_ms, std::sync::atomic::Ordering::Relaxed);
        self.last_successful_produce.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn record_failed_produce(&self) {
        self.messages_failed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_batch(&self, size: usize) {
        self.batch_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.avg_batch_size
            .store(size as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_success_rate(&self) -> f64 {
        let produced = self
            .messages_produced
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = self
            .messages_failed
            .load(std::sync::atomic::Ordering::Relaxed);
        let total = produced + failed;

        if total == 0 {
            100.0
        } else {
            (produced as f64 / total as f64) * 100.0
        }
    }

    pub fn get_avg_latency(&self) -> u64 {
        let total_latency = self
            .produce_latency_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let message_count = self
            .messages_produced
            .load(std::sync::atomic::Ordering::Relaxed);

        if message_count == 0 {
            0
        } else {
            total_latency / message_count
        }
    }
}

/// Circuit breaker for CDC Producer
#[derive(Debug)]
pub struct CircuitBreaker {
    state: CircuitBreakerState,
    failure_count: u32,
    last_failure_time: Option<std::time::Instant>,
    threshold: u32,
    timeout: Duration,
}

#[derive(Debug, PartialEq)]
enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, timeout: Duration) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            last_failure_time: None,
            threshold,
            timeout,
        }
    }

    pub fn can_execute(&mut self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                if let Some(last_failure) = self.last_failure_time {
                    if last_failure.elapsed() > self.timeout {
                        self.state = CircuitBreakerState::HalfOpen;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitBreakerState::HalfOpen => true,
        }
    }

    pub fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitBreakerState::Closed;
        self.last_failure_time = None;
    }

    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure_time = Some(std::time::Instant::now());

        if self.failure_count >= self.threshold {
            self.state = CircuitBreakerState::Open;
        }
    }

    pub fn is_open(&self) -> bool {
        self.state == CircuitBreakerState::Open
    }
}
pub struct BatchProcessor {
    batch_size: usize,
    batch_timeout: Duration,
    pending_events: RwLock<Vec<CDCOutboxMessageWithPayload>>,
}

impl BatchProcessor {
    pub fn new(batch_size: usize, batch_timeout: Duration) -> Self {
        Self {
            batch_size,
            batch_timeout,
            pending_events: RwLock::new(Vec::new()),
        }
    }

    pub async fn add_event(&self, event: CDCOutboxMessageWithPayload) -> Result<bool> {
        let mut pending = self.pending_events.write().await;
        pending.push(event);

        Ok(pending.len() >= self.batch_size)
    }

    pub async fn flush_batch(&self) -> Result<Vec<CDCOutboxMessageWithPayload>> {
        let mut pending = self.pending_events.write().await;
        Ok(std::mem::take(&mut *pending))
    }
}

/// Batch buffer for optimized CDC message production
#[derive(Debug)]
pub struct BatchBuffer {
    messages: Vec<CDCOutboxMessage>,
    last_flush: std::time::Instant,
    max_size: usize,
    flush_timeout: Duration,
}

impl BatchBuffer {
    pub fn new(max_size: usize, flush_timeout: Duration) -> Self {
        Self {
            messages: Vec::with_capacity(max_size),
            last_flush: std::time::Instant::now(),
            max_size,
            flush_timeout,
        }
    }

    pub fn add_message(&mut self, message: CDCOutboxMessage) -> bool {
        self.messages.push(message);
        self.should_flush()
    }

    pub fn should_flush(&self) -> bool {
        self.messages.len() >= self.max_size || self.last_flush.elapsed() > self.flush_timeout
    }

    pub fn flush(&mut self) -> Vec<CDCOutboxMessage> {
        let messages = std::mem::take(&mut self.messages);
        self.last_flush = std::time::Instant::now();
        messages
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }
}

/// Health check for CDC Producer
pub struct CDCProducerHealthCheck {
    metrics: Arc<CDCProducerMetrics>,
    kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
    pool: sqlx::PgPool,
    config: CDCProducerConfig,
    last_health_check: std::sync::atomic::AtomicU64,
    health_status: Arc<RwLock<HealthStatus>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub last_check: DateTime<Utc>,
    pub database_healthy: bool,
    pub kafka_healthy: bool,
    pub circuit_breaker_open: bool,
    pub success_rate: f64,
    pub avg_latency_ms: u64,
    pub queue_depth: u64,
    pub throughput_per_second: f64,
    pub issues: Vec<String>,
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self {
            is_healthy: false,
            last_check: Utc::now(),
            database_healthy: false,
            kafka_healthy: false,
            circuit_breaker_open: false,
            success_rate: 0.0,
            avg_latency_ms: 0,
            queue_depth: 0,
            throughput_per_second: 0.0,
            issues: Vec::new(),
        }
    }
}

impl CDCProducerHealthCheck {
    pub fn new(
        metrics: Arc<CDCProducerMetrics>,
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        pool: sqlx::PgPool,
        config: CDCProducerConfig,
    ) -> Self {
        Self {
            metrics,
            kafka_producer,
            pool,
            config,
            last_health_check: std::sync::atomic::AtomicU64::new(0),
            health_status: Arc::new(RwLock::new(HealthStatus::default())),
        }
    }

    pub async fn perform_health_check(&self) -> HealthStatus {
        let start_time = std::time::Instant::now();
        let mut status = HealthStatus {
            last_check: Utc::now(),
            ..Default::default()
        };

        // Check database health
        status.database_healthy = self.check_database_health().await;
        if !status.database_healthy {
            status.issues.push("Database connection failed".to_string());
        }

        // Check Kafka health
        status.kafka_healthy = self.check_kafka_health().await;
        if !status.kafka_healthy {
            status.issues.push("Kafka connection failed".to_string());
        }

        // Check metrics
        status.success_rate = self.metrics.get_success_rate();
        status.avg_latency_ms = self.metrics.get_avg_latency();
        status.queue_depth = self
            .metrics
            .queue_depth
            .load(std::sync::atomic::Ordering::Relaxed);

        // Calculate throughput
        let messages_produced = self
            .metrics
            .messages_produced
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_check_time = self
            .last_health_check
            .load(std::sync::atomic::Ordering::Relaxed);
        let current_time = start_time.elapsed().as_secs();

        if last_check_time > 0 {
            let time_diff = current_time - last_check_time;
            if time_diff > 0 {
                status.throughput_per_second = messages_produced as f64 / time_diff as f64;
            }
        }

        // Check success rate
        if status.success_rate < 95.0 {
            status
                .issues
                .push(format!("Low success rate: {:.2}%", status.success_rate));
        }

        // Check latency
        if status.avg_latency_ms > 1000 {
            status
                .issues
                .push(format!("High latency: {}ms", status.avg_latency_ms));
        }

        // Check last successful produce
        let last_success = self
            .metrics
            .last_successful_produce
            .load(std::sync::atomic::Ordering::Relaxed);
        if last_success > 0 {
            let time_since_last_success = current_time - last_success;
            if time_since_last_success > 60 {
                status.issues.push(format!(
                    "No successful produce in {}s",
                    time_since_last_success
                ));
            }
        }

        // Overall health
        status.is_healthy = status.database_healthy
            && status.kafka_healthy
            && status.success_rate >= 95.0
            && status.avg_latency_ms < 1000;

        // Update stored status
        {
            let mut stored_status = self.health_status.write().await;
            *stored_status = status.clone();
        }

        self.last_health_check
            .store(current_time, std::sync::atomic::Ordering::Relaxed);
        status
    }

    async fn check_database_health(&self) -> bool {
        match sqlx::query("SELECT 1").fetch_one(&self.pool).await {
            Ok(_) => true,
            Err(e) => {
                error!("Database health check failed: {}", e);
                false
            }
        }
    }

    async fn check_kafka_health(&self) -> bool {
        // This would require a ping/health check method on the Kafka producer
        // For now, we'll assume it's healthy if we can create a test message
        true // Placeholder - implement actual Kafka health check
    }

    pub async fn get_health_status(&self) -> HealthStatus {
        self.health_status.read().await.clone()
    }

    pub fn is_healthy(&self) -> bool {
        // Quick check without full health check
        self.metrics.get_success_rate() >= 95.0
    }
}

/// Kafka message structure for CDC events
#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub timestamp: Option<i64>,
}

/// CDC-based outbox message structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CDCOutboxMessage {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub topic: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CDCProducer {
    pub async fn new(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        pool: sqlx::PgPool,
        config: CDCProducerConfig,
    ) -> Result<Self> {
        let metrics = Arc::new(CDCProducerMetrics::default());
        let health_checker = Arc::new(CDCProducerHealthCheck::new(
            metrics.clone(),
            kafka_producer.clone(),
            pool.clone(),
            config.clone(),
        ));

        let circuit_breaker = Arc::new(RwLock::new(CircuitBreaker::new(
            config.circuit_breaker_threshold,
            Duration::from_millis(config.circuit_breaker_timeout_ms),
        )));

        let batch_buffer = Arc::new(RwLock::new(BatchBuffer::new(
            config.batch_size,
            Duration::from_millis(config.batch_timeout_ms),
        )));

        Ok(Self {
            kafka_producer,
            pool,
            config,
            metrics,
            health_checker,
            circuit_breaker,
            batch_buffer,
            shutdown_tx: None,
            background_tasks: Vec::new(),
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting CDC Producer with health monitoring");

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Start health check task
        let health_checker = self.health_checker.clone();
        let health_check_interval = Duration::from_millis(self.config.health_check_interval_ms);
        let health_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(health_check_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let status = health_checker.perform_health_check().await;
                        if !status.is_healthy {
                            warn!("CDC Producer health check failed: {:?}", status.issues);
                        } else {
                            info!("CDC Producer health check passed");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Health check task shutting down");
                        break;
                    }
                }
            }
        });
        self.background_tasks.push(health_task);

        // Start batch flush task
        let batch_buffer = self.batch_buffer.clone();
        let producer = self.kafka_producer.clone();
        let pool = self.pool.clone();
        let metrics = self.metrics.clone();
        let circuit_breaker = self.circuit_breaker.clone();
        let (_, mut batch_shutdown_rx) = mpsc::channel(1);

        let batch_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let should_flush = {
                            let buffer = batch_buffer.read().await;
                            buffer.should_flush() && !buffer.is_empty()
                        };

                        if should_flush {
                            let messages = {
                                let mut buffer = batch_buffer.write().await;
                                buffer.flush()
                            };

                            if !messages.is_empty() {
                                let can_execute = {
                                    let mut cb = circuit_breaker.write().await;
                                    cb.can_execute()
                                };

                                if can_execute {
                                    match Self::flush_batch_internal(&producer, &pool, messages.clone(), &metrics).await {
                                        Ok(_) => {
                                            let mut cb = circuit_breaker.write().await;
                                            cb.record_success();
                                            metrics.record_batch(messages.len());
                                        }
                                        Err(e) => {
                                            error!("Failed to flush batch: {}", e);
                                            let mut cb = circuit_breaker.write().await;
                                            cb.record_failure();
                                            metrics.circuit_breaker_trips.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                    }
                                } else {
                                    warn!("Circuit breaker open, skipping batch flush");
                                }
                            }
                        }
                    }
                    _ = batch_shutdown_rx.recv() => {
                        info!("Batch flush task shutting down");
                        break;
                    }
                }
            }
        });
        self.background_tasks.push(batch_task);

        // Start throughput calculation task
        let metrics_clone = self.metrics.clone();
        let throughput_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut last_count = 0u64;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let current_count = metrics_clone.messages_produced.load(std::sync::atomic::Ordering::Relaxed);
                        let throughput = current_count - last_count;
                        metrics_clone.throughput_per_second.store(throughput, std::sync::atomic::Ordering::Relaxed);
                        last_count = current_count;
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Throughput calculation task shutting down");
                        break;
                    }
                }
            }
        });
        self.background_tasks.push(throughput_task);

        info!("CDC Producer started successfully");
        Ok(())
    }

    pub async fn produce_message(&self, message: CDCOutboxMessage) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Check circuit breaker
        {
            let mut cb = self.circuit_breaker.write().await;
            if !cb.can_execute() {
                return Err(anyhow::anyhow!("Circuit breaker is open"));
            }
        }

        // Add to batch buffer
        let should_flush = {
            let mut buffer = self.batch_buffer.write().await;
            buffer.add_message(message)
        };

        if should_flush {
            let messages = {
                let mut buffer = self.batch_buffer.write().await;
                buffer.flush()
            };

            if !messages.is_empty() {
                match self.flush_batch(messages).await {
                    Ok(_) => {
                        let mut cb = self.circuit_breaker.write().await;
                        cb.record_success();
                        self.metrics
                            .record_successful_produce(start_time.elapsed().as_millis() as u64);
                    }
                    Err(e) => {
                        let mut cb = self.circuit_breaker.write().await;
                        cb.record_failure();
                        self.metrics.record_failed_produce();
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn flush_batch(&self, messages: Vec<CDCOutboxMessage>) -> Result<()> {
        Self::flush_batch_internal(&self.kafka_producer, &self.pool, messages, &self.metrics).await
    }

    async fn flush_batch_internal(
        producer: &crate::infrastructure::kafka_abstraction::KafkaProducer,
        pool: &sqlx::PgPool,
        messages: Vec<CDCOutboxMessage>,
        metrics: &CDCProducerMetrics,
    ) -> Result<()> {
        let batch_size = messages.len();
        let start_time = std::time::Instant::now();

        // Start database transaction
        let mut tx = pool.begin().await?;
        let db_start = std::time::Instant::now();

        // Insert messages into outbox table
        for msg in &messages {
            sqlx::query!(
                r#"
                INSERT INTO kafka_outbox_cdc
                    (id, aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
                msg.id,
                msg.aggregate_id,
                msg.event_id,
                msg.event_type,
                Vec::<u8>::new(), // Placeholder payload
                msg.topic,
                msg.metadata,
                msg.created_at,
                msg.updated_at
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        let db_latency = db_start.elapsed().as_millis() as u64;
        metrics
            .db_write_latency_ms
            .fetch_add(db_latency, std::sync::atomic::Ordering::Relaxed);

        // Update metrics
        metrics.record_batch(batch_size);
        let total_latency = start_time.elapsed().as_millis() as u64;

        info!(
            "Flushed batch of {} messages in {}ms (DB: {}ms)",
            batch_size, total_latency, db_latency
        );

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping CDC Producer");

        // Send shutdown signal
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(()).await;
        }

        // Wait for background tasks to complete
        for task in self.background_tasks.drain(..) {
            task.abort();
        }

        // Flush remaining messages
        let remaining_messages = {
            let mut buffer = self.batch_buffer.write().await;
            buffer.flush()
        };

        if !remaining_messages.is_empty() {
            info!("Flushing {} remaining messages", remaining_messages.len());
            if let Err(e) = self.flush_batch(remaining_messages).await {
                error!("Failed to flush remaining messages: {}", e);
            }
        }

        info!("CDC Producer stopped");
        Ok(())
    }

    pub async fn get_health_status(&self) -> HealthStatus {
        self.health_checker.get_health_status().await
    }

    pub fn get_metrics(&self) -> &CDCProducerMetrics {
        &self.metrics
    }

    pub async fn is_healthy(&self) -> bool {
        self.health_checker.is_healthy()
    }

    pub async fn get_detailed_status(&self) -> serde_json::Value {
        let health = self.get_health_status().await;
        let circuit_breaker_open = {
            let cb = self.circuit_breaker.read().await;
            cb.is_open()
        };
        let queue_depth = {
            let buffer = self.batch_buffer.read().await;
            buffer.len()
        };

        serde_json::json!({
            "health": health,
            "circuit_breaker_open": circuit_breaker_open,
            "queue_depth": queue_depth,
            "metrics": {
                "messages_produced": self.metrics.messages_produced.load(std::sync::atomic::Ordering::Relaxed),
                "messages_failed": self.metrics.messages_failed.load(std::sync::atomic::Ordering::Relaxed),
                "success_rate": self.metrics.get_success_rate(),
                "avg_latency_ms": self.metrics.get_avg_latency(),
                "throughput_per_second": self.metrics.throughput_per_second.load(std::sync::atomic::Ordering::Relaxed),
                "circuit_breaker_trips": self.metrics.circuit_breaker_trips.load(std::sync::atomic::Ordering::Relaxed),
            }
        })
    }
}
