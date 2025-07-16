use crate::domain::{Account, AccountEvent};
use crate::infrastructure::cache_service::{CacheService, CacheServiceTrait};
use crate::infrastructure::event_processor::EventProcessor;
use crate::infrastructure::event_store::{EventStore, EventStoreTrait};
use crate::infrastructure::kafka_abstraction::{
    EventBatch, KafkaConfig, KafkaConsumer, KafkaProducer,
};
use crate::infrastructure::kafka_dlq::{DeadLetterQueue, DeadLetterQueueTrait};
use crate::infrastructure::kafka_metrics::KafkaMetrics;
use crate::infrastructure::kafka_monitoring::{MonitoringDashboard, MonitoringDashboardTrait};
use crate::infrastructure::kafka_recovery::{KafkaRecovery, KafkaRecoveryTrait};
use crate::infrastructure::kafka_recovery_strategies::{RecoveryStrategies, RecoveryStrategy};
use crate::infrastructure::kafka_tracing::{KafkaTracing, KafkaTracingTrait};
use crate::infrastructure::projections::{
    AccountProjection, ProjectionStore, ProjectionStoreTrait,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono;
use rdkafka::error::KafkaError;
use rust_decimal::Decimal;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Default)]
struct ProcessingState {
    is_processing: bool,
    current_batch: Option<EventBatch>,
    last_processed_offset: i64,
}

#[derive(Clone)]
pub struct KafkaEventProcessor {
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    event_store: Arc<dyn EventStoreTrait + Send + Sync>,
    projections: Arc<dyn ProjectionStoreTrait + Send + Sync>,
    dlq: Arc<DeadLetterQueue>,
    recovery: Arc<KafkaRecovery>,
    recovery_strategies: Arc<RecoveryStrategies>,
    metrics: Arc<KafkaMetrics>,
    monitoring: Arc<dyn MonitoringDashboardTrait + Send + Sync>,
    tracing: Arc<dyn KafkaTracingTrait + Send + Sync>,
    cache_service: Arc<dyn CacheServiceTrait + Send + Sync>,
    processing_state: Arc<RwLock<ProcessingState>>,
    retry_config: RetryConfig,
}

#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay_ms: 100,
        }
    }
}

impl KafkaEventProcessor {
    async fn execute_with_retry<F, Fut, T, E>(
        &self,
        operation_name: &str,
        operation: F,
    ) -> Result<T, anyhow::Error>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display + std::error::Error + Send + Sync + 'static,
    {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match operation().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if attempts >= self.retry_config.max_attempts {
                        error!(
                            "Operation '{}' failed after {} attempts: {}",
                            operation_name, attempts, e
                        );
                        return Err(anyhow::anyhow!(e).context(format!(
                            "Operation '{}' ultimately failed after {} attempts",
                            operation_name, attempts
                        )));
                    }
                    warn!(
                        "Operation '{}' failed (attempt {}/{}): {}. Retrying in {}ms...",
                        operation_name,
                        attempts,
                        self.retry_config.max_attempts,
                        e,
                        self.retry_config.delay_ms
                    );
                    sleep(Duration::from_millis(self.retry_config.delay_ms)).await;
                }
            }
        }
    }

    pub fn new(
        config: KafkaConfig,
        event_store: &Arc<dyn EventStoreTrait + Send + Sync>,
        projections: &Arc<dyn ProjectionStoreTrait + Send + Sync>,
        cache_service: &Arc<dyn CacheServiceTrait + Send + Sync>,
        retry_config: RetryConfig,
    ) -> Result<Self> {
        let metrics = Arc::new(KafkaMetrics::default());
        let producer = KafkaProducer::new(config.clone())?;
        let consumer = KafkaConsumer::new(config.clone())?;

        let dlq_consumer = KafkaConsumer::new(config.clone())?;

        let dlq = Arc::new(DeadLetterQueue::new(
            producer.clone(),
            dlq_consumer,
            metrics.clone(),
            3,
            Duration::from_secs(1),
        ));

        let recovery = Arc::new(KafkaRecovery::new(
            producer.clone(),
            consumer.clone(),
            event_store.clone(),
            projections.clone(),
            dlq.clone(),
            metrics.clone(),
        ));

        let recovery_strategies = Arc::new(RecoveryStrategies::new(
            event_store.clone(),
            producer.clone(),
            consumer.clone(),
            dlq.clone(),
            metrics.clone(),
        ));

        let monitoring = Arc::new(MonitoringDashboard::new(metrics.clone()));
        let tracing = Arc::new(KafkaTracing::new(metrics.clone()));

        Ok(Self {
            producer,
            consumer,
            event_store: event_store.clone(),
            projections: projections.clone(),
            dlq,
            recovery,
            recovery_strategies,
            metrics,
            monitoring,
            tracing,
            cache_service: cache_service.clone(),
            processing_state: Arc::new(RwLock::new(ProcessingState::default())),
            retry_config,
        })
    }

    pub async fn start_processing(&self) -> Result<()> {
        self.tracing.init_tracing().map_err(|e| {
            anyhow::Error::msg("Failed to initialize tracing: ".to_string() + &e.to_string())
        })?;

        self.consumer.subscribe_to_events().await?;

        let dlq = self.dlq.clone();
        tokio::spawn(async move {
            if let Err(e) = dlq.process_dlq().await {
                error!("DLQ processing failed: {}", e);
            }
        });

        let mut monitoring = self.monitoring.clone();
        tokio::spawn(async move {
            loop {
                monitoring.record_metrics();
                monitoring.check_alerts();
                monitoring.update_health_status();
                sleep(Duration::from_secs(60)).await;
            }
        });

        loop {
            let start_time = std::time::Instant::now();

            match self.consumer.poll_events().await {
                Ok(Some(batch)) => {
                    self.metrics
                        .messages_consumed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.metrics.consume_latency.fetch_add(
                        start_time.elapsed().as_millis() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    self.tracing.trace_event_processing(
                        batch.account_id,
                        &batch.events,
                        batch.version,
                    );

                    match self.process_batch(batch.clone()).await {
                        Ok(_) => {
                            self.metrics.events_processed.fetch_add(
                                batch.events.len() as u64,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                        Err(e) => {
                            error!("Failed to process event batch: {}", e);
                            self.metrics
                                .processing_errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.tracing.trace_error(
                                &anyhow::Error::msg(e.to_string()),
                                "batch_processing",
                            );

                            self.dlq
                                .send_to_dlq(
                                    batch.account_id,
                                    batch.events,
                                    batch.version,
                                    "Batch processing failed: ".to_string() + &e.to_string(),
                                )
                                .await?;
                        }
                    }
                }
                Ok(None) => {
                    continue;
                }
                Err(e) => {
                    error!("Error polling Kafka: {}", e);
                    self.metrics
                        .consume_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.tracing
                        .trace_error(&anyhow::Error::msg(e.to_string()), "kafka_polling");

                    if self.should_trigger_recovery().await {
                        if let Err(e) = self.recovery.start_recovery().await {
                            error!("Recovery failed: {}", e);
                            self.tracing
                                .trace_error(&anyhow::Error::msg(e.to_string()), "recovery");
                        }
                    }
                }
            }
            self.tracing.trace_metrics();
            self.tracing.trace_performance_metrics();
        }
    }

    async fn process_batch(&self, batch: EventBatch) -> Result<()> {
        let start_time = std::time::Instant::now();

        let versioned_events: Vec<(i64, AccountEvent)> = batch
            .events
            .iter()
            .enumerate()
            .map(|(i, event)| {
                (
                    batch.version - batch.events.len() as i64 + 1 + i as i64,
                    event.clone(),
                )
            })
            .collect();

        self.cache_service
            .set_account_events(batch.account_id, &versioned_events, None)
            .await?;

        let existing_projection_opt = self.projections.get_account(batch.account_id).await?;

        let mut account_projection_to_update: AccountProjection;
        let mut process_account_state_related_updates = true;

        if let Some(existing_proj) = existing_projection_opt {
            // Since we removed version from projections, we'll always process updates
            // This is simpler and avoids potential issues with version tracking
            let mut current_proj = existing_proj.clone();
            for event in &batch.events {
                current_proj = current_proj.apply_event(event)?;
            }
            current_proj.updated_at = chrono::Utc::now();
            account_projection_to_update = current_proj;
        } else {
            account_projection_to_update = AccountProjection {
                id: batch.account_id,
                owner_name: "".to_string(),
                balance: Decimal::ZERO,
                is_active: false,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            for event in &batch.events {
                account_projection_to_update = account_projection_to_update.apply_event(event)?;
            }
            account_projection_to_update.updated_at = chrono::Utc::now();
            // If it's an AccountCreated event, apply_event should set created_at.
            if account_projection_to_update.created_at.timestamp_millis()
                == chrono::Utc::now().timestamp_millis()
            {
                if let Some(AccountEvent::AccountCreated { .. }) = batch.events.first() {
                    // apply_event should handle this. If not, this is a fallback.
                    // This assumes the projection's created_at is set by the event.
                }
            }
        }

        if process_account_state_related_updates {
            self.projections.upsert_accounts_batch(vec![account_projection_to_update.clone()]).await.map_err(|e| {
                error!("CRITICAL: Failed to upsert account projection for {}: {}. Batch will be sent to DLQ.", batch.account_id, e);
                anyhow::anyhow!(e)
            })?;
            info!(
                "Successfully upserted account projection for account {}",
                batch.account_id
            );

            let final_account_domain_state = Account {
                id: account_projection_to_update.id,
                owner_name: account_projection_to_update.owner_name.clone(),
                balance: account_projection_to_update.balance,
                is_active: account_projection_to_update.is_active,
                version: 0, // Projections don't track version anymore
            };

            self.cache_service
                .set_account(&final_account_domain_state, Some(Duration::from_secs(3600)))
                .await?;

            if let Err(e) = self
                .producer
                .send_cache_update(batch.account_id, &final_account_domain_state)
                .await
            {
                warn!(
                    "Non-fatal: Failed to send Kafka cache update for account {}: {}",
                    batch.account_id, e
                );
            }

            self.metrics
                .cache_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let mut transaction_projections = Vec::new();
        for event in &batch.events {
            match event {
                AccountEvent::MoneyDeposited {
                    transaction_id,
                    amount,
                    ..
                } => {
                    transaction_projections.push(
                        crate::infrastructure::projections::TransactionProjection {
                            id: *transaction_id,
                            account_id: batch.account_id,
                            transaction_type: "MoneyDeposited".to_string(),
                            amount: *amount,
                            timestamp: chrono::Utc::now(),
                        },
                    );
                }
                AccountEvent::MoneyWithdrawn {
                    transaction_id,
                    amount,
                    ..
                } => {
                    transaction_projections.push(
                        crate::infrastructure::projections::TransactionProjection {
                            id: *transaction_id,
                            account_id: batch.account_id,
                            transaction_type: "MoneyWithdrawn".to_string(),
                            amount: *amount,
                            timestamp: chrono::Utc::now(),
                        },
                    );
                }
                _ => {}
            }
        }

        if !transaction_projections.is_empty() {
            self.projections.insert_transactions_batch(transaction_projections.clone()).await.map_err(|e| {
                error!("CRITICAL: Failed to insert transaction projections for {}: {}. Batch will be sent to DLQ.", batch.account_id, e);
                anyhow::anyhow!(e)
            })?;
            info!(
                "Successfully processed {} transaction projections for account {}",
                transaction_projections.len(),
                batch.account_id
            );
        }

        self.metrics.processing_latency.fetch_add(
            start_time.elapsed().as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    async fn should_trigger_recovery(&self) -> bool {
        let error_rate = self.metrics.get_error_rate();
        let consumer_lag = self
            .metrics
            .consumer_lag
            .load(std::sync::atomic::Ordering::Relaxed);
        error_rate > 0.1 || consumer_lag > 1000
    }

    pub async fn get_processing_metrics(&self) -> ProcessingMetrics {
        ProcessingMetrics {
            error_rate: self.metrics.get_error_rate(),
            average_processing_latency: self.metrics.get_average_processing_latency(),
            average_consume_latency: self.metrics.get_average_consume_latency(),
            dlq_retry_success_rate: self.metrics.get_dlq_retry_success_rate(),
            consumer_lag: self
                .metrics
                .consumer_lag
                .load(std::sync::atomic::Ordering::Relaxed),
            events_processed: self
                .metrics
                .events_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            processing_errors: self
                .metrics
                .processing_errors
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub async fn execute_recovery_strategy(
        &self,
        strategy: RecoveryStrategy,
        account_id: Option<Uuid>,
    ) -> Result<()> {
        let strategy_str = "{:?}".to_string() + &(strategy).to_string();
        self.tracing
            .trace_recovery_operation(&strategy_str, account_id, "started");

        match self
            .recovery_strategies
            .execute_recovery(strategy, account_id)
            .await
        {
            Ok(_) => {
                self.tracing
                    .trace_recovery_operation(&strategy_str, account_id, "completed");
                Ok(())
            }
            Err(e) => {
                self.tracing.trace_error(
                    &anyhow::Error::msg(e.to_string()),
                    &("recovery_strategy_".to_string() + &strategy_str),
                );
                Err(e)
            }
        }
    }
}

#[async_trait]
impl EventProcessor for KafkaEventProcessor {
    async fn process_event(&self, event: serde_json::Value) -> Result<()> {
        let batch: EventBatch = serde_json::from_value(event)?;
        self.process_batch(batch).await
    }
}

#[derive(Debug)]
pub struct ProcessingMetrics {
    pub error_rate: f64,
    pub average_processing_latency: f64,
    pub average_consume_latency: f64,
    pub dlq_retry_success_rate: f64,
    pub consumer_lag: u64,
    pub events_processed: u64,
    pub processing_errors: u64,
}
