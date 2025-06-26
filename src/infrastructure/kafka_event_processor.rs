use crate::domain::{Account, AccountEvent};
use crate::infrastructure::cache_service::{CacheService, CacheServiceTrait};
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
use tracing::{error, info};
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
}

impl KafkaEventProcessor {
    pub fn new(
        config: KafkaConfig,
        event_store: &Arc<dyn EventStoreTrait + Send + Sync>,
        projections: &Arc<dyn ProjectionStoreTrait + Send + Sync>,
        cache_service: &Arc<dyn CacheServiceTrait + Send + Sync>,
    ) -> Result<Self> {
        let metrics = Arc::new(KafkaMetrics::default());
        let producer = KafkaProducer::new(config.clone())?;
        let consumer = KafkaConsumer::new(config.clone())?;

        // Create a separate consumer for DLQ
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
        })
    }

    pub async fn start_processing(&self) -> Result<()> {
        // Initialize tracing
        self.tracing.init_tracing().map_err(|e| {
            anyhow::Error::msg("Failed to initialize tracing: ".to_string() + &e.to_string())
        })?;

        self.consumer.subscribe_to_events().await?;

        // Start DLQ processing in background
        let dlq = self.dlq.clone();
        tokio::spawn(async move {
            if let Err(e) = dlq.process_dlq().await {
                let _ = std::io::stderr().write_all(
                    ("DLQ processing failed: ".to_string() + &e.to_string() + "\n").as_bytes(),
                );
            }
        });

        // Start monitoring in background
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
                            let _ = std::io::stderr().write_all(
                                ("Failed to process event batch: ".to_string()
                                    + &e.to_string()
                                    + "\n")
                                    .as_bytes(),
                            );
                            self.metrics
                                .processing_errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.tracing.trace_error(
                                &anyhow::Error::msg(e.to_string()),
                                "batch_processing",
                            );

                            // Send to DLQ
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
                    // No messages available, continue polling
                    continue;
                }
                Err(e) => {
                    let _ = std::io::stderr().write_all(
                        ("Error polling Kafka: ".to_string() + &e.to_string() + "\n").as_bytes(),
                    );
                    self.metrics
                        .consume_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.tracing
                        .trace_error(&anyhow::Error::msg(e.to_string()), "kafka_polling");

                    // If we encounter persistent errors, trigger recovery
                    if self.should_trigger_recovery().await {
                        if let Err(e) = self.recovery.start_recovery().await {
                            let _ = std::io::stderr().write_all(
                                ("Recovery failed: ".to_string() + &e.to_string() + "\n")
                                    .as_bytes(),
                            );
                            self.tracing
                                .trace_error(&anyhow::Error::msg(e.to_string()), "recovery");
                        }
                    }
                }
            }

            // Record metrics periodically
            self.tracing.trace_metrics();
            self.tracing.trace_performance_metrics();
        }
    }

    async fn process_batch(&self, batch: EventBatch) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Don't save events to event store again - they're already saved by the repository
        // The repository handles event storage and optimistic concurrency control

        // Convert events to versioned format for caching
        let versioned_events: Vec<(i64, AccountEvent)> = batch
            .events
            .iter()
            .enumerate()
            .map(|(i, event)| (batch.version + i as i64, event.clone()))
            .collect();

        // Cache the events
        self.cache_service
            .set_account_events(batch.account_id, &versioned_events, None)
            .await?;

        // Get account from projections and apply latest events
        let account = self.projections.get_account(batch.account_id).await?;
        if let Some(mut account_proj) = account {
            // Apply latest events to get final state
            for event in &batch.events {
                account_proj = account_proj.apply_event(event)?;
            }

            // Update the account projection using batch upsert
            if let Err(e) = self
                .projections
                .upsert_accounts_batch(vec![account_proj.clone()])
                .await
            {
                error!("Failed to update account projection: {}", e);
                // Don't fail the entire operation if projection update fails
            } else {
                info!(
                    "Successfully updated account projection for account {}",
                    batch.account_id
                );
            }

            // Convert projection to account with updated state
            let account = Account {
                id: account_proj.id,
                owner_name: account_proj.owner_name,
                balance: account_proj.balance,
                is_active: account_proj.is_active,
                version: batch.version + batch.events.len() as i64,
            };

            // Cache the updated account
            self.cache_service
                .set_account(&account, Some(Duration::from_secs(3600)))
                .await?;

            // Send cache update with final state
            self.producer
                .send_cache_update(batch.account_id, &account)
                .await?;
            self.metrics
                .cache_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            // Account doesn't exist in projections, create it from events
            let mut account_proj = AccountProjection {
                id: batch.account_id,
                owner_name: "Unknown".to_string(),
                balance: rust_decimal::Decimal::ZERO,
                is_active: true,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            for event in &batch.events {
                account_proj = account_proj.apply_event(event)?;
            }

            // Insert the new account projection using batch upsert
            if let Err(e) = self
                .projections
                .upsert_accounts_batch(vec![account_proj.clone()])
                .await
            {
                error!("Failed to insert account projection: {}", e);
                // Don't fail the entire operation if projection insert fails
            } else {
                info!(
                    "Successfully created account projection for account {}",
                    batch.account_id
                );
            }

            // Convert projection to account with updated state
            let account = Account {
                id: account_proj.id,
                owner_name: account_proj.owner_name,
                balance: account_proj.balance,
                is_active: account_proj.is_active,
                version: batch.version + batch.events.len() as i64,
            };

            // Cache the updated account
            self.cache_service
                .set_account(&account, Some(Duration::from_secs(3600)))
                .await?;

            // Send cache update with final state
            self.producer
                .send_cache_update(batch.account_id, &account)
                .await?;
            self.metrics
                .cache_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Create transaction projections for MoneyDeposited and MoneyWithdrawn events
        let mut transaction_projections = Vec::new();
        let mut transaction_count = 0;
        for event in &batch.events {
            match event {
                AccountEvent::MoneyDeposited {
                    account_id, amount, ..
                } => {
                    transaction_count += 1;
                    transaction_projections.push(
                        crate::infrastructure::projections::TransactionProjection {
                            id: uuid::Uuid::new_v4(),
                            account_id: *account_id,
                            transaction_type: "MoneyDeposited".to_string(),
                            amount: *amount,
                            timestamp: chrono::Utc::now(),
                        },
                    );
                }
                AccountEvent::MoneyWithdrawn {
                    account_id, amount, ..
                } => {
                    transaction_count += 1;
                    transaction_projections.push(
                        crate::infrastructure::projections::TransactionProjection {
                            id: uuid::Uuid::new_v4(),
                            account_id: *account_id,
                            transaction_type: "MoneyWithdrawn".to_string(),
                            amount: *amount,
                            timestamp: chrono::Utc::now(),
                        },
                    );
                }
                _ => {} // Skip other event types
            }
        }

        // Insert transaction projections if any were created
        if !transaction_projections.is_empty() {
            if let Err(e) = self
                .projections
                .insert_transactions_batch(transaction_projections)
                .await
            {
                error!("Failed to insert transaction projections: {}", e);
                // Don't fail the entire operation if transaction projections fail
            } else {
                info!(
                    "Successfully created {} transaction projections",
                    transaction_count
                );
            }
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

        // Trigger recovery if error rate is high or consumer lag is significant
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
