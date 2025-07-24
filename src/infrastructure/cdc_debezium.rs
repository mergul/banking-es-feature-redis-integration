use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

// Import the optimized components
use crate::infrastructure::cdc_event_processor::{
    BusinessLogicConfig, UltraOptimizedCDCEventProcessor,
};
use crate::infrastructure::cdc_integration_helper::{
    CDCIntegrationConfig, CDCIntegrationHelper, CDCIntegrationHelperBuilder,
    MigrationIntegrityReport, MigrationStats,
};
use crate::infrastructure::cdc_producer::{BusinessLogicValidator, CDCProducer, CDCProducerConfig};
use crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics;
use crate::infrastructure::event_processor::EventProcessor;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

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

#[derive(Debug, Clone)]
pub struct CDCOutboxMessageWithPayload {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub topic: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Debezium CDC connector configuration
#[derive(Debug, Clone)]
pub struct DebeziumConfig {
    pub connector_name: String,
    pub database_host: String,
    pub database_port: u16,
    pub database_name: String,
    pub database_user: String,
    pub database_password: String,
    pub table_include_list: String,
    pub topic_prefix: String,
    pub snapshot_mode: String,
    pub poll_interval_ms: u64,
}

impl Default for DebeziumConfig {
    fn default() -> Self {
        Self {
            connector_name: "banking-es-connector".to_string(),
            database_host: "localhost".to_string(),
            database_port: 5432,
            database_name: "banking_es".to_string(),
            database_user: "postgres".to_string(),
            database_password: "Francisco1".to_string(),
            table_include_list: "public.kafka_outbox_cdc".to_string(), // Match actual Debezium config
            topic_prefix: "banking-es".to_string(), // Match actual Debezium config
            snapshot_mode: "initial".to_string(),
            poll_interval_ms: 100, // Much faster than 5-second polling
        }
    }
}

/// CDC-based outbox repository trait
#[async_trait]
pub trait CDCOutboxRepositoryTrait: Send + Sync {
    /// Add messages to outbox (simplified - no status tracking needed)
    async fn add_outbox_messages(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        messages: Vec<CDCOutboxMessage>,
    ) -> Result<()>;

    /// Get outbox table schema for Debezium configuration
    fn get_outbox_schema(&self) -> &str;

    /// Clean up old processed messages (optional)
    async fn cleanup_old_messages(&self, older_than: Duration) -> Result<usize>;
}

/// CDC-based outbox repository that implements the existing OutboxRepositoryTrait
/// This allows seamless integration with the current CQRS system
#[derive(Clone)]
pub struct CDCOutboxRepository {
    pools: Arc<PartitionedPools>,
}

impl CDCOutboxRepository {
    pub fn new(pools: Arc<PartitionedPools>) -> Self {
        Self { pools }
    }

    /// Create optimized outbox table for CDC
    pub async fn create_cdc_outbox_table(&self) -> Result<()> {
        let write_pool = self.pools.select_pool(OperationType::Write);

        // Create the table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS kafka_outbox_cdc (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                aggregate_id UUID NOT NULL,
                event_id UUID NOT NULL UNIQUE,
                event_type VARCHAR(255) NOT NULL,
                payload BYTEA NOT NULL,
                topic VARCHAR(255) NOT NULL,
                metadata JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            "#,
        )
        .execute(write_pool)
        .await?;

        // Create indexes separately
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_cdc_created_at ON kafka_outbox_cdc(created_at)",
        )
        .execute(write_pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_outbox_cdc_aggregate_id ON kafka_outbox_cdc(aggregate_id)")
            .execute(write_pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_cdc_event_id ON kafka_outbox_cdc(event_id)",
        )
        .execute(write_pool)
        .await?;

        // Enable logical replication
        sqlx::query("ALTER TABLE kafka_outbox_cdc REPLICA IDENTITY FULL")
            .execute(write_pool)
            .await?;

        Ok(())
    }

    /// Generate Debezium connector configuration
    pub fn generate_debezium_config(&self, config: &DebeziumConfig) -> serde_json::Value {
        serde_json::json!({
            "name": config.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": config.database_host,
                "database.port": config.database_port,
                "database.user": config.database_user,
                "database.password": config.database_password,
                "database.dbname": config.database_name,
                "database.server.name": "banking_es_server",
                "table.include.list": config.table_include_list,
                "topic.prefix": config.topic_prefix,
                "snapshot.mode": config.snapshot_mode,
                "poll.interval.ms": config.poll_interval_ms,
                "publication.autocreate.mode": "filtered",
                "slot.name": "banking_outbox_slot",
                "plugin.name": "pgoutput",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": false,
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": false,
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.unwrap.operation.header": true
            }
        })
    }

    /// Clean up old processed messages (optional)
    pub async fn cleanup_old_messages(&self, older_than: Duration) -> Result<usize> {
        let cutoff_time = Utc::now() - chrono::Duration::from_std(older_than)?;
        let result = sqlx::query!(
            "DELETE FROM kafka_outbox_cdc WHERE created_at < $1",
            cutoff_time
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await?;

        Ok(result.rows_affected() as usize)
    }
}

/// OutboxBatcher - Buffers outbox messages and flushes them in batches
/// OPTIMIZED: Flushes when buffer reaches batch_size (10) or after batch_timeout (10ms)
pub struct OutboxBatcher {
    sender: mpsc::Sender<crate::infrastructure::outbox::OutboxMessage>,
}

impl Clone for OutboxBatcher {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl OutboxBatcher {
    pub fn new(
        repo: Arc<dyn crate::infrastructure::outbox::OutboxRepositoryTrait + Send + Sync>,
        pools: Arc<PartitionedPools>,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(1000);
        let mut buffer = Vec::new();
        let mut last_flush = tokio::time::Instant::now();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = receiver.recv() => {
                        match msg {
                            Some(msg) => {
                                buffer.push(msg);
                                // Flush only when the buffer is full
                                if buffer.len() >= batch_size {
                                    Self::flush(&repo, &pools, &mut buffer).await;
                                    last_flush = tokio::time::Instant::now();
                                }
                            }
                            None => {
                                // Channel closed, flush remaining messages
                                if !buffer.is_empty() {
                                    Self::flush(&repo, &pools, &mut buffer).await;
                                }
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(last_flush + batch_timeout) => {
                        if !buffer.is_empty() {
                            Self::flush(&repo, &pools, &mut buffer).await;
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn submit(&self, msg: crate::infrastructure::outbox::OutboxMessage) -> Result<()> {
        self.sender
            .send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))
    }

    async fn flush(
        repo: &Arc<dyn crate::infrastructure::outbox::OutboxRepositoryTrait + Send + Sync>,
        pools: &Arc<PartitionedPools>,
        buffer: &mut Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) {
        if buffer.is_empty() {
            return;
        }

        let start_time = std::time::Instant::now();
        let message_count = buffer.len();

        let write_pool = pools.select_pool(OperationType::Write);
        let mut transaction = match write_pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                error!("Failed to begin transaction for outbox batch flush: {}", e);
                return;
            }
        };

        let messages = std::mem::replace(buffer, Vec::new());
        if let Err(e) = repo.add_pending_messages(&mut transaction, messages).await {
            error!("Failed to add outbox messages: {}", e);
            if let Err(e) = transaction.rollback().await {
                error!("Failed to rollback transaction: {}", e);
            }
            return;
        }

        if let Err(e) = transaction.commit().await {
            error!("Failed to commit outbox batch: {}", e);
            return;
        }

        let duration = start_time.elapsed();
        info!(
            "✅ OutboxBatcher: Flushed {} messages in {:?}",
            message_count, duration
        );
    }

    pub fn new_default(
        repo: Arc<dyn crate::infrastructure::outbox::OutboxRepositoryTrait + Send + Sync>,
        pools: Arc<PartitionedPools>,
    ) -> Self {
        // OPTIMIZED: Updated batch size and timeout for better performance
        Self::new(repo, pools, 250, Duration::from_millis(25))
    }
}

/// Implementation of the existing OutboxRepositoryTrait for CDC
/// This allows the CDC outbox to be used as a drop-in replacement
#[async_trait]
impl crate::infrastructure::outbox::OutboxRepositoryTrait for CDCOutboxRepository {
    async fn add_pending_messages(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        // OPTIMIZED: Use single bulk insert instead of chunking for small batches
        if messages.len() <= 100 {
            // Single bulk insert for small batches
            let mut query = String::from(
                "INSERT INTO kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at) VALUES "
            );

            let mut values = Vec::new();
            let mut params: Vec<(
                Uuid,
                Uuid,
                String,
                Vec<u8>,
                String,
                Option<serde_json::Value>,
            )> = Vec::new();
            let mut param_index = 1;

            for msg in &messages {
                values.push(format!(
                    "(${},${},${},${},${},${},NOW(),NOW())",
                    param_index,
                    param_index + 1,
                    param_index + 2,
                    param_index + 3,
                    param_index + 4,
                    param_index + 5
                ));

                params.push((
                    msg.aggregate_id,
                    msg.event_id,
                    msg.event_type.clone(),
                    msg.payload.clone(),
                    msg.topic.clone(),
                    msg.metadata.clone(),
                ));

                param_index += 6;
            }

            query.push_str(&values.join(","));

            let mut query = sqlx::query(&query);
            for (aggregate_id, event_id, event_type, payload, topic, metadata) in params {
                query = query
                    .bind(aggregate_id)
                    .bind(event_id)
                    .bind(event_type)
                    .bind(payload)
                    .bind(topic)
                    .bind(metadata);
            }

            let result = query.execute(&mut **tx).await;
            if let Err(e) = &result {
                tracing::error!(
                    "CDCOutboxRepository: Failed to bulk insert OutboxMessages: {:?}",
                    e
                );
                return Err(anyhow::anyhow!("Bulk insert failed: {:?}", e));
            }
        } else {
            // Chunked insert for large batches
            let chunk_size = 1000;
            for chunk in messages.chunks(chunk_size) {
                let mut query = String::from(
                    "INSERT INTO kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at) VALUES "
                );

                let mut values = Vec::new();
                let mut params: Vec<(
                    Uuid,
                    Uuid,
                    String,
                    Vec<u8>,
                    String,
                    Option<serde_json::Value>,
                )> = Vec::new();
                let mut param_index = 1;

                for msg in chunk {
                    values.push(format!(
                        "(${},${},${},${},${},${},NOW(),NOW())",
                        param_index,
                        param_index + 1,
                        param_index + 2,
                        param_index + 3,
                        param_index + 4,
                        param_index + 5
                    ));

                    params.push((
                        msg.aggregate_id,
                        msg.event_id,
                        msg.event_type.clone(),
                        msg.payload.clone(),
                        msg.topic.clone(),
                        msg.metadata.clone(),
                    ));

                    param_index += 6;
                }

                query.push_str(&values.join(","));

                let mut query = sqlx::query(&query);
                for (aggregate_id, event_id, event_type, payload, topic, metadata) in params {
                    query = query
                        .bind(aggregate_id)
                        .bind(event_id)
                        .bind(event_type)
                        .bind(payload)
                        .bind(topic)
                        .bind(metadata);
                }

                let result = query.execute(&mut **tx).await;
                if let Err(e) = &result {
                    tracing::error!(
                        "CDCOutboxRepository: Failed to batch insert OutboxMessages: {:?}",
                        e
                    );
                    return Err(anyhow::anyhow!("Batch insert failed: {:?}", e));
                }
            }
        }

        let duration = start_time.elapsed();
        tracing::debug!(
            "CDCOutboxRepository: Inserted {} messages in {:?}",
            messages.len(),
            duration
        );

        Ok(())
    }

    async fn fetch_and_lock_pending_messages(
        &self,
        _limit: i64,
    ) -> Result<Vec<crate::infrastructure::outbox::PersistedOutboxMessage>> {
        // CDC doesn't need polling - messages are automatically captured by Debezium
        // This method is kept for interface compatibility but returns empty
        Ok(Vec::new())
    }

    async fn mark_as_processed(&self, _outbox_message_id: Uuid) -> Result<()> {
        // CDC doesn't need status tracking - messages are automatically processed
        // This method is kept for interface compatibility
        Ok(())
    }

    async fn delete_processed_batch(&self, _outbox_message_ids: &[Uuid]) -> Result<usize> {
        // CDC doesn't need batch deletion - messages are automatically cleaned up
        // This method is kept for interface compatibility
        Ok(0)
    }

    async fn record_failed_attempt(
        &self,
        _outbox_message_id: Uuid,
        _max_retries: i32,
        _error_message: Option<String>,
    ) -> Result<()> {
        // CDC doesn't need retry tracking - failures are handled by the CDC consumer
        // This method is kept for interface compatibility
        Ok(())
    }

    async fn mark_as_failed(
        &self,
        _outbox_message_id: Uuid,
        _error_message: Option<String>,
    ) -> Result<()> {
        // CDC doesn't need failure marking - failures are handled by the CDC consumer
        // This method is kept for interface compatibility
        Ok(())
    }

    async fn find_stuck_processing_messages(
        &self,
        _stuck_threshold: Duration,
        _limit: i32,
    ) -> Result<Vec<crate::infrastructure::outbox::PersistedOutboxMessage>> {
        // CDC doesn't have stuck messages - all processing is real-time
        // This method is kept for interface compatibility
        Ok(Vec::new())
    }

    async fn reset_stuck_messages(&self, _outbox_message_ids: &[Uuid]) -> Result<usize> {
        // CDC doesn't need stuck message reset - all processing is real-time
        // This method is kept for interface compatibility
        Ok(0)
    }
}

/// Enhanced CDC Event Processor - now uses the optimized processor
pub struct EnhancedCDCEventProcessor {
    // Use the optimized event processor
    optimized_processor: UltraOptimizedCDCEventProcessor,
    // Keep the old metrics for backward compatibility
    metrics: Arc<EnhancedCDCMetrics>,
}

impl EnhancedCDCEventProcessor {
    pub fn new(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        business_config: Option<BusinessLogicConfig>,
        consistency_manager: Option<
            Arc<crate::infrastructure::consistency_manager::ConsistencyManager>,
        >,
    ) -> Self {
        let metrics = Arc::new(EnhancedCDCMetrics::default());
        let optimized_processor = UltraOptimizedCDCEventProcessor::new(
            kafka_producer,
            cache_service,
            projection_store,
            metrics.clone(),
            business_config,
            None, // Use default performance config
            consistency_manager,
        );

        Self {
            optimized_processor,
            metrics,
        }
    }

    /// Process CDC event from Debezium using the optimized processor
    pub async fn process_cdc_event(&self, cdc_event: serde_json::Value) -> Result<()> {
        let start_time = std::time::Instant::now();
        tracing::info!(
            "🔍 CDC Event Processor: Starting to process CDC event with optimized processor"
        );
        tracing::info!("🔍 CDC Event Processor: Event details: {:?}", cdc_event);

        // Use the optimized processor
        match self
            .optimized_processor
            .process_cdc_event_ultra_fast(cdc_event)
            .await
        {
            Ok(_) => {
                // Update legacy metrics for backward compatibility
                let latency = start_time.elapsed().as_millis() as u64;
                self.metrics
                    .processing_latency_ms
                    .fetch_add(latency, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .events_processed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::info!(
                    "✅ CDC Event Processor: Event processed successfully with optimized processor (latency: {}ms)",
                    latency
                );
                Ok(())
            }
            Err(e) => {
                self.metrics
                    .events_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!("CDC Event Processor: Failed to process event: {}", e);
                tracing::error!("CDC Event Processor: Failed to process event: {}", e);
                Err(e)
            }
        }
    }

    /// Mark an aggregate as completed by CDC processing
    pub async fn mark_aggregate_completed(&self, aggregate_id: Uuid) {
        // This method can be called by external consistency managers
        // when CDC events are successfully processed
        info!(
            "CDC Event Processor: Marked aggregate {} as completed",
            aggregate_id
        );

        // Use the optimized processor to mark as completed
        self.optimized_processor
            .mark_aggregate_completed(aggregate_id)
            .await;
    }

    /// Mark an aggregate as failed by CDC processing
    pub async fn mark_aggregate_failed(&self, aggregate_id: Uuid, error: String) {
        // This method can be called by external consistency managers
        // when CDC events fail to process
        error!(
            "CDC Event Processor: Marked aggregate {} as failed: {}",
            aggregate_id, error
        );

        // Use the optimized processor to mark as failed
        self.optimized_processor
            .mark_aggregate_failed(aggregate_id, error.clone())
            .await;
    }

    /// Get metrics from the optimized processor
    pub async fn get_optimized_metrics(
        &self,
    ) -> crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics {
        self.optimized_processor.get_metrics().await
    }

    /// Get legacy metrics for backward compatibility
    pub fn get_metrics(&self) -> &EnhancedCDCMetrics {
        &self.metrics
    }

    /// Get business logic configuration
    pub async fn get_business_config(&self) -> BusinessLogicConfig {
        self.optimized_processor.get_business_config().await
    }

    /// Update business logic configuration
    pub async fn update_business_config(&mut self, config: BusinessLogicConfig) {
        self.optimized_processor
            .update_business_config(config)
            .await
    }

    /// Start batch processor
    pub async fn start_batch_processor(&mut self) -> Result<()> {
        self.optimized_processor.start_batch_processor().await
    }

    /// Enable and start batch processor
    pub async fn enable_and_start_batch_processor(&mut self) -> Result<()> {
        self.optimized_processor
            .enable_and_start_batch_processor()
            .await
    }

    /// Check if batch processor is running
    pub async fn is_batch_processor_running(&self) -> bool {
        self.optimized_processor.is_batch_processor_running().await
    }

    /// Shutdown the processor
    pub async fn shutdown(&mut self) -> Result<()> {
        self.optimized_processor.shutdown().await
    }
}

#[async_trait]
impl EventProcessor for EnhancedCDCEventProcessor {
    async fn process_event(&self, event: serde_json::Value) -> Result<()> {
        self.process_cdc_event(event).await
    }
}

/// CDC Consumer - consumes CDC events from Kafka Connect
pub struct CDCConsumer {
    kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
    cdc_topic: String,
}

impl CDCConsumer {
    pub fn new(
        kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
        cdc_topic: String,
    ) -> Self {
        Self {
            kafka_consumer,
            cdc_topic,
        }
    }

    /// Start consuming CDC events
    pub async fn start_consuming(
        &mut self,
        processor: Arc<EnhancedCDCEventProcessor>,
    ) -> Result<()> {
        Err(anyhow::anyhow!(
            "Use start_consuming_with_cancellation_token instead"
        ))
    }

    /// Start consuming CDC events with unified cancellation token
    pub async fn start_consuming_with_cancellation_token(
        &mut self,
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        // Static counter for active polling loops
        static ACTIVE_CONSUMERS: AtomicUsize = AtomicUsize::new(0);
        let current = ACTIVE_CONSUMERS.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(
            "CDCConsumer: Entering main polling loop. Active consumers: {}",
            current
        );
        tracing::info!(
            "CDCConsumer::start_consuming_with_cancellation_token() called for topic: {}",
            self.cdc_topic
        );
        info!("Starting CDC consumer for topic: {}", self.cdc_topic);
        tracing::info!(
            "CDCConsumer: Entered start_consuming async loop for topic: {}",
            self.cdc_topic
        );

        // Validate Kafka consumer is properly configured
        let kafka_config = self.kafka_consumer.get_config();
        if !kafka_config.enabled {
            tracing::error!("CDCConsumer: Kafka consumer is disabled");
            return Err(anyhow::anyhow!("Kafka consumer is disabled"));
        }

        tracing::info!(
            "CDCConsumer: Kafka consumer config - enabled: {}, group_id: {}, bootstrap_servers: {}",
            kafka_config.enabled,
            kafka_config.group_id,
            kafka_config.bootstrap_servers
        );

        // Subscribe to CDC topic with retry logic
        tracing::info!("CDCConsumer: Subscribing to topic: {}", self.cdc_topic);
        let max_subscription_retries = 5; // Increased retries
        let mut subscription_retries = 0;

        loop {
            let subscribe_result = self
                .kafka_consumer
                .subscribe_to_topic(&self.cdc_topic)
                .await;

            match &subscribe_result {
                Ok(_) => {
                    tracing::info!(
                        "CDCConsumer: ✅ Successfully subscribed to topic: {}",
                        self.cdc_topic
                    );

                    // Wait a moment for the consumer to join the group
                    tracing::info!("CDCConsumer: Waiting for consumer to join group...");
                    tokio::time::sleep(Duration::from_secs(5)).await; // Increased wait time

                    // Log consumer group status
                    tracing::info!("CDCConsumer: Consumer group join completed");
                    break;
                }
                Err(e) => {
                    subscription_retries += 1;
                    tracing::error!(
                        "CDCConsumer: ❌ Failed to subscribe to topic: {} (attempt {}/{}): {}",
                        self.cdc_topic,
                        subscription_retries,
                        max_subscription_retries,
                        e
                    );

                    if subscription_retries >= max_subscription_retries {
                        tracing::error!(
                            "CDCConsumer: Failed to subscribe to CDC topic after {} attempts: {}",
                            max_subscription_retries,
                            e
                        );
                        return Err(anyhow::anyhow!(
                            "Failed to subscribe to CDC topic after {} attempts: {}",
                            max_subscription_retries,
                            e
                        ));
                    }

                    // Wait before retry
                    tokio::time::sleep(Duration::from_secs(5)).await; // Increased wait time
                }
            }
        }

        tracing::info!(
            "CDCConsumer: Starting main consumption loop for topic: {}",
            self.cdc_topic
        );
        let mut poll_count = 0;
        let mut last_log_time = std::time::Instant::now();
        let mut consecutive_empty_polls = 0;
        let max_consecutive_empty_polls = 100; // Log warning after 100 empty polls
        let max_concurrent = 32; // Tune as needed
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let offsets = Arc::new(Mutex::new(Vec::<(String, i32, i64)>::new()));
        let offsets_clone = offsets.clone();
        let (dlq_tx, mut dlq_rx) =
            mpsc::channel::<(String, i32, i64, Vec<u8>, Option<Vec<u8>>, String)>(1000);
        let kafka_consumer = self.kafka_consumer.clone();
        // Background offset committer
        tokio::spawn(async move {
            let mut last_commit_time = std::time::Instant::now();
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let mut offsets = offsets_clone.lock().await;
                let should_commit = offsets.len() >= 100 || // Commit every 100 messages
                                   last_commit_time.elapsed() > std::time::Duration::from_secs(2); // Or every 2 seconds

                if should_commit && !offsets.is_empty() {
                    // Only commit the highest offset per (topic, partition)
                    let mut highest: HashMap<(String, i32), i64> = HashMap::new();
                    for (topic, partition, offset) in offsets.drain(..) {
                        let key = (topic.clone(), partition);
                        highest
                            .entry(key)
                            .and_modify(|v| *v = (*v).max(offset))
                            .or_insert(offset);
                    }
                    let offset_count = highest.len(); // Store length before moving
                    let mut tpl = TopicPartitionList::new();
                    for ((topic, partition), offset) in highest {
                        tpl.add_partition_offset(&topic, partition, Offset::Offset(offset + 1))
                            .unwrap();
                    }
                    if let Err(e) = kafka_consumer.commit(&tpl, CommitMode::Async) {
                        tracing::error!("Failed to batch commit offsets: {}", e);
                    } else {
                        tracing::info!("✅ Successfully committed {} offsets", offset_count);
                        last_commit_time = std::time::Instant::now();
                    }
                }
            }
        });
        // Background DLQ handler (batch)
        let dlq_producer = processor.clone();
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(100);
            let mut last_send = std::time::Instant::now();
            loop {
                tokio::select! {
                    Some((topic, partition, offset, payload, key, error)) = dlq_rx.recv() => {
                        batch.push((topic, partition, offset, payload, key, error));
                        if batch.len() >= 100 || last_send.elapsed() > std::time::Duration::from_millis(100) {
                            let to_send = std::mem::take(&mut batch);
                            for (topic, partition, offset, payload, key, error) in to_send {
                                if let Err(e) = dlq_producer.send_to_dlq_from_cdc_parts(&topic, partition, offset, &payload, key.as_deref(), &error).await {
                                    tracing::error!("Failed to send to DLQ: {}", e);
                                }
                            }
                            last_send = std::time::Instant::now();
                        }
                    }
                    else => {
                        if !batch.is_empty() {
                            let to_send = std::mem::take(&mut batch);
                            for (topic, partition, offset, payload, key, error) in to_send {
                                if let Err(e) = dlq_producer.send_to_dlq_from_cdc_parts(&topic, partition, offset, &payload, key.as_deref(), &error).await {
                                    tracing::error!("Failed to send to DLQ: {}", e);
                                }
                            }
                        }
                        break;
                    }
                }
            }
        });

        loop {
            poll_count += 1;

            // Log every 10 polls initially, then every 100
            if poll_count <= 10
                || poll_count % 100 == 0
                || last_log_time.elapsed() > Duration::from_secs(10)
            {
                tracing::info!(
                    "CDCConsumer: Poll attempt #{} for topic: {}",
                    poll_count,
                    self.cdc_topic
                );
                last_log_time = std::time::Instant::now();
            }

            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("CDC consumer received shutdown signal");
                    tracing::info!("CDCConsumer: Received shutdown signal, breaking loop");
                    break;
                }
                message_result = tokio::time::timeout(Duration::from_millis(100), self.kafka_consumer.recv()) => {
                    match message_result {
                        Ok(Ok(Ok(message))) => {
                            consecutive_empty_polls = 0; // Reset counter on successful message
                            tracing::info!("[CDCConsumer] Received message on poll #{}: {:?}", poll_count, message);

                            let cdc_event: serde_json::Value = match message.payload_view::<[u8]>() {
                                Some(Ok(payload)) => match serde_json::from_slice(payload) {
                                    Ok(event) => event,
                                    Err(e) => {
                                        tracing::error!("Failed to deserialize CDC event payload: {}", e);
                                        continue;
                                    }
                                },
                                Some(Err(e)) => {
                                    tracing::error!("Error viewing message payload: {:?}", e);
                                    continue;
                                }
                                None => {
                                    tracing::warn!("Received message with no payload");
                                    continue;
                                }
                            };

                            tracing::info!("CDCConsumer: 📊 Message details - Topic: {:?}, Partition: {:?}, Offset: {:?}",
                                message.topic(), message.partition(), message.offset());
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let processor = processor.clone();
                            let offsets = offsets.clone();
                            let dlq_tx = dlq_tx.clone();
                            // For DLQ and offset batching, extract info from message
                            let topic = message.topic().to_string();
                            let partition = message.partition();
                            let offset = message.offset();
                            let payload = message.payload().map(|p| p.to_vec()).unwrap_or_default();
                            let key = message.key().map(|k| k.to_vec());
                            tokio::spawn(async move {
                                let _permit = permit;
                            match processor.process_cdc_event_ultra_fast(cdc_event).await {
                                Ok(_) => {
                                        // Push offset for batch commit
                                        offsets.lock().await.push((topic.clone(), partition, offset));
                                }
                                Err(e) => {
                                        tracing::error!("Failed to process CDC event: {}", e);
                                        // Send to DLQ in parallel
                                        let _ = dlq_tx.send((topic, partition, offset, payload, key, e.to_string())).await;
                                    }
                                }
                            });
                        }
                        Ok(Ok(Err(e))) => {
                            tracing::error!("CDCConsumer: ❌ Error polling CDC message on poll #{}: {}", poll_count, e);
                            // Add delay on error to avoid tight error loops, but check for shutdown signal
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                                    // Continue after delay
                                }
                                _ = shutdown_token.cancelled() => {
                                    info!("CDC consumer received shutdown signal during error handling");
                                    tracing::info!("CDCConsumer: Received shutdown signal during error handling, breaking loop");
                                    break;
                                }
                            }
                        }
                        Ok(Err(_)) => {
                            // Timeout
                            consecutive_empty_polls += 1;
                            if poll_count <= 10 || poll_count % 50 == 0 { // Log every 50th empty poll to avoid spam
                                tracing::debug!(
                                    "CDCConsumer: ⏳ No CDC event available on poll #{} for topic: {} (consecutive empty: {})",
                                    poll_count, self.cdc_topic, consecutive_empty_polls
                                );
                            }

                            // Log warning if too many consecutive empty polls
                            if consecutive_empty_polls >= max_consecutive_empty_polls {
                                tracing::warn!(
                                    "CDCConsumer: ⚠️ No messages received for {} consecutive polls. Check if Debezium is producing messages to topic: {}",
                                    consecutive_empty_polls,
                                    self.cdc_topic
                                );
                                consecutive_empty_polls = 0; // Reset to avoid spam
                            }
                        }
                        Err(e) => {
                             tracing::error!("CDCConsumer: ❌ Error receiving from consumer: {}", e);
                        }
                    }
                }
            }
        }

        info!("CDC consumer stopped after {} polls", poll_count);
        tracing::info!(
            "CDCConsumer: Exiting start_consuming after {} polls",
            poll_count
        );
        let current = ACTIVE_CONSUMERS.fetch_sub(1, Ordering::SeqCst) - 1;
        tracing::info!(
            "CDCConsumer: Exiting main polling loop. Active consumers: {}",
            current
        );
        Ok(())
    }

    async fn process_cdc_message(
        &self,
        message: crate::infrastructure::kafka_abstraction::KafkaMessage,
        processor: &Arc<tokio::sync::Mutex<EnhancedCDCEventProcessor>>,
    ) -> Result<()> {
        // Parse CDC event from Kafka message
        let cdc_event: serde_json::Value = serde_json::from_slice(&message.payload)?;

        // Process the CDC event
        processor.lock().await.process_cdc_event(cdc_event).await?;

        Ok(())
    }
}

/// Health check for CDC service
pub struct CDCHealthCheck {
    metrics: Arc<EnhancedCDCMetrics>,
    optimized_metrics: Option<crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics>,
    producer_health: Option<crate::infrastructure::cdc_producer::HealthStatus>,
}

impl CDCHealthCheck {
    pub fn new(metrics: Arc<EnhancedCDCMetrics>) -> Self {
        Self {
            metrics,
            optimized_metrics: None,
            producer_health: None,
        }
    }

    pub fn with_optimized_metrics(
        mut self,
        optimized_metrics: crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics,
    ) -> Self {
        self.optimized_metrics = Some(optimized_metrics);
        self
    }

    pub fn with_producer_health(
        mut self,
        producer_health: crate::infrastructure::cdc_producer::HealthStatus,
    ) -> Self {
        self.producer_health = Some(producer_health);
        self
    }

    pub fn is_healthy(&self) -> bool {
        // Check if CDC service is processing events
        let processed = self
            .metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = self
            .metrics
            .events_failed
            .load(std::sync::atomic::Ordering::Relaxed);

        // Consider healthy if processing events and failure rate is low
        processed > 0 && (failed as f64 / processed as f64) < 0.1
    }

    pub fn get_health_status(&self) -> serde_json::Value {
        let processed = self
            .metrics
            .events_processed
            .load(std::sync::atomic::Ordering::Relaxed);
        let avg_latency = if processed > 0 {
            self.metrics
                .processing_latency_ms
                .load(std::sync::atomic::Ordering::Relaxed)
                / processed
        } else {
            0
        };

        let mut status = serde_json::json!({
            "healthy": self.is_healthy(),
            "events_processed": processed,
            "events_failed": self.metrics.events_failed.load(std::sync::atomic::Ordering::Relaxed),
            "avg_processing_latency_ms": avg_latency,
            "cache_invalidations": self.metrics.cache_invalidations.load(std::sync::atomic::Ordering::Relaxed),
            "projection_updates": self.metrics.projection_updates.load(std::sync::atomic::Ordering::Relaxed)
        });

        // Add optimized metrics if available
        if let Some(ref opt_metrics) = self.optimized_metrics {
            status["optimized_metrics"] = serde_json::json!({
                "events_failed": opt_metrics.events_failed.load(std::sync::atomic::Ordering::Relaxed),
                "events_processed": opt_metrics.events_processed.load(std::sync::atomic::Ordering::Relaxed),
                "processing_latency_ms": opt_metrics.processing_latency_ms.load(std::sync::atomic::Ordering::Relaxed),
                "total_latency_ms": opt_metrics.total_latency_ms.load(std::sync::atomic::Ordering::Relaxed),
                "cache_invalidations": opt_metrics.cache_invalidations.load(std::sync::atomic::Ordering::Relaxed),
                "projection_updates": opt_metrics.projection_updates.load(std::sync::atomic::Ordering::Relaxed),
                "batches_processed": opt_metrics.batches_processed.load(std::sync::atomic::Ordering::Relaxed),
                "circuit_breaker_trips": opt_metrics.circuit_breaker_trips.load(std::sync::atomic::Ordering::Relaxed),
                "consumer_restarts": opt_metrics.consumer_restarts.load(std::sync::atomic::Ordering::Relaxed),
                "cleanup_cycles": opt_metrics.cleanup_cycles.load(std::sync::atomic::Ordering::Relaxed),
                "memory_usage_bytes": opt_metrics.memory_usage_bytes.load(std::sync::atomic::Ordering::Relaxed),
                "active_connections": opt_metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed),
                "queue_depth": opt_metrics.queue_depth.load(std::sync::atomic::Ordering::Relaxed),
                "avg_batch_size": opt_metrics.avg_batch_size.load(std::sync::atomic::Ordering::Relaxed),
                "p95_processing_latency_ms": opt_metrics.p95_processing_latency_ms.load(std::sync::atomic::Ordering::Relaxed),
                "p99_processing_latency_ms": opt_metrics.p99_processing_latency_ms.load(std::sync::atomic::Ordering::Relaxed),
                "throughput_per_second": opt_metrics.throughput_per_second.load(std::sync::atomic::Ordering::Relaxed),
                "error_rate": opt_metrics.error_rate.load(std::sync::atomic::Ordering::Relaxed),
                "consecutive_failures": opt_metrics.consecutive_failures.load(std::sync::atomic::Ordering::Relaxed),
                "last_error_time": opt_metrics.last_error_time.load(std::sync::atomic::Ordering::Relaxed),
                "integration_helper_initialized": opt_metrics.integration_helper_initialized.load(std::sync::atomic::Ordering::Relaxed),
            });
        }

        // Add producer health if available
        if let Some(ref prod_health) = self.producer_health {
            status["producer_health"] =
                serde_json::to_value(prod_health).unwrap_or(serde_json::Value::Null);
        }

        status
    }
}
