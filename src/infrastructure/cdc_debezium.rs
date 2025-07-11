use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

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
            connector_name: "banking-outbox-connector".to_string(),
            database_host: "localhost".to_string(),
            database_port: 5432,
            database_name: "banking_es".to_string(),
            database_user: "postgres".to_string(),
            database_password: "password".to_string(),
            table_include_list: "banking_es.kafka_outbox_cdc".to_string(), // Updated to use CDC table
            topic_prefix: "banking_es.cdc.".to_string(),
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
    pool: sqlx::PgPool,
}

impl CDCOutboxRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Create optimized outbox table for CDC
    pub async fn create_cdc_outbox_table(&self) -> Result<()> {
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
            );
            
            -- Optimized indexes for CDC
            CREATE INDEX IF NOT EXISTS idx_outbox_cdc_created_at ON kafka_outbox_cdc(created_at);
            CREATE INDEX IF NOT EXISTS idx_outbox_cdc_aggregate_id ON kafka_outbox_cdc(aggregate_id);
            CREATE INDEX IF NOT EXISTS idx_outbox_cdc_event_id ON kafka_outbox_cdc(event_id);
            
            -- Enable logical replication
            ALTER TABLE kafka_outbox_cdc REPLICA IDENTITY FULL;
            "#,
        )
        .execute(&self.pool)
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
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as usize)
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

        for msg in messages {
            sqlx::query!(
                r#"
                INSERT INTO kafka_outbox_cdc
                    (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at)
                VALUES
                    ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                "#,
                msg.aggregate_id,
                msg.event_id,
                msg.event_type,
                msg.payload,
                msg.topic,
                msg.metadata
            )
            .execute(&mut **tx)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to insert CDC outbox message: {}", e))?;
        }
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

/// CDC Event Processor - processes Debezium CDC events
pub struct CDCEventProcessor {
    kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
    metrics: Arc<CDCMetrics>,
}

#[derive(Debug, Default)]
pub struct CDCMetrics {
    pub events_processed: std::sync::atomic::AtomicU64,
    pub events_failed: std::sync::atomic::AtomicU64,
    pub processing_latency_ms: std::sync::atomic::AtomicU64,
    pub total_latency_ms: std::sync::atomic::AtomicU64,
}

impl CDCEventProcessor {
    pub fn new(kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer) -> Self {
        Self {
            kafka_producer,
            metrics: Arc::new(CDCMetrics::default()),
        }
    }

    /// Process CDC event from Debezium
    pub async fn process_cdc_event(&self, cdc_event: serde_json::Value) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Extract the actual outbox message from CDC event
        let outbox_message = self.extract_outbox_message(cdc_event)?;

        // Deserialize the domain event
        let domain_event: crate::domain::AccountEvent =
            bincode::deserialize(&outbox_message.payload)?;

        // Create event batch
        let event_batch = crate::infrastructure::kafka_abstraction::EventBatch {
            account_id: outbox_message.aggregate_id,
            events: vec![domain_event],
            version: 0,
            timestamp: outbox_message.created_at,
        };

        // Serialize and publish to Kafka
        let batch_payload = bincode::serialize(&event_batch)?;

        self.kafka_producer
            .publish_binary_event(
                &outbox_message.topic,
                &batch_payload,
                &outbox_message.event_id.to_string(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to publish CDC event: {}", e))?;

        // Update metrics
        let latency = start_time.elapsed().as_millis() as u64;
        self.metrics
            .processing_latency_ms
            .fetch_add(latency, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .events_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            "CDC event processed: {} -> {} (latency: {}ms)",
            outbox_message.event_id,
            outbox_message.topic,
            latency
        );

        Ok(())
    }

    fn extract_outbox_message(&self, cdc_event: serde_json::Value) -> Result<CDCOutboxMessage> {
        // Extract the "after" field from Debezium CDC event
        let after = cdc_event
            .get("after")
            .ok_or_else(|| anyhow::anyhow!("Missing 'after' field in CDC event"))?;

        let outbox_message: CDCOutboxMessage = serde_json::from_value(after.clone())?;
        Ok(outbox_message)
    }

    pub fn get_metrics(&self) -> &CDCMetrics {
        &self.metrics
    }
}

/// CDC Consumer - consumes CDC events from Kafka Connect
pub struct CDCConsumer {
    kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
    cdc_topic: String,
    shutdown_rx: mpsc::Receiver<()>,
}

impl CDCConsumer {
    pub fn new(
        kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
        cdc_topic: String,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            kafka_consumer,
            cdc_topic,
            shutdown_rx,
        }
    }

    /// Start consuming CDC events
    pub async fn start_consuming(&mut self, _processor: Arc<CDCEventProcessor>) -> Result<()> {
        info!("Starting CDC consumer for topic: {}", self.cdc_topic);

        // Subscribe to CDC topic - using existing method
        self.kafka_consumer
            .subscribe_to_events()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to subscribe to CDC topic: {}", e))?;

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("CDC consumer received shutdown signal");
                    break;
                }
                message_result = self.kafka_consumer.poll_events() => {
                    match message_result {
                        Ok(Some(_event_batch)) => {
                            // For now, we'll process events using the existing poll_events method
                            // In a real implementation, you'd want a dedicated CDC consumer
                            info!("Received CDC event batch");
                        }
                        Ok(None) => {
                            // No message available, continue
                        }
                        Err(e) => {
                            error!("Error polling CDC message: {}", e);
                        }
                    }
                }
            }
        }

        info!("CDC consumer stopped");
        Ok(())
    }

    async fn process_cdc_message(
        &self,
        message: crate::infrastructure::kafka_abstraction::KafkaMessage,
        processor: &Arc<CDCEventProcessor>,
    ) -> Result<()> {
        // Parse CDC event from Kafka message
        let cdc_event: serde_json::Value = serde_json::from_slice(&message.payload)?;

        // Process the CDC event
        processor.process_cdc_event(cdc_event).await?;

        Ok(())
    }
}

/// CDC Service Manager - manages the entire CDC pipeline
pub struct CDCServiceManager {
    config: DebeziumConfig,
    outbox_repo: Arc<CDCOutboxRepository>,
    cdc_consumer: Option<CDCConsumer>,
    processor: Arc<CDCEventProcessor>,
    shutdown_tx: mpsc::Sender<()>,
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl CDCServiceManager {
    pub fn new(
        config: DebeziumConfig,
        outbox_repo: Arc<CDCOutboxRepository>,
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
    ) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let processor = Arc::new(CDCEventProcessor::new(kafka_producer));
        let cdc_topic = format!("{}{}", config.topic_prefix, "kafka_outbox_cdc");

        let cdc_consumer = CDCConsumer::new(kafka_consumer, cdc_topic, shutdown_rx);

        Ok(Self {
            config,
            outbox_repo,
            cdc_consumer: Some(cdc_consumer),
            processor,
            shutdown_tx,
            cleanup_handle: None,
        })
    }

    /// Start the CDC service
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting CDC Service Manager");

        // Create CDC table if it doesn't exist
        self.outbox_repo.create_cdc_outbox_table().await?;

        // Start CDC consumer
        if let Some(mut consumer) = self.cdc_consumer.take() {
            let processor = self.processor.clone();
            tokio::spawn(async move {
                if let Err(e) = consumer.start_consuming(processor).await {
                    error!("CDC consumer failed: {}", e);
                }
            });
        }

        // Start cleanup task
        let outbox_repo = self.outbox_repo.clone();
        let cleanup_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Run every hour
            loop {
                interval.tick().await;
                if let Err(e) = outbox_repo
                    .cleanup_old_messages(Duration::from_secs(86400 * 7))
                    .await
                {
                    // Clean up messages older than 7 days
                    error!("Failed to cleanup old CDC messages: {}", e);
                }
            }
        });
        self.cleanup_handle = Some(cleanup_handle);

        info!("CDC Service Manager started successfully");
        Ok(())
    }

    /// Stop the CDC service
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping CDC Service Manager");

        // Send shutdown signal
        if let Err(e) = self.shutdown_tx.send(()).await {
            warn!("Failed to send shutdown signal to CDC consumer: {}", e);
        }

        // Cancel cleanup task
        if let Some(handle) = &self.cleanup_handle {
            handle.abort();
        }

        info!("CDC Service Manager stopped");
        Ok(())
    }

    /// Get CDC metrics
    pub fn get_metrics(&self) -> &CDCMetrics {
        self.processor.get_metrics()
    }

    /// Generate Debezium connector configuration
    pub fn get_debezium_config(&self) -> serde_json::Value {
        self.outbox_repo.generate_debezium_config(&self.config)
    }
}

/// Extension trait to allow downcasting
pub trait AsAny {
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T: 'static> AsAny for T {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Integration helper for existing outbox system
pub struct CDCIntegrationHelper;

impl CDCIntegrationHelper {
    /// Convert existing outbox message to CDC format
    pub fn convert_to_cdc_message(
        outbox_msg: &crate::infrastructure::outbox::OutboxMessage,
    ) -> CDCOutboxMessage {
        CDCOutboxMessage {
            id: uuid::Uuid::new_v4(), // Generate new ID for CDC table
            aggregate_id: outbox_msg.aggregate_id,
            event_id: outbox_msg.event_id,
            event_type: outbox_msg.event_type.clone(),
            payload: outbox_msg.payload.clone(),
            topic: outbox_msg.topic.clone(),
            metadata: outbox_msg.metadata.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Migrate existing outbox to CDC format
    pub async fn migrate_existing_outbox(
        old_repo: &crate::infrastructure::outbox::PostgresOutboxRepository,
        new_repo: &CDCOutboxRepository,
    ) -> Result<usize> {
        // This would implement migration logic from old outbox to CDC outbox
        // For now, return 0 as placeholder
        Ok(0)
    }
}

/// Health check for CDC service
pub struct CDCHealthCheck {
    metrics: Arc<CDCMetrics>,
}

impl CDCHealthCheck {
    pub fn new(metrics: Arc<CDCMetrics>) -> Self {
        Self { metrics }
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

        serde_json::json!({
            "healthy": self.is_healthy(),
            "events_processed": processed,
            "events_failed": self.metrics.events_failed.load(std::sync::atomic::Ordering::Relaxed),
            "avg_processing_latency_ms": avg_latency
        })
    }
}
