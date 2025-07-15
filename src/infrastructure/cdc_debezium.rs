use crate::infrastructure::cache_service::CacheServiceTrait;
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
use crate::infrastructure::cdc_service_manager::{
    CDCServiceManager as OptimizedCDCServiceManager, EnhancedCDCMetrics, OptimizationConfig,
    ServiceState,
};
use crate::infrastructure::event_processor::EventProcessor;

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
    pool: sqlx::PgPool,
}

impl CDCOutboxRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Create optimized outbox table for CDC
    pub async fn create_cdc_outbox_table(&self) -> Result<()> {
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
        .execute(&self.pool)
        .await?;

        // Create indexes separately
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_cdc_created_at ON kafka_outbox_cdc(created_at)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_outbox_cdc_aggregate_id ON kafka_outbox_cdc(aggregate_id)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_cdc_event_id ON kafka_outbox_cdc(event_id)",
        )
        .execute(&self.pool)
        .await?;

        // Enable logical replication
        sqlx::query("ALTER TABLE kafka_outbox_cdc REPLICA IDENTITY FULL")
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

/// Enhanced CDC Event Processor - now uses the optimized processor
pub struct CDCEventProcessor {
    // Use the optimized event processor
    optimized_processor: UltraOptimizedCDCEventProcessor,
    // Keep the old metrics for backward compatibility
    metrics: Arc<CDCMetrics>,
}

#[derive(Debug, Default)]
pub struct CDCMetrics {
    pub events_processed: std::sync::atomic::AtomicU64,
    pub events_failed: std::sync::atomic::AtomicU64,
    pub processing_latency_ms: std::sync::atomic::AtomicU64,
    pub total_latency_ms: std::sync::atomic::AtomicU64,
    pub cache_invalidations: std::sync::atomic::AtomicU64,
    pub projection_updates: std::sync::atomic::AtomicU64,
}

impl CDCEventProcessor {
    pub fn new(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        business_config: Option<BusinessLogicConfig>,
    ) -> Self {
        let optimized_processor = UltraOptimizedCDCEventProcessor::new(
            kafka_producer,
            cache_service,
            projection_store,
            business_config,
        );

        Self {
            optimized_processor,
            metrics: Arc::new(CDCMetrics::default()),
        }
    }

    /// Process CDC event from Debezium using the optimized processor
    pub async fn process_cdc_event(&self, cdc_event: serde_json::Value) -> Result<()> {
        let start_time = std::time::Instant::now();
        tracing::info!(
            "🔍 CDC Event Processor: Starting to process CDC event with optimized processor"
        );

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
                Err(e)
            }
        }
    }

    /// Get metrics from the optimized processor
    pub async fn get_optimized_metrics(
        &self,
    ) -> crate::infrastructure::cdc_event_processor::OptimizedCDCMetrics {
        self.optimized_processor.get_metrics().await
    }

    /// Get legacy metrics for backward compatibility
    pub fn get_metrics(&self) -> &CDCMetrics {
        &self.metrics
    }

    /// Get business logic configuration
    pub fn get_business_config(&self) -> &BusinessLogicConfig {
        self.optimized_processor.get_business_config()
    }

    /// Update business logic configuration
    pub fn update_business_config(&mut self, config: BusinessLogicConfig) {
        self.optimized_processor.update_business_config(config);
    }

    /// Start batch processor
    pub async fn start_batch_processor(&mut self) -> Result<()> {
        self.optimized_processor.start_batch_processor().await
    }

    /// Shutdown the processor
    pub async fn shutdown(&mut self) -> Result<()> {
        self.optimized_processor.shutdown().await
    }
}

#[async_trait]
impl EventProcessor for CDCEventProcessor {
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
    pub async fn start_consuming(&mut self, processor: Arc<CDCEventProcessor>) -> Result<()> {
        // This method is kept for backward compatibility but should not be used
        // Use start_consuming_with_mutex directly
        Err(anyhow::anyhow!("Use start_consuming_with_mutex instead"))
    }

    /// Start consuming CDC events with mutex-wrapped processor
    pub async fn start_consuming_with_mutex(
        &mut self,
        processor: Arc<tokio::sync::Mutex<CDCEventProcessor>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<()> {
        info!("Starting CDC consumer for topic: {}", self.cdc_topic);
        tracing::info!(
            "CDCConsumer: Entered start_consuming async loop for topic: {}",
            self.cdc_topic
        );

        // Subscribe to CDC topic directly
        tracing::info!("CDCConsumer: Subscribing to topic: {}", self.cdc_topic);
        let subscribe_result = self
            .kafka_consumer
            .subscribe_to_topic(&self.cdc_topic)
            .await;
        match &subscribe_result {
            Ok(_) => tracing::info!(
                "CDCConsumer: Successfully subscribed to topic: {}",
                self.cdc_topic
            ),
            Err(e) => tracing::error!(
                "CDCConsumer: Failed to subscribe to topic: {}: {}",
                self.cdc_topic,
                e
            ),
        }
        subscribe_result.map_err(|e| anyhow::anyhow!("Failed to subscribe to CDC topic: {}", e))?;

        tracing::info!(
            "CDCConsumer: Starting main consumption loop for topic: {}",
            self.cdc_topic
        );
        let mut poll_count = 0;
        let mut last_log_time = std::time::Instant::now();

        // Log immediately to confirm we're in the loop
        tracing::info!("CDCConsumer: Entering main polling loop");

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
                _ = shutdown_rx.recv() => {
                    info!("CDC consumer received shutdown signal");
                    tracing::info!("CDCConsumer: Received shutdown signal, breaking loop");
                    break;
                }
                message_result = self.kafka_consumer.poll_cdc_events() => {
                    match message_result {
                        Ok(Some(cdc_event)) => {
                            tracing::info!("CDCConsumer: ✅ Received CDC event on poll #{}: {:?}", poll_count, cdc_event);
                            match processor.lock().await.process_cdc_event(cdc_event).await {
                                Ok(_) => {
                                tracing::info!("CDCConsumer: ✅ Successfully processed CDC event on poll #{}", poll_count);
                                    // CRITICAL: Commit offset after successful processing
                                    if let Err(e) = self.kafka_consumer.commit_current_message().await {
                                        error!("CDCConsumer: ❌ Failed to commit offset: {}", e);
                                    } else {
                                        tracing::info!("CDCConsumer: ✅ Successfully committed offset for poll #{}", poll_count);
                                    }
                                }
                                Err(e) => {
                                    error!("CDCConsumer: ❌ Failed to process CDC event: {}", e);
                                    // Don't commit offset on failure - let it be retried
                                }
                            }
                        }
                        Ok(None) => {
                            if poll_count <= 10 || poll_count % 50 == 0 { // Log every 50th empty poll to avoid spam
                                tracing::info!(
                                    "CDCConsumer: ⏳ No CDC event available on poll #{} for topic: {}",
                                    poll_count, self.cdc_topic
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!("CDCConsumer: ❌ Error polling CDC message on poll #{}: {}", poll_count, e);
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
        Ok(())
    }

    async fn process_cdc_message(
        &self,
        message: crate::infrastructure::kafka_abstraction::KafkaMessage,
        processor: &Arc<tokio::sync::Mutex<CDCEventProcessor>>,
    ) -> Result<()> {
        // Parse CDC event from Kafka message
        let cdc_event: serde_json::Value = serde_json::from_slice(&message.payload)?;

        // Process the CDC event
        processor.lock().await.process_cdc_event(cdc_event).await?;

        Ok(())
    }
}

/// Enhanced CDC Service Manager - manages the entire CDC pipeline with optimized components
pub struct CDCServiceManager {
    config: DebeziumConfig,
    outbox_repo: Arc<CDCOutboxRepository>,
    cdc_consumer: Option<CDCConsumer>,
    processor: Arc<tokio::sync::Mutex<CDCEventProcessor>>,
    // Add the optimized producer
    cdc_producer: Option<Arc<tokio::sync::Mutex<CDCProducer>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    cdc_consumer_handle: Option<tokio::task::JoinHandle<()>>,

    // Enhanced components from service manager
    optimized_service_manager: Option<OptimizedCDCServiceManager>,
    integration_helper: Option<CDCIntegrationHelper>,

    // Enhanced state management
    service_state: Arc<tokio::sync::RwLock<ServiceState>>,
    enhanced_metrics: Arc<EnhancedCDCMetrics>,
}

impl CDCServiceManager {
    pub async fn new(
        config: DebeziumConfig,
        outbox_repo: Arc<CDCOutboxRepository>,
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        business_config: Option<BusinessLogicConfig>,
        producer_config: Option<CDCProducerConfig>,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Create optimized event processor
        let processor = Arc::new(tokio::sync::Mutex::new(CDCEventProcessor::new(
            kafka_producer.clone(),
            cache_service.clone(),
            projection_store.clone(),
            business_config,
        )));

        // Create optimized CDC producer
        let cdc_producer = if let Some(prod_config) = producer_config {
            Some(Arc::new(tokio::sync::Mutex::new(
                CDCProducer::new(
                    kafka_producer.clone(),
                    outbox_repo.pool.clone(),
                    prod_config,
                )
                .await?,
            )))
        } else {
            None
        };

        let cdc_topic = format!(
            "{}.{}",
            config.topic_prefix,
            config.table_include_list // Do not remove 'public.'
        );

        let cdc_consumer = CDCConsumer::new(kafka_consumer.clone(), cdc_topic);

        // Initialize enhanced service manager with proper configuration
        let optimization_config = OptimizationConfig::default();
        let enhanced_service_manager = OptimizedCDCServiceManager::new(
            config.clone(),
            outbox_repo.clone(),
            kafka_producer.clone(),
            kafka_consumer.clone(),
            cache_service.clone(),
            projection_store.clone(),
        )?;

        Ok(Self {
            config,
            outbox_repo,
            cdc_consumer: Some(cdc_consumer),
            processor,
            cdc_producer,
            shutdown_tx: Some(shutdown_tx),
            cleanup_handle: None,
            cdc_consumer_handle: None,
            optimized_service_manager: Some(enhanced_service_manager),
            integration_helper: None,
            service_state: Arc::new(tokio::sync::RwLock::new(ServiceState::Stopped)),
            enhanced_metrics: Arc::new(EnhancedCDCMetrics::default()),
        })
    }

    /// Start the CDC service
    pub async fn start(&mut self) -> Result<()> {
        // Update service state
        self.set_service_state(ServiceState::Starting).await;

        info!("Starting CDC Service Manager with optimized components");

        // Create CDC table if it doesn't exist
        tracing::info!("CDC Service Manager: Creating CDC outbox table...");
        self.outbox_repo.create_cdc_outbox_table().await?;
        tracing::info!("CDC Service Manager: ✅ CDC outbox table created/verified");

        // Start the optimized CDC producer if configured
        if let Some(ref mut producer) = self.cdc_producer {
            tracing::info!("CDC Service Manager: Starting optimized CDC producer...");
            producer.lock().await.start().await?;
            tracing::info!("CDC Service Manager: ✅ Optimized CDC producer started");
        }

        // Start batch processor for the event processor
        tracing::info!("CDC Service Manager: Starting batch processor...");
        self.processor.lock().await.start_batch_processor().await?;
        tracing::info!("CDC Service Manager: ✅ Batch processor started");

        // Start enhanced service manager if available
        if let Some(ref mut enhanced_manager) = self.optimized_service_manager {
            tracing::info!("CDC Service Manager: Starting enhanced service manager...");
            enhanced_manager.start().await?;
            tracing::info!("CDC Service Manager: ✅ Enhanced service manager started");
        }

        // Start CDC consumer
        if let Some(mut consumer) = self.cdc_consumer.take() {
            let processor = self.processor.clone();
            let cdc_topic = format!(
                "{}.{}",
                self.config.topic_prefix,
                self.config.table_include_list // Do not remove 'public.'
            );
            tracing::info!(
                "CDC Service Manager: Starting CDC consumer for topic: {}",
                cdc_topic
            );
            tracing::info!("CDC Service Manager: About to spawn CDC consumer task");

            let handle = tokio::spawn(async move {
                tracing::info!("CDC Service Manager: CDC consumer task spawned and running");
                tracing::info!("CDC Service Manager: About to call consumer.start_consuming()");

                match consumer.start_consuming_with_mutex(processor, shutdown_rx).await {
                    Ok(_) => {
                        tracing::info!("CDC Service Manager: CDC consumer completed normally");
                    }
                    Err(e) => {
                        error!("CDC Service Manager: CDC consumer failed: {}", e);
                        tracing::error!("CDC Service Manager: CDC consumer error details: {:?}", e);
                    }
                }
                tracing::error!("CDC Service Manager: CDC consumer task exited unexpectedly!");
            });

            self.cdc_consumer_handle = Some(handle);
            tracing::info!("CDC Service Manager: ✅ CDC consumer task spawned and handle stored");
        } else {
            tracing::error!("CDC Service Manager: ❌ No CDC consumer available to start");
        }

        // Start cleanup task
        let outbox_repo = self.outbox_repo.clone();
        let cleanup_handle = tokio::spawn(async move {
            tracing::info!("CDC Service Manager: Starting cleanup task");
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
        tracing::info!("CDC Service Manager: ✅ Cleanup task spawned");

        // Update service state to running
        self.set_service_state(ServiceState::Running).await;

        info!("CDC Service Manager started successfully with optimized components");
        Ok(())
    }

    /// Stop the CDC service
    pub async fn stop(&self) -> Result<()> {
        // Update service state
        self.set_service_state(ServiceState::Stopping).await;

        info!("Stopping CDC Service Manager");

        // Send shutdown signal
        if let Some(shutdown_tx) = &self.shutdown_tx {
            if let Err(e) = shutdown_tx.send(()).await {
                warn!("Failed to send shutdown signal to CDC consumer: {}", e);
            }
        }

        // Stop the enhanced service manager if available
        if let Some(ref enhanced_manager) = self.optimized_service_manager {
            if let Err(e) = enhanced_manager.stop().await {
                error!("Failed to stop enhanced service manager: {}", e);
            }
        }

        // Stop the CDC producer
        if let Some(ref producer) = self.cdc_producer {
            if let Err(e) = producer.lock().await.stop().await {
                error!("Failed to stop CDC producer: {}", e);
            }
        }

        // Shutdown the event processor
        if let Err(e) = self.processor.lock().await.shutdown().await {
            error!("Failed to shutdown event processor: {}", e);
        }

        // Cancel cleanup task
        if let Some(handle) = &self.cleanup_handle {
            handle.abort();
        }

        // Cancel CDC consumer task
        if let Some(handle) = &self.cdc_consumer_handle {
            handle.abort();
        }

        // Update service state to stopped
        self.set_service_state(ServiceState::Stopped).await;

        info!("CDC Service Manager stopped");
        Ok(())
    }

    /// Get CDC metrics
    pub async fn get_metrics(&self) -> Result<CDCMetrics> {
        let processor = self.processor.lock().await;
        let metrics = processor.get_metrics();
        Ok(CDCMetrics {
            events_processed: std::sync::atomic::AtomicU64::new(
                metrics
                    .events_processed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            events_failed: std::sync::atomic::AtomicU64::new(
                metrics
                    .events_failed
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            processing_latency_ms: std::sync::atomic::AtomicU64::new(
                metrics
                    .processing_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            total_latency_ms: std::sync::atomic::AtomicU64::new(
                metrics
                    .total_latency_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            cache_invalidations: std::sync::atomic::AtomicU64::new(
                metrics
                    .cache_invalidations
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            projection_updates: std::sync::atomic::AtomicU64::new(
                metrics
                    .projection_updates
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        })
    }

    /// Get optimized metrics
    pub async fn get_optimized_metrics(
        &self,
    ) -> Result<crate::infrastructure::cdc_event_processor::OptimizedCDCMetrics> {
        Ok(self.processor.lock().await.get_optimized_metrics().await)
    }

    /// Get producer metrics if available
    pub async fn get_producer_metrics(
        &self,
    ) -> Option<crate::infrastructure::cdc_producer::CDCProducerMetrics> {
        if let Some(ref producer) = self.cdc_producer {
            let producer_guard = producer.lock().await;
            let metrics = producer_guard.get_metrics();
            Some(crate::infrastructure::cdc_producer::CDCProducerMetrics {
                messages_produced: std::sync::atomic::AtomicU64::new(
                    metrics
                        .messages_produced
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                messages_failed: std::sync::atomic::AtomicU64::new(
                    metrics
                        .messages_failed
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                batch_count: std::sync::atomic::AtomicU64::new(
                    metrics
                        .batch_count
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                avg_batch_size: std::sync::atomic::AtomicU64::new(
                    metrics
                        .avg_batch_size
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                produce_latency_ms: std::sync::atomic::AtomicU64::new(
                    metrics
                        .produce_latency_ms
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                db_write_latency_ms: std::sync::atomic::AtomicU64::new(
                    metrics
                        .db_write_latency_ms
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                kafka_produce_latency_ms: std::sync::atomic::AtomicU64::new(
                    metrics
                        .kafka_produce_latency_ms
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                circuit_breaker_trips: std::sync::atomic::AtomicU64::new(
                    metrics
                        .circuit_breaker_trips
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                retries_attempted: std::sync::atomic::AtomicU64::new(
                    metrics
                        .retries_attempted
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                health_check_failures: std::sync::atomic::AtomicU64::new(
                    metrics
                        .health_check_failures
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                last_successful_produce: std::sync::atomic::AtomicU64::new(
                    metrics
                        .last_successful_produce
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                queue_depth: std::sync::atomic::AtomicU64::new(
                    metrics
                        .queue_depth
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                throughput_per_second: std::sync::atomic::AtomicU64::new(
                    metrics
                        .throughput_per_second
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                validation_failures: std::sync::atomic::AtomicU64::new(
                    metrics
                        .validation_failures
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
                duplicate_messages_rejected: std::sync::atomic::AtomicU64::new(
                    metrics
                        .duplicate_messages_rejected
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
            })
        } else {
            None
        }
    }

    /// Get producer health status if available
    pub async fn get_producer_health(
        &self,
    ) -> Option<crate::infrastructure::cdc_producer::HealthStatus> {
        if let Some(ref producer) = self.cdc_producer {
            Some(producer.lock().await.get_health_status().await)
        } else {
            None
        }
    }

    /// Generate Debezium connector configuration
    pub fn get_debezium_config(&self) -> serde_json::Value {
        self.outbox_repo.generate_debezium_config(&self.config)
    }

    /// Produce a message using the optimized producer
    pub async fn produce_message(&self, message: CDCOutboxMessage) -> Result<()> {
        if let Some(ref producer) = self.cdc_producer {
            // Convert to producer's CDCOutboxMessage type
            let producer_message = crate::infrastructure::cdc_producer::CDCOutboxMessage {
                id: message.id,
                aggregate_id: message.aggregate_id,
                event_id: message.event_id,
                event_type: message.event_type,
                topic: message.topic,
                metadata: message.metadata,
                created_at: message.created_at,
                updated_at: message.updated_at,
            };
            producer
                .lock()
                .await
                .produce_message(producer_message)
                .await
        } else {
            Err(anyhow::anyhow!("CDC producer not configured"))
        }
    }

    /// Initialize integration helper for migration operations
    pub async fn initialize_integration_helper(
        &mut self,
        config: CDCIntegrationConfig,
    ) -> Result<()> {
        let integration_helper = CDCIntegrationHelper::new(self.outbox_repo.pool.clone(), config);
        self.integration_helper = Some(integration_helper);

        // Update enhanced metrics to reflect integration helper initialization
        self.enhanced_metrics
            .integration_helper_initialized
            .store(true, std::sync::atomic::Ordering::Relaxed);

        info!("CDC Integration Helper initialized");
        Ok(())
    }

    /// Migrate existing outbox to CDC format
    pub async fn migrate_existing_outbox(
        &self,
        old_repo: &crate::infrastructure::outbox::PostgresOutboxRepository,
    ) -> Result<MigrationStats> {
        if let Some(ref helper) = self.integration_helper {
            // Update service state during migration
            self.set_service_state(ServiceState::Migrating).await;

            let result = helper
                .migrate_existing_outbox(old_repo, &self.outbox_repo)
                .await;

            // Update service state back to running after migration
            self.set_service_state(ServiceState::Running).await;

            result
        } else {
            Err(anyhow::anyhow!(
                "Integration helper not initialized. Call initialize_integration_helper first."
            ))
        }
    }

    /// Verify migration integrity
    pub async fn verify_migration_integrity(&self) -> Result<MigrationIntegrityReport> {
        if let Some(ref helper) = self.integration_helper {
            helper.verify_migration_integrity(&self.outbox_repo).await
        } else {
            Err(anyhow::anyhow!("Integration helper not initialized"))
        }
    }

    /// Get enhanced service status with all metrics
    pub async fn get_enhanced_status(&self) -> serde_json::Value {
        let processor_guard = self.processor.lock().await;
        let processor_metrics = processor_guard.get_metrics();
        let mut status = serde_json::json!({
            "service_state": format!("{:?}", *self.service_state.read().await),
            "basic_metrics": {
                "events_processed": processor_metrics.events_processed.load(std::sync::atomic::Ordering::Relaxed),
                "events_failed": processor_metrics.events_failed.load(std::sync::atomic::Ordering::Relaxed),
                "cache_invalidations": processor_metrics.cache_invalidations.load(std::sync::atomic::Ordering::Relaxed),
                "projection_updates": processor_metrics.projection_updates.load(std::sync::atomic::Ordering::Relaxed),
            },
            "enhanced_metrics": {
                "memory_usage_bytes": self.enhanced_metrics.memory_usage_bytes.load(std::sync::atomic::Ordering::Relaxed),
                "throughput_per_second": self.enhanced_metrics.throughput_per_second.load(std::sync::atomic::Ordering::Relaxed),
                "error_rate": self.enhanced_metrics.error_rate.load(std::sync::atomic::Ordering::Relaxed),
                "circuit_breaker_trips": self.enhanced_metrics.circuit_breaker_trips.load(std::sync::atomic::Ordering::Relaxed),
                "integration_helper_initialized": self.enhanced_metrics.integration_helper_initialized.load(std::sync::atomic::Ordering::Relaxed),
            }
        });

        // Add optimized metrics if available
        if let Ok(optimized_metrics) = self.get_optimized_metrics().await {
            status["optimized_metrics"] = serde_json::json!({
                "business_validation_failures": optimized_metrics.business_validation_failures.load(std::sync::atomic::Ordering::Relaxed),
                "duplicate_events_skipped": optimized_metrics.duplicate_events_skipped.load(std::sync::atomic::Ordering::Relaxed),
                "projection_update_failures": optimized_metrics.projection_update_failures.load(std::sync::atomic::Ordering::Relaxed),
            });
        }

        // Add producer health if available
        if let Some(producer_health) = self.get_producer_health().await {
            status["producer_health"] =
                serde_json::to_value(producer_health).unwrap_or(serde_json::Value::Null);
        }

        // Add enhanced service manager status if available
        if let Some(ref enhanced_manager) = self.optimized_service_manager {
            let enhanced_status = enhanced_manager.get_service_status().await;
            status["enhanced_service_manager"] = enhanced_status;
        }

        status
    }

    /// Update service state
    pub async fn set_service_state(&self, state: ServiceState) {
        let mut state_guard = self.service_state.write().await;
        *state_guard = state;
    }

    /// Get current service state
    pub async fn get_service_state(&self) -> ServiceState {
        self.service_state.read().await.clone()
    }

    /// Get enhanced metrics
    pub fn get_enhanced_metrics(&self) -> &EnhancedCDCMetrics {
        &self.enhanced_metrics
    }

    /// Cleanup old messages from both outbox systems
    pub async fn cleanup_old_messages(
        &self,
        old_repo: &crate::infrastructure::outbox::PostgresOutboxRepository,
        older_than: std::time::Duration,
    ) -> Result<(usize, usize)> {
        if let Some(ref helper) = self.integration_helper {
            helper
                .cleanup_old_messages(old_repo, &self.outbox_repo, older_than)
                .await
        } else {
            // Fallback to basic cleanup
            let old_cleaned = 0; // Would need to implement
            let new_cleaned = self.outbox_repo.cleanup_old_messages(older_than).await?;
            Ok((old_cleaned, new_cleaned))
        }
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

// Integration helper is now imported from cdc_integration_helper module

/// Health check for CDC service
pub struct CDCHealthCheck {
    metrics: Arc<CDCMetrics>,
    optimized_metrics: Option<crate::infrastructure::cdc_event_processor::OptimizedCDCMetrics>,
    producer_health: Option<crate::infrastructure::cdc_producer::HealthStatus>,
}

impl CDCHealthCheck {
    pub fn new(metrics: Arc<CDCMetrics>) -> Self {
        Self {
            metrics,
            optimized_metrics: None,
            producer_health: None,
        }
    }

    pub fn with_optimized_metrics(
        mut self,
        optimized_metrics: crate::infrastructure::cdc_event_processor::OptimizedCDCMetrics,
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
                "business_validation_failures": opt_metrics.business_validation_failures.load(std::sync::atomic::Ordering::Relaxed),
                "duplicate_events_skipped": opt_metrics.duplicate_events_skipped.load(std::sync::atomic::Ordering::Relaxed),
                "projection_update_failures": opt_metrics.projection_update_failures.load(std::sync::atomic::Ordering::Relaxed),
                "memory_usage_bytes": opt_metrics.memory_usage_bytes.load(std::sync::atomic::Ordering::Relaxed),
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
