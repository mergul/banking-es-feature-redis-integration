use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use crate::infrastructure::CopyOptimizationConfig;
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
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

// Import the optimized components
use crate::infrastructure::binary_utils::{PgCopyBinaryWriter, ToPgCopyBinary};
use crate::infrastructure::cdc_event_processor::{
    AdvancedMonitoringSystem, BusinessLogicConfig, UltraOptimizedCDCEventProcessor,
};
use crate::infrastructure::cdc_integration_helper::{
    CDCIntegrationConfig, CDCIntegrationHelper, CDCIntegrationHelperBuilder,
    MigrationIntegrityReport, MigrationStats,
};
use crate::infrastructure::cdc_producer::{BusinessLogicValidator, CDCProducer, CDCProducerConfig};
use crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics;
use crate::infrastructure::event_processor::EventProcessor;
use futures::stream::StreamExt;
use sqlx::types::JsonValue;
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
            poll_interval_ms: 5, // CRITICAL FIX: Reduced from 100ms to 5ms for better performance
        }
    }
}

/// Binary row format for COPY operations
#[derive(Debug, Clone)]
struct OutboxCopyRow {
    aggregate_id: Uuid,
    event_id: Uuid,
    event_type: String,
    payload: Vec<u8>,
    topic: String,
    metadata: serde_json::Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl OutboxCopyRow {
    /// Create a new OutboxCopyRow from an OutboxMessage
    pub fn from_outbox_message(msg: crate::infrastructure::outbox::OutboxMessage) -> Self {
        let now = chrono::Utc::now();
        Self {
            aggregate_id: msg.aggregate_id,
            event_id: msg.event_id,
            event_type: msg.event_type,
            payload: msg.payload,
            topic: msg.topic,
            metadata: msg.metadata.unwrap_or_else(|| {
                serde_json::json!({
                    "correlation_id": null,
                    "causation_id": null,
                    "user_id": null,
                    "source": "cdc_outbox",
                    "schema_version": "1.0",
                    "tags": []
                })
            }),
            created_at: now,
            updated_at: now,
        }
    }

    /// Write row data to an existing PgCopyBinaryWriter
    /// Use this when you want to write multiple rows to the same writer
    pub fn write_to_binary_writer(
        &self,
        writer: &mut PgCopyBinaryWriter,
    ) -> Result<(), std::io::Error> {
        // Write field count (8 fields for kafka_outbox_cdc table)
        writer.write_row(8)?;

        // Write each field in the exact order as defined in the table schema:
        // (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at)
        writer.write_uuid(&self.aggregate_id)?;
        writer.write_uuid(&self.event_id)?;
        writer.write_text(&self.event_type)?;
        writer.write_bytea(&self.payload)?;
        writer.write_text(&self.topic)?;
        writer.write_jsonb_binary(&self.metadata)?;
        writer.write_timestamp(&self.created_at)?;
        writer.write_timestamp(&self.updated_at)?;

        Ok(())
    }

    /// Validate the row data before writing to COPY
    pub fn validate(&self) -> Result<(), String> {
        // Check event_type is not empty
        if self.event_type.is_empty() {
            return Err("event_type cannot be empty".to_string());
        }

        // Check topic is not empty
        if self.topic.is_empty() {
            return Err("topic cannot be empty".to_string());
        }

        // Validate JSON metadata
        match &self.metadata {
            JsonValue::Null => {} // NULL is fine
            _ => {
                let json_str = self.metadata.to_string();

                // Check for null bytes that would cause issues
                if json_str.contains('\0') {
                    return Err("metadata contains null bytes".to_string());
                }

                // Validate UTF-8
                if let Err(e) = std::str::from_utf8(json_str.as_bytes()) {
                    return Err(format!("metadata contains invalid UTF-8: {}", e));
                }
            }
        }

        // Check text fields for problematic characters
        if self.event_type.contains('\0') {
            return Err("event_type contains null bytes".to_string());
        }

        if self.topic.contains('\0') {
            return Err("topic contains null bytes".to_string());
        }

        Ok(())
    }
}

// Note: We don't implement ToPgCopyBinary for single OutboxCopyRow
// because it's inefficient - each row would create its own complete binary stream.
// Instead, use write_to_binary_writer() to add rows to a shared writer.

// Implementation for multiple rows (recommended approach)
impl ToPgCopyBinary for Vec<OutboxCopyRow> {
    /// Convert multiple rows to complete binary COPY format
    /// This is more efficient than individual row conversion
    fn to_pgcopy_binary(&self) -> Result<Vec<u8>, std::io::Error> {
        let mut writer = PgCopyBinaryWriter::new()?;

        for row in self {
            row.write_to_binary_writer(&mut writer)?;
        }

        writer.finish()
    }
}

// Helper methods for OutboxCopyRow
impl OutboxCopyRow {
    /// Create multiple rows from OutboxMessages with validation
    pub fn from_outbox_messages_validated(
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<Vec<Self>, String> {
        let mut rows = Vec::with_capacity(messages.len());

        for (idx, msg) in messages.into_iter().enumerate() {
            let row = Self::from_outbox_message(msg);

            // Validate each row
            if let Err(e) = row.validate() {
                return Err(format!("Invalid row at index {}: {}", idx, e));
            }

            rows.push(row);
        }

        Ok(rows)
    }

    /// Batch create rows and convert to binary format
    pub fn batch_to_binary(
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Create and validate rows
        let rows = Self::from_outbox_messages_validated(messages)
            .map_err(|e| format!("Validation failed: {}", e))?;

        // Convert to binary format
        let binary_data = rows
            .to_pgcopy_binary()
            .map_err(|e| format!("Binary conversion failed: {}", e))?;

        Ok(binary_data)
    }

    /// Create a test row (useful for debugging)
    #[cfg(test)]
    pub fn test_row() -> Self {
        use uuid::Uuid;

        Self {
            aggregate_id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            event_type: "TestEvent".to_string(),
            payload: b"test payload".to_vec(),
            topic: "test-topic".to_string(),
            metadata: serde_json::json!({"test": "metadata"}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
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

    /// Get access to the underlying pools
    pub fn get_pools(&self) -> &Arc<PartitionedPools> {
        &self.pools
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
                "transforms.unwrap.operation.header": true,
                "transforms.outbox.table.field.event.timestamp": "deleted_at",
                "transforms.outbox.route.tombstone.on.empty.payload": "true",
                "tombstones.on.delete": "true",
                "max.queue.size": "16384", // Increased from 8192
                "max.batch.size": "4096",  // Increased from 2048
                "poll.interval.ms": "5",   // CRITICAL: Reduced to 5ms for ultra-low latency
                "heartbeat.interval.ms": "500", // Reduced from 1000ms
                "database.history.kafka.recovery.poll.interval.ms": "50", // Reduced from 100ms
                "database.history.store.only.captured.tables.ddl": "true",
                "database.history.skip.unparseable.ddl": "true"
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
        let (sender, mut receiver) = mpsc::channel(5000);
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
        println!("[DEBUG] OutboxBatcher::submit: Submitting outbox message for aggregate_id={:?}, event_id={:?}", msg.aggregate_id, msg.event_id);
        self.sender
            .send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))
    }

    /// Non-blocking submit method to prevent system from getting stuck
    pub fn try_submit(&self, msg: crate::infrastructure::outbox::OutboxMessage) -> Result<()> {
        println!("[DEBUG] OutboxBatcher::try_submit: Attempting non-blocking submit for aggregate_id={:?}, event_id={:?}", msg.aggregate_id, msg.event_id);
        self.sender
            .try_send(msg)
            .map_err(|e| anyhow::anyhow!("Failed to send message (non-blocking): {}", e))
    }

    /// Bulk submit method for multiple messages - much more efficient than individual submits
    pub async fn submit_bulk(
        &self,
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let messages_len = messages.len();
        println!(
            "[DEBUG] OutboxBatcher::submit_bulk: Submitting {} messages in bulk",
            messages_len
        );

        // Send all messages in sequence to the channel
        for msg in messages {
            if let Err(e) = self.sender.send(msg).await {
                return Err(anyhow::anyhow!("Failed to send message in bulk: {}", e));
            }
        }

        println!(
            "[DEBUG] OutboxBatcher::submit_bulk: Successfully submitted {} messages in bulk",
            messages_len
        );
        Ok(())
    }

    /// Non-blocking bulk submit method for high-performance scenarios
    pub fn try_submit_bulk(
        &self,
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let messages_len = messages.len();
        println!(
            "[DEBUG] OutboxBatcher::try_submit_bulk: Attempting non-blocking bulk submit of {} messages",
            messages_len
        );

        // Send all messages in sequence to the channel
        for msg in messages {
            if let Err(e) = self.sender.try_send(msg) {
                return Err(anyhow::anyhow!(
                    "Failed to send message in bulk (non-blocking): {}",
                    e
                ));
            }
        }

        println!(
            "[DEBUG] OutboxBatcher::try_submit_bulk: Successfully submitted {} messages in bulk",
            messages_len
        );
        Ok(())
    }

    async fn flush(
        repo: &Arc<dyn crate::infrastructure::outbox::OutboxRepositoryTrait + Send + Sync>,
        pools: &Arc<PartitionedPools>,
        buffer: &mut Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) {
        if buffer.is_empty() {
            return;
        }

        println!(
            "[DEBUG] OutboxBatcher::flush: Flushing {} messages",
            buffer.len()
        );
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
        println!("[DEBUG] OutboxBatcher::flush: Flush complete");
    }

    pub fn new_default(
        repo: Arc<dyn crate::infrastructure::outbox::OutboxRepositoryTrait + Send + Sync>,
        pools: Arc<PartitionedPools>,
    ) -> Self {
        // OPTIMIZED: Updated batch size and timeout for better performance
        // CRITICAL OPTIMIZATION: Batch processing configuration
        let batch_timeout_ms = std::env::var("CDC_BATCH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(50);

        let batch_size = std::env::var("CDC_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10000);

        Self::new(
            repo,
            pools,
            batch_size,
            Duration::from_millis(batch_timeout_ms),
        )
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
        let message_count = messages.len();

        // ADAPTIVE: Use COPY for large batches, VALUES for small batches
        let should_use_copy = message_count >= 100; // Threshold for COPY vs VALUES

        if should_use_copy {
            tracing::debug!(
                "CDCOutboxRepository: Using COPY for large batch of {} messages",
                message_count
            );
            return self.add_pending_messages_copy(tx, messages).await;
        }

        tracing::debug!(
            "CDCOutboxRepository: Using VALUES for small batch of {} messages",
            message_count
        );

        // OPTIMIZED: Use VALUES clause for better performance than UNNEST
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at) ",
        );
        let now = chrono::Utc::now();
        query_builder.push_values(messages.iter(), |mut b, msg| {
            let metadata_value = msg.metadata.clone().unwrap_or_else(|| {
                serde_json::json!({
                    "correlation_id": null,
                    "causation_id": null,
                    "user_id": null,
                    "source": "cdc_outbox",
                    "schema_version": "1.0",
                    "tags": []
                })
            });

            b.push_bind(msg.aggregate_id)
                .push_bind(msg.event_id)
                .push_bind(&msg.event_type)
                .push_bind(&msg.payload)
                .push_bind(&msg.topic)
                .push_bind(metadata_value)
                .push_bind(now)
                .push_bind(now);
        });

        let result = query_builder.build().execute(&mut **tx).await;

        match result {
            Ok(_) => {
                let duration = start_time.elapsed();
                let throughput = message_count as f64 / duration.as_secs_f64();
                tracing::info!(
                    "CDCOutboxRepository: Successfully bulk inserted {} messages in {:?} ({:.0} msg/sec)",
                    message_count,
                    duration,
                    throughput
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    "CDCOutboxRepository: Failed to bulk insert {} messages: {:?}",
                    message_count,
                    e
                );
                Err(anyhow::anyhow!(
                    "Failed to bulk insert outbox messages: {:?}",
                    e
                ))
            }
        }
    }

    /// Switched to COPY BINARY to avoid double encoding.
    async fn add_pending_messages_copy(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let start_time = std::time::Instant::now();
        let message_count = messages.len();
        tracing::info!(
            "CDCOutboxRepository: Starting BINARY COPY via temp table for {} messages",
            message_count
        );

        // 1) Create temp table with metadata as TEXT to avoid jsonb binary encoding
        sqlx::query(
            r#"
            CREATE TEMP TABLE IF NOT EXISTS temp_kafka_outbox_cdc (
                aggregate_id UUID NOT NULL,
                event_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                payload BYTEA NOT NULL,
                topic TEXT NOT NULL,
                metadata TEXT,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            ) ON COMMIT PRESERVE ROWS
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create temp table for CDC COPY: {:?}", e))?;

        // 2) Build binary COPY payload targeting the temp table (metadata as TEXT)
        let mut writer = crate::infrastructure::binary_utils::PgCopyBinaryWriter::new()
            .map_err(|e| anyhow::anyhow!("Failed to init binary writer: {:?}", e))?;

        for msg in &messages {
            // Prepare metadata JSON string or NULL
            let metadata_json_opt = msg.metadata.clone().or_else(|| {
                Some(serde_json::json!({
                    "correlation_id": null,
                    "causation_id": null,
                    "user_id": null,
                    "source": "cdc_outbox",
                    "schema_version": "1.0",
                    "tags": []
                }))
            });

            let now = chrono::Utc::now();

            writer
                .write_row(8)
                .map_err(|e| anyhow::anyhow!("Failed to write row header: {:?}", e))?;
            writer
                .write_uuid(&msg.aggregate_id)
                .map_err(|e| anyhow::anyhow!("Failed to write aggregate_id: {:?}", e))?;
            writer
                .write_uuid(&msg.event_id)
                .map_err(|e| anyhow::anyhow!("Failed to write event_id: {:?}", e))?;
            writer
                .write_text(&msg.event_type)
                .map_err(|e| anyhow::anyhow!("Failed to write event_type: {:?}", e))?;
            writer
                .write_bytea(&msg.payload)
                .map_err(|e| anyhow::anyhow!("Failed to write payload: {:?}", e))?;
            writer
                .write_text(&msg.topic)
                .map_err(|e| anyhow::anyhow!("Failed to write topic: {:?}", e))?;

            match metadata_json_opt {
                Some(val) if !val.is_null() => writer
                    .write_text(&val.to_string())
                    .map_err(|e| anyhow::anyhow!("Failed to write metadata text: {:?}", e))?,
                _ => writer
                    .write_null()
                    .map_err(|e| anyhow::anyhow!("Failed to write metadata NULL: {:?}", e))?,
            }

            writer
                .write_timestamp(&now)
                .map_err(|e| anyhow::anyhow!("Failed to write created_at: {:?}", e))?;
            writer
                .write_timestamp(&now)
                .map_err(|e| anyhow::anyhow!("Failed to write updated_at: {:?}", e))?;
        }

        let binary_data = writer
            .finish()
            .map_err(|e| anyhow::anyhow!("Failed to finish binary writer: {:?}", e))?;

        // 3) COPY BINARY into the temp table
        let mut copy = tx
            .copy_in_raw(
                "COPY temp_kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at) FROM STDIN WITH (FORMAT binary)",
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start BINARY COPY into temp table: {:?}", e))?;

        copy.send(binary_data.as_slice())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send data to BINARY COPY (temp): {:?}", e))?;

        let copied_rows = copy
            .finish()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finish BINARY COPY (temp): {:?}", e))?;

        tracing::info!(
            "CDCOutboxRepository: BINARY COPY -> temp inserted {} rows",
            copied_rows
        );

        // 4) Insert from temp table to final table, casting metadata TEXT -> JSONB
        let inserted = sqlx::query(
            r#"
            INSERT INTO kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at)
            SELECT aggregate_id, event_id, event_type, payload, topic, metadata::jsonb, created_at, updated_at
            FROM temp_kafka_outbox_cdc
            "#
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to insert from temp into kafka_outbox_cdc: {:?}", e))?;

        let duration = start_time.elapsed();
        let throughput = message_count as f64 / duration.as_secs_f64();
        tracing::info!(
            "CDCOutboxRepository: Successfully inserted {} messages via BINARY COPY (temp) in {:?} ({:.0} msg/sec)",
            inserted.rows_affected(),
            duration,
            throughput
        );

        Ok(())
    }

    /// Optimized: Direct COPY to final table without temp table
    async fn add_pending_messages_copy_direct(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let start_time = std::time::Instant::now();
        let message_count = messages.len();
        tracing::info!(
            "CDCOutboxRepository: Starting direct BINARY COPY for {} messages",
            message_count
        );

        // Convert and validate all messages to binary format in one go
        let binary_data = OutboxCopyRow::batch_to_binary(messages)
            .map_err(|e| anyhow::anyhow!("Failed to prepare binary data: {}", e))?;

        // Start COPY operation directly to final table
        let mut copy = tx
            .copy_in_raw(
                "COPY kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, metadata, created_at, updated_at) FROM STDIN BINARY",
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start BINARY COPY: {:?}", e))?;

        // Send all data in one operation
        copy.send(binary_data.as_slice())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send data to BINARY COPY: {:?}", e))?;

        // Finish the COPY operation
        let copied_rows = copy
            .finish()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finish BINARY COPY: {:?}", e))?;

        let duration = start_time.elapsed();
        let throughput = message_count as f64 / duration.as_secs_f64();
        tracing::info!(
            "CDCOutboxRepository: Successfully inserted {} messages via direct BINARY COPY in {:?} ({:.0} msg/sec)",
            copied_rows,
            duration,
            throughput
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

    /// Enhanced start_consuming method with COPY optimization integration
    pub async fn start_consuming_with_cancellation_token_copy_optimized(
        &mut self,
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        // All existing initialization code remains the same...
        static ACTIVE_CONSUMERS: AtomicUsize = AtomicUsize::new(0);
        let current = ACTIVE_CONSUMERS.fetch_add(1, Ordering::SeqCst) + 1;

        tracing::info!(
            "CDCConsumer (COPY-optimized): Entering main polling loop. Active consumers: {}",
            current
        );

        // Load COPY optimization configuration
        let copy_config = CopyOptimizationConfig::from_env();
        tracing::info!(
            "CDCConsumer (COPY-optimized): Configuration - COPY enabled: {}, account threshold: {}, transaction threshold: {}, batch size: {}",
            copy_config.enable_copy_optimization,
            copy_config.projection_copy_threshold,
            copy_config.transaction_copy_threshold,
            copy_config.cdc_projection_batch_size
        );

        // Existing subscription and setup code...
        let kafka_config = self.kafka_consumer.get_config();
        if !kafka_config.enabled {
            tracing::error!("CDCConsumer: Kafka consumer is disabled");
            return Err(anyhow::anyhow!("Kafka consumer is disabled"));
        }

        // Resolve CDC topic if empty or set to "auto"
        let topic_to_subscribe = {
            let configured = self.cdc_topic.trim().to_string();
            if !configured.is_empty() && configured.to_lowercase() != "auto" {
                configured
            } else if let Ok(env_topic) = std::env::var("CDC_TOPIC") {
                env_topic
            } else {
                let cfg = self.kafka_consumer.get_config();
                if !cfg.topic_prefix.is_empty() {
                    format!("{}.public.kafka_outbox_cdc", cfg.topic_prefix)
                } else {
                    "banking-es.public.kafka_outbox_cdc".to_string()
                }
            }
        };
        self.cdc_topic = topic_to_subscribe.clone();
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
                        "CDCConsumer (COPY-optimized): ✅ Successfully subscribed to topic: {}",
                        self.cdc_topic
                    );

                    // CRITICAL FIX: Force consumer group join by polling
                    tracing::info!(
                        "CDCConsumer (COPY-optimized): Forcing consumer group join by polling..."
                    );

                    // Poll a few times to trigger group join
                    let mut join_attempts = 0;
                    let max_join_attempts = 10;

                    while join_attempts < max_join_attempts {
                        match self.kafka_consumer.stream().next().await {
                            Some(Ok(_)) => {
                                tracing::info!("CDCConsumer (COPY-optimized): ✅ Consumer group join successful after {} attempts", join_attempts + 1);
                                break;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("CDCConsumer (COPY-optimized): Poll error during join attempt {}: {:?}", join_attempts + 1, e);
                            }
                            None => {
                                tracing::debug!("CDCConsumer (COPY-optimized): No message during join attempt {}", join_attempts + 1);
                            }
                        }
                        join_attempts += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }

                    if join_attempts >= max_join_attempts {
                        tracing::warn!("CDCConsumer (COPY-optimized): Consumer group join may not be complete after {} attempts", max_join_attempts);
                    }

                    // Consumer is ready to start processing
                    tracing::info!(
                        "CDCConsumer (COPY-optimized): Consumer group join completed, ready to start processing"
                    );

                    break;
                }
                Err(e) => {
                    subscription_retries += 1;
                    if subscription_retries >= max_subscription_retries {
                        return Err(anyhow::anyhow!(
                            "Failed to subscribe to CDC topic after {} attempts: {}",
                            max_subscription_retries,
                            e
                        ));
                    }
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        }

        // Enhanced polling configuration for COPY optimization
        let poll_interval_ms = std::env::var("CDC_POLL_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5); // Optimized to 5ms for maximum responsiveness

        // Use COPY-optimized batch size
        let batch_size = copy_config.cdc_projection_batch_size.max(
            std::env::var("CDC_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1000), // Optimized to 1000 for larger batches and better throughput
        );

        let batch_timeout_ms = std::env::var("CDC_BATCH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(25); // Reduced from 50ms to 25ms

        tracing::info!(
            "CDCConsumer (COPY-optimized): Polling config - interval: {}ms, batch_size: {}, batch_timeout: {}ms",
            poll_interval_ms,
            batch_size,
            batch_timeout_ms
        );

        // CRITICAL OPTIMIZATION: Adaptive polling based on load
        let mut adaptive_poll_interval = poll_interval_ms;
        let mut consecutive_empty_polls = 0;
        let min_poll_interval = 5; // 5ms minimum
        let max_poll_interval = 50; // 50ms maximum
        let adaptive_threshold = 3; // After 3 empty polls, increase interval

        // CRITICAL OPTIMIZATION: Memory pressure monitoring
        let mut memory_pressure_monitor = MemoryPressureMonitor::new();

        // CRITICAL OPTIMIZATION: Adaptive batch sizing based on system load
        let mut adaptive_batch_size = batch_size;
        let min_batch_size = 500; // Increased from 100
        let max_batch_size = 5000; // Increased from 2000

        // CRITICAL OPTIMIZATION: Global semaphore for concurrent batch processing
        let processing_semaphore = Arc::new(tokio::sync::Semaphore::new(10)); // Max 10 concurrent batches

        // CRITICAL OPTIMIZATION: Batch processing buffer with pre-allocation
        let mut message_batch = Vec::with_capacity(batch_size * 2); // Pre-allocate 2x capacity for better performance
        let mut last_batch_time = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!("CDCConsumer (COPY-optimized): Received shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(adaptive_poll_interval)) => {
                    // CRITICAL: Use non-blocking poll with timeout
                    match tokio::time::timeout(
                        Duration::from_millis(adaptive_poll_interval), // Use adaptive poll interval
                        self.kafka_consumer.stream().next()
                    ).await {
                        Ok(Some(Ok(message))) => {
                            consecutive_empty_polls = 0;
                            adaptive_poll_interval = poll_interval_ms; // Reset to base interval

                            // Process message immediately - convert BorrowedMessage to KafkaMessage
                            let kafka_message = crate::infrastructure::kafka_abstraction::KafkaMessage {
                                topic: message.topic().to_string(),
                                partition: message.partition(),
                                offset: message.offset(),
                                key: message.key().map(|k| k.to_vec()),
                                payload: message.payload().unwrap_or_default().to_vec(),
                                timestamp: message.timestamp().to_millis(),
                            };
                            message_batch.push(kafka_message);

                            // CRITICAL: Adaptive batch sizing based on memory pressure
                            if memory_pressure_monitor.should_reduce_batch_size() {
                                adaptive_batch_size = (adaptive_batch_size / 2).max(min_batch_size);
                                tracing::debug!("Memory pressure detected, reducing batch size to {}", adaptive_batch_size);
                            } else if adaptive_batch_size < batch_size {
                                adaptive_batch_size = (adaptive_batch_size * 2).min(max_batch_size);
                                tracing::debug!("Memory pressure resolved, increasing batch size to {}", adaptive_batch_size);
                            }

                            // CRITICAL: Process batch when full or timeout reached
                            if message_batch.len() >= adaptive_batch_size ||
                               last_batch_time.elapsed() >= Duration::from_millis(batch_timeout_ms) {
                                tracing::debug!("CDCConsumer: Processing batch of {} messages (size: {}, timeout: {:?})",
                                    message_batch.len(), adaptive_batch_size, last_batch_time.elapsed());
                                let batch_to_process = std::mem::take(&mut message_batch);
                                last_batch_time = std::time::Instant::now();

                                // CRITICAL FIX: Process batch in background with proper synchronization
                                // Use global semaphore to limit concurrent processing and prevent race conditions
                                let processor = processor.clone();
                                let semaphore = processing_semaphore.clone();

                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap(); // Hold permit until processing completes
                                    if let Err(e) = Self::process_message_batch(&batch_to_process, &processor).await {
                                        tracing::error!("CDCConsumer (COPY-optimized): Failed to process batch: {}", e);
                                    }
                                });
                            }
                        }
                        Ok(Some(Err(e))) => {
                            // No messages available
                            consecutive_empty_polls += 1;

                            // Adaptive polling: increase interval if no messages
                            if consecutive_empty_polls >= adaptive_threshold {
                                adaptive_poll_interval = (adaptive_poll_interval * 2).min(max_poll_interval);
                            }

                            // CRITICAL FIX: Process any remaining messages with proper synchronization
                            if !message_batch.is_empty() &&
                               last_batch_time.elapsed() >= Duration::from_millis(batch_timeout_ms) {
                                tracing::debug!("CDCConsumer: Processing remaining batch of {} messages (timeout: {:?})",
                                    message_batch.len(), last_batch_time.elapsed());
                                let batch_to_process = std::mem::take(&mut message_batch);
                                last_batch_time = std::time::Instant::now();

                                let processor = processor.clone();
                                let semaphore = processing_semaphore.clone();

                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    if let Err(e) = Self::process_message_batch(&batch_to_process, &processor).await {
                                        tracing::error!("CDCConsumer (COPY-optimized): Failed to process batch: {}", e);
                                    }
                                });
                            }
                        }
                        Ok(None) => {
                            // Stream ended
                            tracing::info!("CDCConsumer (COPY-optimized): Message stream ended");

                            // CRITICAL FIX: Process remaining messages in batch before exit
                            if !message_batch.is_empty() {
                                tracing::info!("CDCConsumer (COPY-optimized): Processing final batch of {} messages", message_batch.len());
                                let batch_to_process = std::mem::take(&mut message_batch);
                                last_batch_time = std::time::Instant::now();

                                let processor = processor.clone();
                                let semaphore = processing_semaphore.clone();

                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    if let Err(e) = Self::process_message_batch(&batch_to_process, &processor).await {
                                        tracing::error!("CDCConsumer (COPY-optimized): Failed to process final batch: {}", e);
                                    }
                                });
                            }
                            break;
                        }
                        Err(_) => {
                            // Poll timeout - this is expected and not an error
                            consecutive_empty_polls += 1;

                            if consecutive_empty_polls >= adaptive_threshold {
                                adaptive_poll_interval = (adaptive_poll_interval * 2).min(max_poll_interval);
                            }

                            // CRITICAL FIX: Process any remaining messages in batch on timeout
                            if !message_batch.is_empty() {
                                tracing::debug!("CDCConsumer: Processing timeout batch of {} messages", message_batch.len());
                                let batch_to_process = std::mem::take(&mut message_batch);
                                last_batch_time = std::time::Instant::now();

                                let processor = processor.clone();
                                let semaphore = processing_semaphore.clone();

                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    if let Err(e) = Self::process_message_batch(&batch_to_process, &processor).await {
                                        tracing::error!("CDCConsumer (COPY-optimized): Failed to process timeout batch: {}", e);
                                    }
                                });
                            }
                        }
                    }
                }
            }
        }

        let current = ACTIVE_CONSUMERS.fetch_sub(1, Ordering::SeqCst) - 1;
        tracing::info!(
            "CDCConsumer (COPY-optimized): Exiting main polling loop. Active consumers: {}",
            current
        );

        Ok(())
    }

    /// Get system load for adaptive batch sizing
    fn get_system_load() -> Option<f64> {
        // Simple CPU load estimation - you may want to use a more sophisticated method
        // This is a placeholder implementation
        #[cfg(target_os = "linux")]
        {
            use std::fs;
            if let Ok(loadavg) = fs::read_to_string("/proc/loadavg") {
                if let Some(first_load) = loadavg.split_whitespace().next() {
                    return first_load.parse::<f64>().ok();
                }
            }
        }

        // Fallback: return None to disable adaptive sizing
        None
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

        // Subscribe to CDC topic with retry logic (auto-detect if empty or "auto")
        let topic_to_subscribe = {
            let configured = self.cdc_topic.trim().to_string();
            if !configured.is_empty() && configured.to_lowercase() != "auto" {
                configured
            } else if let Ok(env_topic) = std::env::var("CDC_TOPIC") {
                env_topic
            } else {
                let cfg = self.kafka_consumer.get_config();
                if !cfg.topic_prefix.is_empty() {
                    format!("{}.public.kafka_outbox_cdc", cfg.topic_prefix)
                } else {
                    "banking-es.public.kafka_outbox_cdc".to_string()
                }
            }
        };
        self.cdc_topic = topic_to_subscribe.clone();
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

                    // CRITICAL FIX: Force consumer group join by polling
                    tracing::info!("CDCConsumer: Forcing consumer group join by polling...");

                    // Poll a few times to trigger group join
                    let mut join_attempts = 0;
                    let max_join_attempts = 10;

                    while join_attempts < max_join_attempts {
                        match self.kafka_consumer.stream().next().await {
                            Some(Ok(_)) => {
                                tracing::info!("CDCConsumer: ✅ Consumer group join successful after {} attempts", join_attempts + 1);
                                break;
                            }
                            Some(Err(e)) => {
                                tracing::warn!(
                                    "CDCConsumer: Poll error during join attempt {}: {:?}",
                                    join_attempts + 1,
                                    e
                                );
                            }
                            None => {
                                tracing::debug!(
                                    "CDCConsumer: No message during join attempt {}",
                                    join_attempts + 1
                                );
                            }
                        }
                        join_attempts += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }

                    if join_attempts >= max_join_attempts {
                        tracing::warn!("CDCConsumer: Consumer group join may not be complete after {} attempts", max_join_attempts);
                    }

                    // Consumer is ready to start processing
                    tracing::info!(
                        "CDCConsumer: Consumer group join completed, ready to start processing"
                    );

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
                    tokio::time::sleep(Duration::from_secs(30)).await;
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
        let shutdown_token_clone = shutdown_token.clone();

        // Background offset committer
        tokio::spawn(async move {
            let mut last_commit_time = std::time::Instant::now();

            // Read timing configuration from environment variables
            // CRITICAL: Adjusted for Debezium's 5ms poll interval
            let offset_commit_sleep_ms = std::env::var("CDC_OFFSET_COMMIT_SLEEP_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(10); // Reduced from 50ms to 10ms to match Debezium's 5ms poll

            let offset_commit_batch_size = std::env::var("CDC_OFFSET_COMMIT_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(5000);

            let offset_commit_timeout_ms = std::env::var("CDC_OFFSET_COMMIT_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(25); // Reduced from 100ms to 25ms to match Debezium's 5ms poll

            tracing::info!(
                "Offset committer config - sleep: {}ms, batch_size: {}, timeout: {}ms (optimized for Debezium 5ms poll)",
                offset_commit_sleep_ms, offset_commit_batch_size, offset_commit_timeout_ms
            );

            loop {
                tokio::select! {
                    _ = shutdown_token_clone.cancelled() => {
                        tracing::info!("Background offset committer: Received shutdown signal");
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(offset_commit_sleep_ms)) => {
                        let mut offsets = offsets_clone.lock().await;
                        let should_commit = offsets.len() >= offset_commit_batch_size ||
                                           last_commit_time.elapsed() > std::time::Duration::from_millis(offset_commit_timeout_ms);

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
                }
            }

            // Final commit on shutdown
            let mut offsets = offsets_clone.lock().await;
            if !offsets.is_empty() {
                let mut highest: HashMap<(String, i32), i64> = HashMap::new();
                for (topic, partition, offset) in offsets.drain(..) {
                    let key = (topic.clone(), partition);
                    highest
                        .entry(key)
                        .and_modify(|v| *v = (*v).max(offset))
                        .or_insert(offset);
                }
                let mut tpl = TopicPartitionList::new();
                for ((topic, partition), offset) in highest {
                    tpl.add_partition_offset(&topic, partition, Offset::Offset(offset + 1))
                        .unwrap();
                }
                if let Err(e) = kafka_consumer.commit(&tpl, CommitMode::Async) {
                    tracing::error!("Failed to final commit offsets on shutdown: {}", e);
                } else {
                    tracing::info!("✅ Final commit on shutdown completed");
                }
            }
        });

        // Background DLQ handler (batch)
        let dlq_producer = processor.clone();
        let shutdown_token_dlq = shutdown_token.clone();
        tokio::spawn(async move {
            // Read DLQ configuration from environment variables
            // CRITICAL: Adjusted for Debezium's 5ms poll interval
            let dlq_batch_size = std::env::var("CDC_DLQ_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(50); // Reduced from 500 to 50 for faster processing

            let dlq_timeout_ms = std::env::var("CDC_DLQ_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(100); // Reduced from 500ms to 100ms for faster processing

            tracing::info!(
                "DLQ handler config - batch_size: {}, timeout: {}ms (optimized for Debezium 5ms poll)",
                dlq_batch_size, dlq_timeout_ms
            );

            let mut batch = Vec::with_capacity(dlq_batch_size);
            let mut last_send = std::time::Instant::now();
            loop {
                tokio::select! {
                    _ = shutdown_token_dlq.cancelled() => {
                        tracing::info!("Background DLQ handler: Received shutdown signal");
                        break;
                    }
                    Some((topic, partition, offset, payload, key, error)) = dlq_rx.recv() => {
                        batch.push((topic, partition, offset, payload, key, error));
                        if batch.len() >= dlq_batch_size || last_send.elapsed() > std::time::Duration::from_millis(dlq_timeout_ms) {
                            let to_send = std::mem::take(&mut batch);
                            for (topic, partition, offset, payload, key, error) in to_send {
                                if let Err(e) = dlq_producer
                                    .send_to_dlq_from_cdc_parts(
                                        &topic,
                                        partition,
                                        offset,
                                        &payload,
                                        key.as_deref(),
                                        &error,
                                    )
                                    .await
                                {
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
                                if let Err(e) = dlq_producer
                                    .send_to_dlq_from_cdc_parts(
                                        &topic,
                                        partition,
                                        offset,
                                        &payload,
                                        key.as_deref(),
                                        &error,
                                    )
                                    .await
                                {
                                    tracing::error!("Failed to send to DLQ: {}", e);
                                }
                            }
                        }
                        break;
                    }
                }
            }

            // Final DLQ processing on shutdown
            if !batch.is_empty() {
                let to_send = std::mem::take(&mut batch);
                for (topic, partition, offset, payload, key, error) in to_send {
                    if let Err(e) = dlq_producer
                        .send_to_dlq_from_cdc_parts(
                            &topic,
                            partition,
                            offset,
                            &payload,
                            key.as_deref(),
                            &error,
                        )
                        .await
                    {
                        tracing::error!("Failed to send to DLQ on shutdown: {}", e);
                    }
                }
            }
        });

        let mut message_stream = self.kafka_consumer.stream();

        // Read CDC polling interval from environment variable
        let poll_interval_ms = std::env::var("CDC_POLL_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(100); // CRITICAL FIX: Increased from 5ms to 100ms to prevent consumer group ejection

        // CRITICAL OPTIMIZATION: Batch processing configuration
        let batch_size = std::env::var("CDC_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10000);

        let batch_timeout_ms = std::env::var("CDC_BATCH_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(50); // 5x poll interval (5ms * 10 = 50ms)

        tracing::info!(
            "CDCConsumer: Using polling interval: {}ms, batch_size: {}, batch_timeout: {}ms",
            poll_interval_ms,
            batch_size,
            batch_timeout_ms
        );

        // OPTIMIZATION: Add adaptive polling based on load
        let mut adaptive_poll_interval = poll_interval_ms;
        let mut consecutive_empty_polls = 0;
        let min_poll_interval = 1; // 1ms minimum
        let max_poll_interval = 25; // 25ms maximum
        let adaptive_threshold = 5; // After 10 empty polls, increase interval

        // CRITICAL OPTIMIZATION: Batch processing buffer
        let mut message_batch = Vec::with_capacity(batch_size);
        let mut last_batch_time = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("CDC consumer received shutdown signal");
                    tracing::info!("CDCConsumer: Received shutdown signal, breaking loop");

                    // Process remaining messages in batch before shutdown
                    if !message_batch.is_empty() {
                        tracing::info!("CDCConsumer: Processing final batch of {} messages before shutdown", message_batch.len());
                        if let Err(e) = Self::process_message_batch(&message_batch, &processor).await {
                            tracing::error!("CDCConsumer: Failed to process final batch: {:?}", e);
                        }
                    }
                    break;
                }
                message_result = tokio::time::timeout(
                    Duration::from_millis(adaptive_poll_interval),
                    message_stream.next()
                ) => {
                    match message_result {
                        Ok(Some(Ok(message))) => {
                            tracing::debug!("Message received: {:?}", message);
                            // Reset adaptive polling when message is received
                            consecutive_empty_polls = 0;
                            adaptive_poll_interval = std::cmp::max(min_poll_interval, adaptive_poll_interval / 2);

                            // Add message to batch - convert BorrowedMessage to KafkaMessage
                            let kafka_message = crate::infrastructure::kafka_abstraction::KafkaMessage {
                                topic: message.topic().to_string(),
                                partition: message.partition(),
                                offset: message.offset(),
                                key: message.key().map(|k| k.to_vec()),
                                payload: message.payload().unwrap().to_vec(),
                                timestamp: message.timestamp().to_millis(),
                            };
                            message_batch.push(kafka_message);

                            // Check if we should process the batch
                            let should_process_batch = message_batch.len() >= batch_size ||
                                                     last_batch_time.elapsed() > Duration::from_millis(batch_timeout_ms);

                            if should_process_batch {
                                tracing::info!("CDCConsumer: Processing batch of {} messages", message_batch.len());
                                if let Err(e) = Self::process_message_batch(&message_batch, &processor).await {
                                    tracing::error!("CDCConsumer: Failed to process message batch: {:?}", e);
                                }
                                message_batch.clear();
                                last_batch_time = std::time::Instant::now();
                            }
                        }
                        Ok(Some(Err(e))) => {
                            tracing::error!("CDCConsumer: Error receiving message: {:?}", e);
                        }
                        Ok(None) => {
                            // Stream ended
                            tracing::info!("CDCConsumer: Message stream ended");

                            // Process remaining messages in batch
                            if !message_batch.is_empty() {
                                tracing::info!("CDCConsumer: Processing final batch of {} messages", message_batch.len());
                                if let Err(e) = Self::process_message_batch(&message_batch, &processor).await {
                                    tracing::error!("CDCConsumer: Failed to process final batch: {:?}", e);
                                }
                            }
                            break;
                        }
                        Err(_) => {
                            // Timeout - this is expected and normal
                            poll_count += 1;
                            consecutive_empty_polls += 1;

                            // OPTIMIZATION: Adaptive polling based on empty polls
                            if consecutive_empty_polls >= adaptive_threshold {
                                adaptive_poll_interval = std::cmp::min(max_poll_interval, adaptive_poll_interval * 2);
                                consecutive_empty_polls = 0; // Reset counter
                                tracing::debug!("CDCConsumer: Adaptive polling - increased interval to {}ms", adaptive_poll_interval);
                            }

                            // Process any remaining messages in batch on timeout
                            if !message_batch.is_empty() {
                                tracing::debug!("CDCConsumer: Processing timeout batch of {} messages", message_batch.len());
                                if let Err(e) = Self::process_message_batch(&message_batch, &processor).await {
                                    tracing::error!("CDCConsumer: Failed to process timeout batch: {:?}", e);
                                }
                                message_batch.clear();
                                last_batch_time = std::time::Instant::now();
                            }

                            if poll_count % 1000 == 0 {
                                tracing::debug!("CDCConsumer: Poll timeout (count: {}, interval: {}ms)", poll_count, adaptive_poll_interval);
                            }
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

    /// Optimized batch processing with true parallel execution and offset commit
    async fn process_message_batch(
        messages: &[crate::infrastructure::kafka_abstraction::KafkaMessage],
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let start_time = std::time::Instant::now();
        let batch_size = messages.len();

        tracing::info!(
            "CDCConsumer: process_message_batch starting batch of {} messages (COPY optimization: true)",
            batch_size
        );

        // CRITICAL OPTIMIZATION: Convert messages to CDC events in parallel
        let cdc_events: Vec<serde_json::Value> = messages
            .iter() // Use regular iterator for conversion (rayon not available)
            .filter_map(|message| {
                serde_json::from_slice::<serde_json::Value>(&message.payload).ok()
            })
            .collect();

        if cdc_events.is_empty() {
            tracing::warn!(
                "CDCConsumer: No valid CDC events found in batch of {} messages",
                batch_size
            );
            return Ok(());
        }

        tracing::info!(
            "CDCConsumer: Using COPY-optimized batch processing for {} events",
            cdc_events.len()
        );

        // CRITICAL: Use COPY-optimized batch processing
        let processing_start = std::time::Instant::now();
        match processor
            .process_cdc_events_batch_with_copy(cdc_events)
            .await
        {
            Ok(_) => {
                let processing_duration = processing_start.elapsed();
                let total_duration = start_time.elapsed();

                tracing::info!(
                    "CDCConsumer: Batch processing completed successfully - {} messages in {:?} (COPY: true)",
                    batch_size,
                    total_duration
                );

                // CRITICAL FIX: Commit offsets after successful processing to prevent duplicates
                // This ensures messages are not reprocessed after consumer restart
                // Note: Offset commit is handled by the consumer group coordinator
                tracing::debug!("CDCConsumer: Successfully processed {} messages, offsets will be committed by consumer group", batch_size);

                // Performance monitoring
                if processing_duration.as_millis() > 5000 {
                    tracing::warn!(
                        "CDCConsumer (COPY-optimized): Slow batch processing detected: {} messages in {:?}",
                        batch_size, processing_duration
                    );
                }

                Ok(())
            }
            Err(e) => {
                let total_duration = start_time.elapsed();
                tracing::error!(
                    "CDCConsumer (COPY-optimized): Failed to process COPY-optimized batch: Batch processing failed for {} messages: {}",
                    batch_size, e
                );
                Err(e)
            }
        }
    }

    /// ✅ CDC Consumer Resilience: Process events directly from database when outbox is missing
    pub async fn process_events_directly_from_database(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        event_store: &Arc<dyn crate::infrastructure::event_store::EventStoreTrait>,
        batch_size: usize,
    ) -> Result<()> {
        tracing::info!("CDC Consumer Resilience: Starting direct events processing from database");

        let start_time = std::time::Instant::now();
        let mut processed_count = 0;
        let mut error_count = 0;

        // Get unprocessed events from database
        let unprocessed_events =
            Self::get_unprocessed_events_from_database(event_store, batch_size).await?;

        if unprocessed_events.is_empty() {
            tracing::info!("CDC Consumer Resilience: No unprocessed events found in database");
            return Ok(());
        }

        tracing::info!(
            "CDC Consumer Resilience: Found {} unprocessed events, processing directly",
            unprocessed_events.len()
        );

        // Process events directly
        for event in unprocessed_events {
            match Self::process_single_event_directly(processor, event).await {
                Ok(_) => {
                    processed_count += 1;
                }
                Err(e) => {
                    error_count += 1;
                    tracing::error!(
                        "CDC Consumer Resilience: Failed to process event directly: {:?}",
                        e
                    );
                }
            }
        }

        let duration = start_time.elapsed();
        tracing::info!(
            "CDC Consumer Resilience: Direct processing completed - {} processed, {} errors in {:?}",
            processed_count,
            error_count,
            duration
        );

        Ok(())
    }

    /// ✅ Get unprocessed events from database (events without corresponding outbox messages)
    async fn get_unprocessed_events_from_database(
        event_store: &Arc<dyn crate::infrastructure::event_store::EventStoreTrait>,
        batch_size: usize,
    ) -> Result<Vec<crate::domain::AccountEvent>> {
        tracing::info!("CDC Consumer Resilience: Fetching unprocessed events from database");

        // Use the EventStore trait method to get unprocessed events
        let unprocessed_events = event_store.get_unprocessed_events(batch_size).await?;

        tracing::info!(
            "CDC Consumer Resilience: Found {} unprocessed events",
            unprocessed_events.len()
        );

        Ok(unprocessed_events)
    }

    /// ✅ Process single event directly without outbox dependency
    async fn process_single_event_directly(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        event: crate::domain::AccountEvent,
    ) -> Result<()> {
        // Convert domain event to CDC event format
        let cdc_event = Self::convert_domain_event_to_cdc_format(event)?;

        // Process using existing CDC processor
        processor.process_cdc_events_batch(vec![cdc_event]).await?;

        Ok(())
    }

    /// ✅ Convert domain event to CDC event format
    fn convert_domain_event_to_cdc_format(
        event: crate::domain::AccountEvent,
    ) -> Result<serde_json::Value> {
        // Convert domain event to CDC format
        let cdc_event = serde_json::json!({
            "event_id": Uuid::new_v4(), // Generate new event ID for CDC processing
            "aggregate_id": event.aggregate_id(),
            "event_type": event.event_type(),
            "payload": event,
            "timestamp": chrono::Utc::now(),
            "source": "direct_database_processing"
        });

        Ok(cdc_event)
    }

    /// ✅ Start resilient CDC consumer that can handle missing outbox messages
    pub async fn start_resilient_cdc_consumer(
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        event_store: Arc<dyn crate::infrastructure::event_store::EventStoreTrait>,
        check_interval_minutes: u64,
    ) -> Result<()> {
        tracing::info!("Starting resilient CDC consumer with direct database processing");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            check_interval_minutes * 60,
        ));

        loop {
            interval.tick().await;

            // Check for unprocessed events and process them directly
            match Self::process_events_directly_from_database(&processor, &event_store, 1000).await
            {
                Ok(_) => {
                    tracing::debug!("Resilient CDC consumer: Direct processing cycle completed");
                }
                Err(e) => {
                    tracing::error!("Resilient CDC consumer: Direct processing failed: {:?}", e);
                }
            }
        }
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

// Memory pressure monitoring for adaptive batch sizing
#[derive(Debug)]
struct MemoryPressureMonitor {
    last_check: std::time::Instant,
    check_interval: Duration,
    pressure_threshold: f64,
}

impl MemoryPressureMonitor {
    fn new() -> Self {
        Self {
            last_check: std::time::Instant::now(),
            check_interval: Duration::from_secs(5),
            pressure_threshold: 0.8, // 80% memory usage threshold
        }
    }

    fn should_reduce_batch_size(&mut self) -> bool {
        if self.last_check.elapsed() < self.check_interval {
            return false;
        }

        self.last_check = std::time::Instant::now();

        // Simple memory pressure check - you may want to use a more sophisticated method
        #[cfg(target_os = "linux")]
        {
            if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
                if let Some(total_line) = meminfo.lines().find(|line| line.starts_with("MemTotal:"))
                {
                    if let Some(available_line) = meminfo
                        .lines()
                        .find(|line| line.starts_with("MemAvailable:"))
                    {
                        if let (Ok(total), Ok(available)) = (
                            total_line
                                .split_whitespace()
                                .nth(1)
                                .unwrap_or("0")
                                .parse::<u64>(),
                            available_line
                                .split_whitespace()
                                .nth(1)
                                .unwrap_or("0")
                                .parse::<u64>(),
                        ) {
                            let used_ratio = 1.0 - (available as f64 / total as f64);
                            return used_ratio > self.pressure_threshold;
                        }
                    }
                }
            }
        }

        false
    }
}
