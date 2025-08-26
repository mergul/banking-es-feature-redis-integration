use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use crate::infrastructure::{AccountProjection, CopyOptimizationConfig, TransactionProjection};
use anyhow::Context;
use anyhow::{anyhow, Result};
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
    BusinessLogicConfig, UltraOptimizedCDCEventProcessor,
};
use crate::infrastructure::cdc_event_processor::{
    EventBatches, ProcessableEvent, ProjectionBatches,
};
use crate::infrastructure::cdc_integration_helper::{
    CDCIntegrationConfig, CDCIntegrationHelper, CDCIntegrationHelperBuilder,
    MigrationIntegrityReport, MigrationStats,
};
use crate::infrastructure::cdc_producer::{BusinessLogicValidator, CDCProducer, CDCProducerConfig};
use crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics;
use crate::infrastructure::event_processor::EventProcessor;
use futures::future::join_all;
use futures::stream::StreamExt;
use rayon::prelude::*;
use sqlx::types::JsonValue;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt as TokioStreamExt;
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
                "connector.max.sleep.time": "100",
                "max.sleep.time": "50",
                "flush.lsn.source": "true",
                "publication.autocreate.mode": "filtered",
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

// /// Enhanced CDC Event Processor - now uses the optimized processor
// pub struct EnhancedCDCEventProcessor {
//     // Use the optimized event processor
//     optimized_processor: UltraOptimizedCDCEventProcessor,
//     // Keep the old metrics for backward compatibility
//     metrics: Arc<EnhancedCDCMetrics>,
// }

// impl EnhancedCDCEventProcessor {
//     pub fn new(
//         kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
//         cache_service: Arc<dyn CacheServiceTrait>,
//         projection_store: Arc<dyn ProjectionStoreTrait>,
//         business_config: Option<BusinessLogicConfig>,
//         consistency_manager: Option<
//             Arc<crate::infrastructure::consistency_manager::ConsistencyManager>,
//         >,
//         cdc_batching_service: Option<
//             Arc<crate::infrastructure::cdc_write_service::OptimizedCDCProcessor>,
//         >,
//     ) -> Self {
//         let metrics = Arc::new(EnhancedCDCMetrics::default());
//         let optimized_processor = UltraOptimizedCDCEventProcessor::new(
//             kafka_producer,
//             cache_service,
//             projection_store,
//             metrics.clone(),
//             business_config,
//             None, // Use default performance config
//             consistency_manager,
//             cdc_batching_service,
//         );

//         Self {
//             optimized_processor,
//             metrics,
//         }
//     }

//     /// Process CDC event from Debezium using the optimized processor
//     pub async fn process_cdc_event(&self, cdc_event: serde_json::Value) -> Result<()> {
//         let start_time = std::time::Instant::now();
//         tracing::info!(
//             "🔍 CDC Event Processor: Starting to process CDC event with optimized processor"
//         );
//         tracing::info!("🔍 CDC Event Processor: Event details: {:?}", cdc_event);

//         // Use the optimized processor
//         match self
//             .optimized_processor
//             .process_cdc_event_ultra_fast(cdc_event)
//             .await
//         {
//             Ok(_) => {
//                 // Update legacy metrics for backward compatibility
//                 let latency = start_time.elapsed().as_millis() as u64;
//                 self.metrics
//                     .processing_latency_ms
//                     .fetch_add(latency, std::sync::atomic::Ordering::Relaxed);
//                 self.metrics
//                     .events_processed
//                     .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

//                 tracing::info!(
//                     "✅ CDC Event Processor: Event processed successfully with optimized processor (latency: {}ms)",
//                     latency
//                 );
//                 Ok(())
//             }
//             Err(e) => {
//                 self.metrics
//                     .events_failed
//                     .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
//                 error!("CDC Event Processor: Failed to process event: {}", e);
//                 tracing::error!("CDC Event Processor: Failed to process event: {}", e);
//                 Err(e)
//             }
//         }
//     }

//     /// Mark an aggregate as completed by CDC processing
//     pub async fn mark_aggregate_completed(&self, aggregate_id: Uuid) {
//         // This method can be called by external consistency managers
//         // when CDC events are successfully processed
//         info!(
//             "CDC Event Processor: Marked aggregate {} as completed",
//             aggregate_id
//         );

//         // Use the optimized processor to mark as completed
//         self.optimized_processor
//             .mark_aggregate_completed(aggregate_id)
//             .await;
//     }

//     /// Mark an aggregate as failed by CDC processing
//     pub async fn mark_aggregate_failed(&self, aggregate_id: Uuid, error: String) {
//         // This method can be called by external consistency managers
//         // when CDC events fail to process
//         error!(
//             "CDC Event Processor: Marked aggregate {} as failed: {}",
//             aggregate_id, error
//         );

//         // Use the optimized processor to mark as failed
//         self.optimized_processor
//             .mark_aggregate_failed(aggregate_id, error.clone())
//             .await;
//     }

//     /// Get metrics from the optimized processor
//     pub async fn get_optimized_metrics(
//         &self,
//     ) -> crate::infrastructure::cdc_service_manager::EnhancedCDCMetrics {
//         self.optimized_processor.get_metrics().await
//     }

//     /// Get legacy metrics for backward compatibility
//     pub fn get_metrics(&self) -> &EnhancedCDCMetrics {
//         &self.metrics
//     }

//     /// Get business logic configuration
//     pub async fn get_business_config(&self) -> BusinessLogicConfig {
//         self.optimized_processor.get_business_config().await
//     }

//     /// Update business logic configuration
//     pub async fn update_business_config(&mut self, config: BusinessLogicConfig) {
//         self.optimized_processor
//             .update_business_config(config)
//             .await
//     }

//     /// Start batch processor
//     pub async fn start_batch_processor(&mut self) -> Result<()> {
//         self.optimized_processor.start_batch_processor().await
//     }

//     /// Enable and start batch processor
//     pub async fn enable_and_start_batch_processor(&mut self) -> Result<()> {
//         self.optimized_processor
//             .enable_and_start_batch_processor()
//             .await
//     }

//     /// Check if batch processor is running
//     pub async fn is_batch_processor_running(&self) -> bool {
//         self.optimized_processor.is_batch_processor_running().await
//     }

//     /// Shutdown the processor
//     pub async fn shutdown(&mut self) -> Result<()> {
//         self.optimized_processor.shutdown().await
//     }
// }

// #[async_trait]
// impl EventProcessor for EnhancedCDCEventProcessor {
//     async fn process_event(&self, event: serde_json::Value) -> Result<()> {
//         self.process_cdc_event(event).await
//     }
// }

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
    // pub async fn start_consuming(
    //     &mut self,
    //     processor: Arc<EnhancedCDCEventProcessor>,
    // ) -> Result<()> {
    //     Err(anyhow::anyhow!(
    //         "Use start_consuming_with_cancellation_token instead"
    //     ))
    // }

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
        // Subscribe to CDC topic and join consumer group
        tracing::info!("CDCConsumer: Subscribing to topic: {}", self.cdc_topic);
        let handles = self
            .start_staged_processing(processor, shutdown_token)
            .await?;
        join_all(handles).await;
        Ok(())
    }

    /// Orchestrates the staged event processing pipeline.
    pub async fn start_staged_processing(
        &mut self,
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        shutdown_token: CancellationToken,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>> {
        info!("🚀 Initializing Staged Event Processing Pipeline...");
        const NUM_SHARDS: usize = 8;

        // --- Channel Setup ---
        const CHANNEL_CAPACITY: usize = 20000;

        // Create a set of channels for each shard
        // NEW: A single, shared channel for all account creations
        let (creation_tx, creation_rx) = mpsc::channel(CHANNEL_CAPACITY * NUM_SHARDS);
        let (modifications_txs, mut modifications_rxs): (
            Vec<mpsc::Sender<Vec<ProcessableEvent>>>,
            Vec<_>,
        ) = (0..NUM_SHARDS)
            .map(|_| mpsc::channel(CHANNEL_CAPACITY))
            .unzip();
        // NEW: A single, shared channel for all prepared account updates
        let (prepared_tx, prepared_rx) = mpsc::channel(CHANNEL_CAPACITY * NUM_SHARDS);
        // NEW: A single, shared channel for all transaction projections
        let (transactions_tx, transactions_rx) = mpsc::channel(CHANNEL_CAPACITY * NUM_SHARDS);

        // --- Task Spawning ---
        let mut handles = Vec::new();

        // Stage 1: Consume and Distribute (1 worker, distributing to N channels)
        let consume_handle = tokio::spawn(Self::consume_and_distribute_task(
            self.kafka_consumer.clone(),
            self.cdc_topic.clone(),
            processor.clone(),
            creation_tx.clone(), // Clone for the consumer task
            modifications_txs,
            shutdown_token.clone(),
        ));
        handles.push(consume_handle);
        info!("✅ Stage 1 (Consume & Distribute) worker started.");

        // Stage 2: Prepare Modifications (N workers, one per shard)
        for i in 0..NUM_SHARDS {
            let modifications_rx_for_worker = modifications_rxs.remove(0);
            let prepared_tx_for_worker = prepared_tx.clone(); // Clone the shared sender
                                                              // NEW: Clone the shared transactions sender for each worker
            let transactions_tx_for_worker = transactions_tx.clone();
            let prepare_handle = tokio::spawn(Self::prepare_modifications_task(
                processor.clone(),
                modifications_rx_for_worker,
                prepared_tx_for_worker,
                transactions_tx_for_worker, // Pass it in
                i,
            ));
            handles.push(prepare_handle);
            info!(
                "✅ Stage 2 (Prepare Modifications) worker for shard {} started.",
                i
            );
        }
        // Drop the original senders so channels close when all workers are done
        drop(creation_tx);
        drop(prepared_tx);
        // Drop the original sender so the channel closes when all workers are done
        drop(transactions_tx);

        // Stage 3: Write Account Creations and Updates to Database (Single dedicated worker)
        let account_write_handle = tokio::spawn(Self::write_to_database_task(
            processor.clone(),
            creation_rx, // Pass the single receiver
            prepared_rx, // Pass the single receiver
        ));
        handles.push(account_write_handle);
        info!("✅ Stage 3 (Account Writer) worker started.");
        // NEW Stage 3.5: Write Transaction Projections to Database (Single dedicated worker)
        let transaction_write_handle = tokio::spawn(Self::write_transactions_task(
            processor.clone(),
            transactions_rx, // Pass the receiver directly
            0,
        ));
        handles.push(transaction_write_handle);
        info!("✅ Stage 3.5 (Transaction Writer) worker started.");

        Ok(handles)
    }

    // Round-robin distribution to workers (no mutex contention)
    async fn parse_and_distribute_batch(
        messages: &[crate::infrastructure::kafka_abstraction::KafkaMessage],
        cdc_events_buffer: &mut Vec<serde_json::Value>,
        worker_senders: &[mpsc::Sender<Vec<serde_json::Value>>],
        worker_selector: &Arc<AtomicUsize>,
    ) {
        if messages.is_empty() {
            return;
        }

        let parse_start = std::time::Instant::now();

        // OPTIMIZATION: Parallelize JSON parsing using rayon
        let parsed_events: Vec<serde_json::Value> = messages
            .par_iter()
            .filter_map(|message| {
                let mut payload = message.payload.clone();
                simd_json::from_slice::<serde_json::Value>(&mut payload).ok()
            })
            .collect();

        cdc_events_buffer.clear();
        cdc_events_buffer.extend(parsed_events);

        if !cdc_events_buffer.is_empty() {
            // Round-robin selection of worker
            let worker_index =
                worker_selector.fetch_add(1, Ordering::Relaxed) % worker_senders.len();
            let selected_worker = &worker_senders[worker_index];

            let events_to_send = cdc_events_buffer.clone();

            // Try to send to selected worker
            match selected_worker.try_send(events_to_send) {
                Ok(_) => {
                    let parse_duration = parse_start.elapsed();
                    tracing::debug!(
                        "Distributed {} events to worker {} (parse: {:?})",
                        cdc_events_buffer.len(),
                        worker_index,
                        parse_duration
                    );
                }
                Err(mpsc::error::TrySendError::Full(events)) => {
                    // Worker is busy, try next worker
                    tracing::warn!("Worker {} busy, trying next worker", worker_index);
                    Self::try_distribute_to_available_worker(events, worker_senders, worker_index)
                        .await;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("Worker {} channel closed", worker_index);
                }
            }
        }
    }

    // Fallback to find available worker
    async fn try_distribute_to_available_worker(
        events: Vec<serde_json::Value>,
        worker_senders: &[mpsc::Sender<Vec<serde_json::Value>>],
        skip_worker: usize,
    ) {
        for (i, sender) in worker_senders.iter().enumerate() {
            if i == skip_worker {
                continue; // Skip the busy worker
            }

            match sender.try_send(events.clone()) {
                Ok(_) => {
                    tracing::debug!("Successfully distributed to fallback worker {}", i);
                    return;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    continue; // Try next worker
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("Fallback worker {} channel closed", i);
                }
            }
        }

        tracing::error!(
            "All workers busy, dropping batch of {} events",
            events.len()
        );
        // Consider sending to DLQ here instead of dropping
    }
    // --- Pipeline Stage Implementations ---

    /// STAGE 1: Consumes from Kafka, extracts events, and distributes them to channels.
    async fn consume_and_distribute_task(
        kafka_consumer: crate::infrastructure::kafka_abstraction::KafkaConsumer,
        cdc_topic: String,
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        creation_tx: mpsc::Sender<Vec<ProcessableEvent>>,
        modifications_txs: Vec<mpsc::Sender<Vec<ProcessableEvent>>>, // Stays sharded
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        kafka_consumer.subscribe_to_topic(&cdc_topic).await?;
        info!("Stage 1: Subscribed to topic '{}'", cdc_topic);

        let mut message_stream = kafka_consumer.stream();
        let batch_size = 5000;
        let batch_timeout = Duration::from_millis(50);
        let mut message_batch = Vec::with_capacity(batch_size);
        let mut last_batch_time = Instant::now();

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("Stage 1: Shutdown signal received.");
                    break;
                }
                message_result = tokio::time::timeout(batch_timeout, StreamExt::next(&mut message_stream)) => {
                    if let Ok(Some(Ok(message))) = message_result {
                        message_batch.push(message.detach());
                    }

                    let should_process = message_batch.len() >= batch_size || last_batch_time.elapsed() >= batch_timeout;

                    if should_process && !message_batch.is_empty() {
                        let batch_to_process = std::mem::take(&mut message_batch);
                        let raw_events: Vec<serde_json::Value> = batch_to_process
                            .par_iter()
                            .filter_map(|msg| {
                                // simd_json::from_slice requires &mut [u8], so we need a mutable copy.
                                // msg.payload() returns &[u8], so we convert to Vec<u8> and then take a mutable slice.
                                let mut payload_bytes = msg.payload()?.to_vec();
                                simd_json::from_slice(&mut payload_bytes).ok()
                            })
                            .collect();

                        if let Ok(event_batches) = processor.extract_events_parallel_optimized(&raw_events).await {
                            // Send all creations to the single, centralized writer task
                            if !event_batches.account_created.is_empty() {
                                if let Err(e) = creation_tx.send(event_batches.account_created).await {
                                    error!("Stage 1: Failed to send creation event batch: {}", e);
                                }
                            }

                            // Distribute modifications across sharded workers
                            let num_shards = modifications_txs.len();
                            if num_shards > 0 && !event_batches.transactions.is_empty() {
                                let mut modification_shards: Vec<Vec<_>> = vec![Vec::new(); num_shards];
                                for event in event_batches.transactions {
                                    let shard_id = (event.aggregate_id.as_u128() as usize) % num_shards;
                                    modification_shards[shard_id].push(event);
                                }

                                for i in 0..num_shards {
                                    if !modification_shards[i].is_empty() {
                                        if let Err(e) = modifications_txs[i].send(std::mem::take(&mut modification_shards[i])).await {
                                            error!("Stage 1: Failed to send modification event batch to shard {}: {}", i, e);
                                        }
                                    }
                                }
                            }
                        }

                        // Commit offsets for the processed batch
                        if let Some(last_message) = batch_to_process.last() {
                             let mut tpl = TopicPartitionList::new();
                             tpl.add_partition_offset(last_message.topic(), last_message.partition(), Offset::Offset(last_message.offset() + 1))?;
                             if let Err(e) = kafka_consumer.commit(&tpl, CommitMode::Async) {
                                 error!("Stage 1: Failed to commit offset: {}", e);
                             }
                        }
                        last_batch_time = Instant::now();
                    }
                }
            }
        }
        Ok(())
    }

    /// STAGE 2: Receives modification events, fetches current projections, and prepares them for update.
    async fn prepare_modifications_task(
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        mut modifications_rx: mpsc::Receiver<Vec<ProcessableEvent>>,
        prepared_tx: mpsc::Sender<ProjectionBatches>,
        transactions_tx: mpsc::Sender<Vec<TransactionProjection>>, // NEW: Channel for transactions
        shard_id: usize,
    ) -> Result<()> {
        while let Some(batch) = modifications_rx.recv().await {
            if batch.is_empty() {
                continue;
            }

            let event_batches = EventBatches {
                transactions: batch,
                ..Default::default()
            };

            match processor.prepare_projection_batches(event_batches).await {
                Ok(mut projection_batches) => {
                    if !projection_batches.transactions_to_create.is_empty() {
                        let txs_to_send =
                            std::mem::take(&mut projection_batches.transactions_to_create);
                        if let Err(e) = transactions_tx.send(txs_to_send).await {
                            error!(
                                "Stage 2 (Shard {}): Failed to send prepared transactions: {}",
                                shard_id, e
                            );
                        }
                    }
                    if !projection_batches.accounts_to_update.is_empty() {
                        if let Err(e) = prepared_tx.send(projection_batches).await {
                            error!(
                                "Stage 2 (Shard {}): Failed to send prepared batch: {}",
                                shard_id, e
                            );
                        }
                    }
                }
                Err(e) => error!(
                    "Stage 2 (Shard {}): Error preparing modification batch: {}",
                    shard_id, e
                ),
            }
        }
        Ok(())
    }

    /// STAGE 3: Receives prepared projections and writes them to the database in bulk.
    async fn write_to_database_task(
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        creations_rx: mpsc::Receiver<Vec<ProcessableEvent>>,
        prepared_rx: mpsc::Receiver<ProjectionBatches>, // This now only contains account updates
    ) -> Result<()> {
        // Batching parameters are tuned to be more "patient" for account updates.
        // The stream of account updates is less dense than transactions, so we wait
        // longer to accumulate larger, more efficient batches.
        let micro_batch_chunk_size = 100;
        let timeout = Duration::from_millis(500);

        // Map incoming batches to a common enum type without flattening them.
        let creations_stream = StreamExt::map(
            ReceiverStream::new(creations_rx),
            WriterInputBatch::Creation,
        );
        let updates_stream = StreamExt::map(ReceiverStream::new(prepared_rx), |batch| {
            WriterInputBatch::Update(batch.accounts_to_update)
        });

        // Merge the streams of batches.
        let merged_batch_stream = creations_stream.merge(updates_stream);

        // Use chunks_timeout to group the incoming batches into larger chunks for processing.
        let mut chunked_stream =
            TokioStreamExt::chunks_timeout(merged_batch_stream, micro_batch_chunk_size, timeout);
        tokio::pin!(chunked_stream);

        while let Some(chunk_of_batches) = StreamExt::next(&mut chunked_stream).await {
            let mut creation_events = Vec::new();
            let mut accounts_to_update = Vec::new();

            // Consolidate the chunk of micro-batches into two large vectors.
            for batch in chunk_of_batches {
                match batch {
                    WriterInputBatch::Creation(events) => {
                        creation_events.extend(events);
                    }
                    WriterInputBatch::Update(updates) => {
                        accounts_to_update.extend(updates);
                    }
                }
            }

            if !creation_events.is_empty() || !accounts_to_update.is_empty() {
                let accounts_to_create = if !creation_events.is_empty() {
                    processor.prepare_account_creations(creation_events).await?
                } else {
                    vec![]
                };

                // De-duplicate account updates before flushing. It's common for a single
                // batch to contain multiple updates for the same account. We only need
                // to write the most recent state to the database.
                let final_accounts_to_update = if !accounts_to_update.is_empty() {
                    let mut latest_updates: std::collections::HashMap<Uuid, AccountProjection> =
                        std::collections::HashMap::with_capacity(accounts_to_update.len());
                    // The `extend` in the loop above preserves the order of events from the stream.
                    // By iterating and inserting into the HashMap, we ensure that for any given ID,
                    // the last projection encountered in the batch is the one that remains. This is
                    // the correct "latest state" for this batch.
                    for (id, projection) in accounts_to_update {
                        latest_updates.insert(id, projection);
                    }
                    latest_updates.into_iter().collect()
                } else {
                    vec![]
                };

                Self::flush_write_batches(&processor, accounts_to_create, final_accounts_to_update)
                    .await?;
            }
        }

        info!("Account writer: Channels closed. Exiting.");
        Ok(())
    }

    /// NEW Stage 3.5: Receives prepared transaction projections and writes them to the database in parallel.
    async fn write_transactions_task(
        processor: Arc<UltraOptimizedCDCEventProcessor>,
        transactions_rx: mpsc::Receiver<Vec<TransactionProjection>>,
        _shard_id: usize,
    ) -> Result<()> {
        // Batching parameters: group multiple incoming micro-batches of transactions together.
        // Tuned to be more patient to accommodate the upstream idempotency check latency.
        let micro_batch_chunk_size = 100;
        let timeout = Duration::from_millis(200);

        // This stream receives Vec<TransactionProjection>
        let transaction_batch_stream = ReceiverStream::new(transactions_rx);

        // Group the incoming Vecs into larger chunks for processing.
        let mut chunked_stream = TokioStreamExt::chunks_timeout(
            transaction_batch_stream,
            micro_batch_chunk_size,
            timeout,
        );
        tokio::pin!(chunked_stream);

        while let Some(chunk_of_batches) = StreamExt::next(&mut chunked_stream).await {
            // Flatten the collected batches into a single large Vec for flushing.
            let consolidated_batch: Vec<TransactionProjection> =
                chunk_of_batches.into_iter().flatten().collect();

            if !consolidated_batch.is_empty() {
                Self::flush_transaction_batch(&processor, consolidated_batch).await?;
            }
        }

        info!("Transaction writer: Channel closed. Exiting.");
        Ok(())
    }

    /// Helper to flush write batches to the database.
    async fn flush_write_batches(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        accounts_to_create: Vec<(Uuid, AccountProjection)>,
        accounts_to_update: Vec<(Uuid, AccountProjection)>,
    ) -> Result<()> {
        let batching_service = processor
            .get_cdc_batching_service()
            .ok_or_else(|| anyhow::anyhow!("CDC Batching Service not available"))?;

        let creation_ids: Vec<Uuid> = accounts_to_create.iter().map(|(id, _)| *id).collect();
        let update_ids: Vec<Uuid> = accounts_to_update.iter().map(|(id, _)| *id).collect();

        let create_count = accounts_to_create.len();
        let update_count = accounts_to_update.len();

        if create_count == 0 && update_count == 0 {
            return Ok(());
        }

        info!(
            "Stage 3: Flushing to DB - Creates: {}, Updates: {}",
            create_count, update_count
        );

        let (create_res, update_res) = tokio::join!(
            async {
                if create_count > 0 {
                    batching_service
                        .submit_account_creations_bulk(accounts_to_create)
                        .await
                } else {
                    Ok(vec![])
                }
            },
            async {
                if update_count > 0 {
                    batching_service
                        .submit_account_updates_bulk(accounts_to_update)
                        .await
                } else {
                    Ok(vec![])
                }
            }
        );

        create_res.context("Failed to submit account creations")?;
        update_res.context("Failed to submit account updates")?;

        // Invalidate cache for successfully written projections
        processor.invalidate_projection_cache(&update_ids).await;

        Ok(())
    }

    /// NEW: Helper to flush only transaction batches.
    async fn flush_transaction_batch(
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        transactions_to_create: Vec<TransactionProjection>,
    ) -> Result<()> {
        let batching_service = processor
            .get_cdc_batching_service()
            .ok_or_else(|| anyhow::anyhow!("CDC Batching Service not available"))?;

        let tx_count = transactions_to_create.len();
        if tx_count == 0 {
            return Ok(());
        }

        info!("Stage 3.5: Flushing {} transactions to DB", tx_count);

        batching_service
            .submit_transaction_creations_bulk(transactions_to_create)
            .await
            .context("Failed to submit transaction creations")?;

        Ok(())
    }

    fn convert_message_fast(
        message: rdkafka::message::BorrowedMessage,
    ) -> crate::infrastructure::kafka_abstraction::KafkaMessage {
        crate::infrastructure::kafka_abstraction::KafkaMessage {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            key: message.key().map(|k| k.to_vec()),
            payload: message.payload().unwrap_or(&[]).to_vec(),
            timestamp: message.timestamp().to_millis(),
        }
    }
    async fn process_message_batch_optimized(
        messages: &[crate::infrastructure::kafka_abstraction::KafkaMessage],
        processor: &Arc<UltraOptimizedCDCEventProcessor>,
        cdc_events_buffer: &mut Vec<serde_json::Value>, // Reuse buffer
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let start_time = std::time::Instant::now();
        let batch_size = messages.len();

        // OPTIMIZATION 10: Clear and reuse buffer instead of allocating new
        cdc_events_buffer.clear();
        cdc_events_buffer.reserve(batch_size);

        // OPTIMIZATION 11: Use rayon for parallel JSON parsing if available
        // Otherwise use efficient sequential parsing
        for message in messages {
            if let Ok(event) =
                simd_json::from_slice::<serde_json::Value>(&mut message.payload.clone())
            {
                cdc_events_buffer.push(event);
            }
        }

        if cdc_events_buffer.is_empty() {
            tracing::warn!("No valid CDC events in batch of {} messages", batch_size);
            return Ok(());
        }

        // OPTIMIZATION 12: Use optimized batch processing
        match processor.process_cdc_events_batch(cdc_events_buffer).await {
            Ok(_) => {
                let duration = start_time.elapsed();
                tracing::debug!(
                    "Batch processed: {} messages in {:?} ({:.2} msg/sec)",
                    batch_size,
                    duration,
                    batch_size as f64 / duration.as_secs_f64()
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!("Batch processing failed: {}", e);
                Err(e)
            }
        }
    }

    // OPTIMIZATION 13: Efficient batched offset commits
    async fn commit_offsets_batch(
        kafka_consumer: &crate::infrastructure::kafka_abstraction::KafkaConsumer,
        offsets: &[(String, i32, i64)],
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        // Get highest offset per (topic, partition)
        let mut highest: std::collections::HashMap<(String, i32), i64> =
            std::collections::HashMap::new();
        for (topic, partition, offset) in offsets {
            let key = (topic.clone(), *partition);
            highest
                .entry(key)
                .and_modify(|v| *v = (*v).max(*offset))
                .or_insert(*offset);
        }

        let mut tpl = rdkafka::TopicPartitionList::new();
        for ((topic, partition), offset) in highest.clone() {
            tpl.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset + 1))
                .map_err(|e| anyhow::anyhow!("Failed to add partition offset: {}", e))?;
        }

        kafka_consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            .map_err(|e| anyhow::anyhow!("Failed to commit offsets: {}", e))?;

        tracing::debug!("Committed {} partition offsets", highest.len());
        Ok(())
    }

    // async fn process_cdc_message(
    //     &self,
    //     message: crate::infrastructure::kafka_abstraction::KafkaMessage,
    //     processor: &Arc<tokio::sync::Mutex<EnhancedCDCEventProcessor>>,
    // ) -> Result<()> {
    //     // Parse CDC event from Kafka message
    //     let cdc_event: serde_json::Value = serde_json::from_slice(&message.payload)?;

    //     // Process the CDC event
    //     processor.lock().await.process_cdc_event(cdc_event).await?;

    //     Ok(())
    // }
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

/// An enum to represent the different types of batches the writer task can receive.
/// This avoids flattening the batches into individual items prematurely.
enum WriterInputBatch {
    /// A batch of account creation events.
    Creation(Vec<ProcessableEvent>),
    /// A batch of account update projections.
    Update(Vec<(Uuid, AccountProjection)>),
}
