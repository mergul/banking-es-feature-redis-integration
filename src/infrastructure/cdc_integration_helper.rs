use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Row, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::infrastructure::outbox::{OutboxMessage, OutboxStatus, PersistedOutboxMessage};

/// Configuration for CDC integration operations
#[derive(Debug, Clone)]
pub struct CDCIntegrationConfig {
    pub batch_size: usize,
    pub migration_chunk_size: usize,
    pub enable_validation: bool,
    pub cleanup_old_messages: bool,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
}

impl Default for CDCIntegrationConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            migration_chunk_size: 500,
            enable_validation: true,
            cleanup_old_messages: false,
            max_retries: 3,
            retry_delay_ms: 100,
        }
    }
}

/// Migration statistics
#[derive(Debug, Default)]
pub struct MigrationStats {
    pub total_messages: usize,
    pub migrated_messages: usize,
    pub failed_messages: usize,
    pub skipped_messages: usize,
    pub duplicate_messages: usize,
    pub processing_time_ms: u64,
}

/// Enhanced integration helper for CDC system
pub struct CDCIntegrationHelper {
    config: CDCIntegrationConfig,
    pool: sqlx::PgPool,
}

impl CDCIntegrationHelper {
    pub fn new(pool: sqlx::PgPool, config: CDCIntegrationConfig) -> Self {
        Self { config, pool }
    }

    /// Convert existing outbox message to CDC format with enhanced validation
    pub fn convert_to_cdc_message(&self, outbox_msg: &OutboxMessage) -> Result<CDCOutboxMessage> {
        // Validate input message
        if self.config.enable_validation {
            self.validate_outbox_message(outbox_msg)?;
        }

        Ok(CDCOutboxMessage {
            id: Uuid::new_v4(), // Generate new ID for CDC table
            aggregate_id: outbox_msg.aggregate_id,
            event_id: outbox_msg.event_id,
            event_type: outbox_msg.event_type.clone(),
            topic: outbox_msg.topic.clone(),
            metadata: outbox_msg.metadata.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    /// Convert persisted outbox message to CDC format
    pub fn convert_persisted_to_cdc_message(
        &self,
        persisted_msg: &PersistedOutboxMessage,
    ) -> Result<CDCOutboxMessage> {
        // Validate that the message is in a suitable state for migration
        if persisted_msg.status != OutboxStatus::Processed
            && persisted_msg.status != OutboxStatus::Pending
        {
            return Err(anyhow::anyhow!(
                "Cannot migrate message {} with status {:?}",
                persisted_msg.id,
                persisted_msg.status
            ));
        }

        Ok(CDCOutboxMessage {
            id: Uuid::new_v4(),
            aggregate_id: persisted_msg.aggregate_id,
            event_id: persisted_msg.event_id,
            event_type: persisted_msg.event_type.clone(),
            topic: persisted_msg.topic.clone(),
            metadata: persisted_msg.metadata.clone(),
            created_at: persisted_msg.created_at,
            updated_at: Utc::now(),
        })
    }

    /// Batch convert multiple messages with optimized processing
    pub fn batch_convert_to_cdc_messages(
        &self,
        outbox_msgs: &[OutboxMessage],
    ) -> Result<Vec<CDCOutboxMessage>> {
        let mut cdc_messages = Vec::with_capacity(outbox_msgs.len());
        let mut errors = Vec::new();

        for (index, msg) in outbox_msgs.iter().enumerate() {
            match self.convert_to_cdc_message(msg) {
                Ok(cdc_msg) => cdc_messages.push(cdc_msg),
                Err(e) => {
                    errors.push((index, e));
                    if !self.config.enable_validation {
                        // Skip invalid messages if validation is disabled
                        continue;
                    }
                }
            }
        }

        if !errors.is_empty() && self.config.enable_validation {
            return Err(anyhow::anyhow!(
                "Failed to convert {} messages: {:?}",
                errors.len(),
                errors
            ));
        }

        Ok(cdc_messages)
    }

    /// Migrate existing outbox to CDC format with enhanced error handling and progress tracking
    pub async fn migrate_existing_outbox(
        &self,
        old_repo: &crate::infrastructure::outbox::PostgresOutboxRepository,
        new_repo: &super::CDCOutboxRepository,
    ) -> Result<MigrationStats> {
        let start_time = std::time::Instant::now();
        let mut stats = MigrationStats::default();

        info!(
            "Starting CDC migration with chunk size: {}",
            self.config.migration_chunk_size
        );

        // Get total count for progress tracking
        let total_count = self.get_total_outbox_count().await?;
        stats.total_messages = total_count;
        info!("Total messages to migrate: {}", total_count);

        let mut offset = 0;
        let mut processed = 0;

        // Create a transaction for the migration
        let mut tx = self.pool.begin().await?;

        while processed < total_count {
            let batch_result = self
                .migrate_batch(old_repo, new_repo, &mut tx, offset, &mut stats)
                .await;

            match batch_result {
                Ok(batch_processed) => {
                    processed += batch_processed;
                    offset += self.config.migration_chunk_size;

                    // Log progress every 10 batches
                    if processed % (self.config.migration_chunk_size * 10) == 0 {
                        info!(
                            "Migration progress: {}/{} messages processed",
                            processed, total_count
                        );
                    }
                }
                Err(e) => {
                    error!("Migration batch failed: {}", e);
                    stats.failed_messages += self.config.migration_chunk_size;

                    // Rollback transaction on critical failure
                    tx.rollback().await?;
                    return Err(e);
                }
            }

            // Break if we've processed all available messages
            if processed >= total_count {
                break;
            }
        }

        // Commit the transaction
        tx.commit().await?;

        stats.processing_time_ms = start_time.elapsed().as_millis() as u64;

        info!("Migration completed: {:?}", stats);
        Ok(stats)
    }

    /// Migrate a single batch of messages
    async fn migrate_batch(
        &self,
        old_repo: &crate::infrastructure::outbox::PostgresOutboxRepository,
        new_repo: &super::CDCOutboxRepository,
        tx: &mut Transaction<'_, Postgres>,
        offset: usize,
        stats: &mut MigrationStats,
    ) -> Result<usize> {
        // Fetch batch of messages from old outbox
        let old_messages = self.fetch_outbox_batch(offset).await?;

        if old_messages.is_empty() {
            return Ok(0);
        }

        info!(
            "Processing migration batch: {} messages",
            old_messages.len()
        );

        // Convert to CDC format
        let mut cdc_messages = Vec::new();
        let mut retry_count = 0;

        while retry_count < self.config.max_retries {
            match self
                .convert_batch_with_deduplication(&old_messages, &mut cdc_messages, stats)
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= self.config.max_retries {
                        return Err(e);
                    }
                    warn!("Batch conversion failed (attempt {}): {}", retry_count, e);
                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.retry_delay_ms,
                    ))
                    .await;
                }
            }
        }

        // Insert into CDC outbox
        if !cdc_messages.is_empty() {
            self.insert_cdc_batch(new_repo, tx, cdc_messages, stats)
                .await?;
        }

        Ok(old_messages.len())
    }

    /// Convert batch with deduplication logic
    async fn convert_batch_with_deduplication(
        &self,
        old_messages: &[PersistedOutboxMessage],
        cdc_messages: &mut Vec<CDCOutboxMessage>,
        stats: &mut MigrationStats,
    ) -> Result<()> {
        let mut event_ids_seen = HashMap::new();
        cdc_messages.clear();

        for msg in old_messages {
            // Check for duplicates
            if event_ids_seen.contains_key(&msg.event_id) {
                stats.duplicate_messages += 1;
                continue;
            }

            // Skip failed messages if configured
            if msg.status == OutboxStatus::Failed {
                stats.skipped_messages += 1;
                continue;
            }

            match self.convert_persisted_to_cdc_message(msg) {
                Ok(cdc_msg) => {
                    event_ids_seen.insert(msg.event_id, true);
                    cdc_messages.push(cdc_msg);
                }
                Err(e) => {
                    error!("Failed to convert message {}: {}", msg.id, e);
                    stats.failed_messages += 1;
                }
            }
        }

        Ok(())
    }

    /// Insert CDC batch with optimized performance
    async fn insert_cdc_batch(
        &self,
        new_repo: &super::CDCOutboxRepository,
        tx: &mut Transaction<'_, Postgres>,
        cdc_messages: Vec<CDCOutboxMessage>,
        stats: &mut MigrationStats,
    ) -> Result<()> {
        // Convert to OutboxMessage format for compatibility
        let outbox_messages: Vec<OutboxMessage> = cdc_messages
            .into_iter()
            .map(|cdc_msg| OutboxMessage {
                aggregate_id: cdc_msg.aggregate_id,
                event_id: cdc_msg.event_id,
                event_type: cdc_msg.event_type,
                payload: Vec::new(), // This would need to be populated based on your specific needs
                topic: cdc_msg.topic,
                metadata: cdc_msg.metadata,
            })
            .collect();

        // Use the existing add_pending_messages method
        new_repo.add_pending_messages(tx, outbox_messages).await?;
        stats.migrated_messages += outbox_messages.len();

        Ok(())
    }

    /// Get total count of messages in old outbox
    async fn get_total_outbox_count(&self) -> Result<usize> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM outbox_messages")
            .fetch_one(&self.pool)
            .await?;

        Ok(row.get::<i64, _>("count") as usize)
    }

    /// Fetch a batch of messages from old outbox
    async fn fetch_outbox_batch(&self, offset: usize) -> Result<Vec<PersistedOutboxMessage>> {
        let rows = sqlx::query_as!(
            PersistedOutboxMessage,
            r#"
            SELECT id, aggregate_id, event_id, event_type, payload, topic, metadata, 
                   status as "status: OutboxStatus", created_at, updated_at, 
                   last_attempted_at, attempt_count, last_error
            FROM outbox_messages 
            ORDER BY created_at ASC 
            LIMIT $1 OFFSET $2
            "#,
            self.config.migration_chunk_size as i64,
            offset as i64
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Validate outbox message before conversion
    fn validate_outbox_message(&self, msg: &OutboxMessage) -> Result<()> {
        if msg.event_type.is_empty() {
            return Err(anyhow::anyhow!("Event type cannot be empty"));
        }

        if msg.topic.is_empty() {
            return Err(anyhow::anyhow!("Topic cannot be empty"));
        }

        if msg.payload.is_empty() {
            return Err(anyhow::anyhow!("Payload cannot be empty"));
        }

        Ok(())
    }

    /// Cleanup old messages from both outbox systems
    pub async fn cleanup_old_messages(
        &self,
        old_repo: &crate::infrastructure::outbox::PostgresOutboxRepository,
        new_repo: &super::CDCOutboxRepository,
        older_than: std::time::Duration,
    ) -> Result<(usize, usize)> {
        let old_cleaned = self.cleanup_old_outbox_messages(older_than).await?;
        let new_cleaned = new_repo.cleanup_old_messages(older_than).await?;

        info!(
            "Cleanup completed: {} old outbox, {} CDC outbox",
            old_cleaned, new_cleaned
        );
        Ok((old_cleaned, new_cleaned))
    }

    /// Cleanup old messages from original outbox
    async fn cleanup_old_outbox_messages(&self, older_than: std::time::Duration) -> Result<usize> {
        let cutoff_time = Utc::now() - chrono::Duration::from_std(older_than)?;

        let result = sqlx::query!(
            "DELETE FROM outbox_messages WHERE created_at < $1 AND status = 'processed'",
            cutoff_time
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as usize)
    }

    /// Verify migration integrity by comparing counts and key metrics
    pub async fn verify_migration_integrity(
        &self,
        new_repo: &super::CDCOutboxRepository,
    ) -> Result<MigrationIntegrityReport> {
        let old_count = self.get_total_outbox_count().await?;
        let new_count = self.get_cdc_outbox_count().await?;

        let old_unique_events = self.get_unique_event_count("outbox_messages").await?;
        let new_unique_events = self.get_unique_event_count("kafka_outbox_cdc").await?;

        let report = MigrationIntegrityReport {
            old_total_messages: old_count,
            new_total_messages: new_count,
            old_unique_events,
            new_unique_events,
            integrity_passed: old_unique_events == new_unique_events,
            message_count_difference: (new_count as i64) - (old_count as i64),
        };

        info!("Migration integrity report: {:?}", report);
        Ok(report)
    }

    /// Get CDC outbox count
    async fn get_cdc_outbox_count(&self) -> Result<usize> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM kafka_outbox_cdc")
            .fetch_one(&self.pool)
            .await?;

        Ok(row.get::<i64, _>("count") as usize)
    }

    /// Get unique event count for a table
    async fn get_unique_event_count(&self, table_name: &str) -> Result<usize> {
        let query = format!(
            "SELECT COUNT(DISTINCT event_id) as count FROM {}",
            table_name
        );
        let row = sqlx::query(&query).fetch_one(&self.pool).await?;

        Ok(row.get::<i64, _>("count") as usize)
    }
}

/// Report structure for migration integrity verification
#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationIntegrityReport {
    pub old_total_messages: usize,
    pub new_total_messages: usize,
    pub old_unique_events: usize,
    pub new_unique_events: usize,
    pub integrity_passed: bool,
    pub message_count_difference: i64,
}

/// Builder pattern for CDCIntegrationHelper
pub struct CDCIntegrationHelperBuilder {
    config: CDCIntegrationConfig,
}

impl CDCIntegrationHelperBuilder {
    pub fn new() -> Self {
        Self {
            config: CDCIntegrationConfig::default(),
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    pub fn with_migration_chunk_size(mut self, chunk_size: usize) -> Self {
        self.config.migration_chunk_size = chunk_size;
        self
    }

    pub fn with_validation(mut self, enable_validation: bool) -> Self {
        self.config.enable_validation = enable_validation;
        self
    }

    pub fn with_cleanup(mut self, cleanup_old_messages: bool) -> Self {
        self.config.cleanup_old_messages = cleanup_old_messages;
        self
    }

    pub fn with_retry_config(mut self, max_retries: u32, retry_delay_ms: u64) -> Self {
        self.config.max_retries = max_retries;
        self.config.retry_delay_ms = retry_delay_ms;
        self
    }

    pub fn build(self, pool: sqlx::PgPool) -> CDCIntegrationHelper {
        CDCIntegrationHelper::new(pool, self.config)
    }
}

impl Default for CDCIntegrationHelperBuilder {
    fn default() -> Self {
        Self::new()
    }
}
