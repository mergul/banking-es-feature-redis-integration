use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json; // For JSONB metadata
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use std::time::Duration; // Required for find_stuck_processing_messages
use uuid::Uuid;

/// Represents a message to be stored in the kafka_outbox table.
#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub aggregate_id: Uuid,
    pub event_id: Uuid, // Unique ID of the domain event, for idempotency
    pub event_type: String,
    pub payload: Vec<u8>, // Serialized domain event
    pub topic: String,
    pub metadata: Option<serde_json::Value>, // For Kafka headers or other context
}

/// Represents a message retrieved from the outbox table by the poller.
/// Includes database row ID and status fields.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PersistedOutboxMessage {
    pub id: Uuid, // PK of the outbox table row
    pub aggregate_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub topic: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub retry_count: i32,
    pub metadata: Option<serde_json::Value>,
    pub error_details: Option<String>, // Error message when processing fails
}

#[async_trait]
pub trait OutboxRepositoryTrait: Send + Sync {
    /// Adds a batch of messages to the outbox table within an existing database transaction.
    /// These messages will have a status of 'PENDING'.
    async fn add_pending_messages(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        messages: Vec<OutboxMessage>,
    ) -> Result<()>;

    /// Fetches a batch of pending messages for the poller to process.
    /// This method should ideally implement a way to lock the fetched rows
    /// (e.g., SELECT ... FOR UPDATE SKIP LOCKED) or update their status to 'PROCESSING'
    /// immediately to prevent concurrent pollers from picking the same messages.
    async fn fetch_and_lock_pending_messages(
        &self,
        limit: i64,
    ) -> Result<Vec<PersistedOutboxMessage>>;

    /// Marks a message as successfully processed (e.g., sent to Kafka).
    /// This typically involves deleting it from the outbox.
    async fn mark_as_processed(&self, outbox_message_id: Uuid) -> Result<()>;

    /// Deletes a batch of messages by their outbox IDs.
    /// Note: This might be redundant if mark_as_processed deletes individual messages.
    /// Kept for now if batch deletion is specifically needed elsewhere.
    async fn delete_processed_batch(&self, outbox_message_ids: &[Uuid]) -> Result<usize>;

    /// Increments the retry count and updates the last attempt timestamp for a message.
    /// If retries exceed a max, it updates status to 'FAILED'. Otherwise, sets back to 'PENDING'.
    async fn record_failed_attempt(
        &self,
        outbox_message_id: Uuid,
        max_retries: i32,
        error_message: Option<String>, // Store the last error message
    ) -> Result<()>;

    /// Directly marks a message as 'FAILED'. Used if a message should not be retried.
    async fn mark_as_failed(
        &self,
        outbox_message_id: Uuid,
        error_message: Option<String>,
    ) -> Result<()>;

    /// Finds messages that might be stuck in 'PROCESSING' state for too long.
    async fn find_stuck_processing_messages(
        &self,
        stuck_threshold: Duration,
        limit: i32,
    ) -> Result<Vec<PersistedOutboxMessage>>;

    /// Resets the status of stuck 'PROCESSING' messages back to 'PENDING'.
    async fn reset_stuck_messages(&self, outbox_message_ids: &[Uuid]) -> Result<usize>;
}

// Need to add to src/infrastructure/mod.rs:
// pub mod outbox;
// pub use outbox::{OutboxMessage, PersistedOutboxMessage, OutboxRepositoryTrait, PostgresOutboxRepository};

// --- PostgresOutboxRepository Implementation ---

use sqlx::PgPool;

#[derive(Clone)]
pub struct PostgresOutboxRepository {
    pools: Arc<PartitionedPools>,
}

impl PostgresOutboxRepository {
    pub fn new(pools: Arc<PartitionedPools>) -> Self {
        Self { pools }
    }
}

#[async_trait]
impl OutboxRepositoryTrait for PostgresOutboxRepository {
    async fn add_pending_messages(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        messages: Vec<OutboxMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // For simplicity in this sketch, inserting one by one.
        // In a production system, consider using `sqlx::QueryBuilder` for batch inserts
        // or PostgreSQL's `COPY` for very high throughput if events per transaction are many.
        for msg in messages {
            sqlx::query!(
                r#"
                INSERT INTO kafka_outbox
                    (aggregate_id, event_id, event_type, payload, topic, metadata, status, created_at, updated_at, last_attempt_at, retry_count)
                VALUES
                    ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), NULL, 0)
                "#,
                msg.aggregate_id,
                msg.event_id,
                msg.event_type,
                msg.payload,
                msg.topic,
                msg.metadata // Option<serde_json::Value> is handled by sqlx for JSONB
            )
            .execute(&mut **tx) // Execute within the passed transaction
            .await
            .map_err(|e| anyhow::anyhow!("Failed to insert message into kafka_outbox: {}", e))?;
        }
        Ok(())
    }

    async fn fetch_and_lock_pending_messages(
        &self,
        limit: i64,
    ) -> Result<Vec<PersistedOutboxMessage>> {
        let mut tx = self.pools.select_pool(OperationType::Write).begin().await?;

        // Select PENDING messages, lock them, and update their status to PROCESSING
        // The RETURNING clause gets the updated rows, including their original values before the update.
        // Note: The actual PersistedOutboxMessage fields (like status) will reflect the state *after* this update if selected directly.
        // It's often better to select first, then update. However, for atomicity:
        // 1. Select IDs of PENDING messages with FOR UPDATE SKIP LOCKED.
        // 2. Update these IDs to PROCESSING.
        // 3. Select the full rows for these IDs.

        let messages_to_process: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id FROM kafka_outbox
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        if messages_to_process.is_empty() {
            tx.commit().await?;
            return Ok(Vec::new());
        }

        // Update status to 'PROCESSING' for the selected messages
        sqlx::query(
            r#"
            UPDATE kafka_outbox
            SET status = 'PROCESSING', updated_at = NOW(), last_attempt_at = NOW()
            WHERE id = ANY($1)
            "#,
        )
        .bind(&messages_to_process)
        .execute(&mut *tx)
        .await?;

        // Now fetch the full details of these messages that are now 'PROCESSING'
        let fetched_messages = sqlx::query_as!(
            PersistedOutboxMessage,
            "SELECT id, aggregate_id, event_id, event_type, payload, topic, status, created_at, updated_at, last_attempt_at, retry_count, metadata, error_details FROM kafka_outbox WHERE id = ANY($1) ORDER BY created_at ASC",
            &messages_to_process
        )
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(fetched_messages)
    }

    async fn mark_as_processed(&self, outbox_message_id: Uuid) -> Result<()> {
        sqlx::query!("DELETE FROM kafka_outbox WHERE id = $1", outbox_message_id)
            .execute(self.pools.select_pool(OperationType::Write))
            .await
            .map_err(|e| {
                tracing::error!(
                    "Failed to delete processed outbox message {}: {}",
                    outbox_message_id,
                    e
                );
                anyhow::anyhow!(
                    "Failed to delete processed outbox message {}: {}",
                    outbox_message_id,
                    e
                )
            })?;
        tracing::info!(
            "Successfully processed and deleted outbox message {}",
            outbox_message_id
        );
        Ok(())
    }

    async fn delete_processed_batch(&self, outbox_message_ids: &[Uuid]) -> Result<usize> {
        if outbox_message_ids.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query!(
            "DELETE FROM kafka_outbox WHERE id = ANY($1)",
            outbox_message_ids
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete processed outbox messages batch: {}", e))?;
        Ok(result.rows_affected() as usize)
    }

    async fn record_failed_attempt(
        &self,
        outbox_message_id: Uuid,
        max_retries: i32,
        error_message: Option<String>,
    ) -> Result<()> {
        let mut tx = self.pools.select_pool(OperationType::Write).begin().await?;

        let current: Option<(i32, String)> =
            sqlx::query_as("SELECT retry_count, status FROM kafka_outbox WHERE id = $1 FOR UPDATE")
                .bind(outbox_message_id)
                .fetch_optional(&mut *tx)
                .await?;

        match current {
            Some((current_retry_count, current_status)) => {
                if current_status == "PROCESSED" || current_status == "FAILED" {
                    tracing::warn!(
                        "Attempted to record failure for already {} message: {}",
                        current_status,
                        outbox_message_id
                    );
                    tx.commit().await?; // Or rollback, depending on desired strictness
                    return Ok(()); // Or an error indicating invalid state transition
                }

                let new_retry_count = current_retry_count + 1;
                let new_last_attempt_at = Utc::now();

                if new_retry_count >= max_retries {
                    sqlx::query!(
                        "UPDATE kafka_outbox SET status = 'FAILED', retry_count = $1, last_attempt_at = $2, updated_at = NOW(), error_details = $3 WHERE id = $4",
                        new_retry_count,
                        new_last_attempt_at,
                        error_message,
                        outbox_message_id
                    )
                    .execute(&mut *tx)
                    .await?;
                    tracing::error!(
                        "Message {} failed after {} retries. Last error: {:?}",
                        outbox_message_id,
                        new_retry_count,
                        error_message
                    );
                } else {
                    // Set back to PENDING for the poller to pick it up again after a delay (poller will implement the delay)
                    sqlx::query!(
                        "UPDATE kafka_outbox SET status = 'PENDING', retry_count = $1, last_attempt_at = $2, updated_at = NOW(), error_details = $3 WHERE id = $4",
                        new_retry_count,
                        new_last_attempt_at,
                        error_message,
                        outbox_message_id
                    )
                    .execute(&mut *tx)
                    .await?;
                    tracing::warn!(
                        "Message {} failed attempt {}. Will retry. Error: {:?}",
                        outbox_message_id,
                        new_retry_count,
                        error_message
                    );
                }
                tx.commit().await?;
            }
            None => {
                tx.rollback().await?;
                tracing::error!(
                    "Outbox message {} not found for recording failed attempt.",
                    outbox_message_id
                );
                return Err(anyhow::anyhow!(
                    "Outbox message {} not found for recording failed attempt.",
                    outbox_message_id
                ));
            }
        }
        Ok(())
    }

    async fn mark_as_failed(
        &self,
        outbox_message_id: Uuid,
        error_message: Option<String>,
    ) -> Result<()> {
        sqlx::query!(
            "UPDATE kafka_outbox SET status = 'FAILED', updated_at = NOW(), last_attempt_at = NOW(), error_details = $1 WHERE id = $2",
            error_message,
            outbox_message_id,
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(|e| {
            tracing::error!("Failed to mark outbox message {} as FAILED: {}", outbox_message_id, e);
            anyhow::anyhow!("Failed to mark outbox message {} as FAILED: {}", outbox_message_id, e)
        })?;
        tracing::info!(
            "Marked outbox message {} as FAILED. Error: {:?}",
            outbox_message_id,
            error_message
        );
        Ok(())
    }

    async fn find_stuck_processing_messages(
        &self,
        stuck_threshold: Duration,
        limit: i32,
    ) -> Result<Vec<PersistedOutboxMessage>> {
        let threshold_timestamp = Utc::now() - chrono::Duration::from_std(stuck_threshold)?;
        let messages = sqlx::query_as!(
            PersistedOutboxMessage,
            "SELECT id, aggregate_id, event_id, event_type, payload, topic, status, created_at, updated_at, last_attempt_at, retry_count, metadata, error_details FROM kafka_outbox WHERE status = 'PROCESSING' AND updated_at < $1 ORDER BY updated_at ASC LIMIT $2",
            threshold_timestamp,
            limit as i64
        )
        .fetch_all(self.pools.select_pool(OperationType::Read))
        .await?;
        Ok(messages)
    }

    async fn reset_stuck_messages(&self, outbox_message_ids: &[Uuid]) -> Result<usize> {
        if outbox_message_ids.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query!(
            "UPDATE kafka_outbox SET status = 'PENDING', updated_at = NOW(), retry_count = retry_count + 1 WHERE id = ANY($1) AND status = 'PROCESSING'",
            outbox_message_ids
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await?;
        Ok(result.rows_affected() as usize)
    }
}
