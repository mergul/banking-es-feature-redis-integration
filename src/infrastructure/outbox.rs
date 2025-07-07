use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json; // For JSONB metadata
use sqlx::{Postgres, Transaction};
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
    async fn fetch_pending_messages(&self, limit: i32) -> Result<Vec<PersistedOutboxMessage>>;

    /// Marks a message as successfully processed (e.g., sent to Kafka).
    /// This might involve updating its status to 'PROCESSED' or deleting it.
    async fn mark_as_processed(&self, outbox_message_id: Uuid) -> Result<()>;

    /// Deletes a batch of messages by their outbox IDs.
    async fn delete_processed_batch(&self, outbox_message_ids: &[Uuid]) -> Result<usize>;

    /// Increments the retry count and updates the last attempt timestamp for a message.
    /// Optionally, if retries exceed a max, it could update status to 'FAILED'.
    async fn increment_retry_attempt(
        &self,
        outbox_message_id: Uuid,
        new_last_attempt_at: DateTime<Utc>,
        max_retries: i32,
    ) -> Result<()>;

    /// Marks a message as 'FAILED' after exhausting retries.
    async fn mark_as_failed(&self, outbox_message_id: Uuid) -> Result<()>;

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
    pool: PgPool,
}

impl PostgresOutboxRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
            sqlx::query(
                r#"
                INSERT INTO kafka_outbox
                    (aggregate_id, event_id, event_type, payload, topic, metadata, status, created_at, updated_at, last_attempt_at, retry_count)
                VALUES
                    ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), NULL, 0)
                "#
            )
            .bind(msg.aggregate_id)
            .bind(msg.event_id)
            .bind(msg.event_type)
            .bind(msg.payload)
            .bind(msg.topic)
            .bind(msg.metadata)
            .execute(&mut **tx)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to insert message into kafka_outbox: {}", e))?;
        }
        Ok(())
    }

    async fn fetch_pending_messages(&self, limit: i32) -> Result<Vec<PersistedOutboxMessage>> {
        // Poller Service Logic:
        // This would involve a query like:
        // SELECT id, aggregate_id, event_id, event_type, payload, topic, status, created_at, updated_at, last_attempt_at, retry_count, metadata
        // FROM kafka_outbox
        // WHERE status = 'PENDING' OR (status = 'PROCESSING' AND last_attempt_at < NOW() - '5 minutes'::interval)
        // ORDER BY created_at ASC
        // LIMIT $1
        // FOR UPDATE SKIP LOCKED;
        // Then update status to 'PROCESSING' for fetched messages.
        // For sketch purposes, returning empty.
        tracing::warn!(
            "PostgresOutboxRepository::fetch_pending_messages is not fully implemented (sketch)."
        );
        Ok(Vec::new())
    }

    async fn mark_as_processed(&self, outbox_message_id: Uuid) -> Result<()> {
        // Poller Service Logic:
        // UPDATE kafka_outbox SET status = 'PROCESSED', updated_at = NOW() WHERE id = $1;
        // Or DELETE FROM kafka_outbox WHERE id = $1;
        tracing::warn!(
            "PostgresOutboxRepository::mark_as_processed is not fully implemented (sketch)."
        );
        let _ = outbox_message_id; //
        Ok(())
    }

    async fn delete_processed_batch(&self, outbox_message_ids: &[Uuid]) -> Result<usize> {
        if outbox_message_ids.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query("DELETE FROM kafka_outbox WHERE id = ANY($1)")
            .bind(outbox_message_ids)
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to delete processed outbox messages: {}", e))?;
        Ok(result.rows_affected() as usize)
    }

    async fn increment_retry_attempt(
        &self,
        outbox_message_id: Uuid,
        new_last_attempt_at: DateTime<Utc>,
        max_retries: i32,
    ) -> Result<()> {
        // Poller Service Logic:
        // Atomically increment retry_count and update last_attempt_at.
        // If retry_count >= max_retries, set status = 'FAILED'.
        // Else, ensure status is 'PENDING' (or remains 'PROCESSING' if poller manages this state for retries).
        let mut tx = self.pool.begin().await?;
        let current: Option<PersistedOutboxMessage> = sqlx::query_as!(
            PersistedOutboxMessage,
            "SELECT * FROM kafka_outbox WHERE id = $1 FOR UPDATE",
            outbox_message_id
        )
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(msg) = current {
            let new_retry_count = msg.retry_count + 1;
            if new_retry_count >= max_retries {
                sqlx::query(
                    "UPDATE kafka_outbox SET status = 'FAILED', retry_count = $1, last_attempt_at = $2, updated_at = NOW() WHERE id = $3"
                )
                .bind(new_retry_count)
                .bind(new_last_attempt_at)
                .bind(outbox_message_id)
                .execute(&mut *tx).await?;
            } else {
                sqlx::query(
                    "UPDATE kafka_outbox SET retry_count = $1, last_attempt_at = $2, updated_at = NOW() WHERE id = $3"
                )
                .bind(new_retry_count)
                .bind(new_last_attempt_at)
                .bind(outbox_message_id)
                .execute(&mut *tx).await?;
            }
            tx.commit().await?;
        } else {
            // Message not found, perhaps already processed and deleted.
            tx.rollback().await?; // Rollback the transaction
            return Err(anyhow::anyhow!(
                "Outbox message {} not found for retry increment.",
                outbox_message_id
            ));
        }
        tracing::warn!("PostgresOutboxRepository::increment_retry_attempt is a partial sketch.");
        Ok(())
    }

    async fn mark_as_failed(&self, outbox_message_id: Uuid) -> Result<()> {
        sqlx::query("UPDATE kafka_outbox SET status = 'FAILED', updated_at = NOW() WHERE id = $1")
            .bind(outbox_message_id)
            .execute(&self.pool) // This should ideally be within a transaction if called by poller
            .await?;
        tracing::warn!("PostgresOutboxRepository::mark_as_failed is a sketch.");
        Ok(())
    }

    async fn find_stuck_processing_messages(
        &self,
        stuck_threshold: Duration,
        limit: i32,
    ) -> Result<Vec<PersistedOutboxMessage>> {
        let threshold_timestamp = Utc::now() - chrono::Duration::from_std(stuck_threshold)?;
        sqlx::query_as!(PersistedOutboxMessage,
            "SELECT * FROM kafka_outbox WHERE status = 'PROCESSING' AND updated_at < $1 ORDER BY updated_at ASC LIMIT $2",
            threshold_timestamp,
            limit as i64
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn reset_stuck_messages(&self, outbox_message_ids: &[Uuid]) -> Result<usize> {
        if outbox_message_ids.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query("UPDATE kafka_outbox SET status = 'PENDING', updated_at = NOW(), retry_count = retry_count + 1 WHERE id = ANY($1) AND status = 'PROCESSING'")
            .bind(outbox_message_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to reset stuck outbox messages: {}", e))?;
        Ok(result.rows_affected() as usize)
    }
}
