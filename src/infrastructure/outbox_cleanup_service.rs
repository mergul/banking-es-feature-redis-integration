use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use sqlx::{Pool, Postgres, Row};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Deserialize)]
pub struct CleanupConfig {
    /// How long to retain records before marking for deletion
    pub retention_hours: i64,
    /// Safety margin before physical deletion after marking
    pub safety_margin_minutes: i64,
    /// How often to run cleanup cycles
    pub cleanup_interval_minutes: u64,
    /// Maximum records to process in one batch
    pub batch_size: i64,
    /// Maximum number of batches to process in one cycle (0 = unlimited)
    pub max_batches_per_cycle: u32,
    /// Delay between batches in milliseconds
    pub batch_delay_ms: u64,
    /// Maximum total time for cleanup cycle in minutes
    pub max_cycle_duration_minutes: u64,
    /// Enable vacuum after cleanup
    pub enable_vacuum: bool,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            retention_hours: 1,
            safety_margin_minutes: 10,
            cleanup_interval_minutes: 15,
            batch_size: 1000,
            max_batches_per_cycle: 0, // unlimited
            batch_delay_ms: 100,
            max_cycle_duration_minutes: 30,
            enable_vacuum: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CleanupMetrics {
    pub marked_for_deletion: i64,
    pub physically_deleted: i64,
    pub cleanup_duration_ms: i64,
    pub mark_batches: u32,
    pub delete_batches: u32,
    pub errors: Vec<String>,
}

pub struct OutboxCleaner {
    pool: Pool<Postgres>,
    config: CleanupConfig,
}

impl OutboxCleaner {
    pub fn new(pool: Pool<Postgres>, config: CleanupConfig) -> Self {
        Self { pool, config }
    }

    /// Mark a single batch of records for deletion
    async fn mark_deletion_batch(&self, mark_cutoff: DateTime<Utc>) -> Result<i64> {
        let query = r#"
                UPDATE kafka_outbox_cdc 
                SET deleted_at = NOW() 
                WHERE id IN (
                    SELECT id FROM kafka_outbox_cdc 
                    WHERE created_at < $1 
                    AND deleted_at IS NULL 
                    ORDER BY created_at ASC
                    LIMIT $2
                )
            "#;

        let result = sqlx::query(query)
            .bind(mark_cutoff)
            .bind(self.config.batch_size)
            .execute(&self.pool)
            .await
            .context("Failed to mark batch for deletion")?;

        Ok(result.rows_affected() as i64)
    }

    /// Delete a single batch of marked records
    async fn delete_batch(&self, delete_cutoff: DateTime<Utc>) -> Result<i64> {
        let query = r#"
                DELETE FROM kafka_outbox_cdc 
                WHERE id IN (
                    SELECT id FROM kafka_outbox_cdc 
                    WHERE deleted_at IS NOT NULL 
                    AND deleted_at < $1 
                    ORDER BY deleted_at ASC
                    LIMIT $2
                )
            "#;

        let result = sqlx::query(query)
            .bind(delete_cutoff)
            .bind(self.config.batch_size)
            .execute(&self.pool)
            .await
            .context("Failed to delete batch")?;

        Ok(result.rows_affected() as i64)
    }

    /// Start the cleanup service (runs continuously)
    pub async fn start_service(&self) -> Result<()> {
        info!(
            "Starting outbox cleanup service with config: {:?}",
            self.config
        );

        let mut interval = interval(tokio::time::Duration::from_secs(
            self.config.cleanup_interval_minutes * 60,
        ));

        loop {
            interval.tick().await;

            match self.cleanup_cycle().await {
                Ok(metrics) => {
                    info!(
                        "Cleanup cycle completed - marked: {}, deleted: {}, duration: {}ms",
                        metrics.marked_for_deletion,
                        metrics.physically_deleted,
                        metrics.cleanup_duration_ms
                    );

                    if !metrics.errors.is_empty() {
                        warn!("Cleanup completed with errors: {:?}", metrics.errors);
                    }
                }
                Err(e) => {
                    error!("Cleanup cycle failed: {:?}", e);
                    // Wait a bit before retrying on error
                    sleep(tokio::time::Duration::from_secs(30)).await;
                }
            }
        }
    }

    /// Run a single cleanup cycle
    pub async fn cleanup_cycle(&self) -> Result<CleanupMetrics> {
        let start_time = Utc::now();
        let mut metrics = CleanupMetrics {
            marked_for_deletion: 0,
            physically_deleted: 0,
            cleanup_duration_ms: 0,
            mark_batches: 0,
            delete_batches: 0,
            errors: Vec::new(),
        };

        // Phase 1: Mark records for deletion
        match self.mark_for_deletion_with_limits().await {
            Ok((count, batches)) => {
                metrics.marked_for_deletion = count;
                metrics.mark_batches = batches;
                info!(
                    "Phase 1: Marked {} records for deletion in {} batches",
                    count, batches
                );
            }
            Err(e) => {
                let error_msg = format!("Failed to mark records for deletion: {:?}", e);
                error!("{}", error_msg);
                metrics.errors.push(error_msg);
            }
        }

        // Phase 2: Physical deletion with safety margin
        match self.physical_deletion_with_limits().await {
            Ok((count, batches)) => {
                metrics.physically_deleted = count;
                metrics.delete_batches = batches;
                info!(
                    "Phase 2: Physically deleted {} records in {} batches",
                    count, batches
                );
            }
            Err(e) => {
                let error_msg = format!("Failed to physically delete records: {:?}", e);
                error!("{}", error_msg);
                metrics.errors.push(error_msg);
            }
        }

        // Phase 3: Optional vacuum
        if self.config.enable_vacuum {
            if let Err(e) = self.vacuum_if_needed().await {
                let error_msg = format!("Vacuum operation failed: {:?}", e);
                warn!("{}", error_msg);
                metrics.errors.push(error_msg);
            }
        }

        metrics.cleanup_duration_ms = (Utc::now() - start_time).num_milliseconds();
        Ok(metrics)
    }

    /// Phase 1: Mark old records for deletion in batches with limits
    async fn mark_for_deletion_with_limits(&self) -> Result<(i64, u32)> {
        let mark_cutoff = Utc::now() - Duration::hours(self.config.retention_hours);
        let cycle_start = Utc::now();
        let max_duration = Duration::minutes(self.config.max_cycle_duration_minutes as i64);

        let mut total_marked = 0i64;
        let mut batch_count = 0u32;

        loop {
            // Check time limit
            if Utc::now() - cycle_start > max_duration {
                warn!(
                    "Mark phase stopped due to time limit: {} batches, {} records in {}min",
                    batch_count, total_marked, self.config.max_cycle_duration_minutes
                );
                break;
            }

            // Check batch limit
            if self.config.max_batches_per_cycle > 0
                && batch_count >= self.config.max_batches_per_cycle
            {
                info!(
                    "Mark phase stopped due to batch limit: {} batches, {} records",
                    batch_count, total_marked
                );
                break;
            }

            let batch_result = self.mark_deletion_batch(mark_cutoff).await?;
            total_marked += batch_result;
            batch_count += 1;

            info!(
                "Mark batch #{}: {} records marked (total: {})",
                batch_count, batch_result, total_marked
            );

            // If batch was smaller than batch_size, we're done
            if batch_result < self.config.batch_size {
                break;
            }

            // Delay between batches
            if batch_result > 0 && self.config.batch_delay_ms > 0 {
                sleep(tokio::time::Duration::from_millis(
                    self.config.batch_delay_ms,
                ))
                .await;
            }
        }

        info!(
            "Mark phase completed: {} total records marked in {} batches",
            total_marked, batch_count
        );

        Ok((total_marked, batch_count))
    }

    /// Phase 2: Physically delete marked records in batches with limits
    async fn physical_deletion_with_limits(&self) -> Result<(i64, u32)> {
        let delete_cutoff = Utc::now() - Duration::minutes(self.config.safety_margin_minutes);
        let cycle_start = Utc::now();
        let max_duration = Duration::minutes(self.config.max_cycle_duration_minutes as i64);

        let mut total_deleted = 0i64;
        let mut batch_count = 0u32;

        loop {
            // Check time limit
            if Utc::now() - cycle_start > max_duration {
                warn!(
                    "Delete phase stopped due to time limit: {} batches, {} records in {}min",
                    batch_count, total_deleted, self.config.max_cycle_duration_minutes
                );
                break;
            }

            // Check batch limit
            if self.config.max_batches_per_cycle > 0
                && batch_count >= self.config.max_batches_per_cycle
            {
                info!(
                    "Delete phase stopped due to batch limit: {} batches, {} records",
                    batch_count, total_deleted
                );
                break;
            }

            let batch_result = self.delete_batch(delete_cutoff).await?;
            total_deleted += batch_result;
            batch_count += 1;

            info!(
                "Delete batch #{}: {} records deleted (total: {})",
                batch_count, batch_result, total_deleted
            );

            // If batch was smaller than batch_size, we're done
            if batch_result < self.config.batch_size {
                break;
            }

            // Delay between batches
            if batch_result > 0 && self.config.batch_delay_ms > 0 {
                sleep(tokio::time::Duration::from_millis(
                    self.config.batch_delay_ms,
                ))
                .await;
            }
        }

        info!(
            "Delete phase completed: {} total records deleted in {} batches",
            total_deleted, batch_count
        );

        Ok((total_deleted, batch_count))
    }

    /// Phase 1: Mark old records for deletion in batches
    async fn mark_for_deletion(&self) -> Result<i64> {
        let (total, _) = self.mark_for_deletion_with_limits().await?;
        Ok(total)
    }

    /// Phase 2: Physically delete marked records in batches after safety margin
    async fn physical_deletion(&self) -> Result<i64> {
        let (total, _) = self.physical_deletion_with_limits().await?;
        Ok(total)
    }

    /// Optional vacuum operation to reclaim space
    async fn vacuum_if_needed(&self) -> Result<()> {
        // Check if vacuum is needed (if significant deletes occurred)
        let stats_query = r#"
            SELECT 
                schemaname,
                relname,
                n_dead_tup,
                n_live_tup
            FROM pg_stat_user_tables 
            WHERE relname = 'kafka_outbox_cdc'
        "#;

        let row = sqlx::query(stats_query)
            .fetch_optional(&self.pool)
            .await
            .context("Failed to fetch table statistics")?;

        if let Some(row) = row {
            let dead_tuples: i64 = row.try_get("n_dead_tup").unwrap_or(0);
            let live_tuples: i64 = row.try_get("n_live_tup").unwrap_or(1);

            // Vacuum if dead tuples > 20% of live tuples and > 1000
            if dead_tuples > 1000 && dead_tuples > live_tuples / 5 {
                info!(
                    "Running VACUUM on kafka_outbox_cdc table (dead: {}, live: {})",
                    dead_tuples, live_tuples
                );

                sqlx::query("VACUUM kafka_outbox_cdc")
                    .execute(&self.pool)
                    .await
                    .context("Failed to vacuum kafka_outbox_cdc table")?;

                info!("VACUUM completed successfully");
            }
        }

        Ok(())
    }

    /// Get cleanup statistics
    pub async fn get_outbox_stats(&self) -> Result<OutboxStats> {
        let query = r#"
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) as marked_for_deletion,
                COUNT(*) FILTER (WHERE deleted_at IS NULL) as active_records,
                MIN(created_at) as oldest_record,
                MAX(created_at) as newest_record
            FROM kafka_outbox_cdc
        "#;

        let row = sqlx::query(query)
            .fetch_one(&self.pool)
            .await
            .context("Failed to fetch outbox statistics")?;

        Ok(OutboxStats {
            total_records: row.try_get("total_records")?,
            marked_for_deletion: row.try_get("marked_for_deletion")?,
            active_records: row.try_get("active_records")?,
            oldest_record: row.try_get("oldest_record")?,
            newest_record: row.try_get("newest_record")?,
        })
    }

    /// Manual cleanup trigger (useful for testing or admin operations)
    pub async fn force_cleanup(&self) -> Result<CleanupMetrics> {
        info!("Forcing manual cleanup cycle");
        self.cleanup_cycle().await
    }

    /// Cleanup orphaned outbox messages (outbox exists but event doesn't)
    pub async fn cleanup_orphaned_messages(&self) -> Result<CleanupMetrics> {
        let start_time = Utc::now();
        let mut metrics = CleanupMetrics {
            marked_for_deletion: 0,
            physically_deleted: 0,
            cleanup_duration_ms: 0,
            mark_batches: 0,
            delete_batches: 0,
            errors: Vec::new(),
        };

        info!("Starting orphaned messages cleanup...");

        // Find orphaned outbox messages
        match self.find_and_mark_orphaned_messages().await {
            Ok((count, batches)) => {
                metrics.marked_for_deletion = count;
                metrics.mark_batches = batches;
                info!(
                    "Found and marked {} orphaned outbox messages in {} batches",
                    count, batches
                );
            }
            Err(e) => {
                let error_msg = format!("Failed to find orphaned messages: {:?}", e);
                error!("{}", error_msg);
                metrics.errors.push(error_msg);
            }
        }

        // Physically delete marked orphaned messages
        match self.physical_deletion_with_limits().await {
            Ok((count, batches)) => {
                metrics.physically_deleted = count;
                metrics.delete_batches = batches;
                info!(
                    "Physically deleted {} orphaned messages in {} batches",
                    count, batches
                );
            }
            Err(e) => {
                let error_msg = format!("Failed to delete orphaned messages: {:?}", e);
                error!("{}", error_msg);
                metrics.errors.push(error_msg);
            }
        }

        let duration = Utc::now().signed_duration_since(start_time);
        metrics.cleanup_duration_ms = duration.num_milliseconds();

        info!(
            "Orphaned messages cleanup completed - marked: {}, deleted: {}, duration: {}ms",
            metrics.marked_for_deletion, metrics.physically_deleted, metrics.cleanup_duration_ms
        );

        Ok(metrics)
    }

    /// Find and mark orphaned outbox messages (outbox exists but event doesn't)
    async fn find_and_mark_orphaned_messages(&self) -> Result<(i64, u32)> {
        let mut total_marked = 0;
        let mut batch_count = 0;

        loop {
            // Find orphaned messages: outbox exists but event doesn't
            let orphaned_query = r#"
                UPDATE kafka_outbox_cdc 
                SET deleted_at = NOW() 
                WHERE id IN (
                    SELECT o.id 
                    FROM kafka_outbox_cdc o
                    LEFT JOIN events e ON o.event_id = e.id
                    WHERE o.deleted_at IS NULL 
                    AND e.id IS NULL  -- Event doesn't exist
                    ORDER BY o.created_at ASC
                    LIMIT $1
                )
            "#;

            let result = sqlx::query(orphaned_query)
                .bind(self.config.batch_size)
                .execute(&self.pool)
                .await
                .context("Failed to mark orphaned messages")?;

            let marked_count = result.rows_affected() as i64;
            total_marked += marked_count;
            batch_count += 1;

            info!(
                "Batch {}: Marked {} orphaned messages for deletion",
                batch_count, marked_count
            );

            // Stop if no more orphaned messages or reached batch limit
            if marked_count == 0
                || (self.config.max_batches_per_cycle > 0
                    && batch_count >= self.config.max_batches_per_cycle)
            {
                break;
            }

            // Delay between batches
            sleep(tokio::time::Duration::from_millis(
                self.config.batch_delay_ms,
            ))
            .await;
        }

        Ok((total_marked, batch_count))
    }

    /// Get statistics about orphaned messages
    pub async fn get_orphaned_messages_stats(&self) -> Result<serde_json::Value> {
        let query = r#"
            SELECT 
                COUNT(*) as total_outbox_messages,
                COUNT(*) FILTER (WHERE deleted_at IS NOT NULL) as marked_for_deletion,
                COUNT(*) FILTER (WHERE deleted_at IS NULL) as active_messages,
                COUNT(*) FILTER (WHERE e.id IS NULL) as orphaned_messages,
                MIN(o.created_at) as oldest_message,
                MAX(o.created_at) as newest_message
            FROM kafka_outbox_cdc o
            LEFT JOIN events e ON o.event_id = e.id
        "#;

        let row = sqlx::query(query)
            .fetch_one(&self.pool)
            .await
            .context("Failed to get orphaned messages stats")?;

        Ok(serde_json::json!({
            "total_outbox_messages": row.get::<i64, _>("total_outbox_messages"),
            "marked_for_deletion": row.get::<i64, _>("marked_for_deletion"),
            "active_messages": row.get::<i64, _>("active_messages"),
            "orphaned_messages": row.get::<i64, _>("orphaned_messages"),
            "oldest_message": row.get::<Option<DateTime<Utc>>, _>("oldest_message"),
            "newest_message": row.get::<Option<DateTime<Utc>>, _>("newest_message"),
        }))
    }

    /// Start orphaned messages cleanup service (runs periodically)
    pub async fn start_orphaned_cleanup_service(&self) -> Result<()> {
        info!("Starting orphaned messages cleanup service...");

        let mut interval = interval(tokio::time::Duration::from_secs(
            self.config.cleanup_interval_minutes * 60,
        ));

        loop {
            interval.tick().await;

            match self.cleanup_orphaned_messages().await {
                Ok(metrics) => {
                    if metrics.marked_for_deletion > 0 || metrics.physically_deleted > 0 {
                        info!(
                            "Orphaned cleanup completed - marked: {}, deleted: {}, duration: {}ms",
                            metrics.marked_for_deletion,
                            metrics.physically_deleted,
                            metrics.cleanup_duration_ms
                        );
                    } else {
                        debug!("No orphaned messages found in cleanup cycle");
                    }

                    if !metrics.errors.is_empty() {
                        warn!(
                            "Orphaned cleanup completed with errors: {:?}",
                            metrics.errors
                        );
                    }
                }
                Err(e) => {
                    error!("Orphaned cleanup cycle failed: {:?}", e);
                    sleep(tokio::time::Duration::from_secs(30)).await;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct OutboxStats {
    pub total_records: i64,
    pub marked_for_deletion: i64,
    pub active_records: i64,
    pub oldest_record: Option<DateTime<Utc>>,
    pub newest_record: Option<DateTime<Utc>>,
}

// Health check for the cleanup service
pub struct CleanupHealthCheck {
    cleaner: OutboxCleaner,
}

impl CleanupHealthCheck {
    pub fn new(cleaner: OutboxCleaner) -> Self {
        Self { cleaner }
    }

    pub async fn health_check(&self) -> Result<bool> {
        // Check if outbox table is accessible
        let stats = self.cleaner.get_outbox_stats().await?;

        // Health check passes if:
        // 1. We can query the table
        // 2. Marked records aren't accumulating excessively
        let health_ok = stats.marked_for_deletion < stats.active_records * 2;

        if !health_ok {
            warn!(
                "Outbox health check failed - too many marked records: {} marked vs {} active",
                stats.marked_for_deletion, stats.active_records
            );
        }

        Ok(health_ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;

    #[sqlx::test]
    #[ignore]
    async fn test_cleanup_cycle(pool: PgPool) -> Result<()> {
        // Setup test table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS kafka_outbox_cdc (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                aggregate_id UUID NOT NULL,
                event_id UUID NOT NULL,
                event_type VARCHAR NOT NULL,
                payload BYTEA NOT NULL,
                topic VARCHAR NOT NULL,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                deleted_at TIMESTAMP
            )
        "#,
        )
        .execute(&pool)
        .await?;

        // Insert test data
        sqlx::query(
            r#"
            INSERT INTO kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, created_at)
            VALUES 
                ('550e8400-e29b-41d4-a716-446655440001'::uuid, '550e8400-e29b-41d4-a716-446655440002'::uuid, 'TestEvent', '\x7b7d', 'test-topic', NOW() - INTERVAL '2 hours'),
                ('550e8400-e29b-41d4-a716-446655440003'::uuid, '550e8400-e29b-41d4-a716-446655440004'::uuid, 'TestEvent', '\x7b7d', 'test-topic', NOW() - INTERVAL '30 minutes')
        "#,
        )
        .execute(&pool)
        .await?;

        let config = CleanupConfig {
            retention_hours: 1,
            safety_margin_minutes: 5,
            ..Default::default()
        };

        let cleaner = OutboxCleaner::new(pool, config);
        let metrics = cleaner.cleanup_cycle().await?;

        assert!(metrics.marked_for_deletion > 0);
        Ok(())
    }
}

// Example usage
// #[tokio::main]
// async fn main() -> Result<()> {
//     tracing_subscriber::fmt::init();

//     let database_url =
//         std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;

//     let pool = sqlx::postgres::PgPoolOptions::new()
//         .max_connections(10)
//         .connect(&database_url)
//         .await
//         .context("Failed to connect to database")?;

//     let config = CleanupConfig {
//         retention_hours: 2,
//         safety_margin_minutes: 15,
//         cleanup_interval_minutes: 10,
//         batch_size: 5000,
//         max_batches_per_cycle: 20,      // Limit to 20 batches per cycle
//         batch_delay_ms: 50,             // 50ms delay between batches
//         max_cycle_duration_minutes: 15, // Maximum 15 minutes per cycle
//         enable_vacuum: true,
//     };

//     let cleaner = OutboxCleaner::new(pool, config);

//     // Start the cleanup service
//     cleaner.start_service().await?;

//     Ok(())
// }
