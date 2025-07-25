use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Represents the status of a CDC operation
#[derive(Debug, Clone, PartialEq)]
pub enum CDCStatus {
    Pending,
    Processing,
    Completed,
    Failed(String),
}

/// Represents the status of projection synchronization
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionStatus {
    Pending,
    Processing,
    Completed,
    Failed(String),
}

/// Tracks CDC processing and projection synchronization status for read-after-write consistency
#[derive(Debug)]
pub struct ConsistencyManager {
    // Track CDC processing status by aggregate ID
    cdc_status: Arc<RwLock<HashMap<Uuid, CDCStatus>>>,

    // Track projection synchronization status by aggregate ID
    projection_status: Arc<RwLock<HashMap<Uuid, ProjectionStatus>>>,

    // Notifiers for waiting tasks
    cdc_notifiers: Arc<RwLock<HashMap<Uuid, Arc<Notify>>>>,
    projection_notifiers: Arc<RwLock<HashMap<Uuid, Arc<Notify>>>>,

    // Track operation timestamps for cleanup
    operation_timestamps: Arc<RwLock<HashMap<Uuid, Instant>>>,

    // Configuration
    max_wait_time: Duration,
    cleanup_interval: Duration,
    last_cleanup: Arc<Mutex<Instant>>,
}

impl ConsistencyManager {
    pub fn new(max_wait_time: Duration, cleanup_interval: Duration) -> Self {
        Self {
            cdc_status: Arc::new(RwLock::new(HashMap::new())),
            projection_status: Arc::new(RwLock::new(HashMap::new())),
            cdc_notifiers: Arc::new(RwLock::new(HashMap::new())),
            projection_notifiers: Arc::new(RwLock::new(HashMap::new())),
            operation_timestamps: Arc::new(RwLock::new(HashMap::new())),
            max_wait_time,
            cleanup_interval,
            last_cleanup: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Mark an operation as pending CDC processing
    pub async fn mark_pending(&self, aggregate_id: Uuid) {
        let mut status = self.cdc_status.write().await;
        status.insert(aggregate_id, CDCStatus::Pending);

        let mut timestamps = self.operation_timestamps.write().await;
        timestamps.insert(aggregate_id, Instant::now());

        // Create a notifier for this aggregate if one doesn't exist
        let mut notifiers = self.cdc_notifiers.write().await;
        notifiers
            .entry(aggregate_id)
            .or_insert_with(|| Arc::new(Notify::new()));

        debug!(
            "Marked aggregate {} as pending CDC processing",
            aggregate_id
        );
    }

    /// Mark an operation as processing in CDC pipeline
    pub async fn mark_processing(&self, aggregate_id: Uuid) {
        let mut status = self.cdc_status.write().await;
        status.insert(aggregate_id, CDCStatus::Processing);

        debug!("Marked aggregate {} as processing in CDC", aggregate_id);
    }

    /// Mark an operation as completed by CDC pipeline
    pub async fn mark_completed(&self, aggregate_id: Uuid) {
        let mut status = self.cdc_status.write().await;
        status.insert(aggregate_id, CDCStatus::Completed);
        tracing::info!(
            "[ConsistencyManager] mark_completed: aggregate_id={} status=Completed",
            aggregate_id
        );
        // Notify waiters
        if let Some(notifier) = self.cdc_notifiers.read().await.get(&aggregate_id) {
            notifier.notify_waiters();
        }
    }

    /// Mark an operation as failed in CDC pipeline
    pub async fn mark_failed(&self, aggregate_id: Uuid, error: String) {
        let mut status = self.cdc_status.write().await;
        status.insert(aggregate_id, CDCStatus::Failed(error.clone()));

        error!(
            "Marked aggregate {} as failed in CDC: {}",
            aggregate_id, error
        );
        // Notify waiters
        if let Some(notifier) = self.cdc_notifiers.read().await.get(&aggregate_id) {
            notifier.notify_waiters();
        }
    }

    // Projection synchronization methods
    /// Mark projection synchronization as pending
    pub async fn mark_projection_pending(&self, aggregate_id: Uuid) {
        let mut status = self.projection_status.write().await;
        status.insert(aggregate_id, ProjectionStatus::Pending);

        let mut timestamps = self.operation_timestamps.write().await;
        timestamps.insert(aggregate_id, Instant::now());

        // Create a notifier for this aggregate if one doesn't exist
        let mut notifiers = self.projection_notifiers.write().await;
        notifiers
            .entry(aggregate_id)
            .or_insert_with(|| Arc::new(Notify::new()));

        debug!(
            "Marked aggregate {} as pending projection synchronization",
            aggregate_id
        );
    }

    /// Mark projection synchronization as processing
    pub async fn mark_projection_processing(&self, aggregate_id: Uuid) {
        let mut status = self.projection_status.write().await;
        status.insert(aggregate_id, ProjectionStatus::Processing);

        debug!("Marked aggregate {} as processing projection", aggregate_id);
    }

    /// Mark projection synchronization as completed
    pub async fn mark_projection_completed(&self, aggregate_id: Uuid) {
        let mut status = self.projection_status.write().await;
        status.insert(aggregate_id, ProjectionStatus::Completed);
        tracing::info!(
            "[ConsistencyManager] mark_projection_completed: aggregate_id={} status=Completed",
            aggregate_id
        );
        // Notify waiters
        if let Some(notifier) = self.projection_notifiers.read().await.get(&aggregate_id) {
            notifier.notify_waiters();
        }
    }

    /// Mark projection synchronization as failed
    pub async fn mark_projection_failed(&self, aggregate_id: Uuid, error: String) {
        let mut status = self.projection_status.write().await;
        status.insert(aggregate_id, ProjectionStatus::Failed(error.clone()));

        error!(
            "Marked aggregate {} as failed projection: {}",
            aggregate_id, error
        );
        // Notify waiters
        if let Some(notifier) = self.projection_notifiers.read().await.get(&aggregate_id) {
            notifier.notify_waiters();
        }
    }

    /// Wait for CDC processing to complete for a specific aggregate
    pub async fn wait_for_consistency(&self, aggregate_id: Uuid) -> Result<()> {
        info!(
            "[ConsistencyManager] Waiting for CDC consistency for aggregate {}",
            aggregate_id
        );
        self.wait_for_status(
            aggregate_id,
            &self.cdc_status,
            &self.cdc_notifiers,
            |status| matches!(status, CDCStatus::Completed),
            |status| match status {
                CDCStatus::Failed(e) => Some(e.clone()),
                _ => None,
            },
            "CDC consistency",
        )
        .await
    }

    /// Wait for projection synchronization to complete for a specific aggregate
    pub async fn wait_for_projection_sync(&self, aggregate_id: Uuid) -> Result<()> {
        info!(
            "[ConsistencyManager] Waiting for projection sync for aggregate {}",
            aggregate_id
        );
        self.wait_for_status(
            aggregate_id,
            &self.projection_status,
            &self.projection_notifiers,
            |status| matches!(status, ProjectionStatus::Completed),
            |status| match status {
                ProjectionStatus::Failed(e) => Some(e.clone()),
                _ => None,
            },
            "Projection synchronization",
        )
        .await
    }

    /// Generic function to wait for a specific status
    async fn wait_for_status<S, F, E>(
        &self,
        aggregate_id: Uuid,
        status_map: &Arc<RwLock<HashMap<Uuid, S>>>,
        notifiers_map: &Arc<RwLock<HashMap<Uuid, Arc<Notify>>>>,
        is_completed: F,
        get_error: E,
        wait_description: &str,
    ) -> Result<()>
    where
        S: Clone + std::fmt::Debug,
        F: Fn(&S) -> bool,
        E: Fn(&S) -> Option<String>,
    {
        let start_time = Instant::now();

        let notifier = {
            let notifiers = notifiers_map.read().await;
            notifiers.get(&aggregate_id).cloned()
        };

        let notifier = match notifier {
            Some(n) => n,
            None => {
                warn!("[ConsistencyManager] No notifier found for aggregate {}, assuming operation is complete", aggregate_id);
                return Ok(());
            }
        };

        loop {
            let status = {
                let statuses = status_map.read().await;
                statuses.get(&aggregate_id).cloned()
            };

            if let Some(status) = status {
                if is_completed(&status) {
                    info!(
                        "[ConsistencyManager] {} completed for aggregate {}",
                        wait_description, aggregate_id
                    );
                    return Ok(());
                }
                if let Some(error) = get_error(&status) {
                    warn!(
                        "[ConsistencyManager] {} failed for aggregate {}: {}",
                        wait_description, aggregate_id, error
                    );
                    return Err(anyhow::anyhow!(
                        "{} failed for aggregate {}: {}",
                        wait_description,
                        aggregate_id,
                        error
                    ));
                }
            }

            if start_time.elapsed() >= self.max_wait_time {
                warn!(
                    "[ConsistencyManager] TIMEOUT waiting for {} for aggregate {} after {:?}",
                    wait_description, aggregate_id, self.max_wait_time
                );
                return Err(anyhow::anyhow!(
                    "Timeout waiting for {} for aggregate {} after {:?}",
                    wait_description,
                    aggregate_id,
                    self.max_wait_time
                ));
            }

            match tokio::time::timeout(
                self.max_wait_time - start_time.elapsed(),
                notifier.notified(),
            )
            .await
            {
                Ok(_) => continue, // Notified, re-check status
                Err(_) => {
                    warn!(
                        "[ConsistencyManager] TIMEOUT waiting for {} for aggregate {} after {:?}",
                        wait_description, aggregate_id, self.max_wait_time
                    );
                    return Err(anyhow::anyhow!(
                        "Timeout waiting for {} for aggregate {} after {:?}",
                        wait_description,
                        aggregate_id,
                        self.max_wait_time
                    ));
                }
            }
        }
    }

    /// Wait for CDC processing to complete for multiple aggregates
    pub async fn wait_for_consistency_batch(&self, aggregate_ids: Vec<Uuid>) -> Result<()> {
        let tasks: Vec<_> = aggregate_ids
            .into_iter()
            .map(|id| self.wait_for_consistency(id))
            .collect();

        let results = futures::future::join_all(tasks).await;

        // Check for failures
        let failures: Vec<_> = results.into_iter().filter_map(Result::err).collect();

        if !failures.is_empty() {
            let error_messages: Vec<String> = failures.iter().map(|e| e.to_string()).collect();
            return Err(anyhow::anyhow!(
                "CDC consistency failed for some aggregates: {}",
                error_messages.join(", ")
            ));
        }

        Ok(())
    }

    /// Wait for projection synchronization to complete for multiple aggregates
    pub async fn wait_for_projection_sync_batch(&self, aggregate_ids: Vec<Uuid>) -> Result<()> {
        let tasks: Vec<_> = aggregate_ids
            .into_iter()
            .map(|id| self.wait_for_projection_sync(id))
            .collect();

        let results = futures::future::join_all(tasks).await;

        // Check for failures
        let failures: Vec<_> = results.into_iter().filter_map(Result::err).collect();

        if !failures.is_empty() {
            let error_messages: Vec<String> = failures.iter().map(|e| e.to_string()).collect();
            return Err(anyhow::anyhow!(
                "Projection sync failed for some aggregates: {}",
                error_messages.join(", ")
            ));
        }

        Ok(())
    }

    /// Clean up old status entries
    pub async fn cleanup_old_entries(&self) {
        let now = Instant::now();
        let mut last_cleanup = self.last_cleanup.lock().await;

        if now.duration_since(*last_cleanup) < self.cleanup_interval {
            return;
        }

        let cutoff_time = now - self.max_wait_time;

        let mut timestamps = self.operation_timestamps.write().await;
        let mut cdc_status = self.cdc_status.write().await;
        let mut projection_status = self.projection_status.write().await;
        let mut cdc_notifiers = self.cdc_notifiers.write().await;
        let mut projection_notifiers = self.projection_notifiers.write().await;

        let mut to_remove = Vec::new();

        for (aggregate_id, timestamp) in timestamps.iter() {
            if *timestamp < cutoff_time {
                to_remove.push(*aggregate_id);
            }
        }

        for aggregate_id in to_remove {
            timestamps.remove(&aggregate_id);
            cdc_status.remove(&aggregate_id);
            projection_status.remove(&aggregate_id);

            // Notify any waiters that the operation has timed out
            if let Some(notifier) = cdc_notifiers.remove(&aggregate_id) {
                notifier.notify_waiters();
            }
            if let Some(notifier) = projection_notifiers.remove(&aggregate_id) {
                notifier.notify_waiters();
            }

            debug!("Cleaned up old status for aggregate {}", aggregate_id);
        }

        *last_cleanup = now;
    }

    /// Get current status for an aggregate
    pub async fn get_status(&self, aggregate_id: Uuid) -> Option<CDCStatus> {
        let status = self.cdc_status.read().await;
        status.get(&aggregate_id).cloned()
    }

    /// Get current projection status for an aggregate
    pub async fn get_projection_status(&self, aggregate_id: Uuid) -> Option<ProjectionStatus> {
        let status = self.projection_status.read().await;
        status.get(&aggregate_id).cloned()
    }

    /// Get statistics about current CDC processing and projection synchronization
    pub async fn get_stats(&self) -> serde_json::Value {
        let cdc_status = self.cdc_status.read().await;
        let projection_status = self.projection_status.read().await;
        let timestamps = self.operation_timestamps.read().await;

        let mut cdc_pending = 0;
        let mut cdc_processing = 0;
        let mut cdc_completed = 0;
        let mut cdc_failed = 0;

        for (_, status_value) in cdc_status.iter() {
            match status_value {
                CDCStatus::Pending => cdc_pending += 1,
                CDCStatus::Processing => cdc_processing += 1,
                CDCStatus::Completed => cdc_completed += 1,
                CDCStatus::Failed(_) => cdc_failed += 1,
            }
        }

        let mut projection_pending = 0;
        let mut projection_processing = 0;
        let mut projection_completed = 0;
        let mut projection_failed = 0;

        for (_, status_value) in projection_status.iter() {
            match status_value {
                ProjectionStatus::Pending => projection_pending += 1,
                ProjectionStatus::Processing => projection_processing += 1,
                ProjectionStatus::Completed => projection_completed += 1,
                ProjectionStatus::Failed(_) => projection_failed += 1,
            }
        }

        serde_json::json!({
            "total_tracked": timestamps.len(),
            "cdc": {
                "total": cdc_status.len(),
                "pending": cdc_pending,
                "processing": cdc_processing,
                "completed": cdc_completed,
                "failed": cdc_failed,
            },
            "projection": {
                "total": projection_status.len(),
                "pending": projection_pending,
                "processing": projection_processing,
                "completed": projection_completed,
                "failed": projection_failed,
            },
            "max_wait_time_ms": self.max_wait_time.as_millis(),
            "cleanup_interval_ms": self.cleanup_interval.as_millis(),
        })
    }
}

impl Default for ConsistencyManager {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(10), // 10 second max wait time
            Duration::from_secs(60), // Clean up every minute
        )
    }
}

/// Trait for services that need read-after-write consistency
#[async_trait::async_trait]
pub trait ConsistencyAware {
    /// Wait for CDC consistency before proceeding
    async fn wait_for_consistency(&self, aggregate_id: Uuid) -> Result<()>;

    /// Mark an operation as pending CDC processing
    async fn mark_pending(&self, aggregate_id: Uuid);

    /// Mark an operation as completed by CDC
    async fn mark_completed(&self, aggregate_id: Uuid);
}

#[cfg(test)]
#[ignore]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_consistency_manager() {
        let manager =
            ConsistencyManager::new(Duration::from_millis(100), Duration::from_millis(50));

        let aggregate_id = Uuid::new_v4();

        // Test pending -> processing -> completed flow
        manager.mark_pending(aggregate_id).await;
        assert_eq!(
            manager.get_status(aggregate_id).await,
            Some(CDCStatus::Pending)
        );

        manager.mark_processing(aggregate_id).await;
        assert_eq!(
            manager.get_status(aggregate_id).await,
            Some(CDCStatus::Processing)
        );

        manager.mark_completed(aggregate_id).await;
        assert_eq!(
            manager.get_status(aggregate_id).await,
            Some(CDCStatus::Completed)
        );

        // Test wait for consistency
        let result = manager.wait_for_consistency(aggregate_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_consistency_timeout() {
        let manager = ConsistencyManager::new(
            Duration::from_millis(50), // Short timeout
            Duration::from_millis(100),
        );

        let aggregate_id = Uuid::new_v4();
        manager.mark_pending(aggregate_id).await;

        // This should timeout
        let result = manager.wait_for_consistency(aggregate_id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Timeout"));
    }

    #[tokio::test]
    async fn test_consistency_failure() {
        let manager =
            ConsistencyManager::new(Duration::from_millis(100), Duration::from_millis(50));

        let aggregate_id = Uuid::new_v4();
        manager.mark_pending(aggregate_id).await;
        manager
            .mark_failed(aggregate_id, "Test error".to_string())
            .await;

        let result = manager.wait_for_consistency(aggregate_id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Test error"));
    }
}
