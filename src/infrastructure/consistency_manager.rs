use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
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
    }

    /// Mark an operation as failed in CDC pipeline
    pub async fn mark_failed(&self, aggregate_id: Uuid, error: String) {
        let mut status = self.cdc_status.write().await;
        status.insert(aggregate_id, CDCStatus::Failed(error.clone()));

        error!(
            "Marked aggregate {} as failed in CDC: {}",
            aggregate_id, error
        );
    }

    // Projection synchronization methods
    /// Mark projection synchronization as pending
    pub async fn mark_projection_pending(&self, aggregate_id: Uuid) {
        let mut status = self.projection_status.write().await;
        status.insert(aggregate_id, ProjectionStatus::Pending);

        let mut timestamps = self.operation_timestamps.write().await;
        timestamps.insert(aggregate_id, Instant::now());

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
    }

    /// Mark projection synchronization as failed
    pub async fn mark_projection_failed(&self, aggregate_id: Uuid, error: String) {
        let mut status = self.projection_status.write().await;
        status.insert(aggregate_id, ProjectionStatus::Failed(error.clone()));

        error!(
            "Marked aggregate {} as failed projection: {}",
            aggregate_id, error
        );
    }

    /// Wait for CDC processing to complete for a specific aggregate
    pub async fn wait_for_consistency(&self, aggregate_id: Uuid) -> Result<()> {
        let start_time = Instant::now();
        let poll_interval = Duration::from_millis(10);
        tracing::info!(
            "[ConsistencyManager] Enter wait_for_consistency for {}",
            aggregate_id
        );

        loop {
            if start_time.elapsed() > self.max_wait_time {
                tracing::warn!(
                    "[ConsistencyManager] TIMEOUT in wait_for_consistency for {} after {:?}",
                    aggregate_id,
                    self.max_wait_time
                );
                return Err(anyhow::anyhow!(
                    "Timeout waiting for CDC consistency for aggregate {} after {:?}",
                    aggregate_id,
                    self.max_wait_time
                ));
            }

            let status = {
                let status_map = self.cdc_status.read().await;
                status_map.get(&aggregate_id).cloned()
            };
            tracing::info!("[ConsistencyManager] wait_for_consistency: aggregate_id={} status={:?} elapsed_ms={}", aggregate_id, status, start_time.elapsed().as_millis());

            match status {
                Some(CDCStatus::Completed) => {
                    tracing::info!(
                        "[ConsistencyManager] CDCStatus::Completed for {}",
                        aggregate_id
                    );
                    return Ok(());
                }
                Some(CDCStatus::Failed(ref error)) => {
                    tracing::warn!(
                        "[ConsistencyManager] CDCStatus::Failed for {}: {}",
                        aggregate_id,
                        error
                    );
                    return Err(anyhow::anyhow!(
                        "CDC processing failed for aggregate {}: {}",
                        aggregate_id,
                        error
                    ));
                }
                Some(CDCStatus::Processing) => {
                    tracing::debug!(
                        "[ConsistencyManager] CDCStatus::Processing for {}",
                        aggregate_id
                    );
                }
                Some(CDCStatus::Pending) => {
                    tracing::debug!(
                        "[ConsistencyManager] CDCStatus::Pending for {}",
                        aggregate_id
                    );
                }
                None => {
                    if start_time.elapsed() > Duration::from_secs(3) {
                        tracing::warn!("[ConsistencyManager] FALLBACK: No CDC status for {} after 3s, allowing read", aggregate_id);
                        return Ok(());
                    }
                    tracing::debug!(
                        "[ConsistencyManager] No CDC status for {}, waiting for fallback timeout",
                        aggregate_id
                    );
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Wait for projection synchronization to complete for a specific aggregate
    pub async fn wait_for_projection_sync(&self, aggregate_id: Uuid) -> Result<()> {
        let start_time = Instant::now();
        let poll_interval = Duration::from_millis(25);
        let fallback_timeout = Duration::from_secs(5);
        tracing::info!(
            "[ConsistencyManager] Enter wait_for_projection_sync for {}",
            aggregate_id
        );

        loop {
            if start_time.elapsed() > self.max_wait_time {
                tracing::warn!(
                    "[ConsistencyManager] TIMEOUT in wait_for_projection_sync for {} after {:?}",
                    aggregate_id,
                    self.max_wait_time
                );
                return Err(anyhow::anyhow!(
                    "Timeout waiting for projection synchronization for aggregate {} after {:?}",
                    aggregate_id,
                    self.max_wait_time
                ));
            }

            let status = {
                let status_map = self.projection_status.read().await;
                status_map.get(&aggregate_id).cloned()
            };
            tracing::info!("[ConsistencyManager] wait_for_projection_sync: aggregate_id={} status={:?} elapsed_ms={}", aggregate_id, status, start_time.elapsed().as_millis());

            match status {
                Some(ProjectionStatus::Completed) => {
                    tracing::info!(
                        "[ConsistencyManager] ProjectionStatus::Completed for {}",
                        aggregate_id
                    );
                    return Ok(());
                }
                Some(ProjectionStatus::Failed(ref error)) => {
                    tracing::warn!(
                        "[ConsistencyManager] ProjectionStatus::Failed for {}: {}",
                        aggregate_id,
                        error
                    );
                    return Err(anyhow::anyhow!(
                        "Projection synchronization failed for aggregate {}: {}",
                        aggregate_id,
                        error
                    ));
                }
                Some(ProjectionStatus::Processing) => {
                    tracing::debug!(
                        "[ConsistencyManager] ProjectionStatus::Processing for {}",
                        aggregate_id
                    );
                }
                Some(ProjectionStatus::Pending) => {
                    tracing::debug!(
                        "[ConsistencyManager] ProjectionStatus::Pending for {}",
                        aggregate_id
                    );
                }
                None => {
                    if start_time.elapsed() > fallback_timeout {
                        tracing::warn!("[ConsistencyManager] FALLBACK: No projection status for {} after {:?}, allowing read", aggregate_id, fallback_timeout);
                        return Ok(());
                    }
                    tracing::debug!("[ConsistencyManager] No projection status for {}, waiting for fallback timeout", aggregate_id);
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Wait for CDC processing to complete for multiple aggregates
    pub async fn wait_for_consistency_batch(&self, aggregate_ids: Vec<Uuid>) -> Result<()> {
        let mut results = Vec::new();

        for aggregate_id in aggregate_ids {
            let result = self.wait_for_consistency(aggregate_id).await;
            results.push((aggregate_id, result));
        }

        // Check if any failed
        let failures: Vec<_> = results
            .iter()
            .filter_map(|(id, result)| {
                if let Err(ref e) = result {
                    Some((*id, e.clone()))
                } else {
                    None
                }
            })
            .collect();

        if !failures.is_empty() {
            let error_messages: Vec<String> = failures
                .iter()
                .map(|(id, e)| format!("Aggregate {}: {}", id, e))
                .collect();

            return Err(anyhow::anyhow!(
                "CDC consistency failed for some aggregates: {}",
                error_messages.join(", ")
            ));
        }

        Ok(())
    }

    /// Wait for projection synchronization to complete for multiple aggregates
    pub async fn wait_for_projection_sync_batch(&self, aggregate_ids: Vec<Uuid>) -> Result<()> {
        let mut results = Vec::new();

        for aggregate_id in aggregate_ids {
            let result = self.wait_for_projection_sync(aggregate_id).await;
            results.push((aggregate_id, result));
        }

        // Check if any failed
        let failures: Vec<_> = results
            .iter()
            .filter_map(|(id, result)| {
                if let Err(ref e) = result {
                    Some((*id, e.clone()))
                } else {
                    None
                }
            })
            .collect();

        if !failures.is_empty() {
            let error_messages: Vec<String> = failures
                .iter()
                .map(|(id, e)| format!("Aggregate {}: {}", id, e))
                .collect();

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
