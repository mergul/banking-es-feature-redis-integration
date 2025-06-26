use anyhow::Result;
use serde;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct DeadlockInfo {
    pub operation_id: String,
    pub operation_type: String,
    pub start_time: Instant,
    pub timeout: Duration,
    pub resources: Vec<String>,
    pub stack_trace: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeadlockDetector {
    active_operations: Arc<RwLock<HashMap<String, DeadlockInfo>>>,
    resource_locks: Arc<RwLock<HashMap<String, String>>>, // resource -> operation_id
    config: DeadlockConfig,
}

#[derive(Debug, Clone)]
pub struct DeadlockConfig {
    pub check_interval: Duration,
    pub operation_timeout: Duration,
    pub max_concurrent_operations: usize,
    pub enable_auto_resolution: bool,
    pub log_suspicious_operations: bool,
}

impl Default for DeadlockConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(30),
            max_concurrent_operations: 1000,
            enable_auto_resolution: true,
            log_suspicious_operations: true,
        }
    }
}

impl DeadlockDetector {
    pub fn new(config: DeadlockConfig) -> Self {
        let detector = Self {
            active_operations: Arc::new(RwLock::new(HashMap::new())),
            resource_locks: Arc::new(RwLock::new(HashMap::new())),
            config,
        };

        // Start background monitoring
        let detector_clone = detector.clone();
        tokio::spawn(async move {
            detector_clone.monitor_operations().await;
        });

        detector
    }

    pub async fn start_operation(
        &self,
        operation_id: String,
        operation_type: String,
    ) -> Result<(), String> {
        let mut operations = self.active_operations.write().await;

        if operations.len() >= self.config.max_concurrent_operations {
            return Err("Too many concurrent operations".to_string());
        }

        let info = DeadlockInfo {
            operation_id: operation_id.clone(),
            operation_type,
            start_time: Instant::now(),
            timeout: self.config.operation_timeout,
            resources: Vec::new(),
            stack_trace: None,
        };

        operations.insert(operation_id, info);
        Ok(())
    }

    pub async fn end_operation(&self, operation_id: &str) {
        let mut operations = self.active_operations.write().await;
        operations.remove(operation_id);

        // Clean up resource locks
        let mut resource_locks = self.resource_locks.write().await;
        resource_locks.retain(|_, op_id| op_id != operation_id);
    }

    pub async fn acquire_resource(
        &self,
        operation_id: &str,
        resource: &str,
    ) -> Result<bool, String> {
        let mut resource_locks = self.resource_locks.write().await;

        if let Some(existing_op) = resource_locks.get(resource) {
            if existing_op != operation_id {
                // Potential deadlock detected
                warn!(
                    "Potential deadlock detected: operation {} trying to acquire resource {} held by {}",
                    operation_id, resource, existing_op
                );
                return Ok(false);
            }
        }

        resource_locks.insert(resource.to_string(), operation_id.to_string());

        // Update operation info
        let mut operations = self.active_operations.write().await;
        if let Some(op_info) = operations.get_mut(operation_id) {
            if !op_info.resources.contains(&resource.to_string()) {
                op_info.resources.push(resource.to_string());
            }
        }

        Ok(true)
    }

    pub async fn release_resource(&self, operation_id: &str, resource: &str) {
        let mut resource_locks = self.resource_locks.write().await;
        if let Some(op_id) = resource_locks.get(resource) {
            if op_id == operation_id {
                resource_locks.remove(resource);
            }
        }
    }

    async fn monitor_operations(&self) {
        let mut interval = tokio::time::interval(self.config.check_interval);

        loop {
            interval.tick().await;

            let operations = self.active_operations.read().await;
            let now = Instant::now();
            let mut stuck_operations = Vec::new();

            for (op_id, info) in operations.iter() {
                if now.duration_since(info.start_time) > info.timeout {
                    stuck_operations.push((op_id.clone(), info.clone()));
                }
            }

            if !stuck_operations.is_empty() {
                warn!("Found {} stuck operations", stuck_operations.len());

                for (op_id, info) in stuck_operations {
                    warn!(
                        "Stuck operation: {} (type: {}) running for {:.2}s",
                        op_id,
                        info.operation_type,
                        now.duration_since(info.start_time).as_secs_f64()
                    );

                    if self.config.enable_auto_resolution {
                        self.attempt_resolution(&op_id, &info).await;
                    }
                }
            }

            // Log suspicious operations
            if self.config.log_suspicious_operations {
                for (op_id, info) in operations.iter() {
                    let duration = now.duration_since(info.start_time);
                    if duration > info.timeout / 2 {
                        warn!(
                            "Suspicious operation: {} (type: {}) running for {:.2}s",
                            op_id,
                            info.operation_type,
                            duration.as_secs_f64()
                        );
                    }
                }
            }
        }
    }

    async fn attempt_resolution(&self, operation_id: &str, info: &DeadlockInfo) {
        info!("Attempting to resolve stuck operation: {}", operation_id);

        // Release all resources held by this operation
        let mut resource_locks = self.resource_locks.write().await;
        resource_locks.retain(|_, op_id| op_id != operation_id);

        // Remove the operation
        let mut operations = self.active_operations.write().await;
        operations.remove(operation_id);

        info!("Successfully resolved stuck operation: {}", operation_id);
    }

    pub async fn get_stats(&self) -> DeadlockStats {
        let operations = self.active_operations.read().await;
        let resource_locks = self.resource_locks.read().await;

        DeadlockStats {
            active_operations: operations.len(),
            locked_resources: resource_locks.len(),
            operation_types: operations
                .values()
                .map(|op| op.operation_type.clone())
                .collect::<std::collections::HashSet<_>>()
                .len(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeadlockStats {
    pub active_operations: usize,
    pub locked_resources: usize,
    pub operation_types: usize,
}

// Helper macro for automatic operation tracking
#[macro_export]
macro_rules! track_operation {
    ($detector:expr, $operation_type:expr, $operation_id:expr, $block:expr) => {{
        let op_id = $operation_id.to_string();
        let op_type = $operation_type.to_string();

        if let Err(e) = $detector
            .start_operation(op_id.clone(), op_type.clone())
            .await
        {
            return Err(anyhow::anyhow!("Failed to start operation tracking: {}", e));
        }

        let result = $block.await;

        $detector.end_operation(&op_id).await;

        result
    }};
}

// Resource acquisition helper
#[macro_export]
macro_rules! with_resource {
    ($detector:expr, $operation_id:expr, $resource:expr, $block:expr) => {{
        let resource = $resource.to_string();

        if !$detector.acquire_resource($operation_id, &resource).await? {
            return Err(anyhow::anyhow!("Failed to acquire resource: {}", resource));
        }

        let result = $block.await;

        $detector.release_resource($operation_id, &resource).await;

        result
    }};
}
