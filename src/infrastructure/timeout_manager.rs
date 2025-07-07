use serde;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    pub database_operation_timeout: Duration,
    pub cache_operation_timeout: Duration,
    pub kafka_operation_timeout: Duration,
    pub redis_operation_timeout: Duration,
    pub batch_processing_timeout: Duration,
    pub health_check_timeout: Duration,
    pub connection_acquire_timeout: Duration,
    pub transaction_timeout: Duration,
    pub lock_timeout: Duration,
    pub retry_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            database_operation_timeout: Duration::from_secs(30),
            cache_operation_timeout: Duration::from_secs(10),
            kafka_operation_timeout: Duration::from_secs(15),
            redis_operation_timeout: Duration::from_secs(5),
            batch_processing_timeout: Duration::from_secs(60),
            health_check_timeout: Duration::from_secs(10),
            connection_acquire_timeout: Duration::from_secs(10),
            transaction_timeout: Duration::from_secs(30),
            lock_timeout: Duration::from_secs(5),
            retry_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeoutManager {
    config: TimeoutConfig,
    active_timeouts: Arc<RwLock<HashMap<String, TimeoutInfo>>>,
    timeout_stats: Arc<RwLock<TimeoutStats>>,
}

#[derive(Debug, Clone)]
pub struct TimeoutInfo {
    pub operation_id: String,
    pub operation_type: String,
    pub start_time: Instant,
    pub timeout: Duration,
    pub resource: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TimeoutStats {
    pub total_timeouts: u64,
    pub database_timeouts: u64,
    pub cache_timeouts: u64,
    pub kafka_timeouts: u64,
    pub redis_timeouts: u64,
    pub batch_timeouts: u64,
    pub last_reset_timestamp: u64,
}

impl TimeoutManager {
    pub fn new(config: TimeoutConfig) -> Self {
        let manager = Self {
            config,
            active_timeouts: Arc::new(RwLock::new(HashMap::new())),
            timeout_stats: Arc::new(RwLock::new(TimeoutStats::default())),
        };

        // Start background cleanup
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            manager_clone.cleanup_expired_timeouts().await;
        });

        manager
    }

    pub async fn with_timeout<F, T, E>(
        &self,
        operation_type: &str,
        operation_id: &str,
        timeout_duration: Duration,
        operation: F,
    ) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = Result<T, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        let timeout_id = operation_type.to_string() + "_" + operation_id;

        // Register timeout
        self.register_timeout(&timeout_id, operation_type, timeout_duration)
            .await;

        let result = timeout(timeout_duration, operation).await;

        // Remove timeout registration
        self.remove_timeout(&timeout_id).await;

        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(TimeoutError::OperationError(Box::new(e))),
            Err(_) => {
                self.record_timeout(operation_type).await;
                Err(TimeoutError::Timeout(operation_type.to_string()))
            }
        }
    }

    pub async fn with_database_timeout<F, T, E>(
        &self,
        operation_id: &str,
        operation: F,
    ) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = Result<T, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.with_timeout(
            "database",
            operation_id,
            self.config.database_operation_timeout,
            operation,
        )
        .await
    }

    pub async fn with_cache_timeout<F, T, E>(
        &self,
        operation_id: &str,
        operation: F,
    ) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = Result<T, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.with_timeout(
            "cache",
            operation_id,
            self.config.cache_operation_timeout,
            operation,
        )
        .await
    }

    pub async fn with_kafka_timeout<F, T, E>(
        &self,
        operation_id: &str,
        operation: F,
    ) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = Result<T, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.with_timeout(
            "kafka",
            operation_id,
            self.config.kafka_operation_timeout,
            operation,
        )
        .await
    }

    pub async fn with_redis_timeout<F, T, E>(
        &self,
        operation_id: &str,
        operation: F,
    ) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = Result<T, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.with_timeout(
            "redis",
            operation_id,
            self.config.redis_operation_timeout,
            operation,
        )
        .await
    }

    pub async fn with_batch_timeout<F, T, E>(
        &self,
        operation_id: &str,
        operation: F,
    ) -> Result<T, TimeoutError>
    where
        F: std::future::Future<Output = Result<T, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.with_timeout(
            "batch",
            operation_id,
            self.config.batch_processing_timeout,
            operation,
        )
        .await
    }

    async fn register_timeout(
        &self,
        timeout_id: &str,
        operation_type: &str,
        timeout_duration: Duration,
    ) {
        let mut timeouts = self.active_timeouts.write().await;
        timeouts.insert(
            timeout_id.to_string(),
            TimeoutInfo {
                operation_id: timeout_id.to_string(),
                operation_type: operation_type.to_string(),
                start_time: Instant::now(),
                timeout: timeout_duration,
                resource: "timeout".to_string(),
            },
        );
    }

    async fn remove_timeout(&self, timeout_id: &str) {
        let mut timeouts = self.active_timeouts.write().await;
        timeouts.remove(timeout_id);
    }

    async fn record_timeout(&self, operation_type: &str) {
        let mut stats = self.timeout_stats.write().await;
        stats.total_timeouts += 1;

        match operation_type {
            "database" => stats.database_timeouts += 1,
            "cache" => stats.cache_timeouts += 1,
            "kafka" => stats.kafka_timeouts += 1,
            "redis" => stats.redis_timeouts += 1,
            "batch" => stats.batch_timeouts += 1,
            _ => {}
        }
    }

    async fn cleanup_expired_timeouts(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            let mut timeouts = self.active_timeouts.write().await;
            let now = Instant::now();
            let expired: Vec<String> = timeouts
                .iter()
                .filter(|(_, info)| now.duration_since(info.start_time) > info.timeout)
                .map(|(id, _)| id.clone())
                .collect();

            for timeout_id in expired {
                info!("Found expired timeout: {}", timeout_id);
                timeouts.remove(&timeout_id);
            }
        }
    }

    pub async fn get_stats(&self) -> TimeoutStats {
        self.timeout_stats.read().await.clone()
    }

    pub async fn reset_stats(&self) {
        let mut stats = self.timeout_stats.write().await;
        *stats = TimeoutStats::default();
    }

    pub async fn get_active_timeouts(&self) -> Vec<TimeoutInfo> {
        self.active_timeouts
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub fn get_config(&self) -> &TimeoutConfig {
        &self.config
    }
}

#[derive(Debug)]
pub enum TimeoutError {
    Timeout(String),
    OperationError(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutError::Timeout(msg) => f
                .write_str("Operation timed out: ")
                .and_then(|_| f.write_str(msg)),
            TimeoutError::OperationError(err) => f
                .write_str("Operation failed: ")
                .and_then(|_| f.write_str(&err.to_string())),
        }
    }
}

impl std::error::Error for TimeoutError {}

// Helper macro for automatic timeout handling
#[macro_export]
macro_rules! with_timeout {
    ($manager:expr, $operation_type:expr, $operation_id:expr, $block:expr) => {
        $manager
            .with_timeout(
                $operation_type,
                $operation_id,
                $manager.get_config().database_operation_timeout,
                $block,
            )
            .await
    };
}

#[macro_export]
macro_rules! with_db_timeout {
    ($manager:expr, $operation_id:expr, $block:expr) => {
        $manager.with_database_timeout($operation_id, $block).await
    };
}

#[macro_export]
macro_rules! with_cache_timeout {
    ($manager:expr, $operation_id:expr, $block:expr) => {
        $manager.with_cache_timeout($operation_id, $block).await
    };
}

#[macro_export]
macro_rules! with_kafka_timeout {
    ($manager:expr, $operation_id:expr, $block:expr) => {
        $manager.with_kafka_timeout($operation_id, $block).await
    };
}

#[macro_export]
macro_rules! with_redis_timeout {
    ($manager:expr, $operation_id:expr, $block:expr) => {
        $manager.with_redis_timeout($operation_id, $block).await
    };
}

impl Default for TimeoutStats {
    fn default() -> Self {
        Self {
            total_timeouts: 0,
            database_timeouts: 0,
            cache_timeouts: 0,
            kafka_timeouts: 0,
            redis_timeouts: 0,
            batch_timeouts: 0,
            last_reset_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}
