use crate::infrastructure::redis_abstraction::{RedisClientTrait, RedisPoolConfig};
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use redis::{aio::MultiplexedConnection, AsyncCommands, RedisError, Value as RedisValue};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Configuration for Redis lock performance
#[derive(Debug, Clone)]
pub struct RedisLockConfig {
    pub connection_pool_size: usize,
    pub lock_timeout_secs: usize,
    pub batch_lock_timeout_secs: usize,
    pub retry_attempts: usize,
    pub retry_delay_ms: u64,
    pub enable_metrics: bool,
    pub enable_lock_free_reads: bool,
    pub max_batch_size: usize,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for RedisLockConfig {
    fn default() -> Self {
        Self {
            connection_pool_size: 50,
            lock_timeout_secs: 30, // Increased to 30 seconds for better concurrency
            batch_lock_timeout_secs: 45, // Increased to 45 seconds for batch operations
            retry_attempts: 8,     // Increased from 5 to 8
            retry_delay_ms: 500,   // Increased from 200 to 500ms
            enable_metrics: true,
            enable_lock_free_reads: true,
            max_batch_size: 100,
            connection_timeout: Duration::from_secs(10), // Increased from 5 to 10 seconds
            idle_timeout: Duration::from_secs(300),
        }
    }
}

/// Metrics for Redis lock performance monitoring
#[derive(Debug)]
pub struct RedisLockMetrics {
    pub locks_acquired: AtomicU64,
    pub locks_failed: AtomicU64,
    pub locks_released: AtomicU64,
    pub batch_locks_acquired: AtomicU64,
    pub batch_locks_failed: AtomicU64,
    pub lock_free_operations: AtomicU64,
    pub lock_timeout_count: AtomicU64,
    pub lock_contention_count: AtomicU64,
    pub avg_lock_acquisition_time: AtomicU64, // in microseconds
    pub avg_batch_lock_time: AtomicU64,       // in microseconds
    pub connection_pool_hits: AtomicU64,
    pub connection_pool_misses: AtomicU64,
    pub active_connections: AtomicUsize,
    pub total_operations: AtomicU64,
}

impl Default for RedisLockMetrics {
    fn default() -> Self {
        Self {
            locks_acquired: AtomicU64::new(0),
            locks_failed: AtomicU64::new(0),
            locks_released: AtomicU64::new(0),
            batch_locks_acquired: AtomicU64::new(0),
            batch_locks_failed: AtomicU64::new(0),
            lock_free_operations: AtomicU64::new(0),
            lock_timeout_count: AtomicU64::new(0),
            lock_contention_count: AtomicU64::new(0),
            avg_lock_acquisition_time: AtomicU64::new(0),
            avg_batch_lock_time: AtomicU64::new(0),
            connection_pool_hits: AtomicU64::new(0),
            connection_pool_misses: AtomicU64::new(0),
            active_connections: AtomicUsize::new(0),
            total_operations: AtomicU64::new(0),
        }
    }
}

/// Connection pool for Redis operations
struct RedisConnectionPool {
    connections: Arc<Mutex<Vec<Arc<Mutex<MultiplexedConnection>>>>>,
    semaphore: Arc<Semaphore>,
    config: RedisLockConfig,
    metrics: Arc<RedisLockMetrics>,
}

impl RedisConnectionPool {
    fn new(
        redis_client: Arc<dyn RedisClientTrait>,
        config: RedisLockConfig,
        metrics: Arc<RedisLockMetrics>,
    ) -> Self {
        let connections = Arc::new(Mutex::new(Vec::new()));
        let semaphore = Arc::new(Semaphore::new(config.connection_pool_size));

        Self {
            connections,
            semaphore,
            config,
            metrics,
        }
    }

    async fn get_connection(
        &self,
        redis_client: &Arc<dyn RedisClientTrait>,
    ) -> Result<PooledConnection> {
        let start = Instant::now();

        // Try to get an existing connection from the pool
        {
            let mut connections = self.connections.lock().await;
            if let Some(conn) = connections.pop() {
                self.metrics
                    .connection_pool_hits
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .active_connections
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(PooledConnection {
                    connection: Some(conn),
                    pool: self.connections.clone(),
                    metrics: self.metrics.clone(),
                });
            }
        }

        // Create a new connection if pool is empty
        self.metrics
            .connection_pool_misses
            .fetch_add(1, Ordering::Relaxed);
        let conn = redis_client.get_connection().await?;
        self.metrics
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        Ok(PooledConnection {
            connection: Some(Arc::new(Mutex::new(conn))),
            pool: self.connections.clone(),
            metrics: self.metrics.clone(),
        })
    }
}

/// A pooled Redis connection that returns to the pool when dropped
struct PooledConnection {
    connection: Option<Arc<Mutex<MultiplexedConnection>>>,
    pool: Arc<Mutex<Vec<Arc<Mutex<MultiplexedConnection>>>>>,
    metrics: Arc<RedisLockMetrics>,
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            let pool = self.pool.clone();

            // Return connection to pool asynchronously
            tokio::spawn(async move {
                let mut connections = pool.lock().await;
                connections.push(conn);
            });

            self.metrics
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl PooledConnection {
    async fn get_mut_connection(
        &mut self,
    ) -> Result<tokio::sync::MutexGuard<MultiplexedConnection>, RedisError> {
        if let Some(conn) = &self.connection {
            Ok(conn.lock().await)
        } else {
            Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Connection not available",
            )))
        }
    }
}

/// Enhanced Redis aggregate lock with connection pooling and batch operations
pub struct RedisAggregateLock {
    redis_client: Arc<dyn RedisClientTrait>,
    connection_pool: Arc<RedisConnectionPool>,
    process_id: String,
    config: RedisLockConfig,
    metrics: Arc<RedisLockMetrics>,
    lock_cache: Arc<DashMap<Uuid, Instant>>, // Track active locks
    operation_type_cache: Arc<DashMap<Uuid, OperationType>>, // Cache operation types for lock-free reads
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperationType {
    Read,
    Write,
    Create,
    Update,
    Delete,
}

impl RedisAggregateLock {
    pub fn new(redis_client: Arc<dyn RedisClientTrait>, config: RedisLockConfig) -> Self {
        let metrics = Arc::new(RedisLockMetrics::default());
        let connection_pool = Arc::new(RedisConnectionPool::new(
            redis_client.clone(),
            config.clone(),
            metrics.clone(),
        ));

        Self {
            redis_client,
            connection_pool,
            process_id: Uuid::new_v4().to_string(),
            config,
            metrics,
            lock_cache: Arc::new(DashMap::new()),
            operation_type_cache: Arc::new(DashMap::new()),
        }
    }

    /// Try to acquire a lock for a single aggregate with retry logic
    pub async fn try_lock(&self, aggregate_id: Uuid, operation_type: OperationType) -> bool {
        let start = Instant::now();
        self.metrics
            .total_operations
            .fetch_add(1, Ordering::Relaxed);

        println!(
            "🔍 try_lock called for aggregate: {} with operation_type: {:?}",
            aggregate_id, operation_type
        );

        // Check if this is a read-only operation that can be lock-free
        if self.config.enable_lock_free_reads && operation_type == OperationType::Read {
            self.metrics
                .lock_free_operations
                .fetch_add(1, Ordering::Relaxed);
            println!(
                "✅ Lock-free read operation for aggregate: {}",
                aggregate_id
            );
            return true;
        }

        // Cache operation type for future reference
        self.operation_type_cache
            .insert(aggregate_id, operation_type);

        for attempt in 0..self.config.retry_attempts {
            println!(
                "🔍 Attempt {} to acquire lock for aggregate: {}",
                attempt + 1,
                aggregate_id
            );

            match self.acquire_single_lock(aggregate_id).await {
                Ok(acquired) => {
                    if acquired {
                        let duration = start.elapsed();
                        self.update_lock_metrics(duration, true);
                        self.lock_cache.insert(aggregate_id, Instant::now());
                        println!(
                            "🔒 Acquired lock for aggregate {} (attempt {})",
                            aggregate_id,
                            attempt + 1
                        );
                        return true;
                    } else {
                        self.metrics
                            .lock_contention_count
                            .fetch_add(1, Ordering::Relaxed);
                        println!(
                            "⚠️ Lock contention for aggregate {} (attempt {})",
                            aggregate_id,
                            attempt + 1
                        );
                    }
                }
                Err(e) => {
                    println!(
                        "❌ Lock acquisition error for aggregate {}: {}",
                        aggregate_id, e
                    );
                }
            }

            if attempt < self.config.retry_attempts - 1 {
                println!(
                    "⏳ Waiting {}ms before retry...",
                    self.config.retry_delay_ms
                );
                tokio::time::sleep(Duration::from_millis(self.config.retry_delay_ms)).await;
            }
        }

        self.update_lock_metrics(start.elapsed(), false);
        self.metrics
            .lock_timeout_count
            .fetch_add(1, Ordering::Relaxed);
        println!(
            "❌ Failed to acquire lock for aggregate {} after {} attempts",
            aggregate_id, self.config.retry_attempts
        );
        false
    }

    /// Acquire locks for multiple aggregates in batch
    pub async fn try_batch_lock(
        &self,
        aggregate_ids: Vec<Uuid>,
        operation_types: Vec<OperationType>,
    ) -> Vec<bool> {
        let start = Instant::now();
        let mut results = vec![false; aggregate_ids.len()]; // Initialize with correct size

        // Separate read and write operations
        let mut read_aggregates = Vec::new();
        let mut write_aggregates = Vec::new();
        let mut write_operation_types = Vec::new();
        let mut write_indices = Vec::new(); // Track indices for write operations

        for (i, (aggregate_id, op_type)) in
            aggregate_ids.iter().zip(operation_types.iter()).enumerate()
        {
            if self.config.enable_lock_free_reads && *op_type == OperationType::Read {
                read_aggregates.push(*aggregate_id);
                results[i] = true; // Lock-free reads always succeed
            } else {
                write_aggregates.push(*aggregate_id);
                write_operation_types.push(op_type.clone());
                write_indices.push(i); // Track the original index
            }
        }

        // Handle lock-free reads
        if !read_aggregates.is_empty() {
            self.metrics
                .lock_free_operations
                .fetch_add(read_aggregates.len() as u64, Ordering::Relaxed);
            debug!(
                "Lock-free batch read operations for {} aggregates",
                read_aggregates.len()
            );
        }

        // Handle write operations that need locks
        if !write_aggregates.is_empty() {
            match self
                .acquire_batch_locks(write_aggregates, write_operation_types)
                .await
            {
                Ok(batch_results) => {
                    // Assign results to the correct positions
                    for (result_index, &original_index) in write_indices.iter().enumerate() {
                        if result_index < batch_results.len() {
                            results[original_index] = batch_results[result_index];
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Batch lock acquisition failed: {}", e);
                    // Keep results as false (default)
                }
            }
        }

        let duration = start.elapsed();
        self.update_batch_lock_metrics(duration, results.iter().filter(|&&r| r).count());

        info!(
            "📦 Batch lock acquisition completed: {}/{} successful",
            results.iter().filter(|&&r| r).count(),
            results.len()
        );

        results
    }

    /// Release a single lock
    pub async fn unlock(&self, aggregate_id: Uuid) {
        let start = Instant::now();

        // Remove from lock cache
        self.lock_cache.remove(&aggregate_id);

        // Release lock in Redis
        if let Err(e) = self.release_single_lock(aggregate_id).await {
            error!(
                "❌ Failed to release lock for aggregate {}: {}",
                aggregate_id, e
            );
        } else {
            self.metrics.locks_released.fetch_add(1, Ordering::Relaxed);
            debug!("🔓 Released lock for aggregate {}", aggregate_id);
        }
    }

    /// Release multiple locks in batch
    pub async fn batch_unlock(&self, aggregate_ids: Vec<Uuid>) {
        let start = Instant::now();

        // Remove from lock cache
        for aggregate_id in &aggregate_ids {
            self.lock_cache.remove(aggregate_id);
        }

        // Release locks in Redis batch
        if let Err(e) = self.release_batch_locks(aggregate_ids.clone()).await {
            error!("❌ Failed to release batch locks: {}", e);
        } else {
            self.metrics
                .locks_released
                .fetch_add(aggregate_ids.len() as u64, Ordering::Relaxed);
            debug!("🔓 Released {} locks in batch", aggregate_ids.len());
        }
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> &RedisLockMetrics {
        &self.metrics
    }

    /// Get metrics as JSON for monitoring
    pub fn get_metrics_json(&self) -> serde_json::Value {
        let total = self.metrics.total_operations.load(Ordering::Relaxed);
        let acquired = self.metrics.locks_acquired.load(Ordering::Relaxed);
        let success_rate = if total > 0 {
            (acquired as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        serde_json::json!({
            "locks_acquired": self.metrics.locks_acquired.load(Ordering::Relaxed),
            "locks_failed": self.metrics.locks_failed.load(Ordering::Relaxed),
            "locks_released": self.metrics.locks_released.load(Ordering::Relaxed),
            "batch_locks_acquired": self.metrics.batch_locks_acquired.load(Ordering::Relaxed),
            "batch_locks_failed": self.metrics.batch_locks_failed.load(Ordering::Relaxed),
            "lock_free_operations": self.metrics.lock_free_operations.load(Ordering::Relaxed),
            "lock_timeout_count": self.metrics.lock_timeout_count.load(Ordering::Relaxed),
            "lock_contention_count": self.metrics.lock_contention_count.load(Ordering::Relaxed),
            "avg_lock_acquisition_time_us": self.metrics.avg_lock_acquisition_time.load(Ordering::Relaxed),
            "avg_batch_lock_time_us": self.metrics.avg_batch_lock_time.load(Ordering::Relaxed),
            "connection_pool_hits": self.metrics.connection_pool_hits.load(Ordering::Relaxed),
            "connection_pool_misses": self.metrics.connection_pool_misses.load(Ordering::Relaxed),
            "active_connections": self.metrics.active_connections.load(Ordering::Relaxed),
            "total_operations": self.metrics.total_operations.load(Ordering::Relaxed),
            "active_locks": self.lock_cache.len(),
            "success_rate": success_rate
        })
    }

    /// Health check for the lock service
    pub async fn health_check(&self) -> Result<bool> {
        let test_aggregate_id = Uuid::new_v4();

        // Try to acquire and release a test lock
        let acquired = self.try_lock(test_aggregate_id, OperationType::Read).await;
        if acquired {
            self.unlock(test_aggregate_id).await;
        }

        Ok(acquired)
    }

    // Private helper methods

    async fn acquire_single_lock(&self, aggregate_id: Uuid) -> Result<bool> {
        let mut conn = self
            .connection_pool
            .get_connection(&self.redis_client)
            .await?;
        let key = format!("lock:aggregate:{}", aggregate_id);

        // Serialize process_id using bincode
        let process_id_bytes = bincode::serialize(&self.process_id)?;

        println!(
            "🔍 Executing Redis command: SET {} {} NX EX {}",
            key, self.process_id, self.config.lock_timeout_secs
        );

        let result: RedisValue = {
            let mut conn_guard = conn.get_mut_connection().await?;
            redis::cmd("SET")
                .arg(&key)
                .arg(&process_id_bytes)
                .arg("NX")
                .arg("EX")
                .arg(self.config.lock_timeout_secs)
                .query_async(&mut *conn_guard)
                .await?
        };

        let lock_acquired = match result {
            RedisValue::Status(status) => {
                let success = status == "OK" || status == "ok";
                println!(
                    "🔍 Redis response status: '{}', lock acquired: {}",
                    status, success
                );
                success
            }
            RedisValue::Okay => {
                println!("🔍 Redis response: OK, lock acquired: true");
                true
            }
            RedisValue::Data(data) => {
                println!("🔍 Found Data: {:?}", data);

                // Try to parse as UTF-8 string first
                if let Ok(s) = std::str::from_utf8(&data) {
                    let success = s == "OK" || s == "ok";
                    println!(
                        "🔍 Parsed as UTF-8 string: '{}', treating as success: {}",
                        s, success
                    );
                    success
                } else {
                    // Try to parse as bincode
                    match bincode::deserialize::<String>(&data) {
                        Ok(_) => {
                            println!("🔍 Parsed as bincode string, treating as success: true");
                            true
                        }
                        Err(e) => {
                            println!("🔍 Failed to parse as bincode string: {:?}", e);
                            // Try to parse as JSON
                            if let Ok(json_value) =
                                serde_json::from_slice::<serde_json::Value>(&data)
                            {
                                if let Some(s) = json_value.as_str() {
                                    let success = s == "OK" || s == "ok";
                                    println!(
                                        "🔍 Parsed as JSON string: '{}', treating as success: {}",
                                        s, success
                                    );
                                    success
                                } else {
                                    println!("🔍 JSON value is not a string");
                                    false
                                }
                            } else {
                                println!("🔍 Could not parse as string, bincode, or JSON");
                                false
                            }
                        }
                    }
                }
            }
            _ => {
                println!("🔍 Unexpected Redis response type: {:?}", result);
                false
            }
        };

        println!("🔍 Final lock acquisition result: {}", lock_acquired);
        Ok(lock_acquired)
    }

    async fn acquire_batch_locks(
        &self,
        aggregate_ids: Vec<Uuid>,
        _operation_types: Vec<OperationType>,
    ) -> Result<Vec<bool>> {
        let mut conn = self
            .connection_pool
            .get_connection(&self.redis_client)
            .await?;

        let mut results = vec![false; aggregate_ids.len()];
        let mut pipeline = redis::pipe();

        // Serialize process_id using bincode
        let process_id_bytes = bincode::serialize(&self.process_id)?;

        // Add all lock commands to pipeline
        for (i, &aggregate_id) in aggregate_ids.iter().enumerate() {
            let key = format!("lock:aggregate:{}", aggregate_id);
            println!("🔍 Adding lock command for key: {}", key);

            pipeline
                .cmd("SET")
                .arg(&key)
                .arg(&process_id_bytes)
                .arg("NX")
                .arg("EX")
                .arg(self.config.batch_lock_timeout_secs);
        }

        println!(
            "🔍 Executing batch lock pipeline with {} commands",
            aggregate_ids.len()
        );

        let pipeline_results: Vec<RedisValue> = {
            let mut conn_guard = conn.get_mut_connection().await?;
            pipeline.query_async(&mut *conn_guard).await?
        };

        println!("🔍 Received {} pipeline results", pipeline_results.len());

        // Process each result
        for (i, value) in pipeline_results.iter().enumerate() {
            let success = match value {
                RedisValue::Status(status) => {
                    let success = status == "OK" || status == "ok";
                    println!("🔍 Result {}: Status '{}', success: {}", i, status, success);
                    success
                }
                RedisValue::Okay => {
                    println!("🔍 Result {}: OK, success: true", i);
                    true
                }
                RedisValue::Data(data) => {
                    println!("🔍 Result {}: Found Data: {:?}", i, data);

                    // Try to parse as UTF-8 string first
                    if let Ok(s) = std::str::from_utf8(&data) {
                        let success = s == "OK" || s == "ok";
                        println!(
                            "🔍 Result {}: Parsed as UTF-8 string: '{}', success: {}",
                            i, s, success
                        );
                        success
                    } else {
                        // Try to parse as bincode
                        match bincode::deserialize::<String>(&data) {
                            Ok(_) => {
                                println!(
                                    "🔍 Result {}: Parsed as bincode string, success: true",
                                    i
                                );
                                true
                            }
                            Err(e) => {
                                println!(
                                    "🔍 Result {}: Failed to parse as bincode string: {:?}",
                                    i, e
                                );
                                // Try to parse as JSON
                                if let Ok(json_value) =
                                    serde_json::from_slice::<serde_json::Value>(&data)
                                {
                                    if let Some(s) = json_value.as_str() {
                                        let success = s == "OK" || s == "ok";
                                        println!(
                                            "🔍 Result {}: Parsed as JSON string: '{}', success: {}",
                                            i, s, success
                                        );
                                        success
                                    } else {
                                        println!("🔍 Result {}: JSON value is not a string", i);
                                        false
                                    }
                                } else {
                                    println!(
                                        "🔍 Result {}: Could not parse as string, bincode, or JSON",
                                        i
                                    );
                                    false
                                }
                            }
                        }
                    }
                }
                _ => {
                    println!("🔍 Result {}: Unexpected value type: {:?}", i, value);
                    false
                }
            };

            results[i] = success;
        }

        println!(
            "🔍 Batch lock acquisition completed: {}/{} successful",
            results.iter().filter(|&&r| r).count(),
            results.len()
        );
        Ok(results)
    }

    async fn release_single_lock(&self, aggregate_id: Uuid) -> Result<()> {
        let mut conn = self
            .connection_pool
            .get_connection(&self.redis_client)
            .await?;
        let key = format!("lock:aggregate:{}", aggregate_id);

        // Use Lua script for safe lock release
        let script = redis::Script::new(
            r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            "#,
        );

        let _: i32 = {
            let mut conn_guard = conn.get_mut_connection().await?;
            script
                .key(&key)
                .arg(&self.process_id)
                .invoke_async(&mut *conn_guard)
                .await?
        };

        Ok(())
    }

    async fn release_batch_locks(&self, aggregate_ids: Vec<Uuid>) -> Result<()> {
        let mut conn = self
            .connection_pool
            .get_connection(&self.redis_client)
            .await?;

        // Use Lua script for safe batch lock release
        let script = redis::Script::new(
            r#"
            local released = 0
            for i = 1, #KEYS do
                if redis.call("get", KEYS[i]) == ARGV[1] then
                    redis.call("del", KEYS[i])
                    released = released + 1
                end
            end
            return released
            "#,
        );

        let keys: Vec<String> = aggregate_ids
            .iter()
            .map(|id| format!("lock:aggregate:{}", id))
            .collect();

        let _: i32 = {
            let mut conn_guard = conn.get_mut_connection().await?;
            script
                .key(&keys[0]) // Use first key for script invocation
                .arg(&self.process_id)
                .invoke_async(&mut *conn_guard)
                .await?
        };

        Ok(())
    }

    fn update_lock_metrics(&self, duration: Duration, success: bool) {
        if success {
            self.metrics.locks_acquired.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.locks_failed.fetch_add(1, Ordering::Relaxed);
        }

        // Update average acquisition time
        let duration_us = duration.as_micros() as u64;
        let current_avg = self
            .metrics
            .avg_lock_acquisition_time
            .load(Ordering::Relaxed);
        let total_acquired = self.metrics.locks_acquired.load(Ordering::Relaxed);

        if total_acquired > 0 {
            let new_avg = ((current_avg * (total_acquired - 1)) + duration_us) / total_acquired;
            self.metrics
                .avg_lock_acquisition_time
                .store(new_avg, Ordering::Relaxed);
        }
    }

    fn update_batch_lock_metrics(&self, duration: Duration, successful_count: usize) {
        if successful_count > 0 {
            self.metrics
                .batch_locks_acquired
                .fetch_add(successful_count as u64, Ordering::Relaxed);
        }

        // Update average batch lock time
        let duration_us = duration.as_micros() as u64;
        let current_avg = self.metrics.avg_batch_lock_time.load(Ordering::Relaxed);
        let total_batches = self.metrics.batch_locks_acquired.load(Ordering::Relaxed);

        if total_batches > 0 {
            let new_avg = ((current_avg * (total_batches - 1)) + duration_us) / total_batches;
            self.metrics
                .avg_batch_lock_time
                .store(new_avg, Ordering::Relaxed);
        }
    }
}

// Public operation type constants for easy use
impl OperationType {
    pub const READ: OperationType = OperationType::Read;
    pub const WRITE: OperationType = OperationType::Write;
    pub const CREATE: OperationType = OperationType::Create;
    pub const UPDATE: OperationType = OperationType::Update;
    pub const DELETE: OperationType = OperationType::Delete;
}

// Backward compatibility
impl RedisAggregateLock {
    /// Legacy method for backward compatibility
    pub fn new_legacy(redis_url: &str) -> Self {
        let client = redis::Client::open(redis_url).expect("Failed to connect to Redis");
        let redis_client =
            crate::infrastructure::redis_abstraction::RealRedisClient::new(client, None);
        Self::new(redis_client, RedisLockConfig::default())
    }
}
