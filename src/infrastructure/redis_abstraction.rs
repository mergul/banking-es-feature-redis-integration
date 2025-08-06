use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection, AsyncCommands, AsyncIter, Client as NativeRedisClient,
    ErrorKind as RedisErrorKind, ExistenceCheck, FromRedisValue, Pipeline, RedisError, SetExpiry,
    SetOptions, Value as RedisValue,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore};
// Required for the trait methods even if not used by RealRedisConnection directly for all methods now
// use mockall::automock; // Removed: no longer used
use redis::aio::ConnectionLike;
#[allow(unused_imports)]
use redis::ToRedisArgs;

/// Defines a trait for a Redis client that can provide connections.
/// This allows for mocking the client itself in unit tests.
#[async_trait]
pub trait RedisClientTrait: Send + Sync {
    /// Gets a new asynchronous Redis connection, boxed as a trait object.
    async fn get_connection(&self) -> Result<MultiplexedConnection, RedisError>;
    /// Clones the client, returning an `Arc` of the trait object.
    fn clone_client(&self) -> Arc<dyn RedisClientTrait>;

    /// Gets a connection from the pool
    async fn get_pooled_connection(
        &self,
    ) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError>;

    /// Gets the pool configuration
    fn get_pool_config(&self) -> RedisPoolConfig;

    async fn get(&self, key: &str) -> Result<Option<String>, RedisError>;
    async fn set(&self, key: &str, value: &str) -> Result<(), RedisError>;
    async fn del(&self, key: &str) -> Result<(), RedisError>;
}

/// Concrete implementation of `RedisClientTrait` using a `redis::Client` (aliased as `NativeRedisClient`).
pub struct RealRedisClient {
    client: NativeRedisClient,
    pool_config: RedisPoolConfig,
}

impl RealRedisClient {
    /// Creates a new `RealRedisClient` wrapped in an `Arc` suitable for trait object usage.
    pub fn new(
        client: NativeRedisClient,
        pool_config: Option<RedisPoolConfig>,
    ) -> Arc<dyn RedisClientTrait> {
        Arc::new(Self {
            client,
            pool_config: pool_config.unwrap_or_default(),
        })
    }
}

#[async_trait]
impl RedisClientTrait for RealRedisClient {
    /// Gets a Redis connection from the underlying `NativeRedisClient`.
    async fn get_connection(&self) -> Result<MultiplexedConnection, RedisError> {
        self.client.get_multiplexed_async_connection().await
    }
    /// Clones the `RealRedisClient` by cloning its internal `NativeRedisClient` and wrapping in a new `Arc`.
    fn clone_client(&self) -> Arc<dyn RedisClientTrait> {
        Arc::new(Self {
            client: self.client.clone(),
            pool_config: self.pool_config.clone(),
        })
    }

    async fn get_pooled_connection(
        &self,
    ) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError> {
        let conn = self.client.get_multiplexed_async_connection().await?;
        Ok(Box::new(RedisConnection::new(conn)))
    }

    fn get_pool_config(&self) -> RedisPoolConfig {
        self.pool_config.clone()
    }

    async fn get(&self, key: &str) -> Result<Option<String>, RedisError> {
        let mut conn = self.get_connection().await?;
        conn.get(key).await
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        let mut conn = self.get_connection().await?;
        conn.set(key, value).await
    }

    async fn del(&self, key: &str) -> Result<(), RedisError> {
        let mut conn = self.get_connection().await?;
        conn.del(key).await
    }
}

#[async_trait::async_trait]
pub trait RedisConnectionCommands: Send + Sync {
    async fn execute_pipeline(
        &mut self,
        pipeline: &RedisPipeline,
    ) -> Result<Vec<RedisValue>, RedisError>;
}

/// Concrete implementation of `RedisConnectionCommands` using a `redis::aio::MultiplexedConnection`.
pub struct RedisConnection {
    conn: Arc<Mutex<MultiplexedConnection>>,
}

impl RedisConnection {
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    pub async fn get_bytes(&self, key: &[u8]) -> Result<RedisValue, RedisError> {
        let mut conn = self.conn.lock().await;
        redis::AsyncCommands::get(&mut *conn, key).await
    }

    pub async fn set_ex_bytes(
        &self,
        key: &[u8],
        value: &[u8],
        seconds: u64,
    ) -> Result<(), RedisError> {
        let mut conn = self.conn.lock().await;
        redis::AsyncCommands::set_ex(&mut *conn, key, value, seconds).await
    }

    pub async fn del_bytes(&self, key: &[u8]) -> Result<(), RedisError> {
        let mut conn = self.conn.lock().await;
        redis::AsyncCommands::del(&mut *conn, key).await
    }

    pub async fn rpush_bytes(&self, key: &[u8], values: &[&[u8]]) -> Result<(), RedisError> {
        let mut conn = self.conn.lock().await;
        redis::AsyncCommands::rpush(&mut *conn, key, values).await
    }

    pub async fn lrange_bytes(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
    ) -> Result<Vec<RedisValue>, RedisError> {
        let mut conn = self.conn.lock().await;
        conn.lrange(key, start, stop).await
    }

    pub async fn scan_match_bytes(&self, pattern: &[u8]) -> Result<Vec<String>, RedisError> {
        let mut conn = self.conn.lock().await;
        let mut iter: AsyncIter<String> =
            redis::AsyncCommands::scan_match(&mut *conn, pattern).await?;
        let mut keys = Vec::new();
        while let Some(key) = iter.next_item().await {
            keys.push(key);
        }
        Ok(keys)
    }

    pub async fn set_options_bytes(
        &self,
        key: &[u8],
        value: &[u8],
        options: SetOptions,
    ) -> Result<Option<String>, RedisError> {
        let mut conn = self.conn.lock().await;
        conn.set_options(key, value, options).await
    }

    pub async fn set_nx_ex_bytes(
        &self,
        key: &[u8],
        value: &[u8],
        seconds: u64,
    ) -> Result<bool, RedisError> {
        let mut conn = self.conn.lock().await;
        let result: RedisValue = redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(seconds)
            .query_async(&mut *conn)
            .await?;

        // Check for both "OK" and "ok" status responses
        match result {
            RedisValue::Status(status) => Ok(status == "OK" || status == "ok"),
            RedisValue::Okay => Ok(true),
            _ => Ok(false),
        }
    }
}

#[async_trait::async_trait]
impl RedisConnectionCommands for RedisConnection {
    async fn execute_pipeline(
        &mut self,
        pipeline: &RedisPipeline,
    ) -> Result<Vec<RedisValue>, RedisError> {
        let mut conn = self.conn.lock().await;
        let mut pipeline = pipeline.pipeline.clone();
        pipeline.query_async(&mut *conn).await
    }
}

// Add connection pool configuration
#[derive(Debug, Clone)]
pub struct RedisPoolConfig {
    pub min_connections: u32,
    pub max_connections: u32,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for RedisPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: std::env::var("REDIS_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "50".to_string()) // 20'den 50'ye artırıldı
                .parse()
                .unwrap_or(50),
            max_connections: std::env::var("REDIS_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "500".to_string()) // 200'den 500'e artırıldı
                .parse()
                .unwrap_or(500),
            connection_timeout: Duration::from_secs(30), // 5'ten 30'a artırıldı
            idle_timeout: Duration::from_secs(600),      // 300'den 600'e artırıldı
        }
    }
}

// Add pipeline support
pub struct RedisPipeline {
    pipeline: Pipeline,
}

impl std::fmt::Debug for RedisPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPipeline")
            .field("pipeline", &"<Pipeline>")
            .finish()
    }
}

impl RedisPipeline {
    pub fn new() -> Self {
        Self {
            pipeline: redis::pipe(),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> &mut Self {
        self.pipeline.get(key);
        self
    }

    pub fn set_ex(&mut self, key: &[u8], value: &[u8], seconds: u64) -> &mut Self {
        self.pipeline.set_ex(key, value, seconds);
        self
    }

    pub fn del(&mut self, key: &[u8]) -> &mut Self {
        self.pipeline.del(key);
        self
    }

    pub fn rpush(&mut self, key: &[u8], values: &[&[u8]]) -> &mut Self {
        self.pipeline.rpush(key, values);
        self
    }

    pub async fn execute_pipeline(
        &self,
        conn: &mut dyn RedisConnectionCommands,
    ) -> Result<Vec<RedisValue>, RedisError> {
        conn.execute_pipeline(self).await
    }
}

impl Clone for RedisPipeline {
    fn clone(&self) -> Self {
        Self {
            pipeline: self.pipeline.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u64,
    pub reset_timeout: Duration,
    pub half_open_timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            half_open_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug)]
enum CircuitState {
    Closed,
    Open(Instant),
    HalfOpen,
}

pub struct CircuitBreaker {
    state: RwLock<CircuitState>,
    failure_count: AtomicU64,
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU64::new(0),
            config,
        }
    }

    async fn allow_request(&self) -> bool {
        let mut state = self.state.write().await;
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open(open_time) => {
                if open_time.elapsed() >= self.config.reset_timeout {
                    *state = CircuitState::HalfOpen;
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                // In HalfOpen state, we don't need to check elapsed time
                // Just allow one request to test if the service has recovered
                true
            }
        }
    }

    async fn record_success(&self) {
        let mut state = self.state.write().await;
        match *state {
            CircuitState::HalfOpen => {
                *state = CircuitState::Closed;
                self.failure_count.store(0, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    async fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= self.config.failure_threshold {
            let mut state = self.state.write().await;
            *state = CircuitState::Open(Instant::now());
        }
    }
}

pub struct CircuitBreakerRedisClient {
    inner: Arc<dyn RedisClientTrait>,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl CircuitBreakerRedisClient {
    pub fn new(client: Arc<dyn RedisClientTrait>, config: CircuitBreakerConfig) -> Self {
        Self {
            inner: client,
            circuit_breaker: Arc::new(CircuitBreaker::new(config)),
        }
    }
}

#[async_trait]
impl RedisClientTrait for CircuitBreakerRedisClient {
    async fn get_connection(&self) -> Result<MultiplexedConnection, RedisError> {
        if !self.circuit_breaker.allow_request().await {
            return Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Circuit breaker is open",
            )));
        }

        match self.inner.get_connection().await {
            Ok(conn) => {
                self.circuit_breaker.record_success().await;
                Ok(conn)
            }
            Err(e) => {
                self.circuit_breaker.record_failure().await;
                Err(e)
            }
        }
    }

    fn clone_client(&self) -> Arc<dyn RedisClientTrait> {
        Arc::new(Self {
            inner: self.inner.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
        })
    }

    async fn get_pooled_connection(
        &self,
    ) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError> {
        if !self.circuit_breaker.allow_request().await {
            return Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Circuit breaker is open",
            )));
        }

        match self.inner.get_pooled_connection().await {
            Ok(conn) => {
                self.circuit_breaker.record_success().await;
                Ok(conn)
            }
            Err(e) => {
                self.circuit_breaker.record_failure().await;
                Err(e)
            }
        }
    }

    fn get_pool_config(&self) -> RedisPoolConfig {
        self.inner.get_pool_config()
    }

    async fn get(&self, key: &str) -> Result<Option<String>, RedisError> {
        if !self.circuit_breaker.allow_request().await {
            return Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Circuit breaker is open",
            )));
        }

        match self.inner.get(key).await {
            Ok(value) => {
                self.circuit_breaker.record_success().await;
                Ok(value)
            }
            Err(e) => {
                self.circuit_breaker.record_failure().await;
                Err(e)
            }
        }
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        if !self.circuit_breaker.allow_request().await {
            return Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Circuit breaker is open",
            )));
        }

        match self.inner.set(key, value).await {
            Ok(_) => {
                self.circuit_breaker.record_success().await;
                Ok(())
            }
            Err(e) => {
                self.circuit_breaker.record_failure().await;
                Err(e)
            }
        }
    }

    async fn del(&self, key: &str) -> Result<(), RedisError> {
        if !self.circuit_breaker.allow_request().await {
            return Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Circuit breaker is open",
            )));
        }

        match self.inner.del(key).await {
            Ok(_) => {
                self.circuit_breaker.record_success().await;
                Ok(())
            }
            Err(e) => {
                self.circuit_breaker.record_failure().await;
                Err(e)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadShedderConfig {
    pub max_concurrent_requests: usize,
    pub max_queue_size: usize,
    pub queue_timeout: Duration,
    pub cpu_threshold: f64,    // CPU usage threshold (0.0 to 1.0)
    pub memory_threshold: f64, // Memory usage threshold (0.0 to 1.0)
}

impl Default for LoadShedderConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 2000,
            max_queue_size: 10000,
            queue_timeout: Duration::from_millis(50),
            cpu_threshold: 0.8,
            memory_threshold: 0.8,
        }
    }
}

pub struct LoadShedder {
    semaphore: Arc<Semaphore>,
    config: LoadShedderConfig,
    current_load: AtomicU64,
    rejected_requests: AtomicU64,
}

impl LoadShedder {
    pub fn new(config: LoadShedderConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            config,
            current_load: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
        }
    }

    pub async fn acquire_permit(&self) -> Result<LoadShedderPermit, RedisError> {
        // Check system metrics
        if self.is_system_overloaded() {
            self.rejected_requests.fetch_add(1, Ordering::SeqCst);
            return Err(RedisError::from((
                RedisErrorKind::IoError,
                "System is overloaded",
            )));
        }

        // Try to acquire a permit with timeout
        match tokio::time::timeout(self.config.queue_timeout, self.semaphore.acquire()).await {
            Ok(Ok(permit)) => {
                self.current_load.fetch_add(1, Ordering::SeqCst);
                Ok(LoadShedderPermit {
                    shedder: self.clone(),
                    _permit: permit,
                })
            }
            _ => {
                self.rejected_requests.fetch_add(1, Ordering::SeqCst);
                Err(RedisError::from((
                    RedisErrorKind::IoError,
                    "Request queue is full",
                )))
            }
        }
    }

    fn is_system_overloaded(&self) -> bool {
        // Get system metrics
        let cpu_usage = self.get_cpu_usage();
        let memory_usage = self.get_memory_usage();

        // Check if either metric exceeds threshold
        cpu_usage > self.config.cpu_threshold || memory_usage > self.config.memory_threshold
    }

    fn get_cpu_usage(&self) -> f64 {
        // TODO: Implement actual CPU usage monitoring
        // For now, return a placeholder value
        0.0
    }

    fn get_memory_usage(&self) -> f64 {
        // TODO: Implement actual memory usage monitoring
        // For now, return a placeholder value
        0.0
    }

    pub fn get_metrics(&self) -> LoadShedderMetrics {
        LoadShedderMetrics {
            current_load: self.current_load.load(Ordering::SeqCst),
            rejected_requests: self.rejected_requests.load(Ordering::SeqCst),
            max_concurrent_requests: self.config.max_concurrent_requests,
            max_queue_size: self.config.max_queue_size,
        }
    }
}

impl Clone for LoadShedder {
    fn clone(&self) -> Self {
        Self {
            semaphore: self.semaphore.clone(),
            config: self.config.clone(),
            current_load: AtomicU64::new(self.current_load.load(Ordering::SeqCst)),
            rejected_requests: AtomicU64::new(self.rejected_requests.load(Ordering::SeqCst)),
        }
    }
}

pub struct LoadShedderPermit<'a> {
    shedder: LoadShedder,
    _permit: tokio::sync::SemaphorePermit<'a>,
}

impl<'a> Drop for LoadShedderPermit<'a> {
    fn drop(&mut self) {
        self.shedder.current_load.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct LoadShedderMetrics {
    pub current_load: u64,
    pub rejected_requests: u64,
    pub max_concurrent_requests: usize,
    pub max_queue_size: usize,
}

pub struct LoadSheddingRedisClient {
    inner: Arc<dyn RedisClientTrait>,
    load_shedder: Arc<LoadShedder>,
}

impl LoadSheddingRedisClient {
    pub fn new(client: Arc<dyn RedisClientTrait>, config: LoadShedderConfig) -> Self {
        Self {
            inner: client,
            load_shedder: Arc::new(LoadShedder::new(config)),
        }
    }

    pub fn get_metrics(&self) -> LoadShedderMetrics {
        self.load_shedder.get_metrics()
    }
}

#[async_trait]
impl RedisClientTrait for LoadSheddingRedisClient {
    async fn get_connection(&self) -> Result<MultiplexedConnection, RedisError> {
        let _permit = self.load_shedder.acquire_permit().await?;
        self.inner.get_connection().await
    }

    fn clone_client(&self) -> Arc<dyn RedisClientTrait> {
        Arc::new(Self {
            inner: self.inner.clone(),
            load_shedder: self.load_shedder.clone(),
        })
    }

    async fn get_pooled_connection(
        &self,
    ) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError> {
        let _permit = self.load_shedder.acquire_permit().await?;
        self.inner.get_pooled_connection().await
    }

    fn get_pool_config(&self) -> RedisPoolConfig {
        self.inner.get_pool_config()
    }

    async fn get(&self, key: &str) -> Result<Option<String>, RedisError> {
        let _permit = self.load_shedder.acquire_permit().await?;
        self.inner.get(key).await
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        let _permit = self.load_shedder.acquire_permit().await?;
        self.inner.set(key, value).await
    }

    async fn del(&self, key: &str) -> Result<(), RedisError> {
        let _permit = self.load_shedder.acquire_permit().await?;
        self.inner.del(key).await
    }
}

pub struct RedisClient {
    conn: Arc<Mutex<MultiplexedConnection>>,
}

impl RedisClient {
    pub async fn new() -> Result<Self, RedisError> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, RedisError> {
        let mut conn = self.conn.lock().await;
        conn.get(key).await
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        let mut conn = self.conn.lock().await;
        conn.set(key, value).await
    }

    pub async fn set_ex(&self, key: &str, value: &str, seconds: u64) -> Result<(), RedisError> {
        let mut conn = self.conn.lock().await;
        conn.set_ex(key, value, seconds).await
    }

    pub async fn del(&self, key: &str) -> Result<(), RedisError> {
        let mut conn = self.conn.lock().await;
        conn.del(key).await
    }

    pub async fn exists(&self, key: &str) -> Result<bool, RedisError> {
        let mut conn = self.conn.lock().await;
        conn.exists(key).await
    }
}
