use crate::domain::{Account, AccountEvent};
use crate::infrastructure::redis_abstraction::RedisClientTrait;
use anyhow::Result;
use async_trait::async_trait;
use bincode;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub default_ttl: Duration,
    pub max_size: usize,
    pub shard_count: usize,
    pub warmup_batch_size: usize,
    pub warmup_interval: Duration,
    pub eviction_policy: EvictionPolicy,
}

#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    LRU,
    LFU,
    TTL,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(3600),
            max_size: 10000,
            shard_count: 16,
            warmup_batch_size: 100,
            warmup_interval: Duration::from_secs(300),
            eviction_policy: EvictionPolicy::LRU,
        }
    }
}

#[derive(Debug)]
pub struct CacheMetrics {
    pub hits: std::sync::atomic::AtomicU64,
    pub misses: std::sync::atomic::AtomicU64,
    pub evictions: std::sync::atomic::AtomicU64,
    pub errors: std::sync::atomic::AtomicU64,
    pub warmups: std::sync::atomic::AtomicU64,
    pub shard_hits: std::sync::atomic::AtomicU64,
    pub shard_misses: std::sync::atomic::AtomicU64,
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self {
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            evictions: std::sync::atomic::AtomicU64::new(0),
            errors: std::sync::atomic::AtomicU64::new(0),
            warmups: std::sync::atomic::AtomicU64::new(0),
            shard_hits: std::sync::atomic::AtomicU64::new(0),
            shard_misses: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheEntry<T> {
    pub value: T,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub ttl: Duration,
}

impl<T> CacheEntry<T> {
    fn is_expired(&self) -> bool {
        self.last_accessed.elapsed() > self.ttl
    }
}

#[derive(Clone)]
pub struct CacheService {
    redis_client: Arc<dyn RedisClientTrait>,
    config: CacheConfig,
    metrics: Arc<CacheMetrics>,
    // L1 Cache: In-memory sharded caches
    account_shards: Arc<Vec<DashMap<Uuid, CacheEntry<Account>>>>,
    event_shards: Arc<Vec<DashMap<Uuid, CacheEntry<Vec<AccountEvent>>>>>,
}

#[async_trait]
pub trait CacheServiceTrait: Send + Sync {
    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>>;
    async fn set_account(&self, account: &Account, ttl: Option<Duration>) -> Result<()>;
    async fn delete_account(&self, account_id: Uuid) -> Result<()>;
    async fn get_account_events(&self, account_id: Uuid) -> Result<Option<Vec<AccountEvent>>>;
    async fn set_account_events(
        &self,
        account_id: Uuid,
        events: &[(i64, AccountEvent)],
        ttl: Option<Duration>,
    ) -> Result<()>;
    async fn delete_account_events(&self, account_id: Uuid) -> Result<()>;
    async fn invalidate_account(&self, account_id: Uuid) -> Result<()>;
    async fn warmup_cache(&self, account_ids: Vec<Uuid>) -> Result<()>;
    fn get_metrics(&self) -> &CacheMetrics;
}

impl CacheService {
    fn get_shard_index(&self, account_id: Uuid) -> usize {
        // Use account_id hash to determine shard
        let hash = account_id.as_u128();
        (hash % self.config.shard_count as u128) as usize
    }

    fn cleanup_expired_entries(&self) {
        let now = Instant::now();

        // Cleanup account shards
        for shard in self.account_shards.iter() {
            shard.retain(|_, entry| !entry.is_expired());
        }

        // Cleanup event shards
        for shard in self.event_shards.iter() {
            shard.retain(|_, entry| !entry.is_expired());
        }
    }

    // Helper: set binary value in Redis
    async fn redis_set_bin(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut conn = self.redis_client.get_connection().await?;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }
    // Helper: get binary value from Redis
    async fn redis_get_bin(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.redis_client.get_connection().await?;
        let result: Option<Vec<u8>> = redis::cmd("GET").arg(key).query_async(&mut conn).await?;
        Ok(result)
    }
}

#[async_trait]
impl CacheServiceTrait for CacheService {
    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>> {
        let shard_index = self.get_shard_index(account_id);
        let shard = &self.account_shards[shard_index];

        // Try L1 cache first
        if let Some(entry) = shard.get(&account_id) {
            if !entry.is_expired() {
                // Clone the value before updating the entry
                let value = entry.value.clone();

                // Update access time
                let mut entry = entry.clone();
                entry.last_accessed = Instant::now();
                entry.access_count += 1;
                shard.insert(account_id, entry);

                self.metrics
                    .shard_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!("[CacheService] L1 cache hit for account: {}", account_id);
                return Ok(Some(value));
            } else {
                // Remove expired entry
                shard.remove(&account_id);
            }
        }

        self.metrics
            .shard_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // L1 cache miss, try L2 cache (Redis)
        let key = format!("account:{}", account_id);
        match self.redis_get_bin(&key).await {
            Ok(Some(data_bytes)) => {
                let account: Account = bincode::deserialize(&data_bytes)?;

                // Store in L1 cache
                let entry = CacheEntry {
                    value: account.clone(),
                    created_at: Instant::now(),
                    last_accessed: Instant::now(),
                    access_count: 1,
                    ttl: self.config.default_ttl,
                };
                shard.insert(account_id, entry);

                self.metrics
                    .hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!("[CacheService] L2 cache hit for account: {}", account_id);
                Ok(Some(account))
            }
            Ok(None) => {
                self.metrics
                    .misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(None)
            }
            Err(_) => {
                self.metrics
                    .misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(None)
            }
        }
    }

    async fn set_account(&self, account: &Account, ttl: Option<Duration>) -> Result<()> {
        let account_id = account.id;
        let shard_index = self.get_shard_index(account_id);
        let shard = &self.account_shards[shard_index];

        // Update L1 cache
        let entry = CacheEntry {
            value: account.clone(),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 1,
            ttl: ttl.unwrap_or(self.config.default_ttl),
        };
        shard.insert(account_id, entry);

        // Update L2 cache (Redis)
        let key = format!("account:{}", account_id);
        let data = bincode::serialize(account)?;
        self.redis_set_bin(&key, &data).await?;

        info!(
            "[CacheService] Updated in-memory cache for account: {}",
            account_id
        );
        Ok(())
    }

    async fn delete_account(&self, account_id: Uuid) -> Result<()> {
        // Remove from L1 cache
        let shard_index = self.get_shard_index(account_id);
        let shard = &self.account_shards[shard_index];
        shard.remove(&account_id);

        // Remove from L2 cache
        let key = format!("account:{}", account_id);
        self.redis_client.del(&key).await?;
        Ok(())
    }

    async fn get_account_events(&self, account_id: Uuid) -> Result<Option<Vec<AccountEvent>>> {
        let shard_index = self.get_shard_index(account_id);
        let shard = &self.event_shards[shard_index];

        // Try L1 cache first
        if let Some(entry) = shard.get(&account_id) {
            if !entry.is_expired() {
                // Clone the value before updating the entry
                let value = entry.value.clone();

                // Update access time
                let mut entry = entry.clone();
                entry.last_accessed = Instant::now();
                entry.access_count += 1;
                shard.insert(account_id, entry);

                self.metrics
                    .shard_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(Some(value));
            } else {
                // Remove expired entry
                shard.remove(&account_id);
            }
        }

        self.metrics
            .shard_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // L1 cache miss, try L2 cache (Redis)
        let key = format!("events:{}", account_id);
        match self.redis_get_bin(&key).await {
            Ok(Some(data_bytes)) => {
                let events: Vec<AccountEvent> = bincode::deserialize(&data_bytes)?;

                // Store in L1 cache
                let entry = CacheEntry {
                    value: events.clone(),
                    created_at: Instant::now(),
                    last_accessed: Instant::now(),
                    access_count: 1,
                    ttl: self.config.default_ttl,
                };
                shard.insert(account_id, entry);

                self.metrics
                    .hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(Some(events))
            }
            Ok(None) => {
                self.metrics
                    .misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(None)
            }
            Err(_) => {
                self.metrics
                    .misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(None)
            }
        }
    }

    async fn set_account_events(
        &self,
        account_id: Uuid,
        events: &[(i64, AccountEvent)],
        ttl: Option<Duration>,
    ) -> Result<()> {
        let shard_index = self.get_shard_index(account_id);
        let shard = &self.event_shards[shard_index];

        // Convert (i64, AccountEvent) to Vec<AccountEvent> for storage
        let events_only: Vec<AccountEvent> =
            events.iter().map(|(_, event)| event.clone()).collect();

        // Update L1 cache
        let entry = CacheEntry {
            value: events_only.clone(),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 1,
            ttl: ttl.unwrap_or(self.config.default_ttl),
        };
        shard.insert(account_id, entry);

        // Update L2 cache (Redis)
        let key = format!("events:{}", account_id);
        let data = bincode::serialize(&events_only)?;
        self.redis_set_bin(&key, &data).await?;

        info!(
            "[CacheService] Updated in-memory event cache for account: {}",
            account_id
        );
        Ok(())
    }

    async fn delete_account_events(&self, account_id: Uuid) -> Result<()> {
        // Remove from L1 cache
        let shard_index = self.get_shard_index(account_id);
        let shard = &self.event_shards[shard_index];
        shard.remove(&account_id);

        // Remove from L2 cache
        let key = format!("events:{}", account_id);
        self.redis_client.del(&key).await?;
        Ok(())
    }

    async fn invalidate_account(&self, account_id: Uuid) -> Result<()> {
        // Invalidate L1 cache
        let account_shard_index = self.get_shard_index(account_id);
        let account_shard = &self.account_shards[account_shard_index];
        account_shard.remove(&account_id);

        let event_shard_index = self.get_shard_index(account_id);
        let event_shard = &self.event_shards[event_shard_index];
        event_shard.remove(&account_id);

        // Invalidate L2 cache
        let account_key = format!("account:{}", account_id);
        let events_key = format!("events:{}", account_id);
        self.redis_client.del(&account_key).await?;
        self.redis_client.del(&events_key).await?;
        Ok(())
    }

    async fn warmup_cache(&self, account_ids: Vec<Uuid>) -> Result<()> {
        // Simple warmup - just increment the warmup counter
        self.metrics
            .warmups
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        info!("Cache warmup requested for {} accounts", account_ids.len());
        Ok(())
    }

    fn get_metrics(&self) -> &CacheMetrics {
        &self.metrics
    }
}

impl CacheService {
    pub fn new(redis_client: Arc<dyn RedisClientTrait>, config: CacheConfig) -> Self {
        let metrics = Arc::new(CacheMetrics::default());

        // Initialize L1 cache shards
        let mut account_shards = Vec::with_capacity(config.shard_count);
        let mut event_shards = Vec::with_capacity(config.shard_count);

        for _ in 0..config.shard_count {
            account_shards.push(DashMap::new());
            event_shards.push(DashMap::new());
        }

        let account_shards = Arc::new(account_shards);
        let event_shards = Arc::new(event_shards);

        Self {
            redis_client,
            config,
            metrics,
            account_shards,
            event_shards,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::redis_abstraction::{
        RedisClientTrait, RedisConnectionCommands, RedisPipeline, RedisPoolConfig,
    };
    use redis::{aio::MultiplexedConnection, Client, RedisError, Value as RedisValue};

    struct RedisConnection {
        conn: redis::aio::Connection,
    }

    impl RedisConnection {
        fn new(conn: redis::aio::Connection) -> Self {
            Self { conn }
        }
    }

    #[async_trait]
    impl RedisConnectionCommands for RedisConnection {
        async fn execute_pipeline(
            &mut self,
            pipeline: &RedisPipeline,
        ) -> Result<Vec<RedisValue>, RedisError> {
            pipeline.execute_pipeline(self).await
        }
    }

    struct TestRedisClient {
        client: Client,
    }

    #[async_trait]
    impl RedisClientTrait for TestRedisClient {
        async fn get_connection(&self) -> Result<MultiplexedConnection, RedisError> {
            self.client.get_multiplexed_async_connection().await
        }

        fn clone_client(&self) -> Arc<dyn RedisClientTrait> {
            Arc::new(TestRedisClient {
                client: self.client.clone(),
            })
        }

        async fn get_pooled_connection(
            &self,
        ) -> Result<Box<dyn RedisConnectionCommands + Send>, RedisError> {
            let conn = self.client.get_async_connection().await?;
            Ok(Box::new(RedisConnection::new(conn)))
        }

        fn get_pool_config(&self) -> RedisPoolConfig {
            RedisPoolConfig::default()
        }

        async fn get(&self, key: &str) -> Result<Option<String>, RedisError> {
            let mut conn = self.client.get_async_connection().await?;
            redis::cmd("GET").arg(key).query_async(&mut conn).await
        }

        async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
            let mut conn = self.client.get_async_connection().await?;
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .query_async(&mut conn)
                .await
        }

        async fn del(&self, key: &str) -> Result<(), RedisError> {
            let mut conn = self.client.get_async_connection().await?;
            redis::cmd("DEL").arg(key).query_async(&mut conn).await
        }
    }

    #[tokio::test]
    async fn test_cache_service_initialization() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let redis_client = TestRedisClient { client };
        let cache_service = CacheService::new(Arc::new(redis_client), CacheConfig::default());
        // CacheService is ready to use after construction, no initialization needed
        assert!(
            cache_service
                .get_metrics()
                .hits
                .load(std::sync::atomic::Ordering::Relaxed)
                == 0
        );
    }

    #[tokio::test]
    async fn test_get_account_cache_hit() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let redis_client = TestRedisClient { client };
        let account_id = Uuid::new_v4();
        let account = Account {
            id: account_id,
            owner_name: "Test User".to_string(),
            balance: 1000.into(),
            is_active: true,
            version: 1,
        };

        let cache_service = CacheService::new(Arc::new(redis_client), CacheConfig::default());
        cache_service.set_account(&account, None).await.unwrap();

        let result = cache_service.get_account(account_id).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap().id, account_id);
    }
}
