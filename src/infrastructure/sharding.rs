use crate::infrastructure::redis_abstraction::{RedisClient, RedisClientTrait};
use anyhow::{Context, Result};
use dashmap::DashMap;
use redis::{aio::Connection, AsyncCommands, RedisError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub id: String,
    pub range_start: u64,
    pub range_end: u64,
    pub instance_id: Option<String>,
    pub status: ShardStatus,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardStatus {
    Available,
    Assigned,
    Rebalancing,
    Failed,
}

#[derive(Debug, Clone)]
pub struct ShardManager {
    redis_client: Arc<dyn RedisClientTrait>,
    shards: Arc<DashMap<String, ShardInfo>>,
    lock_manager: Arc<LockManager>,
    config: ShardConfig,
}

#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub shard_count: usize,
    pub rebalance_threshold: f64,
    pub rebalance_interval: Duration,
    pub lock_timeout: Duration,
    pub lock_retry_interval: Duration,
    pub max_retries: u32,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shard_count: 16,
            rebalance_threshold: 0.2, // 20% imbalance triggers rebalancing
            rebalance_interval: Duration::from_secs(300), // 5 minutes
            lock_timeout: Duration::from_secs(30),
            lock_retry_interval: Duration::from_millis(100),
            max_retries: 3,
        }
    }
}

impl ShardManager {
    pub fn new(redis_client: Arc<dyn RedisClientTrait>, config: ShardConfig) -> Self {
        Self {
            redis_client,
            shards: Arc::new(DashMap::new()),
            lock_manager: Arc::new(LockManager::new(redis_client.clone())),
            config,
        }
    }

    pub async fn initialize_shards(&self) -> Result<()> {
        let range_size = u64::MAX / self.config.shard_count as u64;
        
        for i in 0..self.config.shard_count {
            let shard_id = format!("shard-{}", i);
            let range_start = i as u64 * range_size;
            let range_end = if i == self.config.shard_count - 1 {
                u64::MAX
            } else {
                (i + 1) as u64 * range_size - 1
            };

            let shard = ShardInfo {
                id: shard_id.clone(),
                range_start,
                range_end,
                instance_id: None,
                status: ShardStatus::Available,
                last_updated: chrono::Utc::now(),
            };

            self.shards.insert(shard_id, shard);
        }

        Ok(())
    }

    pub async fn assign_shard(&self, shard_id: &str, instance_id: &str) -> Result<()> {
        let lock_key = format!("shard:lock:{}", shard_id);
        let lock = self.lock_manager.acquire_lock(&lock_key, self.config.lock_timeout).await?;

        if let Some(mut shard) = self.shards.get_mut(shard_id) {
            shard.instance_id = Some(instance_id.to_string());
            shard.status = ShardStatus::Assigned;
            shard.last_updated = chrono::Utc::now();
        }

        self.lock_manager.release_lock(lock).await?;
        Ok(())
    }

    pub async fn get_shard_for_key(&self, key: &str) -> Result<Option<ShardInfo>> {
        let hash = self.hash_key(key);
        
        for shard in self.shards.iter() {
            if hash >= shard.range_start && hash <= shard.range_end {
                return Ok(Some(shard.clone()));
            }
        }

        Ok(None)
    }

    pub async fn rebalance_shards(&self) -> Result<()> {
        let lock_key = "shard:rebalance:lock";
        let lock = self.lock_manager.acquire_lock(lock_key, self.config.lock_timeout).await?;

        // Calculate current distribution
        let mut instance_shards: DashMap<String, Vec<ShardInfo>> = DashMap::new();
        for shard in self.shards.iter() {
            if let Some(instance_id) = &shard.instance_id {
                instance_shards
                    .entry(instance_id.clone())
                    .or_insert_with(Vec::new)
                    .push(shard.clone());
            }
        }

        // Check for imbalance
        let avg_shards = self.shards.len() as f64 / instance_shards.len() as f64;
        let mut needs_rebalancing = false;

        for shards in instance_shards.iter() {
            let deviation = (shards.len() as f64 - avg_shards).abs() / avg_shards;
            if deviation > self.config.rebalance_threshold {
                needs_rebalancing = true;
                break;
            }
        }

        if needs_rebalancing {
            info!("Starting shard rebalancing");
            self.perform_rebalancing(&instance_shards).await?;
        }

        self.lock_manager.release_lock(lock).await?;
        Ok(())
    }

    async fn perform_rebalancing(
        &self,
        instance_shards: &DashMap<String, Vec<ShardInfo>>,
    ) -> Result<()> {
        let avg_shards = self.shards.len() as f64 / instance_shards.len() as f64;
        let mut overloaded = Vec::new();
        let mut underloaded = Vec::new();

        // Identify overloaded and underloaded instances
        for entry in instance_shards.iter() {
            let instance_id = entry.key().clone();
            let shard_count = entry.value().len() as f64;
            
            if shard_count > avg_shards * (1.0 + self.config.rebalance_threshold) {
                overloaded.push((instance_id, shard_count));
            } else if shard_count < avg_shards * (1.0 - self.config.rebalance_threshold) {
                underloaded.push((instance_id, shard_count));
            }
        }

        // Rebalance shards
        for (overloaded_id, _) in overloaded {
            for (underloaded_id, _) in &underloaded {
                if let Some(shards) = instance_shards.get(&overloaded_id) {
                    if let Some(shard) = shards.value().first() {
                        self.assign_shard(&shard.id, underloaded_id).await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn hash_key(&self, key: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug)]
pub struct LockManager {
    redis_client: Arc<dyn RedisClientTrait>,
}

impl LockManager {
    pub fn new(redis_client: Arc<dyn RedisClientTrait>) -> Self {
        Self { redis_client }
    }

    pub async fn acquire_lock(&self, key: &str, timeout: Duration) -> Result<DistributedLock> {
        let lock_key = format!("lock:{}", key);
        let lock_id = Uuid::new_v4().to_string();
        let mut retries = 0;

        while retries < 3 {
            let mut conn = self.redis_client.get_async_connection().await?;
            
            let acquired: bool = redis::cmd("SET")
                .arg(&lock_key)
                .arg(&lock_id)
                .arg("NX")
                .arg("PX")
                .arg(timeout.as_millis() as u64)
                .query_async(&mut *conn)
                .await?;

            if acquired {
                return Ok(DistributedLock {
                    key: lock_key,
                    id: lock_id,
                    redis_client: self.redis_client.clone(),
                });
            }

            retries += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(anyhow::anyhow!("Failed to acquire lock after retries"))
    }
}

#[derive(Debug)]
pub struct DistributedLock {
    key: String,
    id: String,
    redis_client: Arc<dyn RedisClientTrait>,
}

impl DistributedLock {
    pub async fn release_lock(self) -> Result<()> {
        let mut conn = self.redis_client.get_async_connection().await?;
        
        let script = redis::Script::new(
            r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            "#,
        );

        let result: i32 = script
            .key(&self.key)
            .arg(&self.id)
            .invoke_async(&mut *conn)
            .await?;

        if result == 0 {
            warn!("Lock {} was already released or taken by another client", self.key);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Client;

    #[tokio::test]
    async fn test_shard_assignment() {
        let redis_client = Client::open("redis://127.0.0.1/").unwrap();
        let redis_client = Arc::new(RedisClient::new(redis_client, None).unwrap());
        
        let config = ShardConfig::default();
        let shard_manager = ShardManager::new(redis_client, config);
        
        shard_manager.initialize_shards().await.unwrap();
        
        let instance_id = "test-instance-1";
        shard_manager.assign_shard("shard-0", instance_id).await.unwrap();
        
        let shard = shard_manager.shards.get("shard-0").unwrap();
        assert_eq!(shard.instance_id, Some(instance_id.to_string()));
        assert_eq!(shard.status, ShardStatus::Assigned);
    }

    #[tokio::test]
    async fn test_distributed_lock() {
        let redis_client = Client::open("redis://127.0.0.1/").unwrap();
        let redis_client = Arc::new(RedisClient::new(redis_client, None).unwrap());
        
        let lock_manager = LockManager::new(redis_client.clone());
        
        let lock = lock_manager
            .acquire_lock("test-lock", Duration::from_secs(30))
            .await
            .unwrap();
            
        // Try to acquire the same lock
        let result = lock_manager
            .acquire_lock("test-lock", Duration::from_secs(30))
            .await;
            
        assert!(result.is_err());
        
        // Release the lock
        lock.release_lock().await.unwrap();
        
        // Should be able to acquire the lock again
        let lock = lock_manager
            .acquire_lock("test-lock", Duration::from_secs(30))
            .await
            .unwrap();
            
        lock.release_lock().await.unwrap();
    }
} 