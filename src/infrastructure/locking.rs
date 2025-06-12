// use crate::infrastructure::redis_abstraction::RedisClientTrait;
// use std::sync::Arc;
// use std::time::Duration;
// use uuid::Uuid;

// pub struct LockManager {
//     redis_client: Arc<dyn RedisClientTrait>,
// }

// impl LockManager {
//     pub fn new(redis_client: Arc<dyn RedisClientTrait>) -> Self {
//         Self { redis_client }
//     }

//     pub async fn acquire_lock(&self, key: &str, ttl: Duration) -> Result<DistributedLock, String> {
//         let lock_id = Uuid::new_v4().to_string();
//         let lock_key = format!("lock:{}", key);

//         let acquired = self
//             .redis_client
//             .set_nx(&lock_key, &lock_id, ttl)
//             .await
//             .map_err(|e| format!("Failed to acquire lock: {}", e))?;

//         if acquired {
//             Ok(DistributedLock {
//                 redis_client: self.redis_client.clone(),
//                 lock_key,
//                 lock_id,
//             })
//         } else {
//             Err("Failed to acquire lock: lock already held".to_string())
//         }
//     }
// }

// pub struct DistributedLock {
//     redis_client: Arc<dyn RedisClientTrait>,
//     lock_key: String,
//     lock_id: String,
// }

// impl DistributedLock {
//     pub async fn release(&self) -> Result<(), String> {
//         let script = r#"
//             if redis.call("get", KEYS[1]) == ARGV[1] then
//                 return redis.call("del", KEYS[1])
//             else
//                 return 0
//             end
//         "#;

//         self.redis_client
//             .eval(script, &[&self.lock_key], &[&self.lock_id])
//             .await
//             .map_err(|e| format!("Failed to release lock: {}", e))?;

//         Ok(())
//     }
// }
