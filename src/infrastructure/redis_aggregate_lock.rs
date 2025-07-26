use redis::AsyncCommands;
use uuid::Uuid;

pub struct RedisAggregateLock {
    client: redis::Client,
    process_id: String,
}

impl RedisAggregateLock {
    pub fn new(redis_url: &str) -> Self {
        let client = redis::Client::open(redis_url).expect("Failed to connect to Redis");
        let process_id = Uuid::new_v4().to_string();
        Self { client, process_id }
    }

    /// Try to acquire a lock for the aggregate. Returns true if acquired.
    pub async fn try_lock(&self, aggregate_id: Uuid, expiration_secs: usize) -> bool {
        let mut conn = self
            .client
            .get_async_connection()
            .await
            .expect("Redis conn fail");
        let key = format!("lock:aggregate:{}", aggregate_id);
        let value = &self.process_id;
        let result: Result<String, _> = redis::cmd("SET")
            .arg(&key)
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(expiration_secs)
            .query_async(&mut conn)
            .await;
        matches!(result, Ok(ref s) if s == "OK")
    }

    /// Release the lock for the aggregate.
    pub async fn unlock(&self, aggregate_id: Uuid) {
        let mut conn = self
            .client
            .get_async_connection()
            .await
            .expect("Redis conn fail");
        let key = format!("lock:aggregate:{}", aggregate_id);
        let _: () = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}
