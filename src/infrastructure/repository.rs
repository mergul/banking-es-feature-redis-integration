use crate::domain::{Account, AccountError, AccountEvent};
use crate::infrastructure::cache_service::{CacheConfig, CacheService, EvictionPolicy};
use crate::infrastructure::event_store::{EventStore, EventStoreConfig};
use crate::infrastructure::kafka_abstraction::KafkaConfig;
use crate::infrastructure::kafka_event_processor::KafkaEventProcessor;
use crate::infrastructure::projections::ProjectionStore;
use crate::infrastructure::redis_abstraction::RealRedisClient;
use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("Account not found: {0}")]
    NotFound(Uuid),
    #[error("Version conflict: expected {expected}, found {actual}")]
    VersionConflict { expected: i64, actual: i64 },
    #[error("Infrastructure error: {0}")]
    InfrastructureError(#[from] anyhow::Error),
}

#[async_trait]
pub trait AccountRepositoryTrait: Send + Sync {
    async fn create_account(&self, owner_name: String, initial_balance: Decimal)
        -> Result<Account>;
    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>>;
    async fn deposit_money(&self, account_id: Uuid, amount: Decimal) -> Result<Account>;
    async fn withdraw_money(&self, account_id: Uuid, amount: Decimal) -> Result<Account>;
    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError>;
    async fn save_batched(
        &self,
        account_id: Uuid,
        expected_version: i64,
        events: Vec<AccountEvent>,
    ) -> Result<()>;
    async fn flush_all(&self) -> Result<()>;
    fn start_batch_flush_task(&self);
}

pub struct AccountRepository {
    event_store: EventStore,
    kafka_processor: Arc<KafkaEventProcessor>,
}

impl AccountRepository {
    pub fn new(
        event_store: EventStore,
        kafka_config: KafkaConfig,
        projections: ProjectionStore,
        redis_client: redis::Client,
    ) -> Result<Self> {
        let cache_config = CacheConfig {
            default_ttl: Duration::from_secs(3600), // 1 hour
            max_size: 10000,
            shard_count: 4,
            warmup_batch_size: 100,
            warmup_interval: Duration::from_secs(300), // 5 minutes
            eviction_policy: EvictionPolicy::LRU,
        };

        let redis_client = RealRedisClient::new(redis_client, None);
        let cache_service = CacheService::new(redis_client, cache_config);
        let kafka_processor = Arc::new(KafkaEventProcessor::new(
            kafka_config,
            event_store.clone(),
            projections.clone(),
            cache_service,
        )?);

        Ok(Self {
            event_store,
            kafka_processor,
        })
    }

    pub async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.kafka_processor.start_processing().await?;
        Ok(())
    }

    pub async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        let stored_events = self.event_store.get_events(id, None).await.map_err(|e| {
            error!("Failed to get events for account {}: {}", id, e);
            AccountError::InfrastructureError(format!("Event store error: {}", e))
        })?;
        if stored_events.is_empty() {
            return Ok(None);
        }
        let mut account = Account::default();
        account.id = id;
        for event in stored_events {
            let account_event: AccountEvent =
                serde_json::from_value(event.event_data).map_err(|e| {
                    AccountError::InfrastructureError(format!("Deserialization error: {}", e))
                })?;

            account.apply_event(&account_event);
        }
        Ok(Some(account))
    }
}

#[async_trait]
impl AccountRepositoryTrait for AccountRepository {
    async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Account> {
        // Implementation needed
        unimplemented!()
    }

    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>> {
        // Implementation needed
        unimplemented!()
    }

    async fn deposit_money(&self, account_id: Uuid, amount: Decimal) -> Result<Account> {
        // Implementation needed
        unimplemented!()
    }

    async fn withdraw_money(&self, account_id: Uuid, amount: Decimal) -> Result<Account> {
        // Implementation needed
        unimplemented!()
    }

    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.kafka_processor.start_processing().await?;
        Ok(())
    }

    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.save(account, events).await
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        self.get_by_id(id).await
    }

    async fn save_batched(
        &self,
        account_id: Uuid,
        expected_version: i64,
        events: Vec<AccountEvent>,
    ) -> Result<()> {
        self.kafka_processor.start_processing().await?;
        Ok(())
    }

    async fn flush_all(&self) -> Result<()> {
        // If you have a flush method in KafkaEventProcessor, call it here. Otherwise, this can be a no-op.
        Ok(())
    }

    fn start_batch_flush_task(&self) {
        // If you want to periodically flush Kafka, implement it here. Otherwise, this can be a no-op.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Account;
    use crate::infrastructure::event_store::EventStore;
    use crate::infrastructure::kafka_abstraction::KafkaConfig;
    use crate::infrastructure::projections::ProjectionStore;
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_get_by_id_not_found() {
        // Create test database pools
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect("postgres://postgres:postgres@localhost:5432/banking_test")
            .await
            .expect("Failed to create test database pool");

        let event_store = EventStore::new(pool.clone());
        let kafka_config = KafkaConfig::default();
        let projections = ProjectionStore::new(pool);
        let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let repo =
            AccountRepository::new(event_store, kafka_config, projections, redis_client).unwrap();
        let id = Uuid::new_v4();
        let result = repo.get_by_id(id).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}

// Fix the error by using tokio::runtime::Runtime to block on the async connect call
impl Default for AccountRepository {
    fn default() -> Self {
        let pool = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                sqlx::PgPool::connect("postgres://postgres:postgres@localhost:5432/banking_test")
                    .await
            })
            .expect("Failed to connect to database");
        let event_store = EventStore::new(pool);
        let kafka_config = KafkaConfig::default();
        let projection_store = ProjectionStore::default();
        let redis_client =
            redis::Client::open("redis://localhost:6379").expect("Failed to connect to Redis");
        AccountRepository::new(event_store, kafka_config, projection_store, redis_client)
            .expect("Failed to create AccountRepository")
    }
}
