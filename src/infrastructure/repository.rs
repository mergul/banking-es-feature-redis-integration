use crate::domain::{Account, AccountError, AccountEvent};
use crate::infrastructure::cache_service::{CacheConfig, CacheService, EvictionPolicy};
use crate::infrastructure::event_store::{
    EventPriority, EventStore, EventStoreExt, EventStoreTrait,
};
use crate::infrastructure::kafka_abstraction::{KafkaConfig, KafkaProducer};
use crate::infrastructure::kafka_event_processor::KafkaEventProcessor;
use crate::infrastructure::projections::ProjectionStore;
use crate::infrastructure::redis_abstraction::RealRedisClient;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use rust_decimal::Decimal;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
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
    async fn create_account(&self, owner_name: String) -> Result<Account>;
    async fn get_account(&self, account_id: Uuid) -> Result<Account>;
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

#[derive(Debug, Clone)]
struct CacheEntry<T> {
    data: T,
    created_at: Instant,
    last_accessed: Instant,
    version: i64,
}

#[derive(Debug, Default)]
struct RepositoryMetrics {
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
    batch_flushes: std::sync::atomic::AtomicU64,
    events_processed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
pub struct AccountRepository {
    event_store: Arc<dyn EventStoreTrait + 'static>,
    kafka_producer: Option<KafkaProducer>,
    pending_events: Arc<DashMap<Uuid, Vec<AccountEvent>>>,
    account_cache: Arc<RwLock<HashMap<Uuid, CacheEntry<Account>>>>,
    flush_interval: Duration,
    max_batch_size: usize,
    metrics: Arc<RepositoryMetrics>,
}

impl AccountRepository {
    pub fn new(event_store: Arc<dyn EventStoreTrait + 'static>) -> Self {
        let repo = Self {
            event_store,
            kafka_producer: None,
            pending_events: Arc::new(DashMap::new()),
            account_cache: Arc::new(RwLock::new(HashMap::new())),
            flush_interval: Duration::from_millis(50),
            max_batch_size: 1000,
            metrics: Arc::new(RepositoryMetrics::default()),
        };

        repo.start_batch_flush_task();
        repo.start_metrics_reporter();

        repo
    }

    pub fn with_kafka_producer(
        event_store: Arc<dyn EventStoreTrait + 'static>,
        kafka_config: KafkaConfig,
    ) -> Result<Self> {
        let kafka_producer = KafkaProducer::new(kafka_config)?;

        let repo = Self {
            event_store,
            kafka_producer: Some(kafka_producer),
            pending_events: Arc::new(DashMap::new()),
            account_cache: Arc::new(RwLock::new(HashMap::new())),
            flush_interval: Duration::from_millis(50),
            max_batch_size: 1000,
            metrics: Arc::new(RepositoryMetrics::default()),
        };

        repo.start_batch_flush_task();
        repo.start_metrics_reporter();

        Ok(repo)
    }

    fn start_metrics_reporter(&self) {
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let hits = metrics
                    .cache_hits
                    .load(std::sync::atomic::Ordering::Relaxed);
                let misses = metrics
                    .cache_misses
                    .load(std::sync::atomic::Ordering::Relaxed);
                let flushes = metrics
                    .batch_flushes
                    .load(std::sync::atomic::Ordering::Relaxed);
                let processed = metrics
                    .events_processed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let errors = metrics.errors.load(std::sync::atomic::Ordering::Relaxed);

                let hit_rate = if hits + misses > 0 {
                    (hits as f64 / (hits + misses) as f64) * 100.0
                } else {
                    0.0
                };

                info!(
                    "Repository Metrics - Cache Hit Rate: {:.1}%, Batch Flushes: {}, Events Processed: {}, Errors: {}",
                    hit_rate, flushes, processed, errors
                );
            }
        });
    }
}

#[async_trait]
impl AccountRepositoryTrait for AccountRepository {
    async fn create_account(&self, owner_name: String) -> Result<Account> {
        let account_id = Uuid::new_v4();
        let initial_balance = Decimal::ZERO;
        let account = Account::new(account_id, owner_name.clone(), initial_balance)?;
        let event = AccountEvent::AccountCreated {
            account_id,
            owner_name,
            initial_balance,
        };
        self.save_immediate(&account, vec![event]).await?;
        Ok(account)
    }

    async fn get_account(&self, account_id: Uuid) -> Result<Account> {
        self.get_by_id(account_id)
            .await
            .map_err(|e| anyhow::anyhow!(e))?
            .ok_or_else(|| anyhow::anyhow!(AccountError::NotFound))
    }

    async fn deposit_money(&self, account_id: Uuid, amount: Decimal) -> Result<Account> {
        let mut account = self
            .get_by_id(account_id)
            .await
            .map_err(|e| anyhow::anyhow!(e))?
            .ok_or_else(|| anyhow::anyhow!(AccountError::NotFound))?;

        let event = AccountEvent::MoneyDeposited {
            account_id,
            amount,
            transaction_id: Uuid::new_v4(),
        };

        self.save_immediate(&account, vec![event.clone()]).await?;

        account.apply_event(&event);

        Ok(account)
    }

    async fn withdraw_money(&self, account_id: Uuid, amount: Decimal) -> Result<Account> {
        let mut account = self
            .get_by_id(account_id)
            .await
            .map_err(|e| anyhow::anyhow!(e))?
            .ok_or_else(|| anyhow::anyhow!(AccountError::NotFound))?;

        let event = AccountEvent::MoneyWithdrawn {
            account_id,
            amount,
            transaction_id: Uuid::new_v4(),
        };

        self.save_immediate(&account, vec![event.clone()]).await?;

        account.apply_event(&event);

        Ok(account)
    }

    async fn save_immediate(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        // First save events to event store
        self.event_store
            .save_events(account.id, events.clone(), account.version)
            .await?;

        // Then publish events to Kafka if producer is available
        if let Some(producer) = &self.kafka_producer {
            if let Err(e) = producer
                .send_event_batch(account.id, events.clone(), account.version)
                .await
            {
                error!(
                    "Failed to publish events to Kafka for account {}: {}",
                    account.id, e
                );
                // Don't fail the operation if Kafka publishing fails, just log the error
            } else {
                info!(
                    "Successfully published {} events to Kafka for account {}",
                    events.len(),
                    account.id
                );
            }
        }

        Ok(())
    }

    async fn save(&self, account: &Account, events: Vec<AccountEvent>) -> Result<()> {
        self.pending_events
            .entry(account.id)
            .or_insert_with(Vec::new)
            .extend(events);
        Ok(())
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Account>, AccountError> {
        let stored_events = self.event_store.get_events(id, None).await.map_err(|e| {
            error!("Failed to get events for account {}: {}", id, e);
            AccountError::InfrastructureError(format!("Event store error: {}", e))
        })?;
        if stored_events.is_empty() {
            return Ok(None);
        }
        let mut account = Account {
            id,
            owner_name: String::new(),
            balance: Decimal::ZERO,
            is_active: true,
            version: 0,
        };
        for event in stored_events {
            let account_event: AccountEvent =
                bincode::deserialize(&event.event_data).map_err(|e| {
                    AccountError::InfrastructureError(format!("Deserialization error: {}", e))
                })?;

            account.apply_event(&account_event);
            account.version = event.version;
        }
        Ok(Some(account))
    }

    async fn save_batched(
        &self,
        account_id: Uuid,
        expected_version: i64,
        events: Vec<AccountEvent>,
    ) -> Result<()> {
        Ok(self
            .event_store
            .save_events(account_id, events, expected_version)
            .await?)
    }

    async fn flush_all(&self) -> Result<()> {
        let mut all_events = Vec::new();
        for entry in self.pending_events.iter() {
            all_events.push((entry.key().clone(), entry.value().clone()));
        }
        self.pending_events.clear();
        for (account_id, events) in all_events {
            if let Err(e) = self.event_store.save_events(account_id, events, 0).await {
                error!("Failed to flush events for account {}: {}", account_id, e);
            }
        }
        Ok(())
    }

    fn start_batch_flush_task(&self) {
        let pending_events = Arc::clone(&self.pending_events);
        let event_store = Arc::clone(&self.event_store);
        let flush_interval = self.flush_interval;
        let max_batch_size = self.max_batch_size;
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                interval.tick().await;
                let mut batch = Vec::new();
                for entry in pending_events.iter() {
                    for event in entry.value().iter() {
                        batch.push((*entry.key(), event.clone()));
                    }
                }
                pending_events.clear();
                if !batch.is_empty() {
                    for chunk in batch.chunks(max_batch_size) {
                        let mut grouped: HashMap<Uuid, Vec<AccountEvent>> = HashMap::new();
                        for (account_id, event) in chunk.iter() {
                            grouped.entry(*account_id).or_default().push(event.clone());
                        }
                        for (account_id, events) in grouped {
                            if let Err(e) = event_store.save_events(account_id, events, 0).await {
                                error!("Failed to flush events for account {}: {}", account_id, e);
                                metrics
                                    .errors
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                metrics
                                    .batch_flushes
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                metrics.events_processed.fetch_add(
                                    chunk.len() as u64,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Account;
    use crate::infrastructure::event_store::EventStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_get_by_id_not_found() {
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
        });

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("Failed to create test database pool");

        let event_store = Arc::new(EventStore::new(pool)) as Arc<dyn EventStoreTrait + 'static>;
        let repo = AccountRepository::new(event_store);
        let id = Uuid::new_v4();
        let result = repo.get_by_id(id).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}

impl Default for AccountRepository {
    fn default() -> Self {
        let event_store = Arc::new(EventStore::default()) as Arc<dyn EventStoreTrait + 'static>;
        AccountRepository::new(event_store)
    }
}
