use crate::domain::Account;
use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::kafka_abstraction::{KafkaConfig, KafkaConsumer};
use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

pub struct L1CacheUpdater {
    consumer: KafkaConsumer,
    cache_service: Arc<dyn CacheServiceTrait>,
}

impl L1CacheUpdater {
    pub fn new(config: KafkaConfig, cache_service: Arc<dyn CacheServiceTrait>) -> Result<Self> {
        let consumer = KafkaConsumer::new(config)?;
        Ok(Self {
            consumer,
            cache_service,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.consumer.subscribe_to_cache().await?;

        loop {
            match self.consumer.poll_cache_updates().await {
                Ok(Some(account)) => {
                    info!("Received cache update for account: {}", account.id);

                    // Update L1 cache
                    if let Err(e) = self.cache_service.set_account(&account, None).await {
                        error!(
                            "Failed to update L1 cache for account {}: {}",
                            account.id, e
                        );
                    }

                    // Invalidate L1 event cache
                    if let Err(e) = self.cache_service.delete_account_events(account.id).await {
                        error!(
                            "Failed to invalidate L1 event cache for account {}: {}",
                            account.id, e
                        );
                    }
                }
                Ok(None) => continue,
                Err(e) => {
                    error!("Error polling Kafka: {}", e);
                }
            }
        }
    }
}
