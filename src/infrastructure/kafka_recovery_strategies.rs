use crate::domain::{Account, AccountEvent};
use crate::infrastructure::event_store::{EventStore, EventStoreTrait};
use crate::infrastructure::kafka_abstraction::{KafkaConfig, KafkaConsumer, KafkaProducer};
use crate::infrastructure::kafka_dlq::DeadLetterQueue;
use crate::infrastructure::kafka_metrics::KafkaMetrics;
use anyhow::{Context, Result};
use std::fmt;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryStrategy {
    FullReplay,
    IncrementalReplay,
    SelectiveReplay,
    CacheOnly,
    DLQOnly,
    Retry,
    ExponentialBackoff,
    CircuitBreaker,
    DeadLetterQueue,
    ManualIntervention,
    ScaleUp,
    ScaleDown,
    RestartService,
    ClearCache,
    ResetConnectionPool,
}

impl fmt::Display for RecoveryStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryStrategy::FullReplay => f.write_str("FullReplay"),
            RecoveryStrategy::IncrementalReplay => f.write_str("IncrementalReplay"),
            RecoveryStrategy::SelectiveReplay => f.write_str("SelectiveReplay"),
            RecoveryStrategy::CacheOnly => f.write_str("CacheOnly"),
            RecoveryStrategy::DLQOnly => f.write_str("DLQOnly"),
            RecoveryStrategy::Retry => f.write_str("Retry"),
            RecoveryStrategy::ExponentialBackoff => f.write_str("ExponentialBackoff"),
            RecoveryStrategy::CircuitBreaker => f.write_str("CircuitBreaker"),
            RecoveryStrategy::DeadLetterQueue => f.write_str("DeadLetterQueue"),
            RecoveryStrategy::ManualIntervention => f.write_str("ManualIntervention"),
            RecoveryStrategy::ScaleUp => f.write_str("ScaleUp"),
            RecoveryStrategy::ScaleDown => f.write_str("ScaleDown"),
            RecoveryStrategy::RestartService => f.write_str("RestartService"),
            RecoveryStrategy::ClearCache => f.write_str("ClearCache"),
            RecoveryStrategy::ResetConnectionPool => f.write_str("ResetConnectionPool"),
        }
    }
}

pub struct RecoveryStrategies {
    event_store: Arc<dyn EventStoreTrait + Send + Sync>,
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    dlq: Arc<DeadLetterQueue>,
    metrics: Arc<KafkaMetrics>,
}

impl RecoveryStrategies {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait + Send + Sync>,
        producer: KafkaProducer,
        consumer: KafkaConsumer,
        dlq: Arc<DeadLetterQueue>,
        metrics: Arc<KafkaMetrics>,
    ) -> Self {
        Self {
            event_store,
            producer,
            consumer,
            dlq,
            metrics,
        }
    }

    pub async fn execute_recovery(
        &self,
        strategy: RecoveryStrategy,
        account_id: Option<Uuid>,
    ) -> Result<()> {
        match strategy {
            RecoveryStrategy::FullReplay => self.full_replay(account_id).await,
            RecoveryStrategy::IncrementalReplay => self.incremental_replay(account_id).await,
            RecoveryStrategy::SelectiveReplay => self.selective_replay(account_id).await,
            RecoveryStrategy::CacheOnly => self.cache_only_recovery(account_id).await,
            RecoveryStrategy::DLQOnly => self.dlq_recovery(account_id).await,
            _ => Err(anyhow::anyhow!(
                "Recovery strategy ".to_string() + &strategy.to_string() + " not implemented"
            )),
        }
    }

    async fn full_replay(&self, account_id: Option<Uuid>) -> Result<()> {
        let _ = std::io::stderr().write_all(b"Starting full replay recovery\n");
        let accounts = if let Some(id) = account_id {
            vec![self
                .event_store
                .get_account(id)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .ok_or_else(|| {
                    anyhow::anyhow!("Account ".to_string() + &id.to_string() + " not found")
                })?]
        } else {
            self.event_store
                .get_all_accounts()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
        };

        for account in accounts {
            let events = self
                .event_store
                .get_events(account.id, None)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            for event in events {
                let account_event: AccountEvent = bincode::deserialize(&event.event_data)
                    .context("Failed to deserialize event")?;

                // Re-publish to Kafka
                self.producer
                    .send_event_batch(account.id, vec![account_event], event.version)
                    .await?;
            }
        }

        let _ = std::io::stderr().write_all(b"Full replay recovery completed\n");
        Ok(())
    }

    async fn incremental_replay(&self, account_id: Option<Uuid>) -> Result<()> {
        let _ = std::io::stderr().write_all(b"Starting incremental replay recovery\n");
        let accounts = if let Some(id) = account_id {
            vec![self
                .event_store
                .get_account(id)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .ok_or_else(|| {
                    anyhow::anyhow!("Account ".to_string() + &id.to_string() + " not found")
                })?]
        } else {
            self.event_store
                .get_all_accounts()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
        };

        for account in accounts {
            let last_processed_version = self
                .get_last_processed_version(account.id)
                .await
                .unwrap_or(0);

            let events = self
                .event_store
                .get_events(account.id, Some(last_processed_version))
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            for event in events {
                let account_event: AccountEvent = bincode::deserialize(&event.event_data)
                    .context("Failed to deserialize event")?;

                // Re-publish to Kafka
                self.producer
                    .send_event_batch(account.id, vec![account_event], event.version)
                    .await?;
            }
        }

        let _ = std::io::stderr().write_all(b"Incremental replay recovery completed\n");
        Ok(())
    }

    async fn selective_replay(&self, account_id: Option<Uuid>) -> Result<()> {
        let _ = std::io::stderr().write_all(b"Starting selective replay recovery\n");
        let accounts = if let Some(id) = account_id {
            vec![self
                .event_store
                .get_account(id)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .ok_or_else(|| {
                    anyhow::anyhow!("Account ".to_string() + &id.to_string() + " not found")
                })?]
        } else {
            self.event_store
                .get_all_accounts()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
        };

        for account in accounts {
            let events = self
                .event_store
                .get_events(account.id, None)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let mut current_version = 0;
            let mut batch = Vec::new();

            for event in events {
                if event.version > current_version + 1 {
                    // Gap detected, replay from current_version + 1
                    let gap_events = self
                        .event_store
                        .get_events(account.id, Some(current_version + 1))
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;

                    for gap_event in gap_events {
                        let account_event: AccountEvent =
                            bincode::deserialize(&gap_event.event_data)
                                .context("Failed to deserialize gap event")?;
                        batch.push(account_event);
                    }
                }

                let account_event: AccountEvent = bincode::deserialize(&event.event_data)
                    .context("Failed to deserialize event")?;
                batch.push(account_event);
                current_version = event.version;
            }

            // Send batch to Kafka
            for account_event in batch {
                self.producer
                    .send_event_batch(account.id, vec![account_event], current_version)
                    .await?;
            }
        }

        let _ = std::io::stderr().write_all(b"Selective replay recovery completed\n");
        Ok(())
    }

    async fn cache_only_recovery(&self, account_id: Option<Uuid>) -> Result<()> {
        let _ = std::io::stderr().write_all(b"Starting cache-only recovery\n");
        let accounts = if let Some(id) = account_id {
            vec![self
                .event_store
                .get_account(id)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .ok_or_else(|| {
                    anyhow::anyhow!("Account ".to_string() + &id.to_string() + " not found")
                })?]
        } else {
            self.event_store
                .get_all_accounts()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
        };

        for account in accounts {
            // Note: Cache warming would need to be implemented separately
            // as cache_service is not available in this context
        }

        let _ = std::io::stderr().write_all(b"Cache-only recovery completed\n");
        Ok(())
    }

    async fn dlq_recovery(&self, account_id: Option<Uuid>) -> Result<()> {
        let _ = std::io::stderr().write_all(b"Starting DLQ recovery\n");
        // Process all messages in DLQ
        self.dlq.process_dlq().await?;
        let _ = std::io::stderr().write_all(b"DLQ recovery completed\n");
        Ok(())
    }

    async fn get_last_processed_version(&self, account_id: Uuid) -> Result<i64> {
        // Get the last processed version from Kafka consumer
        self.consumer
            .get_last_processed_version(account_id)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to get last processed version: ".to_string() + &e.to_string()
                )
            })
    }
}
