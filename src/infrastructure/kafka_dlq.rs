use crate::domain::{Account, AccountEvent};
use crate::infrastructure::kafka_abstraction::{KafkaConfig, KafkaConsumer, KafkaProducer};
use crate::infrastructure::kafka_metrics::KafkaMetrics;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

// Custom module for bincode-compatible DateTime<Utc> serialization
mod bincode_datetime {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::de::Deserialize;
    use serde::{self, Deserializer, Serializer};

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(dt.timestamp())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts = i64::deserialize(deserializer)?;
        Ok(Utc.timestamp_opt(ts, 0).single().unwrap())
    }

    pub mod option {
        use super::*;
        pub fn serialize<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match dt {
                Some(dt) => serializer.serialize_some(&dt.timestamp()),
                None => serializer.serialize_none(),
            }
        }
        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let opt = Option::<i64>::deserialize(deserializer)?;
            Ok(opt.map(|ts| Utc.timestamp_opt(ts, 0).single().unwrap()))
        }
    }
}

#[async_trait]
pub trait DeadLetterQueueTrait: Send + Sync {
    async fn send_to_dlq(
        &self,
        account_id: Uuid,
        events: Vec<AccountEvent>,
        version: i64,
        failure_reason: String,
    ) -> Result<()>;
    async fn process_dlq(&self) -> Result<()>;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeadLetterMessage {
    pub account_id: Uuid,
    pub events: Vec<AccountEvent>,
    pub version: i64,
    #[serde(with = "bincode_datetime")]
    pub original_timestamp: DateTime<Utc>,
    pub failure_reason: String,
    pub retry_count: u32,
    #[serde(with = "bincode_datetime::option")]
    pub last_retry: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct DeadLetterQueue {
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    metrics: Arc<KafkaMetrics>,
    max_retries: u32,
    initial_retry_delay: Duration,
}

impl DeadLetterQueue {
    pub fn new(
        producer: KafkaProducer,
        consumer: KafkaConsumer,
        metrics: Arc<KafkaMetrics>,
        max_retries: u32,
        initial_retry_delay: Duration,
    ) -> Self {
        Self {
            producer,
            consumer,
            metrics,
            max_retries,
            initial_retry_delay,
        }
    }

    pub async fn send_to_dlq(
        &self,
        account_id: Uuid,
        events: Vec<AccountEvent>,
        version: i64,
        failure_reason: String,
    ) -> Result<()> {
        let dlq_message = DeadLetterMessage {
            account_id,
            events,
            version,
            original_timestamp: Utc::now(),
            failure_reason,
            retry_count: 0,
            last_retry: None,
        };

        self.producer.send_dlq_message(&dlq_message).await?;

        self.metrics
            .dlq_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub async fn process_dlq(&self) -> Result<()> {
        // Subscribe to DLQ topic
        self.consumer.subscribe_to_dlq().await?;

        loop {
            if let Some(message) = self.consumer.poll_dlq_message().await? {
                if message.retry_count >= self.max_retries {
                    let _ = std::io::stderr().write_all(
                        ("Message for account ".to_string()
                            + &message.account_id.to_string()
                            + " exceeded max retries ("
                            + &self.max_retries.to_string()
                            + "), giving up\n")
                            .as_bytes(),
                    );
                    continue;
                }

                self.metrics
                    .dlq_retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Exponential backoff
                let delay = self.initial_retry_delay * 2u32.pow(message.retry_count);
                sleep(delay).await;

                match self.retry_message(&message).await {
                    Ok(_) => {
                        self.metrics
                            .dlq_retry_success
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = std::io::stderr().write_all(
                            ("Successfully retried message for account ".to_string()
                                + &message.account_id.to_string()
                                + "\n")
                                .as_bytes(),
                        );
                    }
                    Err(e) => {
                        self.metrics
                            .dlq_retry_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = std::io::stderr().write_all(
                            ("Failed to retry message for account ".to_string()
                                + &message.account_id.to_string()
                                + ": "
                                + &e.to_string()
                                + "\n")
                                .as_bytes(),
                        );

                        // Update retry count and send back to DLQ
                        let mut updated_message = message;
                        updated_message.retry_count += 1;
                        updated_message.last_retry = Some(Utc::now());

                        self.producer.send_dlq_message(&updated_message).await?;
                    }
                }
            }
        }
    }

    async fn retry_message(&self, message: &DeadLetterMessage) -> Result<()> {
        // Attempt to reprocess the events
        self.producer
            .send_event_batch(message.account_id, message.events.clone(), message.version)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl DeadLetterQueueTrait for DeadLetterQueue {
    async fn send_to_dlq(
        &self,
        account_id: Uuid,
        events: Vec<AccountEvent>,
        version: i64,
        failure_reason: String,
    ) -> Result<()> {
        let message = DeadLetterMessage {
            account_id,
            events,
            version,
            original_timestamp: Utc::now(),
            failure_reason,
            retry_count: 0,
            last_retry: None,
        };

        self.producer.send_dlq_message(&message).await?;
        Ok(())
    }

    async fn process_dlq(&self) -> Result<()> {
        if let Some(message) = self.consumer.poll_dlq_message().await? {
            if message.retry_count >= self.max_retries {
                let _ = std::io::stderr().write_all(
                    ("Message for account ".to_string()
                        + &message.account_id.to_string()
                        + " exceeded max retries ("
                        + &self.max_retries.to_string()
                        + "), giving up\n")
                        .as_bytes(),
                );
                return Ok(());
            }

            // Calculate backoff delay
            let delay = self.initial_retry_delay * 2u32.pow(message.retry_count);
            sleep(delay).await;

            // Retry the message
            self.retry_message(&message).await?;
        }
        Ok(())
    }
}
