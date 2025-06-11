use async_trait::async_trait;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    Message,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::domain::{Account, AccountEvent};
use crate::infrastructure::kafka_dlq::DeadLetterMessage;
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::ClientContext,
    error::{KafkaError, KafkaResult, RDKafkaErrorCode},
    message::{Header, Headers, OwnedMessage},
    util::Timeout,
    Offset, TopicPartitionList,
};
use serde_json;
use tokio::time::timeout;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topic_prefix: String,
    pub producer_acks: i16,
    pub producer_retries: i32,
    pub consumer_max_poll_interval_ms: i32,
    pub consumer_session_timeout_ms: i32,
    pub consumer_max_poll_records: i32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "banking-es-group".to_string(),
            topic_prefix: "banking-es".to_string(),
            producer_acks: 1,
            producer_retries: 3,
            consumer_max_poll_interval_ms: 300000,
            consumer_session_timeout_ms: 10000,
            consumer_max_poll_records: 500,
        }
    }
}

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
    config: KafkaConfig,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("acks", config.producer_acks.to_string())
            .set("retries", config.producer_retries.to_string())
            .create()?;

        Ok(Self { producer, config })
    }

    pub async fn send_event_batch(
        &self,
        account_id: Uuid,
        events: Vec<AccountEvent>,
        version: i64,
    ) -> Result<(), KafkaError> {
        let topic = format!("{}-events", self.config.topic_prefix);
        let key = account_id.to_string();
        let batch = EventBatch {
            account_id,
            events,
            version,
            timestamp: Utc::now(),
        };

        let payload = serde_json::to_vec(&batch)
            .map_err(|e| KafkaError::MessageProduction(RDKafkaErrorCode::InvalidMessage))?;

        self.producer
            .send(
                FutureRecord::to(&topic)
                    .key(&key)
                    .payload(&payload)
                    .partition(account_id.as_u128() as i32),
                Duration::from_secs(5),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e)
    }

    pub async fn send_cache_update(
        &self,
        account_id: Uuid,
        account: &Account,
    ) -> Result<(), KafkaError> {
        let topic = format!("{}-cache", self.config.topic_prefix);
        let key = account_id.to_string();

        let payload = serde_json::to_vec(account)
            .map_err(|e| KafkaError::MessageProduction(RDKafkaErrorCode::InvalidMessage))?;

        self.producer
            .send(
                FutureRecord::to(&topic)
                    .key(&key)
                    .payload(&payload)
                    .partition(account_id.as_u128() as i32),
                Duration::from_secs(5),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e)
    }

    pub async fn send_dlq_message(&self, message: &DeadLetterMessage) -> Result<(), KafkaError> {
        let topic = format!("{}-dlq", self.config.topic_prefix);
        let key = message.account_id.to_string();

        let payload = serde_json::to_vec(message)
            .map_err(|e| KafkaError::MessageProduction(RDKafkaErrorCode::InvalidMessage))?;

        self.producer
            .send(
                FutureRecord::to(&topic)
                    .key(&key)
                    .payload(&payload)
                    .partition(message.account_id.as_u128() as i32),
                Duration::from_secs(5),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e)
    }

    pub async fn poll_dlq_message(&self) -> Result<Option<DeadLetterMessage>, KafkaError> {
        let topic = format!("{}-dlq", self.config.topic_prefix);
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .create()?;

        consumer.subscribe(&[&topic])?;

        let mut stream = consumer.stream();
        match timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let payload = msg.payload().ok_or_else(|| {
                    KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidMessage)
                })?;

                let dlq_message: DeadLetterMessage =
                    serde_json::from_slice(payload).map_err(|e| {
                        KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidMessage)
                    })?;

                Ok(Some(dlq_message))
            }
            Ok(Some(Err(e))) => Err(e),
            Ok(None) => Ok(None),
            Err(_) => Ok(None), // Timeout
        }
    }
}

#[derive(Clone)]
pub struct KafkaConsumer {
    consumer: Arc<StreamConsumer>,
    config: KafkaConfig,
}

impl KafkaConsumer {
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set(
                "max.poll.interval.ms",
                config.consumer_max_poll_interval_ms.to_string(),
            )
            .set(
                "session.timeout.ms",
                config.consumer_session_timeout_ms.to_string(),
            )
            .set(
                "fetch.message.max.bytes",
                "1048576", // 1MB
            )
            .set("fetch.wait.max.ms", "100")
            .create()?;

        Ok(Self {
            consumer: Arc::new(consumer),
            config,
        })
    }

    pub async fn subscribe_to_events(&self) -> Result<(), KafkaError> {
        let topic = format!("{}-events", self.config.topic_prefix);
        self.consumer.subscribe(&[&topic])
    }

    pub async fn subscribe_to_cache(&self) -> Result<(), KafkaError> {
        let topic = format!("{}-cache", self.config.topic_prefix);
        self.consumer.subscribe(&[&topic])
    }

    pub async fn get_last_processed_version(&self, account_id: Uuid) -> Result<i64, KafkaError> {
        let topic = format!("{}-events", self.config.topic_prefix);
        let partition = account_id.as_u128() as i32;

        let last_offset = self.consumer.fetch_watermarks(
            &topic,
            partition,
            Timeout::After(Duration::from_secs(5)),
        )?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&topic, partition);

        let position = self.consumer.position()?;
        let partition = position
            .find_partition(&topic, partition)
            .ok_or_else(|| KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidMessage))?;

        match partition.offset() {
            Offset::Offset(offset) => Ok(offset),
            _ => Ok(0), // Default to 0 for invalid offsets
        }
    }

    pub async fn poll_events(&self) -> Result<Option<EventBatch>, KafkaError> {
        let mut stream = self.consumer.stream();
        match timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let payload = msg.payload().ok_or_else(|| {
                    KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidMessage)
                })?;

                let batch: EventBatch = serde_json::from_slice(payload).map_err(|e| {
                    KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidMessage)
                })?;

                Ok(Some(batch))
            }
            Ok(Some(Err(e))) => Err(e),
            Ok(None) => Ok(None),
            Err(_) => Ok(None), // Timeout
        }
    }

    pub async fn poll_cache_updates(&self) -> Result<Option<Account>, KafkaError> {
        let mut stream = self.consumer.stream();
        match timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let payload = msg.payload().ok_or_else(|| {
                    KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidMessage)
                })?;

                let account: Account = serde_json::from_slice(payload).map_err(|e| {
                    KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidMessage)
                })?;

                Ok(Some(account))
            }
            Ok(Some(Err(e))) => Err(e),
            Ok(None) => Ok(None),
            Err(_) => Ok(None), // Timeout
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventBatch {
    pub account_id: Uuid,
    pub events: Vec<AccountEvent>,
    pub version: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}
