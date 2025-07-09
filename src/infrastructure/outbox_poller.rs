use anyhow::Result;
use bincode;
use chrono;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::domain::AccountEvent;
use crate::infrastructure::kafka_abstraction::{EventBatch, KafkaProducer, KafkaProducerTrait};
use crate::infrastructure::outbox::{OutboxRepositoryTrait, PersistedOutboxMessage};

#[derive(Clone, Debug)]
pub struct OutboxPollerConfig {
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub max_retries: i32,
}

impl Default for OutboxPollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            batch_size: 10,
            max_retries: 3,
        }
    }
}

pub struct OutboxPollingService {
    outbox_repo: Arc<dyn OutboxRepositoryTrait>,
    kafka_producer: Arc<dyn KafkaProducerTrait>,
    config: OutboxPollerConfig,
    shutdown_rx: mpsc::Receiver<()>,
    // To allow signaling shutdown from another part of the application
    // _shutdown_tx: mpsc::Sender<()>, // Keep the sender if needed to signal from this struct itself
}

impl OutboxPollingService {
    pub fn new(
        outbox_repo: Arc<dyn OutboxRepositoryTrait>,
        kafka_producer: Arc<dyn KafkaProducerTrait>,
        config: OutboxPollerConfig,
        shutdown_rx: mpsc::Receiver<()>,
        // _shutdown_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            outbox_repo,
            kafka_producer,
            config,
            shutdown_rx,
            // _shutdown_tx,
        }
    }

    pub async fn run(mut self) {
        info!(
            "OutboxPollingService started with config: {:?}",
            self.config
        );

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("OutboxPollingService received shutdown signal. Exiting.");
                    break;
                }
                _ = sleep(self.config.poll_interval) => {
                    // Continue to polling logic
                }
            }

            match self
                .outbox_repo
                .fetch_and_lock_pending_messages(self.config.batch_size)
                .await
            {
                Ok(messages) => {
                    if !messages.is_empty() {
                        info!(
                            "Fetched {} messages from outbox for processing.",
                            messages.len()
                        );
                    }
                    for message in messages {
                        self.process_message(message).await;
                    }
                }
                Err(e) => {
                    error!("Error fetching messages from outbox: {}", e);
                    // Sleep for a short duration before retrying to avoid tight loop on DB errors
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
        info!("OutboxPollingService stopped.");
    }

    async fn process_message(&self, message: PersistedOutboxMessage) {
        info!("Processing outbox message ID: {}", message.id);

        // Deserialize the individual event from the outbox
        let event: AccountEvent = match bincode::deserialize(&message.payload) {
            Ok(event) => event,
            Err(e) => {
                error!(
                    "Failed to deserialize event from outbox message {}: {}",
                    message.id, e
                );
                if let Err(repo_err) = self
                    .outbox_repo
                    .record_failed_attempt(message.id, self.config.max_retries, Some(e.to_string()))
                    .await
                {
                    error!(
                        "Failed to record failed attempt for message {}: {}",
                        message.id, repo_err
                    );
                }
                return;
            }
        };

        // Create an EventBatch with the single event
        let batch = EventBatch {
            account_id: message.aggregate_id,
            events: vec![event],
            version: 0, // We don't track version in outbox, so use 0
            timestamp: chrono::Utc::now(),
        };

        // Serialize the batch
        let batch_payload = match bincode::serialize(&batch) {
            Ok(payload) => payload,
            Err(e) => {
                error!(
                    "Failed to serialize EventBatch for outbox message {}: {}",
                    message.id, e
                );
                if let Err(repo_err) = self
                    .outbox_repo
                    .record_failed_attempt(message.id, self.config.max_retries, Some(e.to_string()))
                    .await
                {
                    error!(
                        "Failed to record failed attempt for message {}: {}",
                        message.id, repo_err
                    );
                }
                return;
            }
        };

        // Send the batch using the publish_binary_event method
        match self
            .kafka_producer
            .publish_binary_event(
                &message.topic,
                &batch_payload,
                &message.event_id.to_string(),
            )
            .await
        {
            Ok(_) => {
                info!(
                    "Successfully published EventBatch for outbox message {} (event_id: {}) to Kafka topic {}.",
                    message.id, message.event_id, message.topic
                );
                if let Err(e) = self.outbox_repo.mark_as_processed(message.id).await {
                    error!(
                        "Failed to mark message {} as processed after Kafka publish: {}",
                        message.id, e
                    );
                    // This is a critical situation: event published but not marked.
                    // May lead to duplicate processing if not handled (e.g. by idempotent consumers).
                }
            }
            Err(e) => {
                error!(
                    "Failed to publish EventBatch for outbox message {} (event_id: {}) to Kafka: {}. Recording failed attempt.",
                    message.id, message.event_id, e
                );
                let error_string = Some(e.to_string());
                if let Err(repo_err) = self
                    .outbox_repo
                    .record_failed_attempt(message.id, self.config.max_retries, error_string)
                    .await
                {
                    error!(
                        "Failed to record failed attempt for message {}: {}",
                        message.id, repo_err
                    );
                }
            }
        }
    }
}

// Example of how to create and run the service (would typically be in main.rs or similar)
/*
async fn main_example() {
    // ... initialize outbox_repo, kafka_producer, config ...
    let outbox_repo = Arc::new(your_outbox_repo_impl);
    let kafka_producer = Arc::new(your_kafka_producer_impl);
    let poller_config = OutboxPollerConfig::default();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let poller_service = OutboxPollingService::new(
        outbox_repo,
        kafka_producer,
        poller_config,
        shutdown_rx,
        // shutdown_tx.clone() // if needed by the service itself
    );

    tokio::spawn(poller_service.run());

    // To initiate shutdown:
    // let _ = shutdown_tx.send(()).await;
}
*/
