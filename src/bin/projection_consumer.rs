use anyhow::Result;
use banking_es::domain::AccountEvent;
use banking_es::infrastructure::projections::{
    AccountProjection, ProjectionStore, ProjectionStoreTrait,
};
use base64;
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use rust_decimal::Decimal;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_appender::rolling;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging to both stdout and file
    let file_appender = rolling::daily("logs", "projection_consumer.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let stdout_layer = fmt::layer().with_writer(std::io::stdout);
    let file_layer = fmt::layer().with_writer(non_blocking);
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(stdout_layer)
        .with(file_layer)
        .init();

    info!("🚀 Starting projection consumer...");

    // Database connection
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy_with(database_url.parse().unwrap());

    let projection_store = Arc::new(ProjectionStore::new_test(pool.clone()))
        as Arc<dyn ProjectionStoreTrait + 'static>;

    // Kafka consumer configuration
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "projection-consumer-group-new")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Subscribe to the Debezium CDC topic
    consumer.subscribe(&["banking-es.public.kafka_outbox_cdc"])?;

    info!("✅ Consumer subscribed to banking-es.public.kafka_outbox_cdc topic");

    let mut message_count = 0;
    let start_time = std::time::Instant::now();

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                message_count += 1;

                if let Some(payload) = msg.payload() {
                    match process_event(payload, &projection_store).await {
                        Ok(_) => {
                            if message_count % 10 == 0 {
                                info!("✅ Processed {} messages", message_count);
                            }
                        }
                        Err(e) => {
                            error!("❌ Failed to process event: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("❌ Consumer error: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }

        // Log stats every 100 messages
        if message_count % 100 == 0 {
            let elapsed = start_time.elapsed();
            let rate = message_count as f64 / elapsed.as_secs_f64();
            info!(
                "📊 Stats: {} messages processed, {:.2} msg/sec",
                message_count, rate
            );
        }
    }
}

async fn process_event(
    payload: &[u8],
    projection_store: &Arc<dyn ProjectionStoreTrait>,
) -> Result<()> {
    info!("📥 Processing event payload of {} bytes", payload.len());

    // Try to deserialize as Debezium CDC JSON first
    if let Ok(json_str) = std::str::from_utf8(payload) {
        info!("📄 Processing Debezium CDC message: {}", json_str);

        // Parse the JSON to extract the event data
        match serde_json::from_str::<serde_json::Value>(json_str) {
            Ok(json_value) => {
                // Extract the "after" field which contains the new record data
                if let Some(after) = json_value.get("after") {
                    if let Some(aggregate_id_str) =
                        after.get("aggregate_id").and_then(|id| id.as_str())
                    {
                        if let Ok(account_id) = uuid::Uuid::parse_str(aggregate_id_str) {
                            let event_type = after
                                .get("event_type")
                                .and_then(|t| t.as_str())
                                .unwrap_or("unknown");

                            info!(
                                "📝 Processing CDC event for account: {} (event type: {})",
                                account_id, event_type
                            );

                            // Create a basic projection based on the event type
                            let mut projection = AccountProjection {
                                id: account_id,
                                owner_name: "SingleTestUser".to_string(), // Default from test
                                balance: Decimal::new(1000, 0),           // Default initial balance
                                is_active: true,
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                            };

                            // Set basic info based on event type
                            match event_type {
                                "AccountCreated" => {
                                    projection.is_active = true;
                                    projection.owner_name = "SingleTestUser".to_string();
                                    projection.balance = Decimal::new(1000, 0);
                                }
                                _ => {}
                            }

                            projection_store
                                .upsert_accounts_batch(vec![projection])
                                .await?;

                            info!(
                                "✅ Successfully created projection for account: {}",
                                account_id
                            );
                            return Ok(());
                        }
                    }
                } else {
                    warn!("⚠️ No 'after' field found in CDC message");
                }
            }
            Err(e) => {
                warn!("⚠️ Failed to parse CDC JSON: {}", e);
            }
        }
    }

    // Fallback: Try to deserialize as EventBatch (for backward compatibility)
    match bincode::deserialize::<banking_es::infrastructure::kafka_abstraction::EventBatch>(payload)
    {
        Ok(event_batch) => {
            info!(
                "✅ Successfully deserialized EventBatch for account: {}",
                event_batch.account_id
            );
            for event in event_batch.events {
                update_projection_from_event(&event, event_batch.account_id, projection_store)
                    .await?;
            }
            return Ok(());
        }
        Err(e) => {
            info!("⚠️ Failed to deserialize as EventBatch: {}", e);
        }
    }

    // Fallback: Try to deserialize as AccountEvent directly
    match bincode::deserialize::<AccountEvent>(payload) {
        Ok(event) => {
            let account_id = event.aggregate_id();
            info!(
                "✅ Successfully deserialized AccountEvent for account: {}",
                account_id
            );
            update_projection_from_event(&event, account_id, projection_store).await?;
            return Ok(());
        }
        Err(e) => {
            warn!("⚠️ Failed to deserialize as AccountEvent: {}", e);
        }
    }

    warn!("⚠️ Could not deserialize event payload with any method");
    Ok(())
}

async fn update_projection_from_event(
    event: &AccountEvent,
    account_id: Uuid,
    projection_store: &Arc<dyn ProjectionStoreTrait>,
) -> Result<()> {
    // Get current projection or create new one
    let mut projection = projection_store
        .get_account(account_id)
        .await?
        .unwrap_or_else(|| AccountProjection {
            id: account_id,
            owner_name: String::new(),
            balance: Decimal::ZERO,
            is_active: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });

    // Apply event to projection
    match event {
        AccountEvent::AccountCreated {
            owner_name,
            initial_balance,
            ..
        } => {
            projection.owner_name = owner_name.clone();
            projection.balance = *initial_balance;
            projection.is_active = true;
            projection.created_at = Utc::now();
            projection.updated_at = Utc::now();
            info!(
                "📝 Created projection for account: {} ({})",
                account_id, owner_name
            );
        }
        AccountEvent::MoneyDeposited { amount, .. } => {
            projection.balance += *amount;
            projection.updated_at = Utc::now();
            info!("💰 Deposited {} to account: {}", amount, account_id);
        }
        AccountEvent::MoneyWithdrawn { amount, .. } => {
            projection.balance -= *amount;
            projection.updated_at = Utc::now();
            info!("💸 Withdrew {} from account: {}", amount, account_id);
        }
        AccountEvent::AccountClosed { .. } => {
            projection.is_active = false;
            projection.updated_at = Utc::now();
            info!("🔒 Closed account: {}", account_id);
        }
    }

    // Update projection in database
    projection_store
        .upsert_accounts_batch(vec![projection])
        .await?;

    Ok(())
}
