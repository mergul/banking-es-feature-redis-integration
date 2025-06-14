use anyhow::Result;
use banking_es::domain::{AccountEvent, AccountCommand, Account}; // Assuming Account is needed for context
use banking_es::infrastructure::event_store::{EventStore, EventStoreConfig, EventStoreError, Event as PersistedEvent};
use banking_es::infrastructure::kafka_abstraction::{KafkaProducer, KafkaConsumer, KafkaConfig, EventBatch};
use banking_es::infrastructure::kafka_event_processor::KafkaEventProcessor;
use banking_es::infrastructure::projections::{ProjectionStore, ProjectionConfig, AccountProjection};
use banking_es::infrastructure::cache_service::{CacheService, CacheConfig};
use banking_es::infrastructure::redis_abstraction::RealRedisClient;

use rust_decimal::Decimal;
use sqlx::{PgPool, Executor, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;
use chrono::Utc;

const TEST_DATABASE_URL: &str = "postgresql://postgres:Francisco1@localhost:5432/banking_es_test";
const TEST_KAFKA_BROKERS: &str = "localhost:9092";
const KAFKA_TEST_TOPIC_PREFIX: &str = "test_banking_es_occ";
const KAFKA_TEST_GROUP_ID: &str = "test_banking_es_occ_group";

async fn setup_test_db() -> Result<PgPool> {
    // In a real project, you'd use something like sqlx-cli to create the test DB
    // and run migrations before starting tests.
    // For this environment, we assume the DB exists and migrations can be run if needed.
    // However, running migrations programmatically can be complex.
    // We'll connect and assume schema is managed externally for now.
    let pool = PgPool::connect(TEST_DATABASE_URL).await?;
    Ok(pool)
}

async fn clear_tables(pool: &PgPool) -> Result<()> {
    pool.execute("TRUNCATE TABLE events CASCADE;").await?;
    pool.execute("TRUNCATE TABLE snapshots CASCADE;").await?;
    pool.execute("TRUNCATE TABLE account_projections CASCADE;").await?;
    pool.execute("TRUNCATE TABLE transaction_projections CASCADE;").await?;
    // Add other tables if necessary
    Ok(())
}

struct TestContext {
    pool: PgPool,
    event_store: Arc<EventStore>,
    kafka_producer: KafkaProducer,
    // We might not directly use KafkaConsumer in tests, but KafkaEventProcessor will
    kafka_event_processor: Arc<KafkaEventProcessor>,
    projection_store: Arc<ProjectionStore>,
    // Add other services if needed by KafkaEventProcessor
}

async fn setup_test_environment() -> Result<TestContext> {
    let pool = setup_test_db().await?;
    clear_tables(&pool).await?;

    let mut event_store_config = EventStoreConfig::default();
    event_store_config.database_url = TEST_DATABASE_URL.to_string();
    let event_store = Arc::new(EventStore::new_with_config(event_store_config).await?);

    let mut kafka_config = KafkaConfig::default();
    kafka_config.bootstrap_servers = TEST_KAFKA_BROKERS.to_string();
    kafka_config.topic_prefix = KAFKA_TEST_TOPIC_PREFIX.to_string();
    kafka_config.group_id = KAFKA_TEST_GROUP_ID.to_string();
    kafka_config.enabled = true; // Ensure Kafka is enabled for tests

    let kafka_producer = KafkaProducer::new(kafka_config.clone())?;

    // For KafkaEventProcessor dependencies
    let redis_client = redis::Client::open("redis://localhost:6379")?;
    let redis_client_trait = RealRedisClient::new(redis_client, None);
    let cache_config = CacheConfig::default();
    let cache_service = CacheService::new(redis_client_trait, cache_config);

    let mut proj_config = ProjectionConfig::default();
    proj_config.database_url = TEST_DATABASE_URL.to_string(); // Ensure projection store uses test DB
    let projection_store = Arc::new(ProjectionStore::new_with_config(proj_config).await?);


    let kafka_event_processor = Arc::new(KafkaEventProcessor::new(
        kafka_config.clone(),
        event_store.clone(),
        projection_store.clone(),
        cache_service.clone(),
    )?);

    // Start the KafkaEventProcessor in a background task
    let processor_clone = kafka_event_processor.clone();
    tokio::spawn(async move {
        if let Err(e) = processor_clone.start_processing().await {
            eprintln!("KafkaEventProcessor failed during test: {:?}", e);
        }
    });
    // Give processor a moment to start up and subscribe
    sleep(Duration::from_secs(2)).await;


    Ok(TestContext {
        pool,
        event_store,
        kafka_producer,
        kafka_event_processor,
        projection_store,
    })
}

async fn send_events_to_kafka(
    producer: &KafkaProducer,
    aggregate_id: Uuid,
    events: Vec<AccountEvent>,
    expected_version: i64,
) -> Result<()> {
    let batch = EventBatch {
        account_id: aggregate_id,
        events,
        version: expected_version,
        timestamp: Utc::now(),
    };
    producer.send_event_batch(batch.account_id, batch.events, batch.version).await?;
    // Brief pause to allow Kafka to process and consumer to pick up
    // This is the flaky part - ideally, we'd have a more deterministic way
    sleep(Duration::from_millis(500)).await; // Adjust as needed, or find a better way
    Ok(())
}

async fn get_events_from_db(pool: &PgPool, aggregate_id: Uuid) -> Result<Vec<PersistedEvent>> {
    let rows = sqlx::query_as!(
        PersistedEvent,
        r#"
        SELECT id, aggregate_id, event_type, event_data, version, timestamp, metadata as "metadata: Json<EventMetadata>"
        FROM events
        WHERE aggregate_id = $1
        ORDER BY version ASC
        "#,
        aggregate_id
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[tokio::test]
async fn test_01_successful_sequential_updates() -> Result<()> {
    let ctx = setup_test_environment().await?;
    let aggregate_id = Uuid::new_v4();

    // Initial event
    let event1 = AccountEvent::AccountCreated {
        account_id: aggregate_id,
        owner_name: "Test User 1".to_string(),
        initial_balance: Decimal::ZERO,
    };
    send_events_to_kafka(&ctx.kafka_producer, aggregate_id, vec![event1.clone()], 0).await?;
    sleep(Duration::from_secs(1)).await; // Wait for processing

    let persisted_events1 = get_events_from_db(&ctx.pool, aggregate_id).await?;
    assert_eq!(persisted_events1.len(), 1);
    assert_eq!(persisted_events1[0].version, 1);
    assert_eq!(persisted_events1[0].aggregate_id, aggregate_id);

    // Second event
    let event2 = AccountEvent::MoneyDeposited {
        account_id: aggregate_id,
        amount: Decimal::new(100, 0),
        transaction_id: Uuid::new_v4(),
    };
    send_events_to_kafka(&ctx.kafka_producer, aggregate_id, vec![event2.clone()], 1).await?;
    sleep(Duration::from_secs(1)).await; // Wait for processing

    let persisted_events2 = get_events_from_db(&ctx.pool, aggregate_id).await?;
    assert_eq!(persisted_events2.len(), 2);
    assert_eq!(persisted_events2[1].version, 2);
    let deposited_event_data: AccountEvent = serde_json::from_value(persisted_events2[1].event_data.clone())?;
    if let AccountEvent::MoneyDeposited { amount, .. } = deposited_event_data {
        assert_eq!(amount, Decimal::new(100,0));
    } else {
        panic!("Incorrect event type persisted for event2");
    }

    Ok(())
}

#[tokio::test]
async fn test_02_occ_violation_stale_expected_version() -> Result<()> {
    let ctx = setup_test_environment().await?;
    let aggregate_id = Uuid::new_v4();

    // Command 1: Create account (version 0 -> 1)
    let event1 = AccountEvent::AccountCreated {
        account_id: aggregate_id,
        owner_name: "Test User OCC".to_string(),
        initial_balance: Decimal::new(50, 0),
    };
    send_events_to_kafka(&ctx.kafka_producer, aggregate_id, vec![event1.clone()], 0).await?;
    sleep(Duration::from_secs(1)).await; // Ensure it's processed

    let events_after_cmd1 = get_events_from_db(&ctx.pool, aggregate_id).await?;
    assert_eq!(events_after_cmd1.len(), 1, "Event from command 1 should be persisted");
    assert_eq!(events_after_cmd1[0].version, 1);

    // Command 2: Deposit, but with stale expected_version = 0 (should be 1)
    let event2_deposit_stale = AccountEvent::MoneyDeposited {
        account_id: aggregate_id,
        amount: Decimal::new(100, 0),
        transaction_id: Uuid::new_v4(),
    };
    // This send should lead to an OCC error during processing by KafkaEventProcessor -> EventStore
    send_events_to_kafka(&ctx.kafka_producer, aggregate_id, vec![event2_deposit_stale.clone()], 0).await?;
    sleep(Duration::from_secs(2)).await; // Wait for processing attempt & potential OCC failure

    // Assert: Command 2's events are NOT persisted
    let events_after_cmd2_attempt = get_events_from_db(&ctx.pool, aggregate_id).await?;
    assert_eq!(events_after_cmd2_attempt.len(), 1, "Events from command 2 (stale) should NOT be persisted");

    // Optional: Check EventStore metrics for OCC failure if accessible
    // let metrics = ctx.event_store.get_metrics();
    // assert!(metrics.occ_failures.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    // This direct metrics check might be tricky if the EventStore instance is not easily accessible
    // or if metrics are aggregated. For now, relying on DB state.

    Ok(())
}

#[tokio::test]
async fn test_03_near_concurrent_processing_violation() -> Result<()> {
    let ctx = setup_test_environment().await?;
    let aggregate_id = Uuid::new_v4();

    // Initial event (version 0 -> 1)
    let event_create = AccountEvent::AccountCreated {
        account_id: aggregate_id,
        owner_name: "Concurrent Test".to_string(),
        initial_balance: Decimal::ZERO,
    };
    send_events_to_kafka(&ctx.kafka_producer, aggregate_id, vec![event_create], 0).await?;
    sleep(Duration::from_secs(1)).await; // Ensure creation is processed

    let events_after_create = get_events_from_db(&ctx.pool, aggregate_id).await?;
    assert_eq!(events_after_create.len(), 1);
    let current_version = events_after_create.last().map_or(0, |e| e.version);
    assert_eq!(current_version, 1);

    // Simulate two commands processed "concurrently" based on the same state (version 1)
    let event_op1 = AccountEvent::MoneyDeposited {
        account_id: aggregate_id,
        amount: Decimal::new(10, 0),
        transaction_id: Uuid::new_v4(),
    };
    let event_op2 = AccountEvent::MoneyWithdrawn { // Different operation
        account_id: aggregate_id,
        amount: Decimal::new(5, 0),
        transaction_id: Uuid::new_v4(),
    };

    // Send both event batches to Kafka, both expecting to be based on version 1
    let producer1 = ctx.kafka_producer.clone();
    let producer2 = ctx.kafka_producer.clone();

    let task1 = tokio::spawn(async move {
        send_events_to_kafka(&producer1, aggregate_id, vec![event_op1], current_version).await
    });
    let task2 = tokio::spawn(async move {
        send_events_to_kafka(&producer2, aggregate_id, vec![event_op2], current_version).await
    });

    let _ = task1.await?;
    let _ = task2.await?;

    sleep(Duration::from_secs(3)).await; // Allow ample time for KafkaEventProcessor to attempt both

    let final_events = get_events_from_db(&ctx.pool, aggregate_id).await?;
    // Only one of the operations should succeed, plus the initial creation event
    assert_eq!(final_events.len(), 2, "Only one concurrent operation should succeed");
    assert_eq!(final_events[0].version, 1); // Create event
    assert_eq!(final_events[1].version, 2); // The one that succeeded

    // We can't easily assert which one succeeded without more info, but count is key.
    // One of them should have resulted in an OCC error.
    // Check metrics if possible, or logs in a real scenario.

    Ok(())
}

#[tokio::test]
async fn test_04_initial_event_persistence() -> Result<()> {
    let ctx = setup_test_environment().await?;
    let aggregate_id = Uuid::new_v4();

    let event_create = AccountEvent::AccountCreated {
        account_id: aggregate_id,
        owner_name: "Initial Event Test".to_string(),
        initial_balance: Decimal::new(1000, 0),
    };
    // expected_version for the very first event of an aggregate is 0
    send_events_to_kafka(&ctx.kafka_producer, aggregate_id, vec![event_create.clone()], 0).await?;
    sleep(Duration::from_secs(1)).await;

    let persisted_events = get_events_from_db(&ctx.pool, aggregate_id).await?;
    assert_eq!(persisted_events.len(), 1);
    assert_eq!(persisted_events[0].version, 1);
    assert_eq!(persisted_events[0].aggregate_id, aggregate_id);
    let created_event_data: AccountEvent = serde_json::from_value(persisted_events[0].event_data.clone())?;
     if let AccountEvent::AccountCreated { initial_balance, .. } = created_event_data {
        assert_eq!(initial_balance, Decimal::new(1000,0));
    } else {
        panic!("Incorrect event type persisted for initial event");
    }
    Ok(())
}

#[tokio::test]
async fn test_05_occ_violation_with_initial_event() -> Result<()> {
    let ctx = setup_test_environment().await?;
    let aggregate_id = Uuid::new_v4();

    let event_create1 = AccountEvent::AccountCreated {
        account_id: aggregate_id,
        owner_name: "Initial Conflict 1".to_string(),
        initial_balance: Decimal::new(100, 0),
    };
    let event_create2 = AccountEvent::AccountCreated { // Slightly different data for distinction if needed
        account_id: aggregate_id,
        owner_name: "Initial Conflict 2".to_string(),
        initial_balance: Decimal::new(200, 0),
    };

    // Send two "initial" event batches for the same aggregate_id, both with expected_version = 0
    let producer1 = ctx.kafka_producer.clone();
    let producer2 = ctx.kafka_producer.clone();

    let task1 = tokio::spawn(async move {
        send_events_to_kafka(&producer1, aggregate_id, vec![event_create1], 0).await
    });
    let task2 = tokio::spawn(async move {
        // Small delay to make it "near" concurrent rather than perfectly same time,
        // though Kafka partitioning and consumer polling order will ultimately decide.
        sleep(Duration::from_millis(50)).await;
        send_events_to_kafka(&producer2, aggregate_id, vec![event_create2], 0).await
    });

    let _ = task1.await?;
    let _ = task2.await?;

    sleep(Duration::from_secs(3)).await; // Allow time for processing attempts

    let final_events = get_events_from_db(&ctx.pool, aggregate_id).await?;
    assert_eq!(final_events.len(), 1, "Only one initial event creation should succeed");
    assert_eq!(final_events[0].version, 1);
    // The one that "won" depends on Kafka processing order.

    Ok(())
}

// TODO: Add a test that specifically checks if EventStoreError::OptimisticConcurrencyConflict
// is propagated or logged if a way to inspect that from KafkaEventProcessor's spawned task is devised.
// For now, tests rely on the DB state as the source of truth for what was accepted.

// Helper to deserialize event_data for assertions (if needed for specific event content)
// fn deserialize_event(persisted_event: &PersistedEvent) -> Result<AccountEvent> {
//     serde_json::from_value(persisted_event.event_data.clone())
//         .map_err(|e| anyhow::anyhow!("Failed to deserialize event data: {}", e))
// }
