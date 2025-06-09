use ::banking_es::domain::{Account, AccountEvent};
use ::banking_es::infrastructure::redis_abstraction::RedisPoolConfig;
use ::banking_es::{
    AccountError, AccountRepository, AccountRepositoryTrait, AccountService, EventStore,
    EventStoreConfig, ProjectionStore, RealRedisClient, RedisClientTrait,
};
use dotenv;
use redis::Client as NativeRedisClient;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

// Helper structure to hold common test dependencies
struct TestContext {
    account_service: AccountService,
    // Expose repository if direct calls are needed for assertions/setup not covered by service methods
    account_repository: Arc<AccountRepository>,
    redis_conn_for_direct_checks: Option<NativeRedisClient>, // For direct Redis checks if needed
    db_pool: PgPool,
}

async fn setup_test_environment() -> Result<TestContext, Box<dyn std::error::Error>> {
    dotenv::dotenv().ok(); // Load .env file if present

    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set for integration tests");

    // Setup Database
    let db_pool = PgPool::connect(&database_url).await?;
    // Optionally run migrations if not handled by an external script
    // sqlx::migrate!("./migrations").run(&db_pool).await?;

    // Setup EventStore and ProjectionStore
    let event_store = EventStore::new_with_config(EventStoreConfig::default()).await?;
    let projection_store = ProjectionStore::new(db_pool.clone());

    // Setup Redis Client with test-appropriate pool config
    let native_redis_client = NativeRedisClient::open(redis_url)?;
    let redis_pool_config = RedisPoolConfig {
        min_connections: 1,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
    };
    let redis_client_trait: Arc<dyn RedisClientTrait> =
        RealRedisClient::new(native_redis_client.clone(), Some(redis_pool_config));

    // Setup Repository and Service
    let account_repository = Arc::new(AccountRepository::new(
        event_store.clone(), // EventStore might need to be cloneable
        redis_client_trait.clone(),
    ));
    let account_service = AccountService::new(
        account_repository.clone(),
        projection_store,
        redis_client_trait.clone(),
    );

    Ok(TestContext {
        account_service,
        account_repository,
        redis_conn_for_direct_checks: Some(native_redis_client),
        db_pool,
    })
}

// Helper to flush Redis DB (use with caution, only for test environments)
async fn flush_redis_db(client: &NativeRedisClient) -> Result<(), redis::RedisError> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    redis::cmd("FLUSHDB").query_async(&mut conn).await?;
    Ok(())
}

const TEST_DLQ_KEY: &str = "event_batch_dlq"; // Mirroring from repository.rs

// Helper to get current account version from event store DB
async fn get_account_current_version(account_id: Uuid, pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT MAX(sequence_number) FROM events WHERE aggregate_id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map_or(0, |(max_seq,)| max_seq)) // Version is max sequence number, or 0 if no events
}

// Helper to get all items from DLQ
async fn get_dlq_items(
    redis_client: &Arc<dyn RedisClientTrait>,
) -> Result<Vec<String>, redis::RedisError> {
    let mut conn = redis_client.get_async_connection().await?;
    let values = conn.lrange_bytes(TEST_DLQ_KEY.as_bytes(), 0, -1).await?;
    Ok(values
        .into_iter()
        .filter_map(|v| match v {
            redis::Value::Data(data) => String::from_utf8(data).ok(),
            _ => None,
        })
        .collect())
}

// Helper to clear DLQ
async fn clear_dlq(redis_client: &Arc<dyn RedisClientTrait>) -> Result<(), redis::RedisError> {
    let mut conn = redis_client.get_async_connection().await?;
    conn.del_bytes(TEST_DLQ_KEY.as_bytes()).await
}

// Helper to get all items from a specific batching list
async fn get_batch_list_items(
    redis_client: &Arc<dyn RedisClientTrait>,
    account_id: Uuid,
) -> Result<Vec<String>, redis::RedisError> {
    let mut conn = redis_client.get_async_connection().await?;
    let key = format!("events_for_batching:{}", account_id);
    let values = conn.lrange_bytes(key.as_bytes(), 0, -1).await?;
    Ok(values
        .into_iter()
        .filter_map(|v| match v {
            redis::Value::Data(data) => String::from_utf8(data).ok(),
            _ => None,
        })
        .collect())
}

#[tokio::test]
async fn test_account_data_caching_and_invalidation() -> Result<(), Box<dyn std::error::Error>> {
    let context = setup_test_environment().await?;
    if let Some(ref client) = context.redis_conn_for_direct_checks {
        flush_redis_db(client)
            .await
            .expect("Failed to flush Redis DB for test");
    }

    let owner_name = format!("TestUser_{}", Uuid::new_v4());
    let initial_balance = Decimal::new(100, 2);

    // 1. Create an account using AccountService
    let account_id = context
        .account_service
        .create_account(owner_name.clone(), initial_balance)
        .await?;

    // 2. Fetch the account using AccountRepository::get_by_id (first fetch)
    // This should fetch from DB and populate cache.
    let account_v1_opt = context.account_repository.get_by_id(account_id).await?;
    assert!(account_v1_opt.is_some(), "Account not found after creation");
    let account_v1 = account_v1_opt.unwrap();
    assert_eq!(account_v1.owner_name, owner_name);
    assert_eq!(account_v1.balance, initial_balance);

    // At this point, the account should be in Redis cache.
    // We can verify this if we had direct Redis access and knew the key.
    // For now, we assume unit tests for repository.get_by_id cover this.

    // 3. Fetch the same account again (should hit cache - though hard to assert directly here)
    let account_v2_opt = context.account_repository.get_by_id(account_id).await?;
    assert!(account_v2_opt.is_some());
    let account_v2 = account_v2_opt.unwrap();
    assert_eq!(account_v2.id, account_v1.id);
    assert_eq!(account_v2.balance, account_v1.balance);

    // 4. Perform an operation that updates the account (e.g., deposit)
    let deposit_amount = Decimal::new(50, 2);
    context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await?;

    // The `deposit_money` method via service calls `repository.save`, which should invalidate cache.

    // 5. Fetch the account again using AccountRepository::get_by_id
    // This should fetch from DB again (due to invalidation) and re-cache.
    let account_v3_opt = context.account_repository.get_by_id(account_id).await?;
    assert!(account_v3_opt.is_some(), "Account not found after deposit");
    let account_v3 = account_v3_opt.unwrap();

    assert_eq!(account_v3.id, account_id);
    assert_eq!(
        account_v3.balance,
        initial_balance + deposit_amount,
        "Balance not updated after deposit and re-fetch"
    );

    Ok(())
}

#[tokio::test]
async fn test_command_deduplication_integration() -> Result<(), Box<dyn std::error::Error>> {
    let context = setup_test_environment().await?;
    let redis_pool_config = RedisPoolConfig {
        min_connections: 1,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
    };
    let redis_client_trait = RealRedisClient::new(
        context.redis_conn_for_direct_checks.clone().unwrap(),
        Some(redis_pool_config),
    );

    flush_redis_db(&context.redis_conn_for_direct_checks.as_ref().unwrap())
        .await
        .expect("Failed to flush Redis DB for test");
    clear_dlq(&redis_client_trait)
        .await
        .expect("Failed to clear DLQ before test");

    let owner_name_base = format!("DedupeTest_{}", Uuid::new_v4());
    let initial_balance = Decimal::new(200, 0);

    // For `create_account`, the command_id for deduplication is the new account_id.
    // This makes testing tricky as we don't know it beforehand for the second call.
    // Let's test with `deposit_money` where we control the `account_id` (used as command_id).

    // First, create an account to deposit into.
    let account_id = context
        .account_service
        .create_account(format!("{}_Account", owner_name_base), initial_balance)
        .await?;

    let deposit_amount = Decimal::new(10, 0);

    // 1. First call to deposit_money (should succeed)
    let res1 = context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await;
    assert!(res1.is_ok(), "First deposit call failed: {:?}", res1.err());

    // 2. Second call (immediate duplicate, should fail)
    let res2 = context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await;
    assert!(
        res2.is_err(),
        "Second deposit call should have failed as duplicate"
    );
    if let Err(AccountError::InfrastructureError(msg)) = res2 {
        assert!(
            msg.contains("Duplicate deposit command"),
            "Error message mismatch for duplicate: {}",
            msg
        );
    } else {
        panic!("Expected InfrastructureError for duplicate, got {:?}", res2);
    }

    // 3. Wait for TTL to expire (COMMAND_DEDUP_TTL_SECONDS is 60s, make it shorter for tests or configurable)
    // For now, using the 60s TTL. This will make the test slow.
    // A better way would be to have a test-specific TTL or a way to mock time for Redis.
    // Or, for this test, manually delete the deduplication key from Redis.
    if let Some(ref client) = context.redis_conn_for_direct_checks {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let dedup_key = format!("{}{}", "command_dedup:", account_id); // Assuming prefix and command_id format
        let _: () = redis::cmd("DEL")
            .arg(&dedup_key)
            .query_async(&mut conn)
            .await?;
        println!("Manually deleted Redis key: {}", dedup_key);
    } else {
        println!("Cannot manually delete Redis key, test will be slow due to TTL. Consider direct Redis client for tests.");
        // Fallback to sleeping if direct Redis access isn't available for DEL
        // This is not ideal. Test will be very slow.
        // sleep(Duration::from_secs(super::COMMAND_DEDUP_TTL_SECONDS as u64 + 1)).await;
        // The COMMAND_DEDUP_TTL_SECONDS is not directly accessible here from super.
        // This highlights the need for better test configuration or direct Redis key manipulation.
        // For now, the manual DEL above is the preferred path if redis_conn_for_direct_checks is Some.
    }
    // If the manual DEL failed or was not possible, this test might show a false positive for the third call if TTL is long.
    // The current setup relies on `redis_conn_for_direct_checks` being available.

    // 4. Third call (after TTL or manual key deletion, should succeed)
    let res3 = context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await;
    assert!(
        res3.is_ok(),
        "Third deposit call failed after TTL/DEL: {:?}",
        res3.err()
    );

    Ok(())
}

#[tokio::test]
async fn test_event_flusher_successful_batch_processing() -> Result<(), Box<dyn std::error::Error>>
{
    let context = setup_test_environment().await?;
    let redis_pool_config = RedisPoolConfig {
        min_connections: 1,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
    };
    let redis_client_trait = RealRedisClient::new(
        context.redis_conn_for_direct_checks.clone().unwrap(),
        Some(redis_pool_config),
    );

    flush_redis_db(&context.redis_conn_for_direct_checks.as_ref().unwrap()).await?;
    clear_dlq(&redis_client_trait).await?;

    let owner_name = format!("FlushSuccessUser_{}", Uuid::new_v4());
    let initial_balance = Decimal::new(1000, 2);

    // Create an account
    let account_id = context
        .account_service
        .create_account(owner_name.clone(), initial_balance)
        .await?;

    // Wait for potential create_account batch to be flushed (AccountService now uses save_batched)
    sleep(Duration::from_millis(500)).await; // Adjust based on typical flusher speed

    let initial_version = get_account_current_version(account_id, &context.db_pool).await?;
    assert_eq!(
        initial_version, 1,
        "Account creation event should result in version 1"
    );

    // Perform a deposit, which should queue a RedisEventBatch
    let deposit_amount = Decimal::new(500, 2);
    context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await?;

    // Wait for the flusher to process the deposit batch
    sleep(Duration::from_millis(500)).await; // Adjust based on flusher interval + processing time

    // Assertions
    let final_version = get_account_current_version(account_id, &context.db_pool).await?;
    assert_eq!(
        final_version,
        initial_version + 1,
        "Version should increment after deposit event"
    );

    // Verify account balance from projection (assuming projection store is updated relatively quickly after event processing)
    // This might need more robust checks or waiting if projections are slow.
    // For now, let's check the direct repository/cache state for the account.
    let account_after_deposit_opt = context.account_repository.get_by_id(account_id).await?;
    assert!(
        account_after_deposit_opt.is_some(),
        "Account not found after deposit"
    );
    let account_after_deposit = account_after_deposit_opt.unwrap();
    assert_eq!(
        account_after_deposit.balance,
        initial_balance + deposit_amount,
        "Balance in account data incorrect after deposit"
    );
    assert_eq!(
        account_after_deposit.version, final_version,
        "Version in account data incorrect after deposit"
    );

    let batch_list = get_batch_list_items(&redis_client_trait, account_id).await?;
    assert!(
        batch_list.is_empty(),
        "Redis event batching list should be empty after successful flush"
    );

    let dlq_items = get_dlq_items(&redis_client_trait).await?;
    assert!(
        dlq_items.is_empty(),
        "DLQ should be empty after successful processing. Found: {:?}",
        dlq_items
    );

    // Verify Redis account cache key `account:{account_id}` is deleted or updated.
    // After a successful flush, the cache for the account is invalidated (DEL).
    // So a subsequent get_by_id would be a cache miss, then fill.
    // This is implicitly tested by `account_after_deposit_opt` being correct.
    // To be more explicit, one could try to fetch from Redis directly if a helper was available.

    Ok(())
}

#[tokio::test]
async fn test_event_flusher_version_conflict_moves_to_dlq() -> Result<(), Box<dyn std::error::Error>>
{
    let context = setup_test_environment().await?;
    let redis_pool_config = RedisPoolConfig {
        min_connections: 1,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
    };
    let redis_client_trait = RealRedisClient::new(
        context.redis_conn_for_direct_checks.clone().unwrap(),
        Some(redis_pool_config),
    );

    flush_redis_db(&context.redis_conn_for_direct_checks.as_ref().unwrap()).await?;
    clear_dlq(&redis_client_trait).await?;

    let owner_name = format!("VersionConflictUser_{}", Uuid::new_v4());
    let initial_balance = Decimal::new(200, 2);

    // 1. Create an account. This will use save_batched.
    let account_id = context
        .account_service
        .create_account(owner_name.clone(), initial_balance)
        .await?;

    // Wait for the creation event to be flushed and account to be version 1.
    sleep(Duration::from_millis(500)).await;
    let version_after_create = get_account_current_version(account_id, &context.db_pool).await?;
    assert_eq!(
        version_after_create, 1,
        "Account should be version 1 after creation and flush."
    );

    // 2. Manually insert a dummy event to create a version conflict.
    // This makes the DB version V2 (1 + 1), but app thinks it's V1.
    // This requires direct DB access and knowledge of the `events` table structure.
    // Let's assume StoredEvent has a simple structure for this insertion.
    // For simplicity, we'll just increment sequence_number.
    // This is a bit hacky; a proper test might use EventStore's save directly if possible
    // or have a backdoor in tests to create this state.
    let dummy_event_data = serde_json::json!({"type": "DummyEvent", "data": "conflict_payload"});
    sqlx::query("INSERT INTO events (id, aggregate_id, event_type, event_data, sequence_number, timestamp, version) VALUES ($1, $2, $3, $4, $5, $6, $7)")
        .bind(Uuid::new_v4()) // event id
        .bind(account_id)     // aggregate_id
        .bind("DummyConflictEvent") // event_type
        .bind(dummy_event_data) // event_data
        .bind(version_after_create + 1) // sequence_number (making it 2)
        .bind(chrono::Utc::now()) // timestamp
        .bind(1) // schema version of event
        .execute(&context.db_pool)
        .await?;

    let version_after_manual_insert =
        get_account_current_version(account_id, &context.db_pool).await?;
    assert_eq!(
        version_after_manual_insert,
        version_after_create + 1,
        "DB version not updated by manual insert."
    );

    // 3. Perform a service call (deposit). App still thinks account is at `version_after_create` (e.g. 1).
    // This will queue a RedisEventBatch with `expected_version = version_after_create`.
    let deposit_amount = Decimal::new(10, 0);
    context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await?;

    // 4. Wait for the flusher. It should encounter a version conflict.
    sleep(Duration::from_millis(500)).await;

    // Assertions
    let batch_list_items = get_batch_list_items(&redis_client_trait, account_id).await?;
    // The list might be empty if the flusher processed and moved to DLQ, or trimmed.
    // A more robust check is on DLQ content and DB state.
    if !batch_list_items.is_empty() {
        println!("Batch list not empty, content: {:?}", batch_list_items);
        // This might happen if LTRIM logic keeps the failed item if it was the only one.
    }

    let dlq_items = get_dlq_items(&redis_client_trait).await?;
    assert_eq!(
        dlq_items.len(),
        1,
        "DLQ should contain one item after version conflict. Found: {:?}",
        dlq_items
    );

    // Deserialize the item from DLQ
    let dlq_item_str = dlq_items.first().unwrap();
    // The DLQ item for an event_store_save_failure contains `failed_batch_item` which is `RedisEventBatch`
    let dlq_entry: serde_json::Value = serde_json::from_str(dlq_item_str)?;
    let failed_batch_json = dlq_entry
        .get("failed_batch_item")
        .expect("DLQ entry missing failed_batch_item");
    let failed_batch: banking_es::infrastructure::RedisEventBatchForTest =
        serde_json::from_value(failed_batch_json.clone())?;

    assert_eq!(
        failed_batch.expected_version, version_after_create,
        "DLQ'd batch has incorrect expected_version"
    );
    assert_eq!(
        failed_batch.events.len(),
        1,
        "DLQ'd batch should have one event"
    ); // Assuming deposit creates one event

    // Verify no new events from the conflicting batch were persisted
    let current_db_version_after_flush_attempt =
        get_account_current_version(account_id, &context.db_pool).await?;
    assert_eq!(
        current_db_version_after_flush_attempt, version_after_manual_insert,
        "DB version should not change after failed flush"
    );

    Ok(())
}

// Need to define RedisEventBatch for deserialization from DLQ if it's not pub from repository
// Or make it pub. For now, let's define a compatible struct for test deserialization.
// This assumes RedisEventBatch in repository.rs is:
// #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
// struct RedisEventBatch { expected_version: i64, events: Vec<AccountEvent> }
// And AccountEvent is also Serialize/Deserialize.
// This is a common pattern: test code might need to mirror non-pub structs.
// If `banking_es::infrastructure::RedisEventBatch` was public, we'd use that.
// For now, let's assume we can make it pub or use this workaround.
mod banking_es {
    pub mod infrastructure {
        use crate::banking_es::domain::AccountEvent; // Need to make AccountEvent path correct
        use serde::{Deserialize, Serialize};
        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct RedisEventBatchForTest {
            pub expected_version: i64,
            pub events: Vec<AccountEvent>,
        }
    }
    pub mod domain {
        // Assuming AccountEvent is here
        use rust_decimal::Decimal;
        use serde::{Deserialize, Serialize};
        use uuid::Uuid;
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)] // Added PartialEq for easier event comparison if needed
        #[serde(tag = "type")]
        pub enum AccountEvent {
            AccountCreated {
                account_id: Uuid,
                owner_name: String,
                initial_balance: Decimal,
            },
            MoneyDeposited {
                account_id: Uuid,
                amount: Decimal,
                transaction_id: Uuid,
            },
            MoneyWithdrawn {
                account_id: Uuid,
                amount: Decimal,
                transaction_id: Uuid,
            },
            AccountClosed {
                account_id: Uuid,
                reason: String,
            },
        }
    }
}

#[tokio::test]
async fn test_event_flusher_multiple_batches_same_account() -> Result<(), Box<dyn std::error::Error>>
{
    let context = setup_test_environment().await?;
    let redis_pool_config = RedisPoolConfig {
        min_connections: 1,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
    };
    let redis_client_trait = RealRedisClient::new(
        context.redis_conn_for_direct_checks.clone().unwrap(),
        Some(redis_pool_config),
    );

    flush_redis_db(&context.redis_conn_for_direct_checks.as_ref().unwrap()).await?;
    clear_dlq(&redis_client_trait).await?;

    let owner_name = format!("MultiBatchUser_{}", Uuid::new_v4());
    let initial_balance = Decimal::new(1000, 2);

    // Create account
    let account_id = context
        .account_service
        .create_account(owner_name, initial_balance)
        .await?;
    sleep(Duration::from_millis(500)).await; // Wait for creation event to flush

    let version_after_create = get_account_current_version(account_id, &context.db_pool).await?;
    assert_eq!(version_after_create, 1);

    // Action: Two deposits in quick succession
    let deposit1_amount = Decimal::new(100, 2);
    let deposit2_amount = Decimal::new(200, 2);

    context
        .account_service
        .deposit_money(account_id, deposit1_amount)
        .await?;
    // No sleep here, queue second batch immediately.
    // AccountService internally applies the event to its Account state before calling save_batched,
    // so the version for the second deposit should be correct.
    context
        .account_service
        .deposit_money(account_id, deposit2_amount)
        .await?;

    // Wait for both batches to be flushed
    sleep(Duration::from_millis(1000)).await; // Longer wait to ensure both can be processed

    // Assertions
    let final_version = get_account_current_version(account_id, &context.db_pool).await?;
    // Expected: Create (1) + Deposit1 (1) + Deposit2 (1) = 3 events total. Max sequence number is 3.
    assert_eq!(
        final_version,
        version_after_create + 2,
        "Version should reflect both deposits."
    );

    let account_opt = context.account_repository.get_by_id(account_id).await?;
    assert!(account_opt.is_some());
    let account = account_opt.unwrap();
    assert_eq!(
        account.balance,
        initial_balance + deposit1_amount + deposit2_amount,
        "Final balance is incorrect."
    );
    assert_eq!(
        account.version, final_version,
        "Final account version in data is incorrect."
    );

    let batch_list = get_batch_list_items(&redis_client_trait, account_id).await?;
    assert!(
        batch_list.is_empty(),
        "Redis event batching list should be empty after flushing multiple batches. Found: {:?}",
        batch_list
    );

    let dlq_items = get_dlq_items(&redis_client_trait).await?;
    assert!(
        dlq_items.is_empty(),
        "DLQ should be empty after successful multi-batch processing. Found: {:?}",
        dlq_items
    );

    Ok(())
}
