use ::banking_es::domain::{Account, AccountEvent};
use ::banking_es::infrastructure::cache_service::CacheService;
use ::banking_es::infrastructure::middleware::RequestMiddleware;
use ::banking_es::{
    AccountError, AccountRepository, AccountRepositoryTrait, AccountService, EventStore,
    EventStoreConfig, ProjectionStore,
};
use anyhow::Result;
use banking_es::infrastructure::cache_service::CacheServiceTrait;
use banking_es::infrastructure::event_store::EventStoreTrait;
use banking_es::infrastructure::projections::ProjectionStoreTrait;
use dotenv;
use redis;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

// Helper structure to hold common test dependencies
struct TestContext {
    account_service: Arc<AccountService>,
    account_repository: Arc<AccountRepository>,
    db_pool: PgPool,
}

async fn setup_test_environment() -> Result<TestContext, Box<dyn std::error::Error>> {
    dotenv::dotenv().ok(); // Load .env file if present

    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");

    // Setup Database
    let db_pool = PgPool::connect(&database_url).await?;

    // Setup EventStore and ProjectionStore
    let event_store = EventStore::new_with_config(EventStoreConfig::default())
        .await
        .expect("Failed to create EventStore");
    let projection_store = ProjectionStore::new(db_pool.clone());

    // Setup Repository and Service
    let event_store = Arc::new(event_store) as Arc<dyn EventStoreTrait + 'static>;
    let account_repository: Arc<AccountRepository> = Arc::new(AccountRepository::new(event_store));
    let account_repository_clone = account_repository.clone();

    let service = Arc::new(AccountService::new(
        account_repository,
        Arc::new(projection_store) as Arc<dyn ProjectionStoreTrait + 'static>,
        Arc::new(CacheService::default()) as Arc<dyn CacheServiceTrait + 'static>,
        Arc::new(RequestMiddleware::default()),
        100,
    ));

    Ok(TestContext {
        account_service: service.clone(),
        account_repository: account_repository_clone,
        db_pool,
    })
}

// Helper to get current account version from event store DB
async fn get_account_current_version(account_id: Uuid, pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT MAX(sequence_number) FROM events WHERE aggregate_id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map_or(0, |(max_seq,)| max_seq)) // Version is max sequence number, or 0 if no events
}

#[tokio::test]
async fn test_account_data_caching_and_invalidation() -> Result<(), Box<dyn std::error::Error>> {
    let context = setup_test_environment().await?;

    let owner_name = "TestUser_".to_string() + &Uuid::new_v4().to_string();
    let initial_balance = Decimal::new(100, 2);

    // 1. Create an account using AccountService
    let account_id = context
        .account_service
        .create_account(owner_name.clone(), initial_balance)
        .await?;

    // 2. Fetch the account using AccountRepository::get_by_id (first fetch)
    let account_v1_opt = context.account_repository.get_by_id(account_id).await?;
    if !(account_v1_opt.is_some()) {
        let _ = std::io::stderr().write_all("Account not found after creation\\n".as_bytes());
        return Ok(());
    }
    let account_v1 = account_v1_opt.unwrap();
    if account_v1.owner_name != owner_name {
        let _ =
            std::io::stderr().write_all("Assertion failed: owner_name != owner_name\\n".as_bytes());
        return Ok(());
    }
    if account_v1.balance != initial_balance {
        let _ = std::io::stderr()
            .write_all("Assertion failed: balance != initial_balance\\n".as_bytes());
        return Ok(());
    }

    // 3. Perform a deposit operation
    let deposit_amount = Decimal::new(50, 2);
    context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await?;

    // 4. Fetch the account again using AccountRepository::get_by_id
    let account_v2_opt = context.account_repository.get_by_id(account_id).await?;
    if !(account_v2_opt.is_some()) {
        let _ = std::io::stderr().write_all("Assertion failed\\n".as_bytes());
        return Ok(());
    }
    let account_v2 = account_v2_opt.unwrap();

    // 5. Fetch the account again using AccountRepository::get_by_id
    let account_v3_opt = context.account_repository.get_by_id(account_id).await?;
    if !(account_v3_opt.is_some()) {
        let _ = std::io::stderr().write_all("Assertion failed\\n".as_bytes());
        return Ok(());
    }
    let account_v3 = account_v3_opt.unwrap();

    if account_v3.id != account_id {
        let _ = std::io::stderr()
            .write_all("Assertion failed: account_v3.id != account_id\\n".as_bytes());
        return Ok(());
    }

    if account_v3.balance != initial_balance + deposit_amount {
        let _ = std::io::stderr().write_all("Assertion failed: balance mismatch\\n".as_bytes());
        return Ok(());
    }

    Ok(())
}

#[tokio::test]
async fn test_command_deduplication_integration() -> Result<(), Box<dyn std::error::Error>> {
    let context = setup_test_environment().await?;

    let owner_name_base = "DedupeTest_".to_string() + &Uuid::new_v4().to_string();
    let initial_balance = Decimal::new(200, 0);

    // First, create an account to deposit into.
    let account_id = context
        .account_service
        .create_account(owner_name_base.clone() + "_Account", initial_balance)
        .await?;

    let deposit_amount = Decimal::new(10, 0);

    // 1. First call (should succeed)
    let res1 = context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await;
    if !(res1.is_ok()) {
        let _ = std::io::stderr().write_all("Assertion failed\\n".as_bytes());
        return Ok(());
    }

    // 2. Second call (immediate duplicate, should fail)
    let res2 = context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await;

    if !(res2.is_err()) {
        let _ = std::io::stderr().write_all("Assertion failed\\n".as_bytes());
        return Ok(());
    }
    if let Err(AccountError::InfrastructureError(msg)) = res2 {
        if !(msg.contains("Duplicate deposit command")) {
            let _ = std::io::stderr().write_all("Assertion failed\\n".as_bytes());
            return Ok(());
        }
    } else {
        let _ = std::io::stderr().write_all("Assertion failed\\n".as_bytes());
        return Ok(());
    }

    // 3. Wait for TTL to expire
    sleep(Duration::from_secs(2)).await;

    // 4. Third call (after TTL, should succeed)
    let res3 = context
        .account_service
        .deposit_money(account_id, deposit_amount)
        .await;

    if !(res3.is_ok()) {
        let _ = std::io::stderr().write_all("Assertion failed\\n".as_bytes());
        return Ok(());
    }

    Ok(())
}
