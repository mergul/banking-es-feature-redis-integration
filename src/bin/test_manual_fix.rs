use banking_es::application::services::CQRSAccountService;
use banking_es::infrastructure::{
    cache_service::{CacheConfig, CacheService},
    event_store::EventStore,
    kafka_abstraction::KafkaConfig,
    projections::ProjectionStore,
    redis_abstraction::RealRedisClient,
};
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🔧 Manual test of withdrawal and CDC fixes...");

    // Connect to database
    let database_url = "postgresql://postgres:Francisco1@localhost:5432/banking_es";
    let pool = sqlx::PgPool::connect(database_url).await?;

    // Setup services
    let event_store = Arc::new(EventStore::new(pool.clone()));
    let projection_store = Arc::new(ProjectionStore::new(pool.clone()));

    let redis_url = "redis://localhost:6379";
    let redis_client = redis::Client::open(redis_url)?;
    let redis_client_trait = RealRedisClient::new(redis_client, None);
    let cache_config = CacheConfig::default();
    let cache_service = Arc::new(CacheService::new(redis_client_trait, cache_config));

    let kafka_config = KafkaConfig::default();

    // Create CQRS service
    let cqrs_service = Arc::new(CQRSAccountService::new(
        event_store.clone(),
        projection_store.clone(),
        cache_service.clone(),
        kafka_config,
        10,                                    // max_concurrent_operations
        50,                                    // batch_size
        std::time::Duration::from_millis(100), // batch_timeout
        true,                                  // enable_write_batching
        None,                                  // consistency_manager
    ));

    // Start write batching
    cqrs_service.start_write_batching().await?;

    // Create a test account with unique name
    let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let account_name = format!("ManualTest_{}", test_id);
    println!("📝 Creating test account: {}", account_name);

    let account_id = cqrs_service
        .create_account(account_name, Decimal::new(1000, 0))
        .await?;
    println!("✅ Created account: {}", account_id);

    // Wait for CDC processing
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Perform a withdrawal
    println!("💰 Performing withdrawal of 100...");
    cqrs_service
        .withdraw_money(account_id, Decimal::new(100, 0))
        .await?;
    println!("✅ Withdrawal completed");

    // Wait for CDC processing
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Check database state
    println!("🔍 Checking database state...");

    // Check events
    let events = sqlx::query!(
        "SELECT event_type, COUNT(*) as count FROM events WHERE aggregate_id = $1 GROUP BY event_type",
        account_id
    )
    .fetch_all(&pool)
    .await?;

    println!("📊 Events for account {}:", account_id);
    for event in events {
        println!("  - {}: {}", event.event_type, event.count.unwrap_or(0));
    }

    // Check outbox
    let outbox = sqlx::query!(
        "SELECT event_type, COUNT(*) as count FROM kafka_outbox_cdc WHERE aggregate_id = $1 GROUP BY event_type",
        account_id
    )
    .fetch_all(&pool)
    .await?;

    println!("📦 Outbox for account {}:", account_id);
    for msg in outbox {
        println!("  - {}: {}", msg.event_type, msg.count.unwrap_or(0));
    }

    // Check transaction projections
    let transactions = sqlx::query!(
        "SELECT COUNT(*) as count FROM transaction_projections WHERE account_id = $1",
        account_id
    )
    .fetch_one(&pool)
    .await?;

    println!(
        "💳 Transaction projections for account {}: {}",
        account_id,
        transactions.count.unwrap_or(0)
    );

    // Check account projection
    let account = sqlx::query!(
        "SELECT balance FROM account_projections WHERE id = $1",
        account_id
    )
    .fetch_optional(&pool)
    .await?;

    if let Some(acc) = account {
        println!("💰 Account balance: {}", acc.balance);
    } else {
        println!("❌ Account projection not found");
    }

    println!("✅ Manual test completed!");
    Ok(())
}
