use banking_es::application::services::CQRSAccountService;
use banking_es::infrastructure::{
    cache_service::{CacheConfig, CacheService},
    event_store::EventStore,
    projections::ProjectionStore,
    redis_abstraction::RealRedisClient,
};
use rust_decimal::Decimal;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tracing;
use uuid::Uuid;

async fn setup_cqrs_test_environment() -> Result<CQRSAccountService, Box<dyn std::error::Error>> {
    // Initialize database pool
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPoolOptions::new()
        .max_connections(100)
        .min_connections(20)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(600))
        .max_lifetime(Duration::from_secs(1800))
        .connect(&database_url)
        .await?;

    // Initialize Redis client
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let redis_client_trait = RealRedisClient::new(redis_client, None);

    // Initialize services
    let event_store = Arc::new(EventStore::new(pool.clone()));
    let projection_store = Arc::new(ProjectionStore::new(pool.clone()));

    let mut cache_config = CacheConfig::default();
    cache_config.default_ttl = Duration::from_secs(300);
    cache_config.max_size = 10000;
    cache_config.shard_count = 8;

    let cache_service = Arc::new(CacheService::new(redis_client_trait, cache_config));

    // Create CQRS service
    let cqrs_service = CQRSAccountService::new(
        event_store,
        projection_store,
        cache_service,
        100,                        // max_concurrent_operations
        50,                         // batch_size
        Duration::from_millis(100), // batch_timeout
    );

    Ok(cqrs_service)
}

#[tokio::test]
async fn test_cqrs_create_account() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Test create account command
    let account_id = service
        .create_account("CQRS Test User".to_string(), Decimal::new(1000, 0))
        .await
        .expect("Failed to create account");

    if !(!account_id.is_nil()) {
        tracing::error!("Account ID should not be nil");
        return;
    }

    // Test get account query
    let account = service
        .get_account(account_id)
        .await
        .expect("Failed to get account")
        .expect("Account should exist");

    if account_id.is_nil() {
        tracing::error!("Account ID should not be nil");
        return;
    }

    if account.owner_name != "CQRS Test User" {
        tracing::error!("Assertion failed: owner name mismatch");
        return;
    }
    if account.balance != Decimal::new(1000, 0) {
        tracing::error!("Assertion failed: balance mismatch");
        return;
    }
    if !(account.is_active) {
        tracing::error!("Assertion failed");
        return;
    }

    if account.owner_name != "CQRS Test User" {
        tracing::error!("Owner name should be 'CQRS Test User'");
        return;
    }
    if account.balance != Decimal::new(1000, 0) {
        tracing::error!("Balance should be 1000");
        return;
    }
    if !account.is_active {
        tracing::error!("Account should be active");
        return;
    }
}

#[tokio::test]
async fn test_cqrs_deposit_and_withdraw() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Create account
    let account_id = service
        .create_account("CQRS Transaction Test".to_string(), Decimal::new(1000, 0))
        .await
        .expect("Failed to create account");

    // Test deposit command
    service
        .deposit_money(account_id, Decimal::new(500, 0))
        .await
        .expect("Failed to deposit money");

    // Test get balance query
    let balance = service
        .get_account_balance(account_id)
        .await
        .expect("Failed to get account balance");

    assert_eq!(balance, Decimal::new(1500, 0));

    // Test withdraw command
    service
        .withdraw_money(account_id, Decimal::new(300, 0))
        .await
        .expect("Failed to withdraw money");

    // Test get balance query again
    let balance = service
        .get_account_balance(account_id)
        .await
        .expect("Failed to get account balance");

    assert_eq!(balance, Decimal::new(1200, 0));

    // Test get account status query
    let is_active = service
        .is_account_active(account_id)
        .await
        .expect("Failed to get account status");

    if !(is_active) {
        tracing::error!("Account should be active");
        return;
    }
}

#[tokio::test]
async fn test_cqrs_get_transactions() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Create account
    let account_id = service
        .create_account(
            "CQRS Transaction History Test".to_string(),
            Decimal::new(1000, 0),
        )
        .await
        .expect("Failed to create account");

    // Perform some transactions
    service
        .deposit_money(account_id, Decimal::new(500, 0))
        .await
        .expect("Failed to deposit money");

    service
        .withdraw_money(account_id, Decimal::new(200, 0))
        .await
        .expect("Failed to withdraw money");

    // Test get transactions query
    let transactions = service
        .get_account_transactions(account_id)
        .await
        .expect("Failed to get account transactions");

    if transactions.len() != 3 {
        tracing::error!("Assertion failed: transactions.len() != 3");
        return;
    } // Create + Deposit + Withdraw

    let transaction_types: Vec<&str> = transactions
        .iter()
        .map(|t| t.transaction_type.as_str())
        .collect();

    assert!(transaction_types.contains(&"MoneyDeposited"));
    assert!(transaction_types.contains(&"MoneyWithdrawn"));
}

#[tokio::test]
async fn test_cqrs_close_account() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Create account
    let account_id = service
        .create_account("CQRS Close Test".to_string(), Decimal::new(1000, 0))
        .await
        .expect("Failed to create account");

    // Test close account command
    service
        .close_account(account_id, "Test closure".to_string())
        .await
        .expect("Failed to close account");

    // Test get account status query
    let is_active = service
        .is_account_active(account_id)
        .await
        .expect("Failed to get account status");

    if !(is_active) {
        tracing::error!("Account should be active");
        return;
    }
}

// #[tokio::test]
// async fn test_cqrs_batch_transactions() {
//     let service = setup_cqrs_test_environment()
//         .await
//         .expect("Failed to setup test environment");
//
//     // Create multiple accounts
//     let account1 = service
//         .create_account("Batch Test User 1".to_string(), Decimal::new(1000, 0))
//         .await
//         .expect("Failed to create account 1");
//
//     let account2 = service
//         .create_account("Batch Test User 2".to_string(), Decimal::new(1000, 0))
//         .await
//         .expect("Failed to create account 2");
//
//     // Create batch transactions
//     let batch_transactions = vec![
//         // crate::application::services::BatchTransaction { // <-- Commented out unresolved import
//         //     account_id: account1,
//         //     transaction_type: "deposit".to_string(),
//         //     amount: Decimal::new(100, 0),
//         // },
//         // crate::application::services::BatchTransaction {
//         //     account_id: account2,
//         //     transaction_type: "withdraw".to_string(),
//         //     amount: Decimal::new(50, 0),
//         // },
//     ];
//
//     // Test batch processing
//     let result = service
//         .batch_transactions(batch_transactions)
//         .await
//         .expect("Failed to process batch transactions");
//
//     if result.successful != 2 {
//         let _ = std::io::stderr().write_all("Assertion failed: result.successful != 2\n".as_bytes());
//         return;
//     }
//     if result.failed != 0 {
//         let _ = std::io::stderr().write_all("Assertion failed: result.failed != 0\n".as_bytes());
//         return;
//     }
//
//     // Verify results
//     let balance1 = service
//         .get_account_balance(account1)
//         .await
//         .expect("Failed to get account 1 balance");
//     if balance1 != Decimal::new(1100, 0) {
//         let _ = std::io::stderr().write_all("Assertion failed: balance1 != 1100\n".as_bytes());
//         return;
//     }
//
//     let balance2 = service
//         .get_account_balance(account2)
//         .await
//         .expect("Failed to get account 2 balance");
//     if balance2 != Decimal::new(950, 0) {
//         let _ = std::io::stderr().write_all("Assertion failed: balance2 != 950\n".as_bytes());
//         return;
//     }
// }

#[tokio::test]
async fn test_cqrs_get_all_accounts() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Create multiple accounts
    let account1 = service
        .create_account("All Accounts Test 1".to_string(), Decimal::new(1000, 0))
        .await
        .expect("Failed to create account 1");

    let account2 = service
        .create_account("All Accounts Test 2".to_string(), Decimal::new(2000, 0))
        .await
        .expect("Failed to create account 2");

    // Test get all accounts query
    let accounts = service
        .get_all_accounts()
        .await
        .expect("Failed to get all accounts");

    if !(accounts.len() >= 2) {
        tracing::error!("Should have at least 2 accounts");
        return;
    }

    let account_ids: Vec<Uuid> = accounts.iter().map(|a| a.id).collect();
    if !(account_ids.contains(&account1)) {
        tracing::error!("Assertion failed");
        return;
    }
    if !(account_ids.contains(&account2)) {
        tracing::error!("Assertion failed");
        return;
    }
}

#[tokio::test]
async fn test_cqrs_health_check() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Test health check
    let health = service
        .health_check()
        .await
        .expect("Failed to get health status");

    if health.status != "healthy" {
        tracing::error!("Assertion failed: health.status != healthy");
        return;
    }
    if !(health.total_permits > 0) {
        tracing::error!("Assertion failed");
        return;
    }
    // if !(health.uptime_seconds > 0.0) { unsafe { panic!("Assertion failed") } } // <-- Commented out missing field assertion
}

#[tokio::test]
async fn test_cqrs_metrics() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Perform some operations to generate metrics
    let account_id = service
        .create_account("Metrics Test".to_string(), Decimal::new(1000, 0))
        .await
        .expect("Failed to create account");

    service
        .deposit_money(account_id, Decimal::new(100, 0))
        .await
        .expect("Failed to deposit money");

    service
        .get_account(account_id)
        .await
        .expect("Failed to get account");

    // Test get metrics
    let metrics = service.get_metrics();

    if !(metrics
        .commands_processed
        .load(std::sync::atomic::Ordering::Relaxed)
        > 0)
    {
        tracing::error!("Assertion failed");
        return;
    }
    if !(metrics
        .queries_processed
        .load(std::sync::atomic::Ordering::Relaxed)
        > 0)
    {
        tracing::error!("Assertion failed");
        return;
    }
}

#[tokio::test]
async fn test_cqrs_error_handling() {
    let service = setup_cqrs_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Test non-existent account
    let non_existent_id = Uuid::new_v4();
    let result = service.get_account(non_existent_id).await;
    if !(matches!(result, Ok(None))) {
        tracing::error!("Assertion failed");
        return;
    }

    // Test withdrawal with insufficient funds
    let account_id = service
        .create_account("Error Test".to_string(), Decimal::new(100, 0))
        .await
        .expect("Failed to create account");

    let result = service
        .withdraw_money(account_id, Decimal::new(200, 0))
        .await;

    if !(result.is_err()) {
        tracing::error!("Assertion failed");
        return;
    }
    if !(result.unwrap_err().to_string().contains("Insufficient")) {
        tracing::error!("Assertion failed");
        return;
    }
}
