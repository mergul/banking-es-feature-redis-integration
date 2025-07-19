use banking_es::infrastructure::connection_pool_partitioning::*;
use sqlx::Row;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("🚀 Starting Connection Pool Partitioning Example");

    // === STEP 1: CREATE PARTITIONED POOLS ===
    let database_url = "postgresql://postgres:Francisco1@localhost:5432/banking_es";

    // Option 1: Use default configuration
    let pools = create_partitioned_pools(database_url).await?;

    // Option 2: Use custom configuration
    /*
    let config = PoolPartitioningConfig {
        database_url: database_url.to_string(),
        write_pool_max_connections: 10,
        write_pool_min_connections: 3,
        read_pool_max_connections: 100,
        read_pool_min_connections: 20,
        acquire_timeout_secs: 60,
        write_idle_timeout_secs: 1200,  // 20 minutes
        read_idle_timeout_secs: 1800,   // 30 minutes
        write_max_lifetime_secs: 3600,  // 1 hour
        read_max_lifetime_secs: 7200,   // 2 hours
    };
    let pools = create_partitioned_pools_with_config(config).await?;
    */

    // === STEP 2: PERFORM HEALTH CHECK ===
    info!("🔍 Performing pool health check...");
    let health = pools.health_check().await?;
    info!(
        "Health check result: write={}, read={}, response_time={:?}",
        health.write_pool_healthy, health.read_pool_healthy, health.response_time
    );

    // === STEP 3: DEMONSTRATE POOL SELECTION ===
    info!("📊 Demonstrating pool selection...");

    // Write operation (uses write pool)
    let write_pool = pools.select_pool(OperationType::Write);
    let write_result = {
        let mut conn = write_pool.acquire().await?;
        sqlx::query("SELECT 'write_operation' as operation_type, NOW() as timestamp")
            .fetch_one(&mut *conn)
            .await?
    };
    info!(
        "Write operation completed: {}",
        write_result.get::<String, _>("operation_type")
    );

    // Read operation (uses read pool)
    let read_pool = pools.select_pool(OperationType::Read);
    let read_result = {
        let mut conn = read_pool.acquire().await?;
        sqlx::query("SELECT 'read_operation' as operation_type, NOW() as timestamp")
            .fetch_one(&mut *conn)
            .await?
    };
    info!(
        "Read operation completed: {}",
        read_result.get::<String, _>("operation_type")
    );

    // === STEP 4: CONCURRENT OPERATIONS DEMO ===
    info!("⚡ Demonstrating concurrent operations...");

    // Concurrent write operations
    let write_tasks: Vec<_> = (0..5)
        .map(|i| {
            let write_pool = pools.write_pool_arc();
            tokio::spawn(async move {
                let mut conn = write_pool.acquire().await?;
                let result = sqlx::query("SELECT $1 as task_id, NOW() as timestamp")
                    .bind(i)
                    .fetch_one(&mut *conn)
                    .await?;
                Ok::<_, sqlx::Error>(result.get::<i32, _>("task_id"))
            })
        })
        .collect();

    // Concurrent read operations
    let read_tasks: Vec<_> = (0..10)
        .map(|i| {
            let read_pool = pools.read_pool_arc();
            tokio::spawn(async move {
                let mut conn = read_pool.acquire().await?;
                let result = sqlx::query("SELECT $1 as task_id, NOW() as timestamp")
                    .bind(i)
                    .fetch_one(&mut *conn)
                    .await?;
                Ok::<_, sqlx::Error>(result.get::<i32, _>("task_id"))
            })
        })
        .collect();

    // Wait for all tasks to complete
    let write_results = futures::future::join_all(write_tasks).await;
    let read_results = futures::future::join_all(read_tasks).await;

    let write_successful = write_results.iter().filter(|r| r.is_ok()).count();
    let read_successful = read_results.iter().filter(|r| r.is_ok()).count();

    info!(
        "Concurrent operations completed: {}/{} writes, {}/{} reads",
        write_successful,
        write_results.len(),
        read_successful,
        read_results.len()
    );

    // === STEP 5: POOL STATISTICS ===
    info!("📈 Pool statistics:");
    let stats = pools.get_stats();

    info!("Write Pool:");
    info!("   • Total connections: {}", stats.write_pool.total);
    info!("   • Active connections: {}", stats.write_pool.active);
    info!("   • Idle connections: {}", stats.write_pool.idle);
    info!("   • Utilization: {:.1}%", stats.write_pool.utilization);

    info!("Read Pool:");
    info!("   • Total connections: {}", stats.read_pool.total);
    info!("   • Active connections: {}", stats.read_pool.active);
    info!("   • Idle connections: {}", stats.read_pool.idle);
    info!("   • Utilization: {:.1}%", stats.read_pool.utilization);

    // === STEP 6: PRODUCTION INTEGRATION EXAMPLE ===
    info!("🏭 Production integration example:");

    // Example: Event Store with partitioned pools
    let event_store_pools = EventStorePools {
        write_pool: pools.write_pool_arc(),
        read_pool: pools.read_pool_arc(),
    };

    // Example: Repository with partitioned pools
    let account_repository = AccountRepositoryWithPartitioning {
        write_pool: pools.write_pool_arc(),
        read_pool: pools.read_pool_arc(),
    };

    info!("✅ Connection pool partitioning example completed successfully");

    // Clean up
    pools.close().await;

    Ok(())
}

// Example production integration structures
struct EventStorePools {
    write_pool: std::sync::Arc<sqlx::PgPool>,
    read_pool: std::sync::Arc<sqlx::PgPool>,
}

struct AccountRepositoryWithPartitioning {
    write_pool: std::sync::Arc<sqlx::PgPool>,
    read_pool: std::sync::Arc<sqlx::PgPool>,
}

impl EventStorePools {
    async fn store_event(&self, event_data: &str) -> Result<(), sqlx::Error> {
        let mut conn = self.write_pool.acquire().await?;
        sqlx::query("INSERT INTO events (data, created_at) VALUES ($1, NOW())")
            .bind(event_data)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    async fn get_events(&self, limit: i64) -> Result<Vec<String>, sqlx::Error> {
        let mut conn = self.read_pool.acquire().await?;
        let rows = sqlx::query("SELECT data FROM events ORDER BY created_at DESC LIMIT $1")
            .bind(limit)
            .fetch_all(&mut *conn)
            .await?;

        Ok(rows
            .iter()
            .map(|row| row.get::<String, _>("data"))
            .collect())
    }
}

impl AccountRepositoryWithPartitioning {
    async fn create_account(&self, account_data: &str) -> Result<(), sqlx::Error> {
        let mut conn = self.write_pool.acquire().await?;
        sqlx::query("INSERT INTO accounts (data, created_at) VALUES ($1, NOW())")
            .bind(account_data)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    async fn get_account(&self, account_id: i32) -> Result<Option<String>, sqlx::Error> {
        let mut conn = self.read_pool.acquire().await?;
        let row = sqlx::query("SELECT data FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(&mut *conn)
            .await?;

        Ok(row.map(|r| r.get::<String, _>("data")))
    }
}
