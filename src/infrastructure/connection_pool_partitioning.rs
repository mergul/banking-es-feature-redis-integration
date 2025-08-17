use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use tracing::{error, info, warn};

/// Configuration for connection pool partitioning
#[derive(Debug, Clone)]
pub struct PoolPartitioningConfig {
    pub database_url: String,
    pub write_pool_max_connections: u32,
    pub write_pool_min_connections: u32,
    pub read_pool_max_connections: u32,
    pub read_pool_min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub write_idle_timeout_secs: u64,
    pub read_idle_timeout_secs: u64,
    pub write_max_lifetime_secs: u64,
    pub read_max_lifetime_secs: u64,
}

impl Default for PoolPartitioningConfig {
    fn default() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
            }),
            write_pool_max_connections: std::env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "200".to_string())
                .parse()
                .unwrap_or(200)
                / 5, // Write pool gets 1/5 of total connections
            write_pool_min_connections: std::env::var("DB_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10)
                / 5, // Write pool gets 1/5 of min connections
            read_pool_max_connections: std::env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "200".to_string())
                .parse()
                .unwrap_or(200)
                * 4
                / 5, // Read pool gets 4/5 of total connections
            read_pool_min_connections: std::env::var("DB_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10)
                * 4
                / 5, // Read pool gets 4/5 of min connections
            acquire_timeout_secs: std::env::var("DB_ACQUIRE_TIMEOUT")
                .unwrap_or_else(|_| "15".to_string())
                .parse()
                .unwrap_or(15),
            write_idle_timeout_secs: std::env::var("DB_IDLE_TIMEOUT")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
            read_idle_timeout_secs: std::env::var("DB_IDLE_TIMEOUT")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
            write_max_lifetime_secs: std::env::var("DB_WRITE_MAX_LIFETIME")
                .unwrap_or_else(|_| "900".to_string())
                .parse()
                .unwrap_or(900),
            read_max_lifetime_secs: std::env::var("DB_MAX_LIFETIME")
                .unwrap_or_else(|_| "900".to_string())
                .parse()
                .unwrap_or(900)
                * 2, // Longer lifetime for reads
        }
    }
}

/// Partitioned connection pools for read/write separation
#[derive(Debug)]
pub struct PartitionedPools {
    pub write_pool: PgPool,
    pub read_pool: PgPool,
    pub config: PoolPartitioningConfig,
}

impl PartitionedPools {
    /// Create new partitioned connection pools
    pub async fn new(config: PoolPartitioningConfig) -> Result<Self, sqlx::Error> {
        info!("🔀 Creating partitioned connection pools...");

        // Create write pool (smaller, conservative settings)
        info!(
            "Creating WRITE pool: {} max connections",
            config.write_pool_max_connections
        );
        let write_pool = PgPoolOptions::new()
            .max_connections(config.write_pool_max_connections)
            .min_connections(config.write_pool_min_connections)
            .acquire_timeout(std::time::Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(std::time::Duration::from_secs(
                config.write_idle_timeout_secs,
            ))
            .max_lifetime(std::time::Duration::from_secs(
                config.write_max_lifetime_secs,
            ))
            .connect(&config.database_url)
            .await?;

        // Create read pool (larger, optimized for reads)
        info!(
            "Creating READ pool: {} max connections",
            config.read_pool_max_connections
        );
        let read_pool = PgPoolOptions::new()
            .max_connections(config.read_pool_max_connections)
            .min_connections(config.read_pool_min_connections)
            .acquire_timeout(std::time::Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(std::time::Duration::from_secs(
                config.read_idle_timeout_secs,
            ))
            .max_lifetime(std::time::Duration::from_secs(
                config.read_max_lifetime_secs,
            ))
            .connect(&config.database_url)
            .await?;

        info!("✅ Partitioned connection pools created successfully");
        info!(
            "   • Write pool: {}/{} connections",
            write_pool.size() - write_pool.num_idle() as u32,
            write_pool.size()
        );
        info!(
            "   • Read pool: {}/{} connections",
            read_pool.size() - read_pool.num_idle() as u32,
            read_pool.size()
        );

        Ok(Self {
            write_pool,
            read_pool,
            config,
        })
    }

    /// Get write pool for write operations
    pub fn write_pool(&self) -> &PgPool {
        &self.write_pool
    }

    /// Get read pool for read operations
    pub fn read_pool(&self) -> &PgPool {
        &self.read_pool
    }

    /// Get write pool as Arc for sharing
    pub fn write_pool_arc(&self) -> Arc<PgPool> {
        Arc::new(self.write_pool.clone())
    }

    /// Get read pool as Arc for sharing
    pub fn read_pool_arc(&self) -> Arc<PgPool> {
        Arc::new(self.read_pool.clone())
    }

    /// Get pool statistics
    pub fn get_stats(&self) -> PoolStats {
        let write_pool_size = self.write_pool.size() as u32;
        let write_idle = self.write_pool.num_idle() as u32;
        let write_active = write_pool_size - write_idle;

        let read_pool_size = self.read_pool.size() as u32;
        let read_idle = self.read_pool.num_idle() as u32;
        let read_active = read_pool_size - read_idle;

        PoolStats {
            write_pool: PoolInfo {
                total: write_pool_size,
                active: write_active,
                idle: write_idle,
                utilization: if write_pool_size > 0 {
                    (write_active as f64 / write_pool_size as f64) * 100.0
                } else {
                    0.0
                },
            },
            read_pool: PoolInfo {
                total: read_pool_size,
                active: read_active,
                idle: read_idle,
                utilization: if read_pool_size > 0 {
                    (read_active as f64 / read_pool_size as f64) * 100.0
                } else {
                    0.0
                },
            },
        }
    }

    /// Test pool health with simple operations
    pub async fn health_check(&self) -> Result<PoolHealth, sqlx::Error> {
        let start_time = std::time::Instant::now();

        // Test write pool
        let write_result = {
            let mut conn = self.write_pool.acquire().await?;
            sqlx::query("SELECT 1").fetch_one(&mut *conn).await
        };

        // Test read pool
        let read_result = {
            let mut conn = self.read_pool.acquire().await?;
            sqlx::query("SELECT 1").fetch_one(&mut *conn).await
        };

        let duration = start_time.elapsed();

        let health = PoolHealth {
            write_pool_healthy: write_result.is_ok(),
            read_pool_healthy: read_result.is_ok(),
            response_time: duration,
            timestamp: std::time::SystemTime::now(),
        };

        if health.write_pool_healthy && health.read_pool_healthy {
            info!("✅ Pool health check passed in {:?}", duration);
        } else {
            error!(
                "❌ Pool health check failed: write={}, read={}",
                health.write_pool_healthy, health.read_pool_healthy
            );
        }

        Ok(health)
    }

    /// Close all pools
    pub async fn close(&self) {
        info!("Closing partitioned connection pools...");
        self.write_pool.close().await;
        self.read_pool.close().await;
        info!("✅ Partitioned connection pools closed");
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub write_pool: PoolInfo,
    pub read_pool: PoolInfo,
}

/// Individual pool information
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub total: u32,
    pub active: u32,
    pub idle: u32,
    pub utilization: f64,
}

/// Pool health status
#[derive(Debug, Clone)]
pub struct PoolHealth {
    pub write_pool_healthy: bool,
    pub read_pool_healthy: bool,
    pub response_time: std::time::Duration,
    pub timestamp: std::time::SystemTime,
}

/// Convenience trait for operations that need to choose the right pool
pub trait PoolSelector {
    fn select_pool(&self, operation_type: OperationType) -> &PgPool;
}

impl PoolSelector for PartitionedPools {
    fn select_pool(&self, operation_type: OperationType) -> &PgPool {
        match operation_type {
            OperationType::Read => &self.read_pool,
            OperationType::Write => &self.write_pool,
        }
    }
}

/// Operation type for pool selection
#[derive(Debug, Clone, Copy)]
pub enum OperationType {
    Read,
    Write,
}

impl OperationType {
    pub fn is_read(&self) -> bool {
        matches!(self, OperationType::Read)
    }

    pub fn is_write(&self) -> bool {
        matches!(self, OperationType::Write)
    }
}

/// Helper function to create partitioned pools with default configuration
pub async fn create_partitioned_pools(database_url: &str) -> Result<PartitionedPools, sqlx::Error> {
    let config = PoolPartitioningConfig {
        database_url: database_url.to_string(),
        ..Default::default()
    };

    PartitionedPools::new(config).await
}

/// Helper function to create partitioned pools with custom configuration
pub async fn create_partitioned_pools_with_config(
    config: PoolPartitioningConfig,
) -> Result<PartitionedPools, sqlx::Error> {
    PartitionedPools::new(config).await
}

#[cfg(test)]
#[ignore]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_pool_partitioning_creation() {
        let config = PoolPartitioningConfig {
            database_url: "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string(),
            write_pool_max_connections: 3,
            read_pool_max_connections: 10,
            ..Default::default()
        };

        let pools = PartitionedPools::new(config).await;
        assert!(pools.is_ok());

        if let Ok(pools) = pools {
            let stats = pools.get_stats();
            // The pool starts with min_connections, not max_connections
            assert_eq!(stats.write_pool.total, 2); // min_connections from default
            assert_eq!(stats.read_pool.total, 10); // min_connections from default

            pools.close().await;
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_pool_selector() {
        let config = PoolPartitioningConfig::default();
        let pools = PartitionedPools::new(config).await.unwrap();

        let read_pool = pools.select_pool(OperationType::Read);
        let write_pool = pools.select_pool(OperationType::Write);

        assert_ne!(read_pool.size(), write_pool.size());

        pools.close().await;
    }
}
