use serde;
use sqlx::PgPool;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct PoolMetrics {
    pub total_connections: u32,
    pub active_connections: u32,
    pub idle_connections: u32,
    pub waiting_requests: u32,
    pub connection_acquire_time: Duration,
    pub connection_acquire_errors: u64,
    pub connection_timeouts: u64,
    pub last_health_check: Instant,
}

#[derive(Debug, Clone)]
pub struct PoolMonitorConfig {
    pub health_check_interval: Duration,
    pub connection_timeout: Duration,
    pub max_connection_wait_time: Duration,
    pub pool_exhaustion_threshold: f64, // percentage of max connections
    pub enable_auto_scaling: bool,
    pub max_connections: u32,
    pub min_connections: u32,
}

impl Default for PoolMonitorConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(10),
            connection_timeout: Duration::from_secs(30),
            max_connection_wait_time: Duration::from_secs(5),
            pool_exhaustion_threshold: 0.8,
            enable_auto_scaling: true,
            max_connections: 100,
            min_connections: 5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionPoolMonitor {
    pool: PgPool,
    config: PoolMonitorConfig,
    metrics: Arc<RwLock<PoolMetrics>>,
    stuck_connections: Arc<RwLock<Vec<StuckConnection>>>,
}

#[derive(Debug, Clone)]
pub struct StuckConnection {
    pub connection_id: String,
    pub acquired_at: Instant,
    pub operation_type: String,
    pub timeout: Duration,
}

impl ConnectionPoolMonitor {
    pub fn new(pool: PgPool, config: PoolMonitorConfig) -> Self {
        let monitor = Self {
            pool: pool.clone(),
            config,
            metrics: Arc::new(RwLock::new(PoolMetrics {
                total_connections: 0,
                active_connections: 0,
                idle_connections: 0,
                waiting_requests: 0,
                connection_acquire_time: Duration::ZERO,
                connection_acquire_errors: 0,
                connection_timeouts: 0,
                last_health_check: Instant::now(),
            })),
            stuck_connections: Arc::new(RwLock::new(Vec::new())),
        };

        // Start monitoring
        let monitor_clone = monitor.clone();
        tokio::spawn(async move {
            monitor_clone.monitor_pool().await;
        });

        monitor
    }

    pub async fn monitor_pool(&self) {
        let mut interval = tokio::time::interval(self.config.health_check_interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.check_pool_health().await {
                error!("Pool health check failed: {}", e);
            }

            if let Err(e) = self.detect_stuck_connections().await {
                error!("Stuck connection detection failed: {}", e);
            }

            if self.config.enable_auto_scaling {
                if let Err(e) = self.auto_scale_pool().await {
                    error!("Auto-scaling failed: {}", e);
                }
            }
        }
    }

    async fn check_pool_health(&self) -> Result<(), Box<dyn std::error::Error>> {
        let total = self.pool.size();
        let idle = self.pool.num_idle() as u32;
        let active = total - idle;

        let mut metrics = self.metrics.write().await;
        metrics.total_connections = total;
        metrics.active_connections = active;
        metrics.idle_connections = idle;
        metrics.last_health_check = Instant::now();

        // Check for pool exhaustion
        let utilization = active as f64 / total as f64;
        if utilization > self.config.pool_exhaustion_threshold {
            warn!(
                "Connection pool utilization high: {:.1}% ({}/{})",
                utilization * 100.0,
                active,
                total
            );
        }

        // Check for connection leaks
        if active > total * 9 / 10 {
            warn!(
                "Potential connection leak detected: {}/{} connections active",
                active, total
            );
        }

        Ok(())
    }

    async fn detect_stuck_connections(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut stuck_connections = self.stuck_connections.write().await;
        let now = Instant::now();

        // Remove expired stuck connections
        stuck_connections.retain(|conn| now.duration_since(conn.acquired_at) < conn.timeout);

        // Check for new stuck connections
        let metrics = self.metrics.read().await;
        if metrics.active_connections > 0 {
            // This is a simplified check - in a real implementation, you'd track individual connections
            let avg_connection_time = metrics.connection_acquire_time;
            if avg_connection_time > self.config.connection_timeout {
                warn!(
                    "Average connection time ({:.1}s) exceeds timeout ({:.1}s)",
                    avg_connection_time.as_secs_f64(),
                    self.config.connection_timeout.as_secs_f64()
                );
            }
        }

        Ok(())
    }

    async fn auto_scale_pool(&self) -> Result<(), Box<dyn std::error::Error>> {
        let metrics = self.metrics.read().await;
        let utilization = metrics.active_connections as f64 / metrics.total_connections as f64;

        if utilization > 0.9 && metrics.total_connections < self.config.max_connections {
            // Scale up
            let new_max = (metrics.total_connections as f64 * 1.5) as u32;
            let new_max = std::cmp::min(new_max, self.config.max_connections);

            info!(
                "Auto-scaling pool up: {} -> {}",
                metrics.total_connections, new_max
            );
            // Note: In a real implementation, you'd need to reconfigure the pool
            // This is a simplified version
        } else if utilization < 0.3 && metrics.total_connections > self.config.min_connections {
            // Scale down
            let new_max = (metrics.total_connections as f64 * 0.8) as u32;
            let new_max = std::cmp::max(new_max, self.config.min_connections);

            info!(
                "Auto-scaling pool down: {} -> {}",
                metrics.total_connections, new_max
            );
        }

        Ok(())
    }

    pub async fn get_metrics(&self) -> PoolMetrics {
        self.metrics.read().await.clone()
    }

    pub async fn get_stuck_connections(&self) -> Vec<StuckConnection> {
        self.stuck_connections.read().await.clone()
    }

    pub async fn force_cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Forcing connection pool cleanup");

        // Clear stuck connections
        let mut stuck_connections = self.stuck_connections.write().await;
        stuck_connections.clear();

        // Reset metrics
        let mut metrics = self.metrics.write().await;
        metrics.connection_acquire_errors = 0;
        metrics.connection_timeouts = 0;

        info!("Connection pool cleanup completed");
        Ok(())
    }

    pub async fn test_connection(&self) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();

        let mut conn = self.pool.acquire().await?;
        let acquire_time = start.elapsed();

        // Test the connection
        sqlx::query("SELECT 1").execute(&mut *conn).await?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.connection_acquire_time = acquire_time;

        Ok(())
    }
}

// Helper trait for pool monitoring
pub trait PoolMonitorTrait {
    fn get_monitor(&self) -> &ConnectionPoolMonitor;
    async fn health_check(&self) -> Result<PoolHealth, Box<dyn std::error::Error>>;
}

impl PoolMonitorTrait for ConnectionPoolMonitor {
    fn get_monitor(&self) -> &ConnectionPoolMonitor {
        self
    }

    async fn health_check(&self) -> Result<PoolHealth, Box<dyn std::error::Error>> {
        Ok(PoolHealth::new(self).await)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PoolHealth {
    pub is_healthy: bool,
    pub utilization: f64,
    pub active_connections: u32,
    pub total_connections: u32,
    pub stuck_connections: usize,
    pub last_check_timestamp: u64, // Use timestamp instead of Instant
}

impl PoolHealth {
    pub async fn new(monitor: &ConnectionPoolMonitor) -> Self {
        let metrics = monitor.metrics.read().await;
        let stuck_connections = monitor.stuck_connections.read().await;

        Self {
            is_healthy: metrics.active_connections < metrics.total_connections * 9 / 10,
            utilization: metrics.active_connections as f64 / metrics.total_connections as f64,
            active_connections: metrics.active_connections,
            total_connections: metrics.total_connections,
            stuck_connections: stuck_connections.len(),
            last_check_timestamp: metrics.last_health_check.elapsed().as_secs(),
        }
    }
}
