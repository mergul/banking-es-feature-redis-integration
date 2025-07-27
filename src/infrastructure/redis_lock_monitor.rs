use crate::infrastructure::redis_abstraction::RealRedisClient;
use crate::infrastructure::redis_aggregate_lock::{
    RedisAggregateLock, RedisLockConfig, RedisLockMetrics,
};
use anyhow::Result;
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Configuration for Redis lock monitoring
#[derive(Debug, Clone)]
pub struct RedisLockMonitorConfig {
    pub monitoring_interval_secs: u64,
    pub alert_threshold_success_rate: f64,
    pub alert_threshold_avg_lock_time_ms: u64,
    pub alert_threshold_contention_rate: f64,
    pub alert_threshold_timeout_rate: f64,
    pub enable_alerts: bool,
    pub enable_metrics_logging: bool,
    pub enable_performance_tracking: bool,
}

impl Default for RedisLockMonitorConfig {
    fn default() -> Self {
        Self {
            monitoring_interval_secs: 30,
            alert_threshold_success_rate: 95.0, // Alert if success rate drops below 95%
            alert_threshold_avg_lock_time_ms: 100, // Alert if avg lock time exceeds 100ms
            alert_threshold_contention_rate: 10.0, // Alert if contention rate exceeds 10%
            alert_threshold_timeout_rate: 5.0,  // Alert if timeout rate exceeds 5%
            enable_alerts: true,
            enable_metrics_logging: true,
            enable_performance_tracking: true,
        }
    }
}

/// Performance tracking data
#[derive(Debug, Clone)]
pub struct LockPerformanceData {
    pub timestamp: Instant,
    pub success_rate: f64,
    pub avg_lock_time_us: u64,
    pub contention_rate: f64,
    pub timeout_rate: f64,
    pub total_operations: u64,
    pub lock_free_operations: u64,
    pub connection_pool_hit_rate: f64,
}

/// Alert types for Redis lock monitoring
#[derive(Debug, Clone)]
pub enum LockAlert {
    LowSuccessRate { current: f64, threshold: f64 },
    HighLockTime { current_ms: u64, threshold_ms: u64 },
    HighContentionRate { current: f64, threshold: f64 },
    HighTimeoutRate { current: f64, threshold: f64 },
    ConnectionPoolExhaustion { hit_rate: f64 },
    LockServiceUnhealthy { error: String },
}

/// Redis lock monitoring service
pub struct RedisLockMonitor {
    redis_lock: Arc<RedisAggregateLock>,
    config: RedisLockMonitorConfig,
    performance_history: Arc<Mutex<Vec<LockPerformanceData>>>,
    alert_history: Arc<Mutex<Vec<LockAlert>>>,
    last_check: Arc<Mutex<Instant>>,
    monitoring_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl RedisLockMonitor {
    pub fn new(redis_lock: Arc<RedisAggregateLock>, config: RedisLockMonitorConfig) -> Self {
        Self {
            redis_lock,
            config,
            performance_history: Arc::new(Mutex::new(Vec::new())),
            alert_history: Arc::new(Mutex::new(Vec::new())),
            last_check: Arc::new(Mutex::new(Instant::now())),
            monitoring_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the monitoring service
    pub async fn start(&self) -> Result<()> {
        let config = self.config.clone();
        let redis_lock = self.redis_lock.clone();
        let performance_history = self.performance_history.clone();
        let alert_history = self.alert_history.clone();
        let last_check = self.last_check.clone();

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(config.monitoring_interval_secs));

            loop {
                interval.tick().await;

                // Collect performance data
                let performance_data = Self::collect_performance_data(&redis_lock).await;

                // Store performance data
                {
                    let mut history = performance_history.lock().await;
                    history.push(performance_data.clone());

                    // Keep only last 1000 entries
                    if history.len() > 1000 {
                        history.remove(0);
                    }
                }

                // Check for alerts
                if config.enable_alerts {
                    let alerts = Self::check_alerts(&performance_data, &config);
                    for alert in alerts {
                        Self::handle_alert(&alert).await;

                        // Store alert
                        let mut alert_hist = alert_history.lock().await;
                        alert_hist.push(alert);

                        // Keep only last 100 alerts
                        if alert_hist.len() > 100 {
                            alert_hist.remove(0);
                        }
                    }
                }

                // Log metrics if enabled
                if config.enable_metrics_logging {
                    Self::log_metrics(&performance_data).await;
                }

                // Update last check time
                *last_check.lock().await = Instant::now();
            }
        });

        *self.monitoring_handle.lock().await = Some(handle);
        info!("🚀 Redis lock monitoring started");
        Ok(())
    }

    /// Stop the monitoring service
    pub async fn stop(&self) -> Result<()> {
        if let Some(handle) = self.monitoring_handle.lock().await.take() {
            handle.abort();
            info!("🛑 Redis lock monitoring stopped");
        }
        Ok(())
    }

    /// Get current performance metrics
    pub async fn get_current_metrics(&self) -> LockPerformanceData {
        Self::collect_performance_data(&self.redis_lock).await
    }

    /// Get performance history
    pub async fn get_performance_history(&self) -> Vec<LockPerformanceData> {
        self.performance_history.lock().await.clone()
    }

    /// Get alert history
    pub async fn get_alert_history(&self) -> Vec<LockAlert> {
        self.alert_history.lock().await.clone()
    }

    /// Get comprehensive monitoring report
    pub async fn get_monitoring_report(&self) -> Value {
        let current_metrics = self.get_current_metrics().await;
        let performance_history = self.get_performance_history().await;
        let alert_history = self.get_alert_history().await;
        let last_check = *self.last_check.lock().await;
        let redis_metrics = self.redis_lock.get_metrics_json();

        serde_json::json!({
            "current_metrics": {
                "success_rate": current_metrics.success_rate,
                "avg_lock_time_us": current_metrics.avg_lock_time_us,
                "contention_rate": current_metrics.contention_rate,
                "timeout_rate": current_metrics.timeout_rate,
                "total_operations": current_metrics.total_operations,
                "lock_free_operations": current_metrics.lock_free_operations,
                "connection_pool_hit_rate": current_metrics.connection_pool_hit_rate,
            },
            "performance_history_count": performance_history.len(),
            "alert_history_count": alert_history.len(),
            "last_check": last_check.elapsed().as_secs(),
            "redis_metrics": redis_metrics,
            "config": {
                "monitoring_interval_secs": self.config.monitoring_interval_secs,
                "alert_threshold_success_rate": self.config.alert_threshold_success_rate,
                "alert_threshold_avg_lock_time_ms": self.config.alert_threshold_avg_lock_time_ms,
                "alert_threshold_contention_rate": self.config.alert_threshold_contention_rate,
                "alert_threshold_timeout_rate": self.config.alert_threshold_timeout_rate,
            }
        })
    }

    /// Health check for the monitoring service
    pub async fn health_check(&self) -> Result<bool> {
        let last_check = *self.last_check.lock().await;
        let time_since_last_check = last_check.elapsed();

        // Check if monitoring is running (last check should be recent)
        if time_since_last_check > Duration::from_secs(self.config.monitoring_interval_secs * 2) {
            return Ok(false);
        }

        // Check if Redis lock service is healthy
        match self.redis_lock.health_check().await {
            Ok(healthy) => Ok(healthy),
            Err(_) => Ok(false),
        }
    }

    // Private helper methods

    async fn collect_performance_data(redis_lock: &Arc<RedisAggregateLock>) -> LockPerformanceData {
        let metrics = redis_lock.get_metrics();

        let total_operations = metrics.total_operations.load(Ordering::Relaxed);
        let locks_acquired = metrics.locks_acquired.load(Ordering::Relaxed);
        let locks_failed = metrics.locks_failed.load(Ordering::Relaxed);
        let lock_free_operations = metrics.lock_free_operations.load(Ordering::Relaxed);
        let lock_contention_count = metrics.lock_contention_count.load(Ordering::Relaxed);
        let lock_timeout_count = metrics.lock_timeout_count.load(Ordering::Relaxed);
        let connection_pool_hits = metrics.connection_pool_hits.load(Ordering::Relaxed);
        let connection_pool_misses = metrics.connection_pool_misses.load(Ordering::Relaxed);
        let avg_lock_time_us = metrics.avg_lock_acquisition_time.load(Ordering::Relaxed);

        let success_rate = if total_operations > 0 {
            (locks_acquired as f64 / total_operations as f64) * 100.0
        } else {
            100.0
        };

        let contention_rate = if total_operations > 0 {
            (lock_contention_count as f64 / total_operations as f64) * 100.0
        } else {
            0.0
        };

        let timeout_rate = if total_operations > 0 {
            (lock_timeout_count as f64 / total_operations as f64) * 100.0
        } else {
            0.0
        };

        let connection_pool_hit_rate = if connection_pool_hits + connection_pool_misses > 0 {
            (connection_pool_hits as f64 / (connection_pool_hits + connection_pool_misses) as f64)
                * 100.0
        } else {
            0.0
        };

        LockPerformanceData {
            timestamp: Instant::now(),
            success_rate,
            avg_lock_time_us,
            contention_rate,
            timeout_rate,
            total_operations,
            lock_free_operations,
            connection_pool_hit_rate,
        }
    }

    fn check_alerts(
        performance_data: &LockPerformanceData,
        config: &RedisLockMonitorConfig,
    ) -> Vec<LockAlert> {
        let mut alerts = Vec::new();

        // Check success rate
        if performance_data.success_rate < config.alert_threshold_success_rate {
            alerts.push(LockAlert::LowSuccessRate {
                current: performance_data.success_rate,
                threshold: config.alert_threshold_success_rate,
            });
        }

        // Check average lock time
        let avg_lock_time_ms = performance_data.avg_lock_time_us / 1000;
        if avg_lock_time_ms > config.alert_threshold_avg_lock_time_ms {
            alerts.push(LockAlert::HighLockTime {
                current_ms: avg_lock_time_ms,
                threshold_ms: config.alert_threshold_avg_lock_time_ms,
            });
        }

        // Check contention rate
        if performance_data.contention_rate > config.alert_threshold_contention_rate {
            alerts.push(LockAlert::HighContentionRate {
                current: performance_data.contention_rate,
                threshold: config.alert_threshold_contention_rate,
            });
        }

        // Check timeout rate
        if performance_data.timeout_rate > config.alert_threshold_timeout_rate {
            alerts.push(LockAlert::HighTimeoutRate {
                current: performance_data.timeout_rate,
                threshold: config.alert_threshold_timeout_rate,
            });
        }

        // Check connection pool hit rate
        if performance_data.connection_pool_hit_rate < 80.0 {
            alerts.push(LockAlert::ConnectionPoolExhaustion {
                hit_rate: performance_data.connection_pool_hit_rate,
            });
        }

        alerts
    }

    async fn handle_alert(alert: &LockAlert) {
        match alert {
            LockAlert::LowSuccessRate { current, threshold } => {
                error!(
                    "🚨 Redis Lock Alert: Low success rate - {}% (threshold: {}%)",
                    current, threshold
                );
            }
            LockAlert::HighLockTime {
                current_ms,
                threshold_ms,
            } => {
                warn!(
                    "⚠️ Redis Lock Alert: High lock acquisition time - {}ms (threshold: {}ms)",
                    current_ms, threshold_ms
                );
            }
            LockAlert::HighContentionRate { current, threshold } => {
                warn!(
                    "⚠️ Redis Lock Alert: High contention rate - {}% (threshold: {}%)",
                    current, threshold
                );
            }
            LockAlert::HighTimeoutRate { current, threshold } => {
                error!(
                    "🚨 Redis Lock Alert: High timeout rate - {}% (threshold: {}%)",
                    current, threshold
                );
            }
            LockAlert::ConnectionPoolExhaustion { hit_rate } => {
                warn!(
                    "⚠️ Redis Lock Alert: Connection pool exhaustion - hit rate: {}%",
                    hit_rate
                );
            }
            LockAlert::LockServiceUnhealthy { error } => {
                error!("🚨 Redis Lock Alert: Service unhealthy - {}", error);
            }
        }
    }

    async fn log_metrics(performance_data: &LockPerformanceData) {
        info!(
            "📊 Redis Lock Metrics - Success: {:.2}%, Avg Time: {}μs, Contention: {:.2}%, Timeout: {:.2}%, Operations: {}, Lock-Free: {}, Pool Hit: {:.2}%",
            performance_data.success_rate,
            performance_data.avg_lock_time_us,
            performance_data.contention_rate,
            performance_data.timeout_rate,
            performance_data.total_operations,
            performance_data.lock_free_operations,
            performance_data.connection_pool_hit_rate
        );
    }
}

#[cfg(test)]
#[ignore]
mod tests {
    use super::*;
    use crate::infrastructure::redis_abstraction::RealRedisClient;
    use redis::Client;

    #[tokio::test]
    #[ignore]
    async fn test_redis_lock_monitor_creation() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let redis_client = RealRedisClient::new(client, None);
        let redis_lock = Arc::new(RedisAggregateLock::new(
            redis_client,
            RedisLockConfig::default(),
        ));
        let config = RedisLockMonitorConfig::default();

        let monitor = RedisLockMonitor::new(redis_lock, config);
        assert!(monitor.health_check().await.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_performance_data_collection() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let redis_client = RealRedisClient::new(client, None);
        let redis_lock = Arc::new(RedisAggregateLock::new(
            redis_client,
            RedisLockConfig::default(),
        ));

        let performance_data = RedisLockMonitor::collect_performance_data(&redis_lock).await;

        assert!(performance_data.success_rate >= 0.0 && performance_data.success_rate <= 100.0);
        assert!(performance_data.contention_rate >= 0.0);
        assert!(performance_data.timeout_rate >= 0.0);
    }
}
