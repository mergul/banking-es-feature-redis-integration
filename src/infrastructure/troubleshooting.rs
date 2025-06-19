use crate::infrastructure::connection_pool_monitor::{ConnectionPoolMonitor, PoolHealth, PoolMonitorTrait};
use crate::infrastructure::deadlock_detector::{DeadlockDetector, DeadlockStats};
use crate::infrastructure::timeout_manager::{TimeoutManager, TimeoutStats};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub overall_status: HealthStatus,
    pub database_health: DatabaseHealth,
    pub cache_health: CacheHealth,
    pub kafka_health: KafkaHealth,
    pub redis_health: RedisHealth,
    pub connection_pool_health: PoolHealth,
    pub deadlock_stats: DeadlockStats,
    pub timeout_stats: TimeoutStats,
    pub stuck_operations: Vec<StuckOperation>,
    pub recommendations: Vec<Recommendation>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Critical,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseHealth {
    pub is_healthy: bool,
    pub connection_count: u32,
    pub active_queries: u32,
    pub idle_connections: u32,
    pub slow_queries: u32,
    pub deadlocks: u32,
    pub lock_wait_time: Duration,
    pub last_check_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheHealth {
    pub is_healthy: bool,
    pub hit_rate: f64,
    pub miss_rate: f64,
    pub eviction_rate: f64,
    pub memory_usage: u64,
    pub connection_count: u32,
    pub slow_operations: u32,
    pub last_check_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaHealth {
    pub is_healthy: bool,
    pub consumer_lag: u64,
    pub producer_throughput: f64,
    pub consumer_throughput: f64,
    pub error_rate: f64,
    pub partition_count: u32,
    pub topic_count: u32,
    pub last_check_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisHealth {
    pub is_healthy: bool,
    pub connection_count: u32,
    pub memory_usage: u64,
    pub command_count: u64,
    pub slow_commands: u32,
    pub error_rate: f64,
    pub last_check_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StuckOperation {
    pub operation_id: String,
    pub operation_type: String,
    pub start_time_timestamp: u64,
    pub duration: Duration,
    pub resource: String,
    pub stack_trace: Option<String>,
    pub priority: OperationPriority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationPriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    pub severity: RecommendationSeverity,
    pub title: String,
    pub description: String,
    pub action: String,
    pub impact: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendationSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Clone)]
pub struct Troubleshooter {
    deadlock_detector: Arc<DeadlockDetector>,
    timeout_manager: Arc<TimeoutManager>,
    connection_pool_monitor: Arc<ConnectionPoolMonitor>,
    health_history: Arc<tokio::sync::RwLock<Vec<SystemHealth>>>,
    config: TroubleshooterConfig,
}

#[derive(Debug, Clone)]
pub struct TroubleshooterConfig {
    pub health_check_interval: Duration,
    pub max_health_history: usize,
    pub critical_thresholds: CriticalThresholds,
    pub enable_auto_recovery: bool,
    pub enable_notifications: bool,
}

#[derive(Debug, Clone)]
pub struct CriticalThresholds {
    pub max_connection_utilization: f64,
    pub max_deadlock_count: u64,
    pub max_timeout_rate: f64,
    pub max_stuck_operations: usize,
    pub max_error_rate: f64,
}

impl Default for TroubleshooterConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(30),
            max_health_history: 100,
            critical_thresholds: CriticalThresholds {
                max_connection_utilization: 0.9,
                max_deadlock_count: 10,
                max_timeout_rate: 0.1,
                max_stuck_operations: 5,
                max_error_rate: 0.05,
            },
            enable_auto_recovery: true,
            enable_notifications: true,
        }
    }
}

impl Troubleshooter {
    pub fn new(
        deadlock_detector: Arc<DeadlockDetector>,
        timeout_manager: Arc<TimeoutManager>,
        connection_pool_monitor: Arc<ConnectionPoolMonitor>,
        config: TroubleshooterConfig,
    ) -> Self {
        let troubleshooter = Self {
            deadlock_detector,
            timeout_manager,
            connection_pool_monitor,
            health_history: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            config,
        };

        // Start background health monitoring
        let troubleshooter_clone = troubleshooter.clone();
        tokio::spawn(async move {
            troubleshooter_clone.monitor_health().await;
        });

        troubleshooter
    }

    pub async fn diagnose_system(&self) -> Result<SystemHealth> {
        let start_time = Instant::now();

        // Collect health data from all components
        let deadlock_stats = self.deadlock_detector.get_stats().await;
        let timeout_stats = self.timeout_manager.get_stats().await;
        let pool_health = PoolMonitorTrait::health_check(&*self.connection_pool_monitor).await
            .unwrap_or_else(|_| PoolHealth {
                is_healthy: false,
                utilization: 0.0,
                active_connections: 0,
                total_connections: 0,
                stuck_connections: 0,
                last_check_timestamp: 0,
            });

        // Analyze stuck operations
        let stuck_operations = self.analyze_stuck_operations().await;

        // Generate recommendations
        let recommendations = self.generate_recommendations(
            &deadlock_stats,
            &timeout_stats,
            &pool_health,
            &stuck_operations,
        ).await;

        // Determine overall health status
        let overall_status = self.determine_overall_health(
            &deadlock_stats,
            &timeout_stats,
            &pool_health,
            &stuck_operations,
        );

        let system_health = SystemHealth {
            overall_status,
            database_health: self.get_database_health().await,
            cache_health: self.get_cache_health().await,
            kafka_health: self.get_kafka_health().await,
            redis_health: self.get_redis_health().await,
            connection_pool_health: pool_health,
            deadlock_stats,
            timeout_stats,
            stuck_operations,
            recommendations,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Store in history
        let mut history = self.health_history.write().await;
        history.push(system_health.clone());
        if history.len() > self.config.max_health_history {
            history.remove(0);
        }

        Ok(system_health)
    }

    async fn analyze_stuck_operations(&self) -> Vec<StuckOperation> {
        let mut stuck_operations = Vec::new();

        // Get stuck operations from deadlock detector
        let deadlock_stats = self.deadlock_detector.get_stats().await;
        if deadlock_stats.active_operations > 0 {
            // This would need to be enhanced to get actual stuck operation details
            stuck_operations.push(StuckOperation {
                operation_id: "deadlock_detected".to_string(),
                operation_type: "unknown".to_string(),
                start_time_timestamp: 0,
                duration: Duration::from_secs(0),
                resource: "database".to_string(),
                stack_trace: None,
                priority: OperationPriority::High,
            });
        }

        // Get timeout information
        let timeout_stats = self.timeout_manager.get_stats().await;
        if timeout_stats.total_timeouts > 0 {
            stuck_operations.push(StuckOperation {
                operation_id: "timeout_detected".to_string(),
                operation_type: "timeout".to_string(),
                start_time_timestamp: 0,
                duration: Duration::from_secs(0),
                resource: "system".to_string(),
                stack_trace: None,
                priority: OperationPriority::Normal,
            });
        }

        stuck_operations
    }

    async fn generate_recommendations(
        &self,
        deadlock_stats: &DeadlockStats,
        timeout_stats: &TimeoutStats,
        pool_health: &PoolHealth,
        stuck_operations: &[StuckOperation],
    ) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        // Connection pool recommendations
        if pool_health.utilization > self.config.critical_thresholds.max_connection_utilization {
            recommendations.push(Recommendation {
                severity: RecommendationSeverity::Critical,
                title: "High Connection Pool Utilization".to_string(),
                description: format!("Connection pool utilization is {:.1}%", pool_health.utilization * 100.0),
                action: "Consider increasing max_connections or optimizing queries".to_string(),
                impact: "May cause connection timeouts and degraded performance".to_string(),
            });
        }

        // Deadlock recommendations
        if deadlock_stats.active_operations > self.config.critical_thresholds.max_deadlock_count as usize {
            recommendations.push(Recommendation {
                severity: RecommendationSeverity::Critical,
                title: "High Deadlock Count".to_string(),
                description: format!("{} active operations detected", deadlock_stats.active_operations),
                action: "Review transaction isolation levels and query patterns".to_string(),
                impact: "May cause data inconsistency and performance degradation".to_string(),
            });
        }

        // Timeout recommendations
        let total_operations = timeout_stats.total_timeouts + 1; // Avoid division by zero
        let timeout_rate = timeout_stats.total_timeouts as f64 / total_operations as f64;
        if timeout_rate > self.config.critical_thresholds.max_timeout_rate {
            recommendations.push(Recommendation {
                severity: RecommendationSeverity::Warning,
                title: "High Timeout Rate".to_string(),
                description: format!("{:.1}% of operations are timing out", timeout_rate * 100.0),
                action: "Increase timeout values or optimize slow operations".to_string(),
                impact: "May cause user experience degradation".to_string(),
            });
        }

        // Stuck operations recommendations
        if stuck_operations.len() > self.config.critical_thresholds.max_stuck_operations {
            recommendations.push(Recommendation {
                severity: RecommendationSeverity::Critical,
                title: "Multiple Stuck Operations".to_string(),
                description: format!("{} operations are stuck", stuck_operations.len()),
                action: "Investigate and potentially restart affected services".to_string(),
                impact: "May cause system-wide performance issues".to_string(),
            });
        }

        recommendations
    }

    fn determine_overall_health(
        &self,
        deadlock_stats: &DeadlockStats,
        timeout_stats: &TimeoutStats,
        pool_health: &PoolHealth,
        stuck_operations: &[StuckOperation],
    ) -> HealthStatus {
        let mut critical_issues = 0;
        let mut warnings = 0;

        // Check critical thresholds
        if pool_health.utilization > self.config.critical_thresholds.max_connection_utilization {
            critical_issues += 1;
        }
        if deadlock_stats.active_operations > self.config.critical_thresholds.max_deadlock_count as usize {
            critical_issues += 1;
        }
        if stuck_operations.len() > self.config.critical_thresholds.max_stuck_operations {
            critical_issues += 1;
        }

        // Check warning thresholds
        if timeout_stats.total_timeouts > 0 {
            warnings += 1;
        }
        if !pool_health.is_healthy {
            warnings += 1;
        }

        match (critical_issues, warnings) {
            (0, 0) => HealthStatus::Healthy,
            (0, _) => HealthStatus::Degraded,
            (1..=2, _) => HealthStatus::Critical,
            _ => HealthStatus::Unhealthy,
        }
    }

    async fn get_database_health(&self) -> DatabaseHealth {
        // This would integrate with actual database monitoring
        DatabaseHealth {
            is_healthy: true,
            connection_count: 0,
            active_queries: 0,
            idle_connections: 0,
            slow_queries: 0,
            deadlocks: 0,
            lock_wait_time: Duration::ZERO,
            last_check_timestamp: 0,
        }
    }

    async fn get_cache_health(&self) -> CacheHealth {
        // This would integrate with actual cache monitoring
        CacheHealth {
            is_healthy: true,
            hit_rate: 0.0,
            miss_rate: 0.0,
            eviction_rate: 0.0,
            memory_usage: 0,
            connection_count: 0,
            slow_operations: 0,
            last_check_timestamp: 0,
        }
    }

    async fn get_kafka_health(&self) -> KafkaHealth {
        // This would integrate with actual Kafka monitoring
        KafkaHealth {
            is_healthy: true,
            consumer_lag: 0,
            producer_throughput: 0.0,
            consumer_throughput: 0.0,
            error_rate: 0.0,
            partition_count: 0,
            topic_count: 0,
            last_check_timestamp: 0,
        }
    }

    async fn get_redis_health(&self) -> RedisHealth {
        // This would integrate with actual Redis monitoring
        RedisHealth {
            is_healthy: true,
            connection_count: 0,
            memory_usage: 0,
            command_count: 0,
            slow_commands: 0,
            error_rate: 0.0,
            last_check_timestamp: 0,
        }
    }

    async fn monitor_health(&self) {
        let mut interval = tokio::time::interval(self.config.health_check_interval);
        
        loop {
            interval.tick().await;
            
            match self.diagnose_system().await {
                Ok(health) => {
                    match health.overall_status {
                        HealthStatus::Healthy => {
                            info!("System health check: HEALTHY");
                        }
                        HealthStatus::Degraded => {
                            warn!("System health check: DEGRADED - {} recommendations", health.recommendations.len());
                        }
                        HealthStatus::Critical => {
                            error!("System health check: CRITICAL - {} critical issues", 
                                   health.recommendations.iter().filter(|r| matches!(r.severity, RecommendationSeverity::Critical)).count());
                        }
                        HealthStatus::Unhealthy => {
                            error!("System health check: UNHEALTHY - System requires immediate attention");
                        }
                    }

                    if self.config.enable_auto_recovery {
                        self.attempt_auto_recovery(&health).await;
                    }
                }
                Err(e) => {
                    error!("Failed to diagnose system health: {}", e);
                }
            }
        }
    }

    async fn attempt_auto_recovery(&self, health: &SystemHealth) {
        for recommendation in &health.recommendations {
            match recommendation.severity {
                RecommendationSeverity::Critical => {
                    warn!("Attempting auto-recovery for critical issue: {}", recommendation.title);
                    // Implement specific recovery actions based on the issue
                    self.execute_recovery_action(recommendation).await;
                }
                _ => {
                    // Log but don't auto-recover for non-critical issues
                    info!("Auto-recovery skipped for non-critical issue: {}", recommendation.title);
                }
            }
        }
    }

    async fn execute_recovery_action(&self, recommendation: &Recommendation) {
        match recommendation.title.as_str() {
            "High Connection Pool Utilization" => {
                warn!("Auto-recovery: Connection pool utilization is high - consider manual intervention");
            }
            "High Deadlock Count" => {
                warn!("Auto-recovery: Deadlock count is high - consider manual intervention");
            }
            "Multiple Stuck Operations" => {
                warn!("Auto-recovery: Multiple stuck operations detected - consider manual intervention");
            }
            _ => {
                info!("Auto-recovery: No specific action for {}", recommendation.title);
            }
        }
    }

    pub async fn get_health_history(&self) -> Vec<SystemHealth> {
        self.health_history.read().await.clone()
    }

    pub async fn clear_health_history(&self) {
        let mut history = self.health_history.write().await;
        history.clear();
    }
} 