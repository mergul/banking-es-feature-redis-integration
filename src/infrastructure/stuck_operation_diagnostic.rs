use crate::infrastructure::connection_pool_monitor::{ConnectionPoolMonitor, PoolHealth, PoolMonitorTrait};
use crate::infrastructure::deadlock_detector::{DeadlockDetector, DeadlockStats};
use crate::infrastructure::timeout_manager::{TimeoutManager, TimeoutStats};
use crate::infrastructure::event_store::{EventStore, EventStoreTrait};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StuckOperationDiagnostic {
    pub timestamp: u64,
    pub overall_status: DiagnosticStatus,
    pub stuck_operations: Vec<StuckOperationDetail>,
    pub connection_pool_issues: Vec<ConnectionPoolIssue>,
    pub deadlock_issues: Vec<DeadlockIssue>,
    pub timeout_issues: Vec<TimeoutIssue>,
    pub recommendations: Vec<DiagnosticRecommendation>,
    pub recovery_actions: Vec<RecoveryAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiagnosticStatus {
    Healthy,
    Degraded,
    Critical,
    Emergency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StuckOperationDetail {
    pub operation_id: String,
    pub operation_type: String,
    pub start_time: u64,
    pub duration_seconds: u64,
    pub resource: String,
    pub stack_trace: Option<String>,
    pub priority: OperationPriority,
    pub impact_level: ImpactLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationPriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImpactLevel {
    Minimal,
    Moderate,
    High,
    Severe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolIssue {
    pub issue_type: PoolIssueType,
    pub description: String,
    pub severity: IssueSeverity,
    pub current_utilization: f64,
    pub active_connections: u32,
    pub total_connections: u32,
    pub stuck_connections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PoolIssueType {
    Exhaustion,
    ConnectionLeak,
    HighUtilization,
    StuckConnections,
    TimeoutIssues,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadlockIssue {
    pub issue_type: DeadlockIssueType,
    pub description: String,
    pub severity: IssueSeverity,
    pub active_operations: usize,
    pub locked_resources: usize,
    pub operation_types: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeadlockIssueType {
    HighActiveOperations,
    ResourceContention,
    LongRunningOperations,
    CircularDependencies,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutIssue {
    pub issue_type: TimeoutIssueType,
    pub description: String,
    pub severity: IssueSeverity,
    pub total_timeouts: u64,
    pub database_timeouts: u64,
    pub cache_timeouts: u64,
    pub kafka_timeouts: u64,
    pub redis_timeouts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeoutIssueType {
    HighTimeoutRate,
    DatabaseTimeouts,
    CacheTimeouts,
    KafkaTimeouts,
    RedisTimeouts,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IssueSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticRecommendation {
    pub severity: IssueSeverity,
    pub title: String,
    pub description: String,
    pub action: String,
    pub impact: String,
    pub priority: u8, // 1-10, higher is more urgent
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryAction {
    pub action_type: RecoveryActionType,
    pub description: String,
    pub can_auto_execute: bool,
    pub risk_level: RiskLevel,
    pub estimated_duration: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryActionType {
    RestartService,
    ClearConnectionPool,
    KillStuckOperations,
    IncreaseTimeouts,
    ScaleResources,
    ManualIntervention,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

pub struct StuckOperationDiagnostic {
    deadlock_detector: Arc<DeadlockDetector>,
    timeout_manager: Arc<TimeoutManager>,
    connection_pool_monitor: Arc<ConnectionPoolMonitor>,
    event_store: Arc<dyn EventStoreTrait + Send + Sync>,
    config: DiagnosticConfig,
}

#[derive(Debug, Clone)]
pub struct DiagnosticConfig {
    pub check_interval: Duration,
    pub critical_thresholds: CriticalThresholds,
    pub enable_auto_recovery: bool,
    pub enable_notifications: bool,
    pub max_stuck_operations: usize,
    pub max_connection_utilization: f64,
    pub max_timeout_rate: f64,
}

#[derive(Debug, Clone)]
pub struct CriticalThresholds {
    pub max_stuck_operations: usize,
    pub max_connection_utilization: f64,
    pub max_deadlock_count: u64,
    pub max_timeout_rate: f64,
    pub max_operation_duration: Duration,
}

impl Default for DiagnosticConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            critical_thresholds: CriticalThresholds {
                max_stuck_operations: 5,
                max_connection_utilization: 0.9,
                max_deadlock_count: 10,
                max_timeout_rate: 0.1,
                max_operation_duration: Duration::from_secs(60),
            },
            enable_auto_recovery: true,
            enable_notifications: true,
            max_stuck_operations: 10,
            max_connection_utilization: 0.8,
            max_timeout_rate: 0.05,
        }
    }
}

impl StuckOperationDiagnostic {
    pub fn new(
        deadlock_detector: Arc<DeadlockDetector>,
        timeout_manager: Arc<TimeoutManager>,
        connection_pool_monitor: Arc<ConnectionPoolMonitor>,
        event_store: Arc<dyn EventStoreTrait + Send + Sync>,
        config: DiagnosticConfig,
    ) -> Self {
        Self {
            deadlock_detector,
            timeout_manager,
            connection_pool_monitor,
            event_store,
            config,
        }
    }

    pub async fn run_diagnostic(&self) -> Result<StuckOperationDiagnostic> {
        let start_time = Instant::now();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Collect diagnostic data
        let stuck_operations = self.analyze_stuck_operations().await;
        let connection_pool_issues = self.analyze_connection_pool().await;
        let deadlock_issues = self.analyze_deadlocks().await;
        let timeout_issues = self.analyze_timeouts().await;

        // Generate recommendations
        let recommendations = self.generate_recommendations(
            &stuck_operations,
            &connection_pool_issues,
            &deadlock_issues,
            &timeout_issues,
        ).await;

        // Generate recovery actions
        let recovery_actions = self.generate_recovery_actions(
            &stuck_operations,
            &connection_pool_issues,
            &deadlock_issues,
            &timeout_issues,
        ).await;

        // Determine overall status
        let overall_status = self.determine_overall_status(
            &stuck_operations,
            &connection_pool_issues,
            &deadlock_issues,
            &timeout_issues,
        );

        let diagnostic = StuckOperationDiagnostic {
            timestamp,
            overall_status,
            stuck_operations,
            connection_pool_issues,
            deadlock_issues,
            timeout_issues,
            recommendations,
            recovery_actions,
        };

        // Log diagnostic results
        self.log_diagnostic_results(&diagnostic).await;

        // Auto-recovery if enabled
        if self.config.enable_auto_recovery {
            self.execute_auto_recovery(&diagnostic).await?;
        }

        Ok(diagnostic)
    }

    async fn analyze_stuck_operations(&self) -> Vec<StuckOperationDetail> {
        let mut stuck_operations = Vec::new();

        // Get active operations from deadlock detector
        let deadlock_stats = self.deadlock_detector.get_stats().await;
        
        // This would need to be enhanced to get actual operation details
        // For now, we'll create a placeholder for detected issues
        if deadlock_stats.active_operations > 0 {
            stuck_operations.push(StuckOperationDetail {
                operation_id: "deadlock_detected".to_string(),
                operation_type: "unknown".to_string(),
                start_time: 0,
                duration_seconds: 0,
                resource: "database".to_string(),
                stack_trace: None,
                priority: OperationPriority::High,
                impact_level: ImpactLevel::High,
            });
        }

        // Check for long-running database operations
        if let Ok(health) = self.event_store.health_check().await {
            if health.health_check_duration > self.config.critical_thresholds.max_operation_duration {
                stuck_operations.push(StuckOperationDetail {
                    operation_id: "health_check_timeout".to_string(),
                    operation_type: "health_check".to_string(),
                    start_time: timestamp,
                    duration_seconds: health.health_check_duration.as_secs(),
                    resource: "event_store".to_string(),
                    stack_trace: None,
                    priority: OperationPriority::Critical,
                    impact_level: ImpactLevel::Severe,
                });
            }
        }

        stuck_operations
    }

    async fn analyze_connection_pool(&self) -> Vec<ConnectionPoolIssue> {
        let mut issues = Vec::new();

        let pool_health = PoolMonitorTrait::health_check(&*self.connection_pool_monitor).await
            .unwrap_or_else(|_| PoolHealth {
                is_healthy: false,
                utilization: 0.0,
                active_connections: 0,
                total_connections: 0,
                stuck_connections: 0,
                last_check_timestamp: 0,
            });

        // Check for high utilization
        if pool_health.utilization > self.config.critical_thresholds.max_connection_utilization {
            issues.push(ConnectionPoolIssue {
                issue_type: PoolIssueType::HighUtilization,
                description: format!("Connection pool utilization is {:.1}%", pool_health.utilization * 100.0),
                severity: IssueSeverity::Critical,
                current_utilization: pool_health.utilization,
                active_connections: pool_health.active_connections,
                total_connections: pool_health.total_connections,
                stuck_connections: pool_health.stuck_connections,
            });
        }

        // Check for stuck connections
        if pool_health.stuck_connections > 0 {
            issues.push(ConnectionPoolIssue {
                issue_type: PoolIssueType::StuckConnections,
                description: format!("{} stuck connections detected", pool_health.stuck_connections),
                severity: IssueSeverity::Critical,
                current_utilization: pool_health.utilization,
                active_connections: pool_health.active_connections,
                total_connections: pool_health.total_connections,
                stuck_connections: pool_health.stuck_connections,
            });
        }

        // Check for pool exhaustion
        if pool_health.utilization > 0.95 {
            issues.push(ConnectionPoolIssue {
                issue_type: PoolIssueType::Exhaustion,
                description: "Connection pool is nearly exhausted".to_string(),
                severity: IssueSeverity::Emergency,
                current_utilization: pool_health.utilization,
                active_connections: pool_health.active_connections,
                total_connections: pool_health.total_connections,
                stuck_connections: pool_health.stuck_connections,
            });
        }

        issues
    }

    async fn analyze_deadlocks(&self) -> Vec<DeadlockIssue> {
        let mut issues = Vec::new();

        let deadlock_stats = self.deadlock_detector.get_stats().await;

        // Check for high active operations
        if deadlock_stats.active_operations > self.config.critical_thresholds.max_deadlock_count as usize {
            issues.push(DeadlockIssue {
                issue_type: DeadlockIssueType::HighActiveOperations,
                description: format!("{} active operations detected", deadlock_stats.active_operations),
                severity: IssueSeverity::Critical,
                active_operations: deadlock_stats.active_operations,
                locked_resources: deadlock_stats.locked_resources,
                operation_types: deadlock_stats.operation_types,
            });
        }

        // Check for resource contention
        if deadlock_stats.locked_resources > 0 {
            issues.push(DeadlockIssue {
                issue_type: DeadlockIssueType::ResourceContention,
                description: format!("{} resources are locked", deadlock_stats.locked_resources),
                severity: IssueSeverity::Warning,
                active_operations: deadlock_stats.active_operations,
                locked_resources: deadlock_stats.locked_resources,
                operation_types: deadlock_stats.operation_types,
            });
        }

        issues
    }

    async fn analyze_timeouts(&self) -> Vec<TimeoutIssue> {
        let mut issues = Vec::new();

        let timeout_stats = self.timeout_manager.get_stats().await;

        // Check for high timeout rate
        let total_operations = timeout_stats.total_timeouts + 1; // Avoid division by zero
        let timeout_rate = timeout_stats.total_timeouts as f64 / total_operations as f64;
        
        if timeout_rate > self.config.critical_thresholds.max_timeout_rate {
            issues.push(TimeoutIssue {
                issue_type: TimeoutIssueType::HighTimeoutRate,
                description: format!("{:.1}% of operations are timing out", timeout_rate * 100.0),
                severity: IssueSeverity::Critical,
                total_timeouts: timeout_stats.total_timeouts,
                database_timeouts: timeout_stats.database_timeouts,
                cache_timeouts: timeout_stats.cache_timeouts,
                kafka_timeouts: timeout_stats.kafka_timeouts,
                redis_timeouts: timeout_stats.redis_timeouts,
            });
        }

        // Check for specific timeout types
        if timeout_stats.database_timeouts > 0 {
            issues.push(TimeoutIssue {
                issue_type: TimeoutIssueType::DatabaseTimeouts,
                description: format!("{} database timeouts detected", timeout_stats.database_timeouts),
                severity: IssueSeverity::Warning,
                total_timeouts: timeout_stats.total_timeouts,
                database_timeouts: timeout_stats.database_timeouts,
                cache_timeouts: timeout_stats.cache_timeouts,
                kafka_timeouts: timeout_stats.kafka_timeouts,
                redis_timeouts: timeout_stats.redis_timeouts,
            });
        }

        if timeout_stats.cache_timeouts > 0 {
            issues.push(TimeoutIssue {
                issue_type: TimeoutIssueType::CacheTimeouts,
                description: format!("{} cache timeouts detected", timeout_stats.cache_timeouts),
                severity: IssueSeverity::Warning,
                total_timeouts: timeout_stats.total_timeouts,
                database_timeouts: timeout_stats.database_timeouts,
                cache_timeouts: timeout_stats.cache_timeouts,
                kafka_timeouts: timeout_stats.kafka_timeouts,
                redis_timeouts: timeout_stats.redis_timeouts,
            });
        }

        issues
    }

    async fn generate_recommendations(
        &self,
        stuck_operations: &[StuckOperationDetail],
        connection_pool_issues: &[ConnectionPoolIssue],
        deadlock_issues: &[DeadlockIssue],
        timeout_issues: &[TimeoutIssue],
    ) -> Vec<DiagnosticRecommendation> {
        let mut recommendations = Vec::new();

        // Connection pool recommendations
        for issue in connection_pool_issues {
            match issue.issue_type {
                PoolIssueType::Exhaustion => {
                    recommendations.push(DiagnosticRecommendation {
                        severity: IssueSeverity::Emergency,
                        title: "Connection Pool Exhaustion".to_string(),
                        description: issue.description.clone(),
                        action: "Immediately increase max_connections or restart the service".to_string(),
                        impact: "System-wide performance degradation and potential outages".to_string(),
                        priority: 10,
                    });
                }
                PoolIssueType::HighUtilization => {
                    recommendations.push(DiagnosticRecommendation {
                        severity: IssueSeverity::Critical,
                        title: "High Connection Pool Utilization".to_string(),
                        description: issue.description.clone(),
                        action: "Consider increasing max_connections or optimizing queries".to_string(),
                        impact: "May cause connection timeouts and degraded performance".to_string(),
                        priority: 8,
                    });
                }
                PoolIssueType::StuckConnections => {
                    recommendations.push(DiagnosticRecommendation {
                        severity: IssueSeverity::Critical,
                        title: "Stuck Connections Detected".to_string(),
                        description: issue.description.clone(),
                        action: "Force cleanup of stuck connections and investigate root cause".to_string(),
                        impact: "Connection pool exhaustion and degraded performance".to_string(),
                        priority: 9,
                    });
                }
                _ => {}
            }
        }

        // Deadlock recommendations
        for issue in deadlock_issues {
            match issue.issue_type {
                DeadlockIssueType::HighActiveOperations => {
                    recommendations.push(DiagnosticRecommendation {
                        severity: IssueSeverity::Critical,
                        title: "High Active Operations Count".to_string(),
                        description: issue.description.clone(),
                        action: "Review transaction isolation levels and query patterns".to_string(),
                        impact: "May cause data inconsistency and performance degradation".to_string(),
                        priority: 8,
                    });
                }
                DeadlockIssueType::ResourceContention => {
                    recommendations.push(DiagnosticRecommendation {
                        severity: IssueSeverity::Warning,
                        title: "Resource Contention Detected".to_string(),
                        description: issue.description.clone(),
                        action: "Review resource locking patterns and consider reducing concurrency".to_string(),
                        impact: "Potential deadlocks and performance issues".to_string(),
                        priority: 6,
                    });
                }
                _ => {}
            }
        }

        // Timeout recommendations
        for issue in timeout_issues {
            match issue.issue_type {
                TimeoutIssueType::HighTimeoutRate => {
                    recommendations.push(DiagnosticRecommendation {
                        severity: IssueSeverity::Critical,
                        title: "High Timeout Rate".to_string(),
                        description: issue.description.clone(),
                        action: "Increase timeout values or optimize slow operations".to_string(),
                        impact: "User experience degradation and potential data loss".to_string(),
                        priority: 7,
                    });
                }
                TimeoutIssueType::DatabaseTimeouts => {
                    recommendations.push(DiagnosticRecommendation {
                        severity: IssueSeverity::Warning,
                        title: "Database Timeouts".to_string(),
                        description: issue.description.clone(),
                        action: "Optimize database queries and consider increasing database timeout".to_string(),
                        impact: "Slow database operations and potential connection issues".to_string(),
                        priority: 6,
                    });
                }
                _ => {}
            }
        }

        // Stuck operations recommendations
        if !stuck_operations.is_empty() {
            recommendations.push(DiagnosticRecommendation {
                severity: IssueSeverity::Critical,
                title: "Stuck Operations Detected".to_string(),
                description: format!("{} operations are stuck", stuck_operations.len()),
                action: "Investigate and potentially restart affected services".to_string(),
                impact: "System-wide performance issues and potential data inconsistency".to_string(),
                priority: 9,
            });
        }

        recommendations.sort_by(|a, b| b.priority.cmp(&a.priority));
        recommendations
    }

    async fn generate_recovery_actions(
        &self,
        stuck_operations: &[StuckOperationDetail],
        connection_pool_issues: &[ConnectionPoolIssue],
        deadlock_issues: &[DeadlockIssue],
        timeout_issues: &[TimeoutIssue],
    ) -> Vec<RecoveryAction> {
        let mut actions = Vec::new();

        // Connection pool recovery actions
        for issue in connection_pool_issues {
            match issue.issue_type {
                PoolIssueType::Exhaustion => {
                    actions.push(RecoveryAction {
                        action_type: RecoveryActionType::RestartService,
                        description: "Restart service to clear connection pool".to_string(),
                        can_auto_execute: false,
                        risk_level: RiskLevel::High,
                        estimated_duration: Duration::from_secs(30),
                    });
                }
                PoolIssueType::StuckConnections => {
                    actions.push(RecoveryAction {
                        action_type: RecoveryActionType::ClearConnectionPool,
                        description: "Force cleanup of stuck connections".to_string(),
                        can_auto_execute: true,
                        risk_level: RiskLevel::Medium,
                        estimated_duration: Duration::from_secs(5),
                    });
                }
                _ => {}
            }
        }

        // Deadlock recovery actions
        for issue in deadlock_issues {
            if issue.active_operations > 0 {
                actions.push(RecoveryAction {
                    action_type: RecoveryActionType::KillStuckOperations,
                    description: format!("Kill {} stuck operations", issue.active_operations),
                    can_auto_execute: true,
                    risk_level: RiskLevel::High,
                    estimated_duration: Duration::from_secs(10),
                });
            }
        }

        // Timeout recovery actions
        for issue in timeout_issues {
            if issue.total_timeouts > 0 {
                actions.push(RecoveryAction {
                    action_type: RecoveryActionType::IncreaseTimeouts,
                    description: "Increase operation timeout values".to_string(),
                    can_auto_execute: false,
                    risk_level: RiskLevel::Low,
                    estimated_duration: Duration::from_secs(60),
                });
            }
        }

        // General recovery actions
        if !stuck_operations.is_empty() {
            actions.push(RecoveryAction {
                action_type: RecoveryActionType::ManualIntervention,
                description: "Manual intervention required for stuck operations".to_string(),
                can_auto_execute: false,
                risk_level: RiskLevel::Critical,
                estimated_duration: Duration::from_secs(300),
            });
        }

        actions
    }

    fn determine_overall_status(
        &self,
        stuck_operations: &[StuckOperationDetail],
        connection_pool_issues: &[ConnectionPoolIssue],
        deadlock_issues: &[DeadlockIssue],
        timeout_issues: &[TimeoutIssue],
    ) -> DiagnosticStatus {
        let mut critical_issues = 0;
        let mut emergency_issues = 0;

        // Count critical and emergency issues
        for issue in connection_pool_issues {
            match issue.severity {
                IssueSeverity::Critical => critical_issues += 1,
                IssueSeverity::Emergency => emergency_issues += 1,
                _ => {}
            }
        }

        for issue in deadlock_issues {
            match issue.severity {
                IssueSeverity::Critical => critical_issues += 1,
                IssueSeverity::Emergency => emergency_issues += 1,
                _ => {}
            }
        }

        for issue in timeout_issues {
            match issue.severity {
                IssueSeverity::Critical => critical_issues += 1,
                IssueSeverity::Emergency => emergency_issues += 1,
                _ => {}
            }
        }

        if !stuck_operations.is_empty() {
            critical_issues += 1;
        }

        match (emergency_issues, critical_issues) {
            (0, 0) => DiagnosticStatus::Healthy,
            (0, 1..=2) => DiagnosticStatus::Degraded,
            (0, _) => DiagnosticStatus::Critical,
            _ => DiagnosticStatus::Emergency,
        }
    }

    async fn log_diagnostic_results(&self, diagnostic: &StuckOperationDiagnostic) {
        match diagnostic.overall_status {
            DiagnosticStatus::Healthy => {
                info!("System diagnostic: HEALTHY - No issues detected");
            }
            DiagnosticStatus::Degraded => {
                warn!("System diagnostic: DEGRADED - {} issues detected", 
                      diagnostic.recommendations.len());
            }
            DiagnosticStatus::Critical => {
                error!("System diagnostic: CRITICAL - {} critical issues", 
                       diagnostic.recommendations.iter().filter(|r| matches!(r.severity, IssueSeverity::Critical)).count());
            }
            DiagnosticStatus::Emergency => {
                error!("System diagnostic: EMERGENCY - Immediate attention required");
            }
        }

        // Log specific issues
        for operation in &diagnostic.stuck_operations {
            error!("Stuck operation: {} (type: {}) running for {}s", 
                   operation.operation_id, operation.operation_type, operation.duration_seconds);
        }

        for issue in &diagnostic.connection_pool_issues {
            error!("Connection pool issue: {} - {}", issue.issue_type, issue.description);
        }

        for issue in &diagnostic.deadlock_issues {
            error!("Deadlock issue: {} - {}", issue.issue_type, issue.description);
        }

        for issue in &diagnostic.timeout_issues {
            error!("Timeout issue: {} - {}", issue.issue_type, issue.description);
        }
    }

    async fn execute_auto_recovery(&self, diagnostic: &StuckOperationDiagnostic) -> Result<()> {
        for action in &diagnostic.recovery_actions {
            if action.can_auto_execute {
                match action.action_type {
                    RecoveryActionType::ClearConnectionPool => {
                        info!("Auto-executing: {}", action.description);
                        if let Err(e) = self.connection_pool_monitor.force_cleanup().await {
                            error!("Failed to clear connection pool: {}", e);
                        }
                    }
                    RecoveryActionType::KillStuckOperations => {
                        info!("Auto-executing: {}", action.description);
                        // This would need to be implemented in the deadlock detector
                        warn!("Kill stuck operations not yet implemented");
                    }
                    _ => {
                        info!("Skipping auto-execution for: {}", action.description);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get_diagnostic_summary(&self) -> Result<String> {
        let diagnostic = self.run_diagnostic().await?;
        
        let mut summary = format!(
            "=== System Diagnostic Summary ===\n\
             Status: {:?}\n\
             Timestamp: {}\n\
             Stuck Operations: {}\n\
             Connection Pool Issues: {}\n\
             Deadlock Issues: {}\n\
             Timeout Issues: {}\n\
             Recommendations: {}\n\
             Recovery Actions: {}\n",
            diagnostic.overall_status,
            diagnostic.timestamp,
            diagnostic.stuck_operations.len(),
            diagnostic.connection_pool_issues.len(),
            diagnostic.deadlock_issues.len(),
            diagnostic.timeout_issues.len(),
            diagnostic.recommendations.len(),
            diagnostic.recovery_actions.len(),
        );

        if !diagnostic.recommendations.is_empty() {
            summary.push_str("\n=== Top Recommendations ===\n");
            for (i, rec) in diagnostic.recommendations.iter().take(5).enumerate() {
                summary.push_str(&format!(
                    "{}. [{}] {} - {}\n",
                    i + 1, rec.severity, rec.title, rec.description
                ));
            }
        }

        if !diagnostic.recovery_actions.is_empty() {
            summary.push_str("\n=== Recovery Actions ===\n");
            for (i, action) in diagnostic.recovery_actions.iter().take(3).enumerate() {
                summary.push_str(&format!(
                    "{}. [{}] {} - {}\n",
                    i + 1, action.risk_level, action.action_type, action.description
                ));
            }
        }

        Ok(summary)
    }
} 