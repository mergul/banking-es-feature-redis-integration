use crate::infrastructure::kafka_metrics::KafkaMetrics;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

// Custom module for bincode-compatible DateTime<Utc> serialization
mod bincode_datetime {
    use chrono::{DateTime, Utc, TimeZone};
    use serde::{self, Serializer, Deserializer};
    use serde::de::Deserialize;

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_i64(dt.timestamp())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where D: Deserializer<'de> {
        let ts = i64::deserialize(deserializer)?;
        Ok(Utc.timestamp_opt(ts, 0).single().unwrap())
    }
}

#[async_trait]
pub trait MonitoringDashboardTrait: Send + Sync {
    fn record_metrics(&self);
    fn check_alerts(&self);
    fn update_health_status(&self);
}

#[derive(Debug, Clone)]
pub struct MonitoringDashboard {
    pub metrics: Arc<KafkaMetrics>,
    pub time_series_data: Vec<TimeSeriesPoint>,
    pub alerts: Vec<Alert>,
    pub health_status: HealthStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    #[serde(with = "bincode_datetime")]
    pub timestamp: DateTime<Utc>,
    pub metrics: MetricsSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub error_rate: f64,
    pub processing_latency: f64,
    pub consumer_lag: u64,
    pub memory_usage: u64,
    pub cpu_usage: u64,
    pub events_processed: u64,
    pub dlq_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub severity: AlertSeverity,
    pub message: String,
    #[serde(with = "bincode_datetime")]
    pub timestamp: DateTime<Utc>,
    pub metric_name: String,
    pub threshold: f64,
    pub current_value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: ServiceStatus,
    #[serde(with = "bincode_datetime")]
    pub last_check: DateTime<Utc>,
    pub components: Vec<ComponentHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: ServiceStatus,
    pub details: String,
}

impl MonitoringDashboard {
    pub fn new(metrics: Arc<KafkaMetrics>) -> Self {
        Self {
            metrics,
            time_series_data: Vec::new(),
            alerts: Vec::new(),
            health_status: HealthStatus {
                status: ServiceStatus::Healthy,
                last_check: Utc::now(),
                components: Vec::new(),
            },
        }
    }

    pub fn record_metrics(&mut self) {
        let snapshot = MetricsSnapshot {
            error_rate: self.metrics.get_error_rate(),
            processing_latency: self.metrics.get_average_processing_latency(),
            consumer_lag: self
                .metrics
                .consumer_lag
                .load(std::sync::atomic::Ordering::Relaxed),
            memory_usage: self
                .metrics
                .memory_usage
                .load(std::sync::atomic::Ordering::Relaxed),
            cpu_usage: self
                .metrics
                .cpu_usage
                .load(std::sync::atomic::Ordering::Relaxed),
            events_processed: self
                .metrics
                .events_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            dlq_size: self
                .metrics
                .dlq_messages
                .load(std::sync::atomic::Ordering::Relaxed),
        };

        self.time_series_data.push(TimeSeriesPoint {
            timestamp: Utc::now(),
            metrics: snapshot,
        });

        // Keep only last 24 hours of data
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        self.time_series_data
            .retain(|point| point.timestamp > cutoff);
    }

    pub fn check_alerts(&mut self) {
        let error_rate = self.metrics.get_error_rate();
        if error_rate > 0.1 {
            self.alerts.push(Alert {
                severity: AlertSeverity::Critical,
                message: "High error rate: {:.2}%".to_string() + &(error_rate * 100.0).to_string(),
                timestamp: Utc::now(),
                metric_name: "error_rate".to_string(),
                threshold: 0.1,
                current_value: error_rate,
            });
        }

        let consumer_lag = self
            .metrics
            .consumer_lag
            .load(std::sync::atomic::Ordering::Relaxed);
        if consumer_lag > 1000 {
            self.alerts.push(Alert {
                severity: AlertSeverity::Warning,
                message: "High consumer lag: {}".to_string() + &(consumer_lag).to_string(),
                timestamp: Utc::now(),
                metric_name: "consumer_lag".to_string(),
                threshold: 1000.0,
                current_value: consumer_lag as f64,
            });
        }

        let memory_usage = self
            .metrics
            .memory_usage
            .load(std::sync::atomic::Ordering::Relaxed);
        if memory_usage > 1_000_000_000 {
            // 1GB
            self.alerts.push(Alert {
                severity: AlertSeverity::Warning,
                message: "High memory usage: {:.2}GB".to_string() + &(memory_usage as f64 / 1e9).to_string(),
                timestamp: Utc::now(),
                metric_name: "memory_usage".to_string(),
                threshold: 1_000_000_000.0,
                current_value: memory_usage as f64,
            });
        }
    }

    pub fn update_health_status(&mut self) {
        let mut components = Vec::new();
        let mut overall_status = ServiceStatus::Healthy;

        // Check Kafka producer
        let producer_errors = self
            .metrics
            .send_errors
            .load(std::sync::atomic::Ordering::Relaxed);
        let producer_status = if producer_errors > 100 {
            overall_status = ServiceStatus::Degraded;
            ServiceStatus::Degraded
        } else {
            ServiceStatus::Healthy
        };
        components.push(ComponentHealth {
            name: "Kafka Producer".to_string(),
            status: producer_status,
            details: "Send errors: {}".to_string() + &(producer_errors).to_string(),
        });

        // Check Kafka consumer
        let consumer_errors = self
            .metrics
            .consume_errors
            .load(std::sync::atomic::Ordering::Relaxed);
        let consumer_status = if consumer_errors > 100 {
            overall_status = ServiceStatus::Degraded;
            ServiceStatus::Degraded
        } else {
            ServiceStatus::Healthy
        };
        components.push(ComponentHealth {
            name: "Kafka Consumer".to_string(),
            status: consumer_status,
            details: "Consume errors: {}".to_string() + &(consumer_errors).to_string(),
        });

        // Check DLQ
        let dlq_size = self
            .metrics
            .dlq_messages
            .load(std::sync::atomic::Ordering::Relaxed);
        let dlq_status = if dlq_size > 1000 {
            overall_status = ServiceStatus::Degraded;
            ServiceStatus::Degraded
        } else {
            ServiceStatus::Healthy
        };
        components.push(ComponentHealth {
            name: "Dead Letter Queue".to_string(),
            status: dlq_status,
            details: "DLQ size: {}".to_string() + &(dlq_size).to_string(),
        });

        self.health_status = HealthStatus {
            status: overall_status,
            last_check: Utc::now(),
            components,
        };
    }

    pub fn get_metrics_summary(&self) -> MetricsSummary {
        let latest = self.time_series_data.last().map(|p| &p.metrics);

        MetricsSummary {
            current_error_rate: latest.map(|m| m.error_rate).unwrap_or(0.0),
            current_processing_latency: latest.map(|m| m.processing_latency).unwrap_or(0.0),
            current_consumer_lag: latest.map(|m| m.consumer_lag).unwrap_or(0),
            current_memory_usage: latest.map(|m| m.memory_usage).unwrap_or(0),
            current_cpu_usage: latest.map(|m| m.cpu_usage).unwrap_or(0),
            total_events_processed: self
                .metrics
                .events_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            total_errors: self
                .metrics
                .processing_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            dlq_size: self
                .metrics
                .dlq_messages
                .load(std::sync::atomic::Ordering::Relaxed),
            active_alerts: self.alerts.len(),
            health_status: self.health_status.status.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub current_error_rate: f64,
    pub current_processing_latency: f64,
    pub current_consumer_lag: u64,
    pub current_memory_usage: u64,
    pub current_cpu_usage: u64,
    pub total_events_processed: u64,
    pub total_errors: u64,
    pub dlq_size: u64,
    pub active_alerts: usize,
    pub health_status: ServiceStatus,
}

impl MonitoringDashboardTrait for MonitoringDashboard {
    fn record_metrics(&self) {
        // Implementation
    }

    fn check_alerts(&self) {
        // Implementation
    }

    fn update_health_status(&self) {
        // Implementation
    }
}
