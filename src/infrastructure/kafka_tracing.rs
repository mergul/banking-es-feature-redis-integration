use crate::domain::{Account, AccountEvent};
use crate::infrastructure::kafka_metrics::KafkaMetrics;
use anyhow::Result;
use async_trait::async_trait;
use opentelemetry::KeyValue;
use opentelemetry_sdk::trace::{self, IdGenerator, RandomIdGenerator, Sampler};
use opentelemetry_sdk::Resource;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use uuid::Uuid;

#[async_trait]
pub trait KafkaTracingTrait: Send + Sync {
    fn init_tracing(&self) -> Result<()>;
    fn trace_event_processing(&self, account_id: Uuid, events: &[AccountEvent], version: i64);
    fn trace_error(&self, error: &anyhow::Error, context: &str);
    fn trace_metrics(&self);
    fn trace_performance_metrics(&self);
    fn trace_recovery_operation(&self, strategy: &str, account_id: Option<Uuid>, status: &str);
}

#[derive(Clone)]
pub struct KafkaTracing {
    metrics: Arc<KafkaMetrics>,
}

impl KafkaTracing {
    pub fn new(metrics: Arc<KafkaMetrics>) -> Self {
        Self { metrics }
    }

    pub fn init_tracing(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Configure OpenTelemetry
        let tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_service_name("banking-es-kafka")
            .with_endpoint("localhost:6831")
            .with_trace_config(
                opentelemetry_sdk::trace::config()
                    .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
                    .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                    .with_resource(opentelemetry_sdk::Resource::new(vec![
                        opentelemetry::KeyValue::new("service.name", "banking-es-kafka"),
                        opentelemetry::KeyValue::new("deployment.environment", "production"),
                    ])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        // Create OpenTelemetry layer
        let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Configure logging
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,banking_es=debug"));

        // Initialize subscriber
        tracing_subscriber::registry()
            .with(env_filter)
            .with(opentelemetry_layer)
            .init();

        Ok(())
    }

    pub fn trace_event_processing(&self, account_id: Uuid, events: &[AccountEvent], version: i64) {
        let span = tracing::info_span!(
            "process_events",
            account_id = %account_id,
            event_count = events.len(),
            version = version
        );

        let _guard = span.enter();
        let _ = std::io::stderr().write_all(
            ("Processing events for account ".to_string() + &account_id.to_string() + "\n")
                .as_bytes(),
        );

        for event in events {
            match event {
                AccountEvent::AccountCreated { .. } => {
                    let _ = std::io::stderr().write_all(b"Processing AccountCreated event\n");
                }
                AccountEvent::MoneyDeposited { amount, .. } => {
                    let _ = std::io::stderr().write_all(
                        ("Processing MoneyDeposited event: ".to_string()
                            + &amount.to_string()
                            + "\n")
                            .as_bytes(),
                    );
                }
                AccountEvent::MoneyWithdrawn { amount, .. } => {
                    let _ = std::io::stderr().write_all(
                        ("Processing MoneyWithdrawn event: ".to_string()
                            + &amount.to_string()
                            + "\n")
                            .as_bytes(),
                    );
                }
                AccountEvent::AccountClosed { reason, .. } => {
                    let _ = std::io::stderr().write_all(
                        ("Processing AccountClosed event: ".to_string() + reason + "\n").as_bytes(),
                    );
                }
            }
        }
    }

    pub fn trace_dlq_operation(&self, account_id: Uuid, operation: &str, retry_count: u32) {
        let span = tracing::info_span!(
            "dlq_operation",
            account_id = %account_id,
            operation = operation,
            retry_count = retry_count
        );

        let _guard = span.enter();
        let _ = std::io::stderr().write_all(
            ("DLQ operation '".to_string()
                + operation
                + "' for account "
                + &account_id.to_string()
                + " (retry "
                + &retry_count.to_string()
                + ")\n")
                .as_bytes(),
        );
    }

    pub fn trace_recovery_operation(&self, strategy: &str, account_id: Option<Uuid>, status: &str) {
        let span = tracing::info_span!(
            "recovery_operation",
            strategy = strategy,
            account_id = ?account_id,
            status = status
        );

        let _guard = span.enter();
        let account_str = account_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "None".to_string());
        let _ = std::io::stderr().write_all(
            ("Recovery operation '".to_string()
                + strategy
                + "' for account "
                + &account_str
                + ": "
                + status
                + "\n")
                .as_bytes(),
        );
    }

    pub fn trace_metrics(&self) {
        let span = tracing::info_span!("metrics_snapshot");
        let _guard = span.enter();

        let error_rate = self.metrics.get_error_rate();
        let processing_latency = self.metrics.get_average_processing_latency();
        let consumer_lag = self
            .metrics
            .consumer_lag
            .load(std::sync::atomic::Ordering::Relaxed);

        let _ = std::io::stderr().write_all(
            ("Metrics snapshot: error_rate=".to_string()
                + &(error_rate * 100.0).to_string()
                + "%, processing_latency="
                + &processing_latency.to_string()
                + "ms, consumer_lag="
                + &consumer_lag.to_string()
                + "\n")
                .as_bytes(),
        );

        if error_rate > 0.1 {
            let _ = std::io::stderr().write_all(
                ("High error rate detected: ".to_string()
                    + &(error_rate * 100.0).to_string()
                    + "%\n")
                    .as_bytes(),
            );
        }

        if consumer_lag > 1000 {
            let _ = std::io::stderr().write_all(
                ("High consumer lag detected: ".to_string() + &consumer_lag.to_string() + "\n")
                    .as_bytes(),
            );
        }
    }

    pub fn trace_performance_metrics(&self) {
        let span = tracing::info_span!("performance_metrics");
        let _guard = span.enter();

        let memory_usage = self
            .metrics
            .memory_usage
            .load(std::sync::atomic::Ordering::Relaxed);
        let cpu_usage = self
            .metrics
            .cpu_usage
            .load(std::sync::atomic::Ordering::Relaxed);
        let thread_count = self
            .metrics
            .thread_count
            .load(std::sync::atomic::Ordering::Relaxed);

        let _ = std::io::stderr().write_all(
            ("Performance metrics: memory=".to_string()
                + &(memory_usage as f64 / 1_000_000.0).to_string()
                + "MB, cpu="
                + &(cpu_usage as f64 / 100.0).to_string()
                + "%, threads="
                + &thread_count.to_string()
                + "\n")
                .as_bytes(),
        );

        if memory_usage > 1_000_000_000 {
            let _ = std::io::stderr().write_all(
                ("High memory usage: ".to_string()
                    + &(memory_usage as f64 / 1e9).to_string()
                    + "GB\n")
                    .as_bytes(),
            );
        }

        if cpu_usage > 80 {
            let _ = std::io::stderr().write_all(
                ("High CPU usage: ".to_string() + &(cpu_usage as f64 / 100.0).to_string() + "%\n")
                    .as_bytes(),
            );
        }
    }

    pub fn trace_error(&self, error: &anyhow::Error, context: &str) {
        let span = tracing::error_span!(
            "error",
            context = context,
            error = %error
        );

        let _guard = span.enter();
        let _ = std::io::stderr().write_all(
            ("Error in ".to_string() + context + ": " + &error.to_string() + "\n").as_bytes(),
        );
    }
}

impl Drop for KafkaTracing {
    fn drop(&mut self) {
        // Shutdown OpenTelemetry tracer
        opentelemetry::global::shutdown_tracer_provider();
    }
}

impl KafkaTracingTrait for KafkaTracing {
    fn init_tracing(&self) -> Result<()> {
        // Implementation
        Ok(())
    }

    fn trace_event_processing(&self, account_id: Uuid, events: &[AccountEvent], version: i64) {
        // Implementation
    }

    fn trace_error(&self, error: &anyhow::Error, context: &str) {
        // Implementation
    }

    fn trace_metrics(&self) {
        // Implementation
    }

    fn trace_performance_metrics(&self) {
        // Implementation
    }

    fn trace_recovery_operation(&self, strategy: &str, account_id: Option<Uuid>, status: &str) {
        // Implementation
    }
}
