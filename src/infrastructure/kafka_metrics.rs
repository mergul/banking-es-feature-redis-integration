use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct KafkaMetrics {
    // Producer metrics
    pub messages_sent: AtomicU64,
    pub send_errors: AtomicU64,
    pub send_latency: AtomicU64, // in milliseconds
    pub batch_size: AtomicU64,
    pub compression_ratio: AtomicU64, // percentage

    // Consumer metrics
    pub messages_consumed: AtomicU64,
    pub consume_errors: AtomicU64,
    pub consume_latency: AtomicU64, // in milliseconds
    pub consumer_lag: AtomicU64,
    pub rebalance_count: AtomicU64,

    // Event processing metrics
    pub events_processed: AtomicU64,
    pub processing_errors: AtomicU64,
    pub processing_latency: AtomicU64, // in milliseconds
    pub version_conflicts: AtomicU64,
    pub cache_updates: AtomicU64,
    pub cache_update_errors: AtomicU64,

    // DLQ metrics
    pub dlq_messages: AtomicU64,
    pub dlq_retries: AtomicU64,
    pub dlq_retry_success: AtomicU64,
    pub dlq_retry_failures: AtomicU64,

    // System metrics
    pub memory_usage: AtomicU64, // in bytes
    pub cpu_usage: AtomicU64,    // percentage
    pub thread_count: AtomicU64,
    pub connection_count: AtomicU64,
}

impl KafkaMetrics {
    pub fn record_send_latency(&self, duration: Duration) {
        self.send_latency
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_consume_latency(&self, duration: Duration) {
        self.consume_latency
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_processing_latency(&self, duration: Duration) {
        self.processing_latency
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn get_average_send_latency(&self) -> f64 {
        let total = self.send_latency.load(Ordering::Relaxed);
        let count = self.messages_sent.load(Ordering::Relaxed);
        if count > 0 {
            total as f64 / count as f64
        } else {
            0.0
        }
    }

    pub fn get_average_consume_latency(&self) -> f64 {
        let total = self.consume_latency.load(Ordering::Relaxed);
        let count = self.messages_consumed.load(Ordering::Relaxed);
        if count > 0 {
            total as f64 / count as f64
        } else {
            0.0
        }
    }

    pub fn get_average_processing_latency(&self) -> f64 {
        let total = self.processing_latency.load(Ordering::Relaxed);
        let count = self.events_processed.load(Ordering::Relaxed);
        if count > 0 {
            total as f64 / count as f64
        } else {
            0.0
        }
    }

    pub fn get_error_rate(&self) -> f64 {
        let errors = self.send_errors.load(Ordering::Relaxed)
            + self.consume_errors.load(Ordering::Relaxed)
            + self.processing_errors.load(Ordering::Relaxed);
        let total = self.messages_sent.load(Ordering::Relaxed)
            + self.messages_consumed.load(Ordering::Relaxed)
            + self.events_processed.load(Ordering::Relaxed);

        if total > 0 {
            errors as f64 / total as f64
        } else {
            0.0
        }
    }

    pub fn get_dlq_retry_success_rate(&self) -> f64 {
        let successes = self.dlq_retry_success.load(Ordering::Relaxed);
        let total = self.dlq_retries.load(Ordering::Relaxed);

        if total > 0 {
            successes as f64 / total as f64
        } else {
            0.0
        }
    }
}
