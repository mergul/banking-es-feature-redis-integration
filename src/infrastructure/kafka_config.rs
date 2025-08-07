use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub client_id: String,
    pub topic_prefix: String,
    pub auto_offset_reset: String,
    pub enable_auto_commit: bool,
    pub session_timeout_ms: i32,
    pub heartbeat_interval_ms: i32,
    pub max_poll_interval_ms: i32,
    pub max_poll_records: i32,
    pub retry_backoff_ms: i32,
    pub retry_count: i32,
    pub request_timeout_ms: i32,
    pub socket_timeout_ms: i32,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
    pub enabled: bool,
    pub producer_acks: i32,
    pub producer_retries: i32,
    pub consumer_max_poll_interval_ms: i32,
    pub consumer_session_timeout_ms: i32,
    pub cache_invalidation_topic: String,
    pub event_topic: String,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "banking-es-group".to_string(),
            client_id: "banking-es-client".to_string(),
            topic_prefix: "banking-es".to_string(),
            auto_offset_reset: "latest".to_string(),
            enable_auto_commit: true,
            session_timeout_ms: 10000,   // 10 saniye (Kafka minimum)
            heartbeat_interval_ms: 1000, // 1 saniye (session_timeout/10)
            max_poll_interval_ms: 10000, // 10 saniye
            max_poll_records: 5000,
            retry_backoff_ms: 100,
            retry_count: 3,
            request_timeout_ms: 30000,
            socket_timeout_ms: 30000,
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: "PLAIN".to_string(),
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            producer_acks: 1,
            producer_retries: 3,
            consumer_max_poll_interval_ms: 10000,
            consumer_session_timeout_ms: 10000,   // 10 saniye
            consumer_heartbeat_interval_ms: 1000, // 1 saniye
            consumer_max_poll_records: 5000,
            cache_invalidation_topic: "banking-es-cache-invalidation".to_string(),
            event_topic: "banking-es-events".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaProducerConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub acks: String,
    pub retries: i32,
    pub retry_backoff_ms: i32,
    pub request_timeout_ms: i32,
    pub socket_timeout_ms: i32,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            client_id: "banking-es-producer".to_string(),
            acks: "all".to_string(),
            retries: 3,
            retry_backoff_ms: 100,
            request_timeout_ms: 30000,
            socket_timeout_ms: 30000,
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConsumerConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub client_id: String,
    pub auto_offset_reset: String,
    pub enable_auto_commit: bool,
    pub session_timeout_ms: i32,
    pub heartbeat_interval_ms: i32,
    pub max_poll_interval_ms: i32,
    pub max_poll_records: i32,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "banking-es-group".to_string(),
            client_id: "banking-es-consumer".to_string(),
            auto_offset_reset: "latest".to_string(),
            enable_auto_commit: true,
            session_timeout_ms: 10000,   // 10 saniye (Kafka minimum)
            heartbeat_interval_ms: 1000, // 1 saniye (session_timeout/10)
            max_poll_interval_ms: 10000, // 10 saniye
            max_poll_records: 5000,
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTopicConfig {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub retention_ms: Option<i64>,
    pub retention_bytes: Option<i64>,
    pub cleanup_policy: Option<String>,
    pub min_insync_replicas: Option<i32>,
    pub max_message_bytes: Option<i32>,
}

impl Default for KafkaTopicConfig {
    fn default() -> Self {
        Self {
            name: "banking-es-topic".to_string(),
            partitions: 1,
            replication_factor: 1,
            retention_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days
            retention_bytes: None,
            cleanup_policy: Some("delete".to_string()),
            min_insync_replicas: Some(1),
            max_message_bytes: Some(1048576), // 1MB
        }
    }
}
