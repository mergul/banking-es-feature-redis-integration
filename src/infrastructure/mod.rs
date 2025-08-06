pub mod auth;
pub mod cache_service;
pub mod cdc_debezium; // Added CDC module
pub mod cdc_event_processor;
pub mod cdc_integration_helper;
pub mod cdc_producer;
pub mod cdc_service_manager;
pub mod config;
pub mod connection_pool_monitor;
pub mod connection_pool_partitioning; // Added connection pool partitioning
pub mod consistency_manager;
pub mod deadlock_detector;
pub mod dlq_router;
pub mod event_processor;
pub mod event_store;
pub mod init;
pub mod kafka_abstraction;
pub mod kafka_dlq;
pub mod kafka_event_processor;
pub mod kafka_metrics;
pub mod kafka_monitoring;
pub mod kafka_recovery;
pub mod kafka_recovery_strategies;
pub mod kafka_tracing;
pub mod l1_cache_updater;
pub mod lock_free_operations;
pub mod logging;
pub mod middleware;
pub mod outbox; // Added
pub mod outbox_cleanup_service; // Added new cleanup service
pub mod outbox_poller;
pub mod redis_aggregate_lock;
pub mod redis_lock_monitor;

pub mod projections;
pub mod rate_limiter;
pub mod redis_abstraction;
pub mod repository;
pub mod scaling;
pub mod sharding;
pub mod shutdown;
pub mod stuck_operation_diagnostic;
pub mod timeout_manager;
pub mod troubleshooting;
pub mod user_repository; // Added for OutboxPollingService
pub mod write_batching; // Added write batching

pub use auth::*;
pub use cache_service::*;
pub use cdc_debezium::*; // Added CDC exports
pub use config::*;
pub use connection_pool_partitioning::*; // Added connection pool partitioning exports
pub use event_store::{EventStore, EventStoreConfig};
pub use kafka_abstraction::KafkaConfig;
pub use kafka_dlq::*;
pub use kafka_event_processor::KafkaEventProcessor;
pub use kafka_metrics::*;
pub use kafka_monitoring::*;
pub use kafka_recovery::*;
pub use kafka_recovery_strategies::*;
pub use kafka_tracing::*;
pub use middleware::*;
pub use outbox::{
    OutboxMessage, OutboxRepositoryTrait, PersistedOutboxMessage, PostgresOutboxRepository,
}; // Added PostgresOutboxRepository
pub use outbox_cleanup_service::{
    CleanupConfig, CleanupHealthCheck, CleanupMetrics, OutboxCleaner, OutboxStats,
}; // Added new cleanup service exports
pub use outbox_poller::{OutboxPollerConfig, OutboxPollingService};

pub use projections::ProjectionStore;
pub use projections::*;
pub use rate_limiter::*;
pub use redis_abstraction::*;
pub use redis_abstraction::{RealRedisClient, RedisClientTrait};
pub use repository::*;
pub use repository::{AccountRepository, AccountRepositoryTrait, RepositoryError};
pub use scaling::*;
pub use sharding::*;
pub use user_repository::*;
pub use user_repository::{NewUser, User, UserRepository, UserRepositoryError}; // Added re-export // Added for OutboxPollingService
pub use write_batching::*; // Added write batching exports
