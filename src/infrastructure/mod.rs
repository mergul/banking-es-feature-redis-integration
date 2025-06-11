pub mod event_store;
pub mod kafka_abstraction;
pub mod kafka_event_processor;
pub mod projections;
//pub mod redis_abstraction;
pub mod kafka_dlq;
pub mod kafka_metrics;
pub mod kafka_monitoring;
pub mod kafka_recovery;
pub mod kafka_recovery_strategies;
pub mod kafka_tracing;
pub mod repository;

pub use event_store::{EventStore, EventStoreConfig};
pub use kafka_abstraction::KafkaConfig;
pub use kafka_event_processor::KafkaEventProcessor;
pub use projections::ProjectionStore;
//pub use redis_abstraction::{RealRedisClient, RedisClientTrait};

pub use repository::AccountRepository;
pub use repository::AccountRepositoryTrait;
pub use repository::RepositoryError;
