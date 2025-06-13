pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod web;

// Re-export commonly used types
pub use application::AccountService;
pub use domain::AccountError;
pub use infrastructure::repository::AccountRepositoryTrait;
pub use infrastructure::{
    AccountRepository, EventStore, EventStoreConfig, KafkaConfig, KafkaEventProcessor,
    ProjectionStore,
};
