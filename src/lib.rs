pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod web;

// Remove deprecated AccountService export
// pub use application::AccountService;
pub use domain::AccountError;
pub use infrastructure::repository::AccountRepositoryTrait;
pub use infrastructure::{
    config::AppConfig, AccountRepository, EventStore, EventStoreConfig, KafkaConfig,
    KafkaEventProcessor, ProjectionStore,
};
