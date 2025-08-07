// Remove all AccountService references - only use CQRS services
// use crate::application::services::AccountService; // Commented out

use crate::application::services::CQRSAccountService;
use crate::infrastructure::auth::{AuthConfig, AuthService};
use crate::infrastructure::cache_service::{CacheConfig, CacheService, EvictionPolicy};
use crate::infrastructure::connection_pool_monitor::{ConnectionPoolMonitor, PoolMonitorConfig};
use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use crate::infrastructure::deadlock_detector::DeadlockDetector;
use crate::infrastructure::event_store::{EventStore, EventStoreConfig, EventStoreTrait};
use crate::infrastructure::kafka_abstraction::KafkaConfig;
use crate::infrastructure::kafka_event_processor::KafkaEventProcessor;
use crate::infrastructure::l1_cache_updater::L1CacheUpdater;
use crate::infrastructure::projections::{ProjectionConfig, ProjectionStore, ProjectionStoreTrait};
use crate::infrastructure::redis_abstraction::{RealRedisClient, RedisClient, RedisPoolConfig};
use crate::infrastructure::repository::{AccountRepository, AccountRepositoryTrait};
use crate::infrastructure::scaling::{ScalingConfig, ScalingManager};
use crate::infrastructure::sharding::{ShardConfig, ShardManager};
use crate::infrastructure::user_repository::UserRepository;
use anyhow::Result;
use redis::Client;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

/// Service context containing all initialized services
pub struct ServiceContext {
    pub event_store: Arc<dyn EventStoreTrait + Send + Sync>,
    pub projection_store: Arc<dyn ProjectionStoreTrait + Send + Sync>,
    pub cache_service:
        Arc<dyn crate::infrastructure::cache_service::CacheServiceTrait + Send + Sync>,
    pub auth_service: Arc<AuthService>,
    pub user_repository: Arc<UserRepository>,
    pub account_repository: Arc<dyn AccountRepositoryTrait + Send + Sync>,
    pub shard_manager: Arc<ShardManager>,
    pub scaling_manager: Arc<ScalingManager>,
}

impl ServiceContext {
    pub async fn shutdown(self) {
        info!("Shutting down ServiceContext...");
        // Add any cleanup logic here
    }
}

pub async fn init_all_services(
    consistency_manager: Option<
        Arc<crate::infrastructure::consistency_manager::ConsistencyManager>,
    >,
    partitioned_pools: Arc<crate::infrastructure::connection_pool_partitioning::PartitionedPools>,
) -> Result<ServiceContext> {
    info!("Initializing services...");

    // Initialize timeout manager
    let timeout_config = crate::infrastructure::timeout_manager::TimeoutConfig {
        database_operation_timeout: Duration::from_secs(
            std::env::var("DB_OPERATION_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        cache_operation_timeout: Duration::from_secs(
            std::env::var("CACHE_OPERATION_TIMEOUT")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
        ),
        kafka_operation_timeout: Duration::from_secs(
            std::env::var("KAFKA_OPERATION_TIMEOUT")
                .unwrap_or_else(|_| "15".to_string())
                .parse()
                .unwrap_or(15),
        ),
        redis_operation_timeout: Duration::from_secs(
            std::env::var("REDIS_OPERATION_TIMEOUT")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        ),
        batch_processing_timeout: Duration::from_secs(
            std::env::var("BATCH_PROCESSING_TIMEOUT")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60),
        ),
        health_check_timeout: Duration::from_secs(
            std::env::var("HEALTH_CHECK_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        connection_acquire_timeout: Duration::from_secs(
            std::env::var("CONNECTION_ACQUIRE_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        transaction_timeout: Duration::from_secs(
            std::env::var("TRANSACTION_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        lock_timeout: Duration::from_secs(
            std::env::var("LOCK_TIMEOUT")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        ),
        retry_timeout: Duration::from_secs(
            std::env::var("RETRY_TIMEOUT")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        ),
    };
    let timeout_manager = Arc::new(crate::infrastructure::timeout_manager::TimeoutManager::new(
        timeout_config,
    ));

    // Initialize deadlock detector
    let deadlock_config = crate::infrastructure::deadlock_detector::DeadlockConfig {
        check_interval: Duration::from_secs(
            std::env::var("DEADLOCK_CHECK_INTERVAL")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        ),
        operation_timeout: Duration::from_secs(
            std::env::var("DEADLOCK_OPERATION_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        max_concurrent_operations: std::env::var("MAX_CONCURRENT_OPERATIONS")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000),
        enable_auto_resolution: std::env::var("ENABLE_AUTO_RESOLUTION")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        log_suspicious_operations: std::env::var("LOG_SUSPICIOUS_OPERATIONS")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
    };
    let deadlock_detector =
        Arc::new(crate::infrastructure::deadlock_detector::DeadlockDetector::new(deadlock_config));

    // Initialize Redis client Singleton with connection pool and multiplexing
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let redis_client = Arc::new(redis::Client::open(redis_url)?);

    // Configure Redis connection pool with multiplexing
    let redis_pool_config = crate::infrastructure::redis_abstraction::RedisPoolConfig {
        min_connections: std::env::var("REDIS_MIN_CONNECTIONS")
            .unwrap_or_else(|_| "20".to_string())
            .parse()
            .unwrap_or(20),
        max_connections: std::env::var("REDIS_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "200".to_string())
            .parse()
            .unwrap_or(200),
        connection_timeout: Duration::from_secs(
            std::env::var("REDIS_CONNECTION_TIMEOUT")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        ),
        idle_timeout: Duration::from_secs(
            std::env::var("REDIS_IDLE_TIMEOUT")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
        ),
    };

    let redis_client_trait =
        RealRedisClient::new(redis_client.as_ref().clone(), Some(redis_pool_config));

    // Initialize EventStore with the provided partitioned pools
    let event_store_config = EventStoreConfig {
        database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
        }),
        max_connections: std::env::var("DB_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "800".to_string())
            .parse()
            .unwrap_or(800),
        min_connections: std::env::var("DB_MIN_CONNECTIONS")
            .unwrap_or_else(|_| "400".to_string())
            .parse()
            .unwrap_or(400),
        acquire_timeout_secs: std::env::var("DB_ACQUIRE_TIMEOUT")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(10),
        idle_timeout_secs: std::env::var("DB_IDLE_TIMEOUT")
            .unwrap_or_else(|_| "300".to_string())
            .parse()
            .unwrap_or(300),
        max_lifetime_secs: std::env::var("DB_MAX_LIFETIME")
            .unwrap_or_else(|_| "900".to_string())
            .parse()
            .unwrap_or(900),
        batch_size: std::env::var("DB_BATCH_SIZE")
            .unwrap_or_else(|_| "2000".to_string())
            .parse()
            .unwrap_or(2000),
        batch_timeout_ms: std::env::var("DB_BATCH_TIMEOUT_MS")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(10),
        max_batch_queue_size: std::env::var("DB_MAX_BATCH_QUEUE_SIZE")
            .unwrap_or_else(|_| "20000".to_string())
            .parse()
            .unwrap_or(20000),
        batch_processor_count: std::env::var("DB_BATCH_PROCESSOR_COUNT")
            .unwrap_or_else(|_| "32".to_string())
            .parse()
            .unwrap_or(32),
        snapshot_threshold: std::env::var("DB_SNAPSHOT_THRESHOLD")
            .unwrap_or_else(|_| "2000".to_string())
            .parse()
            .unwrap_or(2000),
        snapshot_interval_secs: std::env::var("DB_SNAPSHOT_INTERVAL")
            .unwrap_or_else(|_| "600".to_string())
            .parse()
            .unwrap_or(600),
        snapshot_cache_ttl_secs: std::env::var("DB_SNAPSHOT_CACHE_TTL")
            .unwrap_or_else(|_| "3600".to_string())
            .parse()
            .unwrap_or(3600),
        max_snapshots_per_run: std::env::var("DB_MAX_SNAPSHOTS_PER_RUN")
            .unwrap_or_else(|_| "50".to_string())
            .parse()
            .unwrap_or(50),
        synchronous_commit: true,
        full_page_writes: true,
    };

    let event_store: Arc<dyn EventStoreTrait + Send + Sync> =
        Arc::new(EventStore::new_with_config_and_pools(
            partitioned_pools.clone(),
            event_store_config,
            (*deadlock_detector).clone(),
            crate::infrastructure::connection_pool_monitor::ConnectionPoolMonitor::new(
                partitioned_pools.select_pool(OperationType::Write).clone(),
                crate::infrastructure::connection_pool_monitor::PoolMonitorConfig::default(),
            ),
        ));

    // Initialize connection pool monitor
    let pool_monitor_config = crate::infrastructure::connection_pool_monitor::PoolMonitorConfig {
        health_check_interval: Duration::from_secs(
            std::env::var("POOL_HEALTH_CHECK_INTERVAL")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        ),
        connection_timeout: Duration::from_secs(
            std::env::var("POOL_CONNECTION_TIMEOUT")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
        ),
        max_connection_wait_time: Duration::from_secs(
            std::env::var("POOL_MAX_WAIT_TIME")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .unwrap_or(2),
        ),
        pool_exhaustion_threshold: std::env::var("POOL_EXHAUSTION_THRESHOLD")
            .unwrap_or_else(|_| "0.9".to_string())
            .parse()
            .unwrap_or(0.9),
        enable_auto_scaling: std::env::var("POOL_AUTO_SCALING")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        max_connections: std::env::var("POOL_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "500".to_string())
            .parse()
            .unwrap_or(500),
        min_connections: std::env::var("POOL_MIN_CONNECTIONS")
            .unwrap_or_else(|_| "250".to_string())
            .parse()
            .unwrap_or(250),
    };
    let connection_pool_monitor = Arc::new(
        crate::infrastructure::connection_pool_monitor::ConnectionPoolMonitor::new(
            event_store.get_pool().clone(),
            pool_monitor_config,
        ),
    );

    // Initialize UserRepository
    let user_repository = Arc::new(UserRepository::new(
        event_store.get_partitioned_pools().clone(),
    ));

    // Initialize AuthService with config from environment
    let auth_config = AuthConfig {
        jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".to_string()),
        refresh_token_secret: std::env::var("REFRESH_TOKEN_SECRET")
            .unwrap_or_else(|_| "default_refresh_secret".to_string()),
        access_token_expiry: std::env::var("ACCESS_TOKEN_EXPIRY")
            .unwrap_or_else(|_| "3600".to_string())
            .parse()
            .unwrap_or(3600),
        refresh_token_expiry: std::env::var("REFRESH_TOKEN_EXPIRY")
            .unwrap_or_else(|_| "604800".to_string())
            .parse()
            .unwrap_or(604800),
        rate_limit_requests: std::env::var("RATE_LIMIT_REQUESTS")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000),
        rate_limit_window: std::env::var("RATE_LIMIT_WINDOW")
            .unwrap_or_else(|_| "60".to_string())
            .parse()
            .unwrap_or(60),
        max_failed_attempts: std::env::var("MAX_FAILED_ATTEMPTS")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .unwrap_or(5),
        lockout_duration_minutes: std::env::var("LOCKOUT_DURATION_MINUTES")
            .unwrap_or_else(|_| "30".to_string())
            .parse()
            .unwrap_or(30),
    };

    let auth_service = Arc::new(AuthService::new(
        redis_client.clone(),
        auth_config,
        user_repository.clone(),
    ));

    // Initialize ProjectionStore with optimized config for high throughput
    let mut projection_config = ProjectionConfig {
        cache_ttl_secs: std::env::var("PROJECTION_CACHE_TTL")
            .unwrap_or_else(|_| "600".to_string())
            .parse()
            .unwrap_or(600),
        batch_size: std::env::var("PROJECTION_BATCH_SIZE")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000),
        batch_timeout_ms: std::env::var("PROJECTION_BATCH_TIMEOUT_MS")
            .unwrap_or_else(|_| "25".to_string())
            .parse()
            .unwrap_or(25),
        max_connections: std::env::var("PROJECTION_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "500".to_string())
            .parse()
            .unwrap_or(500),
        min_connections: std::env::var("PROJECTION_MIN_CONNECTIONS")
            .unwrap_or_else(|_| "250".to_string())
            .parse()
            .unwrap_or(250),
        acquire_timeout_secs: std::env::var("PROJECTION_ACQUIRE_TIMEOUT")
            .unwrap_or_else(|_| "30".to_string())
            .parse()
            .unwrap_or(30),
        idle_timeout_secs: std::env::var("PROJECTION_IDLE_TIMEOUT")
            .unwrap_or_else(|_| "600".to_string())
            .parse()
            .unwrap_or(600),
        max_lifetime_secs: std::env::var("PROJECTION_MAX_LIFETIME")
            .unwrap_or_else(|_| "1800".to_string())
            .parse()
            .unwrap_or(1800),
        synchronous_commit: true,
        full_page_writes: true,
    };

    let projection_store: Arc<dyn ProjectionStoreTrait + Send + Sync> = Arc::new(
        ProjectionStore::from_pool_with_config(event_store.get_pool().clone(), projection_config),
    );

    // Initialize CacheService with optimized config for high throughput
    let cache_config = CacheConfig {
        default_ttl: Duration::from_secs(
            std::env::var("CACHE_DEFAULT_TTL")
                .unwrap_or_else(|_| "3600".to_string())
                .parse()
                .unwrap_or(3600),
        ),
        max_size: std::env::var("CACHE_MAX_SIZE")
            .unwrap_or_else(|_| "50000".to_string())
            .parse()
            .unwrap_or(50000),
        shard_count: std::env::var("CACHE_SHARD_COUNT")
            .unwrap_or_else(|_| "16".to_string())
            .parse()
            .unwrap_or(16),
        warmup_batch_size: std::env::var("CACHE_WARMUP_BATCH_SIZE")
            .unwrap_or_else(|_| "200".to_string())
            .parse()
            .unwrap_or(200),
        warmup_interval: Duration::from_secs(
            std::env::var("CACHE_WARMUP_INTERVAL")
                .unwrap_or_else(|_| "15".to_string())
                .parse()
                .unwrap_or(15),
        ),
        eviction_policy: EvictionPolicy::LRU,
    };

    let cache_service: Arc<
        dyn crate::infrastructure::cache_service::CacheServiceTrait + Send + Sync,
    > = Arc::new(CacheService::new(redis_client_trait.clone(), cache_config));

    // Initialize KafkaConfig
    let kafka_config = KafkaConfig {
        enabled: std::env::var("KAFKA_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        bootstrap_servers: std::env::var("KAFKA_BOOTSTRAP_SERVERS")
            .unwrap_or_else(|_| "localhost:9092".to_string()),
        group_id: std::env::var("KAFKA_GROUP_ID")
            .unwrap_or_else(|_| "banking-es-group".to_string()),
        topic_prefix: std::env::var("KAFKA_TOPIC_PREFIX")
            .unwrap_or_else(|_| "banking-es".to_string()),
        producer_acks: std::env::var("KAFKA_PRODUCER_ACKS")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        producer_retries: std::env::var("KAFKA_PRODUCER_RETRIES")
            .unwrap_or_else(|_| "3".to_string())
            .parse()
            .unwrap_or(3),
        consumer_max_poll_interval_ms: std::env::var("KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS")
            .unwrap_or_else(|_| "10000".to_string())
            .parse()
            .unwrap_or(10000),
        consumer_session_timeout_ms: std::env::var("KAFKA_CONSUMER_SESSION_TIMEOUT_MS")
            .unwrap_or_else(|_| "10000".to_string())
            .parse()
            .unwrap_or(10000),
        consumer_heartbeat_interval_ms: std::env::var("KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000),
        consumer_max_poll_records: std::env::var("KAFKA_CONSUMER_MAX_POLL_RECORDS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000),
        fetch_max_bytes: std::env::var("KAFKA_FETCH_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(5 * 1024 * 1024),
        security_protocol: std::env::var("KAFKA_SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "PLAINTEXT".to_string()),
        sasl_mechanism: std::env::var("KAFKA_SASL_MECHANISM")
            .unwrap_or_else(|_| "PLAIN".to_string()),
        ssl_ca_location: std::env::var("KAFKA_SSL_CA_LOCATION").ok(),
        auto_offset_reset: std::env::var("KAFKA_AUTO_OFFSET_RESET")
            .unwrap_or_else(|_| "earliest".to_string()),
        cache_invalidation_topic: std::env::var("KAFKA_CACHE_INVALIDATION_TOPIC")
            .unwrap_or_else(|_| "banking-es-cache-invalidation".to_string()),
        event_topic: std::env::var("KAFKA_EVENT_TOPIC")
            .unwrap_or_else(|_| "banking-es-events".to_string()),
    };

    // Initialize AccountRepository with Kafka producer
    let account_repository: Arc<dyn AccountRepositoryTrait + Send + Sync> = {
        match AccountRepository::with_kafka_producer(event_store.clone(), kafka_config.clone()) {
            Ok(repo) => Arc::new(repo),
            Err(e) => {
                error!("Failed to create repository with Kafka producer: {}", e);
                // Fallback to repository without Kafka producer
                Arc::new(AccountRepository::new(event_store.clone()))
            }
        }
    };

    // Initialize RequestMiddleware with optimized config
    let rate_limit_config = crate::infrastructure::middleware::RateLimitConfig {
        requests_per_minute: std::env::var("RATE_LIMIT_REQUESTS_PER_MINUTE")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100),
        burst_size: std::env::var("RATE_LIMIT_BURST_SIZE")
            .unwrap_or_else(|_| "20".to_string())
            .parse()
            .unwrap_or(20),
        window_size: Duration::from_secs(
            std::env::var("RATE_LIMIT_WINDOW_SIZE")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60),
        ),
        max_clients: std::env::var("RATE_LIMIT_MAX_CLIENTS")
            .unwrap_or_else(|_| "500".to_string())
            .parse()
            .unwrap_or(500),
    };

    let middleware = Arc::new(crate::infrastructure::middleware::RequestMiddleware::new(
        rate_limit_config,
    ));
    middleware.register_validator(
        "create_account".to_string(),
        Box::new(crate::infrastructure::middleware::AccountCreationValidator),
    );
    middleware.register_validator(
        "deposit_money".to_string(),
        Box::new(crate::infrastructure::middleware::TransactionValidator),
    );
    middleware.register_validator(
        "withdraw_money".to_string(),
        Box::new(crate::infrastructure::middleware::TransactionValidator),
    );

    // Initialize ScalingManager with optimized config
    let scaling_config = ScalingConfig {
        min_instances: std::env::var("SCALING_MIN_INSTANCES")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        max_instances: std::env::var("SCALING_MAX_INSTANCES")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .unwrap_or(5),
        scale_up_threshold: std::env::var("SCALING_UP_THRESHOLD")
            .unwrap_or_else(|_| "0.8".to_string())
            .parse()
            .unwrap_or(0.8),
        scale_down_threshold: std::env::var("SCALING_DOWN_THRESHOLD")
            .unwrap_or_else(|_| "0.2".to_string())
            .parse()
            .unwrap_or(0.2),
        cooldown_period: Duration::from_secs(
            std::env::var("SCALING_COOLDOWN_PERIOD")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
        ),
        health_check_interval: Duration::from_secs(
            std::env::var("SCALING_HEALTH_CHECK_INTERVAL")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        instance_timeout: Duration::from_secs(
            std::env::var("SCALING_INSTANCE_TIMEOUT")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60),
        ),
    };

    let scaling_manager = Arc::new(ScalingManager::new(
        redis_client_trait.clone(),
        scaling_config,
    ));

    // Initialize ShardManager with proper config
    let shard_config = ShardConfig {
        shard_count: std::env::var("SHARD_COUNT")
            .unwrap_or_else(|_| "16".to_string())
            .parse()
            .unwrap_or(16),
        rebalance_threshold: std::env::var("SHARD_REBALANCE_THRESHOLD")
            .unwrap_or_else(|_| "0.2".to_string())
            .parse()
            .unwrap_or(0.2),
        rebalance_interval: Duration::from_secs(
            std::env::var("SHARD_REBALANCE_INTERVAL")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
        ),
        lock_timeout: Duration::from_secs(
            std::env::var("SHARD_LOCK_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        lock_retry_interval: Duration::from_millis(
            std::env::var("SHARD_LOCK_RETRY_INTERVAL")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
        ),
        max_retries: std::env::var("SHARD_MAX_RETRIES")
            .unwrap_or_else(|_| "3".to_string())
            .parse()
            .unwrap_or(3),
    };
    let shard_manager = Arc::new(ShardManager::new(redis_client_trait.clone(), shard_config));

    // Initialize L1CacheUpdater
    let l1_cache_updater = Arc::new(L1CacheUpdater::new(
        kafka_config.clone(),
        cache_service.clone(),
    )?);

    // Initialize KafkaEventProcessor
    // let retry_config = crate::infrastructure::kafka_event_processor::RetryConfig::default(); // Or load from env
    // let kafka_processor = Arc::new(KafkaEventProcessor::new(
    //     kafka_config,
    //     &event_store,
    //     &projection_store,
    //     &cache_service,
    //     retry_config,
    // )?);

    // Start L1 cache updater in background
    let l1_updater = l1_cache_updater.clone();
    let l1_handle = tokio::spawn(async move {
        if let Err(_) = l1_updater.start().await {
            error!("L1 cache updater error");
        }
    });

    // Start Kafka event processor in background
    // let kafka_processor_for_start = kafka_processor.clone();
    // let kafka_handle = tokio::spawn(async move {
    //     if let Err(_) = kafka_processor_for_start.start_processing().await {
    //         error!("Kafka event processor error");
    //     }
    // });

    info!("All services initialized successfully");

    // Create ServiceContext with only essential services
    let service_context = ServiceContext {
        event_store,
        projection_store,
        cache_service,
        auth_service,
        user_repository: user_repository.clone(),
        account_repository,
        shard_manager,
        scaling_manager,
    };

    Ok(service_context)
}

pub async fn init_all_services_with_pool(
    consistency_manager: Option<
        Arc<crate::infrastructure::consistency_manager::ConsistencyManager>,
    >,
    partitioned_pools: Arc<PartitionedPools>,
) -> anyhow::Result<ServiceContext> {
    use crate::infrastructure::auth::{AuthConfig, AuthService};
    use crate::infrastructure::cache_service::{
        CacheConfig, CacheService, CacheServiceTrait, EvictionPolicy,
    };
    use crate::infrastructure::event_store::EventStore;
    use crate::infrastructure::kafka_abstraction::KafkaConfig;
    use crate::infrastructure::projections::{
        ProjectionConfig, ProjectionStore, ProjectionStoreTrait,
    };
    use crate::infrastructure::redis_abstraction::{RealRedisClient, RedisPoolConfig};
    use crate::infrastructure::repository::{AccountRepository, AccountRepositoryTrait};
    use crate::infrastructure::scaling::{ScalingConfig, ScalingManager};
    use crate::infrastructure::sharding::{ShardConfig, ShardManager};
    use crate::infrastructure::user_repository::UserRepository;
    use std::sync::Arc;
    use std::time::Duration;

    // Use the provided partitioned pools for all DB-backed services
    let event_store: Arc<dyn EventStoreTrait + Send + Sync> =
        Arc::new(EventStore::new_with_config_and_pools(
            partitioned_pools.clone(),
            EventStoreConfig::default(),
            DeadlockDetector::new(
                crate::infrastructure::deadlock_detector::DeadlockConfig::default(),
            ),
            ConnectionPoolMonitor::new(
                partitioned_pools.select_pool(OperationType::Write).clone(),
                PoolMonitorConfig::default(),
            ),
        ));
    let mut projection_config = ProjectionConfig::default();

    let projection_store: Arc<dyn ProjectionStoreTrait + Send + Sync> = Arc::new(
        ProjectionStore::from_pools_with_config(partitioned_pools.clone(), projection_config),
    );

    // Redis setup
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let redis_client = Arc::new(redis::Client::open(redis_url)?);
    let redis_pool_config = RedisPoolConfig {
        min_connections: 20,
        max_connections: 200,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(300),
    };
    let redis_client_trait =
        RealRedisClient::new(redis_client.as_ref().clone(), Some(redis_pool_config));

    // CacheService
    let cache_config = CacheConfig::default();
    let cache_service: Arc<dyn CacheServiceTrait + Send + Sync> =
        Arc::new(CacheService::new(redis_client_trait.clone(), cache_config));

    // UserRepository
    let user_repository = Arc::new(UserRepository::new(
        event_store.get_partitioned_pools().clone(),
    ));

    // AuthService
    let auth_config = AuthConfig {
        jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".to_string()),
        refresh_token_secret: std::env::var("REFRESH_TOKEN_SECRET")
            .unwrap_or_else(|_| "default_refresh_secret".to_string()),
        access_token_expiry: 3600,
        refresh_token_expiry: 604800,
        rate_limit_requests: 1000,
        rate_limit_window: 60,
        max_failed_attempts: 5,
        lockout_duration_minutes: 30,
    };
    let auth_service = Arc::new(AuthService::new(
        redis_client.clone(),
        auth_config,
        user_repository.clone(),
    ));

    // AccountRepository
    let kafka_config = KafkaConfig::default();
    let account_repository: Arc<dyn AccountRepositoryTrait + Send + Sync> =
        match AccountRepository::with_kafka_producer(event_store.clone(), kafka_config.clone()) {
            Ok(repo) => Arc::new(repo),
            Err(_) => Arc::new(AccountRepository::new(event_store.clone())),
        };

    // ShardManager
    let shard_config = ShardConfig {
        shard_count: 16,
        rebalance_threshold: 0.2,
        rebalance_interval: Duration::from_secs(300),
        lock_timeout: Duration::from_secs(30),
        lock_retry_interval: Duration::from_millis(100),
        max_retries: 3,
    };
    let shard_manager = Arc::new(ShardManager::new(redis_client_trait.clone(), shard_config));

    // ScalingManager
    let scaling_config = ScalingConfig {
        min_instances: 1,
        max_instances: 5,
        scale_up_threshold: 0.8,
        scale_down_threshold: 0.2,
        cooldown_period: Duration::from_secs(300),
        health_check_interval: Duration::from_secs(30),
        instance_timeout: Duration::from_secs(60),
    };
    let scaling_manager = Arc::new(ScalingManager::new(
        redis_client_trait.clone(),
        scaling_config,
    ));

    Ok(ServiceContext {
        event_store,
        projection_store,
        cache_service,
        auth_service,
        user_repository,
        account_repository,
        shard_manager,
        scaling_manager,
    })
}
