use crate::application::services::AccountService;
use crate::infrastructure::auth::{AuthConfig, AuthService};
use crate::infrastructure::cache_service::{
    CacheConfig, CacheService, CacheServiceTrait, EvictionPolicy,
};
use crate::infrastructure::connection_pool_monitor::{ConnectionPoolMonitor, PoolMonitorConfig};
use crate::infrastructure::deadlock_detector::{DeadlockConfig, DeadlockDetector};
use crate::infrastructure::event_store::{EventStore, EventStoreConfig, EventStoreTrait};
use crate::infrastructure::kafka_abstraction::{KafkaConfig, KafkaConsumer, KafkaProducer};
use crate::infrastructure::kafka_event_processor::KafkaEventProcessor;
use crate::infrastructure::l1_cache_updater::L1CacheUpdater;
use crate::infrastructure::middleware::{
    AccountCreationValidator, RequestMiddleware, TransactionValidator,
};
use crate::infrastructure::projections::{ProjectionConfig, ProjectionStore, ProjectionStoreTrait};
use crate::infrastructure::redis_abstraction::{RealRedisClient, RedisClientTrait};
use crate::infrastructure::repository::{AccountRepository, AccountRepositoryTrait};
use crate::infrastructure::scaling::{ScalingConfig, ScalingManager};
use crate::infrastructure::timeout_manager::{TimeoutConfig, TimeoutManager};
use crate::infrastructure::user_repository::UserRepository;
use anyhow::Result;
use redis;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use uuid::Uuid;

pub struct ServiceContext {
    pub account_service: Arc<AccountService>,
    pub auth_service: Arc<AuthService>,
    pub scaling_manager: Arc<ScalingManager>,
    pub kafka_processor: Arc<KafkaEventProcessor>,
    pub l1_cache_updater: Arc<L1CacheUpdater>,
    pub timeout_manager: Arc<TimeoutManager>,
    pub deadlock_detector: Arc<DeadlockDetector>,
    pub connection_pool_monitor: Arc<ConnectionPoolMonitor>,
    warmup_handle: tokio::task::JoinHandle<()>,
    l1_handle: tokio::task::JoinHandle<()>,
    kafka_handle: tokio::task::JoinHandle<()>,
}

impl ServiceContext {
    pub async fn shutdown(mut self) {
        info!("Starting graceful shutdown of services...");

        // Cancel Kafka event processor
        self.kafka_handle.abort();
        // Wait for it to finish
        if let Err(_) = self.kafka_handle.await {
            error!("Error during Kafka event processor shutdown");
        }

        // Cancel L1 cache updater
        self.l1_handle.abort();
        // Wait for it to finish
        if let Err(_) = self.l1_handle.await {
            error!("Error during L1 cache updater shutdown");
        }

        // Cancel warmup task if it's still running
        self.warmup_handle.abort();
        if let Err(_) = self.warmup_handle.await {
            error!("Error during warmup task shutdown");
        }

        info!("Service shutdown complete");
    }

    pub async fn check_background_tasks(mut self) -> Result<()> {
        // Check warmup task status
        let warmup_result = tokio::spawn(async move { self.warmup_handle.await }).await?;

        if let Err(_) = warmup_result {
            error!("Warmup task failed");
            return Err(anyhow::anyhow!("Warmup task failed"));
        }

        // Check L1 cache updater status
        let l1_result = tokio::spawn(async move { self.l1_handle.await }).await?;

        if let Err(_) = l1_result {
            error!("L1 cache updater task failed");
            return Err(anyhow::anyhow!("L1 cache updater task failed"));
        }

        Ok(())
    }
}

pub async fn init_all_services() -> Result<ServiceContext> {
    info!("Initializing services...");

    // Initialize timeout manager
    let timeout_config = TimeoutConfig {
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
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
        ),
        connection_acquire_timeout: Duration::from_secs(
            std::env::var("CONNECTION_ACQUIRE_TIMEOUT")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
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
    let timeout_manager = Arc::new(TimeoutManager::new(timeout_config));

    // Initialize deadlock detector
    let deadlock_config = DeadlockConfig {
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
    let deadlock_detector = Arc::new(DeadlockDetector::new(deadlock_config));

    // Initialize Redis client Singleton with connection pool
    let redis_client = Arc::new(redis::Client::open("redis://127.0.0.1/")?);
    let redis_client_trait = RealRedisClient::new(redis_client.as_ref().clone(), None);

    // Initialize EventStore with optimized pool size
    let event_store: Arc<dyn EventStoreTrait + Send + Sync> =
        Arc::new(EventStore::new_with_pool_size(5).await?);

    // Initialize connection pool monitor
    let pool_monitor_config = PoolMonitorConfig {
        health_check_interval: Duration::from_secs(
            std::env::var("POOL_HEALTH_CHECK_INTERVAL")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
        ),
        connection_timeout: Duration::from_secs(
            std::env::var("POOL_CONNECTION_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        ),
        max_connection_wait_time: Duration::from_secs(
            std::env::var("POOL_MAX_WAIT_TIME")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        ),
        pool_exhaustion_threshold: std::env::var("POOL_EXHAUSTION_THRESHOLD")
            .unwrap_or_else(|_| "0.8".to_string())
            .parse()
            .unwrap_or(0.8),
        enable_auto_scaling: std::env::var("POOL_AUTO_SCALING")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        max_connections: std::env::var("POOL_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100),
        min_connections: std::env::var("POOL_MIN_CONNECTIONS")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .unwrap_or(5),
    };
    let connection_pool_monitor = Arc::new(ConnectionPoolMonitor::new(
        event_store.get_pool().clone(),
        pool_monitor_config,
    ));

    // Initialize UserRepository
    let user_repository = Arc::new(UserRepository::new(event_store.get_pool().clone()));

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
        user_repository,
    ));

    // Initialize ProjectionStore with optimized config
    let projection_config = ProjectionConfig {
        max_connections: std::env::var("PROJECTION_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .unwrap_or(5),
        min_connections: std::env::var("PROJECTION_MIN_CONNECTIONS")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        batch_timeout_ms: 1, // Set to 1ms for immediate batch processing
        ..ProjectionConfig::default()
    };

    let projection_store: Arc<dyn ProjectionStoreTrait + Send + Sync> =
        Arc::new(ProjectionStore::new_with_config(projection_config).await?);

    // Initialize CacheService with optimized config
    let cache_config = CacheConfig {
        default_ttl: Duration::from_secs(
            std::env::var("CACHE_DEFAULT_TTL")
                .unwrap_or_else(|_| "3600".to_string())
                .parse()
                .unwrap_or(3600),
        ),
        max_size: std::env::var("CACHE_MAX_SIZE")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000),
        shard_count: std::env::var("CACHE_SHARD_COUNT")
            .unwrap_or_else(|_| "8".to_string())
            .parse()
            .unwrap_or(8),
        warmup_batch_size: std::env::var("CACHE_WARMUP_BATCH_SIZE")
            .unwrap_or_else(|_| "50".to_string())
            .parse()
            .unwrap_or(50),
        warmup_interval: Duration::from_secs(
            std::env::var("CACHE_WARMUP_INTERVAL")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
        ),
        eviction_policy: EvictionPolicy::LRU,
    };

    let cache_service: Arc<dyn CacheServiceTrait + Send + Sync> =
        Arc::new(CacheService::new(redis_client_trait.clone(), cache_config));

    // Initialize AccountRepository with Kafka producer
    let account_repository: Arc<dyn AccountRepositoryTrait + Send + Sync> = {
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
                .unwrap_or_else(|_| "300000".to_string())
                .parse()
                .unwrap_or(300000),
            consumer_session_timeout_ms: std::env::var("KAFKA_CONSUMER_SESSION_TIMEOUT_MS")
                .unwrap_or_else(|_| "10000".to_string())
                .parse()
                .unwrap_or(10000),
            consumer_max_poll_records: std::env::var("KAFKA_CONSUMER_MAX_POLL_RECORDS")
                .unwrap_or_else(|_| "500".to_string())
                .parse()
                .unwrap_or(500),
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

    let middleware = Arc::new(RequestMiddleware::new(rate_limit_config));
    middleware.register_validator(
        "create_account".to_string(),
        Box::new(AccountCreationValidator),
    );
    middleware.register_validator("deposit_money".to_string(), Box::new(TransactionValidator));
    middleware.register_validator("withdraw_money".to_string(), Box::new(TransactionValidator));

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

    // Initialize AccountService
    let account_service = Arc::new(AccountService::new(
        account_repository,
        projection_store.clone(),
        cache_service.clone(),
        middleware,
        100,
    ));

    // Use the same kafka_config for all Kafka-related services
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
            .unwrap_or_else(|_| "300000".to_string())
            .parse()
            .unwrap_or(300000),
        consumer_session_timeout_ms: std::env::var("KAFKA_CONSUMER_SESSION_TIMEOUT_MS")
            .unwrap_or_else(|_| "10000".to_string())
            .parse()
            .unwrap_or(10000),
        consumer_max_poll_records: std::env::var("KAFKA_CONSUMER_MAX_POLL_RECORDS")
            .unwrap_or_else(|_| "500".to_string())
            .parse()
            .unwrap_or(500),
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

    // Start warmup task early with explicit Arc cloning
    let event_store_for_warmup = event_store.clone();
    let cache_service_for_warmup = cache_service.clone();
    let warmup_handle = tokio::spawn(async move {
        if let Ok(accounts) = event_store_for_warmup.get_all_accounts().await {
            let account_ids: Vec<Uuid> = accounts.iter().map(|a| a.id).collect();
            if let Err(_) = cache_service_for_warmup.warmup_cache(account_ids).await {
                error!("Cache warmup error");
            }
        }
    });

    // Initialize L1CacheUpdater
    let l1_cache_updater = Arc::new(L1CacheUpdater::new(
        kafka_config.clone(),
        cache_service.clone(),
    )?);

    // Initialize KafkaEventProcessor
    let kafka_processor = Arc::new(KafkaEventProcessor::new(
        kafka_config,
        &event_store,
        &projection_store,
        &cache_service,
    )?);

    // Start L1 cache updater in background
    let l1_updater = l1_cache_updater.clone();
    let l1_handle = tokio::spawn(async move {
        if let Err(_) = l1_updater.start().await {
            error!("L1 cache updater error");
        }
    });

    // Start Kafka event processor in background
    let kafka_processor_for_start = kafka_processor.clone();
    let kafka_handle = tokio::spawn(async move {
        if let Err(_) = kafka_processor_for_start.start_processing().await {
            error!("Kafka event processor error");
        }
    });

    info!("All services initialized successfully");

    // Create ServiceContext
    let service_context = ServiceContext {
        account_service,
        auth_service,
        scaling_manager,
        kafka_processor,
        l1_cache_updater,
        timeout_manager,
        deadlock_detector,
        connection_pool_monitor,
        warmup_handle,
        l1_handle,
        kafka_handle,
    };

    Ok(service_context)
}
