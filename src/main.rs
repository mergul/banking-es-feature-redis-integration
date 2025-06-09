use axum::{
    extract::Extension,
    http::{HeaderValue, Method},
    routing::{get, post},
    Router, ServiceExt,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Import redis client and trait abstractions
use crate::infrastructure::{
    projections::ProjectionConfig,
    redis_abstraction::{
        CircuitBreakerConfig, CircuitBreakerRedisClient, LoadShedderConfig,
        LoadSheddingRedisClient, RedisPoolConfig,
    },
    RealRedisClient, RedisClientTrait,
}; // Added trait imports
use redis::Client as NativeRedisClient; // Renamed for clarity
use std::env;
use std::time::Duration;

mod application;
mod domain;
mod infrastructure;
mod web;

use crate::infrastructure::{AccountRepository, EventStore, ProjectionStore};
use crate::web::handlers::RateLimitedService;
use crate::{application::AccountService, infrastructure::EventStoreConfig};

#[derive(Debug)]
struct AppConfig {
    database_pool_size: u32,
    max_concurrent_operations: usize,
    max_requests_per_second: usize,
    batch_flush_interval_ms: u64,
    cache_size: usize,
    port: u16,
    // Add new performance-related configs
    redis_pool_config: RedisPoolConfig,
    bulk_batch_size: usize,
    bulk_processing_interval_ms: u64,
    cache_ttl_seconds: u64,
    max_retries: u32,
    backoff_duration_ms: u64,
    circuit_breaker_config: CircuitBreakerConfig,
    load_shedder_config: LoadShedderConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database_pool_size: 10,
            max_concurrent_operations: 100,
            max_requests_per_second: 1000,
            batch_flush_interval_ms: 1000,
            cache_size: 1000,
            port: 8080,
            redis_pool_config: RedisPoolConfig::default(),
            bulk_batch_size: 100,
            bulk_processing_interval_ms: 5000,
            cache_ttl_seconds: 3600,
            max_retries: 3,
            backoff_duration_ms: 100,
            circuit_breaker_config: CircuitBreakerConfig::default(),
            load_shedder_config: LoadShedderConfig::default(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for observability
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting high-performance banking service");

    // Configuration for high throughput
    let config = AppConfig::default();

    // Initialize infrastructure with optimized settings
    let event_store = EventStore::new_with_config(EventStoreConfig::default()).await?;
    let projection_store = ProjectionStore::new_with_config(ProjectionConfig::default()).await?;

    // Initialize Redis Client with connection pooling
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| {
        info!("REDIS_URL not set, defaulting to redis://127.0.0.1:6379");
        "redis://127.0.0.1:6379".to_string()
    });

    // Create Redis client with circuit breaker and load shedding
    let native_redis_client = NativeRedisClient::open(redis_url)?;
    let redis_client_trait: Arc<dyn RedisClientTrait> = Arc::new(LoadSheddingRedisClient::new(
        Arc::new(CircuitBreakerRedisClient::new(
            RealRedisClient::new(
                native_redis_client.clone(),
                Some(config.redis_pool_config.clone()),
            ),
            config.circuit_breaker_config,
        )),
        config.load_shedder_config,
    ));

    // Create the repository
    let repository = Arc::new(AccountRepository::new(
        event_store,
        redis_client_trait.clone(),
    ));

    // Create the service
    let service = AccountService::new(repository, projection_store, redis_client_trait);

    // Start background tasks for cache warming and consistency checking
    service.start_background_tasks();

    // Create the rate-limited service
    let rate_limited_service = RateLimitedService::new(service, config.max_requests_per_second);

    // Start the server
    let addr = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);

    // Configure the TCP listener for high performance
    configure_tcp_listener(&listener)?;

    // Start the server
    axum::serve(listener, rate_limited_service.into_make_service()).await?;

    Ok(())
}

fn configure_tcp_listener(listener: &TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    use libc::{setsockopt, IPPROTO_TCP, SOL_SOCKET, SO_REUSEADDR, SO_REUSEPORT, TCP_NODELAY};
    use std::os::unix::io::AsRawFd;

    let fd = listener.as_raw_fd();
    let optval: libc::c_int = 1;

    unsafe {
        // Enable SO_REUSEADDR for faster restarts
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );

        // Enable SO_REUSEPORT for load balancing across multiple processes
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );

        // Enable TCP_NODELAY for low latency
        setsockopt(
            fd,
            IPPROTO_TCP,
            TCP_NODELAY,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );
    }

    Ok(())
}
