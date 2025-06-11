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

mod application;
mod domain;
mod infrastructure;
mod web;

use crate::application::AccountService;
use crate::infrastructure::projections::ProjectionConfig;
use crate::infrastructure::{
    AccountRepository, EventStore, EventStoreConfig, KafkaConfig, ProjectionStore,
};
use crate::web::handlers::RateLimitedService;

#[derive(Debug)]
struct AppConfig {
    database_pool_size: u32,
    max_concurrent_operations: usize,
    max_requests_per_second: usize,
    batch_flush_interval_ms: u64,
    cache_size: usize,
    port: u16,
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
    let kafka_config = KafkaConfig::default();

    // Create optimized repository
    let repository = Arc::new(AccountRepository::new(
        event_store,
        kafka_config,
        projection_store.clone(),
    )?);

    // Create high-performance service
    let account_service = AccountService::new(repository, projection_store);

    // Create the rate-limited service
    let rate_limited_service =
        RateLimitedService::new(account_service, config.max_requests_per_second);

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
