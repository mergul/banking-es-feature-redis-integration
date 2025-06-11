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
use crate::infrastructure::redis_abstraction::RedisClient;
use crate::infrastructure::scaling::{ScalingConfig, ScalingManager, ServiceInstance};
use crate::infrastructure::event_store::EventStore;
use crate::infrastructure::kafka_abstraction::KafkaConfig;
use crate::infrastructure::projections::ProjectionStore;
use crate::web::web_handlers::create_routes;
use anyhow::Result;
use tokio::signal;
use uuid::Uuid;
use crate::infrastructure::auth::{AuthService, AuthConfig};
use std::time::Duration;
use chrono::{Utc, Duration as ChronoDuration};

mod application;
mod domain;
mod infrastructure;
mod web;

use crate::application::AccountService;
use crate::infrastructure::projections::ProjectionConfig;
use crate::infrastructure::{
    AccountRepository, EventStoreConfig, KafkaConfig, ProjectionStore,
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
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Initialize Redis client
    let redis_client = Arc::new(RedisClient::new("redis://localhost:6379")?);

    // Initialize scaling manager
    let scaling_config = ScalingConfig {
        min_instances: 1,
        max_instances: 5,
        scale_up_threshold: 0.8,
        scale_down_threshold: 0.2,
        check_interval: Duration::from_secs(30),
    };
    let scaling_manager = Arc::new(ScalingManager::new(redis_client.clone(), scaling_config));

    // Initialize auth service
    let auth_config = AuthConfig {
        jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string()),
        refresh_token_secret: std::env::var("REFRESH_TOKEN_SECRET").unwrap_or_else(|_| "your-refresh-secret-key".to_string()),
        access_token_expiry: 3600, // 1 hour
        refresh_token_expiry: 604800, // 7 days
        rate_limit_requests: 100,
        rate_limit_window: 60,
        max_failed_attempts: 5,
        lockout_duration_minutes: 30,
    };
    let auth_service = Arc::new(AuthService::new(redis_client.clone(), auth_config));

    // Register this instance
    let instance = ServiceInstance {
        id: Uuid::new_v4().to_string(),
        host: "localhost".to_string(),
        port: 8080,
        status: crate::infrastructure::scaling::InstanceStatus::Active,
        metrics: crate::infrastructure::scaling::InstanceMetrics {
            cpu_usage: 0.0,
            memory_usage: 0.0,
            request_count: 0,
            error_count: 0,
            latency_ms: 0.0,
        },
        shard_assignments: vec![],
        last_heartbeat: chrono::Utc::now(),
    };
    scaling_manager.register_instance(instance).await?;

    // Start scaling manager
    let scaling_manager_clone = scaling_manager.clone();
    tokio::spawn(async move {
        if let Err(e) = scaling_manager_clone.start().await {
            eprintln!("Scaling manager error: {}", e);
        }
    });

    // Initialize other components
    let event_store = EventStore::new_with_config(Default::default()).await?;
    let kafka_config = KafkaConfig::default();
    let projections = ProjectionStore::new(redis_client.clone());

    // Create routes
    let app = create_routes(
        event_store,
        kafka_config,
        projections,
        redis_client,
        scaling_manager,
        auth_service,
    );

    // Start server
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    info!("Starting server on {}", addr);
    
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutting down gracefully...");
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
