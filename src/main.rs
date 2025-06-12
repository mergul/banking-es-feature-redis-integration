use crate::infrastructure::auth::{AuthConfig, AuthService};
use crate::infrastructure::cache_service::{CacheConfig, CacheService, EvictionPolicy};
use crate::infrastructure::event_store::EventStore;
use crate::infrastructure::kafka_abstraction::KafkaConfig;
use crate::infrastructure::projections::ProjectionStore;
use crate::infrastructure::redis_abstraction::RealRedisClient;
use crate::infrastructure::redis_abstraction::RedisClient;
use crate::infrastructure::scaling::{ScalingConfig, ScalingManager, ServiceInstance};
use crate::web::routes::create_router;
use anyhow::Result;
use axum::{http::Method, routing::IntoMakeService, Router};
use chrono::Utc;
use dotenv;
use redis;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};
use tokio::signal;
use tower_http::cors::CorsLayer;
use tracing::{info, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

mod application;
mod domain;
mod infrastructure;
mod web;

use crate::application::AccountService;
use crate::infrastructure::middleware::RequestMiddleware;
use crate::infrastructure::{AccountRepository, EventStoreConfig};

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
    dotenv::dotenv().ok();
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Initialize Redis client
    let redis_client = Arc::new(redis::Client::open("redis://localhost:6379")?);
    let redis_client_trait = RealRedisClient::new(redis_client.as_ref().clone(), None);

    // Initialize scaling manager
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

    // Initialize auth service
    let auth_config = AuthConfig {
        jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string()),
        refresh_token_secret: std::env::var("REFRESH_TOKEN_SECRET")
            .unwrap_or_else(|_| "your-refresh-secret-key".to_string()),
        access_token_expiry: 3600,    // 1 hour
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
            latency_ms: 0,
        },
        shard_assignments: vec![],
        last_heartbeat: Utc::now(),
    };
    scaling_manager.register_instance(instance).await?;

    // Start scaling manager
    let scaling_manager_clone = scaling_manager.clone();
    tokio::spawn(async move {
        if let Err(e) = scaling_manager_clone.start_scaling_manager().await {
            eprintln!("Scaling manager error: {}", e);
        }
    });

    // Initialize other components
    let event_store = EventStore::new_with_config(EventStoreConfig::default()).await?;
    let kafka_config = KafkaConfig::default();
    let projections = ProjectionStore::new(
        crate::infrastructure::event_store::DB_POOL
            .get()
            .unwrap()
            .as_ref()
            .clone(),
    );
    let cache_config = CacheConfig {
        default_ttl: Duration::from_secs(3600),
        max_size: 10000,
        shard_count: 16,
        warmup_batch_size: 100,
        warmup_interval: Duration::from_secs(300),
        eviction_policy: EvictionPolicy::LRU,
    };
    let cache_service = CacheService::new(redis_client_trait.clone(), cache_config);
    let middleware = Arc::new(RequestMiddleware::default());
    let repository = Arc::new(AccountRepository::new(
        event_store.clone(),
        kafka_config,
        projections.clone(),
        redis_client.as_ref().clone(),
    )?);

    // Initialize services
    let (service, auth_service) = web::handlers::initialize_services().await?;

    // Create router without state
    let app = web::routes::create_router(
        Arc::new(AccountService::default()),
        Arc::new(AuthService::default()),
    );

    // Add state to the router
    let app = app.with_state((service, auth_service));

    // Start server
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    info!("Starting server on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
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
