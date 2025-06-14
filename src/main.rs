use crate::infrastructure::auth::{AuthConfig, AuthService};
use crate::infrastructure::cache_service::{CacheConfig, CacheService, EvictionPolicy};
use crate::infrastructure::event_store::{EventStore, DB_POOL}; // Import DB_POOL
use crate::infrastructure::user_repository::UserRepository; // Import UserRepository
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

use opentelemetry::sdk::export::trace::SpanExporter;
use opentelemetry::trace::TracerProvider;

use infrastructure::config::AppConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

    // Initialize tracing with OpenTelemetry
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("banking-es")
        .with_endpoint("localhost:6831")
        .with_trace_config(
            opentelemetry::sdk::trace::config()
                .with_sampler(opentelemetry::sdk::trace::Sampler::AlwaysOn)
                .with_id_generator(opentelemetry::sdk::trace::RandomIdGenerator::default())
                .with_resource(opentelemetry::sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", "banking-es"),
                    opentelemetry::KeyValue::new("deployment.environment", "production"),
                ])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,banking_es=debug"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(opentelemetry_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

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

    // Ensure DB_POOL is initialized for UserRepository, before AuthService in main needs it.
    // This is a bit indirect; ideally, a shared pool is explicitly created and passed.
    // We rely on EventStore::new_with_config to initialize DB_POOL.
    // A dummy EventStoreConfig can be used if we only need to init the pool.
    // Or, better, ensure initialize_services() is called first if its pool is to be reused.
    // For this specific AuthService instance in main, let's ensure pool is ready.
    // One way to ensure DB_POOL is initialized:
    let _event_store_for_pool_init = EventStore::new_with_config(EventStoreConfig::default()).await?;
    let pool_for_main_auth = DB_POOL.get().expect("DB_POOL not initialized in main").clone();
    let user_repository_for_main_auth = Arc::new(UserRepository::new(pool_for_main_auth));

    // Initialize auth service (this instance might be for non-web components if any)
    let auth_config_main = AuthConfig { // Renamed to avoid conflict if initialize_services also defines one
        jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string()),
        refresh_token_secret: std::env::var("REFRESH_TOKEN_SECRET")
            .unwrap_or_else(|_| "your-refresh-secret-key".to_string()),
        access_token_expiry: 3600,    // 1 hour
        refresh_token_expiry: 604800, // 7 days
        rate_limit_requests: 100,
        rate_limit_window: 60,
        max_failed_attempts: 5, // Ensure this is i32 if AuthConfig expects i32
        lockout_duration_minutes: 30, // Ensure this is i64 if AuthConfig expects i64
    };
    // This auth_service instance is created here but then shadowed by the one from initialize_services.
    // If it were used by other components before initialize_services, it would need the user_repository.
    let _auth_service_main_instance = Arc::new(AuthService::new(
        redis_client.clone(),
        auth_config_main,
        user_repository_for_main_auth.clone()
    ));
    // NOTE: The auth_service used for the router is the one from initialize_services()

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

    // Initialize services
    let (service, auth_service) = web::handlers::initialize_services().await?;

    // Create router
    let app = web::routes::create_router(service, auth_service);

    // Start server
    let config = AppConfig::default();
    let addr = SocketAddr::from(([127, 0, 0, 1], config.port));
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
