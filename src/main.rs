use crate::infrastructure::auth::{AuthConfig, AuthService};
use crate::infrastructure::cache_service::{CacheConfig, CacheService, EvictionPolicy};
use crate::infrastructure::event_store::{EventStore, DB_POOL};
use crate::infrastructure::kafka_abstraction::KafkaConfig;
use crate::infrastructure::projections::ProjectionStore;
use crate::infrastructure::redis_abstraction::RealRedisClient;
use crate::infrastructure::redis_abstraction::RedisClient;
use crate::infrastructure::scaling::{ScalingConfig, ScalingManager, ServiceInstance};
use crate::web::routes::create_router;
use anyhow::Result;
use axum::{
    http::Method,
    response::Html,
    routing::IntoMakeService,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use dotenv;
use redis;
use sqlx::PgPool;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};
use tokio::signal;
use tower_http::cors::CorsLayer;
use tracing::{error, info, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

mod application;
mod domain;
mod infrastructure;
mod web;

use crate::application::AccountService;
use crate::infrastructure::middleware::RequestMiddleware;
use crate::infrastructure::{AccountRepository, EventStoreConfig, UserRepository};

use opentelemetry::sdk::export::trace::SpanExporter;
use opentelemetry::trace::TracerProvider;

use crate::infrastructure::init::init_all_services;
use infrastructure::config::AppConfig;

async fn root() -> Html<&'static str> {
    Html(
        r#"
        <html>
            <head>
                <title>Banking Service API</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                    h1 { color: #333; }
                    .endpoint { background: #f4f4f4; padding: 10px; margin: 5px 0; border-radius: 4px; }
                </style>
            </head>
            <body>
                <h1>Banking Service API</h1>
                <p>Welcome to the Banking Service API. Available endpoints:</p>
                <div class="endpoint">GET /health - Health check endpoint</div>
                <div class="endpoint">GET /metrics - Service metrics</div>
                <div class="endpoint">POST /accounts - Create new account</div>
                <div class="endpoint">GET /accounts/{id} - Get account details</div>
                <div class="endpoint">POST /accounts/{id}/deposit - Deposit money</div>
                <div class="endpoint">POST /accounts/{id}/withdraw - Withdraw money</div>
                <div class="endpoint">GET /accounts/{id}/transactions - Get account transactions</div>
                <div class="endpoint">POST /batch/transactions - Batch process transactions</div>
            </body>
        </html>
    "#,
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with better formatting
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("Starting high-performance banking service");

    // Load environment variables
    dotenv::dotenv().ok();

    // Initialize all services with background tasks
    let service_context = init_all_services().await?;

    // Clone services once for router state
    let router_state = (
        service_context.account_service.clone(),
        service_context.auth_service.clone(),
    );

    // Clone for shutdown before checking tasks
    let service_context_for_shutdown = service_context.clone();

    // Check background tasks status
    if let Err(e) = service_context.check_background_tasks().await {
        error!("Background tasks failed: {}", e);
        return Err(e.into());
    }

    // Build the router with optimized middleware stack
    let app = Router::new()
        // Auth operations
        .route("/api/auth/register", post(web::handlers::register))
        .route("/api/auth/login", post(web::handlers::login))
        .route("/api/auth/logout", post(web::handlers::logout))
        // Account operations
        .route("/api/accounts", post(web::handlers::create_account))
        .route("/api/accounts/{id}", get(web::handlers::get_account))
        .route(
            "/api/accounts/{id}/deposit",
            post(web::handlers::deposit_money),
        )
        .route(
            "/api/accounts/{id}/withdraw",
            post(web::handlers::withdraw_money),
        )
        .route(
            "/api/accounts/{id}/transactions",
            get(web::handlers::get_account_transactions),
        )
        // Batch operations for high throughput
        .route(
            "/api/transactions/batch",
            post(web::handlers::batch_transactions),
        )
        // Health and metrics
        .route("/api/health", get(web::handlers::health_check))
        .route("/api/metrics", get(web::handlers::metrics))
        // Add optimized middleware stack
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new())
                .layer(CorsLayer::permissive())
                .into_inner(),
        )
        .with_state(router_state)
        // Serve static files as fallback
        .fallback_service(ServeDir::new("static"));

    // Setup TCP listener with optimized settings
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = TcpListener::bind(addr).await?;
    configure_tcp_listener(&listener)?;

    info!("Server running on {}", addr);

    // Start the server with graceful shutdown
    let server = axum::serve(listener, app);
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    if let Err(e) = graceful.await {
        error!("Server error: {}", e);
        return Err(e.into());
    }

    // Graceful shutdown of services
    service_context_for_shutdown.shutdown().await;
    info!("Server shutdown complete");

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
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        );
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
