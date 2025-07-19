use crate::infrastructure::auth::{AuthConfig, AuthService};
use crate::infrastructure::cache_service::{CacheConfig, CacheService, EvictionPolicy};
use crate::infrastructure::event_store::{EventStore, DB_POOL};
use crate::infrastructure::kafka_abstraction::{KafkaConfig, KafkaProducer, KafkaProducerTrait};
use crate::infrastructure::logging::{init_logging, start_log_rotation_task, LoggingConfig};
use crate::infrastructure::projections::ProjectionStore;
use crate::infrastructure::redis_abstraction::RealRedisClient;
use crate::infrastructure::redis_abstraction::RedisClient;
use crate::infrastructure::scaling::{ScalingConfig, ScalingManager, ServiceInstance};
use crate::web::cqrs_routes::create_cqrs_router;
// use crate::web::routes::create_router; // Commented out: deprecated router
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
use tracing_appender::rolling::{RollingFileAppender, Rotation};
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

use crate::application::services::CQRSAccountService;
// use crate::application::AccountService; // Commented out: deprecated service
use crate::infrastructure::middleware::RequestMiddleware; // Keep if CQRS handlers use similar middleware logic
use crate::infrastructure::{AccountRepository, EventStoreConfig, UserRepository}; // AccountRepository might be unused if not by AccountService

use opentelemetry::sdk::export::trace::SpanExporter;
use opentelemetry::trace::TracerProvider;

use crate::infrastructure::init::init_all_services;
use crate::infrastructure::outbox::PostgresOutboxRepository;
use crate::infrastructure::outbox_poller::{OutboxPollerConfig, OutboxPollingService};
use crate::infrastructure::shutdown::{Shutdown, ShutdownManager};
use infrastructure::config::{AppConfig, DataCaptureMethod};
use tokio::sync::mpsc;

use crate::infrastructure::cdc_debezium::{CDCOutboxRepository, DebeziumConfig};
use crate::infrastructure::cdc_service_manager::CDCServiceManager;

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
                <h2>CQRS Endpoints</h2>
                <div class="endpoint">POST /api/cqrs/accounts - Create new account (CQRS)</div>
                <div class="endpoint">GET /api/cqrs/accounts/{id} - Get account details (CQRS)</div>
                <div class="endpoint">POST /api/cqrs/accounts/{id}/deposit - Deposit money (CQRS)</div>
                <div class="endpoint">POST /api/cqrs/accounts/{id}/withdraw - Withdraw money (CQRS)</div>
                <div class="endpoint">GET /api/cqrs/accounts/{id}/balance - Get account balance (CQRS)</div>
                <div class="endpoint">GET /api/cqrs/accounts/{id}/status - Check account status (CQRS)</div>
                <div class="endpoint">GET /api/cqrs/accounts/{id}/transactions - Get account transactions (CQRS)</div>
                <div class="endpoint">POST /api/cqrs/transactions/batch - Batch process transactions (CQRS)</div>
                <div class="endpoint">GET /api/cqrs/health - CQRS health check</div>
                <div class="endpoint">GET /api/cqrs/metrics - CQRS metrics</div>
            </body>
        </html>
    "#,
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize advanced logging configuration
    let logging_config = LoggingConfig {
        log_dir: "logs".to_string(),
        max_files: 30,                    // Keep 30 days of logs
        max_file_size: 100 * 1024 * 1024, // 100MB
        enable_console: true,
        enable_file: true,
        log_level: Level::INFO,
        enable_json: false,
    };

    init_logging(Some(logging_config))?;

    // Start log rotation task
    let log_dir = "logs".to_string();
    let max_files = 30;
    tokio::spawn(start_log_rotation_task(
        log_dir,
        max_files,
        Duration::from_secs(3600), // Run every hour
    ));

    // Log application startup
    info!("Starting high-performance banking service with CQRS");
    info!("Advanced logging initialized with file rotation");

    // Load environment variables
    dotenv::dotenv().ok();

    // Initialize all services with background tasks
    let service_context = init_all_services().await?;

    // Create KafkaConfig instance (can be loaded from env or defaults)
    let kafka_config = KafkaConfig::default(); // Or load from env
    let app_config = AppConfig::from_env();

    // Initialize CQRS service using the services from ServiceContext
    let cqrs_service = Arc::new(CQRSAccountService::new(
        service_context.event_store.clone(),
        service_context.projection_store.clone(),
        service_context.cache_service.clone(),
        kafka_config.clone(),       // Clone KafkaConfig for CQRS service
        1000,                       // max_concurrent_operations
        100,                        // batch_size
        Duration::from_millis(100), // batch_timeout
    ));

    let mut cdc_service_manager: Option<CDCServiceManager> = None;

    if app_config.data_capture.method == DataCaptureMethod::CdcDebezium {
        info!("Using CDC Debezium for data capture");
        let cdc_outbox_repo = Arc::new(
            crate::infrastructure::cdc_debezium::CDCOutboxRepository::new(
                service_context.event_store.get_partitioned_pools().clone(),
            ),
        );

        let kafka_producer_for_cdc =
            crate::infrastructure::kafka_abstraction::KafkaProducer::new(kafka_config.clone())?;
        let kafka_consumer_for_cdc =
            crate::infrastructure::kafka_abstraction::KafkaConsumer::new(kafka_config.clone())?;

        let cdc_config = crate::infrastructure::cdc_debezium::DebeziumConfig::default();
        let mut manager = crate::infrastructure::cdc_service_manager::CDCServiceManager::new(
            cdc_config,
            cdc_outbox_repo,
            kafka_producer_for_cdc,
            kafka_consumer_for_cdc,
            service_context.cache_service.clone(),
            service_context.projection_store.clone(),
            None, // <-- pass None for metrics in production
        )?;

        // Start CDC service
        manager.start().await?;
        info!("CDC Service Manager started.");
        cdc_service_manager = Some(manager);
    } else if app_config.data_capture.method == DataCaptureMethod::OutboxPoller {
        info!("Using Outbox Poller for data capture");
        let outbox_repo = Arc::new(PostgresOutboxRepository::new(
            service_context.event_store.get_partitioned_pools().clone(),
        ));
        let kafka_producer = Arc::new(KafkaProducer::new(kafka_config.clone())?);
        let poller_config = OutboxPollerConfig::default();
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let poller_service =
            OutboxPollingService::new(outbox_repo, kafka_producer, poller_config, shutdown_rx);

        tokio::spawn(poller_service.run());
        info!("Outbox Poller started.");

        // --- Initialize and Start Kafka Event Processor ---
        let kafka_event_processor =
            crate::infrastructure::kafka_event_processor::KafkaEventProcessor::new(
                kafka_config.clone(),
                &service_context.event_store,
                &service_context.projection_store,
                &service_context.cache_service,
                crate::infrastructure::kafka_event_processor::RetryConfig::default(),
            )?;

        tokio::spawn(async move {
            if let Err(e) = kafka_event_processor.start_processing().await {
                error!("Kafka Event Processor failed: {}", e);
            }
        });
        info!("Kafka Event Processor started.");
    }

    // Create CQRS router
    // The auth_service is cloned for potential future use in CQRS auth middleware/handlers
    let cqrs_router =
        create_cqrs_router(cqrs_service.clone(), service_context.auth_service.clone());

    // The main app is now just the CQRS router, potentially with some global/static routes.
    // For now, the root HTML page lists both old and new endpoints. This should be updated later.
    let app = Router::new()
        .route("/", get(root)) // Keep the root informational page for now
        .merge(cqrs_router)
        // Global middleware can still be applied here if needed, outside of create_cqrs_router
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http()) // This was already in create_router, ensure it's not duplicated if also in create_cqrs_router
                .layer(CompressionLayer::new()) // Same as above
                .layer(CorsLayer::permissive()) // Same as above
                .into_inner(),
        )
        // Fallback for static files
        .fallback_service(ServeDir::new("static"));

    // The old router and its state are no longer used.
    // The /api/auth routes were part of the old router. They need to be re-evaluated.
    // For now, they are removed. A new auth strategy for CQRS would be needed.
    // The /api/health, /api/metrics, /api/logs/stats from the old router are also removed.
    // The CQRS router has its own /api/cqrs/health and /api/cqrs/metrics.
    // A new /api/logs/stats might be added to the main app if still desired globally.

    // Setup TCP listener with optimized settings
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = TcpListener::bind(addr).await?;
    configure_tcp_listener(&listener)?;

    info!("Server running on {}", addr);
    info!("CQRS endpoints available at /api/cqrs/*");

    // Start the server with graceful shutdown
    let server = axum::serve(listener, app);
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    if let Err(e) = graceful.await {
        error!("Server error: {}", e);
        return Err(e.into());
    }

    // Graceful shutdown of services
    if let Some(mut manager) = cdc_service_manager {
        info!("Sending shutdown signal to CDC service...");
        manager.stop().await?;
    }

    // Add a small delay to allow the services to process the shutdown signal
    tokio::time::sleep(Duration::from_millis(1000)).await;

    service_context.shutdown().await;
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

    // Use futures::select! instead of tokio::select!
    use futures::future::select;
    use futures::pin_mut;

    pin_mut!(ctrl_c);
    pin_mut!(terminate);

    select(ctrl_c, terminate).await;

    info!("Shutting down gracefully...");
}

fn configure_tcp_listener(listener: &TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        use libc::{setsockopt, SOL_SOCKET, SO_REUSEADDR, SO_REUSEPORT};
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
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawSocket;
        use windows::Win32::Networking::WinSock::SOCKET;
        use windows::Win32::Networking::WinSock::{setsockopt, SOL_SOCKET, SO_REUSEADDR};

        let socket = SOCKET(listener.as_raw_socket() as usize);
        let optval: i32 = 1;

        unsafe {
            setsockopt(
                socket,
                SOL_SOCKET,
                SO_REUSEADDR,
                Some(std::slice::from_raw_parts(
                    &optval as *const _ as *const u8,
                    std::mem::size_of::<i32>(),
                )),
            );
        }
    }

    Ok(())
}
