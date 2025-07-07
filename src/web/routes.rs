use crate::application::services::AccountService;
use crate::infrastructure::auth::AuthService;
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

// New function that only sets up the router with routes, expecting services to be passed in
#[deprecated(note = "Use create_cqrs_router in src/web/cqrs_routes.rs instead.")]
pub fn create_router(
    account_service: Arc<AccountService>,
    auth_service: Arc<AuthService>,
) -> Router {
    Router::new()
        // Auth operations
        .route("/api/auth/register", post(crate::web::handlers::register))
        .route("/api/auth/login", post(crate::web::handlers::login))
        .route("/api/auth/logout", post(crate::web::handlers::logout))
        // Account operations
        .route("/api/accounts", get(crate::web::handlers::get_all_accounts))
        .route("/api/accounts", post(crate::web::handlers::create_account))
        .route("/api/accounts/:id", get(crate::web::handlers::get_account))
        .route(
            "/api/accounts/:id/deposit",
            post(crate::web::handlers::deposit_money),
        )
        .route(
            "/api/accounts/:id/withdraw",
            post(crate::web::handlers::withdraw_money),
        )
        .route(
            "/api/accounts/:id/transactions",
            get(crate::web::handlers::get_account_transactions),
        )
        // Batch operations for high throughput
        .route(
            "/api/transactions/batch",
            post(crate::web::handlers::batch_transactions),
        )
        // Health and metrics
        .route("/api/health", get(crate::web::handlers::health_check))
        .route("/api/metrics", get(crate::web::handlers::metrics))
        // Add optimized middleware stack
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new())
                .layer(CorsLayer::permissive())
                .into_inner(),
        )
        .with_state((account_service, auth_service))
        // Serve static files as fallback
        .fallback_service(ServeDir::new("static"))
}
