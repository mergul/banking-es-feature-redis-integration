use crate::application::services::CQRSAccountService;
use crate::infrastructure::auth::AuthService;
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub fn create_cqrs_router(
    account_service: Arc<CQRSAccountService>,
    _auth_service: Arc<AuthService>,
) -> Router {
    Router::new()
        // Account operations (Commands)
        .route(
            "/api/cqrs/accounts",
            post(crate::web::cqrs_handlers::create_account),
        )
        .route(
            "/api/cqrs/accounts/:id/deposit",
            post(crate::web::cqrs_handlers::deposit_money),
        )
        .route(
            "/api/cqrs/accounts/:id/withdraw",
            post(crate::web::cqrs_handlers::withdraw_money),
        )
        .route(
            "/api/cqrs/accounts/:id/close",
            post(crate::web::cqrs_handlers::close_account),
        )
        // Account queries
        .route(
            "/api/cqrs/accounts",
            get(crate::web::cqrs_handlers::get_all_accounts),
        )
        .route(
            "/api/cqrs/accounts/:id",
            get(crate::web::cqrs_handlers::get_account),
        )
        .route(
            "/api/cqrs/accounts/:id/balance",
            get(crate::web::cqrs_handlers::get_account_balance),
        )
        .route(
            "/api/cqrs/accounts/:id/status",
            get(crate::web::cqrs_handlers::is_account_active),
        )
        .route(
            "/api/cqrs/accounts/:id/transactions",
            get(crate::web::cqrs_handlers::get_account_transactions),
        )
        // Batch operations
        .route(
            "/api/cqrs/transactions/batch",
            post(crate::web::cqrs_handlers::batch_transactions),
        )
        // Health and metrics
        .route(
            "/api/cqrs/health",
            get(crate::web::cqrs_handlers::health_check),
        )
        .route("/api/cqrs/metrics", get(crate::web::cqrs_handlers::metrics))
        // Add optimized middleware stack
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new())
                .layer(CorsLayer::permissive())
                .into_inner(),
        )
        .with_state(account_service)
}
