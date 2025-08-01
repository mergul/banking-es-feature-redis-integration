use crate::application::services::CQRSAccountService;
use crate::infrastructure::auth::{AuthService, Claims, TokenType};
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware,
    response::Response,
    routing::{get, post},
    Router,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

async fn auth_middleware(
    State(auth_service): State<Arc<AuthService>>,
    request: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Result<Response, StatusCode> {
    // Extract token from Authorization header
    let auth_header = request
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));

    match auth_header {
        Some(token) => {
            // Validate token
            match auth_service.validate_token(token, TokenType::Access).await {
                Ok(_claims) => {
                    // Token is valid, proceed with request
                    Ok(next.run(request).await)
                }
                Err(_) => {
                    // Token is invalid
                    Err(StatusCode::UNAUTHORIZED)
                }
            }
        }
        None => {
            // No Authorization header
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

pub fn create_cqrs_router(
    account_service: Arc<CQRSAccountService>,
    auth_service: Arc<AuthService>,
) -> Router {
    Router::new()
        // Account operations (Commands) - Require authentication
        .route(
            "/api/cqrs/accounts",
            post(crate::web::cqrs_handlers::create_account),
        )
        .route(
            "/api/cqrs/accounts/{id}/deposit",
            post(crate::web::cqrs_handlers::deposit_money),
        )
        .route(
            "/api/cqrs/accounts/{id}/withdraw",
            post(crate::web::cqrs_handlers::withdraw_money),
        )
        .route(
            "/api/cqrs/accounts/{id}/close",
            post(crate::web::cqrs_handlers::close_account),
        )
        // Account queries - Require authentication
        .route(
            "/api/cqrs/accounts",
            get(crate::web::cqrs_handlers::get_all_accounts),
        )
        .route(
            "/api/cqrs/accounts/{id}",
            get(crate::web::cqrs_handlers::get_account),
        )
        .route(
            "/api/cqrs/accounts/{id}/balance",
            get(crate::web::cqrs_handlers::get_account_balance),
        )
        .route(
            "/api/cqrs/accounts/{id}/status",
            get(crate::web::cqrs_handlers::is_account_active),
        )
        .route(
            "/api/cqrs/accounts/{id}/transactions",
            get(crate::web::cqrs_handlers::get_account_transactions),
        )
        // Batch operations - Require authentication
        .route(
            "/api/cqrs/transactions/batch",
            post(crate::web::cqrs_handlers::batch_transactions),
        )
        .route(
            "/api/cqrs/operations/batch",
            post(crate::web::cqrs_handlers::execute_batch_operations),
        )
        .route(
            "/api/cqrs/commands/batch",
            post(crate::web::cqrs_handlers::execute_batch_commands),
        )
        // Version and aggregate info endpoints
        .route(
            "/api/cqrs/aggregates/{id}/version",
            get(crate::web::cqrs_handlers::get_aggregate_version),
        )
        .route(
            "/api/cqrs/aggregates/versions",
            post(crate::web::cqrs_handlers::get_aggregate_versions),
        )
        .route(
            "/api/cqrs/aggregates/{id}/info",
            get(crate::web::cqrs_handlers::get_aggregate_info),
        )
        // Health and metrics - No auth required
        .route(
            "/api/cqrs/health",
            get(crate::web::cqrs_handlers::health_check),
        )
        .route("/api/cqrs/metrics", get(crate::web::cqrs_handlers::metrics))
        // Add auth middleware for protected routes
        .layer(middleware::from_fn_with_state(
            auth_service.clone(),
            auth_middleware,
        ))
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
