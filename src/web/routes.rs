use crate::{application::AccountService, web::handlers::*};
use axum::{
    routing::{get, post, put},
    Router,
};
use tower_http::cors::CorsLayer;

pub fn create_routes(service: AccountService) -> Router {
    // Wrap the service in rate limiting (adjust max_requests as needed)
    let rate_limited_service = RateLimitedService::new(service, 1000);

    Router::new()
        .route("/accounts", post(create_account))
        .route("/accounts", get(get_all_accounts))
        .route("/accounts/:id", get(get_account))
        .route("/accounts/:id/deposit", put(deposit_money))
        .route("/accounts/:id/withdraw", put(withdraw_money))
        .route("/accounts/:id/transactions", get(get_account_transactions))
        .route("/accounts/batch", post(batch_transactions)) // New batch endpoint
        .route("/health", get(health_check))
        .route("/metrics", get(metrics))
        .with_state(rate_limited_service)
        .layer(CorsLayer::permissive())
}
