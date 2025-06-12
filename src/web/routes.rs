use crate::{application::AccountService, infrastructure::auth::AuthService, web::handlers::*};
use axum::{
    routing::{get, post, put},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

// New function that only sets up the router with routes, expecting services to be passed in
pub fn create_router(
    service: Arc<AccountService>,
    auth_service: Arc<AuthService>,
) -> Router<(Arc<AccountService>, Arc<AuthService>)> {
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
        .with_state((service, auth_service))
        .layer(CorsLayer::permissive())
}
