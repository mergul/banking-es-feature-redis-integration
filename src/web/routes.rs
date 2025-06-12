use crate::{application::AccountService, web::handlers::*, infrastructure::auth::AuthService};
use axum::{
    routing::{get, post, put},
    Router,
};
use tower_http::cors::CorsLayer;
use std::sync::Arc;

pub fn create_routes(service: AccountService, auth_service: AuthService) -> Router<(Arc<AccountService>, Arc<AuthService>)> {
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
        .with_state((Arc::new(service), Arc::new(auth_service)))
        .layer(CorsLayer::permissive())
}
