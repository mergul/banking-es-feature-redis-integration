// This file is deprecated - use cqrs_routes.rs instead
// Remove all AccountService usage

// use crate::application::services::AccountService;
use crate::application::services::CQRSAccountService;
use crate::infrastructure::auth::AuthService;
use crate::infrastructure::middleware::RequestMiddleware;
use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde_json::json;
use std::sync::Arc;

// Replace AccountService with CQRSAccountService
pub fn create_router(
    account_service: Arc<CQRSAccountService>,
    auth_service: Arc<AuthService>,
) -> Router {
    // This router is deprecated - use CQRS routes instead
    Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(get_metrics))
        .with_state((account_service, auth_service))
}

async fn health_check() -> Json<serde_json::Value> {
    Json(json!({
        "status": "healthy",
        "service": "banking-service",
        "version": "1.0.0"
    }))
}

async fn get_metrics(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
) -> Json<serde_json::Value> {
    let metrics = service.get_metrics();
    Json(json!({
        "commands_processed": metrics.commands_processed.load(std::sync::atomic::Ordering::Relaxed),
        "commands_failed": metrics.commands_failed.load(std::sync::atomic::Ordering::Relaxed),
        "queries_processed": metrics.queries_processed.load(std::sync::atomic::Ordering::Relaxed),
        "queries_failed": metrics.queries_failed.load(std::sync::atomic::Ordering::Relaxed),
    }))
}
