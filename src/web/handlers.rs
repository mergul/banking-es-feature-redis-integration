use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Request, Response, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Semaphore;
use tower::{Layer, Service, ServiceBuilder, ServiceExt};
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use uuid::Uuid;

use crate::domain::{AccountCommand, AccountError};
use crate::infrastructure::{
    auth::{AuthConfig, AuthService, Claims, LoginRequest, LoginResponse, LogoutRequest, UserRole},
    cache_service::{CacheConfig, CacheService, EvictionPolicy},
    event_store::{EventStore, EventStoreConfig, DB_POOL},
    kafka_abstraction::KafkaConfig,
    logging::get_log_stats,
    middleware::{
        AccountCreationValidator, RequestContext, RequestMiddleware, TransactionValidator,
    },
    projections::{AccountProjection, ProjectionConfig, ProjectionStore, TransactionProjection},
    rate_limiter::RateLimitConfig,
    redis_abstraction::{RealRedisClient, RedisClient, RedisPoolConfig},
    repository::{AccountRepository, AccountRepositoryTrait},
    scaling::{InstanceMetrics, ScalingConfig, ScalingManager, ServiceInstance},
    sharding::{LockManager, ShardConfig, ShardManager},
};
use crate::{application::AccountService, infrastructure::UserRepository};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateAccountRequest {
    pub owner_name: String,
    pub initial_balance: f64,
}

#[derive(Debug, Serialize)]
pub struct CreateAccountResponse {
    pub account_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TransactionRequest {
    pub amount: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct BatchTransactionRequest {
    pub transactions: Vec<SingleTransaction>,
}

#[derive(Debug, Deserialize)]
pub struct SingleTransaction {
    pub account_id: Uuid,
    pub amount: Decimal,
    pub transaction_type: String, // "deposit" or "withdraw"
}

#[derive(Debug, Serialize)]
pub struct BatchTransactionResponse {
    pub successful: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct AccountResponse {
    pub id: String,
    pub balance: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
    pub roles: Vec<UserRole>,
}

#[derive(Debug, Serialize)]
pub struct TransactionResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct AccountsResponse {
    pub accounts: Vec<AccountProjection>,
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn create_account(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(payload): Json<CreateAccountRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let headers = HeaderMap::new();
    let ctx = create_request_context(
        get_client_id(&headers),
        "create_account".to_string(),
        serde_json::to_string(&payload).unwrap(),
        headers,
    );
    let validation = service.middleware.process_request(ctx).await;
    if let Ok(result) = validation {
        if !result.is_valid {
            return Err((StatusCode::BAD_REQUEST, result.errors.join(", ")));
        }
    }
    let account = service
        .create_account(
            payload.owner_name,
            Decimal::from_f64(payload.initial_balance).unwrap_or(Decimal::ZERO),
        )
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(CreateAccountResponse {
        account_id: account,
    }))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn get_account(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let account = service
        .get_account(id)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    match account {
        Some(acc) => Ok(Json(AccountResponse {
            id: acc.id.to_string(),
            balance: acc.balance.to_f64().unwrap_or(0.0),
        })),
        None => Err((StatusCode::NOT_FOUND, "Account not found".to_string())),
    }
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn deposit_money(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service
        .deposit_money(id, payload.amount)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(TransactionResponse {
        success: true,
        message: "Deposit successful".to_string(),
    }))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn withdraw_money(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service
        .withdraw_money(id, payload.amount)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(TransactionResponse {
        success: true,
        message: "Withdrawal successful".to_string(),
    }))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn get_all_accounts(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
) -> Result<Json<AccountsResponse>, (StatusCode, Json<ErrorResponse>)> {
    match service.get_all_accounts().await {
        Ok(accounts) => Ok(Json(AccountsResponse { accounts })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn get_account_transactions(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Path(account_id): Path<Uuid>,
) -> Result<Json<Vec<TransactionProjection>>, (StatusCode, Json<ErrorResponse>)> {
    match service.get_account_transactions(account_id).await {
        Ok(transactions) => Ok(Json(transactions)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "healthy" }))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn metrics(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let metrics = service.get_metrics();
    Ok(Json(serde_json::json!({
        "commands_processed": metrics.commands_processed.load(std::sync::atomic::Ordering::Relaxed),
        "commands_failed": metrics.commands_failed.load(std::sync::atomic::Ordering::Relaxed),
        "projection_updates": metrics.projection_updates.load(std::sync::atomic::Ordering::Relaxed),
        "cache_hits": metrics.cache_hits.load(std::sync::atomic::Ordering::Relaxed),
        "cache_misses": metrics.cache_misses.load(std::sync::atomic::Ordering::Relaxed),
    })))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead. Auth logic might be centralized or use CQRS patterns too.")]
pub async fn login(
    State((_, auth_service)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(payload): Json<LoginRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let response = auth_service
        .login(&payload.username, &payload.password)
        .await
        .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;
    Ok(Json(response))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead. Auth logic might be centralized or use CQRS patterns too.")]
pub async fn logout(
    State((_, auth_service)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(payload): Json<LogoutRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    auth_service
        .blacklist_token(&payload.token)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "message": "Successfully logged out"
    })))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead.")]
pub async fn batch_transactions(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(request): Json<BatchTransactionRequest>,
) -> Result<Json<BatchTransactionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut successful = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for transaction in request.transactions {
        let result = match transaction.transaction_type.as_str() {
            "deposit" => {
                service
                    .deposit_money(transaction.account_id, transaction.amount)
                    .await
            }
            "withdraw" => {
                service
                    .withdraw_money(transaction.account_id, transaction.amount)
                    .await
            }
            _ => {
                errors
                    .push("Invalid transaction type: ".to_string() + &transaction.transaction_type);
                failed += 1;
                continue;
            }
        };

        match result {
            Ok(_) => successful += 1,
            Err(e) => {
                errors.push(
                    "Transaction failed for account ".to_string()
                        + &transaction.account_id.to_string()
                        + ": "
                        + &e.to_string(),
                );
                failed += 1;
            }
        }
    }

    Ok(Json(BatchTransactionResponse {
        successful,
        failed,
        errors,
    }))
}

#[deprecated(note = "Use handlers in src/web/cqrs_handlers.rs instead. Auth logic might be centralized or use CQRS patterns too.")]
pub async fn register(
    State((_, auth_service)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(payload): Json<RegisterRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    auth_service
        .register_user(
            &payload.username,
            &payload.email,
            &payload.password,
            payload.roles,
        )
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "message": "User registered successfully"
    })))
}

fn get_client_id(headers: &HeaderMap) -> String {
    headers
        .get("x-client-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string()
}

fn create_request_context(
    client_id: String,
    request_type: String,
    payload: String,
    headers: HeaderMap,
) -> RequestContext {
    RequestContext {
        client_id,
        request_type,
        payload,
        headers,
    }
}

/// Get log statistics
#[deprecated(note = "Consider moving to a dedicated operational/admin endpoint if needed, or use centralized logging platform.")]
pub async fn get_log_statistics() -> Result<Json<serde_json::Value>, StatusCode> {
    match get_log_stats("logs") {
        Ok(stats) => {
            let response = serde_json::json!({
                "total_files": stats.total_files,
                "total_size_bytes": stats.total_size,
                "total_size_mb": stats.total_size_mb(),
                "total_size_gb": stats.total_size_gb(),
                "files_last_24h": stats.files_last_24h,
                "timestamp": chrono::Utc::now().to_rfc3339()
            });

            // info!(
            //     "Log statistics requested: {} files, {:.2} MB",
            //     stats.total_files,
            //     stats.total_size_mb()
            // );

            Ok(Json(response))
        }
        Err(e) => {
            // error!("Failed to get log statistics: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
