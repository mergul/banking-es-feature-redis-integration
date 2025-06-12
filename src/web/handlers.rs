use axum::{
    body::Body,
    extract::{Path, State},
    http::{Request, Response, StatusCode, HeaderMap},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use rust_decimal::{Decimal, prelude::{ToPrimitive, FromPrimitive}};
use serde::{Deserialize, Serialize};
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

use crate::application::AccountService;
use crate::domain::{AccountCommand, AccountError};
use crate::infrastructure::{
    auth::{AuthService, Claims, LoginRequest, LoginResponse, LogoutRequest, PasswordResetResponse, UserRole, AuthConfig},
    cache_service::{CacheService, CacheConfig, EvictionPolicy},
    event_store::{EventStore, EventStoreConfig},
    kafka_abstraction::KafkaConfig,
    middleware::{RequestMiddleware, RequestContext, AccountCreationValidator, TransactionValidator},
    projections::{AccountProjection, TransactionProjection, ProjectionStore, ProjectionConfig},
    rate_limiter::RateLimitConfig,
    redis_abstraction::{RedisClient, RealRedisClient, RedisPoolConfig},
    repository::{AccountRepository, AccountRepositoryTrait},
    scaling::{ScalingManager, ServiceInstance, InstanceMetrics, ScalingConfig},
    sharding::{ShardManager, ShardConfig, LockManager},
};

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

pub async fn create_account(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(payload): Json<CreateAccountRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let ctx = create_request_context(
        get_client_id(&HeaderMap::new()),
        "create_account".to_string(),
        serde_json::to_value(&payload).unwrap(),
        HeaderMap::new(),
    );
    let validation = service.middleware.process_request(ctx).await;
    if let Ok(result) = validation {
        if !result.is_valid {
            return Err((StatusCode::BAD_REQUEST, result.errors.join(", ")));
        }
    }
    let account = service.create_account(payload.owner_name, Decimal::from_f64(payload.initial_balance).unwrap_or(Decimal::ZERO)).await.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(CreateAccountResponse { account_id: account }))
}

pub async fn get_account(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let account = service.get_account(id).await.map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    match account {
        Some(acc) => Ok(Json(AccountResponse {
            id: acc.id.to_string(),
            balance: acc.balance.to_f64().unwrap_or(0.0),
        })),
        None => Err((StatusCode::NOT_FOUND, "Account not found".to_string())),
    }
}

pub async fn deposit_money(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service.deposit_money(id, payload.amount).await.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(StatusCode::OK)
}

pub async fn withdraw_money(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service.withdraw_money(id, payload.amount).await.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(StatusCode::OK)
}

pub async fn get_all_accounts(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
) -> Result<Json<Vec<AccountProjection>>, (StatusCode, Json<ErrorResponse>)> {
    match service.get_all_accounts().await {
        Ok(accounts) => Ok(Json(accounts)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

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

pub async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn metrics(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let metrics = serde_json::json!({
        "available_request_permits": service.semaphore.available_permits(),
        "max_requests_per_second": service.max_requests_per_second,
    });

    Ok(Json(metrics))
}

pub async fn create_routes() -> Result<Router<(Arc<AccountService>, Arc<AuthService>)>, anyhow::Error> {
    // Initialize Redis client
    let redis_client = Arc::new(redis::Client::open("redis://127.0.0.1/")?);
    let redis_client_trait = RealRedisClient::new(
        redis_client.as_ref().clone(),
        Some(RedisPoolConfig::default()),
    );

    // Initialize AuthService
    let auth_config = AuthConfig {
        jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".to_string()),
        refresh_token_secret: std::env::var("REFRESH_TOKEN_SECRET").unwrap_or_else(|_| "default_refresh_secret".to_string()),
        access_token_expiry: 3600,
        refresh_token_expiry: 604800,
        rate_limit_requests: 1000,
        rate_limit_window: 60,
        max_failed_attempts: 5,
        lockout_duration_minutes: 30,
    };
    let auth_service = Arc::new(AuthService::new(redis_client.clone(), auth_config));

    // Initialize CacheService
    let cache_config = CacheConfig {
        default_ttl: Duration::from_secs(3600),
        max_size: 10000,
        shard_count: 16,
        warmup_batch_size: 100,
        warmup_interval: Duration::from_secs(300),
        eviction_policy: EvictionPolicy::LRU,
    };
    let cache_service = Arc::new(CacheService::new(redis_client_trait.clone(), cache_config));

    // Initialize ShardManager
    let shard_config = ShardConfig {
        shard_count: 16,
        rebalance_threshold: 0.2,
        rebalance_interval: Duration::from_secs(300),
        lock_timeout: Duration::from_secs(30),
        lock_retry_interval: Duration::from_millis(100),
        max_retries: 3,
    };
    let shard_manager = Arc::new(ShardManager::new(redis_client_trait.clone(), shard_config));

    // Initialize RequestMiddleware
    let rate_limit_config = crate::infrastructure::middleware::RateLimitConfig {
        requests_per_minute: 100,
        burst_size: 20,
        window_size: Duration::from_secs(60),
        max_clients: 1000,
    };
    let middleware = Arc::new(RequestMiddleware::new(rate_limit_config));
    middleware.register_validator("create_account".to_string(), Box::new(AccountCreationValidator));
    middleware.register_validator("deposit_money".to_string(), Box::new(TransactionValidator));
    middleware.register_validator("withdraw_money".to_string(), Box::new(TransactionValidator));

    // Initialize ScalingManager
    let scaling_config = ScalingConfig {
        min_instances: 1,
        max_instances: 10,
        scale_up_threshold: 0.8,
        scale_down_threshold: 0.2,
        cooldown_period: Duration::from_secs(300),
        health_check_interval: Duration::from_secs(30),
        instance_timeout: Duration::from_secs(60),
    };
    let scaling_manager = Arc::new(ScalingManager::new(redis_client_trait.clone(), scaling_config));

    // Initialize EventStore and ProjectionStore
    let event_store = Arc::new(EventStore::new_with_pool_size(10).await?);
    let projection_store = Arc::new(ProjectionStore::new_with_config(ProjectionConfig::default()).await?);

    // Initialize AccountRepository
    let repository = Arc::new(AccountRepository::new(
        event_store.as_ref().clone(),
        KafkaConfig::default(),
        projection_store.as_ref().clone(),
        redis_client.as_ref().clone(),
    )?);

    // Initialize AccountService
    let service = Arc::new(AccountService::new(
        repository.clone(),
        projection_store.as_ref().clone(),
        cache_service.as_ref().clone(),
        middleware.clone(),
        100,
    ));

    // Create router with routes
    let router = Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics))
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/accounts", get(get_all_accounts))
        .route("/accounts/:id", get(get_account))
        .route("/accounts", post(create_account))
        .route("/accounts/:id/deposit", post(deposit_money))
        .route("/accounts/:id/withdraw", post(withdraw_money))
        .route("/accounts/:id/transactions", get(get_account_transactions))
        .route("/batch-transactions", post(batch_transactions))
        .with_state((service, auth_service));

    Ok(router)
}

pub async fn login(
    State((_, auth_service)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(payload): Json<LoginRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    match auth_service.login(&payload.username, &payload.password).await {
        Ok(token) => Ok(Json(token)),
        Err(e) => Err((StatusCode::UNAUTHORIZED, e.to_string())),
    }
}

pub async fn logout(
    State((_, auth_service)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(payload): Json<LogoutRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    match auth_service.blacklist_token(&payload.token).await {
        Ok(_) => Ok(StatusCode::OK),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

pub async fn batch_transactions(
    State((service, _)): State<(Arc<AccountService>, Arc<AuthService>)>,
    Json(request): Json<BatchTransactionRequest>,
) -> Result<Json<BatchTransactionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = service.semaphore.acquire().await.unwrap();

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
            _ => Err(AccountError::InvalidAmount(transaction.amount)),
        };

        match result {
            Ok(()) => successful += 1,
            Err(e) => {
                failed += 1;
                errors.push(format!(
                    "Account {}: {}",
                    transaction.account_id,
                    e.to_string()
                ));
            }
        }
    }

    Ok(Json(BatchTransactionResponse {
        successful,
        failed,
        errors,
    }))
}

fn get_client_id(headers: &HeaderMap) -> String {
    headers
        .get("X-Client-ID")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string()
}

fn create_request_context(
    client_id: String,
    request_type: String,
    payload: serde_json::Value,
    headers: HeaderMap,
) -> RequestContext {
    RequestContext {
        client_id,
        request_type,
        payload,
        headers,
    }
}
