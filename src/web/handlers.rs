use axum::{
    body::Body,
    extract::{Path, State},
    http::{Request, Response, StatusCode, HeaderMap},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Semaphore;
use tower::{Layer, Service, ServiceBuilder, ServiceExt};
use tower_http::trace::TraceLayer;
use tracing::{error, info, Level};
use uuid::Uuid;

use crate::application::AccountService;
use crate::domain::{AccountCommand, AccountError};
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use crate::infrastructure::repository::AccountRepositoryTrait;
use crate::infrastructure::middleware::{RequestMiddleware, RequestContext, MiddlewareResult};
use crate::infrastructure::middleware::{AccountCreationValidator, TransactionValidator};
use crate::infrastructure::rate_limiter::RateLimitConfig;
use crate::infrastructure::repository::AccountRepositoryTrait;
use crate::infrastructure::projections::{ProjectionStore, AccountProjection, TransactionProjection};
use crate::infrastructure::cache_service::CacheService;
use crate::infrastructure::event_store::EventStore;
use crate::infrastructure::kafka_abstraction::KafkaConfig;
use crate::infrastructure::redis_abstraction::RedisClient;
use crate::infrastructure::scaling::{ScalingManager, ServiceInstance, InstanceMetrics};
use crate::infrastructure::sharding::{ShardManager, ShardConfig, DistributedLock, LockManager};
use anyhow::Result;
use crate::infrastructure::auth::{AuthService, UserRole, Claims, AuthError};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Deserialize)]
pub struct CreateAccountRequest {
    pub owner_name: String,
    pub initial_balance: Decimal,
}

#[derive(Debug, Serialize)]
pub struct CreateAccountResponse {
    pub account_id: Uuid,
}

#[derive(Debug, Deserialize)]
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

#[derive(Clone)]
pub struct RateLimitedService {
    service: Arc<AccountService>,
    semaphore: Arc<Semaphore>,
    max_requests_per_second: usize,
}

impl RateLimitedService {
    pub fn new(service: AccountService, max_requests_per_second: usize) -> Self {
        Self {
            service: Arc::new(service),
            semaphore: Arc::new(Semaphore::new(max_requests_per_second)),
            max_requests_per_second,
        }
    }
}

impl Service<Request<Body>> for RateLimitedService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let service = self.service.clone();
        let permit = self.semaphore.clone().acquire_owned();
        let rate_limited = self.clone();

        Box::pin(async move {
            let _permit = permit.await.unwrap();

            // Rate limiting logic here
            let command_id = req
                .headers()
                .get("X-Command-ID")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| uuid::Uuid::parse_str(s).ok());

            if let Some(cmd_id) = command_id {
                // Check if command ID is already in the service's command cache
                if service.is_duplicate_command(cmd_id).await {
                    return Ok(Response::builder()
                        .status(StatusCode::CONFLICT)
                        .body(Body::from("Duplicate command ID"))
                        .unwrap());
                }
            }

            // No command ID provided or not a duplicate, proceed with the request
            let router = Router::new()
                .route("/accounts", get(get_all_accounts))
                .route("/accounts/:id", get(get_account))
                .route("/accounts/:id/transactions", get(get_account_transactions))
                .route("/accounts", post(create_account))
                .route("/accounts/:id/deposit", post(deposit_money))
                .route("/accounts/:id/withdraw", post(withdraw_money))
                .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()))
                .with_state(rate_limited.clone());

            match router.oneshot(req).await {
                Ok(resp) => Ok(resp),
                Err(e) => Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(format!("Internal server error: {}", e)))
                    .unwrap()),
            }
        })
    }
}

pub async fn create_account(
    State(rate_limited): State<RateLimitedService>,
    Json(request): Json<CreateAccountRequest>,
) -> Result<Json<CreateAccountResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.semaphore.acquire().await.unwrap();

    match rate_limited
        .service
        .create_account(request.owner_name, request.initial_balance)
        .await
    {
        Ok(account_id) => Ok(Json(CreateAccountResponse { account_id })),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn get_account(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
) -> Result<Json<AccountProjection>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.semaphore.acquire().await.unwrap();

    match rate_limited.service.get_account(account_id).await {
        Ok(Some(account)) => Ok(Json(account)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Account not found".to_string(),
            }),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn get_all_accounts(
    State(rate_limited): State<RateLimitedService>,
) -> Result<Json<Vec<AccountProjection>>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.semaphore.acquire().await.unwrap();

    match rate_limited.service.get_all_accounts().await {
        Ok(accounts) => Ok(Json(accounts)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn deposit_money(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
    Json(request): Json<TransactionRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.semaphore.acquire().await.unwrap();

    match rate_limited
        .service
        .deposit_money(account_id, request.amount)
        .await
    {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn withdraw_money(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
    Json(request): Json<TransactionRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.semaphore.acquire().await.unwrap();

    match rate_limited
        .service
        .withdraw_money(account_id, request.amount)
        .await
    {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn batch_transactions(
    State(rate_limited): State<RateLimitedService>,
    Json(request): Json<BatchTransactionRequest>,
) -> Result<Json<BatchTransactionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.semaphore.acquire().await.unwrap();

    let mut successful = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for transaction in request.transactions {
        let result = match transaction.transaction_type.as_str() {
            "deposit" => {
                rate_limited
                    .service
                    .deposit_money(transaction.account_id, transaction.amount)
                    .await
            }
            "withdraw" => {
                rate_limited
                    .service
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

pub async fn get_account_transactions(
    State(rate_limited): State<RateLimitedService>,
    Path(account_id): Path<Uuid>,
) -> Result<Json<Vec<TransactionProjection>>, (StatusCode, Json<ErrorResponse>)> {
    let _permit = rate_limited.semaphore.acquire().await.unwrap();

    match rate_limited
        .service
        .get_account_transactions(account_id)
        .await
    {
        Ok(transactions) => Ok(Json(transactions)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn health_check() -> StatusCode {
    StatusCode::OK
}

pub async fn metrics(
    State(rate_limited): State<RateLimitedService>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Return current system metrics
    let available_permits = rate_limited.semaphore.available_permits();

    let metrics = serde_json::json!({
        "available_request_permits": available_permits,
        "max_requests_per_second": rate_limited.max_requests_per_second,
    });

    Ok(Json(metrics))
}

pub struct AppState {
    repository: Arc<dyn AccountRepositoryTrait>,
    projections: ProjectionStore,
    cache_service: CacheService,
    middleware: RequestMiddleware,
    event_store: Arc<EventStore>,
    kafka_config: KafkaConfig,
    redis_client: Arc<RedisClient>,
    scaling_manager: Arc<ScalingManager>,
    shard_manager: Arc<ShardManager>,
    lock_manager: Arc<LockManager>,
    auth_service: Arc<AuthService>,
}

impl AppState {
    pub fn new(
        repository: Arc<dyn AccountRepositoryTrait>,
        projections: ProjectionStore,
        cache_service: CacheService,
        event_store: EventStore,
        kafka_config: KafkaConfig,
        redis_client: Arc<RedisClient>,
        scaling_manager: Arc<ScalingManager>,
        auth_service: Arc<AuthService>,
    ) -> Self {
        let shard_config = ShardConfig::default();
        let shard_manager = Arc::new(ShardManager::new(redis_client.clone(), shard_config));
        let lock_manager = Arc::new(LockManager::new(redis_client.clone()));

        let rate_limit_config = RateLimitConfig {
            requests_per_minute: 1000,
            burst_size: 100,
            window_size: 60,
        };

        let middleware = RequestMiddleware::new(rate_limit_config);
        middleware.register_validator("create_account", Box::new(AccountCreationValidator::new()));
        middleware.register_validator("transaction", Box::new(TransactionValidator::new()));

        Self {
            repository,
            projections,
            cache_service,
            middleware,
            event_store: Arc::new(event_store),
            kafka_config,
            redis_client,
            scaling_manager,
            shard_manager,
            lock_manager,
            auth_service,
        }
    }
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
        user_id: headers
            .get("X-User-ID")
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        request_type,
        payload,
        headers,
    }
}

pub fn create_routes(
    event_store: EventStore,
    kafka_config: KafkaConfig,
    projections: ProjectionStore,
    redis_client: Arc<RedisClient>,
    scaling_manager: Arc<ScalingManager>,
    auth_service: Arc<AuthService>,
) -> Router {
    let state = AppState::new(
        Arc::new(AccountRepository::new(event_store.clone(), kafka_config.clone(), projections.clone())),
        projections,
        CacheService::new(redis_client.clone()),
        event_store,
        kafka_config,
        redis_client,
        scaling_manager,
        auth_service.clone(),
    );

    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh_token))
        .route("/auth/logout", post(logout))
        .route("/auth/change-password", post(change_password))
        .route("/auth/reset-password", post(request_password_reset))
        .route("/accounts", post(create_account))
        .route("/accounts/:id", get(get_account))
        .route("/accounts/:id/deposit", post(deposit_money))
        .route("/accounts/:id/withdraw", post(withdraw_money))
        .route("/health", get(health_check))
        .route("/metrics", get(get_metrics))
        .with_state(state)
}

async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorResponse>)> {
    // In a real application, you would validate credentials against a database
    // For this example, we'll use a simple check
    if payload.username == "admin" && payload.password == "admin123" {
        let token = state
            .auth_service
            .generate_token(&payload.username, vec![UserRole::Admin])
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Failed to generate token: {}", e),
                    }),
                )
            })?;

        Ok(Json(LoginResponse {
            token,
            user_id: payload.username,
            roles: vec![UserRole::Admin],
        }))
    } else if payload.username == "manager" && payload.password == "manager123" {
        let token = state
            .auth_service
            .generate_token(&payload.username, vec![UserRole::BankManager])
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Failed to generate token: {}", e),
                    }),
                )
            })?;

        Ok(Json(LoginResponse {
            token,
            user_id: payload.username,
            roles: vec![UserRole::BankManager],
        }))
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Invalid credentials".to_string(),
            }),
        ))
    }
}

async fn logout(
    State(state): State<AppState>,
    Json(payload): Json<LogoutRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    state
        .auth_service
        .blacklist_token(&payload.token)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to blacklist token: {}", e),
                }),
            )
        })?;

    Ok(StatusCode::OK)
}

async fn create_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateAccountRequest>,
) -> impl IntoResponse {
    let claims = headers
        .get("x-user-claims")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| serde_json::from_str::<Claims>(s).ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Missing authentication context".to_string(),
                }),
            )
        })?;

    let client_id = get_client_id(&headers);
    let request_context = create_request_context(
        client_id,
        "create_account".to_string(),
        serde_json::to_value(&payload).unwrap(),
        headers,
    );

    match state.middleware.process_request(request_context).await {
        Ok(_) => {
            // Acquire distributed lock for account creation
            let lock_key = format!("account:create:{}", client_id);
            let lock = match state.lock_manager.acquire_lock(&lock_key, Duration::from_secs(30)).await {
                Ok(lock) => lock,
                Err(e) => {
                    error!("Failed to acquire lock for account creation: {}", e);
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(ErrorResponse {
                            error: "Service temporarily unavailable".to_string(),
                        }),
                    ).into_response();
                }
            };

            // Create account and get shard assignment
            match state.repository.create_account(payload.owner_name, payload.initial_balance).await {
                Ok(account_id) => {
                    // Assign account to appropriate shard
                    if let Err(e) = state.shard_manager.assign_shard(
                        &format!("shard-{}", account_id.as_u128() % 16),
                        &state.scaling_manager.get_instance_id(),
                    ).await {
                        error!("Failed to assign shard for account {}: {}", account_id, e);
                    }

                    lock.release_lock().await.unwrap_or_else(|e| {
                        error!("Failed to release lock: {}", e);
                    });

                    (
                        StatusCode::CREATED,
                        Json(CreateAccountResponse { account_id }),
                    ).into_response()
                }
                Err(e) => {
                    lock.release_lock().await.unwrap_or_else(|e| {
                        error!("Failed to release lock: {}", e);
                    });

                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!("Failed to create account: {}", e),
                        }),
                    ).into_response()
                }
            }
        }
        Err(e) => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse {
                error: format!("Rate limit exceeded: {}", e),
            }),
        ).into_response(),
    }
}

async fn get_account(
    State(state): State<AppState>,
    Path(account_id): Path<Uuid>,
) -> impl IntoResponse {
    match state.repository.get_by_id(account_id).await {
        Ok(Some(account)) => (StatusCode::OK, Json(AccountResponse {
            id: account.id.to_string(),
            balance: account.balance.to_f64().unwrap_or(0.0),
        })),
        Ok(None) => (StatusCode::NOT_FOUND, Json(AccountResponse {
            id: String::new(),
            balance: 0.0,
        })),
        Err(e) => {
            error!("Failed to get account: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(AccountResponse {
                id: String::new(),
                balance: 0.0,
            }))
        }
    }
}

async fn deposit_money(
    State(state): State<AppState>,
    Path(account_id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> impl IntoResponse {
    // Get shard for account
    let shard = match state.shard_manager.get_shard_for_key(&account_id.to_string()).await {
        Ok(Some(shard)) => shard,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "Account not found".to_string(),
                }),
            ).into_response();
        }
        Err(e) => {
            error!("Failed to get shard for account {}: {}", account_id, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            ).into_response();
        }
    };

    // Verify shard is assigned to this instance
    if let Some(instance_id) = &shard.instance_id {
        if instance_id != &state.scaling_manager.get_instance_id() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "Account is currently being processed by another instance".to_string(),
                }),
            ).into_response();
        }
    }

    // Acquire distributed lock for transaction
    let lock_key = format!("account:transaction:{}", account_id);
    let lock = match state.lock_manager.acquire_lock(&lock_key, Duration::from_secs(30)).await {
        Ok(lock) => lock,
        Err(e) => {
            error!("Failed to acquire lock for transaction: {}", e);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "Service temporarily unavailable".to_string(),
                }),
            ).into_response();
        }
    };

    match state.repository.deposit_money(account_id, payload.amount).await {
        Ok(_) => {
            lock.release_lock().await.unwrap_or_else(|e| {
                error!("Failed to release lock: {}", e);
            });

            StatusCode::OK.into_response()
        }
        Err(e) => {
            lock.release_lock().await.unwrap_or_else(|e| {
                error!("Failed to release lock: {}", e);
            });

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to deposit money: {}", e),
                }),
            ).into_response()
        }
    }
}

async fn withdraw_money(
    State(state): State<AppState>,
    Path(account_id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> impl IntoResponse {
    // Get shard for account
    let shard = match state.shard_manager.get_shard_for_key(&account_id.to_string()).await {
        Ok(Some(shard)) => shard,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "Account not found".to_string(),
                }),
            ).into_response();
        }
        Err(e) => {
            error!("Failed to get shard for account {}: {}", account_id, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            ).into_response();
        }
    };

    // Verify shard is assigned to this instance
    if let Some(instance_id) = &shard.instance_id {
        if instance_id != &state.scaling_manager.get_instance_id() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "Account is currently being processed by another instance".to_string(),
                }),
            ).into_response();
        }
    }

    // Acquire distributed lock for transaction
    let lock_key = format!("account:transaction:{}", account_id);
    let lock = match state.lock_manager.acquire_lock(&lock_key, Duration::from_secs(30)).await {
        Ok(lock) => lock,
        Err(e) => {
            error!("Failed to acquire lock for transaction: {}", e);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "Service temporarily unavailable".to_string(),
                }),
            ).into_response();
        }
    };

    match state.repository.withdraw_money(account_id, payload.amount).await {
        Ok(_) => {
            lock.release_lock().await.unwrap_or_else(|e| {
                error!("Failed to release lock: {}", e);
            });

            StatusCode::OK.into_response()
        }
        Err(e) => {
            lock.release_lock().await.unwrap_or_else(|e| {
                error!("Failed to release lock: {}", e);
            });

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to withdraw money: {}", e),
                }),
            ).into_response()
        }
    }
}

async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let instance = ServiceInstance {
        id: Uuid::new_v4().to_string(),
        host: "localhost".to_string(),
        port: 8080,
        status: crate::infrastructure::scaling::InstanceStatus::Active,
        metrics: InstanceMetrics::default(),
        shard_assignments: vec![],
        last_heartbeat: chrono::Utc::now(),
    };

    match state.scaling_manager.register_instance(instance).await {
        Ok(_) => StatusCode::OK,
        Err(e) => {
            error!("Health check failed: {}", e);
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

#[derive(Serialize)]
struct MetricsResponse {
    instance_metrics: InstanceMetrics,
    total_instances: usize,
    active_instances: usize,
}

async fn get_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = state.scaling_manager.get_instance_metrics().await;
    let total_instances = state.scaling_manager.get_total_instances().await;
    let active_instances = state.scaling_manager.get_active_instances().await;

    Json(MetricsResponse {
        instance_metrics: metrics,
        total_instances,
        active_instances,
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PasswordResetRequest {
    pub email: String,
}

async fn refresh_token(
    State(state): State<AppState>,
    Json(payload): Json<RefreshTokenRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorResponse>)> {
    match state.auth_service.refresh_token(&payload.refresh_token).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => Err((StatusCode::UNAUTHORIZED, Json(ErrorResponse { error: e.to_string() }))),
    }
}

async fn change_password(
    State(state): State<AppState>,
    claims: Claims,
    Json(payload): Json<ChangePasswordRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    match state
        .auth_service
        .change_password(&claims.sub, &payload.current_password, &payload.new_password)
        .await
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(e) => Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e.to_string() }))),
    }
}

async fn request_password_reset(
    State(state): State<AppState>,
    Json(payload): Json<PasswordResetRequest>,
) -> Result<Json<PasswordResetResponse>, (StatusCode, Json<ErrorResponse>)> {
    match state.auth_service.request_password_reset(&payload.email).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e.to_string() }))),
    }
}
