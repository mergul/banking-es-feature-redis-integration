use axum::{
    body::Body,
    extract::{Path, State},
    http::{Request, Response, StatusCode},
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
use crate::infrastructure::redis_abstraction::RedisClientTrait;
use crate::infrastructure::repository::AccountRepositoryTrait;

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
    redis_client: Arc<dyn RedisClientTrait>,
    semaphore: Arc<Semaphore>,
    max_requests_per_second: usize,
}

impl RateLimitedService {
    pub fn new(service: AccountService, max_requests_per_second: usize) -> Self {
        let redis_client = service.redis_client.clone();
        Self {
            service: Arc::new(service),
            redis_client,
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
        let redis_client = self.redis_client.clone();
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
                let mut con = match redis_client.get_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Rate limiting: Failed to get Redis connection: {}", e);
                        return Ok(Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from("Rate limiting service unavailable"))
                            .unwrap());
                    }
                };

                let key = format!("cmd:{}", cmd_id);
                match con.set_nx_ex_bytes(key.as_bytes(), b"1", 3600).await {
                    Ok(true) => {
                        // Command ID is new, proceed with the request
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
                    }
                    Ok(false) => {
                        // Command ID already exists, return 409 Conflict
                        Ok(Response::builder()
                            .status(StatusCode::CONFLICT)
                            .body(Body::from("Duplicate command ID"))
                            .unwrap())
                    }
                    Err(e) => {
                        error!("Rate limiting: Redis error: {}", e);
                        Ok(Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from("Rate limiting service unavailable"))
                            .unwrap())
                    }
                }
            } else {
                // No command ID provided, proceed with the request
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
