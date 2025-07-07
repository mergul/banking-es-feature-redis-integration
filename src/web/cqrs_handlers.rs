use crate::application::cqrs::handlers::BatchTransaction;
use crate::application::services::cqrs_service::{
    BatchTransactionRequest, BatchTransactionResponse, CreateAccountRequest, CreateAccountResponse,
    TransactionRequest, TransactionResponse,
};
use crate::application::services::CQRSAccountService;
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

// Request/Response DTOs - only keep the ones not defined in cqrs_service
#[derive(Debug, Deserialize)]
pub struct CloseAccountRequest {
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct CloseAccountResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct AccountsResponse {
    pub accounts: Vec<AccountProjection>,
}

#[derive(Debug, Serialize)]
pub struct AccountResponse {
    pub account: AccountProjection,
}

#[derive(Debug, Serialize)]
pub struct AccountBalanceResponse {
    pub account_id: Uuid,
    pub balance: Decimal,
}

#[derive(Debug, Serialize)]
pub struct AccountStatusResponse {
    pub account_id: Uuid,
    pub is_active: bool,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub commands_processed: u64,
    pub commands_successful: u64,
    pub commands_failed: u64,
    pub queries_processed: u64,
    pub queries_successful: u64,
    pub queries_failed: u64,
    pub available_permits: usize,
    pub total_permits: usize,
    pub uptime_seconds: f64,
}

// CQRS-based handlers
pub async fn create_account(
    State(service): State<Arc<CQRSAccountService>>,
    Json(payload): Json<CreateAccountRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service
        .create_account(payload.owner_name, payload.initial_balance)
        .await
        .map(|account_id| {
            Json(CreateAccountResponse {
                account_id,
                message: "Account created successfully".to_string(),
            })
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

pub async fn get_account(
    State(service): State<Arc<CQRSAccountService>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match service.get_account(id).await {
        Ok(Some(account)) => Ok(Json(AccountResponse { account })),
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
    State(service): State<Arc<CQRSAccountService>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
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

pub async fn deposit_money(
    State(service): State<Arc<CQRSAccountService>>,
    Path(id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service
        .deposit_money(id, payload.amount)
        .await
        .map(|_| {
            Json(TransactionResponse {
                success: true,
                message: "Deposit successful".to_string(),
            })
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

pub async fn withdraw_money(
    State(service): State<Arc<CQRSAccountService>>,
    Path(id): Path<Uuid>,
    Json(payload): Json<TransactionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service
        .withdraw_money(id, payload.amount)
        .await
        .map(|_| {
            Json(TransactionResponse {
                success: true,
                message: "Withdrawal successful".to_string(),
            })
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

pub async fn close_account(
    State(service): State<Arc<CQRSAccountService>>,
    Path(id): Path<Uuid>,
    Json(payload): Json<CloseAccountRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    service
        .close_account(id, payload.reason)
        .await
        .map(|_| {
            Json(CloseAccountResponse {
                success: true,
                message: "Account closed successfully".to_string(),
            })
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

pub async fn get_account_transactions(
    State(service): State<Arc<CQRSAccountService>>,
    Path(account_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
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

pub async fn get_account_balance(
    State(service): State<Arc<CQRSAccountService>>,
    Path(account_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match service.get_account_balance(account_id).await {
        Ok(balance) => Ok(Json(AccountBalanceResponse {
            account_id,
            balance,
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn is_account_active(
    State(service): State<Arc<CQRSAccountService>>,
    Path(account_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match service.is_account_active(account_id).await {
        Ok(is_active) => Ok(Json(AccountStatusResponse {
            account_id,
            is_active,
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn batch_transactions(
    State(service): State<Arc<CQRSAccountService>>,
    Json(request): Json<BatchTransactionRequest>,
) -> Result<Json<BatchTransactionResponse>, (StatusCode, Json<ErrorResponse>)> {
    match service.batch_transactions(request.transactions).await {
        Ok(result) => Ok(Json(BatchTransactionResponse { result })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn health_check(
    State(service): State<Arc<CQRSAccountService>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match service.health_check().await {
        Ok(health) => Ok(Json(HealthResponse {
            status: health.status,
            commands_processed: health.commands_processed,
            commands_successful: health.commands_successful,
            commands_failed: health.commands_failed,
            queries_processed: health.queries_processed,
            queries_successful: health.queries_successful,
            queries_failed: health.queries_failed,
            available_permits: health.available_permits,
            total_permits: health.total_permits,
            uptime_seconds: health.uptime.as_secs_f64(),
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn metrics(
    State(service): State<Arc<CQRSAccountService>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metrics = service.get_metrics();
    let response = serde_json::json!({
        "commands_processed": metrics.commands_processed.load(std::sync::atomic::Ordering::Relaxed),
        "commands_successful": metrics.commands_successful.load(std::sync::atomic::Ordering::Relaxed),
        "commands_failed": metrics.commands_failed.load(std::sync::atomic::Ordering::Relaxed),
        "queries_processed": metrics.queries_processed.load(std::sync::atomic::Ordering::Relaxed),
        "queries_successful": metrics.queries_successful.load(std::sync::atomic::Ordering::Relaxed),
        "queries_failed": metrics.queries_failed.load(std::sync::atomic::Ordering::Relaxed),
    });
    Ok(Json(response))
}
