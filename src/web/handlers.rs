// This file is deprecated - use cqrs_handlers.rs instead
// Remove all AccountService usage and replace with CQRSAccountService

use crate::application::services::CQRSAccountService;
use crate::domain::AccountError;
use crate::infrastructure::auth::{AuthService, LoginRequest, UserRole};
use crate::infrastructure::middleware::RequestMiddleware;
use crate::infrastructure::projections::ProjectionStoreTrait;
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use crate::infrastructure::repository::AccountRepositoryTrait;
use crate::infrastructure::{AccountRepository, UserRepository};
use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

// Replace AccountService with CQRSAccountService in all handler signatures
// Note: These handlers are deprecated - use CQRS handlers instead

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateAccountRequest {
    pub owner_name: String,
    pub initial_balance: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DepositRequest {
    pub amount: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WithdrawRequest {
    pub amount: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchTransaction {
    pub account_id: Uuid,
    pub transaction_type: String,
    pub amount: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
}

// Deprecated handlers - use CQRS handlers instead
pub async fn register(
    State((_, auth_service)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match auth_service
        .register_user(&payload.username, &payload.email, &payload.password, vec![])
        .await
    {
        Ok(_) => {
            info!("User registered successfully: {}", payload.username);
            Ok(Json(json!({
                "message": "User registered successfully",
                "username": payload.username
            })))
        }
        Err(e) => {
            error!("Registration failed: {}", e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Registration failed",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn login(
    State((_, auth_service)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match auth_service
        .login(&payload.username, &payload.password)
        .await
    {
        Ok(token) => {
            info!("User logged in successfully: {}", payload.username);
            Ok(Json(json!({
                "message": "Login successful",
                "token": token,
                "username": payload.username
            })))
        }
        Err(e) => {
            error!("Login failed: {}", e);
            Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": "Login failed",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn logout(
    State((_, auth_service)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // In a real implementation, you'd invalidate the token
    info!("User logged out successfully");
    Ok(Json(json!({
        "message": "Logout successful"
    })))
}

// Deprecated account handlers - use CQRS handlers instead
pub async fn create_account(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Json(payload): Json<CreateAccountRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match service
        .create_account(payload.owner_name, payload.initial_balance)
        .await
    {
        Ok(account_id) => {
            info!("Account created successfully: {}", account_id);
            Ok(Json(json!({
                "message": "Account created successfully",
                "account_id": account_id
            })))
        }
        Err(e) => {
            error!("Account creation failed: {}", e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Account creation failed",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn get_account(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match service.get_account(id).await {
        Ok(Some(account)) => Ok(Json(json!({
            "account": account
        }))),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "Account not found"
            })),
        )),
        Err(e) => {
            error!("Failed to get account: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to get account",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn deposit_money(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
    Json(payload): Json<DepositRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match service.deposit_money(id, payload.amount).await {
        Ok(_) => {
            info!("Deposit successful for account: {}", id);
            Ok(Json(json!({
                "message": "Deposit successful",
                "account_id": id,
                "amount": payload.amount
            })))
        }
        Err(e) => {
            error!("Deposit failed: {}", e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Deposit failed",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn withdraw_money(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Path(id): Path<Uuid>,
    Json(payload): Json<WithdrawRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match service.withdraw_money(id, payload.amount).await {
        Ok(_) => {
            info!("Withdrawal successful for account: {}", id);
            Ok(Json(json!({
                "message": "Withdrawal successful",
                "account_id": id,
                "amount": payload.amount
            })))
        }
        Err(e) => {
            error!("Withdrawal failed: {}", e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Withdrawal failed",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn get_all_accounts(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // Note: CQRSAccountService doesn't have get_all_accounts method
    // This would need to be implemented as a query
    Err((
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "get_all_accounts not implemented in CQRS service"
        })),
    ))
}

pub async fn get_account_transactions(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Path(account_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match service.get_account_transactions(account_id).await {
        Ok(transactions) => Ok(Json(json!({
            "transactions": transactions
        }))),
        Err(e) => {
            error!("Failed to get transactions: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to get transactions",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "banking-service",
        "version": "1.0.0"
    }))
}

pub async fn metrics(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
) -> Json<serde_json::Value> {
    let metrics = service.get_metrics();
    Json(serde_json::json!({
        "commands_processed": metrics.commands_processed.load(std::sync::atomic::Ordering::Relaxed),
        "commands_failed": metrics.commands_failed.load(std::sync::atomic::Ordering::Relaxed),
        "queries_processed": metrics.queries_processed.load(std::sync::atomic::Ordering::Relaxed),
        "queries_failed": metrics.queries_failed.load(std::sync::atomic::Ordering::Relaxed),
    }))
}

pub async fn batch_transactions(
    State((service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Json(transactions): Json<Vec<BatchTransaction>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // Convert to CQRS batch format
    let batch_transactions: Vec<crate::application::cqrs::handlers::BatchTransaction> =
        transactions
            .into_iter()
            .map(|t| crate::application::cqrs::handlers::BatchTransaction {
                account_id: t.account_id,
                transaction_type: t.transaction_type,
                amount: t.amount,
            })
            .collect();

    match service.batch_transactions(batch_transactions).await {
        Ok(result) => {
            info!("Batch transactions processed successfully");
            Ok(Json(json!({
                "message": "Batch transactions processed successfully",
                "successful": result.successful,
                "failed": result.failed
            })))
        }
        Err(e) => {
            error!("Batch transactions failed: {}", e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Batch transactions failed",
                    "message": e.to_string()
                })),
            ))
        }
    }
}
