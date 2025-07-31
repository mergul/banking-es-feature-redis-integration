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
    State((cqrs_service, auth_service)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // Step 1: Create auth user
    let auth_result = auth_service
        .register_user(&payload.username, &payload.email, &payload.password, vec![])
        .await;

    match auth_result {
        Ok(_) => {
            info!("Auth user registered successfully: {}", payload.username);

            // Step 2: Create banking account for this user
            let initial_balance = Decimal::new(1000, 0); // Start with $1000
            let banking_account_result = cqrs_service
                .create_account(payload.username.clone(), initial_balance)
                .await;

            match banking_account_result {
                Ok(account_id) => {
                    info!(
                        "Banking account created successfully for user: {} with account_id: {}",
                        payload.username, account_id
                    );
                    Ok(Json(json!({
                        "message": "User and banking account created successfully",
                        "username": payload.username,
                        "account_id": account_id,
                        "initial_balance": initial_balance
                    })))
                }
                Err(e) => {
                    error!(
                        "Failed to create banking account for user {}: {}",
                        payload.username, e
                    );
                    // Auth user was created but banking account failed
                    // We could delete the auth user here, but for now just return error
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "error": "Registration partially failed",
                            "message": "Auth user created but banking account creation failed",
                            "details": e.to_string()
                        })),
                    ))
                }
            }
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
    State((cqrs_service, auth_service)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match auth_service
        .login(&payload.username, &payload.password)
        .await
    {
        Ok(token) => {
            info!("User logged in successfully: {}", payload.username);

            // Find all banking accounts for this user
            let accounts_result = cqrs_service.get_all_accounts().await;

            match accounts_result {
                Ok(accounts) => {
                    // Find all accounts owned by this user
                    let user_accounts: Vec<_> = accounts
                        .into_iter()
                        .filter(|account| account.owner_name == payload.username)
                        .collect();

                    if !user_accounts.is_empty() {
                        info!(
                            "Found {} banking accounts for user: {}",
                            user_accounts.len(),
                            payload.username
                        );

                        // Determine primary account (highest balance or first active)
                        let primary_account = user_accounts
                            .iter()
                            .filter(|acc| acc.is_active)
                            .max_by_key(|acc| acc.balance)
                            .or(user_accounts.first());

                        // Convert accounts to JSON format
                        let accounts_json: Vec<serde_json::Value> = user_accounts
                            .iter()
                            .map(|account| {
                                let is_primary =
                                    primary_account.map(|p| p.id == account.id).unwrap_or(false);
                                json!({
                                    "id": account.id,
                                    "balance": account.balance,
                                    "is_active": account.is_active,
                                    "is_primary": is_primary,
                                    "created_at": account.created_at
                                })
                            })
                            .collect();

                        Ok(Json(json!({
                            "message": "Login successful",
                            "token": token,
                            "username": payload.username,
                            "accounts": accounts_json,
                            "primary_account_id": primary_account.map(|acc| acc.id)
                        })))
                    } else {
                        warn!("No banking accounts found for user: {}", payload.username);
                        Ok(Json(json!({
                            "message": "Login successful (no banking accounts found)",
                            "token": token,
                            "username": payload.username,
                            "accounts": [],
                            "primary_account_id": null
                        })))
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to get accounts for user {}: {}",
                        payload.username, e
                    );
                    Ok(Json(json!({
                        "message": "Login successful (failed to get account info)",
                        "token": token,
                        "username": payload.username,
                        "accounts": [],
                        "primary_account_id": null
                    })))
                }
            }
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

// New account management handlers
pub async fn get_user_accounts(
    State((cqrs_service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Path(username): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match cqrs_service.get_all_accounts().await {
        Ok(accounts) => {
            let user_accounts: Vec<_> = accounts
                .into_iter()
                .filter(|account| account.owner_name == username)
                .collect();

            if !user_accounts.is_empty() {
                // Determine primary account (highest balance or first active)
                let primary_account = user_accounts
                    .iter()
                    .filter(|acc| acc.is_active)
                    .max_by_key(|acc| acc.balance)
                    .or(user_accounts.first());

                // Convert accounts to JSON format
                let accounts_json: Vec<serde_json::Value> = user_accounts
                    .iter()
                    .map(|account| {
                        let is_primary =
                            primary_account.map(|p| p.id == account.id).unwrap_or(false);
                        json!({
                            "id": account.id,
                            "balance": account.balance,
                            "is_active": account.is_active,
                            "is_primary": is_primary,
                            "created_at": account.created_at
                        })
                    })
                    .collect();

                Ok(Json(json!({
                    "message": "User accounts retrieved successfully",
                    "username": username,
                    "accounts": accounts_json,
                    "primary_account_id": primary_account.map(|acc| acc.id),
                    "total_accounts": user_accounts.len()
                })))
            } else {
                Ok(Json(json!({
                    "message": "No accounts found for user",
                    "username": username,
                    "accounts": [],
                    "primary_account_id": null,
                    "total_accounts": 0
                })))
            }
        }
        Err(e) => {
            error!("Failed to get accounts for user {}: {}", username, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to get user accounts",
                    "message": e.to_string()
                })),
            ))
        }
    }
}

pub async fn create_additional_account(
    State((cqrs_service, _)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
    Json(payload): Json<CreateAccountRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let owner_name = payload.owner_name.clone();
    let initial_balance = payload.initial_balance;

    match cqrs_service
        .create_account(owner_name.clone(), initial_balance)
        .await
    {
        Ok(account_id) => {
            info!(
                "Additional account created successfully: {} for user: {}",
                account_id, owner_name
            );
            Ok(Json(json!({
                "message": "Additional account created successfully",
                "account_id": account_id,
                "owner_name": owner_name,
                "initial_balance": initial_balance
            })))
        }
        Err(e) => {
            error!("Additional account creation failed: {}", e);
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
