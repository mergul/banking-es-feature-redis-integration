// Authentication handlers only
// Business logic handlers are in cqrs_handlers.rs

use crate::application::services::CQRSAccountService;
use crate::domain::AccountError;
use crate::infrastructure::auth::{AuthService, LoginRequest, UserRole};
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

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateAccountRequest {
    pub owner_name: String,
    pub initial_balance: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
}

// Authentication handlers
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
                            "error": "Failed to create banking account",
                            "details": format!("{}", e)
                        })),
                    ))
                }
            }
        }
        Err(e) => {
            error!("Failed to register auth user {}: {}", payload.username, e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Failed to register user",
                    "details": format!("{}", e)
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

            // Get all banking accounts for this user
            let accounts_result = cqrs_service.get_all_accounts().await;
            let user_accounts = match accounts_result {
                Ok(accounts) => {
                    // Filter accounts for this specific user
                    accounts
                        .into_iter()
                        .filter(|account| account.owner_name == payload.username)
                        .collect::<Vec<_>>()
                }
                Err(e) => {
                    warn!(
                        "Failed to get accounts for user {}: {}",
                        payload.username, e
                    );
                    vec![]
                }
            };

            // Determine primary account (highest balance or first active)
            let primary_account = user_accounts
                .iter()
                .filter(|account| account.is_active)
                .max_by_key(|account| account.balance)
                .or_else(|| user_accounts.iter().find(|account| account.is_active))
                .or_else(|| user_accounts.first());

            let primary_account_id = primary_account.map(|account| account.id);

            Ok(Json(json!({
                "message": "Login successful",
                "token": {
                    "access_token": token,
                    "token_type": "Bearer"
                },
                "username": payload.username,
                "accounts": user_accounts,
                "primaryAccountId": primary_account_id
            })))
        }
        Err(e) => {
            error!("Login failed for user {}: {}", payload.username, e);
            Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": "Login failed",
                    "details": format!("{}", e)
                })),
            ))
        }
    }
}

pub async fn logout(
    State((_, auth_service)): State<(Arc<CQRSAccountService>, Arc<AuthService>)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // For now, just return success
    // In a real implementation, you might want to invalidate the token
    Ok(Json(json!({
        "message": "Logout successful"
    })))
}

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

            Ok(Json(json!({
                "username": username,
                "accounts": user_accounts,
                "count": user_accounts.len()
            })))
        }
        Err(e) => {
            error!("Failed to get accounts for user {}: {}", username, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to get user accounts",
                    "details": format!("{}", e)
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
            info!("Additional account created successfully: {}", account_id);
            Ok(Json(json!({
                "message": "Additional account created successfully",
                "account_id": account_id,
                "owner_name": owner_name,
                "initial_balance": initial_balance
            })))
        }
        Err(e) => {
            error!("Failed to create additional account: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to create additional account",
                    "details": format!("{}", e)
                })),
            ))
        }
    }
}
