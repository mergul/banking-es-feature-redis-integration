// Authentication handlers only
// Business logic handlers are in cqrs_handlers.rs

use crate::application::services::CQRSAccountService;
use crate::domain::AccountError;
use crate::infrastructure::auth::{AuthService, LoginRequest, UserRole};
use crate::infrastructure::websocket::WebSocketManager;
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
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
}

// Authentication handlers
pub async fn register(
    State((cqrs_service, auth_service, ws_manager)): State<(
        Arc<CQRSAccountService>,
        Arc<AuthService>,
        Arc<WebSocketManager>,
    )>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // Step 1: Create auth user
    let auth_result = auth_service
        .register_user(
            &payload.username,
            &payload.email,
            &payload.password,
            vec![crate::infrastructure::auth::UserRole::Customer],
        )
        .await;

    match auth_result {
        Ok(auth_user) => {
            info!(
                "Auth user registered successfully: {} with ID: {}",
                payload.username, auth_user.id
            );

            // Step 2: Create banking account for this user using auth_user.id
            let initial_balance = Decimal::new(1000, 0); // Start with $1000
            let banking_account_result = cqrs_service
                .create_account_with_auth_user(
                    auth_user.id,
                    payload.username.clone(),
                    initial_balance,
                )
                .await;

            match banking_account_result {
                Ok(account_id) => {
                    info!(
                        "Banking account created successfully for user: {} with account_id: {} and auth_user_id: {}",
                        payload.username, account_id, auth_user.id
                    );

                    // Broadcast account creation via WebSocket
                    ws_manager.broadcast_account_created(
                        &payload.username,
                        &account_id.to_string(),
                        &initial_balance.to_string(),
                    );

                    // Also broadcast projection update
                    ws_manager.broadcast_projection_updated(
                        &payload.username,
                        &account_id.to_string(),
                        &initial_balance.to_string(),
                        true, // is_active
                    );

                    Ok(Json(json!({
                        "message": "User and banking account created successfully",
                        "username": payload.username,
                        "auth_user_id": auth_user.id,
                        "account_id": account_id,
                        "initial_balance": initial_balance,
                        "account_created_at": chrono::Utc::now().to_rfc3339(),
                        "account_status": "active"
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
    State((cqrs_service, auth_service, ws_manager)): State<(
        Arc<CQRSAccountService>,
        Arc<AuthService>,
        Arc<WebSocketManager>,
    )>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match auth_service
        .login(&payload.username, &payload.password)
        .await
    {
        Ok(token) => {
            info!("User logged in successfully: {}", payload.username);

            // Get accounts for this specific user efficiently
            let user_accounts = match cqrs_service.get_accounts_by_owner(&payload.username).await {
                Ok(accounts) => {
                    info!(
                        "Retrieved {} accounts for user {}",
                        accounts.len(),
                        payload.username
                    );
                    accounts
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

            // Broadcast projection updates via WebSocket for each account
            for account in &user_accounts {
                ws_manager.broadcast_projection_updated(
                    &payload.username,
                    &account.id.to_string(),
                    &account.balance.to_string(),
                    account.is_active,
                );
            }

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
    State((_, auth_service, _)): State<(
        Arc<CQRSAccountService>,
        Arc<AuthService>,
        Arc<WebSocketManager>,
    )>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // For now, just return success
    // In a real implementation, you might want to invalidate the token
    Ok(Json(json!({
        "message": "Logout successful"
    })))
}

pub async fn get_user_accounts(
    State((cqrs_service, _, _)): State<(
        Arc<CQRSAccountService>,
        Arc<AuthService>,
        Arc<WebSocketManager>,
    )>,
    Path(username): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match cqrs_service.get_accounts_by_owner(&username).await {
        Ok(user_accounts) => {
            info!(
                "Retrieved {} accounts for user {}",
                user_accounts.len(),
                username
            );

            Ok(Json(json!({
                "message": "Accounts retrieved successfully",
                "token": {
                    "access_token": "temp_token"
                },
                "username": username,
                "accounts": user_accounts,
                "primaryAccountId": user_accounts.first().map(|acc| acc.id)
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
