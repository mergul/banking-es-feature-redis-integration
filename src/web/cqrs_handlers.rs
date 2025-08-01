use crate::application::cqrs::commands::CommandResult;
use crate::application::cqrs::handlers::BatchTransaction;
use crate::application::services::cqrs_service::{
    BatchTransactionRequest, BatchTransactionResponse, CreateAccountRequest, CreateAccountResponse,
    TransactionRequest, TransactionResponse,
};
use crate::application::services::CQRSAccountService;
use crate::domain::{AccountCommand, AccountEvent};
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
use std::collections::HashMap;
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

#[derive(serde::Serialize)]
pub struct VersionResponse {
    pub aggregate_id: Uuid,
    pub version: i64,
    pub exists: bool,
}

#[derive(serde::Serialize)]
pub struct VersionsResponse {
    pub versions: HashMap<Uuid, i64>,
    pub count: usize,
}

#[derive(serde::Serialize)]
pub struct AggregateInfoResponse {
    pub aggregate_id: Uuid,
    pub version: i64,
    pub is_active: bool,
    pub exists: bool,
}

#[derive(serde::Deserialize)]
pub struct BatchOperationsRequest {
    pub operations: Vec<BatchOperation>,
}

#[derive(serde::Deserialize)]
pub struct BatchOperation {
    pub operation_type: String, // "deposit", "withdraw"
    pub account_id: Uuid,
    pub amount: Decimal,
}

#[derive(serde::Serialize)]
pub struct BatchOperationsResponse {
    pub account_ids: Vec<Uuid>,
    pub count: usize,
    pub message: String,
}

#[derive(serde::Deserialize)]
pub struct BatchCommandsRequest {
    pub commands: Vec<BatchCommand>,
}

#[derive(serde::Deserialize)]
pub struct BatchCommand {
    pub command_type: String, // "CreateAccount", "DepositMoney", "WithdrawMoney", "CloseAccount"
    pub account_id: Option<Uuid>, // None for CreateAccount
    pub owner_name: Option<String>, // Only for CreateAccount
    pub initial_balance: Option<Decimal>, // Only for CreateAccount
    pub amount: Option<Decimal>, // Only for Deposit/Withdraw
    pub reason: Option<String>, // Only for CloseAccount
}

#[derive(serde::Serialize)]
pub struct BatchCommandsResponse {
    pub results: Vec<CommandResult>,
    pub count: usize,
    pub message: String,
}

pub async fn execute_batch_operations(
    State(service): State<Arc<CQRSAccountService>>,
    Json(request): Json<BatchOperationsRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let operations: Vec<(String, Uuid, Decimal)> = request
        .operations
        .into_iter()
        .map(|op| (op.operation_type, op.account_id, op.amount))
        .collect();

    match service.execute_batch_operations(operations).await {
        Ok(account_ids) => {
            let count = account_ids.len();
            let response = BatchOperationsResponse {
                account_ids,
                count,
                message: "Batch operations completed successfully".to_string(),
            };
            Ok(Json(response))
        }
        Err(e) => {
            let error_response = ErrorResponse {
                error: format!("Batch operations failed: {}", e),
            };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)))
        }
    }
}

pub async fn execute_batch_commands(
    State(service): State<Arc<CQRSAccountService>>,
    Json(request): Json<BatchCommandsRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    // Convert BatchCommand to AccountCommand
    let commands: Vec<AccountCommand> = request
        .commands
        .into_iter()
        .filter_map(|cmd| match cmd.command_type.as_str() {
            "CreateAccount" => {
                if let (Some(owner_name), Some(initial_balance)) =
                    (cmd.owner_name, cmd.initial_balance)
                {
                    Some(AccountCommand::CreateAccount {
                        account_id: Uuid::new_v4(),
                        owner_name,
                        initial_balance,
                    })
                } else {
                    None
                }
            }
            "DepositMoney" => {
                if let (Some(account_id), Some(amount)) = (cmd.account_id, cmd.amount) {
                    Some(AccountCommand::DepositMoney { account_id, amount })
                } else {
                    None
                }
            }
            "WithdrawMoney" => {
                if let (Some(account_id), Some(amount)) = (cmd.account_id, cmd.amount) {
                    Some(AccountCommand::WithdrawMoney { account_id, amount })
                } else {
                    None
                }
            }
            "CloseAccount" => {
                if let (Some(account_id), Some(reason)) = (cmd.account_id, cmd.reason) {
                    Some(AccountCommand::CloseAccount { account_id, reason })
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();

    // Use the command handler's batch method through the service
    match service.execute_batch_commands(commands).await {
        Ok(results) => {
            let count = results.len();
            let response = BatchCommandsResponse {
                results,
                count,
                message: "Batch commands completed successfully".to_string(),
            };
            Ok(Json(response))
        }
        Err(e) => {
            let error_response = ErrorResponse {
                error: format!("Batch commands failed: {}", e),
            };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)))
        }
    }
}

pub async fn get_aggregate_version(
    State(service): State<Arc<CQRSAccountService>>,
    Path(aggregate_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match service.get_aggregate_version(aggregate_id).await {
        Ok(version) => {
            let response = VersionResponse {
                aggregate_id,
                version,
                exists: true,
            };
            Ok(Json(response))
        }
        Err(e) => {
            let error_response = ErrorResponse {
                error: format!("Version retrieval failed: {}", e),
            };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)))
        }
    }
}

pub async fn get_aggregate_versions(
    State(service): State<Arc<CQRSAccountService>>,
    Json(aggregate_ids): Json<Vec<Uuid>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match service.get_aggregate_versions(aggregate_ids).await {
        Ok(versions) => {
            let response = VersionsResponse {
                count: versions.len(),
                versions,
            };
            Ok(Json(response))
        }
        Err(e) => {
            let error_response = ErrorResponse {
                error: format!("Batch version retrieval failed: {}", e),
            };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)))
        }
    }
}

pub async fn get_aggregate_info(
    State(service): State<Arc<CQRSAccountService>>,
    Path(aggregate_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match service.get_aggregate_info(aggregate_id).await {
        Ok(Some((version, is_active))) => {
            let response = AggregateInfoResponse {
                aggregate_id,
                version,
                is_active,
                exists: true,
            };
            Ok(Json(response))
        }
        Ok(None) => {
            let response = AggregateInfoResponse {
                aggregate_id,
                version: 0,
                is_active: false,
                exists: false,
            };
            Ok(Json(response))
        }
        Err(e) => {
            let error_response = ErrorResponse {
                error: format!("Aggregate info retrieval failed: {}", e),
            };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)))
        }
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
