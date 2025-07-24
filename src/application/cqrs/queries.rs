use crate::domain::AccountError;
use crate::infrastructure::{
    cache_service::CacheServiceTrait,
    projections::{AccountProjection, ProjectionStoreTrait, TransactionProjection},
};
use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Query handler trait for CQRS pattern
#[async_trait]
pub trait QueryHandler<Q, R>: Send + Sync {
    async fn handle(&self, query: Q) -> Result<R, AccountError>;
}

/// Query bus for routing queries to appropriate handlers
pub struct QueryBus {
    account_query_handler: Arc<AccountQueryHandler>,
}

impl QueryBus {
    pub fn new(
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_service: Arc<dyn CacheServiceTrait>,
    ) -> Self {
        let account_query_handler =
            Arc::new(AccountQueryHandler::new(projection_store, cache_service));

        Self {
            account_query_handler,
        }
    }

    pub async fn execute<Q, R>(&self, query: Q) -> Result<R, AccountError>
    where
        Q: Into<AccountQuery>,
        R: From<QueryResult>,
    {
        println!("[DEBUG] QueryBus::execute: start");
        let account_query = query.into();
        println!("[DEBUG] QueryBus::execute: before handle");
        let result = self.account_query_handler.handle(account_query).await;
        println!("[DEBUG] QueryBus::execute: after handle");
        let result = result?;
        Ok(R::from(result))
    }

    pub fn get_cache_metrics(&self) -> &crate::infrastructure::cache_service::CacheMetrics {
        self.account_query_handler.get_cache_metrics()
    }

    pub fn get_cache_service(&self) -> Arc<dyn CacheServiceTrait> {
        self.account_query_handler.get_cache_service()
    }
}

/// Result of query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub success: bool,
    pub data: Option<serde_json::Value>,
    pub message: String,
}

impl From<QueryResult> for Option<AccountProjection> {
    fn from(result: QueryResult) -> Self {
        if let Some(data) = result.data {
            serde_json::from_value(data).ok()
        } else {
            None
        }
    }
}

impl From<QueryResult> for Vec<AccountProjection> {
    fn from(result: QueryResult) -> Self {
        if let Some(data) = result.data {
            serde_json::from_value(data).unwrap_or_default()
        } else {
            Vec::new()
        }
    }
}

impl From<QueryResult> for Vec<TransactionProjection> {
    fn from(result: QueryResult) -> Self {
        if let Some(data) = result.data {
            serde_json::from_value(data).unwrap_or_default()
        } else {
            Vec::new()
        }
    }
}

impl From<QueryResult> for Decimal {
    fn from(result: QueryResult) -> Self {
        if let Some(data) = result.data {
            serde_json::from_value(data).unwrap_or_default()
        } else {
            Decimal::ZERO
        }
    }
}

impl From<QueryResult> for bool {
    fn from(result: QueryResult) -> Self {
        result.success
    }
}

/// Account query handler implementing CQRS read model
pub struct AccountQueryHandler {
    projection_store: Arc<dyn ProjectionStoreTrait>,
    cache_service: Arc<dyn CacheServiceTrait>,
}

impl AccountQueryHandler {
    pub fn new(
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_service: Arc<dyn CacheServiceTrait>,
    ) -> Self {
        Self {
            projection_store,
            cache_service,
        }
    }

    pub fn get_cache_metrics(&self) -> &crate::infrastructure::cache_service::CacheMetrics {
        self.cache_service.get_metrics()
    }

    pub fn get_cache_service(&self) -> Arc<dyn CacheServiceTrait> {
        self.cache_service.clone()
    }

    /// Get account by ID
    pub async fn get_account_by_id(
        &self,
        query: AccountQuery,
    ) -> Result<QueryResult, AccountError> {
        println!("[DEBUG] AccountQueryHandler::get_account_by_id: start");
        let start_time = Instant::now();

        match query {
            AccountQuery::GetAccountById { account_id } => {
                println!(
                    "[DEBUG] get_account_by_id: before cache lookup for {}",
                    account_id
                );
                // Try cache first
                let cache_result = self.cache_service.get_account(account_id).await;
                println!(
                    "[DEBUG] get_account_by_id: after cache lookup for {} (result: {:?})",
                    account_id, cache_result
                );
                if let Ok(Some(cached_account)) = cache_result {
                    // Increment cache hit metric
                    self.cache_service
                        .get_metrics()
                        .hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    info!("Cache hit for account {}", account_id);

                    // Convert Account to AccountProjection for consistency
                    let projection = crate::infrastructure::projections::AccountProjection {
                        id: cached_account.id,
                        owner_name: cached_account.owner_name.clone(),
                        balance: cached_account.balance,
                        is_active: cached_account.is_active,
                        created_at: chrono::Utc::now(), // Use current time as fallback
                        updated_at: chrono::Utc::now(), // Use current time as fallback
                    };

                    return Ok(QueryResult {
                        success: true,
                        data: Some(serde_json::to_value(projection).unwrap()),
                        message: "Account retrieved from cache".to_string(),
                    });
                }

                // Increment cache miss metric
                self.cache_service
                    .get_metrics()
                    .misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                println!(
                    "[DEBUG] get_account_by_id: before projection_store.get_account for {}",
                    account_id
                );
                // Cache miss - get from projection store
                let account = self
                    .projection_store
                    .get_account(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;
                println!("[DEBUG] get_account_by_id: after projection_store.get_account for {} (result: {:?})", account_id, account);

                match account {
                    Some(projection) => {
                        // Cache the result
                        match self.convert_projection_to_account(&projection) {
                            Ok(account_to_cache) => {
                                info!("[AccountQueryHandler] Attempting to cache account {} after projection store fetch.", account_id);
                                match self
                                    .cache_service
                                    .set_account(
                                        &account_to_cache,
                                        Some(std::time::Duration::from_secs(3600)),
                                    )
                                    .await
                                {
                                    Ok(_) => info!(
                                        "[AccountQueryHandler] Successfully cached account {}.",
                                        account_id
                                    ),
                                    Err(e) => {
                                        error!(
                                            "[AccountQueryHandler] Failed to cache account {}: {}",
                                            account_id, e
                                        );
                                        // Optionally, decide if this should be a hard error for the query
                                        // For now, just log, as the data was retrieved from projection.
                                    }
                                }
                            }
                            Err(e) => {
                                error!("[AccountQueryHandler] Failed to convert projection to account for caching for account_id {}: {:?}", account_id, e);
                            }
                        }

                        info!(
                            "Account retrieved from projection store: {} in {:.2}s",
                            account_id,
                            start_time.elapsed().as_secs_f64()
                        );

                        Ok(QueryResult {
                            success: true,
                            data: Some(serde_json::to_value(projection).unwrap()),
                            message: "Account retrieved successfully".to_string(),
                        })
                    }
                    None => Err(AccountError::NotFound),
                }
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid query type for get_account_by_id".to_string(),
            )),
        }
    }

    /// Get all accounts
    pub async fn get_all_accounts(&self, query: AccountQuery) -> Result<QueryResult, AccountError> {
        let start_time = Instant::now();

        match query {
            AccountQuery::GetAllAccounts => {
                let accounts = self
                    .projection_store
                    .get_all_accounts()
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                info!(
                    "All accounts retrieved: {} accounts in {:.2}s",
                    accounts.len(),
                    start_time.elapsed().as_secs_f64()
                );

                Ok(QueryResult {
                    success: true,
                    data: Some(serde_json::to_value(&accounts).unwrap()),
                    message: "Retrieved {} accounts".to_string() + &(accounts.len().to_string()),
                })
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid query for get all accounts handler".to_string(),
            )),
        }
    }

    /// Get account transactions
    pub async fn get_account_transactions(
        &self,
        query: AccountQuery,
    ) -> Result<QueryResult, AccountError> {
        let start_time = Instant::now();

        match query {
            AccountQuery::GetAccountTransactions { account_id } => {
                // Try cache first
                if let Ok(Some(cached_events)) =
                    self.cache_service.get_account_events(account_id).await
                {
                    // Increment cache hit metric
                    self.cache_service
                        .get_metrics()
                        .hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    info!("Cache hit for account transactions {}", account_id);
                    return Ok(QueryResult {
                        success: true,
                        data: Some(serde_json::to_value(cached_events).unwrap()),
                        message: "Account transactions retrieved from cache".to_string(),
                    });
                }

                // Increment cache miss metric
                self.cache_service
                    .get_metrics()
                    .misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let transactions = self
                    .projection_store
                    .get_account_transactions(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                // Cache the result
                match self.convert_transactions_to_events(&transactions) {
                    Ok(events_to_cache) => {
                        info!("[AccountQueryHandler] Attempting to cache account events for {} after projection store fetch.", account_id);
                        match self
                            .cache_service
                            .set_account_events(
                                account_id,
                                &events_to_cache,
                                Some(std::time::Duration::from_secs(1800)),
                            )
                            .await
                        {
                            Ok(_) => info!(
                                "[AccountQueryHandler] Successfully cached account events for {}.",
                                account_id
                            ),
                            Err(e) => {
                                error!("[AccountQueryHandler] Failed to cache account events for {}: {}", account_id, e);
                                // Optionally, decide if this should be a hard error.
                            }
                        }
                    }
                    Err(e) => {
                        error!("[AccountQueryHandler] Failed to convert transactions to events for caching for account_id {}: {:?}", account_id, e);
                    }
                }

                info!(
                    "Account transactions retrieved from projection store: {} transactions in {:.2}s",
                    transactions.len(),
                    start_time.elapsed().as_secs_f64()
                );

                Ok(QueryResult {
                    success: true,
                    data: Some(serde_json::to_value(&transactions).unwrap()),
                    message: "Retrieved {} transactions".to_string()
                        + &(transactions.len().to_string()),
                })
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid query for get account transactions handler".to_string(),
            )),
        }
    }

    /// Get account balance
    pub async fn get_account_balance(
        &self,
        query: AccountQuery,
    ) -> Result<QueryResult, AccountError> {
        let start_time = Instant::now();

        match query {
            AccountQuery::GetAccountBalance { account_id } => {
                // Try cache first
                if let Ok(Some(cached_account)) = self.cache_service.get_account(account_id).await {
                    // Increment cache hit metric
                    self.cache_service
                        .get_metrics()
                        .hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    info!("Cache hit for account balance {}", account_id);
                    return Ok(QueryResult {
                        success: true,
                        data: Some(serde_json::to_value(cached_account.balance).unwrap()),
                        message: "Account balance retrieved from cache".to_string(),
                    });
                }

                // Increment cache miss metric
                self.cache_service
                    .get_metrics()
                    .misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let account = self
                    .projection_store
                    .get_account(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                match account {
                    Some(projection) => {
                        info!(
                            "Account balance retrieved: {} balance {} in {:.2}s",
                            account_id,
                            projection.balance,
                            start_time.elapsed().as_secs_f64()
                        );

                        Ok(QueryResult {
                            success: true,
                            data: Some(serde_json::to_value(projection.balance).unwrap()),
                            message: "Account balance retrieved successfully".to_string(),
                        })
                    }
                    None => Err(AccountError::NotFound),
                }
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid query for get account balance handler".to_string(),
            )),
        }
    }

    /// Check if account is active
    pub async fn is_account_active(
        &self,
        query: AccountQuery,
    ) -> Result<QueryResult, AccountError> {
        let start_time = Instant::now();

        match query {
            AccountQuery::IsAccountActive { account_id } => {
                let account = self
                    .projection_store
                    .get_account(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                match account {
                    Some(projection) => {
                        info!(
                            "Account active status checked: {} active {} in {:.2}s",
                            account_id,
                            projection.is_active,
                            start_time.elapsed().as_secs_f64()
                        );

                        Ok(QueryResult {
                            success: true,
                            data: Some(serde_json::to_value(projection.is_active).unwrap()),
                            message: "Account active status checked successfully".to_string(),
                        })
                    }
                    None => Err(AccountError::NotFound),
                }
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid query for is account active handler".to_string(),
            )),
        }
    }

    /// Convert projection to domain account
    fn convert_projection_to_account(
        &self,
        projection: &AccountProjection,
    ) -> Result<crate::domain::Account, AccountError> {
        Ok(crate::domain::Account {
            id: projection.id,
            owner_name: projection.owner_name.clone(),
            balance: projection.balance,
            is_active: projection.is_active,
            version: 0, // Projections don't track version
        })
    }

    /// Convert transactions to events for caching
    fn convert_transactions_to_events(
        &self,
        transactions: &[TransactionProjection],
    ) -> Result<Vec<(i64, crate::domain::AccountEvent)>, AccountError> {
        let mut events = Vec::new();
        for (i, transaction) in transactions.iter().enumerate() {
            let event = match transaction.transaction_type.as_str() {
                "MoneyDeposited" => crate::domain::AccountEvent::MoneyDeposited {
                    account_id: transaction.account_id,
                    amount: transaction.amount,
                    transaction_id: transaction.id,
                },
                "MoneyWithdrawn" => crate::domain::AccountEvent::MoneyWithdrawn {
                    account_id: transaction.account_id,
                    amount: transaction.amount,
                    transaction_id: transaction.id,
                },
                "AccountCreated" => crate::domain::AccountEvent::AccountCreated {
                    account_id: transaction.account_id,
                    owner_name: "Unknown".to_string(),
                    initial_balance: transaction.amount,
                },
                "AccountClosed" => crate::domain::AccountEvent::AccountClosed {
                    account_id: transaction.account_id,
                    reason: "Unknown".to_string(),
                },
                _ => continue,
            };
            events.push((i as i64, event));
        }
        Ok(events)
    }
}

#[async_trait]
impl QueryHandler<AccountQuery, QueryResult> for AccountQueryHandler {
    async fn handle(&self, query: AccountQuery) -> Result<QueryResult, AccountError> {
        println!("[DEBUG] AccountQueryHandler::handle: start");
        match query {
            AccountQuery::GetAccountById { .. } => self.get_account_by_id(query).await,
            AccountQuery::GetAllAccounts => self.get_all_accounts(query).await,
            AccountQuery::GetAccountTransactions { .. } => {
                self.get_account_transactions(query).await
            }
            AccountQuery::GetAccountBalance { .. } => self.get_account_balance(query).await,
            AccountQuery::IsAccountActive { .. } => self.is_account_active(query).await,
        }
    }
}

/// Account query enum
#[derive(Debug, Clone)]
pub enum AccountQuery {
    GetAccountById { account_id: Uuid },
    GetAllAccounts,
    GetAccountTransactions { account_id: Uuid },
    GetAccountBalance { account_id: Uuid },
    IsAccountActive { account_id: Uuid },
}

// Query DTOs for API layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAccountQuery {
    pub account_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAccountTransactionsQuery {
    pub account_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAccountBalanceQuery {
    pub account_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsAccountActiveQuery {
    pub account_id: Uuid,
}

// Implement From traits for query conversion
impl From<GetAccountQuery> for AccountQuery {
    fn from(query: GetAccountQuery) -> Self {
        AccountQuery::GetAccountById {
            account_id: query.account_id,
        }
    }
}

impl From<GetAccountTransactionsQuery> for AccountQuery {
    fn from(query: GetAccountTransactionsQuery) -> Self {
        AccountQuery::GetAccountTransactions {
            account_id: query.account_id,
        }
    }
}

impl From<GetAccountBalanceQuery> for AccountQuery {
    fn from(query: GetAccountBalanceQuery) -> Self {
        AccountQuery::GetAccountBalance {
            account_id: query.account_id,
        }
    }
}

impl From<IsAccountActiveQuery> for AccountQuery {
    fn from(query: IsAccountActiveQuery) -> Self {
        AccountQuery::IsAccountActive {
            account_id: query.account_id,
        }
    }
}
