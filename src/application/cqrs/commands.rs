use crate::domain::{Account, AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::{
    cache_service::CacheServiceTrait,
    event_store::EventStoreTrait,
    projections::{AccountProjection, ProjectionStoreTrait, TransactionProjection},
    repository::AccountRepositoryTrait,
};
use anyhow::Result;
use async_trait::async_trait;
use bincode;
use chrono::Utc;
use rust_decimal::Decimal;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Command handler trait for CQRS pattern
#[async_trait]
pub trait CommandHandler<C, R>: Send + Sync {
    async fn handle(&self, command: C) -> Result<R, AccountError>;
}

/// Command bus for routing commands to appropriate handlers
pub struct CommandBus {
    account_command_handler: Arc<AccountCommandHandler>,
}

impl CommandBus {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_service: Arc<dyn CacheServiceTrait>,
    ) -> Self {
        let account_command_handler = Arc::new(AccountCommandHandler::new(
            event_store,
            projection_store,
            cache_service,
        ));

        Self {
            account_command_handler,
        }
    }

    pub async fn execute<C, R>(&self, command: C) -> Result<R, AccountError>
    where
        C: Into<AccountCommand>,
        R: From<CommandResult>,
    {
        let account_command = command.into();
        let result = self.account_command_handler.handle(account_command).await?;
        Ok(R::from(result))
    }
}

/// Result of command execution
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub success: bool,
    pub account_id: Option<Uuid>,
    pub events: Vec<AccountEvent>,
    pub message: String,
}

impl From<CommandResult> for Uuid {
    fn from(result: CommandResult) -> Self {
        result.account_id.unwrap_or_default()
    }
}

impl From<CommandResult> for () {
    fn from(_: CommandResult) -> Self {}
}

/// Account command handler implementing CQRS write model
pub struct AccountCommandHandler {
    event_store: Arc<dyn EventStoreTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    cache_service: Arc<dyn CacheServiceTrait>,
}

impl AccountCommandHandler {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_service: Arc<dyn CacheServiceTrait>,
    ) -> Self {
        Self {
            event_store,
            projection_store,
            cache_service,
        }
    }

    /// Handle create account command
    pub async fn handle_create_account(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        let start_time = Instant::now();

        match command {
            AccountCommand::CreateAccount {
                account_id,
                ref owner_name,
                initial_balance,
            } => {
                // Create new account aggregate
                let account = Account::new(account_id, owner_name.clone(), initial_balance)?;
                let command_for_handling = AccountCommand::CreateAccount {
                    account_id,
                    owner_name: owner_name.clone(),
                    initial_balance,
                };
                let events = account.handle_command(&command_for_handling)?;

                // Save events to event store
                self.event_store
                    .save_events(account_id, events.clone(), 0)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                // Update projections asynchronously
                let projection = AccountProjection {
                    id: account_id,
                    owner_name: owner_name.clone(),
                    balance: initial_balance,
                    is_active: true,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };

                if let Err(e) = self
                    .projection_store
                    .upsert_accounts_batch(vec![projection])
                    .await
                {
                    let _ = std::io::stderr().write_all(
                        ("Failed to update projection for account ".to_string()
                            + &account_id.to_string()
                            + &": ".to_string()
                            + &e.to_string()
                            + "\n")
                            .as_bytes(),
                    );
                }

                // Cache the account
                self.cache_service
                    .set_account(&account, Some(std::time::Duration::from_secs(3600)))
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                let _ = std::io::stderr().write_all(
                    ("Account created successfully: ".to_string()
                        + &account_id.to_string()
                        + &" in ".to_string()
                        + &start_time.elapsed().as_secs_f64().to_string()
                        + "\n")
                        .as_bytes(),
                );

                Ok(CommandResult {
                    success: true,
                    account_id: Some(account_id),
                    events,
                    message: "Account created successfully".to_string(),
                })
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid command for create account handler".to_string(),
            )),
        }
    }

    /// Handle deposit money command
    pub async fn handle_deposit_money(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        let start_time = Instant::now();

        match command {
            AccountCommand::DepositMoney { account_id, amount } => {
                // Get current account state
                let mut account = self.get_account_state(account_id).await?;
                let events = account.handle_command(&command)?;

                // Save events to event store
                self.event_store
                    .save_events(account_id, events.clone(), account.version)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                // Apply events to account
                for event in &events {
                    account.apply_event(event);
                }

                // Update projections asynchronously
                let projection = AccountProjection {
                    id: account.id,
                    owner_name: account.owner_name.clone(),
                    balance: account.balance,
                    is_active: account.is_active,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };

                if let Err(e) = self
                    .projection_store
                    .upsert_accounts_batch(vec![projection])
                    .await
                {
                    let _ = std::io::stderr().write_all(
                        ("Failed to update projection for account ".to_string()
                            + &account_id.to_string()
                            + &": ".to_string()
                            + &e.to_string()
                            + "\n")
                            .as_bytes(),
                    );
                }

                // Update transaction projection
                for event in &events {
                    if let AccountEvent::MoneyDeposited { amount, .. } = event {
                        let transaction = TransactionProjection {
                            id: Uuid::new_v4(),
                            account_id,
                            transaction_type: "MoneyDeposited".to_string(),
                            amount: *amount,
                            timestamp: Utc::now(),
                        };

                        if let Err(e) = self
                            .projection_store
                            .insert_transactions_batch(vec![transaction])
                            .await
                        {
                            let _ = std::io::stderr().write_all(
                                ("Failed to insert transaction projection: ".to_string()
                                    + &e.to_string()
                                    + "\n")
                                    .as_bytes(),
                            );
                        }
                    }
                }

                // Update cache
                self.cache_service
                    .set_account(&account, Some(std::time::Duration::from_secs(3600)))
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                let _ = std::io::stderr().write_all(
                    ("Deposit successful: ".to_string()
                        + &account_id.to_string()
                        + &" amount ".to_string()
                        + &amount.to_string()
                        + &" in ".to_string()
                        + &start_time.elapsed().as_secs_f64().to_string()
                        + "\n")
                        .as_bytes(),
                );

                Ok(CommandResult {
                    success: true,
                    account_id: Some(account_id),
                    events,
                    message: "Deposit successful".to_string(),
                })
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid command for deposit handler".to_string(),
            )),
        }
    }

    /// Handle withdraw money command
    pub async fn handle_withdraw_money(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        let start_time = Instant::now();

        match command {
            AccountCommand::WithdrawMoney { account_id, amount } => {
                // Get current account state
                let mut account = self.get_account_state(account_id).await?;
                let events = account.handle_command(&command)?;

                // Save events to event store
                self.event_store
                    .save_events(account_id, events.clone(), account.version)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                // Apply events to account
                for event in &events {
                    account.apply_event(event);
                }

                // Update projections asynchronously
                let projection = AccountProjection {
                    id: account.id,
                    owner_name: account.owner_name.clone(),
                    balance: account.balance,
                    is_active: account.is_active,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };

                if let Err(e) = self
                    .projection_store
                    .upsert_accounts_batch(vec![projection])
                    .await
                {
                    let _ = std::io::stderr().write_all(
                        ("Failed to update projection for account ".to_string()
                            + &account_id.to_string()
                            + &": ".to_string()
                            + &e.to_string()
                            + "\n")
                            .as_bytes(),
                    );
                }

                // Update transaction projection
                for event in &events {
                    if let AccountEvent::MoneyWithdrawn { amount, .. } = event {
                        let transaction = TransactionProjection {
                            id: Uuid::new_v4(),
                            account_id,
                            transaction_type: "MoneyWithdrawn".to_string(),
                            amount: *amount,
                            timestamp: Utc::now(),
                        };

                        if let Err(e) = self
                            .projection_store
                            .insert_transactions_batch(vec![transaction])
                            .await
                        {
                            let _ = std::io::stderr().write_all(
                                ("Failed to insert transaction projection: ".to_string()
                                    + &e.to_string()
                                    + "\n")
                                    .as_bytes(),
                            );
                        }
                    }
                }

                // Update cache
                self.cache_service
                    .set_account(&account, Some(std::time::Duration::from_secs(3600)))
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                let _ = std::io::stderr().write_all(
                    ("Withdrawal successful: ".to_string()
                        + &account_id.to_string()
                        + &" amount ".to_string()
                        + &amount.to_string()
                        + &" in ".to_string()
                        + &start_time.elapsed().as_secs_f64().to_string()
                        + "\n")
                        .as_bytes(),
                );

                Ok(CommandResult {
                    success: true,
                    account_id: Some(account_id),
                    events,
                    message: "Withdrawal successful".to_string(),
                })
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid command for withdraw handler".to_string(),
            )),
        }
    }

    /// Handle close account command
    pub async fn handle_close_account(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        let start_time = Instant::now();

        match command {
            AccountCommand::CloseAccount {
                account_id,
                ref reason,
            } => {
                // Get current account state
                let mut account = self.get_account_state(account_id).await?;
                let command_for_handling = AccountCommand::CloseAccount {
                    account_id,
                    reason: reason.clone(),
                };
                let events = account.handle_command(&command_for_handling)?;

                // Save events to event store
                self.event_store
                    .save_events(account_id, events.clone(), account.version)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                // Apply events to account
                for event in &events {
                    account.apply_event(event);
                }

                // Update projections asynchronously
                let projection = AccountProjection {
                    id: account.id,
                    owner_name: account.owner_name.clone(),
                    balance: account.balance,
                    is_active: account.is_active,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };

                if let Err(e) = self
                    .projection_store
                    .upsert_accounts_batch(vec![projection])
                    .await
                {
                    let _ = std::io::stderr().write_all(
                        ("Failed to update projection for account ".to_string()
                            + &account_id.to_string()
                            + &": ".to_string()
                            + &e.to_string()
                            + "\n")
                            .as_bytes(),
                    );
                }

                // Invalidate cache
                self.cache_service
                    .delete_account(account_id)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                let _ = std::io::stderr().write_all(
                    ("Account closed successfully: ".to_string()
                        + &account_id.to_string()
                        + &" in ".to_string()
                        + &start_time.elapsed().as_secs_f64().to_string()
                        + "\n")
                        .as_bytes(),
                );

                Ok(CommandResult {
                    success: true,
                    account_id: Some(account_id),
                    events,
                    message: "Account closed: {}".to_string() + &(reason).to_string(),
                })
            }
            _ => Err(AccountError::InfrastructureError(
                "Invalid command for close account handler".to_string(),
            )),
        }
    }

    /// Get current account state by replaying events
    async fn get_account_state(&self, account_id: Uuid) -> Result<Account, AccountError> {
        // Try cache first
        if let Ok(Some(cached_account)) = self.cache_service.get_account(account_id).await {
            return Ok(cached_account);
        }

        // Get events from event store and replay them
        let events = self
            .event_store
            .get_events(account_id, None)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        if events.is_empty() {
            return Err(AccountError::NotFound);
        }

        // Replay events to build current state
        let mut account = Account::default();
        for event in events {
            // Convert Event to AccountEvent
            let account_event: AccountEvent = bincode::deserialize(&event.event_data)
                .map_err(|e| AccountError::EventDeserializationError(e.to_string()))?;
            account.apply_event(&account_event);
        }

        Ok(account)
    }
}

#[async_trait]
impl CommandHandler<AccountCommand, CommandResult> for AccountCommandHandler {
    async fn handle(&self, command: AccountCommand) -> Result<CommandResult, AccountError> {
        match command {
            AccountCommand::CreateAccount { .. } => self.handle_create_account(command).await,
            AccountCommand::DepositMoney { .. } => self.handle_deposit_money(command).await,
            AccountCommand::WithdrawMoney { .. } => self.handle_withdraw_money(command).await,
            AccountCommand::CloseAccount { .. } => self.handle_close_account(command).await,
        }
    }
}

// Command DTOs for API layer
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CreateAccountCommand {
    pub owner_name: String,
    pub initial_balance: Decimal,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DepositMoneyCommand {
    pub account_id: Uuid,
    pub amount: Decimal,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WithdrawMoneyCommand {
    pub account_id: Uuid,
    pub amount: Decimal,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CloseAccountCommand {
    pub account_id: Uuid,
    pub reason: String,
}

// Implement From traits for command conversion
impl From<CreateAccountCommand> for AccountCommand {
    fn from(cmd: CreateAccountCommand) -> Self {
        AccountCommand::CreateAccount {
            account_id: Uuid::new_v4(),
            owner_name: cmd.owner_name,
            initial_balance: cmd.initial_balance,
        }
    }
}

impl From<DepositMoneyCommand> for AccountCommand {
    fn from(cmd: DepositMoneyCommand) -> Self {
        AccountCommand::DepositMoney {
            account_id: cmd.account_id,
            amount: cmd.amount,
        }
    }
}

impl From<WithdrawMoneyCommand> for AccountCommand {
    fn from(cmd: WithdrawMoneyCommand) -> Self {
        AccountCommand::WithdrawMoney {
            account_id: cmd.account_id,
            amount: cmd.amount,
        }
    }
}

impl From<CloseAccountCommand> for AccountCommand {
    fn from(cmd: CloseAccountCommand) -> Self {
        AccountCommand::CloseAccount {
            account_id: cmd.account_id,
            reason: cmd.reason,
        }
    }
}
