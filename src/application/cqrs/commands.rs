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
        kafka_producer: Arc<crate::infrastructure::kafka_abstraction::KafkaProducer>,
    ) -> Self {
        let account_command_handler = Arc::new(AccountCommandHandler::new(
            event_store,
            projection_store,
            cache_service,
            kafka_producer,
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
    projection_store: Arc<dyn ProjectionStoreTrait>, // Kept for other handlers, will be removed if they are also refactored
    cache_service: Arc<dyn CacheServiceTrait>,       // Kept for other handlers, will be removed if they are also refactored
    kafka_producer: Arc<crate::infrastructure::kafka_abstraction::KafkaProducer>,
}

impl AccountCommandHandler {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>, // Kept for now
        cache_service: Arc<dyn CacheServiceTrait>,       // Kept for now
        kafka_producer: Arc<crate::infrastructure::kafka_abstraction::KafkaProducer>,
    ) -> Self {
        Self {
            event_store,
            projection_store,
            cache_service,
            kafka_producer,
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

                // Synchronous projection and cache updates are removed.
                // This will now be handled by KafkaEventProcessor consuming events.

                // Publish events to Kafka
                // For AccountCreated, the account object itself might not have an updated version yet (it's 0).
                // The event store handles versioning, so we pass the events and the initial version (0).
                // The Kafka message should contain the initial state or enough info for the consumer.
                // The `account.version` (which is 0 for a new account before first event is applied by store)
                // or specifically `0` for create. Event store `save_events` uses `expected_version`.
                // Kafka producer's `send_event_batch` takes `version` which implies the version *of the batch* or *after the batch*.
                // For create, the version *after* this first batch of events (usually 1 event) is 1.
                // Let's assume `account.version` after `handle_command` is still 0 (as it's a new aggregate state not yet reflecting store version).
                // The `events` vector contains one `AccountCreated` event.
                // The `self.event_store.save_events` call uses `0` as `expected_version`.
                // We should publish the events with the version they will have in the store, which is 1.
                // Or, if `send_event_batch` expects the version *of the aggregate before these events*, it would be 0.
                // Given `AccountRepository` uses `account.version` which *is* updated after apply_event,
                // let's apply the event to the local `account` instance to get its version updated for publishing.
                let mut local_account_for_version = account.clone();
                for event in &events {
                    local_account_for_version.apply_event(event); // This should set version to 0 for AccountCreated if it's the first event
                }
                // However, `EventStore` handles versioning. The first event gets version 1.
                // So, we publish with version 1.
                // A better way for `send_event_batch` might be to take `expected_version` and `events`,
                // and derive resulting versions internally or expect the final version of the last event.
                // For now, assuming `send_event_batch` wants the version of the aggregate *after* these events are applied.
                // The `events` themselves don't have versions yet. The `EventStore` assigns them.
                // The `EventBatch` struct in Kafka abstraction takes `version` - this should be the aggregate version *after* these events.
                // After `AccountCreated` event, version becomes 1.

                if let Err(e) = self
                    .kafka_producer
                    .send_event_batch(account_id, events.clone(), 1) // Version after AccountCreated is 1
                    .await
                {
                    error!(
                        "Failed to publish AccountCreated event to Kafka for account {}: {}",
                        account_id, e
                    );
                }

                info!(
                    "Account created successfully: {} in {:.2}s",
                    account_id,
                    start_time.elapsed().as_secs_f64()
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

                // Apply events to account (state is needed for CommandResult, but not for sync updates)
                // The actual state update for queries will happen via KafkaEventProcessor
                for event in &events {
                    account.apply_event(event);
                }

                // Synchronous projection and cache updates are removed.
                // This will now be handled by KafkaEventProcessor consuming events.

                // Note: Event publishing to Kafka will be handled in the next step,
                // ensuring it happens after successful event store commit.

                // Publish events to Kafka
                // The `account.version` here is the version *after* the current events have been applied,
                // which is typically what you'd associate with the batch of events just processed.
                if let Err(e) = self
                    .kafka_producer
                    .send_event_batch(account_id, events.clone(), account.version)
                    .await
                {
                    // Log error, but don't fail the command as event store commit was successful.
                    // For critical systems, a more robust outbox pattern might be needed.
                    error!(
                        "Failed to publish deposit events to Kafka for account {}: {}",
                        account_id, e
                    );
                }

                info!(
                    "Deposit successful: {} amount {} in {:.2}s",
                    account_id,
                    amount,
                    start_time.elapsed().as_secs_f64()
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

                // Apply events to account locally for potential immediate use (e.g. in CommandResult)
                for event in &events {
                    account.apply_event(event);
                }

                // Synchronous projection and cache updates are removed.
                // This will now be handled by KafkaEventProcessor consuming events.

                // Publish events to Kafka
                // `account.version` reflects the version after these events are applied.
                if let Err(e) = self
                    .kafka_producer
                    .send_event_batch(account_id, events.clone(), account.version)
                    .await
                {
                    error!(
                        "Failed to publish withdraw events to Kafka for account {}: {}",
                        account_id, e
                    );
                }

                info!(
                    "Withdrawal successful: {} amount {} in {:.2}s",
                    account_id,
                    amount,
                    start_time.elapsed().as_secs_f64()
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

                // Apply events to account locally for potential immediate use
                for event in &events {
                    account.apply_event(event);
                }

                // Synchronous projection update and cache invalidation are removed.
                // KafkaEventProcessor will handle updating projections (mark as inactive)
                // and cache invalidation/updates based on the AccountClosed event.

                // Publish events to Kafka
                // `account.version` reflects the version after these events are applied.
                if let Err(e) = self
                    .kafka_producer
                    .send_event_batch(account_id, events.clone(), account.version)
                    .await
                {
                    error!(
                        "Failed to publish close account events to Kafka for account {}: {}",
                        account_id, e
                    );
                }

                info!(
                    "Account closed successfully: {} in {:.2}s",
                    account_id,
                    start_time.elapsed().as_secs_f64()
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
