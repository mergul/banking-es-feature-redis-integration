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
    // projection_store: Arc<dyn ProjectionStoreTrait>, // No longer needed if all handlers are refactored
    // cache_service: Arc<dyn CacheServiceTrait>,       // No longer needed if all handlers are refactored
    // kafka_producer: Arc<crate::infrastructure::kafka_abstraction::KafkaProducer>, // Removed, replaced by outbox
    outbox_repository: Arc<crate::infrastructure::OutboxRepositoryTrait>, // ADDED
    db_pool: Arc<sqlx::PgPool>, // ADDED - to manage transactions for outbox + event store
    kafka_config: Arc<crate::infrastructure::kafka_abstraction::KafkaConfig>, // ADDED - for topic names
}

impl AccountCommandHandler {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        // projection_store: Arc<dyn ProjectionStoreTrait>, // Remove if not needed
        // cache_service: Arc<dyn CacheServiceTrait>,       // Remove if not needed
        outbox_repository: Arc<crate::infrastructure::OutboxRepositoryTrait>, // ADDED
        db_pool: Arc<sqlx::PgPool>, // ADDED
        kafka_config: Arc<crate::infrastructure::kafka_abstraction::KafkaConfig>, // ADDED
    ) -> Self {
        Self {
            event_store,
            // projection_store,
            // cache_service,
            outbox_repository,
            db_pool,
            outbox_repository,
            db_pool,
            kafka_config,
        }
    }

    // Helper to generate event_id for outbox message
    fn generate_event_id_for_outbox(event: &AccountEvent) -> Uuid {
        match event {
            AccountEvent::MoneyDeposited { transaction_id, .. } => *transaction_id,
            AccountEvent::MoneyWithdrawn { transaction_id, .. } => *transaction_id,
            // For AccountCreated and AccountClosed, transaction_id might not be present directly in the event struct
            // or might not be the desired unique ID for the event itself.
            // Here, we generate a new UUID if no specific ID is available from the event.
            // Alternatively, domain events could be designed to always carry a unique event_id.
            _ => Uuid::new_v4(),
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
                let account = Account::new(account_id, owner_name.clone(), initial_balance)?;
                let command_for_handling = AccountCommand::CreateAccount {
                    account_id,
                    owner_name: owner_name.clone(),
                    initial_balance,
                };
                let events = account.handle_command(&command_for_handling)?;

                let mut tx = self.db_pool.begin().await.map_err(|e| AccountError::InfrastructureError(format!("DB Error: {}", e)))?;

                // CONCEPTUAL: self.event_store.save_events_in_transaction(&mut tx, ...).await
                // For now, assuming EventStore commits its own transaction before outbox write.
                // This is the main atomicity challenge with the current EventStore design.
                self.event_store
                    .save_events(account_id, events.clone(), 0)
                    .await
                    .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

                let outbox_messages: Vec<crate::infrastructure::OutboxMessage> = events.iter().map(|event| {
                    crate::infrastructure::OutboxMessage {
                        aggregate_id: account_id,
                        event_id: Self::generate_event_id_for_outbox(event),
                        event_type: event.event_type().to_string(),
                        payload: bincode::serialize(event).unwrap_or_default(),
                        topic: self.kafka_config.event_topic.clone(),
                        metadata: None,
                    }
                }).collect();

                if let Err(e) = self.outbox_repository.add_pending_messages(&mut tx, outbox_messages).await {
                    error!("Outbox write failed for account {}: {}. Attempting rollback.", account_id, e);
                    // Note: Rollback here won't affect EventStore if it committed separately.
                    tx.rollback().await.map_err(|re| AccountError::InfrastructureError(format!("DB Rollback Error: {}", re)))?;
                    return Err(AccountError::InfrastructureError(format!("Outbox write failed: {}", e)));
                }

                tx.commit().await.map_err(|e| AccountError::InfrastructureError(format!("DB Commit Error: {}", e)))?;

                // Removed direct Kafka publishing
                // Removed direct projection/cache updates

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

                // Start Transaction
                let mut tx = self.db_pool.begin().await.map_err(|e| AccountError::InfrastructureError(format!("DB Error starting transaction: {}", e)))?;

                // CONCEPTUAL: EventStore operation should ideally use `tx`
                // self.event_store.save_events_in_transaction(&mut tx, account_id, events.clone(), account.version).await...
                // Current: EventStore manages its own transaction, risk of partial failure.
                match self.event_store.save_events(account_id, events.clone(), account.version).await {
                    Ok(_) => {
                        let outbox_messages: Vec<crate::infrastructure::OutboxMessage> = events.iter().map(|event| {
                            crate::infrastructure::OutboxMessage {
                                aggregate_id: account_id,
                                event_id: Self::generate_event_id_for_outbox(event),
                                event_type: event.event_type().to_string(),
                                payload: bincode::serialize(event).unwrap_or_default(), // Handle error
                                topic: self.kafka_config.event_topic.clone(),
                                metadata: None,
                            }
                        }).collect();

                        if let Err(e) = self.outbox_repository.add_pending_messages(&mut tx, outbox_messages).await {
                            error!("Outbox write failed for account {}: {}. Attempting rollback.", account_id, e);
                            // Note: Rollback here won't affect EventStore if it committed separately.
                            tx.rollback().await.map_err(|re| AccountError::InfrastructureError(format!("DB Rollback Error: {}", re)))?;
                            return Err(AccountError::InfrastructureError(format!("Outbox write failed: {}", e)));
                        }

                        tx.commit().await.map_err(|e| AccountError::InfrastructureError(format!("DB Commit Error: {}", e)))?;

                        // Removed direct Kafka publishing. It's now responsibility of Outbox Poller.
                    }
                    Err(e) => {
                        // Event store save failed
                        tx.rollback().await.map_err(|re| AccountError::InfrastructureError(format!("DB Rollback Error after event store failure: {}", re)))?;
                        return Err(AccountError::InfrastructureError(e.to_string()));
                    }
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
