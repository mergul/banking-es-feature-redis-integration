use crate::domain::{Account, AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::{
    cdc_debezium::OutboxBatcher, // Added for batched outbox processing
    connection_pool_partitioning::PoolSelector, // Added for pool selection trait
    // cache_service::CacheServiceTrait, // No longer directly used by command handlers
    event_store::EventStoreTrait,
    kafka_abstraction::KafkaConfig, // For topic names
    OperationType,
    OutboxMessage, // Added for outbox pattern
    // projections::{AccountProjection, ProjectionStoreTrait, TransactionProjection}, // No longer directly used
    // repository::AccountRepositoryTrait, // Not used by this specific handler
    OutboxRepositoryTrait, // Added for outbox pattern
    PartitionedPools,
};
use anyhow::Result; // Result from anyhow
use async_trait::async_trait;
use bincode;
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::PgPool; // For transaction management
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info}; // Removed warn as it wasn't used after refactor
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
        // projection_store: Arc<dyn ProjectionStoreTrait>, // Removed
        // cache_service: Arc<dyn CacheServiceTrait>,       // Removed
        outbox_repository: Arc<dyn OutboxRepositoryTrait>,
        pools: Arc<PartitionedPools>,
        kafka_config: Arc<KafkaConfig>,
    ) -> Self {
        // Create OutboxBatcher for efficient batching with write pool
        let outbox_batcher = OutboxBatcher::new_default(outbox_repository, pools.clone());

        let account_command_handler = Arc::new(AccountCommandHandler::new(
            event_store,
            outbox_batcher,
            pools,
            kafka_config,
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
    pub events: Vec<AccountEvent>, // Return events for client/caller context if needed
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
    outbox_batcher: OutboxBatcher,
    pools: Arc<PartitionedPools>,
    kafka_config: Arc<KafkaConfig>,
}

impl AccountCommandHandler {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        outbox_batcher: OutboxBatcher,
        pools: Arc<PartitionedPools>,
        kafka_config: Arc<KafkaConfig>,
    ) -> Self {
        Self {
            event_store,
            outbox_batcher,
            pools,
            kafka_config,
        }
    }

    // Helper to generate event_id for outbox message
    fn generate_event_id_for_outbox(event: &AccountEvent) -> Uuid {
        match event {
            AccountEvent::MoneyDeposited { transaction_id, .. } => *transaction_id,
            AccountEvent::MoneyWithdrawn { transaction_id, .. } => *transaction_id,
            _ => Uuid::new_v4(),
        }
    }

    // OPTIMIZED: Fast outbox message creation with minimal overhead
    fn create_outbox_messages(
        &self,
        account_id: Uuid,
        events: &[AccountEvent],
    ) -> Vec<OutboxMessage> {
        // OPTIMIZED: Pre-format topic string once
        let cdc_topic = format!("{}.public.kafka_outbox_cdc", self.kafka_config.topic_prefix);

        events
            .iter()
            .filter_map(|event| {
                // OPTIMIZED: Fast serialization with minimal error handling
                match bincode::serialize(event) {
                    Ok(payload) => Some(OutboxMessage {
                        aggregate_id: account_id,
                        event_id: Self::generate_event_id_for_outbox(event),
                        event_type: event.event_type().to_string(),
                        payload,
                        topic: cdc_topic.clone(),
                        metadata: None,
                    }),
                    Err(e) => {
                        tracing::error!("Failed to serialize event for outbox: {}", e);
                        None
                    }
                }
            })
            .collect()
    }

    async fn handle_command_with_outbox(
        &self,
        account_id: Uuid,
        initial_account_version: i64, // For CreateAccount this is 0, for others it's account.version
        original_command: &AccountCommand, // To regenerate events if needed, or pass events directly
        mut account_for_state: Account,    // Account state for validation and event generation
        success_message_prefix: &str,
    ) -> Result<CommandResult, AccountError> {
        let events = account_for_state
            .handle_command(original_command)
            .map_err(|e| {
                // If handle_command itself fails (e.g. validation error), no DB transaction needed yet.
                error!(
                    "Domain command handling failed for account {}: {:?}",
                    account_id, e
                );
                e
            })?;

        if events.is_empty() {
            // No events produced, possibly a NOP command or already handled by domain logic.
            // Consider what to return. For now, assuming success if no domain error.
            info!("No events produced by command for account {}", account_id);
            return Ok(CommandResult {
                success: true,
                account_id: Some(account_id),
                events: Vec::new(),
                message: format!("{} (no state change)", success_message_prefix),
            });
        }

        let mut tx = self
            .pools
            .select_pool(OperationType::Write)
            .begin()
            .await
            .map_err(|e| {
                AccountError::InfrastructureError(format!("DB Error starting transaction: {}", e))
            })?;

        match self
            .event_store
            .save_events_in_transaction(
                &mut tx,
                account_id,
                events.clone(),
                initial_account_version,
            )
            .await
        {
            Ok(_) => {
                // Apply events to local account state for CommandResult
                for event in &events {
                    account_for_state.apply_event(event);
                }

                let outbox_messages = self.create_outbox_messages(account_id, &events);

                if !outbox_messages.is_empty() {
                    // Submit messages to the batcher instead of direct repository call
                    for message in outbox_messages {
                        if let Err(e) = self.outbox_batcher.submit(message).await {
                            error!(
                                "Outbox batcher submission failed for account {}: {}. Attempting rollback.",
                                account_id, e
                            );
                            tx.rollback().await.map_err(|re| {
                                AccountError::InfrastructureError(format!(
                                    "DB Rollback Error: {}",
                                    re
                                ))
                            })?;
                            return Err(AccountError::InfrastructureError(format!(
                                "Outbox batcher submission failed: {}",
                                e
                            )));
                        }
                    }
                } else if events
                    .iter()
                    .any(|event| bincode::serialize(event).is_err())
                {
                    // This case means all events failed serialization
                    error!("All events failed to serialize for outbox for account {}. Attempting rollback.", account_id);
                    tx.rollback().await.map_err(|re| {
                        AccountError::InfrastructureError(format!("DB Rollback Error: {}", re))
                    })?;
                    return Err(AccountError::InfrastructureError(
                        "Critical: Event serialization failed for outbox.".to_string(),
                    ));
                }

                tx.commit().await.map_err(|e| {
                    AccountError::InfrastructureError(format!("DB Commit Error: {}", e))
                })?;

                info!(
                    "{} (events saved to store & submitted to outbox batcher) for account {}",
                    success_message_prefix, account_id
                );

                Ok(CommandResult {
                    success: true,
                    account_id: Some(account_id),
                    events,
                    message: format!("{}, pending publish", success_message_prefix),
                })
            }
            Err(e) => {
                tx.rollback().await.map_err(|re| {
                    AccountError::InfrastructureError(format!(
                        "DB Rollback Error after event store failure: {}",
                        re
                    ))
                })?;
                Err(AccountError::InfrastructureError(format!(
                    "Event store error: {}",
                    e
                )))
            }
        }
    }

    // OPTIMIZED: Fast command handling with minimal overhead
    async fn handle_command_with_outbox_optimized(
        &self,
        account_id: Uuid,
        initial_account_version: i64,
        original_command: &AccountCommand,
        mut account_for_state: Account,
        success_message_prefix: &str,
    ) -> Result<CommandResult, AccountError> {
        // OPTIMIZED: Generate events with minimal error handling
        let events = account_for_state
            .handle_command(original_command)
            .map_err(|e| {
                tracing::error!(
                    "Domain command handling failed for account {}: {:?}",
                    account_id,
                    e
                );
                e
            })?;

        if events.is_empty() {
            return Ok(CommandResult {
                success: true,
                account_id: Some(account_id),
                events: Vec::new(),
                message: format!("{} (no state change)", success_message_prefix),
            });
        }

        // OPTIMIZED: Start transaction with minimal error handling
        let mut tx = self
            .pools
            .select_pool(OperationType::Write)
            .begin()
            .await
            .map_err(|e| AccountError::InfrastructureError(format!("DB Error: {}", e)))?;

        // OPTIMIZED: Save events with minimal overhead
        self.event_store
            .save_events_in_transaction(
                &mut tx,
                account_id,
                events.clone(),
                initial_account_version,
            )
            .await
            .map_err(|e| {
                tracing::error!("Failed to save events for account {}: {}", account_id, e);
                AccountError::InfrastructureError(format!("Event save error: {}", e))
            })?;

        // OPTIMIZED: Apply events once and create outbox messages efficiently
        for event in &events {
            account_for_state.apply_event(event);
        }

        let outbox_messages = self.create_outbox_messages(account_id, &events);

        // OPTIMIZED: Submit outbox messages with minimal error handling
        if !outbox_messages.is_empty() {
            for message in outbox_messages {
                if let Err(e) = self.outbox_batcher.submit(message).await {
                    tracing::error!("Outbox submission failed for account {}: {}", account_id, e);
                    tx.rollback().await.map_err(|re| {
                        AccountError::InfrastructureError(format!("Rollback error: {}", re))
                    })?;
                    return Err(AccountError::InfrastructureError(format!(
                        "Outbox error: {}",
                        e
                    )));
                }
            }
        }

        // OPTIMIZED: Commit transaction with minimal logging
        tx.commit()
            .await
            .map_err(|e| AccountError::InfrastructureError(format!("Commit error: {}", e)))?;

        tracing::debug!("{} for account {}", success_message_prefix, account_id);

        Ok(CommandResult {
            success: true,
            account_id: Some(account_id),
            events,
            message: format!("{}, pending publish", success_message_prefix),
        })
    }

    // OPTIMIZED: Fast create account handling with minimal overhead
    pub async fn handle_create_account(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        // OPTIMIZED: Direct extraction without pattern matching
        let (account_id, owner_name, initial_balance) = match command {
            AccountCommand::CreateAccount {
                account_id,
                ref owner_name,
                initial_balance,
            } => (account_id, owner_name.clone(), initial_balance),
            _ => {
                return Err(AccountError::InfrastructureError(
                    "Invalid command type for handle_create_account".to_string(),
                ))
            }
        };

        // OPTIMIZED: Create account directly without extra validation
        let account = Account::new(account_id, owner_name, initial_balance)?;

        self.handle_command_with_outbox_optimized(
            account_id,
            0,
            &command,
            account,
            "Account created successfully",
        )
        .await
    }

    // OPTIMIZED: Fast deposit money handling with minimal overhead
    pub async fn handle_deposit_money(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        // OPTIMIZED: Direct extraction without pattern matching
        let account_id = match command {
            AccountCommand::DepositMoney { account_id, .. } => account_id,
            _ => {
                return Err(AccountError::InfrastructureError(
                    "Invalid command type for handle_deposit_money".to_string(),
                ))
            }
        };

        // OPTIMIZED: Load account state and handle command in one flow
        let account = self.get_account_state(account_id).await?;

        self.handle_command_with_outbox_optimized(
            account_id,
            account.version,
            &command,
            account,
            "Deposit successful",
        )
        .await
    }

    // OPTIMIZED: Fast withdraw money handling with minimal overhead
    pub async fn handle_withdraw_money(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        // OPTIMIZED: Direct extraction without pattern matching
        let account_id = match command {
            AccountCommand::WithdrawMoney { account_id, .. } => account_id,
            _ => {
                return Err(AccountError::InfrastructureError(
                    "Invalid command type for handle_withdraw_money".to_string(),
                ))
            }
        };

        // OPTIMIZED: Load account state and handle command in one flow
        let account = self.get_account_state(account_id).await?;

        self.handle_command_with_outbox_optimized(
            account_id,
            account.version,
            &command,
            account,
            "Withdrawal successful",
        )
        .await
    }

    // OPTIMIZED: Fast close account handling with minimal overhead
    pub async fn handle_close_account(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        // OPTIMIZED: Direct extraction without pattern matching
        let (account_id, reason) = match command {
            AccountCommand::CloseAccount {
                account_id,
                ref reason,
            } => (account_id, reason.clone()),
            _ => {
                return Err(AccountError::InfrastructureError(
                    "Invalid command type for handle_close_account".to_string(),
                ))
            }
        };

        // OPTIMIZED: Load account state and handle command in one flow
        let account = self.get_account_state(account_id).await?;

        // OPTIMIZED: Pre-format message to avoid repeated formatting
        let message_prefix = format!("Account closed (reason: {})", reason);

        self.handle_command_with_outbox_optimized(
            account_id,
            account.version,
            &command,
            account,
            &message_prefix,
        )
        .await
    }

    // OPTIMIZED: Fast account state loading with reduced logging
    async fn get_account_state(&self, account_id: Uuid) -> Result<Account, AccountError> {
        let start_time = std::time::Instant::now();
        tracing::debug!(
            "🔍 get_account_state: Loading account state for {}",
            account_id
        );

        let events = self
            .event_store
            .get_events(account_id, None)
            .await
            .map_err(|e| {
                tracing::error!(
                    "🔍 get_account_state: EventStore Error for {}: {}",
                    account_id,
                    e
                );
                AccountError::InfrastructureError(format!("EventStore Error: {}", e.to_string()))
            })?;

        if events.is_empty() {
            tracing::warn!(
                "🔍 get_account_state: No events found for account {}",
                account_id
            );
            return Err(AccountError::NotFound);
        }

        // OPTIMIZED: Pre-allocate account and apply events efficiently
        let mut account = Account::default();
        account.id = account_id;

        // OPTIMIZED: Process events in a single pass with minimal logging
        for event_wrapper in events {
            let account_event: AccountEvent = bincode::deserialize(&event_wrapper.event_data)
                .map_err(|e| {
                    tracing::error!(
                        "🔍 get_account_state: Event deserialization error for account {}: {}",
                        account_id,
                        e
                    );
                    AccountError::EventDeserializationError(e.to_string())
                })?;

            account.apply_event(&account_event);
            account.version = event_wrapper.version;
        }

        tracing::debug!(
            "🔍 get_account_state: Loaded account {} (version: {}) in {:?}",
            account_id,
            account.version,
            start_time.elapsed()
        );

        Ok(account)
    }
}

#[async_trait]
impl CommandHandler<AccountCommand, CommandResult> for AccountCommandHandler {
    async fn handle(&self, command: AccountCommand) -> Result<CommandResult, AccountError> {
        let start_time = Instant::now(); // Keep overall timing if desired
        let result = match command {
            AccountCommand::CreateAccount { .. } => {
                self.handle_create_account(command.clone()).await
            }
            AccountCommand::DepositMoney { .. } => self.handle_deposit_money(command.clone()).await,
            AccountCommand::WithdrawMoney { .. } => {
                self.handle_withdraw_money(command.clone()).await
            }
            AccountCommand::CloseAccount { .. } => self.handle_close_account(command.clone()).await,
        };
        // Log timing or other general aspects here if needed, using original `command` for context
        if let Ok(ref cmd_res) = result {
            info!(
                "Command {:?} for account {:?} processed in {:.2}s. Success: {}. Message: {}",
                original_command_type(&command), // Helper to get command type string
                cmd_res.account_id.unwrap_or_default(),
                start_time.elapsed().as_secs_f64(),
                cmd_res.success,
                cmd_res.message
            );
        } else if let Err(ref e) = result {
            error!(
                "Command {:?} for account {} failed after {:.2}s: {:?}",
                original_command_type(&command),
                get_account_id_from_command(&command).unwrap_or_default(),
                start_time.elapsed().as_secs_f64(),
                e
            );
        }
        result
    }
}

// Helper to get command type for logging
fn original_command_type(command: &AccountCommand) -> &'static str {
    match command {
        AccountCommand::CreateAccount { .. } => "CreateAccount",
        AccountCommand::DepositMoney { .. } => "DepositMoney",
        AccountCommand::WithdrawMoney { .. } => "WithdrawMoney",
        AccountCommand::CloseAccount { .. } => "CloseAccount",
    }
}
// Helper to get account_id from command for logging, if available
fn get_account_id_from_command(command: &AccountCommand) -> Option<Uuid> {
    match command {
        AccountCommand::CreateAccount { account_id, .. } => Some(*account_id),
        AccountCommand::DepositMoney { account_id, .. } => Some(*account_id),
        AccountCommand::WithdrawMoney { account_id, .. } => Some(*account_id),
        AccountCommand::CloseAccount { account_id, .. } => Some(*account_id),
    }
}

// Command DTOs for API layer (assuming these are defined elsewhere or not needed directly here)
// ... (CreateAccountCommand, DepositMoneyCommand, etc. DTOs and From traits remain the same) ...
// Implement From traits for command conversion (assuming these are defined elsewhere or not needed directly here)

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

impl From<CreateAccountCommand> for AccountCommand {
    fn from(cmd: CreateAccountCommand) -> Self {
        AccountCommand::CreateAccount {
            account_id: Uuid::new_v4(), // ID generated here for Create command
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
