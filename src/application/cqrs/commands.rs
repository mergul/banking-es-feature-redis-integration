use crate::domain::{Account, AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::{
    // cache_service::CacheServiceTrait, // No longer directly used by command handlers
    event_store::EventStoreTrait,
    kafka_abstraction::KafkaConfig, // For topic names
    OutboxMessage,                  // Added for outbox pattern
    // projections::{AccountProjection, ProjectionStoreTrait, TransactionProjection}, // No longer directly used
    // repository::AccountRepositoryTrait, // Not used by this specific handler
    OutboxRepositoryTrait, // Added for outbox pattern
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
        db_pool: Arc<PgPool>,
        kafka_config: Arc<KafkaConfig>,
    ) -> Self {
        let account_command_handler = Arc::new(AccountCommandHandler::new(
            event_store,
            outbox_repository,
            db_pool,
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
    outbox_repository: Arc<dyn OutboxRepositoryTrait>,
    db_pool: Arc<PgPool>,
    kafka_config: Arc<KafkaConfig>,
}

impl AccountCommandHandler {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        outbox_repository: Arc<dyn OutboxRepositoryTrait>,
        db_pool: Arc<PgPool>,
        kafka_config: Arc<KafkaConfig>,
    ) -> Self {
        Self {
            event_store,
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
            _ => Uuid::new_v4(),
        }
    }

    // Helper to create outbox messages from domain events
    fn create_outbox_messages(
        &self,
        account_id: Uuid,
        events: &[AccountEvent],
    ) -> Vec<OutboxMessage> {
        events
            .iter()
            .filter_map(|event| {
                match bincode::serialize(event) {
                    Ok(payload) => Some(OutboxMessage {
                        aggregate_id: account_id,
                        event_id: Self::generate_event_id_for_outbox(event),
                        event_type: event.event_type().to_string(),
                        payload,
                        topic: self.kafka_config.event_topic.clone(),
                        metadata: None,
                    }),
                    Err(e) => {
                        error!("Failed to serialize event {:?} for outbox: {}", event, e);
                        None // Skip events that fail to serialize
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

        let mut tx = self.db_pool.begin().await.map_err(|e| {
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
                    if let Err(e) = self
                        .outbox_repository
                        .add_pending_messages(&mut tx, outbox_messages)
                        .await
                    {
                        error!(
                            "Outbox write failed for account {}: {}. Attempting rollback.",
                            account_id, e
                        );
                        tx.rollback().await.map_err(|re| {
                            AccountError::InfrastructureError(format!("DB Rollback Error: {}", re))
                        })?;
                        return Err(AccountError::InfrastructureError(format!(
                            "Outbox write failed: {}",
                            e
                        )));
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
                    "{} (events saved to store & outbox) for account {}",
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

    pub async fn handle_create_account(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        if let AccountCommand::CreateAccount {
            account_id,
            ref owner_name,
            initial_balance,
        } = command
        {
            let account = Account::new(account_id, owner_name.clone(), initial_balance)?;
            self.handle_command_with_outbox(
                account_id,
                0,
                &command,
                account,
                "Account created successfully",
            )
            .await
        } else {
            Err(AccountError::InfrastructureError(
                "Invalid command type for handle_create_account".to_string(),
            ))
        }
    }

    pub async fn handle_deposit_money(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        if let AccountCommand::DepositMoney { account_id, .. } = command {
            let account = self.get_account_state(account_id).await?; // Load aggregate
            self.handle_command_with_outbox(
                account_id,
                account.version,
                &command,
                account,
                "Deposit successful",
            )
            .await
        } else {
            Err(AccountError::InfrastructureError(
                "Invalid command type for handle_deposit_money".to_string(),
            ))
        }
    }

    pub async fn handle_withdraw_money(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        if let AccountCommand::WithdrawMoney { account_id, .. } = command {
            let account = self.get_account_state(account_id).await?; // Load aggregate
            self.handle_command_with_outbox(
                account_id,
                account.version,
                &command,
                account,
                "Withdrawal successful",
            )
            .await
        } else {
            Err(AccountError::InfrastructureError(
                "Invalid command type for handle_withdraw_money".to_string(),
            ))
        }
    }

    pub async fn handle_close_account(
        &self,
        command: AccountCommand,
    ) -> Result<CommandResult, AccountError> {
        if let AccountCommand::CloseAccount {
            account_id,
            ref reason,
        } = command
        {
            let account = self.get_account_state(account_id).await?; // Load aggregate
            let message_prefix = format!("Account closed (reason: {})", reason);
            self.handle_command_with_outbox(
                account_id,
                account.version,
                &command,
                account,
                &message_prefix,
            )
            .await
        } else {
            Err(AccountError::InfrastructureError(
                "Invalid command type for handle_close_account".to_string(),
            ))
        }
    }

    // get_account_state should ideally also use the transaction if it's part of read-modify-write
    // For now, it reads before the transaction starts. It also uses CacheService which is no longer a direct dependency.
    // This method needs to be refactored or its interaction with transactions clarified.
    // For this sketch, we assume it can fetch the state needed.
    // A proper solution would involve passing `&mut Transaction` to `get_events` or loading within the transaction.
    async fn get_account_state(&self, account_id: Uuid) -> Result<Account, AccountError> {
        // TODO: Refactor this to not use self.cache_service if it's removed,
        // and ideally, load aggregate within the same transaction as command processing.
        // For now, it relies on EventStoreTrait::get_events which doesn't take a tx.

        // Placeholder for direct cache access if AccountCommandHandler had it:
        // if let Ok(Some(cached_account)) = self.cache_service.get_account(account_id).await {
        //     return Ok(cached_account);
        // }

        let events = self
            .event_store
            .get_events(account_id, None)
            .await
            .map_err(|e| {
                AccountError::InfrastructureError(format!("EventStore Error: {}", e.to_string()))
            })?;

        if events.is_empty() {
            return Err(AccountError::NotFound);
        }

        let mut account = Account::default(); // Make sure Account::default initializes id and version correctly for rehydration
        account.id = account_id; // Explicitly set id
        for event_wrapper in events {
            let account_event: AccountEvent = bincode::deserialize(&event_wrapper.event_data)
                .map_err(|e| AccountError::EventDeserializationError(e.to_string()))?;
            account.apply_event(&account_event);
            account.version = event_wrapper.version; // Crucial: update version from stored event
        }
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
