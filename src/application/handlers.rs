// use crate::application::services::AccountService;
use crate::domain::{Account, AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use crate::infrastructure::repository::AccountRepositoryTrait; // Changed
use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid; // Added

#[derive(Clone)]
pub struct AccountCommandHandler {
    repository: Arc<dyn AccountRepositoryTrait>,
}

impl AccountCommandHandler {
    pub fn new(repository: Arc<dyn AccountRepositoryTrait>) -> Self {
        Self { repository }
    }

    pub async fn handle_create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let account_id = Uuid::new_v4();
        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name,
            initial_balance,
        };

        // Account::handle_command for CreateAccount is static-like, doesn't use self's state.
        let events = Account::default().handle_command(&command)?;

        // For CreateAccount, the expected version for the EventStore is 0.
        // The Account object passed to repository.save needs to reflect this.
        let temp_account_for_save = Account {
            id: account_id,
            owner_name: String::new(),
            balance: Decimal::ZERO,
            is_active: false, // Initial state before AccountCreated event is applied by store
            version: 0,       // Version before this first event
        };

        self.repository
            .save(&temp_account_for_save, events)
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        Ok(account_id)
    }

    pub async fn handle_deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<Vec<AccountEvent>, AccountError> {
        let account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::DepositMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Pass the updated account (which now has an incremented version) to save.
        // The AccountRepository's save method uses this account's version as the expected_version for optimistic locking.
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        Ok(events)
    }

    pub async fn handle_withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<Vec<AccountEvent>, AccountError> {
        let account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::WithdrawMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?;

        Ok(events)
    }

    pub async fn handle_close_account(
        &self,
        account_id: Uuid,
        reason: String,
    ) -> Result<Vec<AccountEvent>, AccountError> {
        let account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::CloseAccount { account_id, reason };
        let events = account.handle_command(&command)?;

        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))?; // Changed error mapping
        Ok(events)
    }
}

#[derive(Clone)]
pub struct AccountQueryHandler {
    repository: Arc<dyn AccountRepositoryTrait>,
}

impl AccountQueryHandler {
    pub fn new(repository: Arc<dyn AccountRepositoryTrait>) -> Self {
        Self { repository }
    }

    pub async fn get_account_by_id(
        &self,
        account_id: Uuid,
    ) -> Result<Option<Account>, AccountError> {
        self.repository.get_by_id(account_id).await
    }

    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Decimal, AccountError> {
        let account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;
        Ok(account.balance)
    }

    pub async fn is_account_active(&self, account_id: Uuid) -> Result<bool, AccountError> {
        let account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;
        Ok(account.is_active)
    }
}
