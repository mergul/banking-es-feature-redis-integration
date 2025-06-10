use crate::domain::{AccountCommand, AccountEvent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Account {
    pub id: Uuid,
    pub owner_name: String,
    pub balance: Decimal,
    pub is_active: bool,
    pub version: i64,
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum AccountError {
    #[error("Account not found")]
    NotFound,
    #[error("Insufficient funds: available {available}, requested {requested}")]
    InsufficientFunds {
        available: Decimal,
        requested: Decimal,
    },
    #[error("Account is closed")]
    AccountClosed,
    #[error("Invalid amount: {0}")]
    InvalidAmount(Decimal),
    #[error("Event deserialization error: {0}")]
    EventDeserializationError(String),
    #[error("Infrastructure error: {0}")]
    InfrastructureError(String),
    #[error("Version conflict: expected {expected}, found {actual}")]
    VersionConflict { expected: i64, actual: i64 },
}

impl Account {
    pub fn new(
        id: Uuid,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Self, AccountError> {
        if initial_balance < Decimal::ZERO {
            return Err(AccountError::InvalidAmount(initial_balance));
        }

        Ok(Account {
            id,
            owner_name,
            balance: initial_balance,
            is_active: true,
            version: 0,
        })
    }

    pub fn apply_event(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::AccountCreated {
                owner_name,
                initial_balance,
                ..
            } => {
                self.owner_name = owner_name.clone();
                self.balance = *initial_balance;
                self.is_active = true;
            }
            AccountEvent::MoneyDeposited { amount, .. } => {
                self.balance += amount;
            }
            AccountEvent::MoneyWithdrawn { amount, .. } => {
                self.balance -= amount;
            }
            AccountEvent::AccountClosed { .. } => {
                self.is_active = false;
            }
        }
        self.version += 1;
    }

    pub fn handle_command(
        &self,
        command: &AccountCommand,
    ) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.is_active && !matches!(command, AccountCommand::CreateAccount { .. }) {
            return Err(AccountError::AccountClosed);
        }

        match command {
            AccountCommand::CreateAccount {
                account_id,
                owner_name,
                initial_balance,
            } => {
                if *initial_balance < Decimal::ZERO {
                    return Err(AccountError::InvalidAmount(*initial_balance));
                }
                Ok(vec![AccountEvent::AccountCreated {
                    account_id: *account_id,
                    owner_name: owner_name.clone(),
                    initial_balance: *initial_balance,
                }])
            }
            AccountCommand::DepositMoney { account_id, amount } => {
                if *amount <= Decimal::ZERO {
                    return Err(AccountError::InvalidAmount(*amount));
                }
                Ok(vec![AccountEvent::MoneyDeposited {
                    account_id: *account_id,
                    amount: *amount,
                    transaction_id: Uuid::new_v4(),
                }])
            }
            AccountCommand::WithdrawMoney { account_id, amount } => {
                if *amount <= Decimal::ZERO {
                    return Err(AccountError::InvalidAmount(*amount));
                }
                if self.balance < *amount {
                    return Err(AccountError::InsufficientFunds {
                        available: self.balance,
                        requested: *amount,
                    });
                }
                Ok(vec![AccountEvent::MoneyWithdrawn {
                    account_id: *account_id,
                    amount: *amount,
                    transaction_id: Uuid::new_v4(),
                }])
            }
            AccountCommand::CloseAccount { account_id, reason } => {
                Ok(vec![AccountEvent::AccountClosed {
                    account_id: *account_id,
                    reason: reason.clone(),
                }])
            }
        }
    }
}

impl Default for Account {
    fn default() -> Self {
        Account {
            id: Uuid::new_v4(),
            owner_name: String::new(),
            balance: Decimal::ZERO,
            is_active: false,
            version: 0,
        }
    }
}
