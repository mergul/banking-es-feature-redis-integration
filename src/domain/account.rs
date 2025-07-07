use crate::domain::{AccountCommand, AccountEvent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Custom module for bincode-compatible Decimal serialization
mod bincode_decimal {
    use rust_decimal::Decimal;
    use serde::de::Deserialize;
    use serde::{self, Deserializer, Serializer};

    pub fn serialize<S>(decimal: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as string to avoid precision issues
        serializer.serialize_str(&decimal.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<Decimal>().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Account {
    pub id: Uuid,
    pub owner_name: String,
    #[serde(with = "bincode_decimal")]
    pub balance: Decimal,
    pub is_active: bool,
    pub version: i64,
}

#[derive(Debug, Clone)]
pub enum AccountError {
    NotFound,
    InsufficientFunds {
        available: Decimal,
        requested: Decimal,
    },
    AccountClosed,
    InvalidAmount(Decimal),
    EventDeserializationError(String),
    InfrastructureError(String),
    VersionConflict {
        expected: i64,
        actual: i64,
    },
}

impl std::fmt::Display for AccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountError::NotFound => f.write_str("Account not found"),
            AccountError::InsufficientFunds {
                available,
                requested,
            } => f
                .write_str("Insufficient funds: available ")
                .and_then(|_| f.write_str(&available.to_string()))
                .and_then(|_| f.write_str(", requested "))
                .and_then(|_| f.write_str(&requested.to_string())),
            AccountError::AccountClosed => f.write_str("Account is closed"),
            AccountError::InvalidAmount(amount) => f
                .write_str("Invalid amount: ")
                .and_then(|_| f.write_str(&amount.to_string())),
            AccountError::EventDeserializationError(msg) => f
                .write_str("Event deserialization error: ")
                .and_then(|_| f.write_str(msg)),
            AccountError::InfrastructureError(msg) => f
                .write_str("Infrastructure error: ")
                .and_then(|_| f.write_str(msg)),
            AccountError::VersionConflict { expected, actual } => f
                .write_str("Version conflict: expected ")
                .and_then(|_| f.write_str(&expected.to_string()))
                .and_then(|_| f.write_str(", found "))
                .and_then(|_| f.write_str(&actual.to_string())),
        }
    }
}

impl std::error::Error for AccountError {}

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
                self.version = 0;
            }
            AccountEvent::MoneyDeposited { amount, .. } => {
                self.balance += amount;
                self.version += 1;
            }
            AccountEvent::MoneyWithdrawn { amount, .. } => {
                self.balance -= amount;
                self.version += 1;
            }
            AccountEvent::AccountClosed { .. } => {
                self.is_active = false;
                self.version += 1;
            }
        }
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

impl std::fmt::Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Account(id: ")
            .and_then(|_| f.write_str(&self.id.to_string()))
            .and_then(|_| f.write_str(", owner: "))
            .and_then(|_| f.write_str(&self.owner_name))
            .and_then(|_| f.write_str(", balance: "))
            .and_then(|_| f.write_str(&self.balance.to_string()))
            .and_then(|_| f.write_str(")"))
    }
}
