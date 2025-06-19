use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub event_data: Vec<u8>,
    pub version: i64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AccountEvent {
    AccountCreated {
        account_id: Uuid,
        owner_name: String,
        initial_balance: Decimal,
    },
    MoneyDeposited {
        account_id: Uuid,
        amount: Decimal,
        transaction_id: Uuid,
    },
    MoneyWithdrawn {
        account_id: Uuid,
        amount: Decimal,
        transaction_id: Uuid,
    },
    AccountClosed {
        account_id: Uuid,
        reason: String,
    },
}

impl AccountEvent {
    pub fn aggregate_id(&self) -> Uuid {
        match self {
            AccountEvent::AccountCreated { account_id, .. } => *account_id,
            AccountEvent::MoneyDeposited { account_id, .. } => *account_id,
            AccountEvent::MoneyWithdrawn { account_id, .. } => *account_id,
            AccountEvent::AccountClosed { account_id, .. } => *account_id,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            AccountEvent::AccountCreated { .. } => "AccountCreated",
            AccountEvent::MoneyDeposited { .. } => "MoneyDeposited",
            AccountEvent::MoneyWithdrawn { .. } => "MoneyWithdrawn",
            AccountEvent::AccountClosed { .. } => "AccountClosed",
        }
    }
}