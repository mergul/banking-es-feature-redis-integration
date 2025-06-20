use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

// Custom module for bincode-compatible Decimal serialization
mod bincode_decimal {
    use rust_decimal::Decimal;
    use serde::{self, Serializer, Deserializer};
    use serde::de::Deserialize;

    pub fn serialize<S>(decimal: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        // Serialize as string to avoid precision issues
        serializer.serialize_str(&decimal.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        s.parse::<Decimal>().map_err(serde::de::Error::custom)
    }
}

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
pub enum AccountEvent {
    AccountCreated {
        account_id: Uuid,
        owner_name: String,
        #[serde(with = "bincode_decimal")]
        initial_balance: Decimal,
    },
    MoneyDeposited {
        account_id: Uuid,
        #[serde(with = "bincode_decimal")]
        amount: Decimal,
        transaction_id: Uuid,
    },
    MoneyWithdrawn {
        account_id: Uuid,
        #[serde(with = "bincode_decimal")]
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