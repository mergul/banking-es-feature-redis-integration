use serde::{Deserialize, Serialize};
use uuid::Uuid;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccountCommand {
    CreateAccount {
        account_id: Uuid,
        owner_name: String,
        initial_balance: Decimal,
    },
    DepositMoney {
        account_id: Uuid,
        amount: Decimal,
    },
    WithdrawMoney {
        account_id: Uuid,
        amount: Decimal,
    },
    CloseAccount {
        account_id: Uuid,
        reason: String,
    },
}