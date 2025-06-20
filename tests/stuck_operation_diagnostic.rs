use banking_es::domain::{Account, AccountEvent};
use anyhow::Result;
use uuid::Uuid;
use bincode;

#[tokio::test]
async fn test_stuck_operation_diagnostic() -> Result<()> {
    // Test Account serialization/deserialization
    let test_account = Account {
        id: Uuid::new_v4(),
        owner_name: "Test User".to_string(),
        balance: rust_decimal::Decimal::new(10000, 2),
        is_active: true,
        version: 1,
    };
    let serialized = bincode::serialize(&test_account)?;
    let deserialized: Account = bincode::deserialize(&serialized)?;
    assert_eq!(test_account.id, deserialized.id);
    assert_eq!(test_account.owner_name, deserialized.owner_name);
    assert_eq!(test_account.balance, deserialized.balance);
    assert_eq!(test_account.is_active, deserialized.is_active);
    assert_eq!(test_account.version, deserialized.version);

    // Test AccountEvent serialization/deserialization
    let test_event = AccountEvent::AccountCreated {
        account_id: Uuid::new_v4(),
        owner_name: "Test User".to_string(),
        initial_balance: rust_decimal::Decimal::new(5000, 2),
    };
    let serialized_event = bincode::serialize(&test_event)?;
    let deserialized_event: AccountEvent = bincode::deserialize(&serialized_event)?;
    match deserialized_event {
        AccountEvent::AccountCreated { account_id: _, owner_name, initial_balance } => {
            assert_eq!(owner_name, "Test User");
            assert_eq!(initial_balance, rust_decimal::Decimal::new(5000, 2));
        },
        _ => panic!("Deserialized event is not AccountCreated"),
    }

    Ok(())
} 