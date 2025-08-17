use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

/// CDC Event Debug Script
/// Bu script CDC event'lerinin gerçek yapısını analiz eder
#[tokio::main]
async fn main() {
    println!("🔍 CDC Event Debug Script Başlatılıyor...");

    // Test CDC event'leri oluştur
    let test_events = create_test_cdc_events();

    for (i, event) in test_events.iter().enumerate() {
        println!("\n📊 Test Event {}: {:?}", i + 1, event);

        // Event'i parse et
        match parse_cdc_event(event) {
            Ok(parsed_event) => {
                println!("✅ Successfully parsed event:");
                println!("   Event ID: {}", parsed_event.event_id);
                println!("   Aggregate ID: {}", parsed_event.aggregate_id);
                println!("   Event Type: {}", parsed_event.event_type);
                println!("   Domain Event: {:?}", parsed_event.domain_event);

                // Transaction projection oluştur
                if let Some(domain_event) = &parsed_event.domain_event {
                    match domain_event {
                        crate::domain::AccountEvent::MoneyDeposited { amount, .. } => {
                            println!(
                                "💰 Would create MoneyDeposited transaction projection: amount={}",
                                amount
                            );
                        }
                        crate::domain::AccountEvent::MoneyWithdrawn { amount, .. } => {
                            println!(
                                "💰 Would create MoneyWithdrawn transaction projection: amount={}",
                                amount
                            );
                        }
                        _ => {
                            println!("⏭️ Non-transaction event, no transaction projection needed");
                        }
                    }
                }
            }
            Err(e) => {
                println!("❌ Failed to parse event: {}", e);
            }
        }
    }

    println!("\n🎯 Debug tamamlandı!");
}

/// Test CDC event'leri oluştur
fn create_test_cdc_events() -> Vec<Value> {
    let mut events = Vec::new();

    // Test 1: MoneyDeposited event
    let money_deposited_event = serde_json::json!({
        "payload": {
            "event_id": Uuid::new_v4().to_string(),
            "aggregate_id": Uuid::new_v4().to_string(),
            "event_type": "MoneyDeposited",
            "payload": "base64_encoded_payload_here", // Gerçek base64 payload
            "partition_key": "test_partition"
        }
    });
    events.push(money_deposited_event);

    // Test 2: MoneyWithdrawn event
    let money_withdrawn_event = serde_json::json!({
        "payload": {
            "event_id": Uuid::new_v4().to_string(),
            "aggregate_id": Uuid::new_v4().to_string(),
            "event_type": "MoneyWithdrawn",
            "payload": "base64_encoded_payload_here", // Gerçek base64 payload
            "partition_key": "test_partition"
        }
    });
    events.push(money_withdrawn_event);

    // Test 3: AccountCreated event (non-transaction)
    let account_created_event = serde_json::json!({
        "payload": {
            "event_id": Uuid::new_v4().to_string(),
            "aggregate_id": Uuid::new_v4().to_string(),
            "event_type": "AccountCreated",
            "payload": "base64_encoded_payload_here", // Gerçek base64 payload
            "partition_key": "test_partition"
        }
    });
    events.push(account_created_event);

    events
}

/// CDC event'ini parse et
fn parse_cdc_event(cdc_event: &Value) -> Result<ParsedEvent, Box<dyn std::error::Error>> {
    // Event data'yı al
    let event_data = cdc_event.get("payload").ok_or("Missing payload field")?;

    // Event ID'yi al
    let event_id = event_data
        .get("event_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or("Invalid event_id")?;

    // Aggregate ID'yi al
    let aggregate_id = event_data
        .get("aggregate_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or("Invalid aggregate_id")?;

    // Event type'ını al
    let event_type = event_data
        .get("event_type")
        .and_then(|v| v.as_str())
        .ok_or("Missing event_type")?
        .to_string();

    // Payload'ı al
    let payload_str = event_data
        .get("payload")
        .and_then(|v| v.as_str())
        .ok_or("Missing payload")?;

    // Base64 decode (gerçek implementasyonda)
    let payload_bytes = base64::decode(payload_str)?;

    // Domain event'i deserialize et (gerçek implementasyonda)
    let domain_event = match bincode::deserialize::<crate::domain::AccountEvent>(&payload_bytes) {
        Ok(event) => Some(event),
        Err(_) => None, // Deserialization failed
    };

    Ok(ParsedEvent {
        event_id,
        aggregate_id,
        event_type,
        payload: payload_bytes,
        domain_event,
    })
}

/// Parsed event structure
#[derive(Debug)]
struct ParsedEvent {
    event_id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    payload: Vec<u8>,
    domain_event: Option<crate::domain::AccountEvent>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_event_parsing() {
        let events = create_test_cdc_events();
        assert_eq!(events.len(), 3);

        for event in events {
            let result = parse_cdc_event(&event);
            assert!(
                result.is_ok(),
                "Failed to parse CDC event: {:?}",
                result.err()
            );
        }
    }

    #[test]
    fn test_event_type_extraction() {
        let events = create_test_cdc_events();

        let event_types: Vec<String> = events
            .iter()
            .filter_map(|event| {
                event
                    .get("payload")
                    .and_then(|p| p.get("event_type"))
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert!(event_types.contains(&"MoneyDeposited".to_string()));
        assert!(event_types.contains(&"MoneyWithdrawn".to_string()));
        assert!(event_types.contains(&"AccountCreated".to_string()));
    }
}
