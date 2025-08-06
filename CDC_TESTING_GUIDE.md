# CDC Testing Guide

This guide explains why we can't use the full CDC pipeline in regular tests and how to properly test the CDC pipeline.

## 🔍 **Why We Can't Use Full CDC Pipeline in Regular Tests**

### 1. **External Dependencies**

The full CDC pipeline requires:

- **Debezium** (Kafka Connect with PostgreSQL connector)
- **Kafka** cluster
- **PostgreSQL** with logical replication enabled
- **Network connectivity** between all components

### 2. **Complex Setup Requirements**

```bash
# Full CDC setup requires:
- Kafka Connect server running
- Debezium connector configured
- PostgreSQL logical replication enabled
- Proper network configuration
- Multiple services orchestrated
```

### 3. **Test Environment Constraints**

- **Isolation**: Tests should be self-contained
- **Speed**: Tests should run quickly
- **Reliability**: Tests shouldn't fail due to external service issues
- **CI/CD**: Tests need to run in automated environments

## 🧪 **How to Properly Test the CDC Pipeline**

### **Option 1: Integration Tests with Real CDC Setup**

Create dedicated integration tests that run with the full CDC pipeline:

```rust
#[tokio::test]
#[ignore] // Ignored by default - requires full CDC setup
async fn test_real_cdc_pipeline() {
    // Check if CDC environment is available
    if !is_cdc_environment_available().await {
        tracing::warn!("⚠️ CDC environment not available, skipping real CDC test");
        return;
    }

    // Setup real CDC environment
    let context = setup_real_cdc_test_environment().await?;

    // Test the full CDC pipeline
    let account_id = context.cqrs_service.create_account("TestUser".to_string(), Decimal::new(1000, 0)).await?;

    // Wait for CDC to process the event
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify CDC metrics show processing
    let cdc_metrics = context.cdc_service_manager.get_metrics();
    let events_processed = cdc_metrics.events_processed.load(Ordering::Relaxed);

    assert!(events_processed > 0, "CDC did not process any events");

    // Verify projection was updated
    let account = context.cqrs_service.get_account(account_id).await?;
    assert!(account.is_some(), "Account projection not found after CDC processing");
}
```

**Run with:**

```bash
# Run only real CDC tests
cargo test test_real_cdc_pipeline -- --ignored

# Run all tests including real CDC tests
cargo test -- --include-ignored
```

### **Option 2: Test-Specific CDC Service (Current Approach)**

The current approach uses a test-specific CDC service that bypasses Debezium:

```rust
struct TestCDCService {
    outbox_repo: Arc<CDCOutboxRepository>,
    cache_service: Arc<dyn CacheServiceTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    db_pool: PgPool,
}

impl TestCDCService {
    async fn start_processing(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            interval.tick().await;

            // Directly query the outbox table for new messages
            match self.process_pending_outbox_messages().await {
                Ok(processed_count) => {
                    if processed_count > 0 {
                        tracing::info!("🧪 Test CDC Service: Processed {} outbox messages", processed_count);
                    }
                }
                Err(e) => {
                    tracing::error!("🧪 Test CDC Service: Error processing outbox messages: {}", e);
                }
            }
        }
    }
}
```

**Benefits:**

- ✅ **Test Isolation**: No external dependencies
- ✅ **Fast Execution**: Direct database queries
- ✅ **Reliable**: No network dependencies
- ✅ **Same Logic**: Uses same event processing logic

### **Option 3: Mocked Debezium**

Create a mock Debezium that simulates publishing to Kafka:

```rust
struct MockDebezium {
    kafka_producer: KafkaProducer,
}

impl MockDebezium {
    async fn publish_cdc_event(&self, account_id: Uuid) -> Result<()> {
        // Simulate Debezium publishing a CDC event to Kafka
        let cdc_event = serde_json::json!({
            "payload": {
                "after": {
                    "id": Uuid::new_v4(),
                    "aggregate_id": account_id,
                    "event_id": Uuid::new_v4(),
                    "event_type": "AccountCreated",
                    "payload": "base64_encoded_event_payload",
                    "topic": "banking-es.public.kafka_outbox_cdc",
                    "metadata": null,
                    "created_at": "2025-07-14T15:16:11.123Z",
                    "updated_at": "2025-07-14T15:16:11.123Z"
                }
            }
        });

        // Publish to the CDC topic
        let topic = "banking-es.public.kafka_outbox_cdc";
        let payload = serde_json::to_vec(&cdc_event)?;

        self.kafka_producer
            .send_message(topic, Some(&account_id.to_string()), &payload)
            .await?;

        Ok(())
    }
}
```

### **Option 4: Docker Compose for CDC Testing**

Create a `docker-compose.test.yml` for CDC testing:

```yaml
version: "3.8"
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: banking_es
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: Francisco1
    command: postgres -c wal_level=logical
    ports:
      - "5432:5432"

  zookeeper:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.0.1
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.0.1
    depends_on:
      - kafka
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: kafka-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: connect-status
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: false
    ports:
      - "8083:8083"
```

**Run CDC tests with:**

```bash
# Start CDC infrastructure
docker-compose -f docker-compose.test.yml up -d

# Wait for services to be ready
sleep 30

# Configure Debezium connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-config.json

# Run CDC tests
cargo test test_real_cdc_pipeline -- --ignored

# Cleanup
docker-compose -f docker-compose.test.yml down
```

## 🎯 **Recommended Testing Strategy**

### **1. Unit Tests (Current Approach)**

- Use test-specific CDC service
- Fast, reliable, isolated
- Tests the event processing logic
- Good for CI/CD

### **2. Integration Tests (Real CDC)**

- Use real Debezium and Kafka
- Tests the full CDC pipeline
- Run manually or in staging environment
- Good for end-to-end validation

### **3. Performance Tests**

- Use real CDC pipeline
- Measure latency and throughput
- Run in performance testing environment
- Good for capacity planning

## 📊 **Testing Coverage**

| Test Type         | CDC Pipeline | Debezium | Kafka | Speed   | Reliability | Coverage               |
| ----------------- | ------------ | -------- | ----- | ------- | ----------- | ---------------------- |
| Unit Tests        | ❌           | ❌       | ❌    | ⚡ Fast | 🔒 High     | Event Processing Logic |
| Integration Tests | ✅           | ✅       | ✅    | 🐌 Slow | ⚠️ Medium   | Full Pipeline          |
| Performance Tests | ✅           | ✅       | ✅    | 🐌 Slow | ⚠️ Medium   | Performance Metrics    |

## 🚀 **Running Different Test Types**

```bash
# Run unit tests (fast, isolated)
cargo test

# Run integration tests with real CDC
cargo test -- --ignored

# Run specific CDC test
cargo test test_real_cdc_pipeline -- --ignored

# Run all tests including CDC
cargo test -- --include-ignored
```

## 🔧 **Environment Setup for Real CDC Testing**

### **Prerequisites:**

1. **PostgreSQL** with logical replication enabled
2. **Kafka** cluster running
3. **Kafka Connect** with Debezium connector
4. **Network connectivity** between services

### **Configuration:**

```bash
# Enable logical replication in PostgreSQL
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
SELECT pg_reload_conf();

# Create publication for CDC table
CREATE PUBLICATION banking_cdc_publication FOR TABLE kafka_outbox_cdc;

# Configure Debezium connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-config.json
```

## 📈 **Monitoring CDC Tests**

### **Metrics to Track:**

- Events processed per second
- Processing latency
- Error rates
- Cache invalidation success
- Projection update success

### **Health Checks:**

```rust
let health_check = CDCHealthCheck::new(metrics.clone());
if health_check.is_healthy() {
    println!("CDC service is healthy");
} else {
    println!("CDC service has issues");
}
```

## 🎯 **Conclusion**

The current test-specific CDC service approach is the right choice for:

- **Unit tests** (fast, reliable, isolated)
- **CI/CD pipelines** (no external dependencies)
- **Development workflow** (quick feedback)

For **true CDC pipeline testing**, use:

- **Integration tests** with real Debezium
- **Performance tests** with full infrastructure
- **Staging environment** for end-to-end validation

This gives us the best of both worlds: fast, reliable tests for development and comprehensive CDC pipeline testing when needed.
