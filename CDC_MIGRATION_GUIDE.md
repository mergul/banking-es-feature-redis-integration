# CDC Migration Guide

This guide explains how to migrate from the polling-based outbox pattern to the CDC (Change Data Capture) pattern using Debezium.

## Overview

The CDC migration replaces the current polling-based outbox system with a real-time CDC system that provides:

- **50-333x better latency** (milliseconds vs seconds)
- **Simplified architecture** (no status tracking)
- **Better scalability** (horizontal scaling)
- **Operational excellence** (standard tooling)

## Migration Files

### Database Migrations

1. **`migrations/013_create_cdc_outbox_table.sql`** - Creates the CDC-optimized outbox table
2. **`migrations/013_create_cdc_outbox_table.down.sql`** - Rollback script
3. **`migrations/014_migrate_outbox_to_cdc.sql`** - Data migration functions

### Deployment Script

- **`scripts/deploy_cdc_migration.sh`** - Automated deployment script

## Prerequisites

1. **PostgreSQL 10+** with logical replication enabled
2. **Debezium** (Kafka Connect with Debezium connector)
3. **Kafka** cluster
4. **psql** client installed

## Migration Steps

### Step 1: Prepare the Environment

```bash
# Set database environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=banking_es
export DB_USER=postgres
export DB_PASSWORD=your_password
```

### Step 2: Run the Migration

```bash
# Deploy the CDC migration
./scripts/deploy_cdc_migration.sh deploy

# Check migration status
./scripts/deploy_cdc_migration.sh status
```

### Step 3: Configure Debezium

1. **Get Debezium Configuration:**

   ```rust
   let cdc_manager = CDCServiceManager::new(config, repo, producer, consumer)?;
   let debezium_config = cdc_manager.get_debezium_config();
   println!("{}", serde_json::to_string_pretty(&debezium_config)?);
   ```

2. **Create Kafka Connect Connector:**
   ```bash
   curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @debezium-config.json
   ```

### Step 4: Update Application Code

1. **Replace OutboxPollingService with CDCServiceManager:**

   ```rust
   // Old: OutboxPollingService
   let outbox_poller_service = OutboxPollingService::new(...);
   tokio::spawn(outbox_poller_service.run());

   // New: CDCServiceManager
   let mut cdc_manager = CDCServiceManager::new(config, repo, producer, consumer)?;
   cdc_manager.start().await?;
   ```

2. **Update Repository Usage:**

   ```rust
   // Old: PostgresOutboxRepository
   let outbox_repo = Arc::new(PostgresOutboxRepository::new(pool));

   // New: PostgresCDCOutboxRepository
   let cdc_outbox_repo = Arc::new(PostgresCDCOutboxRepository::new(pool));
   ```

### Step 5: Monitor Migration

```sql
-- Check migration status
SELECT * FROM outbox_migration_status;

-- Get detailed statistics
SELECT * FROM get_outbox_migration_stats();

-- Monitor CDC table
SELECT COUNT(*) as total_messages,
       MIN(created_at) as oldest_message,
       MAX(created_at) as newest_message
FROM kafka_outbox_cdc;
```

### Step 6: Cleanup (After Verification)

```sql
-- Clean up old outbox after confirming CDC is working
SELECT cleanup_old_outbox_after_migration();
```

## Rollback

If you need to rollback the migration:

```bash
./scripts/deploy_cdc_migration.sh rollback
```

## Performance Comparison

| Metric            | Polling Approach              | CDC Approach              | Improvement |
| ----------------- | ----------------------------- | ------------------------- | ----------- |
| **Latency**       | 5,000ms (5s)                  | 15-100ms                  | 50-333x     |
| **Database Load** | High (constant polling)       | Low (event-driven)        | Significant |
| **Scalability**   | Limited (coordination needed) | High (horizontal scaling) | Better      |
| **Complexity**    | High (status tracking)        | Low (simple inserts)      | Simpler     |
| **Monitoring**    | Custom metrics                | Standard tooling          | Better      |

## Architecture Comparison

### Before (Polling)

```
Command → Event Store → Outbox Table → Poller → Kafka → Event Store
                    ↑
               5-second delay
```

### After (CDC)

```
Command → Event Store → CDC Outbox → Debezium → Kafka → Event Store
                    ↑
               Real-time
```

## Monitoring and Health Checks

### CDC Health Check

```rust
let health_check = CDCHealthCheck::new(metrics.clone());
if health_check.is_healthy() {
    println!("CDC service is healthy");
} else {
    println!("CDC service has issues");
}
```

### Metrics

```rust
let metrics = cdc_manager.get_metrics();
println!("Events processed: {}", metrics.events_processed.load(Ordering::Relaxed));
println!("Events failed: {}", metrics.events_failed.load(Ordering::Relaxed));
```

## Troubleshooting

### Common Issues

1. **Logical Replication Not Enabled:**

   ```sql
   -- Check if logical replication is enabled
   SHOW wal_level;
   -- Should be 'logical' or 'replica'
   ```

2. **Publication Not Created:**

   ```sql
   -- Check publications
   SELECT * FROM pg_publication;
   ```

3. **Debezium Connector Issues:**
   ```bash
   # Check connector status
   curl http://localhost:8083/connectors/banking-outbox-connector/status
   ```

### Debugging Commands

```sql
-- Check CDC table structure
\d kafka_outbox_cdc

-- Check for stuck messages
SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PROCESSING';

-- Monitor CDC events
SELECT * FROM kafka_outbox_cdc ORDER BY created_at DESC LIMIT 10;
```

## Production Considerations

1. **Backup Strategy:** Ensure CDC table is included in backups
2. **Monitoring:** Set up alerts for CDC service health
3. **Scaling:** Use multiple Debezium connectors for high throughput
4. **Security:** Use proper database permissions for Debezium user
5. **Retention:** Configure cleanup policies for old CDC messages

## Migration Checklist

- [ ] Database migrations applied
- [ ] Debezium connector configured
- [ ] Application code updated
- [ ] CDC service started
- [ ] Data migration completed
- [ ] Performance verified
- [ ] Monitoring configured
- [ ] Old outbox cleaned up

## Support

For issues with the CDC migration:

1. Check the troubleshooting section above
2. Review application logs
3. Check Debezium connector logs
4. Verify database connectivity and permissions
5. Ensure logical replication is properly configured
