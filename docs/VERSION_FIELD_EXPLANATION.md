# Version Field in AccountProjection

## Overview

The `version` field in `AccountProjection` is a crucial component for maintaining data consistency in our event-sourced banking system.

## Why Version Field is Important

### 1. Optimistic Concurrency Control

- Prevents race conditions when multiple events are processed concurrently
- Ensures that only the latest version of an account projection is updated
- Prevents data corruption from concurrent writes

### 2. Event Ordering

- Ensures events are applied in the correct sequence
- Maintains the integrity of the event stream
- Prevents out-of-order event processing

### 3. Idempotency

- Prevents duplicate event processing
- Allows safe retry of failed operations
- Maintains consistency even with network failures

### 4. Consistency Between Event Store and Projections

- Keeps projections synchronized with the event store
- Enables proper event replay and rebuilding
- Maintains data integrity across the system

## Database Migration

To add the version column to your database, run the migration script:

```bash
# Set your database URL
export DATABASE_URL="postgresql://username:password@localhost:5432/database_name"

# Run the migration
./scripts/run_migration.sh
```

## Migration Details

The migration (`migrations/20241201000001_add_version_to_account_projections.sql`) does the following:

1. **Adds version column**: `ALTER TABLE account_projections ADD COLUMN version BIGINT DEFAULT 0;`
2. **Creates index**: `CREATE INDEX idx_account_projections_version ON account_projections(version);`
3. **Adds documentation**: Comments explaining the column's purpose

## How Version Field Works

### In Command Handlers

- Each command handler checks the current version before applying events
- Events are only applied if the version matches expectations
- Version conflicts trigger optimistic concurrency control

### In Event Processors

- Event processors check projection versions before updating
- Skips processing if projection is already up-to-date
- Updates version after successful event application

### In Projections

- Version is incremented with each event applied
- Used in bulk upsert operations for conflict resolution
- Enables efficient querying and caching

## Benefits

1. **Data Integrity**: Ensures consistent state across the system
2. **Performance**: Enables efficient caching and querying
3. **Reliability**: Prevents data corruption from concurrent operations
4. **Scalability**: Supports high-throughput event processing
5. **Debugging**: Provides clear audit trail of state changes

## Best Practices

1. **Always check versions**: Before applying events, verify the current version
2. **Handle conflicts gracefully**: Implement proper error handling for version conflicts
3. **Use transactions**: Wrap version updates in database transactions
4. **Monitor version gaps**: Alert on unexpected version differences
5. **Test thoroughly**: Ensure version logic works correctly under load

## Example Usage

```rust
// In event processor
if existing_proj.version >= batch.version {
    // Skip processing - already up to date
    info!("Skipping update for account {} (current: {}, batch: {})",
          account_id, existing_proj.version, batch.version);
} else {
    // Apply events and update version
    let mut projection = existing_proj.clone();
    for event in &batch.events {
        projection = projection.apply_event(event)?;
    }
    projection.version = batch.version;
    // Save to database
}
```

This version field is essential for maintaining a robust, scalable, and consistent event-sourced banking system.
