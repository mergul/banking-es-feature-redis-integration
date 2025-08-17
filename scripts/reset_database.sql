-- Database Reset Script
-- This script completely resets the database to a fresh state
-- Run this script to start with a completely clean database

-- Disable foreign key checks temporarily
SET session_replication_role = replica;

-- Drop all tables in the correct order (dependencies first)
DROP TABLE IF EXISTS 
    kafka_outbox_cdc CASCADE,
    transactions CASCADE,
    accounts CASCADE,
    snapshots CASCADE,
    events_2025_01 CASCADE,
    events_2025_02 CASCADE,
    events_2025_03 CASCADE,
    events_2025_04 CASCADE,
    events_2025_05 CASCADE,
    events_2025_06 CASCADE,
    events_2025_07 CASCADE,
    events_2025_08 CASCADE,
    events_2025_09 CASCADE,
    events_2025_10 CASCADE,
    events_2025_11 CASCADE,
    events_2025_12 CASCADE,
    events CASCADE,
    users CASCADE;

-- Drop all sequences
DROP SEQUENCE IF EXISTS 
    events_id_seq,
    accounts_id_seq,
    transactions_id_seq,
    snapshots_id_seq,
    kafka_outbox_cdc_id_seq;

-- Drop all indexes (in case they exist)
DROP INDEX IF EXISTS 
    idx_events_aggregate_version,
    idx_events_timestamp,
    idx_events_event_type,
    idx_events_recent,
    idx_events_data_gin,
    idx_accounts_owner_name,
    idx_transactions_account_id,
    idx_transactions_timestamp,
    idx_snapshots_aggregate_id,
    idx_kafka_outbox_cdc_topic,
    idx_kafka_outbox_cdc_status,
    idx_kafka_outbox_cdc_aggregate_id,
    idx_kafka_outbox_cdc_event_id,
    idx_kafka_outbox_cdc_created_at;

-- Drop all constraints
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT conname FROM pg_constraint WHERE conrelid = 'events'::regclass) LOOP
        EXECUTE 'ALTER TABLE events DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
    END LOOP;
    
    FOR r IN (SELECT conname FROM pg_constraint WHERE conrelid = 'accounts'::regclass) LOOP
        EXECUTE 'ALTER TABLE accounts DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
    END LOOP;
    
    FOR r IN (SELECT conname FROM pg_constraint WHERE conrelid = 'transactions'::regclass) LOOP
        EXECUTE 'ALTER TABLE transactions DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
    END LOOP;
    
    FOR r IN (SELECT conname FROM pg_constraint WHERE conrelid = 'snapshots'::regclass) LOOP
        EXECUTE 'ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
    END LOOP;
    
    FOR r IN (SELECT conname FROM pg_constraint WHERE conrelid = 'kafka_outbox'::regclass) LOOP
        EXECUTE 'ALTER TABLE kafka_outbox DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
    END LOOP;
    

    
    FOR r IN (SELECT conname FROM pg_constraint WHERE conrelid = 'kafka_outbox_cdc'::regclass) LOOP
        EXECUTE 'ALTER TABLE kafka_outbox_cdc DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
    END LOOP;
END $$;

-- Drop all functions and procedures
DROP FUNCTION IF EXISTS 
    create_event_partition(text),
    cleanup_old_events(),
    update_account_balance(),
    process_kafka_outbox(),
    process_cdc_outbox();

-- Drop all triggers
DROP TRIGGER IF EXISTS 
    trigger_create_event_partition ON events,
    trigger_cleanup_old_events ON events,
    trigger_update_account_balance ON events,
    trigger_process_kafka_outbox ON kafka_outbox,
    trigger_process_cdc_outbox ON cdc_outbox;

-- Drop all views
DROP VIEW IF EXISTS 
    account_balances,
    recent_transactions,
    event_summary,
    outbox_summary;

-- Drop all materialized views
DROP MATERIALIZED VIEW IF EXISTS 
    account_balances_mv,
    transaction_summary_mv,
    event_analytics_mv;

-- Re-enable foreign key checks
SET session_replication_role = DEFAULT;

-- Vacuum and analyze to clean up
VACUUM FULL;
ANALYZE;

-- Print completion message
DO $$
BEGIN
    RAISE NOTICE 'Database reset completed successfully!';
    RAISE NOTICE 'All tables, indexes, constraints, and data have been removed.';
    RAISE NOTICE 'Run your migration scripts to recreate the schema.';
END $$; 