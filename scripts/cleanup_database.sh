#!/bin/bash

# Database Cleanup Script
# This script cleans up all data in the PostgreSQL database

set -e

# Configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-banking_es}"
DB_USER="${DB_USER:-postgres}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to run SQL command
run_sql() {
    sql="$1"
    description="$2"
    
    log_info "Running: $description"
    sudo -u postgres psql -d "$DB_NAME" -c "$sql"
}

# Function to check if table exists
table_exists() {
    table_name="$1"
    result=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$table_name');")
    echo "$result" | tr -d ' '
}

# Function to get table row count
get_table_count() {
    table_name="$1"
    result=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM $table_name;" 2>/dev/null || echo "0")
    echo "$result" | tr -d ' '
}

# Main cleanup function
cleanup_database() {
    log_info "Starting database cleanup..."
    
    # Step 1: Show current state
    log_info "Step 1: Current database state..."
    
    tables=(
        "kafka_outbox"
        "kafka_outbox_cdc"
        "events"
        "accounts"
        "transactions"
        "users"
        "account_projections"
        "transaction_projections"
    )
    
    for table in "${tables[@]}"; do
        if [ "$(table_exists "$table")" = "t" ]; then
            count=$(get_table_count "$table")
            log_info "Table $table: $count rows"
        else
            log_info "Table $table: does not exist"
        fi
    done
    
    # Step 2: Confirm cleanup
    log_warning "This will DELETE ALL DATA from the banking_es database!"
    read -p "Are you absolutely sure you want to continue? Type 'YES' to confirm: " -r
    if [ "$REPLY" != "YES" ]; then
        log_info "Cleanup cancelled by user."
        exit 0
    fi
    
    # Step 3: Disable triggers temporarily
    log_info "Step 3: Disabling triggers..."
    run_sql "SET session_replication_role = replica;" "Disabling triggers"
    
    # Step 4: Clean up tables
    log_info "Step 4: Cleaning up tables..."
    
    # Clean up outbox tables
    if [ "$(table_exists 'kafka_outbox')" = "t" ]; then
        run_sql "TRUNCATE TABLE kafka_outbox RESTART IDENTITY CASCADE;" "Cleaning kafka_outbox"
        log_success "Cleaned kafka_outbox table"
    fi
    
    if [ "$(table_exists 'kafka_outbox_cdc')" = "t" ]; then
        run_sql "TRUNCATE TABLE kafka_outbox_cdc RESTART IDENTITY CASCADE;" "Cleaning kafka_outbox_cdc"
        log_success "Cleaned kafka_outbox_cdc table"
    fi
    
    # Clean up projection tables
    if [ "$(table_exists 'account_projections')" = "t" ]; then
        run_sql "TRUNCATE TABLE account_projections RESTART IDENTITY CASCADE;" "Cleaning account_projections"
        log_success "Cleaned account_projections table"
    fi
    
    if [ "$(table_exists 'transaction_projections')" = "t" ]; then
        run_sql "TRUNCATE TABLE transaction_projections RESTART IDENTITY CASCADE;" "Cleaning transaction_projections"
        log_success "Cleaned transaction_projections table"
    fi
    
    # Clean up domain tables
    if [ "$(table_exists 'transactions')" = "t" ]; then
        run_sql "TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;" "Cleaning transactions"
        log_success "Cleaned transactions table"
    fi
    
    if [ "$(table_exists 'accounts')" = "t" ]; then
        run_sql "TRUNCATE TABLE accounts RESTART IDENTITY CASCADE;" "Cleaning accounts"
        log_success "Cleaned accounts table"
    fi
    
    if [ "$(table_exists 'events')" = "t" ]; then
        run_sql "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" "Cleaning events"
        log_success "Cleaned events table"
    fi
    
    if [ "$(table_exists 'users')" = "t" ]; then
        run_sql "TRUNCATE TABLE users RESTART IDENTITY CASCADE;" "Cleaning users"
        log_success "Cleaned users table"
    fi
    
    # Step 5: Re-enable triggers
    log_info "Step 5: Re-enabling triggers..."
    run_sql "SET session_replication_role = DEFAULT;" "Re-enabling triggers"
    
    # Step 6: Reset sequences
    log_info "Step 6: Resetting sequences..."
    run_sql "SELECT setval(pg_get_serial_sequence('events', 'id'), 1, false);" "Resetting events sequence" 2>/dev/null || true
    run_sql "SELECT setval(pg_get_serial_sequence('accounts', 'id'), 1, false);" "Resetting accounts sequence" 2>/dev/null || true
    run_sql "SELECT setval(pg_get_serial_sequence('transactions', 'id'), 1, false);" "Resetting transactions sequence" 2>/dev/null || true
    run_sql "SELECT setval(pg_get_serial_sequence('users', 'id'), 1, false);" "Resetting users sequence" 2>/dev/null || true
    
    # Step 7: Verify cleanup
    log_info "Step 7: Verifying cleanup..."
    for table in "${tables[@]}"; do
        if [ "$(table_exists "$table")" = "t" ]; then
            count=$(get_table_count "$table")
            if [ "$count" = "0" ]; then
                log_success "Table $table: cleaned (0 rows)"
            else
                log_warning "Table $table: still has $count rows"
            fi
        fi
    done
    
    # Step 8: Clean up functions and views
    log_info "Step 8: Cleaning up migration functions..."
    run_sql "DROP FUNCTION IF EXISTS migrate_outbox_to_cdc();" "Dropping migration function"
    run_sql "DROP FUNCTION IF EXISTS get_outbox_migration_stats();" "Dropping stats function"
    run_sql "DROP FUNCTION IF EXISTS cleanup_old_outbox_after_migration();" "Dropping cleanup function"
    run_sql "DROP VIEW IF EXISTS outbox_migration_status;" "Dropping migration status view"
    
    log_success "Database cleanup completed successfully!"
    
    # Step 9: Show final database state
    log_info "Final database state:"
    for table in "${tables[@]}"; do
        if [ "$(table_exists "$table")" = "t" ]; then
            count=$(get_table_count "$table")
            log_info "Table $table: $count rows"
        else
            log_info "Table $table: does not exist"
        fi
    done
}

# Function to drop all tables (nuclear option)
drop_all_tables() {
    log_warning "NUCLEAR OPTION: This will DROP ALL TABLES from the database!"
    read -p "Are you absolutely sure? Type 'DROP ALL' to confirm: " -r
    if [ "$REPLY" != "DROP ALL" ]; then
        log_info "Nuclear cleanup cancelled by user."
        exit 0
    fi
    
    log_info "Dropping all tables..."
    
    # Get all tables in the database
    tables=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public';" | tr -d ' ')
    
    for table in $tables; do
        if [ -n "$table" ] && [ "$table" != "" ]; then
            log_info "Dropping table: $table"
            run_sql "DROP TABLE IF EXISTS $table CASCADE;" "Dropping table $table"
        fi
    done
    
    # Drop all functions
    log_info "Dropping all functions..."
    run_sql "DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;" "Dropping update function"
    
    # Drop all views
    log_info "Dropping all views..."
    views=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT viewname FROM pg_views WHERE schemaname = 'public';" | tr -d ' ')
    
    for view in $views; do
        if [ -n "$view" ] && [ "$view" != "" ]; then
            log_info "Dropping view: $view"
            run_sql "DROP VIEW IF EXISTS $view CASCADE;" "Dropping view $view"
        fi
    done
    
    log_success "All tables, functions, and views dropped!"
}

# Main script logic
case "${1:-cleanup}" in
    "cleanup")
        cleanup_database
        ;;
    "drop-all")
        drop_all_tables
        ;;
    "status")
        log_info "Database status:"
        tables=(
            "kafka_outbox"
            "kafka_outbox_cdc"
            "events"
            "accounts"
            "transactions"
            "users"
            "account_projections"
            "transaction_projections"
        )
        
        for table in "${tables[@]}"; do
            if [ "$(table_exists "$table")" = "t" ]; then
                count=$(get_table_count "$table")
                log_info "Table $table: $count rows"
            else
                log_info "Table $table: does not exist"
            fi
        done
        ;;
    *)
        echo "Usage: $0 {cleanup|drop-all|status}"
        echo "  cleanup  - Clean all data from tables (default)"
        echo "  drop-all - Drop all tables, functions, and views (nuclear option)"
        echo "  status   - Show current database status"
        exit 1
        ;;
esac 