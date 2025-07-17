#!/bin/bash

# Enhanced Database Cleanup Script for Banking ES
# This script provides comprehensive cleanup options for the PostgreSQL database

set -e

# Configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-banking_es}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-Francisco1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
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

log_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

log_debug() {
    echo -e "${CYAN}[DEBUG]${NC} $1"
}

# Function to run SQL command with better error handling
run_sql() {
    local sql="$1"
    local description="$2"
    local suppress_output="${3:-false}"
    
    log_info "Running: $description"
    
    if [ "$suppress_output" = "true" ]; then
        PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$sql" >/dev/null 2>&1 || {
            log_error "Failed to execute: $description"
            return 1
        }
    else
        PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$sql" || {
            log_error "Failed to execute: $description"
            return 1
        }
    fi
}

# Function to check if table exists
table_exists() {
    local table_name="$1"
    local result
    result=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$table_name');" 2>/dev/null || echo "f")
    echo "$result" | tr -d ' '
}

# Function to get table row count
get_table_count() {
    local table_name="$1"
    local result
    result=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM $table_name;" 2>/dev/null || echo "0")
    echo "$result" | tr -d ' '
}

# Function to show database status
show_database_status() {
    log_step "Current Database Status"
    
    local tables=(
        "kafka_outbox"
        "kafka_outbox_cdc"
        "events"
        "accounts"
        "transactions"
        "users"
        "account_projections"
        "transaction_projections"
        "schema_changes"
    )
    
    local total_rows=0
    
    for table in "${tables[@]}"; do
        if [ "$(table_exists "$table")" = "t" ]; then
            local count
            count=$(get_table_count "$table")
            log_info "Table $table: $count rows"
            total_rows=$((total_rows + count))
        else
            log_info "Table $table: does not exist"
        fi
    done
    
    log_info "Total rows across all tables: $total_rows"
}

# Function to cleanup CDC outbox and related data
cleanup_cdc_data() {
    log_step "Cleaning up CDC and Outbox Data"
    
    # Clean up CDC outbox table
    if [ "$(table_exists 'kafka_outbox_cdc')" = "t" ]; then
        run_sql "TRUNCATE TABLE kafka_outbox_cdc RESTART IDENTITY CASCADE;" "Cleaning kafka_outbox_cdc table"
        log_success "Cleaned kafka_outbox_cdc table"
    fi
    
    # Clean up legacy outbox table
    if [ "$(table_exists 'kafka_outbox')" = "t" ]; then
        run_sql "TRUNCATE TABLE kafka_outbox RESTART IDENTITY CASCADE;" "Cleaning kafka_outbox table"
        log_success "Cleaned kafka_outbox table"
    fi
    
    # Clean up schema changes table
    if [ "$(table_exists 'schema_changes')" = "t" ]; then
        run_sql "TRUNCATE TABLE schema_changes RESTART IDENTITY CASCADE;" "Cleaning schema_changes table"
        log_success "Cleaned schema_changes table"
    fi
}

# Function to cleanup domain data
cleanup_domain_data() {
    log_step "Cleaning up Domain Data"
    
    # Clean up events table
    if [ "$(table_exists 'events')" = "t" ]; then
        run_sql "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" "Cleaning events table"
        log_success "Cleaned events table"
    fi
    
    # Clean up accounts table
    if [ "$(table_exists 'accounts')" = "t" ]; then
        run_sql "TRUNCATE TABLE accounts RESTART IDENTITY CASCADE;" "Cleaning accounts table"
        log_success "Cleaned accounts table"
    fi
    
    # Clean up transactions table
    if [ "$(table_exists 'transactions')" = "t" ]; then
        run_sql "TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;" "Cleaning transactions table"
        log_success "Cleaned transactions table"
    fi
    
    # Clean up users table
    if [ "$(table_exists 'users')" = "t" ]; then
        run_sql "TRUNCATE TABLE users RESTART IDENTITY CASCADE;" "Cleaning users table"
        log_success "Cleaned users table"
    fi
}

# Function to cleanup projection data
cleanup_projection_data() {
    log_step "Cleaning up Projection Data"
    
    # Clean up account projections
    if [ "$(table_exists 'account_projections')" = "t" ]; then
        run_sql "TRUNCATE TABLE account_projections RESTART IDENTITY CASCADE;" "Cleaning account_projections table"
        log_success "Cleaned account_projections table"
    fi
    
    # Clean up transaction projections
    if [ "$(table_exists 'transaction_projections')" = "t" ]; then
        run_sql "TRUNCATE TABLE transaction_projections RESTART IDENTITY CASCADE;" "Cleaning transaction_projections table"
        log_success "Cleaned transaction_projections table"
    fi
}

# Function to cleanup functions and views
cleanup_functions_and_views() {
    log_step "Cleaning up Functions and Views"
    
    # Drop migration functions
    run_sql "DROP FUNCTION IF EXISTS migrate_outbox_to_cdc() CASCADE;" "Dropping migration function" true
    run_sql "DROP FUNCTION IF EXISTS get_outbox_migration_stats() CASCADE;" "Dropping stats function" true
    run_sql "DROP FUNCTION IF EXISTS cleanup_old_outbox_after_migration() CASCADE;" "Dropping cleanup function" true
    
    # Drop views
    run_sql "DROP VIEW IF EXISTS outbox_migration_status CASCADE;" "Dropping migration status view" true
    run_sql "DROP VIEW IF EXISTS cdc_status CASCADE;" "Dropping CDC status view" true
    
    # Drop other functions
    run_sql "DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;" "Dropping update function" true
    
    log_success "Cleaned up functions and views"
}

# Function to reset sequences
reset_sequences() {
    log_step "Resetting Sequences"
    
    local sequences=(
        "events_id_seq"
        "accounts_id_seq"
        "transactions_id_seq"
        "users_id_seq"
        "kafka_outbox_id_seq"
        "kafka_outbox_cdc_id_seq"
    )
    
    for seq in "${sequences[@]}"; do
        run_sql "SELECT setval('$seq', 1, false);" "Resetting sequence $seq" true 2>/dev/null || true
    done
    
    log_success "Reset all sequences"
}

# Function to cleanup Debezium replication slots
cleanup_debezium_slots() {
    log_step "Cleaning up Debezium Replication Slots"
    
    # Get list of replication slots
    local slots
    slots=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '%debezium%' OR slot_name LIKE '%banking%';" 2>/dev/null || echo "")
    
    if [ -n "$slots" ]; then
        echo "$slots" | while read -r slot; do
            slot=$(echo "$slot" | tr -d ' ')
            if [ -n "$slot" ]; then
                log_info "Dropping replication slot: $slot"
                run_sql "SELECT pg_drop_replication_slot('$slot');" "Dropping replication slot $slot" true 2>/dev/null || true
            fi
        done
        log_success "Cleaned up replication slots"
    else
        log_info "No Debezium replication slots found"
    fi
}

# Function to cleanup publications
cleanup_publications() {
    log_step "Cleaning up Publications"
    
    # Get list of publications
    local publications
    publications=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT pubname FROM pg_publication WHERE pubname LIKE '%debezium%' OR pubname LIKE '%banking%';" 2>/dev/null || echo "")
    
    if [ -n "$publications" ]; then
        echo "$publications" | while read -r pub; do
            pub=$(echo "$pub" | tr -d ' ')
            if [ -n "$pub" ]; then
                log_info "Dropping publication: $pub"
                run_sql "DROP PUBLICATION IF EXISTS $pub;" "Dropping publication $pub" true 2>/dev/null || true
            fi
        done
        log_success "Cleaned up publications"
    else
        log_info "No Debezium publications found"
    fi
}

# Main cleanup function
cleanup_database() {
    log_step "Starting Enhanced Database Cleanup"
    
    # Show initial status
    show_database_status
    
    # Confirm cleanup
    log_warning "This will DELETE ALL DATA from the $DB_NAME database!"
    read -p "Are you absolutely sure you want to continue? Type 'YES' to confirm: " -r
    if [ "$REPLY" != "YES" ]; then
        log_info "Cleanup cancelled by user."
        exit 0
    fi
    
    # Disable triggers temporarily
    log_info "Disabling triggers..."
    run_sql "SET session_replication_role = replica;" "Disabling triggers" true
    
    # Perform cleanup operations
    cleanup_cdc_data
    cleanup_domain_data
    cleanup_projection_data
    cleanup_functions_and_views
    reset_sequences
    cleanup_debezium_slots
    cleanup_publications
    
    # Re-enable triggers
    log_info "Re-enabling triggers..."
    run_sql "SET session_replication_role = DEFAULT;" "Re-enabling triggers" true
    
    # Show final status
    log_step "Final Database Status"
    show_database_status
    
    log_success "Enhanced database cleanup completed successfully!"
}

# Function to drop all tables (nuclear option)
drop_all_tables() {
    log_warning "NUCLEAR OPTION: This will DROP ALL TABLES from the database!"
    read -p "Are you absolutely sure? Type 'DROP ALL' to confirm: " -r
    if [ "$REPLY" != "DROP ALL" ]; then
        log_info "Nuclear cleanup cancelled by user."
        exit 0
    fi
    
    log_step "Dropping all tables..."
    
    # Get all tables in the database
    local tables
    tables=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public';" 2>/dev/null || echo "")
    
    if [ -n "$tables" ]; then
        echo "$tables" | while read -r table; do
            table=$(echo "$table" | tr -d ' ')
            if [ -n "$table" ]; then
                log_info "Dropping table: $table"
                run_sql "DROP TABLE IF EXISTS $table CASCADE;" "Dropping table $table" true
            fi
        done
    fi
    
    # Drop all functions
    log_info "Dropping all functions..."
    run_sql "DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;" "Dropping update function" true
    
    # Drop all views
    log_info "Dropping all views..."
    local views
    views=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT viewname FROM pg_views WHERE schemaname = 'public';" 2>/dev/null || echo "")
    
    if [ -n "$views" ]; then
        echo "$views" | while read -r view; do
            view=$(echo "$view" | tr -d ' ')
            if [ -n "$view" ]; then
                log_info "Dropping view: $view"
                run_sql "DROP VIEW IF EXISTS $view CASCADE;" "Dropping view $view" true
            fi
        done
    fi
    
    log_success "All tables, functions, and views dropped!"
}

# Function to show help
show_help() {
    echo "Enhanced Database Cleanup Script"
    echo ""
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  cleanup    - Clean up all data (truncate tables, reset sequences)"
    echo "  drop-all   - Nuclear option: drop all tables, functions, and views"
    echo "  status     - Show current database status"
    echo "  help       - Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  DB_HOST     - Database host (default: localhost)"
    echo "  DB_PORT     - Database port (default: 5432)"
    echo "  DB_NAME     - Database name (default: banking_es)"
    echo "  DB_USER     - Database user (default: postgres)"
    echo "  DB_PASSWORD - Database password (default: Francisco1)"
    echo ""
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
        show_database_status
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        log_error "Unknown option: $1"
        show_help
        exit 1
        ;;
esac 