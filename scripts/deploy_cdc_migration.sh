#!/bin/bash

# CDC Migration Deployment Script
# This script safely deploys the CDC outbox migration

set -e

# Configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-banking_es}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-password}"

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
    local sql="$1"
    local description="$2"
    
    log_info "Running: $description"
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$sql"
}

# Function to check if table exists
table_exists() {
    local table_name="$1"
    local result=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$table_name');")
    echo "$result" | tr -d ' '
}

# Function to check migration status
check_migration_status() {
    log_info "Checking migration status..."
    
    local old_outbox_count=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PENDING';" | tr -d ' ')
    local processing_count=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM kafka_outbox WHERE status = 'PROCESSING';" | tr -d ' ')
    local cdc_count=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM kafka_outbox_cdc;" | tr -d ' ')
    
    log_info "Old outbox pending messages: $old_outbox_count"
    log_info "Old outbox processing messages: $processing_count"
    log_info "CDC outbox messages: $cdc_count"
    
    if [ "$processing_count" -gt 0 ]; then
        log_warning "There are still processing messages in the old outbox. Migration may not be safe."
        return 1
    fi
    
    return 0
}

# Main deployment function
deploy_cdc_migration() {
    log_info "Starting CDC outbox migration deployment..."
    
    # Step 1: Check prerequisites
    log_info "Step 1: Checking prerequisites..."
    
    if ! command -v psql &> /dev/null; then
        log_error "psql command not found. Please install PostgreSQL client."
        exit 1
    fi
    
    # Test database connection
    if ! PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" &> /dev/null; then
        log_error "Cannot connect to database. Please check your database configuration."
        exit 1
    fi
    
    log_success "Database connection successful"
    
    # Step 2: Check if old outbox exists
    log_info "Step 2: Checking existing outbox table..."
    
    if [ "$(table_exists 'kafka_outbox')" != "t" ]; then
        log_error "Old outbox table (kafka_outbox) does not exist. Cannot proceed with migration."
        exit 1
    fi
    
    log_success "Old outbox table found"
    
    # Step 3: Create CDC outbox table
    log_info "Step 3: Creating CDC outbox table..."
    
    if [ "$(table_exists 'kafka_outbox_cdc')" = "t" ]; then
        log_warning "CDC outbox table already exists. Skipping table creation."
    else
        run_sql "$(cat migrations/013_create_cdc_outbox_table.sql)" "Creating CDC outbox table"
        log_success "CDC outbox table created successfully"
    fi
    
    # Step 4: Check migration readiness
    log_info "Step 4: Checking migration readiness..."
    
    if ! check_migration_status; then
        log_warning "Migration may not be safe. Please ensure no messages are being processed."
        read -p "Do you want to continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Migration cancelled by user."
            exit 0
        fi
    fi
    
    # Step 5: Create migration functions
    log_info "Step 5: Creating migration functions..."
    run_sql "$(cat migrations/014_migrate_outbox_to_cdc.sql)" "Creating migration functions"
    log_success "Migration functions created"
    
    # Step 6: Perform data migration
    log_info "Step 6: Performing data migration..."
    
    local migrated_count=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT migrate_outbox_to_cdc();" | tr -d ' ')
    
    log_success "Migrated $migrated_count messages to CDC outbox"
    
    # Step 7: Verify migration
    log_info "Step 7: Verifying migration..."
    check_migration_status
    
    # Step 8: Show final status
    log_info "Step 8: Final migration status..."
    run_sql "SELECT * FROM outbox_migration_status;" "Migration status"
    
    log_success "CDC migration deployment completed successfully!"
    
    # Step 9: Provide next steps
    log_info "Next steps:"
    echo "1. Configure Debezium connector using the configuration from CDCServiceManager::get_debezium_config()"
    echo "2. Start the CDC service in your application"
    echo "3. Monitor the migration using: SELECT * FROM outbox_migration_status;"
    echo "4. Once confirmed working, you can clean up the old outbox using: SELECT cleanup_old_outbox_after_migration();"
}

# Rollback function
rollback_cdc_migration() {
    log_info "Starting CDC migration rollback..."
    
    # Check if CDC table exists
    if [ "$(table_exists 'kafka_outbox_cdc')" != "t" ]; then
        log_warning "CDC outbox table does not exist. Nothing to rollback."
        return 0
    fi
    
    # Confirm rollback
    read -p "This will delete the CDC outbox table and all its data. Are you sure? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Rollback cancelled by user."
        return 0
    fi
    
    # Run rollback migration
    run_sql "$(cat migrations/013_create_cdc_outbox_table.down.sql)" "Rolling back CDC outbox table"
    
    log_success "CDC migration rollback completed successfully!"
}

# Main script logic
case "${1:-deploy}" in
    "deploy")
        deploy_cdc_migration
        ;;
    "rollback")
        rollback_cdc_migration
        ;;
    "status")
        check_migration_status
        ;;
    *)
        echo "Usage: $0 {deploy|rollback|status}"
        echo "  deploy   - Deploy CDC migration (default)"
        echo "  rollback - Rollback CDC migration"
        echo "  status   - Check migration status"
        exit 1
        ;;
esac 